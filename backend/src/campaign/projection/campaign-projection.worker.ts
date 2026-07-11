import {
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { OnEvent } from '@nestjs/event-emitter';
import { InjectDataSource } from '@nestjs/typeorm';
import { createHash } from 'node:crypto';
import { DataSource, EntityManager } from 'typeorm';
import { MLEngine } from '../../rtb/ml/mlEngine.interface';
import { buildCampaignDocumentText } from '../../rtb/ml/embedding-text';
import { CampaignEntity } from '../entities/campaign.entity';
import {
  CAMPAIGN_SERVING_EVENT_SCHEMA_VERSION,
  type CampaignServingEvent,
} from '../events/campaign-serving-event';
import { toCampaignServingDocument } from './campaign-serving-document.mapper';
import {
  CampaignOutboxState,
  CampaignProjectionOperation,
  CampaignProjectionRequestEntity,
} from './entities/campaign-projection-request.entity';
import {
  CampaignServingOutboxEntity,
  CampaignServingPublishState,
} from './entities/campaign-serving-outbox.entity';
import { CampaignServingProjectionEntity } from './entities/campaign-serving-projection.entity';

@Injectable()
export class CampaignProjectionWorker
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(CampaignProjectionWorker.name);
  private readonly enabled: boolean;
  private readonly pollMs: number;
  private readonly retryBaseMs: number;
  private running = false;
  private loopPromise: Promise<void> | null = null;

  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly mlEngine: MLEngine,
    configService: ConfigService
  ) {
    this.enabled =
      configService.get<string>('RTB_PROJECTION_PIPELINE_ENABLED', 'false') ===
      'true';
    this.pollMs = this.positiveInt(
      configService.get<string>('RTB_PROJECTION_POLL_MS'),
      100
    );
    this.retryBaseMs = this.positiveInt(
      configService.get<string>('RTB_PROJECTION_RETRY_BASE_MS'),
      1000
    );
  }

  async onApplicationBootstrap(): Promise<void> {
    if (!this.enabled) return;
    await this.recoverStaleClaims();
    if (this.mlEngine.isReady()) this.startLoop();
  }

  @OnEvent('ml.model.ready')
  onModelReady(): void {
    if (this.enabled) this.startLoop();
  }

  private startLoop(): void {
    if (this.running) return;
    this.running = true;
    this.loopPromise = this.runLoop();
    this.logger.log(`Campaign projection worker 시작: poll=${this.pollMs}ms`);
  }

  async onApplicationShutdown(): Promise<void> {
    this.running = false;
    await this.loopPromise;
  }

  async processOnce(): Promise<'idle' | 'completed' | 'failed'> {
    const request = await this.claimNext();
    if (!request) return 'idle';

    try {
      if (request.operation === CampaignProjectionOperation.DELETE) {
        await this.commitProjection(request, null);
      } else {
        const campaign = await this.dataSource
          .getRepository(CampaignEntity)
          .findOne({
            where: { id: request.campaignId },
            relations: ['tags'],
            withDeleted: true,
          });
        if (!campaign || campaign.deletedAt) {
          await this.commitProjection(request, null);
        } else {
          const existing = await this.dataSource
            .getRepository(CampaignServingProjectionEntity)
            .findOne({ where: { campaignId: campaign.id } });
          const reusable = this.canReuseEmbeddings(
            existing?.document ?? null,
            campaign
          );
          const tags: Record<string, number[]> = reusable
            ? (existing?.document?.embeddingTags ?? {})
            : {};
          if (!reusable) {
            for (const tag of campaign.tags ?? []) {
              tags[tag.name] = await this.mlEngine.getEmbedding(
                tag.name,
                'passage'
              );
            }
          }
          const document = reusable
            ? (existing?.document?.embeddingDocument ?? [])
            : await this.mlEngine.getEmbedding(
                buildCampaignDocumentText({
                  title: campaign.title,
                  content: campaign.content,
                  tags: (campaign.tags ?? []).map((tag) => tag.name),
                }),
                'passage'
              );
          await this.commitProjection(
            request,
            toCampaignServingDocument(campaign, {
              modelVersion: this.mlEngine.getModelVersion(),
              document,
              tags,
            })
          );
        }
      }
      return 'completed';
    } catch (error) {
      await this.markFailed(request, error);
      return 'failed';
    }
  }

  private async runLoop(): Promise<void> {
    while (this.running) {
      const outcome = await this.processOnce();
      if (outcome === 'idle') await this.delay(this.pollMs);
    }
  }

  private claimNext(): Promise<CampaignProjectionRequestEntity | null> {
    return this.dataSource.transaction(async (manager) => {
      const request = await manager
        .getRepository(CampaignProjectionRequestEntity)
        .createQueryBuilder('request')
        .setLock('pessimistic_write')
        .setOnLocked('skip_locked')
        .where('request.state IN (:...states)', {
          states: [CampaignOutboxState.PENDING, CampaignOutboxState.FAILED],
        })
        .andWhere('request.availableAt <= :now', { now: new Date() })
        .andWhere(
          `
          NOT EXISTS (
            SELECT 1 FROM CampaignProjectionRequestOutbox prior
            WHERE prior.campaign_id = request.campaign_id
              AND prior.id < request.id
              AND prior.state <> 'COMPLETED'
          )
        `
        )
        .orderBy('request.id', 'ASC')
        .getOne();
      if (!request) return null;
      request.state = CampaignOutboxState.PROCESSING;
      request.lockedAt = new Date();
      request.attempts += 1;
      request.lastError = null;
      return manager.save(request);
    });
  }

  private async commitProjection(
    request: CampaignProjectionRequestEntity,
    document: CampaignServingProjectionEntity['document']
  ): Promise<void> {
    await this.dataSource.transaction(async (manager) => {
      const version = request.id;
      const deleted = document === null;
      const hash = document
        ? createHash('sha256').update(JSON.stringify(document)).digest('hex')
        : null;
      await manager.getRepository(CampaignServingProjectionEntity).upsert(
        {
          campaignId: request.campaignId,
          version,
          schemaVersion: CAMPAIGN_SERVING_EVENT_SCHEMA_VERSION,
          document,
          documentHash: hash,
          deleted,
        },
        ['campaignId']
      );

      const event: CampaignServingEvent = deleted
        ? {
            schemaVersion: CAMPAIGN_SERVING_EVENT_SCHEMA_VERSION,
            eventId: `db:${version}`,
            type: 'DELETE',
            campaignId: request.campaignId,
            campaignVersion: Number(version),
            sequence: 0,
            occurredAtMs: Date.now(),
          }
        : {
            schemaVersion: CAMPAIGN_SERVING_EVENT_SCHEMA_VERSION,
            eventId: `db:${version}`,
            type: 'UPSERT',
            campaignId: request.campaignId,
            campaignVersion: Number(version),
            sequence: 0,
            occurredAtMs: Date.now(),
            campaign: document,
          };
      const outboxRepository = manager.getRepository(
        CampaignServingOutboxEntity
      );
      const outbox = await outboxRepository.save({
        campaignId: request.campaignId,
        campaignVersion: version,
        eventType: event.type,
        payload: event,
        publishAttempts: 0,
        state: CampaignServingPublishState.PENDING,
        availableAt: new Date(),
        lockedAt: null,
        publishedAt: null,
        kafkaOffset: null,
        lastError: null,
      });
      event.sequence = Number(outbox.id);
      event.eventId = `db:${outbox.id}`;
      outbox.payload = event;
      await outboxRepository.save(outbox);

      await this.completeRequest(manager, request.id);
    });
  }

  private completeRequest(
    manager: EntityManager,
    id: string
  ): Promise<unknown> {
    return manager.getRepository(CampaignProjectionRequestEntity).update(
      { id },
      {
        state: CampaignOutboxState.COMPLETED,
        lockedAt: null,
        lastError: null,
      }
    );
  }

  private async markFailed(
    request: CampaignProjectionRequestEntity,
    error: unknown
  ): Promise<void> {
    const message = error instanceof Error ? error.message : String(error);
    const retryMs = Math.min(
      60_000,
      this.retryBaseMs * 2 ** Math.min(request.attempts - 1, 6)
    );
    await this.dataSource.getRepository(CampaignProjectionRequestEntity).update(
      { id: request.id },
      {
        state: CampaignOutboxState.FAILED,
        lockedAt: null,
        lastError: message.slice(0, 65_535),
        availableAt: new Date(Date.now() + retryMs),
      }
    );
    this.logger.error(
      `Campaign projection 실패: request=${request.id}, campaign=${request.campaignId}`,
      error
    );
  }

  private async recoverStaleClaims(): Promise<void> {
    const staleBefore = new Date(Date.now() - 5 * 60_000);
    await this.dataSource
      .getRepository(CampaignProjectionRequestEntity)
      .createQueryBuilder()
      .update()
      .set({
        state: CampaignOutboxState.FAILED,
        lockedAt: null,
        availableAt: new Date(),
        lastError: 'stale projection claim recovered after worker restart',
      })
      .where('state = :state', { state: CampaignOutboxState.PROCESSING })
      .andWhere('locked_at < :staleBefore', { staleBefore })
      .execute();
  }

  private positiveInt(raw: string | undefined, fallback: number): number {
    const value = raw ? Number.parseInt(raw, 10) : fallback;
    return Number.isFinite(value) && value > 0 ? value : fallback;
  }

  private canReuseEmbeddings(
    existing: CampaignServingProjectionEntity['document'],
    campaign: CampaignEntity
  ): boolean {
    if (
      !existing ||
      existing.embeddingModelVersion !== this.mlEngine.getModelVersion() ||
      !existing.embeddingDocument?.length ||
      !existing.embeddingTags
    ) {
      return false;
    }
    const oldTags = [...(existing.tags ?? [])].sort();
    const newTags = (campaign.tags ?? []).map((tag) => tag.name).sort();
    return (
      existing.title === campaign.title &&
      existing.content === campaign.content &&
      oldTags.length === newTags.length &&
      oldTags.every((tag, index) => tag === newTags[index])
    );
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
