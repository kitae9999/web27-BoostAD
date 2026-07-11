import {
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';
import { CampaignServingKafkaProducer } from '../../kafka/campaign-serving-kafka.producer';
import {
  CampaignServingOutboxEntity,
  CampaignServingPublishState,
} from './entities/campaign-serving-outbox.entity';

@Injectable()
export class CampaignServingOutboxPublisher
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(CampaignServingOutboxPublisher.name);
  private readonly enabled: boolean;
  private readonly pollMs: number;
  private readonly retryBaseMs: number;
  private running = false;
  private loopPromise: Promise<void> | null = null;

  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly producer: CampaignServingKafkaProducer,
    configService: ConfigService
  ) {
    this.enabled =
      configService.get<string>('RTB_PROJECTION_PIPELINE_ENABLED', 'false') ===
      'true';
    this.pollMs = this.positiveInt(
      configService.get<string>('RTB_SERVING_OUTBOX_POLL_MS'),
      50
    );
    this.retryBaseMs = this.positiveInt(
      configService.get<string>('RTB_SERVING_OUTBOX_RETRY_BASE_MS'),
      500
    );
  }

  async onApplicationBootstrap(): Promise<void> {
    if (!this.enabled) return;
    await this.recoverStaleClaims();
    this.running = true;
    this.loopPromise = this.runLoop();
  }

  async onApplicationShutdown(): Promise<void> {
    this.running = false;
    await this.loopPromise;
  }

  async processOnce(): Promise<'idle' | 'published' | 'failed'> {
    const outbox = await this.claimNext();
    if (!outbox) return 'idle';
    try {
      const kafkaOffset = await this.producer.publish(outbox.payload);
      await this.dataSource.getRepository(CampaignServingOutboxEntity).update(
        { id: outbox.id },
        {
          state: CampaignServingPublishState.PUBLISHED,
          publishedAt: new Date(),
          kafkaOffset,
          lockedAt: null,
          lastError: null,
        }
      );
      return 'published';
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const retryMs = Math.min(
        60_000,
        this.retryBaseMs * 2 ** Math.min(outbox.publishAttempts - 1, 7)
      );
      await this.dataSource.getRepository(CampaignServingOutboxEntity).update(
        { id: outbox.id },
        {
          state: CampaignServingPublishState.FAILED,
          availableAt: new Date(Date.now() + retryMs),
          lockedAt: null,
          lastError: message.slice(0, 65_535),
        }
      );
      this.logger.error(`Serving outbox 발행 실패: id=${outbox.id}`, error);
      return 'failed';
    }
  }

  private claimNext(): Promise<CampaignServingOutboxEntity | null> {
    return this.dataSource.transaction(async (manager) => {
      const outbox = await manager
        .getRepository(CampaignServingOutboxEntity)
        .createQueryBuilder('outbox')
        .setLock('pessimistic_write')
        .setOnLocked('skip_locked')
        .where('outbox.state IN (:...states)', {
          states: [
            CampaignServingPublishState.PENDING,
            CampaignServingPublishState.FAILED,
          ],
        })
        .andWhere('outbox.availableAt <= :now', { now: new Date() })
        .andWhere(
          `
          NOT EXISTS (
            SELECT 1 FROM CampaignServingOutbox prior
            WHERE prior.id < outbox.id
              AND prior.state <> 'PUBLISHED'
          )
        `
        )
        .orderBy('outbox.id', 'ASC')
        .getOne();
      if (!outbox) return null;
      outbox.state = CampaignServingPublishState.PUBLISHING;
      outbox.lockedAt = new Date();
      outbox.publishAttempts += 1;
      outbox.lastError = null;
      return manager.save(outbox);
    });
  }

  private async runLoop(): Promise<void> {
    while (this.running) {
      const outcome = await this.processOnce();
      if (outcome === 'idle') await this.delay(this.pollMs);
    }
  }

  private async recoverStaleClaims(): Promise<void> {
    const staleBefore = new Date(Date.now() - 60_000);
    await this.dataSource
      .getRepository(CampaignServingOutboxEntity)
      .createQueryBuilder()
      .update()
      .set({
        state: CampaignServingPublishState.FAILED,
        availableAt: new Date(),
        lockedAt: null,
        lastError: 'stale publish claim recovered after worker restart',
      })
      .where('state = :state', {
        state: CampaignServingPublishState.PUBLISHING,
      })
      .andWhere('locked_at < :staleBefore', { staleBefore })
      .execute();
  }

  private positiveInt(raw: string | undefined, fallback: number): number {
    const value = raw ? Number.parseInt(raw, 10) : fallback;
    return Number.isFinite(value) && value > 0 ? value : fallback;
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
