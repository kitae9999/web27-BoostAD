import {
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectQueue } from '@nestjs/bullmq';
import { InjectDataSource, InjectRepository } from '@nestjs/typeorm';
import { Queue } from 'bullmq';
import { randomUUID } from 'node:crypto';
import { DataSource, Repository } from 'typeorm';
import { CampaignBudgetRepository } from '../repository/campaign-budget.repository.interface';
import { CampaignSearchRepository } from '../repository/campaign-search.repository.interface';
import { CampaignEntity } from '../entities/campaign.entity';
import {
  CreditHistoryEntity,
  CreditHistoryType,
} from '../../advertiser/entities/credit-history.entity';
import { UserEntity } from '../../user/entities/user.entity';
import { EMBEDDING_QUEUE_NAME } from '../../queue/queue.names';
import type { CampaignEmbeddingJobData } from '../../queue/types/queue.type';
import {
  resolveEmbeddingProfile,
  toEmbeddingNamespace,
} from '../../rtb/ml/embedding-profile';
import {
  CampaignProjectionEventType,
  CampaignProjectionOutboxState,
} from './campaign-projection.types';
import { CampaignProjectionOutboxEntity } from './entities/campaign-projection-outbox.entity';

class ProjectionLeaseLostError extends Error {}

@Injectable()
export class CampaignProjectionWorker
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(CampaignProjectionWorker.name);
  private readonly workerId = `projection-${randomUUID()}`;
  private readonly enabled: boolean;
  private readonly deletionSettlementEnabled: boolean;
  private readonly concurrency: number;
  private readonly pollIntervalMs: number;
  private readonly leaseMs: number;
  private readonly maxAttempts: number;
  private readonly modelVersion: string;
  private stopping = false;
  private loop: Promise<void> | null = null;

  constructor(
    @InjectRepository(CampaignProjectionOutboxEntity)
    private readonly outboxRepository: Repository<CampaignProjectionOutboxEntity>,
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly budgetRepository: CampaignBudgetRepository,
    private readonly searchRepository: CampaignSearchRepository,
    @InjectQueue(EMBEDDING_QUEUE_NAME)
    private readonly embeddingQueue: Queue<CampaignEmbeddingJobData>,
    configService: ConfigService
  ) {
    const mode = configService.get<string>('CAMPAIGN_PROJECTION_MODE', 'off');
    this.enabled = mode === 'shadow' || mode === 'active';
    this.deletionSettlementEnabled = mode === 'active';
    this.concurrency = this.positiveInt(
      configService,
      'PROJECTION_WORKER_CONCURRENCY',
      8
    );
    this.pollIntervalMs = this.positiveInt(
      configService,
      'PROJECTION_WORKER_POLL_MS',
      100
    );
    this.leaseMs = this.positiveInt(
      configService,
      'PROJECTION_WORKER_LEASE_MS',
      30_000
    );
    this.maxAttempts = this.positiveInt(
      configService,
      'PROJECTION_WORKER_MAX_ATTEMPTS',
      20
    );
    this.modelVersion = resolveEmbeddingProfile(
      configService.get<string>('RTB_EMBEDDING_PROFILE')
    ).modelVersion;
  }

  onApplicationBootstrap(): void {
    if (!this.enabled) {
      this.logger.log(
        'CAMPAIGN_PROJECTION_MODE=off: worker를 시작하지 않습니다.'
      );
      return;
    }
    this.loop = this.runLoop();
  }

  async onApplicationShutdown(): Promise<void> {
    this.stopping = true;
    await this.loop;
  }

  async processAvailableOnce(): Promise<number> {
    const events = await this.claimAvailable(this.concurrency);
    await Promise.all(events.map((event) => this.processClaimed(event)));
    return events.length;
  }

  private async runLoop(): Promise<void> {
    this.logger.log(
      `Campaign Projection Worker 시작: concurrency=${this.concurrency}`
    );
    while (!this.stopping) {
      try {
        const processed = await this.processAvailableOnce();
        if (processed === 0) await this.delay(this.pollIntervalMs);
      } catch (error) {
        this.logger.error('Projection poll 실패', error);
        await this.delay(Math.max(1_000, this.pollIntervalMs));
      }
    }
  }

  private async claimAvailable(
    limit: number
  ): Promise<CampaignProjectionOutboxEntity[]> {
    return this.dataSource.transaction(async (manager) => {
      const safeLimit = Math.max(1, Math.min(100, Math.floor(limit)));
      const rows = (await manager.query(`
        SELECT candidate.id
        FROM CampaignProjectionOutbox candidate
        WHERE candidate.state IN ('PENDING', 'PROCESSING', 'WAITING', 'RETRY')
          AND candidate.available_at <= NOW(3)
          AND (candidate.locked_until IS NULL OR candidate.locked_until < NOW(3))
          AND NOT EXISTS (
            SELECT 1
            FROM CampaignProjectionOutbox prior
            WHERE prior.campaign_id = candidate.campaign_id
              AND prior.serving_version < candidate.serving_version
              AND prior.state <> 'COMPLETED'
          )
        ORDER BY candidate.id ASC
        LIMIT ${safeLimit}
        FOR UPDATE SKIP LOCKED
      `)) as Array<{ id: string | number }>;
      if (rows.length === 0) return [];

      const ids = rows.map((row) => String(row.id));
      const lockedUntil = new Date(Date.now() + this.leaseMs);
      await manager
        .getRepository(CampaignProjectionOutboxEntity)
        .createQueryBuilder()
        .update()
        .set({
          state: CampaignProjectionOutboxState.PROCESSING,
          lockedBy: this.workerId,
          lockedUntil,
        })
        .whereInIds(ids)
        .execute();
      return manager
        .getRepository(CampaignProjectionOutboxEntity)
        .createQueryBuilder('outbox')
        .where('outbox.id IN (:...ids)', { ids })
        .orderBy('outbox.id', 'ASC')
        .getMany();
    });
  }

  private async processClaimed(
    event: CampaignProjectionOutboxEntity
  ): Promise<void> {
    try {
      if (!event.budgetAppliedAt) {
        if (event.eventType === CampaignProjectionEventType.DELETE) {
          if (this.deletionSettlementEnabled && !event.deletionSettledAt) {
            const existing = await this.budgetRepository.getBudgetState(
              event.campaignId
            );
            if (!existing) {
              throw new Error(
                `삭제 정산 전 Budget projection이 없습니다: ${event.campaignId}`
              );
            }
          }
          await this.budgetRepository.applyBudgetTombstone(
            event.campaignId,
            event.servingVersion,
            event.payload.campaign
          );
        } else {
          await this.budgetRepository.applyBudgetProjection(
            event.payload.campaign
          );
        }
        await this.checkpoint(event.id, { budgetAppliedAt: new Date() });
      }

      let requiresEmbedding = false;
      if (!event.searchAppliedAt) {
        if (event.eventType === CampaignProjectionEventType.DELETE) {
          await this.searchRepository.applySearchTombstone(
            event.campaignId,
            event.servingVersion
          );
        } else {
          const result = await this.searchRepository.applySearchProjection(
            event.payload.campaign
          );
          requiresEmbedding = result.requiresEmbedding;
        }
        await this.checkpoint(event.id, { searchAppliedAt: new Date() });
      }

      if (
        event.eventType === CampaignProjectionEventType.UPSERT &&
        !event.embeddingEnqueuedAt
      ) {
        const current = await this.searchRepository.findCampaignById(
          event.campaignId
        );
        requiresEmbedding =
          requiresEmbedding ||
          !current?.indexReady ||
          current.semanticHash !== event.payload.campaign.semanticHash;
        if (requiresEmbedding) await this.enqueueEmbedding(event);
        await this.checkpoint(event.id, { embeddingEnqueuedAt: new Date() });
      }

      if (
        event.eventType === CampaignProjectionEventType.DELETE &&
        !event.deletionSettledAt
      ) {
        if (!this.deletionSettlementEnabled) {
          await this.waitForSettlement(event.id, 60_000);
          return;
        }
        const settled = await this.settleDeletion(event);
        if (!settled) {
          await this.waitForSettlement(event.id);
          return;
        }
      }

      await this.updateOwned(event.id, {
        state: CampaignProjectionOutboxState.COMPLETED,
        completedAt: new Date(),
        lockedBy: null,
        lockedUntil: null,
        lastError: null,
      });
    } catch (error) {
      await this.recordFailure(event, error);
    }
  }

  private async enqueueEmbedding(
    event: CampaignProjectionOutboxEntity
  ): Promise<void> {
    const campaign = event.payload.campaign;
    const jobId = `campaign-${campaign.id}-v${campaign.servingVersion}-m${toEmbeddingNamespace(this.modelVersion)}`;
    await this.embeddingQueue.add(
      'generate-campaign-embedding',
      {
        campaignId: campaign.id,
        servingVersion: campaign.servingVersion,
        semanticHash: campaign.semanticHash,
        modelVersion: this.modelVersion,
        title: campaign.title,
        content: campaign.content,
        tags: campaign.tags,
      },
      {
        jobId,
        attempts: 3,
        backoff: { type: 'exponential', delay: 1_000 },
        removeOnComplete: true,
        removeOnFail: false,
      }
    );
  }

  private async settleDeletion(
    event: CampaignProjectionOutboxEntity
  ): Promise<boolean> {
    const budget = await this.budgetRepository.getBudgetState(event.campaignId);
    if (!budget) {
      throw new Error(
        `삭제 정산 중 Budget projection이 없습니다: ${event.campaignId}`
      );
    }
    if (budget.totalReserved > 0) return false;

    await this.dataSource.transaction(async (manager) => {
      const lockedEvent = await manager
        .getRepository(CampaignProjectionOutboxEntity)
        .findOne({
          where: { id: event.id },
          lock: { mode: 'pessimistic_write' },
        });
      if (
        !lockedEvent ||
        lockedEvent.lockedBy !== this.workerId ||
        !lockedEvent.lockedUntil ||
        lockedEvent.lockedUntil.getTime() <= Date.now()
      ) {
        throw new ProjectionLeaseLostError(
          `Projection lease 상실: ${event.eventId}`
        );
      }

      const campaignRepo = manager.getRepository(CampaignEntity);
      const campaign = await campaignRepo.findOne({
        where: { id: event.campaignId },
        withDeleted: true,
        lock: { mode: 'pessimistic_write' },
      });
      if (!campaign) return;
      campaign.dailySpent = budget.dailySpent;
      campaign.totalSpent = budget.totalSpent;
      await campaignRepo.save(campaign);

      const operationKey = `campaign-delete-refund:${event.campaignId}:v${event.servingVersion}`;
      const historyRepo = manager.getRepository(CreditHistoryEntity);
      const alreadySettled = await historyRepo.exist({
        where: { operationKey },
      });
      const refund = Math.max(
        0,
        (campaign.totalBudget ?? 0) - campaign.totalSpent
      );
      if (!alreadySettled && refund > 0) {
        const userRepo = manager.getRepository(UserEntity);
        const user = await userRepo.findOne({
          where: { id: campaign.userId },
          lock: { mode: 'pessimistic_write' },
        });
        if (!user) throw new Error(`삭제 정산 사용자 없음: ${campaign.userId}`);
        user.balance += refund;
        await userRepo.save(user);
        await historyRepo.save({
          userId: campaign.userId,
          type: CreditHistoryType.CHARGE,
          amount: refund,
          balanceAfter: user.balance,
          campaignId: campaign.id,
          operationKey,
          description: `'${campaign.title}' 캠페인 삭제 - 남은 예산 환불`,
        });
      }
      lockedEvent.deletionSettledAt = new Date();
      lockedEvent.lockedUntil = new Date(Date.now() + this.leaseMs);
      await manager
        .getRepository(CampaignProjectionOutboxEntity)
        .save(lockedEvent);
    });
    return true;
  }

  private async checkpoint(
    id: string,
    values: Partial<CampaignProjectionOutboxEntity>
  ): Promise<void> {
    await this.updateOwned(id, values);
  }

  private async waitForSettlement(id: string, delayMs = 1_000): Promise<void> {
    await this.updateOwned(id, {
      state: CampaignProjectionOutboxState.WAITING,
      availableAt: new Date(Date.now() + delayMs),
      lockedBy: null,
      lockedUntil: null,
      lastError: null,
    });
  }

  private async recordFailure(
    event: CampaignProjectionOutboxEntity,
    error: unknown
  ): Promise<void> {
    const attempts = event.attempts + 1;
    const dead = attempts >= this.maxAttempts;
    const backoffMs = Math.min(60_000, 1_000 * 2 ** Math.min(attempts - 1, 10));
    const message =
      error instanceof Error ? (error.stack ?? error.message) : String(error);
    const result = await this.outboxRepository.update(
      { id: event.id, lockedBy: this.workerId },
      {
        state: dead
          ? CampaignProjectionOutboxState.DEAD
          : CampaignProjectionOutboxState.RETRY,
        attempts,
        availableAt: new Date(Date.now() + backoffMs),
        lockedBy: null,
        lockedUntil: null,
        lastError: message,
      }
    );
    if (result.affected !== 1) {
      this.logger.warn(
        `Projection lease가 이미 이전되어 실패 상태를 기록하지 않음: ${event.eventId}`
      );
      return;
    }
    this.logger.error(
      `Projection 실패 event=${event.eventId} attempt=${attempts}`,
      error
    );
  }

  private async updateOwned(
    id: string,
    values: Partial<CampaignProjectionOutboxEntity>
  ): Promise<void> {
    const result = await this.outboxRepository.update(
      { id, lockedBy: this.workerId },
      values.lockedUntil === undefined
        ? { ...values, lockedUntil: new Date(Date.now() + this.leaseMs) }
        : values
    );
    if (result.affected !== 1) {
      throw new ProjectionLeaseLostError(`Projection lease 상실: outbox=${id}`);
    }
  }

  private positiveInt(
    configService: ConfigService,
    key: string,
    fallback: number
  ): number {
    const value = Number(configService.get<string | number>(key, fallback));
    return Number.isInteger(value) && value > 0 ? value : fallback;
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
