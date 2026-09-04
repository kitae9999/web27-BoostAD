import { ConfigService } from '@nestjs/config';
import type { Queue } from 'bullmq';
import type { DataSource, Repository } from 'typeorm';
import type { CampaignBudgetRepository } from '../repository/campaign-budget.repository.interface';
import type { CampaignSearchRepository } from '../repository/campaign-search.repository.interface';
import { CampaignEntity } from '../entities/campaign.entity';
import { CreditHistoryEntity } from '../../advertiser/entities/credit-history.entity';
import { UserEntity } from '../../user/entities/user.entity';
import { CampaignProjectionWorker } from './campaign-projection.worker';
import {
  CampaignProjectionEventType,
  CampaignProjectionOutboxState,
  type CampaignProjectionDocument,
} from './campaign-projection.types';
import { CampaignProjectionOutboxEntity } from './entities/campaign-projection-outbox.entity';

const campaign: CampaignProjectionDocument = {
  id: 'campaign-1',
  userId: 1,
  servingVersion: 3,
  title: 'title',
  content: 'content',
  image: null,
  url: 'https://example.com',
  maxCpc: 100,
  dailyBudget: 1_000,
  totalBudget: 10_000,
  dailySpent: 10,
  totalSpent: 20,
  lastResetDate: '2026-09-03T00:00:00.000Z',
  isHighIntent: false,
  status: 'ACTIVE',
  startDate: '2026-09-01T00:00:00.000Z',
  endDate: '2026-10-01T00:00:00.000Z',
  createdAt: '2026-09-01T00:00:00.000Z',
  deletedAt: null,
  tags: ['redis'],
  semanticHash: 'semantic-hash',
};

function event(
  overrides: Partial<CampaignProjectionOutboxEntity> = {}
): CampaignProjectionOutboxEntity {
  return {
    id: '11',
    eventId: 'event-1',
    campaignId: campaign.id,
    servingVersion: campaign.servingVersion,
    eventType: CampaignProjectionEventType.UPSERT,
    payload: {
      eventId: 'event-1',
      eventType: CampaignProjectionEventType.UPSERT,
      campaign,
    },
    state: CampaignProjectionOutboxState.PROCESSING,
    attempts: 0,
    availableAt: new Date(),
    lockedUntil: new Date(Date.now() + 30_000),
    lockedBy: 'worker',
    lastError: null,
    budgetAppliedAt: null,
    searchAppliedAt: null,
    embeddingEnqueuedAt: null,
    deletionSettledAt: null,
    completedAt: null,
    createdAt: new Date(),
    updatedAt: new Date(),
    ...overrides,
  };
}

function buildWorker(
  options: {
    maxAttempts?: number;
    mode?: 'off' | 'shadow' | 'active';
  } = {}
) {
  const order: string[] = [];
  const outboxRepository = {
    update: jest.fn(
      async (_criteria: unknown, values: Record<string, unknown>) => {
        const checkpoint = [
          'budgetAppliedAt',
          'searchAppliedAt',
          'embeddingEnqueuedAt',
          'deletionSettledAt',
        ].find((key) => values[key]);
        order.push(
          checkpoint ??
            `state:${String(values.state ?? CampaignProjectionOutboxState.PROCESSING)}`
        );
        return { affected: 1 };
      }
    ),
  } as unknown as Repository<CampaignProjectionOutboxEntity> & {
    update: jest.Mock;
  };
  const dataSource = {
    transaction: jest.fn(),
  } as unknown as DataSource & { transaction: jest.Mock };
  const budgetRepository = {
    applyBudgetProjection: jest.fn(async () => {
      order.push('budget');
      return true;
    }),
    applyBudgetTombstone: jest.fn(async () => {
      order.push('budget-tombstone');
      return true;
    }),
    getBudgetState: jest.fn().mockResolvedValue(null),
  } as unknown as CampaignBudgetRepository & {
    applyBudgetProjection: jest.Mock;
    applyBudgetTombstone: jest.Mock;
    getBudgetState: jest.Mock;
  };
  const searchRepository = {
    applySearchProjection: jest.fn(async () => {
      order.push('search');
      return { applied: true, requiresEmbedding: true };
    }),
    applySearchTombstone: jest.fn(async () => {
      order.push('search-tombstone');
      return true;
    }),
    findCampaignById: jest.fn().mockResolvedValue({
      servingVersion: campaign.servingVersion,
      semanticHash: campaign.semanticHash,
      indexReady: false,
    }),
  } as unknown as CampaignSearchRepository & {
    applySearchProjection: jest.Mock;
    applySearchTombstone: jest.Mock;
    findCampaignById: jest.Mock;
  };
  const embeddingQueue = {
    add: jest.fn(async () => {
      order.push('embedding');
    }),
  } as unknown as Queue & { add: jest.Mock };
  const config = {
    get: jest.fn((key: string, fallback?: unknown) => {
      if (key === 'CAMPAIGN_PROJECTION_MODE') return options.mode ?? 'active';
      if (key === 'PROJECTION_WORKER_MAX_ATTEMPTS') {
        return options.maxAttempts ?? fallback;
      }
      return fallback;
    }),
  } as unknown as ConfigService;
  const worker = new CampaignProjectionWorker(
    outboxRepository,
    dataSource,
    budgetRepository,
    searchRepository,
    embeddingQueue,
    config
  );

  return {
    worker,
    order,
    outboxRepository,
    dataSource,
    budgetRepository,
    searchRepository,
    embeddingQueue,
  };
}

async function processClaimed(
  worker: CampaignProjectionWorker,
  claimed: CampaignProjectionOutboxEntity
): Promise<void> {
  await (
    worker as unknown as {
      processClaimed(event: CampaignProjectionOutboxEntity): Promise<void>;
    }
  ).processClaimed(claimed);
}

describe('CampaignProjectionWorker', () => {
  it('applies Budget, Search and embedding enqueue in checkpoint order', async () => {
    const harness = buildWorker();

    await processClaimed(harness.worker, event());

    expect(harness.order).toEqual([
      'budget',
      'budgetAppliedAt',
      'search',
      'searchAppliedAt',
      'embedding',
      'embeddingEnqueuedAt',
      `state:${CampaignProjectionOutboxState.COMPLETED}`,
    ]);
    expect(harness.embeddingQueue.add).toHaveBeenCalledWith(
      'generate-campaign-embedding',
      expect.objectContaining({
        campaignId: campaign.id,
        servingVersion: campaign.servingVersion,
        semanticHash: campaign.semanticHash,
      }),
      expect.objectContaining({
        jobId: expect.stringContaining(`campaign-${campaign.id}-v3-`),
      })
    );
  });

  it('resumes at the first incomplete checkpoint', async () => {
    const harness = buildWorker();

    await processClaimed(
      harness.worker,
      event({ budgetAppliedAt: new Date(), searchAppliedAt: new Date() })
    );

    expect(
      harness.budgetRepository.applyBudgetProjection
    ).not.toHaveBeenCalled();
    expect(
      harness.searchRepository.applySearchProjection
    ).not.toHaveBeenCalled();
    expect(harness.embeddingQueue.add).toHaveBeenCalledTimes(1);
  });

  it('moves an exhausted retry to DEAD', async () => {
    const harness = buildWorker({ maxAttempts: 20 });
    harness.budgetRepository.applyBudgetProjection.mockRejectedValue(
      new Error('budget unavailable')
    );

    await processClaimed(harness.worker, event({ attempts: 19 }));

    expect(harness.outboxRepository.update).toHaveBeenLastCalledWith(
      expect.objectContaining({ id: '11' }),
      expect.objectContaining({
        state: CampaignProjectionOutboxState.DEAD,
        attempts: 20,
        lockedBy: null,
        lockedUntil: null,
      })
    );
  });

  it('keeps a deletion WAITING while reservations remain', async () => {
    const harness = buildWorker();
    harness.budgetRepository.getBudgetState.mockResolvedValue({
      servingVersion: 3,
      tombstone: true,
      status: 'PAUSED',
      maxCpc: 100,
      dailyBudget: 1_000,
      totalBudget: 10_000,
      dailySpent: 10,
      totalSpent: 20,
      dailyReserved: 100,
      totalReserved: 100,
    });
    const deletion = event({
      eventType: CampaignProjectionEventType.DELETE,
      payload: {
        eventId: 'event-1',
        eventType: CampaignProjectionEventType.DELETE,
        campaign: { ...campaign, deletedAt: new Date().toISOString() },
      },
    });

    await processClaimed(harness.worker, deletion);

    expect(harness.embeddingQueue.add).not.toHaveBeenCalled();
    expect(harness.outboxRepository.update).toHaveBeenLastCalledWith(
      expect.objectContaining({ id: '11' }),
      expect.objectContaining({
        state: CampaignProjectionOutboxState.WAITING,
        lockedBy: null,
        lockedUntil: null,
      })
    );
  });

  it('does not run authoritative deletion settlement in shadow mode', async () => {
    const harness = buildWorker({ mode: 'shadow' });
    const deletion = event({
      eventType: CampaignProjectionEventType.DELETE,
      payload: {
        eventId: 'event-1',
        eventType: CampaignProjectionEventType.DELETE,
        campaign: { ...campaign, deletedAt: new Date().toISOString() },
      },
    });

    await processClaimed(harness.worker, deletion);

    expect(harness.budgetRepository.getBudgetState).not.toHaveBeenCalled();
    expect(harness.outboxRepository.update).toHaveBeenLastCalledWith(
      expect.objectContaining({ id: '11' }),
      expect.objectContaining({
        state: CampaignProjectionOutboxState.WAITING,
        lockedBy: null,
        lockedUntil: null,
      })
    );
  });

  it('does not refund an active deletion when its Budget projection is missing', async () => {
    const harness = buildWorker();
    const deletion = event({
      eventType: CampaignProjectionEventType.DELETE,
      payload: {
        eventId: 'event-1',
        eventType: CampaignProjectionEventType.DELETE,
        campaign: { ...campaign, deletedAt: new Date().toISOString() },
      },
    });

    await processClaimed(harness.worker, deletion);

    expect(
      harness.budgetRepository.applyBudgetTombstone
    ).not.toHaveBeenCalled();
    expect(
      harness.searchRepository.applySearchTombstone
    ).not.toHaveBeenCalled();
    expect(harness.dataSource.transaction).not.toHaveBeenCalled();
    expect(harness.outboxRepository.update).toHaveBeenLastCalledWith(
      expect.objectContaining({ id: '11' }),
      expect.objectContaining({
        state: CampaignProjectionOutboxState.RETRY,
        attempts: 1,
      })
    );
  });

  it('uses the deletion operation key to avoid refunding twice on redelivery', async () => {
    const harness = buildWorker();
    const workerId = (harness.worker as unknown as { workerId: string })
      .workerId;
    const deletion = event({
      lockedBy: workerId,
      eventType: CampaignProjectionEventType.DELETE,
      payload: {
        eventId: 'event-1',
        eventType: CampaignProjectionEventType.DELETE,
        campaign: { ...campaign, deletedAt: new Date().toISOString() },
      },
    });
    harness.budgetRepository.getBudgetState.mockResolvedValue({
      servingVersion: 3,
      tombstone: true,
      status: 'PAUSED',
      maxCpc: 100,
      dailyBudget: 1_000,
      totalBudget: 10_000,
      dailySpent: 100,
      totalSpent: 200,
      dailyReserved: 0,
      totalReserved: 0,
    });
    const storedCampaign = {
      ...campaign,
      tags: [],
      totalSpent: 100,
    } as unknown as CampaignEntity;
    const outboxRepo = {
      findOne: jest.fn().mockResolvedValue(deletion),
      save: jest.fn(async (value: CampaignProjectionOutboxEntity) => value),
    };
    const campaignRepo = {
      findOne: jest.fn().mockResolvedValue(storedCampaign),
      save: jest.fn(async (value: CampaignEntity) => value),
    };
    const historyRepo = {
      exist: jest.fn().mockResolvedValue(true),
      save: jest.fn(),
    };
    const userRepo = {
      findOne: jest.fn(),
      save: jest.fn(),
    };
    const manager = {
      getRepository: jest.fn((entity: unknown) => {
        if (entity === CampaignProjectionOutboxEntity) return outboxRepo;
        if (entity === CampaignEntity) return campaignRepo;
        if (entity === CreditHistoryEntity) return historyRepo;
        if (entity === UserEntity) return userRepo;
        throw new Error('unexpected repository');
      }),
    };
    harness.dataSource.transaction.mockImplementation(
      (callback: (value: typeof manager) => unknown) => callback(manager)
    );

    await processClaimed(harness.worker, deletion);

    expect(storedCampaign.totalSpent).toBe(200);
    expect(historyRepo.exist).toHaveBeenCalledWith({
      where: { operationKey: 'campaign-delete-refund:campaign-1:v3' },
    });
    expect(historyRepo.save).not.toHaveBeenCalled();
    expect(userRepo.findOne).not.toHaveBeenCalled();
    expect(outboxRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({ deletionSettledAt: expect.any(Date) })
    );
  });

  it('stops before the next projection stage after losing its lease', async () => {
    const harness = buildWorker();
    harness.outboxRepository.update.mockResolvedValue({ affected: 0 });

    await processClaimed(harness.worker, event());

    expect(
      harness.budgetRepository.applyBudgetProjection
    ).toHaveBeenCalledTimes(1);
    expect(
      harness.searchRepository.applySearchProjection
    ).not.toHaveBeenCalled();
    expect(harness.outboxRepository.update).toHaveBeenCalledWith(
      expect.objectContaining({ id: '11', lockedBy: expect.any(String) }),
      expect.any(Object)
    );
  });

  it('claims only the earliest available campaign version with SKIP LOCKED', async () => {
    const harness = buildWorker();
    let sql = '';
    const manager = {
      query: jest.fn((value: string) => {
        sql = value;
        return Promise.resolve([]);
      }),
    };
    harness.dataSource.transaction.mockImplementation(
      (callback: (value: typeof manager) => unknown) => callback(manager)
    );

    await harness.worker.processAvailableOnce();

    expect(sql).toContain('FOR UPDATE SKIP LOCKED');
    expect(sql).toContain('prior.serving_version < candidate.serving_version');
    expect(sql).toContain("prior.state <> 'COMPLETED'");
    expect(sql).toContain('candidate.locked_until < NOW(3)');
  });
});
