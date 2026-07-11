/* eslint-disable @typescript-eslint/unbound-method */
import type { ConfigService } from '@nestjs/config';
import type { DataSource } from 'typeorm';
import { MLEngine } from '../../rtb/ml/mlEngine.interface';
import {
  CampaignStatus,
  type CampaignEntity,
} from '../entities/campaign.entity';
import { CampaignProjectionWorker } from './campaign-projection.worker';
import {
  CampaignOutboxState,
  CampaignProjectionOperation,
  type CampaignProjectionRequestEntity,
} from './entities/campaign-projection-request.entity';
import { CampaignServingOutboxEntity } from './entities/campaign-serving-outbox.entity';
import { CampaignServingProjectionEntity } from './entities/campaign-serving-projection.entity';

describe('CampaignProjectionWorker', () => {
  const campaign = {
    id: 'c1',
    userId: 1,
    title: 'title',
    content: 'content',
    image: 'image',
    url: 'https://example.com',
    maxCpc: 100,
    dailyBudget: 1000,
    totalBudget: 10000,
    dailySpent: 0,
    totalSpent: 0,
    lastResetDate: new Date(),
    isHighIntent: false,
    status: CampaignStatus.ACTIVE,
    startDate: new Date(),
    endDate: new Date(),
    createdAt: new Date(),
    deletedAt: null,
    tags: [{ id: 1, name: 'tag' }],
  } as CampaignEntity;
  const request = {
    id: '7',
    campaignId: 'c1',
    operation: CampaignProjectionOperation.UPSERT,
    state: CampaignOutboxState.PROCESSING,
    attempts: 1,
  } as CampaignProjectionRequestEntity;

  const build = (existingDocument: Record<string, unknown> | null) => {
    const campaignRepository = {
      findOne: jest.fn().mockResolvedValue(campaign),
    };
    const projectionRepository = {
      findOne: jest
        .fn()
        .mockResolvedValue(
          existingDocument ? { document: existingDocument } : null
        ),
    };
    const dataSource = {
      getRepository: jest
        .fn()
        .mockReturnValueOnce(campaignRepository)
        .mockReturnValueOnce(projectionRepository),
    } as unknown as DataSource;
    const mlEngine = {
      isReady: jest.fn().mockReturnValue(true),
      getModelVersion: jest.fn().mockReturnValue('e5-v1'),
      getEmbedding: jest
        .fn()
        .mockResolvedValueOnce([0.1])
        .mockResolvedValueOnce([0.2]),
    } as unknown as MLEngine;
    const worker = new CampaignProjectionWorker(dataSource, mlEngine, {
      get: jest.fn((key: string, fallback?: string) =>
        key === 'RTB_PROJECTION_PIPELINE_ENABLED' ? 'true' : fallback
      ),
    } as unknown as ConfigService);
    const internals = worker as unknown as {
      claimNext: jest.Mock;
      commitProjection: jest.Mock;
      markFailed: jest.Mock;
    };
    internals.claimNext = jest.fn().mockResolvedValue(request);
    internals.commitProjection = jest.fn().mockResolvedValue(undefined);
    internals.markFailed = jest.fn().mockResolvedValue(undefined);
    return { worker, internals, mlEngine };
  };

  it('reuses embeddings when only serving metadata changed', async () => {
    const { worker, internals, mlEngine } = build({
      id: 'c1',
      title: 'title',
      content: 'content',
      tags: ['tag'],
      embeddingModelVersion: 'e5-v1',
      embeddingDocument: [0.9],
      embeddingTags: { tag: [0.8] },
    });

    await expect(worker.processOnce()).resolves.toBe('completed');

    expect(mlEngine.getEmbedding).not.toHaveBeenCalled();
    expect(internals.commitProjection).toHaveBeenCalledWith(
      request,
      expect.objectContaining({
        embeddingDocument: [0.9],
        embeddingTags: { tag: [0.8] },
      })
    );
  });

  it('regenerates passage embeddings when semantic text changed', async () => {
    const { worker, internals, mlEngine } = build(null);

    await expect(worker.processOnce()).resolves.toBe('completed');

    expect(mlEngine.getEmbedding).toHaveBeenCalledWith('tag', 'passage');
    expect(mlEngine.getEmbedding).toHaveBeenCalledWith(
      'title\ncontent\ntag',
      'passage'
    );
    expect(internals.commitProjection).toHaveBeenCalledWith(
      request,
      expect.objectContaining({ embeddingDocument: [0.2] })
    );
  });

  it('updates the generated event through the typed outbox repository', async () => {
    const projectionRepository = {
      upsert: jest.fn().mockResolvedValue(undefined),
    };
    const outboxRepository = {
      save: jest
        .fn()
        .mockImplementationOnce((value: object) =>
          Promise.resolve({ ...value, id: '12' })
        )
        .mockImplementationOnce((value: object) => Promise.resolve(value)),
    };
    const requestRepository = {
      update: jest.fn().mockResolvedValue(undefined),
    };
    const manager = {
      getRepository: jest.fn((target: unknown) => {
        if (target === CampaignServingProjectionEntity)
          return projectionRepository;
        if (target === CampaignServingOutboxEntity) return outboxRepository;
        return requestRepository;
      }),
    };
    const dataSource = {
      transaction: jest.fn((callback: (value: typeof manager) => unknown) =>
        Promise.resolve(callback(manager))
      ),
    } as unknown as DataSource;
    const worker = new CampaignProjectionWorker(
      dataSource,
      {
        getModelVersion: jest.fn().mockReturnValue('e5-v1'),
      } as unknown as MLEngine,
      { get: jest.fn((_key: string, fallback?: string) => fallback) } as never
    );
    const document = {
      id: 'c1',
      embeddingModelVersion: 'e5-v1',
    } as never;

    await (
      worker as unknown as {
        commitProjection(
          value: CampaignProjectionRequestEntity,
          payload: typeof document
        ): Promise<void>;
      }
    ).commitProjection(request, document);

    expect(outboxRepository.save).toHaveBeenCalledTimes(2);
    expect(outboxRepository.save).toHaveBeenLastCalledWith(
      expect.objectContaining({
        id: '12',
        payload: expect.objectContaining({ eventId: 'db:12', sequence: 12 }),
      })
    );
  });
});
