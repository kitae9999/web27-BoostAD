import { ConfigService } from '@nestjs/config';
import { EventEmitter2 } from '@nestjs/event-emitter';
import type { AppIORedisClient } from 'src/redis/redis.type';
import { RedisCampaignCacheRepository } from './redis-campaign.cache.repository';

describe('RedisCampaignCacheRepository winner-only reservation', () => {
  const buildRepository = (evalResult: [number, number]) => {
    const redis = {
      eval: jest.fn().mockResolvedValue(evalResult),
    } as unknown as AppIORedisClient & { eval: jest.Mock };
    const config = {
      get: jest.fn((_key: string, defaultValue: number) => defaultValue),
    } as unknown as ConfigService;

    return {
      repository: new RedisCampaignCacheRepository(
        redis,
        config,
        new EventEmitter2()
      ),
      redis,
    };
  };

  it('maps the Lua selected index back to the ranked campaign ID', async () => {
    const { repository, redis } = buildRepository([2, 2]);

    const result = await repository.reserveFirstAvailable([
      { campaignId: 'first', cpc: 10 },
      { campaignId: 'second', cpc: 20 },
    ]);

    expect(result).toEqual({ campaignId: 'second', attemptedCount: 2 });
    expect(redis.eval).toHaveBeenCalledWith(
      expect.any(String),
      4,
      'rtb:budget:daily-exhausted-campaigns',
      'rtb:budget:total-exhausted-campaigns',
      'campaign:first',
      'campaign:second',
      '10',
      '20',
      'first',
      'second'
    );
  });

  it('merges daily and total exhausted campaign IDs from Redis', async () => {
    const pipeline = {
      smembers: jest.fn().mockReturnThis(),
      exec: jest.fn().mockResolvedValue([
        [null, ['daily-only', 'both']],
        [null, ['total-only', 'both']],
      ]),
    };
    const redis = {
      pipeline: jest.fn(() => pipeline),
    } as unknown as AppIORedisClient;
    const config = {
      get: jest.fn((_key: string, defaultValue: number) => defaultValue),
    } as unknown as ConfigService;
    const repository = new RedisCampaignCacheRepository(
      redis,
      config,
      new EventEmitter2()
    );

    await expect(repository.getBudgetExhaustedCampaignIds()).resolves.toEqual(
      expect.arrayContaining(['daily-only', 'total-only', 'both'])
    );
    expect(pipeline.smembers).toHaveBeenNthCalledWith(
      1,
      'rtb:budget:daily-exhausted-campaigns'
    );
    expect(pipeline.smembers).toHaveBeenNthCalledWith(
      2,
      'rtb:budget:total-exhausted-campaigns'
    );
  });

  it('clears both eligibility scopes when budget inputs change', async () => {
    const redis = {
      srem: jest.fn().mockResolvedValue(1),
    } as unknown as AppIORedisClient & { srem: jest.Mock };
    const config = {
      get: jest.fn((_key: string, defaultValue: number) => defaultValue),
    } as unknown as ConfigService;
    const repository = new RedisCampaignCacheRepository(
      redis,
      config,
      new EventEmitter2()
    );

    await repository.clearBudgetExhaustion('campaign-1');

    expect(redis.srem).toHaveBeenCalledWith(
      'rtb:budget:daily-exhausted-campaigns',
      'campaign-1'
    );
    expect(redis.srem).toHaveBeenCalledWith(
      'rtb:budget:total-exhausted-campaigns',
      'campaign-1'
    );
  });

  it('maps an atomic auction reservation result and passes reservation keys', async () => {
    const reservation = {
      auctionId: 'auction-1',
      requestFingerprint: 'fingerprint-1',
      campaignId: 'campaign-2',
      blogId: 7,
      reservedAmount: 20,
      budgetDate: '2026-07-11',
      status: 'RESERVED',
      createdAt: 1000,
      updatedAt: 1000,
      expiresAt: 2000,
    };
    const redis = {
      eval: jest
        .fn()
        .mockResolvedValue([1, 'campaign-2', 2, JSON.stringify(reservation)]),
    } as unknown as AppIORedisClient & { eval: jest.Mock };
    const config = {
      get: jest.fn((_key: string, defaultValue: number) => defaultValue),
    } as unknown as ConfigService;
    const repository = new RedisCampaignCacheRepository(
      redis,
      config,
      new EventEmitter2()
    );

    await expect(
      repository.reserveAuction({
        auctionId: 'auction-1',
        requestFingerprint: 'fingerprint-1',
        blogId: 7,
        budgetDate: '2026-07-11',
        expiresAt: 2000,
        resultTtlSeconds: 1800,
        candidates: [
          { campaignId: 'campaign-1', cpc: 10 },
          { campaignId: 'campaign-2', cpc: 20 },
        ],
      })
    ).resolves.toEqual({
      outcome: 'reserved',
      reservation,
      attemptedCount: 2,
    });

    expect(redis.eval).toHaveBeenCalledWith(
      expect.any(String),
      8,
      'rtb:reservation:auction-1',
      'rtb:reservation:expirations',
      'rtb:budget:daily-reserved:2026-07-11',
      'rtb:budget:total-reserved',
      'rtb:budget:daily-exhausted-campaigns',
      'rtb:budget:total-exhausted-campaigns',
      'campaign:campaign-1',
      'campaign:campaign-2',
      'fingerprint-1',
      'auction-1',
      '7',
      '2026-07-11',
      '2000',
      '1800',
      '10',
      '20',
      'campaign-1',
      'campaign-2'
    );
  });

  it('treats a repeated auction with another fingerprint as conflict', async () => {
    const { repository } = buildRepository([-2, 'campaign-1', 0] as unknown as [
      number,
      number,
    ]);

    await expect(
      repository.reserveAuction({
        auctionId: 'auction-1',
        requestFingerprint: 'different',
        blogId: 7,
        budgetDate: '2026-07-11',
        expiresAt: 2000,
        resultTtlSeconds: 1800,
        candidates: [{ campaignId: 'campaign-1', cpc: 10 }],
      })
    ).resolves.toMatchObject({ outcome: 'conflict', attemptedCount: 0 });
  });

  it('returns null when no campaign in the window is reservable', async () => {
    const { repository } = buildRepository([0, 2]);

    await expect(
      repository.reserveFirstAvailable([
        { campaignId: 'first', cpc: 10 },
        { campaignId: 'second', cpc: 20 },
      ])
    ).resolves.toBeNull();
  });

  it('rejects campaign vectors from a different model space', async () => {
    const { repository } = buildRepository([0, 0]);

    await expect(
      repository.updateCampaignEmbeddings('campaign-1', {
        modelVersion: 'other-model',
        document: Array<number>(384).fill(0),
        tags: { react: Array<number>(384).fill(0) },
      })
    ).rejects.toThrow('campaign embedding model version 불일치');
  });

  it('uses a model-versioned document index for multilingual E5', async () => {
    const redis = {
      call: jest.fn((command: string) => {
        if (command === 'FT.INFO') {
          return Promise.reject(new Error('Unknown index name'));
        }
        if (command === 'FT.SEARCH') {
          return Promise.resolve([
            1,
            'campaign-doc-vec:key',
            ['campaignId', 'campaign-1', 'vector_distance', '0.2'],
          ]);
        }
        return Promise.resolve('OK');
      }),
    } as unknown as AppIORedisClient & { call: jest.Mock };
    const config = {
      get: jest.fn((key: string, defaultValue?: number) => {
        if (key === 'RTB_EMBEDDING_PROFILE') {
          return 'multilingual_e5_small';
        }
        return defaultValue;
      }),
    } as unknown as ConfigService;
    const repository = new RedisCampaignCacheRepository(
      redis,
      config,
      new EventEmitter2()
    );

    await expect(
      repository.searchCampaignDocumentVectors({
        queryEmbedding: Array<number>(384).fill(0),
        topL: 10,
        isHighIntent: false,
        nowTs: Date.now(),
      })
    ).resolves.toEqual([
      {
        campaignId: 'campaign-1',
        distance: 0.2,
        similarity: 0.8,
      },
    ]);

    const createCall = redis.call.mock.calls.find(
      ([command]) => command === 'FT.CREATE'
    );
    expect(createCall).toEqual(
      expect.arrayContaining([
        'FT.CREATE',
        'idx:campaign_doc_vec:xenova-multilingual-e5-small-retrieval-v1-mean-normalized',
        'campaign-doc-vec:xenova-multilingual-e5-small-retrieval-v1-mean-normalized:',
        '384',
      ])
    );
  });
});
