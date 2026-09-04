import { ConfigService } from '@nestjs/config';
import { EventEmitter2 } from '@nestjs/event-emitter';
import type { AppIORedisClient } from 'src/redis/redis.type';
import { RedisCampaignCacheRepository } from './redis-campaign.cache.repository';
import {
  REDIS_COMMIT_AUCTION_SCRIPT,
  REDIS_INCREMENT_SPENT_SCRIPT,
  REDIS_RELEASE_AUCTION_SCRIPT,
  REDIS_REPLACE_SPENT_SCRIPT,
  REDIS_RESERVE_AUCTION_SCRIPT,
  REDIS_SAVE_CAMPAIGN_PRESERVING_RESERVED_SCRIPT,
} from '../scripts/lua-script';
import {
  REDIS_HASH_INCREMENT_SPENT_SCRIPT,
  REDIS_HASH_REPLACE_SPENT_SCRIPT,
  REDIS_HASH_RESERVE_AUCTION_SCRIPT,
} from '../scripts/budget-lua-script';
import { CachedCampaign } from '../types/campaign.types';

const cachedCampaign: CachedCampaign = {
  id: 'campaign-1',
  userId: 1,
  servingVersion: 1,
  title: 'title',
  content: 'content',
  image: null,
  url: 'https://example.com',
  maxCpc: 100,
  dailyBudget: 1_000,
  totalBudget: 10_000,
  dailySpent: 0,
  totalSpent: 0,
  lastResetDate: '2026-08-17T00:00:00.000Z',
  isHighIntent: false,
  status: 'ACTIVE',
  startDate: '2026-08-01T00:00:00.000Z',
  endDate: '2026-08-31T00:00:00.000Z',
  createdAt: '2026-08-01T00:00:00.000Z',
  deletedAt: null,
  tags: [],
};

describe('RedisCampaignCacheRepository winner-only reservation', () => {
  const buildRepository = (
    evalResult: unknown,
    topology: 'legacy' | 'split' = 'split'
  ) => {
    const pipeline = {
      del: jest.fn(),
      call: jest.fn(),
      sadd: jest.fn(),
      exec: jest.fn().mockResolvedValue([]),
    };
    const redis = {
      eval: jest.fn().mockResolvedValue(evalResult),
      get: jest.fn().mockResolvedValue(null),
      zrem: jest.fn().mockResolvedValue(1),
      zrangebyscore: jest.fn().mockResolvedValue([]),
      call: jest.fn().mockResolvedValue('OK'),
      expire: jest.fn().mockResolvedValue(1),
      persist: jest.fn().mockResolvedValue(1),
      sadd: jest.fn().mockResolvedValue(1),
      hgetall: jest.fn().mockResolvedValue({}),
      hset: jest.fn().mockResolvedValue(1),
      exists: jest.fn().mockResolvedValue(1),
      smembers: jest.fn().mockResolvedValue([]),
      del: jest.fn().mockResolvedValue(1),
      pipeline: jest.fn(() => pipeline),
    } as unknown as AppIORedisClient & {
      eval: jest.Mock;
      get: jest.Mock;
      zrem: jest.Mock;
      zrangebyscore: jest.Mock;
      call: jest.Mock;
    };
    const config = {
      get: jest.fn((key: string, defaultValue: unknown) =>
        key === 'REDIS_TOPOLOGY_MODE' ? topology : defaultValue
      ),
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

  it('creates a versioned reservation using campaign keys and the expiration ZSET', async () => {
    const reservation = {
      version: 2,
      auctionId: 'auction-1',
      campaignId: 'campaign-1',
      campaignServingVersion: 4,
      blogId: 7,
      cost: 100,
      status: 'RESERVED',
      budgetDate: '2026-08-17',
      createdAt: 1,
      updatedAt: 1,
      expiresAt: 10_000,
    } as const;
    const { repository, redis } = buildRepository([
      1,
      'campaign-1',
      1,
      JSON.stringify(reservation),
      0,
    ]);

    await expect(
      repository.reserveAuction({
        auctionId: 'auction-1',
        blogId: 7,
        budgetDate: '2026-08-17',
        expiresAt: 10_000,
        candidates: [{ campaignId: 'campaign-1', servingVersion: 4 }],
      })
    ).resolves.toEqual({
      outcome: 'reserved',
      attemptedCount: 1,
      reservation,
      versionMismatchCount: 0,
    });
    expect(redis.eval).toHaveBeenCalledWith(
      REDIS_HASH_RESERVE_AUCTION_SCRIPT,
      3,
      'rtb:reservation:expirations',
      'auction:auction-1',
      'budget:campaign:campaign-1',
      'auction-1',
      '7',
      '2026-08-17',
      '10000',
      '4',
      'campaign-1'
    );
  });

  it('reconciliation replaces only spent fields through Lua', async () => {
    const { repository, redis } = buildRepository(1);

    await repository.replaceSpentCacheById('campaign-1', 200, 500);

    expect(redis.eval).toHaveBeenCalledWith(
      REDIS_HASH_REPLACE_SPENT_SCRIPT,
      1,
      'budget:campaign:campaign-1',
      '200',
      '500'
    );
    expect(REDIS_HASH_REPLACE_SPENT_SCRIPT).not.toContain('dailyReserved');
    expect(REDIS_HASH_REPLACE_SPENT_SCRIPT).not.toContain('totalReserved');
  });

  it('keeps Budget counters out of the Search campaign document', () => {
    const { repository } = buildRepository(1);
    const searchDocument = (
      repository as unknown as {
        toSearchCampaign(value: object): Record<string, unknown>;
      }
    ).toSearchCampaign({
      ...cachedCampaign,
      semanticHash: 'semantic-hash',
    });

    expect(searchDocument).toMatchObject({
      id: cachedCampaign.id,
      servingVersion: 1,
      maxCpc: 100,
      indexReady: false,
    });
    for (const budgetOnlyField of [
      'dailyBudget',
      'totalBudget',
      'dailySpent',
      'totalSpent',
      'dailyReserved',
      'totalReserved',
      'lastResetDate',
    ]) {
      expect(searchDocument).not.toHaveProperty(budgetOnlyField);
    }
  });

  it('preserves existing reserved fields when replacing a campaign document', async () => {
    const { repository, redis } = buildRepository(1, 'legacy');

    await repository.saveCampaignCacheById('campaign-1', cachedCampaign);

    expect(redis.eval).toHaveBeenCalledWith(
      REDIS_SAVE_CAMPAIGN_PRESERVING_RESERVED_SCRIPT,
      1,
      'campaign:campaign-1',
      expect.stringContaining('"dailyReserved":0'),
      expect.stringMatching(/^\d{4}-\d{2}-\d{2}$/)
    );
    expect(REDIS_SAVE_CAMPAIGN_PRESERVING_RESERVED_SCRIPT).toContain(
      'existing.dailyReserved'
    );
  });

  it('ignores legacy auction data when reading a reservation', async () => {
    const { repository, redis } = buildRepository(1);
    redis.get.mockResolvedValue(
      JSON.stringify({ campaignId: 'campaign-1', blogId: 7, cost: 100 })
    );

    await expect(
      repository.getAuctionReservation('auction-1')
    ).resolves.toBeNull();
  });

  it('passes only the Budget key and CPC to the split spent Lua', async () => {
    const { repository, redis } = buildRepository(1);

    await expect(repository.incrementSpent('campaign-1', 15)).resolves.toBe(
      true
    );
    expect(redis.eval).toHaveBeenCalledWith(
      expect.any(String),
      1,
      'budget:campaign:campaign-1',
      '15'
    );
    expect(REDIS_HASH_INCREMENT_SPENT_SCRIPT).not.toContain('ARGV[2]');
    for (const path of [
      "'status'",
      "'dailyBudget'",
      "'totalBudget'",
      "'dailySpent'",
      "'totalSpent'",
    ]) {
      expect(REDIS_HASH_INCREMENT_SPENT_SCRIPT).toContain(path);
    }
  });

  it('keeps legacy RedisJSON spent updates on legacy topology', async () => {
    const { repository, redis } = buildRepository(1, 'legacy');

    await expect(repository.incrementSpent('campaign-1', 15)).resolves.toBe(
      true
    );
    await repository.replaceSpentCacheById('campaign-1', 20, 30);

    expect(redis.eval).toHaveBeenNthCalledWith(
      1,
      REDIS_INCREMENT_SPENT_SCRIPT,
      1,
      'campaign:campaign-1',
      '15'
    );
    expect(redis.eval).toHaveBeenNthCalledWith(
      2,
      REDIS_REPLACE_SPENT_SCRIPT,
      1,
      'campaign:campaign-1',
      '20',
      '30'
    );
  });

  it('reads and temporarily changes budget state through RedisJSON in legacy mode', async () => {
    const { repository, redis } = buildRepository(1, 'legacy');
    redis.call.mockImplementation((command: string) => {
      if (command === 'JSON.GET') {
        return Promise.resolve(JSON.stringify(cachedCampaign));
      }
      return Promise.resolve('OK');
    });

    await expect(
      repository.getBudgetState('campaign-1')
    ).resolves.toMatchObject({
      servingVersion: 1,
      status: 'ACTIVE',
      dailySpent: 0,
      totalReserved: 0,
    });
    await expect(
      repository.setOperationalStatus('campaign-1', 'PAUSED')
    ).resolves.toBe(true);
    expect(redis.call).toHaveBeenCalledWith(
      'JSON.SET',
      'campaign:campaign-1',
      '$.status',
      JSON.stringify('PAUSED')
    );
  });

  it('creates and finishes version 1 reservations during legacy rollout', async () => {
    const reservation = {
      version: 1,
      auctionId: 'auction-legacy',
      campaignId: 'campaign-1',
      blogId: 7,
      cost: 100,
      status: 'RESERVED',
      budgetDate: '2026-08-17',
      createdAt: 1,
      updatedAt: 1,
      expiresAt: 10_000,
    } as const;
    const { repository, redis } = buildRepository(
      [1, 'campaign-1', 1, JSON.stringify(reservation)],
      'legacy'
    );

    await repository.reserveAuction({
      auctionId: 'auction-legacy',
      blogId: 7,
      budgetDate: '2026-08-17',
      expiresAt: 10_000,
      candidates: [{ campaignId: 'campaign-1', servingVersion: 4, cpc: 100 }],
    });
    expect(redis.eval).toHaveBeenCalledWith(
      REDIS_RESERVE_AUCTION_SCRIPT,
      3,
      'rtb:reservation:expirations',
      'auction:auction-legacy',
      'campaign:campaign-1',
      'auction-legacy',
      '7',
      '2026-08-17',
      '10000',
      '86400',
      '100',
      'campaign-1'
    );

    redis.get.mockResolvedValue(JSON.stringify(reservation));
    redis.eval.mockResolvedValue([
      1,
      JSON.stringify({
        version: 1,
        auctionId: 'auction-legacy',
        status: 'COMMITTED',
        updatedAt: 2,
      }),
    ]);
    await repository.commitAuction('auction-legacy', '2026-08-17', 300);
    expect(redis.eval).toHaveBeenLastCalledWith(
      REDIS_COMMIT_AUCTION_SCRIPT,
      3,
      'auction:auction-legacy',
      'rtb:reservation:expirations',
      'campaign:campaign-1',
      'auction-legacy',
      '2026-08-17',
      '300',
      expect.any(String)
    );

    redis.eval.mockResolvedValue([
      1,
      JSON.stringify({
        version: 1,
        auctionId: 'auction-legacy',
        status: 'RELEASED',
        updatedAt: 3,
      }),
    ]);
    await repository.releaseAuction('auction-legacy', 300);
    expect(redis.eval).toHaveBeenLastCalledWith(
      REDIS_RELEASE_AUCTION_SCRIPT,
      3,
      'auction:auction-legacy',
      'rtb:reservation:expirations',
      'campaign:campaign-1',
      'auction-legacy',
      '300',
      expect.any(String)
    );
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
            [
              'campaignId',
              'campaign-1',
              'servingVersion',
              '7',
              'vector_distance',
              '0.2',
            ],
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
        servingVersion: 7,
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
