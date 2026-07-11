/* eslint-disable @typescript-eslint/unbound-method */
import type { ConfigService } from '@nestjs/config';
import type { AppIORedisClient } from '../../redis/redis.type';
import type { CampaignCacheRepository } from '../repository/campaign.cache.repository.interface';
import { CampaignSearchIndexerConsumer } from './campaign-search-indexer.consumer';
import type { CampaignServingProjectionRepository } from './campaign-serving-projection.repository';

describe('CampaignSearchIndexerConsumer', () => {
  const event = {
    schemaVersion: 1 as const,
    eventId: 'db:2',
    type: 'UPSERT' as const,
    campaignId: 'c1',
    campaignVersion: 2,
    sequence: 2,
    occurredAtMs: 1000,
    campaign: { id: 'c1' } as never,
  };

  const build = (checkpoint = '1') => {
    const redis = {
      get: jest.fn().mockResolvedValue(checkpoint),
      set: jest.fn().mockResolvedValue('OK'),
      hget: jest.fn().mockResolvedValue('0'),
      hset: jest.fn().mockResolvedValue(1),
      pipeline: jest.fn(() => ({
        del: jest.fn().mockReturnThis(),
        hset: jest.fn().mockReturnThis(),
        exec: jest.fn().mockResolvedValue([]),
      })),
    } as unknown as AppIORedisClient;
    const cache = {
      saveCampaignCacheById: jest.fn().mockResolvedValue(undefined),
      deleteCampaignCacheById: jest.fn().mockResolvedValue(undefined),
      getAllCampaigns: jest.fn().mockResolvedValue([]),
    } as unknown as CampaignCacheRepository;
    const projection = {
      loadSnapshot: jest.fn().mockResolvedValue({
        campaigns: [],
        checkpoint: { eventId: 'db:9', sequence: 9 },
        campaignVersions: new Map(),
      }),
    } as unknown as CampaignServingProjectionRepository;
    const config = {
      get: jest.fn((_key: string, fallback?: string) => fallback),
    } as unknown as ConfigService;
    return {
      consumer: new CampaignSearchIndexerConsumer(
        config,
        projection,
        cache,
        redis
      ),
      redis,
      cache,
      projection,
    };
  };

  it('applies the exact next event without creating another serving event', async () => {
    const { consumer, cache, redis } = build();

    await consumer.applyEvent(event);

    expect(cache.saveCampaignCacheById).toHaveBeenCalledWith(
      'c1',
      event.campaign,
      undefined,
      { durableEvent: false, localEvent: false }
    );
    expect(redis.set).toHaveBeenCalledWith(
      'rtb:campaign-search-indexer:sequence',
      '2'
    );
  });

  it('rebuilds from the DB projection when a sequence gap is found', async () => {
    const { consumer, projection } = build('0');

    await consumer.applyEvent(event);

    expect(projection.loadSnapshot).toHaveBeenCalledTimes(1);
  });

  it('advances the Kafka checkpoint without overwriting a newer campaign projection', async () => {
    const { consumer, cache, redis } = build();
    (redis.hget as jest.Mock).mockResolvedValue('99');

    await consumer.applyEvent(event);

    expect(cache.saveCampaignCacheById).not.toHaveBeenCalled();
    expect(redis.set).toHaveBeenCalledWith(
      'rtb:campaign-search-indexer:sequence',
      '2'
    );
  });
});
