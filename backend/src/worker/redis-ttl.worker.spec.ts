import { ConfigService } from '@nestjs/config';
import { CacheRepository } from 'src/cache/repository/cache.repository.interface';
import { CampaignCacheRepository } from 'src/campaign/repository/campaign.cache.repository.interface';
import type { AppIORedisClient } from 'src/redis/redis.type';
import { RedisTTLWorker } from './redis-ttl.worker';
import { MetricsService } from 'src/metrics/metrics.service';

describe('RedisTTLWorker reservation sweep', () => {
  const createWorker = () => {
    const subscriber = {
      subscribe: jest.fn().mockResolvedValue(undefined),
      unsubscribe: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn(),
      on: jest.fn(),
    };
    const redis = {
      config: jest.fn().mockResolvedValue('OK'),
      duplicate: jest.fn().mockReturnValue(subscriber),
    } as unknown as AppIORedisClient;
    const campaignCacheRepository = {
      findExpiredAuctionIds: jest
        .fn()
        .mockResolvedValue(['auction-1', 'auction-2']),
      releaseAuction: jest.fn().mockResolvedValue({ outcome: 'released' }),
    } as unknown as jest.Mocked<CampaignCacheRepository>;
    const configService = {
      get: jest.fn((key: string) => {
        const values: Record<string, string> = {
          RTB_BUDGET_MODE: 'reservation_lifecycle',
          RTB_RESERVATION_SWEEP_INTERVAL_MS: '1000',
          RTB_RESERVATION_SWEEP_BATCH_SIZE: '20',
          RTB_AUCTION_RESULT_TTL_SECONDS: '1800',
        };
        return values[key];
      }),
    } as unknown as ConfigService;
    const worker = new RedisTTLWorker(
      redis,
      {} as CacheRepository,
      campaignCacheRepository,
      {
        recordRtbAuctionTransition: jest.fn(),
      } as unknown as MetricsService,
      configService
    );

    return { worker, campaignCacheRepository, subscriber };
  };

  it('releases every expired reservation with the configured terminal TTL', async () => {
    const { worker, campaignCacheRepository } = createWorker();

    await worker.sweepExpiredReservations();

    expect(campaignCacheRepository.findExpiredAuctionIds).toHaveBeenCalledWith(
      expect.any(Number),
      20
    );
    expect(campaignCacheRepository.releaseAuction).toHaveBeenCalledTimes(2);
    expect(campaignCacheRepository.releaseAuction).toHaveBeenCalledWith(
      'auction-1',
      1800
    );
    expect(campaignCacheRepository.releaseAuction).toHaveBeenCalledWith(
      'auction-2',
      1800
    );
  });

  it('does not start an overlapping sweep while the previous scan is pending', async () => {
    const { worker, campaignCacheRepository } = createWorker();
    let resolveScan: ((ids: string[]) => void) | undefined;
    campaignCacheRepository.findExpiredAuctionIds.mockReturnValue(
      new Promise<string[]>((resolve) => {
        resolveScan = resolve;
      })
    );

    const first = worker.sweepExpiredReservations();
    const second = worker.sweepExpiredReservations();
    await second;
    expect(campaignCacheRepository.findExpiredAuctionIds).toHaveBeenCalledTimes(
      1
    );

    resolveScan?.([]);
    await first;
  });

  it('starts and stops the sweep with the worker lifecycle', async () => {
    jest.useFakeTimers();
    const { worker, subscriber } = createWorker();

    await worker.onModuleInit();
    await Promise.resolve();
    await worker.onModuleDestroy();

    expect(subscriber.subscribe).toHaveBeenCalledWith('__keyevent@0__:expired');
    expect(subscriber.unsubscribe).toHaveBeenCalledWith(
      '__keyevent@0__:expired'
    );
    jest.useRealTimers();
  });
});
