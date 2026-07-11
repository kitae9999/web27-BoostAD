import { BadRequestException, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { BlogRepository } from 'src/blog/repository/blog.repository.interface';
import { CacheRepository } from 'src/cache/repository/cache.repository.interface';
import { CampaignCacheRepository } from 'src/campaign/repository/campaign.cache.repository.interface';
import { CampaignRepository } from 'src/campaign/repository/campaign.repository.interface';
import type { AuctionReservation } from 'src/campaign/types/campaign.types';
import { LogRepository } from 'src/log/repository/log.repository.interface';
import type { SaveViewLog } from 'src/log/types/log.type';
import { UserRepository } from 'src/user/repository/user.repository.interface';
import { SdkService } from './sdk.service';

describe('SdkService reservation lifecycle', () => {
  const auctionId = '550e8400-e29b-41d4-a716-446655440000';
  const reservation: AuctionReservation = {
    auctionId,
    requestFingerprint: 'fingerprint',
    campaignId: 'winner-campaign',
    blogId: 7,
    reservedAmount: 120,
    budgetDate: '2026-07-11',
    status: 'RESERVED',
    createdAt: Date.now(),
    updatedAt: Date.now(),
    expiresAt: Date.now() + 60_000,
  };
  const viewLog: SaveViewLog = {
    auctionId,
    campaignId: reservation.campaignId,
    blogId: reservation.blogId,
    postUrl: 'https://example.com/post',
    cost: reservation.reservedAmount,
    positionRatio: null,
    isHighIntent: false,
    behaviorScore: 30,
  };

  const createHarness = () => {
    const logRepository = {
      saveViewLog: jest.fn().mockResolvedValue(101),
      getViewLog: jest.fn().mockResolvedValue(viewLog),
      saveClickLog: jest.fn().mockResolvedValue(201),
      getBlogIdAndCostByViewId: jest.fn().mockResolvedValue(null),
    } as unknown as jest.Mocked<LogRepository>;
    const cacheRepository = {
      acquireAuctionViewIdempotencyKey: jest
        .fn()
        .mockResolvedValue({ status: 'acquired' }),
      setAuctionViewIdempotencyKey: jest.fn().mockResolvedValue(undefined),
      getAuctionViewIdByIdempotencyKey: jest.fn().mockResolvedValue(null),
      setClickIdempotencyKey: jest.fn().mockResolvedValue(false),
    } as unknown as jest.Mocked<CacheRepository>;
    const campaignCacheRepository = {
      getAuctionReservation: jest.fn().mockResolvedValue(reservation),
      commitAuction: jest.fn().mockResolvedValue({ outcome: 'committed' }),
      releaseAuction: jest.fn().mockResolvedValue({ outcome: 'released' }),
    } as unknown as jest.Mocked<CampaignCacheRepository>;
    const campaignRepository = {
      incrementSpent: jest.fn().mockResolvedValue(undefined),
    } as unknown as jest.Mocked<CampaignRepository>;
    const blogRepository = {
      getUserIdByBlogId: jest.fn().mockResolvedValue(null),
    } as unknown as jest.Mocked<BlogRepository>;
    const userRepository = {} as jest.Mocked<UserRepository>;
    const configService = {
      get: jest.fn((key: string) =>
        key === 'RTB_BUDGET_MODE' ? 'reservation_lifecycle' : undefined
      ),
    } as unknown as ConfigService;

    return {
      service: new SdkService(
        logRepository,
        cacheRepository,
        campaignCacheRepository,
        campaignRepository,
        blogRepository,
        userRepository,
        configService
      ),
      logRepository,
      cacheRepository,
      campaignCacheRepository,
      campaignRepository,
    };
  };

  const viewDto = {
    auctionId,
    blogKey: 'blog-key',
    postUrl: 'https://example.com/post',
    isHighIntent: false,
    behaviorScore: 30,
  };

  it('records the server-side reserved winner instead of trusting client campaign data', async () => {
    const harness = createHarness();

    await expect(harness.service.recordView(viewDto, 'visitor')).resolves.toBe(
      101
    );
    expect(harness.logRepository.saveViewLog).toHaveBeenCalledWith(viewLog);

    await expect(
      harness.service.recordView(
        { ...viewDto, campaignId: 'forged-campaign' },
        'visitor'
      )
    ).rejects.toBeInstanceOf(BadRequestException);
  });

  it('returns the existing view for a retried auction without writing another log', async () => {
    const harness = createHarness();
    harness.cacheRepository.acquireAuctionViewIdempotencyKey.mockResolvedValue({
      status: 'exists',
      viewId: 88,
    });

    await expect(harness.service.recordView(viewDto, 'visitor')).resolves.toBe(
      88
    );
    expect(harness.logRepository.saveViewLog).not.toHaveBeenCalled();
  });

  it('commits budget atomically and persists a duplicate click only once', async () => {
    const harness = createHarness();
    harness.campaignCacheRepository.commitAuction
      .mockResolvedValueOnce({ outcome: 'committed' })
      .mockResolvedValueOnce({ outcome: 'already_committed' });
    harness.cacheRepository.setClickIdempotencyKey
      .mockResolvedValueOnce(false)
      .mockResolvedValueOnce(true);

    await expect(
      harness.service.recordClick({
        viewId: 101,
        blogKey: 'blog-key',
        postUrl: 'https://example.com/post',
      })
    ).resolves.toBe(201);
    await expect(
      harness.service.recordClick({
        viewId: 101,
        blogKey: 'blog-key',
        postUrl: 'https://example.com/post',
      })
    ).resolves.toBeNull();

    expect(harness.logRepository.saveClickLog).toHaveBeenCalledTimes(1);
    expect(harness.campaignRepository.incrementSpent).toHaveBeenCalledTimes(1);
  });

  it('releases on dismiss and rejects a click after release', async () => {
    const harness = createHarness();

    await harness.service.recordDismiss({
      viewId: 101,
      blogKey: 'blog-key',
      postUrl: 'https://example.com/post',
    });
    expect(harness.campaignCacheRepository.releaseAuction).toHaveBeenCalledWith(
      auctionId,
      1800
    );

    harness.campaignCacheRepository.commitAuction.mockResolvedValue({
      outcome: 'released',
    });
    await expect(
      harness.service.recordClick({
        viewId: 101,
        blogKey: 'blog-key',
        postUrl: 'https://example.com/post',
      })
    ).resolves.toBeNull();
    expect(harness.logRepository.saveClickLog).not.toHaveBeenCalled();
  });

  it('rejects a view after its reservation has been released', async () => {
    const harness = createHarness();
    harness.campaignCacheRepository.getAuctionReservation.mockResolvedValue({
      ...reservation,
      status: 'RELEASED',
    });

    await expect(
      harness.service.recordView(viewDto, 'visitor')
    ).rejects.toBeInstanceOf(NotFoundException);
  });
});
