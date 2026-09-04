import type { ConfigService } from '@nestjs/config';
import type { Queue } from 'bullmq';
import type { DataSource } from 'typeorm';
import { CreditHistoryEntity } from '../advertiser/entities/credit-history.entity';
import { AVAILABLE_TAGS } from '../common/constants';
import { UserEntity } from '../user/entities/user.entity';
import type { UserRepository } from '../user/repository/user.repository.interface';
import { CampaignService } from './campaign.service';
import type { CreateCampaignDto } from './dto/create-campaign.dto';
import { CampaignEntity, CampaignStatus } from './entities/campaign.entity';
import { CampaignProjectionOutboxWriter } from './projection/campaign-projection-outbox.writer';
import { CampaignProjectionEventType } from './projection/campaign-projection.types';
import type { CampaignCacheRepository } from './repository/campaign.cache.repository.interface';
import type { CampaignRepository } from './repository/campaign.repository.interface';
import type { LogRepository } from '../log/repository/log.repository.interface';
import { TagEntity } from '../tag/entities/tag.entity';

function buildHarness() {
  const tag = { id: AVAILABLE_TAGS[0].id, name: AVAILABLE_TAGS[0].name };
  const campaignRepository = {
    create: jest.fn((value: Partial<CampaignEntity>) => value),
    save: jest.fn(async (value: CampaignEntity) => ({
      ...value,
      createdAt: value.createdAt ?? new Date('2026-09-03T00:00:00.000Z'),
    })),
    findOne: jest.fn(),
  };
  const tagRepository = {
    find: jest.fn().mockResolvedValue([tag]),
  };
  const userEntityRepository = {
    findOne: jest.fn().mockResolvedValue({ id: 7, balance: 100_000 }),
    save: jest.fn(async (value: UserEntity) => value),
  };
  const creditHistoryRepository = {
    save: jest.fn(async (value: unknown) => value),
    exist: jest.fn().mockResolvedValue(false),
  };
  const manager = {
    getRepository: jest.fn((entity: unknown) => {
      if (entity === CampaignEntity) return campaignRepository;
      if (entity === TagEntity) return tagRepository;
      if (entity === UserEntity) return userEntityRepository;
      if (entity === CreditHistoryEntity) return creditHistoryRepository;
      throw new Error('unexpected repository');
    }),
  };
  const dataSource = {
    transaction: jest.fn((callback: (value: typeof manager) => unknown) =>
      callback(manager)
    ),
  } as unknown as DataSource & { transaction: jest.Mock };
  const cacheRepository = {
    saveCampaignCacheById: jest.fn(),
    deleteCampaignCacheById: jest.fn(),
  } as unknown as CampaignCacheRepository & {
    saveCampaignCacheById: jest.Mock;
    deleteCampaignCacheById: jest.Mock;
  };
  const outboxWriter = {
    append: jest.fn().mockResolvedValue(undefined),
  } as unknown as CampaignProjectionOutboxWriter & { append: jest.Mock };
  const config = {
    get: jest.fn((key: string, fallback?: unknown) =>
      key === 'CAMPAIGN_PROJECTION_MODE' ? 'active' : fallback
    ),
  } as unknown as ConfigService;
  const userRepository = {
    getBalanceById: jest.fn().mockResolvedValue(100_000),
  } as unknown as UserRepository;
  const service = new CampaignService(
    {} as CampaignRepository,
    userRepository,
    cacheRepository,
    {} as LogRepository,
    dataSource,
    {} as Queue,
    config,
    outboxWriter
  );

  return {
    service,
    manager,
    dataSource,
    campaignRepository,
    cacheRepository,
    outboxWriter,
  };
}

describe('CampaignService transactional outbox', () => {
  const createDto: CreateCampaignDto = {
    title: 'campaign',
    content: 'content',
    image: 'https://example.com/image.png',
    url: 'https://example.com',
    tags: [AVAILABLE_TAGS[0].name],
    maxCpc: 100,
    dailyBudget: 3_000,
    totalBudget: 10_000,
    startDate: '2026-09-01T00:00:00.000Z',
    endDate: '2026-10-01T00:00:00.000Z',
    isHighIntent: false,
  };

  it('writes a new campaign and its immutable event through the same manager', async () => {
    const harness = buildHarness();

    const created = await harness.service.createCampaign(7, createDto);

    expect(created.servingVersion).toBe(1);
    expect(harness.campaignRepository.save).toHaveBeenCalledTimes(1);
    expect(harness.outboxWriter.append).toHaveBeenCalledWith(
      harness.manager,
      expect.objectContaining({
        id: created.id,
        servingVersion: 1,
      }),
      CampaignProjectionEventType.UPSERT
    );
    expect(
      harness.cacheRepository.saveCampaignCacheById
    ).not.toHaveBeenCalled();
  });

  it('propagates an Outbox insert failure and never runs the post-commit mirror', async () => {
    const harness = buildHarness();
    harness.outboxWriter.append.mockRejectedValue(
      new Error('outbox insert failed')
    );

    await expect(harness.service.createCampaign(7, createDto)).rejects.toThrow(
      'outbox insert failed'
    );

    expect(harness.dataSource.transaction).toHaveBeenCalledTimes(1);
    expect(
      harness.cacheRepository.saveCampaignCacheById
    ).not.toHaveBeenCalled();
  });

  it('versions a deletion and records its tombstone event in the transaction', async () => {
    const harness = buildHarness();
    const campaign = {
      id: 'campaign-1',
      userId: 7,
      servingVersion: 4,
      title: 'campaign',
      content: 'content',
      image: null,
      url: 'https://example.com',
      maxCpc: 100,
      dailyBudget: 3_000,
      totalBudget: null,
      dailySpent: 0,
      totalSpent: 0,
      lastResetDate: new Date(),
      isHighIntent: false,
      status: CampaignStatus.ACTIVE,
      startDate: new Date('2026-09-01T00:00:00.000Z'),
      endDate: new Date('2026-10-01T00:00:00.000Z'),
      createdAt: new Date('2026-09-01T00:00:00.000Z'),
      deletedAt: null,
      tags: [],
    } as CampaignEntity;
    harness.campaignRepository.findOne.mockResolvedValue(campaign);

    await harness.service.deleteCampaign(campaign.id, campaign.userId);

    expect(campaign.servingVersion).toBe(5);
    expect(campaign.deletedAt).toBeInstanceOf(Date);
    expect(harness.outboxWriter.append).toHaveBeenCalledWith(
      harness.manager,
      expect.objectContaining({ servingVersion: 5 }),
      CampaignProjectionEventType.DELETE
    );
    expect(
      harness.cacheRepository.deleteCampaignCacheById
    ).not.toHaveBeenCalled();
  });
});
