import { ConflictException } from '@nestjs/common';
import { LoadtestService } from './loadtest.service';
import { ConfigService } from '@nestjs/config';
import type { CampaignBudgetRepository } from 'src/campaign/repository/campaign-budget.repository.interface';
import { CampaignEntity } from 'src/campaign/entities/campaign.entity';
import { BidLogEntity } from 'src/bid-log/entities/bid-log.entity';
import { ViewLogEntity } from 'src/log/entities/view-log.entity';
import { ClickLogEntity } from 'src/log/entities/click-log.entity';

describe('LoadtestService', () => {
  let service: LoadtestService;
  let configService: { get: jest.Mock };
  let dataSource: {
    getRepository: jest.Mock;
    transaction: jest.Mock;
  };
  let ioRedisClient: {
    scan: jest.Mock;
    del: jest.Mock;
  };
  let campaignBudgetRepository: {
    resetForLoadTest: jest.Mock;
  };
  let bidlogQueue: {
    getJobCounts: jest.Mock;
    drain: jest.Mock;
  };

  beforeEach(() => {
    configService = {
      get: jest.fn((key: string) => {
        if (key === 'LOADTEST_RESET_ENABLED') return 'true';
        if (key === 'LOADTEST_RESET_TOKEN') return 'secret-token';
        return undefined;
      }),
    };

    const campaignRepo = {
      find: jest.fn().mockResolvedValue([{ id: 'campaign-1' }]),
      update: jest.fn().mockResolvedValue({ affected: 1 }),
    };
    const viewLogRepo = {
      find: jest.fn().mockResolvedValue([{ id: 10 }]),
      delete: jest.fn().mockResolvedValue({ affected: 1 }),
      createQueryBuilder: jest.fn(() => ({
        delete: jest.fn().mockReturnThis(),
        execute: jest.fn().mockResolvedValue({ affected: 0 }),
      })),
    };
    const clickLogRepo = {
      delete: jest.fn().mockResolvedValue({ affected: 1 }),
      createQueryBuilder: jest.fn(() => ({
        delete: jest.fn().mockReturnThis(),
        execute: jest.fn().mockResolvedValue({ affected: 0 }),
      })),
    };
    const bidLogRepo = {
      delete: jest.fn().mockResolvedValue({ affected: 2 }),
      createQueryBuilder: jest.fn(() => ({
        delete: jest.fn().mockReturnThis(),
        execute: jest.fn().mockResolvedValue({ affected: 0 }),
      })),
    };

    dataSource = {
      getRepository: jest.fn((entity: unknown) => {
        if (entity === CampaignEntity) return campaignRepo;
        throw new Error('Unexpected repository');
      }),
      transaction: jest.fn(
        async (
          callback: (manager: {
            getRepository: (entity: unknown) => unknown;
          }) => Promise<unknown>
        ) =>
          callback({
            getRepository: (entity: unknown) => {
              if (entity === CampaignEntity) return campaignRepo;
              if (entity === ViewLogEntity) return viewLogRepo;
              if (entity === ClickLogEntity) return clickLogRepo;
              if (entity === BidLogEntity) return bidLogRepo;
              throw new Error('Unexpected manager repository');
            },
          })
      ),
    };

    ioRedisClient = {
      scan: jest.fn().mockResolvedValue(['0', []]),
      del: jest.fn().mockResolvedValue(0),
    };

    campaignBudgetRepository = {
      resetForLoadTest: jest.fn().mockResolvedValue(true),
    };

    bidlogQueue = {
      getJobCounts: jest.fn().mockResolvedValue({
        active: 0,
        waiting: 0,
        delayed: 0,
        prioritized: 0,
        paused: 0,
        completed: 0,
        failed: 0,
      }),
      drain: jest.fn().mockResolvedValue(undefined),
    };

    service = new LoadtestService(
      configService as unknown as ConfigService,
      dataSource as never,
      ioRedisClient as never,
      campaignBudgetRepository as unknown as CampaignBudgetRepository,
      bidlogQueue as never
    );
  });

  it('resets campaign spent, logs, and redis aux state when token is valid', async () => {
    const result = await service.resetRtbState({}, 'secret-token');

    expect(result.scopedCampaignIds).toEqual(['campaign-1']);
    expect(result.counts.dbCampaignsReset).toBe(1);
    expect(result.counts.deletedClickLogs).toBe(1);
    expect(result.counts.deletedViewLogs).toBe(1);
    expect(result.counts.deletedBidLogs).toBe(2);
    expect(campaignBudgetRepository.resetForLoadTest).toHaveBeenCalledWith(
      'campaign-1'
    );
  });

  it('rejects reset when bidlog queue has active jobs', async () => {
    bidlogQueue.getJobCounts.mockResolvedValueOnce({
      active: 1,
      waiting: 0,
      delayed: 0,
      prioritized: 0,
      paused: 0,
      completed: 0,
      failed: 0,
    });

    await expect(service.resetRtbState({}, 'secret-token')).rejects.toThrow(
      ConflictException
    );
  });
});
