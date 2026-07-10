import { ConfigService } from '@nestjs/config';
import { CampaignCronService } from './campaign-cron.service';
import { CampaignStatus } from './entities/campaign.entity';

describe('CampaignCronService budget eligibility separation', () => {
  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('does not persist PAUSED status only because daily budget is exhausted', async () => {
    const overspentActiveCampaign = {
      id: 'campaign-1',
      status: CampaignStatus.ACTIVE,
      deletedAt: null,
      startDate: '2026-01-01T00:00:00.000Z',
      endDate: '2099-01-01T00:00:00.000Z',
      dailyBudget: 100,
      dailySpent: 100,
      totalBudget: 1_000,
      totalSpent: 100,
    };
    const campaignRepository = {
      getAll: jest.fn().mockResolvedValue([]),
      updateStatus: jest.fn(),
      resetAllDailySpent: jest.fn().mockResolvedValue(undefined),
    };
    const campaignCacheRepository = {
      getAllCampaigns: jest.fn().mockResolvedValue([overspentActiveCampaign]),
      updateCampaignStatus: jest.fn().mockResolvedValue(undefined),
      resetDailySpentCache: jest.fn().mockResolvedValue(undefined),
    };
    const service = new CampaignCronService(
      campaignRepository as never,
      campaignCacheRepository as never,
      {} as never,
      {
        get: jest.fn().mockReturnValue('test'),
      } as unknown as ConfigService,
      {} as never
    );

    const resetPromise = service.manualReset();
    await jest.runAllTimersAsync();
    const result = await resetPromise;

    expect(result.statusUpdate.paused).toBe(0);
    expect(campaignRepository.updateStatus).not.toHaveBeenCalledWith(
      'campaign-1',
      CampaignStatus.PAUSED
    );
    // Redis의 짧은 reset lock은 사용하지만 원래 ACTIVE 상태로 복구한다.
    expect(
      campaignCacheRepository.updateCampaignStatus
    ).toHaveBeenNthCalledWith(1, 'campaign-1', CampaignStatus.PAUSED);
    expect(
      campaignCacheRepository.updateCampaignStatus
    ).toHaveBeenNthCalledWith(2, 'campaign-1', CampaignStatus.ACTIVE);
  });
});
