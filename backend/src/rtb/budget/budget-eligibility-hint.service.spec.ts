import { ConfigService } from '@nestjs/config';
import { CampaignCacheRepository } from '../../campaign/repository/campaign.cache.repository.interface';
import { MetricsService } from '../../metrics/metrics.service';
import { BudgetEligibilityHintService } from './budget-eligibility-hint.service';

describe('BudgetEligibilityHintService', () => {
  const buildService = () => {
    const repository = {
      getBudgetExhaustedCampaignIds: jest
        .fn()
        .mockResolvedValue(['daily-exhausted']),
    } as unknown as CampaignCacheRepository & {
      getBudgetExhaustedCampaignIds: jest.Mock;
    };
    const metrics = {
      incRtbBudgetHintExcluded: jest.fn(),
      setRtbBudgetHintSnapshotSize: jest.fn(),
      incRtbBudgetHintRefreshError: jest.fn(),
    } as unknown as MetricsService & {
      incRtbBudgetHintExcluded: jest.Mock;
      setRtbBudgetHintSnapshotSize: jest.Mock;
      incRtbBudgetHintRefreshError: jest.Mock;
    };
    const config = {
      get: jest.fn((key: string, defaultValue?: string) => {
        if (key === 'RTB_BUDGET_ELIGIBILITY_HINT_REFRESH_MS') return '250';
        return defaultValue;
      }),
    } as unknown as ConfigService;

    return {
      service: new BudgetEligibilityHintService(repository, metrics, config),
      repository,
      metrics,
    };
  };

  it('filters exhausted campaigns from the in-memory snapshot', async () => {
    const { service, metrics } = buildService();
    await service.onModuleInit();

    expect(
      service.filterEligible([{ id: 'daily-exhausted' }, { id: 'available' }])
    ).toEqual([{ id: 'available' }]);
    expect(metrics.incRtbBudgetHintExcluded).toHaveBeenCalledWith(1);
    expect(metrics.setRtbBudgetHintSnapshotSize).toHaveBeenCalledWith(1);
  });

  it('filters ANN hits by campaign ID before campaign hydration', async () => {
    const { service } = buildService();
    await service.onModuleInit();

    expect(
      service.filterEligibleBy(
        [
          { campaignId: 'daily-exhausted', similarity: 0.9 },
          { campaignId: 'available', similarity: 0.8 },
        ],
        (hit) => hit.campaignId
      )
    ).toEqual([{ campaignId: 'available', similarity: 0.8 }]);
  });

  it('keeps the last successful snapshot when Redis refresh fails', async () => {
    const { service, repository, metrics } = buildService();
    await service.onModuleInit();
    repository.getBudgetExhaustedCampaignIds.mockRejectedValueOnce(
      new Error('redis unavailable')
    );

    // onModuleInit을 재호출해 refresh failure 경로를 결정적으로 검증한다.
    await service.onModuleInit();

    expect(service.filterEligible([{ id: 'daily-exhausted' }])).toEqual([]);
    expect(metrics.incRtbBudgetHintRefreshError).toHaveBeenCalledTimes(1);
  });

  it('can disable prefilter without changing final Lua reservation', async () => {
    const repository = {
      getBudgetExhaustedCampaignIds: jest.fn(),
    } as unknown as CampaignCacheRepository;
    const metrics = {} as MetricsService;
    const config = {
      get: jest.fn((key: string, defaultValue?: string) =>
        key === 'RTB_BUDGET_ELIGIBILITY_HINT_ENABLED' ? 'false' : defaultValue
      ),
    } as unknown as ConfigService;
    const service = new BudgetEligibilityHintService(
      repository,
      metrics,
      config
    );

    await service.onModuleInit();
    expect(service.filterEligible([{ id: 'exhausted' }])).toEqual([
      { id: 'exhausted' },
    ]);
  });
});
