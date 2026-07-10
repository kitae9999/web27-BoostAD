import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { CampaignCacheRepository } from '../../campaign/repository/campaign.cache.repository.interface';
import { MetricsService } from '../../metrics/metrics.service';

type CampaignIdentity = { id: string };

@Injectable()
export class BudgetEligibilityHintService implements OnModuleInit {
  private readonly logger = new Logger(BudgetEligibilityHintService.name);
  private readonly enabled: boolean;
  private readonly refreshMs: number;
  private exhaustedIds = new Set<string>();
  private nextRefreshAt = 0;
  private refreshInFlight: Promise<void> | null = null;

  constructor(
    private readonly campaignCacheRepository: CampaignCacheRepository,
    private readonly metricsService: MetricsService,
    private readonly configService: ConfigService
  ) {
    this.enabled =
      this.configService.get<string>(
        'RTB_BUDGET_ELIGIBILITY_HINT_ENABLED',
        'true'
      ) === 'true';
    this.refreshMs = this.getPositiveInt(
      'RTB_BUDGET_ELIGIBILITY_HINT_REFRESH_MS',
      250
    );
  }

  async onModuleInit(): Promise<void> {
    if (this.enabled) {
      await this.refreshSnapshot();
    }
  }

  filterEligible<T extends CampaignIdentity>(campaigns: T[]): T[] {
    if (!this.enabled || campaigns.length === 0) {
      return campaigns;
    }

    this.scheduleRefreshIfStale();
    const eligible = campaigns.filter(
      (campaign) => !this.exhaustedIds.has(campaign.id)
    );
    const excludedCount = campaigns.length - eligible.length;
    if (excludedCount > 0) {
      this.metricsService.incRtbBudgetHintExcluded(excludedCount);
    }
    return eligible;
  }

  private scheduleRefreshIfStale(): void {
    if (Date.now() < this.nextRefreshAt || this.refreshInFlight) {
      return;
    }
    void this.refreshSnapshot();
  }

  private async refreshSnapshot(): Promise<void> {
    if (this.refreshInFlight) {
      return this.refreshInFlight;
    }

    this.refreshInFlight = (async () => {
      try {
        const ids =
          await this.campaignCacheRepository.getBudgetExhaustedCampaignIds();
        this.exhaustedIds = new Set(ids);
        this.nextRefreshAt = Date.now() + this.refreshMs;
        this.metricsService.setRtbBudgetHintSnapshotSize(
          this.exhaustedIds.size
        );
      } catch (error) {
        // Redis 장애 때 빈 set으로 덮으면 소진 캠페인이 대량 재진입한다.
        // 마지막 성공 snapshot을 유지하고 Lua 최종 검증에 맡긴다.
        this.nextRefreshAt = Date.now() + Math.min(this.refreshMs, 1_000);
        this.metricsService.incRtbBudgetHintRefreshError();
        this.logger.warn('예산 소진 hint snapshot 갱신 실패', error);
      } finally {
        this.refreshInFlight = null;
      }
    })();

    return this.refreshInFlight;
  }

  private getPositiveInt(name: string, defaultValue: number): number {
    const raw = this.configService.get<string>(name);
    const parsed = raw ? Number.parseInt(raw, 10) : defaultValue;
    return Number.isFinite(parsed) && parsed > 0 ? parsed : defaultValue;
  }
}
