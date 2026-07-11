import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { CampaignCacheRepository } from '../../campaign/repository/campaign.cache.repository.interface';
import { MetricsService } from '../../metrics/metrics.service';

type CampaignIdentity = { id: string };

/**
 * 예산 소진 캠페인 사전 제외용 힌트 서비스
 *
 * Redis SET(daily/total exhausted)을 인스턴스 로컬 Set으로 짧게 캐시하고,
 * matcher 후보에서 소진 ID를 메모리로 걸러낸다.
 * 실제 예약 가능 여부는 여전히 reserve Lua가 campaign JSON으로 최종 검증한다.
 */
@Injectable()
export class BudgetEligibilityHintService implements OnModuleInit {
  private readonly logger = new Logger(BudgetEligibilityHintService.name);
  /** false면 사전 제외를 끄고 Phase 1A처럼 Lua만 사용 */
  private readonly enabled: boolean;
  /** 로컬 snapshot 재사용 주기 (기본 250ms) */
  private readonly refreshMs: number;
  /** Redis exhausted SET을 합친 로컬 스냅샷 */
  private exhaustedIds = new Set<string>();
  /** 이 시각 이전에는 Redis를 다시 읽지 않음 */
  private nextRefreshAt = 0;
  /** 동시에 여러 refresh가 나가지 않도록 single-flight */
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

  /** Nest 모듈 초기 직후: 첫 요청 전에 exhausted 목록을 미리 로드 */
  async onModuleInit(): Promise<void> {
    if (this.enabled) {
      await this.refreshSnapshot();
    }
  }

  /**
   * matcher hot path용 필터.
   * 예산 숫자를 계산하지 않고, 로컬 exhaustedIds에 있는 캠페인만 제외한다.
   * Redis 조회는 여기서 기다리지 않는다 (필요 시 background refresh만 예약).
   */
  filterEligible<T extends CampaignIdentity>(campaigns: T[]): T[] {
    return this.filterEligibleBy(campaigns, (campaign) => campaign.id);
  }

  filterEligibleBy<T>(items: T[], selectId: (item: T) => string): T[] {
    if (!this.enabled || items.length === 0) {
      return items;
    }

    this.scheduleRefreshIfStale();
    const eligible = items.filter(
      (item) => !this.exhaustedIds.has(selectId(item))
    );
    const excludedCount = items.length - eligible.length;
    if (excludedCount > 0) {
      this.metricsService.incRtbBudgetHintExcluded(excludedCount);
    }
    return eligible;
  }

  /** TTL이 지났고 진행 중인 refresh가 없으면 백그라운드로 snapshot 갱신 */
  private scheduleRefreshIfStale(): void {
    if (Date.now() < this.nextRefreshAt || this.refreshInFlight) {
      return;
    }
    void this.refreshSnapshot();
  }

  /**
   * Redis daily/total exhausted SET → 로컬 Set 교체.
   * 실패 시 빈 set으로 덮지 않고 마지막 성공 값을 유지한다 (fail-open이 아니라 stale 유지).
   */
  private async refreshSnapshot(): Promise<void> {
    if (this.refreshInFlight) {
      return this.refreshInFlight;
    }

    this.refreshInFlight = (async () => {
      try {
        const ids =
          await this.campaignCacheRepository.getBudgetExhaustedCampaignIds(); // Redis로부터 새로운 목록받아와서 인메모리에 저장
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
