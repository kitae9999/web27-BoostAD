import {
  BadRequestException,
  ConflictException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { CreateViewLogDto } from './dto/create-view-log.dto';
import { LogRepository } from 'src/log/repository/log.repository.interface';
import { CacheRepository } from 'src/cache/repository/cache.repository.interface';
import { CreateClickLogDto } from './dto/create-click-log.dto';
import { CreateDismissLogDto } from './dto/create-dismiss-log.dto';
import { CampaignCacheRepository } from 'src/campaign/repository/campaign.cache.repository.interface';
import { CampaignRepository } from 'src/campaign/repository/campaign.repository.interface';
import { BlogRepository } from 'src/blog/repository/blog.repository.interface';
import { UserRepository } from 'src/user/repository/user.repository.interface';
import { UserRole } from 'src/user/entities/user.entity';
import { ConfigService } from '@nestjs/config';
import { MetricsService } from 'src/metrics/metrics.service';
import { buildClickAbuseDedupKey } from './click-abuse-key.util';

@Injectable()
export class SdkService {
  private readonly logger = new Logger(SdkService.name);
  private readonly reservationLifecycleEnabled: boolean;
  private readonly reservationResultTtlSeconds: number;
  private readonly clickAbuseWindowSeconds: number;

  constructor(
    private readonly logRepository: LogRepository,
    private readonly cacheRepository: CacheRepository,
    private readonly campaignCacheRepository: CampaignCacheRepository,
    private readonly campaignRepository: CampaignRepository,
    private readonly blogRepository: BlogRepository,
    private readonly userRepository: UserRepository,
    private readonly metricsService: MetricsService,
    private readonly configService: ConfigService
  ) {
    this.reservationLifecycleEnabled =
      this.configService.get<string>('RTB_BUDGET_MODE') ===
      'reservation_lifecycle';
    this.reservationResultTtlSeconds = this.getPositiveIntConfig(
      'RTB_AUCTION_RESULT_TTL_SECONDS',
      30 * 60
    );
    this.clickAbuseWindowSeconds = this.getPositiveIntConfig(
      'RTB_CLICK_ABUSE_WINDOW_SECONDS',
      15 * 60
    );
  }

  async recordView(dto: CreateViewLogDto, visitorId: string) {
    if (this.reservationLifecycleEnabled) {
      return this.recordReservationView(dto);
    }

    const {
      auctionId,
      campaignId,
      postUrl,
      isHighIntent,
      behaviorScore,
      positionRatio,
    } = dto;

    if (!campaignId) {
      throw new BadRequestException('campaignId가 필요합니다.');
    }

    const auctionData = await this.cacheRepository.getAuctionData(auctionId);
    if (!auctionData) {
      throw new NotFoundException('404 not found');
    }

    const { blogId, cost } = auctionData;

    const dedupResult = await this.cacheRepository.acquireViewIdempotencyKey(
      postUrl,
      visitorId,
      isHighIntent
    );
    if (dedupResult.status === 'exists') {
      const existingViewId = dedupResult.viewId;

      // 중복 viewId의 경우 Rollback 정보가 없을 수 있으므로 재생성
      const existingRollback =
        await this.cacheRepository.getRollbackInfo(existingViewId);
      if (!existingRollback) {
        this.logger.debug(
          `[SDK ViewLog] 중복 viewId=${existingViewId} Rollback 정보 재생성: campaign=${campaignId}, cost=${cost}`
        );
        await this.cacheRepository.setRollbackInfo(existingViewId, {
          campaignId,
          cost,
          createdAt: new Date().toISOString(),
        });

        // Rollback 백업 정보 저장 (기본 TTL - Worker용)
        await this.cacheRepository.setRollbackBackup(existingViewId, {
          campaignId,
          cost,
          createdAt: new Date().toISOString(),
        });
      }

      return existingViewId;
    }

    if (dedupResult.status === 'locked') {
      const existingViewId =
        await this.cacheRepository.getViewIdByIdempotencyKey(
          postUrl,
          visitorId,
          isHighIntent
        );
      if (existingViewId !== null) {
        return existingViewId;
      }
      throw new ConflictException('중복 요청 처리 중입니다.');
    }

    const viewId = await this.logRepository.saveViewLog({
      auctionId,
      campaignId,
      blogId,
      postUrl,
      cost,
      positionRatio: positionRatio ?? null,
      isHighIntent,
      behaviorScore,
    });

    await this.cacheRepository.setViewIdempotencyKey(
      postUrl,
      visitorId,
      isHighIntent,
      viewId
    );

    // Rollback 정보 Redis 저장 (기본 TTL)
    await this.cacheRepository.setRollbackInfo(viewId, {
      campaignId,
      cost,
      createdAt: new Date().toISOString(),
    });

    // Rollback 백업 정보 저장 (기본 TTL - Worker용)
    await this.cacheRepository.setRollbackBackup(viewId, {
      campaignId,
      cost,
      createdAt: new Date().toISOString(),
    });

    this.logger.debug(
      `[SDK ViewLog] Rollback 정보 저장: viewId=${viewId}, campaign=${campaignId}, cost=${cost} (TTL + Backup)`
    );

    return viewId;
  }

  private async recordReservationView(dto: CreateViewLogDto): Promise<number> {
    const reservation =
      await this.campaignCacheRepository.getAuctionReservation(dto.auctionId);
    if (!reservation || reservation.status === 'RELEASED') {
      throw new NotFoundException('유효한 auction reservation이 없습니다.');
    }
    if (dto.campaignId && dto.campaignId !== reservation.campaignId) {
      throw new BadRequestException(
        'campaignId가 auction winner와 일치하지 않습니다.'
      );
    }

    const dedupResult =
      await this.cacheRepository.acquireAuctionViewIdempotencyKey(
        dto.auctionId
      );
    if (dedupResult.status === 'exists') return dedupResult.viewId;
    if (dedupResult.status === 'locked') {
      const existingViewId =
        await this.cacheRepository.getAuctionViewIdByIdempotencyKey(
          dto.auctionId
        );
      if (existingViewId !== null) return existingViewId;
      throw new ConflictException('동일 auction의 View를 처리 중입니다.');
    }

    const viewId = await this.logRepository.saveViewLog({
      auctionId: dto.auctionId,
      campaignId: reservation.campaignId,
      blogId: reservation.blogId,
      postUrl: dto.postUrl,
      cost: reservation.reservedAmount,
      positionRatio: dto.positionRatio ?? null,
      isHighIntent: dto.isHighIntent,
      behaviorScore: dto.behaviorScore,
    });
    await this.cacheRepository.setAuctionViewIdempotencyKey(
      dto.auctionId,
      viewId
    );
    return viewId;
  }

  async recordClick(
    dto: CreateClickLogDto,
    visitorId: string
  ): Promise<number | null> {
    const { viewId } = dto;

    if (this.reservationLifecycleEnabled) {
      return this.recordReservationClick(viewId, visitorId);
    }

    const exists = await this.logRepository.existsByViewId(viewId);
    if (!exists) {
      throw new BadRequestException('잘못된 요청입니다.');
    }

    // Dismiss/TTL 만료 후 클릭 방지: rollbackInfo 또는 backup이 없으면 이미 롤백된 것
    const rollbackInfo = await this.cacheRepository.getRollbackInfo(viewId);
    const rollbackBackup = await this.cacheRepository.getRollbackBackup(viewId);

    if (!rollbackInfo && !rollbackBackup) {
      this.logger.warn(
        `[SDK ClickLog] 롤백된 View 클릭 시도: viewId=${viewId} (Dismiss/TTL 만료 후 클릭 무효)`
      );
      return null;
    }
    const isDup = await this.cacheRepository.setClickIdempotencyKey(viewId);

    if (isDup) {
      this.logger.debug(`[SDK ClickLog] 중복 클릭: viewId=${viewId}`);

      // 중복 클릭 시 Spent 롤백
      const rollbackInfo = await this.cacheRepository.getRollbackInfo(viewId);
      if (rollbackInfo) {
        const { campaignId, cost } = rollbackInfo;
        await this.campaignCacheRepository.decrementSpent(campaignId, cost);

        // Rollback 정보 + 백업 삭제
        await this.cacheRepository.deleteRollbackInfo(viewId);
        await this.cacheRepository.deleteRollbackBackup(viewId);

        this.logger.log(
          `[SDK ClickLog] 중복 클릭 롤백 완료: viewId=${viewId}, campaign=${campaignId}, cost=-${cost}`
        );
      }

      return null;
    }

    // 클릭 시 Rollback 정보 + 백업 삭제 (Dismiss Beacon이 와도 무시되도록)
    await this.cacheRepository.deleteRollbackInfo(viewId);
    await this.cacheRepository.deleteRollbackBackup(viewId);

    return this.persistClickAndRevenue(viewId);
  }

  private async recordReservationClick(
    viewId: number,
    visitorId: string
  ): Promise<number | null> {
    const viewLog = await this.logRepository.getViewLog(viewId);
    if (!viewLog) {
      throw new BadRequestException('잘못된 요청입니다.');
    }
    const transition = await this.campaignCacheRepository.commitAuction(
      viewLog.auctionId,
      this.getKstBudgetDate(Date.now()),
      this.reservationResultTtlSeconds,
      {
        dedupKey: buildClickAbuseDedupKey({
          visitorId,
          postUrl: viewLog.postUrl ?? '',
          isHighIntent: viewLog.isHighIntent,
        }),
        dedupTtlSeconds: this.clickAbuseWindowSeconds,
      }
    );
    this.metricsService.recordRtbAuctionTransition(
      'commit',
      transition.outcome
    );
    if (transition.outcome === 'duplicate_released') {
      this.logger.debug(
        `[SDK ClickLog] 반복 클릭 예약 해제: auctionId=${viewLog.auctionId}`
      );
      return null;
    }
    if (
      transition.outcome !== 'committed' &&
      transition.outcome !== 'already_committed'
    ) {
      this.logger.warn(
        `[SDK ClickLog] 확정할 수 없는 auction: auctionId=${viewLog.auctionId}, outcome=${transition.outcome}`
      );
      return null;
    }

    const isDup = await this.cacheRepository.setClickIdempotencyKey(
      viewId,
      this.reservationResultTtlSeconds * 1000
    );
    if (isDup) return null;
    return this.persistClickAndRevenue(viewId);
  }

  private async persistClickAndRevenue(viewId: number): Promise<number> {
    const clickId = await this.logRepository.saveClickLog({ viewId });

    // DB dailySpent 동기화: ClickLog 저장과 함께 DB에도 spent 증가
    const viewLog = await this.logRepository.getViewLog(viewId);
    if (viewLog) {
      this.campaignRepository.incrementSpent(viewLog.campaignId, viewLog.cost); // await 기다릴 필요 없음
      this.logger.debug(
        `[SDK ClickLog] DB dailySpent 동기화: campaign=${viewLog.campaignId}, cost=+${viewLog.cost}`
      );
    }

    this.logger.log(
      `[SDK ClickLog] 클릭 기록 완료: viewId=${viewId}, clickId=${clickId} (예산 확정 + DB 동기화)`
    );

    // 퍼블리셔 수익 지급 (cost의 80%, PUBLISHER만)
    try {
      const viewLogData =
        await this.logRepository.getBlogIdAndCostByViewId(viewId);
      if (viewLogData) {
        const { blogId, cost } = viewLogData;
        // TODO(Blog)(보류): Redis에서 불러오면 더 빠르지 않을까 싶음 - 비딩 성능에 직접적 영향 X
        const publisherId = await this.blogRepository.getUserIdByBlogId(blogId);

        if (publisherId) {
          // PUBLISHER 역할 확인
          const isPublisher = await this.userRepository.verifyRole(
            publisherId,
            UserRole.PUBLISHER
          );

          if (isPublisher) {
            const revenue = Math.floor(cost * 0.8);
            await this.userRepository.incrementBalance(publisherId, revenue);
            this.logger.log(
              `[SDK ClickLog] 퍼블리셔 수익 지급: publisherId=${publisherId}, revenue=${revenue} (cost=${cost})`
            );
          } else {
            this.logger.debug(
              `[SDK ClickLog] 수익 지급 스킵: userId=${publisherId}는 퍼블리셔가 아님`
            );
          }
        }
      }
    } catch (error) {
      this.logger.error(
        `[SDK ClickLog] 퍼블리셔 수익 지급 실패: viewId=${viewId}`,
        error
      );
      // 수익 지급 실패해도 클릭 로그는 유지
    }

    return clickId;
  }

  async recordDismiss(dto: CreateDismissLogDto): Promise<void> {
    const { viewId } = dto;

    this.logger.debug(`[SDK Dismiss] 시작: viewId=${viewId}`);

    if (this.reservationLifecycleEnabled) {
      const viewLog = await this.logRepository.getViewLog(viewId);
      if (!viewLog) return;
      const transition = await this.campaignCacheRepository.releaseAuction(
        viewLog.auctionId,
        this.reservationResultTtlSeconds
      );
      this.metricsService.recordRtbAuctionTransition(
        'dismiss_release',
        transition.outcome
      );
      this.logger.debug(
        `[SDK Dismiss] auction release: auctionId=${viewLog.auctionId}, outcome=${transition.outcome}`
      );
      return;
    }

    // 1. Redis에서 Rollback 정보 조회
    const rollbackInfo = await this.cacheRepository.getRollbackInfo(viewId);
    if (!rollbackInfo) {
      // TTL 만료 또는 이미 처리됨 → 무시
      this.logger.warn(
        `[SDK Dismiss] RollbackInfo not found for viewId=${viewId} (TTL 만료 또는 이미 처리됨)`
      );
      return;
    }

    const { campaignId, cost, createdAt } = rollbackInfo;
    const elapsedMs = Date.now() - new Date(createdAt).getTime();

    this.logger.debug(
      `[SDK Dismiss] RollbackInfo 조회 성공: viewId=${viewId}, campaign=${campaignId}, cost=${cost}, elapsed=${Math.floor(elapsedMs / 1000)}s`
    );

    // 2. Spent 롤백 (Phase 2에서 구현된 메서드 사용)
    await this.campaignCacheRepository.decrementSpent(campaignId, cost);

    // 3. Redis에서 Rollback 정보 + 백업 삭제 (중복 방지)
    await this.cacheRepository.deleteRollbackInfo(viewId);
    await this.cacheRepository.deleteRollbackBackup(viewId);

    this.logger.log(
      `[SDK Dismiss] 롤백 완료: viewId=${viewId}, campaign=${campaignId}, cost=-${cost} (일일/총), elapsed=${Math.floor(elapsedMs / 1000)}s`
    );
  }

  private getKstBudgetDate(nowMs: number): string {
    const kst = new Date(nowMs + 9 * 60 * 60 * 1000);
    return `${kst.getUTCFullYear()}-${String(kst.getUTCMonth() + 1).padStart(2, '0')}-${String(kst.getUTCDate()).padStart(2, '0')}`;
  }

  private getPositiveIntConfig(name: string, defaultValue: number): number {
    const raw = this.configService.get<string>(name);
    const parsed = raw ? Number.parseInt(raw, 10) : defaultValue;
    return Number.isFinite(parsed) && parsed > 0 ? parsed : defaultValue;
  }
}
