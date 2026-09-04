import {
  BadRequestException,
  ForbiddenException,
  Injectable,
  Logger,
  NotFoundException,
  Optional,
} from '@nestjs/common';
import { OnEvent } from '@nestjs/event-emitter';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import type { EmbeddingJobData } from 'src/queue/types/queue.type';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource, In, IsNull, type EntityManager } from 'typeorm';
import { randomUUID } from 'node:crypto';
import { CampaignRepository } from './repository/campaign.repository.interface';
import { CreateCampaignDto } from './dto/create-campaign.dto';
import { UpdateCampaignDto } from './dto/update-campaign.dto';
import { GetCampaignListDto } from './dto/get-campaign-list.dto';
import { CampaignEntity, CampaignStatus } from './entities/campaign.entity';
import { TagEntity } from '../tag/entities/tag.entity';
import type {
  CachedCampaign,
  CachedCampaignWithoutSpent,
  CampaignWithStats,
  CampaignWithTags,
} from './types/campaign.types';
import { AVAILABLE_TAGS } from '../common/constants';
import { UserRepository } from 'src/user/repository/user.repository.interface';
import { CampaignCacheRepository } from './repository/campaign.cache.repository.interface';
// import { CreditHistoryRepository } from 'src/advertiser/repository/credit-history.repository.interface';
import { LogRepository } from 'src/log/repository/log.repository.interface';
import { UserEntity } from 'src/user/entities/user.entity';
import {
  CreditHistoryEntity,
  CreditHistoryType,
} from 'src/advertiser/entities/credit-history.entity';
import { ConfigService } from '@nestjs/config';
import {
  resolveEmbeddingProfile,
  toEmbeddingNamespace,
  type EmbeddingProfile,
} from 'src/rtb/ml/embedding-profile';
import { EMBEDDING_QUEUE_NAME } from 'src/queue/queue.names';
import { CampaignProjectionOutboxWriter } from './projection/campaign-projection-outbox.writer';
import { CampaignProjectionEventType } from './projection/campaign-projection.types';

type CampaignProjectionMode = 'off' | 'shadow' | 'active';

@Injectable()
export class CampaignService {
  private readonly logger = new Logger(CampaignService.name);
  private readonly embeddingProfile: EmbeddingProfile;
  private readonly requireDocumentEmbedding: boolean;
  private readonly projectionMode: CampaignProjectionMode;
  private readonly outboxWriter: CampaignProjectionOutboxWriter;

  constructor(
    private readonly campaignRepository: CampaignRepository,
    private readonly userRepository: UserRepository,
    private readonly campaignCacheRepository: CampaignCacheRepository,
    private readonly logRepository: LogRepository,
    @InjectDataSource() private readonly dataSource: DataSource,
    @InjectQueue(EMBEDDING_QUEUE_NAME)
    private readonly embeddingQueue: Queue<EmbeddingJobData>,
    configService: ConfigService,
    @Optional() outboxWriter?: CampaignProjectionOutboxWriter
  ) {
    this.outboxWriter = outboxWriter ?? new CampaignProjectionOutboxWriter();
    this.embeddingProfile = resolveEmbeddingProfile(
      configService.get<string>('RTB_EMBEDDING_PROFILE')
    );
    this.requireDocumentEmbedding =
      configService.get<string>(
        'RTB_DENSE_RETRIEVAL_MODE',
        'semantic_document'
      ) === 'semantic_document';
    const configuredMode = configService.get<string>(
      'CAMPAIGN_PROJECTION_MODE',
      'off'
    );
    this.projectionMode =
      configuredMode === 'active' || configuredMode === 'shadow'
        ? configuredMode
        : 'off';
  }

  @OnEvent('ml.model.ready')
  onModelReady(): void {
    if (this.projectionMode === 'active') {
      this.logger.log('Campaign 초기 적재는 Projection Worker가 담당합니다.');
      return;
    }
    this.logger.log('🚀 Campaign 초기 로딩 시작 (ML 모델 준비 완료)');

    // 백그라운드 실행 (await 없음)
    this.loadAllCampaigns().catch((error) => {
      this.logger.error('Campaign 초기 로딩 실패:', error);
    });
  }

  private async loadAllCampaigns(): Promise<void> {
    try {
      const campaigns = await this.campaignRepository.getAll();

      this.logger.log(`📦 총 ${campaigns.length}개 Campaign 로딩 중...`);

      let loaded = 0;
      let embeddingQueued = 0;

      for (const campaign of campaigns) {
        const cached = await this.campaignCacheRepository.findCampaignCacheById(
          campaign.id
        );
        const campaignCache = this.mergeReusableEmbeddings(
          this.convertToCachedCampaignType(campaign),
          cached
        );

        // Redis에 캐싱
        await this.campaignCacheRepository.saveCampaignCacheById(
          campaign.id,
          campaignCache
        );

        loaded++;

        if (!this.hasRequiredEmbeddings(campaignCache)) {
          const queued = await this.enqueueInitialCampaignEmbedding(
            campaign.id
          );
          if (queued) {
            embeddingQueued++;
          }
        }

        // 진행 상황 로깅 (100개당 1번)
        if (loaded % 100 === 0) {
          this.logger.log(
            `📊 Campaign 로딩 진행: ${loaded}/${campaigns.length}`
          );
        }
      }

      this.logger.log(
        `✅ Campaign 로딩 완료: ${loaded}개, 임베딩 큐: ${embeddingQueued}개`
      );
    } catch (error) {
      this.logger.error('Campaign 로딩 중 에러 발생:', error);
      throw error;
    }
  }

  private mergeReusableEmbeddings(
    campaign: CachedCampaign,
    cached: CachedCampaign | null
  ): CachedCampaign {
    if (!campaign.tags || !cached?.embeddingTags) {
      return campaign;
    }

    const sameModel =
      cached.embeddingModelVersion === this.embeddingProfile.modelVersion ||
      (this.embeddingProfile.name === 'legacy_minilm' &&
        !cached.embeddingModelVersion);
    if (!sameModel) {
      return campaign;
    }

    const reusableEmbeddingTags = Object.fromEntries(
      campaign.tags
        .filter(
          (tagName) =>
            cached.embeddingTags?.[tagName]?.length ===
            this.embeddingProfile.dimension
        )
        .map((tagName) => [tagName, cached.embeddingTags![tagName]])
    );

    if (Object.keys(reusableEmbeddingTags).length === 0) {
      return campaign;
    }

    return {
      ...campaign,
      embeddingTags: reusableEmbeddingTags,
      embeddingModelVersion: this.embeddingProfile.modelVersion,
      ...(cached.embeddingDocument?.length === this.embeddingProfile.dimension
        ? { embeddingDocument: cached.embeddingDocument }
        : {}),
    };
  }

  private hasRequiredEmbeddings(campaign: CachedCampaign): boolean {
    const hasTags = Boolean(
      campaign.tags?.length &&
      campaign.tags.every(
        (tagName) =>
          campaign.embeddingTags?.[tagName]?.length ===
          this.embeddingProfile.dimension
      )
    );
    const sameModel =
      campaign.embeddingModelVersion === this.embeddingProfile.modelVersion;
    const hasDocument =
      campaign.embeddingDocument?.length === this.embeddingProfile.dimension;
    return Boolean(
      hasTags && sameModel && (!this.requireDocumentEmbedding || hasDocument)
    );
  }

  private async enqueueInitialCampaignEmbedding(
    campaignId: string
  ): Promise<boolean> {
    const jobId = `campaign-embedding-${toEmbeddingNamespace(
      this.embeddingProfile.modelVersion
    )}-${campaignId}`;
    const existingJob = await this.embeddingQueue.getJob(jobId);

    if (existingJob) {
      const state = await existingJob.getState();
      // active/waiting/delayed만 중복 방지. completed·failed·unknown 잔여 jobId는
      // embedding이 비어 있어도 재큐를 막아 document ANN 재색인이 스킵될 수 있다.
      if (state === 'active' || state === 'waiting' || state === 'delayed') {
        return false;
      }
      await existingJob.remove();
    }

    await this.embeddingQueue.add(
      'generate-campaign-embedding',
      { campaignId, modelVersion: this.embeddingProfile.modelVersion },
      {
        jobId,
        removeOnComplete: true,
        removeOnFail: false,
        attempts: 3,
      }
    );
    return true;
  }

  // 캠페인 생성 (태그 검증 + 날짜 유효성 체크 + 시작일 기준 상태 설정 + 크레딧 차감)
  async createCampaign(
    userId: number,
    dto: CreateCampaignDto
  ): Promise<CampaignWithTags> {
    await this.validateBudget({
      userId,
      maxCpc: dto.maxCpc,
      dailyBudget: dto.dailyBudget,
      totalBudget: dto.totalBudget,
      checkBalance: true,
    });
    const tagIds = this.validateAndGetTagIds(dto.tags);

    if (new Date(dto.startDate) > new Date(dto.endDate)) {
      throw new BadRequestException('시작일은 종료일보다 앞서야 합니다.');
    }

    // 시작일이 오늘 이하면 ACTIVE, 내일 이상이면 PENDING
    const initialStatus = this.determineInitialStatus(dto.startDate);

    const campaign = await this.dataSource.transaction(async (manager) => {
      const campaignRepo = manager.getRepository(CampaignEntity);
      const tagRepo = manager.getRepository(TagEntity);
      const tags = await tagRepo.find({ where: { id: In(tagIds) } });
      const entity = campaignRepo.create({
        id: randomUUID(),
        userId,
        servingVersion: 1,
        title: dto.title,
        content: dto.content,
        image: dto.image,
        url: dto.url,
        maxCpc: dto.maxCpc,
        dailyBudget: dto.dailyBudget,
        totalBudget: dto.totalBudget,
        dailySpent: 0,
        totalSpent: 0,
        lastResetDate: new Date(),
        isHighIntent: dto.isHighIntent,
        status: initialStatus,
        startDate: new Date(dto.startDate),
        endDate: new Date(dto.endDate),
        deletedAt: null,
        tags,
      });
      const saved = await campaignRepo.save(entity);
      saved.tags = tags;

      if (dto.totalBudget !== null) {
        const userRepo = manager.getRepository(UserEntity);
        const user = await userRepo.findOne({
          where: { id: userId },
          lock: { mode: 'pessimistic_write' },
        });
        if (!user) {
          throw new NotFoundException('사용자를 찾을 수 없습니다');
        }
        if (user.balance < dto.totalBudget) {
          throw new BadRequestException(
            '총 예산은 보유 잔액을 초과할 수 없습니다.'
          );
        }
        user.balance -= dto.totalBudget;
        await userRepo.save(user);
        await manager.getRepository(CreditHistoryEntity).save({
          userId,
          type: CreditHistoryType.WITHDRAW,
          amount: dto.totalBudget,
          balanceAfter: user.balance,
          campaignId: saved.id,
          operationKey: null,
        });
      }

      await this.outboxWriter.append(
        manager,
        saved,
        CampaignProjectionEventType.UPSERT
      );
      return this.toCampaignWithTags(saved);
    });

    await this.mirrorLegacyCreate(campaign);
    return campaign;
  }

  // 캠페인 목록 조회 (페이지네이션 + 정렬 + 통계)
  async getCampaignList(userId: number, dto: GetCampaignListDto) {
    const { campaigns, total } = await this.campaignRepository.findByUserId(
      userId,
      dto.status,
      dto.limit,
      dto.offset,
      dto.sortBy,
      dto.order
    );

    // 통계 필드 추가
    const campaignsWithStats =
      await this.addStatsToMultipleCampaigns(campaigns);

    // hasMore 계산
    const hasMore = (dto.offset || 0) + (dto.limit || 3) < total;

    return {
      campaigns: campaignsWithStats,
      total,
      hasMore,
    };
  }

  // 특정 캠페인 조회 (소유권 검증 + 통계)
  async getCampaignById(
    campaignId: string,
    userId: number
  ): Promise<CampaignWithStats> {
    // NOTICE : 이 부분은 RTB에 영향 없는 대쉬보드의 요청이기 때문에 바로 DB로 트랜젝션 굳이 수정할 필요 없을 거 같음
    const campaign = await this.campaignRepository.findOne(campaignId, userId);

    if (!campaign) {
      throw new NotFoundException('캠페인을 찾을 수 없습니다.');
    }

    // 통계 필드 추가
    return this.addStatsToCampaign(campaign);
  }

  // 캠페인 수정: DB row와 Outbox event만 같은 transaction에서 확정한다.
  async updateCampaign(
    campaignId: string,
    userId: number,
    dto: UpdateCampaignDto
  ): Promise<CampaignWithTags> {
    const tagIds = dto.tags ? this.validateAndGetTagIds(dto.tags) : undefined;
    let semanticChanged = false;
    const updatedCampaign = await this.dataSource.transaction(
      async (manager) => {
        const campaignRepo = manager.getRepository(CampaignEntity);
        const entity = await campaignRepo.findOne({
          where: { id: campaignId, deletedAt: IsNull() },
          relations: ['tags'],
          lock: { mode: 'pessimistic_write' },
        });
        if (!entity) {
          throw new NotFoundException('캠페인을 찾을 수 없습니다.');
        }
        if (entity.userId !== userId) {
          throw new ForbiddenException('해당 캠페인에 접근할 수 없습니다.');
        }

        const nextStartDate = dto.startDate
          ? new Date(dto.startDate)
          : entity.startDate;
        const nextEndDate = dto.endDate
          ? new Date(dto.endDate)
          : entity.endDate;
        if (nextEndDate <= nextStartDate) {
          throw new BadRequestException('종료일은 시작일보다 이후여야 합니다.');
        }

        const maxCpc = dto.maxCpc ?? entity.maxCpc;
        const dailyBudget = dto.dailyBudget ?? entity.dailyBudget;
        const totalBudget = dto.totalBudget ?? entity.totalBudget;
        await this.validateBudget({
          userId,
          maxCpc,
          dailyBudget,
          totalBudget,
          checkBalance: false,
        });

        if (
          dto.totalBudget !== undefined &&
          dto.totalBudget !== entity.totalBudget
        ) {
          const budgetDiff = dto.totalBudget - (entity.totalBudget ?? 0);
          if (budgetDiff < 0) {
            throw new BadRequestException(
              '캠페인 예산은 감액할 수 없습니다. 기존 예산보다 크거나 같은 값만 설정할 수 있습니다.'
            );
          }
          if (budgetDiff > 0) {
            const userRepo = manager.getRepository(UserEntity);
            const user = await userRepo.findOne({
              where: { id: userId },
              lock: { mode: 'pessimistic_write' },
            });
            if (!user) throw new NotFoundException('사용자를 찾을 수 없습니다');
            if (user.balance < budgetDiff) {
              throw new BadRequestException(
                `잔액이 부족합니다. 필요 금액: ${budgetDiff}원, 보유 잔액: ${user.balance}원`
              );
            }
            user.balance -= budgetDiff;
            await userRepo.save(user);
            await manager.getRepository(CreditHistoryEntity).save({
              userId,
              type: CreditHistoryType.WITHDRAW,
              amount: budgetDiff,
              balanceAfter: user.balance,
              campaignId,
              operationKey: null,
              description: `'${entity.title}' 캠페인 예산 추가`,
            });
          }
        }

        const previousTags = (entity.tags ?? []).map((tag) => tag.name);
        semanticChanged = Boolean(
          dto.title !== undefined ||
          dto.content !== undefined ||
          (dto.tags && !this.areTagsEqual(dto.tags, previousTags))
        );
        if (dto.title !== undefined) entity.title = dto.title;
        if (dto.content !== undefined) entity.content = dto.content;
        if (dto.image !== undefined) entity.image = dto.image;
        if (dto.url !== undefined) entity.url = dto.url;
        if (dto.isHighIntent !== undefined)
          entity.isHighIntent = dto.isHighIntent;
        if (dto.maxCpc !== undefined) entity.maxCpc = dto.maxCpc;
        if (dto.dailyBudget !== undefined) entity.dailyBudget = dto.dailyBudget;
        if (dto.totalBudget !== undefined) entity.totalBudget = dto.totalBudget;
        entity.startDate = nextStartDate;
        entity.endDate = nextEndDate;
        if (
          dto.startDate &&
          (entity.status === CampaignStatus.PENDING ||
            entity.status === CampaignStatus.ACTIVE)
        ) {
          entity.status = this.determineInitialStatus(dto.startDate);
        } else if (dto.status !== undefined) {
          entity.status =
            dto.status === 'ACTIVE'
              ? CampaignStatus.ACTIVE
              : CampaignStatus.PAUSED;
        }
        if (tagIds) {
          entity.tags = await manager
            .getRepository(TagEntity)
            .find({ where: { id: In(tagIds) } });
        }
        entity.servingVersion = Number(entity.servingVersion) + 1;
        const saved = await campaignRepo.save(entity);

        await this.outboxWriter.append(
          manager,
          saved,
          CampaignProjectionEventType.UPSERT
        );
        return this.toCampaignWithTags(saved);
      }
    );

    await this.mirrorLegacyUpdate(updatedCampaign, semanticChanged);
    return updatedCampaign;
  }

  // 캠페인 클릭 히스토리 조회
  async getClickHistory(
    campaignId: string,
    userId: number,
    limit: number = 5,
    offset: number = 0
  ) {
    // 소유권 검증
    const cachedCampaign =
      await this.campaignCacheRepository.findCampaignCacheById(campaignId);

    if (!cachedCampaign) {
      throw new NotFoundException('캠페인을 찾을 수 없습니다.');
    }

    if (cachedCampaign.userId !== userId) {
      throw new ForbiddenException('해당 캠페인에 접근할 수 없습니다.');
    }

    // LogRepository에서 클릭 히스토리 조회
    const { logs, total } =
      await this.logRepository.getClickHistoryByCampaignId(
        campaignId,
        limit,
        offset
      );

    return {
      logs,
      total,
      hasMore: offset + limit < total,
    };
  }

  // 캠페인 삭제 (소프트 삭제, 소유권 검증)
  async deleteCampaign(campaignId: string, userId: number): Promise<void> {
    await this.dataSource.transaction(async (manager) => {
      const campaignRepo = manager.getRepository(CampaignEntity);
      const entity = await campaignRepo.findOne({
        where: { id: campaignId, deletedAt: IsNull() },
        relations: ['tags'],
        lock: { mode: 'pessimistic_write' },
      });
      if (!entity) throw new NotFoundException('캠페인을 찾을 수 없습니다.');
      if (entity.userId !== userId) {
        throw new ForbiddenException('해당 캠페인에 접근할 수 없습니다.');
      }

      entity.status = CampaignStatus.PAUSED;
      entity.deletedAt = new Date();
      entity.servingVersion = Number(entity.servingVersion) + 1;
      const saved = await campaignRepo.save(entity);

      await this.outboxWriter.append(
        manager,
        saved,
        CampaignProjectionEventType.DELETE
      );
      if (this.projectionMode !== 'active') {
        await this.refundDeletedCampaign(
          manager,
          saved,
          `campaign-delete-refund:${saved.id}:v${saved.servingVersion}`
        );
      }
    });

    await this.runLegacyMirror(() =>
      this.campaignCacheRepository.deleteCampaignCacheById(campaignId)
    );
  }

  // ============================================================================
  // 모듈화 된 함수들
  // ============================================================================
  private async validateBudget({
    userId,
    maxCpc,
    dailyBudget,
    totalBudget,
    checkBalance,
  }: {
    userId: number;
    maxCpc: number;
    dailyBudget: number;
    totalBudget: number | null;
    checkBalance?: boolean;
  }): Promise<void> {
    if (maxCpc > dailyBudget) {
      throw new BadRequestException('CPC값은 일 예산을 초과할 수 없습니다.');
    }

    if (totalBudget !== null && dailyBudget > totalBudget) {
      throw new BadRequestException('일 예산은 총 예산을 초과할 수 없습니다.');
    }

    if (checkBalance && totalBudget !== null) {
      const balance = await this.userRepository.getBalanceById(userId);

      if (balance == null) {
        throw new NotFoundException();
      }

      if (totalBudget > balance) {
        throw new BadRequestException(
          '총 예산은 보유 잔액을 초과할 수 없습니다.'
        );
      }
    }
  }
  // 시작일 기준 초기 상태 결정
  private determineInitialStatus(
    startDate: string
  ): CampaignStatus.ACTIVE | CampaignStatus.PENDING {
    const today = new Date();
    today.setHours(0, 0, 0, 0);

    const start = new Date(startDate);
    start.setHours(0, 0, 0, 0);

    return start <= today ? CampaignStatus.ACTIVE : CampaignStatus.PENDING;
  }

  // 태그 이름 배열을 태그 ID 배열로 변환
  // TODO: tag부분도 Redis캐싱을 필요할듯
  private validateAndGetTagIds(tagNames: string[]): number[] {
    const tagIds: number[] = [];

    for (const name of tagNames) {
      const tag = AVAILABLE_TAGS.find((t) => t.name === name);
      if (!tag) {
        throw new BadRequestException(`존재하지 않는 태그입니다: ${name}`);
      }
      tagIds.push(tag.id);
    }

    return tagIds;
  }
  // 단일 캠페인에 통계 필드 추가
  private async addStatsToCampaign(
    campaign: CampaignWithTags
  ): Promise<CampaignWithStats> {
    const viewCounts = await this.campaignRepository.getViewCountsByCampaignIds(
      [campaign.id]
    );
    const clickCounts =
      await this.campaignRepository.getClickCountsByCampaignIds([campaign.id]);

    const impressions = viewCounts.get(campaign.id) || 0;
    const clicks = clickCounts.get(campaign.id) || 0;

    // DB Campaign 테이블의 dailySpent, totalSpent 사용 (Cron Job으로 동기화됨)
    const dailySpent = campaign.dailySpent;
    const totalSpent = campaign.totalSpent;

    return {
      ...campaign,
      dailySpent,
      totalSpent,
      impressions,
      clicks,
      ctr: this.calculateCTR(clicks, impressions),
      dailySpentPercent: this.calculatePercent(
        dailySpent,
        campaign.dailyBudget
      ),
      totalSpentPercent: this.calculatePercent(
        totalSpent,
        campaign.totalBudget
      ),
    };
  }

  // CTR 계산 (소수점 2자리)
  private calculateCTR(clicks: number, impressions: number): number {
    if (impressions === 0) return 0;
    return parseFloat(((clicks / impressions) * 100).toFixed(2));
  }

  // 퍼센트 계산 (소수점 2자리)
  private calculatePercent(spent: number, budget: number | null): number {
    if (budget === null || budget === 0) return 0;
    return parseFloat(((spent / budget) * 100).toFixed(2));
  }

  // 여러 캠페인에 통계 필드 추가
  private async addStatsToMultipleCampaigns(
    campaigns: CampaignWithTags[]
  ): Promise<CampaignWithStats[]> {
    if (campaigns.length === 0) {
      return [];
    }

    const campaignIds = campaigns.map((c) => c.id);

    // 일괄 집계
    const viewCounts =
      await this.campaignRepository.getViewCountsByCampaignIds(campaignIds);
    const clickCounts =
      await this.campaignRepository.getClickCountsByCampaignIds(campaignIds);

    // 각 캠페인에 통계 추가
    return campaigns.map((campaign) => {
      const impressions = viewCounts.get(campaign.id) || 0;
      const clicks = clickCounts.get(campaign.id) || 0;

      // DB Campaign 테이블의 dailySpent, totalSpent 사용
      const dailySpent = campaign.dailySpent;
      const totalSpent = campaign.totalSpent;

      return {
        ...campaign,
        dailySpent,
        totalSpent,
        impressions,
        clicks,
        ctr: this.calculateCTR(clicks, impressions),
        dailySpentPercent: this.calculatePercent(
          dailySpent,
          campaign.dailyBudget
        ),
        totalSpentPercent: this.calculatePercent(
          totalSpent,
          campaign.totalBudget
        ),
      };
    });
  }

  private convertToCachedCampaignType(
    campaign: CampaignWithTags
  ): CachedCampaign {
    return {
      id: campaign.id,
      userId: campaign.userId,
      servingVersion: campaign.servingVersion,
      title: campaign.title,
      content: campaign.content,
      image: campaign.image,
      url: campaign.url,
      maxCpc: campaign.maxCpc,
      dailyBudget: campaign.dailyBudget,
      totalBudget: campaign.totalBudget ?? null,
      dailySpent: campaign.dailySpent,
      totalSpent: campaign.totalSpent,
      dailyReserved: 0,
      totalReserved: 0,
      dailyReservedDate: new Date(Date.now() + 9 * 60 * 60 * 1000)
        .toISOString()
        .slice(0, 10),
      lastResetDate: campaign.lastResetDate.toISOString(),
      isHighIntent: campaign.isHighIntent,
      status: campaign.status,
      startDate: campaign.startDate.toISOString(),
      endDate: campaign.endDate.toISOString(),
      createdAt: campaign.createdAt.toISOString(),
      deletedAt: campaign.deletedAt ? campaign.deletedAt.toISOString() : null,

      // 태그 이름 배열 추가
      tags: campaign.tags.map((t) => t.name),

      // embeddingTags는 Worker가 나중에 추가
    };
  }

  private convertToCachedCampaignTypeWithoutSpent(
    campaign: CampaignWithTags
  ): CachedCampaignWithoutSpent {
    return {
      id: campaign.id,
      userId: campaign.userId,
      servingVersion: campaign.servingVersion,
      title: campaign.title,
      content: campaign.content,
      image: campaign.image,
      url: campaign.url,
      maxCpc: campaign.maxCpc,
      dailyBudget: campaign.dailyBudget,
      totalBudget: campaign.totalBudget ?? null,
      lastResetDate: campaign.lastResetDate.toISOString(),
      isHighIntent: campaign.isHighIntent,
      status: campaign.status,
      startDate: campaign.startDate.toISOString(),
      endDate: campaign.endDate.toISOString(),
      createdAt: campaign.createdAt.toISOString(),
      deletedAt: campaign.deletedAt ? campaign.deletedAt.toISOString() : null,

      // 태그 이름 배열 추가
      tags: campaign.tags.map((t) => t.name),
    };
  }

  private toCampaignWithTags(entity: CampaignEntity): CampaignWithTags {
    return {
      id: entity.id,
      userId: entity.userId,
      servingVersion: Number(entity.servingVersion),
      title: entity.title,
      content: entity.content,
      image: entity.image,
      url: entity.url,
      maxCpc: entity.maxCpc,
      dailyBudget: entity.dailyBudget,
      totalBudget: entity.totalBudget,
      dailySpent: entity.dailySpent,
      totalSpent: entity.totalSpent,
      lastResetDate: entity.lastResetDate,
      isHighIntent: entity.isHighIntent,
      status: entity.status,
      startDate: entity.startDate,
      endDate: entity.endDate,
      createdAt: entity.createdAt,
      deletedAt: entity.deletedAt,
      tags: (entity.tags ?? []).map((tag) => ({ id: tag.id, name: tag.name })),
    };
  }

  private async mirrorLegacyCreate(campaign: CampaignWithTags): Promise<void> {
    await this.runLegacyMirror(async () => {
      await this.campaignCacheRepository.saveCampaignCacheById(
        campaign.id,
        this.convertToCachedCampaignType(campaign)
      );
      await this.enqueueInitialCampaignEmbedding(campaign.id);
    });
  }

  private async mirrorLegacyUpdate(
    campaign: CampaignWithTags,
    semanticChanged: boolean
  ): Promise<void> {
    await this.runLegacyMirror(async () => {
      const next = this.convertToCachedCampaignType(campaign);
      const current = semanticChanged
        ? null
        : await this.campaignCacheRepository.findCampaignCacheById(campaign.id);
      await this.campaignCacheRepository.saveCampaignCacheById(
        campaign.id,
        this.mergeReusableEmbeddings(next, current)
      );
      if (semanticChanged) {
        await this.enqueueInitialCampaignEmbedding(campaign.id);
      }
    });
  }

  private async runLegacyMirror(work: () => Promise<void>): Promise<void> {
    if (this.projectionMode === 'active') return;
    try {
      await work();
    } catch (error) {
      // MySQL + Outbox commit 이후의 호환 mirror는 HTTP 성공 기준에 포함하지
      // 않는다. 실패 이벤트는 durable Outbox가 보존하며 shadow/active worker가
      // 재처리한다.
      this.logger.warn(
        `${this.projectionMode} legacy projection 갱신 실패`,
        error
      );
    }
  }

  private async refundDeletedCampaign(
    manager: EntityManager,
    campaign: CampaignEntity,
    operationKey: string
  ): Promise<void> {
    if (!campaign.totalBudget || campaign.totalBudget <= 0) return;
    const historyRepo = manager.getRepository(CreditHistoryEntity);
    if (await historyRepo.exist({ where: { operationKey } })) return;

    const remainingBudget = Math.max(
      0,
      campaign.totalBudget - campaign.totalSpent
    );
    if (remainingBudget === 0) return;
    const userRepo = manager.getRepository(UserEntity);
    const user = await userRepo.findOne({
      where: { id: campaign.userId },
      lock: { mode: 'pessimistic_write' },
    });
    if (!user) throw new NotFoundException('사용자를 찾을 수 없습니다');
    user.balance += remainingBudget;
    await userRepo.save(user);
    await historyRepo.save({
      userId: campaign.userId,
      type: CreditHistoryType.CHARGE,
      amount: remainingBudget,
      balanceAfter: user.balance,
      campaignId: campaign.id,
      operationKey,
      description: `'${campaign.title}' 캠페인 삭제 - 남은 예산 환불`,
    });
  }

  private areTagsEqual(dtoTags: string[], redisTags: string[]): boolean {
    // 1. 개수 비교
    if (dtoTags.length !== redisTags.length) {
      return false;
    }

    // 2. Set을 이용한 비교
    const dtoSet = new Set(dtoTags);
    return redisTags.every((tag) => dtoSet.has(tag));
  }
}
