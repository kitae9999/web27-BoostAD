import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';
import { CampaignEntity, CampaignStatus } from '../entities/campaign.entity';
import { CampaignCacheRepository } from '../repository/campaign.cache.repository.interface';
import { CampaignProjectionOutboxWriter } from './campaign-projection-outbox.writer';
import { CampaignProjectionEventType } from './campaign-projection.types';

type ProjectionMode = 'off' | 'shadow' | 'active';

/**
 * 영속 캠페인 상태 전이를 serving projection 변경과 같은 트랜잭션 경계에
 * 넣는다. 정산 중 사용하는 일시적인 Budget 잠금은 이 서비스의 책임이 아니다.
 */
@Injectable()
export class CampaignProjectionCommandService {
  private readonly logger = new Logger(CampaignProjectionCommandService.name);
  private readonly mode: ProjectionMode;

  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly outboxWriter: CampaignProjectionOutboxWriter,
    private readonly campaignCacheRepository: CampaignCacheRepository,
    configService: ConfigService
  ) {
    const configured = configService.get<string>(
      'CAMPAIGN_PROJECTION_MODE',
      'off'
    );
    this.mode =
      configured === 'shadow' || configured === 'active' ? configured : 'off';
  }

  async updateStatus(
    campaignId: string,
    status: CampaignStatus
  ): Promise<boolean> {
    const updated = await this.dataSource.transaction(async (manager) => {
      const repository = manager.getRepository(CampaignEntity);
      const campaign = await repository.findOne({
        where: { id: campaignId },
        relations: ['tags'],
        lock: { mode: 'pessimistic_write' },
      });
      if (!campaign || campaign.status === status) return false;

      campaign.status = status;
      campaign.servingVersion = Number(campaign.servingVersion) + 1;
      const saved = await repository.save(campaign);
      await this.outboxWriter.append(
        manager,
        saved,
        CampaignProjectionEventType.UPSERT
      );
      return true;
    });

    if (updated && this.mode !== 'active') {
      try {
        await this.campaignCacheRepository.updateCampaignStatus(
          campaignId,
          status
        );
      } catch (error) {
        // off/shadow의 동기 캐시는 호환 경로다. MySQL(+ Outbox) commit은 되돌리지 않는다.
        this.logger.warn(
          `legacy 상태 캐시 동기화 실패: campaign=${campaignId}`,
          error
        );
      }
    }
    return updated;
  }
}
