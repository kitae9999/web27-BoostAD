import { Injectable, Logger } from '@nestjs/common';
import { RedisCampaignCacheRepository } from 'src/campaign/repository/redis-campaign.cache.repository';

@Injectable()
export class CampaignIndexBackfillService {
  private readonly logger = new Logger(CampaignIndexBackfillService.name);

  constructor(
    private readonly redisCampaignCacheRepository: RedisCampaignCacheRepository
  ) {}

  async run(): Promise<void> {
    const batchSize = Number(process.env.CAMPAIGN_INDEX_BACKFILL_BATCH_SIZE ?? 200);
    const scanCount = Number(process.env.CAMPAIGN_INDEX_BACKFILL_SCAN_COUNT ?? 200);
    const lockTtlSeconds = Number(
      process.env.CAMPAIGN_INDEX_BACKFILL_LOCK_TTL_SECONDS ?? 600
    );

    const result =
      await this.redisCampaignCacheRepository.rebuildCampaignIndicesFromRedisScan(
        {
          batchSize,
          scanCount,
          lockTtlSeconds,
        }
      );

    this.logger.log(
      `Campaign index backfill 완료: keys=${result.scannedKeys}, ids=${result.indexedIds}, active=${result.activeIds}, duration=${result.durationMs}ms`
    );
  }
}

