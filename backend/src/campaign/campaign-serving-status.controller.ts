import {
  Controller,
  Get,
  Inject,
  ServiceUnavailableException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Public } from '../auth/decorators/public.decorator';
import { IOREDIS_CLIENT } from '../redis/redis.constant';
import type { AppIORedisClient } from '../redis/redis.type';
import { CampaignServingSnapshotService } from './campaign-serving-snapshot.service';

@Public()
@Controller('healthz')
export class CampaignServingStatusController {
  private readonly projectionPipelineEnabled: boolean;
  private readonly searchIndexerCheckpointKey: string;

  constructor(
    private readonly snapshot: CampaignServingSnapshotService,
    @Inject(IOREDIS_CLIENT) private readonly redis: AppIORedisClient,
    configService: ConfigService
  ) {
    this.projectionPipelineEnabled =
      configService.get<string>('RTB_PROJECTION_PIPELINE_ENABLED', 'false') ===
      'true';
    this.searchIndexerCheckpointKey = configService.get<string>(
      'RTB_SEARCH_INDEXER_CHECKPOINT_KEY',
      'rtb:campaign-search-indexer:sequence'
    );
  }

  @Get()
  async getHealth() {
    const metadata = this.snapshot.getMetadata();
    if (this.snapshot.isEnabled() && !metadata.ready) {
      throw new ServiceUnavailableException({
        status: 'not_ready',
        campaignSnapshot: metadata,
      });
    }
    const searchProjectionSequence = this.projectionPipelineEnabled
      ? Number((await this.redis.get(this.searchIndexerCheckpointKey)) ?? 0)
      : metadata.sequence;
    if (
      this.projectionPipelineEnabled &&
      searchProjectionSequence < metadata.sequence
    ) {
      throw new ServiceUnavailableException({
        status: 'search_projection_not_ready',
        campaignSnapshot: metadata,
        searchProjectionSequence,
      });
    }
    return {
      status: 'ok',
      campaignSnapshot: metadata,
      searchProjectionSequence,
    };
  }
}
