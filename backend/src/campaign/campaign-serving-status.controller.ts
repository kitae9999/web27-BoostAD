import { Controller, Get, ServiceUnavailableException } from '@nestjs/common';
import { Public } from '../auth/decorators/public.decorator';
import { CampaignServingSnapshotService } from './campaign-serving-snapshot.service';

@Public()
@Controller('healthz')
export class CampaignServingStatusController {
  constructor(private readonly snapshot: CampaignServingSnapshotService) {}

  @Get()
  getHealth() {
    const metadata = this.snapshot.getMetadata();
    if (this.snapshot.isEnabled() && !metadata.ready) {
      throw new ServiceUnavailableException({
        status: 'not_ready',
        campaignSnapshot: metadata,
      });
    }
    return { status: 'ok', campaignSnapshot: metadata };
  }
}
