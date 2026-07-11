import { ServiceUnavailableException } from '@nestjs/common';
import { CampaignServingSnapshotService } from './campaign-serving-snapshot.service';
import { CampaignServingStatusController } from './campaign-serving-status.controller';

describe('CampaignServingStatusController', () => {
  it('returns 503 while a local snapshot is recovering', () => {
    const snapshot = {
      isEnabled: jest.fn().mockReturnValue(true),
      getMetadata: jest.fn().mockReturnValue({ ready: false, sequence: 7 }),
    } as unknown as CampaignServingSnapshotService;
    const controller = new CampaignServingStatusController(snapshot);

    expect(() => controller.getHealth()).toThrow(ServiceUnavailableException);
  });
});
