import { ServiceUnavailableException } from '@nestjs/common';
import { CampaignServingSnapshotService } from './campaign-serving-snapshot.service';
import { CampaignServingStatusController } from './campaign-serving-status.controller';

describe('CampaignServingStatusController', () => {
  const build = (options: {
    ready: boolean;
    snapshotSequence: number;
    searchSequence?: number;
    projection?: boolean;
  }) => {
    const snapshot = {
      isEnabled: jest.fn().mockReturnValue(true),
      getMetadata: jest.fn().mockReturnValue({
        ready: options.ready,
        sequence: options.snapshotSequence,
      }),
    } as unknown as CampaignServingSnapshotService;
    return new CampaignServingStatusController(
      snapshot,
      {
        get: jest.fn().mockResolvedValue(String(options.searchSequence ?? 0)),
      } as never,
      {
        get: jest.fn((key: string, fallback?: string) =>
          key === 'RTB_PROJECTION_PIPELINE_ENABLED' && options.projection
            ? 'true'
            : fallback
        ),
      } as never
    );
  };

  it('returns 503 while a local snapshot is recovering', async () => {
    const controller = build({ ready: false, snapshotSequence: 7 });

    await expect(controller.getHealth()).rejects.toBeInstanceOf(
      ServiceUnavailableException
    );
  });

  it('returns 503 until RedisSearch reaches the snapshot Kafka offset', async () => {
    const controller = build({
      ready: true,
      snapshotSequence: 7,
      searchSequence: 6,
      projection: true,
    });

    await expect(controller.getHealth()).rejects.toBeInstanceOf(
      ServiceUnavailableException
    );
  });

  it('opens readiness after snapshot and RedisSearch reach the same offset', async () => {
    const controller = build({
      ready: true,
      snapshotSequence: 7,
      searchSequence: 7,
      projection: true,
    });

    await expect(controller.getHealth()).resolves.toEqual(
      expect.objectContaining({ status: 'ok', searchProjectionSequence: 7 })
    );
  });
});
