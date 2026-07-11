/* eslint-disable @typescript-eslint/unbound-method */
import { ConfigService } from '@nestjs/config';
import type { AppIORedisClient } from '../redis/redis.type';
import { CampaignServingEventConsumer } from './campaign-serving-event.consumer';
import { CampaignServingSnapshotService } from './campaign-serving-snapshot.service';
import { CampaignServingEventStore } from './events/campaign-serving-event.store';

describe('CampaignServingEventConsumer', () => {
  const build = (applyOutcome: 'applied' | 'gap' = 'applied') => {
    const eventStore = {
      isEnabled: jest.fn().mockReturnValue(true),
      getCheckpoint: jest
        .fn()
        .mockResolvedValue({ eventId: '5-0', sequence: 5 }),
      readAfter: jest.fn().mockResolvedValue([
        {
          schemaVersion: 1,
          eventId: '6-0',
          type: 'DELETE',
          campaignId: 'c1',
          campaignVersion: 2,
          sequence: 6,
          occurredAtMs: 1000,
        },
      ]),
    } as unknown as jest.Mocked<CampaignServingEventStore>;
    const snapshot = {
      isEnabled: jest.fn().mockReturnValue(true),
      markNotReady: jest.fn(),
      reloadFromSource: jest.fn().mockResolvedValue(undefined),
      getMetadata: jest.fn().mockReturnValue({ lastEventId: '5-0' }),
      applyServingEvent: jest.fn().mockReturnValue(applyOutcome),
    } as unknown as jest.Mocked<CampaignServingSnapshotService>;
    const reader = { disconnect: jest.fn() };
    const redis = {
      duplicate: jest.fn().mockReturnValue(reader),
    } as unknown as AppIORedisClient;
    const config = {
      get: jest.fn((_key: string, fallback?: string | number) => fallback),
    } as unknown as ConfigService;
    const metricsService = {
      recordCampaignSnapshotRecovery: jest.fn(),
      setCampaignSnapshotState: jest.fn(),
    };
    const consumer = new CampaignServingEventConsumer(
      eventStore,
      snapshot,
      redis,
      config,
      metricsService as never
    );
    Object.assign(consumer, { reader });
    return { consumer, eventStore, snapshot, reader, metricsService };
  };

  it('rebuilds from a head watermark before consuming deltas', async () => {
    const { consumer, snapshot, metricsService } = build();

    await consumer.recoverFromHead('test');

    expect(snapshot.markNotReady).toHaveBeenCalled();
    expect(snapshot.reloadFromSource).toHaveBeenCalledWith({
      eventId: '5-0',
      sequence: 5,
    });
    expect(metricsService.recordCampaignSnapshotRecovery).toHaveBeenCalledWith(
      'test'
    );
  });

  it('applies every event independently without a consumer group', async () => {
    const { consumer, eventStore, snapshot, reader } = build();

    await expect(consumer.consumeOnce()).resolves.toBe('applied');
    expect(eventStore.readAfter).toHaveBeenCalledWith(
      '5-0',
      { count: 100, blockMs: 1000 },
      reader
    );
    expect(snapshot.applyServingEvent).toHaveBeenCalledTimes(1);
  });

  it('reloads the full snapshot when a sequence gap is detected', async () => {
    const { consumer, snapshot } = build('gap');

    await expect(consumer.consumeOnce()).resolves.toBe('recovered');
    expect(snapshot.reloadFromSource).toHaveBeenCalledWith({
      eventId: '5-0',
      sequence: 5,
    });
  });
});
