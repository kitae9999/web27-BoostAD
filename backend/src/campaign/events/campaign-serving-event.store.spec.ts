import { ConfigService } from '@nestjs/config';
import type { AppIORedisClient } from '../../redis/redis.type';
import type { CachedCampaign } from '../types/campaign.types';
import { CampaignServingEventStore } from './campaign-serving-event.store';

const campaign = { id: 'campaign-1', title: 'title' } as CachedCampaign;

describe('CampaignServingEventStore', () => {
  const config = (enabled: boolean) =>
    ({
      get: jest.fn((key: string, fallback?: string) => {
        if (key === 'RTB_CAMPAIGN_EVENT_SYNC_ENABLED') {
          return String(enabled);
        }
        return fallback;
      }),
    }) as unknown as ConfigService;

  it('does not touch Redis while event sync is disabled', async () => {
    const redis = { eval: jest.fn() } as unknown as AppIORedisClient & {
      eval: jest.Mock;
    };
    const store = new CampaignServingEventStore(redis, config(false));

    await expect(store.publishUpsert(campaign)).resolves.toBeNull();
    expect(redis.eval).not.toHaveBeenCalled();
  });

  it('maps the atomically allocated stream id, sequence and campaign version', async () => {
    const redis = {
      eval: jest.fn().mockResolvedValue(['123-0', '7', '3', '1000']),
    } as unknown as AppIORedisClient & { eval: jest.Mock };
    const store = new CampaignServingEventStore(redis, config(true));

    await expect(store.publishUpsert(campaign)).resolves.toEqual({
      schemaVersion: 1,
      eventId: '123-0',
      type: 'UPSERT',
      campaignId: 'campaign-1',
      campaignVersion: 3,
      sequence: 7,
      occurredAtMs: 1000,
      campaign,
    });
    expect(redis.eval).toHaveBeenCalledWith(
      expect.any(String),
      3,
      'rtb:campaign-serving:events',
      'rtb:campaign-serving:events:sequence',
      'rtb:campaign-serving:events:campaign-versions',
      'UPSERT',
      'campaign-1',
      JSON.stringify(campaign),
      '10000',
      '1'
    );
  });

  it('reads and validates complete serving documents after a checkpoint', async () => {
    const fields = [
      'schemaVersion',
      '1',
      'type',
      'UPSERT',
      'campaignId',
      'campaign-1',
      'campaignVersion',
      '4',
      'sequence',
      '9',
      'occurredAtMs',
      '1000',
      'payload',
      JSON.stringify(campaign),
    ];
    const redis = {
      xread: jest
        .fn()
        .mockResolvedValue([
          ['rtb:campaign-serving:events', [['9-0', fields]]],
        ]),
    } as unknown as AppIORedisClient & { xread: jest.Mock };
    const store = new CampaignServingEventStore(redis, config(true));

    await expect(
      store.readAfter('8-0', { count: 10, blockMs: 100 })
    ).resolves.toEqual([
      {
        schemaVersion: 1,
        eventId: '9-0',
        type: 'UPSERT',
        campaignId: 'campaign-1',
        campaignVersion: 4,
        sequence: 9,
        occurredAtMs: 1000,
        campaign,
      },
    ]);
  });

  it('returns the latest stream checkpoint', async () => {
    const redis = {
      xrevrange: jest
        .fn()
        .mockResolvedValue([
          [
            '12-0',
            [
              'schemaVersion',
              '1',
              'type',
              'DELETE',
              'campaignId',
              'campaign-1',
              'campaignVersion',
              '5',
              'sequence',
              '12',
              'occurredAtMs',
              '1000',
              'payload',
              '',
            ],
          ],
        ]),
    } as unknown as AppIORedisClient & { xrevrange: jest.Mock };
    const store = new CampaignServingEventStore(redis, config(true));

    await expect(store.getCheckpoint()).resolves.toEqual({
      eventId: '12-0',
      sequence: 12,
    });
  });
});
