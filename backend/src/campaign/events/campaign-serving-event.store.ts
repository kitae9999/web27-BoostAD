import { Inject, Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { IOREDIS_CLIENT } from '../../redis/redis.constant';
import type { AppIORedisClient } from '../../redis/redis.type';
import type { CachedCampaign } from '../types/campaign.types';
import {
  CAMPAIGN_SERVING_EVENT_SCHEMA_VERSION,
  type CampaignServingEvent,
  type CampaignServingEventType,
} from './campaign-serving-event';

const APPEND_CAMPAIGN_SERVING_EVENT_SCRIPT = `
  local sequence = redis.call('INCR', KEYS[2])
  local campaignVersion = redis.call('HINCRBY', KEYS[3], ARGV[2], 1)
  local redisTime = redis.call('TIME')
  local occurredAtMs = tonumber(redisTime[1]) * 1000 + math.floor(tonumber(redisTime[2]) / 1000)
  local eventId = redis.call(
    'XADD', KEYS[1], 'MAXLEN', '~', ARGV[4], '*',
    'schemaVersion', ARGV[5],
    'type', ARGV[1],
    'campaignId', ARGV[2],
    'campaignVersion', tostring(campaignVersion),
    'sequence', tostring(sequence),
    'occurredAtMs', tostring(occurredAtMs),
    'payload', ARGV[3]
  )
  return {eventId, tostring(sequence), tostring(campaignVersion), tostring(occurredAtMs)}
`;

@Injectable()
export class CampaignServingEventStore {
  private readonly enabled: boolean;
  private readonly streamKey: string;
  private readonly sequenceKey: string;
  private readonly campaignVersionsKey: string;
  private readonly maxLength: number;

  constructor(
    @Inject(IOREDIS_CLIENT)
    private readonly redis: AppIORedisClient,
    configService: ConfigService
  ) {
    this.enabled =
      configService.get<string>('RTB_CAMPAIGN_EVENT_SYNC_ENABLED', 'false') ===
      'true';
    this.streamKey = configService.get<string>(
      'RTB_CAMPAIGN_EVENT_STREAM_KEY',
      'rtb:campaign-serving:events'
    );
    this.sequenceKey = `${this.streamKey}:sequence`;
    this.campaignVersionsKey = `${this.streamKey}:campaign-versions`;
    this.maxLength = this.getPositiveInt(
      configService,
      'RTB_CAMPAIGN_EVENT_STREAM_MAXLEN',
      10_000
    );
  }

  isEnabled(): boolean {
    return this.enabled;
  }

  async publishUpsert(
    campaign: CachedCampaign
  ): Promise<CampaignServingEvent | null> {
    return this.append('UPSERT', campaign.id, JSON.stringify(campaign));
  }

  async publishDelete(
    campaignId: string
  ): Promise<CampaignServingEvent | null> {
    return this.append('DELETE', campaignId, '');
  }

  private async append(
    type: CampaignServingEventType,
    campaignId: string,
    payload: string
  ): Promise<CampaignServingEvent | null> {
    if (!this.enabled) {
      return null;
    }

    const result = (await this.redis.eval(
      APPEND_CAMPAIGN_SERVING_EVENT_SCRIPT,
      3,
      this.streamKey,
      this.sequenceKey,
      this.campaignVersionsKey,
      type,
      campaignId,
      payload,
      String(this.maxLength),
      String(CAMPAIGN_SERVING_EVENT_SCHEMA_VERSION)
    )) as [string, string, string, string];
    const base = {
      schemaVersion: CAMPAIGN_SERVING_EVENT_SCHEMA_VERSION,
      eventId: result[0],
      type,
      campaignId,
      sequence: Number(result[1]),
      campaignVersion: Number(result[2]),
      occurredAtMs: Number(result[3]),
    };

    return type === 'UPSERT'
      ? { ...base, type, campaign: JSON.parse(payload) as CachedCampaign }
      : { ...base, type };
  }

  private getPositiveInt(
    configService: ConfigService,
    key: string,
    fallback: number
  ): number {
    const raw = configService.get<string>(key);
    const value = raw ? Number.parseInt(raw, 10) : fallback;
    return Number.isFinite(value) && value > 0 ? value : fallback;
  }
}
