import { Inject, Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { IOREDIS_CLIENT } from '../../redis/redis.constant';
import type { AppIORedisClient } from '../../redis/redis.type';
import type { CachedCampaign } from '../types/campaign.types';
import {
  CAMPAIGN_SERVING_EVENT_SCHEMA_VERSION,
  type CampaignServingEvent,
  type CampaignServingEventCheckpoint,
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
      configService.get<string>('RTB_PROJECTION_PIPELINE_ENABLED', 'false') !==
        'true' &&
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

  async getCheckpoint(): Promise<CampaignServingEventCheckpoint> {
    if (!this.enabled) {
      return { eventId: '0-0', sequence: 0 };
    }
    const rows = (await this.redis.xrevrange(
      this.streamKey,
      '+',
      '-',
      'COUNT',
      1
    )) as [string, string[]][];
    if (rows.length > 0) {
      const fields = this.toFieldRecord(rows[0][1]);
      return {
        eventId: rows[0][0],
        sequence: this.toPositiveInt(fields.sequence, 'sequence'),
      };
    }
    const sequence = Number((await this.redis.get(this.sequenceKey)) ?? 0);
    return {
      eventId: '0-0',
      sequence: Number.isFinite(sequence) ? sequence : 0,
    };
  }

  async readAfter(
    eventId: string,
    options: { count: number; blockMs: number },
    reader: AppIORedisClient = this.redis
  ): Promise<CampaignServingEvent[]> {
    if (!this.enabled) {
      return [];
    }
    const result = (await reader.xread(
      'COUNT',
      options.count,
      'BLOCK',
      options.blockMs,
      'STREAMS',
      this.streamKey,
      eventId
    )) as [string, [string, string[]][]][] | null;
    const entries = result?.[0]?.[1] ?? [];
    return entries.map(([id, fields]) => this.parseEvent(id, fields));
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

  private parseEvent(
    eventId: string,
    rawFields: string[]
  ): CampaignServingEvent {
    const fields = this.toFieldRecord(rawFields);
    const schemaVersion = this.toPositiveInt(
      fields.schemaVersion,
      'schemaVersion'
    );
    if (schemaVersion !== CAMPAIGN_SERVING_EVENT_SCHEMA_VERSION) {
      throw new Error(`지원하지 않는 campaign event schema: ${schemaVersion}`);
    }
    const type = fields.type;
    if (type !== 'UPSERT' && type !== 'DELETE') {
      throw new Error(`지원하지 않는 campaign event type: ${type}`);
    }
    const campaignId = fields.campaignId;
    if (!campaignId) {
      throw new Error('campaign event campaignId가 없습니다.');
    }
    const base = {
      schemaVersion: CAMPAIGN_SERVING_EVENT_SCHEMA_VERSION,
      eventId,
      type,
      campaignId,
      campaignVersion: this.toPositiveInt(
        fields.campaignVersion,
        'campaignVersion'
      ),
      sequence: this.toPositiveInt(fields.sequence, 'sequence'),
      occurredAtMs: this.toPositiveInt(fields.occurredAtMs, 'occurredAtMs'),
    };
    if (type === 'DELETE') {
      return { ...base, type };
    }
    const campaign = JSON.parse(fields.payload ?? '') as CachedCampaign;
    if (!campaign || campaign.id !== campaignId) {
      throw new Error(
        'campaign event payload와 campaignId가 일치하지 않습니다.'
      );
    }
    return { ...base, type, campaign };
  }

  private toFieldRecord(fields: string[]): Record<string, string> {
    const record: Record<string, string> = {};
    for (let index = 0; index < fields.length; index += 2) {
      record[fields[index]] = fields[index + 1];
    }
    return record;
  }

  private toPositiveInt(value: string | undefined, label: string): number {
    const parsed = Number(value);
    if (!Number.isSafeInteger(parsed) || parsed < 0) {
      throw new Error(`campaign event ${label}가 유효하지 않습니다: ${value}`);
    }
    return parsed;
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
