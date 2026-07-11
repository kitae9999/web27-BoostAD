import type { CachedCampaign } from '../types/campaign.types';

export const CAMPAIGN_SERVING_EVENT_SCHEMA_VERSION = 1 as const;

export type CampaignServingEventType = 'UPSERT' | 'DELETE';

type CampaignServingEventBase = {
  schemaVersion: typeof CAMPAIGN_SERVING_EVENT_SCHEMA_VERSION;
  eventId: string;
  type: CampaignServingEventType;
  campaignId: string;
  campaignVersion: number;
  sequence: number;
  occurredAtMs: number;
};

export type CampaignServingUpsertEvent = CampaignServingEventBase & {
  type: 'UPSERT';
  campaign: CachedCampaign;
};

export type CampaignServingDeleteEvent = CampaignServingEventBase & {
  type: 'DELETE';
};

export type CampaignServingEvent =
  | CampaignServingUpsertEvent
  | CampaignServingDeleteEvent;

export type CampaignServingEventCheckpoint = {
  eventId: string;
  sequence: number;
};
