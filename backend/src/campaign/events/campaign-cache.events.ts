import type { CachedCampaign } from '../types/campaign.types';
import type { CampaignServingEvent } from './campaign-serving-event';

export const CAMPAIGN_CACHE_UPSERTED_EVENT = 'campaign.cache.upserted';
export const CAMPAIGN_CACHE_REMOVED_EVENT = 'campaign.cache.removed';

export type CampaignCacheUpsertedEvent = {
  campaign: CachedCampaign;
  servingEvent?: CampaignServingEvent;
};

export type CampaignCacheRemovedEvent = {
  campaignId: string;
  servingEvent?: CampaignServingEvent;
};
