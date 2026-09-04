import type { CachedCampaign, SearchCampaign } from '../types/campaign.types';

export const CAMPAIGN_CACHE_UPSERTED_EVENT = 'campaign.cache.upserted';
export const CAMPAIGN_CACHE_REMOVED_EVENT = 'campaign.cache.removed';

export type CampaignCacheUpsertedEvent = {
  campaign: CachedCampaign | SearchCampaign;
};

export type CampaignCacheRemovedEvent = {
  campaignId: string;
  servingVersion?: number;
};
