import type {
  CampaignDocumentVectorSearchHit,
  CampaignEmbeddingPayload,
  SearchCampaign,
  CampaignTagVectorSearchHit,
  CampaignTagVectorSearchOptions,
} from '../types/campaign.types';
import type {
  CampaignProjectionDocument,
  SearchSnapshotEvent,
} from '../projection/campaign-projection.types';

export abstract class CampaignSearchRepository {
  abstract applySearchProjection(
    campaign: CampaignProjectionDocument
  ): Promise<{ applied: boolean; requiresEmbedding: boolean }>;
  abstract applySearchTombstone(
    campaignId: string,
    servingVersion: number
  ): Promise<boolean>;
  abstract findCampaignById(id: string): Promise<SearchCampaign | null>;
  abstract findCampaignsByIds(ids: string[]): Promise<SearchCampaign[]>;
  abstract getAllSearchCampaigns(): Promise<SearchCampaign[]>;
  abstract updateEmbeddingsIfCurrent(
    id: string,
    servingVersion: number,
    semanticHash: string,
    payload: CampaignEmbeddingPayload
  ): Promise<boolean>;
  abstract searchCampaignTagVectors(
    options: CampaignTagVectorSearchOptions
  ): Promise<CampaignTagVectorSearchHit[]>;
  abstract searchCampaignDocumentVectors(
    options: CampaignTagVectorSearchOptions
  ): Promise<CampaignDocumentVectorSearchHit[]>;
  abstract appendSnapshotEvent(event: SearchSnapshotEvent): Promise<string>;
}
