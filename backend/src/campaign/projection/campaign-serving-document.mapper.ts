import type { CampaignEntity } from '../entities/campaign.entity';
import type { CachedCampaign } from '../types/campaign.types';

export function toCampaignServingDocument(
  campaign: CampaignEntity,
  embeddings: {
    modelVersion: string;
    document: number[];
    tags: Record<string, number[]>;
  }
): CachedCampaign {
  return {
    id: campaign.id,
    userId: campaign.userId,
    title: campaign.title,
    content: campaign.content,
    image: campaign.image,
    url: campaign.url,
    maxCpc: campaign.maxCpc,
    dailyBudget: campaign.dailyBudget,
    totalBudget: campaign.totalBudget,
    dailySpent: campaign.dailySpent,
    totalSpent: campaign.totalSpent,
    lastResetDate: campaign.lastResetDate.toISOString(),
    isHighIntent: campaign.isHighIntent,
    status: campaign.status,
    startDate: campaign.startDate.toISOString(),
    endDate: campaign.endDate.toISOString(),
    createdAt: campaign.createdAt.toISOString(),
    deletedAt: campaign.deletedAt?.toISOString() ?? null,
    tags: (campaign.tags ?? []).map((tag) => tag.name),
    embeddingModelVersion: embeddings.modelVersion,
    embeddingDocument: embeddings.document,
    embeddingTags: embeddings.tags,
  };
}
