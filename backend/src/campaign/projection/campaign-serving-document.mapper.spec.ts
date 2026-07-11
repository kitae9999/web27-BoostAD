import {
  CampaignStatus,
  type CampaignEntity,
} from '../entities/campaign.entity';
import { toCampaignServingDocument } from './campaign-serving-document.mapper';

describe('toCampaignServingDocument', () => {
  it('publishes campaign, tags and embeddings as one versioned document payload', () => {
    const campaign = {
      id: 'c1',
      userId: 1,
      title: '새 제목',
      content: '새 본문',
      image: 'image',
      url: 'https://example.com',
      maxCpc: 100,
      dailyBudget: 1000,
      totalBudget: 10000,
      dailySpent: 0,
      totalSpent: 0,
      lastResetDate: new Date('2026-07-11T00:00:00Z'),
      isHighIntent: true,
      status: CampaignStatus.ACTIVE,
      startDate: new Date('2026-07-01T00:00:00Z'),
      endDate: new Date('2026-08-01T00:00:00Z'),
      createdAt: new Date('2026-07-01T00:00:00Z'),
      deletedAt: null,
      tags: [{ id: 1, name: 'typescript' }],
    } as CampaignEntity;

    const result = toCampaignServingDocument(campaign, {
      modelVersion: 'e5-v1',
      document: [0.1, 0.2],
      tags: { typescript: [0.3, 0.4] },
    });

    expect(result).toEqual(
      expect.objectContaining({
        id: 'c1',
        title: '새 제목',
        tags: ['typescript'],
        embeddingModelVersion: 'e5-v1',
        embeddingDocument: [0.1, 0.2],
        embeddingTags: { typescript: [0.3, 0.4] },
      })
    );
  });
});
