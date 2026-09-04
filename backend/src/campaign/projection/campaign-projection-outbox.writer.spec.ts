import type { EntityManager } from 'typeorm';
import type { CampaignEntity } from '../entities/campaign.entity';
import { CampaignStatus } from '../entities/campaign.entity';
import { CampaignProjectionOutboxWriter } from './campaign-projection-outbox.writer';
import { CampaignProjectionEventType } from './campaign-projection.types';

describe('CampaignProjectionOutboxWriter', () => {
  it('stores a normalized immutable snapshot and version key', async () => {
    const saved: unknown[] = [];
    const repository = {
      create: jest.fn((value: unknown) => value),
      save: jest.fn(async (value: unknown) => {
        saved.push(value);
        return value;
      }),
    };
    const manager = {
      getRepository: jest.fn(() => repository),
    } as unknown as EntityManager;
    const campaign = {
      id: 'campaign-1',
      userId: 1,
      servingVersion: 7,
      title: ' Redis ',
      content: ' projection ',
      image: null,
      url: 'https://example.com',
      maxCpc: 100,
      dailyBudget: 1_000,
      totalBudget: 10_000,
      dailySpent: 0,
      totalSpent: 0,
      lastResetDate: new Date('2026-09-03T00:00:00.000Z'),
      isHighIntent: false,
      status: CampaignStatus.ACTIVE,
      startDate: new Date('2026-09-01T00:00:00.000Z'),
      endDate: new Date('2026-10-01T00:00:00.000Z'),
      createdAt: new Date('2026-09-01T00:00:00.000Z'),
      deletedAt: null,
      tags: [
        { id: 2, name: 'TypeScript' },
        { id: 1, name: 'Redis' },
        { id: 3, name: 'Redis' },
      ],
    } as CampaignEntity;
    const writer = new CampaignProjectionOutboxWriter();

    await writer.append(manager, campaign, CampaignProjectionEventType.UPSERT);
    campaign.title = 'mutated-after-append';
    campaign.tags[0].name = 'mutated';

    expect(saved[0]).toEqual(
      expect.objectContaining({
        campaignId: 'campaign-1',
        servingVersion: 7,
        payload: expect.objectContaining({
          campaign: expect.objectContaining({
            title: ' Redis ',
            tags: ['Redis', 'TypeScript'],
            semanticHash: expect.stringMatching(/^[a-f0-9]{64}$/),
          }),
        }),
      })
    );
  });
});
