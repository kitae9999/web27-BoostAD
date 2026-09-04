import { Injectable } from '@nestjs/common';
import { createHash, randomUUID } from 'node:crypto';
import type { EntityManager } from 'typeorm';
import type { CampaignEntity } from '../entities/campaign.entity';
import {
  CampaignProjectionEventType,
  CampaignProjectionOutboxState,
  type CampaignProjectionDocument,
  type CampaignProjectionPayload,
} from './campaign-projection.types';
import { CampaignProjectionOutboxEntity } from './entities/campaign-projection-outbox.entity';

@Injectable()
export class CampaignProjectionOutboxWriter {
  async append(
    manager: EntityManager,
    campaign: CampaignEntity,
    eventType: CampaignProjectionEventType
  ): Promise<CampaignProjectionOutboxEntity> {
    const eventId = randomUUID();
    const document = this.toDocument(campaign);
    const payload: CampaignProjectionPayload = {
      eventId,
      eventType,
      campaign: document,
    };
    const repository = manager.getRepository(CampaignProjectionOutboxEntity);
    return repository.save(
      repository.create({
        eventId,
        campaignId: campaign.id,
        servingVersion: Number(campaign.servingVersion),
        eventType,
        payload,
        state: CampaignProjectionOutboxState.PENDING,
        attempts: 0,
        availableAt: new Date(),
      })
    );
  }

  toDocument(campaign: CampaignEntity): CampaignProjectionDocument {
    const tags = [...new Set((campaign.tags ?? []).map((tag) => tag.name))]
      .map((tag) => tag.normalize('NFC').trim())
      .filter(Boolean)
      .sort((a, b) => a.localeCompare(b));

    return {
      id: campaign.id,
      userId: campaign.userId,
      servingVersion: Number(campaign.servingVersion),
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
      tags,
      semanticHash: createHash('sha256')
        .update(
          JSON.stringify({
            title: campaign.title.normalize('NFC').trim(),
            content: campaign.content.normalize('NFC').trim(),
            tags,
          })
        )
        .digest('hex'),
    };
  }
}
