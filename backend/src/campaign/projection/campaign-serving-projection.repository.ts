import { Injectable } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource, In } from 'typeorm';
import type { CampaignServingEventCheckpoint } from '../events/campaign-serving-event';
import type { CachedCampaign } from '../types/campaign.types';
import { CampaignServingOutboxEntity } from './entities/campaign-serving-outbox.entity';
import { CampaignServingProjectionEntity } from './entities/campaign-serving-projection.entity';

export type CampaignProjectionSnapshot = {
  campaigns: CachedCampaign[];
  checkpoint: CampaignServingEventCheckpoint;
  campaignVersions: Map<string, number>;
  complete: boolean;
};

@Injectable()
export class CampaignServingProjectionRepository {
  constructor(@InjectDataSource() private readonly dataSource: DataSource) {}

  loadSnapshot(): Promise<CampaignProjectionSnapshot> {
    return this.dataSource.transaction('REPEATABLE READ', async (manager) => {
      const raw = await manager
        .getRepository(CampaignServingOutboxEntity)
        .createQueryBuilder('outbox')
        .select('COALESCE(MAX(outbox.kafkaOffset), -1)', 'offset')
        .getRawOne<{ offset?: string }>();
      const sequence = Number(raw?.offset ?? -1) + 1;
      const rows = await manager
        .getRepository(CampaignServingProjectionEntity)
        .find({
          where: { deleted: false },
          order: { campaignId: 'ASC' },
        });
      const complete = await this.isCaughtUpWithManager(manager);
      return {
        campaigns: rows.flatMap((row) => (row.document ? [row.document] : [])),
        campaignVersions: new Map(
          rows.map((row) => [row.campaignId, Number(row.version)])
        ),
        checkpoint: {
          eventId: sequence > 0 ? `kafka:0:${sequence - 1}` : 'kafka:0:-1',
          sequence,
        },
        complete,
      };
    });
  }

  async isCaughtUp(): Promise<boolean> {
    return this.isCaughtUpWithManager(this.dataSource.manager);
  }

  async findCampaignsByIds(ids: string[]): Promise<CachedCampaign[]> {
    if (ids.length === 0) return [];
    const rows = await this.dataSource
      .getRepository(CampaignServingProjectionEntity)
      .find({ where: { campaignId: In(ids), deleted: false } });
    const byId = new Map(
      rows.flatMap((row) =>
        row.document ? [[row.campaignId, row.document] as const] : []
      )
    );
    return ids.flatMap((id) => {
      const campaign = byId.get(id);
      return campaign ? [campaign] : [];
    });
  }

  private async isCaughtUpWithManager(manager: {
    query: (query: string) => Promise<unknown>;
  }): Promise<boolean> {
    const rows = (await manager.query(`
      SELECT
        (SELECT COUNT(*) FROM Campaign) AS campaignCount,
        (SELECT COUNT(*) FROM CampaignServingProjection) AS projectionCount,
        (SELECT COUNT(*) FROM CampaignProjectionRequestOutbox
          WHERE state <> 'COMPLETED') AS incompleteRequests,
        (SELECT COUNT(*) FROM CampaignServingOutbox
          WHERE state <> 'PUBLISHED') AS unpublishedEvents
    `)) as Array<{
      campaignCount: string | number;
      projectionCount: string | number;
      incompleteRequests: string | number;
      unpublishedEvents: string | number;
    }>;
    const row = rows[0];
    return (
      Number(row?.projectionCount ?? 0) >= Number(row?.campaignCount ?? 0) &&
      Number(row?.incompleteRequests ?? 0) === 0 &&
      Number(row?.unpublishedEvents ?? 0) === 0
    );
  }
}
