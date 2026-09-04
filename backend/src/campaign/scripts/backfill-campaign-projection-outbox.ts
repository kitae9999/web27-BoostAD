import 'reflect-metadata';
import dataSource from '../../data-source';
import { CampaignEntity } from '../entities/campaign.entity';
import { CampaignProjectionOutboxWriter } from '../projection/campaign-projection-outbox.writer';
import { CampaignProjectionEventType } from '../projection/campaign-projection.types';
import { CampaignProjectionOutboxEntity } from '../projection/entities/campaign-projection-outbox.entity';

const BATCH_SIZE = 100;

async function main(): Promise<void> {
  await dataSource.initialize();
  const writer = new CampaignProjectionOutboxWriter();
  let cursor = '';
  let created = 0;
  let skipped = 0;

  try {
    while (true) {
      const campaigns = await dataSource
        .getRepository(CampaignEntity)
        .createQueryBuilder('campaign')
        .withDeleted()
        .leftJoinAndSelect('campaign.tags', 'tag')
        .where(cursor ? 'campaign.id > :cursor' : '1 = 1', { cursor })
        .orderBy('campaign.id', 'ASC')
        .take(BATCH_SIZE)
        .getMany();
      if (campaigns.length === 0) break;

      for (const source of campaigns) {
        const appended = await dataSource.transaction(async (manager) => {
          const campaign = await manager
            .getRepository(CampaignEntity)
            .createQueryBuilder('campaign')
            .withDeleted()
            .leftJoinAndSelect('campaign.tags', 'tag')
            .setLock('pessimistic_write')
            .where('campaign.id = :id', { id: source.id })
            .getOne();
          if (!campaign) return false;

          const exists = await manager
            .getRepository(CampaignProjectionOutboxEntity)
            .exist({
              where: {
                campaignId: campaign.id,
                servingVersion: Number(campaign.servingVersion),
              },
            });
          if (exists) return false;

          const outbox = await writer.append(
            manager,
            campaign,
            campaign.deletedAt
              ? CampaignProjectionEventType.DELETE
              : CampaignProjectionEventType.UPSERT
          );
          // Backfill 이전 삭제는 기존 동기 경로가 이미 환불했다. tombstone은
          // 재생하되 과거 삭제를 새 정산 작업으로 취급하지 않는다.
          if (campaign.deletedAt) {
            outbox.deletionSettledAt = new Date();
            await manager
              .getRepository(CampaignProjectionOutboxEntity)
              .save(outbox);
          }
          return true;
        });
        appended ? (created += 1) : (skipped += 1);
      }

      cursor = campaigns[campaigns.length - 1].id;
      console.log(
        `Campaign Projection backfill 진행: cursor=${cursor}, created=${created}, skipped=${skipped}`
      );
    }
    console.log(
      `Campaign Projection backfill 완료: created=${created}, skipped=${skipped}`
    );
  } finally {
    await dataSource.destroy();
  }
}

void main().catch((error) => {
  console.error('Campaign Projection backfill 실패', error);
  process.exitCode = 1;
});
