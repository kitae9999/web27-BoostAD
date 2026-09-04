import 'reflect-metadata';
import { ConfigService } from '@nestjs/config';
import Redis from 'ioredis';
import dataSource from '../../data-source';
import { resolveRedisConnection } from '../../redis/redis.config';
import { CampaignEntity } from '../entities/campaign.entity';
import { CampaignProjectionOutboxWriter } from '../projection/campaign-projection-outbox.writer';

type ParityMismatch = {
  campaignId: string;
  mysqlVersion: number;
  budgetVersion: number | null;
  searchVersion: number | null;
  reason: string;
};

async function main(): Promise<void> {
  const config = new ConfigService(process.env);
  const searchRedis = new Redis(
    resolveRedisConnection(config, 'SEARCH').options
  );
  const budgetRedis = new Redis(
    resolveRedisConnection(config, 'BUDGET').options
  );
  await dataSource.initialize();
  const documentWriter = new CampaignProjectionOutboxWriter();

  try {
    const campaigns = await dataSource.getRepository(CampaignEntity).find({
      withDeleted: true,
      relations: { tags: true },
      order: { id: 'ASC' },
    });
    const mismatches: ParityMismatch[] = [];

    for (const campaign of campaigns) {
      const mysqlVersion = Number(campaign.servingVersion);
      const budget = await budgetRedis.hgetall(
        `budget:campaign:${campaign.id}`
      );
      const budgetVersion = budget.servingVersion
        ? Number(budget.servingVersion)
        : null;
      const searchRaw = (await searchRedis.call(
        'JSON.GET',
        `campaign:${campaign.id}`
      )) as string | null;
      const searchCampaign = searchRaw
        ? (JSON.parse(searchRaw) as Record<string, unknown>)
        : null;
      const tombstoneRaw = await searchRedis.get(
        `campaign:tombstone:${campaign.id}`
      );
      const searchVersion = searchCampaign?.servingVersion
        ? Number(searchCampaign.servingVersion)
        : tombstoneRaw
          ? Number(tombstoneRaw)
          : null;

      const expected = documentWriter.toDocument(campaign);
      const deleted = Boolean(campaign.deletedAt);
      const reasons: string[] = [];
      if (
        budgetVersion !== mysqlVersion ||
        (budget.tombstone === '1') !== deleted
      ) {
        reasons.push('budget-version-or-tombstone');
      }
      if (!deleted) {
        const projectedTotalBudget =
          budget.totalBudget === undefined || budget.totalBudget === ''
            ? null
            : Number(budget.totalBudget);
        if (
          budget.status !== expected.status ||
          Number(budget.maxCpc) !== expected.maxCpc ||
          Number(budget.dailyBudget) !== expected.dailyBudget ||
          projectedTotalBudget !== expected.totalBudget
        ) {
          reasons.push('budget-config');
        }
      }
      if (searchVersion !== mysqlVersion || Boolean(tombstoneRaw) !== deleted) {
        reasons.push('search-version-or-tombstone');
      }
      if (!deleted) {
        if (!searchCampaign) {
          reasons.push('search-missing');
        } else {
          const searchFieldsMatch =
            searchCampaign.title === expected.title &&
            searchCampaign.content === expected.content &&
            searchCampaign.image === expected.image &&
            searchCampaign.url === expected.url &&
            Number(searchCampaign.maxCpc) === expected.maxCpc &&
            searchCampaign.status === expected.status &&
            searchCampaign.isHighIntent === expected.isHighIntent &&
            searchCampaign.startDate === expected.startDate &&
            searchCampaign.endDate === expected.endDate &&
            searchCampaign.semanticHash === expected.semanticHash &&
            JSON.stringify(searchCampaign.tags) ===
              JSON.stringify(expected.tags);
          if (!searchFieldsMatch) reasons.push('search-serving-fields');
          if (searchCampaign.indexReady !== true) {
            reasons.push('search-index-not-ready');
          }
          if (
            [
              'dailyBudget',
              'totalBudget',
              'dailySpent',
              'totalSpent',
              'dailyReserved',
              'totalReserved',
              'lastResetDate',
            ].some((field) => field in searchCampaign)
          ) {
            reasons.push('search-contains-budget-state');
          }
        }
      }
      if (reasons.length > 0) {
        mismatches.push({
          campaignId: campaign.id,
          mysqlVersion,
          budgetVersion,
          searchVersion,
          reason: reasons.join('+'),
        });
      }
    }

    console.log(
      JSON.stringify(
        {
          checked: campaigns.length,
          matched: campaigns.length - mismatches.length,
          mismatched: mismatches.length,
          samples: mismatches.slice(0, 50),
        },
        null,
        2
      )
    );
    if (mismatches.length > 0) process.exitCode = 2;
  } finally {
    await Promise.allSettled([searchRedis.quit(), budgetRedis.quit()]);
    await dataSource.destroy();
  }
}

void main().catch((error) => {
  console.error('Campaign Projection parity 점검 실패', error);
  process.exitCode = 1;
});
