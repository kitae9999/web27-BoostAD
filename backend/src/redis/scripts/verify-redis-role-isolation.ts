import { strict as assert } from 'node:assert';
import { randomUUID } from 'node:crypto';
import { ConfigService } from '@nestjs/config';
import Redis from 'ioredis';
import { Queue } from 'bullmq';
import {
  REDIS_APPLY_BUDGET_PROJECTION_SCRIPT,
  REDIS_APPLY_BUDGET_TOMBSTONE_SCRIPT,
  REDIS_HASH_RESERVE_AUCTION_SCRIPT,
} from '../../campaign/scripts/budget-lua-script';
import {
  REDIS_APPLY_SEARCH_EMBEDDING_SCRIPT,
  REDIS_APPLY_SEARCH_PROJECTION_SCRIPT,
  REDIS_APPLY_SEARCH_TOMBSTONE_SCRIPT,
} from '../../campaign/scripts/search-projection-lua-script';
import { resolveRedisConnection } from '../redis.config';

async function main(): Promise<void> {
  const config = new ConfigService(process.env);
  assert.equal(
    config.get<string>('REDIS_TOPOLOGY_MODE'),
    'split',
    'REDIS_TOPOLOGY_MODE=split에서만 실행할 수 있습니다'
  );
  const search = new Redis(resolveRedisConnection(config, 'SEARCH').options);
  const budget = new Redis(resolveRedisConnection(config, 'BUDGET').options);
  const queue = new Redis(resolveRedisConnection(config, 'QUEUE').options);
  const namespace = `role-smoke:${randomUUID()}`;
  const queueName = namespace.replaceAll(':', '-');
  const bullQueue = new Queue(queueName, {
    connection: resolveRedisConnection(config, 'QUEUE').options,
  });
  const campaignId = `${namespace}:campaign`;
  const searchKey = `${namespace}:search-campaign`;
  const searchTombstone = `${namespace}:search-tombstone`;
  const searchKeys = `${namespace}:search-keys`;
  const budgetKey = `${namespace}:budget-campaign`;
  const lowerBudgetKey = `${namespace}:budget-campaign-lower`;
  const auctionKey = `${namespace}:auction`;
  const mismatchAuctionKey = `${namespace}:auction-mismatch`;
  const strictMismatchAuctionKey = `${namespace}:auction-strict-mismatch`;
  const expirations = `${namespace}:expirations`;
  const stream = `${namespace}:stream`;
  const queueProbe = `${namespace}:queue-probe`;

  try {
    await Promise.all([search.ping(), budget.ping(), queue.ping()]);
    assert.deepEqual(await search.config('GET', 'maxmemory-policy'), [
      'maxmemory-policy',
      'volatile-lfu',
    ]);
    assert.deepEqual(await budget.config('GET', 'maxmemory-policy'), [
      'maxmemory-policy',
      'noeviction',
    ]);
    assert.deepEqual(await queue.config('GET', 'maxmemory-policy'), [
      'maxmemory-policy',
      'noeviction',
    ]);

    const document = {
      id: campaignId,
      userId: 1,
      servingVersion: 1,
      title: 'Redis role smoke',
      content: 'projection consistency',
      image: null,
      url: 'https://example.com',
      maxCpc: 100,
      dailyBudget: 1_000,
      totalBudget: 10_000,
      dailySpent: 20,
      totalSpent: 30,
      lastResetDate: '2026-09-03T00:00:00.000Z',
      isHighIntent: false,
      status: 'ACTIVE',
      startDate: '2026-09-01T00:00:00.000Z',
      endDate: '2026-10-01T00:00:00.000Z',
      createdAt: '2026-09-01T00:00:00.000Z',
      deletedAt: null,
      tags: ['Redis'],
      semanticHash: 'hash-v1',
    };

    const budgetApplied = await budget.eval(
      REDIS_APPLY_BUDGET_PROJECTION_SCRIPT,
      1,
      budgetKey,
      JSON.stringify({ ...document, budgetDate: '2026-09-03' })
    );
    assert.equal(Number(budgetApplied), 1);
    const reserve = (await budget.eval(
      REDIS_HASH_RESERVE_AUCTION_SCRIPT,
      3,
      expirations,
      auctionKey,
      budgetKey,
      'auction-1',
      '7',
      '2026-09-03',
      String(Date.now() + 60_000),
      '1',
      campaignId
    )) as unknown[];
    assert.equal(Number(reserve[0]), 1);
    const reservation = JSON.parse(String(reserve[3])) as {
      cost: number;
      campaignServingVersion: number;
    };
    assert.equal(reservation.cost, 100, 'CPC는 Budget 권위값이어야 합니다');
    assert.equal(reservation.campaignServingVersion, 1);

    const version2 = {
      ...document,
      servingVersion: 2,
      maxCpc: 200,
      dailySpent: 999,
      totalSpent: 999,
    };
    await budget.eval(
      REDIS_APPLY_BUDGET_PROJECTION_SCRIPT,
      1,
      budgetKey,
      JSON.stringify({ ...version2, budgetDate: '2026-09-03' })
    );
    const budgetState = await budget.hgetall(budgetKey);
    assert.equal(budgetState.servingVersion, '2');
    assert.equal(budgetState.maxCpc, '200');
    assert.equal(budgetState.dailySpent, '20');
    assert.equal(budgetState.totalSpent, '30');
    assert.equal(budgetState.totalReserved, '100');

    const mismatch = (await budget.eval(
      REDIS_HASH_RESERVE_AUCTION_SCRIPT,
      3,
      expirations,
      mismatchAuctionKey,
      budgetKey,
      'auction-2',
      '7',
      '2026-09-03',
      String(Date.now() + 60_000),
      '1',
      campaignId
    )) as unknown[];
    assert.equal(Number(mismatch[0]), -3);
    assert.equal(await budget.exists(mismatchAuctionKey), 0);

    await budget.eval(
      REDIS_APPLY_BUDGET_PROJECTION_SCRIPT,
      1,
      lowerBudgetKey,
      JSON.stringify({
        ...document,
        id: `${campaignId}:lower`,
        budgetDate: '2026-09-03',
      })
    );
    const strictMismatch = (await budget.eval(
      REDIS_HASH_RESERVE_AUCTION_SCRIPT,
      4,
      expirations,
      strictMismatchAuctionKey,
      budgetKey,
      lowerBudgetKey,
      'auction-3',
      '7',
      '2026-09-03',
      String(Date.now() + 60_000),
      '1',
      '1',
      campaignId,
      `${campaignId}:lower`
    )) as unknown[];
    assert.equal(
      Number(strictMismatch[0]),
      -3,
      '상위 후보 버전이 다르면 하위 후보를 예약하지 않고 재매칭해야 합니다'
    );
    assert.equal(await budget.exists(strictMismatchAuctionKey), 0);

    assert.equal(
      Number(
        await budget.eval(
          REDIS_APPLY_BUDGET_TOMBSTONE_SCRIPT,
          1,
          budgetKey,
          '3'
        )
      ),
      1
    );
    assert.equal(
      Number(
        await budget.eval(
          REDIS_APPLY_BUDGET_PROJECTION_SCRIPT,
          1,
          budgetKey,
          JSON.stringify({ ...version2, budgetDate: '2026-09-03' })
        )
      ),
      -1,
      '삭제 전 버전은 Budget tombstone을 넘을 수 없습니다'
    );

    const searchDocument = {
      id: document.id,
      userId: document.userId,
      servingVersion: document.servingVersion,
      title: document.title,
      content: document.content,
      image: document.image,
      url: document.url,
      maxCpc: document.maxCpc,
      isHighIntent: document.isHighIntent,
      status: document.status,
      startDate: document.startDate,
      endDate: document.endDate,
      createdAt: document.createdAt,
      deletedAt: document.deletedAt,
      tags: document.tags,
      semanticHash: document.semanticHash,
      indexReady: false,
    };
    const searchApplied = (await search.eval(
      REDIS_APPLY_SEARCH_PROJECTION_SCRIPT,
      3,
      searchKey,
      searchTombstone,
      searchKeys,
      JSON.stringify(searchDocument),
      'model-v1'
    )) as unknown[];
    assert.equal(Number(searchApplied[0]), 1);
    assert.equal(await search.ttl(searchKey), -1);
    const storedSearch = JSON.parse(
      String(await search.call('JSON.GET', searchKey))
    ) as Record<string, unknown>;
    assert.equal(
      'dailySpent' in storedSearch,
      false,
      'Budget counter는 Search projection에 저장하면 안 됩니다'
    );
    assert.equal(
      Number(
        await search.eval(
          REDIS_APPLY_SEARCH_EMBEDDING_SCRIPT,
          2,
          searchKey,
          searchTombstone,
          '1',
          'hash-v1',
          JSON.stringify({
            modelVersion: 'model-v1',
            document: [0.1, 0.2],
            tags: { Redis: [0.1, 0.2] },
          })
        )
      ),
      1
    );
    assert.equal(
      Number(
        await search.eval(
          REDIS_APPLY_SEARCH_TOMBSTONE_SCRIPT,
          3,
          searchKey,
          searchTombstone,
          searchKeys,
          '2'
        )
      ),
      1
    );
    const staleSearchApply = (await search.eval(
      REDIS_APPLY_SEARCH_PROJECTION_SCRIPT,
      3,
      searchKey,
      searchTombstone,
      searchKeys,
      JSON.stringify(searchDocument),
      'model-v1'
    )) as unknown[];
    assert.equal(
      Number(staleSearchApply[0]),
      -1,
      '삭제 전 버전은 Search tombstone을 넘을 수 없습니다'
    );
    await search.xadd(
      stream,
      'MAXLEN',
      '~',
      '100000',
      '*',
      'type',
      'UPSERT',
      'campaignId',
      campaignId,
      'servingVersion',
      '2',
      'indexReady',
      '1'
    );
    assert.equal(await search.ttl(stream), -1);

    await queue.set(queueProbe, 'ok');
    assert.equal(await queue.get(queueProbe), 'ok');
    const queueJobId = `${queueName}-job`;
    await bullQueue.add('probe', { ok: true }, { jobId: queueJobId });
    assert.equal((await bullQueue.getJob(queueJobId))?.data.ok, true);
    console.log('Redis role isolation smoke 검증 완료');
  } finally {
    await bullQueue.obliterate({ force: true }).catch(() => undefined);
    await bullQueue.close().catch(() => undefined);
    await Promise.allSettled([
      search.del(searchKey, searchTombstone, searchKeys, stream),
      budget.del(
        budgetKey,
        lowerBudgetKey,
        auctionKey,
        mismatchAuctionKey,
        strictMismatchAuctionKey,
        expirations
      ),
      queue.del(queueProbe),
    ]);
    await Promise.allSettled([search.quit(), budget.quit(), queue.quit()]);
  }
}

void main().catch((error) => {
  console.error('Redis role isolation smoke 검증 실패', error);
  process.exitCode = 1;
});
