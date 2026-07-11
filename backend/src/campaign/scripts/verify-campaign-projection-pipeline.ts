import { Kafka, logLevel, type Consumer } from 'kafkajs';
import Redis from 'ioredis';
import mysql from 'mysql2/promise';
import { randomUUID } from 'node:crypto';

type ObservedEvent = {
  campaignId: string;
  type: 'UPSERT' | 'DELETE';
  campaign?: { title?: string };
};

const timeoutMs = Number(process.env.PHASE5_SMOKE_TIMEOUT_MS ?? 180_000);
const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:19092').split(',');
const topic = process.env.KAFKA_CAMPAIGN_SERVING_TOPIC ?? 'campaign-serving-v1';

async function main(): Promise<void> {
  const db = await mysql.createConnection({
    host: process.env.DB_HOST ?? 'localhost',
    port: Number(process.env.DB_PORT ?? 13306),
    user: process.env.DB_USERNAME ?? 'test_user',
    password: process.env.DB_PASSWORD ?? 'test_user',
    database: process.env.DB_DATABASE ?? 'test_db',
  });
  const redis = new Redis({
    host: process.env.REDIS_HOST ?? 'localhost',
    port: Number(process.env.REDIS_PORT ?? 16379),
    lazyConnect: true,
  });
  await redis.connect();
  const kafka = new Kafka({
    clientId: `phase5-smoke-${randomUUID()}`,
    brokers,
    logLevel: logLevel.WARN,
  });
  const consumers = [
    kafka.consumer({ groupId: `phase5-smoke-a-${randomUUID()}` }),
    kafka.consumer({ groupId: `phase5-smoke-b-${randomUUID()}` }),
  ];
  const observed = [
    new Map<string, ObservedEvent>(),
    new Map<string, ObservedEvent>(),
  ];

  let campaignId = '';
  let originalTitle = '';
  try {
    const [rows] = await db.query<
      Array<{ id: string; title: string }> & mysql.RowDataPacket[]
    >(
      'SELECT id, title FROM Campaign WHERE deleted_at IS NULL ORDER BY created_at LIMIT 1'
    );
    const campaign = rows[0];
    if (!campaign) throw new Error('검증할 ACTIVE campaign이 없습니다.');
    campaignId = campaign.id;
    originalTitle = campaign.title;
    const changedTitle = `${originalTitle} [phase5-${Date.now()}]`;

    await Promise.all(
      consumers.map(async (consumer, index) => {
        let joined = false;
        const removeGroupJoinListener = consumer.on(
          consumer.events.GROUP_JOIN,
          () => {
            joined = true;
          }
        );
        await consumer.connect();
        await consumer.subscribe({ topic, fromBeginning: false });
        void consumer.run({
          eachMessage: ({ message }) => {
            if (message.value) {
              const event = JSON.parse(
                message.value.toString()
              ) as ObservedEvent;
              observed[index].set(
                `${event.campaignId}:${event.campaign?.title ?? event.type}`,
                event
              );
            }
            return Promise.resolve();
          },
        });
        await waitFor(() => Promise.resolve(joined ? true : null));
        removeGroupJoinListener();
      })
    );

    await db.execute('UPDATE Campaign SET title = ? WHERE id = ?', [
      changedTitle,
      campaignId,
    ]);

    const projection = await waitFor(async () => {
      const [projectionRows] = await db.query<
        Array<{
          version: string;
          document: string | { title?: string };
        }> &
          mysql.RowDataPacket[]
      >(
        'SELECT version, document FROM CampaignServingProjection WHERE campaign_id = ?',
        [campaignId]
      );
      const row = projectionRows[0];
      if (!row) return null;
      const document =
        typeof row.document === 'string'
          ? (JSON.parse(row.document) as { title?: string })
          : row.document;
      return document.title === changedTitle ? { ...row, document } : null;
    });

    const published = await waitFor(async () => {
      const [outboxRows] = await db.query<
        Array<{ id: string; kafka_offset: string; state: string }> &
          mysql.RowDataPacket[]
      >(
        `SELECT id, kafka_offset, state FROM CampaignServingOutbox
         WHERE campaign_id = ? AND campaign_version = ?`,
        [campaignId, projection.version]
      );
      return outboxRows[0]?.state === 'PUBLISHED' &&
        outboxRows[0].kafka_offset !== null
        ? outboxRows[0]
        : null;
    });

    await waitFor(async () => {
      const raw = await redis.call('JSON.GET', `campaign:${campaignId}`);
      if (typeof raw !== 'string') return null;
      const document = JSON.parse(raw) as { title?: string };
      return document.title === changedTitle ? document : null;
    });

    await waitFor(() =>
      Promise.resolve(
        observed.every((events) => events.has(`${campaignId}:${changedTitle}`))
          ? true
          : null
      )
    );

    const healthUrl =
      process.env.BACKEND_HEALTH_URL ?? 'http://localhost:3000/api/healthz';
    const health = await waitFor(async () => {
      const response = await fetch(healthUrl);
      return response.ok ? ((await response.json()) as object) : null;
    });

    process.stdout.write(
      `${JSON.stringify(
        {
          result: 'PASS',
          campaignId,
          projectionVersion: projection.version,
          kafkaOffset: published.kafka_offset,
          independentConsumers: observed.length,
          redisSearchProjectionUpdated: true,
          health,
        },
        null,
        2
      )}\n`
    );
  } finally {
    if (campaignId && originalTitle) {
      await db.execute('UPDATE Campaign SET title = ? WHERE id = ?', [
        originalTitle,
        campaignId,
      ]);
    }
    await Promise.all(consumers.map((consumer) => safeDisconnect(consumer)));
    redis.disconnect();
    await db.end();
  }
}

async function waitFor<T>(probe: () => Promise<T | null>): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      const value = await probe();
      if (value !== null) return value;
    } catch (error) {
      lastError = error;
    }
    await delay(250);
  }
  const detail =
    lastError instanceof Error
      ? lastError.message
      : typeof lastError === 'string'
        ? lastError
        : lastError
          ? 'probe failed with a non-Error value'
          : '';
  throw new Error(`Phase 5 smoke timeout${detail ? `: ${detail}` : ''}`);
}

async function safeDisconnect(consumer: Consumer): Promise<void> {
  try {
    await consumer.disconnect();
  } catch {
    // best-effort cleanup
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

void main().catch((error: unknown) => {
  process.stderr.write(
    `${error instanceof Error ? error.stack : String(error)}\n`
  );
  process.exitCode = 1;
});
