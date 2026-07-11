import {
  Inject,
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Kafka, logLevel, type Consumer } from 'kafkajs';
import {
  resolveCampaignKafkaConfig,
  type CampaignKafkaConfig,
} from '../../kafka/kafka.config';
import { IOREDIS_CLIENT } from '../../redis/redis.constant';
import type { AppIORedisClient } from '../../redis/redis.type';
import type { CampaignServingEvent } from '../events/campaign-serving-event';
import { CampaignCacheRepository } from '../repository/campaign.cache.repository.interface';
import { CampaignServingProjectionRepository } from './campaign-serving-projection.repository';

@Injectable()
export class CampaignSearchIndexerConsumer
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(CampaignSearchIndexerConsumer.name);
  private readonly config: CampaignKafkaConfig;
  private readonly kafka: Kafka;
  private readonly checkpointKey: string;
  private readonly campaignVersionsKey: string;
  private readonly retryMs: number;
  private consumer: Consumer | null = null;
  private running = false;
  private consumeLoopPromise: Promise<void> | null = null;

  constructor(
    configService: ConfigService,
    private readonly projectionRepository: CampaignServingProjectionRepository,
    private readonly campaignCacheRepository: CampaignCacheRepository,
    @Inject(IOREDIS_CLIENT) private readonly redis: AppIORedisClient
  ) {
    this.config = resolveCampaignKafkaConfig(configService, 'search-indexer');
    this.checkpointKey = configService.get<string>(
      'RTB_SEARCH_INDEXER_CHECKPOINT_KEY',
      'rtb:campaign-search-indexer:sequence'
    );
    this.campaignVersionsKey = configService.get<string>(
      'RTB_SEARCH_INDEXER_CAMPAIGN_VERSIONS_KEY',
      'rtb:campaign-search-indexer:campaign-versions'
    );
    this.retryMs = this.positiveInt(
      configService.get<string>('KAFKA_CONSUMER_RETRY_MS'),
      1000
    );
    this.kafka = new Kafka({
      clientId: this.config.clientId,
      brokers: this.config.brokers,
      ssl: this.config.ssl,
      sasl: this.config.sasl,
      logLevel: logLevel.WARN,
    });
  }

  async onApplicationBootstrap(): Promise<void> {
    if (!this.config.enabled) return;
    await this.rebuildFromProjection();
    this.running = true;
    this.consumeLoopPromise = this.runConsumerLoop();
  }

  async onApplicationShutdown(): Promise<void> {
    this.running = false;
    await this.consumer?.disconnect();
    await this.consumeLoopPromise;
    this.consumeLoopPromise = null;
    this.consumer = null;
  }

  private async runConsumerLoop(): Promise<void> {
    while (this.running) {
      const consumer = this.kafka.consumer({
        groupId: 'boostad-campaign-search-indexer-v1',
      });
      this.consumer = consumer;
      try {
        await consumer.connect();
        await consumer.subscribe({
          topic: this.config.topic,
          fromBeginning: true,
        });
        const crashed = new Promise<never>((_resolve, reject) => {
          consumer.on(consumer.events.CRASH, (event) => {
            reject(event.payload.error);
          });
        });
        const stopped = new Promise<void>((resolve) => {
          consumer.on(consumer.events.STOP, () => resolve());
        });
        await consumer.run({
          partitionsConsumedConcurrently: 1,
          eachMessage: async ({ message }) => {
            if (!message.value) return;
            const parsed = JSON.parse(
              message.value.toString()
            ) as CampaignServingEvent;
            const event: CampaignServingEvent = {
              ...parsed,
              eventId: `kafka:0:${message.offset}`,
              sequence: Number(message.offset) + 1,
            };
            await this.applyEvent(event);
          },
        });
        await Promise.race([crashed, stopped]);
        if (this.running) {
          throw new Error(
            'Campaign search indexer consumer가 예기치 않게 종료됨'
          );
        }
      } catch (error) {
        if (this.running) {
          this.logger.error(
            `Campaign search indexer 재접속 예정: ${this.retryMs}ms`,
            error
          );
        }
      } finally {
        await consumer.disconnect().catch(() => undefined);
        if (this.consumer === consumer) this.consumer = null;
      }
      if (this.running) await this.delay(this.retryMs);
    }
  }

  async applyEvent(event: CampaignServingEvent): Promise<void> {
    const current = Number((await this.redis.get(this.checkpointKey)) ?? 0);
    if (event.sequence <= current) return;
    if (event.sequence !== current + 1) {
      await this.rebuildFromProjection();
      return;
    }
    const currentCampaignVersion = Number(
      (await this.redis.hget(this.campaignVersionsKey, event.campaignId)) ?? 0
    );
    if (event.campaignVersion > currentCampaignVersion) {
      if (event.type === 'UPSERT') {
        await this.campaignCacheRepository.saveCampaignCacheById(
          event.campaignId,
          event.campaign,
          undefined,
          { durableEvent: false, localEvent: false }
        );
      } else {
        await this.campaignCacheRepository.deleteCampaignCacheById(
          event.campaignId,
          { durableEvent: false, localEvent: false }
        );
      }
      await this.redis.hset(
        this.campaignVersionsKey,
        event.campaignId,
        String(event.campaignVersion)
      );
    }
    await this.redis.set(this.checkpointKey, String(event.sequence));
  }

  async rebuildFromProjection(): Promise<void> {
    const source = await this.projectionRepository.loadSnapshot();
    const existing = await this.campaignCacheRepository.getAllCampaigns({
      allowStale: false,
    });
    const targetIds = new Set(source.campaigns.map((campaign) => campaign.id));
    for (const campaign of existing) {
      if (!targetIds.has(campaign.id)) {
        await this.campaignCacheRepository.deleteCampaignCacheById(
          campaign.id,
          { durableEvent: false, localEvent: false }
        );
      }
    }
    for (const campaign of source.campaigns) {
      await this.campaignCacheRepository.saveCampaignCacheById(
        campaign.id,
        campaign,
        undefined,
        { durableEvent: false, localEvent: false }
      );
    }
    const pipeline = this.redis.pipeline();
    pipeline.del(this.campaignVersionsKey);
    for (const [campaignId, version] of source.campaignVersions) {
      pipeline.hset(this.campaignVersionsKey, campaignId, String(version));
    }
    await pipeline.exec();
    await this.redis.set(
      this.checkpointKey,
      String(source.checkpoint.sequence)
    );
    this.logger.log(
      `RedisSearch projection 재구축 완료: campaigns=${source.campaigns.length}, sequence=${source.checkpoint.sequence}`
    );
  }

  private positiveInt(raw: string | undefined, fallback: number): number {
    const value = raw ? Number.parseInt(raw, 10) : fallback;
    return Number.isFinite(value) && value > 0 ? value : fallback;
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
