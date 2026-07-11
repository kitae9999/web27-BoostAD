import {
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { hostname } from 'node:os';
import { Kafka, logLevel, type Consumer } from 'kafkajs';
import { MetricsService } from '../../metrics/metrics.service';
import {
  resolveCampaignKafkaConfig,
  type CampaignKafkaConfig,
} from '../../kafka/kafka.config';
import type { CampaignServingEvent } from '../events/campaign-serving-event';
import { CampaignServingSnapshotService } from '../campaign-serving-snapshot.service';
import { CampaignServingProjectionRepository } from './campaign-serving-projection.repository';

@Injectable()
export class CampaignServingKafkaConsumer
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(CampaignServingKafkaConsumer.name);
  private readonly config: CampaignKafkaConfig;
  private readonly kafka: Kafka;
  private readonly groupId: string;
  private readonly retryMs: number;
  private consumer: Consumer | null = null;
  private running = false;
  private consumeLoopPromise: Promise<void> | null = null;
  private consumerConnected = false;
  private bootstrapProbeTimer: NodeJS.Timeout | null = null;
  private bootstrapProbeInFlight = false;

  constructor(
    configService: ConfigService,
    private readonly snapshot: CampaignServingSnapshotService,
    private readonly projectionRepository: CampaignServingProjectionRepository,
    private readonly metrics: MetricsService
  ) {
    const instanceId =
      configService.get<string>('RTB_INSTANCE_ID')?.trim() ||
      `${hostname()}-${process.pid}`;
    this.config = resolveCampaignKafkaConfig(
      configService,
      `rtb-${instanceId}`
    );
    const groupPrefix = configService.get<string>(
      'KAFKA_RTB_CONSUMER_GROUP_PREFIX',
      'boostad-rtb'
    );
    this.groupId = `${groupPrefix}-${instanceId}`;
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
    if (!this.config.enabled || !this.snapshot.isEnabled()) return;
    await this.snapshot.reloadFromSource({ eventId: 'db:0', sequence: 0 });
    this.updateMetrics();
    this.running = true;
    this.consumeLoopPromise = this.runConsumerLoop();
    this.bootstrapProbeTimer = setInterval(() => {
      void this.probeBootstrapReadiness();
    }, 1000);
    this.bootstrapProbeTimer.unref();
  }

  async onApplicationShutdown(): Promise<void> {
    this.running = false;
    if (this.bootstrapProbeTimer) clearInterval(this.bootstrapProbeTimer);
    this.bootstrapProbeTimer = null;
    await this.consumer?.disconnect();
    await this.consumeLoopPromise;
    this.consumeLoopPromise = null;
    this.consumer = null;
  }

  private async runConsumerLoop(): Promise<void> {
    while (this.running) {
      const consumer = this.kafka.consumer({ groupId: this.groupId });
      this.consumer = consumer;
      this.consumerConnected = false;
      try {
        await consumer.connect();
        await consumer.subscribe({
          topic: this.config.topic,
          fromBeginning: true,
        });
        let resolveGroupJoin: (() => void) | null = null;
        const groupJoined = new Promise<void>((resolve) => {
          resolveGroupJoin = resolve;
        });
        const removeGroupJoinListener = consumer.on(
          consumer.events.GROUP_JOIN,
          () => resolveGroupJoin?.()
        );
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
          eachMessage: async ({ partition, message }) => {
            if (!message.value) return;
            const event = this.withKafkaSequence(
              this.parseEvent(message.value.toString()),
              partition,
              message.offset
            );
            const outcome = this.snapshot.applyServingEvent(event);
            if (outcome === 'gap' || outcome === 'schema_mismatch') {
              this.snapshot.markNotReady();
              this.updateMetrics();
              await this.snapshot.reloadFromSource({
                eventId: event.eventId,
                sequence: event.sequence,
              });
              this.metrics.recordCampaignSnapshotRecovery(`kafka_${outcome}`);
            }
            this.updateMetrics();
          },
        });
        await Promise.race([groupJoined, crashed]);
        removeGroupJoinListener();
        this.consumerConnected = true;
        await this.snapshot.reloadFromSource({ eventId: 'db:0', sequence: 0 });
        this.updateMetrics();
        this.logger.log(
          `Campaign Kafka broadcast consumer 시작: topic=${this.config.topic}, group=${this.groupId}`
        );
        await Promise.race([crashed, stopped]);
        if (this.running) {
          throw new Error('Campaign Kafka consumer가 예기치 않게 종료됨');
        }
      } catch (error) {
        this.consumerConnected = false;
        this.snapshot.markNotReady();
        this.updateMetrics();
        if (this.running) {
          this.logger.error(
            `Campaign Kafka consumer 재접속 예정: ${this.retryMs}ms`,
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

  private async probeBootstrapReadiness(): Promise<void> {
    if (
      !this.consumerConnected ||
      this.snapshot.getMetadata().ready ||
      this.bootstrapProbeInFlight
    )
      return;
    this.bootstrapProbeInFlight = true;
    try {
      if (!(await this.projectionRepository.isCaughtUp())) return;
      await this.snapshot.reloadFromSource({ eventId: 'db:0', sequence: 0 });
      this.updateMetrics();
      this.logger.log(
        'Campaign projection 초기 backfill 완료, RTB readiness 개방'
      );
    } catch (error) {
      this.logger.warn('Campaign projection readiness 확인 실패', error);
    } finally {
      this.bootstrapProbeInFlight = false;
    }
  }

  private parseEvent(raw: string): CampaignServingEvent {
    const parsed = JSON.parse(raw) as Partial<CampaignServingEvent>;
    if (
      typeof parsed.schemaVersion !== 'number' ||
      typeof parsed.eventId !== 'string' ||
      (parsed.type !== 'UPSERT' && parsed.type !== 'DELETE') ||
      typeof parsed.campaignId !== 'string' ||
      typeof parsed.campaignVersion !== 'number' ||
      typeof parsed.sequence !== 'number' ||
      typeof parsed.occurredAtMs !== 'number' ||
      (parsed.type === 'UPSERT' && !parsed.campaign)
    ) {
      throw new Error('유효하지 않은 CampaignServingEvent Kafka payload');
    }
    return parsed as CampaignServingEvent;
  }

  private withKafkaSequence(
    event: CampaignServingEvent,
    partition: number,
    offset: string
  ): CampaignServingEvent {
    return {
      ...event,
      eventId: `kafka:${partition}:${offset}`,
      sequence: Number(offset) + 1,
    };
  }

  private updateMetrics(): void {
    this.metrics.setCampaignSnapshotState(this.snapshot.getMetadata());
  }

  private positiveInt(raw: string | undefined, fallback: number): number {
    const value = raw ? Number.parseInt(raw, 10) : fallback;
    return Number.isFinite(value) && value > 0 ? value : fallback;
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
