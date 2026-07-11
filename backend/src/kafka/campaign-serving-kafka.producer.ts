import {
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Kafka, logLevel, type Producer } from 'kafkajs';
import type { CampaignServingEvent } from '../campaign/events/campaign-serving-event';
import {
  resolveCampaignKafkaConfig,
  type CampaignKafkaConfig,
} from './kafka.config';

@Injectable()
export class CampaignServingKafkaProducer
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(CampaignServingKafkaProducer.name);
  private readonly config: CampaignKafkaConfig;
  private readonly kafka: Kafka;
  private producer: Producer | null = null;
  private connectPromise: Promise<void> | null = null;

  constructor(configService: ConfigService) {
    this.config = resolveCampaignKafkaConfig(configService, 'projection');
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
    await this.ensureConnected().catch((error: unknown) => {
      this.logger.warn(
        'Kafka가 준비되지 않아 serving outbox 발행을 지연합니다.',
        error
      );
    });
  }

  private async connect(): Promise<void> {
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      const topics = await admin.listTopics();
      if (!topics.includes(this.config.topic)) {
        await admin.createTopics({
          waitForLeaders: true,
          topics: [
            {
              topic: this.config.topic,
              numPartitions: 1,
              replicationFactor: this.config.replicationFactor,
              configEntries: [
                { name: 'cleanup.policy', value: 'delete' },
                { name: 'retention.ms', value: '604800000' },
              ],
            },
          ],
        });
      }
      const metadata = await admin.fetchTopicMetadata({
        topics: [this.config.topic],
      });
      const partitions = metadata.topics[0]?.partitions.length ?? 0;
      if (partitions !== 1) {
        throw new Error(
          `Campaign serving topic은 전역 순서를 위해 1 partition이어야 합니다: ${partitions}`
        );
      }
    } finally {
      await admin.disconnect();
    }
    const producer = this.kafka.producer({
      idempotent: true,
      maxInFlightRequests: 1,
    });
    await producer.connect();
    this.producer = producer;
    this.logger.log(
      `Campaign serving Kafka producer 준비 완료: topic=${this.config.topic}`
    );
  }

  async onApplicationShutdown(): Promise<void> {
    await this.connectPromise?.catch(() => undefined);
    await this.producer?.disconnect();
    this.producer = null;
  }

  async publish(event: CampaignServingEvent): Promise<string> {
    if (!this.config.enabled) return '0';
    await this.ensureConnected();
    if (!this.producer)
      throw new Error('Kafka producer가 준비되지 않았습니다.');
    const producer = this.producer;
    let result: Awaited<ReturnType<Producer['send']>>;
    try {
      result = await producer.send({
        topic: this.config.topic,
        acks: -1,
        messages: [
          {
            key: 'campaign-serving-global-order',
            value: JSON.stringify(event),
            headers: {
              schemaVersion: String(event.schemaVersion),
              sequence: String(event.sequence),
            },
          },
        ],
      });
    } catch (error) {
      if (this.producer === producer) this.producer = null;
      await producer.disconnect().catch(() => undefined);
      throw error;
    }
    const offset = result[0]?.baseOffset;
    if (offset === undefined) {
      throw new Error('Kafka publish 결과에 offset이 없습니다.');
    }
    return offset;
  }

  private ensureConnected(): Promise<void> {
    if (this.producer) return Promise.resolve();
    if (!this.connectPromise) {
      this.connectPromise = this.connect().finally(() => {
        this.connectPromise = null;
      });
    }
    return this.connectPromise;
  }
}
