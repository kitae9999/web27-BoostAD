import type { ConfigService } from '@nestjs/config';
import type { SASLOptions } from 'kafkajs';

export type CampaignKafkaConfig = {
  enabled: boolean;
  brokers: string[];
  clientId: string;
  topic: string;
  ssl: boolean;
  sasl?: SASLOptions;
  replicationFactor: number;
};

export function resolveCampaignKafkaConfig(
  configService: ConfigService,
  clientSuffix: string
): CampaignKafkaConfig {
  const username = configService.get<string>('KAFKA_SASL_USERNAME');
  const password = configService.get<string>('KAFKA_SASL_PASSWORD');
  const mechanism = configService.get<string>('KAFKA_SASL_MECHANISM', 'plain');
  let sasl: SASLOptions | undefined;
  if (username && password) {
    if (mechanism === 'scram-sha-256' || mechanism === 'scram-sha-512') {
      sasl = { mechanism, username, password };
    } else {
      sasl = { mechanism: 'plain', username, password };
    }
  }
  return {
    enabled:
      configService.get<string>('RTB_PROJECTION_PIPELINE_ENABLED', 'false') ===
      'true',
    brokers: configService
      .get<string>('KAFKA_BROKERS', 'localhost:9092')
      .split(',')
      .map((broker) => broker.trim())
      .filter(Boolean),
    clientId: `${configService.get<string>('KAFKA_CLIENT_ID', 'boostad')}-${clientSuffix}`,
    topic: configService.get<string>(
      'KAFKA_CAMPAIGN_SERVING_TOPIC',
      'campaign-serving-v1'
    ),
    ssl: configService.get<string>('KAFKA_SSL', 'false') === 'true',
    ...(sasl ? { sasl } : {}),
    replicationFactor: positiveInt(
      configService.get<string>('KAFKA_TOPIC_REPLICATION_FACTOR'),
      1
    ),
  };
}

function positiveInt(raw: string | undefined, fallback: number): number {
  const value = raw ? Number.parseInt(raw, 10) : fallback;
  return Number.isFinite(value) && value > 0 ? value : fallback;
}
