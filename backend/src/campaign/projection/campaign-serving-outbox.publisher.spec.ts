/* eslint-disable @typescript-eslint/unbound-method */
import type { ConfigService } from '@nestjs/config';
import type { DataSource } from 'typeorm';
import type { CampaignServingKafkaProducer } from '../../kafka/campaign-serving-kafka.producer';
import { CampaignServingOutboxPublisher } from './campaign-serving-outbox.publisher';
import {
  CampaignServingPublishState,
  type CampaignServingOutboxEntity,
} from './entities/campaign-serving-outbox.entity';

describe('CampaignServingOutboxPublisher', () => {
  const outbox = {
    id: '3',
    publishAttempts: 1,
    payload: {
      schemaVersion: 1,
      eventId: 'db:3',
      type: 'DELETE',
      campaignId: 'c1',
      campaignVersion: 2,
      sequence: 3,
      occurredAtMs: 1000,
    },
  } as CampaignServingOutboxEntity;

  const build = (publishError?: Error) => {
    const update = jest.fn().mockResolvedValue(undefined);
    const dataSource = {
      getRepository: jest.fn(() => ({ update })),
    } as unknown as DataSource;
    const producer = {
      publish: publishError
        ? jest.fn().mockRejectedValue(publishError)
        : jest.fn().mockResolvedValue('2'),
    } as unknown as CampaignServingKafkaProducer;
    const publisher = new CampaignServingOutboxPublisher(dataSource, producer, {
      get: jest.fn((key: string, fallback?: string) =>
        key === 'RTB_PROJECTION_PIPELINE_ENABLED' ? 'true' : fallback
      ),
    } as unknown as ConfigService);
    (publisher as unknown as { claimNext: jest.Mock }).claimNext = jest
      .fn()
      .mockResolvedValue(outbox);
    return { publisher, producer, update };
  };

  it('marks the outbox published only after Kafka acknowledgement', async () => {
    const { publisher, producer, update } = build();

    await expect(publisher.processOnce()).resolves.toBe('published');

    expect(producer.publish).toHaveBeenCalledWith(outbox.payload);
    expect(update).toHaveBeenCalledWith(
      { id: '3' },
      expect.objectContaining({
        state: CampaignServingPublishState.PUBLISHED,
        publishedAt: expect.any(Date),
        kafkaOffset: '2',
      })
    );
  });

  it('keeps a failed Kafka publish retryable', async () => {
    const { publisher, update } = build(new Error('kafka unavailable'));

    await expect(publisher.processOnce()).resolves.toBe('failed');

    expect(update).toHaveBeenCalledWith(
      { id: '3' },
      expect.objectContaining({
        state: CampaignServingPublishState.FAILED,
        availableAt: expect.any(Date),
      })
    );
  });
});
