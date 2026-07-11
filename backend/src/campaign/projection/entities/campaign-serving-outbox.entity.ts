import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  PrimaryGeneratedColumn,
} from 'typeorm';
import type { CampaignServingEvent } from '../../events/campaign-serving-event';

export enum CampaignServingPublishState {
  PENDING = 'PENDING',
  PUBLISHING = 'PUBLISHING',
  PUBLISHED = 'PUBLISHED',
  FAILED = 'FAILED',
}

@Entity('CampaignServingOutbox')
@Index('idx_campaign_serving_outbox_poll', ['state', 'availableAt', 'id'])
@Index(
  'uq_campaign_serving_outbox_version',
  ['campaignId', 'campaignVersion'],
  {
    unique: true,
  }
)
export class CampaignServingOutboxEntity {
  @PrimaryGeneratedColumn({ type: 'bigint', unsigned: true })
  id: string;

  @Column({ name: 'campaign_id', type: 'varchar', length: 255 })
  campaignId: string;

  @Column({ name: 'campaign_version', type: 'bigint', unsigned: true })
  campaignVersion: string;

  @Column({ name: 'event_type', type: 'enum', enum: ['UPSERT', 'DELETE'] })
  eventType: 'UPSERT' | 'DELETE';

  @Column({ type: 'json' })
  payload: CampaignServingEvent;

  @Column({ name: 'publish_attempts', type: 'int', unsigned: true, default: 0 })
  publishAttempts: number;

  @Column({
    type: 'enum',
    enum: CampaignServingPublishState,
    default: CampaignServingPublishState.PENDING,
  })
  state: CampaignServingPublishState;

  @Column({
    name: 'available_at',
    type: 'datetime',
    precision: 3,
    default: () => 'CURRENT_TIMESTAMP(3)',
  })
  availableAt: Date;

  @Column({ name: 'locked_at', type: 'datetime', precision: 3, nullable: true })
  lockedAt: Date | null;

  @Column({
    name: 'published_at',
    type: 'datetime',
    precision: 3,
    nullable: true,
  })
  publishedAt: Date | null;

  @Column({
    name: 'kafka_offset',
    type: 'bigint',
    unsigned: true,
    nullable: true,
  })
  kafkaOffset: string | null;

  @Column({ name: 'last_error', type: 'text', nullable: true })
  lastError: string | null;

  @CreateDateColumn({
    name: 'created_at',
    type: 'datetime',
    precision: 3,
    default: () => 'CURRENT_TIMESTAMP(3)',
  })
  createdAt: Date;
}
