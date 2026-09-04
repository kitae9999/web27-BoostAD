import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
} from 'typeorm';
import {
  CampaignProjectionEventType,
  CampaignProjectionOutboxState,
  type CampaignProjectionPayload,
} from '../campaign-projection.types';

@Entity('CampaignProjectionOutbox')
@Index('uq_campaign_projection_version', ['campaignId', 'servingVersion'], {
  unique: true,
})
@Index('idx_campaign_projection_claim', [
  'state',
  'availableAt',
  'lockedUntil',
  'id',
])
export class CampaignProjectionOutboxEntity {
  @PrimaryGeneratedColumn({ type: 'bigint', unsigned: true })
  id: string;

  @Column({ name: 'event_id', type: 'char', length: 36, unique: true })
  eventId: string;

  @Column({ name: 'campaign_id', type: 'varchar', length: 255 })
  campaignId: string;

  @Column({ name: 'serving_version', type: 'bigint', unsigned: true })
  servingVersion: number;

  @Column({
    name: 'event_type',
    type: 'enum',
    enum: CampaignProjectionEventType,
  })
  eventType: CampaignProjectionEventType;

  @Column({ type: 'json' })
  payload: CampaignProjectionPayload;

  @Column({
    type: 'enum',
    enum: CampaignProjectionOutboxState,
    default: CampaignProjectionOutboxState.PENDING,
  })
  state: CampaignProjectionOutboxState;

  @Column({ type: 'int', unsigned: true, default: 0 })
  attempts: number;

  @Column({ name: 'available_at', type: 'datetime', precision: 3 })
  availableAt: Date;

  @Column({
    name: 'locked_until',
    type: 'datetime',
    precision: 3,
    nullable: true,
  })
  lockedUntil: Date | null;

  @Column({ name: 'locked_by', type: 'varchar', length: 128, nullable: true })
  lockedBy: string | null;

  @Column({ name: 'last_error', type: 'text', nullable: true })
  lastError: string | null;

  @Column({
    name: 'budget_applied_at',
    type: 'datetime',
    precision: 3,
    nullable: true,
  })
  budgetAppliedAt: Date | null;

  @Column({
    name: 'search_applied_at',
    type: 'datetime',
    precision: 3,
    nullable: true,
  })
  searchAppliedAt: Date | null;

  @Column({
    name: 'embedding_enqueued_at',
    type: 'datetime',
    precision: 3,
    nullable: true,
  })
  embeddingEnqueuedAt: Date | null;

  @Column({
    name: 'deletion_settled_at',
    type: 'datetime',
    precision: 3,
    nullable: true,
  })
  deletionSettledAt: Date | null;

  @Column({
    name: 'completed_at',
    type: 'datetime',
    precision: 3,
    nullable: true,
  })
  completedAt: Date | null;

  @CreateDateColumn({
    name: 'created_at',
    type: 'datetime',
    precision: 3,
    default: () => 'CURRENT_TIMESTAMP(3)',
  })
  createdAt: Date;

  @UpdateDateColumn({
    name: 'updated_at',
    type: 'datetime',
    precision: 3,
    default: () => 'CURRENT_TIMESTAMP(3)',
    onUpdate: 'CURRENT_TIMESTAMP(3)',
  })
  updatedAt: Date;
}
