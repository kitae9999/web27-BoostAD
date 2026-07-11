import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
} from 'typeorm';

export enum CampaignProjectionOperation {
  UPSERT = 'UPSERT',
  DELETE = 'DELETE',
}

export enum CampaignOutboxState {
  PENDING = 'PENDING',
  PROCESSING = 'PROCESSING',
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
}

@Entity('CampaignProjectionRequestOutbox')
@Index('idx_campaign_projection_request_poll', ['state', 'availableAt', 'id'])
@Index('idx_campaign_projection_request_campaign', ['campaignId', 'id'])
export class CampaignProjectionRequestEntity {
  @PrimaryGeneratedColumn({ type: 'bigint', unsigned: true })
  id: string;

  @Column({ name: 'campaign_id', type: 'varchar', length: 255 })
  campaignId: string;

  @Column({ type: 'enum', enum: CampaignProjectionOperation })
  operation: CampaignProjectionOperation;

  @Column({
    type: 'enum',
    enum: CampaignOutboxState,
    default: CampaignOutboxState.PENDING,
  })
  state: CampaignOutboxState;

  @Column({ type: 'int', unsigned: true, default: 0 })
  attempts: number;

  @Column({
    name: 'available_at',
    type: 'datetime',
    precision: 3,
    default: () => 'CURRENT_TIMESTAMP(3)',
  })
  availableAt: Date;

  @Column({ name: 'locked_at', type: 'datetime', precision: 3, nullable: true })
  lockedAt: Date | null;

  @Column({ name: 'last_error', type: 'text', nullable: true })
  lastError: string | null;

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
