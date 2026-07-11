import {
  Column,
  Entity,
  Index,
  PrimaryColumn,
  UpdateDateColumn,
} from 'typeorm';
import type { CachedCampaign } from '../../types/campaign.types';

@Entity('CampaignServingProjection')
@Index('idx_campaign_serving_projection_version', ['version'])
export class CampaignServingProjectionEntity {
  @PrimaryColumn({ name: 'campaign_id', type: 'varchar', length: 255 })
  campaignId: string;

  @Column({ type: 'bigint', unsigned: true })
  version: string;

  @Column({
    name: 'schema_version',
    type: 'smallint',
    unsigned: true,
    default: 1,
  })
  schemaVersion: number;

  @Column({ type: 'json', nullable: true })
  document: CachedCampaign | null;

  @Column({ name: 'document_hash', type: 'char', length: 64, nullable: true })
  documentHash: string | null;

  @Column({ type: 'boolean', default: false })
  deleted: boolean;

  @UpdateDateColumn({
    name: 'updated_at',
    type: 'datetime',
    precision: 3,
    default: () => 'CURRENT_TIMESTAMP(3)',
    onUpdate: 'CURRENT_TIMESTAMP(3)',
  })
  updatedAt: Date;
}
