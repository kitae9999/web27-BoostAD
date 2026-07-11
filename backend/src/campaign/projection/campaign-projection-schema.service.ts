import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';
import { CreateCampaignProjectionPipeline1783789200000 } from '../../migrations/1783789200000-CreateCampaignProjectionPipeline';

@Injectable()
export class CampaignProjectionSchemaService implements OnModuleInit {
  private readonly logger = new Logger(CampaignProjectionSchemaService.name);
  private readonly enabled: boolean;

  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    configService: ConfigService
  ) {
    this.enabled =
      configService.get<string>('RTB_PROJECTION_PIPELINE_ENABLED', 'false') ===
      'true';
  }

  async onModuleInit(): Promise<void> {
    if (!this.enabled) return;
    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();
    try {
      const lockRows = (await queryRunner.query(
        "SELECT GET_LOCK('boostad_campaign_projection_schema', 30) AS acquired"
      )) as Array<{ acquired: number }>;
      if (Number(lockRows[0]?.acquired) !== 1) {
        throw new Error('Campaign projection schema lock 획득 실패');
      }
      try {
        const rows = (await queryRunner.query(`
          SELECT
            (SELECT COUNT(*) FROM information_schema.tables
             WHERE table_schema = DATABASE()
               AND table_name IN ('CampaignProjectionRequestOutbox', 'CampaignServingProjection', 'CampaignServingOutbox')) AS tableCount,
            (SELECT COUNT(*) FROM information_schema.triggers
             WHERE trigger_schema = DATABASE()
               AND trigger_name IN ('trg_campaign_projection_insert', 'trg_campaign_projection_update', 'trg_campaign_projection_delete', 'trg_campaign_tag_projection_insert', 'trg_campaign_tag_projection_delete')) AS triggerCount
        `)) as Array<{
          tableCount: string | number;
          triggerCount: string | number;
        }>;
        if (
          Number(rows[0]?.tableCount) !== 3 ||
          Number(rows[0]?.triggerCount) !== 5
        ) {
          await new CreateCampaignProjectionPipeline1783789200000().up(
            queryRunner
          );
        }
      } finally {
        await queryRunner.query(
          "SELECT RELEASE_LOCK('boostad_campaign_projection_schema')"
        );
      }
      this.logger.log('Campaign projection schema 준비 완료');
    } finally {
      await queryRunner.release();
    }
  }
}
