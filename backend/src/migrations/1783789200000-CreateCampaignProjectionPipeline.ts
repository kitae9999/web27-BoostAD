import type { MigrationInterface, QueryRunner } from 'typeorm';

export class CreateCampaignProjectionPipeline1783789200000 implements MigrationInterface {
  name = 'CreateCampaignProjectionPipeline1783789200000';

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      CREATE TABLE IF NOT EXISTS CampaignProjectionRequestOutbox (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        campaign_id VARCHAR(255) NOT NULL,
        operation ENUM('UPSERT', 'DELETE') NOT NULL,
        state ENUM('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED') NOT NULL DEFAULT 'PENDING',
        attempts INT UNSIGNED NOT NULL DEFAULT 0,
        available_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        locked_at DATETIME(3) NULL,
        last_error TEXT NULL,
        created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
        PRIMARY KEY (id),
        INDEX idx_campaign_projection_request_poll (state, available_at, id),
        INDEX idx_campaign_projection_request_campaign (campaign_id, id)
      ) ENGINE=InnoDB
    `);
    await queryRunner.query(`
      CREATE TABLE IF NOT EXISTS CampaignServingProjection (
        campaign_id VARCHAR(255) NOT NULL,
        version BIGINT UNSIGNED NOT NULL,
        schema_version SMALLINT UNSIGNED NOT NULL DEFAULT 1,
        document JSON NULL,
        document_hash CHAR(64) NULL,
        deleted TINYINT(1) NOT NULL DEFAULT 0,
        updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
        PRIMARY KEY (campaign_id),
        INDEX idx_campaign_serving_projection_version (version)
      ) ENGINE=InnoDB
    `);
    await queryRunner.query(`
      CREATE TABLE IF NOT EXISTS CampaignServingOutbox (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        campaign_id VARCHAR(255) NOT NULL,
        campaign_version BIGINT UNSIGNED NOT NULL,
        event_type ENUM('UPSERT', 'DELETE') NOT NULL,
        payload JSON NOT NULL,
        publish_attempts INT UNSIGNED NOT NULL DEFAULT 0,
        state ENUM('PENDING', 'PUBLISHING', 'PUBLISHED', 'FAILED') NOT NULL DEFAULT 'PENDING',
        available_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        locked_at DATETIME(3) NULL,
        published_at DATETIME(3) NULL,
        last_error TEXT NULL,
        created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        PRIMARY KEY (id),
        UNIQUE KEY uq_campaign_serving_outbox_version (campaign_id, campaign_version),
        INDEX idx_campaign_serving_outbox_poll (state, available_at, id)
      ) ENGINE=InnoDB
    `);

    await this.replaceTriggers(queryRunner);

    await queryRunner.query(`
      INSERT INTO CampaignProjectionRequestOutbox (campaign_id, operation)
      SELECT id, IF(deleted_at IS NULL, 'UPSERT', 'DELETE') FROM Campaign
    `);
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      'DROP TRIGGER IF EXISTS trg_campaign_tag_projection_delete'
    );
    await queryRunner.query(
      'DROP TRIGGER IF EXISTS trg_campaign_tag_projection_insert'
    );
    await queryRunner.query(
      'DROP TRIGGER IF EXISTS trg_campaign_projection_delete'
    );
    await queryRunner.query(
      'DROP TRIGGER IF EXISTS trg_campaign_projection_update'
    );
    await queryRunner.query(
      'DROP TRIGGER IF EXISTS trg_campaign_projection_insert'
    );
    await queryRunner.query('DROP TABLE IF EXISTS CampaignServingOutbox');
    await queryRunner.query('DROP TABLE IF EXISTS CampaignServingProjection');
    await queryRunner.query(
      'DROP TABLE IF EXISTS CampaignProjectionRequestOutbox'
    );
  }

  private async replaceTriggers(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      'DROP TRIGGER IF EXISTS trg_campaign_projection_insert'
    );
    await queryRunner.query(`
      CREATE TRIGGER trg_campaign_projection_insert AFTER INSERT ON Campaign
      FOR EACH ROW INSERT INTO CampaignProjectionRequestOutbox (campaign_id, operation)
      VALUES (NEW.id, IF(NEW.deleted_at IS NULL, 'UPSERT', 'DELETE'))
    `);

    await queryRunner.query(
      'DROP TRIGGER IF EXISTS trg_campaign_projection_update'
    );
    await queryRunner.query(`
      CREATE TRIGGER trg_campaign_projection_update AFTER UPDATE ON Campaign
      FOR EACH ROW
      BEGIN
        IF NOT (OLD.title <=> NEW.title)
          OR NOT (OLD.content <=> NEW.content)
          OR NOT (OLD.image <=> NEW.image)
          OR NOT (OLD.url <=> NEW.url)
          OR NOT (OLD.max_cpc <=> NEW.max_cpc)
          OR NOT (OLD.daily_budget <=> NEW.daily_budget)
          OR NOT (OLD.total_budget <=> NEW.total_budget)
          OR NOT (OLD.is_high_intent <=> NEW.is_high_intent)
          OR NOT (OLD.status <=> NEW.status)
          OR NOT (OLD.start_date <=> NEW.start_date)
          OR NOT (OLD.end_date <=> NEW.end_date)
          OR NOT (OLD.deleted_at <=> NEW.deleted_at)
        THEN
          INSERT INTO CampaignProjectionRequestOutbox (campaign_id, operation)
          VALUES (NEW.id, IF(NEW.deleted_at IS NULL, 'UPSERT', 'DELETE'));
        END IF;
      END
    `);

    await queryRunner.query(
      'DROP TRIGGER IF EXISTS trg_campaign_projection_delete'
    );
    await queryRunner.query(`
      CREATE TRIGGER trg_campaign_projection_delete AFTER DELETE ON Campaign
      FOR EACH ROW INSERT INTO CampaignProjectionRequestOutbox (campaign_id, operation)
      VALUES (OLD.id, 'DELETE')
    `);

    await queryRunner.query(
      'DROP TRIGGER IF EXISTS trg_campaign_tag_projection_insert'
    );
    await queryRunner.query(`
      CREATE TRIGGER trg_campaign_tag_projection_insert AFTER INSERT ON CampaignTag
      FOR EACH ROW INSERT INTO CampaignProjectionRequestOutbox (campaign_id, operation)
      VALUES (NEW.campaign_id, 'UPSERT')
    `);

    await queryRunner.query(
      'DROP TRIGGER IF EXISTS trg_campaign_tag_projection_delete'
    );
    await queryRunner.query(`
      CREATE TRIGGER trg_campaign_tag_projection_delete AFTER DELETE ON CampaignTag
      FOR EACH ROW INSERT INTO CampaignProjectionRequestOutbox (campaign_id, operation)
      VALUES (OLD.campaign_id, 'UPSERT')
    `);
  }
}
