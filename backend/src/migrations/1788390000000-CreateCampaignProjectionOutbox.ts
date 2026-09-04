import type { MigrationInterface, QueryRunner } from 'typeorm';

export class CreateCampaignProjectionOutbox1788390000000 implements MigrationInterface {
  name = 'CreateCampaignProjectionOutbox1788390000000';

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      ALTER TABLE \`Campaign\`
      ADD COLUMN \`serving_version\` BIGINT UNSIGNED NOT NULL DEFAULT 1
      COMMENT 'Search/Budget serving projection revision'
      AFTER \`user_id\`
    `);
    await queryRunner.query(`
      UPDATE \`Campaign\` SET \`serving_version\` = 1
      WHERE \`serving_version\` = 0
    `);
    await queryRunner.query(`
      ALTER TABLE \`CreditHistory\`
      ADD COLUMN \`operation_key\` VARCHAR(255) NULL
      COMMENT '비동기 정산의 exactly-once idempotency key'
      AFTER \`campaign_id\`
    `);
    await queryRunner.query(`
      CREATE UNIQUE INDEX \`uq_credit_history_operation_key\`
      ON \`CreditHistory\` (\`operation_key\`)
    `);
    await queryRunner.query(`
      CREATE TABLE \`CampaignProjectionOutbox\` (
        \`id\` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        \`event_id\` CHAR(36) NOT NULL,
        \`campaign_id\` VARCHAR(255) NOT NULL,
        \`serving_version\` BIGINT UNSIGNED NOT NULL,
        \`event_type\` ENUM('UPSERT', 'DELETE') NOT NULL,
        \`payload\` JSON NOT NULL,
        \`state\` ENUM('PENDING', 'PROCESSING', 'WAITING', 'RETRY', 'COMPLETED', 'DEAD') NOT NULL DEFAULT 'PENDING',
        \`attempts\` INT UNSIGNED NOT NULL DEFAULT 0,
        \`available_at\` DATETIME(3) NOT NULL,
        \`locked_until\` DATETIME(3) NULL,
        \`locked_by\` VARCHAR(128) NULL,
        \`last_error\` TEXT NULL,
        \`budget_applied_at\` DATETIME(3) NULL,
        \`search_applied_at\` DATETIME(3) NULL,
        \`embedding_enqueued_at\` DATETIME(3) NULL,
        \`deletion_settled_at\` DATETIME(3) NULL,
        \`completed_at\` DATETIME(3) NULL,
        \`created_at\` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
        \`updated_at\` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
        PRIMARY KEY (\`id\`),
        UNIQUE KEY \`uq_campaign_projection_event_id\` (\`event_id\`),
        UNIQUE KEY \`uq_campaign_projection_version\` (\`campaign_id\`, \`serving_version\`),
        KEY \`idx_campaign_projection_claim\` (\`state\`, \`available_at\`, \`locked_until\`, \`id\`),
        KEY \`idx_campaign_projection_order\` (\`campaign_id\`, \`serving_version\`, \`state\`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query('DROP TABLE `CampaignProjectionOutbox`');
    await queryRunner.query(
      'DROP INDEX `uq_credit_history_operation_key` ON `CreditHistory`'
    );
    await queryRunner.query(
      'ALTER TABLE `CreditHistory` DROP COLUMN `operation_key`'
    );
    await queryRunner.query(
      'ALTER TABLE `Campaign` DROP COLUMN `serving_version`'
    );
  }
}
