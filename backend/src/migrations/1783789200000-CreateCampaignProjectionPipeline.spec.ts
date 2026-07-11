import type { QueryRunner } from 'typeorm';
import { CreateCampaignProjectionPipeline1783789200000 } from './1783789200000-CreateCampaignProjectionPipeline';

describe('CreateCampaignProjectionPipeline migration', () => {
  it('creates both outboxes, the versioned projection and all mutation triggers', async () => {
    const statements: string[] = [];
    const queryRunner = {
      query: jest.fn((sql: string) => {
        statements.push(sql);
        return Promise.resolve(undefined);
      }),
    } as unknown as QueryRunner;

    await new CreateCampaignProjectionPipeline1783789200000().up(queryRunner);

    const sql = statements.join('\n');
    expect(sql).toContain('CampaignProjectionRequestOutbox');
    expect(sql).toContain('CampaignServingProjection');
    expect(sql).toContain('CampaignServingOutbox');
    expect(sql).toContain('trg_campaign_projection_insert');
    expect(sql).toContain('trg_campaign_projection_update');
    expect(sql).toContain('trg_campaign_projection_delete');
    expect(sql).toContain('trg_campaign_tag_projection_insert');
    expect(sql).toContain('trg_campaign_tag_projection_delete');
  });
});
