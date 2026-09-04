import type { ConfigService } from '@nestjs/config';
import type { DataSource } from 'typeorm';
import {
  CampaignStatus,
  type CampaignEntity,
} from '../entities/campaign.entity';
import type { CampaignCacheRepository } from '../repository/campaign.cache.repository.interface';
import { CampaignProjectionCommandService } from './campaign-projection-command.service';
import type { CampaignProjectionOutboxWriter } from './campaign-projection-outbox.writer';
import { CampaignProjectionEventType } from './campaign-projection.types';

function buildHarness(mode: 'off' | 'shadow' | 'active') {
  const campaign = {
    id: 'campaign-1',
    servingVersion: 2,
    status: CampaignStatus.ACTIVE,
    tags: [],
  } as CampaignEntity;
  const campaignRepository = {
    findOne: jest.fn().mockResolvedValue(campaign),
    save: jest.fn(async (value: CampaignEntity) => value),
  };
  const manager = {
    getRepository: jest.fn(() => campaignRepository),
  };
  const dataSource = {
    transaction: jest.fn((callback: (value: typeof manager) => unknown) =>
      callback(manager)
    ),
  } as unknown as DataSource & { transaction: jest.Mock };
  const writer = {
    append: jest.fn().mockResolvedValue(undefined),
  } as unknown as CampaignProjectionOutboxWriter & { append: jest.Mock };
  const cache = {
    updateCampaignStatus: jest.fn().mockResolvedValue(undefined),
  } as unknown as CampaignCacheRepository & {
    updateCampaignStatus: jest.Mock;
  };
  const config = {
    get: jest.fn((key: string, fallback?: string) =>
      key === 'CAMPAIGN_PROJECTION_MODE' ? mode : fallback
    ),
  } as unknown as ConfigService;
  const service = new CampaignProjectionCommandService(
    dataSource,
    writer,
    cache,
    config
  );
  return { service, campaign, campaignRepository, manager, writer, cache };
}

describe('CampaignProjectionCommandService', () => {
  it('commits status, servingVersion and Outbox through one manager', async () => {
    const harness = buildHarness('active');

    await expect(
      harness.service.updateStatus('campaign-1', CampaignStatus.PAUSED)
    ).resolves.toBe(true);

    expect(harness.campaignRepository.findOne).toHaveBeenCalledWith(
      expect.objectContaining({ lock: { mode: 'pessimistic_write' } })
    );
    expect(harness.campaign.servingVersion).toBe(3);
    expect(harness.writer.append).toHaveBeenCalledWith(
      harness.manager,
      harness.campaign,
      CampaignProjectionEventType.UPSERT
    );
    expect(harness.cache.updateCampaignStatus).not.toHaveBeenCalled();
  });

  it('propagates Outbox failure so the surrounding transaction rolls back', async () => {
    const harness = buildHarness('active');
    harness.writer.append.mockRejectedValue(new Error('outbox insert failed'));

    await expect(
      harness.service.updateStatus('campaign-1', CampaignStatus.PAUSED)
    ).rejects.toThrow('outbox insert failed');

    expect(harness.cache.updateCampaignStatus).not.toHaveBeenCalled();
  });

  it('records Outbox and keeps the legacy synchronous mirror in off mode', async () => {
    const harness = buildHarness('off');

    await harness.service.updateStatus('campaign-1', CampaignStatus.PAUSED);

    expect(harness.campaign.servingVersion).toBe(3);
    expect(harness.writer.append).toHaveBeenCalledWith(
      harness.manager,
      harness.campaign,
      CampaignProjectionEventType.UPSERT
    );
    expect(harness.cache.updateCampaignStatus).toHaveBeenCalledWith(
      'campaign-1',
      CampaignStatus.PAUSED
    );
  });
});
