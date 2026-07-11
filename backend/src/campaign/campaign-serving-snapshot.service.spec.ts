import { CampaignServingSnapshotService } from './campaign-serving-snapshot.service';
import { CampaignCacheRepository } from './repository/campaign.cache.repository.interface';
import type { CachedCampaign } from './types/campaign.types';
import { ConfigService } from '@nestjs/config';
import type { CampaignServingEvent } from './events/campaign-serving-event';

describe('CampaignServingSnapshotService', () => {
  const embedding = Array.from({ length: 384 }, (_, index) => index / 384);

  const buildCampaign = (
    id: string,
    embeddingTags: Record<string, number[]> = { tag: embedding }
  ): CachedCampaign => ({
    id,
    userId: 1,
    title: id,
    content: 'content',
    image: null,
    url: 'https://example.com',
    maxCpc: 100,
    dailyBudget: 1000,
    totalBudget: 10000,
    dailySpent: 0,
    totalSpent: 0,
    lastResetDate: new Date().toISOString(),
    isHighIntent: false,
    status: 'ACTIVE',
    startDate: new Date(Date.now() - 60_000).toISOString(),
    endDate: new Date(Date.now() + 60_000).toISOString(),
    createdAt: new Date().toISOString(),
    deletedAt: null,
    tags: ['tag'],
    embeddingTags,
  });

  const buildRepository = (campaigns: CachedCampaign[]) =>
    ({
      getAllCampaigns: jest.fn().mockResolvedValue(campaigns),
      findCampaignCachesByIds: jest.fn((ids: string[]) =>
        Promise.resolve(
          campaigns.filter((campaign) => ids.includes(campaign.id))
        )
      ),
    }) as unknown as CampaignCacheRepository & {
      getAllCampaigns: jest.Mock;
      findCampaignCachesByIds: jest.Mock;
    };

  const configService = {
    get: jest.fn((key: string, defaultValue?: string) => {
      if (key === 'RTB_CAMPAIGN_SOURCE') return 'local_snapshot';
      if (key === 'RTB_EMBEDDING_PROFILE') return 'legacy_minilm';
      if (key === 'RTB_DENSE_RETRIEVAL_MODE') return 'legacy_tag';
      return defaultValue;
    }),
  } as unknown as ConfigService;

  const productDefaultConfigService = {
    get: jest.fn((key: string, defaultValue?: string) =>
      key === 'RTB_CAMPAIGN_SOURCE' ? 'local_snapshot' : defaultValue
    ),
  } as unknown as ConfigService;

  it('builds the initial snapshot only once for concurrent reads', async () => {
    const campaign = buildCampaign('c1');
    const repository = buildRepository([campaign]);
    const service = new CampaignServingSnapshotService(
      repository,
      configService
    );

    const [first, second] = await Promise.all([
      service.findCampaignsByIds(['c1']),
      service.findCampaignsByIds(['c1']),
    ]);

    expect(first.map((item) => item.id)).toEqual(['c1']);
    expect(second.map((item) => item.id)).toEqual(['c1']);
    expect(first[0].embeddingTags?.tag).toBeInstanceOf(Float32Array);
    expect(repository.getAllCampaigns).toHaveBeenCalledTimes(1);
    expect(repository.findCampaignCachesByIds).not.toHaveBeenCalled();
  });

  it('bootstraps from the versioned DB projection when the projection pipeline is enabled', async () => {
    const campaign = buildCampaign('projection-c1');
    const repository = buildRepository([]);
    const projectionRepository = {
      loadSnapshot: jest.fn().mockResolvedValue({
        campaigns: [campaign],
        checkpoint: { eventId: 'db:21', sequence: 21 },
        campaignVersions: new Map([['projection-c1', 20]]),
        complete: true,
      }),
    };
    const service = new CampaignServingSnapshotService(
      repository,
      {
        get: jest.fn((key: string, defaultValue?: string) => {
          if (key === 'RTB_CAMPAIGN_SOURCE') return 'local_snapshot';
          if (key === 'RTB_PROJECTION_PIPELINE_ENABLED') return 'true';
          if (key === 'RTB_EMBEDDING_PROFILE') return 'legacy_minilm';
          if (key === 'RTB_DENSE_RETRIEVAL_MODE') return 'legacy_tag';
          return defaultValue;
        }),
      } as unknown as ConfigService,
      projectionRepository as never
    );

    await expect(
      service.findCampaignsByIds(['projection-c1'])
    ).resolves.toHaveLength(1);
    expect(service.getMetadata()).toEqual(
      expect.objectContaining({
        ready: true,
        sequence: 21,
        lastEventId: 'db:21',
      })
    );
    expect(repository.getAllCampaigns).not.toHaveBeenCalled();
  });

  it('does not open readiness from partial projection events during initial backfill', async () => {
    const campaign = buildCampaign('partial-c1');
    const repository = buildRepository([]);
    const service = new CampaignServingSnapshotService(
      repository,
      {
        get: jest.fn((key: string, defaultValue?: string) => {
          if (key === 'RTB_CAMPAIGN_SOURCE') return 'local_snapshot';
          if (key === 'RTB_PROJECTION_PIPELINE_ENABLED') return 'true';
          if (key === 'RTB_EMBEDDING_PROFILE') return 'legacy_minilm';
          if (key === 'RTB_DENSE_RETRIEVAL_MODE') return 'legacy_tag';
          return defaultValue;
        }),
      } as unknown as ConfigService,
      {
        loadSnapshot: jest.fn().mockResolvedValue({
          campaigns: [],
          checkpoint: { eventId: 'kafka:0:-1', sequence: 0 },
          campaignVersions: new Map(),
          complete: false,
        }),
      } as never
    );
    await service.findCampaignsByIds(['missing']);

    service.applyServingEvent({
      schemaVersion: 1,
      eventId: 'kafka:0:0',
      type: 'UPSERT',
      campaignId: campaign.id,
      campaignVersion: 1,
      sequence: 1,
      occurredAtMs: Date.now(),
      campaign,
    });

    expect(service.getMetadata()).toMatchObject({ ready: false, size: 1 });
  });

  it('does not let a late Kafka event overwrite a newer bootstrapped campaign version', async () => {
    const latest = { ...buildCampaign('c1'), title: 'latest' };
    const older = { ...buildCampaign('c1'), title: 'older' };
    const repository = buildRepository([]);
    const service = new CampaignServingSnapshotService(
      repository,
      {
        get: jest.fn((key: string, defaultValue?: string) => {
          if (key === 'RTB_CAMPAIGN_SOURCE') return 'local_snapshot';
          if (key === 'RTB_PROJECTION_PIPELINE_ENABLED') return 'true';
          if (key === 'RTB_EMBEDDING_PROFILE') return 'legacy_minilm';
          if (key === 'RTB_DENSE_RETRIEVAL_MODE') return 'legacy_tag';
          return defaultValue;
        }),
      } as unknown as ConfigService,
      {
        loadSnapshot: jest.fn().mockResolvedValue({
          campaigns: [latest],
          checkpoint: { eventId: 'kafka:0:20', sequence: 21 },
          campaignVersions: new Map([['c1', 30]]),
          complete: true,
        }),
      } as never
    );
    await service.findCampaignsByIds(['c1']);

    expect(
      service.applyServingEvent({
        schemaVersion: 1,
        eventId: 'kafka:0:21',
        type: 'UPSERT',
        campaignId: 'c1',
        campaignVersion: 29,
        sequence: 22,
        occurredAtMs: Date.now(),
        campaign: older,
      })
    ).toBe('stale');
    await expect(service.findCampaignsByIds(['c1'])).resolves.toEqual([
      expect.objectContaining({ title: 'latest' }),
    ]);
  });

  it('rebuilds a bulk-loaded snapshot once while preserving its stream checkpoint', async () => {
    const first = buildCampaign('c1');
    const second = buildCampaign('c2');
    const repository = buildRepository([first]);
    const service = new CampaignServingSnapshotService(
      repository,
      configService
    );
    await service.findCampaignsByIds(['c1']);
    service.setCheckpoint({ eventId: '12-0', sequence: 12 });
    repository.getAllCampaigns.mockResolvedValue([first, second]);

    await service.refreshFromCurrentSource();

    expect(service.getMetadata()).toEqual(
      expect.objectContaining({ ready: true, sequence: 12, size: 2 })
    );
    expect(repository.getAllCampaigns).toHaveBeenCalledTimes(2);
  });

  it('requires the E5 document embedding in the default serving mode', async () => {
    const campaign = {
      ...buildCampaign('c1'),
      embeddingModelVersion:
        'Xenova/multilingual-e5-small@retrieval-v1-mean-normalized',
      embeddingDocument: embedding,
    };
    const repository = buildRepository([campaign]);
    const service = new CampaignServingSnapshotService(
      repository,
      productDefaultConfigService
    );

    const campaigns = await service.findCampaignsByIds(['c1']);

    expect(campaigns[0].embeddingDocument).toBeInstanceOf(Float32Array);
    expect(repository.findCampaignCachesByIds).not.toHaveBeenCalled();
  });

  it('returns campaigns in ANN ID order', async () => {
    const first = buildCampaign('c1');
    const second = buildCampaign('c2');
    const repository = buildRepository([first, second]);
    const service = new CampaignServingSnapshotService(
      repository,
      configService
    );

    const campaigns = await service.findCampaignsByIds(['c2', 'c1']);

    expect(campaigns.map((campaign) => campaign.id)).toEqual(['c2', 'c1']);
  });

  it('repairs a campaign whose embeddings were generated by another process', async () => {
    const incomplete = buildCampaign('c1', {});
    const completed = buildCampaign('c1');
    const repository = buildRepository([incomplete]);
    repository.findCampaignCachesByIds.mockResolvedValue([completed]);
    const service = new CampaignServingSnapshotService(
      repository,
      configService
    );

    const campaigns = await service.findCampaignsByIds(['c1']);

    expect(repository.findCampaignCachesByIds).toHaveBeenCalledWith(['c1']);
    expect(campaigns.map((campaign) => campaign.id)).toEqual(['c1']);
    expect(campaigns[0].embeddingTags?.tag).toBeInstanceOf(Float32Array);
  });

  it('applies cache mutations without rebuilding the whole snapshot', async () => {
    const first = buildCampaign('c1');
    const second = buildCampaign('c2');
    const repository = buildRepository([first]);
    const service = new CampaignServingSnapshotService(
      repository,
      configService
    );
    await service.findCampaignsByIds(['c1']);
    const previousVersion = service.getMetadata().version;

    service.onCampaignCacheUpserted({ campaign: second });
    service.onCampaignCacheRemoved({ campaignId: 'c1' });

    await expect(
      service
        .findCampaignsByIds(['c2'])
        .then((campaigns) => campaigns.map((campaign) => campaign.id))
    ).resolves.toEqual(['c2']);
    expect(service.getMetadata()).toMatchObject({
      version: previousVersion + 2,
      size: 1,
    });
  });

  it('serves normalized tag unions from the local inverted index', async () => {
    const first = { ...buildCampaign('c1'), tags: ['React', 'TypeScript'] };
    const second = { ...buildCampaign('c2'), tags: ['Redis'] };
    const repository = buildRepository([first, second]);
    const service = new CampaignServingSnapshotService(
      repository,
      configService
    );

    const campaigns = await service.findCampaignsByTags([
      ' react ',
      'REDIS',
      'react',
    ]);

    expect(campaigns.map((campaign) => campaign.id)).toEqual(['c1', 'c2']);
    expect(service.getMetadata().tagCount).toBe(3);
    expect(repository.findCampaignCachesByIds).not.toHaveBeenCalled();
  });

  it('keeps the tag index synchronized with upsert and remove events', async () => {
    const first = { ...buildCampaign('c1'), tags: ['before'] };
    const updated = { ...buildCampaign('c1'), tags: ['after'] };
    const repository = buildRepository([first]);
    const service = new CampaignServingSnapshotService(
      repository,
      configService
    );
    await service.findCampaignsByTags(['before']);

    service.onCampaignCacheUpserted({ campaign: updated });
    await expect(service.findCampaignsByTags(['before'])).resolves.toEqual([]);
    await expect(
      service
        .findCampaignsByTags(['after'])
        .then((campaigns) => campaigns.map((campaign) => campaign.id))
    ).resolves.toEqual(['c1']);

    service.onCampaignCacheRemoved({ campaignId: 'c1' });
    await expect(service.findCampaignsByTags(['after'])).resolves.toEqual([]);
  });

  it('does not lose mutations that arrive while the initial snapshot is loading', async () => {
    const stale = buildCampaign('c1');
    const updated = { ...buildCampaign('c1'), title: 'updated' };
    let finishLoading: ((campaigns: CachedCampaign[]) => void) | undefined;
    const repository = buildRepository([]);
    repository.getAllCampaigns.mockImplementation(
      () =>
        new Promise<CachedCampaign[]>((resolve) => {
          finishLoading = resolve;
        })
    );
    const service = new CampaignServingSnapshotService(
      repository,
      configService
    );

    const readInFlight = service.findCampaignsByIds(['c1']);
    await Promise.resolve();
    service.onCampaignCacheUpserted({ campaign: updated });
    finishLoading?.([stale]);

    const campaigns = await readInFlight;

    expect(campaigns[0].title).toBe('updated');
  });

  it('applies ordered serving events once and updates the tag index', async () => {
    const first = { ...buildCampaign('c1'), tags: ['before'] };
    const updated = { ...buildCampaign('c1'), tags: ['after'] };
    const repository = buildRepository([first]);
    const service = new CampaignServingSnapshotService(
      repository,
      configService
    );
    await service.findCampaignsByIds(['c1']);
    const event: CampaignServingEvent = {
      schemaVersion: 1,
      eventId: '1-0',
      type: 'UPSERT',
      campaignId: 'c1',
      campaignVersion: 1,
      sequence: 1,
      occurredAtMs: 1000,
      campaign: updated,
    };

    expect(service.applyServingEvent(event)).toBe('applied');
    const appliedVersion = service.getMetadata().version;
    expect(service.applyServingEvent(event)).toBe('stale');
    expect(service.getMetadata()).toMatchObject({
      version: appliedVersion,
      sequence: 1,
      lastEventId: '1-0',
      ready: true,
    });
    await expect(service.findCampaignsByTags(['before'])).resolves.toEqual([]);
    await expect(
      service
        .findCampaignsByTags(['after'])
        .then((campaigns) => campaigns.map((campaign) => campaign.id))
    ).resolves.toEqual(['c1']);
  });

  it('closes readiness instead of applying an event across a sequence gap', async () => {
    const first = buildCampaign('c1');
    const repository = buildRepository([first]);
    const service = new CampaignServingSnapshotService(
      repository,
      configService
    );
    await service.findCampaignsByIds(['c1']);

    expect(
      service.applyServingEvent({
        schemaVersion: 1,
        eventId: '2-0',
        type: 'DELETE',
        campaignId: 'c1',
        campaignVersion: 1,
        sequence: 2,
        occurredAtMs: 2000,
      })
    ).toBe('gap');
    expect(service.getMetadata()).toMatchObject({ ready: false, sequence: 0 });
    await expect(
      service
        .findCampaignsByIds(['c1'])
        .then((campaigns) => campaigns.map((campaign) => campaign.id))
    ).resolves.toEqual(['c1']);
  });
});
