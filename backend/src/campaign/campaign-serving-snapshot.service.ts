import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { OnEvent } from '@nestjs/event-emitter';
import { CampaignCacheRepository } from './repository/campaign.cache.repository.interface';
import type { CachedCampaign } from './types/campaign.types';
import {
  resolveEmbeddingProfile,
  type EmbeddingProfile,
} from '../rtb/ml/embedding-profile';
import {
  CAMPAIGN_CACHE_REMOVED_EVENT,
  CAMPAIGN_CACHE_UPSERTED_EVENT,
  type CampaignCacheRemovedEvent,
  type CampaignCacheUpsertedEvent,
} from './events/campaign-cache.events';
import {
  CAMPAIGN_SERVING_EVENT_SCHEMA_VERSION,
  type CampaignServingEvent,
  type CampaignServingEventCheckpoint,
} from './events/campaign-serving-event';
import { CampaignServingProjectionRepository } from './projection/campaign-serving-projection.repository';

export type ServingCampaign = Omit<
  CachedCampaign,
  'embeddingTags' | 'embeddingDocument'
> & {
  embeddingTags?: Record<string, Float32Array>;
  embeddingDocument?: Float32Array;
};

type SnapshotMutation = ServingCampaign | null;

type CampaignServingSnapshotState = {
  version: number;
  builtAtMs: number;
  ready: boolean;
  sequence: number;
  lastEventId: string;
  lastEventAtMs: number;
  campaignsById: ReadonlyMap<string, ServingCampaign>;
  campaignIdsByTag: ReadonlyMap<string, ReadonlySet<string>>;
  campaignVersions: ReadonlyMap<string, number>;
};

export type CampaignServingEventApplyResult =
  | 'applied'
  | 'stale'
  | 'gap'
  | 'schema_mismatch';

@Injectable()
export class CampaignServingSnapshotService implements OnApplicationBootstrap {
  private readonly logger = new Logger(CampaignServingSnapshotService.name);
  private state: CampaignServingSnapshotState = {
    version: 0,
    builtAtMs: 0,
    ready: false,
    sequence: 0,
    lastEventId: '0-0',
    lastEventAtMs: 0,
    campaignsById: new Map(),
    campaignIdsByTag: new Map(),
    campaignVersions: new Map(),
  };
  private initialized = false;
  private initializationInFlight: Promise<void> | null = null;
  private readonly mutationsDuringInitialization = new Map<
    string,
    SnapshotMutation
  >();
  private readonly enabled: boolean;
  private readonly eventSyncEnabled: boolean;
  private readonly projectionBootstrapEnabled: boolean;
  private readonly embeddingProfile: EmbeddingProfile;
  private readonly requireDocumentEmbedding: boolean;

  constructor(
    private readonly campaignCacheRepository: CampaignCacheRepository,
    configService: ConfigService,
    private readonly projectionRepository: CampaignServingProjectionRepository
  ) {
    this.embeddingProfile = resolveEmbeddingProfile(
      configService.get<string>('RTB_EMBEDDING_PROFILE')
    );
    this.requireDocumentEmbedding =
      configService.get<string>(
        'RTB_DENSE_RETRIEVAL_MODE',
        'semantic_document'
      ) === 'semantic_document';
    const campaignSource = configService.get<string>('RTB_CAMPAIGN_SOURCE');
    this.enabled = campaignSource
      ? campaignSource === 'local_snapshot'
      : configService.get<string>(
          'RTB_MATCHER_LOCAL_SNAPSHOT_ENABLED',
          'false'
        ) === 'true';
    this.eventSyncEnabled =
      configService.get<string>('RTB_CAMPAIGN_EVENT_SYNC_ENABLED', 'false') ===
      'true';
    this.projectionBootstrapEnabled =
      configService.get<string>('RTB_PROJECTION_PIPELINE_ENABLED', 'false') ===
        'true' ||
      configService.get<string>('RTB_CAMPAIGN_BOOTSTRAP_SOURCE') ===
        'db_projection';
  }

  async onApplicationBootstrap(): Promise<void> {
    if (this.enabled && !this.eventSyncEnabled) {
      await this.ensureInitialized();
    }
  }

  isEnabled(): boolean {
    return this.enabled;
  }

  async findCampaignsByIds(ids: string[]): Promise<ServingCampaign[]> {
    const uniqueIds = [...new Set(ids.filter(Boolean))];
    if (uniqueIds.length === 0) {
      return [];
    }

    await this.ensureInitialized();

    const repairIds = uniqueIds.filter((id) => {
      const campaign = this.state.campaignsById.get(id);
      return !campaign || !this.hasRequiredEmbeddings(campaign);
    });

    if (repairIds.length > 0) {
      const repaired =
        await this.campaignCacheRepository.findCampaignCachesByIds(repairIds);
      this.upsertMany(
        repaired.map((campaign) => this.toServingCampaign(campaign))
      );
    }

    return uniqueIds.flatMap((id) => {
      const campaign = this.state.campaignsById.get(id);
      return campaign ? [campaign] : [];
    });
  }

  async findCampaignsByTags(tags: string[]): Promise<ServingCampaign[]> {
    await this.ensureInitialized();

    const campaignIds = new Set<string>();
    for (const tag of this.normalizeTags(tags)) {
      for (const campaignId of this.state.campaignIdsByTag.get(tag) ?? []) {
        campaignIds.add(campaignId);
      }
    }

    return [...campaignIds].flatMap((campaignId) => {
      const campaign = this.state.campaignsById.get(campaignId);
      return campaign ? [campaign] : [];
    });
  }

  getMetadata(): {
    version: number;
    builtAtMs: number;
    size: number;
    tagCount: number;
    ready: boolean;
    sequence: number;
    lastEventId: string;
    lastEventAtMs: number;
  } {
    return {
      version: this.state.version,
      builtAtMs: this.state.builtAtMs,
      size: this.state.campaignsById.size,
      tagCount: this.state.campaignIdsByTag.size,
      ready: this.state.ready,
      sequence: this.state.sequence,
      lastEventId: this.state.lastEventId,
      lastEventAtMs: this.state.lastEventAtMs,
    };
  }

  @OnEvent(CAMPAIGN_CACHE_UPSERTED_EVENT)
  onCampaignCacheUpserted(event: CampaignCacheUpsertedEvent): void {
    if (!this.enabled) {
      return;
    }
    if (event.servingEvent) {
      this.applyServingEvent(event.servingEvent);
      return;
    }
    const campaign = this.toServingCampaign(event.campaign);
    this.recordMutation(campaign.id, campaign);
    if (this.initialized) {
      this.upsertMany([campaign]);
    }
  }

  @OnEvent(CAMPAIGN_CACHE_REMOVED_EVENT)
  onCampaignCacheRemoved(event: CampaignCacheRemovedEvent): void {
    if (!this.enabled) {
      return;
    }
    if (event.servingEvent) {
      this.applyServingEvent(event.servingEvent);
      return;
    }
    this.recordMutation(event.campaignId, null);
    if (this.initialized) {
      this.remove(event.campaignId);
    }
  }

  applyServingEvent(
    event: CampaignServingEvent
  ): CampaignServingEventApplyResult {
    if (event.schemaVersion !== CAMPAIGN_SERVING_EVENT_SCHEMA_VERSION) {
      this.markNotReady();
      return 'schema_mismatch';
    }
    if (event.sequence <= this.state.sequence) {
      return 'stale';
    }
    if (event.sequence !== this.state.sequence + 1) {
      this.markNotReady();
      return 'gap';
    }

    const currentCampaignVersion =
      this.state.campaignVersions.get(event.campaignId) ?? 0;
    const campaignVersions = new Map(this.state.campaignVersions);
    campaignVersions.set(event.campaignId, event.campaignVersion);
    if (event.campaignVersion <= currentCampaignVersion) {
      this.state = {
        ...this.state,
        sequence: event.sequence,
        lastEventId: event.eventId,
        lastEventAtMs: event.occurredAtMs,
        campaignVersions,
      };
      return 'stale';
    }

    const campaignsById = new Map(this.state.campaignsById);
    if (event.type === 'UPSERT') {
      campaignsById.set(
        event.campaignId,
        this.toServingCampaign(event.campaign)
      );
    } else {
      campaignsById.delete(event.campaignId);
    }
    this.state = {
      ...this.state,
      version: this.state.version + 1,
      builtAtMs: Date.now(),
      ready: this.state.ready,
      sequence: event.sequence,
      lastEventId: event.eventId,
      lastEventAtMs: event.occurredAtMs,
      campaignsById,
      campaignIdsByTag: this.buildTagIndex(campaignsById),
      campaignVersions,
    };
    return 'applied';
  }

  setCheckpoint(checkpoint: CampaignServingEventCheckpoint): void {
    this.state = {
      ...this.state,
      ready: true,
      sequence: checkpoint.sequence,
      lastEventId: checkpoint.eventId,
    };
  }

  async reloadFromSource(
    checkpoint: CampaignServingEventCheckpoint
  ): Promise<void> {
    this.markNotReady();
    const source = this.projectionBootstrapEnabled
      ? await this.projectionRepository.loadSnapshot()
      : {
          campaigns: await this.campaignCacheRepository.getAllCampaigns({
            allowStale: false,
          }),
          checkpoint,
          campaignVersions: new Map<string, number>(),
          complete: true,
        };
    const campaigns = source.campaigns;
    const campaignsById = new Map(
      campaigns.map((campaign) => {
        const servingCampaign = this.toServingCampaign(campaign);
        return [servingCampaign.id, servingCampaign] as const;
      })
    );
    for (const [campaignId, mutation] of this.mutationsDuringInitialization) {
      if (mutation) {
        campaignsById.set(campaignId, mutation);
      } else {
        campaignsById.delete(campaignId);
      }
    }
    this.state = {
      version: this.state.version + 1,
      builtAtMs: Date.now(),
      ready: source.complete,
      sequence: source.checkpoint.sequence,
      lastEventId: source.checkpoint.eventId,
      lastEventAtMs: this.state.lastEventAtMs,
      campaignsById,
      campaignIdsByTag: this.buildTagIndex(campaignsById),
      campaignVersions: source.campaignVersions,
    };
    this.initialized = true;
    this.mutationsDuringInitialization.clear();
    this.logger.log(
      `RTB 캠페인 스냅샷 재구축 완료: ${campaignsById.size}개, sequence=${source.checkpoint.sequence}`
    );
  }

  async refreshFromCurrentSource(): Promise<void> {
    if (!this.enabled) {
      return;
    }
    await this.reloadFromSource({
      eventId: this.state.lastEventId,
      sequence: this.state.sequence,
    });
  }

  markNotReady(): void {
    this.state = { ...this.state, ready: false };
  }

  private async ensureInitialized(): Promise<void> {
    if (this.initialized) {
      return;
    }

    if (!this.initializationInFlight) {
      this.initializationInFlight = this.buildInitialSnapshot().finally(() => {
        this.initializationInFlight = null;
      });
    }

    await this.initializationInFlight;
  }

  private async buildInitialSnapshot(): Promise<void> {
    const source = this.projectionBootstrapEnabled
      ? await this.projectionRepository.loadSnapshot()
      : {
          campaigns: await this.campaignCacheRepository.getAllCampaigns({
            allowStale: false,
          }),
          checkpoint: {
            eventId: this.state.lastEventId,
            sequence: this.state.sequence,
          },
          campaignVersions: this.state.campaignVersions,
          complete: true,
        };
    const campaigns = source.campaigns;
    const campaignsById = new Map(
      campaigns.map((campaign) => {
        const servingCampaign = this.toServingCampaign(campaign);
        return [servingCampaign.id, servingCampaign] as const;
      })
    );

    for (const [campaignId, mutation] of this.mutationsDuringInitialization) {
      if (mutation) {
        campaignsById.set(campaignId, mutation);
      } else {
        campaignsById.delete(campaignId);
      }
    }

    this.state = {
      version: this.state.version + 1,
      builtAtMs: Date.now(),
      ready: source.complete,
      sequence: source.checkpoint.sequence,
      lastEventId: source.checkpoint.eventId,
      lastEventAtMs: this.state.lastEventAtMs,
      campaignsById,
      campaignIdsByTag: this.buildTagIndex(campaignsById),
      campaignVersions: source.campaignVersions,
    };
    this.initialized = true;
    this.mutationsDuringInitialization.clear();
    this.logger.log(`RTB 캠페인 스냅샷 준비 완료: ${campaignsById.size}개`);
  }

  private recordMutation(campaignId: string, mutation: SnapshotMutation): void {
    if (!this.initialized) {
      this.mutationsDuringInitialization.set(campaignId, mutation);
    }
  }

  private upsertMany(campaigns: ServingCampaign[]): void {
    if (campaigns.length === 0) {
      return;
    }

    const campaignsById = new Map(this.state.campaignsById);
    for (const campaign of campaigns) {
      campaignsById.set(campaign.id, campaign);
    }
    this.state = {
      ...this.state,
      version: this.state.version + 1,
      builtAtMs: Date.now(),
      campaignsById,
      campaignIdsByTag: this.buildTagIndex(campaignsById),
      campaignVersions: this.state.campaignVersions,
    };
  }

  private remove(campaignId: string): void {
    if (!this.state.campaignsById.has(campaignId)) {
      return;
    }

    const campaignsById = new Map(this.state.campaignsById);
    campaignsById.delete(campaignId);
    this.state = {
      ...this.state,
      version: this.state.version + 1,
      builtAtMs: Date.now(),
      campaignsById,
      campaignIdsByTag: this.buildTagIndex(campaignsById),
      campaignVersions: this.state.campaignVersions,
    };
  }

  private hasRequiredEmbeddings(campaign: ServingCampaign): boolean {
    const hasTags = Boolean(
      campaign.tags?.length &&
      campaign.tags.every(
        (tagName) =>
          campaign.embeddingTags?.[tagName]?.length ===
          this.embeddingProfile.dimension
      )
    );
    const compatible =
      campaign.embeddingModelVersion === this.embeddingProfile.modelVersion ||
      (this.embeddingProfile.name === 'legacy_minilm' &&
        !campaign.embeddingModelVersion);
    const hasDocument =
      campaign.embeddingDocument?.length === this.embeddingProfile.dimension;
    return Boolean(
      compatible && hasTags && (!this.requireDocumentEmbedding || hasDocument)
    );
  }

  private toServingCampaign(campaign: CachedCampaign): ServingCampaign {
    const embeddingTags = campaign.embeddingTags
      ? Object.fromEntries(
          Object.entries(campaign.embeddingTags).map(([tagName, vector]) => [
            tagName,
            new Float32Array(vector),
          ])
        )
      : undefined;
    const embeddingDocument = campaign.embeddingDocument
      ? new Float32Array(campaign.embeddingDocument)
      : undefined;

    return {
      ...campaign,
      embeddingTags,
      embeddingDocument,
    };
  }

  private buildTagIndex(
    campaignsById: ReadonlyMap<string, ServingCampaign>
  ): ReadonlyMap<string, ReadonlySet<string>> {
    const index = new Map<string, Set<string>>();
    for (const campaign of campaignsById.values()) {
      for (const tag of this.normalizeTags(campaign.tags ?? [])) {
        const campaignIds = index.get(tag) ?? new Set<string>();
        campaignIds.add(campaign.id);
        index.set(tag, campaignIds);
      }
    }
    return index;
  }

  private normalizeTags(tags: string[]): string[] {
    return [
      ...new Set(
        tags
          .map((tag) =>
            tag.normalize('NFC').toLowerCase().replace(/\s+/g, ' ').trim()
          )
          .filter(Boolean)
      ),
    ];
  }
}
