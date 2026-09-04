import {
  Inject,
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnApplicationShutdown,
  Optional,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { OnEvent } from '@nestjs/event-emitter';
import { CampaignCacheRepository } from './repository/campaign.cache.repository.interface';
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
import { toServingCampaign, type ServingCampaign } from './serving-campaign';
import { SEARCH_REDIS_CLIENT } from '../redis/redis.constant';
import type { AppIORedisClient } from '../redis/redis.type';
import type { SearchSnapshotEvent } from './projection/campaign-projection.types';
import { CampaignSearchRepository } from './repository/campaign-search.repository.interface';

export type { ServingCampaign } from './serving-campaign';

type SnapshotMutation =
  | {
      type: 'UPSERT';
      campaignId: string;
      servingVersion: number;
      campaign: ServingCampaign;
    }
  | {
      type: 'DELETE';
      campaignId: string;
      servingVersion: number;
    };

type CampaignServingSnapshotState = {
  version: number;
  builtAtMs: number;
  campaignsById: ReadonlyMap<string, ServingCampaign>;
  campaignIdsByTag: ReadonlyMap<string, ReadonlySet<string>>;
};

@Injectable()
export class CampaignServingSnapshotService
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(CampaignServingSnapshotService.name);
  private state: CampaignServingSnapshotState = {
    version: 0,
    builtAtMs: 0,
    campaignsById: new Map(),
    campaignIdsByTag: new Map(),
  };
  private initialized = false;
  private initializationInFlight: Promise<void> | null = null;
  private readonly mutationsDuringInitialization = new Map<
    string,
    SnapshotMutation
  >();
  private readonly enabled: boolean;
  private readonly embeddingProfile: EmbeddingProfile;
  private readonly requireDocumentEmbedding: boolean;
  private readonly streamEnabled: boolean;
  private readonly sourceRedis?: AppIORedisClient;
  private streamRedis?: AppIORedisClient;
  private streamLoop: Promise<void> | null = null;
  private streamCursor = '0-0';
  private stopping = false;
  private rebuilding = false;
  private readonly streamKey = 'campaign:projection:stream';
  private readonly searchRepository?: CampaignSearchRepository;

  constructor(
    private readonly campaignCacheRepository: CampaignCacheRepository,
    configService: ConfigService,
    @Optional()
    @Inject(SEARCH_REDIS_CLIENT)
    searchRedis?: AppIORedisClient,
    @Optional() searchRepository?: CampaignSearchRepository
  ) {
    this.searchRepository = searchRepository;
    this.sourceRedis = searchRedis;
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
    this.streamEnabled =
      this.enabled &&
      configService.get<string>('CAMPAIGN_PROJECTION_MODE', 'off') ===
        'active' &&
      Boolean(searchRedis);
  }

  async onApplicationBootstrap(): Promise<void> {
    if (this.enabled) {
      await this.ensureInitialized();
      if (this.streamEnabled && this.sourceRedis) {
        this.streamRedis = this.sourceRedis.duplicate();
        this.streamLoop = this.consumeStream();
      }
    }
  }

  async onApplicationShutdown(): Promise<void> {
    this.stopping = true;
    this.streamRedis?.disconnect();
    await this.streamLoop;
  }

  async findCampaignsByIds(ids: string[]): Promise<ServingCampaign[]> {
    const uniqueIds = [...new Set(ids.filter(Boolean))];
    if (uniqueIds.length === 0) {
      return [];
    }

    await this.ensureInitialized();

    // 로컬 스냅샷에 캠페인이 없거나, 임베딩이 불완전한 캠페인들의 ID 찾기
    const repairIds = uniqueIds.filter((id) => {
      const campaign = this.state.campaignsById.get(id);
      return !campaign || !this.hasRequiredEmbeddings(campaign);
    });

    if (repairIds.length > 0) {
      const repaired = this.searchRepository
        ? await this.searchRepository.findCampaignsByIds(repairIds)
        : await this.campaignCacheRepository.findCampaignCachesByIds(repairIds);
      for (const campaign of repaired) {
        const servingCampaign = toServingCampaign(campaign);
        if (
          campaign.indexReady !== false &&
          this.hasRequiredEmbeddings(servingCampaign)
        ) {
          this.upsertMany([servingCampaign]);
        } else {
          this.remove(servingCampaign.id, servingCampaign.servingVersion);
        }
      }
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
  } {
    return {
      version: this.state.version,
      builtAtMs: this.state.builtAtMs,
      size: this.state.campaignsById.size,
      tagCount: this.state.campaignIdsByTag.size,
    };
  }

  @OnEvent(CAMPAIGN_CACHE_UPSERTED_EVENT)
  onCampaignCacheUpserted(event: CampaignCacheUpsertedEvent): void {
    if (!this.enabled) {
      return;
    }
    const campaign = toServingCampaign(event.campaign);
    const ready =
      campaign.indexReady !== false && this.hasRequiredEmbeddings(campaign);
    if (ready) {
      this.recordMutation({
        type: 'UPSERT',
        campaignId: campaign.id,
        servingVersion: campaign.servingVersion,
        campaign,
      });
      if (this.initialized) this.upsertMany([campaign]);
    } else {
      this.recordMutation({
        type: 'DELETE',
        campaignId: campaign.id,
        servingVersion: campaign.servingVersion,
      });
      if (this.initialized) this.remove(campaign.id, campaign.servingVersion);
    }
  }

  @OnEvent(CAMPAIGN_CACHE_REMOVED_EVENT)
  onCampaignCacheRemoved(event: CampaignCacheRemovedEvent): void {
    if (!this.enabled) {
      return;
    }
    const servingVersion = event.servingVersion ?? Number.MAX_SAFE_INTEGER;
    this.recordMutation({
      type: 'DELETE',
      campaignId: event.campaignId,
      servingVersion,
    });
    if (this.initialized) {
      this.remove(event.campaignId, servingVersion);
    }
  }

  private async ensureInitialized(): Promise<void> {
    if (this.initializationInFlight) {
      await this.initializationInFlight;
      return;
    }
    if (this.initialized) {
      return;
    }

    this.initializationInFlight = this.rebuildSnapshot().finally(() => {
      this.initializationInFlight = null;
    });

    await this.initializationInFlight;
  }

  private async buildInitialSnapshot(): Promise<void> {
    const bootstrapHead = this.streamEnabled
      ? await this.readStreamLastId()
      : '0-0';
    const campaigns = this.searchRepository
      ? await this.searchRepository.getAllSearchCampaigns()
      : await this.campaignCacheRepository.getAllCampaigns({
          allowStale: false,
        });
    const campaignsById = new Map(
      campaigns
        .filter(
          (campaign) =>
            campaign.indexReady !== false &&
            this.hasRequiredEmbeddings(toServingCampaign(campaign))
        )
        .map((campaign) => {
          const servingCampaign = toServingCampaign(campaign);
          return [servingCampaign.id, servingCampaign] as const;
        })
    );

    this.applyRecordedMutations(campaignsById);
    let nextCursor = bootstrapHead;
    if (this.streamEnabled) {
      nextCursor = await this.replayStreamIntoSnapshot(
        bootstrapHead,
        campaignsById
      );
    }
    // EventEmitter mutation can arrive while the Stream replay awaits Redis.
    // Reapply the latest per-campaign mutation with a version guard before the
    // single COW reference swap.
    this.applyRecordedMutations(campaignsById);
    this.state = {
      version: this.state.version + 1,
      builtAtMs: Date.now(),
      campaignsById,
      campaignIdsByTag: this.buildTagIndex(campaignsById),
    };
    this.streamCursor = nextCursor;
    this.initialized = true;
    this.mutationsDuringInitialization.clear();
    this.logger.log(`RTB 캠페인 스냅샷 준비 완료: ${campaignsById.size}개`);
  }

  private async consumeStream(): Promise<void> {
    while (!this.stopping && this.streamRedis) {
      try {
        if (await this.hasStreamGap(this.streamCursor)) {
          this.logger.warn(
            'Search Stream gap 감지: 전체 스냅샷을 재구축합니다.'
          );
          await this.rebuildSnapshot();
          continue;
        }
        const response = await this.streamRedis.call(
          'XREAD',
          'BLOCK',
          '5000',
          'COUNT',
          '256',
          'STREAMS',
          this.streamKey,
          this.streamCursor
        );
        await this.applyStreamResponse(response);
      } catch (error) {
        if (this.stopping) break;
        this.logger.warn(`Search Stream read 실패: ${String(error)}`);
        await new Promise((resolve) => setTimeout(resolve, 1_000));
      }
    }
  }

  private async replayStreamIntoSnapshot(
    cursor: string,
    campaignsById: Map<string, ServingCampaign>
  ): Promise<string> {
    if (!this.sourceRedis) return cursor;
    let nextCursor = cursor;
    while (true) {
      const entries = (await this.sourceRedis.xrange(
        this.streamKey,
        `(${nextCursor}`,
        '+',
        'COUNT',
        1_000
      )) as Array<[string, string[]]>;
      if (entries.length === 0) break;
      for (const [id, fields] of entries) {
        const event = this.parseStreamEvent(fields);
        if (event) {
          await this.applyStreamEventToMap(event, campaignsById);
        }
        nextCursor = id;
      }
      if (entries.length < 1_000) break;
    }
    return nextCursor;
  }

  private async applyStreamResponse(response: unknown): Promise<void> {
    if (!Array.isArray(response)) return;
    for (const stream of response) {
      if (!Array.isArray(stream) || !Array.isArray(stream[1])) continue;
      for (const entry of stream[1] as Array<[string, string[]]>) {
        await this.applyStreamEntry(entry[0], entry[1]);
      }
    }
  }

  private async applyStreamEntry(id: string, fields: string[]): Promise<void> {
    const event = this.parseStreamEvent(fields);
    if (!event) {
      this.streamCursor = id;
      return;
    }
    const campaignsById = new Map(this.state.campaignsById);
    if (await this.applyStreamEventToMap(event, campaignsById)) {
      this.replaceState(campaignsById);
    }
    this.streamCursor = id;
  }

  private async applyStreamEventToMap(
    event: SearchSnapshotEvent,
    campaignsById: Map<string, ServingCampaign>
  ): Promise<boolean> {
    const current = campaignsById.get(event.campaignId);
    if (current && current.servingVersion > event.servingVersion) return false;
    if (
      current &&
      event.type === 'UPSERT' &&
      current.servingVersion === event.servingVersion
    ) {
      // A campaign present in the serving snapshot is already index-ready.
      return false;
    }
    if (event.type === 'DELETE' || !event.indexReady) {
      return campaignsById.delete(event.campaignId);
    }

    const campaigns = this.searchRepository
      ? await this.searchRepository.findCampaignsByIds([event.campaignId])
      : await this.campaignCacheRepository.findCampaignCachesByIds([
          event.campaignId,
        ]);
    const campaign = campaigns[0];
    if (
      !campaign ||
      campaign.servingVersion !== event.servingVersion ||
      campaign.indexReady === false
    ) {
      return false;
    }
    const servingCampaign = toServingCampaign(campaign);
    if (!this.hasRequiredEmbeddings(servingCampaign)) return false;
    campaignsById.set(servingCampaign.id, servingCampaign);
    return true;
  }

  private parseStreamEvent(fields: string[]): SearchSnapshotEvent | null {
    const values = new Map<string, string>();
    for (let index = 0; index < fields.length; index += 2) {
      values.set(fields[index], fields[index + 1]);
    }
    const type = values.get('type');
    const campaignId = values.get('campaignId');
    const servingVersion = Number(values.get('servingVersion'));
    if (
      (type !== 'UPSERT' && type !== 'DELETE') ||
      !campaignId ||
      !Number.isFinite(servingVersion)
    ) {
      return null;
    }
    return {
      type,
      campaignId,
      servingVersion,
      indexReady: values.get('indexReady') === '1',
    };
  }

  private async readStreamLastId(): Promise<string> {
    if (!this.sourceRedis) return '0-0';
    const entries = await this.sourceRedis.xrevrange(
      this.streamKey,
      '+',
      '-',
      'COUNT',
      1
    );
    return entries[0]?.[0] ?? '0-0';
  }

  private async hasStreamGap(cursor: string): Promise<boolean> {
    if (!this.sourceRedis || cursor === '0-0') return false;
    const [first, last] = await Promise.all([
      this.sourceRedis.xrange(this.streamKey, '-', '+', 'COUNT', 1),
      this.sourceRedis.xrevrange(this.streamKey, '+', '-', 'COUNT', 1),
    ]);
    const firstId = first[0]?.[0];
    const lastId = last[0]?.[0];
    if (!firstId || !lastId) return true;
    return (
      this.compareStreamIds(firstId, cursor) > 0 ||
      this.compareStreamIds(lastId, cursor) < 0
    );
  }

  private compareStreamIds(left: string, right: string): number {
    const [leftMs, leftSeq] = left.split('-').map((part) => BigInt(part));
    const [rightMs, rightSeq] = right.split('-').map((part) => BigInt(part));
    if (leftMs !== rightMs) return leftMs > rightMs ? 1 : -1;
    if (leftSeq === rightSeq) return 0;
    return leftSeq > rightSeq ? 1 : -1;
  }

  private recordMutation(mutation: SnapshotMutation): void {
    if (!this.initialized || this.rebuilding) {
      const current = this.mutationsDuringInitialization.get(
        mutation.campaignId
      );
      if (!current || current.servingVersion <= mutation.servingVersion) {
        this.mutationsDuringInitialization.set(mutation.campaignId, mutation);
      }
    }
  }

  private applyRecordedMutations(
    campaignsById: Map<string, ServingCampaign>
  ): void {
    for (const mutation of this.mutationsDuringInitialization.values()) {
      const current = campaignsById.get(mutation.campaignId);
      if (current && current.servingVersion > mutation.servingVersion) continue;
      if (mutation.type === 'UPSERT') {
        campaignsById.set(mutation.campaignId, mutation.campaign);
      } else {
        campaignsById.delete(mutation.campaignId);
      }
    }
  }

  private async rebuildSnapshot(): Promise<void> {
    const previousState = this.state;
    const previousCursor = this.streamCursor;
    const wasInitialized = this.initialized;
    this.rebuilding = true;
    try {
      await this.buildInitialSnapshot();
    } catch (error) {
      this.streamCursor = previousCursor;
      this.initialized = wasInitialized;
      if (wasInitialized) {
        const campaignsById = new Map(previousState.campaignsById);
        this.applyRecordedMutations(campaignsById);
        this.state = {
          version:
            previousState.version +
            (this.mutationsDuringInitialization.size > 0 ? 1 : 0),
          builtAtMs:
            this.mutationsDuringInitialization.size > 0
              ? Date.now()
              : previousState.builtAtMs,
          campaignsById,
          campaignIdsByTag: this.buildTagIndex(campaignsById),
        };
        this.mutationsDuringInitialization.clear();
      } else {
        this.state = previousState;
      }
      throw error;
    } finally {
      this.rebuilding = false;
    }
  }

  private upsertMany(campaigns: ServingCampaign[]): void {
    if (campaigns.length === 0) {
      return;
    }

    const campaignsById = new Map(this.state.campaignsById);
    for (const campaign of campaigns) {
      const current = campaignsById.get(campaign.id);
      if (current && current.servingVersion > campaign.servingVersion) {
        continue;
      }
      campaignsById.set(campaign.id, campaign);
    }
    this.replaceState(campaignsById);
  }

  private remove(
    campaignId: string,
    servingVersion = Number.MAX_SAFE_INTEGER
  ): void {
    const current = this.state.campaignsById.get(campaignId);
    if (!current || current.servingVersion > servingVersion) {
      return;
    }

    const campaignsById = new Map(this.state.campaignsById);
    campaignsById.delete(campaignId);
    this.replaceState(campaignsById);
  }

  private replaceState(campaignsById: Map<string, ServingCampaign>): void {
    this.state = {
      version: this.state.version + 1,
      builtAtMs: Date.now(),
      campaignsById,
      campaignIdsByTag: this.buildTagIndex(campaignsById),
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
