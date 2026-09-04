import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { CampaignCacheRepository } from '../../campaign/repository/campaign.cache.repository.interface';
import {
  CampaignServingSnapshotService,
  type ServingCampaign,
} from '../../campaign/campaign-serving-snapshot.service';
import { toServingCampaign } from '../../campaign/serving-campaign';
import { MLEngine } from '../ml/mlEngine.interface';
import { RequestEmbeddingCacheService } from '../ml/request-embedding-cache.service';
import type { EmbeddingPendingReason } from '../ml/request-embedding-cache.service';
import { ContextEmbeddingService } from '../context/context-embedding.service';
import type { DecisionContext, ScoredCandidate } from '../types/decision.types';
import { MetricsService } from '../../metrics/metrics.service';
import {
  createRtbPathLogger,
  rtbPathLogsEnabled,
} from '../../common/logging/rtb-path-logger.util';
import { Matcher, type QualityRetrievalMode } from './matcher.interface';
import {
  fuseHybridRankings,
  type RetrievalHit,
} from '../retrieval/hybrid-retrieval.fusion';
import { createCandidate } from '../candidate.factory';

type RetrievalMode = 'dense_only' | 'hybrid';

@Injectable()
export class TransformerMatcher extends Matcher {
  private readonly logger = createRtbPathLogger(TransformerMatcher.name);
  private readonly logsEnabled = rtbPathLogsEnabled();
  private readonly CPC_WEIGHT = 0.3;
  private readonly SIMILARITY_WEIGHT = 0.7;

  private readonly annTopL: number;
  private readonly annTopM: number;
  private readonly localSnapshotEnabled: boolean;
  private readonly coldMissFastPathEnabled: boolean;
  private readonly lexicalTopM: number;
  private readonly contextDecisionEnabled: boolean;
  private readonly documentSimilarityThreshold: number;
  private readonly retrievalMode: RetrievalMode;
  private readonly hybridRrfK: number;
  private readonly hybridDenseWeight: number;
  private readonly hybridSparseWeight: number;
  private readonly hybridSparseSupplementLimit: number;
  private readonly hybridFinalLimit: number;

  constructor(
    private readonly campaignCacheRepo: CampaignCacheRepository,
    private readonly campaignServingSnapshot: CampaignServingSnapshotService,
    private readonly mlEngine: MLEngine,
    private readonly requestEmbeddingCache: RequestEmbeddingCacheService,
    private readonly contextEmbeddingService: ContextEmbeddingService,
    private readonly metricsService: MetricsService,
    private readonly configService: ConfigService
  ) {
    super();
    this.annTopL = this.getPositiveIntEnv('RTB_MATCHER_ANN_TOP_L', 200);
    this.annTopM = this.getPositiveIntEnv('RTB_MATCHER_ANN_TOP_M', 30);
    const campaignSource = this.configService.get<string>(
      'RTB_CAMPAIGN_SOURCE'
    );
    this.localSnapshotEnabled = campaignSource
      ? campaignSource === 'local_snapshot'
      : this.configService.get<string>(
          'RTB_MATCHER_LOCAL_SNAPSHOT_ENABLED',
          'false'
        ) === 'true';
    this.coldMissFastPathEnabled =
      this.localSnapshotEnabled &&
      this.configService.get<string>(
        'RTB_EMBEDDING_COLD_MISS_FAST_PATH_ENABLED',
        'false'
      ) === 'true';
    this.lexicalTopM = this.getPositiveIntEnv('RTB_LEXICAL_TOP_M', 30);
    this.contextDecisionEnabled =
      this.localSnapshotEnabled &&
      this.configService.get<string>(
        'RTB_CONTEXT_DECISION_ENABLED',
        'false'
      ) === 'true';
    this.documentSimilarityThreshold = this.getNonNegativeFloatEnv(
      'RTB_MATCHER_DOCUMENT_SIMILARITY_THRESHOLD',
      0.3
    );
    const retrievalMode = this.configService.get<string>(
      'RTB_RETRIEVAL_MODE',
      'dense_only'
    );
    if (retrievalMode !== 'dense_only' && retrievalMode !== 'hybrid') {
      throw new Error(
        `지원하지 않는 RTB_RETRIEVAL_MODE입니다: ${retrievalMode}`
      );
    }
    this.retrievalMode = retrievalMode;
    this.hybridRrfK = this.getPositiveIntEnv('RTB_HYBRID_RRF_K', 60);
    this.hybridDenseWeight = this.getNonNegativeFloatEnv(
      'RTB_HYBRID_DENSE_WEIGHT',
      1
    );
    this.hybridSparseWeight = this.getNonNegativeFloatEnv(
      'RTB_HYBRID_SPARSE_WEIGHT',
      0.2
    );
    this.hybridSparseSupplementLimit = this.getPositiveIntEnv(
      'RTB_HYBRID_SPARSE_SUPPLEMENT_LIMIT',
      10
    );
    this.hybridFinalLimit = this.getPositiveIntEnv(
      'RTB_HYBRID_FINAL_LIMIT',
      10
    );
  }

  /**
   * 요청 컨텍스트에 맞는 캠페인을 검색·필터링·채점한다. 예산은 검증하지 않는다.
   * 캠페인 문서 HNSW 검색으로 후보를 좁히고 집행 조건을 재검증한다.
   *
   * 요청 임베딩 확보 우선순위:
   *  1) contextId가 있으면 사전 생성된 글 임베딩 사용
   *  2) cold-miss fast path에서는 태그 임베딩 캐시 hit만 사용
   *  3) 구 동기 경로에서는 캐시 miss 시 runtime 생성을 기다림
   */
  async matchCandidates(context: DecisionContext): Promise<ScoredCandidate[]> {
    // [1. 매칭 입력 준비]
    // 요청 태그를 문서 임베딩 입력용 정규화 문자열로 준비한다.
    const requestText = this.buildRequestText(context.tags);

    // [종료 분기 A: ML 모델 미준비]
    // fast path가 켜져 있으면 모델 없이 lexical 후보를 반환하고, 아니면 빈 결과를 반환한다.
    if (!this.mlEngine.isReady()) {
      if (this.coldMissFastPathEnabled) {
        return this.findCandidatesByLexicalFallback(context, 'model_not_ready');
      }

      this.metricsService.incRtbFallback('matcher_empty');
      if (this.logsEnabled) {
        this.logger.warn('ML 모델이 준비가 안 되었습니다.');
      }
      return [];
    }

    // [2. 요청 임베딩 확보]
    // context 임베딩을 우선 사용하고, 불가능할 때 설정에 따라 캐시 fast path 또는 동기 경로를 사용한다.
    let requestEmbedding: number[];
    const requestEmbeddingStartedAt = process.hrtime.bigint();

    try {
      // [분기 B-1: 사전 생성된 context 임베딩]
      // SDK observe 단계에서 발급한 contextId가 있으면 포스트 문맥 임베딩 상태를 조회한다.
      if (this.contextDecisionEnabled && context.contextId) {
        const contextResult =
          await this.contextEmbeddingService.resolveForDecision(
            context.contextId
          );

        this.metricsService.recordRtbContextDecision(contextResult.status);

        // context 임베딩을 기다리지 않고 PENDING/FAILED 상태는 즉시 lexical 경로로 전환한다.
        if (contextResult.status !== 'READY') {
          this.metricsService.recordRtbStage(
            'match_request_embedding',
            'fallback',
            this.elapsedMs(requestEmbeddingStartedAt)
          );

          return this.findCandidatesByLexicalFallback(
            context,
            `context_${contextResult.status.toLowerCase()}`
          );
        }

        requestEmbedding = contextResult.embedding;
        this.metricsService.incRtbEmbeddingSource('context');
      } else if (this.coldMissFastPathEnabled) {
        // [분기 B-2: deprecated cold-miss fast path]
        // 태그 임베딩 캐시 hit만 즉시 사용한다. miss면 생성을 예약하고 현재 요청은 lexical로 처리한다.
        const resolved =
          await this.requestEmbeddingCache.resolveCachedOrSchedule(requestText);

        if (resolved.status === 'pending') {
          this.metricsService.recordRtbStage(
            'match_request_embedding',
            'fallback',
            this.elapsedMs(requestEmbeddingStartedAt)
          );

          return this.findCandidatesByLexicalFallback(context, resolved.reason);
        }
        requestEmbedding = resolved.embedding;
      } else {
        // [분기 B-3: deprecated 동기 임베딩 경로]
        // 캐시 miss가 발생해도 runtime 임베딩 생성이 끝날 때까지 기다린다.
        requestEmbedding = await this.getEmbeddingCached(requestText);
      }

      // 세 임베딩 경로 중 하나가 정상적으로 벡터를 확보한 경우에만 ok로 기록한다.
      this.metricsService.recordRtbStage(
        'match_request_embedding',
        'ok',
        this.elapsedMs(requestEmbeddingStartedAt)
      );
    } catch (error) {
      // [오류 분기 B-4: 요청 임베딩 확보 실패]
      // fast path에서는 lexical로 복구하고, 동기 경로에서는 오류를 기록한 뒤 빈 결과를 반환한다.
      if (this.coldMissFastPathEnabled) {
        this.metricsService.recordRtbStage(
          'match_request_embedding',
          'fallback',
          this.elapsedMs(requestEmbeddingStartedAt)
        );
        return this.findCandidatesByLexicalFallback(context, 'cache_error');
      }
      this.metricsService.recordRtbStage(
        'match_request_embedding',
        'error',
        this.elapsedMs(requestEmbeddingStartedAt)
      );
      this.metricsService.incRtbFallback('embedding_error');
      if (this.logsEnabled) {
        this.logger.warn(
          '요청 태그 임베딩 생성에 실패했습니다.',
          error as Error
        );
      }
      return [];
    }

    // [3. 문서 ANN 검색]
    try {
      return await this.findCandidatesByDocumentAnn(context, requestEmbedding);
    } catch (error) {
      if (!this.localSnapshotEnabled) throw error;
      this.logger.warn(
        `Search Redis ANN 실패, 로컬 스냅샷 lexical 경로로 전환: ${String(error)}`
      );
      return this.findCandidatesByLexicalFallback(
        context,
        'search_unavailable'
      );
    }
  }

  /**
   * Transformer/ANN을 우회하는 태그 문자열 fast path.
   * local snapshot 역인덱스에서 태그 교집합 캠페인을 모아
   * exact match 수 → coverage → CPC 순으로 top-M을 고른다.
   */
  private async findCandidatesByLexicalFallback(
    context: DecisionContext,
    reason:
      | EmbeddingPendingReason
      | 'model_not_ready'
      | 'cache_error'
      | 'search_unavailable'
      | 'semantic_index_unready'
      | `context_${string}`
  ): Promise<ScoredCandidate[]> {
    const startedAt = process.hrtime.bigint();
    const requestTags = new Set(
      context.tags.map((tag) => this.normalizeText(tag)).filter(Boolean)
    );
    // Redis hydrate 없이 프로세스 로컬 역인덱스만 조회
    const indexedCampaigns =
      await this.campaignServingSnapshot.findCampaignsByTags([...requestTags]);
    const eligibleCampaigns = this.filterEligibleCampaigns(
      indexedCampaigns,
      context.isHighIntent
    );

    const candidates = eligibleCampaigns
      .map((campaign) => {
        const campaignTags = new Set(
          (campaign.tags ?? [])
            .map((tag) => this.normalizeText(tag))
            .filter(Boolean)
        );
        let exactMatchCount = 0;
        for (const tag of requestTags) {
          if (campaignTags.has(tag)) {
            exactMatchCount += 1;
          }
        }
        const coverage =
          requestTags.size === 0 ? 0 : exactMatchCount / requestTags.size;
        return {
          campaign,
          coverage,
          exactMatchCount,
        };
      })
      .filter((item) => item.exactMatchCount > 0)
      .sort((a, b) => {
        if (b.exactMatchCount !== a.exactMatchCount) {
          return b.exactMatchCount - a.exactMatchCount;
        }
        if (b.coverage !== a.coverage) {
          return b.coverage - a.coverage;
        }
        if (b.campaign.maxCpc !== a.campaign.maxCpc) {
          return b.campaign.maxCpc - a.campaign.maxCpc;
        }
        return a.campaign.id.localeCompare(b.campaign.id);
      })
      .slice(0, this.lexicalTopM)
      .map(({ campaign, coverage, exactMatchCount }) => ({
        ...this.buildCandidate(campaign, coverage),
        score: exactMatchCount * 100 + coverage * 10,
      }));

    this.metricsService.incRtbEmbeddingSource('fallback');
    this.metricsService.recordRtbLexicalFallback(reason, candidates.length);
    this.metricsService.incRtbFallback(`embedding_${reason}`);
    this.metricsService.recordRtbStage(
      'match_lexical_fallback',
      candidates.length > 0 ? 'ok' : 'fallback',
      this.elapsedMs(startedAt)
    );
    this.metricsService.observeRtbEligibleCampaignCount(
      eligibleCampaigns.length
    );

    if (candidates.length === 0) {
      this.metricsService.incRtbFallback('matcher_empty');
    }
    return candidates;
  }

  private getPositiveIntEnv(name: string, defaultValue: number): number {
    const raw = this.configService.get<string>(name);
    const parsed = raw ? Number.parseInt(raw, 10) : defaultValue;
    if (!Number.isFinite(parsed) || parsed <= 0) {
      return defaultValue;
    }
    return parsed;
  }

  private getNonNegativeFloatEnv(name: string, defaultValue: number): number {
    const raw = this.configService.get<string>(name);
    const parsed = raw === undefined ? defaultValue : Number.parseFloat(raw);
    if (!Number.isFinite(parsed) || parsed < 0) {
      return defaultValue;
    }
    return parsed;
  }

  /**
   * 데이터 소스와 관계없이 matcher에는 동일한 RTB 조회 모델만 전달한다.
   */
  private async findServingCampaignsByIds(
    ids: string[]
  ): Promise<ServingCampaign[]> {
    if (this.localSnapshotEnabled) {
      return this.campaignServingSnapshot.findCampaignsByIds(ids);
    }

    const cachedCampaigns =
      await this.campaignCacheRepo.findCampaignCachesByIds(ids);
    return cachedCampaigns.map(toServingCampaign);
  }

  /**
   * 문서 ANN Top-L 검색
   * → 유사도 임계값 적용
   * → Top-M 후보 축소
   * → 캠페인 데이터 hydrate
   * → 집행 조건 재검증
   * → 문서 유사도와 CPC로 점수 생성
   * → 필요하면 Hybrid 결과 결합
   * → 최종 후보 반환
   *
   * 문서 ANN 결과 자체가 없으면 lexical fallback으로 전환한다.
   */
  private async findCandidatesByDocumentAnn(
    context: DecisionContext,
    requestEmbedding: number[]
  ): Promise<ScoredCandidate[]> {
    // [1. Dense retrieval]
    // 요청 문서 임베딩과 가까운 캠페인 문서를 ANN 인덱스에서 넓게(top-L) 조회한다.
    const annSearchStartedAt = process.hrtime.bigint();
    const documentHits =
      await this.campaignCacheRepo.searchCampaignDocumentVectors({
        queryEmbedding: requestEmbedding,
        topL: this.annTopL,
        isHighIntent: context.isHighIntent,
        nowTs: Date.now(),
      });
    this.metricsService.recordRtbStage(
      'match_ann_document_search',
      'ok',
      this.elapsedMs(annSearchStartedAt)
    );

    // [종료 분기 A: semantic 인덱스 미준비]
    // 검색 결과 자체가 없으면 인덱스를 사용할 수 없는 상태로 보고 lexical 경로로 전환한다.
    if (documentHits.length === 0) {
      return this.findCandidatesByLexicalFallback(
        context,
        'semantic_index_unready'
      );
    }

    // [2. Dense 후보 축소]
    // 최소 유사도를 통과한 결과만 남긴 뒤 실제로 hydrate할 후보 수를 top-M으로 제한한다.
    const retainedHits = documentHits
      .filter((hit) => hit.similarity >= this.documentSimilarityThreshold)
      .slice(0, this.annTopM);
    this.metricsService.observeRtbAnnRetrievedCampaignCount(
      retainedHits.length
    );

    // [종료 분기 B: 유사도 통과 후보 없음]
    // 인덱스는 정상이지만 기준을 만족하는 캠페인이 없으므로 fallback 없이 빈 후보를 반환한다.
    if (retainedHits.length === 0) {
      this.metricsService.incRtbFallback('matcher_empty');
      return [];
    }

    // [3. 캠페인 hydrate]
    // ANN hit에는 입찰에 필요한 전체 정보가 없으므로 ID로 serving campaign을 조회한다.
    // 설정에 따라 프로세스 로컬 snapshot 또는 Redis JSON을 데이터 소스로 사용한다.
    const loadStartedAt = process.hrtime.bigint();
    const ids = retainedHits.map((hit) => hit.campaignId);
    const retrievedCampaigns = await this.findServingCampaignsByIds(ids);
    this.metricsService.recordRtbStage(
      this.localSnapshotEnabled
        ? 'match_campaign_hydrate_snapshot'
        : 'match_campaign_hydrate_redis',
      'ok',
      this.elapsedMs(loadStartedAt)
    );

    // [4. 집행 자격 재검증]
    // hydrate 사이의 상태 변경 가능성을 고려해 ACTIVE/기간/삭제/high-intent 조건을 다시 확인한다.
    const eligibleById = new Map(
      this.filterEligibleCampaigns(retrievedCampaigns, context.isHighIntent)
        .filter((campaign) => campaign.indexReady !== false)
        .map((campaign) => [campaign.id, campaign])
    );
    const currentHits = retainedHits.filter((hit) => {
      const campaign = eligibleById.get(hit.campaignId);
      return campaign && this.matchesHitVersion(hit, campaign);
    });

    // [5. 최종 후보 점수 생성]
    // 태그 기반 scoreCampaignByTags()를 거치지 않고 ANN이 반환한 문서 유사도를 그대로 사용한다.
    // buildCandidate()에서 CPC 30%와 문서 유사도 70%를 합산한다.
    const scoreStartedAt = process.hrtime.bigint();
    const candidates = currentHits.flatMap((hit) => {
      const campaign = eligibleById.get(hit.campaignId);
      return campaign ? [this.buildCandidate(campaign, hit.similarity)] : [];
    });
    this.metricsService.recordRtbStage(
      'match_document_rerank',
      'ok',
      this.elapsedMs(scoreStartedAt)
    );
    this.metricsService.observeRtbEligibleCampaignCount(eligibleById.size);

    // [종료 분기 C: hydrate/자격 검증 후 후보 없음]
    if (candidates.length === 0) {
      this.metricsService.incRtbFallback('matcher_empty');
      return [];
    }

    // [6. 결과 반환 방식 분기]
    // hybrid 모드는 dense 후보에 sparse 검색 결과를 결합하고, 결합 결과가 비면 dense 결과를 유지한다.
    if (this.retrievalMode === 'hybrid') {
      const hybrid = await this.buildHybridCandidates(
        context,
        requestEmbedding,
        currentHits,
        candidates
      );
      return hybrid.length > 0 ? hybrid : candidates;
    }

    // dense_only 모드는 문서 ANN 후보를 그대로 반환한다.
    return candidates;
  }

  async findQualityRankings(
    context: DecisionContext,
    mode: QualityRetrievalMode = 'dense_only'
  ): Promise<ScoredCandidate[]> {
    const requestText = this.buildRequestText(context.tags);
    const requestEmbedding = await this.resolveRequestEmbeddingForQuality(
      context,
      requestText
    );
    if (!requestEmbedding) {
      return [];
    }

    const documentHits =
      await this.campaignCacheRepo.searchCampaignDocumentVectors({
        queryEmbedding: requestEmbedding,
        topL: this.annTopL,
        isHighIntent: context.isHighIntent,
        nowTs: Date.now(),
      });
    const retainedHits = documentHits
      .filter((hit) => hit.similarity >= this.documentSimilarityThreshold)
      .slice(0, this.annTopM);
    const ids = retainedHits.map((hit) => hit.campaignId);
    const retrievedCampaigns = await this.findServingCampaignsByIds(ids);
    const eligibleById = new Map(
      this.filterEligibleCampaigns(retrievedCampaigns, context.isHighIntent)
        .filter((campaign) => campaign.indexReady !== false)
        .map((campaign) => [campaign.id, campaign])
    );
    const currentHits = retainedHits.filter((hit) => {
      const campaign = eligibleById.get(hit.campaignId);
      return campaign && this.matchesHitVersion(hit, campaign);
    });
    const primary = currentHits.flatMap((hit) => {
      const campaign = eligibleById.get(hit.campaignId);
      return campaign ? [this.buildCandidate(campaign, hit.similarity)] : [];
    });
    if (mode === 'dense_only') {
      return primary;
    }

    const hybrid = await this.buildHybridCandidates(
      context,
      requestEmbedding,
      currentHits,
      primary
    );
    return hybrid.length > 0 ? hybrid : primary;
  }

  private async resolveRequestEmbeddingForQuality(
    context: DecisionContext,
    requestText: string
  ): Promise<number[] | null> {
    if (this.contextDecisionEnabled && context.contextId) {
      const ready = await this.contextEmbeddingService.resolveForDecision(
        context.contextId
      );
      if (ready.status === 'READY' && ready.embedding?.length) {
        return ready.embedding;
      }
    }
    if (!this.mlEngine.isReady()) {
      return null;
    }
    return this.mlEngine.getEmbedding(requestText, 'query');
  }

  private async buildHybridCandidates(
    context: DecisionContext,
    requestEmbedding: number[],
    denseHits: Array<{ campaignId: string; similarity: number }>,
    primary: ScoredCandidate[]
  ): Promise<ScoredCandidate[]> {
    const sparseStartedAt = process.hrtime.bigint();
    const sparseRanked = await this.rankSparseTagCandidates(context);
    this.metricsService.observeRtbHybridSparseLookupDuration(
      this.elapsedMs(sparseStartedAt) / 1000
    );

    const denseRetrievalHits: RetrievalHit[] = denseHits.map((hit) => ({
      campaignId: hit.campaignId,
      rawScore: hit.similarity,
    }));
    const sparseRetrievalHits: RetrievalHit[] = sparseRanked.map(
      (candidate) => ({
        campaignId: candidate.id,
        rawScore: candidate.similarity,
      })
    );

    const fusionStartedAt = process.hrtime.bigint();
    const fused = fuseHybridRankings(denseRetrievalHits, sparseRetrievalHits, {
      rrfK: this.hybridRrfK,
      denseWeight: this.hybridDenseWeight,
      sparseWeight: this.hybridSparseWeight,
      limit: denseRetrievalHits.length + sparseRetrievalHits.length,
    });
    this.metricsService.observeRtbHybridFusionDuration(
      this.elapsedMs(fusionStartedAt) / 1000
    );
    this.metricsService.recordRtbStage(
      'match_hybrid_fusion',
      fused.length > 0 ? 'ok' : 'fallback',
      this.elapsedMs(fusionStartedAt)
    );

    if (fused.length === 0) {
      return [];
    }

    const denseCandidates = fused.filter((item) => item.dense);
    const sparseSupplements = fused
      .filter((item) => !item.dense && item.sparse)
      .slice(0, this.hybridSparseSupplementLimit);
    const rerankPool = [...denseCandidates, ...sparseSupplements];
    const rerankStartedAt = process.hrtime.bigint();
    const rerankIds = rerankPool.map((item) => item.campaignId);
    const hydratedPool = await this.findServingCampaignsByIds(rerankIds);
    const eligibleById = new Map(
      this.filterEligibleCampaigns(hydratedPool, context.isHighIntent).map(
        (campaign) => [campaign.id, campaign]
      )
    );

    const exactReranked = rerankPool.flatMap((item) => {
      const campaign = eligibleById.get(item.campaignId);
      const documentEmbedding = campaign?.embeddingDocument;
      if (!campaign || !documentEmbedding?.length) {
        return [];
      }
      const exactSimilarity = this.mlEngine.calculateSimilarity(
        requestEmbedding,
        documentEmbedding
      );
      if (exactSimilarity < this.documentSimilarityThreshold) {
        return [];
      }
      const candidate = this.buildCandidate(campaign, exactSimilarity);
      // Sparse는 semantic exact score를 뒤집는 주 신호가 아니라 근접 후보의
      // tie-break 보너스로만 사용한다. 기본값에서 RRF 보너스는 1점 미만이다.
      candidate.score += (item.sparse?.contribution ?? 0) * 100;
      return [{ candidate, denseBacked: Boolean(item.dense) }];
    });

    const bestDenseScore = exactReranked
      .filter((item) => item.denseBacked)
      .reduce(
        (best, item) => Math.max(best, item.candidate.score),
        Number.NEGATIVE_INFINITY
      );
    if (!Number.isFinite(bestDenseScore)) {
      return primary.slice(0, this.hybridFinalLimit);
    }

    // Sparse-only 후보는 Top-K를 보충할 수 있지만 winner는 Dense 후보가 맡는다.
    for (const item of exactReranked) {
      if (!item.denseBacked && item.candidate.score >= bestDenseScore) {
        item.candidate.score = bestDenseScore - 1e-9;
      }
    }

    // Sparse 보너스나 exact 재계산이 현재 Dense winner를 바꾸지 않도록 하고,
    // Hybrid 개선은 2~10위 reserve 후보에만 반영한다.
    const denseWinner = [...primary].sort((left, right) => {
      if (right.score !== left.score) return right.score - left.score;
      if (right.maxCpc !== left.maxCpc) return right.maxCpc - left.maxCpc;
      return left.id.localeCompare(right.id);
    })[0];
    const lockedWinner = exactReranked.find(
      (item) => item.candidate.id === denseWinner?.id
    );
    if (lockedWinner) {
      const bestOtherScore = exactReranked
        .filter((item) => item !== lockedWinner)
        .reduce(
          (best, item) => Math.max(best, item.candidate.score),
          Number.NEGATIVE_INFINITY
        );
      if (lockedWinner.candidate.score <= bestOtherScore) {
        lockedWinner.candidate.score = bestOtherScore + 1e-9;
      }
    }

    const result = exactReranked
      .map((item) => item.candidate)
      .sort((left, right) => {
        if (right.score !== left.score) return right.score - left.score;
        if (right.similarity !== left.similarity) {
          return right.similarity - left.similarity;
        }
        if (right.maxCpc !== left.maxCpc) return right.maxCpc - left.maxCpc;
        return left.id.localeCompare(right.id);
      })
      .slice(0, this.hybridFinalLimit);
    this.metricsService.recordRtbStage(
      'match_hybrid_exact_rerank',
      result.length > 0 ? 'ok' : 'fallback',
      this.elapsedMs(rerankStartedAt)
    );
    return result;
  }

  private async rankSparseTagCandidates(
    context: DecisionContext
  ): Promise<ScoredCandidate[]> {
    const requestTags = new Set(
      context.tags.map((tag) => this.normalizeText(tag)).filter(Boolean)
    );
    if (requestTags.size === 0 || !this.localSnapshotEnabled) {
      return [];
    }

    const taggedCampaigns =
      await this.campaignServingSnapshot.findCampaignsByTags([...requestTags]);
    const eligibleCampaigns = this.filterEligibleCampaigns(
      taggedCampaigns,
      context.isHighIntent
    );

    return eligibleCampaigns
      .map((campaign) => {
        const campaignTags = new Set(
          (campaign.tags ?? [])
            .map((tag) => this.normalizeText(tag))
            .filter(Boolean)
        );
        let exactMatchCount = 0;
        for (const tag of requestTags) {
          if (campaignTags.has(tag)) {
            exactMatchCount += 1;
          }
        }
        const coverage =
          requestTags.size === 0 ? 0 : exactMatchCount / requestTags.size;
        return {
          campaign,
          exactMatchCount,
          coverage,
        };
      })
      .filter((item) => item.exactMatchCount > 0)
      .sort((a, b) => {
        if (b.exactMatchCount !== a.exactMatchCount) {
          return b.exactMatchCount - a.exactMatchCount;
        }
        if (b.coverage !== a.coverage) {
          return b.coverage - a.coverage;
        }
        if (b.campaign.maxCpc !== a.campaign.maxCpc) {
          return b.campaign.maxCpc - a.campaign.maxCpc;
        }
        return a.campaign.id.localeCompare(b.campaign.id);
      })
      .slice(0, this.lexicalTopM)
      .map(({ campaign, coverage }) => this.buildCandidate(campaign, coverage));
  }

  private matchesHitVersion(
    hit: { servingVersion?: number },
    campaign: ServingCampaign
  ): boolean {
    return (
      !Number.isFinite(hit.servingVersion) ||
      hit.servingVersion === campaign.servingVersion
    );
  }

  private buildCandidate(
    campaign: ServingCampaign,
    similarity: number
  ): ScoredCandidate {
    const cpcScore = campaign.maxCpc * this.CPC_WEIGHT;
    const similarityScore = similarity * 100 * this.SIMILARITY_WEIGHT;

    return {
      ...createCandidate(campaign, similarity),
      score: cpcScore + similarityScore,
    };
  }

  // 요청 태그 배열을 임베딩을 위한 단일 텍스트로 변환합니다.
  private buildRequestText(tags: string[]): string {
    const canonicalTags = [
      ...new Set(tags.map((tag) => this.normalizeText(tag)).filter(Boolean)),
    ].sort();

    return canonicalTags.join(' ');
  }

  // 비딩 자격 필터링: ACTIVE + 날짜 범위 + deletedAt
  private filterEligibleCampaigns(
    campaigns: ServingCampaign[],
    isHighIntent: boolean
  ): ServingCampaign[] {
    const now = new Date();

    return campaigns.filter((campaign) => {
      // 삭제된 캠페인 제외
      if (campaign.deletedAt) {
        return false;
      }

      // ACTIVE 상태만 허용
      if (campaign.status !== 'ACTIVE') {
        return false;
      }

      // 날짜 범위 검증
      const startDate = new Date(campaign.startDate);
      const endDate = new Date(campaign.endDate);

      if (now < startDate || now >= endDate) {
        return false;
      }

      // isHighIntert가 일치하는지 여부
      if (isHighIntent !== campaign.isHighIntent) {
        return false;
      }

      return true;
    });
  }

  private normalizeText(text: string): string {
    return text.normalize('NFC').toLowerCase().replace(/\s+/g, ' ').trim();
  }

  private elapsedMs(startedAt: bigint): number {
    return Number(process.hrtime.bigint() - startedAt) / 1_000_000;
  }

  private async getEmbeddingCached(text: string): Promise<number[]> {
    const { embedding } = await this.requestEmbeddingCache.resolve(text);
    return embedding;
  }
}
