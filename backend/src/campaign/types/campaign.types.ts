export type CampaignStatus = 'PENDING' | 'ACTIVE' | 'PAUSED' | 'ENDED';

export type Campaign = {
  id: string;
  userId: number;
  servingVersion: number;
  title: string;
  content: string;
  image: string;
  url: string;
  maxCpc: number;
  dailyBudget: number;
  totalBudget: number | null;
  dailySpent: number;
  totalSpent: number;
  lastResetDate: Date;
  isHighIntent: boolean;
  status: CampaignStatus;
  startDate: Date;
  endDate: Date;
  createdAt: Date;
  deletedAt: Date | null;
};

export type Tag = {
  id: number;
  name: string;
};

export type CampaignWithTags = Campaign & {
  tags: Tag[];
};

export type CampaignWithStats = CampaignWithTags & {
  impressions: number;
  clicks: number;
  ctr: number;
  dailySpentPercent: number;
  totalSpentPercent: number;
};

export type CampaignTag = {
  campaignId: string;
  tagId: number;
};

export type CachedCampaign = {
  id: string;
  userId: number;
  servingVersion: number;
  semanticHash?: string;
  indexReady?: boolean;
  title: string;
  content: string;
  image: string | null;
  url: string;
  maxCpc: number;
  dailyBudget: number;
  totalBudget: number | null;
  dailySpent: number;
  totalSpent: number;
  // Decision에서 선점했지만 아직 Click으로 확정되지 않은 금액.
  // 기존 캐시 문서와의 호환을 위해 optional이며 Redis Lua가 누락 값을 0으로 초기화한다.
  dailyReserved?: number;
  totalReserved?: number;
  dailyReservedDate?: string;
  lastResetDate: string;
  isHighIntent: boolean;
  status: CampaignStatus;
  startDate: string;
  endDate: string;
  createdAt: string;
  deletedAt: string | null;

  // 태그 정보 (매칭용)
  tags?: string[];

  // 서로 다른 모델의 384차원 벡터를 혼용하지 않기 위한 namespace
  embeddingModelVersion?: string;

  // title + content + tags를 하나의 passage로 표현한 캠페인 semantic vector
  embeddingDocument?: number[];
};

/**
 * Search Redis에 저장하는 campaign projection. 입찰 순위에 필요한 maxCpc는
 * 포함하지만 예산 한도·spent·reservation 상태는 Budget Redis에만 둔다.
 */
export type SearchCampaign = {
  id: string;
  userId: number;
  servingVersion: number;
  semanticHash: string;
  indexReady: boolean;
  title: string;
  content: string;
  image: string | null;
  url: string;
  maxCpc: number;
  isHighIntent: boolean;
  status: CampaignStatus;
  startDate: string;
  endDate: string;
  createdAt: string;
  deletedAt: string | null;
  tags: string[];
  embeddingModelVersion?: string;
  embeddingDocument?: number[];
};

export type CampaignEmbeddingPayload = {
  modelVersion: string;
  document: number[];
};

export type CachedCampaignWithoutSpent = Omit<
  CachedCampaign,
  | 'dailySpent'
  | 'totalSpent'
  | 'dailyReserved'
  | 'totalReserved'
  | 'dailyReservedDate'
>;

export type CampaignDocumentVectorSearchOptions = {
  queryEmbedding: number[];
  topL: number;
  isHighIntent: boolean;
  nowTs: number;
};

export type CampaignDocumentVectorSearchHit = {
  campaignId: string;
  servingVersion: number;
  distance: number;
  similarity: number;
};

export type BudgetReservationCandidate = {
  campaignId: string;
  servingVersion: number;
  // legacy topology의 RedisJSON 예약 Lua에만 전달한다. split topology에서는
  // Budget Redis의 maxCpc가 권위값이며 이 값은 Lua 인자로 전달하지 않는다.
  cpc?: number;
};

export type AuctionReservationStatus = 'RESERVED' | 'COMMITTED' | 'RELEASED';

type AuctionReservationBase = {
  auctionId: string;
  campaignId: string;
  blogId: number;
  cost: number;
  status: 'RESERVED';
  budgetDate: string;
  createdAt: number;
  updatedAt: number;
  expiresAt: number;
};

export type LegacyActiveAuctionReservation = AuctionReservationBase & {
  version: 1;
};

export type VersionedActiveAuctionReservation = AuctionReservationBase & {
  version: 2;
  campaignServingVersion: number;
};

export type ActiveAuctionReservation =
  | LegacyActiveAuctionReservation
  | VersionedActiveAuctionReservation;

export type AuctionTerminalMarker = {
  version: 1 | 2;
  auctionId: string;
  status: Exclude<AuctionReservationStatus, 'RESERVED'>;
  updatedAt: number;
};

export type AuctionReservationRecord =
  | ActiveAuctionReservation
  | AuctionTerminalMarker;

export type ReserveAuctionRequest = {
  auctionId: string;
  blogId: number;
  budgetDate: string;
  expiresAt: number;
  candidates: BudgetReservationCandidate[];
};

export type ReserveAuctionResult = {
  outcome:
    | 'reserved'
    | 'existing'
    | 'conflict'
    | 'version_mismatch'
    | 'exhausted';
  reservation?: AuctionReservationRecord;
  attemptedCount: number;
  versionMismatchCount?: number;
};

export type AuctionTransitionResult = {
  outcome:
    | 'committed'
    | 'released'
    | 'already_committed'
    | 'already_released'
    | 'expired'
    | 'not_found'
    | 'invalid';
  reservation?: AuctionReservationRecord;
};
