import type {
  AuctionReservationRecord,
  AuctionTransitionResult,
  ReserveAuctionRequest,
  ReserveAuctionResult,
} from '../types/campaign.types';
import type { CampaignProjectionDocument } from '../projection/campaign-projection.types';

export type CampaignBudgetState = {
  servingVersion: number;
  tombstone: boolean;
  status: string;
  maxCpc: number;
  dailyBudget: number;
  totalBudget: number | null;
  dailySpent: number;
  totalSpent: number;
  dailyReserved: number;
  totalReserved: number;
};

export abstract class CampaignBudgetRepository {
  abstract applyBudgetProjection(
    campaign: CampaignProjectionDocument
  ): Promise<boolean>;
  abstract applyBudgetTombstone(
    campaignId: string,
    servingVersion: number,
    fallback?: CampaignProjectionDocument
  ): Promise<boolean>;
  abstract getBudgetState(
    campaignId: string
  ): Promise<CampaignBudgetState | null>;
  abstract setOperationalStatus(
    campaignId: string,
    status: string
  ): Promise<boolean>;
  abstract replaceSpent(
    campaignId: string,
    dailySpent: number,
    totalSpent: number
  ): Promise<boolean>;
  abstract resetDailySpent(campaignId: string): Promise<boolean>;
  abstract resetForLoadTest(campaignId: string): Promise<boolean>;
  abstract incrementSpent(campaignId: string, cpc: number): Promise<boolean>;
  abstract decrementSpent(campaignId: string, cpc: number): Promise<void>;
  abstract reserveAuction(
    request: ReserveAuctionRequest
  ): Promise<ReserveAuctionResult>;
  abstract getAuctionReservation(
    auctionId: string
  ): Promise<AuctionReservationRecord | null>;
  abstract commitAuction(
    auctionId: string,
    currentBudgetDate: string,
    terminalTtlSeconds: number
  ): Promise<AuctionTransitionResult>;
  abstract releaseAuction(
    auctionId: string,
    terminalTtlSeconds: number
  ): Promise<AuctionTransitionResult>;
  abstract findExpiredAuctionIds(
    nowEpochMs: number,
    limit: number
  ): Promise<string[]>;
}
