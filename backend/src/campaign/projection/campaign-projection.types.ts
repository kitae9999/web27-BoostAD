import type { CampaignStatus } from '../types/campaign.types';

export enum CampaignProjectionEventType {
  UPSERT = 'UPSERT',
  DELETE = 'DELETE',
}

export enum CampaignProjectionOutboxState {
  PENDING = 'PENDING',
  PROCESSING = 'PROCESSING',
  WAITING = 'WAITING',
  RETRY = 'RETRY',
  COMPLETED = 'COMPLETED',
  DEAD = 'DEAD',
}

export type CampaignProjectionDocument = {
  id: string;
  userId: number;
  servingVersion: number;
  title: string;
  content: string;
  image: string | null;
  url: string;
  maxCpc: number;
  dailyBudget: number;
  totalBudget: number | null;
  dailySpent: number;
  totalSpent: number;
  lastResetDate: string;
  isHighIntent: boolean;
  status: CampaignStatus;
  startDate: string;
  endDate: string;
  createdAt: string;
  deletedAt: string | null;
  tags: string[];
  semanticHash: string;
};

export type CampaignProjectionPayload = {
  eventId: string;
  eventType: CampaignProjectionEventType;
  campaign: CampaignProjectionDocument;
};

export type SearchSnapshotEventType = 'UPSERT' | 'DELETE';

export type SearchSnapshotEvent = {
  type: SearchSnapshotEventType;
  campaignId: string;
  servingVersion: number;
  indexReady: boolean;
};
