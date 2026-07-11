import {
  Inject,
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { IOREDIS_CLIENT } from '../redis/redis.constant';
import type { AppIORedisClient } from '../redis/redis.type';
import { CampaignServingSnapshotService } from './campaign-serving-snapshot.service';
import { CampaignServingEventStore } from './events/campaign-serving-event.store';
import { MetricsService } from '../metrics/metrics.service';

@Injectable()
export class CampaignServingEventConsumer
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(CampaignServingEventConsumer.name);
  private readonly enabled: boolean;
  private readonly readCount: number;
  private readonly blockMs: number;
  private readonly retryMs: number;
  private running = false;
  private reader: AppIORedisClient | null = null;

  constructor(
    private readonly eventStore: CampaignServingEventStore,
    private readonly snapshot: CampaignServingSnapshotService,
    @Inject(IOREDIS_CLIENT) private readonly redis: AppIORedisClient,
    configService: ConfigService,
    private readonly metricsService: MetricsService
  ) {
    this.enabled = this.eventStore.isEnabled() && this.snapshot.isEnabled();
    this.readCount = this.getPositiveInt(
      configService,
      'RTB_CAMPAIGN_EVENT_READ_COUNT',
      100
    );
    this.blockMs = this.getPositiveInt(
      configService,
      'RTB_CAMPAIGN_EVENT_BLOCK_MS',
      1000
    );
    this.retryMs = this.getPositiveInt(
      configService,
      'RTB_CAMPAIGN_EVENT_RETRY_MS',
      1000
    );
  }

  async onApplicationBootstrap(): Promise<void> {
    if (!this.enabled) {
      return;
    }
    this.reader = this.redis.duplicate();
    await this.recoverFromHead('bootstrap');
    this.running = true;
    void this.consumeLoop();
  }

  onApplicationShutdown(): void {
    this.running = false;
    this.reader?.disconnect();
    this.reader = null;
  }

  async recoverFromHead(reason: string): Promise<void> {
    this.snapshot.markNotReady();
    const checkpoint = await this.eventStore.getCheckpoint();
    await this.snapshot.reloadFromSource(checkpoint);
    this.metricsService.recordCampaignSnapshotRecovery(reason);
    this.updateMetrics();
    this.logger.log(
      `campaign serving sync 복구: reason=${reason}, sequence=${checkpoint.sequence}, eventId=${checkpoint.eventId}`
    );
  }

  async consumeOnce(): Promise<'idle' | 'applied' | 'recovered'> {
    if (!this.enabled || !this.reader) {
      return 'idle';
    }
    const metadata = this.snapshot.getMetadata();
    const events = await this.eventStore.readAfter(
      metadata.lastEventId,
      { count: this.readCount, blockMs: this.blockMs },
      this.reader
    );
    if (events.length === 0) {
      this.updateMetrics();
      return 'idle';
    }
    for (const event of events) {
      const outcome = this.snapshot.applyServingEvent(event);
      if (outcome === 'gap' || outcome === 'schema_mismatch') {
        await this.recoverFromHead(outcome);
        return 'recovered';
      }
    }
    this.updateMetrics();
    return 'applied';
  }

  private async consumeLoop(): Promise<void> {
    while (this.running) {
      try {
        await this.consumeOnce();
      } catch (error) {
        this.snapshot.markNotReady();
        this.updateMetrics();
        this.logger.error('campaign serving event consume 실패', error);
        await this.delay(this.retryMs);
        if (!this.running) {
          return;
        }
        try {
          await this.recoverFromHead('consume_error');
        } catch (recoveryError) {
          this.logger.error(
            'campaign serving snapshot 복구 실패',
            recoveryError
          );
        }
      }
    }
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private updateMetrics(): void {
    this.metricsService.setCampaignSnapshotState(this.snapshot.getMetadata());
  }

  private getPositiveInt(
    configService: ConfigService,
    key: string,
    fallback: number
  ): number {
    const raw = configService.get<string>(key);
    const value = raw ? Number.parseInt(raw, 10) : fallback;
    return Number.isFinite(value) && value > 0 ? value : fallback;
  }
}
