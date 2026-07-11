import {
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';
import { MetricsService } from '../../metrics/metrics.service';

type ProjectionPipelineStatusRow = {
  projectionBacklog: string;
  projectionFailed: string;
  servingOutboxBacklog: string;
  servingOutboxFailed: string;
  oldestPendingAt: Date | string | null;
};

@Injectable()
export class CampaignProjectionObservabilityService
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(
    CampaignProjectionObservabilityService.name
  );
  private readonly enabled: boolean;
  private readonly intervalMs: number;
  private timer: NodeJS.Timeout | null = null;

  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly metrics: MetricsService,
    configService: ConfigService
  ) {
    this.enabled =
      configService.get<string>('RTB_PROJECTION_PIPELINE_ENABLED', 'false') ===
      'true';
    this.intervalMs = this.positiveInt(
      configService.get<string>('RTB_PROJECTION_METRICS_INTERVAL_MS'),
      1000
    );
  }

  async onApplicationBootstrap(): Promise<void> {
    if (!this.enabled) return;
    await this.refresh();
    this.timer = setInterval(() => {
      void this.refresh().catch((error: unknown) => {
        this.logger.warn('Campaign projection metric 갱신 실패', error);
      });
    }, this.intervalMs);
    this.timer.unref();
  }

  onApplicationShutdown(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  async refresh(): Promise<void> {
    const rows = await this.dataSource.query<ProjectionPipelineStatusRow[]>(`
      SELECT
        (SELECT COUNT(*) FROM CampaignProjectionRequestOutbox
         WHERE state IN ('PENDING', 'PROCESSING', 'FAILED')) AS projectionBacklog,
        (SELECT COUNT(*) FROM CampaignProjectionRequestOutbox
         WHERE state = 'FAILED') AS projectionFailed,
        (SELECT COUNT(*) FROM CampaignServingOutbox
         WHERE state <> 'PUBLISHED') AS servingOutboxBacklog,
        (SELECT COUNT(*) FROM CampaignServingOutbox
         WHERE state = 'FAILED') AS servingOutboxFailed,
        LEAST(
          COALESCE((SELECT MIN(created_at) FROM CampaignProjectionRequestOutbox
                    WHERE state IN ('PENDING', 'PROCESSING', 'FAILED')), NOW(3)),
          COALESCE((SELECT MIN(created_at) FROM CampaignServingOutbox
                    WHERE state <> 'PUBLISHED'), NOW(3))
        ) AS oldestPendingAt
    `);
    const row = rows[0];
    if (!row) return;
    const oldestPendingAt = row.oldestPendingAt
      ? new Date(row.oldestPendingAt).getTime()
      : Date.now();
    this.metrics.setCampaignProjectionPipelineState({
      projectionBacklog: Number(row.projectionBacklog),
      projectionFailed: Number(row.projectionFailed),
      servingOutboxBacklog: Number(row.servingOutboxBacklog),
      servingOutboxFailed: Number(row.servingOutboxFailed),
      oldestPendingAgeSeconds: Math.max(
        0,
        (Date.now() - oldestPendingAt) / 1000
      ),
    });
  }

  private positiveInt(raw: string | undefined, fallback: number): number {
    const value = raw ? Number.parseInt(raw, 10) : fallback;
    return Number.isFinite(value) && value > 0 ? value : fallback;
  }
}
