/* eslint-disable @typescript-eslint/unbound-method */
import type { ConfigService } from '@nestjs/config';
import type { DataSource } from 'typeorm';
import type { MetricsService } from '../../metrics/metrics.service';
import { CampaignProjectionObservabilityService } from './campaign-projection-observability.service';

describe('CampaignProjectionObservabilityService', () => {
  it('exports projection and Kafka outbox backlog from MySQL', async () => {
    const now = Date.now();
    const dataSource = {
      query: jest.fn().mockResolvedValue([
        {
          projectionBacklog: '4',
          projectionFailed: '1',
          servingOutboxBacklog: '2',
          servingOutboxFailed: '1',
          oldestPendingAt: new Date(now - 5000),
        },
      ]),
    } as unknown as DataSource;
    const metrics = {
      setCampaignProjectionPipelineState: jest.fn(),
    } as unknown as MetricsService;
    const service = new CampaignProjectionObservabilityService(
      dataSource,
      metrics,
      {
        get: jest.fn((key: string, fallback?: string) =>
          key === 'RTB_PROJECTION_PIPELINE_ENABLED' ? 'true' : fallback
        ),
      } as unknown as ConfigService
    );

    await service.refresh();

    expect(metrics.setCampaignProjectionPipelineState).toHaveBeenCalledWith(
      expect.objectContaining({
        projectionBacklog: 4,
        projectionFailed: 1,
        servingOutboxBacklog: 2,
        servingOutboxFailed: 1,
        oldestPendingAgeSeconds: expect.any(Number),
      })
    );
  });
});
