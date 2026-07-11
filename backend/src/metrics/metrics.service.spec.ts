import { MetricsService } from './metrics.service';

describe('MetricsService', () => {
  it('exports bidlog queue job counts into the registry before scrape', async () => {
    const queue = {
      name: 'bidlog-queue',
      getJobCounts: jest.fn().mockResolvedValue({
        waiting: 7,
        active: 2,
        delayed: 1,
        prioritized: 0,
        paused: 0,
        completed: 11,
        failed: 3,
      }),
    };

    const service = new MetricsService(queue as never);
    service.recordRtbAuctionTransition('reserve', 'reserved');
    service.recordRtbAuctionTransition('reserve', 'replayed');
    service.setCampaignSnapshotState({
      ready: true,
      sequence: 12,
      size: 1000,
      lastEventAtMs: Date.now(),
    });
    service.recordCampaignSnapshotRecovery('gap');
    service.setCampaignProjectionPipelineState({
      projectionBacklog: 3,
      projectionFailed: 1,
      servingOutboxBacklog: 2,
      servingOutboxFailed: 0,
      oldestPendingAgeSeconds: 4.5,
    });

    const metrics = await service.getMetrics();

    expect(queue.getJobCounts).toHaveBeenCalledWith(
      'waiting',
      'active',
      'delayed',
      'prioritized',
      'paused',
      'completed',
      'failed'
    );
    expect(metrics).toContain(
      'boostad_queue_jobs{queue="bidlog-queue",state="waiting"} 7'
    );
    expect(metrics).toContain(
      'boostad_queue_jobs{queue="bidlog-queue",state="active"} 2'
    );
    expect(metrics).toContain(
      'boostad_queue_jobs{queue="bidlog-queue",state="failed"} 3'
    );
    expect(metrics).toContain(
      'boostad_rtb_auction_transition_total{operation="reserve",outcome="reserved"} 1'
    );
    expect(metrics).toContain('boostad_campaign_snapshot_ready 1');
    expect(metrics).toContain('boostad_campaign_snapshot_sequence 12');
    expect(metrics).toContain('boostad_campaign_snapshot_size 1000');
    expect(metrics).toContain(
      'boostad_campaign_snapshot_recovery_total{reason="gap"} 1'
    );
    expect(metrics).toContain('boostad_campaign_projection_request_backlog 3');
    expect(metrics).toContain('boostad_campaign_projection_request_failed 1');
    expect(metrics).toContain('boostad_campaign_serving_outbox_backlog 2');
    expect(metrics).toContain(
      'boostad_campaign_projection_oldest_pending_age_seconds 4.5'
    );
  });
});
