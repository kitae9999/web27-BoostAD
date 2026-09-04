import { OnWorkerEvent, Processor, WorkerHost } from '@nestjs/bullmq';
import { Job } from 'bullmq';
import { Logger, OnApplicationBootstrap, Optional } from '@nestjs/common';
import { OnEvent } from '@nestjs/event-emitter';
import { MLEngine } from 'src/rtb/ml/mlEngine.interface';
import { CampaignCacheRepository } from 'src/campaign/repository/campaign.cache.repository.interface';
import { ContextEmbeddingService } from 'src/rtb/context/context-embedding.service';
import type { ContextEmbeddingJobData } from 'src/queue/types/queue.type';
import { MetricsService } from 'src/metrics/metrics.service';
import { buildCampaignDocumentText } from 'src/rtb/ml/embedding-text';
import { EMBEDDING_QUEUE_NAME } from 'src/queue/queue.names';
import { CampaignSearchRepository } from 'src/campaign/repository/campaign-search.repository.interface';
import type { CampaignEmbeddingJobData } from 'src/queue/types/queue.type';

@Processor(EMBEDDING_QUEUE_NAME, { autorun: false })
export class EmbeddingWorker
  extends WorkerHost
  implements OnApplicationBootstrap
{
  private readonly logger = new Logger(EmbeddingWorker.name);
  private startRequested = false;

  constructor(
    private readonly mlEngine: MLEngine,
    private readonly campaignCacheRepository: CampaignCacheRepository,
    private readonly contextEmbeddingService: ContextEmbeddingService,
    private readonly metricsService: MetricsService,
    @Optional()
    private readonly campaignSearchRepository?: CampaignSearchRepository
  ) {
    super();
  }

  onApplicationBootstrap(): void {
    if (this.mlEngine.isReady()) {
      this.startWorker();
    }
  }

  @OnEvent('ml.model.ready')
  onModelReady(): void {
    this.startWorker();
  }

  private startWorker(): void {
    if (this.startRequested || this.worker.isRunning()) {
      return;
    }

    this.startRequested = true;
    void this.worker.run().catch((error) => {
      this.startRequested = false;
      this.logger.error('Embedding worker 실행 실패:', error);
    });
  }

  async process(job: Job): Promise<void> {
    this.logger.debug(`Processing job ${job.id} of type ${job.name}`);

    try {
      if (job.name === 'generate-campaign-embedding') {
        const campaignJob = job.data as CampaignEmbeddingJobData;
        const { modelVersion } = campaignJob;
        if (modelVersion && modelVersion !== this.mlEngine.getModelVersion()) {
          throw new Error(
            `campaign job model version 불일치: ${modelVersion} vs ${this.mlEngine.getModelVersion()}`
          );
        }
        await this.generateCampaignEmbedding(campaignJob);
      } else if (job.name === 'generate-context-embedding') {
        await this.generateContextEmbedding(
          job as Job<ContextEmbeddingJobData>
        );
      } else {
        this.logger.warn(`Unknown job type: ${job.name}`);
      }
    } catch (error) {
      this.logger.error(`Job ${job.id} failed:`, error);
      throw error;
    }
  }

  @OnWorkerEvent('failed')
  async onWorkerFailed(job: Job | undefined, error: Error): Promise<void> {
    if (!job || job.name !== 'generate-context-embedding') {
      return;
    }

    const configuredAttempts = job.opts.attempts ?? 1;
    if (job.attemptsMade < configuredAttempts) {
      return;
    }

    // BullMQ가 최종 실패 작업을 제거한 뒤 상태를 열어 다음 observe가 재등록할 수 있게 한다.
    await this.contextEmbeddingService.failJob(
      job.data as ContextEmbeddingJobData,
      error
    );
  }

  private async generateContextEmbedding(job: Job<ContextEmbeddingJobData>) {
    const startedAt = process.hrtime.bigint();
    try {
      if (job.data.modelVersion !== this.mlEngine.getModelVersion()) {
        throw new Error(
          `context job model version 불일치: ${job.data.modelVersion} vs ${this.mlEngine.getModelVersion()}`
        );
      }
      const embedding = await this.mlEngine.getEmbedding(
        job.data.text,
        'query'
      );
      await this.contextEmbeddingService.completeJob(job.data, embedding);
      this.metricsService.recordRtbContextJob('completed');
    } catch (error) {
      this.metricsService.recordRtbContextJob('failed');
      throw error;
    } finally {
      this.metricsService.observeRtbContextEmbeddingDuration(
        Number(process.hrtime.bigint() - startedAt) / 1_000_000_000
      );
    }
  }

  private async generateCampaignEmbedding(job: CampaignEmbeddingJobData) {
    const versioned =
      job.servingVersion !== undefined &&
      job.semanticHash !== undefined &&
      job.title !== undefined &&
      job.content !== undefined &&
      job.tags !== undefined;
    const campaign = versioned
      ? {
          id: job.campaignId,
          title: job.title!,
          content: job.content!,
          tags: job.tags!,
        }
      : await this.campaignCacheRepository.findCampaignCacheById(
          job.campaignId
        );

    if (!campaign) {
      this.logger.warn(`Campaign ${job.campaignId}를 찾을 수 없습니다.`);
      return;
    }

    if (!campaign.tags || campaign.tags.length === 0) {
      this.logger.warn(`Campaign ${job.campaignId}에 태그가 없습니다.`);
      return;
    }

    // 2. E5 retrieval 계약에 따라 캠페인은 passage로 생성한다.
    const embeddingTags: { [tagName: string]: number[] } = {};

    for (const tagName of campaign.tags) {
      const embedding = await this.mlEngine.getEmbedding(tagName, 'passage');
      embeddingTags[tagName] = embedding;
    }

    const document = await this.mlEngine.getEmbedding(
      buildCampaignDocumentText(campaign),
      'passage'
    );

    // 3. model version, document, tag vector를 한 번에 publish한다.
    const payload = {
      modelVersion: this.mlEngine.getModelVersion(),
      document,
      tags: embeddingTags,
    };

    if (versioned) {
      if (!this.campaignSearchRepository) {
        throw new Error('versioned embedding용 Search repository가 없습니다');
      }
      const applied =
        await this.campaignSearchRepository.updateEmbeddingsIfCurrent(
          job.campaignId,
          job.servingVersion!,
          job.semanticHash!,
          payload
        );
      if (!applied) {
        this.logger.warn(
          `stale campaign embedding 폐기: ${job.campaignId} v${job.servingVersion}`
        );
        return;
      }
    } else {
      await this.campaignCacheRepository.updateCampaignEmbeddings(
        job.campaignId,
        payload
      );
    }

    this.logger.log(
      `✅ ID:${job.campaignId.slice(0, 8)}... title:${campaign.title.slice(0, 15)}... 임베딩 생성 완료 (${campaign.tags.length}개 태그)`
    );
  }
}
