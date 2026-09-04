import { MLEngine } from 'src/rtb/ml/mlEngine.interface';
import { CampaignCacheRepository } from 'src/campaign/repository/campaign.cache.repository.interface';
import { EmbeddingWorker } from './embedding.worker';
import { ContextEmbeddingService } from 'src/rtb/context/context-embedding.service';
import { MetricsService } from 'src/metrics/metrics.service';
import { Job } from 'bullmq';
import { CampaignSearchRepository } from 'src/campaign/repository/campaign-search.repository.interface';

describe('EmbeddingWorker lifecycle', () => {
  const buildWorker = (modelReady: boolean) => {
    const mlEngine = {
      isReady: jest.fn(() => modelReady),
      getModelVersion: jest.fn(() => 'model-v2'),
      getEmbedding: jest.fn().mockResolvedValue([0.1, 0.2]),
    } as unknown as MLEngine;
    const repository = {
      findCampaignCacheById: jest.fn(),
      updateCampaignEmbeddings: jest.fn(),
    } as unknown as CampaignCacheRepository & {
      findCampaignCacheById: jest.Mock;
      updateCampaignEmbeddings: jest.Mock;
    };
    const contextEmbeddingServiceMock = {
      completeJob: jest.fn(),
      failJob: jest.fn(),
    };
    const contextEmbeddingService =
      contextEmbeddingServiceMock as unknown as ContextEmbeddingService;
    const metricsService = {
      recordRtbContextJob: jest.fn(),
      observeRtbContextEmbeddingDuration: jest.fn(),
    } as unknown as MetricsService;
    const searchRepository = {
      updateEmbeddingsIfCurrent: jest.fn().mockResolvedValue(true),
    } as unknown as CampaignSearchRepository & {
      updateEmbeddingsIfCurrent: jest.Mock;
    };
    const worker = new EmbeddingWorker(
      mlEngine,
      repository,
      contextEmbeddingService,
      metricsService,
      searchRepository
    );
    const bullWorker = {
      isRunning: jest.fn(() => false),
      run: jest.fn().mockResolvedValue(undefined),
    };

    Object.defineProperty(worker, 'worker', {
      configurable: true,
      value: bullWorker,
    });

    return {
      worker,
      bullWorker,
      mlEngine: mlEngine as unknown as { getEmbedding: jest.Mock },
      repository,
      searchRepository,
      contextEmbeddingService: contextEmbeddingServiceMock,
    };
  };

  it('does not consume jobs before the ML model is ready', () => {
    const { worker, bullWorker } = buildWorker(false);

    worker.onApplicationBootstrap();

    expect(bullWorker.run).not.toHaveBeenCalled();
  });

  it('starts once when the model-ready event arrives', () => {
    const { worker, bullWorker } = buildWorker(false);

    worker.onModelReady();
    worker.onModelReady();

    expect(bullWorker.run).toHaveBeenCalledTimes(1);
  });

  it('starts during bootstrap when the model is already ready', () => {
    const { worker, bullWorker } = buildWorker(true);

    worker.onApplicationBootstrap();

    expect(bullWorker.run).toHaveBeenCalledTimes(1);
  });

  it('embeds context jobs with the query role', async () => {
    const { worker, mlEngine, contextEmbeddingService } = buildWorker(true);
    const job = {
      id: 'context-1',
      name: 'generate-context-embedding',
      data: {
        contextId: `ctx_${'a'.repeat(64)}`,
        contentHash: 'a'.repeat(64),
        modelVersion: 'model-v2',
        text: '한국어 블로그 본문',
      },
      opts: { attempts: 3 },
      attemptsMade: 0,
    } as unknown as Job;

    await worker.process(job);

    expect(mlEngine.getEmbedding).toHaveBeenCalledWith(
      '한국어 블로그 본문',
      'query'
    );
    expect(contextEmbeddingService.completeJob).toHaveBeenCalled();
  });

  it('rejects a context job from another model namespace', async () => {
    const { worker, mlEngine, contextEmbeddingService } = buildWorker(true);

    await expect(
      worker.process({
        id: 'context-old',
        name: 'generate-context-embedding',
        data: {
          contextId: `ctx_${'b'.repeat(64)}`,
          contentHash: 'b'.repeat(64),
          modelVersion: 'old-model',
          text: '본문',
        },
        opts: { attempts: 1 },
        attemptsMade: 0,
      } as unknown as Job)
    ).rejects.toThrow('context job model version 불일치');
    expect(mlEngine.getEmbedding).not.toHaveBeenCalled();
    expect(contextEmbeddingService.failJob).not.toHaveBeenCalled();
  });

  it('records FAILED only after BullMQ exhausts all attempts', async () => {
    const { worker, contextEmbeddingService } = buildWorker(true);
    const error = new Error('embedding failed');
    const job = {
      name: 'generate-context-embedding',
      data: {
        contextId: `ctx_${'c'.repeat(64)}`,
        contentHash: 'c'.repeat(64),
        modelVersion: 'model-v2',
        text: '본문',
      },
      opts: { attempts: 3 },
      attemptsMade: 3,
    } as unknown as Job;

    await worker.onWorkerFailed(job, error);

    expect(contextEmbeddingService.failJob).toHaveBeenCalledWith(
      job.data,
      error
    );
  });

  it('keeps PENDING while BullMQ still has a retry attempt', async () => {
    const { worker, contextEmbeddingService } = buildWorker(true);
    const job = {
      name: 'generate-context-embedding',
      data: {},
      opts: { attempts: 3 },
      attemptsMade: 2,
    } as unknown as Job;

    await worker.onWorkerFailed(job, new Error('retrying'));

    expect(contextEmbeddingService.failJob).not.toHaveBeenCalled();
  });

  it('publishes one campaign document embedding from a passage input', async () => {
    const { worker, mlEngine, repository } = buildWorker(true);
    repository.findCampaignCacheById.mockResolvedValue({
      id: 'campaign-1',
      title: '프론트엔드 진단',
      content: 'React 렌더링 병목을 분석합니다.',
      tags: ['React', 'TypeScript'],
    });
    mlEngine.getEmbedding.mockResolvedValueOnce([0.5, 0.6]);

    await worker.process({
      id: 'campaign-job',
      name: 'generate-campaign-embedding',
      data: { campaignId: 'campaign-1' },
    } as unknown as Job);

    expect(mlEngine.getEmbedding).toHaveBeenCalledTimes(1);
    expect(mlEngine.getEmbedding).toHaveBeenCalledWith(
      '프론트엔드 진단\nReact 렌더링 병목을 분석합니다.\nReact TypeScript',
      'passage'
    );
    expect(repository.updateCampaignEmbeddings).toHaveBeenCalledWith(
      'campaign-1',
      {
        modelVersion: 'model-v2',
        document: [0.5, 0.6],
      }
    );
  });

  it('uses version and semantic hash CAS for projection embedding jobs', async () => {
    const { worker, searchRepository, repository } = buildWorker(true);

    await worker.process({
      id: 'campaign-v3',
      name: 'generate-campaign-embedding',
      data: {
        campaignId: 'campaign-1',
        servingVersion: 3,
        semanticHash: 'hash-v3',
        modelVersion: 'model-v2',
        title: 'Redis',
        content: '역할 분리',
        tags: ['redis'],
      },
    } as unknown as Job);

    expect(searchRepository.updateEmbeddingsIfCurrent).toHaveBeenCalledWith(
      'campaign-1',
      3,
      'hash-v3',
      expect.objectContaining({ modelVersion: 'model-v2' })
    );
    expect(repository.findCampaignCacheById).not.toHaveBeenCalled();
    expect(repository.updateCampaignEmbeddings).not.toHaveBeenCalled();
  });

  it('does not publish a stale projection embedding through the legacy path', async () => {
    const { worker, searchRepository, repository } = buildWorker(true);
    searchRepository.updateEmbeddingsIfCurrent.mockResolvedValue(false);

    await worker.process({
      id: 'campaign-stale',
      name: 'generate-campaign-embedding',
      data: {
        campaignId: 'campaign-1',
        servingVersion: 2,
        semanticHash: 'old-hash',
        modelVersion: 'model-v2',
        title: 'old',
        content: 'old',
        tags: ['redis'],
      },
    } as unknown as Job);

    expect(repository.updateCampaignEmbeddings).not.toHaveBeenCalled();
  });
});
