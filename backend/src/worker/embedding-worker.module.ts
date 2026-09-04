import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { EventEmitterModule } from '@nestjs/event-emitter';
import { CampaignCacheRepository } from 'src/campaign/repository/campaign.cache.repository.interface';
import { RedisCampaignCacheRepository } from 'src/campaign/repository/redis-campaign.cache.repository';
import { MetricsModule } from 'src/metrics/metrics.module';
import { QueueModule } from 'src/queue/queue.module';
import { RedisModule } from 'src/redis/redis.module';
import { ContextEmbeddingService } from 'src/rtb/context/context-embedding.service';
import { MLEngine } from 'src/rtb/ml/mlEngine.interface';
import { XenovaMLEngine } from 'src/rtb/ml/xenova-mlEngine';
import { EmbeddingWorker } from './embedding.worker';
import { CampaignSearchRepository } from 'src/campaign/repository/campaign-search.repository.interface';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    EventEmitterModule.forRoot(),
    RedisModule,
    QueueModule,
    MetricsModule,
  ],
  providers: [
    EmbeddingWorker,
    ContextEmbeddingService,
    { provide: MLEngine, useClass: XenovaMLEngine },
    RedisCampaignCacheRepository,
    {
      provide: CampaignCacheRepository,
      useExisting: RedisCampaignCacheRepository,
    },
    {
      provide: CampaignSearchRepository,
      useExisting: RedisCampaignCacheRepository,
    },
  ],
})
export class EmbeddingWorkerModule {}
