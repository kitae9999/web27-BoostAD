import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { EventEmitterModule } from '@nestjs/event-emitter';
import { TypeOrmModule } from '@nestjs/typeorm';
import { CampaignCacheRepository } from 'src/campaign/repository/campaign.cache.repository.interface';
import { RedisCampaignCacheRepository } from 'src/campaign/repository/redis-campaign.cache.repository';
import { QueueModule } from 'src/queue/queue.module';
import { EmbeddingWorker } from 'src/worker/embedding.worker';
import { RedisModule } from 'src/redis/redis.module';
import { MLEngine } from 'src/rtb/ml/mlEngine.interface';
import { XenovaMLEngine } from 'src/rtb/ml/xenova-mlEngine';
import { CacheRepository } from 'src/cache/repository/cache.repository.interface';
import { RedisCacheRepository } from 'src/cache/repository/redis-cache.repository';
import { RedisTTLWorker } from './redis-ttl.worker';
import { getTypeOrmConfig } from 'src/config/typeorm.config';
import { ContextEmbeddingService } from 'src/rtb/context/context-embedding.service';
import { MetricsModule } from 'src/metrics/metrics.module';
import { CampaignServingEventStore } from 'src/campaign/events/campaign-serving-event.store';
import { CampaignProjectionWorker } from 'src/campaign/projection/campaign-projection.worker';
import { CampaignEntity } from 'src/campaign/entities/campaign.entity';
import { TagEntity } from 'src/tag/entities/tag.entity';
import { CampaignProjectionRequestEntity } from 'src/campaign/projection/entities/campaign-projection-request.entity';
import { CampaignServingProjectionEntity } from 'src/campaign/projection/entities/campaign-serving-projection.entity';
import { CampaignServingOutboxEntity } from 'src/campaign/projection/entities/campaign-serving-outbox.entity';
import { CampaignServingKafkaProducer } from 'src/kafka/campaign-serving-kafka.producer';
import { CampaignServingOutboxPublisher } from 'src/campaign/projection/campaign-serving-outbox.publisher';
import { CampaignServingProjectionRepository } from 'src/campaign/projection/campaign-serving-projection.repository';
import { CampaignSearchIndexerConsumer } from 'src/campaign/projection/campaign-search-indexer.consumer';
import { CampaignProjectionSchemaService } from 'src/campaign/projection/campaign-projection-schema.service';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    EventEmitterModule.forRoot(),
    TypeOrmModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (configService: ConfigService) =>
        getTypeOrmConfig(configService),
    }),
    TypeOrmModule.forFeature([
      CampaignEntity,
      TagEntity,
      CampaignProjectionRequestEntity,
      CampaignServingProjectionEntity,
      CampaignServingOutboxEntity,
    ]),
    RedisModule,
    QueueModule,
    MetricsModule,
  ],
  providers: [
    CampaignProjectionSchemaService,
    EmbeddingWorker,
    ContextEmbeddingService,
    RedisTTLWorker,
    CampaignServingEventStore,
    CampaignProjectionWorker,
    CampaignServingKafkaProducer,
    CampaignServingOutboxPublisher,
    CampaignServingProjectionRepository,
    CampaignSearchIndexerConsumer,
    { provide: MLEngine, useClass: XenovaMLEngine },
    {
      provide: CampaignCacheRepository,
      useClass: RedisCampaignCacheRepository,
    },
    {
      provide: CacheRepository,
      useClass: RedisCacheRepository,
    },
  ],
})
export class WorkerModule {}
