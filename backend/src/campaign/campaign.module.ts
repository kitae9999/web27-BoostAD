import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
// import { BullModule } from '@nestjs/bullmq';
import { CampaignRepository } from './repository/campaign.repository.interface';
import { TypeOrmCampaignRepository } from './repository/typeorm-campaign.repository';
import { CampaignService } from './campaign.service';
import { CampaignController } from './campaign.controller';
import { CampaignCronService } from './campaign-cron.service';
import { CampaignEntity } from './entities/campaign.entity';
import { TagEntity } from '../tag/entities/tag.entity';
import { UserEntity } from '../user/entities/user.entity';
import { CreditHistoryEntity } from '../advertiser/entities/credit-history.entity';
import { LogModule } from '../log/log.module';
import { ImageModule } from '../image/image.module';
import { CampaignCacheRepository } from './repository/campaign.cache.repository.interface';
import { RedisCampaignCacheRepository } from './repository/redis-campaign.cache.repository';
import { UserModule } from 'src/user/user.module';
// import { AdvertiserModule } from 'src/advertiser/advertiser.module';
import { RedisModule } from 'src/redis/redis.module';
import { QueueModule } from 'src/queue/queue.module';
import { CampaignServingSnapshotService } from './campaign-serving-snapshot.service';
import { CampaignServingEventStore } from './events/campaign-serving-event.store';
import { CampaignServingEventConsumer } from './campaign-serving-event.consumer';
import { CampaignServingStatusController } from './campaign-serving-status.controller';
import { MetricsModule } from '../metrics/metrics.module';
import { CampaignServingProjectionRepository } from './projection/campaign-serving-projection.repository';
import { CampaignProjectionRequestEntity } from './projection/entities/campaign-projection-request.entity';
import { CampaignServingProjectionEntity } from './projection/entities/campaign-serving-projection.entity';
import { CampaignServingOutboxEntity } from './projection/entities/campaign-serving-outbox.entity';
import { CampaignServingKafkaConsumer } from './projection/campaign-serving-kafka.consumer';
import { CampaignProjectionSchemaService } from './projection/campaign-projection-schema.service';
import { CampaignProjectionObservabilityService } from './projection/campaign-projection-observability.service';

@Module({
  imports: [
    TypeOrmModule.forFeature([
      CampaignEntity,
      TagEntity,
      UserEntity,
      CreditHistoryEntity,
      CampaignProjectionRequestEntity,
      CampaignServingProjectionEntity,
      CampaignServingOutboxEntity,
    ]),
    LogModule,
    ImageModule,
    UserModule,
    // forwardRef(() => AdvertiserModule),
    RedisModule,
    QueueModule,
    MetricsModule,
  ],
  controllers: [CampaignController, CampaignServingStatusController],
  providers: [
    CampaignProjectionSchemaService,
    CampaignProjectionObservabilityService,
    CampaignService,
    CampaignCronService,
    CampaignServingSnapshotService,
    CampaignServingEventStore,
    CampaignServingEventConsumer,
    CampaignServingProjectionRepository,
    CampaignServingKafkaConsumer,
    { provide: CampaignRepository, useClass: TypeOrmCampaignRepository },
    {
      provide: CampaignCacheRepository,
      useClass: RedisCampaignCacheRepository,
    },
  ],
  exports: [
    CampaignRepository,
    CampaignCacheRepository,
    CampaignServingSnapshotService,
  ],
})
export class CampaignModule {}
