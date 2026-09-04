import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { EventEmitterModule } from '@nestjs/event-emitter';
import { TypeOrmModule } from '@nestjs/typeorm';
import { CampaignProjectionWorker } from '../campaign/projection/campaign-projection.worker';
import { CampaignProjectionOutboxEntity } from '../campaign/projection/entities/campaign-projection-outbox.entity';
import { CampaignEntity } from '../campaign/entities/campaign.entity';
import { CreditHistoryEntity } from '../advertiser/entities/credit-history.entity';
import { UserEntity } from '../user/entities/user.entity';
import { RedisCampaignCacheRepository } from '../campaign/repository/redis-campaign.cache.repository';
import { CampaignSearchRepository } from '../campaign/repository/campaign-search.repository.interface';
import { CampaignBudgetRepository } from '../campaign/repository/campaign-budget.repository.interface';
import { RedisModule } from '../redis/redis.module';
import { QueueModule } from '../queue/queue.module';
import { getTypeOrmConfig } from '../config/typeorm.config';

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
      CampaignProjectionOutboxEntity,
      CampaignEntity,
      CreditHistoryEntity,
      UserEntity,
    ]),
    RedisModule,
    QueueModule,
  ],
  providers: [
    CampaignProjectionWorker,
    RedisCampaignCacheRepository,
    {
      provide: CampaignSearchRepository,
      useExisting: RedisCampaignCacheRepository,
    },
    {
      provide: CampaignBudgetRepository,
      useExisting: RedisCampaignCacheRepository,
    },
  ],
})
export class CampaignProjectionWorkerModule {}
