import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { EventEmitterModule } from '@nestjs/event-emitter';
import { CacheRepository } from 'src/cache/repository/cache.repository.interface';
import { RedisCacheRepository } from 'src/cache/repository/redis-cache.repository';
import { CampaignCacheRepository } from 'src/campaign/repository/campaign.cache.repository.interface';
import { RedisCampaignCacheRepository } from 'src/campaign/repository/redis-campaign.cache.repository';
import { RedisModule } from 'src/redis/redis.module';
import { RedisTTLWorker } from './redis-ttl.worker';
import { CampaignBudgetRepository } from 'src/campaign/repository/campaign-budget.repository.interface';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    EventEmitterModule.forRoot(),
    RedisModule,
  ],
  providers: [
    RedisTTLWorker,
    RedisCampaignCacheRepository,
    {
      provide: CampaignCacheRepository,
      useExisting: RedisCampaignCacheRepository,
    },
    {
      provide: CampaignBudgetRepository,
      useExisting: RedisCampaignCacheRepository,
    },
    {
      provide: CacheRepository,
      useClass: RedisCacheRepository,
    },
  ],
})
export class ReservationWorkerModule {}
