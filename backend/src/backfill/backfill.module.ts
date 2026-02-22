import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { RedisModule } from 'src/redis/redis.module';
import { RedisCampaignCacheRepository } from 'src/campaign/repository/redis-campaign.cache.repository';
import { CampaignIndexBackfillService } from './campaign-index-backfill.service';

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true }), RedisModule],
  providers: [RedisCampaignCacheRepository, CampaignIndexBackfillService],
})
export class BackfillModule {}

