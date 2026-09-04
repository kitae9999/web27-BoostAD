import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { EMBEDDING_QUEUE_NAME } from './queue.names';
import { resolveRedisConnection } from '../redis/redis.config';

@Module({
  imports: [
    ConfigModule,
    BullModule.forRootAsync({
      imports: [ConfigModule],
      useFactory: (configService: ConfigService) => {
        const { options } = resolveRedisConnection(configService, 'QUEUE');
        return { connection: options };
      },
      inject: [ConfigService],
    }),
    BullModule.registerQueue(
      {
        name: EMBEDDING_QUEUE_NAME,
      },
      {
        name: 'bidlog-queue',
        defaultJobOptions: {
          removeOnComplete: true,
          removeOnFail: false,
          attempts: 5,
          backoff: {
            type: 'exponential',
            delay: 1000,
          },
        },
      }
    ),
  ],
  exports: [BullModule],
})
export class QueueModule {}
