import {
  Global,
  Inject,
  Logger,
  Module,
  OnApplicationShutdown,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import Redis from 'ioredis';
import {
  BUDGET_REDIS_CLIENT,
  QUEUE_REDIS_CLIENT,
  SEARCH_REDIS_CLIENT,
} from './redis.constant';
import type { AppIORedisClient } from './redis.type';
import { resolveRedisConnection, type RedisRole } from './redis.config';

const REDIS_ROLES = [
  ['SEARCH', SEARCH_REDIS_CLIENT],
  ['BUDGET', BUDGET_REDIS_CLIENT],
  ['QUEUE', QUEUE_REDIS_CLIENT],
] as const satisfies ReadonlyArray<readonly [RedisRole, symbol]>;

function createRedisProvider(role: RedisRole, token: symbol) {
  return {
    provide: token,
    inject: [ConfigService],
    useFactory: (configService: ConfigService): AppIORedisClient => {
      const logger = new Logger(`${role}_REDIS_CLIENT`);
      const connection = resolveRedisConnection(configService, role);
      const client = new Redis({
        ...connection.options,
        retryStrategy: (times) => Math.min(times * 50, 2000),
      });

      client.on('error', (error) => {
        logger.error(`Redis client error: ${String(error)}`);
      });
      client.on('ready', () => {
        logger.log(`Redis 연결 성공: ${connection.label}`);
      });
      client.on('reconnecting', (delay: number) => {
        logger.warn(`Redis 재연결 시도 중 (${delay}ms 후)`);
      });

      return client;
    },
  };
}

@Global()
@Module({
  providers: REDIS_ROLES.map(([role, token]) =>
    createRedisProvider(role, token)
  ),
  exports: REDIS_ROLES.map(([, token]) => token),
})
export class RedisModule implements OnApplicationShutdown {
  private readonly logger = new Logger(RedisModule.name);

  constructor(
    @Inject(SEARCH_REDIS_CLIENT)
    private readonly searchRedis: AppIORedisClient,
    @Inject(BUDGET_REDIS_CLIENT)
    private readonly budgetRedis: AppIORedisClient,
    @Inject(QUEUE_REDIS_CLIENT)
    private readonly queueRedis: AppIORedisClient
  ) {}

  async onApplicationShutdown(signal?: string): Promise<void> {
    const clients = new Set([
      this.searchRedis,
      this.budgetRedis,
      this.queueRedis,
    ]);

    for (const client of clients) {
      if (client.status !== 'ready') {
        client.disconnect();
        continue;
      }
      try {
        await client.quit();
      } catch (error) {
        this.logger.warn(
          `Redis quit 실패(signal=${signal ?? 'unknown'}): ${String(error)}`
        );
        client.disconnect();
      }
    }
  }
}
