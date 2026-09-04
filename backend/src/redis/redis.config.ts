import type { ConfigService } from '@nestjs/config';
import type { RedisOptions } from 'ioredis';

export type RedisRole = 'SEARCH' | 'BUDGET' | 'QUEUE';

export type RedisTopologyMode = 'legacy' | 'split';

export function resolveRedisTopologyMode(
  configService: Pick<ConfigService, 'get'>
): RedisTopologyMode {
  return configService.get<string>('REDIS_TOPOLOGY_MODE', 'legacy') === 'split'
    ? 'split'
    : 'legacy';
}

export function resolveRedisConnection(
  configService: Pick<ConfigService, 'get'>,
  role: RedisRole
): { label: string; options: RedisOptions } {
  const topology = resolveRedisTopologyMode(configService);
  const roleUrl = configService.get<string>(`${role}_REDIS_URL`);

  if (topology === 'split') {
    if (!roleUrl) {
      throw new Error(
        `REDIS_TOPOLOGY_MODE=split에는 ${role}_REDIS_URL이 필요합니다`
      );
    }
    return {
      label: redactRedisUrl(roleUrl),
      options: { lazyConnect: false, ...parseRedisUrl(roleUrl) },
    };
  }

  const host = configService.get<string>('REDIS_HOST', 'localhost');
  const port = configService.get<number>('REDIS_PORT', 16379);

  return {
    label: `${host}:${port}`,
    options: { host, port, lazyConnect: false },
  };
}

export function parseRedisUrl(redisUrl: string): RedisOptions {
  const parsed = new URL(redisUrl);
  if (parsed.protocol !== 'redis:' && parsed.protocol !== 'rediss:') {
    throw new Error(`지원하지 않는 Redis URL protocol: ${parsed.protocol}`);
  }

  const databasePath = parsed.pathname.replace(/^\//, '');
  const database = databasePath ? Number(databasePath) : undefined;
  if (database !== undefined && (!Number.isInteger(database) || database < 0)) {
    throw new Error(`유효하지 않은 Redis DB 번호: ${databasePath}`);
  }

  return {
    host: parsed.hostname,
    port: parsed.port ? Number(parsed.port) : 6379,
    username: parsed.username ? decodeURIComponent(parsed.username) : undefined,
    password: parsed.password ? decodeURIComponent(parsed.password) : undefined,
    db: database,
    tls: parsed.protocol === 'rediss:' ? {} : undefined,
  };
}

function redactRedisUrl(redisUrl: string): string {
  const parsed = new URL(redisUrl);
  parsed.username = parsed.username ? '***' : '';
  parsed.password = parsed.password ? '***' : '';
  return parsed.toString();
}
