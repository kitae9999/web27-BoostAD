import { parseRedisUrl, resolveRedisConnection } from './redis.config';

describe('Redis role configuration', () => {
  const config = (values: Record<string, unknown>) => ({
    get: <T>(key: string, defaultValue?: T): T =>
      (values[key] as T | undefined) ?? (defaultValue as T),
  });

  it('keeps every role on the legacy endpoint by default', () => {
    const connection = resolveRedisConnection(
      config({
        REDIS_HOST: 'legacy-redis',
        REDIS_PORT: 6380,
        SEARCH_REDIS_URL: 'redis://search:6379',
      }),
      'SEARCH'
    );

    expect(connection.options).toMatchObject({
      host: 'legacy-redis',
      port: 6380,
    });
  });

  it('uses the role URL only in split mode', () => {
    const connection = resolveRedisConnection(
      config({
        REDIS_TOPOLOGY_MODE: 'split',
        BUDGET_REDIS_URL: 'rediss://user:secret@budget:6381/2',
      }),
      'BUDGET'
    );

    expect(connection.options).toMatchObject({
      host: 'budget',
      port: 6381,
      username: 'user',
      password: 'secret',
      db: 2,
      tls: {},
    });
    expect(connection.label).not.toContain('secret');
  });

  it('rejects unsupported URL protocols', () => {
    expect(() => parseRedisUrl('http://localhost:6379')).toThrow(
      '지원하지 않는 Redis URL protocol'
    );
  });

  it('fails fast when a split role endpoint is missing', () => {
    expect(() =>
      resolveRedisConnection(config({ REDIS_TOPOLOGY_MODE: 'split' }), 'QUEUE')
    ).toThrow('QUEUE_REDIS_URL이 필요합니다');
  });
});
