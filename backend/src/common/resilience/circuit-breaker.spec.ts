import { CircuitBreaker, CircuitOpenError } from './circuit-breaker';

describe('CircuitBreaker', () => {
  it('opens after consecutive failures and closes after a successful probe', async () => {
    let now = 0;
    const breaker = new CircuitBreaker('budget', 2, 5_000, () => now);
    await expect(
      breaker.execute(async () => Promise.reject('first'))
    ).rejects.toBe('first');
    await expect(
      breaker.execute(async () => Promise.reject('second'))
    ).rejects.toBe('second');
    await expect(breaker.execute(async () => 'blocked')).rejects.toBeInstanceOf(
      CircuitOpenError
    );
    now = 5_000;
    await expect(breaker.execute(async () => 'ok')).resolves.toBe('ok');
    expect(breaker.getState()).toBe('CLOSED');
  });

  it('allows only one half-open probe', async () => {
    let now = 0;
    const breaker = new CircuitBreaker('search', 1, 10, () => now);
    await expect(
      breaker.execute(async () => Promise.reject('fail'))
    ).rejects.toBe('fail');
    now = 10;
    let resolveProbe: ((value: string) => void) | undefined;
    const probe = breaker.execute(
      () => new Promise<string>((resolve) => (resolveProbe = resolve))
    );
    expect(() => breaker.assertAvailable()).toThrow(CircuitOpenError);
    resolveProbe?.('ok');
    await expect(probe).resolves.toBe('ok');
  });
});
