export class CircuitOpenError extends Error {
  constructor(name: string) {
    super(`${name} circuit is open`);
  }
}

export class CircuitBreaker {
  private consecutiveFailures = 0;
  private openedAtMs: number | null = null;
  private halfOpenProbe = false;

  constructor(
    private readonly name: string,
    private readonly failureThreshold = 5,
    private readonly openDurationMs = 5_000,
    private readonly now: () => number = Date.now
  ) {}

  assertAvailable(): void {
    if (this.openedAtMs === null) return;
    if (this.now() - this.openedAtMs < this.openDurationMs) {
      throw new CircuitOpenError(this.name);
    }
    if (this.halfOpenProbe) throw new CircuitOpenError(this.name);
  }

  async execute<T>(operation: () => Promise<T>): Promise<T> {
    this.assertAvailable();
    const isHalfOpen = this.openedAtMs !== null;
    if (isHalfOpen) this.halfOpenProbe = true;
    try {
      const result = await operation();
      this.consecutiveFailures = 0;
      this.openedAtMs = null;
      return result;
    } catch (error) {
      this.consecutiveFailures += 1;
      if (isHalfOpen || this.consecutiveFailures >= this.failureThreshold) {
        this.openedAtMs = this.now();
      }
      throw error;
    } finally {
      if (isHalfOpen) this.halfOpenProbe = false;
    }
  }

  getState(): 'CLOSED' | 'OPEN' | 'HALF_OPEN_READY' {
    if (this.openedAtMs === null) return 'CLOSED';
    return this.now() - this.openedAtMs >= this.openDurationMs
      ? 'HALF_OPEN_READY'
      : 'OPEN';
  }
}
