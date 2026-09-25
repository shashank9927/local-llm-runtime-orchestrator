import { describe, expect, it } from 'vitest';
import { isHeartbeatExpired, restartDelayMs } from './supervisor.js';

describe('worker supervision policies', () => {
  it('uses capped exponential restart backoff', () => {
    expect([1, 2, 3, 4, 5, 6].map(restartDelayMs)).toEqual([1000, 2000, 4000, 8000, 10000, 10000]);
  });

  it('marks only heartbeats beyond the timeout as expired', () => {
    expect(isHeartbeatExpired(1000, 7000, 6000)).toBe(false);
    expect(isHeartbeatExpired(999, 7000, 6000)).toBe(true);
  });
});
