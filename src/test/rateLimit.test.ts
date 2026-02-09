import { describe, expect, it } from 'vitest';
import { createGlobalRateLimiter } from '../lib/rateLimit.js';

describe('createGlobalRateLimiter', () => {
  it('enforces roughly 3 requests per second', async () => {
    const limiter = createGlobalRateLimiter(3);
    const start = Date.now();

    await Promise.all(
      Array.from({ length: 4 }).map(() =>
        limiter.schedule(async () => {
          return Date.now();
        })
      )
    );

    const elapsed = Date.now() - start;

    // 4 jobs with minTime ~334ms should take at least around 1s in sequence.
    expect(elapsed).toBeGreaterThanOrEqual(900);
  });
});
