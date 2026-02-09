import { describe, expect, it, vi } from 'vitest';
import { defaultRetryDecision, retryAsync } from '../lib/retry.js';

describe('retryAsync', () => {
  it('retries on 429 using retry-after header seconds', async () => {
    let attempts = 0;
    const sleep = vi.fn(async () => undefined);

    const result = await retryAsync(
      async () => {
        attempts += 1;
        if (attempts === 1) {
          throw {
            status: 429,
            headers: {
              'retry-after': '1'
            }
          };
        }
        return 'ok';
      },
      { maxRetries: 3 },
      defaultRetryDecision,
      sleep
    );

    expect(result).toBe('ok');
    expect(attempts).toBe(2);
    expect(sleep).toHaveBeenCalledWith(1000);
  });

  it('does not retry on non-429 4xx errors', async () => {
    const sleep = vi.fn(async () => undefined);

    await expect(
      retryAsync(
        async () => {
          throw { status: 400 };
        },
        { maxRetries: 3 },
        defaultRetryDecision,
        sleep
      )
    ).rejects.toEqual({ status: 400 });

    expect(sleep).not.toHaveBeenCalled();
  });
});
