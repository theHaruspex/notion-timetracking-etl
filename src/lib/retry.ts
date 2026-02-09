export interface RetryOptions {
  maxRetries: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
}

export interface RetryDecision {
  shouldRetry: boolean;
  delayMs: number;
}

function parseRetryAfterMs(retryAfterHeader: string | null | undefined): number | null {
  if (!retryAfterHeader) {
    return null;
  }

  const asSeconds = Number.parseFloat(retryAfterHeader);
  if (!Number.isNaN(asSeconds)) {
    return Math.max(0, Math.round(asSeconds * 1000));
  }

  const asDate = Date.parse(retryAfterHeader);
  if (Number.isNaN(asDate)) {
    return null;
  }
  return Math.max(0, asDate - Date.now());
}

function getHeaderValue(headers: unknown, key: string): string | undefined {
  if (!headers || typeof headers !== 'object') {
    return undefined;
  }

  const headerRecord = headers as Record<string, unknown>;
  const direct = headerRecord[key] ?? headerRecord[key.toLowerCase()] ?? headerRecord[key.toUpperCase()];
  if (typeof direct === 'string') {
    return direct;
  }

  if (Array.isArray(direct) && typeof direct[0] === 'string') {
    return direct[0];
  }

  return undefined;
}

export function defaultRetryDecision(
  error: unknown,
  attempt: number,
  options: RetryOptions
): RetryDecision {
  const baseDelayMs = options.baseDelayMs ?? 500;
  const maxDelayMs = options.maxDelayMs ?? 10_000;
  const status =
    (error as { status?: number; statusCode?: number } | null | undefined)?.status ??
    (error as { statusCode?: number } | null | undefined)?.statusCode;

  if (status === 429) {
    const retryAfterHeader = getHeaderValue((error as { headers?: unknown }).headers, 'retry-after');
    const retryAfterMs = parseRetryAfterMs(retryAfterHeader);
    if (retryAfterMs !== null) {
      return { shouldRetry: true, delayMs: retryAfterMs };
    }
    const fallbackDelay = Math.min(baseDelayMs * 2 ** attempt, maxDelayMs);
    return { shouldRetry: true, delayMs: fallbackDelay };
  }

  if (typeof status === 'number' && status >= 500 && status <= 599) {
    const expBackoff = Math.min(baseDelayMs * 2 ** attempt, maxDelayMs);
    const jitter = Math.round(Math.random() * 200);
    return { shouldRetry: true, delayMs: expBackoff + jitter };
  }

  return { shouldRetry: false, delayMs: 0 };
}

export async function retryAsync<T>(
  operation: () => Promise<T>,
  options: RetryOptions,
  decideRetry: (error: unknown, attempt: number, options: RetryOptions) => RetryDecision =
    defaultRetryDecision,
  sleep: (ms: number) => Promise<void> = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
): Promise<T> {
  let attempt = 0;

  while (true) {
    try {
      return await operation();
    } catch (error) {
      if (attempt >= options.maxRetries) {
        throw error;
      }
      const decision = decideRetry(error, attempt, options);
      if (!decision.shouldRetry) {
        throw error;
      }
      await sleep(decision.delayMs);
      attempt += 1;
    }
  }
}
