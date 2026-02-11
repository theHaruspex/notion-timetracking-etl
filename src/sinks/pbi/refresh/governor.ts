export type RefreshLimits = {
  maxRowsPerHour: number;
  maxPostRequestsPerMinute: number;
  maxPostRequestsPerHour: number;
};

type RowEvent = {
  ts: number;
  rows: number;
};

const ONE_MINUTE_MS = 60_000;
const ONE_HOUR_MS = 60 * ONE_MINUTE_MS;

export class RefreshGovernor {
  private readonly limits: RefreshLimits;
  private readonly postRequestTimestamps: number[] = [];
  private readonly rowEvents: RowEvent[] = [];

  constructor(limits: RefreshLimits) {
    this.limits = limits;
  }

  async waitForBudget(input: { rows: number; postRequests: number }): Promise<void> {
    this.validateRequest(input);

    while (true) {
      this.pruneExpired();
      const waitMs = this.computeBlockingDelayMs(input);
      if (waitMs <= 0) {
        return;
      }
      await sleep(waitMs);
    }
  }

  record(input: { rows: number; postRequests: number }): void {
    this.validateRequest(input);
    const now = Date.now();

    if (input.rows > 0) {
      this.rowEvents.push({ ts: now, rows: input.rows });
    }
    for (let i = 0; i < input.postRequests; i += 1) {
      this.postRequestTimestamps.push(now);
    }
    this.pruneExpired();
  }

  private validateRequest(input: { rows: number; postRequests: number }): void {
    if (input.rows < 0 || input.postRequests < 0) {
      throw new Error('Refresh governor input cannot be negative.');
    }
    if (input.rows > this.limits.maxRowsPerHour) {
      throw new Error(
        `Requested rows (${input.rows}) exceed configured maxRowsPerHour (${this.limits.maxRowsPerHour}).`
      );
    }
    if (input.postRequests > this.limits.maxPostRequestsPerMinute) {
      throw new Error(
        `Requested postRequests (${input.postRequests}) exceed configured maxPostRequestsPerMinute (${this.limits.maxPostRequestsPerMinute}).`
      );
    }
    if (input.postRequests > this.limits.maxPostRequestsPerHour) {
      throw new Error(
        `Requested postRequests (${input.postRequests}) exceed configured maxPostRequestsPerHour (${this.limits.maxPostRequestsPerHour}).`
      );
    }
  }

  private pruneExpired(): void {
    const now = Date.now();
    const minuteCutoff = now - ONE_MINUTE_MS;
    const hourCutoff = now - ONE_HOUR_MS;

    while (
      this.postRequestTimestamps.length > 0 &&
      this.postRequestTimestamps[0] <= hourCutoff
    ) {
      this.postRequestTimestamps.shift();
    }

    while (this.rowEvents.length > 0 && this.rowEvents[0].ts <= hourCutoff) {
      this.rowEvents.shift();
    }

    // Keep minute filtering in compute step via cutoff check; here we only drop events that can
    // never contribute to either minute/hour limits.
    if (minuteCutoff < hourCutoff) {
      return;
    }
  }

  private computeBlockingDelayMs(input: { rows: number; postRequests: number }): number {
    const now = Date.now();
    const minuteCutoff = now - ONE_MINUTE_MS;
    const hourCutoff = now - ONE_HOUR_MS;

    const hourRowsUsed = this.rowEvents.reduce((sum, event) => sum + event.rows, 0);
    const minutePostsUsed = this.postRequestTimestamps.filter((ts) => ts > minuteCutoff).length;
    const hourPostsUsed = this.postRequestTimestamps.length;

    const exceedsRows = hourRowsUsed + input.rows > this.limits.maxRowsPerHour;
    const exceedsMinutePosts =
      minutePostsUsed + input.postRequests > this.limits.maxPostRequestsPerMinute;
    const exceedsHourPosts = hourPostsUsed + input.postRequests > this.limits.maxPostRequestsPerHour;

    if (!exceedsRows && !exceedsMinutePosts && !exceedsHourPosts) {
      return 0;
    }

    const candidates: number[] = [];

    if (exceedsRows) {
      const oldestRow = this.rowEvents[0];
      if (oldestRow) {
        candidates.push(oldestRow.ts + ONE_HOUR_MS - now);
      }
    }

    if (exceedsMinutePosts) {
      const oldestMinuteTs = this.postRequestTimestamps.find((ts) => ts > minuteCutoff);
      if (oldestMinuteTs !== undefined) {
        candidates.push(oldestMinuteTs + ONE_MINUTE_MS - now);
      }
    }

    if (exceedsHourPosts) {
      const oldestHourTs = this.postRequestTimestamps[0];
      if (oldestHourTs !== undefined) {
        candidates.push(oldestHourTs + ONE_HOUR_MS - now);
      }
    }

    if (candidates.length === 0) {
      throw new Error('Refresh governor cannot compute wait time for exceeded limits.');
    }

    return Math.max(1, Math.min(...candidates.map((value) => Math.max(0, Math.ceil(value)))));
  }
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}
