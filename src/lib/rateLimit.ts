import Bottleneck from 'bottleneck';

export function createGlobalRateLimiter(requestsPerSecond: number): Bottleneck {
  const minTime = Math.ceil(1000 / requestsPerSecond);
  return new Bottleneck({
    maxConcurrent: 1,
    minTime
  });
}
