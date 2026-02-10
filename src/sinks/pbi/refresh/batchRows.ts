export function batchRows<T>(rows: T[], maxBatchSize = 10_000): T[][] {
  if (!Number.isInteger(maxBatchSize) || maxBatchSize <= 0) {
    throw new Error('maxBatchSize must be a positive integer.');
  }
  if (maxBatchSize > 10_000) {
    throw new Error('Power BI limit exceeded: maxBatchSize cannot be greater than 10000.');
  }

  const batches: T[][] = [];
  for (let index = 0; index < rows.length; index += maxBatchSize) {
    const chunk = rows.slice(index, index + maxBatchSize);
    if (chunk.length > 10_000) {
      throw new Error('Power BI limit exceeded: generated row batch is larger than 10000.');
    }
    batches.push(chunk);
  }

  return batches;
}
