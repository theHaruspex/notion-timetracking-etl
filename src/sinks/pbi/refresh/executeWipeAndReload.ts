import type { PbiDatasetSpec } from '../spec/types.js';
import { PowerBiClient } from '../client/powerbiClient.js';
import { batchRows } from './batchRows.js';
import { RefreshGovernor } from './governor.js';
import {
  DEFAULT_MAX_POST_REQUESTS_PER_HOUR,
  DEFAULT_MAX_POST_REQUESTS_PER_MINUTE,
  DEFAULT_MAX_ROWS_PER_HOUR
} from './limits.js';

export interface ExecuteWipeAndReloadInput {
  workspaceId: string;
  datasetId: string;
  spec: PbiDatasetSpec;
  tableRowsByName: Record<string, object[]>;
  limits?: Partial<{
    maxRowsPerHour: number;
    maxPostRequestsPerMinute: number;
    maxPostRequestsPerHour: number;
  }>;
  log?: (message: string, meta?: unknown) => void;
}

export async function executeWipeAndReload(
  client: PowerBiClient,
  input: ExecuteWipeAndReloadInput
): Promise<{
  tablesProcessed: number;
  totalRowsPosted: number;
  totalPostRequests: number;
}> {
  const specTableNames = new Set(input.spec.tables.map((table) => table.name));
  for (const tableName of Object.keys(input.tableRowsByName)) {
    if (!specTableNames.has(tableName)) {
      throw new Error(
        `executeWipeAndReload input contains unknown table "${tableName}". Ensure it exists in the dataset spec.`
      );
    }
  }

  const tableOrder = input.spec.tables
    .map((table) => table.name)
    .filter((tableName) => Object.prototype.hasOwnProperty.call(input.tableRowsByName, tableName));

  const governor = new RefreshGovernor({
    maxRowsPerHour: input.limits?.maxRowsPerHour ?? DEFAULT_MAX_ROWS_PER_HOUR,
    maxPostRequestsPerMinute:
      input.limits?.maxPostRequestsPerMinute ?? DEFAULT_MAX_POST_REQUESTS_PER_MINUTE,
    maxPostRequestsPerHour:
      input.limits?.maxPostRequestsPerHour ?? DEFAULT_MAX_POST_REQUESTS_PER_HOUR
  });
  const logger =
    input.log ??
    ((message: string, meta?: unknown) => {
      if (meta === undefined) {
        console.log(message);
        return;
      }
      console.log(message, meta);
    });

  let totalRowsPosted = 0;
  let totalPostRequests = 0;

  for (const tableName of tableOrder) {
    logger(`wiping table ${tableName}`);
    await client.deleteRowsInGroup(input.workspaceId, input.datasetId, tableName);

    const rows = input.tableRowsByName[tableName] ?? [];
    const batches = batchRows(rows, 10_000);

    for (let index = 0; index < batches.length; index += 1) {
      const batch = batches[index];
      await governor.waitForBudget({ rows: batch.length, postRequests: 1 });
      await client.postRowsInGroup(input.workspaceId, input.datasetId, tableName, batch);
      governor.record({ rows: batch.length, postRequests: 1 });

      totalRowsPosted += batch.length;
      totalPostRequests += 1;

      logger('posted batch', {
        tableName,
        batchNumber: index + 1,
        batchCount: batches.length,
        batchRows: batch.length,
        totalRowsPosted,
        totalPostRequests
      });
    }
  }

  return {
    tablesProcessed: tableOrder.length,
    totalRowsPosted,
    totalPostRequests
  };
}
