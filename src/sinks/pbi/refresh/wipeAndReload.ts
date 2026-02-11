import type { PbiDatasetSpec } from '../spec/types.js';
import { batchRows } from './batchRows.js';

export interface WipeAndReloadInput {
  workspaceId: string;
  datasetId: string;
  spec: PbiDatasetSpec;
  tableRowsByName: Record<string, object[]>;
}

export interface WipeAndReloadPlan {
  workspaceId: string;
  datasetId: string;
  tables: Array<{
    tableName: string;
    batches: object[][];
  }>;
}

export function wipeAndReload(input: WipeAndReloadInput): WipeAndReloadPlan {
  const specTableNames = new Set(input.spec.tables.map((table) => table.name));
  const inputTableNames = Object.keys(input.tableRowsByName);

  for (const tableName of inputTableNames) {
    if (!specTableNames.has(tableName)) {
      throw new Error(
        `wipeAndReload input contains unknown table "${tableName}". Ensure it exists in the dataset spec.`
      );
    }
  }

  const tables = inputTableNames.map((tableName) => ({
    tableName,
    batches: batchRows(input.tableRowsByName[tableName] ?? [], 10_000)
  }));

  // Planner-only helper. Use executeWipeAndReload() for strict execution (wipe + post).
  return {
    workspaceId: input.workspaceId,
    datasetId: input.datasetId,
    tables
  };
}
