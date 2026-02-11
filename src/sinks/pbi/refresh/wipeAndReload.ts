import type { PbiDatasetSpec } from '../spec/types.js';
import { batchRows } from './batchRows.js';

export interface WipeAndReloadInput {
  groupId: string;
  datasetId: string;
  spec: PbiDatasetSpec;
  tableRowsByName: Record<string, object[]>;
}

export interface WipeAndReloadPlan {
  groupId: string;
  datasetId: string;
  tables: Array<{
    tableName: string;
    batches: object[][];
  }>;
}

export function wipeAndReload(input: WipeAndReloadInput): WipeAndReloadPlan {
  const specTableNames = input.spec.tables.map((table) => table.name);
  const specTableNameSet = new Set(specTableNames);
  const inputTableNames = Object.keys(input.tableRowsByName);
  const missingTables = specTableNames.filter(
    (tableName) => !Object.prototype.hasOwnProperty.call(input.tableRowsByName, tableName)
  );
  const extraTables = inputTableNames.filter((tableName) => !specTableNameSet.has(tableName));
  if (missingTables.length > 0 || extraTables.length > 0) {
    throw new Error(
      `wipeAndReload table set mismatch. Missing: ${
        missingTables.join(', ') || 'none'
      }. Extra: ${extraTables.join(', ') || 'none'}.`
    );
  }

  const tables = specTableNames.map((tableName) => ({
    tableName,
    batches: batchRows(input.tableRowsByName[tableName] ?? [], 10_000)
  }));

  // Planner-only helper. Use executeWipeAndReload() for strict execution (wipe + post).
  return {
    groupId: input.groupId,
    datasetId: input.datasetId,
    tables
  };
}
