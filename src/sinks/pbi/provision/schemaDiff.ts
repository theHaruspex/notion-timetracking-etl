import type { PbiDatasetSpec } from '../spec/types.js';

export interface SchemaDiffResult {
  hasChanges: boolean;
  tablesToUpsert: string[];
  notes: string[];
}

export function schemaDiff(
  desired: PbiDatasetSpec,
  existing: Array<{ name: string }>
): SchemaDiffResult {
  const existingSet = new Set(existing.map((table) => table.name.toLowerCase()));
  const tablesToUpsert = desired.tables
    .filter((table) => !existingSet.has(table.name.toLowerCase()))
    .map((table) => table.name);

  return {
    hasChanges: tablesToUpsert.length > 0,
    tablesToUpsert,
    notes:
      tablesToUpsert.length > 0
        ? ['Detected tables missing in existing dataset.']
        : ['No schema changes detected (placeholder diff logic).']
  };
}
