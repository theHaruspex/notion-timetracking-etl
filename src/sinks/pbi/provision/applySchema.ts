import type { PbiDatasetSpec } from '../spec/types.js';
import { validateSpec } from '../spec/validateSpec.js';
import { PowerBiClient } from '../client/powerbiClient.js';
import { ensureDataset } from './ensureDataset.js';
import { schemaDiff } from './schemaDiff.js';
import type { DatasetRegistryConfig } from '../state/datasetRegistry.js';

export interface ApplySchemaInput {
  workspaceId: string;
  datasetName: string;
  spec: PbiDatasetSpec;
}

export async function applySchema(
  client: PowerBiClient,
  registryConfig: DatasetRegistryConfig,
  input: ApplySchemaInput
): Promise<{ datasetId: string; changesApplied: boolean }> {
  validateSpec(input.spec);

  const datasetId = await ensureDataset(client, registryConfig, input);
  const existingTables = await client.getTablesInGroup(input.workspaceId, datasetId);
  const diff = schemaDiff(
    input.spec,
    existingTables.map((table) => ({ name: table.name }))
  );

  // Placeholder behavior: only upsert missing tables. Future work can include full table/column diffs.
  for (const tableName of diff.tablesToUpsert) {
    const desired = input.spec.tables.find((table) => table.name === tableName);
    if (!desired) {
      continue;
    }
    await client.putTable(datasetId, desired.name, desired.columns);
  }

  return {
    datasetId,
    changesApplied: diff.hasChanges
  };
}
