import type { PbiDatasetSpec } from '../spec/types.js';
import { PowerBiClient } from '../client/powerbiClient.js';
import {
  findDatasetId,
  loadRegistry,
  saveRegistry,
  upsertEntry,
  type DatasetRegistryConfig
} from '../state/datasetRegistry.js';

export interface EnsureDatasetInput {
  workspaceId: string;
  datasetName: string;
  spec: PbiDatasetSpec;
}

export async function ensureDataset(
  client: PowerBiClient,
  registryConfig: DatasetRegistryConfig,
  input: EnsureDatasetInput
): Promise<string> {
  const registry = await loadRegistry(registryConfig);
  const knownId = findDatasetId(registry, {
    workspaceId: input.workspaceId,
    datasetName: input.datasetName
  });

  if (knownId) {
    return knownId;
  }

  const datasets = await client.getDatasetsInGroup(input.workspaceId);
  const existing = datasets.find(
    (dataset) => dataset.name.toLowerCase() === input.datasetName.toLowerCase()
  );

  if (existing) {
    upsertEntry(registry, {
      workspaceId: input.workspaceId,
      datasetName: input.datasetName,
      datasetId: existing.id
    });
    await saveRegistry(registryConfig, registry);
    return existing.id;
  }

  const created = await client.createDatasetInGroup(input.workspaceId, input.spec);
  upsertEntry(registry, {
    workspaceId: input.workspaceId,
    datasetName: input.datasetName,
    datasetId: created.id
  });
  await saveRegistry(registryConfig, registry);
  return created.id;
}
