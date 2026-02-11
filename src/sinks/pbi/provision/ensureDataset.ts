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
  groupId: string;
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
    groupId: input.groupId,
    datasetName: input.datasetName
  });

  if (knownId) {
    return knownId;
  }

  const datasets = await client.getDatasetsInGroup(input.groupId);
  const existing = datasets.find(
    (dataset) => dataset.name.toLowerCase() === input.datasetName.toLowerCase()
  );

  if (existing) {
    upsertEntry(registry, {
      groupId: input.groupId,
      datasetName: input.datasetName,
      datasetId: existing.id
    });
    await saveRegistry(registryConfig, registry);
    return existing.id;
  }

  const created = await client.createDatasetInGroup(input.groupId, input.spec);
  upsertEntry(registry, {
    groupId: input.groupId,
    datasetName: input.datasetName,
    datasetId: created.id
  });
  await saveRegistry(registryConfig, registry);
  return created.id;
}
