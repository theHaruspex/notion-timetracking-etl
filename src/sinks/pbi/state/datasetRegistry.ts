import path from 'node:path';
import { readFile, writeFile } from 'node:fs/promises';
import { ensureDir } from '../../../lib/fs.js';

export interface DatasetRegistryEntry {
  workspaceId: string;
  datasetName: string;
  datasetId: string;
  createdAt: string;
  updatedAt: string;
  lastAppliedSchemaHash?: string;
}

export interface DatasetRegistryFile {
  entries: DatasetRegistryEntry[];
}

export interface DatasetRegistryConfig {
  resolvedDataDir: string;
}

function registryPath(config: DatasetRegistryConfig): string {
  return path.join(config.resolvedDataDir, 'state', 'pbi-dataset-registry.json');
}

export async function loadRegistry(config: DatasetRegistryConfig): Promise<DatasetRegistryFile> {
  const filePath = registryPath(config);

  try {
    const raw = await readFile(filePath, 'utf8');
    const parsed = JSON.parse(raw) as DatasetRegistryFile;
    return {
      entries: Array.isArray(parsed.entries) ? parsed.entries : []
    };
  } catch {
    return { entries: [] };
  }
}

export async function saveRegistry(
  config: DatasetRegistryConfig,
  registry: DatasetRegistryFile
): Promise<void> {
  const filePath = registryPath(config);
  await ensureDir(path.dirname(filePath));
  await writeFile(filePath, `${JSON.stringify(registry, null, 2)}\n`, 'utf8');
}

export function findDatasetId(
  registry: DatasetRegistryFile,
  input: { workspaceId: string; datasetName: string }
): string | null {
  const match = registry.entries.find(
    (entry) =>
      entry.workspaceId === input.workspaceId &&
      entry.datasetName.toLowerCase() === input.datasetName.toLowerCase()
  );
  return match?.datasetId ?? null;
}

export function upsertEntry(
  registry: DatasetRegistryFile,
  input: {
    workspaceId: string;
    datasetName: string;
    datasetId: string;
    lastAppliedSchemaHash?: string;
  }
): DatasetRegistryFile {
  const now = new Date().toISOString();
  const index = registry.entries.findIndex(
    (entry) =>
      entry.workspaceId === input.workspaceId &&
      entry.datasetName.toLowerCase() === input.datasetName.toLowerCase()
  );

  if (index === -1) {
    registry.entries.push({
      workspaceId: input.workspaceId,
      datasetName: input.datasetName,
      datasetId: input.datasetId,
      createdAt: now,
      updatedAt: now,
      lastAppliedSchemaHash: input.lastAppliedSchemaHash
    });
    return registry;
  }

  const existing = registry.entries[index];
  registry.entries[index] = {
    ...existing,
    datasetId: input.datasetId,
    updatedAt: now,
    lastAppliedSchemaHash: input.lastAppliedSchemaHash ?? existing?.lastAppliedSchemaHash
  };
  return registry;
}

export function getDatasetRegistryPath(config: DatasetRegistryConfig): string {
  return registryPath(config);
}
