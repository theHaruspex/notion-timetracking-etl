import path from 'node:path';
import { readFile, writeFile } from 'node:fs/promises';
import { ensureDir } from '../../../lib/fs.js';

export interface DatasetRegistryEntry {
  groupId: string;
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
    const parsed = JSON.parse(raw) as { entries?: Array<Record<string, unknown>> };
    const entries = Array.isArray(parsed.entries) ? parsed.entries : [];
    const normalizedEntries: DatasetRegistryEntry[] = [];
    for (const entry of entries) {
      const groupIdRaw = entry.groupId ?? entry.workspaceId;
      const datasetNameRaw = entry.datasetName;
      const datasetIdRaw = entry.datasetId;
      if (
        typeof groupIdRaw !== 'string' ||
        typeof datasetNameRaw !== 'string' ||
        typeof datasetIdRaw !== 'string'
      ) {
        continue;
      }
      normalizedEntries.push({
        groupId: groupIdRaw,
        datasetName: datasetNameRaw,
        datasetId: datasetIdRaw,
        createdAt: typeof entry.createdAt === 'string' ? entry.createdAt : new Date().toISOString(),
        updatedAt: typeof entry.updatedAt === 'string' ? entry.updatedAt : new Date().toISOString(),
        lastAppliedSchemaHash:
          typeof entry.lastAppliedSchemaHash === 'string' ? entry.lastAppliedSchemaHash : undefined
      });
    }
    return { entries: normalizedEntries };
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
  input: { groupId: string; datasetName: string }
): string | null {
  const match = registry.entries.find(
    (entry) =>
      entry.groupId === input.groupId &&
      entry.datasetName.toLowerCase() === input.datasetName.toLowerCase()
  );
  return match?.datasetId ?? null;
}

export function findMostRecentEntryForGroup(
  registry: DatasetRegistryFile,
  input: { groupId: string }
): DatasetRegistryEntry | null {
  const candidates = registry.entries.filter((entry) => entry.groupId === input.groupId);
  if (candidates.length === 0) {
    return null;
  }

  const toTimestamp = (value: string | undefined): number => {
    if (!value) {
      return 0;
    }
    const parsed = Date.parse(value);
    return Number.isFinite(parsed) ? parsed : 0;
  };

  const sorted = [...candidates].sort((a, b) => {
    const aUpdated = toTimestamp(a.updatedAt);
    const bUpdated = toTimestamp(b.updatedAt);
    if (bUpdated !== aUpdated) {
      return bUpdated - aUpdated;
    }
    const aCreated = toTimestamp(a.createdAt);
    const bCreated = toTimestamp(b.createdAt);
    if (bCreated !== aCreated) {
      return bCreated - aCreated;
    }
    return b.datasetName.localeCompare(a.datasetName);
  });

  return sorted[0] ?? null;
}

export function upsertEntry(
  registry: DatasetRegistryFile,
  input: {
    groupId: string;
    datasetName: string;
    datasetId: string;
    lastAppliedSchemaHash?: string;
  }
): DatasetRegistryFile {
  const now = new Date().toISOString();
  const index = registry.entries.findIndex(
    (entry) =>
      entry.groupId === input.groupId &&
      entry.datasetName.toLowerCase() === input.datasetName.toLowerCase()
  );

  if (index === -1) {
    registry.entries.push({
      groupId: input.groupId,
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
