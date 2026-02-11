import path from 'node:path';
import { readJsonl, listFiles, listSubdirs } from '../lib/fs.js';
import type { RawRecord } from '../ingress/rawRecord.js';

export async function latestDatasetDateDir(baseRawDir: string, dataset: string): Promise<string | null> {
  const datasetPath = path.join(baseRawDir, dataset);
  const dirs = await listSubdirs(datasetPath);
  if (dirs.length === 0) {
    return null;
  }
  return dirs[dirs.length - 1] ?? null;
}

export async function readRawDatasetForDate(
  baseRawDir: string,
  dataset: string,
  dateDir: string
): Promise<RawRecord[]> {
  return readDatasetJsonlForDate<RawRecord>(baseRawDir, dataset, dateDir);
}

export async function readDatasetJsonlForDate<T>(
  baseDir: string,
  dataset: string,
  dateDir: string
): Promise<T[]> {
  const targetDir = path.join(baseDir, dataset, dateDir);
  const files = await listFiles(targetDir);
  const jsonlFiles = files.filter((file) => file.endsWith('.jsonl'));

  const all: T[] = [];
  for (const file of jsonlFiles) {
    const records = await readJsonl<T>(path.join(targetDir, file));
    all.push(...records);
  }

  return all;
}
