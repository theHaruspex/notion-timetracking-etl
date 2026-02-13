#!/usr/bin/env tsx
/**
 * Lightweight local validation for Level 3 wiring:
 * Reports non-null coverage of workflow_instance_page_name in latest canon timeslices.
 */

import { readFileSync, readdirSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const rootDir = join(__dirname, '..');

type CanonTimeslice = {
  timeslice_id: string;
  workflow_instance_page_name?: string | null;
};

function findLatestCanonDate(dataset: string): string {
  const datasetDir = join(rootDir, 'data', 'canon', dataset);
  const dates = readdirSync(datasetDir, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .filter((name) => /^\d{4}-\d{2}-\d{2}$/.test(name))
    .sort()
    .reverse();
  return dates[0] ?? '';
}

function readCanonTimeslices(date: string): CanonTimeslice[] {
  const filePath = join(rootDir, 'data', 'canon', 'timeslices', date, 'records.jsonl');
  const content = readFileSync(filePath, 'utf8');
  return content
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line) as CanonTimeslice);
}

function main(): void {
  const date = findLatestCanonDate('timeslices');
  if (!date) {
    console.log('No canon timeslices dataset found.');
    process.exitCode = 1;
    return;
  }

  const timeslices = readCanonTimeslices(date);
  const withLabel = timeslices.filter(
    (item) =>
      typeof item.workflow_instance_page_name === 'string' &&
      item.workflow_instance_page_name.trim().length > 0
  );
  const pct = timeslices.length === 0 ? 0 : (withLabel.length / timeslices.length) * 100;

  console.log(`canon_date=${date}`);
  console.log(`timeslices_total=${timeslices.length}`);
  console.log(`workflow_instance_page_name_non_null=${withLabel.length}`);
  console.log(`workflow_instance_page_name_non_null_pct=${pct.toFixed(2)}%`);
  console.log('sample_values=');
  for (const sample of withLabel.slice(0, 3)) {
    console.log(`- ${sample.timeslice_id}: ${sample.workflow_instance_page_name}`);
  }
}

main();
