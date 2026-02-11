import fs from 'node:fs/promises';
import path from 'node:path';

type CanonTimeslice = {
  from_step_id?: string | null;
  to_step_id?: string | null;
};

type CanonWorkflowStage = {
  workflow_stage_id?: string | null;
  source_page_id?: string | null;
};

const UUID_RE =
  /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i;

async function main(): Promise<void> {
  const date = getDateArg(process.argv.slice(2)) ?? localDateStamp();
  const repoRoot = process.cwd();
  const canonTimeslicesPath = path.join(repoRoot, 'data', 'canon', 'timeslices', date, 'records.jsonl');
  const canonWorkflowStagesPath = path.join(
    repoRoot,
    'data',
    'canon',
    'workflow_stages',
    date,
    'records.jsonl'
  );
  const rawTimeslicesPath = path.join(repoRoot, 'data', 'raw', 'timeslices', date, 'records.jsonl');

  const [timeslices, workflowStages] = await Promise.all([
    readJsonl<CanonTimeslice>(canonTimeslicesPath),
    readJsonl<CanonWorkflowStage>(canonWorkflowStagesPath)
  ]);

  const stageCanonIdSet = new Set<string>();
  const stagePageIdSet = new Set<string>();
  for (const stage of workflowStages) {
    if (typeof stage.workflow_stage_id === 'string' && stage.workflow_stage_id.length > 0) {
      stageCanonIdSet.add(stage.workflow_stage_id);
    }
    if (typeof stage.source_page_id === 'string' && stage.source_page_id.length > 0) {
      stagePageIdSet.add(stage.source_page_id.toLowerCase());
    }
  }

  let timeslicesWithFromStepId = 0;
  let timeslicesWithToStepId = 0;
  const fromStepIds = new Set<string>();
  const toStepIds = new Set<string>();
  for (const timeslice of timeslices) {
    if (typeof timeslice.from_step_id === 'string' && timeslice.from_step_id.length > 0) {
      timeslicesWithFromStepId += 1;
      fromStepIds.add(timeslice.from_step_id);
    }
    if (typeof timeslice.to_step_id === 'string' && timeslice.to_step_id.length > 0) {
      timeslicesWithToStepId += 1;
      toStepIds.add(timeslice.to_step_id);
    }
  }

  const resolvedFromUnique = intersectionSize(fromStepIds, stageCanonIdSet);
  const resolvedToUnique = intersectionSize(toStepIds, stageCanonIdSet);
  const unresolvedFromUnique = fromStepIds.size - resolvedFromUnique;
  const unresolvedToUnique = toStepIds.size - resolvedToUnique;

  const rawRelationScan = await scanRawTimesliceRelationUuids(rawTimeslicesPath, stagePageIdSet);

  const report = {
    generatedAt: new Date().toISOString(),
    date,
    files: {
      canonTimeslices: path.relative(repoRoot, canonTimeslicesPath),
      canonWorkflowStages: path.relative(repoRoot, canonWorkflowStagesPath),
      rawTimeslices: rawRelationScan.fileFound ? path.relative(repoRoot, rawTimeslicesPath) : null
    },
    counts: {
      workflowStagesTotal: workflowStages.length,
      stageCanonIdCount: stageCanonIdSet.size,
      stagePageIdCount: stagePageIdSet.size,
      timeslicesTotal: timeslices.length,
      timeslicesWithFromStepId,
      timeslicesWithToStepId,
      fromStepCanonicalIdUnique: fromStepIds.size,
      toStepCanonicalIdUnique: toStepIds.size,
      resolvedFromUnique,
      resolvedToUnique,
      unresolvedFromUnique,
      unresolvedToUnique
    },
    samples: {
      unresolvedFrom: sample(Array.from(difference(fromStepIds, stageCanonIdSet)), 20),
      unresolvedTo: sample(Array.from(difference(toStepIds, stageCanonIdSet)), 20)
    },
    rawRelationScan
  };

  const outputDir = path.join(repoRoot, 'data', 'state', 'diagnostics');
  await fs.mkdir(outputDir, { recursive: true });
  const outputPath = path.join(outputDir, `stage-relation-diagnosis.${date}.json`);
  await fs.writeFile(outputPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');

  console.log(`[diagnose-stage-relations] wrote ${path.relative(repoRoot, outputPath)}`);
  console.log(
    `[diagnose-stage-relations] resolvedFromUnique=${resolvedFromUnique}, resolvedToUnique=${resolvedToUnique}`
  );
}

function getDateArg(args: string[]): string | null {
  for (let i = 0; i < args.length; i += 1) {
    if (args[i] === '--date' && args[i + 1]) {
      return args[i + 1];
    }
    if (args[i].startsWith('--date=')) {
      return args[i].slice('--date='.length);
    }
  }
  return null;
}

function localDateStamp(date = new Date()): string {
  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, '0');
  const dd = String(date.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

async function readJsonl<T>(filePath: string): Promise<T[]> {
  const content = await fs.readFile(filePath, 'utf8');
  const lines = content
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  const rows: T[] = [];
  for (const line of lines) {
    try {
      rows.push(JSON.parse(line) as T);
    } catch {
      // Intentionally ignore malformed lines in diagnostics.
    }
  }
  return rows;
}

function intersectionSize(a: Set<string>, b: Set<string>): number {
  const [small, large] = a.size <= b.size ? [a, b] : [b, a];
  let count = 0;
  for (const value of small) {
    if (large.has(value)) {
      count += 1;
    }
  }
  return count;
}

function difference(a: Set<string>, b: Set<string>): Set<string> {
  const out = new Set<string>();
  for (const value of a) {
    if (!b.has(value)) {
      out.add(value);
    }
  }
  return out;
}

function sample<T>(values: T[], max: number): T[] {
  return values.slice(0, max);
}

async function scanRawTimesliceRelationUuids(
  rawTimeslicesPath: string,
  stagePageIdSet: Set<string>
): Promise<{
  fileFound: boolean;
  relationUuidTotal: number;
  relationUuidUnique: number;
  relationUuidsMatchingStagePageIds: number;
  relationUuidsNotMatchingStagePageIds: number;
  sampleMatching: string[];
  sampleNotMatching: string[];
}> {
  try {
    await fs.access(rawTimeslicesPath);
  } catch {
    return {
      fileFound: false,
      relationUuidTotal: 0,
      relationUuidUnique: 0,
      relationUuidsMatchingStagePageIds: 0,
      relationUuidsNotMatchingStagePageIds: 0,
      sampleMatching: [],
      sampleNotMatching: []
    };
  }

  const rawRecords = await readJsonl<unknown>(rawTimeslicesPath);
  let relationUuidTotal = 0;
  const unique = new Set<string>();
  for (const record of rawRecords) {
    const ids = extractRelationUuidsFromUnknown(record);
    relationUuidTotal += ids.length;
    for (const id of ids) {
      unique.add(id.toLowerCase());
    }
  }

  const matching: string[] = [];
  const notMatching: string[] = [];
  for (const id of unique) {
    if (stagePageIdSet.has(id)) {
      matching.push(id);
    } else {
      notMatching.push(id);
    }
  }

  return {
    fileFound: true,
    relationUuidTotal,
    relationUuidUnique: unique.size,
    relationUuidsMatchingStagePageIds: matching.length,
    relationUuidsNotMatchingStagePageIds: notMatching.length,
    sampleMatching: sample(matching, 20),
    sampleNotMatching: sample(notMatching, 20)
  };
}

function extractRelationUuidsFromUnknown(value: unknown): string[] {
  const out: string[] = [];
  walk(value, out);
  return out;
}

function walk(value: unknown, out: string[]): void {
  if (Array.isArray(value)) {
    for (const item of value) {
      walk(item, out);
    }
    return;
  }

  if (!value || typeof value !== 'object') {
    return;
  }

  const rec = value as Record<string, unknown>;

  // Capture common relation shape: { relation: [{ id: "uuid" }] }
  if (Array.isArray(rec.relation)) {
    for (const item of rec.relation) {
      if (item && typeof item === 'object') {
        const itemRec = item as Record<string, unknown>;
        const maybeId = typeof itemRec.id === 'string' ? itemRec.id : null;
        if (maybeId && UUID_RE.test(maybeId)) {
          out.push(maybeId.toLowerCase());
        }
      }
    }
  }

  for (const nested of Object.values(rec)) {
    walk(nested, out);
  }
}

main().catch((error) => {
  console.error('[diagnose-stage-relations] failed');
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
