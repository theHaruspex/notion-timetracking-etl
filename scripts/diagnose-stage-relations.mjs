#!/usr/bin/env node

import { promises as fs } from 'node:fs';
import path from 'node:path';

const DATE_DIR_RE = /^\d{4}-\d{2}-\d{2}$/;
const UUID_HYPHENATED_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const MAX_SAMPLE = 20;
const MAX_RELATION_UUIDS_PER_TIMESLICE = 20;

async function main() {
  const repoRoot = process.cwd();
  const canonRoot = path.join(repoRoot, 'data', 'canon');
  const diagnosticsDir = path.join(repoRoot, 'data', 'state', 'diagnostics');

  await assertPathExists(canonRoot, 'data/canon folder not found. Run normalize first.');

  const allFiles = await walkFiles(canonRoot);
  const jsonlFiles = allFiles.filter((filePath) => filePath.toLowerCase().endsWith('.jsonl'));
  if (jsonlFiles.length === 0) {
    throw new Error(`No JSONL files found under ${canonRoot}`);
  }

  const latestCanonDate = findLatestCanonDateFromPaths(jsonlFiles, canonRoot);
  if (!latestCanonDate) {
    throw new Error(
      'Could not determine latest canon date folder (expected YYYY-MM-DD path component).'
    );
  }

  const candidateFiles = jsonlFiles.filter((filePath) =>
    path
      .relative(canonRoot, filePath)
      .split(path.sep)
      .some((part) => part === latestCanonDate)
  );
  if (candidateFiles.length === 0) {
    throw new Error(`No JSONL files found for canon date ${latestCanonDate}`);
  }

  const timeslicesFile = await chooseDatasetFile(candidateFiles, canonRoot, 'timeslices');
  const workflowStagesFile = await chooseDatasetFile(candidateFiles, canonRoot, 'workflowStages');

  const workflowStages = await readJsonl(workflowStagesFile);
  const timeslices = await readJsonl(timeslicesFile);

  const stageCanonIdSet = new Set();
  const stagePageIdSet = new Set();
  const stageCanonIdByPageId = new Map();
  for (const stage of workflowStages) {
    const canonId = asNonEmptyString(stage?.workflow_stage_id);
    const pageId = normalizeHyphenatedUuid(asNonEmptyString(stage?.source_page_id));
    if (canonId) {
      stageCanonIdSet.add(canonId);
    }
    if (pageId) {
      stagePageIdSet.add(pageId);
      if (canonId) {
        stageCanonIdByPageId.set(pageId, canonId);
      }
    }
  }

  // Also load raw workflow stages to analyze database structure
  const rawRoot = path.join(repoRoot, 'data', 'raw');
  const rawWorkflowStagesFile = await findRawWorkflowStagesFile(rawRoot, latestCanonDate);
  const rawWorkflowStages = rawWorkflowStagesFile ? await readJsonl(rawWorkflowStagesFile) : [];

  let timeslicesWithFrom = 0;
  let timeslicesWithTo = 0;
  const fromStepCanonIdSet = new Set();
  const toStepCanonIdSet = new Set();

  const relationUuidUniqueSet = new Set();
  let relationUuidCount = 0;

  for (const timeslice of timeslices) {
    const fromStepId = asNonEmptyString(timeslice?.from_step_id);
    const toStepId = asNonEmptyString(timeslice?.to_step_id);

    if (fromStepId) {
      timeslicesWithFrom += 1;
      fromStepCanonIdSet.add(fromStepId);
    }
    if (toStepId) {
      timeslicesWithTo += 1;
      toStepCanonIdSet.add(toStepId);
    }

    const attributes = isRecord(timeslice?.attributes) ? timeslice.attributes : {};
    let extractedForTimeslice = 0;
    for (const rawValue of Object.values(attributes)) {
      if (extractedForTimeslice >= MAX_RELATION_UUIDS_PER_TIMESLICE) {
        break;
      }
      const maybeUuid = extractFirstRelationUuid(rawValue);
      if (!maybeUuid) {
        continue;
      }
      relationUuidCount += 1;
      relationUuidUniqueSet.add(maybeUuid);
      extractedForTimeslice += 1;
    }
  }

  const allStepCanonIdSet = unionSets(fromStepCanonIdSet, toStepCanonIdSet);
  const resolvedFrom = intersectionSet(fromStepCanonIdSet, stageCanonIdSet);
  const resolvedTo = intersectionSet(toStepCanonIdSet, stageCanonIdSet);
  const unresolvedFrom = differenceSet(fromStepCanonIdSet, stageCanonIdSet);
  const unresolvedTo = differenceSet(toStepCanonIdSet, stageCanonIdSet);

  const relationUuidsThatAreStages = intersectionSet(relationUuidUniqueSet, stagePageIdSet);
  const relationUuidsNotStages = differenceSet(relationUuidUniqueSet, stagePageIdSet);

  const expectedStageCanonIdsFromMatchingRelations = new Set(
    Array.from(relationUuidsThatAreStages).map((uuid) => stableEntityId('workflow_stage', uuid))
  );
  const expectedCanonIdsSeenInTimeslices = intersectionSet(
    expectedStageCanonIdsFromMatchingRelations,
    allStepCanonIdSet
  );

  // Analyze workflow stages database structure (after sets are built)
  let workflowStagesDbAnalysis = analyzeWorkflowStagesDatabase(rawWorkflowStages, {
    stagePageIdSet,
    unresolvedFrom,
    unresolvedTo
  });
  if (workflowStagesDbAnalysis && rawWorkflowStagesFile) {
    workflowStagesDbAnalysis.rawFile = path.relative(repoRoot, rawWorkflowStagesFile);
  }

  const hypothesis = scoreHypothesis({
    resolvedFromCount: resolvedFrom.size,
    resolvedToCount: resolvedTo.size,
    relationStageMatchesCount: relationUuidsThatAreStages.size,
    relationUuidUniqueCount: relationUuidUniqueSet.size
  });

  const result = {
    generatedAt: new Date().toISOString(),
    canonDate: latestCanonDate,
    selectedFiles: {
      timeslices: path.relative(repoRoot, timeslicesFile),
      workflowStages: path.relative(repoRoot, workflowStagesFile)
    },
    counts: {
      workflowStagesTotal: workflowStages.length,
      stageCanonIdCount: stageCanonIdSet.size,
      stagePageIdCount: stagePageIdSet.size,
      timeslicesTotal: timeslices.length,
      timeslicesWithFromStepId: timeslicesWithFrom,
      timeslicesWithToStepId: timeslicesWithTo,
      fromStepCanonIdUnique: fromStepCanonIdSet.size,
      toStepCanonIdUnique: toStepCanonIdSet.size,
      allStepCanonIdUnique: allStepCanonIdSet.size,
      resolvedFromUnique: resolvedFrom.size,
      resolvedToUnique: resolvedTo.size,
      unresolvedFromUnique: unresolvedFrom.size,
      unresolvedToUnique: unresolvedTo.size,
      relationUuidCount,
      relationUuidUnique: relationUuidUniqueSet.size,
      relationUuidsThatAreStages: relationUuidsThatAreStages.size,
      relationUuidsNotStages: relationUuidsNotStages.size,
      expectedStageCanonIdsFromMatchingRelations: expectedStageCanonIdsFromMatchingRelations.size,
      expectedStageCanonIdsSeenInTimeslices: expectedCanonIdsSeenInTimeslices.size
    },
    samples: {
      unresolvedFrom: sampleSet(unresolvedFrom),
      unresolvedTo: sampleSet(unresolvedTo),
      relationUuidsThatAreStages: sampleSet(relationUuidsThatAreStages),
      relationUuidsNotStages: sampleSet(relationUuidsNotStages),
      expectedStageCanonIdsFromMatchingRelations: sampleSet(expectedStageCanonIdsFromMatchingRelations),
      expectedStageCanonIdsSeenInTimeslices: sampleSet(expectedCanonIdsSeenInTimeslices)
    },
    workflowStagesDatabase: workflowStagesDbAnalysis,
    hypothesis
  };

  printReport({
    canonRoot,
    latestCanonDate,
    selectedFiles: result.selectedFiles,
    result
  });

  await fs.mkdir(diagnosticsDir, { recursive: true });
  const outputPath = path.join(
    diagnosticsDir,
    `stage-relation-diagnosis.${latestCanonDate}.json`
  );
  await fs.writeFile(outputPath, `${JSON.stringify(result, null, 2)}\n`, 'utf8');
  console.log(`\nJSON report written: ${path.relative(repoRoot, outputPath)}`);
}

function isRecord(value) {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function asNonEmptyString(value) {
  return typeof value === 'string' && value.trim().length > 0 ? value.trim() : null;
}

function normalizeHyphenatedUuid(value) {
  if (!value) {
    return null;
  }
  const normalized = value.toLowerCase();
  return UUID_HYPHENATED_RE.test(normalized) ? normalized : null;
}

function stableEntityId(prefix, notionId) {
  if (!notionId) {
    return `${prefix}_unknown`;
  }
  return `${prefix}_${notionId.replace(/-/g, '').toLowerCase()}`;
}

async function assertPathExists(targetPath, message) {
  try {
    await fs.access(targetPath);
  } catch {
    throw new Error(message);
  }
}

async function walkFiles(root) {
  const output = [];
  const stack = [root];
  while (stack.length > 0) {
    const current = stack.pop();
    const entries = await fs.readdir(current, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = path.join(current, entry.name);
      if (entry.isDirectory()) {
        stack.push(fullPath);
      } else if (entry.isFile()) {
        output.push(fullPath);
      }
    }
  }
  return output;
}

function findLatestCanonDateFromPaths(files, canonRoot) {
  const dates = new Set();
  for (const filePath of files) {
    const relative = path.relative(canonRoot, filePath);
    const parts = relative.split(path.sep);
    for (const part of parts) {
      if (DATE_DIR_RE.test(part)) {
        dates.add(part);
      }
    }
  }
  return Array.from(dates).sort().at(-1) ?? null;
}

async function chooseDatasetFile(candidateFiles, canonRoot, datasetType) {
  const scored = [];
  for (const filePath of candidateFiles) {
    const relative = path.relative(canonRoot, filePath);
    const label = relative.toLowerCase();
    const score = scoreDatasetMatch(label, datasetType);
    if (score <= 0) {
      continue;
    }
    const stat = await fs.stat(filePath);
    scored.push({ filePath, score, size: stat.size, relative });
  }

  if (scored.length === 0) {
    throw new Error(`Could not discover ${datasetType} JSONL file for latest canon date.`);
  }

  scored.sort((a, b) => {
    if (b.score !== a.score) {
      return b.score - a.score;
    }
    if (b.size !== a.size) {
      return b.size - a.size;
    }
    return a.relative.localeCompare(b.relative);
  });

  const selected = scored[0];
  console.log(
    `[file-select] ${datasetType}: ${selected.relative} (score=${selected.score}, size=${selected.size})`
  );
  return selected.filePath;
}

function scoreDatasetMatch(label, datasetType) {
  if (datasetType === 'timeslices') {
    if (!label.includes('timeslice')) {
      return 0;
    }
    let score = 10;
    if (label.includes('records.jsonl')) {
      score += 1;
    }
    return score;
  }

  if (datasetType === 'workflowStages') {
    let score = 0;
    if (label.includes('workflowstage')) {
      score = Math.max(score, 12);
    }
    if (label.includes('workflow_stage')) {
      score = Math.max(score, 12);
    }
    if (label.includes('workflow') && label.includes('stage')) {
      score = Math.max(score, 10);
    }
    if (label.includes('records.jsonl') && score > 0) {
      score += 1;
    }
    return score;
  }

  return 0;
}

async function readJsonl(filePath) {
  const text = await fs.readFile(filePath, 'utf8');
  const lines = text
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  const output = [];
  for (const line of lines) {
    try {
      output.push(JSON.parse(line));
    } catch {
      // Ignore malformed lines for one-off diagnostics.
    }
  }
  return output;
}

function extractFirstRelationUuid(rawValue) {
  if (!isRecord(rawValue)) {
    return null;
  }

  // direct relation property shape
  const relationCandidate = firstRelationIdFromRelationArray(rawValue.relation);
  if (relationCandidate) {
    return relationCandidate;
  }

  // rollup -> array -> relation shape
  const rollup = rawValue.rollup;
  if (isRecord(rollup) && Array.isArray(rollup.array)) {
    for (const item of rollup.array) {
      if (!isRecord(item)) {
        continue;
      }
      const nestedRelationCandidate = firstRelationIdFromRelationArray(item.relation);
      if (nestedRelationCandidate) {
        return nestedRelationCandidate;
      }
    }
  }

  return null;
}

function firstRelationIdFromRelationArray(relationArray) {
  if (!Array.isArray(relationArray) || relationArray.length === 0) {
    return null;
  }
  const rawId = relationArray[0]?.id;
  const id = asNonEmptyString(rawId);
  return normalizeHyphenatedUuid(id);
}

function unionSets(a, b) {
  const out = new Set(a);
  for (const value of b) {
    out.add(value);
  }
  return out;
}

function intersectionSet(a, b) {
  const out = new Set();
  const [small, large] = a.size <= b.size ? [a, b] : [b, a];
  for (const value of small) {
    if (large.has(value)) {
      out.add(value);
    }
  }
  return out;
}

function differenceSet(a, b) {
  const out = new Set();
  for (const value of a) {
    if (!b.has(value)) {
      out.add(value);
    }
  }
  return out;
}

function sampleSet(setLike) {
  return Array.from(setLike).slice(0, MAX_SAMPLE);
}

function scoreHypothesis(input) {
  const { resolvedFromCount, resolvedToCount, relationStageMatchesCount, relationUuidUniqueCount } = input;

  if (
    resolvedFromCount === 0 &&
    resolvedToCount === 0 &&
    relationStageMatchesCount <= Math.max(1, Math.floor(relationUuidUniqueCount * 0.01))
  ) {
    return {
      classification: 'likely_wrong_relation_properties_or_entity_domain',
      explanation:
        'No canonical stage resolution and almost no relation UUID overlap with workflow stage page IDs. Most likely from/to relation properties point to a different entity/database, or schema drift broke configured property IDs.'
    };
  }

  if (resolvedFromCount === 0 && resolvedToCount === 0 && relationStageMatchesCount > 0) {
    return {
      classification: 'likely_missing_or_partial_workflow_stage_ingestion',
      explanation:
        'No canonical stage resolution despite relation UUID overlap with known workflow stage page IDs. Likely referenced stages are missing from workflow stage ingestion or split across multiple stage sources.'
    };
  }

  if (resolvedFromCount > 0 || resolvedToCount > 0) {
    return {
      classification: 'partial_overlap',
      explanation:
        'Some stage references resolve while many do not. Likely partial ingestion, mixed stage sources, or inconsistent canonicalization across datasets.'
    };
  }

  return {
    classification: 'undetermined',
    explanation:
      'Signal is inconclusive. Inspect relation UUID samples and verify configured property IDs and database lineage.'
  };
}

async function findRawWorkflowStagesFile(rawRoot, canonDate) {
  try {
    await assertPathExists(rawRoot, '');
  } catch {
    return null;
  }
  const allFiles = await walkFiles(rawRoot);
  const jsonlFiles = allFiles.filter((filePath) => filePath.toLowerCase().endsWith('.jsonl'));
  const candidateFiles = jsonlFiles.filter((filePath) =>
    path
      .relative(rawRoot, filePath)
      .split(path.sep)
      .some((part) => part === canonDate)
  );
  return await chooseDatasetFile(candidateFiles, rawRoot, 'workflowStages').catch(() => null);
}

function analyzeWorkflowStagesDatabase(rawStages, context) {
  if (!rawStages || rawStages.length === 0) {
    return null;
  }

  const pageRecords = rawStages.filter((r) => r.entityType === 'page' && r.pageId);
  if (pageRecords.length === 0) {
    return { totalPageRecords: 0 };
  }

  const databaseIds = new Set();
  const pageIds = new Set();
  const pageIdPrefixes = new Map();
  const propertyIds = new Set();
  const propertyNames = new Map();
  const pageIdToTitle = new Map();

  for (const record of pageRecords) {
    if (record.databaseId) {
      databaseIds.add(record.databaseId);
    }
    if (record.pageId) {
      const normalized = normalizeHyphenatedUuid(record.pageId);
      if (normalized) {
        pageIds.add(normalized);
        const prefix = normalized.substring(0, 8);
        pageIdPrefixes.set(prefix, (pageIdPrefixes.get(prefix) ?? 0) + 1);
      }
    }
    if (isRecord(record.properties)) {
      for (const [propId, prop] of Object.entries(record.properties)) {
        propertyIds.add(propId);
        if (isRecord(prop) && typeof prop.propertyName === 'string') {
          propertyNames.set(propId, prop.propertyName);
        }
        // Extract title from title property
        if (propId === 'title' || (isRecord(prop) && prop.type === 'title')) {
          const title = extractTitleFromRawValue(prop.rawValue ?? prop);
          if (title && record.pageId) {
            const normalized = normalizeHyphenatedUuid(record.pageId);
            if (normalized) {
              pageIdToTitle.set(normalized, title);
            }
          }
        }
      }
    }
  }

  // Extract page ID prefixes from unresolved canon IDs
  const unresolvedFromPrefixes = new Map();
  const unresolvedToPrefixes = new Map();
  for (const canonId of context.unresolvedFrom) {
    const match = /^workflow_stage_([0-9a-f]{8})/i.exec(canonId);
    if (match) {
      const prefix = match[1].toLowerCase();
      unresolvedFromPrefixes.set(prefix, (unresolvedFromPrefixes.get(prefix) ?? 0) + 1);
    }
  }
  for (const canonId of context.unresolvedTo) {
    const match = /^workflow_stage_([0-9a-f]{8})/i.exec(canonId);
    if (match) {
      const prefix = match[1].toLowerCase();
      unresolvedToPrefixes.set(prefix, (unresolvedToPrefixes.get(prefix) ?? 0) + 1);
    }
  }

  const unresolvedFromPrefixSet = new Set(unresolvedFromPrefixes.keys());
  const unresolvedToPrefixSet = new Set(unresolvedToPrefixes.keys());
  const workflowStagePrefixSet = new Set(pageIdPrefixes.keys());
  const fromOverlap = intersectionSet(unresolvedFromPrefixSet, workflowStagePrefixSet);
  const toOverlap = intersectionSet(unresolvedToPrefixSet, workflowStagePrefixSet);

  const samplePropertyNames = {};
  for (const [propId, name] of Array.from(propertyNames.entries()).slice(0, 15)) {
    samplePropertyNames[propId] = name;
  }

  // Build page ID prefix to titles mapping
  const prefixToTitles = new Map();
  for (const [pageId, title] of pageIdToTitle.entries()) {
    const prefix = pageId.substring(0, 8);
    const existing = prefixToTitles.get(prefix) ?? [];
    existing.push({ pageId, title });
    prefixToTitles.set(prefix, existing);
  }

  // Sample pages by prefix (for display)
  const samplePagesByPrefix = Array.from(prefixToTitles.entries())
    .sort((a, b) => {
      const aCount = pageIdPrefixes.get(a[0]) ?? 0;
      const bCount = pageIdPrefixes.get(b[0]) ?? 0;
      return bCount - aCount;
    })
    .slice(0, 10)
    .map(([prefix, pages]) => ({
      prefix,
      count: pageIdPrefixes.get(prefix) ?? 0,
      samplePages: pages.slice(0, 3).map((p) => ({ pageId: p.pageId, title: p.title }))
    }));

  return {
    rawFile: null, // Will be set by caller if needed
    databaseId: databaseIds.size === 1 ? Array.from(databaseIds)[0] : null,
    totalPageRecords: pageRecords.length,
    uniquePageIds: pageIds.size,
    pageIdPrefixes: Array.from(pageIdPrefixes.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 20)
      .map(([prefix, count]) => `${prefix} (${count})`),
    propertyIds: Array.from(propertyIds).sort(),
    samplePropertyNames,
    pageTitles: {
      totalWithTitles: pageIdToTitle.size,
      samplePagesByPrefix
    },
    unresolvedFromPageIdPrefixes: Array.from(unresolvedFromPrefixes.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 20)
      .map(([prefix, count]) => `${prefix} (${count})`),
    unresolvedToPageIdPrefixes: Array.from(unresolvedToPrefixes.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 20)
      .map(([prefix, count]) => `${prefix} (${count})`),
    prefixOverlapAnalysis: {
      unresolvedPrefixCount: unresolvedFromPrefixSet.size + unresolvedToPrefixSet.size,
      overlapCount: fromOverlap.size + toOverlap.size,
      fromOverlapPrefixes: sampleSet(fromOverlap),
      toOverlapPrefixes: sampleSet(toOverlap)
    }
  };
}

function extractTitleFromRawValue(rawValue) {
  if (!isRecord(rawValue)) {
    return null;
  }
  // Handle title property shape
  if (rawValue.type === 'title' && Array.isArray(rawValue.title)) {
    const parts = rawValue.title
      .map((part) => {
        if (isRecord(part) && part.type === 'text') {
          return part.plain_text ?? part.text?.content ?? '';
        }
        return '';
      })
      .filter(Boolean);
    return parts.length > 0 ? parts.join('') : null;
  }
  // Handle direct title array
  if (Array.isArray(rawValue.title)) {
    const parts = rawValue.title
      .map((part) => {
        if (isRecord(part) && part.type === 'text') {
          return part.plain_text ?? part.text?.content ?? '';
        }
        return '';
      })
      .filter(Boolean);
    return parts.length > 0 ? parts.join('') : null;
  }
  return null;
}

function printReport(input) {
  const { latestCanonDate, selectedFiles, result } = input;
  const c = result.counts;

  console.log('\n=== Stage Relation Diagnosis ===');
  console.log(`Canon date: ${latestCanonDate}`);
  console.log(`Timeslices file: ${selectedFiles.timeslices}`);
  console.log(`Workflow stages file: ${selectedFiles.workflowStages}`);

  console.log('\n--- Resolution Summary ---');
  console.log(`timeslices total: ${c.timeslicesTotal}`);
  console.log(`timeslices with from_step_id: ${c.timeslicesWithFromStepId}`);
  console.log(`timeslices with to_step_id: ${c.timeslicesWithToStepId}`);
  console.log(`unique from_step_id: ${c.fromStepCanonIdUnique}`);
  console.log(`unique to_step_id: ${c.toStepCanonIdUnique}`);
  console.log(`resolved from_step_id vs stage canon IDs: ${c.resolvedFromUnique}`);
  console.log(`resolved to_step_id vs stage canon IDs: ${c.resolvedToUnique}`);
  console.log(`unresolved from_step_id: ${c.unresolvedFromUnique}`);
  console.log(`unresolved to_step_id: ${c.unresolvedToUnique}`);
  console.log(`sample unresolved from_step_id: ${JSON.stringify(result.samples.unresolvedFrom)}`);
  console.log(`sample unresolved to_step_id: ${JSON.stringify(result.samples.unresolvedTo)}`);

  console.log('\n--- Attributes Relation UUID Scan ---');
  console.log(`relation UUIDs found (total): ${c.relationUuidCount}`);
  console.log(`relation UUIDs found (unique): ${c.relationUuidUnique}`);
  console.log(`relation UUIDs matching workflow stage source_page_id: ${c.relationUuidsThatAreStages}`);
  console.log(`relation UUIDs not matching workflow stage source_page_id: ${c.relationUuidsNotStages}`);
  console.log(
    `sample relation UUID matches (stage page ids): ${JSON.stringify(result.samples.relationUuidsThatAreStages)}`
  );
  console.log(
    `sample relation UUID non-matches: ${JSON.stringify(result.samples.relationUuidsNotStages)}`
  );

  console.log('\n--- Canon-ID Pattern Check ---');
  console.log(
    `expected stage canon IDs derived from matching relation UUIDs: ${c.expectedStageCanonIdsFromMatchingRelations}`
  );
  console.log(
    `expected stage canon IDs that appear in timeslice from/to refs: ${c.expectedStageCanonIdsSeenInTimeslices}`
  );
  console.log(
    `sample expected stage canon IDs: ${JSON.stringify(
      result.samples.expectedStageCanonIdsFromMatchingRelations
    )}`
  );
  console.log(
    `sample expected canon IDs seen in timeslices: ${JSON.stringify(
      result.samples.expectedStageCanonIdsSeenInTimeslices
    )}`
  );

  console.log('\n--- Workflow Stages Database Analysis ---');
  const db = result.workflowStagesDatabase;
  if (db) {
    console.log(`raw workflow stages file: ${db.rawFile ?? 'not found'}`);
    console.log(`database ID: ${db.databaseId ?? 'unknown'}`);
    console.log(`total page records: ${db.totalPageRecords}`);
    console.log(`unique page IDs: ${db.uniquePageIds}`);
    console.log(`pages with titles: ${db.pageTitles?.totalWithTitles ?? 0}`);
    console.log(`page ID prefixes (top 20): ${JSON.stringify(db.pageIdPrefixes)}`);
    if (db.pageTitles?.samplePagesByPrefix) {
      console.log('\nSample pages by prefix (with titles):');
      for (const prefixGroup of db.pageTitles.samplePagesByPrefix.slice(0, 10)) {
        console.log(`  ${prefixGroup.prefix} (${prefixGroup.count} pages):`);
        for (const page of prefixGroup.samplePages) {
          console.log(`    - ${page.pageId}: "${page.title ?? '(no title)'}"`);
        }
      }
    }
    console.log(`\nproperty IDs found: ${db.propertyIds.length}`);
    console.log(`sample property names: ${JSON.stringify(db.samplePropertyNames)}`);
    if (db.unresolvedFromPageIdPrefixes) {
      console.log(
        `\nunresolved from_step_id page ID prefixes (top 20): ${JSON.stringify(
          db.unresolvedFromPageIdPrefixes
        )}`
      );
    }
    if (db.unresolvedToPageIdPrefixes) {
      console.log(
        `unresolved to_step_id page ID prefixes (top 20): ${JSON.stringify(
          db.unresolvedToPageIdPrefixes
        )}`
      );
    }
    console.log(
      `unresolved from/to page ID prefix overlap with workflow stages DB: ${db.prefixOverlapAnalysis?.overlapCount ?? 0} / ${db.prefixOverlapAnalysis?.unresolvedPrefixCount ?? 0}`
    );
  } else {
    console.log('(raw workflow stages data not available)');
  }

  console.log('\n--- Hypothesis ---');
  console.log(`classification: ${result.hypothesis.classification}`);
  console.log(`explanation: ${result.hypothesis.explanation}`);
}

main().catch((error) => {
  console.error('\nStage relation diagnosis failed.');
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
