import path from 'node:path';
import { Command } from 'commander';
import { loadConfig } from '../config/env.js';
import { utcDateStamp } from '../lib/time.js';
import { writeJsonlSink } from '../sinks/jsonlSink.js';
import { log } from '../lib/log.js';
import { NotionAdapter } from '../clients/notionAdapter.js';
import { pullDatasetFromNotion } from '../ingress/pullNotion.js';
import { latestDatasetDateDir, readRawDatasetForDate } from '../normalize/io.js';
import { normalizeDatasets } from '../normalize/normalizeDatasets.js';

const DATASET_NAMES = {
  workflow_definitions: 'workflow_definitions',
  workflow_stages: 'workflow_stages',
  timeslices: 'timeslices'
} as const;

async function runPullNotion(): Promise<void> {
  const config = loadConfig();
  const adapter = new NotionAdapter({ authToken: config.NOTION_TOKEN });
  const day = utcDateStamp();

  const datasets = [
    {
      name: DATASET_NAMES.workflow_definitions,
      databaseId: config.NOTION_DB_WORKFLOW_DEFINITIONS
    },
    {
      name: DATASET_NAMES.workflow_stages,
      databaseId: config.NOTION_DB_WORKFLOW_STAGES
    },
    {
      name: DATASET_NAMES.timeslices,
      databaseId: config.NOTION_DB_TIMESLICES
    }
  ];

  for (const dataset of datasets) {
    log.info(`pulling ${dataset.name}`);
    const pulled = await pullDatasetFromNotion(adapter, {
      dataset: dataset.name,
      databaseId: dataset.databaseId
    });

    const outPath = path.join(config.resolvedDataDir, 'raw', dataset.name, day, 'records.jsonl');
    await writeJsonlSink(outPath, pulled.rawRecords);
    log.info(`wrote ${dataset.name} raw records`, { count: pulled.rawRecords.length, outPath });
  }
}

async function runNormalize(): Promise<void> {
  const config = loadConfig();
  const rawBase = path.join(config.resolvedDataDir, 'raw');
  const canonBase = path.join(config.resolvedDataDir, 'canon');

  const wfDefDate = await latestDatasetDateDir(rawBase, DATASET_NAMES.workflow_definitions);
  const wfStageDate = await latestDatasetDateDir(rawBase, DATASET_NAMES.workflow_stages);
  const timesliceDate = await latestDatasetDateDir(rawBase, DATASET_NAMES.timeslices);

  if (!wfDefDate || !wfStageDate || !timesliceDate) {
    throw new Error('Missing raw dataset pulls. Run pull:notion first.');
  }

  const workflowDefinitionsRaw = await readRawDatasetForDate(
    rawBase,
    DATASET_NAMES.workflow_definitions,
    wfDefDate
  );
  const workflowStagesRaw = await readRawDatasetForDate(rawBase, DATASET_NAMES.workflow_stages, wfStageDate);
  const timeslicesRaw = await readRawDatasetForDate(rawBase, DATASET_NAMES.timeslices, timesliceDate);

  const normalized = normalizeDatasets({
    workflowDefinitionsRaw,
    workflowStagesRaw,
    timeslicesRaw
  });

  const day = utcDateStamp();

  await writeJsonlSink(
    path.join(canonBase, DATASET_NAMES.workflow_definitions, day, 'records.jsonl'),
    normalized.workflowDefinitions
  );
  await writeJsonlSink(
    path.join(canonBase, DATASET_NAMES.workflow_stages, day, 'records.jsonl'),
    normalized.workflowStages
  );
  await writeJsonlSink(
    path.join(canonBase, DATASET_NAMES.timeslices, day, 'records.jsonl'),
    normalized.timeslices
  );

  log.info('normalization finished', {
    workflowDefinitions: normalized.workflowDefinitions.length,
    workflowStages: normalized.workflowStages.length,
    timeslices: normalized.timeslices.length
  });
}

async function runAll(): Promise<void> {
  await runPullNotion();
  await runNormalize();
}

const program = new Command();
program.name('etl-cli').description('Pull + normalize integration data').version('0.1.0');

program.command('pull:notion').description('Pull raw records from Notion').action(runPullNotion);
program.command('normalize').description('Normalize latest raw records').action(runNormalize);
program.command('run').description('Run pull:notion then normalize').action(runAll);

program.parseAsync(process.argv).catch((error) => {
  log.error('command failed', error);
  process.exitCode = 1;
});
