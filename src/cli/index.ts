import path from 'node:path';
import { writeFile } from 'node:fs/promises';
import { Command } from 'commander';
import { loadConfig, notionConfig, validateConfiguredPropertyIdsOrThrow } from '../config/env.js';
import { utcDateStamp } from '../lib/time.js';
import { writeJsonlSink } from '../sinks/jsonlSink.js';
import { log } from '../lib/log.js';
import { NotionAdapter } from '../clients/notionAdapter.js';
import { pullDatasetFromNotion } from '../ingress/pullNotion.js';
import { latestDatasetDateDir, readRawDatasetForDate } from '../normalize/io.js';
import { normalizeDatasets } from '../normalize/normalizeDatasets.js';
import { ensureDir } from '../lib/fs.js';

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
      databaseId: notionConfig.databaseIds.workflowDefinitions
    },
    {
      name: DATASET_NAMES.workflow_stages,
      databaseId: notionConfig.databaseIds.workflowStages
    },
    {
      name: DATASET_NAMES.timeslices,
      databaseId: notionConfig.databaseIds.timeslices
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
  validateConfiguredPropertyIdsOrThrow();

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

function formatGeneratedSchema(schema: {
  workflowDefinitions: { properties: Record<string, { id: string; type: string }> };
  workflowStages: { properties: Record<string, { id: string; type: string }> };
  timeslices: { properties: Record<string, { id: string; type: string }> };
}): string {
  const pretty = JSON.stringify(schema, null, 2);
  return `export const notionSchema = ${pretty} as const;\n`;
}

function extractDatabaseProperties(database: any): Record<string, { id: string; type: string }> {
  const output: Record<string, { id: string; type: string }> = {};
  const properties = database.properties as Record<string, any>;

  for (const [propertyName, propertyValue] of Object.entries(properties)) {
    output[propertyName] = {
      id: String((propertyValue as { id?: string }).id ?? ''),
      type: String((propertyValue as { type?: string }).type ?? 'unknown')
    };
  }

  return output;
}

async function runAuditNotionSchema(): Promise<void> {
  const config = loadConfig();
  const adapter = new NotionAdapter({ authToken: config.NOTION_TOKEN });

  const datasets = [
    {
      key: 'workflowDefinitions',
      label: 'workflow_definitions',
      databaseId: notionConfig.databaseIds.workflowDefinitions
    },
    {
      key: 'workflowStages',
      label: 'workflow_stages',
      databaseId: notionConfig.databaseIds.workflowStages
    },
    {
      key: 'timeslices',
      label: 'timeslices',
      databaseId: notionConfig.databaseIds.timeslices
    }
  ] as const;

  const schema = {
    workflowDefinitions: { properties: {} as Record<string, { id: string; type: string }> },
    workflowStages: { properties: {} as Record<string, { id: string; type: string }> },
    timeslices: { properties: {} as Record<string, { id: string; type: string }> }
  };

  for (const dataset of datasets) {
    const database = await adapter.retrieveDatabase(dataset.databaseId);
    const properties = extractDatabaseProperties(database);
    schema[dataset.key].properties = properties;

    log.info(`schema for ${dataset.label} (${dataset.databaseId})`);
    for (const [propertyName, property] of Object.entries(properties)) {
      log.info(`  ${propertyName} | id=${property.id} | type=${property.type}`);
    }
  }

  const auditDir = path.join(config.resolvedDataDir, 'audit');
  await ensureDir(auditDir);
  await writeFile(path.join(auditDir, 'notion-schema.json'), `${JSON.stringify(schema, null, 2)}\n`, 'utf8');

  const generatedPath = path.resolve('src/config/notionSchema.generated.ts');
  await writeFile(generatedPath, formatGeneratedSchema(schema), 'utf8');
  log.info('wrote audit schema output', {
    json: path.join(auditDir, 'notion-schema.json'),
    generated: generatedPath
  });
}

async function runAll(): Promise<void> {
  await runPullNotion();
  await runNormalize();
}

const program = new Command();
program.name('etl-cli').description('Pull + normalize integration data').version('0.1.0');

program.command('pull:notion').description('Pull raw records from Notion').action(runPullNotion);
program
  .command('audit:notion-schema')
  .description('Audit Notion database schemas and generate src/config/notionSchema.generated.ts')
  .action(runAuditNotionSchema);
program.command('normalize').description('Normalize latest raw records').action(runNormalize);
program.command('run').description('Run pull:notion then normalize').action(runAll);

program.parseAsync(process.argv).catch((error) => {
  log.error('command failed', error);
  process.exitCode = 1;
});
