import path from 'node:path';
import { writeFile } from 'node:fs/promises';
import { Command } from 'commander';
import {
  loadConfig,
  loadPbiConfig,
  notionConfig,
  validateConfiguredPropertyIdsOrThrow
} from '../config/env.js';
import { utcDateStamp } from '../lib/time.js';
import { writeJsonlSink } from '../sinks/jsonlSink.js';
import { log } from '../lib/log.js';
import { NotionAdapter } from '../ingress/notionAdapter.js';
import { pullDatasetFromNotion } from '../ingress/pullNotion.js';
import { latestDatasetDateDir, readDatasetJsonlForDate, readRawDatasetForDate } from '../normalize/io.js';
import { normalizeAndValidateDatasets } from '../normalize/normalizeDatasets.js';
import { ensureDir } from '../lib/fs.js';
import {
  PowerBiClient,
  PowerBiServicePrincipalAuth,
  buildModelSpec,
  validateSpec,
  applySchema,
  getDatasetRegistryPath,
  executeWipeAndReload
} from '../sinks/pbi/index.js';
import { findDatasetId, findMostRecentEntryForGroup, loadRegistry } from '../sinks/pbi/state/datasetRegistry.js';
import { derivePbiTableRows } from '../sinks/pbi/refresh/derive/index.js';
import type { Timeslice } from '../canon/timeslice.js';
import type { WorkflowDefinition } from '../canon/workflowDefinition.js';
import type { WorkflowStage } from '../canon/workflowStage.js';

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

  const normalized = normalizeAndValidateDatasets({
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
  await writeJsonlSink(
    path.join(canonBase, 'qualityIssues', normalized.qualityReport.run_date, 'qualityIssues.jsonl'),
    normalized.qualityIssues
  );

  console.log(
    `[quality] timeslices_total=${normalized.qualityReport.counts.timeslices_total} excluded_missing_workflow_definition=${normalized.qualityReport.counts.timeslices_excluded_missing_workflow_definition} issues_total=${normalized.qualityReport.counts.issues_total} no_to_step_in_run=${normalized.qualityReport.flags.no_to_step_in_run}`
  );

  log.info('normalization finished', {
    workflowDefinitions: normalized.workflowDefinitions.length,
    workflowStages: normalized.workflowStages.length,
    timeslices: normalized.timeslices.length,
    qualityIssues: normalized.qualityIssues.length
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

async function runPipelineEndToEnd(): Promise<void> {
  await runPullNotion();
  await runNormalize();
  await runPbiProvision();
  await runPbiRefresh();
}

async function runPbiProvision(): Promise<void> {
  const appConfig = loadConfig();
  const pbiConfig = loadPbiConfig();
  if (!pbiConfig.datasetName) {
    throw new Error(
      'PBI_DATASET_NAME is required for pbi:provision so we can create or target a named dataset.'
    );
  }

  const spec = buildModelSpec(pbiConfig.datasetName);
  validateSpec(spec);

  const auth = new PowerBiServicePrincipalAuth({
    tenantId: pbiConfig.tenantId,
    clientId: pbiConfig.clientId,
    clientSecret: pbiConfig.clientSecret
  });
  const client = new PowerBiClient({ auth });

  const { datasetId, changesApplied } = await applySchema(
    client,
    { resolvedDataDir: appConfig.resolvedDataDir },
    {
      groupId: pbiConfig.groupId,
      datasetName: pbiConfig.datasetName,
      spec
    }
  );

  log.info('pbi provision complete', {
    groupId: pbiConfig.groupId,
    datasetName: pbiConfig.datasetName,
    datasetId,
    changesApplied,
    registryPath: getDatasetRegistryPath({ resolvedDataDir: appConfig.resolvedDataDir })
  });
}

async function runPbiRefresh(): Promise<void> {
  const appConfig = loadConfig();
  const pbiConfig = loadPbiConfig();

  const auth = new PowerBiServicePrincipalAuth({
    tenantId: pbiConfig.tenantId,
    clientId: pbiConfig.clientId,
    clientSecret: pbiConfig.clientSecret
  });
  const client = new PowerBiClient({ auth });

  const registry = await loadRegistry({ resolvedDataDir: appConfig.resolvedDataDir });
  const requestedDatasetName = pbiConfig.datasetName;
  const selectedEntry =
    requestedDatasetName
      ? registry.entries.find(
          (entry) =>
            entry.groupId === pbiConfig.groupId &&
            entry.datasetName.toLowerCase() === requestedDatasetName.toLowerCase()
        ) ?? null
      : findMostRecentEntryForGroup(registry, { groupId: pbiConfig.groupId });
  if (!selectedEntry) {
    throw new Error(
      requestedDatasetName
        ? `No dataset registry entry found for group "${pbiConfig.groupId}" and dataset "${requestedDatasetName}". Run: npm run cli -- pbi:provision`
        : `No dataset registry entry found for group "${pbiConfig.groupId}". Run: npm run cli -- pbi:provision`
    );
  }
  const datasetId = selectedEntry.datasetId;
  const datasetName = selectedEntry.datasetName;

  const spec = buildModelSpec(datasetName);
  validateSpec(spec);

  const datasets = await client.getDatasetsInGroup(pbiConfig.groupId);
  if (!datasets.some((dataset) => dataset.id === datasetId)) {
    throw new Error(
      `Dataset ID "${datasetId}" from registry was not found in group "${pbiConfig.groupId}". This command does not auto-recreate datasets. Run: npm run cli -- pbi:provision`
    );
  }

  const canonBase = path.join(appConfig.resolvedDataDir, 'canon');
  const wfDefDate = await latestDatasetDateDir(canonBase, DATASET_NAMES.workflow_definitions);
  const wfStageDate = await latestDatasetDateDir(canonBase, DATASET_NAMES.workflow_stages);
  const timesliceDate = await latestDatasetDateDir(canonBase, DATASET_NAMES.timeslices);
  if (!wfDefDate || !wfStageDate || !timesliceDate) {
    throw new Error('Missing canonical datasets. Run normalize before pbi:refresh.');
  }

  const workflowDefinitions = await readDatasetJsonlForDate<WorkflowDefinition>(
    canonBase,
    DATASET_NAMES.workflow_definitions,
    wfDefDate
  );
  const workflowStages = await readDatasetJsonlForDate<WorkflowStage>(
    canonBase,
    DATASET_NAMES.workflow_stages,
    wfStageDate
  );
  const timeslices = await readDatasetJsonlForDate<Timeslice>(
    canonBase,
    DATASET_NAMES.timeslices,
    timesliceDate
  );
  const tableRowsByName = derivePbiTableRows({
    workflowDefinitions,
    workflowStages,
    timeslices
  });
  const tableRowCounts = Object.fromEntries(
    spec.tables.map((table) => [table.name, tableRowsByName[table.name]?.length ?? 0])
  );
  log.info('pbi refresh table row counts', {
    selectedDatasetName: datasetName,
    selectedDatasetId: datasetId,
    workflowDefinitionsDate: wfDefDate,
    workflowStagesDate: wfStageDate,
    timeslicesDate: timesliceDate,
    tableRowCounts
  });

  const result = await executeWipeAndReload(client, {
    groupId: pbiConfig.groupId,
    datasetId,
    spec,
    tableRowsByName,
    log: log.info
  });

  log.info('pbi refresh complete', {
    groupId: pbiConfig.groupId,
    datasetName,
    datasetId,
    ...result
  });
}

const program = new Command();
program.name('etl-cli').description('Pull + normalize integration data').version('0.1.0');

program.command('pull:notion').description('Pull raw records from Notion').action(runPullNotion);
program
  .command('audit:notion-schema')
  .description('Audit Notion schemas and generate src/config/notionSchema.generated.ts (reference file)')
  .action(runAuditNotionSchema);
program
  .command('pbi:provision')
  .description('Provision Power BI dataset scaffold and persist dataset registry mapping')
  .action(runPbiProvision);
program
  .command('pbi:refresh')
  .description('Strict wipe+reload Power BI tables from canonical datasets')
  .action(runPbiRefresh);
program.command('normalize').description('Normalize latest raw records').action(runNormalize);
program.command('run').description('Run pull:notion then normalize').action(runAll);
program
  .command('run:end-to-end')
  .description('Run pull:notion, normalize, pbi:provision, then pbi:refresh')
  .action(runPipelineEndToEnd);

program.parseAsync(process.argv).catch((error) => {
  log.error('command failed', error);
  process.exitCode = 1;
});
