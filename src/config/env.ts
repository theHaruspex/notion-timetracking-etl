import path from 'node:path';
import dotenv from 'dotenv';
import { z } from 'zod';

dotenv.config();

const envSchema = z.object({
  NOTION_TOKEN: z.string().min(1)
});

const pbiEnvSchema = z.object({
  PBI_TENANT_ID: z.string().optional(),
  PBI_CLIENT_ID: z.string().optional(),
  PBI_CLIENT_SECRET: z.string().optional(),
  PBI_WORKSPACE_ID: z.string().optional(),
  PBI_DATASET_NAME: z.string().optional()
});

export type AppConfig = z.infer<typeof envSchema> & {
  DATA_DIR: string;
  LOG_LEVEL: string;
  resolvedDataDir: string;
};

export function loadConfig(): AppConfig {
  const parsed = envSchema.parse(process.env);
  const DATA_DIR = './data';
  const LOG_LEVEL = 'info';

  return {
    ...parsed,
    DATA_DIR,
    LOG_LEVEL,
    resolvedDataDir: path.resolve(DATA_DIR)
  };
}

export type PbiConfig = {
  tenantId: string;
  clientId: string;
  clientSecret: string;
  workspaceId: string;
  datasetName: string;
};

export function loadPbiConfig(): PbiConfig {
  const parsed = pbiEnvSchema.parse(process.env);
  const missing: string[] = [];

  const tenantId = parsed.PBI_TENANT_ID?.trim() ?? '';
  const clientId = parsed.PBI_CLIENT_ID?.trim() ?? '';
  const clientSecret = parsed.PBI_CLIENT_SECRET?.trim() ?? '';
  const workspaceId = parsed.PBI_WORKSPACE_ID?.trim() ?? '';
  const datasetName = parsed.PBI_DATASET_NAME?.trim() ?? '';

  if (!tenantId) {
    missing.push('PBI_TENANT_ID');
  }
  if (!clientId) {
    missing.push('PBI_CLIENT_ID');
  }
  if (!clientSecret) {
    missing.push('PBI_CLIENT_SECRET');
  }
  if (!workspaceId) {
    missing.push('PBI_WORKSPACE_ID');
  }
  if (!datasetName) {
    missing.push('PBI_DATASET_NAME');
  }

  if (missing.length > 0) {
    throw new Error(
      `Missing required Power BI environment variables: ${missing.join(
        ', '
      )}. These are only required for PBI commands.`
    );
  }

  return {
    tenantId,
    clientId,
    clientSecret,
    workspaceId,
    datasetName
  };
}

type NotionConfig = {
  databaseIds: {
    workflowDefinitions: string;
    workflowStages: string;
    timeslices: string;
  };
  propertyIds: {
    timeslices: {
      workflowDefinitionRel: string;
      fromStageRel: string;
      toStageRel: string;
      startedAtDate: string;
      endedAtDate: string;
    };
    workflowStages: {
      workflowDefinitionRel: string;
      stageNumber: string;
      stageLabel: string;
    };
    workflowDefinitions: {
      title: string;
    };
  };
};

export const notionConfig: NotionConfig = {
  databaseIds: {
    workflowDefinitions: 'f12d805a-4a5f-4281-b6fe-333be2d52c9c',
    workflowStages: '24d3b599-e426-46de-a8f0-3dcad69e28c7',
    timeslices: '87f99225-0658-4079-a0c5-8a81feb35510'
  },
  propertyIds: {
    timeslices: {
      workflowDefinitionRel: 'U%3CU%7B',
      fromStageRel: 'yBeO',
      toStageRel: '%7Dg%40%5E',
      startedAtDate: 'w%5Czt',
      endedAtDate: 'cZbu'
    },
    workflowStages: {
      workflowDefinitionRel: '%5Bn%40l',
      stageNumber: 'j%5D_%3F',
      stageLabel: 'title'
    },
    workflowDefinitions: {
      title: 'title'
    }
  }
};

export function overrideNotionPropertyIdsForTests(input: {
  timeslices?: Partial<(typeof notionConfig.propertyIds.timeslices)>;
  workflowStages?: Partial<(typeof notionConfig.propertyIds.workflowStages)>;
  workflowDefinitions?: Partial<(typeof notionConfig.propertyIds.workflowDefinitions)>;
}): void {
  if (input.timeslices) {
    Object.assign(notionConfig.propertyIds.timeslices as Record<string, string>, input.timeslices);
  }
  if (input.workflowStages) {
    Object.assign(notionConfig.propertyIds.workflowStages as Record<string, string>, input.workflowStages);
  }
  if (input.workflowDefinitions) {
    Object.assign(
      notionConfig.propertyIds.workflowDefinitions as Record<string, string>,
      input.workflowDefinitions
    );
  }
}

export function validateConfiguredPropertyIdsOrThrow(): void {
  const missing: Array<{ dataset: string; key: string }> = [];

  const check = (dataset: string, map: Record<string, string>) => {
    for (const [key, value] of Object.entries(map)) {
      if (value.trim().length === 0) {
        missing.push({ dataset, key });
      }
    }
  };

  check('timeslices', notionConfig.propertyIds.timeslices as unknown as Record<string, string>);
  check('workflowStages', notionConfig.propertyIds.workflowStages as unknown as Record<string, string>);
  check(
    'workflowDefinitions',
    notionConfig.propertyIds.workflowDefinitions as unknown as Record<string, string>
  );

  if (missing.length === 0) {
    return;
  }

  const grouped = missing
    .reduce<Record<string, string[]>>((acc, item) => {
      acc[item.dataset] = acc[item.dataset] ?? [];
      acc[item.dataset].push(item.key);
      return acc;
    }, {});

  const details = Object.entries(grouped)
    .map(([dataset, keys]) => `${dataset}: ${keys.join(', ')}`)
    .join('; ');

  throw new Error(
    `Missing configured Notion property IDs (${details}). Run: npm run cli -- audit:notion-schema, then fill property IDs in src/config/env.ts.`
  );
}
