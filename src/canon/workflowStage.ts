import { z } from 'zod';
import type { RawRecord } from '../ingress/rawRecord.js';
import { notionConfig } from '../config/env.js';
import { normalizeNullableNumber, normalizeNullableString, sortKey, stableEntityId } from './rules.js';

export const workflowStageSchema = z.object({
  workflow_stage_id: z.string(),
  workflow_definition_id: z.string().nullable(),
  source_page_id: z.string(),
  source_database_id: z.string(),
  stage_number: z.number().nullable(),
  stage_label: z.string().nullable(),
  sort_key: z.string(),
  created_time: z.string().nullable(),
  last_edited_time: z.string().nullable(),
  page_url: z.string().nullable(),
  attributes: z.record(z.string(), z.unknown())
});

export type WorkflowStage = z.infer<typeof workflowStageSchema>;

function relationId(rawValue: unknown): string | null {
  if (!rawValue || typeof rawValue !== 'object') {
    return null;
  }
  const value = rawValue as { type?: string; relation?: Array<{ id?: string }> };
  if (value.type !== 'relation' || !Array.isArray(value.relation) || value.relation.length === 0) {
    return null;
  }
  const first = value.relation[0]?.id;
  return typeof first === 'string' && first.length > 0 ? first : null;
}

function firstDisplayText(rawValue: unknown): string | null {
  if (!rawValue || typeof rawValue !== 'object') {
    return null;
  }

  const value = rawValue as {
    type?: string;
    rich_text?: Array<{ plain_text?: string }>;
    title?: Array<{ plain_text?: string }>;
    select?: { name?: string };
    status?: { name?: string };
  };

  if (value.type === 'title' && Array.isArray(value.title)) {
    const joined = value.title.map((part) => part.plain_text ?? '').join('').trim();
    return normalizeNullableString(joined);
  }

  if (value.type === 'rich_text' && Array.isArray(value.rich_text)) {
    const joined = value.rich_text.map((part) => part.plain_text ?? '').join('').trim();
    return normalizeNullableString(joined);
  }

  if (value.type === 'select') {
    return normalizeNullableString(value.select?.name);
  }

  if (value.type === 'status') {
    return normalizeNullableString(value.status?.name);
  }

  return null;
}

function extractNumber(rawValue: unknown): number | null {
  if (!rawValue || typeof rawValue !== 'object') {
    return null;
  }
  const typed = rawValue as { type?: string; number?: unknown };
  if (typed.type !== 'number') {
    return null;
  }
  return normalizeNullableNumber(typed.number);
}

function requireConfiguredWorkflowStagePropertyIds(): void {
  const required = notionConfig.propertyIds.workflowStages;
  const missing = Object.entries(required)
    .filter(([, value]) => value.trim().length === 0)
    .map(([key]) => key);
  if (missing.length > 0) {
    throw new Error(
      `Missing configured property IDs for workflowStages: ${missing.join(
        ', '
      )}. Run: npm run cli -- audit:notion-schema and fill src/config/env.ts.`
    );
  }
}

export function buildWorkflowStage(record: RawRecord): WorkflowStage | null {
  if (record.entityType !== 'page' || !record.pageId) {
    return null;
  }
  requireConfiguredWorkflowStagePropertyIds();
  const ids = notionConfig.propertyIds.workflowStages;

  const workflowDefinitionSource = relationId(record.properties[ids.workflowDefinitionRel]?.rawValue);
  const workflowDefinitionId =
    workflowDefinitionSource ? stableEntityId('workflow_definition', workflowDefinitionSource) : null;
  const stageNumber = extractNumber(record.properties[ids.stageNumber]?.rawValue);
  const stageLabel = firstDisplayText(record.properties[ids.stageLabel]?.rawValue);

  return workflowStageSchema.parse({
    workflow_stage_id: stableEntityId('workflow_stage', record.pageId),
    workflow_definition_id: workflowDefinitionId,
    source_page_id: record.pageId,
    source_database_id: record.databaseId,
    stage_number: stageNumber,
    stage_label: stageLabel,
    sort_key: sortKey([workflowDefinitionId, stageNumber, stageLabel, record.pageId]),
    created_time:
      typeof record.metadata?.created_time === 'string' ? (record.metadata.created_time as string) : null,
    last_edited_time: record.lastEditedTime ?? null,
    page_url: typeof record.metadata?.url === 'string' ? (record.metadata.url as string) : null,
    attributes: Object.fromEntries(
      Object.entries(record.properties).map(([propertyId, property]) => [propertyId, property.rawValue])
    )
  });
}
