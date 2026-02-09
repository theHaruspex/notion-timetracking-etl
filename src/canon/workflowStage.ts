import { z } from 'zod';
import type { RawRecord } from '../ingress/rawRecord.js';
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

function relationIds(rawValue: unknown): string[] {
  if (!rawValue || typeof rawValue !== 'object') {
    return [];
  }
  const value = rawValue as { relation?: Array<{ id?: string }> };
  if (!Array.isArray(value.relation)) {
    return [];
  }
  return value.relation
    .map((item) => item.id)
    .filter((item): item is string => typeof item === 'string' && item.length > 0);
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

export function buildWorkflowStage(record: RawRecord): WorkflowStage | null {
  if (record.entityType !== 'page' || !record.pageId) {
    return null;
  }

  const values = Object.values(record.properties).map((property) => property.rawValue);
  const allRelationTargets = values.flatMap((value) => relationIds(value));
  const workflowDefinitionId =
    allRelationTargets.length > 0
      ? stableEntityId('workflow_definition', allRelationTargets[0])
      : null;

  let stageNumber: number | null = null;
  let stageLabel: string | null = null;

  for (const value of values) {
    if (stageNumber === null && value && typeof value === 'object') {
      const typed = value as { type?: string; number?: unknown };
      if (typed.type === 'number') {
        stageNumber = normalizeNullableNumber(typed.number);
      }
    }

    if (stageLabel === null) {
      stageLabel = firstDisplayText(value);
    }

    if (stageNumber !== null && stageLabel !== null) {
      break;
    }
  }

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
