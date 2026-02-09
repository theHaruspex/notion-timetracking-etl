import { z } from 'zod';
import type { RawRecord } from '../ingress/rawRecord.js';
import { normalizeNullableString, stableEntityId, timesliceIdFromPageId } from './rules.js';

export const timesliceSchema = z.object({
  timeslice_id: z.string(),
  workflow_definition_id: z.string().nullable(),
  from_step_id: z.string().nullable(),
  to_step_id: z.string().nullable(),
  started_at: z.string().nullable(),
  ended_at: z.string().nullable(),
  duration_seconds: z.number().nullable(),
  source_page_id: z.string(),
  source_database_id: z.string(),
  created_time: z.string().nullable(),
  last_edited_time: z.string().nullable(),
  page_url: z.string().nullable(),
  page_title: z.string().nullable(),
  attributes: z.record(z.string(), z.unknown())
});

export type Timeslice = z.infer<typeof timesliceSchema>;

function extractDateStart(rawValue: unknown): string | null {
  if (!rawValue || typeof rawValue !== 'object') {
    return null;
  }

  const typed = rawValue as { type?: string; date?: { start?: string | null } | null };
  if (typed.type !== 'date') {
    return null;
  }
  return typed.date?.start ?? null;
}

function extractTitle(rawValue: unknown): string | null {
  if (!rawValue || typeof rawValue !== 'object') {
    return null;
  }

  const typed = rawValue as { type?: string; title?: Array<{ plain_text?: string }> };
  if (typed.type !== 'title' || !Array.isArray(typed.title)) {
    return null;
  }

  const joined = typed.title.map((part) => part.plain_text ?? '').join('');
  return normalizeNullableString(joined);
}

function extractRelationIds(rawValue: unknown): string[] {
  if (!rawValue || typeof rawValue !== 'object') {
    return [];
  }

  const typed = rawValue as { type?: string; relation?: Array<{ id?: string }> };
  if (typed.type !== 'relation' || !Array.isArray(typed.relation)) {
    return [];
  }

  return typed.relation
    .map((entry) => entry.id)
    .filter((entry): entry is string => typeof entry === 'string' && entry.length > 0);
}

function computeDurationSeconds(startedAt: string | null, endedAt: string | null): number | null {
  if (!startedAt || !endedAt) {
    return null;
  }
  const startMs = Date.parse(startedAt);
  const endMs = Date.parse(endedAt);
  if (Number.isNaN(startMs) || Number.isNaN(endMs)) {
    return null;
  }
  return Math.max(0, Math.round((endMs - startMs) / 1000));
}

export function buildTimeslice(record: RawRecord): Timeslice | null {
  if (record.entityType !== 'page' || !record.pageId) {
    return null;
  }

  const rawProperties = Object.fromEntries(
    Object.entries(record.properties).map(([propertyId, property]) => [propertyId, property.rawValue])
  );

  const relationTargets = Object.values(rawProperties).flatMap((rawValue) => extractRelationIds(rawValue));
  const dateValues = Object.values(rawProperties)
    .map((rawValue) => extractDateStart(rawValue))
    .filter((value): value is string => typeof value === 'string' && value.length > 0);

  const pageTitle =
    Object.values(rawProperties)
      .map((rawValue) => extractTitle(rawValue))
      .find((value): value is string => typeof value === 'string' && value.length > 0) ?? null;

  const workflowDefinitionId =
    relationTargets.length > 0 ? stableEntityId('workflow_definition', relationTargets[0]) : null;
  const fromStepId = relationTargets.length > 1 ? stableEntityId('workflow_stage', relationTargets[1]) : null;
  const toStepId = relationTargets.length > 2 ? stableEntityId('workflow_stage', relationTargets[2]) : null;
  const startedAt = dateValues.at(0) ?? null;
  const endedAt = dateValues.at(1) ?? null;

  return timesliceSchema.parse({
    timeslice_id: timesliceIdFromPageId(record.pageId),
    workflow_definition_id: workflowDefinitionId,
    from_step_id: fromStepId,
    to_step_id: toStepId,
    started_at: startedAt,
    ended_at: endedAt,
    duration_seconds: computeDurationSeconds(startedAt, endedAt),
    source_page_id: record.pageId,
    source_database_id: record.databaseId,
    created_time:
      typeof record.metadata?.created_time === 'string' ? (record.metadata.created_time as string) : null,
    last_edited_time: record.lastEditedTime ?? null,
    page_url: typeof record.metadata?.url === 'string' ? (record.metadata.url as string) : null,
    page_title: pageTitle,
    attributes: rawProperties
  });
}
