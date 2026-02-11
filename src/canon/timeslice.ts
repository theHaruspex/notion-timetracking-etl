import { z } from 'zod';
import type { RawRecord } from '../ingress/rawRecord.js';
import { notionConfig } from '../config/env.js';
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

  const typed = rawValue as {
    type?: string;
    date?: { start?: string | null } | null;
    rollup?: {
      type?: string;
      date?: { start?: string | null } | null;
      array?: Array<{ type?: string; date?: { start?: string | null } | null }>;
    } | null;
  };

  if (typed.type === 'date') {
    return typeof typed.date?.start === 'string' ? typed.date.start : null;
  }

  if (typed.type === 'rollup' && typed.rollup && typeof typed.rollup === 'object') {
    if (typed.rollup.type === 'date') {
      return typeof typed.rollup.date?.start === 'string' ? typed.rollup.date.start : null;
    }

    if (typed.rollup.type === 'array' && Array.isArray(typed.rollup.array)) {
      const firstDateItem = typed.rollup.array.find(
        (item) => item && typeof item === 'object' && item.type === 'date'
      );
      if (!firstDateItem) {
        return null;
      }
      return typeof firstDateItem.date?.start === 'string' ? firstDateItem.date.start : null;
    }
  }

  return null;
}

function extractFirstRelationId(rawValue: unknown): string | null {
  if (!rawValue || typeof rawValue !== 'object') {
    return null;
  }
  const typed = rawValue as { type?: string; relation?: Array<{ id?: string }> };
  if (typed.type !== 'relation' || !Array.isArray(typed.relation) || typed.relation.length === 0) {
    return null;
  }
  const first = typed.relation[0]?.id;
  return typeof first === 'string' && first.length > 0 ? first : null;
}

function extractFirstRollupRelationId(rawValue: unknown): string | null {
  if (!rawValue || typeof rawValue !== 'object') {
    return null;
  }
  const typed = rawValue as {
    type?: string;
    rollup?: { type?: string; array?: Array<{ type?: string; relation?: Array<{ id?: string }> }> };
  };
  if (typed.type !== 'rollup' || !typed.rollup || typed.rollup.type !== 'array') {
    return null;
  }
  if (!Array.isArray(typed.rollup.array)) {
    return null;
  }
  for (const item of typed.rollup.array) {
    if (!item || typeof item !== 'object') {
      continue;
    }
    if (item.type !== 'relation' || !Array.isArray(item.relation) || item.relation.length === 0) {
      continue;
    }
    const id = item.relation[0]?.id;
    if (typeof id === 'string' && id.length > 0) {
      return id;
    }
  }
  return null;
}

function extractFirstRollupDateStart(rawValue: unknown): string | null {
  if (!rawValue || typeof rawValue !== 'object') {
    return null;
  }
  const typed = rawValue as {
    type?: string;
    rollup?: { type?: string; array?: Array<{ type?: string; date?: { start?: string | null } | null }> };
  };
  if (typed.type !== 'rollup' || !typed.rollup || typed.rollup.type !== 'array') {
    return null;
  }
  if (!Array.isArray(typed.rollup.array)) {
    return null;
  }
  for (const item of typed.rollup.array) {
    if (!item || typeof item !== 'object') {
      continue;
    }
    if (item.type !== 'date' || !item.date) {
      continue;
    }
    const start = item.date.start;
    if (typeof start === 'string' && start.length > 0) {
      return start;
    }
  }
  return null;
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

function requireConfiguredTimeslicePropertyIds(): void {
  const required = notionConfig.propertyIds.timeslices;
  const missing = Object.entries(required)
    .filter(([, value]) => value.trim().length === 0)
    .map(([key]) => key);
  if (missing.length > 0) {
    throw new Error(
      `Missing configured property IDs for timeslices: ${missing.join(
        ', '
      )}. Run: npm run cli -- audit:notion-schema and fill src/config/env.ts.`
    );
  }
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
  requireConfiguredTimeslicePropertyIds();

  const rawProperties = Object.fromEntries(
    Object.entries(record.properties).map(([propertyId, property]) => [propertyId, property.rawValue])
  );

  const ids = notionConfig.propertyIds.timeslices;
  const workflowRelationRaw = rawProperties[ids.workflowDefinitionRel];
  const fromStageRelationRaw = rawProperties[ids.fromStageRel];
  const toStageRelationRaw = rawProperties[ids.toStageRel];
  const startedDateRaw = rawProperties[ids.startedAtDate];
  const endedDateRaw = rawProperties[ids.endedAtDate];

  const pageTitle =
    (rawProperties[notionConfig.propertyIds.workflowDefinitions.title]
      ? extractTitle(rawProperties[notionConfig.propertyIds.workflowDefinitions.title])
      : null) ??
    Object.values(rawProperties)
      .map((rawValue) => extractTitle(rawValue))
      .find((value): value is string => typeof value === 'string' && value.length > 0) ??
    null;

  const workflowDefinitionSource = extractFirstRelationId(workflowRelationRaw);
  const fromStepSource = extractFirstRollupRelationId(fromStageRelationRaw);
  const toStepSource = extractFirstRollupRelationId(toStageRelationRaw);
  const workflowDefinitionId =
    workflowDefinitionSource ? stableEntityId('workflow_definition', workflowDefinitionSource) : null;
  const fromStepId = fromStepSource ? stableEntityId('workflow_stage', fromStepSource) : null;
  const toStepId = toStepSource ? stableEntityId('workflow_stage', toStepSource) : null;
  const startedAt = extractFirstRollupDateStart(startedDateRaw) ?? extractDateStart(startedDateRaw);
  const endedAt = extractFirstRollupDateStart(endedDateRaw) ?? extractDateStart(endedDateRaw);

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

export function debugAssertExtractDateStart(): void {
  const nativeDate = {
    type: 'date',
    date: { start: '2026-01-15T23:36:00.000Z', end: null, time_zone: null }
  };
  if (extractDateStart(nativeDate) !== '2026-01-15T23:36:00.000Z') {
    throw new Error('extractDateStart failed for native date payload.');
  }

  const rollupArrayDate = {
    type: 'rollup',
    rollup: {
      type: 'array',
      array: [
        {
          type: 'date',
          date: { start: '2026-01-15T23:36:00.000+00:00', end: null, time_zone: null }
        }
      ],
      function: 'show_original'
    }
  };
  if (extractDateStart(rollupArrayDate) !== '2026-01-15T23:36:00.000+00:00') {
    throw new Error('extractDateStart failed for rollup array date payload.');
  }

  const rollupSingleDate = {
    type: 'rollup',
    rollup: {
      type: 'date',
      date: { start: '2026-01-16T00:00:00.000Z', end: null, time_zone: null }
    }
  };
  if (extractDateStart(rollupSingleDate) !== '2026-01-16T00:00:00.000Z') {
    throw new Error('extractDateStart failed for rollup date payload.');
  }
}
