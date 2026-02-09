import { z } from 'zod';
import type { RawRecord } from '../ingress/rawRecord.js';
import { notionConfig } from '../config/env.js';
import { stableEntityId } from './rules.js';

export const workflowDefinitionSchema = z.object({
  workflow_definition_id: z.string(),
  source_page_id: z.string(),
  source_database_id: z.string(),
  page_title: z.string().nullable(),
  created_time: z.string().nullable(),
  last_edited_time: z.string().nullable(),
  page_url: z.string().nullable(),
  attributes: z.record(z.string(), z.unknown())
});

export type WorkflowDefinition = z.infer<typeof workflowDefinitionSchema>;

function extractTitleFromRawProperties(rawProperties: Record<string, unknown>): string | null {
  for (const propertyValue of Object.values(rawProperties)) {
    if (!propertyValue || typeof propertyValue !== 'object') {
      continue;
    }

    const typed = propertyValue as { type?: string; title?: unknown[] };
    if (typed.type === 'title' && Array.isArray(typed.title)) {
      const joined = typed.title
        .map((chunk) => (chunk && typeof chunk === 'object' ? (chunk as { plain_text?: string }).plain_text : ''))
        .filter((text): text is string => typeof text === 'string' && text.length > 0)
        .join('');
      return joined.length > 0 ? joined : null;
    }
  }

  return null;
}

export function buildWorkflowDefinition(record: RawRecord): WorkflowDefinition | null {
  if (record.entityType !== 'page' || !record.pageId) {
    return null;
  }

  const rawProperties = Object.fromEntries(
    Object.entries(record.properties).map(([propertyId, property]) => [propertyId, property.rawValue])
  );
  const configuredTitleId = notionConfig.propertyIds.workflowDefinitions.title;
  const configuredTitle =
    configuredTitleId.trim().length > 0 ? extractTitleFromRawProperties({ [configuredTitleId]: rawProperties[configuredTitleId] }) : null;

  return workflowDefinitionSchema.parse({
    workflow_definition_id: stableEntityId('workflow_definition', record.pageId),
    source_page_id: record.pageId,
    source_database_id: record.databaseId,
    page_title: configuredTitle ?? extractTitleFromRawProperties(rawProperties),
    created_time:
      typeof record.metadata?.created_time === 'string' ? (record.metadata.created_time as string) : null,
    last_edited_time: record.lastEditedTime ?? null,
    page_url: typeof record.metadata?.url === 'string' ? (record.metadata.url as string) : null,
    attributes: rawProperties
  });
}
