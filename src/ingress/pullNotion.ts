import { rawRecordSchema, type RawRecord, type RawProperty } from './rawRecord.js';
import { NotionAdapter, type NotionPageLike } from './notionAdapter.js';

interface DatasetPullInput {
  dataset: string;
  databaseId: string;
}

export interface PullResult {
  rawRecords: RawRecord[];
}

function mapDatabaseProperties(database: any): Record<string, RawProperty> {
  const result: Record<string, RawProperty> = {};

  const properties = database.properties as Record<string, any>;
  for (const [propertyName, propertyValue] of Object.entries(properties)) {
    const propertyId = String((propertyValue as { id?: string }).id ?? propertyName);
    const propertyType = String((propertyValue as { type?: string }).type ?? 'unknown');
    result[propertyId] = {
      propertyId,
      propertyName,
      propertyType,
      rawValue: propertyValue
    };
  }

  return result;
}

function buildPropertyNameToIdMap(database: any): Map<string, { id: string; type: string }> {
  const mapping = new Map<string, { id: string; type: string }>();
  const properties = database.properties as Record<string, any>;

  for (const [propertyName, propertyValue] of Object.entries(properties)) {
    mapping.set(propertyName, {
      id: String((propertyValue as { id?: string }).id ?? propertyName),
      type: String((propertyValue as { type?: string }).type ?? 'unknown')
    });
  }

  return mapping;
}

function mapPageProperties(
  page: NotionPageLike,
  propertyNameToId: Map<string, { id: string; type: string }>
): Record<string, RawProperty> {
  const output: Record<string, RawProperty> = {};

  for (const [propertyName, propertyValue] of Object.entries(page.properties)) {
    const schemaInfo = propertyNameToId.get(propertyName);
    const propertyId = schemaInfo?.id ?? String((propertyValue as { id?: string }).id ?? propertyName);
    const propertyType = schemaInfo?.type ?? String((propertyValue as { type?: string }).type ?? 'unknown');

    output[propertyId] = {
      propertyId,
      propertyName,
      propertyType,
      rawValue: propertyValue
    };
  }

  return output;
}

export async function pullDatasetFromNotion(
  adapter: NotionAdapter,
  input: DatasetPullInput
): Promise<PullResult> {
  const database = await adapter.retrieveDatabase(input.databaseId);
  const pages = await adapter.queryAllPages(input.databaseId);

  const propertyNameToId = buildPropertyNameToIdMap(database);

  const databaseRecord: RawRecord = {
    source: 'notion',
    entityType: 'database',
    databaseId: input.databaseId,
    pageId: null,
    lastEditedTime: (database as { last_edited_time?: string }).last_edited_time ?? null,
    properties: mapDatabaseProperties(database),
    metadata: {
      dataset: input.dataset,
      title: (database as { title?: unknown }).title ?? null,
      url: (database as { url?: string }).url ?? null
    }
  };

  const pageRecords: RawRecord[] = pages.map((page) => ({
    source: 'notion',
    entityType: 'page',
    databaseId: input.databaseId,
    pageId: page.id,
    lastEditedTime: page.last_edited_time,
    properties: mapPageProperties(page, propertyNameToId),
    metadata: {
      dataset: input.dataset,
      created_time: page.created_time ?? null,
      url: page.url ?? null
    }
  }));

  const allRecords = [databaseRecord, ...pageRecords].map((record) => rawRecordSchema.parse(record));

  return { rawRecords: allRecords };
}
