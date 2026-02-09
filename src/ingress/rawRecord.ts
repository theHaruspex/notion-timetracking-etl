import { z } from 'zod';

export const rawPropertySchema = z.object({
  propertyId: z.string(),
  propertyName: z.string(),
  propertyType: z.string(),
  rawValue: z.unknown()
});

export const rawRecordSchema = z.object({
  source: z.string(),
  entityType: z.union([z.literal('database'), z.literal('page')]),
  databaseId: z.string(),
  pageId: z.string().nullable().optional(),
  lastEditedTime: z.string().nullable().optional(),
  properties: z.record(z.string(), rawPropertySchema),
  metadata: z.record(z.string(), z.unknown()).optional()
});

export type RawProperty = z.infer<typeof rawPropertySchema>;
export type RawRecord = z.infer<typeof rawRecordSchema>;
