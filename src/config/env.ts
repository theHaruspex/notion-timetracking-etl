import path from 'node:path';
import dotenv from 'dotenv';
import { z } from 'zod';

dotenv.config();

const envSchema = z.object({
  NOTION_TOKEN: z.string().min(1),
  NOTION_DB_WORKFLOW_DEFINITIONS: z.string().min(1),
  NOTION_DB_WORKFLOW_STAGES: z.string().min(1),
  NOTION_DB_TIMESLICES: z.string().min(1),
  DATA_DIR: z.string().default('./data'),
  LOG_LEVEL: z.string().default('info')
});

export type AppConfig = z.infer<typeof envSchema> & {
  resolvedDataDir: string;
};

export function loadConfig(): AppConfig {
  const parsed = envSchema.parse(process.env);
  return {
    ...parsed,
    resolvedDataDir: path.resolve(parsed.DATA_DIR)
  };
}
