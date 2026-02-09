import { writeJsonl } from '../lib/fs.js';

export async function writeJsonlSink(filePath: string, records: unknown[]): Promise<void> {
  await writeJsonl(filePath, records);
}
