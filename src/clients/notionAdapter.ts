import { Client } from '@notionhq/client';
import { retryAsync } from '../lib/retry.js';
import { createGlobalRateLimiter } from '../lib/rateLimit.js';

export interface NotionAdapterConfig {
  authToken: string;
}

export interface NotionPageLike {
  id: string;
  last_edited_time: string;
  created_time?: string;
  url?: string;
  properties: Record<string, any>;
}

interface QueryDatabaseRequest {
  database_id: string;
  start_cursor?: string;
  page_size?: number;
}

interface QueryDatabasePageResponse {
  results: Array<NotionPageLike | Record<string, unknown>>;
  has_more: boolean;
  next_cursor: string | null;
}

export class NotionAdapter {
  private readonly client: Client;

  constructor(config: NotionAdapterConfig) {
    this.client = new Client({ auth: config.authToken });
  }

  async retrieveDatabase(databaseId: string): Promise<any> {
    return this.limitedRequest(() =>
      this.client.databases.retrieve({
        database_id: databaseId
      })
    );
  }

  async queryAllPages(databaseId: string): Promise<NotionPageLike[]> {
    const pages: NotionPageLike[] = [];
    let cursor: string | undefined = undefined;

    do {
      const response = await this.queryDatabasePage({
        database_id: databaseId,
        start_cursor: cursor,
        page_size: 100
      });

      const current = response.results.filter(
        (entry): entry is NotionPageLike =>
          typeof entry === 'object' &&
          entry !== null &&
          'properties' in entry &&
          'id' in entry &&
          'last_edited_time' in entry
      );
      pages.push(...current);

      cursor = response.has_more ? response.next_cursor ?? undefined : undefined;
    } while (cursor);

    return pages;
  }

  private async queryDatabasePage(params: QueryDatabaseRequest): Promise<QueryDatabasePageResponse> {
    return this.limitedRequest(() => this.client.databases.query(params) as Promise<QueryDatabasePageResponse>);
  }

  private readonly limiter = createGlobalRateLimiter(3);

  private async limitedRequest<T>(requestFn: () => Promise<T>): Promise<T> {
    return this.limiter.schedule(() =>
      retryAsync(
        () => requestFn(),
        { maxRetries: 5, baseDelayMs: 500, maxDelayMs: 8_000 }
      )
    );
  }
}
