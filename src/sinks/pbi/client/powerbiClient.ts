import { retryAsync, defaultRetryDecision } from '../../../lib/retry.js';
import type { PbiDatasetSpec } from '../spec/types.js';
import { PowerBiServicePrincipalAuth } from './auth.js';

export interface PowerBiClientConfig {
  auth: PowerBiServicePrincipalAuth;
  baseUrl?: string;
}

export interface PowerBiDatasetSummary {
  id: string;
  name: string;
}

export interface PowerBiTableResponse {
  name: string;
  columns?: Array<{ name: string; dataType: string }>;
}

export class PowerBiClient {
  private readonly auth: PowerBiServicePrincipalAuth;
  private readonly baseUrl: string;

  constructor(config: PowerBiClientConfig) {
    this.auth = config.auth;
    this.baseUrl = (config.baseUrl ?? 'https://api.powerbi.com/v1.0/myorg').replace(/\/+$/, '');
  }

  async getDatasetsInGroup(groupId: string): Promise<PowerBiDatasetSummary[]> {
    const result = await this.requestJson<{ value?: PowerBiDatasetSummary[] }>(
      'GET',
      `/groups/${groupId}/datasets`
    );
    return result.value ?? [];
  }

  async createDatasetInGroup(groupId: string, spec: PbiDatasetSpec): Promise<PowerBiDatasetSummary> {
    const defaultRetentionPolicy = spec.defaultRetentionPolicy ?? 'None';
    const route = `/groups/${groupId}/datasets?defaultRetentionPolicy=${encodeURIComponent(
      defaultRetentionPolicy
    )}`;
    const body: {
      name: string;
      defaultMode: 'Push';
      tables: Array<{ name: string; columns: Array<{ name: string; dataType: string }> }>;
      relationships?: Array<{
        name: string;
        fromTable: string;
        fromColumn: string;
        toTable: string;
        toColumn: string;
        crossFilteringBehavior?: string;
      }>;
    } = {
      name: spec.name,
      defaultMode: 'Push',
      tables: spec.tables.map((table) => ({
        name: table.name,
        columns: table.columns.map((column) => ({
          name: column.name,
          dataType: mapColumnTypeForApi(column.dataType)
        }))
      }))
    };
    if (Array.isArray(spec.relationships) && spec.relationships.length > 0) {
      body.relationships = spec.relationships.map((relationship) => ({
        name: relationship.name,
        fromTable: relationship.fromTable,
        fromColumn: relationship.fromColumn,
        toTable: relationship.toTable,
        toColumn: relationship.toColumn,
        crossFilteringBehavior: relationship.crossFilteringBehavior
      }));
    }
    return this.requestJson<PowerBiDatasetSummary>('POST', route, body);
  }

  async getTablesInGroup(groupId: string, datasetId: string): Promise<PowerBiTableResponse[]> {
    const result = await this.requestJson<{ value?: PowerBiTableResponse[] }>(
      'GET',
      `/groups/${groupId}/datasets/${datasetId}/tables`
    );
    return result.value ?? [];
  }

  async putTable(datasetId: string, tableName: string, columns: Array<{ name: string; dataType: string }>): Promise<void> {
    await this.requestNoContent('PUT', `/datasets/${datasetId}/tables/${encodeURIComponent(tableName)}`, {
      name: tableName,
      columns
    });
  }

  async deleteRowsInGroup(groupId: string, datasetId: string, tableName: string): Promise<void> {
    await this.requestNoContent(
      'DELETE',
      `/groups/${groupId}/datasets/${datasetId}/tables/${encodeURIComponent(tableName)}/rows`
    );
  }

  async postRowsInGroup(groupId: string, datasetId: string, tableName: string, rows: object[]): Promise<void> {
    await this.requestNoContent(
      'POST',
      `/groups/${groupId}/datasets/${datasetId}/tables/${encodeURIComponent(tableName)}/rows`,
      { rows }
    );
  }

  private async requestJson<T>(method: string, route: string, body?: unknown): Promise<T> {
    return retryAsync(
      async () => this.executeRequest<T>(method, route, body),
      { maxRetries: 5, baseDelayMs: 500, maxDelayMs: 10_000 },
      defaultRetryDecision
    );
  }

  private async requestNoContent(method: string, route: string, body?: unknown): Promise<void> {
    await this.requestJson<unknown>(method, route, body);
  }

  private async executeRequest<T>(method: string, route: string, body?: unknown): Promise<T> {
    const token = await this.auth.getAccessToken();
    const url = `${this.baseUrl}${route}`;

    let response: Response;
    try {
      response = await fetch(url, {
        method,
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: body === undefined ? undefined : JSON.stringify(body)
      });
    } catch (error) {
      throw { status: 503, message: (error as Error).message };
    }

    if (!response.ok) {
      throw {
        status: response.status,
        headers: {
          'retry-after': response.headers.get('retry-after') ?? undefined
        },
        bodyText: await safeResponseText(response),
        message: `Power BI API error (${response.status}) for ${method} ${route}`
      };
    }

    if (response.status === 204) {
      return {} as T;
    }

    const text = await safeResponseText(response);
    if (text.trim().length === 0) {
      return {} as T;
    }
    return JSON.parse(text) as T;
  }
}

async function safeResponseText(response: Response): Promise<string> {
  try {
    return await response.text();
  } catch {
    return '';
  }
}

function mapColumnTypeForApi(dataType: string): string {
  if (dataType === 'DateTime') {
    return 'Datetime';
  }
  if (dataType === 'Boolean') {
    return 'Bool';
  }
  return dataType;
}

