import { retryAsync, defaultRetryDecision } from '../../../lib/retry.js';

export interface ServicePrincipalAuthInput {
  tenantId: string;
  clientId: string;
  clientSecret: string;
}

interface CachedToken {
  accessToken: string;
  expiresAtMs: number;
}

export class PowerBiServicePrincipalAuth {
  private readonly tenantId: string;
  private readonly clientId: string;
  private readonly clientSecret: string;
  private cachedToken: CachedToken | null = null;

  constructor(input: ServicePrincipalAuthInput) {
    this.tenantId = input.tenantId;
    this.clientId = input.clientId;
    this.clientSecret = input.clientSecret;
  }

  async getAccessToken(): Promise<string> {
    if (this.cachedToken && Date.now() < this.cachedToken.expiresAtMs - 60_000) {
      return this.cachedToken.accessToken;
    }

    const token = await retryAsync(
      async () => this.fetchToken(),
      { maxRetries: 5, baseDelayMs: 500, maxDelayMs: 10_000 },
      defaultRetryDecision
    );

    this.cachedToken = token;
    return token.accessToken;
  }

  private async fetchToken(): Promise<CachedToken> {
    const url = `https://login.microsoftonline.com/${this.tenantId}/oauth2/v2.0/token`;
    const body = new URLSearchParams({
      grant_type: 'client_credentials',
      client_id: this.clientId,
      client_secret: this.clientSecret,
      scope: 'https://analysis.windows.net/powerbi/api/.default'
    });

    let response: Response;
    try {
      response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: body.toString()
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
        message: await safeResponseText(response)
      };
    }

    const payload = (await response.json()) as {
      access_token?: string;
      expires_in?: number;
    };

    if (!payload.access_token) {
      throw new Error('Power BI auth failed: token response missing access_token.');
    }

    const expiresIn = Number.isFinite(payload.expires_in ?? NaN) ? payload.expires_in ?? 3600 : 3600;
    return {
      accessToken: payload.access_token,
      expiresAtMs: Date.now() + expiresIn * 1000
    };
  }
}

async function safeResponseText(response: Response): Promise<string> {
  try {
    return await response.text();
  } catch {
    return '';
  }
}
