export type DexterApiReadinessSnapshot =
  | {
      status: 'unknown' | 'checking';
      checkedAt: string | null;
      expiresAt: string | null;
      reason: null;
    }
  | {
      status: 'ready';
      checkedAt: string;
      expiresAt: string;
      reason: null;
    }
  | {
      status: 'unavailable';
      checkedAt: string;
      expiresAt: string;
      reason: 'dexter_api_unreachable' | 'dexter_api_unhealthy';
    };

export function requireDexterApiBaseUrl(configured: string | undefined): string {
  if (!configured?.trim()) {
    throw new Error('DEXTER_API_URL is required');
  }
  const parsed = new URL(configured);
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    throw new Error('DEXTER_API_URL must use http or https');
  }
  parsed.pathname = parsed.pathname.replace(/\/+$/u, '');
  parsed.search = '';
  parsed.hash = '';
  return parsed.toString().replace(/\/+$/u, '');
}

export interface DexterApiReadinessOptions {
  baseUrl: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  successTtlMs?: number;
  failureTtlMs?: number;
  now?: () => number;
}

export class DexterApiReadiness {
  private snapshot: DexterApiReadinessSnapshot = {
    status: 'unknown',
    checkedAt: null,
    expiresAt: null,
    reason: null,
  };
  private inFlight: Promise<DexterApiReadinessSnapshot> | null = null;
  private readonly fetchImpl: typeof fetch;
  private readonly now: () => number;
  private readonly healthUrl: string;

  constructor(private readonly options: DexterApiReadinessOptions) {
    this.fetchImpl = options.fetchImpl ?? fetch;
    this.now = options.now ?? Date.now;
    this.healthUrl = `${requireDexterApiBaseUrl(options.baseUrl)}/health`;
  }

  current(): DexterApiReadinessSnapshot {
    return structuredClone(this.snapshot);
  }

  async check(force = false): Promise<DexterApiReadinessSnapshot> {
    const expiresAtMs = this.snapshot.expiresAt
      ? Date.parse(this.snapshot.expiresAt)
      : 0;
    if (!force && this.snapshot.status !== 'checking' && expiresAtMs > this.now()) {
      return this.current();
    }
    if (this.inFlight) return this.inFlight;

    this.snapshot = {
      status: 'checking',
      checkedAt: this.snapshot.checkedAt,
      expiresAt: null,
      reason: null,
    };
    this.inFlight = (async (): Promise<DexterApiReadinessSnapshot> => {
      const checkedAt = new Date(this.now()).toISOString();
      let status: DexterApiReadinessSnapshot['status'] = 'unavailable';
      let reason: 'dexter_api_unreachable' | 'dexter_api_unhealthy' | null = null;
      try {
        const response = await this.fetchImpl(this.healthUrl, {
          signal: AbortSignal.timeout(this.options.timeoutMs ?? 2_500),
        });
        let body: unknown = null;
        try {
          body = await response.json();
        } catch {
          // A 2xx from a proxy, static site, or wrong localhost service is not
          // proof that the Dexter API dependency is ready.
        }
        const exactApiHealth = body as { ok?: unknown; service?: unknown } | null;
        if (
          response.ok
          && exactApiHealth?.ok === true
          && exactApiHealth.service === 'dexter-api'
        ) {
          status = 'ready';
        } else {
          reason = 'dexter_api_unhealthy';
        }
      } catch {
        reason = 'dexter_api_unreachable';
      }
      const ttlMs = status === 'ready'
        ? (this.options.successTtlMs ?? 15_000)
        : (this.options.failureTtlMs ?? 5_000);
      this.snapshot = status === 'ready'
        ? {
            status,
            checkedAt,
            expiresAt: new Date(this.now() + ttlMs).toISOString(),
            reason: null,
          }
        : {
            status,
            checkedAt,
            expiresAt: new Date(this.now() + ttlMs).toISOString(),
            reason: reason ?? 'dexter_api_unhealthy',
          };
      return this.current();
    })().finally(() => {
      this.inFlight = null;
    });
    return this.inFlight;
  }
}
