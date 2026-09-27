/** Transport readiness only. Account admission and provider readiness remain separate. */
export type ApiDependencyHealth =
  | { ok: true; releaseId: string }
  | { ok: false; reason: 'configuration_unavailable' | 'unavailable' | 'timeout' | 'http_error' | 'invalid_health' | 'release_mismatch' };
export interface BrokerDependencyHealth {
  ok: boolean;
  checkedAt: string;
  primaryApi: ApiDependencyHealth;
  harkApi?: ApiDependencyHealth;
}
const MAX_BODY_BYTES = 16 * 1024;

export function createBrokerDependencyHealth(options: {
  apiBaseUrl: string;
  harkApiBaseUrl?: string;
  fetchImpl?: typeof fetch;
  now?: () => number;
  timeoutMs?: number;
  cacheMs?: number;
}): () => Promise<BrokerDependencyHealth> {
  const request = options.fetchImpl ?? fetch;
  const now = options.now ?? Date.now;
  const timeoutMs = options.timeoutMs ?? 2000;
  const cacheMs = options.cacheMs ?? 5000;
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 2000
    || !Number.isSafeInteger(cacheMs) || cacheMs < 0 || cacheMs > 5000) throw new Error('invalid_dependency_health_bounds');
  const endpoint = (value: string): string | undefined => {
    try {
      const url = new URL(value);
      if (url.username || url.password || url.search || url.hash || url.pathname !== '/'
        || (url.protocol !== 'https:' && !(url.protocol === 'http:' && ['127.0.0.1', '[::1]', 'localhost'].includes(url.hostname)))) return undefined;
      return new URL('/health', url).href;
    } catch { return undefined; }
  };
  const primaryUrl = endpoint(options.apiBaseUrl);
  const hasHark = options.harkApiBaseUrl !== undefined;
  const harkUrl = hasHark ? endpoint(options.harkApiBaseUrl!) : undefined;
  let cached: { checkedAtMs: number; value: BrokerDependencyHealth } | undefined;
  let active: Promise<BrokerDependencyHealth> | undefined;
  async function probe(url: string | undefined): Promise<ApiDependencyHealth> {
    if (!url) return { ok: false, reason: 'configuration_unavailable' };
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    timer.unref();
    try {
      const response = await request(url, { method: 'GET', redirect: 'error', signal: controller.signal,
        headers: { accept: 'application/json' } });
      if (response.status !== 200) {
        await response.body?.cancel().catch(() => {});
        return { ok: false, reason: 'http_error' };
      }
      const size = response.headers.get('content-length');
      if (size && /^\d+$/.test(size) && Number(size) > MAX_BODY_BYTES) {
        await response.body?.cancel().catch(() => {});
        return { ok: false, reason: 'invalid_health' };
      }
      const reader = response.body?.getReader();
      if (!reader) return { ok: false, reason: 'invalid_health' };
      const chunks: Uint8Array[] = []; let bytes = 0;
      try {
        for (;;) {
          const next = await reader.read();
          if (next.done) break;
          bytes += next.value.byteLength;
          if (bytes > MAX_BODY_BYTES) {
            await reader.cancel().catch(() => {});
            return { ok: false, reason: 'invalid_health' };
          }
          chunks.push(next.value);
        }
      } finally { reader.releaseLock(); }
      let body: any;
      try { body = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks))); }
      catch { return { ok: false, reason: 'invalid_health' }; }
      return body?.ok === true && body?.service === 'dexter-api'
        && typeof body?.release?.releaseId === 'string' && /^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$/.test(body.release.releaseId)
        ? { ok: true, releaseId: body.release.releaseId } : { ok: false, reason: 'invalid_health' };
    } catch { return { ok: false, reason: controller.signal.aborted ? 'timeout' : 'unavailable' }; }
    finally { clearTimeout(timer); }
  }
  return async () => {
    const checkedAtMs = now();
    if (cached && checkedAtMs >= cached.checkedAtMs && checkedAtMs - cached.checkedAtMs < cacheMs)
      return structuredClone(cached.value);
    if (!active) active = (async () => {
      const primaryPromise = probe(primaryUrl);
      const [primaryApi, harkApi] = await Promise.all([primaryPromise,
        hasHark ? harkUrl === primaryUrl ? primaryPromise : probe(harkUrl) : undefined]);
      const matchingHark = harkApi?.ok && primaryApi.ok && harkApi.releaseId !== primaryApi.releaseId
        ? { ok: false as const, reason: 'release_mismatch' as const } : harkApi;
      const value: BrokerDependencyHealth = { ok: primaryApi.ok && (!hasHark || matchingHark?.ok === true),
        checkedAt: new Date(checkedAtMs).toISOString(), primaryApi, ...(matchingHark ? { harkApi: matchingHark } : {}) };
      cached = { checkedAtMs, value };
      return value;
    })().finally(() => { active = undefined; });
    return structuredClone(await active);
  };
}
