import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createBrokerDependencyHealth } from '../src/broker/dependencyHealth.js';
const valid = (releaseId = 'release-one') => ({ ok: true, service: 'dexter-api', release: { releaseId } });

test('same API origin probes once, coalesces health requests, caches at most five seconds and recovers', async () => {
  let now = 10000, reads = 0, healthy = false, release: (() => void) | undefined;
  const gate = new Promise<void>(resolve => { release = resolve; });
  const health = createBrokerDependencyHealth({ apiBaseUrl: 'http://127.0.0.1:3033', harkApiBaseUrl: 'http://127.0.0.1:3033/',
    now: () => now, fetchImpl: async (url, init) => {
      reads++; assert.equal(String(url), 'http://127.0.0.1:3033/health'); assert.equal(init?.method, 'GET');
      assert.equal(init?.redirect, 'error'); assert.deepEqual(init?.headers, { accept: 'application/json' });
      await gate; if (!healthy) throw new Error('private URL/token detail'); return Response.json(valid());
    } });
  const first = health(), second = health(); assert.equal(reads, 1); release!();
  const failed = await first; assert.deepEqual(await second, failed); assert.equal(failed.ok, false);
  assert.equal(JSON.stringify(failed).includes('private'), false);
  healthy = true; now += 4999; assert.equal((await health()).ok, false); assert.equal(reads, 1);
  now++; const ready = await health(); assert.equal(ready.ok, true); assert.equal(reads, 2);
  ready.primaryApi = { ok: false, reason: 'unavailable' }; assert.equal((await health()).primaryApi.ok, true);
});

test('distinct Primary and Hark origins must both be healthy and serve the same release', async () => {
  const seen: string[] = [];
  const health = createBrokerDependencyHealth({ apiBaseUrl: 'http://127.0.0.1:3033', harkApiBaseUrl: 'http://127.0.0.1:3031',
    fetchImpl: async url => { seen.push(String(url)); return Response.json(valid(String(url).includes('3031') ? 'old-release' : 'new-release')); } });
  const result = await health(); assert.equal(seen.length, 2); assert.equal(result.ok, false);
  assert.deepEqual(result.harkApi, { ok: false, reason: 'release_mismatch' });
});

for (const body of [valid(''), { ok: true, service: 'other', release: { releaseId: 'one' } },
  { ok: true, service: 'dexter-api' }, { ...valid(), ok: false }]) test('refuses a malformed or unrelated dependency health identity', async () => {
  const health = createBrokerDependencyHealth({ apiBaseUrl: 'http://127.0.0.1:3033', fetchImpl: async () => Response.json(body) });
  assert.deepEqual((await health()).primaryApi, { ok: false, reason: 'invalid_health' });
});

test('unconfigured Hark is absent, configured invalid Hark is unhealthy without leaking its URL', async () => {
  let reads = 0;
  const fetchImpl: typeof fetch = async () => { reads++; return Response.json(valid()); };
  assert.equal((await createBrokerDependencyHealth({ apiBaseUrl: 'http://127.0.0.1:3033', fetchImpl })()).harkApi, undefined);
  const result = await createBrokerDependencyHealth({ apiBaseUrl: 'http://127.0.0.1:3033', harkApiBaseUrl: 'https://user:secret@example.test', fetchImpl })();
  assert.deepEqual(result.harkApi, { ok: false, reason: 'configuration_unavailable' }); assert.equal(reads, 2);
  assert.equal(JSON.stringify(result).includes('secret'), false);
});

test('bounds advertised and streamed response bodies and refuses HTTP errors', async () => {
  for (const response of [new Response('large', { headers: { 'content-length': '20000' } }),
    new Response('x'.repeat(20000)), new Response('private error', { status: 503 })]) {
    const health = createBrokerDependencyHealth({ apiBaseUrl: 'http://127.0.0.1:3033', fetchImpl: async () => response });
    const result = await health(); assert.equal(result.ok, false); assert.equal(JSON.stringify(result).includes('private error'), false);
  }
});

test('an actual local stalled HTTP response times out and a later probe recovers', async t => {
  let healthy = false;
  const server = createServer((_request, response) => { if (healthy) { response.setHeader('content-type', 'application/json'); response.end(JSON.stringify(valid())); } });
  await new Promise<void>(yes => server.listen(0, '127.0.0.1', yes));
  t.after(() => { server.closeAllConnections(); server.close(); });
  const health = createBrokerDependencyHealth({ apiBaseUrl: `http://127.0.0.1:${(server.address() as { port: number }).port}`,
    timeoutMs: 50, cacheMs: 0 });
  assert.deepEqual((await health()).primaryApi, { ok: false, reason: 'timeout' });
  healthy = true; assert.equal((await health()).ok, true);
});
