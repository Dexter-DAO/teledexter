import assert from 'node:assert/strict';
import test from 'node:test';
import {
  DexterApiReadiness,
  requireDexterApiBaseUrl,
} from '../src/outbound/DexterApiReadiness.js';

test('Dexter API URL is explicit, normalized, and limited to HTTP', () => {
  assert.throws(() => requireDexterApiBaseUrl(undefined), /required/u);
  assert.throws(() => requireDexterApiBaseUrl('file:///tmp/api'), /http/u);
  assert.equal(
    requireDexterApiBaseUrl('http://127.0.0.1:3039///?ignored=yes#ignored'),
    'http://127.0.0.1:3039',
  );
});

test('Dexter API readiness single-flights and expires its cache', async () => {
  let nowMs = Date.parse('2026-08-31T00:00:00.000Z');
  let calls = 0;
  let release: (() => void) | null = null;
  const readiness = new DexterApiReadiness({
    baseUrl: 'http://127.0.0.1:3039',
    now: () => nowMs,
    successTtlMs: 1_000,
    fetchImpl: async (input) => {
      calls += 1;
      assert.equal(String(input), 'http://127.0.0.1:3039/health');
      await new Promise<void>((resolve) => { release = resolve; });
      return new Response(JSON.stringify({ ok: true, service: 'dexter-api' }), {
        status: 200,
      });
    },
  });
  const first = readiness.check();
  const second = readiness.check();
  assert.equal(calls, 1);
  release?.();
  assert.equal((await first).status, 'ready');
  assert.equal((await second).status, 'ready');
  assert.equal((await readiness.check()).status, 'ready');
  assert.equal(calls, 1);

  nowMs += 1_001;
  const expired = readiness.check();
  release?.();
  assert.equal((await expired).status, 'ready');
  assert.equal(calls, 2);
});

test('Dexter API readiness distinguishes unhealthy HTTP from unreachable', async () => {
  const unhealthy = new DexterApiReadiness({
    baseUrl: 'http://127.0.0.1:3039',
    fetchImpl: async () => new Response(
      JSON.stringify({ ok: false, service: 'dexter-api' }),
      { status: 503 },
    ),
  });
  assert.equal((await unhealthy.check()).reason, 'dexter_api_unhealthy');

  const unreachable = new DexterApiReadiness({
    baseUrl: 'http://127.0.0.1:3039',
    fetchImpl: async () => { throw new Error('offline'); },
  });
  assert.equal((await unreachable.check()).reason, 'dexter_api_unreachable');
});

test('Dexter API readiness rejects a 2xx from the wrong service or an invalid body', async () => {
  const wrongService = new DexterApiReadiness({
    baseUrl: 'http://127.0.0.1:3039',
    fetchImpl: async () => new Response(
      JSON.stringify({ ok: true, service: 'dexter-phone' }),
      { status: 200 },
    ),
  });
  assert.equal((await wrongService.check()).reason, 'dexter_api_unhealthy');

  const proxyHtml = new DexterApiReadiness({
    baseUrl: 'http://127.0.0.1:3039',
    fetchImpl: async () => new Response('<html>healthy</html>', { status: 200 }),
  });
  assert.equal((await proxyHtml.check()).reason, 'dexter_api_unhealthy');
});
