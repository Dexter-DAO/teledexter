import assert from 'node:assert/strict';
import test from 'node:test';
import { createHmac } from 'node:crypto';
import { fetchCallerLookup } from '../src/session/callerLookup.js';

const secret = 'phone-service-test-secret-at-least-32-characters';

test('caller lookup authenticates the exact caller and call through the configured API', async () => {
  const response = await fetchCallerLookup({
    apiBaseUrl: 'https://api.example.test/',
    callerNumber: '+15551234567',
    callSid: 'CA-call-123',
    serviceSecret: secret,
    fetchImpl: async (input, init) => {
      const url = new URL(String(input));
      assert.equal(url.origin, 'https://api.example.test');
      assert.equal(url.pathname, '/internal/phone/lookup');
      assert.equal(url.searchParams.get('number'), '+15551234567');
      assert.equal(url.searchParams.get('callSid'), 'CA-call-123');
      const credential = createHmac('sha256', secret)
        .update('dexter-phone/caller-lookup/v1').digest('hex');
      assert.equal(new Headers(init?.headers).get('authorization'), `Bearer ${credential}`);
      assert.equal(new Headers(init?.headers).get('authorization')?.includes(secret), false);
      assert.equal(init?.redirect, 'error');
      return Response.json({ ok: true, found: false });
    },
  });
  assert.deepEqual(await response.json(), { ok: true, found: false });
});

test('missing service authentication cannot dispatch a caller lookup', async () => {
  let dispatched = false;
  await assert.rejects(fetchCallerLookup({
    apiBaseUrl: 'https://api.example.test', callerNumber: '+15551234567',
    serviceSecret: '',
    fetchImpl: async () => { dispatched = true; return Response.json({}); },
  }), /phone_lookup_auth_unavailable/);
  assert.equal(dispatched, false);
});

test('a rejected lookup cannot be interpreted as a successful identity response', async () => {
  await assert.rejects(fetchCallerLookup({
    apiBaseUrl: 'https://api.example.test', callerNumber: '+15551234567',
    serviceSecret: secret,
    fetchImpl: async () => Response.json({ ok: true, found: true }, { status: 401 }),
  }), /phone_lookup_http_401/);
});
