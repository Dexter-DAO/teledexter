import assert from 'node:assert/strict';
import test from 'node:test';
import { createHash } from 'node:crypto';
import { verifySignedAlertRequest } from '../src/outbound/auth.js';
import {
  buildHarkStatusBody, createHarkStatusEvent, deliverHarkStatus,
  type HarkStatusBody, type HarkStatusBinding, type HarkStatusDeliveryOptions,
} from '../src/broker/harkStatusDelivery.js';

const binding: HarkStatusBinding = {
  intentId: '44444444-4444-4444-8444-444444444444',
  callSid: `CA${'1'.repeat(32)}`,
  hark: {
    schemaVersion: 1,
    caseId: '11111111-1111-4111-8111-111111111111',
    watchId: '22222222-2222-4222-8222-222222222222',
    eventId: '33333333-3333-4333-8333-333333333333',
    deliveryId: '44444444-4444-4444-8444-444444444444',
    originTaskId: 'stock-watch-task',
  },
};
const observedAt = '2026-09-23T12:00:00.000Z';
const body = buildHarkStatusBody(binding, 'answered', observedAt);
const secret = 'private-callback-secret-for-offline-tests';
const nowMs = Date.parse('2026-09-23T12:00:10.000Z');
const receipt = (event: HarkStatusBody = body) => ({
  v: 'hark.phone-callback-result.v1', ok: true, replay: false,
  caseId: event.caseId, eventId: event.eventId, deliveryId: event.deliveryId,
  intentId: event.intentId, callSid: event.callSid, originTaskId: event.originTaskId,
  sourceEventId: event.sourceEventId, caseState: 'escalating',
});
const options = (fetchImpl: typeof fetch, overrides: Partial<HarkStatusDeliveryOptions> = {}): HarkStatusDeliveryOptions => ({
  apiBaseUrl: 'https://api.dexter.example/', signingSecret: secret, body,
  now: () => nowMs, nonce: () => 'abcdefghijklmnop', fetchImpl, ...overrides,
});

test('status event uses the legacy source identity and freezes observation time and body digest', () => {
  const expectedIdentity = '{"callSid":"CA11111111111111111111111111111111","caseId":"11111111-1111-4111-8111-111111111111","deliveryId":"44444444-4444-4444-8444-444444444444","intentId":"44444444-4444-4444-8444-444444444444","originTaskId":"stock-watch-task","status":"answered"}';
  assert.equal(body.sourceEventId, `phone-status:${createHash('sha256').update(expectedIdentity).digest('hex')}`);
  const { v: _v, sourceEventId: _id, ...input } = body;
  const event = createHarkStatusEvent(input);
  const later = createHarkStatusEvent({ ...input, observedAt: '2026-09-23T12:00:11.000Z' });
  assert.equal(event.sourceId, body.sourceEventId);
  assert.equal(later.sourceId, event.sourceId);
  assert.notEqual(later.bodyDigest, event.bodyDigest);
  assert.equal(body.observedAt, observedAt);
  assert.ok(Object.isFrozen(event) && Object.isFrozen(event.body));
  assert.throws(() => { (event.body as { observedAt: string }).observedAt = 'changed'; });
});

test('one signed POST returns only the exact status receipt; frozen retry body survives serialization', async () => {
  const requests: { url: string; body: string; timestamp: string; nonce: string }[] = [];
  const fetcher: typeof fetch = async (url, init) => {
    const headers = new Headers(init?.headers);
    const payload = JSON.parse(String(init?.body)) as HarkStatusBody;
    const timestamp = headers.get('x-dexter-alert-timestamp')!;
    const nonce = headers.get('x-dexter-alert-nonce')!;
    assert.deepEqual(verifySignedAlertRequest({ secret, body: payload, nowMs: Number(timestamp) * 1000,
      headers: { timestamp, nonce, signature: headers.get('x-dexter-alert-signature')! } }), { ok: true });
    assert.equal(init?.method, 'POST');
    assert.equal(init?.redirect, 'error');
    assert.ok(init?.signal instanceof AbortSignal);
    requests.push({ url: String(url), body: String(init?.body), timestamp, nonce });
    return Response.json({ ...receipt(payload), replay: requests.length > 1 });
  };
  assert.deepEqual(await deliverHarkStatus(options(fetcher)), { state: 'delivered', httpStatus: 200, receipt: receipt() });
  const saved = JSON.parse(JSON.stringify(body)) as HarkStatusBody;
  const retried = await deliverHarkStatus(options(fetcher, { body: saved, now: () => nowMs + 60_000, nonce: () => 'qrstuvwxyzabcdef' }));
  assert.equal(retried.state, 'delivered');
  if (retried.state === 'delivered') assert.equal(retried.receipt.replay, true);
  assert.equal(requests.length, 2);
  assert.equal(requests[0].url, `https://api.dexter.example/internal/hark/v1/deliveries/${body.deliveryId}/status`);
  assert.equal(requests[0].body, requests[1].body);
  assert.notEqual(requests[0].timestamp, requests[1].timestamp);
  assert.notEqual(requests[0].nonce, requests[1].nonce);
});

for (const status of [401, 403, 408, 429, 500, 503]) {
  test(`HTTP ${status} remains pending without exposing response content`, async () => {
    let calls = 0;
    const result = await deliverHarkStatus(options(async () => { calls++; return new Response(secret, { status }); }));
    assert.deepEqual(result, { state: 'pending', reason: 'http_retry', httpStatus: status });
    assert.equal(calls, 1);
  });
}
for (const status of [400, 404, 409, 422]) {
  test(`HTTP ${status} remains a refusal; status transport cannot resolve admission races`, async () => {
    assert.deepEqual(await deliverHarkStatus(options(async () => new Response(secret, { status }))),
      { state: 'rejected', reason: 'http_refusal', httpStatus: status });
  });
}

for (const field of ['caseId', 'eventId', 'deliveryId', 'intentId', 'callSid', 'originTaskId', 'sourceEventId'] as const) {
  test(`a structurally valid receipt for another ${field} is rejected`, async () => {
    const changed = field === 'callSid' ? `CA${'2'.repeat(32)}`
      : field === 'originTaskId' ? 'foreign-task'
        : field === 'sourceEventId' ? 'phone-status:foreign-event'
          : 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
    const result = await deliverHarkStatus(options(async () => Response.json({ ...receipt(), [field]: changed })));
    assert.deepEqual(result, { state: 'rejected', reason: 'identity_mismatch', httpStatus: 200 });
  });
}

test('malformed success and foreign response/continuation fields stay pending', async () => {
  for (const malformed of [null, {}, { ...receipt(), ok: false }, { ...receipt(), caseState: undefined },
    { ...receipt(), continuation: { secret } }, { ...receipt(), responseId: binding.hark.caseId }]) {
    const result = await deliverHarkStatus(options(async () => Response.json(malformed)));
    assert.deepEqual(result, { state: 'pending', reason: 'invalid_response', httpStatus: 200 });
  }
  assert.deepEqual(await deliverHarkStatus(options(async () => new Response(secret))),
    { state: 'pending', reason: 'invalid_response', httpStatus: 200 });
});

test('oversized successful receipt is bounded and retained for retry', async () => {
  const headerCases: Record<string, string>[] = [{ 'content-length': '20000' }, {}];
  for (const headers of headerCases) {
    const result = await deliverHarkStatus(options(async () => new Response('x'.repeat(20000), { headers })));
    assert.deepEqual(result, { state: 'pending', reason: 'invalid_response', httpStatus: 200 });
  }
});

test('network exceptions reveal only a fixed retry reason', async () => {
  assert.deepEqual(await deliverHarkStatus(options(async () => { throw new Error(`URL/token/raw SQL: ${secret}`); })),
    { state: 'pending', reason: 'request_failed' });
});

test('only valid lifecycle bodies are sent and input is detached before transport', async () => {
  let calls = 0;
  const fetcher: typeof fetch = async () => { calls++; return Response.json(receipt()); };
  for (const invalid of [{ ...body, status: 'acknowledged' }, { ...body, status: 'unknown' },
    { ...body, sourceEventId: 'phone-status:tampered-source' }, { ...body, decision: 'buy' },
    { ...body, observedAt: 'yesterday' }, { ...body, callSid: '' }]) {
    assert.deepEqual(await deliverHarkStatus(options(fetcher, { body: invalid as HarkStatusBody })),
      { state: 'rejected', reason: 'invalid_status_body' });
  }
  assert.equal(calls, 0);
  assert.throws(() => buildHarkStatusBody(binding, 'acknowledged' as never, observedAt), /^Error: invalid_hark_status_input$/);
  const mutable = { ...body };
  const result = await deliverHarkStatus(options(async (_url, init) => {
    assert.equal(JSON.parse(String(init?.body)).caseId, body.caseId);
    return Response.json(receipt());
  }, { body: mutable, now: () => { mutable.caseId = binding.hark.watchId; return nowMs; } }));
  assert.equal(result.state, 'delivered');
});

test('configuration failures are pending and cannot reach fetch', async () => {
  let calls = 0;
  const fetcher: typeof fetch = async () => { calls++; throw new Error('unexpected fetch'); };
  for (const overrides of [
    { signingSecret: 'short' }, { apiBaseUrl: `https://user:${secret}@example.com` },
    { apiBaseUrl: 'http://example.com' }, { apiBaseUrl: 'https://example.com?secret=1' },
    { timeoutMs: 0 }, { timeoutMs: 30001 }, { nonce: () => 'bad' }, { now: () => NaN },
    { nonce: () => { throw new Error(secret); } },
  ]) {
    assert.deepEqual(await deliverHarkStatus(options(fetcher, overrides)),
      { state: 'pending', reason: 'configuration_unavailable' });
  }
  assert.equal(calls, 0);
});
