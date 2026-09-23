import assert from 'node:assert/strict';
import test from 'node:test';
import { createHash, createHmac } from 'node:crypto';
import { canonicalJson } from '../src/outbound/canonical.js';
import {
  deliverHarkBilling, HarkCallEndedReceiptSchema, HarkOutboundStatusBodySchema,
  type HarkBillingDeliveryOptions, type HarkCallEndedBody, type HarkOutboundStatusBody,
} from '../src/broker/harkBillingDelivery.js';

const secret = 'private-callback-secret-for-offline-tests';
const now = Date.parse('2026-09-23T12:00:00.000Z');
const statusBody: HarkOutboundStatusBody = Object.freeze({
  intentId: '44444444-4444-4444-8444-444444444444',
  eventId: '33333333-3333-4333-8333-333333333333',
  callSid: `CA${'1'.repeat(32)}`,
  state: 'completed',
});
const endedBody: HarkCallEndedBody = Object.freeze({
  source: 'phone', direction: 'outbound-alert',
  alert_intent_id: statusBody.intentId, alert_event_id: statusBody.eventId,
  external_sid: statusBody.callSid,
  started_at: '2026-09-23T11:00:00+01:00',
  ended_at: '2026-09-23T11:02:03+01:00', duration_seconds: 123,
  caller_user_id: '22222222-2222-4222-8222-222222222222',
});
const receipt = { ok: true as const, id: '55555555-5555-4555-8555-555555555555', status: 'completed' as const };
const options = (fetchImpl: typeof fetch, callback: Pick<HarkBillingDeliveryOptions, 'kind' | 'body'> = { kind: 'outbound-status', body: statusBody }): HarkBillingDeliveryOptions => ({
  apiBaseUrl: 'http://127.0.0.1:3030/', signingSecret: secret,
  now: () => now, nonce: () => 'abcdefghijklmnop', fetchImpl, ...callback,
} as HarkBillingDeliveryOptions);

test('status uses the exact API v2 path/signature and accepts only the empty 204', async () => {
  let calls = 0;
  const result = await deliverHarkBilling(options(async (url, init) => {
    calls++;
    assert.equal(String(url), 'http://127.0.0.1:3030/internal/voice/outbound-status');
    assert.equal(init?.method, 'POST');
    assert.equal(init?.redirect, 'error');
    assert.ok(init?.signal instanceof AbortSignal);
    assert.equal(init?.body, JSON.stringify(statusBody));
    const headers = new Headers(init?.headers);
    const hash = createHash('sha256').update(canonicalJson(statusBody)).digest('hex');
    const timestamp = String(Math.floor(now / 1000));
    const signature = createHmac('sha256', secret)
      .update(`v2\n/internal/voice/outbound-status\n${timestamp}\nabcdefghijklmnop\n${hash}`).digest('hex');
    assert.equal(headers.get('x-dexter-callback-signature'), `v2=${signature}`);
    assert.equal(headers.get('x-dexter-alert-signature'), null);
    assert.equal(headers.get('x-dexter-alert-timestamp'), timestamp);
    assert.equal(headers.get('x-dexter-alert-nonce'), 'abcdefghijklmnop');
    return new Response(null, { status: 204 });
  }));
  assert.equal(calls, 1);
  assert.deepEqual(result, { state: 'delivered', purpose: 'voice.outbound-status.v2', httpStatus: 204 });
});

test('call-ended retries retain exact receipt body bytes while using a fresh nonce and signature', async () => {
  const requests: { body: string; signature: string; nonce: string }[] = [];
  const fetcher: typeof fetch = async (url, init) => {
    assert.equal(String(url), 'http://127.0.0.1:3030/internal/voice/call-ended');
    const headers = new Headers(init?.headers);
    const timestamp = headers.get('x-dexter-alert-timestamp')!;
    const nonce = headers.get('x-dexter-alert-nonce')!;
    const hash = createHash('sha256').update(canonicalJson(JSON.parse(String(init?.body)))).digest('hex');
    const signature = `v2=${createHmac('sha256', secret)
      .update(`v2\n/internal/voice/call-ended\n${timestamp}\n${nonce}\n${hash}`).digest('hex')}`;
    assert.equal(headers.get('x-dexter-callback-signature'), signature);
    requests.push({ body: String(init?.body), signature, nonce });
    return Response.json(receipt);
  };
  const first = options(fetcher, { kind: 'call-ended', body: endedBody });
  const result = await deliverHarkBilling(first);
  assert.deepEqual(result, { state: 'delivered', purpose: 'voice.call-ended.v2', httpStatus: 200, receipt });
  assert.ok(result.state === 'delivered' && result.purpose === 'voice.call-ended.v2' && Object.isFrozen(result.receipt));
  const restarted = options(fetcher, { kind: 'call-ended', body: JSON.parse(JSON.stringify(endedBody)) as HarkCallEndedBody });
  restarted.now = () => now + 60_000;
  restarted.nonce = () => 'qrstuvwxyzabcdef';
  await deliverHarkBilling(restarted);
  assert.equal(requests.length, 2);
  assert.equal(requests[0].body, JSON.stringify(endedBody));
  assert.equal(requests[0].body, requests[1].body);
  assert.notEqual(requests[0].nonce, requests[1].nonce);
  assert.notEqual(requests[0].signature, requests[1].signature);
  assert.equal(JSON.parse(requests[0].body).started_at, endedBody.started_at);
  assert.equal(JSON.parse(requests[0].body).duration_seconds, 123);
});

test('all API lifecycle states are supported without translating status into an answer or consent', async () => {
  for (const state of HarkOutboundStatusBodySchema.shape.state.options) {
    const body = { ...statusBody, state };
    const result = await deliverHarkBilling(options(async (_url, init) => {
      assert.deepEqual(JSON.parse(String(init?.body)), body);
      return new Response(null, { status: 204 });
    }, { kind: 'outbound-status', body }));
    assert.deepEqual(result, { state: 'delivered', purpose: 'voice.outbound-status.v2', httpStatus: 204 });
  }
});

test('call-ended accepts the classifier receipt without claiming a charge or useful conversation', async () => {
  for (const status of HarkCallEndedReceiptSchema.shape.status.options) {
    const result = await deliverHarkBilling(options(async () => Response.json({ ...receipt, status }), { kind: 'call-ended', body: endedBody }));
    assert.deepEqual(result, { state: 'delivered', purpose: 'voice.call-ended.v2', httpStatus: 200, receipt: { ...receipt, status } });
  }
});

test('optional owner stays absent and zero observed duration remains zero', async () => {
  const { caller_user_id: _owner, ...body } = endedBody;
  body.duration_seconds = 0;
  const result = await deliverHarkBilling(options(async (_url, init) => {
    const actual = JSON.parse(String(init?.body));
    assert.equal(actual.duration_seconds, 0);
    assert.equal(Object.hasOwn(actual, 'caller_user_id'), false);
    return Response.json({ ...receipt, status: 'silent' });
  }, { kind: 'call-ended', body }));
  assert.equal(result.state, 'delivered');
});

for (const status of [401, 403, 408, 429, 500, 503]) {
  test(`HTTP ${status} is a pending single attempt with no response detail leakage`, async () => {
    let calls = 0;
    const result = await deliverHarkBilling(options(async () => { calls++; return new Response(secret, { status }); }));
    assert.equal(calls, 1);
    assert.deepEqual(result, { state: 'pending', reason: 'http_retry', httpStatus: status });
  });
}
for (const status of [302, 400, 404, 409, 422]) {
  test(`HTTP ${status} is refused, including nonce replay or changed financial receipt conflicts`, async () => {
    const result = await deliverHarkBilling(options(async () => new Response(secret, { status }), { kind: 'call-ended', body: endedBody }));
    assert.deepEqual(result, { state: 'rejected', reason: 'http_refusal', httpStatus: status });
  });
}

test('unexpected success statuses and malformed or extended receipts remain pending', async () => {
  for (const response of [Response.json({ ok: true }), new Response(null, { status: 201 }),
    { status: 204, ok: true, headers: new Headers(), body: new Response('unexpected').body } as Response]) {
    const result = await deliverHarkBilling(options(async () => response));
    assert.deepEqual(result, { state: 'pending', reason: 'invalid_response', httpStatus: response.status });
  }
  for (const value of [null, {}, { ...receipt, id: 'foreign' }, { ...receipt, ok: false }, { ...receipt, status: 'charged' },
    { ...receipt, billing: { paid: true } }, { ...receipt, external_sid: statusBody.callSid }]) {
    assert.deepEqual(await deliverHarkBilling(options(async () => Response.json(value), { kind: 'call-ended', body: endedBody })),
      { state: 'pending', reason: 'invalid_response', httpStatus: 200 });
  }
  for (const response of [new Response('not json'), new Response(null, { status: 204 })]) {
    assert.deepEqual(await deliverHarkBilling(options(async () => response, { kind: 'call-ended', body: endedBody })),
      { state: 'pending', reason: 'invalid_response', httpStatus: response.status });
  }
});

test('receipt size is bounded even without a content-length header', async () => {
  for (const response of [new Response('x'.repeat(16 * 1024 + 1)),
    new Response('{}', { headers: { 'content-length': '999999' } })]) {
    assert.deepEqual(await deliverHarkBilling(options(async () => response, { kind: 'call-ended', body: endedBody })),
      { state: 'pending', reason: 'invalid_response', httpStatus: 200 });
  }
});

test('invalid or expanded bodies are refused before any request', async () => {
  const invalid = [
    { kind: 'outbound-status', body: { ...statusBody, state: 'acknowledged' } },
    { kind: 'outbound-status', body: { ...statusBody, intentId: 'unknown' } },
    { kind: 'outbound-status', body: { ...statusBody, eventId: 'short' } },
    { kind: 'outbound-status', body: { ...statusBody, callSid: '' } },
    { kind: 'outbound-status', body: { ...statusBody, caller_user_id: endedBody.caller_user_id } },
    ...[-1, 1.5, Number.MAX_SAFE_INTEGER + 1, NaN].map(duration_seconds => ({ kind: 'call-ended', body: { ...endedBody, duration_seconds } })),
    { kind: 'call-ended', body: { ...endedBody, started_at: 'not observed' } },
    { kind: 'call-ended', body: { ...endedBody, caller_user_id: 'unknown' } },
    { kind: 'call-ended', body: { ...endedBody, mandate_id: 'invented' } },
    { kind: 'call-ended', body: { ...endedBody, source: 'broker' } },
  ];
  for (const callback of invalid) {
    const input = { ...options(async () => { throw new Error('must not fetch'); }), ...callback } as HarkBillingDeliveryOptions;
    assert.deepEqual(await deliverHarkBilling(input), { state: 'rejected', reason: 'invalid_callback_body' });
  }
});

test('bad configuration remains pending and never leaks raw values', async () => {
  const overrides = [
    { apiBaseUrl: 'http://remote.example' }, { apiBaseUrl: 'https://secret:password@example.com' },
    { apiBaseUrl: 'https://example.com/?secret=value' }, { signingSecret: 'short' },
    { signingSecret: ` ${'s'.repeat(31)} ` }, { timeoutMs: 0 }, { timeoutMs: 30001 },
    { now: () => NaN }, { now: () => -1 }, { nonce: () => 'short' },
    { nonce: () => { throw new Error(secret); } },
  ];
  for (const override of overrides) {
    assert.deepEqual(await deliverHarkBilling({ ...options(async () => { throw new Error('must not fetch'); }), ...override }),
      { state: 'pending', reason: 'configuration_unavailable' });
  }
  assert.deepEqual(await deliverHarkBilling(options(async () => { throw new Error(secret); })),
    { state: 'pending', reason: 'request_failed' });
});

test('callback key trimming matches API and body is detached before asynchronous transport', async () => {
  const body = { ...statusBody };
  const input = options(async (_url, init) => {
    body.state = 'failed';
    const sent = JSON.parse(String(init?.body));
    assert.equal(sent.state, 'completed');
    const headers = new Headers(init?.headers);
    const hash = createHash('sha256').update(canonicalJson(sent)).digest('hex');
    const signature = createHmac('sha256', secret)
      .update(`v2\n/internal/voice/outbound-status\n${Math.floor(now / 1000)}\nabcdefghijklmnop\n${hash}`).digest('hex');
    assert.equal(headers.get('x-dexter-callback-signature'), `v2=${signature}`);
    return new Response(null, { status: 204 });
  }, { kind: 'outbound-status', body });
  input.signingSecret = ` ${secret}\n`;
  assert.equal((await deliverHarkBilling(input)).state, 'delivered');
});
