import assert from 'node:assert/strict';
import test from 'node:test';
import { createPhoneApiCallbackSignature } from '../src/outbound/PhoneApiCallback.js';
import { callbackStatusReceipt, callbackEndedReceipt, validateCallbackReceipts, deliverCallbackReceipt, verificationCallbackProviderTiming } from '../src/broker/verificationCallbackReceipts.js';
import { phoneVerificationCallbackPermitSchema } from '../src/broker/verificationCallbackContract.js';
const id = '00000000-0000-4000-8000-000000000001', sid = 'CA' + '1'.repeat(32);
const permit = phoneVerificationCallbackPermitSchema.parse({ v: 1, request: { requestId: id, requestDigest: 'a'.repeat(64) }, purpose: 'enrollment',
  providerAccountSid: 'AC' + '2'.repeat(32), from: '+15555550001', to: '+15555550002',
  answerUrl: `https://broker.test/broker/verification-callback/${id}`, responseUrl: `https://broker.test/broker/verification-callback-response/${id}`,
  createdAt: '2026-09-25T06:00:00.000Z', expiresAt: '2026-09-25T06:05:00.000Z', maxDurationSeconds: 180,
  billing: { intentId: id, eventId: `phone-verification:${id}`, mandateId: id, unitPriceMicro: 1000, freeSeconds: 0 } });
const timing = { startedAt: '2026-09-25T06:01:00.000Z', endedAt: '2026-09-25T06:01:20.000Z', durationSeconds: 19 };
const signingSecret = 'callback-signing-secret-at-least-32-characters';
test('exact observed timing and permit project receipt without owner, price, transcript or number', () => {
  const receipt = callbackEndedReceipt(permit, sid, timing);
  assert.deepEqual(receipt, { kind: 'ended', body: { source: 'phone', direction: 'outbound-verification', purpose: 'phone_verification_v1',
    alert_intent_id: id, alert_event_id: permit.billing.eventId, external_sid: sid,
    started_at: timing.startedAt, ended_at: timing.endedAt, duration_seconds: 19 } });
  assert.throws(() => callbackEndedReceipt(permit, sid, { ...timing, durationSeconds: 21 }));
  validateCallbackReceipts(permit, sid, [callbackStatusReceipt(permit, sid, 'in-progress'), receipt]);
});
test('saved receipt body, order, identity and acknowledgment cannot be removed or rewritten', () => {
  const value = callbackEndedReceipt(permit, sid, timing);
  assert.throws(() => validateCallbackReceipts(permit, sid, [], [value]));
  assert.throws(() => validateCallbackReceipts(permit, sid, [callbackEndedReceipt(permit, sid, { ...timing, durationSeconds: 18 })], [value]));
  assert.throws(() => validateCallbackReceipts(permit, 'CA' + '9'.repeat(32), [value]));
  assert.throws(() => validateCallbackReceipts(permit, sid, [value, value]));
});
test('lost receipt response retries exact durable body with fresh nonce and timestamp', async () => {
  const value = callbackEndedReceipt(permit, sid, timing), requests: RequestInit[] = [];
  let clock = Date.parse(timing.endedAt), nonce = 1;
  const fetchImpl: typeof fetch = async (_url, init) => { requests.push(init!);
    if (requests.length === 1) throw new Error('lost response');
    return new Response(JSON.stringify({ ok: true, id, status: 'completed' }), { headers: { 'content-type': 'application/json' } }); };
  const options = { apiBaseUrl: 'http://127.0.0.1:3032', signingSecret, value, fetchImpl, now: () => clock,
    nonce: () => String(nonce++).repeat(18) };
  assert.equal(await deliverCallbackReceipt(options), undefined); clock += 1000;
  const accepted = await deliverCallbackReceipt(options); assert.ok(accepted?.acknowledgedAt);
  assert.equal(requests[0].body, requests[1].body);
  for (const init of requests) {
    const headers = new Headers(init.headers);
    assert.equal(headers.get('x-dexter-callback-signature'), createPhoneApiCallbackSignature(signingSecret,
      '/internal/voice/call-ended', headers.get('x-dexter-alert-timestamp')!, headers.get('x-dexter-alert-nonce')!, value.body));
    assert.equal(init.redirect, 'error');
  }
  assert.notEqual(new Headers(requests[0].headers).get('x-dexter-alert-nonce'), new Headers(requests[1].headers).get('x-dexter-alert-nonce'));
});
test('status uses existing signed204 sink and maps actual in-progress to answered', async () => {
  const value = callbackStatusReceipt(permit, sid, 'in-progress');
  const accepted = await deliverCallbackReceipt({ apiBaseUrl: 'http://127.0.0.1:3032', signingSecret, value,
    fetchImpl: async (url, init) => { assert.equal(String(url), 'http://127.0.0.1:3032/internal/voice/outbound-status');
      assert.equal(JSON.parse(String(init?.body)).state, 'answered'); return new Response(null, { status: 204 }); } });
  assert.ok(accepted?.acknowledgedAt);
});
test('HTML, oversized receipt, wrong response, or remote sink does not acknowledge billing', async () => {
  const value = callbackEndedReceipt(permit, sid, timing);
  for (const response of [new Response('{}', { headers: { 'content-type': 'text/html' } }),
    new Response('x'.repeat(16385), { headers: { 'content-type': 'application/json' } }),
    new Response('{}', { status: 503 })]) {
    assert.equal(await deliverCallbackReceipt({ apiBaseUrl: 'http://127.0.0.1:3032', signingSecret, value, fetchImpl: async () => response }), undefined);
  }
  await assert.rejects(deliverCallbackReceipt({ apiBaseUrl: 'https://remote.test', signingSecret, value }));
});


test('never-answered zero duration needs actual provider times and never fabricates an answer', () => {
  const value = { status: 'no-answer', startTime: new Date(timing.startedAt), endTime: new Date(timing.endedAt), duration: '0' };
  assert.deepEqual(verificationCallbackProviderTiming(value), { ...timing, durationSeconds: 0 });
  assert.equal(verificationCallbackProviderTiming({ ...value, startTime: null }), undefined);
  assert.equal(verificationCallbackProviderTiming({ ...value, duration: undefined }), undefined);
  assert.equal(verificationCallbackProviderTiming({ ...value, status: 'ringing' }), undefined);
});
