import assert from 'node:assert/strict';
import test from 'node:test';
import { createHmac } from 'node:crypto';
import { createVerificationCallbackClient } from '../src/broker/verificationCallbackClient.js';
import { createPrimaryApiTransport, createPrimaryApiClient, PrimaryApiError } from '../src/broker/primaryApi.js';
import { phoneVerificationCallbackCommandRef, phoneVerificationCallbackAccessDigest,
  type PhoneVerificationCallbackAnswer, type PhoneVerificationCallbackCompletion } from '../src/broker/verificationCallbackContract.js';
import { createVerificationCallbackKickoff, VERIFICATION_CALLBACK_KICKOFF_PATH } from '../src/broker/verificationCallbackKickoff.js';

const id = (n: number) => `00000000-0000-4000-8000-${n.toString().padStart(12, '0')}`;
const ref = { requestId: id(1), requestDigest: 'a'.repeat(64) };
const sid = 'CA' + '1'.repeat(32), secret = 'synthetic-callback-service-secret-at-least-32';
const completion: PhoneVerificationCallbackCompletion = { v: 1, request: ref, brokerCallId: id(1), callSid: sid,
  purpose: 'account_access', accepted: true, turn: { id: id(2), digest: 'b'.repeat(64) } };
const answer: PhoneVerificationCallbackAnswer = { v: 1, request: ref, brokerCallId: id(1), dispatchAttemptId: id(3),
  providerProof: { url: `https://broker.example.test/broker/verification-callback/${id(1)}`, signature: 'synthetic',
    params: { AccountSid: 'AC' + '2'.repeat(32), CallSid: sid, Direction: 'outbound-api',
      From: '+15555550001', To: '+15555550002', CallStatus: 'in-progress' } } };
const answered = { v: 1, request: ref, state: 'answered', dispatchAttemptId: id(3), callSid: sid,
  purpose: 'account_access', expiresAt: '2026-09-25T06:05:00.000Z' };
const json = (value: unknown, status = 200, type = 'application/json; charset=utf-8') =>
  new Response(JSON.stringify(value), { status, headers: { 'content-type': type } });
function harness(responses: Array<Response | Error>) {
  const calls: Array<{ url: string; init: RequestInit }> = [];
  const fetcher: typeof fetch = async (url, init) => { calls.push({ url: String(url), init: init ?? {} });
    const reply = responses.shift(); if (!reply) throw new Error('Unexpected repeat'); if (reply instanceof Error) throw reply; return reply; };
  const options = { apiBaseUrl: 'http://127.0.0.1:3032', phoneServiceSecret: secret, fetch: fetcher };
  return { calls, options, client: createVerificationCallbackClient(options) };
}

test('exact service completion and six-key GET preserve safe command, derived auth and one-attempt serialization', async () => {
  const command = phoneVerificationCallbackCommandRef(completion);
  assert.equal(command.commandDigest, '16bdba745d5f337c73fe62ec8a8eb9a9d1cbd8c9a4be1b6c26edad5dff2120bf');
  const result = { v: 1, command, state: 'admitted', admission: { id: id(9), digest: 'c'.repeat(64), expiresAt: '2026-09-25T06:05:00.000Z' } };
  const h = harness([json(result), json({ v: 1, command, state: 'absent' })]);
  assert.deepEqual(await h.client.complete(completion), result);
  assert.equal(h.calls.length, 1); assert.equal(h.calls[0].init.method, 'POST');
  assert.equal((await h.client.recoverCompletion(command, 'account_access')).state, 'absent');
  assert.equal(h.calls.length, 2); assert.equal(h.calls[1].init.method, 'GET');
  assert.equal(h.calls[0].url, `http://127.0.0.1:3032/internal/primary-dexter/channels/phone/verification-callbacks/${id(1)}/complete`);
  assert.equal(h.calls[0].init.body, JSON.stringify({ v: 1, request: ref, brokerCallId: id(1), callSid: sid,
    turn: { id: id(2), digest: 'b'.repeat(64) }, accepted: true, purpose: 'account_access' }));
  assert.equal(h.calls[1].init.body, undefined);
  assert.equal(new URL(h.calls[1].url).search.slice(1), `requestDigest=${ref.requestDigest}&brokerCallId=${id(1)}&callSid=${sid}&turnId=${id(2)}&turnDigest=${'b'.repeat(64)}&commandDigest=${command.commandDigest}`);
  for (const call of h.calls) {
    assert.equal(new Headers(call.init.headers).get('Authorization'), 'Bearer ' + createHmac('sha256', secret).update('dexter-broker/primary-ingress/v1').digest('hex'));
    assert.equal(call.init.redirect, 'error');
  }
});

test('generic outbound answer exact historical states return without an implicit grant', async () => {
  const h = harness([json(answered), json({ v: 1, request: ref, state: 'cancelled', cancelledAt: '2026-09-25T06:01:00.000Z' })]);
  assert.deepEqual(await h.client.answer(answer, 'account_access'), answered);
  assert.equal((await h.client.answer(answer, 'account_access')).state, 'cancelled');
  assert.deepEqual(JSON.parse(String(h.calls[0].init.body)), answer);
});

test('answer mismatched SID, attempt or purpose and unexpected pending state refuse', async () => {
  for (const mutation of [{ callSid: 'CA' + '9'.repeat(32) }, { dispatchAttemptId: id(99) }, { purpose: 'enrollment' }, { state: 'reserved' }]) {
    const h = harness([json({ ...answered, ...mutation })]);
    await assert.rejects(h.client.answer(answer, 'account_access'), PrimaryApiError);
    assert.equal(h.calls.length, 1);
  }
});

test('finite nested errors preserve status/code/retryability, without retry on loss, HTML or redirect', async () => {
  for (const [status, code, retryable] of [[403, 'callback_owner_required', false], [409, 'phone_tab_required', false],
    [503, 'callback_unavailable', true]] as const) {
    const h = harness([json({ error: { code, retryable } }, status)]);
    await assert.rejects(h.client.recover(ref), error => error instanceof PrimaryApiError
      && error.status === status && error.remoteCode === code && error.retryable === retryable);
    assert.equal(h.calls.length, 1);
  }
  for (const response of [new Error('secret provider failure'), json(answered, 200, 'text/html'), json({}, 302),
    json({ error: 'callback_owner_required', retryable: false }, 403), json({ error: { code: 'callback_owner_required', retryable: true } }, 403)]) {
    const h = harness([response]); await assert.rejects(h.client.recover(ref), PrimaryApiError); assert.equal(h.calls.length, 1);
  }
});

test('access request preserves original signed fields, and GET/cancel have exact ref-only bodies', async () => {
  const input = { v: 1 as const, requestId: id(1), original: { brokerCallId: id(7), providerProof: {
    ...answer.providerProof, url: 'https://broker.example.test/broker/incoming-call', params: {
      ...answer.providerProof.params, Direction: 'inbound', StirVerstat: 'TN-Validation-Passed-C' } } }, explicitlyRequested: true as const };
  const request = { requestId: input.requestId, requestDigest: phoneVerificationCallbackAccessDigest(input) };
  const h = harness([json({ v: 1, request, state: 'absent' }), json({ v: 1, request, state: 'absent' }),
    json({ v: 1, request, state: 'cancelled', cancelledAt: '2026-09-25T06:01:00.000Z' })]);
  await h.client.prepareAccess(input); await h.client.recover(request); await h.client.cancel(request);
  assert.equal(h.calls[0].init.body, JSON.stringify(input));
  assert.equal(new URL(h.calls[1].url).search, `?requestDigest=${request.requestDigest}`);
  assert.equal(h.calls[1].init.body, undefined); assert.equal(h.calls[2].init.body, JSON.stringify({ v: 1, request }));
});

function sizedAnswer(target: number): PhoneVerificationCallbackAnswer {
  const value = structuredClone(answer);
  for (let n = 0; Buffer.byteLength(JSON.stringify(value)) < target && n < 70; n++) {
    const key = `padding${n}`; value.providerProof.params[key] = '';
    const remaining = target - Buffer.byteLength(JSON.stringify(value));
    assert.ok(remaining >= 0);
    const escapes = Math.min(2048, Math.floor(remaining / 6));
    const ascii = Math.min(2048 - escapes, remaining - escapes * 6);
    value.providerProof.params[key] = '\u0001'.repeat(escapes) + 'x'.repeat(ascii);
  }
  assert.equal(Buffer.byteLength(JSON.stringify(value)), target); return value;
}
test('only callback proof routes accept above24KiB through exact262144; overbound refuses before contact', async () => {
  for (const bytes of [24 * 1024 + 1, 262144]) {
    const h = harness([json(answered)]); await h.client.answer(sizedAnswer(bytes), 'account_access');
    assert.equal(Buffer.byteLength(String(h.calls[0].init.body)), bytes);
  }
  const h = harness([]);
  await assert.rejects(h.client.answer(sizedAnswer(262145), 'account_access'), error => error instanceof PrimaryApiError && error.code === 'invalid_request');
  assert.equal(h.calls.length, 0);
  const request = createPrimaryApiTransport(h.options);
  await assert.rejects(request('POST', '/calls', { padding: 'x'.repeat(24 * 1024) }), PrimaryApiError);
  await assert.rejects(request('POST', `/verification-callbacks/${id(1)}/complete`, { padding: 'x'.repeat(4096) }), PrimaryApiError);
  await assert.rejects(request('GET', `/verification-callbacks/${id(1)}?requestDigest=${ref.requestDigest}`, {}), PrimaryApiError);
  assert.equal(h.calls.length, 0);
});

test('strict body rejects raw extra owner/destination fields and changed command echo', async () => {
  const h = harness([]);
  await assert.rejects(h.client.complete({ ...completion, to: '+15555550999' } as PhoneVerificationCallbackCompletion), PrimaryApiError);
  assert.equal(h.calls.length, 0);
  const command = phoneVerificationCallbackCommandRef(completion);
  const bad = harness([json({ v: 1, command: { ...command, turn: { ...command.turn, digest: 'f'.repeat(64) } }, state: 'absent' })]);
  await assert.rejects(bad.client.recoverCompletion(command, 'account_access'), error => error instanceof PrimaryApiError && error.code === 'identity_mismatch');
});

test('kickoff enforces socket locality/control before parse and supplies only strict saved reference', async () => {
  const calls: unknown[] = [], token = 'synthetic-broker-control-token-long-enough';
  const handler = createVerificationCallbackKickoff(token, { async kickoff(input) { calls.push(input); return { v: 1, request: input, state: 'dispatching' }; } });
  assert.equal(VERIFICATION_CALLBACK_KICKOFF_PATH, '/internal/broker/verification-callbacks/dispatch');
  const input = { remoteAddress: '127.0.0.1', authorization: `Bearer ${token}`, contentType: 'application/json', body: Buffer.from(JSON.stringify(ref)) };
  assert.equal((await handler({ ...input, remoteAddress: '203.0.113.1' })).status, 403);
  assert.equal((await handler({ ...input, authorization: 'Bearer wrong', body: Buffer.from('not-json') })).status, 403);
  assert.equal((await handler({ ...input, body: Buffer.from(JSON.stringify({ ...ref, to: '+15555550999' })) })).status, 400);
  assert.equal((await handler(input)).status, 200); assert.deepEqual(calls, [ref]);
});

test('private callback registration has an explicit proof-only cap; legacy /calls remains24KiB', async () => {
  const h = harness([json({ marker: 'transport-only' })]), request = createPrimaryApiTransport(h.options);
  const input = { providerProof: sizedAnswer(40000).providerProof };
  await assert.rejects(request('POST', '/calls', input), PrimaryApiError);
  assert.equal(h.calls.length, 0);
  await request('POST', '/calls', input, undefined, 200, false, { callbackPrivateCall: true });
  assert.deepEqual(JSON.parse(String(h.calls[0].init.body)), input);
  for (const body of [{ ...input, callAssignment: {} }, { ...input, inboundApprovalRef: {} },
    { providerProof: { ...input.providerProof, url: 'https://broker.example.test/broker/answer/' + id(1) } },
    { providerProof: { ...input.providerProof, params: { ...input.providerProof.params, Digits: '01234567' } } }]) {
    await assert.rejects(request('POST', '/calls', body, undefined, 200, false, { callbackPrivateCall: true }), PrimaryApiError);
  }
  assert.equal(h.calls.length, 1);
});


test('actual Primary client callback registration preserves exact proof through262144; ordinary and overlimit never contact', async () => {
  const overhead = Buffer.byteLength(JSON.stringify(answer)) - Buffer.byteLength(JSON.stringify({ providerProof: answer.providerProof }));
  const call = { bindingId: id(90), callSid: sid, state: 'open', context: { primaryDexterId: id(91), threadId: id(92), displayName: 'Dexter', messages: [] } };
  for (const bytes of [24577, 262144]) {
    const h = harness([json(call)]), client = createPrimaryApiClient(h.options);
    const proof = sizedAnswer(bytes + overhead).providerProof;
    await assert.rejects(client.openCall(proof), PrimaryApiError); assert.equal(h.calls.length, 0);
    assert.equal((await client.openVerifiedCallbackCall!(proof)).bindingId, call.bindingId);
    assert.equal(Buffer.byteLength(String(h.calls[0].init.body)), bytes);
    assert.deepEqual(JSON.parse(String(h.calls[0].init.body)), { providerProof: proof });
  }
  const h = harness([]), client = createPrimaryApiClient(h.options);
  await assert.rejects(client.openVerifiedCallbackCall!(sizedAnswer(262145 + overhead).providerProof), PrimaryApiError);
  assert.equal(h.calls.length, 0);
});
