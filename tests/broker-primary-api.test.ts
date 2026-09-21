import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import test from 'node:test';
import {
  createPrimaryApiClient, PrimaryApiError,
  type PrimaryApiOptions, type PrimaryProviderProof, type PrimarySubmission,
} from '../src/broker/primaryApi.js';

const bindingId = '11111111-1111-4111-8111-111111111111';
const requestId = '22222222-2222-4222-8222-222222222222';
const sourceEventId = '33333333-3333-4333-8333-333333333333';
const primaryDexterId = '44444444-4444-4444-8444-444444444444';
const threadId = '55555555-5555-4555-8555-555555555555';
const messageId = '66666666-6666-4666-8666-666666666666';
const eventId = '77777777-7777-4777-8777-777777777777';
const callSid = 'CA' + '1'.repeat(32);
const secret = 'synthetic-phone-service-secret-32-characters';
const when = '2026-09-21T21:00:00.000Z';
const context = { primaryDexterId, threadId, displayName: 'Dexter', messages: [{ id: messageId, role: 'user', text: 'My previous request.' }] };
const call = { bindingId, callSid, state: 'open', context };
const assistant = { messageId, eventId, sequence: '9007199254740993', text: 'Here is the saved result.' };
const completed = { bindingId, requestId, state: 'completed', assistant };
const proof: PrimaryProviderProof = {
  url: 'https://broker.example.test/incoming-call', signature: 'synthetic-twilio-signature',
  params: { AccountSid: 'AC' + '2'.repeat(32), CallSid: callSid, Direction: 'inbound', From: '+15555550123', To: '+15555550124' },
};
const submission: PrimarySubmission = { source: {
  callSid, itemId: 'item_caller_1', sourceEventId, revision: 1, receivedAt: when, role: 'user',
}, text: 'Tell me my current portfolio.' };

const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });
const code = (expected: string) => (error: unknown) => error instanceof PrimaryApiError && error.code === expected;
function harness(replies: Response[], options: Partial<PrimaryApiOptions> = {}) {
  const requests: { url: string; init: RequestInit }[] = [];
  const fetcher: typeof fetch = async (url, init) => {
    requests.push({ url: String(url), init: init ?? {} });
    const response = replies.shift();
    assert.ok(response, 'unexpected extra request');
    return response;
  };
  return { requests, client: createPrimaryApiClient({ apiBaseUrl: 'https://api.example.test', phoneServiceSecret: secret, fetch: fetcher, ...options }) };
}

test('readiness, signed call binding and context use exact HMAC authentication and binding headers', async () => {
  const { client, requests } = harness([json({ eligible: true, reason: 'ready' }), json(call), json(call)]);
  assert.deepEqual(await client.readiness(proof.params.From), { eligible: true, reason: 'ready' });
  assert.deepEqual(await client.openCall(proof), call);
  assert.deepEqual(await client.getContext(bindingId), call);
  const bearer = createHmac('sha256', secret).update('dexter-broker/primary-ingress/v1').digest('hex');
  for (const request of requests) {
    assert.equal(new Headers(request.init.headers).get('Authorization'), `Bearer ${bearer}`);
    assert.equal(request.init.redirect, 'error');
    assert.ok(request.init.signal instanceof AbortSignal);
  }
  assert.equal(requests[1].url, 'https://api.example.test/internal/primary-dexter/channels/phone/calls');
  assert.deepEqual(JSON.parse(String(requests[1].init.body)), { providerProof: proof });
  assert.equal(new Headers(requests[2].init.headers).get('X-Dexter-Binding-Id'), bindingId);
  assert.equal(requests[2].init.method, 'GET');
  assert.equal(requests[2].init.body, undefined);
});

test('submit atomically sends one canonical source with top-level role; result retains string sequence', async () => {
  const { client, requests } = harness([json({ bindingId, requestId, state: 'queued', replayed: false }, 202), json(completed)]);
  assert.equal((await client.submit(bindingId, requestId, submission)).state, 'queued');
  const submitted = JSON.parse(String(requests[0].init.body));
  assert.equal(submitted.role, 'user');
  assert.equal(submitted.source.role, undefined);
  assert.equal(submitted.bindingId, bindingId);
  assert.equal(submitted.source.sourceEventId, sourceEventId);
  assert.equal(new Headers(requests[0].init.headers).get('Idempotency-Key'), requestId);
  assert.equal(new Headers(requests[0].init.headers).get('X-Dexter-Binding-Id'), bindingId);
  assert.deepEqual(await client.result(bindingId, requestId), completed);
  assert.equal(requests.length, 2);
  assert.equal(requests[1].init.method, 'GET');
  assert.equal(new Headers(requests[1].init.headers).get('X-Dexter-Binding-Id'), bindingId);
});

test('request identities, completed payloads and waiting states are never inferred', async () => {
  const cases: [unknown, string][] = [
    [{ ...completed, requestId: sourceEventId }, 'identity_mismatch'],
    [{ ...completed, bindingId: sourceEventId }, 'identity_mismatch'],
    [{ bindingId, requestId, state: 'completed' }, 'invalid_response'],
    [{ ...completed, assistant: { ...assistant, text: '' } }, 'invalid_response'],
    [{ ...completed, assistant: { ...assistant, sequence: 1 } }, 'invalid_response'],
    [{ ...completed, state: 'running' }, 'invalid_response'],
    [{ ...completed, state: 'waiting' }, 'invalid_response'],
    [{ bindingId, requestId, state: 'failed' }, 'invalid_response'],
  ];
  for (const [body, expected] of cases) {
    await assert.rejects(harness([json(body)]).client.result(bindingId, requestId), code(expected));
  }
  const failed = { bindingId, requestId, state: 'failed', error: { code: 'authority_unavailable', retryable: false } };
  assert.deepEqual(await harness([json(failed)]).client.result(bindingId, requestId), failed);
});

test('closed binding replays remain closed and existing account identity cannot change', async () => {
  const { client } = harness([json({ ...call, state: 'closed' }), json({ ...call, context: { ...context, primaryDexterId: sourceEventId } })]);
  assert.equal((await client.openCall(proof)).state, 'closed');
  await assert.rejects(client.getContext(bindingId), code('identity_mismatch'));
  await assert.rejects(harness([json({ ...call, callSid: 'CA' + '3'.repeat(32) })]).client.openCall(proof), code('identity_mismatch'));
  await assert.rejects(harness([json({ ...call, bindingId: sourceEventId })]).client.getContext(bindingId), code('identity_mismatch'));
  const remembered = harness([json(call), json({ ...call, context: { ...context, primaryDexterId: sourceEventId } })]);
  const returned = await remembered.client.openCall(proof);
  returned.context.primaryDexterId = sourceEventId;
  await assert.rejects(remembered.client.getContext(bindingId), code('identity_mismatch'));
  const reopened = harness([json({ ...call, state: 'closed' }), json(call)]);
  await reopened.client.openCall(proof);
  await assert.rejects(reopened.client.getContext(bindingId), code('identity_mismatch'));
});

test('turn receipts and close validate their original binding and source revision', async () => {
  const turn = { sourceEventId, revision: 2, itemId: 'item_caller_1', role: 'user' as const, text: 'Corrected transcript.', occurredAt: when };
  const receipt = { bindingId, sourceEventId, revision: 2, recordId: sourceEventId, replayed: true };
  const closed = { bindingId, callSid, state: 'closed' };
  const { client, requests } = harness([json(receipt), json(closed)]);
  assert.deepEqual(await client.captureTurn(bindingId, turn), receipt);
  assert.deepEqual(await client.close(bindingId), closed);
  assert.deepEqual(JSON.parse(String(requests[1].init.body)), {});
  assert.equal(new Headers(requests[1].init.headers).get('X-Dexter-Binding-Id'), bindingId);
  await assert.rejects(harness([json({ ...receipt, revision: 1 })]).client.captureTurn(bindingId, turn), code('identity_mismatch'));
  await assert.rejects(harness([json({ ...receipt, recordId: messageId })]).client.captureTurn(bindingId, turn), code('identity_mismatch'));
  await assert.rejects(harness([json({ ...closed, bindingId: sourceEventId })]).client.close(bindingId), code('identity_mismatch'));
});

test('HTTP refusal preserves typed code without source text, while a failed POST is never retried', async () => {
  const { client } = harness([json({ error: { code: 'request_not_found', retryable: false }, leaked: secret }, 404)]);
  await assert.rejects(client.result(bindingId, requestId), (error: unknown) => {
    assert.ok(error instanceof PrimaryApiError);
    assert.equal(error.status, 404); assert.equal(error.remoteCode, 'request_not_found'); assert.equal(error.retryable, false);
    assert.doesNotMatch(JSON.stringify(error), new RegExp(secret));
    return true;
  });
  let attempts = 0;
  const broken = createPrimaryApiClient({ apiBaseUrl: 'https://api.example.test', phoneServiceSecret: secret,
    fetch: async () => { attempts++; throw new Error(secret); },
  });
  await assert.rejects(broken.submit(bindingId, requestId, submission), code('request_failed'));
  assert.equal(attempts, 1);
});

test('redirect responses and unexpected successful status codes cannot qualify a request', async () => {
  const { client, requests } = harness([new Response(null, { status: 302, headers: { location: 'https://foreign.example.test' } })]);
  await assert.rejects(client.getContext(bindingId), code('http_error'));
  assert.equal(requests.length, 1);
  await assert.rejects(harness([json(completed, 201)]).client.result(bindingId, requestId), code('invalid_response'));
  await assert.rejects(harness([new Response(null, { status: 204 })]).client.close(bindingId), code('invalid_response'));
});

test('advertised and streamed response bodies are bounded before JSON parsing', async () => {
  const advertised = new Response('{}', { headers: { 'content-length': '100' } });
  await assert.rejects(harness([advertised], { maxBodyBytes: 32 }).client.result(bindingId, requestId), code('response_too_large'));
  let cancelled = false;
  const stream = new ReadableStream<Uint8Array>({
    start(controller) { controller.enqueue(new Uint8Array(40)); },
    cancel() { cancelled = true; },
  });
  await assert.rejects(harness([new Response(stream)], { maxBodyBytes: 32 }).client.result(bindingId, requestId), code('response_too_large'));
  assert.equal(cancelled, true);
});

test('deadline bounds both a hung fetch and a hung body without extra requests', async () => {
  let requests = 0;
  const client = createPrimaryApiClient({ apiBaseUrl: 'https://api.example.test', phoneServiceSecret: secret, timeoutMs: 15,
    fetch: async () => { requests++; return new Promise<Response>(() => {}); },
  });
  await assert.rejects(client.submit(bindingId, requestId, submission), code('request_timeout'));
  assert.equal(requests, 1);
  let cancelled = false;
  const stream = new ReadableStream<Uint8Array>({ cancel() { cancelled = true; } });
  await assert.rejects(harness([new Response(stream)], { timeoutMs: 15 }).client.result(bindingId, requestId), code('request_timeout'));
  assert.equal(cancelled, true);
});

test('malformed, empty and invalid UTF-8 success bodies fail closed', async () => {
  for (const response of [new Response(''), new Response('{'), new Response(new Uint8Array([0xff]))]) {
    await assert.rejects(harness([response]).client.result(bindingId, requestId), code('invalid_response'));
  }
  await assert.rejects(harness([json({ eligible: true, reason: 'authority_unavailable' })]).client.readiness(proof.params.From), code('invalid_response'));
});

test('invalid request identities and oversized source bodies never reach the network', async () => {
  const { client, requests } = harness([]);
  await assert.rejects(client.result('../other', requestId), code('invalid_request'));
  await assert.rejects(client.submit(bindingId, requestId, { ...submission, text: 'x'.repeat(24 * 1024) }), code('invalid_request'));
  await assert.rejects(client.openCall({ ...proof, url: 'https://name:password@broker.example.test/incoming-call' }), code('invalid_request'));
  assert.equal(requests.length, 0);
  const known = harness([json(call)]);
  await known.client.openCall(proof);
  await assert.rejects(known.client.submit(bindingId, requestId, { ...submission, source: { ...submission.source, callSid: 'CA' + '4'.repeat(32) } }), code('identity_mismatch'));
  assert.equal(known.requests.length, 1);
});

test('remote plaintext and credential-bearing base URLs are rejected at configuration', () => {
  for (const apiBaseUrl of ['http://api.example.test', 'https://secret@api.example.test', 'https://api.example.test?token=secret']) {
    assert.throws(() => createPrimaryApiClient({ apiBaseUrl, phoneServiceSecret: secret }), code('invalid_configuration'));
  }
});

test('completed source validation counts Unicode scalars and rejects provisional blank or broken text', async () => {
  const { client, requests } = harness([json({ bindingId, requestId, state: 'queued', replayed: false }, 202)]);
  await client.submit(bindingId, requestId, { ...submission, text: '🙂'.repeat(4000) });
  for (const text of [' ', 'a\0b', '\ud800', '\udfff', '🙂'.repeat(4001)]) {
    await assert.rejects(client.submit(bindingId, requestId, { ...submission, text }), code('invalid_request'));
  }
  for (const itemId of ['a.b', 'a:b', 'x'.repeat(129)]) {
    await assert.rejects(client.submit(bindingId, requestId, { ...submission, source: { ...submission.source, itemId } }), code('invalid_request'));
  }
  assert.equal(requests.length, 1);
});
