import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import test from 'node:test';
import {
  createPrimaryApiClient, PrimaryApiError,
  type PrimaryApiOptions, type PrimaryProviderProof, type PrimarySubmission,
} from '../src/broker/primaryApi.js';
import { originalTurn, rawSubmissionDigest, turnBodyDigest } from '../src/broker/reconciliation.js';
import { HARK_PHONE_SNAPSHOT_MAX_BYTES, harkSourceDigest } from '../src/broker/harkAssignment.js';
import { makeResolvedHarkAssignment } from './fixtures/broker-hark.js';

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

test('publication reference is paired, preserved verbatim in submit and reconciliation, and digest-bound', async () => {
  const input: PrimarySubmission = { ...submission, text: ' Then review it.\t',
    relatedPrimaryRequestId: messageId, referenceOrigin: 'broker_status_publication' };
  const turn = originalTurn(input);
  const proof = { schemaVersion: 1, kind: 'phone_closed_call_non_admission', callSid,
    source: { itemId: input.source.itemId, sourceEventId, revision: 1, receivedAt: when },
    rawSubmissionDigest: rawSubmissionDigest(bindingId, input),
    turn: { recordId: sourceEventId, revisionRecordId: eventId, bodyDigest: turnBodyDigest(turn) }, closedAt: when };
  const { client, requests } = harness([json({ ...completed, replayed: false }, 202),
    json({ outcome: 'not_admitted', bindingId, requestId, proof })]);
  await client.submit(bindingId, requestId, input);
  await client.reconcile(bindingId, requestId, input);
  for (const request of requests) {
    const body = JSON.parse(String(request.init.body));
    assert.equal(body.relatedPrimaryRequestId, messageId);
    assert.equal(body.referenceOrigin, 'broker_status_publication');
    assert.equal(body.text, input.text);
  }
  assert.notEqual(rawSubmissionDigest(bindingId, input), rawSubmissionDigest(bindingId, { ...submission, text: input.text }));
  assert.notEqual(rawSubmissionDigest(bindingId, input), rawSubmissionDigest(bindingId, { ...input, relatedPrimaryRequestId: eventId }));
  assert.equal(turnBodyDigest(turn), turnBodyDigest(originalTurn({ ...submission, text: input.text })), 'reference is not transcript text');
  const oldProof = { ...proof, rawSubmissionDigest: rawSubmissionDigest(bindingId, { ...submission, text: input.text }) };
  await assert.rejects(harness([json({ outcome: 'not_admitted', bindingId, requestId, proof: oldProof })])
    .client.reconcile(bindingId, requestId, input), code('identity_mismatch'));
});

test('invalid, incomplete, and self-referencing publication fields never reach HTTP', async () => {
  for (const fields of [
    { relatedPrimaryRequestId: messageId }, { referenceOrigin: 'broker_status_publication' },
    { relatedPrimaryRequestId: messageId, referenceOrigin: 'caller_consent' },
    { relatedPrimaryRequestId: 'not-a-uuid', referenceOrigin: 'broker_status_publication' },
    { relatedPrimaryRequestId: requestId, referenceOrigin: 'broker_status_publication' },
  ]) {
    const { client, requests } = harness([]);
    const input = { ...submission, ...fields } as PrimarySubmission;
    await assert.rejects(client.submit(bindingId, requestId, input), code('invalid_request'));
    await assert.rejects(client.reconcile(bindingId, requestId, input), code('invalid_request'));
    assert.equal(requests.length, 0);
  }
});

test('reconciliation sends exact original body without Idempotency-Key and validates stable capture/proof digests', async () => {
  const raw = { ...submission, text: ' Original speech\t' };
  const turn = originalTurn(raw);
  const receipt = { bindingId, sourceEventId, revision: 1, recordId: sourceEventId, replayed: true,
    revisionRecordId: messageId, bodyDigest: turnBodyDigest(turn) };
  const proof = { schemaVersion: 1, kind: 'phone_closed_call_non_admission', callSid,
    source: { itemId: raw.source.itemId, sourceEventId, revision: 1, receivedAt: when },
    rawSubmissionDigest: rawSubmissionDigest(bindingId, raw),
    turn: { recordId: sourceEventId, revisionRecordId: messageId, bodyDigest: receipt.bodyDigest }, closedAt: when };
  const response = { outcome: 'not_admitted', bindingId, requestId, proof };
  const { client, requests } = harness([json(receipt), json(response)]);
  assert.deepEqual(await client.captureTurn(bindingId, turn), receipt);
  assert.deepEqual(await client.reconcile(bindingId, requestId, raw), response);
  assert.equal(requests[1].url, `https://api.example.test/internal/primary-dexter/channels/phone/requests/${requestId}/reconcile`);
  assert.equal(new Headers(requests[1].init.headers).get('Idempotency-Key'), null);
  assert.deepEqual(JSON.parse(String(requests[1].init.body)), { bindingId, role: 'user', text: raw.text,
    source: { callSid, itemId: raw.source.itemId, sourceEventId, revision: 1, receivedAt: when } });
  await assert.rejects(harness([json({ ...response, proof: { ...proof, rawSubmissionDigest: 'a'.repeat(64) } })])
    .client.reconcile(bindingId, requestId, raw), code('identity_mismatch'));
  await assert.rejects(harness([json({ ...receipt, bodyDigest: 'a'.repeat(64) })]).client.captureTurn(bindingId, turn), code('identity_mismatch'));
});

test('admitted reconciliation retains original envelope and validates the canonical result identity', async () => {
  const response = { outcome: 'admitted', bindingId, requestId, canonicalRequestId: messageId,
    result: { ...completed, requestId: messageId } };
  assert.deepEqual(await harness([json(response)]).client.reconcile(bindingId, requestId, submission), response);
  await assert.rejects(harness([json({ ...response, result: completed })]).client.reconcile(bindingId, requestId, submission), code('identity_mismatch'));
  await assert.rejects(harness([json({ ...response, requestId: eventId })]).client.reconcile(bindingId, requestId, submission), code('identity_mismatch'));
});

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

const questionId = '88888888-8888-4888-8888-888888888888';
const replyId = '99999999-9999-4999-8999-999999999999';
const pendingQuestion = { id: questionId, revision: 1, expiresAt: '2026-09-28T21:00:00.000Z',
  runState: 'waiting_for_reply', runId: sourceEventId, threadId, agent: { id: primaryDexterId, name: 'Dexter' },
  question: 'Which amount should I use?', context: 'You asked about buying a stock.', proposedAnswer: 'Ten dollars.',
  yesMeaning: 'Use ten dollars.', noMeaning: 'Do not place the order.', state: 'pending', createdAt: when, reply: null };
const replyInput = { source: { callSid, itemId: 'answer_1', sourceEventId, revision: 1 as const, receivedAt: when },
  role: 'user' as const, text: 'No.', reply: { questionRevision: 1 as const, clientReplyId: replyId, answer: 'no' as const, text: null } };
const answeredQuestion = { ...pendingQuestion, runState: 'queued', state: 'answered',
  reply: { ...replyInput.reply, interpretedAnswer: pendingQuestion.noMeaning, receivedAt: when } };

test('waiting exposes the exact saved question and never becomes a completed result', async () => {
  const waiting = { bindingId, requestId, state: 'waiting_for_reply', question: pendingQuestion };
  assert.deepEqual(await harness([json(waiting)]).client.result(bindingId, requestId), waiting);
  await assert.rejects(harness([json({ bindingId, requestId, state: 'waiting_for_reply' })]).client.result(bindingId, requestId), code('invalid_response'));
  await assert.rejects(harness([json({ ...waiting, assistant })]).client.result(bindingId, requestId), code('invalid_response'));
  const wrongThread = harness([json(call), json({ ...waiting, question: { ...pendingQuestion, threadId: eventId } })]);
  await wrongThread.client.openCall(proof);
  await assert.rejects(wrongThread.client.result(bindingId, requestId), code('identity_mismatch'));
});

test('expired question remains visible while its original run is waiting for reply', async () => {
  const expired = { ...pendingQuestion, state: 'expired', expiresAt: when };
  const waiting = { bindingId, requestId, state: 'waiting_for_reply', question: expired };
  const { client, requests } = harness([json(waiting), json(waiting), json({ error: { code: 'question_expired', retryable: false } }, 410)]);
  assert.deepEqual(await client.result(bindingId, requestId), waiting);
  assert.deepEqual(await client.question(bindingId, requestId, questionId), waiting);
  await assert.rejects(client.reply(bindingId, requestId, questionId, replyInput), (error: unknown) => {
    assert.ok(error instanceof PrimaryApiError);
    assert.equal(error.code, 'http_error');
    assert.equal(error.status, 410);
    assert.equal(error.remoteCode, 'question_expired');
    assert.equal(error.retryable, false);
    return true;
  });
  assert.equal(requests.length, 3);
  assert.equal(requests.filter(request => request.init.method === 'POST').length, 1);
  assert.ok(requests[2].url.endsWith(`/requests/${requestId}/questions/${questionId}/reply`));
  for (const state of ['answered', 'cancelled', 'unavailable']) {
    const question = state === 'answered' ? { ...answeredQuestion, runState: 'waiting_for_reply' } : { ...expired, state };
    await assert.rejects(harness([json({ ...waiting, question })]).client.result(bindingId, requestId), code('invalid_response'));
  }
});

test('spoken answers use the exact same request and question with canonical reply identity, without submission headers', async () => {
  const envelope = { bindingId, requestId, state: 'queued', question: answeredQuestion, replayed: false };
  const { client, requests } = harness([json(envelope)]);
  assert.deepEqual(await client.reply(bindingId, requestId, questionId, replyInput), envelope);
  assert.equal(requests.length, 1);
  assert.ok(requests[0].url.endsWith(`/requests/${requestId}/questions/${questionId}/reply`));
  assert.equal(new Headers(requests[0].init.headers).get('Idempotency-Key'), null);
  assert.equal(new Headers(requests[0].init.headers).get('X-Dexter-Binding-Id'), bindingId);
  assert.deepEqual(JSON.parse(String(requests[0].init.body)), replyInput);
  const changed = { ...envelope, question: { ...answeredQuestion, reply: { ...answeredQuestion.reply, answer: 'yes' } } };
  await assert.rejects(harness([json(changed)]).client.reply(bindingId, requestId, questionId, replyInput), code('identity_mismatch'));
});

test('unknown answer dispatch is not retried and exact GET can recover an older answer after a later question', async () => {
  const older = { bindingId, requestId, state: 'waiting_for_reply', question: { ...answeredQuestion, runState: 'waiting_for_reply' } };
  const { client, requests } = harness([json(older)]);
  assert.deepEqual(await client.question(bindingId, requestId, questionId), older);
  assert.equal(requests[0].init.method, 'GET');
  let attempts = 0;
  const uncertain = createPrimaryApiClient({ apiBaseUrl: 'https://api.example.test', phoneServiceSecret: secret,
    fetch: async () => { attempts++; throw new Error('synthetic_transport_loss'); } });
  await assert.rejects(uncertain.reply(bindingId, requestId, questionId, replyInput), code('request_failed'));
  assert.equal(attempts, 1);
  await assert.rejects(harness([json({ ...older, question: { ...older.question, id: eventId } })]).client.question(bindingId, requestId, questionId), code('identity_mismatch'));
});

test('reply validation refuses oversized, contradictory and caller-supplied extra authority fields before dispatch', async () => {
  const { client, requests } = harness([]);
  await assert.rejects(client.reply(bindingId, requestId, questionId, { ...replyInput, reply: { ...replyInput.reply, text: 'Use ten dollars.' } }), code('invalid_request'));
  await assert.rejects(client.reply(bindingId, requestId, questionId, { ...replyInput, reply: { ...replyInput.reply, answer: 'other', text: '🙂'.repeat(501) } }), code('invalid_request'));
  await assert.rejects(client.reply(bindingId, requestId, questionId, { ...replyInput, owner: 'another-owner' } as typeof replyInput), code('invalid_request'));
  assert.equal(requests.length, 0);
});

const assignment = { assignmentId: requestId, brokerCallId: sourceEventId, revision: 1 as const, purpose: 'Read the current portfolio.' };
const assignmentRef = { assignmentId: requestId, revision: 1 as const, digest: 'a'.repeat(64) };
const assignedCall = { ...call, callAssignmentRef: assignmentRef };

test('full Hark snapshot crosses actual openCall serialization without changing values or source digest', async () => {
  const harkAssignment = makeResolvedHarkAssignment().callAssignment;
  harkAssignment.hark.event.facts.padding = '';
  harkAssignment.hark.event.facts.padding = 'x'.repeat(HARK_PHONE_SNAPSHOT_MAX_BYTES
    - Buffer.byteLength(JSON.stringify(harkAssignment.hark)));
  const { sourceDigest: _oldDigest, ...source } = harkAssignment.hark;
  harkAssignment.hark.sourceDigest = harkSourceDigest(source);
  assert.equal(Buffer.byteLength(JSON.stringify(harkAssignment.hark)), 32_768);
  const response = { ...call, callAssignmentRef: { ...assignmentRef, assignmentId: harkAssignment.assignmentId } };
  const { client, requests } = harness([json(response)]);
  assert.deepEqual(await client.openCall(proof, harkAssignment), response);
  assert.equal(requests.length, 1);
  const serialized = String(requests[0].init.body);
  assert.ok(Buffer.byteLength(serialized) > 24 * 1024);
  assert.ok(Buffer.byteLength(serialized) < 48 * 1024);
  assert.deepEqual(JSON.parse(serialized), { providerProof: proof, callAssignment: harkAssignment });
});

function proofWithSerializedSize(bytes: number, callAssignment?: Parameters<ReturnType<typeof createPrimaryApiClient>['openCall']>[1]) {
  const candidate = structuredClone(proof);
  const body = { providerProof: candidate, ...(callAssignment === undefined ? {} : { callAssignment }) };
  for (let index = 0; Buffer.byteLength(JSON.stringify(body)) < bytes; index++) {
    assert.ok(index < 70, 'Proof remains inside its field-count limit');
    const key = `Padding${index}`;
    candidate.params[key] = '';
    const remaining = bytes - Buffer.byteLength(JSON.stringify(body));
    assert.ok(remaining >= 0);
    candidate.params[key] = 'x'.repeat(Math.min(2048, remaining));
  }
  assert.equal(Buffer.byteLength(JSON.stringify(body)), bytes);
  return candidate;
}

test('only validated Hark registration receives the 48 KiB request limit', async () => {
  const harkAssignment = makeResolvedHarkAssignment().callAssignment;
  const response = { ...call, callAssignmentRef: { ...assignmentRef, assignmentId: harkAssignment.assignmentId } };
  const exact = harness([json(response)]);
  await exact.client.openCall(proofWithSerializedSize(48 * 1024, harkAssignment), harkAssignment);
  assert.equal(Buffer.byteLength(String(exact.requests[0].init.body)), 48 * 1024);
  const refused = harness([]);
  await assert.rejects(refused.client.openCall(proofWithSerializedSize(48 * 1024 + 1, harkAssignment), harkAssignment), code('invalid_request'));
  for (const ordinary of [undefined, assignment]) {
    const accepted = harness([json(ordinary === undefined ? call : assignedCall)]);
    await accepted.client.openCall(proofWithSerializedSize(24 * 1024, ordinary), ordinary);
    assert.equal(Buffer.byteLength(String(accepted.requests[0].init.body)), 24 * 1024);
    await assert.rejects(refused.client.openCall(proofWithSerializedSize(24 * 1024 + 1, ordinary), ordinary), code('invalid_request'));
  }
  assert.equal(refused.requests.length, 0);
});

test('call assignment registers separately and exact reference accompanies raw submission and reconciliation', async () => {
  const input = { ...submission, text: ' Hello. ', callAssignmentRef: assignmentRef,
    relatedPrimaryRequestId: messageId, referenceOrigin: 'broker_status_publication' as const };
  const turn = originalTurn(input);
  const nonAdmission = { schemaVersion: 1, kind: 'phone_closed_call_non_admission', callSid,
    source: { itemId: input.source.itemId, sourceEventId, revision: 1, receivedAt: when },
    rawSubmissionDigest: rawSubmissionDigest(bindingId, input),
    turn: { recordId: sourceEventId, revisionRecordId: eventId, bodyDigest: turnBodyDigest(turn) }, closedAt: when };
  const { client, requests } = harness([json(assignedCall), json({ ...completed, replayed: false }, 202),
    json({ outcome: 'not_admitted', bindingId, requestId, proof: nonAdmission })]);
  const opened = await client.openCall(proof, assignment);
  // The caller cannot mutate the client's remembered immutable receipt.
  opened.callAssignmentRef!.digest = 'b'.repeat(64);
  await client.submit(bindingId, requestId, input);
  await client.reconcile(bindingId, requestId, input);
  assert.deepEqual(JSON.parse(String(requests[0].init.body)), { providerProof: proof, callAssignment: assignment });
  for (const request of requests.slice(1)) {
    const body = JSON.parse(String(request.init.body));
    assert.deepEqual(body.callAssignmentRef, assignmentRef);
    assert.equal(body.text, ' Hello. ');
    assert.equal(body.relatedPrimaryRequestId, messageId);
    assert.equal(body.purpose, undefined);
    assert.equal(body.callAssignment, undefined);
  }
  assert.equal(turn.text, ' Hello. ');
  assert.equal(turnBodyDigest(turn), turnBodyDigest(originalTurn({ ...submission, text: input.text })));
});

test('registration and remembered context cannot lose or change an assignment reference', async () => {
  for (const response of [call, { ...assignedCall, callAssignmentRef: { ...assignmentRef, assignmentId: messageId } },
    { ...assignedCall, callAssignmentRef: { ...assignmentRef, revision: 2 } }]) {
    await assert.rejects(harness([json(response)]).client.openCall(proof, assignment), error => error instanceof PrimaryApiError);
  }
  await assert.rejects(harness([json(assignedCall)]).client.openCall(proof), code('identity_mismatch'));
  const contextRefresh = harness([json(assignedCall), json(call)]);
  await contextRefresh.client.openCall(proof, assignment);
  assert.deepEqual((await contextRefresh.client.getContext(bindingId)).callAssignmentRef, assignmentRef, 'GET context omission preserves existing registration receipt');
  for (const response of [{ ...assignedCall, callAssignmentRef: { ...assignmentRef, digest: 'b'.repeat(64) } }]) {
    const { client } = harness([json(assignedCall), json(response)]);
    await client.openCall(proof, assignment);
    await assert.rejects(client.getContext(bindingId), code('identity_mismatch'));
  }
  const { client, requests } = harness([json(assignedCall)]);
  await client.openCall(proof, assignment);
  for (const input of [submission, { ...submission, callAssignmentRef: { ...assignmentRef, digest: 'b'.repeat(64) } }]) {
    await assert.rejects(client.submit(bindingId, requestId, input), code('identity_mismatch'));
    await assert.rejects(client.reconcile(bindingId, requestId, input), code('identity_mismatch'));
  }
  assert.equal(requests.length, 1);
});

test('assignment validation uses exact trimmed purpose and the existing UTF-16 1500 limit', async () => {
  const good = { ...assignment, purpose: '😀'.repeat(750) };
  assert.equal(good.purpose.length, 1500);
  const h = harness([json(assignedCall)]);
  await h.client.openCall(proof, good);
  assert.equal(JSON.parse(String(h.requests[0].init.body)).callAssignment.purpose, good.purpose);
  for (const value of [{ ...good, purpose: good.purpose + 'a' }, { ...assignment, purpose: ' padded ' },
    { ...assignment, purpose: '' }, { ...assignment, purpose: 'Read\u0000portfolio' },
    { ...assignment, purpose: 'Read\uD800portfolio' }, { ...assignment, purpose: 'Read\uDC00portfolio' },
    { ...assignment, assignmentId: 'ABCDEFAB-CDEF-4ABC-8ABC-ABCDEFABCDEF' },
    { ...assignment, brokerCallId: 'ABCDEFAB-CDEF-4ABC-8ABC-ABCDEFABCDEF' },
    { ...assignment, revision: 2 }, { ...assignment, issuer: 'owner' }]) {
    const { client, requests } = harness([]);
    await assert.rejects(client.openCall(proof, value as typeof assignment), code('invalid_request'));
    assert.equal(requests.length, 0);
  }
});

test('question replies reject assignment refs instead of silently treating them as new missions', async () => {
  const { client, requests } = harness([]);
  const { role, ...source } = submission.source;
  await assert.rejects(client.reply(bindingId, requestId, messageId, { source, role, text: 'Yes',
    reply: { questionRevision: 1, clientReplyId: eventId, answer: 'yes', text: null },
    callAssignmentRef: assignmentRef } as never), code('invalid_request'));
  assert.equal(requests.length, 0);
});


test('registration replays echo the same original control UUID and immutable returned receipt', async () => {
  const originalId = 'abcdefab-cdef-4abc-8abc-abcdefabcdef';
  const original = { ...assignment, assignmentId: originalId };
  const reference = { ...assignmentRef, assignmentId: originalId };
  const response = { ...call, callAssignmentRef: reference };
  const { client, requests } = harness([json(response), json(response)]);
  assert.deepEqual(await client.openCall(proof, original), response);
  assert.deepEqual(await client.openCall(proof, original), response);
  assert.equal(requests[0].init.body, requests[1].init.body);
  assert.equal(JSON.parse(String(requests[0].init.body)).callAssignment.assignmentId, originalId);
});
