import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import test from 'node:test';
import { createRequestDeliveryClient, createRequestDeliveryJournal, parseRequestDeliveryJournal,
  retainRequestDeliveryAssociation, retainRequestDeliveryCallerSource, assertRequestDeliveryJournalUpdate,
  retainRequestDeliveryReply,
  createRequestDeliveryQuestionRecord, parseRequestDeliveryQuestionRecord, assertRequestDeliveryQuestionRecordUpdate,
  appendRequestDeliveryReplyAttempt, beginRequestDeliveryReplyAttempt, refuseUndispatchedRequestDeliveryReply,
  observeRequestDeliveryReplyFailure, observeRequestDeliveryReplyResult,
  requestDeliveryHeaders, requestDeliveryRequestSchema, requestDeliveryResponseSchema,
  PHONE_REQUEST_DELIVERY_HEADER, PHONE_REQUEST_DELIVERY_VERSION,
  type RequestDeliveryJournal, type RequestDeliveryJournalInput, type RequestDeliveryResponse,
  type RequestDeliveryQuestionResult,
} from '../src/broker/requestDelivery.js';
import { PrimaryApiError, type PrimaryApiOptions, type PrimaryTurn, type PrimaryReplyInput } from '../src/broker/primaryApi.js';
import { turnBodyDigest } from '../src/broker/reconciliation.js';

const deliveryBinding = '11111111-1111-4111-8111-111111111111';
const executionBinding = '22222222-2222-4222-8222-222222222222';
const requestId = '33333333-3333-4333-8333-333333333333';
const aliasId = '44444444-4444-4444-8444-444444444444';
const runId = '55555555-5555-4555-8555-555555555555';
const questionId = '66666666-6666-4666-8666-666666666666';
const associationId = '77777777-7777-4777-8777-777777777777';
const ownerId = '88888888-8888-4888-8888-888888888888';
const primaryId = '99999999-9999-4999-8999-999999999999';
const threadId = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
const sourceId = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';
const revisionId = 'cccccccc-cccc-4ccc-8ccc-cccccccccccc';
const callId = 'dddddddd-dddd-4ddd-8ddd-dddddddddddd';
const foreignId = 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee';
const callSid = `CA${'1'.repeat(32)}`;
const when = '2026-09-24T12:00:00.000Z';
const secret = 'synthetic-request-delivery-service-secret';
const input = (): RequestDeliveryJournalInput => ({
  deliveryCall: { callId, sessionId: 'live-new-call' },
  scope: { ownerPrincipalId: ownerId, primaryDexterId: primaryId, threadId },
  target: { delivery: { bindingId: deliveryBinding, callSid },
    execution: { bindingId: executionBinding, requestId, runId }, question: { id: questionId, revision: 1 } },
  request: { v: PHONE_REQUEST_DELIVERY_VERSION, originalRequestId: aliasId, questionId, questionRevision: 1 },
});
const response = (): RequestDeliveryResponse => ({ association: {
  id: associationId, v: PHONE_REQUEST_DELIVERY_VERSION,
  ...input().target, createdAt: when,
}, replayed: false });
const journal = () => createRequestDeliveryJournal(input());
const associated = () => retainRequestDeliveryAssociation(journal(), response());
const turn = (): PrimaryTurn => ({ sourceEventId: sourceId, revision: 1, itemId: 'new_call_turn_1',
  role: 'user', text: ' Yes, use NVIDIA.\t', occurredAt: when });
const capture = () => ({ bindingId: deliveryBinding, sourceEventId: sourceId, revision: 1,
  recordId: sourceId, revisionRecordId: revisionId, bodyDigest: turnBodyDigest(turn()), replayed: false });
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status,
  headers: { 'Content-Type': 'application/json' } });
const code = (expected: string) => (error: unknown) => error instanceof PrimaryApiError && error.code === expected;
function harness(responses: Array<Response | Error>) {
  const requests: Array<{ url: string; init: RequestInit }> = [];
  const options: PrimaryApiOptions = { apiBaseUrl: 'https://api.example.test', phoneServiceSecret: secret,
    fetch: async (url, init) => {
      requests.push({ url: String(url), init: init ?? {} });
      const next = responses.shift();
      assert.ok(next, 'unexpected HTTP request');
      if (next instanceof Error) throw next;
      return next;
    } };
  return { client: createRequestDeliveryClient(options), requests };
}

test('association uses exact later binding, service auth, context protocol and strict body', async () => {
  const { client, requests } = harness([json(response())]);
  assert.deepEqual(await client.associate(journal()), response());
  assert.equal(requests.length, 1);
  const sent = requests[0];
  assert.equal(sent.url, `https://api.example.test/internal/primary-dexter/channels/phone/calls/${deliveryBinding}/request-deliveries`);
  assert.equal(sent.init.method, 'POST');
  assert.deepEqual(JSON.parse(String(sent.init.body)), input().request);
  const headers = new Headers(sent.init.headers);
  assert.equal(headers.get('X-Dexter-Binding-Id'), deliveryBinding);
  assert.equal(headers.get('X-Dexter-Phone-Context-Version'), 'memory-eligible-v1');
  assert.equal(headers.get('Authorization'), `Bearer ${createHmac('sha256', secret)
    .update('dexter-broker/primary-ingress/v1').digest('hex')}`);
  assert.equal(headers.get(PHONE_REQUEST_DELIVERY_HEADER), null);
  assert.equal(headers.get('Idempotency-Key'), null);
});

test('explicit alias retains canonical execution request and exact retry preserves association', async () => {
  const first = journal();
  const retained = retainRequestDeliveryAssociation(first, response());
  assert.equal(retained.request.originalRequestId, aliasId);
  assert.equal(retained.association?.execution.requestId, requestId);
  assert.equal(first.association, undefined);
  const replay = { ...response(), replayed: true };
  assert.deepEqual(retainRequestDeliveryAssociation(retained, replay), retained);
  const { client, requests } = harness([json(replay)]);
  assert.deepEqual(await client.associate(retained), replay);
  assert.equal(JSON.parse(String(requests[0].init.body)).originalRequestId, aliasId);
});

test('later-call client exposes association and existing work only, without creating a new execution', () => {
  assert.deepEqual(Object.keys(harness([]).client), ['associate', 'result', 'question', 'reply']);
  assert.equal('answer' in response().association, false);
  assert.equal('state' in response().association, false);
});

test('strict request rejects caller owner, run, source and unknown fields', () => {
  for (const added of [{ ownerId }, { runId }, { callSid }, { text: 'approve' }, { source: turn() }, { bindingId: deliveryBinding }]) {
    assert.equal(requestDeliveryRequestSchema.safeParse({ ...input().request, ...added }).success, false);
  }
  assert.equal(requestDeliveryRequestSchema.safeParse({ ...input().request, questionRevision: 2 }).success, false);
});

test('normalized UUID and question constraints refuse before HTTP without rewriting values', async () => {
  for (const change of [
    (entry: RequestDeliveryJournal) => { entry.request.originalRequestId = threadId.toUpperCase(); },
    (entry: RequestDeliveryJournal) => { entry.target.delivery.bindingId = '../calls'; },
    (entry: RequestDeliveryJournal) => { entry.scope.ownerPrincipalId = 'not-an-id'; },
    (entry: RequestDeliveryJournal) => { entry.request.questionId = foreignId; },
    (entry: RequestDeliveryJournal) => { entry.target.execution.bindingId = deliveryBinding; },
  ]) {
    const entry = journal(); change(entry);
    const { client, requests } = harness([]);
    await assert.rejects(client.associate(entry), error => error instanceof PrimaryApiError);
    assert.equal(requests.length, 0);
  }
});

test('strict response rejects unknown or incomplete nested identity and response keys', () => {
  for (const value of [
    { ...response(), bindingId: deliveryBinding },
    { association: response().association },
    { ...response(), association: { ...response().association, ownerPrincipalId: ownerId } },
    { ...response(), association: { ...response().association, id: sourceId.toUpperCase() } },
    { ...response(), association: { ...response().association, delivery: { bindingId: deliveryBinding } } },
    { ...response(), association: { ...response().association, execution: { bindingId: executionBinding, requestId } } },
    { ...response(), association: { ...response().association, question: { id: questionId, revision: 2 } } },
    { ...response(), association: { ...response().association, createdAt: 'tomorrow' } },
  ]) assert.equal(requestDeliveryResponseSchema.safeParse(value).success, false);
});

test('each returned execution, delivery and question identity must match its pinned target', async () => {
  for (const change of [
    (result: RequestDeliveryResponse) => { result.association.delivery.bindingId = foreignId; },
    (result: RequestDeliveryResponse) => { result.association.delivery.callSid = `CA${'2'.repeat(32)}`; },
    (result: RequestDeliveryResponse) => { result.association.execution.bindingId = foreignId; },
    (result: RequestDeliveryResponse) => { result.association.execution.requestId = aliasId; },
    (result: RequestDeliveryResponse) => { result.association.execution.runId = foreignId; },
    (result: RequestDeliveryResponse) => { result.association.question.id = foreignId; },
  ]) {
    const result = response(); change(result);
    await assert.rejects(harness([json(result)]).client.associate(journal()), code('identity_mismatch'));
  }
});

test('a saved association cannot be replaced or given a different creation timestamp', async () => {
  for (const fields of [{ id: foreignId }, { createdAt: '2026-09-24T12:01:00.000Z' }]) {
    const changed = { ...response(), association: { ...response().association, ...fields }, replayed: true };
    assert.throws(() => retainRequestDeliveryAssociation(associated(), changed), code('identity_mismatch'));
    await assert.rejects(harness([json(changed)]).client.associate(associated()), code('identity_mismatch'));
  }
});

test('uncertain association sends once and leaves the journal available for explicit exact recovery', async () => {
  const entry = journal();
  const before = JSON.stringify(entry);
  const { client, requests } = harness([new Error('transport lost')]);
  await assert.rejects(client.associate(entry), code('request_failed'));
  assert.equal(requests.length, 1);
  assert.equal(JSON.stringify(entry), before);
  assert.deepEqual(await harness([json({ ...response(), replayed: true })]).client.associate(entry),
    { ...response(), replayed: true });
});

test('authority, active-original, expiry and unavailable failures stay refusals without recovery side effects', async () => {
  for (const [status, remoteCode] of [[403, 'binding_unauthorized'], [409, 'original_call_active'],
    [409, 'question_expired'], [409, 'question_already_answered'], [409, 'delivery_association_mismatch'],
    [503, 'temporarily_unavailable']] as const) {
    const { client, requests } = harness([json({ error: { code: remoteCode, retryable: status === 503 } }, status)]);
    const entry = journal();
    await assert.rejects(client.associate(entry), error => error instanceof PrimaryApiError
      && error.status === status && error.remoteCode === remoteCode);
    assert.equal(requests.length, 1);
    assert.equal(entry.association, undefined);
  }
});

test('caller source capture preserves raw digest under new binding and leaves original execution unchanged', () => {
  const entry = associated();
  const oldExecution = structuredClone(entry.target.execution);
  const captured = retainRequestDeliveryCallerSource(entry, turn(), capture());
  assert.deepEqual(captured.target.execution, oldExecution);
  assert.deepEqual(captured.association?.execution, oldExecution);
  assert.equal(entry.callerSource, undefined);
  assert.deepEqual(captured.callerSource, { bindingId: deliveryBinding, callSid, sourceEventId: sourceId,
    revision: 1, itemId: 'new_call_turn_1', role: 'user', occurredAt: when,
    recordId: sourceId, revisionRecordId: revisionId, bodyDigest: turnBodyDigest(turn()) });
  assert.notEqual(captured.callerSource?.bodyDigest, turnBodyDigest({ ...turn(), text: turn().text.trim() }));
  assert.deepEqual(retainRequestDeliveryCallerSource(captured, turn(), { ...capture(), replayed: true }), captured);
});

test('capture needs association plus acknowledged new binding, source and original body digest', () => {
  assert.throws(() => retainRequestDeliveryCallerSource(journal(), turn(), capture()), code('identity_mismatch'));
  for (const change of [
    { bindingId: executionBinding }, { sourceEventId: foreignId }, { recordId: foreignId },
    { bodyDigest: 'f'.repeat(64) }, { revisionRecordId: undefined },
  ]) assert.throws(() => retainRequestDeliveryCallerSource(associated(), turn(), { ...capture(), ...change }),
    error => error instanceof PrimaryApiError);
  assert.throws(() => retainRequestDeliveryCallerSource(associated(), { ...turn(), role: 'assistant' }, capture()), code('invalid_request'));
  assert.throws(() => retainRequestDeliveryCallerSource(associated(), { ...turn(), revision: 2 }, capture()), code('invalid_request'));
});

test('a saved caller source cannot be remapped by a later revision, another source or different receipt', () => {
  const saved = retainRequestDeliveryCallerSource(associated(), turn(), capture());
  const changedTurn = { ...turn(), sourceEventId: foreignId };
  assert.throws(() => retainRequestDeliveryCallerSource(saved, changedTurn, { ...capture(), sourceEventId: foreignId,
    recordId: foreignId, bodyDigest: turnBodyDigest(changedTurn) }), code('identity_mismatch'));
  assert.throws(() => retainRequestDeliveryCallerSource(saved, turn(), { ...capture(), revisionRecordId: foreignId }), code('identity_mismatch'));
});

test('restart accepts exact source receipt and refuses a source transplanted onto the original binding', () => {
  const saved = retainRequestDeliveryCallerSource(associated(), turn(), capture());
  assert.deepEqual(parseRequestDeliveryJournal(JSON.parse(JSON.stringify(saved))), saved);
  for (const change of [
    { bindingId: executionBinding }, { callSid: `CA${'2'.repeat(32)}` }, { recordId: foreignId },
  ]) assert.throws(() => parseRequestDeliveryJournal({ ...saved, callerSource: { ...saved.callerSource, ...change } }), code('identity_mismatch'));
  assert.throws(() => parseRequestDeliveryJournal({ ...saved, association: undefined }), code('identity_mismatch'));
  assert.throws(() => parseRequestDeliveryJournal({ ...saved, originalDelegation: {} }), code('invalid_request'));
});

test('store compare refuses immutable-field changes and removal of saved receipts', () => {
  const saved = retainRequestDeliveryCallerSource(associated(), turn(), capture());
  for (const change of [
    (next: RequestDeliveryJournal) => { next.deliveryCall.callId = foreignId; },
    (next: RequestDeliveryJournal) => { next.deliveryCall.sessionId = 'other-live-session'; },
    (next: RequestDeliveryJournal) => { next.scope.ownerPrincipalId = foreignId; },
    (next: RequestDeliveryJournal) => { next.scope.primaryDexterId = foreignId; },
    (next: RequestDeliveryJournal) => { next.scope.threadId = foreignId; },
    (next: RequestDeliveryJournal) => { next.request.originalRequestId = requestId; },
    (next: RequestDeliveryJournal) => { delete next.callerSource; },
    (next: RequestDeliveryJournal) => { delete next.association; delete next.callerSource; },
  ]) {
    const next = structuredClone(saved); change(next);
    assert.throws(() => assertRequestDeliveryJournalUpdate(saved, next), code('identity_mismatch'));
  }
});

test('pure helpers do not alias input objects or change the original journal when outputs are mutated', () => {
  const fields = input();
  const created = createRequestDeliveryJournal(fields);
  fields.scope.threadId = foreignId;
  assert.equal(created.scope.threadId, threadId);
  const receipt = response();
  const retained = retainRequestDeliveryAssociation(created, receipt);
  receipt.association.execution.runId = foreignId;
  assert.equal(retained.association?.execution.runId, runId);
  retained.scope.threadId = foreignId;
  assert.equal(created.scope.threadId, threadId);
});

test('later-call headers use delivery identity and association; unsaved association cannot select a route', () => {
  assert.throws(() => requestDeliveryHeaders(journal()), code('identity_mismatch'));
  assert.deepEqual(requestDeliveryHeaders(associated()), {
    'X-Dexter-Binding-Id': deliveryBinding,
    'X-Dexter-Phone-Context-Version': 'memory-eligible-v1',
    [PHONE_REQUEST_DELIVERY_HEADER]: associationId,
  });
});

function savedQuestion() {
  return { id: questionId, revision: 1 as const, expiresAt: '2026-09-25T12:00:00.000Z',
    runState: 'waiting_for_reply' as const, runId, threadId, agent: { id: primaryId, name: 'Dexter' },
    question: 'Should I research NVIDIA?', context: 'You asked for a stock comparison.',
    proposedAnswer: 'Research NVIDIA.', yesMeaning: 'Research NVIDIA.', noMeaning: 'Choose another stock.',
    state: 'pending' as const, createdAt: when, reply: null };
}
function questionResult() {
  return { requestId: aliasId, bindingId: executionBinding, state: 'waiting_for_reply' as const,
    question: savedQuestion(), deliveryAssociation: response().association };
}
function replyInput(): PrimaryReplyInput {
  return { source: { callSid, sourceEventId: sourceId, itemId: turn().itemId, revision: 1, receivedAt: when },
    role: 'user', text: turn().text,
    reply: { questionRevision: 1, clientReplyId: foreignId, answer: 'yes', text: null } };
}
const preparedReply = () => retainRequestDeliveryReply(
  retainRequestDeliveryCallerSource(associated(), turn(), capture()), replyInput());
function answered() {
  return { requestId: aliasId, bindingId: executionBinding, state: 'queued' as const, replayed: false,
    question: { ...savedQuestion(), state: 'answered' as const, runState: 'queued' as const,
      reply: { ...replyInput().reply, interpretedAnswer: 'Research NVIDIA.', receivedAt: when } },
    deliveryAssociation: response().association };
}

test('later result and question reads preserve execution envelope with delivery authorization headers', async () => {
  const { client, requests } = harness([json(questionResult()), json(questionResult())]);
  assert.deepEqual(await client.result(associated()), questionResult());
  assert.deepEqual(await client.question(associated()), questionResult());
  assert.equal(requests[0].url.endsWith(`/requests/${aliasId}`), true);
  assert.equal(requests[1].url.endsWith(`/requests/${aliasId}/questions/${questionId}`), true);
  for (const sent of requests) {
    assert.equal(sent.init.method, 'GET');
    const headers = new Headers(sent.init.headers);
    assert.equal(headers.get('X-Dexter-Binding-Id'), deliveryBinding);
    assert.equal(headers.get(PHONE_REQUEST_DELIVERY_HEADER), associationId);
    assert.equal(headers.get('X-Dexter-Phone-Context-Version'), 'memory-eligible-v1');
    assert.equal(sent.init.body, undefined);
  }
});

test('all later result states require the full association and original execution binding', async () => {
  const common = { requestId: aliasId, bindingId: executionBinding, deliveryAssociation: response().association };
  const states = [
    { ...common, state: 'queued' }, { ...common, state: 'running' }, questionResult(),
    { ...common, state: 'completed', assistant: { messageId: sourceId, eventId: revisionId,
      sequence: '15', text: 'Your comparison is ready.' } },
    { ...common, state: 'failed', error: { code: 'work_failed', retryable: false } },
    { ...common, state: 'cancelled', error: { code: 'work_cancelled', retryable: false } },
  ];
  for (const value of states) {
    assert.deepEqual(await harness([json(value)]).client.result(associated()), value);
    const { deliveryAssociation: _association, ...missing } = value;
    await assert.rejects(harness([json(missing)]).client.result(associated()), code('invalid_response'));
    await assert.rejects(harness([json({ ...value, bindingId: deliveryBinding })]).client.result(associated()), code('identity_mismatch'));
  }
});

test('later reads reject mismatched association, alias echo, run and owned context', async () => {
  for (const mutate of [
    (value: ReturnType<typeof questionResult>) => { value.requestId = requestId; },
    (value: ReturnType<typeof questionResult>) => { value.deliveryAssociation.id = foreignId; },
    (value: ReturnType<typeof questionResult>) => { value.question.runId = foreignId; },
    (value: ReturnType<typeof questionResult>) => { value.question.threadId = foreignId; },
    (value: ReturnType<typeof questionResult>) => { value.question.agent.id = foreignId; },
  ]) {
    const value = questionResult(); mutate(value);
    await assert.rejects(harness([json(value)]).client.question(associated()), code('identity_mismatch'));
    await assert.rejects(harness([json(value)]).client.result(associated()), code('identity_mismatch'));
  }
});

test('result discovers a later same-run question without authorizing it under the earlier association', async () => {
  const later = questionResult(); later.question.id = foreignId;
  assert.deepEqual(await harness([json(later)]).client.result(associated()), later);
  await assert.rejects(harness([json(later)]).client.question(associated()), code('identity_mismatch'));
  const laterAnswer = answered(); laterAnswer.question.id = foreignId;
  await assert.rejects(harness([json(laterAnswer)]).client.reply(preparedReply()), code('identity_mismatch'));
  assert.throws(() => appendRequestDeliveryReplyAttempt(createRequestDeliveryQuestionRecord(associated()),
    preparedReply(), later, when), code('identity_mismatch'));
  const nextInput = input(); nextInput.target.question.id = foreignId; nextInput.request.questionId = foreignId;
  const nextResponse = response(); nextResponse.association.id = sourceId; nextResponse.association.question.id = foreignId;
  const next = retainRequestDeliveryAssociation(createRequestDeliveryJournal(nextInput), nextResponse);
  const ownLater = { ...later, deliveryAssociation: nextResponse.association };
  assert.deepEqual(await harness([json(ownLater)]).client.question(next), ownLater);
  assert.equal(next.target.execution.runId, associated().target.execution.runId);
});

test('later read/refused association cannot be silently downgraded to ordinary same-call access', async () => {
  const { client, requests } = harness([]);
  await assert.rejects(client.result(journal()), code('identity_mismatch'));
  await assert.rejects(client.question(journal()), code('identity_mismatch'));
  await assert.rejects(client.reply(journal()), code('identity_mismatch'));
  assert.equal(requests.length, 0);
  const refused = harness([json({ error: { code: 'delivery_association_mismatch', retryable: false } }, 409)]);
  await assert.rejects(refused.client.result(associated()), error => error instanceof PrimaryApiError
    && error.remoteCode === 'delivery_association_mismatch');
  assert.equal(refused.requests.length, 1);
});

test('reply preparation binds new caller source and exact reply identity before network dispatch', () => {
  assert.throws(() => retainRequestDeliveryReply(associated(), replyInput()), code('identity_mismatch'));
  const captured = retainRequestDeliveryCallerSource(associated(), turn(), capture());
  for (const fields of [
    { text: replyInput().text.trim() },
    { source: { ...replyInput().source, callSid: `CA${'2'.repeat(32)}` } },
    { source: { ...replyInput().source, sourceEventId: foreignId } },
    { source: { ...replyInput().source, itemId: 'other_item' } },
    { source: { ...replyInput().source, receivedAt: '2026-09-24T12:01:00.000Z' } },
  ]) assert.throws(() => retainRequestDeliveryReply(captured, { ...replyInput(), ...fields }), code('identity_mismatch'));
  const prepared = preparedReply();
  assert.equal(captured.reply, undefined);
  assert.deepEqual(prepared.reply?.input, replyInput());
  assert.deepEqual(retainRequestDeliveryReply(prepared, replyInput()), prepared);
  assert.throws(() => retainRequestDeliveryReply(prepared, { ...replyInput(),
    reply: { ...replyInput().reply, answer: 'no' } }), code('identity_mismatch'));
});

test('explicit later-call reply sends saved raw source, retains original run and requires exact reply echo', async () => {
  const prepared = preparedReply();
  const { client, requests } = harness([json(answered()), json({ ...answered(), replayed: true })]);
  assert.deepEqual(await client.reply(prepared), answered());
  assert.deepEqual(await client.reply(prepared), { ...answered(), replayed: true });
  for (const sent of requests) {
    assert.equal(sent.url.endsWith(`/requests/${aliasId}/questions/${questionId}/reply`), true);
    assert.equal(sent.init.method, 'POST');
    assert.deepEqual(JSON.parse(String(sent.init.body)), replyInput());
    const headers = new Headers(sent.init.headers);
    assert.equal(headers.get(PHONE_REQUEST_DELIVERY_HEADER), associationId);
    assert.equal(headers.get('X-Dexter-Binding-Id'), deliveryBinding);
    assert.equal(headers.get('Idempotency-Key'), null);
  }
  assert.equal(prepared.target.execution.runId, runId);
  assert.equal(prepared.target.execution.bindingId, executionBinding);
});

test('reply dispatch requires durable reply and does not recalculate a new identity after uncertainty', async () => {
  const missing = harness([]);
  await assert.rejects(missing.client.reply(associated()), code('invalid_request'));
  assert.equal(missing.requests.length, 0);
  const prepared = preparedReply();
  const original = JSON.stringify(prepared);
  const lost = harness([new Error('reply acknowledgement lost')]);
  await assert.rejects(lost.client.reply(prepared), code('request_failed'));
  assert.equal(lost.requests.length, 1);
  assert.equal(JSON.stringify(prepared), original);
  assert.deepEqual(await harness([json({ ...answered(), replayed: true })]).client.reply(prepared),
    { ...answered(), replayed: true });
});

test('reply response must carry matching committed answer and exact association', async () => {
  for (const mutate of [
    (value: ReturnType<typeof answered>) => { value.question.reply.clientReplyId = sourceId; },
    (value: ReturnType<typeof answered>) => { value.question.reply.answer = 'no'; },
    (value: ReturnType<typeof answered>) => { value.deliveryAssociation.id = foreignId; },
    (value: ReturnType<typeof answered>) => { value.bindingId = deliveryBinding; },
  ]) {
    const value = answered(); mutate(value);
    await assert.rejects(harness([json(value)]).client.reply(preparedReply()), code('identity_mismatch'));
  }
  const { replayed: _replayed, ...missing } = answered();
  await assert.rejects(harness([json(missing)]).client.reply(preparedReply()), code('invalid_response'));
});

test('restart and compare preserve association-bound reply checksum and refuse dropped or changed reply', () => {
  const prepared = preparedReply();
  assert.deepEqual(parseRequestDeliveryJournal(JSON.parse(JSON.stringify(prepared))), prepared);
  assert.throws(() => parseRequestDeliveryJournal({ ...prepared,
    reply: { ...prepared.reply, localDigest: 'f'.repeat(64) } }), code('identity_mismatch'));
  const without = structuredClone(prepared); delete without.reply;
  assert.throws(() => assertRequestDeliveryJournalUpdate(prepared, without), code('identity_mismatch'));
  const otherAssociation = { ...response(), association: { ...response().association, id: foreignId } };
  const other = retainRequestDeliveryReply(retainRequestDeliveryCallerSource(
    retainRequestDeliveryAssociation(journal(), otherAssociation), turn(), capture()), replyInput());
  assert.notEqual(prepared.reply?.localDigest, other.reply?.localDigest);
});

const later = '2026-09-24T12:01:00.000Z';
const latest = '2026-09-24T12:02:00.000Z';
const secondSourceId = '12121212-1212-4212-8212-121212121212';
const secondReplyId = '13131313-1313-4313-8313-131313131313';
function anotherReply(sourceEventId = secondSourceId, clientReplyId = secondReplyId): RequestDeliveryJournal {
  const source = { ...turn(), sourceEventId, itemId: 'new_call_turn_2', text: ' No, compare Apple instead.\t', occurredAt: latest };
  const receipt = { ...capture(), sourceEventId, recordId: sourceEventId, revisionRecordId: secondReplyId,
    bodyDigest: turnBodyDigest(source) };
  return retainRequestDeliveryReply(retainRequestDeliveryCallerSource(associated(), source, receipt), {
    source: { callSid, sourceEventId, itemId: source.itemId, revision: 1, receivedAt: source.occurredAt },
    role: 'user', text: source.text,
    reply: { questionRevision: 1, clientReplyId, answer: 'no', text: null },
  });
}
function firstAttempt() {
  return appendRequestDeliveryReplyAttempt(createRequestDeliveryQuestionRecord(associated()),
    preparedReply(), questionResult(), when);
}
function refusedAttempt() {
  return observeRequestDeliveryReplyFailure(beginRequestDeliveryReplyAttempt(firstAttempt(), when),
    new PrimaryApiError('http_error', 409, { code: 'source_conflict', retryable: false }), later);
}

test('reply attempts start only with an associated base, acknowledged explicit source and pending exact question', () => {
  assert.throws(() => createRequestDeliveryQuestionRecord(journal()), code('identity_mismatch'));
  assert.throws(() => createRequestDeliveryQuestionRecord(preparedReply()), code('identity_mismatch'));
  const base = createRequestDeliveryQuestionRecord(associated());
  assert.deepEqual(base.attempts, []);
  for (const value of [associated(), retainRequestDeliveryCallerSource(associated(), turn(), capture())])
    assert.throws(() => appendRequestDeliveryReplyAttempt(base, value, questionResult(), when), code('identity_mismatch'));
  for (const mutate of [
    (value: RequestDeliveryQuestionResult) => { value.question.id = foreignId; },
    (value: RequestDeliveryQuestionResult) => { value.question.runId = foreignId; },
    (value: RequestDeliveryQuestionResult) => { value.question.state = 'expired'; },
    (value: RequestDeliveryQuestionResult) => { value.question.expiresAt = when; },
  ]) {
    const value: RequestDeliveryQuestionResult = questionResult(); mutate(value);
    assert.throws(() => appendRequestDeliveryReplyAttempt(base, preparedReply(), value, when), code('identity_mismatch'));
  }
  const prepared = firstAttempt();
  assert.equal(prepared.attempts[0].state, 'prepared');
  assert.equal(prepared.attempts[0].dispatches, 0);
  assert.deepEqual(base.attempts, []);
});

test('dispatch marker and exact original body survive restart and acknowledgement loss', async () => {
  const prepared = firstAttempt();
  const dispatched = beginRequestDeliveryReplyAttempt(prepared, when);
  assert.equal(prepared.attempts[0].state, 'prepared');
  assert.equal(dispatched.attempts[0].state, 'dispatching');
  const { client, requests } = harness([new Error('synthetic network interruption')]);
  let failure: unknown;
  try { await client.reply(dispatched.attempts[0].journal); } catch (error) { failure = error; }
  assert.ok(failure instanceof PrimaryApiError);
  const unknown = observeRequestDeliveryReplyFailure(dispatched, failure, later);
  assert.equal(unknown.attempts[0].state, 'unknown');
  assert.equal(unknown.attempts[0].refusal, undefined);
  assert.throws(() => appendRequestDeliveryReplyAttempt(unknown, anotherReply(), questionResult(), latest), code('identity_mismatch'));
  const restarted = parseRequestDeliveryQuestionRecord(JSON.parse(JSON.stringify(unknown)));
  const recovery = beginRequestDeliveryReplyAttempt(restarted, latest);
  assert.equal(recovery.attempts[0].dispatches, 2);
  assert.equal(recovery.attempts[0].recovery, true);
  assert.deepEqual(recovery.attempts[0].journal, prepared.attempts[0].journal);
  const successful = harness([json({ ...answered(), replayed: true })]);
  const recoveredResponse = await successful.client.reply(recovery.attempts[0].journal);
  assert.equal(successful.requests[0].init.body, requests[0].init.body);
  const accepted = observeRequestDeliveryReplyResult(recovery, recoveredResponse, latest);
  assert.equal(accepted.attempts[0].state, 'accepted');
  assert.deepEqual(parseRequestDeliveryQuestionRecord(JSON.parse(JSON.stringify(accepted))), accepted);
  assert.deepEqual(observeRequestDeliveryReplyResult(accepted, answered(), latest), accepted);
  assert.throws(() => appendRequestDeliveryReplyAttempt(accepted, anotherReply(), questionResult(), latest), code('identity_mismatch'));
});

test('restart with dispatch in flight never upgrades a later definite refusal into original non-admission', () => {
  const dispatched = beginRequestDeliveryReplyAttempt(firstAttempt(), when);
  const recovered = beginRequestDeliveryReplyAttempt(parseRequestDeliveryQuestionRecord(JSON.parse(JSON.stringify(dispatched))), later);
  const stillUnknown = observeRequestDeliveryReplyFailure(recovered,
    new PrimaryApiError('http_error', 409, { code: 'source_conflict', retryable: false }), latest);
  assert.equal(stillUnknown.attempts[0].state, 'unknown');
  assert.equal(stillUnknown.attempts[0].refusal, undefined);
  assert.throws(() => appendRequestDeliveryReplyAttempt(stillUnknown, anotherReply(), questionResult(), latest), code('identity_mismatch'));
  const pending = observeRequestDeliveryReplyResult(stillUnknown, questionResult(), latest);
  assert.equal(pending.attempts[0].state, 'unknown');
  assert.throws(() => appendRequestDeliveryReplyAttempt(pending, anotherReply(), questionResult(), latest), code('identity_mismatch'));
});

test('proved first refusal allows a fresh explicit reply while preserving the entire old attempt', () => {
  const refused = refusedAttempt();
  assert.equal(refused.attempts[0].state, 'refused');
  assert.deepEqual(refused.attempts[0].refusal, { kind: 'first_attempt_refusal', origin: 'api_non_admission',
    code: 'source_conflict', status: 409, observedAt: later });
  const record = appendRequestDeliveryReplyAttempt(refused, anotherReply(), questionResult(), latest);
  assert.equal(record.attempts.length, 2);
  assert.deepEqual(record.attempts[0], refused.attempts[0]);
  assert.equal(record.attempts[1].state, 'prepared');
  assert.equal(record.attempts[1].sequence, 2);
  assert.equal(record.attempts[1].journal.callerSource?.sourceEventId, secondSourceId);
  assert.equal(record.attempts[0].journal.reply?.input.text, turn().text);
  assert.equal(record.attempts[1].journal.reply?.input.text, anotherReply().reply?.input.text);
  assert.deepEqual(parseRequestDeliveryQuestionRecord(JSON.parse(JSON.stringify(record))), record);
  assert.throws(() => appendRequestDeliveryReplyAttempt(refused, anotherReply(), questionResult(), when), code('identity_mismatch'));
  assert.throws(() => appendRequestDeliveryReplyAttempt(refused, anotherReply(sourceId), questionResult(), latest), code('identity_mismatch'));
  assert.throws(() => appendRequestDeliveryReplyAttempt(refused, anotherReply(secondSourceId, foreignId), questionResult(), latest), code('identity_mismatch'));
  const changedOwner = anotherReply(); changedOwner.scope.ownerPrincipalId = foreignId;
  assert.throws(() => appendRequestDeliveryReplyAttempt(refused, changedOwner, questionResult(), latest), code('identity_mismatch'));
});

test('only finite first-attempt refusal codes can establish non-admission; 503 and generic conflicts stay unknown', () => {
  for (const error of [
    new PrimaryApiError('request_timeout'), new PrimaryApiError('invalid_response'),
    new PrimaryApiError('http_error', 503, { code: 'temporarily_unavailable', retryable: true }),
    new PrimaryApiError('http_error', 503, { code: 'primary_unavailable', retryable: false }),
    new PrimaryApiError('http_error', 409, { code: 'unknown_conflict', retryable: false }),
    new PrimaryApiError('http_error', 409, { code: 'source_conflict', retryable: true }),
    new Error('secret/body/provider detail must not be retained'),
  ]) {
    const record = observeRequestDeliveryReplyFailure(beginRequestDeliveryReplyAttempt(firstAttempt(), when), error, later);
    assert.equal(record.attempts[0].state, 'unknown');
    assert.equal(record.attempts[0].refusal, undefined);
    assert.equal(JSON.stringify(record).includes('secret/body'), false);
  }
  const local = observeRequestDeliveryReplyFailure(beginRequestDeliveryReplyAttempt(firstAttempt(), when),
    new PrimaryApiError('invalid_request'), later);
  assert.equal(local.attempts[0].refusal?.origin, 'local_validation');
  assert.throws(() => observeRequestDeliveryReplyFailure(firstAttempt(), new PrimaryApiError('invalid_request'), later), code('identity_mismatch'));
});

test('exact API question_unavailable refuses only the first dispatch; uncertain replay remains unresolved', () => {
  // Frozen API store750116ce joins canonical reply to its transaction. The route7a6a
  // returns this AgentQuestionError as409/retryable:false after that transaction rejects.
  const error = new PrimaryApiError('http_error', 409, { code: 'question_unavailable', retryable: false });
  const first = beginRequestDeliveryReplyAttempt(firstAttempt(), when);
  const refused = observeRequestDeliveryReplyFailure(first, error, later);
  assert.equal(refused.attempts[0].state, 'refused');
  assert.equal(refused.attempts[0].refusal?.code, 'question_unavailable');
  const next = appendRequestDeliveryReplyAttempt(refused, anotherReply(), questionResult(), latest);
  assert.deepEqual(next.attempts[0], refused.attempts[0]);
  assert.equal(next.attempts[1].state, 'prepared');
  const unknown = observeRequestDeliveryReplyFailure(first, new PrimaryApiError('request_timeout'), later);
  const retried = beginRequestDeliveryReplyAttempt(unknown, latest);
  const stillUnknown = observeRequestDeliveryReplyFailure(retried, error, latest);
  assert.equal(stillUnknown.attempts[0].state, 'unknown');
  assert.equal(stillUnknown.attempts[0].refusal, undefined);
  assert.throws(() => appendRequestDeliveryReplyAttempt(stillUnknown, anotherReply(), questionResult(), latest), code('identity_mismatch'));
});

test('frozen API q1 answer to q2 discovery uses a new association and captured source on the same original run', async () => {
  const secondQuestionId = '14141414-1414-4414-8414-141414141414';
  const secondAssociationId = '15151515-1515-4515-8515-151515151515';
  const q1Journal = preparedReply();
  const q1Bytes = JSON.stringify(q1Journal);
  const q1Dispatched = beginRequestDeliveryReplyAttempt(firstAttempt(), when);
  const q2Question = { ...savedQuestion(), id: secondQuestionId, question: 'Should I include Apple?',
    proposedAnswer: 'Include Apple.', yesMeaning: 'Include Apple.', noMeaning: 'Leave Apple out.' };
  const discovery: RequestDeliveryQuestionResult = { ...questionResult(), question: q2Question };
  const q2Association = { ...response(), association: { ...response().association, id: secondAssociationId,
    question: { id: secondQuestionId, revision: 1 as const }, createdAt: later } };
  const q2Read: RequestDeliveryQuestionResult = { ...discovery, deliveryAssociation: q2Association.association };
  const q2Source: PrimaryTurn = { ...turn(), sourceEventId: secondSourceId, itemId: 'new_call_question_2',
    text: ' No.\t', occurredAt: latest };
  const q2Input: PrimaryReplyInput = { source: { callSid, sourceEventId: secondSourceId, itemId: q2Source.itemId,
    revision: 1, receivedAt: latest }, role: 'user', text: q2Source.text,
    reply: { questionRevision: 1, clientReplyId: secondReplyId, answer: 'no', text: null } };
  const q2Answered: RequestDeliveryQuestionResult = { requestId: aliasId, bindingId: executionBinding,
    state: 'queued', replayed: false, question: { ...q2Question, state: 'answered', runState: 'queued',
      reply: { ...q2Input.reply, interpretedAnswer: q2Question.noMeaning, receivedAt: latest } },
    deliveryAssociation: q2Association.association };
  const q1ReadWhileQ2Waits: RequestDeliveryQuestionResult = { ...answered(), state: 'waiting_for_reply',
    question: { ...answered().question, runState: 'waiting_for_reply' } };
  const { client, requests } = harness([json(answered()), json(discovery), json(q1ReadWhileQ2Waits),
    json(q2Association), json(q2Read), json(q2Answered), json({ ...q2Answered, replayed: true })]);
  const q1Accepted = observeRequestDeliveryReplyResult(q1Dispatched, await client.reply(q1Journal), later);
  assert.equal(q1Accepted.attempts[0].state, 'accepted');
  const found = await client.result(associated());
  assert.equal(found.question!.id, secondQuestionId);
  assert.equal(found.deliveryAssociation.question.id, questionId, 'discovery does not rewrite the old association');
  const oldAnswer = await client.question(associated());
  assert.equal(oldAnswer.question.id, questionId); assert.equal(oldAnswer.question.state, 'answered');
  assert.equal(oldAnswer.state, 'waiting_for_reply', 'run state may describe the later question');
  const q2Fields = input(); q2Fields.target.question.id = secondQuestionId; q2Fields.request.questionId = secondQuestionId;
  let q2Journal = createRequestDeliveryJournal(q2Fields);
  q2Journal = retainRequestDeliveryAssociation(q2Journal, await client.associate(q2Journal));
  q2Journal = retainRequestDeliveryCallerSource(q2Journal, q2Source, { ...capture(), sourceEventId: secondSourceId,
    recordId: secondSourceId, revisionRecordId: secondReplyId, bodyDigest: turnBodyDigest(q2Source) });
  q2Journal = retainRequestDeliveryReply(q2Journal, q2Input);
  const exact = await client.question(q2Journal);
  const q2Base = retainRequestDeliveryAssociation(createRequestDeliveryJournal(q2Fields), q2Association);
  const q2Prepared = appendRequestDeliveryReplyAttempt(createRequestDeliveryQuestionRecord(q2Base), q2Journal, exact, latest);
  const q2Dispatched = beginRequestDeliveryReplyAttempt(q2Prepared, latest);
  const q2Accepted = observeRequestDeliveryReplyResult(q2Dispatched, await client.reply(q2Journal), latest);
  assert.equal(q2Accepted.attempts[0].state, 'accepted');
  assert.deepEqual(await client.reply(parseRequestDeliveryJournal(JSON.parse(JSON.stringify(q2Journal)))),
    { ...q2Answered, replayed: true });
  assert.equal(JSON.stringify(q1Journal), q1Bytes);
  assert.deepEqual(q2Journal.target.execution, q1Journal.target.execution);
  assert.equal(q2Journal.association!.execution.requestId, requestId);
  assert.equal(q2Journal.request.originalRequestId, aliasId);
  assert.equal(q2Journal.callerSource!.bindingId, deliveryBinding);
  assert.equal(q2Journal.callerSource!.callSid, callSid);
  assert.notEqual(q2Journal.callerSource!.sourceEventId, q1Journal.callerSource!.sourceEventId);
  assert.equal(q2Journal.callerSource!.bodyDigest, turnBodyDigest(q2Source));
  assert.notEqual(q2Journal.callerSource!.bodyDigest, turnBodyDigest({ ...q2Source, text: q2Source.text.trim() }));
  assert.deepEqual(JSON.parse(String(requests[3].init.body)), q2Fields.request);
  assert.equal(new Headers(requests[3].init.headers).get(PHONE_REQUEST_DELIVERY_HEADER), null);
  for (const index of [4, 5, 6]) {
    assert.ok(requests[index].url.includes(`/requests/${aliasId}/questions/${secondQuestionId}`));
    assert.equal(new Headers(requests[index].init.headers).get(PHONE_REQUEST_DELIVERY_HEADER), secondAssociationId);
    assert.equal(new Headers(requests[index].init.headers).get('X-Dexter-Binding-Id'), deliveryBinding);
  }
  assert.equal(requests[5].init.body, requests[6].init.body, 'acknowledgement recovery sends the exact saved answer');
});

test('another committed answer and expired or cancelled questions end this attempt without allowing a replacement', () => {
  for (const state of ['answered', 'expired', 'cancelled'] as const) {
    const outcome: RequestDeliveryQuestionResult = state === 'answered'
      ? { ...answered(), question: { ...answered().question, reply: { ...answered().question.reply, clientReplyId: secondReplyId } } }
      : { ...questionResult(), question: { ...savedQuestion(), state } };
    const record = observeRequestDeliveryReplyResult(beginRequestDeliveryReplyAttempt(firstAttempt(), when), outcome, later);
    assert.equal(record.attempts[0].state, 'refused');
    assert.equal(record.attempts[0].refusal, undefined);
    assert.deepEqual(record.attempts[0].terminalQuestion?.value, outcome);
    assert.throws(() => appendRequestDeliveryReplyAttempt(record, anotherReply(), questionResult(), latest), code('identity_mismatch'));
    assert.throws(() => beginRequestDeliveryReplyAttempt(record, latest), code('identity_mismatch'));
  }
});

test('temporarily unavailable question evidence remains unresolved and can later recover the original answer', () => {
  const unavailable: RequestDeliveryQuestionResult = { ...questionResult(), question: { ...savedQuestion(), state: 'unavailable' } };
  const record = observeRequestDeliveryReplyResult(beginRequestDeliveryReplyAttempt(firstAttempt(), when), unavailable, later);
  assert.equal(record.attempts[0].state, 'unknown');
  assert.equal(record.attempts[0].terminalQuestion, undefined);
  const accepted = observeRequestDeliveryReplyResult(record, answered(), latest);
  assert.equal(accepted.attempts[0].state, 'accepted');
  const foreign = answered(); foreign.question.id = foreignId;
  assert.throws(() => observeRequestDeliveryReplyResult(record, foreign, latest), code('identity_mismatch'));
});

test('saved comparison refuses removed or rewritten attempt history and combined refusal-plus-new-answer updates', () => {
  const refused = refusedAttempt();
  const record = appendRequestDeliveryReplyAttempt(refused, anotherReply(), questionResult(), latest);
  const removed = { ...record, attempts: record.attempts.slice(0, 1) };
  assert.throws(() => assertRequestDeliveryQuestionRecordUpdate(record, removed), code('identity_mismatch'));
  const changed = structuredClone(record); changed.attempts[0].refusal!.observedAt = when;
  assert.throws(() => assertRequestDeliveryQuestionRecordUpdate(record, changed), code('identity_mismatch'));
  const reordered = { ...record, attempts: [...record.attempts].reverse() };
  assert.throws(() => parseRequestDeliveryQuestionRecord(reordered), code('identity_mismatch'));
  const inFlight = beginRequestDeliveryReplyAttempt(firstAttempt(), when);
  assert.throws(() => assertRequestDeliveryQuestionRecordUpdate(inFlight, record), code('identity_mismatch'));
  const unmarked = firstAttempt(); unmarked.attempts[0].state = 'accepted';
  unmarked.attempts[0].accepted = { value: answered(), observedAt: later };
  assert.throws(() => parseRequestDeliveryQuestionRecord(unmarked), code('identity_mismatch'));
  const replay = beginRequestDeliveryReplyAttempt(inFlight, later); replay.attempts[0].recovery = false;
  assert.throws(() => parseRequestDeliveryQuestionRecord(replay), code('identity_mismatch'));
});


test('local stop settles a never-dispatched reply without inventing an HTTP attempt or losing captured source', () => {
  const prepared = firstAttempt(); const original = JSON.stringify(prepared.attempts[0].journal);
  for (const reason of ['call_ended', 'presentation_invalidated', 'question_not_pending', 'question_expired'] as const) {
    const stopped = refuseUndispatchedRequestDeliveryReply(prepared, reason, later);
    assert.equal(stopped.attempts[0].state, 'refused'); assert.equal(stopped.attempts[0].dispatches, 0);
    assert.equal(stopped.attempts[0].recovery, false); assert.equal(stopped.attempts[0].lastDispatchAt, undefined);
    assert.equal(stopped.attempts[0].refusal, undefined); assert.equal(JSON.stringify(stopped.attempts[0].journal), original);
    assert.deepEqual(stopped.attempts[0].localRefusal, { kind: 'never_dispatched', code: reason, observedAt: later });
    assert.deepEqual(parseRequestDeliveryQuestionRecord(JSON.parse(JSON.stringify(stopped))), stopped);
    assert.deepEqual(refuseUndispatchedRequestDeliveryReply(stopped, reason, later), stopped);
    assert.throws(() => beginRequestDeliveryReplyAttempt(stopped, latest), code('identity_mismatch'));
  }
});

test('never-dispatched refusal cannot settle an in-flight or unknown POST, or rewrite an earlier refusal', () => {
  const sent = beginRequestDeliveryReplyAttempt(firstAttempt(), when);
  const uncertain = observeRequestDeliveryReplyFailure(sent, new PrimaryApiError('request_timeout'), later);
  for (const record of [sent, uncertain, refusedAttempt(), observeRequestDeliveryReplyResult(sent, answered(), later)]) {
    assert.throws(() => refuseUndispatchedRequestDeliveryReply(record, 'call_ended', latest), code('identity_mismatch'));
    const forged = structuredClone(record); forged.attempts[0].localRefusal = { kind: 'never_dispatched', code: 'call_ended', observedAt: latest };
    assert.throws(() => parseRequestDeliveryQuestionRecord(forged), code('identity_mismatch'));
  }
  const stopped = refuseUndispatchedRequestDeliveryReply(firstAttempt(), 'presentation_invalidated', later);
  assert.throws(() => refuseUndispatchedRequestDeliveryReply(stopped, 'call_ended', later), code('identity_mismatch'));
  assert.throws(() => refuseUndispatchedRequestDeliveryReply(stopped, 'presentation_invalidated', latest), code('identity_mismatch'));
  const forged = structuredClone(stopped); forged.attempts[0].dispatches = 1; forged.attempts[0].lastDispatchAt = when;
  assert.throws(() => parseRequestDeliveryQuestionRecord(forged), code('identity_mismatch'));
});

test('a new explicitly captured answer may follow local refusal while preserving the old source and refusal', () => {
  const stopped = refuseUndispatchedRequestDeliveryReply(firstAttempt(), 'presentation_invalidated', later);
  const next = appendRequestDeliveryReplyAttempt(stopped, anotherReply(), questionResult(), latest);
  assert.deepEqual(next.attempts[0], stopped.attempts[0]); assert.equal(next.attempts[1].dispatches, 0);
  assert.throws(() => appendRequestDeliveryReplyAttempt(stopped, preparedReply(), questionResult(), latest), code('identity_mismatch'));
  assert.throws(() => appendRequestDeliveryReplyAttempt(stopped, anotherReply(), questionResult(), when), code('identity_mismatch'));
  const changed = structuredClone(next); changed.attempts[0].localRefusal!.code = 'call_ended';
  assert.throws(() => assertRequestDeliveryQuestionRecordUpdate(next, changed), code('identity_mismatch'));
});
