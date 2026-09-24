import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import { ZodError } from 'zod';
import { createPrimaryCallerIntentClient } from '../src/broker/callerIntentClient.js';
import { callerIntentInputDigest, callerIntentInputSchema, callerIntentResponseSchema, validateCallerIntentResponse,
  type CallerIntentInput, type CallerIntentResponse } from '../src/broker/callerIntentContract.js';
import { PrimaryApiError } from '../src/broker/primaryApi.js';

const id = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const bindingId = id(1), operationId = id(2);
const issue = (message: string, path?: Array<string | number>) => (error: unknown) =>
  error instanceof ZodError && error.issues.some(value => value.code === 'custom' && value.message === message
    && (path === undefined || JSON.stringify(value.path) === JSON.stringify(path)));
const input = (): CallerIntentInput => ({ v: 'phone.caller-intent.v1',
  source: { callSid: `CA${'a'.repeat(32)}`, sourceEventId: id(3), itemId: 'live_turn_1',
    revision: 1, receivedAt: '2026-09-24T12:00:00.000Z', role: 'user', text: ' Research Apple.\t' },
  generationId: id(4), sourceSessionId: 'voice_session', brokerInputDigest: 'a'.repeat(64),
  presentationDigest: 'b'.repeat(64), callDeadlineAt: '2026-09-24T12:05:00.000Z',
  version: { ownerPrincipalId: id(5), primaryDexterId: id(6), threadId: id(7),
    contextEpoch: '10', threadEventSequence: '22', digest: 'c'.repeat(64) }, questions: [], results: [] });
function response(value = input()): CallerIntentResponse {
  return { v: value.v, operationId, bindingId, inputDigest: callerIntentInputDigest(operationId, bindingId, value),
    brokerInputDigest: value.brokerInputDigest, presentationDigest: value.presentationDigest,
    generationId: value.generationId, sourceSessionId: value.sourceSessionId, callDeadlineAt: value.callDeadlineAt,
    questions: value.questions, results: value.results, version: value.version,
    reservedAt: '2026-09-24T12:00:01.000Z', validUntil: '2026-09-24T12:00:11.000Z', state: 'pending' };
}

test('pending POST and exact GET preserve operation, raw text, deadline and protocol headers', async () => {
  const observed: Array<{ url: string; method: string; body?: unknown }> = [];
  const client = createPrimaryCallerIntentClient({ apiBaseUrl: 'https://api.example.test',
    phoneServiceSecret: 'x'.repeat(40), fetch: async (url, init) => {
      const headers = new Headers(init?.headers);
      assert.equal(headers.get('X-Dexter-Binding-Id'), bindingId);
      assert.equal(headers.get('X-Dexter-Phone-Context-Version'), 'memory-eligible-v1');
      assert.equal(headers.get('Idempotency-Key'), init?.method === 'POST' ? operationId : null);
      const body = init?.body === undefined ? undefined : JSON.parse(String(init.body));
      observed.push({ url: String(url), method: init?.method ?? '', body });
      return Response.json(response(), { status: init?.method === 'POST' ? 202 : 200 });
    } });
  assert.equal((await client.submit(bindingId, operationId, input())).state, 'pending');
  assert.equal((await client.recover(bindingId, operationId, input())).state, 'pending');
  assert.equal(observed.length, 2);
  assert.deepEqual(observed[0]?.body, input());
  assert.ok(observed[1]?.url.endsWith(`/calls/${bindingId}/caller-intents/${operationId}`));
  assert.equal(observed[1]?.body, undefined);
});

test('unknown POST does not retry and later recovery only uses the saved exact GET', async () => {
  const methods: string[] = [];
  const client = createPrimaryCallerIntentClient({ apiBaseUrl: 'https://api.example.test',
    phoneServiceSecret: 'x'.repeat(40), fetch: async (_url, init) => {
      methods.push(init?.method ?? '');
      if (init?.method === 'POST') throw new Error('connection closed after possible acceptance');
      return Response.json({ ...response(), state: 'resolved', decision: { kind: 'conversation' } });
    } });
  await assert.rejects(client.submit(bindingId, operationId, input()), (error: unknown) =>
    error instanceof PrimaryApiError && error.code === 'request_failed');
  assert.deepEqual(methods, ['POST']);
  assert.equal((await client.recover(bindingId, operationId, input())).state, 'resolved');
  assert.deepEqual(methods, ['POST', 'GET']);
});

test('transport refuses wrong HTTP state, changed correlation, extra decision and arbitrary answer', async () => {
  for (const body of [response(), { ...response(), state: 'pending', decision: null },
    { ...response(), state: 'resolved', decision: { kind: 'conversation' }, brokerInputDigest: 'e'.repeat(64) },
    { ...response(), state: 'resolved', decision: { kind: 'answer', target: { kind: 'ordinary',
      execution: { bindingId, requestId: id(8), runId: id(9) }, question: { id: id(10), revision: 1 }, presentationId: 'none' } } }]) {
    let calls = 0;
    const client = createPrimaryCallerIntentClient({ apiBaseUrl: 'https://api.example.test',
      phoneServiceSecret: 'x'.repeat(40), fetch: async () => { calls++; return Response.json(body); } });
    await assert.rejects(client.submit(bindingId, operationId, input()), (error: unknown) =>
      error instanceof PrimaryApiError && error.code === 'invalid_response');
    assert.equal(calls, 1);
  }
});

test('fixed receipt validity and exact source digest cannot be renewed or trimmed on replay', () => {
  const original = input(), saved = response(original);
  assert.deepEqual(validateCallerIntentResponse(bindingId, operationId, original, saved), saved);
  assert.throws(() => validateCallerIntentResponse(bindingId, operationId, original,
    { ...saved, validUntil: '2026-09-24T12:00:12.000Z' }),
    issue('Decision deadline differs from original budget', ['validUntil']));
  assert.notEqual(callerIntentInputDigest(operationId, bindingId, original),
    callerIntentInputDigest(operationId, bindingId, { ...original, source: { ...original.source, text: original.source.text.trim() } }));
});

test('an already stopped call cannot start an intent HTTP request', async () => {
  const stopped = new AbortController(); stopped.abort();
  let requests = 0;
  const client = createPrimaryCallerIntentClient({ apiBaseUrl: 'https://api.example.test', phoneServiceSecret: 'x'.repeat(40),
    fetch: async () => { requests++; throw new Error('forbidden'); } });
  await assert.rejects(client.submit(bindingId, operationId, input(), stopped.signal), PrimaryApiError);
  assert.equal(requests, 0);
});

// Exact bytes copied from the shared API literal fixture (SHA-256 100dd00f...).
// The expected digest is external to the Broker implementation under test.
const literal: { operationId: string; bindingId: string; input: CallerIntentInput; inputDigest: string } =
  JSON.parse(readFileSync(new URL('./fixtures/caller-intent-digest.json', import.meta.url), 'utf8'));
const literalInput = () => structuredClone(literal.input);
const literalDigest = (value: CallerIntentInput) => callerIntentInputDigest(literal.operationId, literal.bindingId, value);
function literalPending(value = literalInput()): CallerIntentResponse {
  return { v: value.v, operationId: literal.operationId, bindingId: literal.bindingId, inputDigest: literalDigest(value),
    brokerInputDigest: value.brokerInputDigest, presentationDigest: value.presentationDigest,
    generationId: value.generationId, sourceSessionId: value.sourceSessionId, callDeadlineAt: value.callDeadlineAt,
    questions: value.questions, results: value.results, version: value.version,
    reservedAt: '2026-09-24T17:00:01.000Z', validUntil: '2026-09-24T17:00:11.000Z', state: 'pending' };
}
const validateLiteral = (body: unknown, value = literalInput()) =>
  validateCallerIntentResponse(literal.bindingId, literal.operationId, value, body);

test('Broker exported digest matches the shared literal and preserves its exact raw text', () => {
  const value = literalInput();
  assert.equal(literal.inputDigest, '1df334e42080de13b1e5a16470e8a9d1c0e30cf211577e8b1bb6ae0d2daa206c');
  assert.deepEqual(callerIntentInputSchema.parse(value), value);
  assert.equal(value.source.text, '  Keep caf\u00e9 and cafe\u0301 separate.\nThen answer.  ');
  assert.equal(literalDigest(value), literal.inputDigest);
  assert.deepEqual(validateLiteral(literalPending()), literalPending());
});

test('raw Unicode, whitespace, timestamp spelling and candidate order each affect the Broker digest', () => {
  const mutations: Array<(value: CallerIntentInput) => void> = [
    value => { value.source.text = value.source.text.normalize('NFC'); },
    value => { value.source.text = value.source.text.trim(); },
    value => { value.source.text = value.source.text.replace('\n', ' '); },
    value => { value.source.receivedAt = '2026-09-24T17:00:00+00:00'; },
    value => { value.sourceSessionId = 'session  opaque 1'; },
    value => { value.questions.reverse(); },
    value => { value.results.reverse(); },
  ];
  for (const mutate of mutations) {
    const value = literalInput(); mutate(value);
    assert.deepEqual(callerIntentInputSchema.parse(value), value, 'valid source bytes must not be normalized');
    assert.notEqual(literalDigest(value), literal.inputDigest);
  }
});

for (const key of ['questions', 'results'] as const) {
  test(`duplicate ${key} source identity is refused even with different executions and presentations`, () => {
    const value = literalInput();
    if (key === 'questions') value.questions[1]!.question.id = value.questions[0]!.question.id;
    else value.results[1]!.assistantMessageId = value.results[0]!.assistantMessageId;
    assert.notDeepEqual(value[key][0]!.execution, value[key][1]!.execution);
    assert.notEqual(value[key][0]!.presentationId, value[key][1]!.presentationId);
    const refusal = issue('Duplicate candidate identity', [key, 1]);
    assert.throws(() => callerIntentInputSchema.parse(value), refusal);
    assert.throws(() => literalDigest(value), refusal);
    assert.throws(() => callerIntentResponseSchema.parse({ ...literalPending(), [key]: value[key] }), refusal);
  });
}

for (const conflicting of ['request', 'run', 'binding', 'association'] as const) {
  test(`cross-candidate ${conflicting} disagreement is refused by input and response schemas`, () => {
    const value = literalInput(), first = value.questions[0]!;
    if (conflicting === 'request') value.results[0]!.execution.requestId = first.execution.requestId;
    if (conflicting === 'run') value.results[0]!.execution.runId = first.execution.runId;
    if (conflicting === 'binding') value.results[0]!.execution = { ...first.execution, bindingId: id(99) };
    if (conflicting === 'association') {
      const selectedQuestion = value.questions[1]!, selectedResult = value.results[1]!;
      assert.ok(selectedQuestion.kind === 'selected' && selectedResult.kind === 'selected');
      selectedResult.deliveryAssociationId = selectedQuestion.deliveryAssociationId;
    }
    const refusal = issue(conflicting === 'association'
      ? 'Delivery association execution identities disagree' : 'Candidate execution identities disagree');
    assert.throws(() => callerIntentInputSchema.parse(value), refusal);
    assert.throws(() => literalDigest(value), refusal);
    assert.throws(() => callerIntentResponseSchema.parse({ ...literalPending(), questions: value.questions, results: value.results }), refusal);
  });
}

for (const key of ['questions', 'results'] as const) {
  test(`ordinary ${key} require the current call binding while selected candidates retain their original binding`, () => {
    const value = literalInput();
    assert.notEqual(value[key][1]!.execution.bindingId, literal.bindingId);
    assert.equal(literalDigest(value), literal.inputDigest);
    value[key][0]!.execution.bindingId = id(99);
    assert.deepEqual(callerIntentInputSchema.parse(value), value, 'scope is checked against the separately supplied binding');
    assert.throws(() => literalDigest(value), /caller_intent_candidate_binding_mismatch/);
    assert.throws(() => callerIntentResponseSchema.parse({ ...literalPending(), [key]: value[key] }),
      issue('Ordinary candidate belongs to another call', ['bindingId']));
  });
}

test('receipt deadline is the original call deadline or ten-second budget, including expired historical receipts', () => {
  const pending = literalPending();
  assert.deepEqual(validateLiteral(pending), pending);
  assert.throws(() => validateLiteral({ ...pending, validUntil: '2026-09-24T17:00:11.001Z' }),
    issue('Decision deadline differs from original budget', ['validUntil']));
  const value = literalInput(); value.callDeadlineAt = '2026-09-24T17:00:05.000Z';
  const shortened = { ...literalPending(value), validUntil: value.callDeadlineAt };
  assert.deepEqual(validateLiteral(shortened, value), shortened);
  assert.throws(() => validateLiteral({ ...shortened, validUntil: pending.validUntil }, value),
    issue('Decision deadline differs from original budget', ['validUntil']));
  const alreadyExpired = { ...shortened, reservedAt: '2026-09-24T17:00:06.000Z',
    state: 'resolved', decision: { kind: 'clarify', reason: 'deadline_exceeded' } };
  assert.deepEqual(validateLiteral(alreadyExpired, value), alreadyExpired, 'historical receipt validation must not renew its deadline');
});

test('answers require the complete offered execution, question, presentation and selected association', () => {
  const value = literalInput();
  for (const target of value.questions) {
    const body = { ...literalPending(), state: 'resolved', decision: { kind: 'answer', target } };
    assert.deepEqual(validateLiteral(body), body);
  }
  const selected = value.questions[1]!; assert.equal(selected.kind, 'selected');
  for (const target of [{ ...selected, presentationId: 'changed' }, { ...selected, deliveryAssociationId: id(99) },
    { ...selected, execution: { ...selected.execution, bindingId: id(99) } },
    { ...selected, execution: { ...selected.execution, requestId: id(99) } },
    { ...selected, execution: { ...selected.execution, runId: id(99) } },
    { ...selected, question: { id: id(99), revision: 1 } }]) {
    assert.throws(() => validateLiteral({ ...literalPending(), state: 'resolved', decision: { kind: 'answer', target } }),
      issue('Answer target was not supplied', ['decision', 'target']));
  }
});

test('task references select only an offered ordinary result or exact associated result', () => {
  const value = literalInput(), ordinary = value.results[0]!, selected = value.results[1]!;
  assert.ok(ordinary.kind === 'ordinary' && selected.kind === 'selected');
  const ordinaryReference = { kind: 'ordinary', relatedPrimaryRequestId: ordinary.execution.requestId,
    referenceOrigin: 'broker_status_publication' };
  const associatedReference = { kind: 'associated', associatedResultReference: { v: 'phone.associated-result-reference.v1',
    deliveryAssociationId: selected.deliveryAssociationId, originalRequestId: selected.execution.requestId,
    assistantMessageId: selected.assistantMessageId } };
  for (const reference of [ordinaryReference, associatedReference]) {
    const body = { ...literalPending(), state: 'resolved', decision: { kind: 'task', reference } };
    assert.deepEqual(validateLiteral(body), body);
  }
  const associated = associatedReference.associatedResultReference;
  for (const reference of [{ ...ordinaryReference, relatedPrimaryRequestId: selected.execution.requestId },
    { ...associatedReference, associatedResultReference: { ...associated, deliveryAssociationId: id(99) } },
    { ...associatedReference, associatedResultReference: { ...associated, originalRequestId: id(99) } },
    { ...associatedReference, associatedResultReference: { ...associated, assistantMessageId: id(99) } }]) {
    assert.throws(() => validateLiteral({ ...literalPending(), state: 'resolved', decision: { kind: 'task', reference } }),
      issue('Task reference was not supplied', ['decision', 'reference']));
  }
});
