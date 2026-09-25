import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import test from 'node:test';
import {
  createEligibleContextClient, eligiblePhoneCallSchema, historyVersionSchema,
  eligibleDeliveryResponseSchema, parseEligiblePhoneCall, sameHistoryVersion,
  PHONE_ELIGIBLE_CONTEXT_HEADER, PHONE_ELIGIBLE_CONTEXT_VERSION,
  ELIGIBLE_CONTEXT_MAX_RESPONSE_BYTES,
  type EligibleContextBinding, type EligibleDeliveryRequest, type HistoryVersion,
} from '../src/broker/eligibleContext.js';
import { createPrimaryApiClient, PrimaryApiError, type PrimaryApiOptions } from '../src/broker/primaryApi.js';
import { createRequestDeliveryJournal, retainRequestDeliveryAssociation, PHONE_REQUEST_DELIVERY_HEADER,
  PHONE_REQUEST_DELIVERY_VERSION } from '../src/broker/requestDelivery.js';

const bindingId = '11111111-1111-4111-8111-111111111111';
const requestId = '22222222-2222-4222-8222-222222222222';
const primaryDexterId = '33333333-3333-4333-8333-333333333333';
const threadId = '44444444-4444-4444-8444-444444444444';
const ownerPrincipalId = '55555555-5555-4555-8555-555555555555';
const messageId = '66666666-6666-4666-8666-666666666666';
const eventId = '77777777-7777-4777-8777-777777777777';
const runId = '88888888-8888-4888-8888-888888888888';
const questionId = '99999999-9999-4999-8999-999999999999';
const sourceEventId = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
const foreignId = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';
const callSid = `CA${'1'.repeat(32)}`;
const when = '2026-09-24T12:00:00.000Z';
const secret = 'synthetic-eligible-context-service-secret';
const binding: EligibleContextBinding = { bindingId, callSid, ownerPrincipalId, primaryDexterId, threadId };
const version: HistoryVersion = { ownerPrincipalId, primaryDexterId, threadId,
  contextEpoch: '7', threadEventSequence: '9007199254740993', digest: 'a'.repeat(64) };
const provenance = { messageCreatedAt: when, eventId, eventOccurredAt: when, runId };
const canonicalSource = { sourceEventId, revision: 2, occurredAt: when, storedProjectionKind: 'derived' as const };
const assignmentRef = { assignmentId: foreignId, revision: 1 as const, digest: 'c'.repeat(64) };

function call() {
  return {
    bindingId, callSid, state: 'open' as const,
    context: { primaryDexterId, threadId, displayName: 'Dexter', messages: [{
      id: messageId, role: 'assistant' as const, text: 'Your saved result.',
      eventSequence: '9007199254740992', provenance: { ...provenance }, canonicalSource: { ...canonicalSource },
    }], history: { version: { ...version }, pageDigest: 'b'.repeat(64), localContext: 'Saved preference: brief answers.',
      nextBeforeSequence: null as string | null, hasMore: false, omittedForBudget: false } },
  };
}
function assistantDelivery() {
  return { bindingId, callSid, requestId, version: { ...version }, delivery: {
    state: 'ready' as const, source: { kind: 'assistant' as const, messageId, eventSequence: '9007199254740992',
      provenance: { ...provenance }, canonicalSource: { ...canonicalSource } }, text: 'Your eligible result.',
  } };
}
function questionDelivery() {
  return { bindingId, callSid, requestId, version: { ...version }, delivery: {
    state: 'ready' as const, source: { kind: 'question' as const, messageId, eventSequence: '9007199254740992',
      provenance: { ...provenance }, canonicalSource: { ...canonicalSource } }, text: 'Which stock should I research?',
    question: { id: questionId, revision: 1 as const, expiresAt: '2030-01-01T00:00:00.000Z',
      runState: 'waiting_for_reply' as const, runId, threadId,
      agent: { id: primaryDexterId, name: 'Dexter' }, question: 'Which stock should I research?',
      context: 'Your research request needs a stock.', proposedAnswer: 'NVIDIA',
      yesMeaning: 'Research NVIDIA.', noMeaning: 'Choose a different stock.',
      state: 'pending' as const, createdAt: when, reply: null },
  } };
}
const assistantInput: EligibleDeliveryRequest = { version, source: { kind: 'assistant', messageId } };
const questionInput: EligibleDeliveryRequest = { version, source: { kind: 'question', questionId, questionRevision: 1 } };
const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status,
  headers: { 'Content-Type': 'application/json' } });
const errorCode = (code: string) => (error: unknown) => error instanceof PrimaryApiError && error.code === code;
function harness(responses: Response[]) {
  const requests: Array<{ url: string; init: RequestInit }> = [];
  const options: PrimaryApiOptions = {
    apiBaseUrl: 'https://api.example.test', phoneServiceSecret: secret,
    fetch: async (input, init) => {
      requests.push({ url: String(input), init: init ?? {} });
      const response = responses.shift();
      assert.ok(response, 'unexpected extra HTTP request');
      return response;
    },
  };
  return { client: createEligibleContextClient(options), requests };
}

test('eligible fetch preserves complete version, attribution, canonical revision, local context and page metadata', async () => {
  const body = call();
  body.context.history.hasMore = true;
  body.context.history.nextBeforeSequence = '9007199254740991';
  body.context.history.omittedForBudget = true;
  const { client, requests } = harness([json(body)]);
  assert.deepEqual(await client.fetch(binding), body);
  assert.equal(requests[0].url, `https://api.example.test/internal/primary-dexter/channels/phone/calls/${bindingId}/context`);
  assert.equal(requests[0].init.method, 'GET');
  assert.equal(requests[0].init.body, undefined);
  const headers = new Headers(requests[0].init.headers);
  assert.equal(headers.get('X-Dexter-Binding-Id'), bindingId);
  assert.equal(headers.get(PHONE_ELIGIBLE_CONTEXT_HEADER), PHONE_ELIGIBLE_CONTEXT_VERSION);
  assert.equal(headers.get('Authorization'), `Bearer ${createHmac('sha256', secret)
    .update('dexter-broker/primary-ingress/v1').digest('hex')}`);
});

test('empty eligible history still requires an authenticated version and never falls back to raw old context', async () => {
  const empty = call();
  empty.context.messages = [];
  empty.context.history.localContext = '';
  empty.context.history.version.threadEventSequence = '0';
  assert.deepEqual(await harness([json(empty)]).client.fetch(binding), empty);
  const { history: _history, ...oldContext } = empty.context;
  const { client, requests } = harness([json({ ...empty, context: oldContext })]);
  await assert.rejects(client.fetch(binding), errorCode('invalid_response'));
  assert.equal(requests.length, 1);
});

test('strict parsing rejects unknown, missing, reconstructed or malformed provenance fields', () => {
  const original = call();
  for (const message of [
    { ...original.context.messages[0], provenance: undefined },
    { ...original.context.messages[0], provenance: { ...provenance, unknown: true } },
    { ...original.context.messages[0], provenance: { ...provenance, eventId: 'not-an-id' } },
    { ...original.context.messages[0], canonicalSource: { ...canonicalSource, revision: 0 } },
    { ...original.context.messages[0], canonicalSource: { ...canonicalSource, revision: 2147483648 } },
    { ...original.context.messages[0], canonicalSource: { ...canonicalSource, storedProjectionKind: 'inferred' } },
    { ...original.context.messages[0], canonicalSource: { ...canonicalSource, messageId } },
    { ...original.context.messages[0], afterAttemptId: requestId },
  ]) {
    assert.equal(eligiblePhoneCallSchema.safeParse({ ...original, context: { ...original.context, messages: [message] } }).success, false);
  }
  assert.equal(eligiblePhoneCallSchema.safeParse({ ...original, rawMessages: [] }).success, false);
  const withoutCanonical = call();
  const { canonicalSource: _canonical, ...ordinary } = withoutCanonical.context.messages[0];
  const result = eligiblePhoneCallSchema.parse({ ...withoutCanonical, context: { ...withoutCanonical.context, messages: [ordinary] } });
  assert.equal('canonicalSource' in result.context.messages[0], false, 'ordinary message must not acquire a synthetic canonical alias');
});

test('the 24000 byte context budget includes local assertions and measures UTF-8 without truncating', () => {
  const exact = call();
  exact.context.messages[0].text = '🙂'.repeat(5999);
  exact.context.history.localContext = '🙂';
  assert.deepEqual(eligiblePhoneCallSchema.parse(exact), exact);
  const oversized = structuredClone(exact);
  oversized.context.history.localContext += 'x';
  assert.equal(eligiblePhoneCallSchema.safeParse(oversized).success, false);
  assert.equal(oversized.context.messages[0].text, exact.context.messages[0].text);
  const tooMany = call();
  tooMany.context.messages = Array.from({ length: 21 }, () => structuredClone(tooMany.context.messages[0]));
  assert.equal(eligiblePhoneCallSchema.safeParse(tooMany).success, false);
});

test('sequence counters preserve values beyond Number precision and reject noncanonical or out-of-range forms safely', () => {
  assert.equal(historyVersionSchema.parse({ ...version, threadEventSequence: '9223372036854775807' }).threadEventSequence, '9223372036854775807');
  for (const counter of ['01', '-1', '+1', '1.0', '1e3', '9223372036854775808', '', 'garbage', ' 1', '1 ']) {
    assert.equal(historyVersionSchema.safeParse({ ...version, contextEpoch: counter }).success, false);
    const invalidMessage = call();
    invalidMessage.context.messages[0].eventSequence = counter;
    assert.equal(eligiblePhoneCallSchema.safeParse(invalidMessage).success, false);
    const invalidHead = call();
    invalidHead.context.history.version.threadEventSequence = counter;
    assert.equal(eligiblePhoneCallSchema.safeParse(invalidHead).success, false);
    const invalidDelivery = assistantDelivery();
    invalidDelivery.delivery.source.eventSequence = counter;
    assert.equal(eligibleDeliveryResponseSchema.safeParse(invalidDelivery).success, false);
  }
  for (const invalid of [{ ...version, contextEpoch: 7 }, { ...version, digest: 'A'.repeat(64) },
    { ...version, ownerPrincipalId: sourceEventId.toUpperCase() }, { ...version, sourceSequence: '3' }]) {
    assert.equal(historyVersionSchema.safeParse(invalid).success, false);
  }
});

test('projection identity and event ordering cannot drift beyond the declared thread version', () => {
  for (const modify of [
    (body: ReturnType<typeof call>) => { body.context.history.version.primaryDexterId = foreignId; },
    (body: ReturnType<typeof call>) => { body.context.history.version.threadId = foreignId; },
    (body: ReturnType<typeof call>) => { body.context.messages[0].eventSequence = '9007199254740994'; },
    (body: ReturnType<typeof call>) => { body.context.messages.push(structuredClone(body.context.messages[0])); },
    (body: ReturnType<typeof call>) => { body.context.history.nextBeforeSequence = '9223372036854775807'; },
  ]) {
    const body = call(); modify(body);
    assert.equal(eligiblePhoneCallSchema.safeParse(body).success, false);
  }
});

test('registration parser pins exact call, owner, Primary, thread and assignment without trimming returned text', () => {
  const body = { ...call(), callAssignmentRef: assignmentRef };
  body.context.messages[0].text = ' Preserved projected text.\n';
  assert.deepEqual(parseEligiblePhoneCall(body, { ...binding, callAssignmentRef: assignmentRef }), body);
  for (const expected of [
    { ...binding, bindingId: foreignId }, { ...binding, callSid: `CA${'2'.repeat(32)}` },
    { ...binding, ownerPrincipalId: foreignId }, { ...binding, primaryDexterId: foreignId },
    { ...binding, threadId: foreignId }, { ...binding, callAssignmentRef: { ...assignmentRef, digest: 'd'.repeat(64) } },
  ]) assert.throws(() => parseEligiblePhoneCall(body, expected), errorCode('identity_mismatch'));
  assert.throws(() => parseEligiblePhoneCall(call(), { ...binding, callAssignmentRef: assignmentRef }), errorCode('identity_mismatch'));
});

test('first authenticated owner adoption is pinned for later reads and checked again after concurrent HTTP completes', async () => {
  const { ownerPrincipalId: _owner, ...bootstrap } = binding;
  const changed = call(); changed.context.history.version.ownerPrincipalId = foreignId;
  const { client, requests } = harness([json(call()), json(changed)]);
  assert.deepEqual(await client.fetch(bootstrap), call());
  await assert.rejects(client.fetch(bootstrap), errorCode('identity_mismatch'));
  await assert.rejects(client.fetch({ ...bootstrap, ownerPrincipalId: foreignId }), errorCode('identity_mismatch'));
  assert.equal(requests.length, 2, 'known foreign identity refuses before HTTP');

  const pending: Array<(response: Response) => void> = [];
  const raced = createEligibleContextClient({ apiBaseUrl: 'https://api.example.test', phoneServiceSecret: secret,
    fetch: async () => new Promise<Response>(resolve => pending.push(resolve)) });
  const first = raced.fetch(bootstrap);
  const second = raced.fetch(bootstrap);
  const rejected = assert.rejects(second, errorCode('identity_mismatch'));
  pending[0](json(call()));
  await first;
  pending[1](json(changed));
  await rejected;
});

test('assignment reference omission or mutation after fetch cannot erase the pinned assignment', async () => {
  const assigned = { ...call(), callAssignmentRef: assignmentRef };
  const { client } = harness([json(assigned), json(call()), json({ ...assigned, callAssignmentRef: { ...assignmentRef, digest: 'd'.repeat(64) } })]);
  await client.fetch(binding);
  await assert.rejects(client.fetch(binding), errorCode('identity_mismatch'));
  await assert.rejects(client.fetch(binding), errorCode('identity_mismatch'));
});

test('assertion sends one strict version and checks exact echo for every identity and counter', async () => {
  const assertion = { bindingId, callSid, current: true, version };
  const { client, requests } = harness([json(assertion)]);
  assert.deepEqual(await client.assert(binding, version), assertion);
  assert.equal(requests[0].url, `https://api.example.test/internal/primary-dexter/channels/phone/calls/${bindingId}/context/assert`);
  assert.deepEqual(JSON.parse(String(requests[0].init.body)), { version });
  assert.equal(new Headers(requests[0].init.headers).get('Idempotency-Key'), null);
  assert.equal(new Headers(requests[0].init.headers).get('X-Dexter-Binding-Id'), bindingId);
  assert.equal(new Headers(requests[0].init.headers).get(PHONE_ELIGIBLE_CONTEXT_HEADER), PHONE_ELIGIBLE_CONTEXT_VERSION);
  for (const changed of [
    { ...assertion, bindingId: foreignId }, { ...assertion, callSid: `CA${'2'.repeat(32)}` },
    ...Object.entries({ ownerPrincipalId: foreignId, primaryDexterId: foreignId, threadId: foreignId,
      contextEpoch: '8', threadEventSequence: '9007199254740994', digest: 'd'.repeat(64) })
      .map(([key, value]) => ({ ...assertion, version: { ...version, [key]: value } })),
  ]) await assert.rejects(harness([json(changed)]).client.assert(binding, version), errorCode('identity_mismatch'));
  await assert.rejects(harness([json({ ...assertion, current: false })]).client.assert(binding, version), errorCode('invalid_response'));
  assert.equal(sameHistoryVersion(version, { ...version }), true);
  assert.equal(sameHistoryVersion(version, { ...version, contextEpoch: '8' }), false);
});

test('assistant delivery preserves exact source data and sends the requested version and target', async () => {
  const response = assistantDelivery();
  const { client, requests } = harness([json(response)]);
  assert.deepEqual(await client.delivery(binding, requestId, assistantInput), response);
  assert.equal(requests[0].url, `https://api.example.test/internal/primary-dexter/channels/phone/requests/${requestId}/delivery`);
  assert.deepEqual(JSON.parse(String(requests[0].init.body)), assistantInput);
  assert.equal(new Headers(requests[0].init.headers).get('X-Dexter-Binding-Id'), bindingId);
  assert.equal(new Headers(requests[0].init.headers).get('Idempotency-Key'), null);
  assert.equal(new Headers(requests[0].init.headers).get(PHONE_ELIGIBLE_CONTEXT_HEADER), PHONE_ELIGIBLE_CONTEXT_VERSION);
});

test('delivery rejects wrong request, call, target, exact version or projected source sequence', async () => {
  for (const modify of [
    (body: ReturnType<typeof assistantDelivery>) => { body.requestId = foreignId; },
    (body: ReturnType<typeof assistantDelivery>) => { body.bindingId = foreignId; },
    (body: ReturnType<typeof assistantDelivery>) => { body.callSid = `CA${'2'.repeat(32)}`; },
    (body: ReturnType<typeof assistantDelivery>) => { body.delivery.source.messageId = foreignId; },
    (body: ReturnType<typeof assistantDelivery>) => { body.version.digest = 'd'.repeat(64); },
  ]) {
    const response = assistantDelivery(); modify(response);
    await assert.rejects(harness([json(response)]).client.delivery(binding, requestId, assistantInput), errorCode('identity_mismatch'));
  }
  const ahead = assistantDelivery(); ahead.delivery.source.eventSequence = '9007199254740994';
  await assert.rejects(harness([json(ahead)]).client.delivery(binding, requestId, assistantInput), errorCode('invalid_response'));
  await assert.rejects(harness([json(questionDelivery())]).client.delivery(binding, requestId, assistantInput), errorCode('identity_mismatch'));
});

test('ready question requires the exact pending question and preserved meanings with owned source provenance', async () => {
  const response = questionDelivery();
  assert.deepEqual(await harness([json(response)]).client.delivery(binding, requestId, questionInput), response);
  const other = questionDelivery(); other.delivery.question.id = foreignId;
  await assert.rejects(harness([json(other)]).client.delivery(binding, requestId, questionInput), errorCode('identity_mismatch'));
  for (const changed of [
    { ...response.delivery.question, state: 'answered' },
    { ...response.delivery.question, revision: 2 },
    { ...response.delivery.question, threadId: foreignId },
    { ...response.delivery.question, runId: foreignId },
    { ...response.delivery.question, reply: {} },
    { ...response.delivery.question, inventedAuthority: true },
    { ...response.delivery.question, agent: { ...response.delivery.question.agent, extra: true } },
  ]) {
    const body = { ...response, delivery: { ...response.delivery, question: changed } };
    await assert.rejects(harness([json(body)]).client.delivery(binding, requestId, questionInput), errorCode('invalid_response'));
  }
});

test('delivery union forbids question data on an assistant or unavailable branch and requires it on a question', async () => {
  const assistant = assistantDelivery();
  const question = questionDelivery();
  const { question: _question, ...withoutQuestion } = question.delivery;
  const invalid = [
    { ...assistant, delivery: { ...assistant.delivery, question: question.delivery.question } },
    { ...question, delivery: withoutQuestion },
    { ...assistant, delivery: { state: 'unavailable', reason: 'source_not_in_projection', text: 'Raw leaked answer.' } },
    { ...assistant, delivery: { state: 'unavailable', reason: 'source_not_in_projection', question: question.delivery.question } },
    { ...assistant, delivery: { ...assistant.delivery, text: '🙂'.repeat(6001) } },
  ];
  for (const body of invalid) assert.equal(eligibleDeliveryResponseSchema.safeParse(body).success, false);
});

test('unavailable delivery remains explicit and does not trigger raw result or question fallback', async () => {
  for (const reason of ['not_ready', 'source_not_in_projection'] as const) {
    const body = { bindingId, callSid, requestId, version, delivery: { state: 'unavailable' as const, reason } };
    const { client, requests } = harness([json(body)]);
    assert.deepEqual(await client.delivery(binding, requestId, assistantInput), body);
    assert.equal(requests.length, 1);
    assert.equal('text' in body.delivery, false);
  }
});

test('stale history and unavailable authority propagate bounded API errors without fallback or retries', async () => {
  for (const operation of ['fetch', 'assert', 'delivery'] as const) {
    for (const refusal of [{ status: 409, code: 'context_changed', retryable: false },
      { status: 503, code: 'temporarily_unavailable', retryable: true },
      { status: 403, code: 'authority_unavailable', retryable: false }]) {
      const { client, requests } = harness([json({ error: { code: refusal.code, retryable: refusal.retryable } }, refusal.status)]);
      const result = operation === 'fetch' ? client.fetch(binding)
        : operation === 'assert' ? client.assert(binding, version) : client.delivery(binding, requestId, assistantInput);
      await assert.rejects(result, (error: unknown) => error instanceof PrimaryApiError && error.code === 'http_error'
        && error.status === refusal.status && error.remoteCode === refusal.code && error.retryable === refusal.retryable);
      assert.equal(requests.length, 1);
    }
  }
});

test('invalid request shapes and foreign input scopes refuse before HTTP', async () => {
  const { client, requests } = harness([]);
  await assert.rejects(client.fetch({ ...binding, bindingId: '../another-call' }), errorCode('invalid_request'));
  await assert.rejects(client.fetch({ ...binding, phoneContextVersion: 'legacy' } as EligibleContextBinding), errorCode('invalid_request'));
  await assert.rejects(client.assert(binding, { ...version, ownerPrincipalId: foreignId }), errorCode('identity_mismatch'));
  await assert.rejects(client.delivery(binding, requestId, { ...assistantInput,
    version: { ...version, threadId: foreignId } }), errorCode('identity_mismatch'));
  for (const source of [
    { kind: 'assistant', messageId, questionId },
    { kind: 'question', questionId, questionRevision: 2 },
    { kind: 'question', questionId, questionRevision: 1, text: 'Unprojected text.' },
  ]) {
    await assert.rejects(client.delivery(binding, requestId, { version, source } as EligibleDeliveryRequest), errorCode('invalid_request'));
  }
  assert.equal(requests.length, 0);
});

test('protocol selection is fixed by the successor client while the existing primary client stays absent', async () => {
  const requests: RequestInit[] = [];
  const snapshot = call();
  const legacy = { bindingId, callSid, state: 'open', context: { primaryDexterId, threadId, displayName: 'Dexter',
    messages: snapshot.context.messages.map(({ id, role, text }) => ({ id, role, text })) } };
  const client = createPrimaryApiClient({ apiBaseUrl: 'https://api.example.test', phoneServiceSecret: secret,
    fetch: async (_input, init) => { requests.push(init ?? {}); return json(legacy); } });
  assert.deepEqual(await client.getContext(bindingId), legacy);
  assert.equal(new Headers(requests[0].headers).get(PHONE_ELIGIBLE_CONTEXT_HEADER), null);
  assert.ok(new Headers(requests[0].headers).get('Authorization')?.startsWith('Bearer '));

  for (const code of ['phone_context_protocol_required', 'phone_context_protocol_invalid']) {
    const { client: successor, requests: attempted } = harness([json({ error: { code, retryable: false } }, 400)]);
    await assert.rejects(successor.fetch(binding), (error: unknown) => error instanceof PrimaryApiError
      && error.status === 400 && error.remoteCode === code && error.retryable === false);
    assert.equal(attempted.length, 1, 'protocol refusal never retries without the selected header');
    assert.equal(new Headers(attempted[0].init.headers).get(PHONE_ELIGIBLE_CONTEXT_HEADER), PHONE_ELIGIBLE_CONTEXT_VERSION);
  }
});

test('eligible endpoints retain shared redirect, body-size and configuration restrictions', async () => {
  await assert.rejects(harness([new Response(null, { status: 302, headers: { Location: 'https://foreign.example.test/' } })])
    .client.fetch(binding), errorCode('http_error'));
  await assert.rejects(harness([new Response('x', { status: 200, headers: { 'content-length': '65537' } })])
    .client.fetch(binding), errorCode('response_too_large'));
  assert.throws(() => createEligibleContextClient({ apiBaseUrl: 'http://foreign.example.test', phoneServiceSecret: secret }), errorCode('invalid_configuration'));
});

test('24,000 text bytes plus metadata can exceed 24 KiB without dropping any returned fields', async () => {
  const context = call();
  context.context.messages[0].text = '🙂'.repeat(6000);
  context.context.history.localContext = '';
  const delivery = assistantDelivery(); delivery.delivery.text = context.context.messages[0].text;
  for (const body of [context, delivery]) {
    const bytes = Buffer.byteLength(JSON.stringify(body));
    assert.ok(bytes > 24 * 1024 && bytes < ELIGIBLE_CONTEXT_MAX_RESPONSE_BYTES);
  }
  const { client } = harness([json(context), json(delivery)]);
  assert.deepEqual(parseEligiblePhoneCall(context, binding), context, 'registration parser preserves full metadata');
  assert.deepEqual(await client.fetch(binding), context);
  assert.deepEqual(await client.delivery(binding, requestId, assistantInput), delivery);
});

test('all eligible methods accept exactly 65,536 response bytes and refuse the next byte without fallback', async () => {
  const bodies = { fetch: call(), assert: { bindingId, callSid, current: true, version }, delivery: assistantDelivery() };
  for (const method of ['fetch', 'assert', 'delivery'] as const) {
    const serialized = JSON.stringify(bodies[method]);
    for (const bytes of [ELIGIBLE_CONTEXT_MAX_RESPONSE_BYTES, ELIGIBLE_CONTEXT_MAX_RESPONSE_BYTES + 1]) {
      // JSON whitespace measures the transport envelope cap independently of the text budget.
      const payload = serialized + ' '.repeat(bytes - Buffer.byteLength(serialized));
      assert.equal(Buffer.byteLength(payload), bytes);
      const response = new Response(payload, { headers: { 'Content-Type': 'application/json' } });
      assert.equal(response.headers.has('content-length'), false, 'exercise the streamed byte counter');
      const { client, requests } = harness([response]);
      const result = method === 'fetch' ? client.fetch(binding)
        : method === 'assert' ? client.assert(binding, version) : client.delivery(binding, requestId, assistantInput);
      if (bytes === ELIGIBLE_CONTEXT_MAX_RESPONSE_BYTES) assert.deepEqual(await result, bodies[method]);
      else await assert.rejects(result, errorCode('response_too_large'));
      assert.equal(requests.length, 1);
    }
  }
});

test('successor response default preserves an explicitly smaller existing transport bound', async () => {
  let requests = 0;
  const body = JSON.stringify(call());
  const client = createEligibleContextClient({ apiBaseUrl: 'https://api.example.test', phoneServiceSecret: secret,
    maxBodyBytes: Buffer.byteLength(body) - 1,
    fetch: async () => { requests++; return new Response(body); } });
  await assert.rejects(client.fetch(binding), errorCode('response_too_large'));
  assert.equal(requests, 1);
});

function laterCallJournal() {
  const target = { delivery: { bindingId, callSid }, execution: { bindingId: foreignId,
    requestId: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc', runId }, question: { id: questionId, revision: 1 as const } };
  const journal = createRequestDeliveryJournal({ deliveryCall: { callId: 'dddddddd-dddd-4ddd-8ddd-dddddddddddd', sessionId: 'new_call_session' },
    scope: { ownerPrincipalId, primaryDexterId, threadId }, target,
    request: { v: PHONE_REQUEST_DELIVERY_VERSION, originalRequestId: requestId, questionId, questionRevision: 1 } });
  return retainRequestDeliveryAssociation(journal, { replayed: false,
    association: { id: sourceEventId, v: PHONE_REQUEST_DELIVERY_VERSION, ...target, createdAt: when } });
}

test('later eligible delivery uses current call plus the full association on ready and unavailable responses', async () => {
  const journal = laterCallJournal();
  for (const original of [assistantDelivery(), questionDelivery(),
    { bindingId, callSid, requestId, version, delivery: { state: 'unavailable' as const, reason: 'source_not_in_projection' as const } }]) {
    for (const pathId of [requestId, journal.association!.execution.requestId]) {
      const body = { ...original, requestId: pathId, deliveryAssociation: journal.association };
      const { client, requests } = harness([json(body)]);
      const input = original.delivery.state === 'ready' && original.delivery.source.kind === 'question' ? questionInput : assistantInput;
      assert.deepEqual(await client.delivery(binding, pathId, input, journal), body);
      assert.equal(new Headers(requests[0].init.headers).get(PHONE_REQUEST_DELIVERY_HEADER), journal.association!.id);
      assert.equal(new Headers(requests[0].init.headers).get('X-Dexter-Binding-Id'), bindingId);
      assert.equal(requests[0].url.endsWith(`/requests/${pathId}/delivery`), true);
      assert.deepEqual(JSON.parse(String(requests[0].init.body)), input, 'association is a header; original request body stays exact');
    }
  }
});

test('later delivery refuses missing or changed associations and original-binding response envelopes', async () => {
  const journal = laterCallJournal();
  const body = { ...assistantDelivery(), deliveryAssociation: journal.association! };
  const invalid = [assistantDelivery(),
    { ...body, bindingId: journal.association!.execution.bindingId },
    { ...body, callSid: `CA${'2'.repeat(32)}` },
    { ...body, deliveryAssociation: { ...journal.association!, createdAt: '2026-09-24T12:01:00.000Z' } },
    { ...body, delivery: { ...body.delivery, source: { ...body.delivery.source,
      provenance: { ...provenance, runId: foreignId } } } },
    { ...body, delivery: { state: 'unavailable', reason: 'not_ready' }, deliveryAssociation: undefined },
  ];
  for (const response of invalid) await assert.rejects(harness([json(response)]).client.delivery(binding, requestId, assistantInput, journal),
    (error: unknown) => error instanceof PrimaryApiError && ['invalid_response', 'identity_mismatch'].includes(error.code));
  await assert.rejects(harness([json(body)]).client.delivery(binding, requestId, assistantInput), errorCode('invalid_response'));
});

test('later delivery validates saved scope, question and exact retained alias before HTTP', async () => {
  const journal = laterCallJournal();
  const { client, requests } = harness([]);
  await assert.rejects(client.delivery(binding, foreignId, assistantInput, journal), errorCode('identity_mismatch'));
  await assert.rejects(client.delivery(binding, requestId, assistantInput,
    { ...journal, scope: { ...journal.scope, ownerPrincipalId: foreignId } }), errorCode('identity_mismatch'));
  await assert.rejects(client.delivery({ ...binding, callSid: `CA${'2'.repeat(32)}` }, requestId, assistantInput, journal), errorCode('identity_mismatch'));
  await assert.rejects(client.delivery(binding, requestId, { ...questionInput,
    source: { kind: 'question', questionId: foreignId, questionRevision: 1 } }, journal), errorCode('identity_mismatch'));
  const { association: _association, ...unassociated } = journal;
  await assert.rejects(client.delivery(binding, requestId, assistantInput, unassociated), errorCode('identity_mismatch'));
  assert.equal(requests.length, 0);
});
