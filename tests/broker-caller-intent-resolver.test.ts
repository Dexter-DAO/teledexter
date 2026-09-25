import assert from 'node:assert/strict';
import { createCallerIntentResolver, buildCallerIntentInput } from '../src/broker/callerIntentResolver.js';
import { createPrimaryCallerIntentClient } from '../src/broker/callerIntentClient.js';
import { PHONE_REQUEST_DELIVERY_VERSION, createRequestDeliveryJournal, retainRequestDeliveryAssociation,
  createRequestDeliveryQuestionRecord } from '../src/broker/requestDelivery.js';
import type { SelectedQuestionSeed } from '../src/broker/selectedQuestion.js';
import test, { type TestContext } from 'node:test';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { BrokerStore, type BrokerCall } from '../src/broker/store.js';
import { brokerIdentity, type BrokerDelegationRecord, type BrokerQuestionRecord } from '../src/broker/backend.js';
import { beginBrokerPublication, beginSelectedQuestionPublication, settleBrokerPublication, rejectBrokerPublication, callerIntentResultPublicationCandidates, callerIntentResultPublicationInventory,
  type OrdinaryPublicationEvidence } from '../src/broker/publications.js';
import { callerRouteRef, findCallerRoute, requireCallerRoute, callerRouteUsedSourceIds, callerRoutingSourceAnchors,
  callerRoutePendingCaptures, callerQuestionPresentationDigest, type CallerRoutePreparation,
  callerIntentQuestionForTarget, requireCallerRouteDispatch, CallerRouteExpiredError,
  type CallerQuestionTarget, type CallerRouteDecision, type CallerRouteRecord } from '../src/broker/callerRouting.js';
import { callerIntentInputDigest, type CallerIntentInput, type CallerIntentResponse, type CallerIntentDecision } from '../src/broker/callerIntentContract.js';
import { turnBodyDigest } from '../src/broker/reconciliation.js';
import type { PrimaryTurnReceipt, PrimaryQuestion } from '../src/broker/primaryApi.js';
import type { LiveDelegationRequest } from '../src/live/types.js';

const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const sid = `CA${'e'.repeat(32)}`, bindingId = uuid(2), owner = uuid(3), primary = uuid(4), thread = uuid(5);
const session = 'route_session_1', generationId = uuid(6);
function fixture(t: TestContext, selected = false) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-caller-routing-'));
  const instances: BrokerStore[] = [];
  let store = new BrokerStore(directory); instances.push(store);
  const call = store.reserve({ requestId: uuid(1), direction: 'outbound', number: '+12025550123', purpose: 'Discuss the portfolio.', callSid: sid,
    ...(selected ? { continuation: { originalRequestId: uuid(201), questionId: uuid(202), questionRevision: 1 as const } } : {}) },
    selected ? 'authenticated_broker_control' : undefined).call;
  const assignment = selected ? { assignmentId: call.requestId, revision: 1 as const, digest: 'f'.repeat(64) } : undefined;
  store.recordPrimaryBinding(call.id, { bindingId, callSid: sid, state: 'open',
    ...(assignment ? { callAssignmentRef: assignment } : {}),
    context: { primaryDexterId: primary, threadId: thread, displayName: 'Dexter', messages: [] } });
  const version = { ownerPrincipalId: owner, primaryDexterId: primary, threadId: thread,
    contextEpoch: '1', threadEventSequence: '2', digest: 'a'.repeat(64) };
  const at = new Date(Date.now() - 10_000).toISOString();
  store.update(call.id, { status: 'in-progress', liveSessionId: session, mediaStartedAt: at,
    liveGenerations: { version: 1, callId: call.id,
      binding: { bindingId, callSid: sid, ownerPrincipalId: owner, primaryDexterId: primary, threadId: thread,
        ...(assignment ? { callAssignmentRef: assignment } : {}) },
      clocks: { mediaStartedAt: at, callDeadlineAt: new Date(Date.now() + 120_000).toISOString() },
      phase: 'active', inputNextSample: 24_000, deliveryAttachment: { generationId, sessionId: session },
      segments: [{ generationId, ordinal: 1, state: 'active', sessionId: session, createdAt: at, readyAt: at,
        version, pageDigest: 'b'.repeat(64), inputFirstSample: 0, inputLastSampleExclusive: 24_000,
        transcript: { revision: 1, callerRevision: 1, fragments: [{ eventId: 'caller_1', sequence: 1,
          role: 'user', text: ' Research Apple.\t', startMs: 1000, endMs: 2000 }] }, outputDiscarded: false }] } });
  const sourceEventId = brokerIdentity('source', call.id, session, 'caller_1');
  const frozen: CallerRoutePreparation['frozen'] = { sessionId: session, firstEventId: 'caller_1', firstSequence: 1,
    lastSequence: 1, callerRevision: 1, startMs: 1000, endMs: 2000, audioThroughMs: 3000, observedSpeechAtMs: Date.parse(at) + 2000,
    turn: { sourceEventId, revision: 1, itemId: `live-turn-${sourceEventId}`, role: 'user', text: ' Research Apple.\t',
      occurredAt: new Date(Date.parse(at) + 1000).toISOString() } };
  const request: LiveDelegationRequest = { callId: call.id, sessionId: session, delegationId: 'delegate_1',
    requestId: 'live_request_1', offsetMs: 2100, transcript: store.get(call.id)!.liveGenerations!.segments[0]!.transcript };
  const input = (): CallerRoutePreparation => {
    const current = store.get(call.id)!, inventory = callerIntentResultPublicationInventory(current, session);
    return { delegation: { sessionId: request.sessionId, delegationId: request.delegationId,
      requestId: request.requestId, offsetMs: request.offsetMs }, frozen: structuredClone(frozen),
      resolutionInput: { version, publicationSequence: current.publications?.records.at(-1)?.publicationSequence ?? 0,
        questions: [], references: inventory.candidates.map(({ id, digest }) => ({ id, digest })),
        resultCoverage: inventory.resultCoverage } };
  };
  const read = () => store.get(call.id)!;
  const prepare = (value = input()) => store.prepareCallerRoute(call.id, value);
  const intentInput = (record = read().callerRouting!.records[0]!): CallerIntentInput => {
    const coverage = record.resolutionInput.resultCoverage;
    const fields = { source: { callSid: sid, sourceEventId: record.frozen.turn.sourceEventId,
        itemId: record.frozen.turn.itemId, revision: 1 as const, receivedAt: record.frozen.turn.occurredAt,
        role: 'user' as const, text: record.frozen.turn.text }, generationId: record.scope.generationId,
      sourceSessionId: record.frozen.sessionId, brokerInputDigest: record.inputDigest,
      presentationDigest: callerQuestionPresentationDigest(record.resolutionInput),
      callDeadlineAt: read().liveGenerations!.clocks.callDeadlineAt, version: record.resolutionInput.version,
      questions: record.resolutionInput.questions.map(callerIntentQuestionForTarget),
      results: coverage?.state === 'overflow' ? [] : record.resolutionInput.references.map(ref => {
        const candidate = callerIntentResultPublicationCandidates(read(), record.frozen.sessionId)
          .find(value => value.id === ref.id && value.digest === ref.digest);
        assert.ok(candidate); return candidate.candidate;
      }),
    };
    // Only explicitly retained fixtures omit coverage and keep their v1 bytes.
    return coverage ? { v: 'phone.caller-intent.v2', ...fields, resultCoverage: coverage }
      : { v: 'phone.caller-intent.v1', ...fields };
  };
  const prepareIntent = (record = read().callerRouting!.records[0]!, value = intentInput(record), operationId = uuid(95)) =>
    store.prepareCallerIntent(call.id, callerRouteRef(record), { operationId, input: value });
  const intentResponse = (record = read().callerRouting!.records[0]!, decision?: CallerIntentDecision,
    reservedAt = new Date().toISOString()): CallerIntentResponse => {
    assert.ok(record.intent);
    const value = record.intent.input;
    const versionFields = value.v === 'phone.caller-intent.v2'
      ? { v: value.v, resultCoverage: value.resultCoverage, resultContextCoverage: value.resultCoverage.state === 'overflow'
        ? { state: 'unavailable' as const, reason: 'inventory_overflow' as const } : { state: 'complete' as const } }
      : { v: value.v };
    const fields = { ...versionFields, operationId: record.intent.operationId, bindingId,
      inputDigest: callerIntentInputDigest(record.intent.operationId, bindingId, value), brokerInputDigest: value.brokerInputDigest,
      presentationDigest: value.presentationDigest, generationId: value.generationId, sourceSessionId: value.sourceSessionId,
      callDeadlineAt: value.callDeadlineAt, questions: value.questions, results: value.results,
      reservedAt, validUntil: new Date(Math.min(Date.parse(value.callDeadlineAt), Date.parse(reservedAt) + 10_000)).toISOString(),
      version: value.version };
    return decision ? { ...fields, state: 'resolved', decision } : { ...fields, state: 'pending' };
  };
  const recordResponse = (response: CallerIntentResponse, record = read().callerRouting!.records[0]!) =>
    store.recordCallerIntentResponse(call.id, callerRouteRef(record), response);
  const decide = (decision: CallerRouteDecision, record = read().callerRouting!.records[0]!) => {
    if (decision.kind === 'task' || decision.kind === 'answer') {
      let current = read().callerRouting!.records.find(value => value.inputDigest === record.inputDigest)!;
      if (!current.intent) current = prepareIntent(current);
      if (!current.intent?.response) {
        const reference = decision.kind === 'task' ? decision.contextReference : undefined;
        const candidate = reference ? callerIntentResultPublicationCandidates(read(), session)
          .find(value => value.id === reference.id && value.digest === reference.digest) : undefined;
        const wire: CallerIntentDecision = decision.kind === 'answer'
          ? { kind: 'answer', target: callerIntentQuestionForTarget(decision.target) }
          : { kind: 'task', ...(decision.contextReference ? { reference: candidate?.reference ?? { kind: 'ordinary' as const,
            relatedPrimaryRequestId: uuid(98), referenceOrigin: 'broker_status_publication' as const } } : {}) };
        recordResponse(intentResponse(current, wire));
      }
    }
    return store.decideCallerRoute(call.id, callerRouteRef(record), { inputDigest: record.inputDigest, decision });
  };
  const restart = () => { store.close(); store = new BrokerStore(directory); instances.push(store); return store; };
  const bytes = () => fs.readFileSync(path.join(directory, 'calls.json'), 'utf8');
  const receipt = (): PrimaryTurnReceipt => ({ bindingId, sourceEventId, revision: 1, recordId: sourceEventId,
    revisionRecordId: uuid(9), bodyDigest: turnBodyDigest(frozen.turn), replayed: false });
  const entry = (): BrokerDelegationRecord => ({ version: 1,
    requestId: brokerIdentity('request', call.id, session, request.delegationId), callId: call.id, callSid: sid, bindingId,
    sessionId: session, delegationIds: [request.delegationId], status: 'prepared', updatedAt: at,
    submission: { source: { callSid: sid, sourceEventId, revision: 1, itemId: frozen.turn.itemId,
      receivedAt: frozen.turn.occurredAt, role: 'user' }, text: frozen.turn.text },
    frozenCallerRevision: 1, firstSequence: 1, lastSequence: 1, audioThroughMs: 3000,
    completionBasis: 'speech_and_transcript_stability', sourceRevision: 1, capturedText: frozen.turn.text });
  t.after(() => { for (const instance of instances) instance.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  return { call, request, frozen, version, input, prepare, decide, restart, bytes, receipt, entry, read, directory,
    intentInput, prepareIntent, intentResponse, recordResponse, store: () => store };
}

function pendingQuestion(f: ReturnType<typeof fixture>) {
  const entry = f.entry(), requestId = uuid(40), questionId = uuid(41), runId = uuid(42);
  entry.requestId = requestId; entry.submission = { source: { callSid: sid, sourceEventId: uuid(43), revision: 1,
    itemId: 'prior_source', receivedAt: f.call.createdAt, role: 'user' }, text: 'Earlier comparison.' };
  entry.status = 'waiting_for_reply';
  const q: PrimaryQuestion = { id: questionId, revision: 1, runId, threadId: thread, agent: { id: primary, name: 'Dexter' },
    state: 'pending', runState: 'waiting_for_reply', question: 'Compare both?', context: 'Research.', proposedAnswer: 'Compare.',
    yesMeaning: 'Compare both.', noMeaning: 'Stop.', reply: null, createdAt: f.call.createdAt,
    expiresAt: new Date(Date.now() + 60_000).toISOString() };
  entry.primaryResult = { bindingId, requestId, state: 'waiting_for_reply', question: q };
  entry.currentQuestionId = questionId;
  entry.questions = { [questionId]: { question: q, identityDigest: 'e'.repeat(64) } };
  f.store().update(f.call.id, { delegations: { [requestId]: entry } });
  const snapshot = { revision: 0, callerRevision: 0, fragments: [] };
  const identity = { callId: f.call.id, bindingId, callSid: sid, sessionId: session };
  const intent = beginBrokerPublication(f.store(), identity, requestId,
    { id: 'question_1', kind: 'result', text: q.question, speak: true, expectedCallerRevision: 0 }, snapshot)!;
  assert.ok(intent);
  settleBrokerPublication(f.store(), intent.record, { eventId: 'question_command_1', status: 'accepted',
    observedCallerRevision: 0, observedTranscriptSequence: 0 }, snapshot, false);
  const current = structuredClone(f.read().delegations[requestId]) as BrokerDelegationRecord;
  const presentation: BrokerQuestionRecord['presentation'] = { sessionId: session, callerRevision: 0, sequence: 0,
    lastCallerEndMs: 0, inputThroughMs: 0, acceptedAt: f.call.createdAt,
    publicationReceipts: [{ sessionId: session, eventId: 'question_command_1', publicationSequence: intent.record.publicationSequence }] };
  current.questions![questionId]!.presentation = presentation;
  f.store().update(f.call.id, { delegations: { [requestId]: current } });
  const target: CallerQuestionTarget = { kind: 'ordinary', requestId, bindingId, runId, questionId,
    questionRevision: 1, presentationDigest: callerQuestionPresentationDigest(presentation) };
  return { target, identity, requestId, questionId, entry: current };
}

function publishedResult(f: ReturnType<typeof fixture>, n: number, execution?: { requestId: string; runId: string }) {
  const entry = f.entry(), requestId = uuid(n), messageId = uuid(n + 1), eventId = uuid(n + 2), runId = execution?.runId ?? uuid(n + 3);
  const canonicalRequestId = execution?.requestId ?? requestId;
  const text = 'The fetched account result is saved.';
  entry.requestId = requestId; entry.status = 'completed';
  entry.submission = { ...entry.submission, source: { ...entry.submission.source, sourceEventId: uuid(n + 4), itemId: `prior_${n}` }, text: 'Prior work.' };
  entry.primaryResult = { requestId: canonicalRequestId, bindingId, state: 'completed', assistant: { messageId, eventId, sequence: '1', text } };
  if (canonicalRequestId !== requestId) entry.reconciliation = { outcome: 'admitted', requestId, bindingId,
    canonicalRequestId, result: entry.primaryResult };
  f.store().update(f.call.id, { delegations: { ...f.read().delegations, [requestId]: entry } });
  const response: OrdinaryPublicationEvidence['response'] = { bindingId, callSid: sid, requestId: canonicalRequestId, version: f.version,
    delivery: { state: 'ready', text, source: { kind: 'assistant', messageId, eventSequence: '1',
      provenance: { messageCreatedAt: f.call.createdAt, eventId, eventOccurredAt: f.call.createdAt, runId } } } };
  const identity = { callId: f.call.id, bindingId, callSid: sid, sessionId: session };
  const publish = (attempt: number) => {
    const intent = beginBrokerPublication(f.store(), identity, requestId,
      { id: `prior_${n}_attempt_${attempt}_0`, kind: 'result', speak: true, text, expectedCallerRevision: 1 }, f.request.transcript,
      { response, chunkIndex: 0 }); assert.ok(intent);
    const event = `prior_${n}_command_${attempt}`;
    settleBrokerPublication(f.store(), intent.record, { eventId: event, status: 'accepted',
      observedCallerRevision: 1, observedTranscriptSequence: 1 }, f.request.transcript, false);
    return event;
  };
  return { identity, response, publish };
}


function operationId(f: ReturnType<typeof fixture>, record = f.read().callerRouting!.records[0]!) {
  return brokerIdentity('caller-intent', f.call.id, record.frozen.sessionId,
    record.delegation.delegationId, record.frozen.turn.sourceEventId);
}
function invoke(f: ReturnType<typeof fixture>, resolver: ReturnType<typeof createCallerIntentResolver>, signal = new AbortController().signal) {
  const record = f.read().callerRouting!.records[0]!;
  return resolver.resolve({ operationId: operationId(f, record), record,
    callDeadlineAt: f.read().liveGenerations!.clocks.callDeadlineAt }, signal);
}
function transport(f: ReturnType<typeof fixture>, fetcher: NonNullable<Parameters<typeof createPrimaryCallerIntentClient>[0]['fetch']>, timeoutMs = 1000) {
  return createCallerIntentResolver({ callId: f.call.id, store: f.store(), timeoutMs,
    client: createPrimaryCallerIntentClient({ apiBaseUrl: 'https://api.example.test',
      phoneServiceSecret: 'x'.repeat(40), fetch: fetcher }) });
}

// Load a retained v1 operation as a historical disk fixture. Fresh store methods
// deliberately cannot prepare a new v1 operation.
function restoreLegacyOperation(f: ReturnType<typeof fixture>) {
  const input = f.input(); delete input.resolutionInput.resultCoverage;
  const record = f.prepare(input), value = f.intentInput(record);
  assert.equal(value.v, 'phone.caller-intent.v1');
  const saved: CallerRouteRecord = { ...record, intent: { operationId: operationId(f, record), input: value } };
  const disk = JSON.parse(f.bytes()); disk.calls[f.call.id].callerRouting.records = [saved];
  f.store().close(); fs.writeFileSync(path.join(f.directory, 'calls.json'), JSON.stringify(disk));
  f.restart(); return saved;
}

for (const code of ['caller_result_inventory_inactive', 'caller_result_inventory_invalid', 'caller_result_inventory_unverifiable'] as const)
  test(`local ${code} remains durable and refuses resolver HTTP instead of claiming an empty inventory`, async t => {
    const f = fixture(t), input = f.input(); delete input.resolutionInput.resultCoverage;
    input.resolutionInput.classificationRefusal = code;
    const record = f.prepare(input), saved = f.bytes(); let calls = 0;
    assert.throws(() => buildCallerIntentInput(f.read(), record), /inventory_refused/);
    await assert.rejects(invoke(f, transport(f, async () => { calls++; throw new Error('forbidden'); })), /inventory_refused/);
    assert.equal(calls, 0); assert.equal(f.bytes(), saved);
    assert.equal(f.read().callerRouting!.records[0]!.intent, undefined);
    assert.deepEqual(f.read().callerRouting!.records[0]!.frozen, f.frozen);
    assert.equal(f.read().callerRouting!.records[0]!.resolutionInput.resultCoverage, undefined);
    assert.equal(callerRoutePendingCaptures(f.read()), 1); assert.deepEqual(f.read().delegations, {});
    f.restart(); assert.equal(f.bytes(), saved);
  });

test('a never-dispatched historical v1 source is refused before a new intent POST', async t => {
  const f = fixture(t), input = f.input(); delete input.resolutionInput.resultCoverage;
  const record = f.prepare(input), saved = f.bytes(); let calls = 0;
  assert.throws(() => f.prepareIntent(record, f.intentInput(record)), /legacy_dispatch_refused/);
  const resolver = transport(f, async () => { calls++; throw new Error('forbidden'); });
  await assert.rejects(invoke(f, resolver), /legacy_dispatch_refused/);
  assert.equal(calls, 0); assert.equal(f.bytes(), saved);
  assert.equal(f.read().callerRouting!.records[0]!.intent, undefined);
  assert.equal(f.read().callerRouting!.records[0]!.resolutionInput.resultCoverage, undefined);
  assert.deepEqual(f.read().delegations, {});
});

test('unknown historical v1 intent recovers by exact GET and cannot authorize a fresh downstream dispatch', async t => {
  const f = fixture(t), saved = restoreLegacyOperation(f), methods: string[] = [];
  const response = f.intentResponse(saved, { kind: 'task' });
  const resolver = transport(f, async (url, init) => {
    methods.push(init!.method!); assert.equal(init!.body, undefined);
    assert.ok(String(url).endsWith(`/calls/${bindingId}/caller-intents/${saved.intent!.operationId}`));
    assert.deepEqual(f.read().callerRouting!.records[0]!.intent!.input, saved.intent!.input);
    return Response.json(response);
  });
  await assert.rejects(invoke(f, resolver), /legacy_dispatch_refused/);
  assert.deepEqual(methods, ['GET']);
  const retained = f.read().callerRouting!.records[0]!;
  assert.deepEqual(retained.intent!.response, response); assert.equal(retained.decision, undefined);
  assert.deepEqual(retained.intent!.input, saved.intent!.input);
  assert.equal(retained.resolutionInput.resultCoverage, undefined); assert.deepEqual(f.read().delegations, {});
  const bytes = f.bytes(); f.restart(); assert.equal(f.bytes(), bytes);
  await assert.rejects(invoke(f, transport(f, async () => { throw new Error('no repeat HTTP'); })), /legacy_dispatch_refused/);
  assert.equal(f.bytes(), bytes);
});

// These cases author a full durable-store/client crossing. Fetch is the sole
// HTTP boundary; they do not qualify semantic model accuracy or latency.
test('raw source and exact operation are durable before POST; full receipt precedes a task decision', async t => {
  const f = fixture(t), route = f.prepare(); let posts = 0;
  const resolver = transport(f, async (_url, init) => {
    posts++; assert.equal(init?.method, 'POST');
    const saved = JSON.parse(f.bytes()).calls[f.call.id].callerRouting.records[0];
    assert.equal(saved.intent.operationId, operationId(f));
    assert.equal(saved.intent.response, undefined); assert.equal(saved.decision, undefined);
    assert.deepEqual(JSON.parse(String(init?.body)), saved.intent.input);
    assert.equal(saved.intent.input.source.text, ' Research Apple.\t');
    assert.equal(saved.intent.input.v, 'phone.caller-intent.v2');
    assert.deepEqual(saved.intent.input.resultCoverage, { state: 'complete', count: 0 });
    assert.equal(new Headers(init?.headers).get('Idempotency-Key'), saved.intent.operationId);
    return Response.json(f.intentResponse(f.read().callerRouting!.records[0]!, { kind: 'task' }));
  });
  const resolution = await invoke(f, resolver);
  assert.deepEqual(resolution, { inputDigest: route.inputDigest, decision: { kind: 'task' } });
  const saved = f.read().callerRouting!.records[0]!;
  assert.equal(saved.intent!.response!.state, 'resolved'); assert.equal(saved.decision, undefined);
  assert.deepEqual(f.read().delegations, {});
  f.store().decideCallerRoute(f.call.id, callerRouteRef(route), resolution);
  assert.equal(requireCallerRoute(f.read(), f.request, callerRouteRef(route), 'task').decision?.kind, 'task');
  assert.equal(posts, 1); assert.equal(callerRoutePendingCaptures(f.read()), 1);
});

test('unknown POST survives restart and exact recovery uses only GET with original input', async t => {
  const f = fixture(t); f.prepare(); const methods: string[] = [];
  const first = transport(f, async (_url, init) => { methods.push(init!.method!); throw new Error('socket lost'); });
  await assert.rejects(invoke(f, first));
  const saved = structuredClone(f.read().callerRouting!.records[0]!); assert.ok(saved.intent);
  f.restart();
  const second = transport(f, async (url, init) => {
    methods.push(init!.method!); assert.equal(init!.method, 'GET'); assert.equal(init!.body, undefined);
    assert.ok(String(url).endsWith(`/calls/${bindingId}/caller-intents/${saved.intent!.operationId}`));
    assert.deepEqual(f.read().callerRouting!.records[0]!.intent, saved.intent);
    return Response.json(f.intentResponse(f.read().callerRouting!.records[0]!, { kind: 'conversation' }));
  });
  const result = await invoke(f, second);
  assert.deepEqual(result.decision, { kind: 'conversation' }); assert.deepEqual(methods, ['POST', 'GET']);
  assert.deepEqual(f.read().callerRouting!.records[0]!.intent!.input, saved.intent.input);
  assert.deepEqual(f.read().delegations, {});
});

test('crash after durable preparation and authoritative GET404 cannot trigger a POST', async t => {
  const f = fixture(t), record = f.prepare();
  f.prepareIntent(record, buildCallerIntentInput(f.read(), record), operationId(f)); f.restart();
  const methods: string[] = [], resolver = transport(f, async (_url, init) => {
    methods.push(init!.method!); return Response.json({ error: 'caller_intent_not_found', retryable: false }, { status: 404 });
  });
  await assert.rejects(invoke(f, resolver));
  assert.deepEqual(methods, ['GET']); assert.equal(f.read().callerRouting!.records[0]!.decision, undefined);
  assert.equal(callerRoutePendingCaptures(f.read()), 1);
});

test('pending response is durable; recovery keeps its reservation and validity unchanged', async t => {
  const f = fixture(t); f.prepare(); let pending: CallerIntentResponse | undefined;
  const first = transport(f, async () => {
    pending = f.intentResponse(); return Response.json(pending, { status: 202 });
  });
  await assert.rejects(invoke(f, first), /caller_intent_pending/);
  assert.deepEqual(f.read().callerRouting!.records[0]!.intent!.response, pending);
  assert.equal(f.read().callerRouting!.records[0]!.decision, undefined); f.restart();
  const recovered = transport(f, async (_url, init) => {
    assert.equal(init!.method, 'GET'); assert.ok(pending);
    return Response.json({ ...pending, state: 'resolved', decision: { kind: 'task' } });
  });
  const result = await invoke(f, recovered); assert.equal(result.decision.kind, 'task');
  const final = f.read().callerRouting!.records[0]!.intent!.response!;
  assert.equal(final.reservedAt, pending!.reservedAt); assert.equal(final.validUntil, pending!.validUntil);
});

test('an existing local clarification is final even if a later resolution becomes available', async t => {
  const f = fixture(t), record = f.prepare(); let calls = 0;
  const resolver = transport(f, async () => { calls++; return Response.json(f.intentResponse(), { status: 202 }); });
  await assert.rejects(invoke(f, resolver), /caller_intent_pending/);
  const final = { inputDigest: record.inputDigest, decision: { kind: 'clarify' as const, reason: 'unavailable' as const } };
  f.store().decideCallerRoute(f.call.id, callerRouteRef(record), final);
  assert.deepEqual(await invoke(f, resolver), final); assert.equal(calls, 1);
  assert.deepEqual(f.read().delegations, {});
});

test('saved resolved receipt is reused without HTTP, and its first-dispatch deadline does not renew', async t => {
  const f = fixture(t); f.prepare(); let calls = 0;
  const resolver = transport(f, async () => { calls++; return Response.json(f.intentResponse(f.read().callerRouting!.records[0]!, { kind: 'task' })); });
  const first = await invoke(f, resolver), bytes = f.bytes();
  assert.deepEqual(await invoke(f, resolver), first); assert.equal(f.bytes(), bytes); assert.equal(calls, 1);
});

for (const changed of ['operation', 'raw', 'deadline'] as const) test(`changed ${changed} refuses before HTTP or operation persistence`, async t => {
  const f = fixture(t), record = f.prepare(); let calls = 0;
  const resolver = transport(f, async () => { calls++; throw new Error('forbidden'); });
  const input = { operationId: operationId(f), record: structuredClone(record), callDeadlineAt: f.read().liveGenerations!.clocks.callDeadlineAt };
  if (changed === 'operation') input.operationId = uuid(90);
  if (changed === 'raw') input.record.frozen.turn.text = 'Sell Apple.';
  if (changed === 'deadline') input.callDeadlineAt = new Date(Date.parse(input.callDeadlineAt) + 1000).toISOString();
  await assert.rejects(resolver.resolve(input, new AbortController().signal));
  assert.equal(calls, 0); assert.equal(f.read().callerRouting!.records[0]!.intent, undefined);
});

test('later caller frontier saves the returned receipt but refuses its now-stale task', async t => {
  const f = fixture(t); f.prepare();
  const resolver = transport(f, async () => {
    const receipt = f.intentResponse(f.read().callerRouting!.records[0]!, { kind: 'task' });
    const g = structuredClone(f.read().liveGenerations!); const transcript = g.segments[0]!.transcript;
    transcript.revision++; transcript.callerRevision++;
    transcript.fragments.push({ eventId: 'caller_2', sequence: transcript.revision, role: 'user', text: 'Wait.', startMs: 5000, endMs: 5500 });
    f.store().update(f.call.id, { liveGenerations: g });
    return Response.json(receipt);
  });
  await assert.rejects(invoke(f, resolver), /context_changed/);
  assert.equal(f.read().callerRouting!.records[0]!.intent!.response!.state, 'resolved');
  assert.equal(f.read().callerRouting!.records[0]!.decision, undefined); assert.deepEqual(f.read().delegations, {});
});

test('call closure during classification preserves the receipt and permits no route', async t => {
  const f = fixture(t); f.prepare();
  const resolver = transport(f, async () => {
    const receipt = f.intentResponse(f.read().callerRouting!.records[0]!, { kind: 'task' });
    f.store().update(f.call.id, { status: 'completed', endedAt: new Date().toISOString() });
    return Response.json(receipt);
  });
  await assert.rejects(invoke(f, resolver), /source_inactive/);
  assert.equal(f.read().callerRouting!.records[0]!.intent!.response!.state, 'resolved');
  assert.equal(f.read().callerRouting!.records[0]!.decision, undefined);
});

test('an expired saved result cannot authorize a first dispatch', async t => {
  const f = fixture(t), record = f.prepare();
  f.prepareIntent(record, buildCallerIntentInput(f.read(), record), operationId(f));
  f.recordResponse(f.intentResponse(f.read().callerRouting!.records[0]!, { kind: 'task' }, new Date(Date.now() - 20_000).toISOString()));
  let calls = 0; const resolver = transport(f, async () => { calls++; throw new Error('forbidden'); });
  await assert.rejects(invoke(f, resolver), /caller_intent_expired/);
  assert.equal(calls, 0); assert.equal(f.read().callerRouting!.records[0]!.decision, undefined);
});

test('bad response correlation is unavailable and cannot write a receipt or action', async t => {
  const f = fixture(t); f.prepare(); let calls = 0;
  const resolver = transport(f, async () => {
    calls++; return Response.json({ ...f.intentResponse(f.read().callerRouting!.records[0]!, { kind: 'task' }), inputDigest: 'f'.repeat(64) });
  });
  await assert.rejects(invoke(f, resolver));
  assert.equal(calls, 1); assert.equal(f.read().callerRouting!.records[0]!.intent!.response, undefined);
  assert.equal(f.read().callerRouting!.records[0]!.decision, undefined);
});

test('aborted source does not prepare an operation; host timeout aborts one HTTP request', async t => {
  const f = fixture(t); f.prepare(); let calls = 0, aborted = false;
  const resolver = transport(f, async (_url, init) => {
    calls++; return new Promise<Response>((_resolve, reject) => {
      const signal = init!.signal; assert.ok(signal);
      signal.addEventListener('abort', () => { aborted = true; reject(new Error('aborted')); }, { once: true });
    });
  }, 10);
  const stopped = new AbortController(); stopped.abort();
  await assert.rejects(invoke(f, resolver, stopped.signal));
  assert.equal(calls, 0); assert.equal(f.read().callerRouting!.records[0]!.intent, undefined);
  await assert.rejects(invoke(f, resolver));
  assert.equal(calls, 1); assert.equal(aborted, true);
  assert.ok(f.read().callerRouting!.records[0]!.intent); assert.equal(f.read().callerRouting!.records[0]!.decision, undefined);
});

test('exact offered ordinary question maps to its saved presentation and never to a new task', async t => {
  const f = fixture(t), pending = pendingQuestion(f), input = f.input();
  input.resolutionInput.questions = [pending.target]; const record = f.prepare(input);
  const resolver = transport(f, async () => Response.json(f.intentResponse(f.read().callerRouting!.records[0]!,
    { kind: 'answer', target: callerIntentQuestionForTarget(pending.target) })));
  const resolution = await invoke(f, resolver);
  assert.deepEqual(resolution.decision, { kind: 'answer', target: pending.target });
  f.store().decideCallerRoute(f.call.id, callerRouteRef(record), resolution);
  assert.equal(requireCallerRoute(f.read(), f.request, callerRouteRef(record), 'ordinary_answer').decision!.kind, 'answer');
  assert.equal(Object.keys(f.read().delegations).length, 1);
});

test('two ordered published results map only the exact returned offered reference', async t => {
  const f = fixture(t); publishedResult(f, 100).publish(1); publishedResult(f, 110).publish(1);
  const candidates = callerIntentResultPublicationCandidates(f.read(), session), input = f.input();
  input.resolutionInput.references = candidates.map(({ id, digest }) => ({ id, digest })); const record = f.prepare(input);
  const resolver = transport(f, async (_url, init) => {
    assert.deepEqual(JSON.parse(String(init!.body)).results, candidates.map(value => value.candidate));
    return Response.json(f.intentResponse(f.read().callerRouting!.records[0]!, { kind: 'task', reference: candidates[1]!.reference }));
  });
  const result = await invoke(f, resolver);
  assert.deepEqual(result.decision, { kind: 'task', contextReference: input.resolutionInput.references[1] });
  f.store().decideCallerRoute(f.call.id, callerRouteRef(record), result);
  assert.equal(requireCallerRoute(f.read(), f.request, callerRouteRef(record), 'task').decision!.kind, 'task');
});

test('equivalent ordinary request references keep the first frozen offer without dropping either message', async t => {
  const f = fixture(t), execution = { requestId: uuid(200), runId: uuid(210) };
  publishedResult(f, 100, execution).publish(1); publishedResult(f, 110, execution).publish(1);
  const candidates = callerIntentResultPublicationCandidates(f.read(), session); assert.equal(candidates.length, 2);
  assert.deepEqual(candidates[0]!.reference, candidates[1]!.reference);
  assert.notEqual(candidates[0]!.candidate.assistantMessageId, candidates[1]!.candidate.assistantMessageId);
  const input = f.input(); input.resolutionInput.references = candidates.map(({ id, digest }) => ({ id, digest }));
  const record = f.prepare(input), resolver = transport(f, async (_url, init) => {
    assert.deepEqual(JSON.parse(String(init!.body)).results, candidates.map(value => value.candidate));
    return Response.json(f.intentResponse(f.read().callerRouting!.records[0]!, { kind: 'task', reference: candidates[1]!.reference }));
  });
  const result = await invoke(f, resolver);
  assert.deepEqual(result.decision, { kind: 'task', contextReference: input.resolutionInput.references[0] });
  f.store().decideCallerRoute(f.call.id, callerRouteRef(record), result); f.restart();
  assert.deepEqual(f.read().callerRouting!.records[0]!.intent!.input.results, candidates.map(value => value.candidate));
  assert.deepEqual(f.read().callerRouting!.records[0]!.decision, result.decision);
});

test('selected result retains its original execution and exact association when mapped to a new task', async t => {
  const f = fixture(t, true), selected = f.read().continuation!;
  const association = { v: PHONE_REQUEST_DELIVERY_VERSION, id: uuid(203), delivery: { bindingId, callSid: sid },
    execution: { bindingId: uuid(204), requestId: selected.originalRequestId, runId: uuid(205) },
    question: { id: selected.questionId, revision: 1 as const }, createdAt: f.call.createdAt };
  const seed: SelectedQuestionSeed = { v: 'broker.selected-question-seed.v1',
    scope: { ownerPrincipalId: owner, primaryDexterId: primary, threadId: thread },
    deliveryCall: { callId: f.call.id, sessionId: session }, delivery: association.delivery,
    request: { v: PHONE_REQUEST_DELIVERY_VERSION, ...selected }, callDeadlineAt: f.read().liveGenerations!.clocks.callDeadlineAt };
  f.store().recordSelectedQuestion(f.call.id, { v: 'broker.selected-question-host.v1', initial: seed,
    questions: [{ seed, admissions: [] }] });
  const base = retainRequestDeliveryAssociation(createRequestDeliveryJournal({ deliveryCall: seed.deliveryCall,
    scope: seed.scope, request: seed.request, target: { delivery: seed.delivery,
      execution: association.execution, question: association.question } }), { association, replayed: false });
  f.store().recordSelectedQuestion(f.call.id, { v: 'broker.selected-question-host.v1', initial: seed,
    questions: [{ seed, record: createRequestDeliveryQuestionRecord(base), admissions: [] }] });
  const response: Parameters<typeof beginSelectedQuestionPublication>[2] = { bindingId, callSid: sid,
    requestId: selected.originalRequestId, version: f.version, deliveryAssociation: association,
    delivery: { state: 'ready', text: 'The earlier comparison is saved.', source: { kind: 'assistant',
      messageId: uuid(206), eventSequence: '1', provenance: { messageCreatedAt: f.call.createdAt,
        eventId: uuid(207), eventOccurredAt: f.call.createdAt, runId: association.execution.runId } } } };
  const current = f.read().liveGenerations!;
  assert.deepEqual(current.deliveryAttachment, { generationId, sessionId: session });
  assert.deepEqual(response.version, current.segments[0]!.version);
  assert.equal(response.version.threadEventSequence, '2');
  assert.ok(response.delivery.state === 'ready');
  assert.ok(BigInt(response.delivery.source.eventSequence) <= BigInt(response.version.threadEventSequence));
  const identity = { callId: f.call.id, bindingId, callSid: sid, sessionId: session };
  const publication = beginSelectedQuestionPublication(f.store(), identity, response, { id: 'selected_0', kind: 'result',
    speak: true, text: 'The earlier comparison is saved.', expectedCallerRevision: 1 }, f.request.transcript, 0)!;
  settleBrokerPublication(f.store(), publication.record, { eventId: 'selected_command', status: 'accepted',
    observedCallerRevision: 1, observedTranscriptSequence: 1 }, f.request.transcript, false);
  const candidates = callerIntentResultPublicationCandidates(f.read(), session); assert.equal(candidates.length, 1);
  const candidate = candidates[0]!; assert.equal(candidate.reference.kind, 'associated');
  const input = f.input(); input.resolutionInput.references = [{ id: candidate.id, digest: candidate.digest }];
  const record = f.prepare(input), resolver = transport(f, async (_url, init) => {
    const value = JSON.parse(String(init!.body));
    assert.deepEqual(value.version, response.version);
    assert.equal(value.results[0].deliveryAssociationId, association.id);
    assert.deepEqual(value.results[0].execution, association.execution);
    return Response.json(f.intentResponse(f.read().callerRouting!.records[0]!, { kind: 'task', reference: candidate.reference }));
  });
  const result = await invoke(f, resolver);
  assert.deepEqual(result.decision, { kind: 'task', contextReference: input.resolutionInput.references[0] });
  f.store().decideCallerRoute(f.call.id, callerRouteRef(record), result);
  assert.equal(requireCallerRoute(f.read(), f.request, callerRouteRef(record), 'task').decision!.kind, 'task');
  assert.deepEqual(f.read().delegations, {}, 'publication does not manufacture a current-call execution');
});

test('seven results reach v2 in journal order with complete coverage and no truncation', async t => {
  const f = fixture(t); for (const n of [100, 110, 120, 130, 140, 150, 160]) publishedResult(f, n).publish(1);
  const candidates = callerIntentResultPublicationCandidates(f.read(), session), record = f.prepare();
  assert.equal(candidates.length, 7);
  let calls = 0; const resolver = transport(f, async (_url, init) => {
    calls++; const input = JSON.parse(String(init!.body));
    assert.equal(input.v, 'phone.caller-intent.v2');
    assert.deepEqual(input.resultCoverage, { state: 'complete', count: 7 });
    assert.deepEqual(input.results, candidates.map(value => value.candidate));
    return Response.json(f.intentResponse(f.read().callerRouting!.records[0]!, { kind: 'task', reference: candidates[6]!.reference }));
  });
  const result = await invoke(f, resolver);
  assert.deepEqual(result.decision, { kind: 'task', contextReference: record.resolutionInput.references[6] });
  assert.equal(calls, 1);
  assert.deepEqual(f.read().callerRouting!.records[0]!.intent!.input.results, candidates.map(value => value.candidate));
});

test('eight results retain the full internal inventory while v2 reports overflow with no partial API results', async t => {
  const f = fixture(t); for (const n of [100, 110, 120, 130, 140, 150, 160, 170]) publishedResult(f, n).publish(1);
  const candidates = callerIntentResultPublicationCandidates(f.read(), session), record = f.prepare();
  assert.equal(candidates.length, 8);
  let calls = 0; const resolver = transport(f, async (_url, init) => {
    calls++; const input = JSON.parse(String(init!.body));
    assert.equal(input.v, 'phone.caller-intent.v2');
    assert.deepEqual(input.resultCoverage, { state: 'overflow', count: 8 });
    assert.deepEqual(input.results, []);
    assert.deepEqual(f.read().callerRouting!.records[0]!.resolutionInput.references,
      candidates.map(({ id, digest }) => ({ id, digest })));
    return Response.json(f.intentResponse(f.read().callerRouting!.records[0]!, { kind: 'clarify', reason: 'candidate_unavailable' }));
  });
  const result = await invoke(f, resolver);
  assert.deepEqual(result.decision, { kind: 'clarify', reason: 'unavailable' });
  f.store().decideCallerRoute(f.call.id, callerRouteRef(record), result);
  const saved = f.bytes(); f.restart(); assert.equal(f.bytes(), saved); assert.equal(calls, 1);
  assert.deepEqual(f.read().callerRouting!.records[0]!.resolutionInput.references,
    candidates.map(({ id, digest }) => ({ id, digest })));
  const response = f.read().callerRouting!.records[0]!.intent!.response!;
  assert.equal(response.v, 'phone.caller-intent.v2');
  if (response.v === 'phone.caller-intent.v2') assert.deepEqual(response.resultContextCoverage,
    { state: 'unavailable', reason: 'inventory_overflow' });
  assert.equal(callerRoutePendingCaptures(f.read()), 1);
});

test('an incomplete saved candidate offer is refused without choosing by recency', async t => {
  const f = fixture(t); publishedResult(f, 100).publish(1); publishedResult(f, 110).publish(1);
  const candidates = callerIntentResultPublicationCandidates(f.read(), session), input = f.input();
  input.resolutionInput.references = candidates.slice(1).map(({ id, digest }) => ({ id, digest }));
  input.resolutionInput.resultCoverage = { state: 'complete', count: input.resolutionInput.references.length }; f.prepare(input);
  let calls = 0; const resolver = transport(f, async () => { calls++; throw new Error('forbidden'); });
  await assert.rejects(invoke(f, resolver), /candidate_coverage_changed/);
  assert.equal(calls, 0); assert.equal(f.read().callerRouting!.records[0]!.intent, undefined);
});

for (const reason of ['mixed_intent', 'context_changed', 'provider_uncertain', 'invalid_output'] as const)
  test(`shared ${reason} remains a non-action disposition with full receipt retained`, async t => {
    const f = fixture(t); f.prepare();
    const resolver = transport(f, async () => Response.json(f.intentResponse(f.read().callerRouting!.records[0]!, { kind: 'clarify', reason })));
    const result = await invoke(f, resolver);
    const mapped = { mixed_intent: 'mixed_intent', context_changed: 'stale_context', provider_uncertain: 'unavailable', invalid_output: 'invalid_resolution' };
    assert.deepEqual(result.decision, { kind: 'clarify', reason: mapped[reason] });
    f.store().decideCallerRoute(f.call.id, callerRouteRef(f.read().callerRouting!.records[0]!), result);
    assert.equal(f.read().callerRouting!.records[0]!.intent!.response!.state, 'resolved');
    assert.deepEqual(f.read().delegations, {});
  });
