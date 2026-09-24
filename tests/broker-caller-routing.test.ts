import assert from 'node:assert/strict';
import test, { type TestContext } from 'node:test';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { BrokerStore, type BrokerCall } from '../src/broker/store.js';
import { brokerIdentity, type BrokerDelegationRecord, type BrokerQuestionRecord } from '../src/broker/backend.js';
import { beginBrokerPublication, settleBrokerPublication, rejectBrokerPublication, callerIntentResultPublicationCandidates,
  type OrdinaryPublicationEvidence } from '../src/broker/publications.js';
import { callerRouteRef, findCallerRoute, requireCallerRoute, callerRouteUsedSourceIds, callerRoutingSourceAnchors,
  callerRoutePendingCaptures, callerQuestionPresentationDigest, type CallerRoutePreparation,
  callerIntentQuestionForTarget, requireCallerRouteDispatch, CallerRouteExpiredError,
  type CallerQuestionTarget, type CallerRouteDecision } from '../src/broker/callerRouting.js';
import { callerIntentInputDigest, type CallerIntentInput, type CallerIntentResponse, type CallerIntentDecision } from '../src/broker/callerIntentContract.js';
import { turnBodyDigest } from '../src/broker/reconciliation.js';
import type { PrimaryTurnReceipt, PrimaryQuestion } from '../src/broker/primaryApi.js';
import type { LiveDelegationRequest } from '../src/live/types.js';

const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const sid = `CA${'e'.repeat(32)}`, bindingId = uuid(2), owner = uuid(3), primary = uuid(4), thread = uuid(5);
const session = 'route_session_1', generationId = uuid(6);
function fixture(t: TestContext) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-caller-routing-'));
  const instances: BrokerStore[] = [];
  let store = new BrokerStore(directory); instances.push(store);
  const call = store.reserve({ requestId: uuid(1), direction: 'outbound', number: '+12025550123', purpose: 'Discuss the portfolio.', callSid: sid }).call;
  store.recordPrimaryBinding(call.id, { bindingId, callSid: sid, state: 'open',
    context: { primaryDexterId: primary, threadId: thread, displayName: 'Dexter', messages: [] } });
  const version = { ownerPrincipalId: owner, primaryDexterId: primary, threadId: thread,
    contextEpoch: '1', threadEventSequence: '2', digest: 'a'.repeat(64) };
  const at = new Date(Date.now() - 10_000).toISOString();
  store.update(call.id, { status: 'in-progress', liveSessionId: session, mediaStartedAt: at,
    liveGenerations: { version: 1, callId: call.id,
      binding: { bindingId, callSid: sid, ownerPrincipalId: owner, primaryDexterId: primary, threadId: thread },
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
  const input = (): CallerRoutePreparation => ({ delegation: { sessionId: request.sessionId, delegationId: request.delegationId,
    requestId: request.requestId, offsetMs: request.offsetMs }, frozen: structuredClone(frozen),
    resolutionInput: { version, publicationSequence: store.get(call.id)!.publications?.records.at(-1)?.publicationSequence ?? 0,
      questions: [], references: [] } });
  const read = () => store.get(call.id)!;
  const prepare = (value = input()) => store.prepareCallerRoute(call.id, value);
  const intentInput = (record = read().callerRouting!.records[0]!): CallerIntentInput => ({
    v: 'phone.caller-intent.v1', source: { callSid: sid, sourceEventId: record.frozen.turn.sourceEventId,
      itemId: record.frozen.turn.itemId, revision: 1, receivedAt: record.frozen.turn.occurredAt,
      role: 'user', text: record.frozen.turn.text }, generationId: record.scope.generationId,
    sourceSessionId: record.frozen.sessionId, brokerInputDigest: record.inputDigest,
    presentationDigest: callerQuestionPresentationDigest(record.resolutionInput),
    callDeadlineAt: read().liveGenerations!.clocks.callDeadlineAt, version: record.resolutionInput.version,
    questions: record.resolutionInput.questions.map(callerIntentQuestionForTarget),
    results: record.resolutionInput.references.map(ref => {
      const candidate = callerIntentResultPublicationCandidates(read(), record.frozen.sessionId)
        .find(value => value.id === ref.id && value.digest === ref.digest);
      assert.ok(candidate); return candidate.candidate;
    }),
  });
  const prepareIntent = (record = read().callerRouting!.records[0]!, value = intentInput(record), operationId = uuid(95)) =>
    store.prepareCallerIntent(call.id, callerRouteRef(record), { operationId, input: value });
  const intentResponse = (record = read().callerRouting!.records[0]!, decision?: CallerIntentDecision,
    reservedAt = new Date().toISOString()): CallerIntentResponse => {
    assert.ok(record.intent);
    const value = record.intent.input;
    const fields = { v: 'phone.caller-intent.v1' as const, operationId: record.intent.operationId, bindingId,
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

test('exact raw source and resolver inputs are durable before classification and survive restart', t => {
  const f = fixture(t), record = f.prepare(), saved = f.bytes();
  assert.equal(record.decision, undefined); assert.equal(record.frozen.turn.text, ' Research Apple.\t');
  assert.equal(callerRoutePendingCaptures(f.read()), 1);
  assert.throws(() => requireCallerRoute(f.read(), f.request, callerRouteRef(record), 'task'), /decision_mismatch/);
  f.restart(); assert.equal(f.bytes(), saved);
  assert.deepEqual(findCallerRoute(f.read(), f.request), record);
  assert.deepEqual(f.prepare(), record); assert.equal(f.bytes(), saved);
  assert.deepEqual(callerRouteUsedSourceIds(f.read()), [f.frozen.turn.sourceEventId]);
  assert.deepEqual(callerRoutingSourceAnchors(f.read())[0]!.frozen, f.frozen);
});

test('exact decision is persisted before task preparation and cannot change route', t => {
  const f = fixture(t), record = f.prepare(), entry = f.entry();
  assert.throws(() => f.store().update(f.call.id, { delegations: { [entry.requestId]: entry } }), /decision_mismatch/);
  f.decide({ kind: 'task' });
  f.store().update(f.call.id, { delegations: { [entry.requestId]: entry } });
  const saved = f.bytes(); f.restart();
  assert.equal(requireCallerRoute(f.read(), f.request, callerRouteRef(record), 'task', 'recover').decision!.kind, 'task');
  f.decide({ kind: 'task' }); assert.equal(f.bytes(), saved);
  assert.throws(() => f.decide({ kind: 'conversation' }), /decision_changed/);
});

for (const field of ['text', 'offset', 'request', 'source', 'version'] as const) test(`changed ${field} cannot reuse a frozen route`, t => {
  const f = fixture(t); f.prepare(); const value = f.input();
  if (field === 'text') value.frozen.turn.text = ' Sell Apple.';
  if (field === 'offset') value.delegation.offsetMs++;
  if (field === 'request') value.delegation.requestId = 'other_request';
  if (field === 'source') value.frozen.firstEventId = 'other_event';
  if (field === 'version') value.resolutionInput.version = { ...value.resolutionInput.version, digest: 'c'.repeat(64) };
  assert.throws(() => f.prepare(value));
  assert.equal(f.read().callerRouting!.records.length, 1);
});

test('duplicate request identity cannot switch delegation or call', t => {
  const f = fixture(t); f.prepare();
  assert.throws(() => findCallerRoute(f.read(), { ...f.request, delegationId: 'other' }), /identity_changed/);
  assert.throws(() => findCallerRoute(f.read(), { ...f.request, callId: uuid(90) }), /identity_changed/);
  const value = f.input(); value.delegation.delegationId = 'other';
  assert.throws(() => f.prepare(value), /identity_changed/);
});

test('wrong resolution digest, offered target or reference cannot establish a decision', t => {
  const f = fixture(t), record = f.prepare(), ref = callerRouteRef(record);
  assert.throws(() => f.store().decideCallerRoute(f.call.id, ref, { inputDigest: 'd'.repeat(64), decision: { kind: 'task' } }), /identity_changed/);
  assert.throws(() => f.decide({ kind: 'task', contextReference: { id: 'unknown', digest: 'a'.repeat(64) } }), /reference_not_offered/);
  assert.throws(() => f.decide({ kind: 'answer', target: { kind: 'selected', bindingId: uuid(80), requestId: uuid(81),
    runId: uuid(82), questionId: uuid(83), questionRevision: 1, presentationDigest: 'a'.repeat(64), associationId: uuid(84) } }), /target_not_offered/);
  assert.equal(f.read().callerRouting!.records[0]!.decision, undefined);
});

for (const decision of [{ kind: 'conversation' }, { kind: 'clarify', reason: 'mixed_intent' },
  { kind: 'clarify', reason: 'ambiguous_target' }] as const) test(`${decision.kind} retains capture but cannot create a task or answer`, t => {
  const f = fixture(t), record = f.prepare(); f.decide(decision);
  const entry = f.entry();
  assert.throws(() => f.store().update(f.call.id, { delegations: { [entry.requestId]: entry } }), /decision_mismatch/);
  for (const kind of ['task', 'ordinary_answer', 'selected_answer'] as const)
    assert.throws(() => requireCallerRoute(f.read(), f.request, callerRouteRef(record), kind), /decision_mismatch/);
  f.store().update(f.call.id, { status: 'completed', endedAt: new Date().toISOString() });
  f.store().captureCallerRoute(f.call.id, callerRouteRef(record), f.receipt());
  assert.equal(callerRoutePendingCaptures(f.read()), 0);
});

test('stopped unresolved source is captured without gaining action authority', t => {
  const f = fixture(t), record = f.prepare();
  f.store().update(f.call.id, { status: 'completed', endedAt: new Date().toISOString() });
  f.restart(); assert.throws(() => f.decide({ kind: 'task' }), /stale_source/);
  f.store().captureCallerRoute(f.call.id, callerRouteRef(record), f.receipt());
  const bytes = f.bytes(); f.store().captureCallerRoute(f.call.id, callerRouteRef(record), { ...f.receipt(), replayed: true });
  assert.equal(f.bytes(), bytes); assert.equal(f.read().callerRouting!.records[0]!.decision, undefined);
  assert.equal(callerRoutePendingCaptures(f.read()), 0); assert.deepEqual(f.read().delegations, {});
});

test('capture must acknowledge the original body and exact binding', t => {
  const f = fixture(t), record = f.prepare();
  for (const receipt of [{ ...f.receipt(), bindingId: uuid(90) }, { ...f.receipt(), bodyDigest: '0'.repeat(64) },
    { ...f.receipt(), recordId: uuid(90) }]) assert.throws(() => f.store().captureCallerRoute(f.call.id, callerRouteRef(record), receipt));
  assert.equal(callerRoutePendingCaptures(f.read()), 1);
});

test('new caller revision makes an unresolved classification stale without changing the saved source', t => {
  const f = fixture(t); f.prepare(); const journal = structuredClone(f.read().liveGenerations!);
  journal.segments[0]!.transcript.callerRevision++;
  f.store().update(f.call.id, { liveGenerations: journal });
  assert.throws(() => f.decide({ kind: 'task' }), /stale_context/);
  assert.equal(f.read().callerRouting!.records[0]!.frozen.turn.text, f.frozen.turn.text);
});

test('generic update and reservation cannot bypass routing transitions', t => {
  const f = fixture(t), record = f.prepare();
  assert.throws(() => f.store().update(f.call.id, { callerRouting: undefined } as unknown as Parameters<BrokerStore['update']>[1]), /explicit transition/);
  const journal = structuredClone(f.read().callerRouting!); journal.records[0]!.decision = { kind: 'task' };
  assert.throws(() => f.store().update(f.call.id, { callerRouting: journal } as unknown as Parameters<BrokerStore['update']>[1]), /explicit transition/);
  assert.throws(() => f.store().reserve({ requestId: uuid(99), direction: 'outbound', purpose: 'Research.', number: '+12025550123',
    callerRouting: journal } as unknown as Parameters<BrokerStore['reserve']>[0]), /admitted call/);
  assert.equal(f.read().callerRouting!.records[0]!.inputDigest, record.inputDigest);
});

test('legacy absent journals remain absent and preserve read bytes', t => {
  const f = fixture(t), before = f.bytes(); f.restart();
  assert.equal(f.read().callerRouting, undefined); assert.equal(f.bytes(), before);
});

test('already owned ordinary source and deferred source cannot be newly classified', t => {
  const f = fixture(t), entry = f.entry();
  f.store().update(f.call.id, { delegations: { [entry.requestId]: entry } });
  assert.throws(() => f.prepare(), /source_already_owned/);
});

test('startup refuses changed journal digest and duplicate route records', t => {
  const f = fixture(t); f.prepare(); const file = path.join(f.directory, 'calls.json');
  const saved = JSON.parse(f.bytes()); f.store().close();
  saved.calls[f.call.id].callerRouting.records.push(structuredClone(saved.calls[f.call.id].callerRouting.records[0]));
  fs.writeFileSync(file, JSON.stringify(saved)); assert.throws(() => new BrokerStore(f.directory), /identity_changed/);
});

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

test('exact ordinary question decision does not become an ordinary task', t => {
  const f = fixture(t), pending = pendingQuestion(f), input = f.input();
  input.resolutionInput.questions = [pending.target];
  const record = f.prepare(input); f.decide({ kind: 'answer', target: pending.target });
  assert.equal(requireCallerRoute(f.read(), f.request, callerRouteRef(record), 'ordinary_answer').decision!.kind, 'answer');
  assert.throws(() => requireCallerRoute(f.read(), f.request, callerRouteRef(record), 'selected_answer'), /decision_mismatch/);
  const task = f.entry();
  assert.throws(() => f.store().update(f.call.id, { delegations: { ...f.read().delegations, [task.requestId]: task } }), /decision_mismatch/);
});

test('late question chunk rejection blocks the saved actionable route while retaining source', t => {
  const f = fixture(t), pending = pendingQuestion(f), input = f.input(); input.resolutionInput.questions = [pending.target];
  const record = f.prepare(input); f.decide({ kind: 'answer', target: pending.target });
  rejectBrokerPublication(f.store(), pending.identity, 'question_command_1', 'rejected');
  assert.throws(() => requireCallerRoute(f.read(), f.request, callerRouteRef(record), 'ordinary_answer'), /stale_target/);
  assert.equal(callerRoutePendingCaptures(f.read()), 1);
  assert.equal(requireCallerRoute(f.read(), f.request, callerRouteRef(record), 'ordinary_answer', 'recover').inputDigest, record.inputDigest);
});

test('changing the pending question during classification refuses the old target', t => {
  const f = fixture(t), pending = pendingQuestion(f), input = f.input(); input.resolutionInput.questions = [pending.target];
  f.prepare(input);
  const entry = structuredClone(f.read().delegations[pending.requestId]) as BrokerDelegationRecord;
  entry.questions![pending.questionId]!.question.state = 'expired';
  f.store().update(f.call.id, { delegations: { [entry.requestId]: entry } });
  assert.throws(() => f.decide({ kind: 'answer', target: pending.target }), /stale_target/);
  assert.equal(f.read().callerRouting!.records[0]!.decision, undefined);
});

test('new output during classification refuses an action while clarification remains available', t => {
  const f = fixture(t), pending = pendingQuestion(f); f.prepare();
  const snapshot = f.request.transcript;
  const intent = beginBrokerPublication(f.store(), pending.identity, pending.requestId,
    { id: 'later_progress', kind: 'progress', text: 'The comparison is pending.', speak: true, expectedCallerRevision: 1 }, snapshot)!;
  assert.ok(intent);
  assert.throws(() => f.decide({ kind: 'task' }), /stale_context/);
  f.decide({ kind: 'clarify', reason: 'stale_context' });
  assert.equal(callerRoutePendingCaptures(f.read()), 1);
});

test('one source cannot persist both a task and an answer through another store update', t => {
  const f = fixture(t), pending = pendingQuestion(f); f.prepare(); f.decide({ kind: 'task' });
  const task = f.entry();
  f.store().update(f.call.id, { delegations: { ...f.read().delegations, [task.requestId]: task } });
  const entry = structuredClone(f.read().delegations[pending.requestId]) as BrokerDelegationRecord;
  entry.questions![pending.questionId]!.reply = { status: 'prepared', callerRevision: 1, firstSequence: 1, lastSequence: 1,
    sourceSessionId: session, inputDigest: 'f'.repeat(64), input: {
      source: { callSid: sid, sourceEventId: f.frozen.turn.sourceEventId, revision: 1, itemId: f.frozen.turn.itemId,
        receivedAt: f.frozen.turn.occurredAt }, role: 'user', text: f.frozen.turn.text,
      reply: { clientReplyId: uuid(60), questionRevision: 1, answer: 'other', text: f.frozen.turn.text.trim() } } };
  assert.throws(() => f.store().update(f.call.id, { delegations: { ...f.read().delegations, [entry.requestId]: entry } }), /double_admission/);
  assert.equal((f.read().delegations[pending.requestId] as BrokerDelegationRecord).questions![pending.questionId]!.reply, undefined);
});

test('retired source keeps its decision for recovery and capture without fresh admission', t => {
  const f = fixture(t), record = f.prepare(); f.decide({ kind: 'task' });
  const journal = structuredClone(f.read().liveGenerations!), at = new Date().toISOString();
  journal.segments[0]!.state = 'closed'; journal.segments[0]!.closedAt = at;
  journal.segments[0]!.outputDiscarded = true;
  journal.segments.push({ ...journal.segments[0]!, generationId: uuid(70), sessionId: 'route_session_2', ordinal: 2,
    state: 'active', createdAt: at, readyAt: at, closedAt: undefined, outputDiscarded: false,
    inputFirstSample: undefined, inputLastSampleExclusive: undefined, transcript: { revision: 0, callerRevision: 0, fragments: [] } });
  journal.deliveryAttachment = { generationId: uuid(70), sessionId: 'route_session_2' };
  f.store().update(f.call.id, { liveSessionId: 'route_session_2', liveGenerations: journal }); f.restart();
  assert.throws(() => requireCallerRoute(f.read(), f.request, callerRouteRef(record), 'task'), /stale_source/);
  assert.equal(requireCallerRoute(f.read(), f.request, callerRouteRef(record), 'task', 'recover').frozen.sessionId, session);
  f.store().captureCallerRoute(f.call.id, callerRouteRef(record), f.receipt());
  assert.equal(callerRoutePendingCaptures(f.read()), 0); assert.deepEqual(f.read().delegations, {});
});

test('a recorded but unsubmitted deferred source cannot be claimed by routing', t => {
  const f = fixture(t), entry = f.entry();
  entry.submission = { ...entry.submission, source: { ...entry.submission.source, sourceEventId: uuid(91), itemId: 'prior_source' }, text: 'Prior work.' };
  entry.deferredTurns = [{ sourceSessionId: session, delegationId: 'old_deferred', sourceEventId: f.frozen.turn.sourceEventId,
    text: f.frozen.turn.text, firstSequence: 1, callerRevision: 1, status: 'recorded_not_submitted' }];
  f.store().update(f.call.id, { delegations: { [entry.requestId]: entry } });
  assert.throws(() => f.prepare(), /source_already_owned/);
});

test('uncertain downstream dispatch keeps the saved route across restart without changing request', t => {
  const f = fixture(t), record = f.prepare(); f.decide({ kind: 'task' });
  const entry = f.entry(); entry.status = 'dispatch_unknown';
  f.store().update(f.call.id, { delegations: { [entry.requestId]: entry } });
  const bytes = f.bytes(); f.restart();
  const recovered = requireCallerRoute(f.read(), f.request, callerRouteRef(record), 'task', 'recover');
  assert.equal(recovered.inputDigest, record.inputDigest); assert.equal(f.bytes(), bytes);
  assert.equal(f.read().delegations[entry.requestId]!.status, 'dispatch_unknown');
  assert.equal(Object.keys(f.read().delegations).length, 1);
  assert.throws(() => f.decide({ kind: 'clarify', reason: 'ambiguous_target' }), /decision_changed/);
});

test('first downstream admission after stop cannot use an earlier saved decision', t => {
  const f = fixture(t); f.prepare(); f.decide({ kind: 'task' });
  f.store().update(f.call.id, { status: 'completed', endedAt: new Date().toISOString() });
  const entry = f.entry();
  assert.throws(() => f.store().update(f.call.id, { delegations: { [entry.requestId]: entry } }), /stale_source/);
  assert.deepEqual(f.read().delegations, {});
});

test('operation input is durable before response and a decision alone cannot admit work', t => {
  const f = fixture(t), route = f.prepare(), ref = callerRouteRef(route);
  assert.throws(() => f.store().decideCallerRoute(f.call.id, ref,
    { inputDigest: route.inputDigest, decision: { kind: 'task' } }), /intent_unresolved/);
  assert.throws(() => f.recordResponse({} as CallerIntentResponse), /not_prepared/);
  const prepared = f.prepareIntent(), saved = f.bytes();
  assert.equal(prepared.intent?.response, undefined);
  assert.equal(prepared.intent?.input.source.text, f.frozen.turn.text);
  f.restart(); assert.equal(f.bytes(), saved); assert.deepEqual(f.prepareIntent(), prepared);
  assert.equal(f.bytes(), saved); assert.equal(callerRoutePendingCaptures(f.read()), 1);
  const pending = f.intentResponse(); f.recordResponse(pending);
  assert.throws(() => f.store().decideCallerRoute(f.call.id, ref,
    { inputDigest: route.inputDigest, decision: { kind: 'task' } }), /intent_unresolved/);
  const resolved = f.intentResponse(f.read().callerRouting!.records[0]!, { kind: 'task' }, pending.reservedAt);
  f.recordResponse(resolved); f.decide({ kind: 'task' });
  assert.doesNotThrow(() => requireCallerRouteDispatch(f.read(), f.frozen.turn.sourceEventId, Date.parse(resolved.reservedAt)));
});

for (const key of ['text', 'session', 'generation', 'version', 'deadline', 'digest', 'presentation'] as const)
  test(`intent ${key} cannot diverge from the durable caller source`, t => {
    const f = fixture(t), route = f.prepare(), input = f.intentInput(route);
    if (key === 'text') input.source.text = 'Buy Apple.';
    if (key === 'session') input.sourceSessionId = 'unrelated_session';
    if (key === 'generation') input.generationId = uuid(80);
    if (key === 'version') input.version.digest = 'f'.repeat(64);
    if (key === 'deadline') input.callDeadlineAt = new Date(Date.parse(input.callDeadlineAt) + 1).toISOString();
    if (key === 'digest') input.brokerInputDigest = 'e'.repeat(64);
    if (key === 'presentation') input.presentationDigest = 'd'.repeat(64);
    assert.throws(() => f.prepareIntent(route, input), /caller_intent/);
    assert.equal(f.read().callerRouting!.records[0]!.intent, undefined);
  });

test('operation ID, full input and first response timing survive exact retries', t => {
  const f = fixture(t); f.prepare(); const prepared = f.prepareIntent();
  assert.throws(() => f.prepareIntent(prepared, prepared.intent!.input, uuid(96)), /operation_changed/);
  const input = structuredClone(prepared.intent!.input); input.source.text = 'Other work.';
  assert.throws(() => f.prepareIntent(prepared, input), /operation_changed/);
  const pending = f.intentResponse(); f.recordResponse(pending); const saved = f.bytes();
  f.restart(); f.recordResponse(pending); assert.equal(f.bytes(), saved);
  const changed = { ...pending, reservedAt: new Date(Date.parse(pending.reservedAt) + 1).toISOString(),
    validUntil: new Date(Date.parse(pending.validUntil) + 1).toISOString() };
  assert.throws(() => f.recordResponse(changed), /reservation_changed/);
  const resolved = f.intentResponse(f.read().callerRouting!.records[0]!, { kind: 'task' }, pending.reservedAt);
  f.recordResponse(resolved); const resolvedBytes = f.bytes(); f.restart();
  f.recordResponse(resolved); assert.equal(f.bytes(), resolvedBytes);
  assert.throws(() => f.recordResponse(pending), /resolution_changed/);
  assert.throws(() => f.recordResponse({ ...resolved, state: 'resolved', decision: { kind: 'conversation' } }), /resolution_changed/);
});

test('all response echoes and the API input digest remain bound to one operation', t => {
  const f = fixture(t); f.prepare(); f.prepareIntent(); const response = f.intentResponse();
  const mutations: Array<Partial<CallerIntentResponse>> = [
    { operationId: uuid(88) }, { bindingId: uuid(89) }, { inputDigest: 'f'.repeat(64) },
    { brokerInputDigest: 'e'.repeat(64) }, { presentationDigest: 'd'.repeat(64) },
    { sourceSessionId: 'other_session' }, { generationId: uuid(90) },
    { callDeadlineAt: new Date(Date.parse(response.callDeadlineAt) + 1).toISOString() },
    { validUntil: new Date(Date.parse(response.validUntil) + 1).toISOString() },
    { version: { ...response.version, contextEpoch: '99' } },
  ];
  for (const mutation of mutations) assert.throws(() => f.recordResponse({ ...response, ...mutation }), /identity_changed/);
  assert.equal(f.read().callerRouting!.records[0]!.intent?.response, undefined);
});

test('local abort clarification retains pending API receipt without converting it into authority', t => {
  const f = fixture(t), record = f.prepare(); f.prepareIntent(); const pending = f.intentResponse(); f.recordResponse(pending);
  f.decide({ kind: 'clarify', reason: 'unavailable' });
  const resolved = f.intentResponse(f.read().callerRouting!.records[0]!, { kind: 'task' }, pending.reservedAt);
  f.recordResponse(resolved); f.restart();
  assert.equal(f.read().callerRouting!.records[0]!.intent?.response?.state, 'resolved');
  assert.equal(f.read().callerRouting!.records[0]!.decision?.kind, 'clarify');
  assert.throws(() => requireCallerRouteDispatch(f.read(), record.frozen.turn.sourceEventId, Date.parse(resolved.reservedAt)), /intent_unresolved/);
  assert.throws(() => f.decide({ kind: 'task' }), /decision_changed/);
  f.store().captureCallerRoute(f.call.id, callerRouteRef(record), f.receipt());
  assert.equal(callerRoutePendingCaptures(f.read()), 0);
});

test('queued first dispatch keeps exact receipt without requiring its old caption or publication frontier', t => {
  const f = fixture(t), pending = pendingQuestion(f); f.prepare(); f.decide({ kind: 'task' });
  const route = f.read().callerRouting!.records[0]!, receipt = route.intent!.response!;
  const generation = structuredClone(f.read().liveGenerations!); generation.segments[0]!.transcript.callerRevision++;
  f.store().update(f.call.id, { liveGenerations: generation });
  beginBrokerPublication(f.store(), pending.identity, pending.requestId,
    { id: 'later_output', kind: 'progress', text: 'The comparison is pending.', speak: true, expectedCallerRevision: 1 }, f.request.transcript);
  assert.throws(() => requireCallerRoute(f.read(), f.request, callerRouteRef(route), 'task'), /stale_context/);
  assert.doesNotThrow(() => requireCallerRouteDispatch(f.read(), route.frozen.turn.sourceEventId, Date.parse(receipt.validUntil) - 1));
  assert.throws(() => requireCallerRouteDispatch(f.read(), route.frozen.turn.sourceEventId, Date.parse(receipt.validUntil)), error => {
    assert.ok(error instanceof CallerRouteExpiredError);
    assert.equal(error.validUntil, receipt.validUntil); assert.equal(error.inputDigest, receipt.inputDigest);
    assert.notEqual(error.inputDigest, route.inputDigest); return true;
  });
  f.restart(); assert.equal(f.read().callerRouting!.records[0]!.intent!.response!.validUntil, receipt.validUntil);
});

test('stopped calls cannot use an unexpired intent but retain GET-only recovery evidence', t => {
  const f = fixture(t), route = f.prepare(); f.decide({ kind: 'task' });
  const response = f.read().callerRouting!.records[0]!.intent!.response!;
  f.store().update(f.call.id, { endedAt: new Date().toISOString(), status: 'completed' });
  assert.throws(() => requireCallerRouteDispatch(f.read(), route.frozen.turn.sourceEventId, Date.parse(response.reservedAt)), /stale_source/);
  f.restart(); assert.deepEqual(requireCallerRoute(f.read(), f.request, callerRouteRef(route), 'task', 'recover').intent?.response, response);
});

test('saved pending operation tampering fails startup and cannot disappear through later writes', t => {
  const f = fixture(t); f.prepare(); f.prepareIntent(); f.recordResponse(f.intentResponse());
  const file = path.join(f.directory, 'calls.json'), saved = JSON.parse(f.bytes()); f.store().close();
  saved.calls[f.call.id].callerRouting.records[0].intent.input.source.text = 'Different work.';
  fs.writeFileSync(file, JSON.stringify(saved));
  assert.throws(() => new BrokerStore(f.directory), /input_changed/);
});

test('store startup and every write reject invented API refusal and expiry evidence', t => {
  const f = fixture(t); f.prepare(); f.decide({ kind: 'task' }); const entry = f.entry();
  f.store().update(f.call.id, { delegations: { [entry.requestId]: entry } });
  const invalid: BrokerDelegationRecord = { ...entry, status: 'admission_expired',
    admissionExpiry: { origin: 'caller_intent_expiry', validUntil: new Date().toISOString(), inputDigest: 'f'.repeat(64) } };
  assert.throws(() => f.store().update(f.call.id, { delegations: { [entry.requestId]: invalid } }), /invalid_saved_admission_expiry/);
  const refused: BrokerDelegationRecord = { ...entry, status: 'admission_refused',
    admissionRefusal: { origin: 'api_non_admission', status: 409, code: 'delivery_association_mismatch', submissionDigest: 'e'.repeat(64) } };
  assert.throws(() => f.store().update(f.call.id, { delegations: { [entry.requestId]: refused } }), /admission_refusal/);
  const file = path.join(f.directory, 'calls.json'), saved = JSON.parse(f.bytes()); f.store().close();
  saved.calls[f.call.id].delegations[entry.requestId] = invalid; fs.writeFileSync(file, JSON.stringify(saved));
  assert.throws(() => new BrokerStore(f.directory), /invalid_saved_admission_expiry/);
});

function publishedResult(f: ReturnType<typeof fixture>, n: number) {
  const entry = f.entry(), requestId = uuid(n), messageId = uuid(n + 1), eventId = uuid(n + 2), runId = uuid(n + 3);
  const text = 'The fetched account result is saved.';
  entry.requestId = requestId; entry.status = 'completed';
  entry.submission = { ...entry.submission, source: { ...entry.submission.source, sourceEventId: uuid(n + 4), itemId: `prior_${n}` }, text: 'Prior work.' };
  entry.primaryResult = { requestId, bindingId, state: 'completed', assistant: { messageId, eventId, sequence: '1', text } };
  f.store().update(f.call.id, { delegations: { ...f.read().delegations, [requestId]: entry } });
  const response: OrdinaryPublicationEvidence['response'] = { bindingId, callSid: sid, requestId, version: f.version,
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

test('offered result order and exact publication attribution bind the task reference', t => {
  const f = fixture(t), first = publishedResult(f, 100), second = publishedResult(f, 110);
  first.publish(1); second.publish(1);
  const candidates = callerIntentResultPublicationCandidates(f.read(), session); assert.equal(candidates.length, 2);
  const input = f.input(); input.resolutionInput.references = candidates.map(({ id, digest }) => ({ id, digest }));
  const route = f.prepare(input), apiInput = f.intentInput(route);
  const changed = structuredClone(apiInput); changed.results.reverse();
  assert.throws(() => f.prepareIntent(route, changed), /candidate_changed/);
  f.prepareIntent(route, apiInput);
  f.recordResponse(f.intentResponse(f.read().callerRouting!.records[0]!, { kind: 'task', reference: candidates[0]!.reference }));
  assert.throws(() => f.store().decideCallerRoute(f.call.id, callerRouteRef(route), { inputDigest: route.inputDigest,
    decision: { kind: 'task', contextReference: input.resolutionInput.references[1]! } }), /intent_reference_changed/);
  f.decide({ kind: 'task', contextReference: input.resolutionInput.references[0]! });
  const saved = f.bytes(); f.restart(); assert.equal(f.bytes(), saved);
  assert.deepEqual(f.read().callerRouting!.records[0]!.intent!.input.results, candidates.map(c => c.candidate));
});

test('a later accepted publication attempt remains historically verifiable after rejection and restart', t => {
  const f = fixture(t), prior = publishedResult(f, 100), firstEvent = prior.publish(1);
  rejectBrokerPublication(f.store(), prior.identity, firstEvent, 'rejected');
  const laterEvent = prior.publish(2), candidates = callerIntentResultPublicationCandidates(f.read(), session);
  assert.equal(candidates.length, 1); const candidate = candidates[0]!;
  const input = f.input(); input.resolutionInput.references = [{ id: candidate.id, digest: candidate.digest }];
  const route = f.prepare(input); f.prepareIntent(route);
  const response = f.intentResponse(f.read().callerRouting!.records[0]!, { kind: 'task', reference: candidate.reference });
  f.recordResponse(response);
  rejectBrokerPublication(f.store(), prior.identity, laterEvent, 'rejected');
  assert.throws(() => f.store().decideCallerRoute(f.call.id, callerRouteRef(route), { inputDigest: route.inputDigest,
    decision: { kind: 'task', contextReference: input.resolutionInput.references[0]! } }), /candidate_changed/);
  f.restart();
  assert.deepEqual(f.read().callerRouting!.records[0]!.intent!.response, response);
  f.decide({ kind: 'clarify', reason: 'stale_context' });
  f.store().captureCallerRoute(f.call.id, callerRouteRef(route), f.receipt());
  assert.equal(callerRoutePendingCaptures(f.read()), 0);
});
