import assert from 'node:assert/strict';
import test, { type TestContext } from 'node:test';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { brokerIdentity, literalPhoneAnswer, liveResultChunks } from '../src/broker/backend.js';
import { canonicalJson } from '../src/outbound/canonical.js';
import { beginSelectedQuestionPublication, settleBrokerPublication, selectedQuestionPublicationReceiptsAccepted,
  rejectBrokerPublication, type BrokerPublicationReceipt } from '../src/broker/publications.js';
import { selectedQuestionPresentation, type SelectedQuestionSourceEvidence } from '../src/broker/selectedQuestionVoice.js';
import type { EligibleDeliveryTarget } from '../src/broker/eligibleContext.js';
import { BrokerStore, type BrokerCall } from '../src/broker/store.js';
import { createSelectedQuestionHost, type SelectedQuestionAnswer, type SelectedQuestionHostOptions,
  type SelectedQuestionJournal, type SelectedQuestionSeed } from '../src/broker/selectedQuestion.js';
import { createRequestDeliveryJournal, retainRequestDeliveryAssociation, createRequestDeliveryQuestionRecord,
  PHONE_REQUEST_DELIVERY_VERSION, type RequestDeliveryJournal, type RequestDeliveryQuestionResult } from '../src/broker/requestDelivery.js';
import { PrimaryApiError, type PrimaryTurn, type PrimaryTurnReceipt } from '../src/broker/primaryApi.js';
import type { LiveGenerationJournal } from '../src/broker/liveGeneration.js';
import { turnBodyDigest } from '../src/broker/reconciliation.js';
import { HARK_PHONE_PURPOSE, harkSourceDigest, type PhoneHarkSnapshot } from '../src/broker/harkAssignment.js';

const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const sid = `CA${'a'.repeat(32)}`;
const owner = uuid(2), primary = uuid(3), thread = uuid(4), binding = uuid(5), request = uuid(6), questionId = uuid(7);
const session = 'selected_initial', nextSession = 'selected_successor';
const continuation = { originalRequestId: request, questionId, questionRevision: 1 as const };

function fixture(t: TestContext, options: { admitted?: boolean; generation?: boolean; owner?: boolean; expired?: boolean } = {}) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-selected-question-store-'));
  const instances: BrokerStore[] = [];
  let store = new BrokerStore(directory); instances.push(store);
  const call = store.reserve({ requestId: uuid(1), direction: 'outbound', purpose: 'Continue the selected comparison.',
    number: '+12025550123', callSid: sid, continuation }, 'authenticated_broker_control').call;
  const at = call.createdAt;
  const deadline = new Date(Date.now() + (options.expired ? -60_000 : 300_000)).toISOString();
  const ref = { assignmentId: call.requestId, revision: 1 as const, digest: 'a'.repeat(64) };
  if (options.admitted !== false) store.recordPrimaryBinding(call.id, { bindingId: binding, callSid: sid, state: 'open',
    callAssignmentRef: ref, context: { primaryDexterId: primary, threadId: thread, displayName: 'Dexter', messages: [] } });
  const version = { ownerPrincipalId: owner, primaryDexterId: primary, threadId: thread,
    contextEpoch: '1', threadEventSequence: '1', digest: 'b'.repeat(64) };
  const generations: LiveGenerationJournal = { version: 1, callId: call.id,
    binding: { bindingId: binding, callSid: sid, ...(options.owner === false ? {} : { ownerPrincipalId: owner }),
      primaryDexterId: primary, threadId: thread, callAssignmentRef: ref },
    clocks: { callDeadlineAt: deadline, mediaStartedAt: at }, phase: 'active', inputNextSample: 0,
    deliveryAttachment: { generationId: uuid(10), sessionId: session },
    segments: [{ generationId: uuid(10), ordinal: 1, state: 'active', sessionId: session, createdAt: at, readyAt: at,
      version, pageDigest: 'c'.repeat(64), transcript: { revision: 0, callerRevision: 0, fragments: [] }, outputDiscarded: false }] };
  store.update(call.id, { status: 'in-progress', mediaStartedAt: at, liveSessionId: session,
    ...(options.admitted !== false && options.generation !== false ? { liveGenerations: generations } : {}) });
  const seed: SelectedQuestionSeed = { v: 'broker.selected-question-seed.v1', deliveryCall: { callId: call.id, sessionId: session },
    delivery: { bindingId: binding, callSid: sid }, scope: { ownerPrincipalId: owner, primaryDexterId: primary, threadId: thread },
    request: { v: PHONE_REQUEST_DELIVERY_VERSION, ...continuation }, callDeadlineAt: deadline };
  const empty = (): SelectedQuestionJournal => ({ v: 'broker.selected-question-host.v1', initial: structuredClone(seed),
    questions: [{ seed: structuredClone(seed), admissions: [] }] });
  const file = path.join(directory, 'calls.json');
  const bytes = () => fs.readFileSync(file, 'utf8');
  const restart = () => { store.close(); store = new BrokerStore(directory); instances.push(store); return store; };
  const rotate = () => {
    const current = store.get(call.id)!, old = current.liveGenerations!, observedAt = new Date().toISOString();
    store.update(call.id, { liveSessionId: nextSession, liveGenerations: { ...old, phase: 'active',
      deliveryAttachment: { generationId: uuid(11), sessionId: nextSession },
      segments: [...old.segments.map(segment => ({ ...segment, state: 'closed' as const, closedAt: observedAt,
        providerFinalized: true, transcriptPersisted: true, outputDiscarded: true })),
      { ...generations.segments[0]!, generationId: uuid(11), ordinal: 2, sessionId: nextSession,
        createdAt: observedAt, readyAt: observedAt, version: { ...version, contextEpoch: '2' } }] } });
  };
  t.after(() => { for (const item of instances) item.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  return { call, seed, at, deadline, empty, bytes, file, directory, restart, rotate, store: () => store };
}
type Fixture = ReturnType<typeof fixture>;

function associated(seed: SelectedQuestionSeed): RequestDeliveryJournal {
  const association = { id: seed.request.questionId === questionId ? uuid(8) : uuid(18), v: PHONE_REQUEST_DELIVERY_VERSION,
    delivery: seed.delivery, execution: { bindingId: uuid(9), requestId: request, runId: uuid(12) },
    question: { id: seed.request.questionId, revision: 1 as const }, createdAt: new Date().toISOString() };
  return retainRequestDeliveryAssociation(createRequestDeliveryJournal({ deliveryCall: seed.deliveryCall, scope: seed.scope,
    request: seed.request, target: { delivery: seed.delivery, execution: association.execution, question: association.question } }),
  { association, replayed: false });
}
function response(journal: RequestDeliveryJournal, state: 'pending' | 'answered' = 'pending'): RequestDeliveryQuestionResult {
  const receivedAt = new Date().toISOString();
  return { bindingId: journal.target.execution.bindingId, requestId: request, deliveryAssociation: journal.association!,
    state: state === 'pending' ? 'waiting_for_reply' : 'running', replayed: false,
    question: { id: journal.target.question.id, revision: 1, runId: journal.target.execution.runId, threadId: thread,
      agent: { id: primary, name: 'Dexter' }, state, runState: state === 'pending' ? 'waiting_for_reply' : 'running',
      question: 'Continue the comparison?', context: 'Research only.', proposedAnswer: 'Compare both stocks.',
      yesMeaning: 'Compare both.', noMeaning: 'Stop the comparison.', createdAt: receivedAt,
      expiresAt: new Date(Date.now() + 240_000).toISOString(), reply: state === 'pending' ? null : {
        ...(journal.reply?.input.reply ?? { questionRevision: 1, clientReplyId: uuid(40), answer: 'yes', text: null }),
        interpretedAnswer: 'Compare both.', receivedAt } } };
}
function answer(sourceSessionId = session, n = 50): SelectedQuestionAnswer {
  return { questionId, sourceSessionId, delegationId: `delegation_${n}`, presentationId: `presentation_${n}`,
    input: { source: { callSid: sid, sourceEventId: uuid(n), revision: 1, itemId: `answer_${n}`, receivedAt: new Date().toISOString() },
      role: 'user', text: ' Yes, compare both.\t', reply: { questionRevision: 1, clientReplyId: uuid(n + 1), answer: 'yes', text: null } } };
}
const captureReceipt = (turn: PrimaryTurn): PrimaryTurnReceipt => ({ bindingId: binding, sourceEventId: turn.sourceEventId,
  revision: 1, recordId: turn.sourceEventId, revisionRecordId: uuid(80), bodyDigest: turnBodyDigest(turn), replayed: false });
function publishSelected(f: Fixture, journal: RequestDeliveryJournal, target: EligibleDeliveryTarget = {
  kind: 'question', questionId: journal.target.question.id, questionRevision: 1,
}, options: { text?: string; beforeEvidence?: (receipts: BrokerPublicationReceipt[], identity: { callId: string; callSid: string; bindingId: string; sessionId: string }) => void } = {}) {
  const call = f.store().get(f.call.id)!, attachment = call.liveGenerations!.deliveryAttachment!;
  const segment = call.liveGenerations!.segments.find(item => item.generationId === attachment.generationId)!;
  const snapshot = segment.transcript, at = new Date().toISOString();
  const pending = { ...response(journal).question, state: 'pending' as const, reply: null };
  const text = options.text ?? (target.kind === 'question'
    ? `${pending.question} ${pending.context} ${pending.proposedAnswer} Yes: ${pending.yesMeaning} No: ${pending.noMeaning}`
    : 'The saved comparison is complete.');
  const source = { messageId: uuid(81), eventSequence: '1', provenance: {
    messageCreatedAt: at, eventId: uuid(82), eventOccurredAt: at, runId: journal.target.execution.runId } };
  const eligible: Parameters<typeof beginSelectedQuestionPublication>[2] = { ...journal.target.delivery,
    requestId: request, version: segment.version, deliveryAssociation: journal.association!,
    delivery: target.kind === 'question' ? { state: 'ready', source: { kind: 'question', ...source }, text, question: pending }
      : { state: 'ready', source: { kind: 'assistant', ...source }, text } };
  const identity = { callId: call.id, ...journal.target.delivery, sessionId: attachment.sessionId };
  const receipts: BrokerPublicationReceipt[] = [];
  for (const [index, chunk] of liveResultChunks(text).entries()) {
    const id = `selected_${call.publications?.records.length ?? 0}_${snapshot.callerRevision}_${index}`;
    const intent = beginSelectedQuestionPublication(f.store(), identity, eligible,
      { id, kind: 'result', text: chunk, speak: true, expectedCallerRevision: snapshot.callerRevision }, snapshot, index)!;
    assert.ok(intent); assert.equal(f.store().get(call.id)!.publications!.records.at(-1)!.outcome, undefined);
    const eventId = `command_${intent.record.publicationSequence}`;
    settleBrokerPublication(f.store(), intent.record, { eventId, status: 'accepted',
      observedCallerRevision: snapshot.callerRevision, observedTranscriptSequence: snapshot.revision }, snapshot, false);
    receipts.push({ publicationSequence: intent.record.publicationSequence, sessionId: attachment.sessionId, eventId });
  }
  options.beforeEvidence?.(receipts, identity);
  if (target.kind === 'question') {
    const presentation = selectedQuestionPresentation(f.store().get(call.id)!, journal, {
      outcome: { requestId: request, attachment, response: eligible, publication: { eventId: receipts.at(-1)!.eventId,
        status: 'accepted', observedCallerRevision: snapshot.callerRevision, observedTranscriptSequence: snapshot.revision,
        publicationReceipts: receipts } }, snapshot, acceptedAt: at,
      inputThroughMs: segment.inputFirstSample === undefined ? 0 : (segment.inputLastSampleExclusive! - segment.inputFirstSample) / 8,
    });
    assert.ok(presentation);
    const before = f.store().get(call.id)!.selectedQuestionVoice ?? { v: 'broker.selected-question-voice.v1' as const, presentations: [], sources: [] };
    f.store().recordSelectedQuestionVoice(call.id, { ...before, presentations: [...before.presentations, presentation] });
  }
  return { eligible, receipts, identity };
}
function qualifiedAnswer(f: Fixture, n = 50): SelectedQuestionAnswer {
  let call = f.store().get(f.call.id)!;
  const slot = call.selectedQuestion!.questions.at(-1)!, sessionId = call.liveSessionId!;
  if (call.selectedQuestionVoice?.presentations.at(-1)?.sessionId !== sessionId) {
    publishSelected(f, slot.record!.base); call = f.store().get(call.id)!;
  }
  const presentation = call.selectedQuestionVoice!.presentations.at(-1)!;
  const generations = structuredClone(call.liveGenerations!), segment = generations.segments.find(s => s.sessionId === sessionId)!;
  const firstSequence = segment.transcript.revision + 1, callerRevision = segment.transcript.callerRevision + 1;
  const startMs = Math.max(presentation.inputThroughMs + 1300, presentation.lastCallerEndMs + 1300);
  const eventId = `caller_${n}`, text = ' Yes, compare both.\t', endMs = startMs + 200;
  segment.inputFirstSample ??= generations.inputNextSample;
  segment.inputLastSampleExclusive = segment.inputFirstSample + (endMs + 100) * 8;
  generations.inputNextSample = segment.inputLastSampleExclusive;
  segment.transcript = { revision: firstSequence, callerRevision, fragments: [...segment.transcript.fragments,
    { eventId, sequence: firstSequence, role: 'user', text, startMs, endMs }] };
  f.store().update(call.id, { liveGenerations: generations });
  const sourceEventId = brokerIdentity('source', call.id, sessionId, eventId);
  const occurredAt = new Date(Date.parse(call.mediaStartedAt!) + segment.inputFirstSample / 8 + startMs).toISOString();
  const input: SelectedQuestionAnswer = { questionId: slot.seed.request.questionId, sourceSessionId: sessionId,
    delegationId: `delegation_${n}`, presentationId: presentation.id, input: {
      source: { callSid: sid, sourceEventId, revision: 1, itemId: `live-turn-${sourceEventId}`, receivedAt: occurredAt },
      role: 'user', text, reply: { questionRevision: 1, clientReplyId: brokerIdentity('question-reply', call.id,
        sessionId, request, slot.seed.request.questionId, sourceEventId), ...literalPhoneAnswer(text) } } };
  const source: SelectedQuestionSourceEvidence = { answer: input, delegationRequestId: `request_${n}`,
    frozen: { sessionId, firstEventId: eventId, callerRevision, firstSequence, lastSequence: firstSequence,
      startMs, endMs, audioThroughMs: endMs + 100, observedSpeechAtMs: Date.parse(presentation.acceptedAt) + 1,
      turn: { sourceEventId, revision: 1, itemId: input.input.source.itemId, role: 'user', text, occurredAt } } };
  const before = f.store().get(call.id)!.selectedQuestionVoice!;
  f.store().recordSelectedQuestionVoice(call.id, { ...before, sources: [...before.sources, source] });
  return input;
}
function saveCapture(f: Fixture, turn: PrimaryTurn, receipt = captureReceipt(turn)): PrimaryTurnReceipt {
  const evidence = f.store().get(f.call.id)!.selectedQuestionVoice!;
  f.store().recordSelectedQuestionVoice(f.call.id, { ...evidence,
    sources: evidence.sources.map(source => source.frozen.turn.sourceEventId === turn.sourceEventId ? { ...source, capture: receipt } : source) });
  return receipt;
}
function validAnswer(f: Fixture, journal: RequestDeliveryJournal, input: SelectedQuestionAnswer): boolean {
  const call = f.store().get(f.call.id)!, evidence = call.selectedQuestionVoice;
  const source = evidence?.sources.find(item => canonicalJson(item.answer) === canonicalJson(input));
  const presentation = source && evidence!.presentations.find(item => item.id === input.presentationId);
  return Boolean(presentation && canonicalJson(presentation.response.deliveryAssociation) === canonicalJson(journal.association)
    && selectedQuestionPublicationReceiptsAccepted(call, presentation.response, presentation.sessionId, presentation.publicationReceipts));
}
function hostOptions(f: Fixture): SelectedQuestionHostOptions {
  return { seed: f.seed, load: () => f.store().get(f.call.id)?.selectedQuestion,
    persist: value => { f.store().recordSelectedQuestion(f.call.id, value); },
    active: () => f.store().get(f.call.id)?.status === 'in-progress',
    association: { async associate(seed) { return associated(seed); } },
    requests: { async associate() { throw new Error('Unexpected alias association'); },
      async result(journal) { return response(journal); }, async question(journal) { return response(journal); },
      async reply(journal) { return response(journal, 'answered'); } },
    async capture(_binding, turn) { return saveCapture(f, turn); }, async deliver(journal, target) { publishSelected(f, journal, target); },
    replyIsQualified: (journal, input) => validAnswer(f, journal, input), admittedReplyIsValid: (journal, input) => validAnswer(f, journal, input) };
}

test('selected seed persists atomically, returns independent copies and reloads without rewriting exact bytes', t => {
  const f = fixture(t), input = f.empty();
  const saved = f.store().recordSelectedQuestion(f.call.id, input), before = f.bytes();
  input.initial.deliveryCall.sessionId = 'mutated_input'; saved.initial.deliveryCall.sessionId = 'mutated_return';
  assert.deepEqual(JSON.parse(before).calls[f.call.id].selectedQuestion, f.empty());
  assert.deepEqual(f.restart().get(f.call.id)!.selectedQuestion, f.empty()); assert.equal(f.bytes(), before);
  assert.deepEqual(f.store().recordSelectedQuestion(f.call.id, f.empty()), f.empty()); assert.equal(f.bytes(), before);
});

for (const missing of ['binding', 'generation', 'owner', 'expired', 'inactive'] as const) {
  test(`new selected seed refuses ${missing} admission evidence without a write`, t => {
    const f = fixture(t, { admitted: missing !== 'binding', generation: missing !== 'generation', owner: missing !== 'owner', expired: missing === 'expired' });
    if (missing === 'inactive') {
      const generations = f.store().get(f.call.id)!.liveGenerations!;
      f.store().update(f.call.id, { liveGenerations: { ...generations, phase: 'idle', deliveryAttachment: undefined } });
    }
    const before = f.bytes(); assert.throws(() => f.store().recordSelectedQuestion(f.call.id, f.empty())); assert.equal(f.bytes(), before);
  });
}

test('seed must match every saved call, continuation, scope, source and deadline identity', t => {
  const f = fixture(t);
  const changes: Array<(seed: SelectedQuestionSeed) => void> = [
    value => { value.deliveryCall.callId = uuid(90); }, value => { value.deliveryCall.sessionId = 'unregistered'; },
    value => { value.delivery.bindingId = uuid(90); }, value => { value.delivery.callSid = `CA${'b'.repeat(32)}`; },
    value => { value.scope.ownerPrincipalId = uuid(90); }, value => { value.scope.primaryDexterId = uuid(90); },
    value => { value.scope.threadId = uuid(90); }, value => { value.request.originalRequestId = uuid(90); },
    value => { value.request.questionId = uuid(90); }, value => { value.callDeadlineAt = new Date(Date.parse(f.deadline) + 1).toISOString(); },
  ];
  const before = f.bytes();
  for (const change of changes) {
    const next = f.empty(); change(next.initial); next.questions[0]!.seed = structuredClone(next.initial);
    assert.throws(() => f.store().recordSelectedQuestion(f.call.id, next)); assert.equal(f.bytes(), before);
  }
  const alreadyAssociated = f.empty(); alreadyAssociated.questions[0]!.record = createRequestDeliveryQuestionRecord(associated(f.seed));
  assert.throws(() => f.store().recordSelectedQuestion(f.call.id, alreadyAssociated), /active initial generation/);
});

test('runtime reservation and generic update cannot install, change or remove selected-question history', t => {
  const f = fixture(t), before = f.bytes();
  const patch = (value: unknown) => ({ selectedQuestion: value }) as unknown as Parameters<BrokerStore['update']>[1];
  assert.throws(() => f.store().update(f.call.id, patch(f.empty())), /explicit transition/);
  assert.throws(() => f.store().reserve({ requestId: uuid(90), direction: 'outbound', purpose: 'Other call', number: '+12025550124',
    selectedQuestion: f.empty() } as Parameters<BrokerStore['reserve']>[0]), /admitted call/);
  assert.equal(f.bytes(), before);
  f.store().recordSelectedQuestion(f.call.id, f.empty()); const admitted = f.bytes();
  assert.throws(() => f.store().update(f.call.id, patch(undefined)), /explicit transition/);
  assert.throws(() => f.store().update(f.call.id, patch({ ...f.empty(), stoppedAt: new Date().toISOString() })), /explicit transition/);
  assert.equal(f.bytes(), admitted);
  f.store().update(f.call.id, { metrics: { receivedAudioBytes: 0, sentAudioBytes: 0, playedAudioBytes: 0, clearedAudioBytes: 0 } });
  assert.deepEqual(f.store().get(f.call.id)!.selectedQuestion, f.empty());
});

test('a retained initial session survives generation rotation, q2 association and restart', async t => {
  const f = fixture(t), options = hostOptions(f), host = createSelectedQuestionHost(options); await host.start();
  const initial = host.snapshot().initial; f.rotate();
  options.requests.question = async journal => response(journal, 'answered');
  options.requests.result = async journal => {
    const next = response(associated({ ...f.seed, request: { ...f.seed.request, questionId: uuid(17) } }));
    return { ...next, deliveryAssociation: journal.association! };
  };
  await host.poll(); const saved = f.store().get(f.call.id)!.selectedQuestion!;
  assert.deepEqual(saved.initial, initial); assert.equal(saved.questions.length, 2);
  assert.equal(saved.questions[1]!.record!.base.target.execution.runId, uuid(12));
  assert.equal(saved.questions[1]!.seed.deliveryCall.sessionId, session);
  const before = f.bytes(); assert.deepEqual(f.restart().get(f.call.id)!.selectedQuestion, saved); assert.equal(f.bytes(), before);
});

test('new selection cannot use the retained closed generation when a different one is active', t => {
  const f = fixture(t); f.rotate(); const before = f.bytes();
  assert.throws(() => f.store().recordSelectedQuestion(f.call.id, f.empty()), /active initial generation/); assert.equal(f.bytes(), before);
  const next = f.empty(); next.initial.deliveryCall.sessionId = nextSession; next.questions[0]!.seed = structuredClone(next.initial);
  assert.deepEqual(f.store().recordSelectedQuestion(f.call.id, next), next);
});

test('new answers require the current source session while original question identity stays unchanged', async t => {
  const f = fixture(t), options = hostOptions(f); options.replyIsQualified = () => true;
  const host = createSelectedQuestionHost(options); await host.start(); f.rotate();
  const before = f.bytes(); await assert.rejects(host.answer(answer()), /active source generation/); assert.equal(f.bytes(), before);
  await assert.rejects(host.answer(answer('foreign_session')), /source identity mismatch/); assert.equal(f.bytes(), before);
  await host.answer(qualifiedAnswer(f)); const saved = f.store().get(f.call.id)!.selectedQuestion!;
  assert.equal(saved.initial.deliveryCall.sessionId, session);
  assert.equal(saved.questions[0]!.admissions[0]!.answer.sourceSessionId, nextSession);
  assert.equal(saved.questions[0]!.record!.attempts[0]!.state, 'accepted');
  const tampered = structuredClone(saved); tampered.questions[0]!.admissions[0]!.answer.sourceSessionId = session;
  assert.throws(() => f.store().recordSelectedQuestion(f.call.id, tampered));
  assert.deepEqual(f.restart().get(f.call.id)!.selectedQuestion, saved);
});

test('call stop permits the already-admitted raw capture acknowledgement but no reply dispatch', async t => {
  const f = fixture(t), options = hostOptions(f); let release!: (value: PrimaryTurnReceipt) => void;
  let entered!: (value: PrimaryTurn) => void; const captured = new Promise<PrimaryTurn>(resolve => { entered = resolve; });
  let posts = 0;
  options.capture = async (_id, turn) => { entered(turn); return new Promise<PrimaryTurnReceipt>(resolve => { release = resolve; }).then(receipt => saveCapture(f, turn, receipt)); };
  options.requests.reply = async journal => { posts++; return response(journal, 'answered'); };
  const host = createSelectedQuestionHost(options); await host.start();
  const input = qualifiedAnswer(f), work = host.answer(input), turn = await captured;
  f.store().update(f.call.id, { status: 'completed', endedAt: new Date().toISOString() }); host.stop();
  release(captureReceipt(turn)); await work;
  const saved = f.store().get(f.call.id)!.selectedQuestion!;
  assert.equal(posts, 0); assert.deepEqual(saved.questions[0]!.admissions[0]!.answer, input);
  assert.equal(saved.questions[0]!.admissions[0]!.captured!.reply!.input.text, input.input.text);
  assert.equal(saved.questions[0]!.admissions[0]!.localRefusal!.code, 'call_ended');
  assert.equal(saved.questions[0]!.record!.attempts.length, 0);
  assert.deepEqual(f.restart().get(f.call.id)!.selectedQuestion, saved);
});

test('an uncertain dispatched reply may reconcile after call stop and restart without another POST', async t => {
  const f = fixture(t), options = hostOptions(f); let posts = 0;
  options.requests.reply = async () => { posts++; throw new PrimaryApiError('request_timeout'); };
  const host = createSelectedQuestionHost(options); await host.start(); await host.answer(qualifiedAnswer(f));
  assert.equal(host.snapshot().questions[0]!.record!.attempts[0]!.state, 'unknown');
  f.store().update(f.call.id, { status: 'completed', endedAt: new Date().toISOString() }); host.stop(); f.restart();
  options.requests.question = async journal => response(journal, 'answered');
  const resumed = createSelectedQuestionHost(options); await resumed.recoverReply();
  assert.equal(posts, 1); assert.equal(resumed.snapshot().questions[0]!.record!.attempts[0]!.state, 'accepted');
  assert.equal(resumed.snapshot().questions[0]!.record!.attempts[0]!.dispatches, 1);
});

test('terminal call cannot add a selection or another answer even before host stop is recorded', async t => {
  const f = fixture(t), options = hostOptions(f), host = createSelectedQuestionHost(options); await host.start();
  f.store().update(f.call.id, { status: 'completed' }); const before = f.bytes();
  const next = host.snapshot(); next.questions[0]!.admissions.push({ sequence: 1, answer: answer(), admittedAt: new Date().toISOString() });
  assert.throws(() => f.store().recordSelectedQuestion(f.call.id, next), /Stopped call/); assert.equal(f.bytes(), before);
  const q2 = host.snapshot(); q2.questions.push({ seed: { ...f.seed, request: { ...f.seed.request, questionId: uuid(17) } }, admissions: [] });
  assert.throws(() => f.store().recordSelectedQuestion(f.call.id, q2), /Stopped call/); assert.equal(f.bytes(), before);
});

test('restart refuses cross-call, source, scope or deadline corruption and preserves the original file', t => {
  const f = fixture(t); f.store().recordSelectedQuestion(f.call.id, f.empty());
  const saved = f.store().get(f.call.id)!; f.store().close();
  const changes: Array<(call: BrokerCall) => void> = [
    call => { call.selectedQuestion!.initial.deliveryCall.callId = uuid(90); },
    call => { call.selectedQuestion!.initial.scope.ownerPrincipalId = uuid(90); },
    call => { call.liveGenerations!.segments[0]!.sessionId = 'another'; call.liveGenerations!.deliveryAttachment!.sessionId = 'another'; },
    call => { call.liveGenerations!.clocks.callDeadlineAt = new Date(Date.parse(f.deadline) + 1).toISOString(); },
  ];
  for (const change of changes) {
    const corrupt = structuredClone(saved); change(corrupt);
    const raw = JSON.stringify({ version: 1, calls: { [saved.id]: corrupt } }); fs.writeFileSync(f.file, raw);
    assert.throws(() => new BrokerStore(f.directory)); assert.equal(f.bytes(), raw);
  }
});

test('legacy and Hark calls keep selected-question absence and exact reload bytes', t => {
  const f = fixture(t);
  const legacy = f.store().reserve({ requestId: 'retained_legacy_request', direction: 'outbound', purpose: 'Retained call', number: '+12025550125' }).call;
  const source: Omit<PhoneHarkSnapshot, 'sourceDigest'> = { kind: 'hark_watch_event_v1', ownerSupabaseUserId: uuid(90), destinationId: uuid(91),
    deliveryId: uuid(92), caseId: uuid(93), originTaskId: 'watch_fixture', expiresAt: f.deadline,
    watch: { id: uuid(94), draftDigest: 'd'.repeat(64), request: 'Call at the threshold.', name: 'Saved watch', source: {}, condition: {}, attention: {} },
    event: { id: uuid(95), bodyDigest: 'e'.repeat(64), observedAt: f.at, acceptedAt: f.at, kind: 'threshold', subject: {}, summary: 'Threshold reached.', facts: {}, evidence: [] },
    billing: { mandateId: uuid(96), unitPriceMicro: 1, freeSeconds: 0 } };
  const hark = f.store().reserveHark({ schemaVersion: 1, to: '+12025550126', callAssignment: {
    assignmentId: uuid(92), brokerCallId: uuid(92), revision: 1, purpose: HARK_PHONE_PURPOSE, hark: { ...source, sourceDigest: harkSourceDigest(source) } } }).call;
  assert.equal(Object.hasOwn(legacy, 'selectedQuestion'), false); assert.equal(Object.hasOwn(hark, 'selectedQuestion'), false);
  const before = f.bytes(); f.restart(); assert.equal(f.bytes(), before);
  assert.equal(Object.hasOwn(f.store().get(legacy.id)!, 'selectedQuestion'), false);
  assert.equal(Object.hasOwn(f.store().get(hark.id)!, 'selectedQuestion'), false);
  assert.throws(() => f.store().recordSelectedQuestion(hark.id, f.empty())); assert.equal(f.bytes(), before);
});


test('store refuses a selected answer when an upstream callback skips the durable voice source', async t => {
  const f = fixture(t), options = hostOptions(f); options.replyIsQualified = () => true;
  const host = createSelectedQuestionHost(options); await host.start(); const before = f.bytes();
  await assert.rejects(host.answer(answer()), /selected_voice_admission_evidence_missing/);
  assert.equal(f.bytes(), before); assert.deepEqual(f.store().get(f.call.id)!.delegations, {});
});

test('voice evidence requires its explicit transition and immutable source/capture history', async t => {
  const f = fixture(t), host = createSelectedQuestionHost(hostOptions(f)); await host.start();
  const input = qualifiedAnswer(f); await host.answer(input);
  const saved = f.store().get(f.call.id)!.selectedQuestionVoice!, before = f.bytes();
  const patch = (value: unknown) => ({ selectedQuestionVoice: value }) as unknown as Parameters<BrokerStore['update']>[1];
  assert.throws(() => f.store().update(f.call.id, patch(undefined)), /explicit transition/);
  assert.throws(() => f.store().reserve({ requestId: uuid(93), direction: 'outbound', purpose: 'Other', number: '+12025550125',
    selectedQuestionVoice: saved } as Parameters<BrokerStore['reserve']>[0]), /admitted call/);
  assert.throws(() => f.store().reserve({ requestId: uuid(93), direction: 'outbound', purpose: 'Other', number: '+12025550125',
    publications: f.store().get(f.call.id)!.publications } as Parameters<BrokerStore['reserve']>[0]), /selected_publication_identity_changed/);
  for (const change of [
    (value: typeof saved) => { value.sources = []; },
    (value: typeof saved) => { value.sources[0]!.frozen.turn.text = 'No'; },
    (value: typeof saved) => { value.sources[0]!.capture!.revisionRecordId = uuid(94); },
    (value: typeof saved) => { value.presentations[0]!.response.deliveryAssociation.id = uuid(94); },
  ]) {
    const changed = structuredClone(saved); change(changed);
    assert.throws(() => f.store().recordSelectedQuestionVoice(f.call.id, changed)); assert.equal(f.bytes(), before);
  }
  assert.deepEqual(f.restart().get(f.call.id)!.selectedQuestionVoice, saved); assert.equal(f.bytes(), before);
});

test('selected publication preserves every long-answer chunk and exact API metadata without a delegation', async t => {
  const f = fixture(t), host = createSelectedQuestionHost(hostOptions(f)); await host.start();
  const journal = host.snapshot().questions[0]!.record!.base;
  const text = 'The comparison includes the requested evidence and its source. '.repeat(30);
  const { eligible, receipts } = publishSelected(f, journal, { kind: 'question', questionId, questionRevision: 1 }, { text });
  assert.ok(receipts.length > 1);
  const call = f.store().get(f.call.id)!;
  const records = receipts.map(receipt => call.publications!.records.find(record => record.publicationSequence === receipt.publicationSequence)!);
  assert.deepEqual(call.delegations, {});
  assert.deepEqual(records.map(record => record.selectedDelivery!.chunkIndex), receipts.map((_, index) => index));
  assert.ok(records.every(record => canonicalJson(record.selectedDelivery!.response) === canonicalJson(eligible)));
  assert.equal(liveResultChunks(text).join(''), text);
  assert.equal(selectedQuestionPublicationReceiptsAccepted(call, eligible, session, receipts), true);
  assert.equal(selectedQuestionPublicationReceiptsAccepted(call, eligible, session, receipts.slice(1)), false);
  assert.equal(selectedQuestionPublicationReceiptsAccepted(call, eligible, session, [...receipts].reverse()), false);
  const changed = structuredClone(eligible); changed.version.digest = 'd'.repeat(64);
  assert.equal(selectedQuestionPublicationReceiptsAccepted(call, changed, session, receipts), false);
  const before = f.bytes(); assert.deepEqual(f.restart().get(f.call.id)!.publications, call.publications); assert.equal(f.bytes(), before);
});

test('changed API scope, execution, question, generation and chunk refuse before publication persistence', async t => {
  const f = fixture(t), host = createSelectedQuestionHost(hostOptions(f)); await host.start();
  const journal = host.snapshot().questions[0]!.record!.base, { eligible, identity } = publishSelected(f, journal);
  const snapshot = f.store().get(f.call.id)!.liveGenerations!.segments[0]!.transcript;
  const update = { id: 'fresh_publication', kind: 'result' as const, speak: true,
    text: eligible.delivery.state === 'ready' ? liveResultChunks(eligible.delivery.text)[0]! : '', expectedCallerRevision: snapshot.callerRevision };
  const changes: Array<(value: typeof eligible) => void> = [
    value => { value.bindingId = uuid(94); }, value => { value.requestId = uuid(94); },
    value => { value.version.ownerPrincipalId = uuid(94); }, value => { value.version.digest = 'd'.repeat(64); },
    value => { value.deliveryAssociation.execution.runId = uuid(94); }, value => { value.deliveryAssociation.question.id = uuid(94); },
  ];
  const before = f.bytes();
  for (const change of changes) {
    const altered = structuredClone(eligible); change(altered);
    let result: ReturnType<typeof beginSelectedQuestionPublication>;
    try { result = beginSelectedQuestionPublication(f.store(), identity, altered, update, snapshot, 0); }
    catch (error) { assert.ok(error instanceof Error); result = undefined; }
    assert.equal(result, undefined);
    assert.equal(f.bytes(), before);
  }
  assert.equal(beginSelectedQuestionPublication(f.store(), { ...identity, sessionId: 'unregistered' }, eligible, update, snapshot, 0), undefined);
  assert.throws(() => beginSelectedQuestionPublication(f.store(), identity, eligible, { ...update, text: 'Changed words' }, snapshot, 0), /render_changed/);
  assert.equal(f.bytes(), before);
});

test('same publication retry keeps its original sequence and refuses changed response bytes', async t => {
  const f = fixture(t), host = createSelectedQuestionHost(hostOptions(f)); await host.start();
  const call = f.store().get(f.call.id)!, record = call.publications!.records[0]!, response = record.selectedDelivery!.response;
  const snapshot = call.liveGenerations!.segments[0]!.transcript;
  assert.equal(response.delivery.state, 'ready'); if (response.delivery.state !== 'ready') return;
  const update = { id: record.updateId, kind: 'result' as const, speak: true,
    text: liveResultChunks(response.delivery.text)[0]!, expectedCallerRevision: record.callerRevision };
  const identity = { callId: call.id, callSid: sid, bindingId: binding, sessionId: session };
  const before = f.bytes(), retried = beginSelectedQuestionPublication(f.store(), identity, response, update, snapshot, 0)!;
  assert.equal(retried.created, false); assert.deepEqual(retried.record, record); assert.equal(f.bytes(), before);
  const changed = structuredClone(response);
  if (changed.delivery.state === 'ready') changed.delivery.source.messageId = uuid(94);
  assert.throws(() => beginSelectedQuestionPublication(f.store(), identity, changed, update, snapshot, 0), /publication_identity_changed/);
  assert.equal(f.bytes(), before);
});

test('late rejection retains history but blocks new admission and presentation', async t => {
  const f = fixture(t), options = hostOptions(f), host = createSelectedQuestionHost(options); await host.start();
  const input = qualifiedAnswer(f), call = f.store().get(f.call.id)!, presentation = call.selectedQuestionVoice!.presentations.at(-1)!;
  const receipt = presentation.publicationReceipts[0]!;
  rejectBrokerPublication(f.store(), { callId: call.id, callSid: sid, bindingId: binding, sessionId: session }, receipt.eventId, 'append_refused');
  options.replyIsQualified = () => true; // Store still enforces the persisted receipt tombstone.
  const bypass = createSelectedQuestionHost(options), before = f.bytes();
  await assert.rejects(bypass.answer(input), /prior durable source and accepted publications/);
  assert.equal(f.bytes(), before);
  assert.deepEqual(f.restart().get(call.id)!.selectedQuestionVoice, call.selectedQuestionVoice);
  const resumed = createSelectedQuestionHost(hostOptions(f)); await resumed.start();
  assert.throws(() => publishSelected(f, resumed.snapshot().questions[0]!.record!.base,
    { kind: 'question', questionId, questionRevision: 1 }, { beforeEvidence(receipts, identity) {
      rejectBrokerPublication(f.store(), identity, receipts[0]!.eventId, 'append_refused');
    } }));
});

test('selected publication outcome, response and rejection removal cannot rewrite saved history', async t => {
  const f = fixture(t), host = createSelectedQuestionHost(hostOptions(f)); await host.start();
  const call = f.store().get(f.call.id)!, record = call.publications!.records[0]!;
  rejectBrokerPublication(f.store(), { callId: call.id, callSid: sid, bindingId: binding, sessionId: session }, record.outcome!.eventId, 'append_refused');
  const saved = f.store().get(call.id)!.publications!, before = f.bytes();
  for (const change of [
    (value: typeof saved) => { value.records = []; },
    (value: typeof saved) => { value.records[0]!.outcome!.status = 'unconfirmed'; },
    (value: typeof saved) => { value.records[0]!.selectedDelivery!.chunkIndex = 4; },
    (value: typeof saved) => { value.rejections = []; },
  ]) { const next = structuredClone(saved); change(next); assert.throws(() => f.store().update(call.id, { publications: next })); assert.equal(f.bytes(), before); }
});

test('ordinary call journals retain absent selected evidence and exact bytes on restart', t => {
  const f = fixture(t); const before = f.bytes(), saved = f.store().get(f.call.id)!;
  assert.equal(Object.hasOwn(saved, 'selectedQuestionVoice'), false);
  assert.equal(Object.hasOwn(saved, 'publications'), false);
  f.restart(); assert.equal(f.bytes(), before);
});


test('store blocks a prepared first dispatch if a question chunk is rejected during its fresh read', async t => {
  const f = fixture(t), options = hostOptions(f); let release!: (value: RequestDeliveryQuestionResult) => void;
  let read!: () => void; const entered = new Promise<void>(resolve => { read = resolve; });
  options.requests.question = async () => { read(); return new Promise(resolve => { release = resolve; }); };
  // The durable store is the final guard even if a host callback is permissive.
  options.admittedReplyIsValid = () => true;
  let posts = 0; options.requests.reply = async journal => { posts++; return response(journal, 'answered'); };
  const host = createSelectedQuestionHost(options); await host.start();
  const input = qualifiedAnswer(f), work = host.answer(input); await entered;
  const call = f.store().get(f.call.id)!, slot = call.selectedQuestion!.questions[0]!;
  const presentation = call.selectedQuestionVoice!.presentations.at(-1)!;
  assert.ok(slot.admissions[0]!.captured); assert.ok(call.selectedQuestionVoice!.sources[0]!.capture);
  rejectBrokerPublication(f.store(), { callId: call.id, callSid: sid, bindingId: binding, sessionId: session },
    presentation.publicationReceipts[0]!.eventId, 'append_refused');
  release(response(slot.admissions[0]!.captured!));
  await assert.rejects(work, /dispatch requires captured source and accepted publications/);
  assert.equal(posts, 0);
  assert.equal(f.store().get(f.call.id)!.selectedQuestion!.questions[0]!.record!.attempts[0]!.dispatches, 0);
});

test('selected completed-result publication keeps original execution and current delivery without a new run', async t => {
  const f = fixture(t), host = createSelectedQuestionHost(hostOptions(f)); await host.start();
  await host.answer(qualifiedAnswer(f));
  const before = f.store().get(f.call.id)!, journal = before.selectedQuestion!.questions[0]!.record!.base;
  const { eligible, receipts } = publishSelected(f, journal, { kind: 'assistant', messageId: uuid(81) });
  const call = f.store().get(f.call.id)!, record = call.publications!.records.at(-1)!;
  assert.equal(record.requestState, 'completed'); assert.equal(record.requestId, request);
  assert.equal(record.bindingId, binding);
  assert.deepEqual(record.selectedDelivery!.response.deliveryAssociation.execution, journal.target.execution);
  assert.equal(selectedQuestionPublicationReceiptsAccepted(call, eligible, session, receipts), true);
  assert.deepEqual(call.delegations, {});
  assert.deepEqual(call.selectedQuestionVoice, before.selectedQuestionVoice);
});
