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
import { createBrokerServer, type BrokerServerOptions } from '../src/broker/server.js';
import type { BrokerConfig } from '../src/broker/config.js';

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
  const cleanup = () => { for (const item of instances) item.close(); fs.rmSync(directory, { recursive: true, force: true }); };
  return { cleanup, call, seed, at, deadline, empty, bytes, file, directory, restart, rotate, store: () => store };
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


// Actual server maintenance and BrokerStore recovery. Only HTTP/provider ports
// are injected. No private active-host map or prepared close result is supplied.
async function serverFixture(t: TestContext, mode: 'capture_pending' | 'reply_unknown', closeFlag = true) {
  const f = fixture(t), options = hostOptions(f);
  let broker: ReturnType<typeof createBrokerServer> | undefined, closed = false;
  const stopServer = async () => { if (closed || !broker) return; closed = true; await broker.close(); };
  t.after(async () => { try { await stopServer(); } finally { f.cleanup(); } });
  if (mode === 'capture_pending') options.capture = async () => { throw new PrimaryApiError('request_failed'); };
  else options.requests.reply = async () => { throw new PrimaryApiError('request_failed'); };
  const host = createSelectedQuestionHost(options); await host.start(); const answer = qualifiedAnswer(f);
  if (mode === 'capture_pending') await assert.rejects(host.answer(answer), /request_failed/);
  else await host.answer(answer);
  const admitted = f.store().get(f.call.id)!;
  assert.equal(admitted.selectedQuestion!.questions[0]!.admissions.length, 1);
  const raw = structuredClone(admitted.selectedQuestionVoice!.sources[0]!.frozen.turn);
  const endedAt = new Date().toISOString(), generations = structuredClone(admitted.liveGenerations!);
  generations.phase = 'stopped'; delete generations.deliveryAttachment;
  generations.segments = generations.segments.map(segment => ({ ...segment, state: 'closed' as const,
    closedAt: endedAt, providerFinalized: true, transcriptPersisted: true, outputDiscarded: true }));
  f.store().update(f.call.id, { status: 'completed', endedAt, finalized: true, liveGenerations: generations,
    ...(closeFlag ? { primaryClosePending: true } : {}) });
  // Simulate a new owner after process loss. The old in-memory host is never used again.
  f.restart();
  const seen = { captures: [] as PrimaryTurn[], questions: 0, closes: 0,
    submits: 0, replies: 0, associations: 0, results: 0, provider: 0, voice: 0 };
  const events: string[] = [];
  const behavior = { capture: 'accept' as 'accept' | 'error' | 'wrong_ack', question: 'pending' as 'pending' | 'answered' | 'wrong_identity' };
  const config: BrokerConfig = { host: '127.0.0.1', port: 3213, publicOrigin: 'https://phone.example.test',
    stateDir: f.directory, controlToken: 'c'.repeat(40), apiKey: 'fixture-key', model: 'gpt-live-1', voice: 'marin',
    twilioAccountSid: `AC${'1'.repeat(32)}`, twilioAuthToken: 'fixture-token', from: '+12025550100',
    allowedTo: ['+12025550123'], apiBaseUrl: 'https://api.example.test', phoneServiceSecret: 's'.repeat(40), maxCallSeconds: 60 };
  const forbidden = (key: 'submits' | 'replies' | 'associations' | 'results' | 'provider' | 'voice') => async (): Promise<never> => {
    seen[key]++; throw new Error(`Maintenance cannot perform ${key}`);
  };
  const primary: NonNullable<BrokerServerOptions['primary']> = {
    readiness: forbidden('provider'), openCall: forbidden('associations'), getContext: forbidden('results'),
    submit: forbidden('submits'), result: forbidden('results'), reconcile: forbidden('results'),
    question: forbidden('results'), reply: forbidden('replies'),
    async captureTurn(receivedBinding, turn) {
      assert.equal(receivedBinding, binding); assert.deepEqual(turn, raw);
      seen.captures.push(structuredClone(turn)); events.push('capture');
      const disk = JSON.parse(f.bytes()).calls[f.call.id] as BrokerCall;
      assert.deepEqual(disk.selectedQuestionVoice!.sources[0]!.frozen.turn, raw);
      assert.equal(disk.primaryClosed, undefined);
      if (behavior.capture === 'error') throw new PrimaryApiError('request_failed');
      const receipt = captureReceipt(turn);
      return behavior.capture === 'wrong_ack' ? { ...receipt, bodyDigest: 'f'.repeat(64) } : receipt;
    },
    async close(receivedBinding) {
      assert.equal(receivedBinding, binding); seen.closes++; events.push('close');
      const disk = JSON.parse(f.bytes()).calls[f.call.id] as BrokerCall;
      const source = disk.selectedQuestionVoice!.sources[0]!, admission = disk.selectedQuestion!.questions[0]!.admissions[0]!;
      assert.deepEqual(source.frozen.turn, raw); assert.ok(source.capture);
      assert.equal(source.capture.bodyDigest, turnBodyDigest(raw));
      assert.equal(admission.captured!.callerSource!.revisionRecordId, source.capture.revisionRecordId);
      assert.ok(admission.localRefusal || disk.selectedQuestion!.questions[0]!.record!.attempts[0]!.state === 'accepted');
      assert.deepEqual(disk.delegations, {});
      return { bindingId: binding, callSid: sid, state: 'closed' };
    },
  };
  const requests: NonNullable<BrokerServerOptions['selectedQuestion']>['requests'] = {
    associate: forbidden('associations'), result: forbidden('results'), reply: forbidden('replies'),
    async question(journal) {
      seen.questions++; events.push('question-get');
      assert.equal(journal.request.originalRequestId, request);
      assert.deepEqual(journal.reply!.input, answer.input);
      assert.equal(journal.target.execution.bindingId, uuid(9));
      assert.equal(journal.target.delivery.bindingId, binding);
      const result = response(journal, behavior.question === 'answered' ? 'answered' : 'pending');
      return behavior.question === 'wrong_identity' ? { ...result, requestId: uuid(99) } : result;
    },
  };
  const calls = Object.assign(() => ({ fetch: forbidden('provider'), update: forbidden('provider') }), { create: forbidden('provider') });
  const open = async () => {
    const instance = createBrokerServer({ config, store: f.store(), primary, provider: { calls } as never,
      selectedQuestion: { association: { associate: forbidden('associations') }, requests },
      probe: forbidden('provider'), voiceFactory: () => { seen.voice++; throw new Error('Maintenance cannot open voice'); },
      harkPrimaryReceiptSender: forbidden('provider') });
    broker = instance;
    await new Promise<void>(resolve => instance.server.listen(0, '127.0.0.1', resolve)); closed = false;
  };
  await open();
  return { ...f, raw, answer, seen, events, behavior, broker: () => broker!,
    async restartServer() { await stopServer(); f.restart(); await open(); },
    assertNoNewWork() { assert.equal(seen.submits, 0); assert.equal(seen.replies, 0); assert.equal(seen.associations, 0);
      assert.equal(seen.results, 0); assert.equal(seen.provider, 0); assert.equal(seen.voice, 0); },
  };
}

test('server maintenance captures the exact pending raw answer before closing its binding', async t => {
  const f = await serverFixture(t, 'capture_pending');
  await f.broker().maintain();
  const call = f.store().get(f.call.id)!;
  assert.deepEqual(f.seen.captures, [f.raw]); assert.equal(f.seen.closes, 1); assert.equal(f.seen.questions, 0);
  assert.deepEqual(f.events, ['capture', 'close']);
  assert.equal(call.primaryClosePending, false); assert.deepEqual(call.primaryClosed, { bindingId: binding, callSid: sid, state: 'closed' });
  assert.equal(call.selectedQuestion!.questions[0]!.admissions[0]!.localRefusal!.code, 'call_ended');
  assert.equal(call.selectedQuestion!.questions[0]!.record!.attempts.length, 0); f.assertNoNewWork();
  const bytes = f.bytes(); await f.broker().maintain(); assert.equal(f.bytes(), bytes); assert.equal(f.seen.closes, 1);
});

test('crash after terminal persistence before close-pending flag still recovers without provider lookup', async t => {
  const f = await serverFixture(t, 'capture_pending', false);
  assert.equal(f.store().get(f.call.id)!.primaryClosePending, undefined);
  await f.broker().maintain();
  assert.deepEqual(f.events, ['capture', 'close']); assert.equal(f.seen.closes, 1);
  assert.equal(f.store().get(f.call.id)!.primaryClosePending, false); f.assertNoNewWork();
});

for (const failure of ['error', 'wrong_ack'] as const) {
  test(`capture ${failure} preserves pending close and raw source across another server restart`, async t => {
    const f = await serverFixture(t, 'capture_pending'); f.behavior.capture = failure;
    await f.broker().maintain();
    const pending = f.store().get(f.call.id)!;
    assert.equal(f.seen.closes, 0); assert.equal(pending.primaryClosePending, true); assert.equal(pending.primaryClosed, undefined);
    assert.equal(pending.selectedQuestionVoice!.sources[0]!.capture, undefined);
    assert.deepEqual(pending.selectedQuestionVoice!.sources[0]!.frozen.turn, f.raw);
    const failedCaptures = f.seen.captures.length;
    assert.ok(failedCaptures > 0);
    await f.restartServer(); f.behavior.capture = 'accept'; await f.broker().maintain();
    assert.equal(f.seen.closes, 1); assert.equal(f.store().get(f.call.id)!.primaryClosePending, false);
    assert.equal(f.seen.captures.length, failedCaptures + 1);
    assert.ok(f.seen.captures.every(turn => canonicalJson(turn) === canonicalJson(f.raw))); f.assertNoNewWork();
  });
}

test('unknown reply remains pending until exact question GET proves acceptance; maintenance never sends another answer', async t => {
  const f = await serverFixture(t, 'reply_unknown');
  const original = f.store().get(f.call.id)!.selectedQuestion!.questions[0]!.record!.attempts[0]!;
  assert.equal(original.state, 'unknown'); assert.equal(original.dispatches, 1);
  await f.broker().maintain();
  assert.equal(f.seen.questions, 1); assert.equal(f.seen.closes, 0); assert.deepEqual(f.seen.captures, []);
  assert.equal(f.store().get(f.call.id)!.primaryClosePending, true);
  await f.restartServer(); f.behavior.question = 'answered'; await f.broker().maintain();
  const final = f.store().get(f.call.id)!.selectedQuestion!.questions[0]!.record!.attempts[0]!;
  assert.equal(final.state, 'accepted'); assert.equal(final.dispatches, 1); assert.deepEqual(final.journal, original.journal);
  assert.equal(f.seen.questions, 2); assert.equal(f.seen.closes, 1); assert.deepEqual(f.seen.captures, []);
  assert.deepEqual(f.events, ['question-get', 'question-get', 'close']); f.assertNoNewWork();
});

test('foreign question read cannot settle an unknown answer or close its delivery binding', async t => {
  const f = await serverFixture(t, 'reply_unknown'); f.behavior.question = 'wrong_identity';
  await f.broker().maintain(); const call = f.store().get(f.call.id)!;
  assert.equal(f.seen.questions, 1); assert.equal(f.seen.closes, 0); assert.equal(call.primaryClosed, undefined);
  assert.equal(call.primaryClosePending, true); assert.equal(call.selectedQuestion!.questions[0]!.record!.attempts[0]!.state, 'unknown');
  assert.deepEqual(call.selectedQuestionVoice!.sources[0]!.frozen.turn, f.raw); f.assertNoNewWork();
});
