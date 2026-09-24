import assert from 'node:assert/strict';
import test from 'node:test';
import { createHash } from 'node:crypto';
import { brokerIdentity, liveResultChunks, withCallerAdmission } from '../src/broker/backend.js';
import { prepareCallerRouting, prepareCallerIntent, recordCallerIntentResponse, decideCallerRouting, callerRouteRef,
  callerIntentQuestionForTarget, callerRoutePresentationDigest, type CallerRouteDecision,
  type CallerQuestionTarget } from '../src/broker/callerRouting.js';
import { callerIntentInputDigest, type CallerIntentInput, type CallerIntentResponse } from '../src/broker/callerIntentContract.js';
import { assertFrozenCallerSpeechCurrent } from '../src/broker/callerTurn.js';
import { createSelectedQuestionVoiceAdapter, assertSelectedQuestionVoiceEvidenceUpdate,
  assertSelectedQuestionVoiceScope, parseSelectedQuestionVoiceEvidence, selectedQuestionPresentation,
  validateSelectedQuestionVoiceCall,
  type FrozenSelectedCallerTurn, type SelectedQuestionDeliveryObservation, type SelectedQuestionVoiceEvidence, type SelectedQuestionVoicePresentation,
  type SelectedQuestionVoiceOptions } from '../src/broker/selectedQuestionVoice.js';
import { createRequestDeliveryJournal, retainRequestDeliveryAssociation, PHONE_REQUEST_DELIVERY_VERSION,
  type RequestDeliveryAssociation, type RequestDeliveryJournal, type RequestDeliveryQuestionResult,
  type RequestDeliveryResult } from '../src/broker/requestDelivery.js';
import type { SelectedQuestionJournal, SelectedQuestionSeed } from '../src/broker/selectedQuestion.js';
import type { EligibleDeliveryTarget } from '../src/broker/eligibleContext.js';
import type { BrokerPublicationReceipt } from '../src/broker/publications.js';
import { PrimaryApiError, type PrimaryTurn, type PrimaryTurnReceipt } from '../src/broker/primaryApi.js';
import type { BrokerCall } from '../src/broker/store.js';
import { turnBodyDigest } from '../src/broker/reconciliation.js';
import type { LiveBackendChannel, LiveDelegationRequest, LiveTranscriptSnapshot } from '../src/live/types.js';

// Actual selected-question host/reducers. Freezing, API HTTP and publication are
// explicit adapter ports here; this file does not claim the server is mounted.
const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const at = '2026-09-24T10:00:00.000Z';
const milliseconds = Date.parse(at);
const seed: SelectedQuestionSeed = { v: 'broker.selected-question-seed.v1',
  deliveryCall: { callId: uuid(1), sessionId: 'selected_g1' },
  scope: { ownerPrincipalId: uuid(2), primaryDexterId: uuid(3), threadId: uuid(4) },
  delivery: { bindingId: uuid(5), callSid: `CA${'a'.repeat(32)}` },
  request: { v: PHONE_REQUEST_DELIVERY_VERSION, originalRequestId: uuid(6), questionId: uuid(7), questionRevision: 1 },
  callDeadlineAt: '2026-09-24T10:05:00.000Z' };
const association = (questionId = uuid(7)): RequestDeliveryAssociation => ({ id: uuid(questionId === uuid(7) ? 8 : 18),
  v: PHONE_REQUEST_DELIVERY_VERSION, delivery: seed.delivery,
  execution: { bindingId: uuid(9), requestId: uuid(6), runId: uuid(10) },
  question: { id: questionId, revision: 1 }, createdAt: at });
const associated = (value: SelectedQuestionSeed) => retainRequestDeliveryAssociation(createRequestDeliveryJournal({
  deliveryCall: value.deliveryCall, scope: value.scope, request: value.request,
  target: { delivery: value.delivery, execution: association().execution, question: association(value.request.questionId).question },
}), { association: association(value.request.questionId), replayed: false });
const question = (id = uuid(7), state: 'pending' | 'answered' = 'pending'): RequestDeliveryQuestionResult => ({
  bindingId: uuid(9), requestId: uuid(6), state: state === 'pending' ? 'waiting_for_reply' : 'running',
  deliveryAssociation: association(id), question: { id, revision: 1, runId: uuid(10), threadId: uuid(4),
    agent: { id: uuid(3), name: 'Dexter' }, state, runState: state === 'pending' ? 'waiting_for_reply' : 'running',
    question: 'Continue the comparison?', context: 'Research only.', proposedAnswer: 'Compare both stocks.',
    yesMeaning: 'Compare both.', noMeaning: 'Stop the comparison.', createdAt: at,
    expiresAt: '2026-09-24T10:04:00.000Z', reply: state === 'pending' ? null : {
      questionRevision: 1, clientReplyId: uuid(11), answer: 'yes', text: null,
      interpretedAnswer: 'Compare both.', receivedAt: at } },
});
const receipt = (turn: PrimaryTurn): PrimaryTurnReceipt => ({ bindingId: seed.delivery.bindingId,
  sourceEventId: turn.sourceEventId, revision: turn.revision, recordId: turn.sourceEventId,
  revisionRecordId: uuid(40), bodyDigest: turnBodyDigest(turn), replayed: false });
const empty = (): LiveTranscriptSnapshot => ({ revision: 0, callerRevision: 0, fragments: [] });
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(yes => { resolve = yes; }); return { promise, resolve };
}

function fixture() {
  let saved: SelectedQuestionJournal | undefined;
  let voice: SelectedQuestionVoiceEvidence | undefined;
  let clock = milliseconds;
  let snapshot = empty();
  let frozen: FrozenSelectedCallerTurn | undefined;
  let acoustic: { sessionId: string; audioThroughMs: number | undefined; observedSpeechAtMs: number | undefined } = {
    sessionId: seed.deliveryCall.sessionId, audioThroughMs: undefined, observedSpeechAtMs: undefined,
  };
  const abort = new AbortController();
  const events: string[] = [], captures: PrimaryTurn[] = [], posts: RequestDeliveryJournal[] = [];
  const associations: string[] = [], deliveries: SelectedQuestionDeliveryObservation[] = [], admissions: string[] = [];
  const version = { ...seed.scope, contextEpoch: '1', threadEventSequence: '2', digest: 'a'.repeat(64) };
  const call: BrokerCall = { id: seed.deliveryCall.callId, requestId: uuid(50), direction: 'outbound',
    purpose: 'Discuss the selected saved question.', number: '+15550000001', status: 'in-progress',
    createdAt: at, updatedAt: at, callSid: seed.delivery.callSid, liveSessionId: seed.deliveryCall.sessionId,
    primary: { bindingId: seed.delivery.bindingId, name: 'Dexter', context: '', primaryDexterId: uuid(3), threadId: uuid(4) },
    mediaStartedAt: at, continuation: { originalRequestId: uuid(6), questionId: uuid(7), questionRevision: 1 },
    delegations: {}, publications: { records: [], rejections: [] },
    liveGenerations: { version: 1, callId: uuid(1), binding: { ...seed.delivery, ...seed.scope },
      clocks: { mediaStartedAt: at, callDeadlineAt: seed.callDeadlineAt }, phase: 'active', inputNextSample: 0,
      deliveryAttachment: { generationId: uuid(30), sessionId: seed.deliveryCall.sessionId },
      segments: [{ generationId: uuid(30), ordinal: 1, createdAt: at, readyAt: at, state: 'active',
        sessionId: seed.deliveryCall.sessionId, version, pageDigest: 'b'.repeat(64), transcript: empty(), outputDiscarded: false }] } };
  const activeSegment = () => call.liveGenerations!.segments.find(s => s.generationId === call.liveGenerations!.deliveryAttachment!.generationId)!;
  const hooks: {
    capture?: (turn: PrimaryTurn) => Promise<PrimaryTurnReceipt>;
    reply?: (journal: RequestDeliveryJournal) => Promise<RequestDeliveryQuestionResult>;
    question?: (journal: RequestDeliveryJournal) => Promise<RequestDeliveryQuestionResult>;
    result?: (journal: RequestDeliveryJournal) => Promise<RequestDeliveryResult>;
    afterPublication?: (value: SelectedQuestionDeliveryObservation) => void;
    persistVoice?: (value: SelectedQuestionVoiceEvidence) => void;
    freezer?: () => Promise<FrozenSelectedCallerTurn | undefined>;
    admit?: () => void;
  } = {};
  let questionText = 'Continue the comparison? Yes compares both stocks. No stops the comparison.';
  const publish = async (journal: RequestDeliveryJournal, target: EligibleDeliveryTarget): Promise<SelectedQuestionDeliveryObservation> => {
    events.push('publication');
    assert.equal(saved!.questions.at(-1)!.record!.base.association!.id, journal.association!.id);
    const attachment = call.liveGenerations!.deliveryAttachment!, segment = activeSegment();
    const receipts: BrokerPublicationReceipt[] = [];
    const text = target.kind === 'question' ? questionText : 'The comparison has been saved.';
    for (const chunk of liveResultChunks(text)) {
      const n = call.publications!.records.length + 1, eventId = `append_${n}`;
      call.publications!.records.push({ callId: call.id, callSid: call.callSid!, bindingId: seed.delivery.bindingId,
        sessionId: attachment.sessionId, publicationSequence: n, updateId: eventId, requestId: uuid(6),
        requestState: target.kind === 'question' ? 'waiting_for_reply' : 'completed', kind: 'result', speak: true,
        textDigest: createHash('sha256').update(chunk).digest('hex'), callerRevision: snapshot.callerRevision,
        transcriptSequence: snapshot.revision, outcome: { eventId, status: 'accepted', callerRevision: snapshot.callerRevision,
          transcriptSequence: snapshot.revision } });
      receipts.push({ publicationSequence: n, sessionId: attachment.sessionId, eventId });
    }
    const source = { messageId: uuid(31), eventSequence: '2', provenance: {
      messageCreatedAt: at, eventId: uuid(32), eventOccurredAt: at, runId: uuid(10) } };
    const readyQuestion = { ...question(journal.target.question.id).question, state: 'pending' as const, reply: null };
    const observed: SelectedQuestionDeliveryObservation = { outcome: { requestId: uuid(6), attachment: { ...attachment },
      response: { ...seed.delivery, requestId: uuid(6), version: segment.version, deliveryAssociation: journal.association!,
        delivery: target.kind === 'question' ? { state: 'ready', source: { kind: 'question', ...source }, text, question: readyQuestion }
          : { state: 'ready', source: { kind: 'assistant', ...source }, text } },
      publication: { eventId: receipts.at(-1)!.eventId, status: 'accepted', observedCallerRevision: snapshot.callerRevision,
        observedTranscriptSequence: snapshot.revision, publicationReceipts: receipts } },
      snapshot: structuredClone(snapshot), acceptedAt: new Date(clock).toISOString(), inputThroughMs:
        segment.inputFirstSample === undefined ? 0 : (segment.inputLastSampleExclusive! - segment.inputFirstSample) / 8 };
    for (const [chunkIndex, receipt] of receipts.entries()) {
      const record = call.publications!.records.find(value => value.publicationSequence === receipt.publicationSequence)!;
      record.selectedDelivery = { response: structuredClone(observed.outcome.response) as SelectedQuestionVoicePresentation['response'],
        chunkIndex, chunkCount: receipts.length };
    }
    hooks.afterPublication?.(observed); deliveries.push(structuredClone(observed)); return observed;
  };
  const options: SelectedQuestionVoiceOptions = { seed, load: () => saved,
    persist(value) { saved = structuredClone(value); call.selectedQuestion = saved; events.push('host-persist'); },
    readCall: () => structuredClone(call), loadVoiceEvidence: () => voice,
    persistVoiceEvidence(value) { voice = structuredClone(value); events.push('voice-persist'); hooks.persistVoice?.(value); },
    active: () => !call.endedAt, now: () => clock,
    association: { async associate(value) { associations.push(value.request.questionId); return associated(value); } },
    freezer: { async freeze(_request, _channel, used) {
      assert.ok(!frozen || !used.includes(frozen.turn.sourceEventId));
      return hooks.freezer ? hooks.freezer() : structuredClone(frozen);
    } },
    assertFrozenCallerSpeechCurrent: source => assertFrozenCallerSpeechCurrent(source, acoustic),
    admissionSettled(request) {
      admissions.push(request.delegationId); events.push('admission-settled'); hooks.admit?.();
    },
    async capture(bindingId, turn) {
      assert.equal(bindingId, seed.delivery.bindingId);
      assert.ok(voice!.sources.some(s => s.frozen.turn.sourceEventId === turn.sourceEventId));
      events.push('capture'); captures.push(structuredClone(turn)); return hooks.capture ? hooks.capture(turn) : receipt(turn);
    },
    requests: {
      async associate() { throw new Error('Use selected association bootstrap'); },
      async question(journal) { events.push('question-read'); return hooks.question ? hooks.question(journal) : question(journal.target.question.id); },
      async result(journal) { return hooks.result ? hooks.result(journal)
        : { ...question(journal.target.question.id), state: 'waiting_for_reply' as const }; },
      async reply(journal) {
        events.push('reply-post'); posts.push(structuredClone(journal));
        assert.ok(voice!.sources.find(s => s.frozen.turn.sourceEventId === journal.reply!.input.source.sourceEventId)!.capture);
        assert.equal(saved!.questions.at(-1)!.record!.attempts.at(-1)!.state, 'dispatching');
        if (hooks.reply) return hooks.reply(journal);
        const result = question(journal.target.question.id, 'answered');
        return { ...result, replayed: false, question: { ...result.question,
          reply: { ...journal.reply!.input.reply, interpretedAnswer: 'Compare both.', receivedAt: new Date(clock).toISOString() } } };
      },
    }, deliver: publish,
  };
  const channel: LiveBackendChannel = { signal: abort.signal, getContext: () => structuredClone(snapshot),
    subscribeContext: () => () => {}, async publish() { throw new Error('Adapter must use eligible associated delivery'); } };
  const caller = (text = ' Yes.\t', startMs = 1000, observedAt = milliseconds + 2000) => {
    clock = Math.max(clock, observedAt);
    const sessionId = call.liveSessionId!, eventId = `${sessionId}_caller_${snapshot.revision + 1}`;
    const sequence = ++snapshot.revision; snapshot.callerRevision++;
    snapshot.fragments.push({ eventId, role: 'user', text, startMs, endMs: startMs + 200, sequence });
    const segment = activeSegment(); segment.inputFirstSample ??= call.liveGenerations!.inputNextSample;
    segment.inputLastSampleExclusive = segment.inputFirstSample + (startMs + 300) * 8;
    call.liveGenerations!.inputNextSample = segment.inputLastSampleExclusive; segment.transcript = structuredClone(snapshot);
    const sourceEventId = brokerIdentity('source', call.id, sessionId, eventId);
    frozen = { sessionId, firstEventId: eventId, callerRevision: snapshot.callerRevision,
      firstSequence: sequence, lastSequence: sequence, startMs, endMs: startMs + 200,
      audioThroughMs: startMs + 300, observedSpeechAtMs: observedAt,
      turn: { sourceEventId, revision: 1, itemId: `live-turn-${sourceEventId}`, role: 'user', text,
        occurredAt: new Date(milliseconds + segment.inputFirstSample / 8 + startMs).toISOString() } };
    acoustic = { sessionId, audioThroughMs: frozen.audioThroughMs, observedSpeechAtMs: observedAt };
    const request: LiveDelegationRequest = { callId: call.id, sessionId, delegationId: `delegation_${eventId}`,
      requestId: `local_${eventId}`, offsetMs: startMs + 250, transcript: structuredClone(snapshot) };
    return request;
  };
  const rotate = () => {
    const generations = call.liveGenerations!, old = activeSegment(); old.state = 'closed'; old.closedAt = new Date(clock).toISOString();
    old.outputDiscarded = true;
    const ordinal = generations.segments.length + 1, sessionId = `selected_g${ordinal}`, generationId = uuid(29 + ordinal);
    generations.deliveryAttachment = { generationId, sessionId };
    generations.segments.push({ generationId, ordinal, sessionId, createdAt: new Date(clock).toISOString(),
      readyAt: new Date(clock).toISOString(), state: 'active', version, pageDigest: 'b'.repeat(64), transcript: empty(), outputDiscarded: false });
    call.liveSessionId = sessionId; snapshot = empty(); frozen = undefined;
    acoustic = { sessionId, audioThroughMs: undefined, observedSpeechAtMs: undefined };
  };
  return { options, call, hooks, events, captures, posts, associations, deliveries, admissions, channel, caller, rotate,
    create: () => createSelectedQuestionVoiceAdapter(options), saved: () => structuredClone(saved!), voice: () => structuredClone(voice!),
    setText(value: string) { questionText = value; }, setTime(value: number) { clock = value; },
    addSpeechWithoutCaption() { acoustic = { ...acoustic, audioThroughMs: (acoustic.audioThroughMs ?? 0) + 20 }; },
    frozen: () => structuredClone(frozen!), changeFrozen(value: Partial<FrozenSelectedCallerTurn>) { frozen = { ...frozen!, ...value }; } };
}

function persistRoute(h: ReturnType<typeof fixture>, request: LiveDelegationRequest,
  decision: CallerRouteDecision, questions: CallerQuestionTarget[]) {
  if (decision.kind === 'task') assert.equal(decision.contextReference, undefined);
  assert.ok(['task', 'answer'].includes(decision.kind));
  const current = h.call.liveGenerations!.segments.find(segment => segment.sessionId === request.sessionId)!;
  const prepared = prepareCallerRouting(h.call, { frozen: h.frozen(),
    delegation: { sessionId: request.sessionId, delegationId: request.delegationId,
      requestId: request.requestId, offsetMs: request.offsetMs },
    resolutionInput: { version: current.version, questions, references: [], resultCoverage: { state: 'complete', count: 0 },
      publicationSequence: h.call.publications!.records.length } });
  h.call.callerRouting = prepared.journal;
  const ref = callerRouteRef(prepared.record);
  const record = prepared.record, frozen = record.frozen;
  const input: CallerIntentInput = { v: 'phone.caller-intent.v2', source: { callSid: h.call.callSid!,
    sourceEventId: frozen.turn.sourceEventId, itemId: frozen.turn.itemId, revision: 1,
    receivedAt: frozen.turn.occurredAt, role: 'user', text: frozen.turn.text }, generationId: current.generationId,
    sourceSessionId: request.sessionId, brokerInputDigest: record.inputDigest,
    presentationDigest: callerRoutePresentationDigest(record.resolutionInput), callDeadlineAt: seed.callDeadlineAt,
    version: current.version, questions: questions.map(callerIntentQuestionForTarget), results: [],
    resultCoverage: { state: 'complete', count: 0 } };
  const operationId = brokerIdentity('test-caller-intent', h.call.id, frozen.turn.sourceEventId);
  h.call.callerRouting = prepareCallerIntent(h.call, ref, { operationId, input });
  const clock = h.options.now!();
  const response: CallerIntentResponse = { v: 'phone.caller-intent.v2', operationId, bindingId: seed.delivery.bindingId,
    inputDigest: callerIntentInputDigest(operationId, seed.delivery.bindingId, input), brokerInputDigest: record.inputDigest,
    presentationDigest: input.presentationDigest, generationId: current.generationId, sourceSessionId: request.sessionId,
    callDeadlineAt: seed.callDeadlineAt, questions: input.questions, results: [], version: input.version,
    resultCoverage: input.resultCoverage, resultContextCoverage: { state: 'complete' },
    reservedAt: new Date(clock).toISOString(), validUntil: new Date(Math.min(clock + 10_000, Date.parse(seed.callDeadlineAt))).toISOString(),
    state: 'resolved', decision: decision.kind === 'answer' ? { kind: 'answer', target: callerIntentQuestionForTarget(decision.target) }
      : decision.kind === 'task' ? { kind: 'task' } : { kind: 'conversation' } };
  h.call.callerRouting = recordCallerIntentResponse(h.call, ref, response);
  h.call.callerRouting = decideCallerRouting(h.call, ref, { inputDigest: ref.inputDigest, decision });
  return ref;
}

test('routed selected answer consumes its persisted frozen source without calling the freezer', async t => {
  t.mock.method(Date, 'now', () => milliseconds);
  const h = fixture();
  h.options.withCallerAdmission = fn => withCallerAdmission(h.call, h.call.id, fn);
  h.hooks.persistVoice = value => { h.call.selectedQuestionVoice = structuredClone(value); };
  h.hooks.freezer = async () => { throw new Error('Frozen admission must not select caller speech again'); };
  const adapter = h.create(); await adapter.host.start();
  const targets = adapter.questionTargets(h.call.liveSessionId!); assert.equal(targets.length, 1);
  const request = h.caller(' Yes.\t');
  const ref = persistRoute(h, request, { kind: 'answer', target: targets[0] }, targets);
  await adapter.answerFrozen(request, ref, h.channel);
  await adapter.answerFrozen(request, ref, h.channel);
  assert.equal(h.posts.length, 1); assert.equal(h.captures.length, 1);
  assert.equal(h.posts[0].reply!.input.text, ' Yes.\t');
  assert.equal(h.posts[0].target.execution.bindingId, uuid(9));
  assert.deepEqual(h.call.delegations, {});
});

test('routed selected answer rechecks speech after a queued host operation before saving admission', { timeout: 4000 }, async t => {
  t.mock.method(Date, 'now', () => milliseconds);
  const h = fixture();
  h.options.withCallerAdmission = fn => withCallerAdmission(h.call, h.call.id, fn);
  const sourceSaved = deferred<void>(), pollEntered = deferred<void>(), releasePoll = deferred<void>();
  h.hooks.persistVoice = value => {
    h.call.selectedQuestionVoice = structuredClone(value);
    if (value.sources.length) sourceSaved.resolve();
  };
  const adapter = h.create(); await adapter.host.start();
  const targets = adapter.questionTargets(h.call.liveSessionId!), request = h.caller(' Yes.\t');
  const ref = persistRoute(h, request, { kind: 'answer', target: targets[0] }, targets);
  const original = structuredClone(h.call.callerRouting), raw = h.frozen();
  h.hooks.result = async () => {
    pollEntered.resolve(); await releasePoll.promise;
    return { ...question(), state: 'waiting_for_reply' };
  };
  const poll = adapter.host.poll();
  let answer: Promise<unknown> | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const bound = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(new Error('Selected host acoustic handshake timed out')), 1500);
  });
  try {
    await Promise.race([pollEntered.promise, bound, poll.then(() => { throw new Error('Poll did not hold'); })]);
    answer = adapter.answerFrozen(request, ref, h.channel).then(() => undefined, error => error);
    await Promise.race([sourceSaved.promise, bound, answer.then(error => { throw error ?? new Error('Answer ended before source save'); })]);
    assert.equal(h.voice().sources.length, 1);
    assert.deepEqual(h.saved().questions[0].admissions, []);
    h.addSpeechWithoutCaption(); // Same wall-clock millisecond, unchanged transcript and intent.
    releasePoll.resolve(); await Promise.race([poll, bound]);
    const error = await Promise.race([answer, bound]);
    assert.ok(error instanceof Error); assert.equal(error.message, 'selected_question_reply_unqualified');
    assert.deepEqual(h.saved().questions[0].admissions, []);
    assert.equal(h.posts.length, 0); assert.equal(h.captures.length, 0);
    assert.deepEqual(h.voice().sources[0].frozen, raw);
    assert.deepEqual(h.call.callerRouting, original);
  } finally {
    releasePoll.resolve(); clearTimeout(timer);
    await Promise.allSettled([poll, ...(answer ? [answer] : [])]);
  }
});

test('task intent cannot enter the selected answer port even after the complete question was published', async t => {
  t.mock.method(Date, 'now', () => milliseconds);
  const h = fixture();
  h.options.withCallerAdmission = fn => withCallerAdmission(h.call, h.call.id, fn);
  h.hooks.persistVoice = value => { h.call.selectedQuestionVoice = structuredClone(value); };
  const adapter = h.create(); await adapter.host.start();
  const targets = adapter.questionTargets(h.call.liveSessionId!);
  const request = h.caller('Buy ten dollars of NVDA.');
  const ref = persistRoute(h, request, { kind: 'task' }, targets);
  await assert.rejects(adapter.answerFrozen(request, ref, h.channel), /caller_route_decision_mismatch/);
  assert.equal(h.posts.length, 0); assert.equal(h.captures.length, 0);
  assert.equal(h.voice().sources.length, 0);
});

test('routed selected source survives rotation for capture but cannot freshly answer from its old presentation', async t => {
  t.mock.method(Date, 'now', () => milliseconds);
  const h = fixture();
  h.options.withCallerAdmission = fn => withCallerAdmission(h.call, h.call.id, fn);
  h.hooks.persistVoice = value => { h.call.selectedQuestionVoice = structuredClone(value); };
  const adapter = h.create(); await adapter.host.start();
  const targets = adapter.questionTargets(h.call.liveSessionId!);
  const request = h.caller('Yes.');
  const ref = persistRoute(h, request, { kind: 'answer', target: targets[0] }, targets);
  const raw = h.frozen(); h.rotate();
  await assert.rejects(adapter.answerFrozen(request, ref, h.channel), /caller_route_stale_source/);
  assert.equal(h.posts.length, 0);
  assert.deepEqual(h.call.callerRouting!.records[0].frozen, raw);
  assert.deepEqual(adapter.questionTargets(h.call.liveSessionId!), []);
});

test('routed q1 and q2 answers retain separate exact associations and replay their original source', async t => {
  t.mock.method(Date, 'now', () => milliseconds);
  const h = fixture();
  h.options.withCallerAdmission = fn => withCallerAdmission(h.call, h.call.id, fn);
  h.hooks.persistVoice = value => { h.call.selectedQuestionVoice = structuredClone(value); };
  const adapter = h.create(); await adapter.host.start();
  const firstTargets = adapter.questionTargets(h.call.liveSessionId!), first = h.caller('Yes.');
  const firstRef = persistRoute(h, first, { kind: 'answer', target: firstTargets[0] }, firstTargets);
  await adapter.answerFrozen(first, firstRef, h.channel);
  const q1 = structuredClone(h.saved().questions[0]);
  h.hooks.result = async () => ({ ...question(uuid(17)), state: 'waiting_for_reply', deliveryAssociation: association(uuid(7)) });
  h.hooks.question = async journal => question(journal.target.question.id, journal.target.question.id === uuid(7) ? 'answered' : 'pending');
  await adapter.host.poll();
  const secondTargets = adapter.questionTargets(h.call.liveSessionId!);
  assert.equal(secondTargets[0].questionId, uuid(17));
  const second = h.caller('No.', 4000, milliseconds + 6000);
  const secondRef = persistRoute(h, second, { kind: 'answer', target: secondTargets[0] }, secondTargets);
  await adapter.answerFrozen(second, secondRef, h.channel);
  await adapter.answerFrozen(first, firstRef, h.channel);
  assert.deepEqual(h.saved().questions[0], q1); assert.equal(h.posts.length, 2);
  assert.equal(h.posts[0].target.question.id, uuid(7)); assert.equal(h.posts[1].target.question.id, uuid(17));
  assert.deepEqual(h.posts[0].target.execution, h.posts[1].target.execution);
  assert.notEqual(h.posts[0].association!.id, h.posts[1].association!.id);
  assert.notEqual(h.posts[0].reply!.input.source.sourceEventId, h.posts[1].reply!.input.source.sourceEventId);
});

test('selected answer expiry during capture retains the original receipt and refuses the first POST', async t => {
  t.mock.method(Date, 'now', () => milliseconds);
  const h = fixture();
  h.options.withCallerAdmission = fn => withCallerAdmission(h.call, h.call.id, fn);
  h.hooks.persistVoice = value => { h.call.selectedQuestionVoice = structuredClone(value); };
  const adapter = h.create(); await adapter.host.start();
  const targets = adapter.questionTargets(h.call.liveSessionId!), request = h.caller('Yes.');
  const ref = persistRoute(h, request, { kind: 'answer', target: targets[0] }, targets);
  const intent = structuredClone(h.call.callerRouting!.records[0].intent!);
  h.hooks.capture = async turn => { h.setTime(Date.parse(intent.response!.validUntil)); return receipt(turn); };
  await adapter.answerFrozen(request, ref, h.channel);
  assert.equal(h.posts.length, 0); assert.equal(h.captures.length, 1);
  assert.equal(h.saved().questions[0].admissions[0].localRefusal?.code, 'presentation_invalidated');
  assert.deepEqual(h.call.callerRouting!.records[0].intent, intent);
  await adapter.answerFrozen(request, ref, h.channel);
  assert.equal(h.posts.length, 0); assert.equal(h.captures.length, 1);
});

test('selected expiry during dispatch-marker persistence is a local refusal with zero HTTP POSTs', async t => {
  t.mock.method(Date, 'now', () => milliseconds);
  const h = fixture();
  h.options.withCallerAdmission = fn => withCallerAdmission(h.call, h.call.id, fn);
  h.hooks.persistVoice = value => { h.call.selectedQuestionVoice = structuredClone(value); };
  const adapter = h.create(); await adapter.host.start();
  const targets = adapter.questionTargets(h.call.liveSessionId!), request = h.caller('Yes.');
  const ref = persistRoute(h, request, { kind: 'answer', target: targets[0] }, targets);
  const intent = structuredClone(h.call.callerRouting!.records[0].intent!);
  const persist = h.options.persist;
  h.options.persist = value => {
    persist(value);
    if (value.questions.at(-1)!.record?.attempts.at(-1)?.state === 'dispatching') h.setTime(Date.parse(intent.response!.validUntil));
  };
  await adapter.answerFrozen(request, ref, h.channel);
  assert.equal(h.posts.length, 0); assert.equal(h.captures.length, 1);
  const attempt = h.saved().questions[0].record!.attempts[0];
  assert.equal(attempt.state, 'refused'); assert.equal(attempt.dispatches, 1);
  assert.equal(attempt.refusal?.kind, 'first_attempt_refusal');
  if (attempt.refusal?.kind === 'first_attempt_refusal') assert.equal(attempt.refusal.origin, 'local_validation');
  assert.deepEqual(h.call.callerRouting!.records[0].intent, intent);
});

test('explicit caller source and acknowledged capture reply to selected original run without ordinary submission', async () => {
  const h = fixture(), adapter = h.create(); await adapter.host.start();
  const request = h.caller(); await adapter.delegate(request, h.channel);
  assert.equal(h.posts.length, 1); assert.equal(h.captures.length, 1);
  assert.equal(h.posts[0]!.target.execution.bindingId, uuid(9)); assert.equal(h.posts[0]!.target.execution.runId, uuid(10));
  assert.equal(h.posts[0]!.callerSource!.bindingId, seed.delivery.bindingId);
  assert.equal(h.posts[0]!.reply!.input.text, ' Yes.\t'); assert.deepEqual(h.call.delegations, {});
  assert.ok(h.events.indexOf('admission-settled') < h.events.indexOf('capture'));
  assert.ok(h.events.indexOf('capture') < h.events.indexOf('reply-post'));
  await adapter.delegate(request, h.channel); assert.equal(h.posts.length, 1); assert.equal(h.captures.length, 1);
  assert.equal(h.admissions.length, 2);
});

test('complete question over 500 bytes needs every exact accepted chunk and keeps all meanings', async () => {
  const h = fixture(); h.setText('The question and its saved context remain together. '.repeat(18));
  const adapter = h.create(); await adapter.host.start();
  const presentation = h.voice().presentations[0]!;
  assert.ok(presentation.publicationReceipts.length > 1);
  assert.equal(presentation.response.delivery.state, 'ready');
  assert.ok('question' in presentation.response.delivery);
  assert.equal(presentation.response.delivery.question.yesMeaning, 'Compare both.');
  const altered = structuredClone(h.deliveries[0]!); altered.outcome.publication!.publicationReceipts!.pop();
  assert.equal(selectedQuestionPresentation(h.call, h.saved().questions[0]!.record!.base, altered), undefined);
});

test('late rejection invalidates an exact saved question and cannot fall back to an earlier presentation', async () => {
  const h = fixture(), adapter = h.create(); await adapter.host.start(); await adapter.host.start();
  const latest = h.voice().presentations.at(-1)!;
  h.call.publications!.rejections.push({ sessionId: latest.sessionId, eventId: latest.publicationReceipts[0]!.eventId, code: 'provider_error' });
  await assert.rejects(adapter.delegate(h.caller(), h.channel), /selected_voice_question_not_presented/);
  assert.equal(h.posts.length, 0); assert.equal(h.voice().sources.length, 0);
});

test('rejection before presentation persistence prevents an actionable barrier', async () => {
  const h = fixture(); h.hooks.afterPublication = observed => {
    h.call.publications!.rejections.push({ sessionId: observed.outcome.attachment.sessionId,
      eventId: observed.outcome.publication!.publicationReceipts![0]!.eventId, code: 'provider_error' });
  };
  const adapter = h.create(); await adapter.host.start(); assert.equal(adapter.evidence().presentations.length, 0);
  await assert.rejects(adapter.delegate(h.caller(), h.channel), /selected_voice_question_not_presented/);
});

test('delayed caption from before question receipt is refused, while a first post-receipt sample at zero is accepted', async () => {
  const h = fixture(), adapter = h.create(); await adapter.host.start();
  const early = h.caller('yes', 0, milliseconds); await assert.rejects(adapter.delegate(early, h.channel), /precedes_question/);
  assert.equal(h.posts.length, 0);
  const fresh = fixture(), next = fresh.create(); await next.host.start();
  await next.delegate(fresh.caller('yes', 0, milliseconds + 1), fresh.channel);
  assert.equal(fresh.posts.length, 1); assert.equal(fresh.voice().sources[0]!.frozen.startMs, 0);
});

test('source ID, original caption text and media origin cannot be substituted', async () => {
  const h = fixture(), adapter = h.create(); await adapter.host.start();
  const request = h.caller(); const frozen = h.frozen();
  h.changeFrozen({ turn: { ...frozen.turn, occurredAt: new Date(milliseconds + 999).toISOString() } });
  await assert.rejects(adapter.delegate(request, h.channel), /precedes_question/);
  assert.equal(h.captures.length, 0); assert.equal(h.posts.length, 0);
});

test('old generation output and old source callback cannot answer a re-presented question', async () => {
  const h = fixture(), adapter = h.create(); await adapter.host.start(); const old = h.caller();
  const oldObservation = structuredClone(h.deliveries[0]!); h.rotate();
  assert.equal(selectedQuestionPresentation(h.call, h.saved().questions[0]!.record!.base, oldObservation), undefined);
  await assert.rejects(adapter.delegate(old, h.channel), /source_inactive/);
  await assert.rejects(adapter.delegate(h.caller('yes', 1000, milliseconds + 5000), h.channel), /question_not_presented/);
  await adapter.host.start();
  await adapter.delegate(h.caller('yes', 3000, milliseconds + 8000), h.channel);
  assert.equal(h.posts.length, 1); assert.equal(h.voice().sources[0]!.answer.sourceSessionId, 'selected_g2');
  assert.equal(h.posts[0]!.target.execution.runId, uuid(10));
});

test('lost capture acknowledgement survives restart and performs the first reply only after exact capture recovery', async () => {
  const h = fixture(), adapter = h.create(); await adapter.host.start();
  h.hooks.capture = async () => { throw new PrimaryApiError('request_timeout'); };
  await assert.rejects(adapter.delegate(h.caller(), h.channel));
  const raw = h.voice().sources[0]!.frozen.turn; assert.equal(h.posts.length, 0);
  h.hooks.capture = async turn => ({ ...receipt(turn), replayed: true });
  const recovered = h.create(); await recovered.host.recoverReply();
  assert.deepEqual(h.captures, [raw, raw]); assert.equal(h.posts.length, 1);
  assert.deepEqual(parseSelectedQuestionVoiceEvidence(JSON.parse(JSON.stringify(h.voice()))), h.voice());
});

test('unknown reply POST is reconciled by a question read after restart without dispatching again', async () => {
  const h = fixture(), adapter = h.create(); await adapter.host.start();
  h.hooks.reply = async () => { throw new PrimaryApiError('request_timeout'); };
  await adapter.delegate(h.caller(), h.channel); assert.equal(h.posts.length, 1);
  const original = h.posts[0]!.reply!.input.reply;
  h.hooks.question = async () => { const q = question(uuid(7), 'answered');
    return { ...q, question: { ...q.question, reply: { ...original, interpretedAnswer: 'Compare both.', receivedAt: at } } }; };
  await h.create().host.recoverReply(); assert.equal(h.posts.length, 1);
  assert.equal(h.saved().questions[0]!.record!.attempts[0]!.state, 'accepted');
});

test('durable answer admission releases the rotation drain before capture or reply HTTP', async () => {
  const h = fixture(), adapter = h.create(); await adapter.host.start();
  const captured = deferred<PrimaryTurnReceipt>(), admitted = deferred<void>();
  h.hooks.capture = () => captured.promise; h.hooks.admit = () => admitted.resolve();
  const pending = adapter.delegate(h.caller(), h.channel); await admitted.promise;
  assert.equal(h.saved().questions[0]!.admissions.length, 1); assert.equal(h.posts.length, 0);
  adapter.stop(); captured.resolve(receipt(h.frozen().turn)); await pending;
  assert.equal(h.posts.length, 0); assert.equal(h.saved().questions[0]!.admissions[0]!.localRefusal!.code, 'call_ended');
});

test('stop after source evidence save retains capture even if the host never admits its reply', async () => {
  const h = fixture(), adapter = h.create(); await adapter.host.start();
  h.hooks.persistVoice = value => { if (value.sources.length) h.call.endedAt = new Date(milliseconds + 2500).toISOString(); };
  await assert.rejects(adapter.delegate(h.caller(), h.channel), /inactive|reply_unqualified/);
  assert.equal(h.voice().sources.length, 1); assert.equal(h.saved().questions[0]!.admissions.length, 0);
  const result = await adapter.prepareCloseout();
  assert.equal(h.captures.length, 1); assert.equal(h.posts.length, 0);
  assert.deepEqual(result, { pendingAdmissions: 0, pendingCaptures: 0, pendingReplies: 0, ready: true });
});

test('ended call keeps unknown dispatched reply pending while a definitely unsent captured answer can close', async () => {
  const unknown = fixture(), one = unknown.create(); await one.host.start();
  unknown.hooks.reply = async () => { throw new PrimaryApiError('request_timeout'); };
  await one.delegate(unknown.caller(), unknown.channel);
  unknown.call.endedAt = new Date(milliseconds + 3000).toISOString();
  assert.deepEqual(await one.prepareCloseout(), { pendingAdmissions: 0, pendingCaptures: 0, pendingReplies: 1, ready: false });
  assert.equal(unknown.posts.length, 1);
  const unsent = fixture(), two = unsent.create(); await two.host.start();
  unsent.hooks.question = async () => { throw new PrimaryApiError('request_timeout'); };
  await assert.rejects(two.delegate(unsent.caller(), unsent.channel));
  unsent.call.endedAt = new Date(milliseconds + 3000).toISOString();
  assert.deepEqual(await two.prepareCloseout(), { pendingAdmissions: 0, pendingCaptures: 0, pendingReplies: 0, ready: true });
  assert.equal(unsent.posts.length, 0);
});

test('q1 answer then q2 creates another exact association on the same run and requires another presentation/source', async () => {
  const h = fixture(), adapter = h.create(); await adapter.host.start();
  await adapter.delegate(h.caller(), h.channel); const q1 = structuredClone(h.saved().questions[0]);
  h.hooks.result = async () => ({ ...question(uuid(17)), state: 'waiting_for_reply', deliveryAssociation: association(uuid(7)) });
  h.hooks.question = async journal => question(journal.target.question.id, journal.target.question.id === uuid(7) ? 'answered' : 'pending');
  await adapter.host.poll();
  assert.deepEqual(h.associations, [uuid(7), uuid(17)]); assert.deepEqual(h.saved().questions[0], q1);
  await adapter.delegate(h.caller('No.', 4000, milliseconds + 6000), h.channel);
  assert.equal(h.posts.length, 2); assert.equal(h.posts[1]!.target.question.id, uuid(17));
  assert.deepEqual(h.posts[1]!.target.execution, h.posts[0]!.target.execution);
  assert.notEqual(h.posts[1]!.association!.id, h.posts[0]!.association!.id);
  assert.notEqual(h.posts[1]!.reply!.input.source.sourceEventId, h.posts[0]!.reply!.input.source.sourceEventId);
});

test('evidence rejects changed owner, source identity, raw rendering, receipt, and historical barrier', async () => {
  const h = fixture(), adapter = h.create(); await adapter.host.start(); await adapter.delegate(h.caller(), h.channel);
  const saved = h.voice();
  assert.throws(() => assertSelectedQuestionVoiceScope({ ...seed, scope: { ...seed.scope, ownerPrincipalId: uuid(90) } }, saved));
  const raw = structuredClone(saved); raw.sources[0]!.frozen.turn.text = 'Yes.';
  assert.throws(() => parseSelectedQuestionVoiceEvidence(raw));
  const barrier = structuredClone(saved); barrier.presentations[0]!.acceptedAt = new Date(milliseconds - 1).toISOString();
  assert.throws(() => assertSelectedQuestionVoiceEvidenceUpdate(saved, barrier));
  const capture = structuredClone(saved); capture.sources[0]!.capture!.bodyDigest = 'c'.repeat(64);
  assert.throws(() => parseSelectedQuestionVoiceEvidence(capture));
  const identity = structuredClone(saved); identity.sources[0]!.frozen.firstEventId = 'different_event';
  assert.throws(() => assertSelectedQuestionVoiceScope(seed, identity));
});

test('changed explicit delegation request ID is refused on exact source replay', async () => {
  const h = fixture(), adapter = h.create(); await adapter.host.start(); const request = h.caller();
  await adapter.delegate(request, h.channel);
  await assert.rejects(adapter.delegate({ ...request, requestId: 'changed_request' }, h.channel), /delegation_changed/);
  assert.equal(h.posts.length, 1);
});

test('call journal validation retains rejected historical proof and refuses missing source evidence or moved clocks', async () => {
  const h = fixture(), adapter = h.create(); await adapter.host.start(); await adapter.delegate(h.caller(), h.channel);
  const saved = h.voice(); assert.deepEqual(validateSelectedQuestionVoiceCall(h.call, saved), saved);
  h.call.publications!.rejections.push({ sessionId: saved.presentations[0]!.sessionId,
    eventId: saved.presentations[0]!.publicationReceipts[0]!.eventId, code: 'late_rejection' });
  assert.deepEqual(validateSelectedQuestionVoiceCall(h.call, saved), saved);
  assert.throws(() => validateSelectedQuestionVoiceCall(h.call, { ...saved, sources: [] }), /evidence_missing/);
  const moved = structuredClone(h.call); moved.liveGenerations!.segments[0]!.inputFirstSample = 8;
  assert.throws(() => validateSelectedQuestionVoiceCall(moved, saved), /audio_origin_changed/);
});

test('stop during source freezing releases the drain but keeps binding close pending until the source is retained', async () => {
  const h = fixture(), adapter = h.create(); await adapter.host.start(); const request = h.caller();
  const freeze = deferred<FrozenSelectedCallerTurn | undefined>(), entered = deferred<void>();
  h.hooks.freezer = () => { entered.resolve(); return freeze.promise; };
  const pending = adapter.delegate(request, h.channel); await entered.promise;
  adapter.stop(); assert.equal(h.admissions.length, 1);
  assert.deepEqual(await adapter.prepareCloseout(), { pendingAdmissions: 1, pendingCaptures: 0, pendingReplies: 0, ready: false });
  freeze.resolve(h.frozen()); await assert.rejects(pending, /call_inactive/);
  assert.equal(h.voice().sources.length, 1);
  assert.deepEqual(await adapter.prepareCloseout(), { pendingAdmissions: 0, pendingCaptures: 0, pendingReplies: 0, ready: true });
  assert.equal(h.captures.length, 1); assert.equal(h.posts.length, 0);
});

test('rotation during source freezing retains original source without relabelling or replying from old presentation', async () => {
  const h = fixture(), adapter = h.create(); await adapter.host.start(); const request = h.caller();
  const original = h.frozen(), freeze = deferred<FrozenSelectedCallerTurn | undefined>(), entered = deferred<void>();
  h.hooks.freezer = () => { entered.resolve(); return freeze.promise; };
  const pending = adapter.delegate(request, h.channel); await entered.promise;
  h.rotate(); freeze.resolve(original); await assert.rejects(pending, /reply_unqualified/);
  assert.deepEqual(h.voice().sources[0]!.frozen, original); assert.equal(h.posts.length, 0);
  await adapter.prepareCloseout(); assert.deepEqual(h.captures, [original.turn]);
});
