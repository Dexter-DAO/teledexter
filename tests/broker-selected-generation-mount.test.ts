import assert from 'node:assert/strict';
import test from 'node:test';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createGenerationVoice } from '../src/broker/generationVoice.js';
import { createSelectedQuestionRecovery, selectedQuestionSourceAnchors } from '../src/broker/selectedQuestionVoice.js';
import { brokerIdentity } from '../src/broker/backend.js';
import { BrokerStore } from '../src/broker/store.js';
import { PrimaryApiError, type PrimaryApiClient, type PrimaryTurn } from '../src/broker/primaryApi.js';
import { createRequestDeliveryJournal, retainRequestDeliveryAssociation, PHONE_REQUEST_DELIVERY_VERSION,
  type RequestDeliveryAssociation, type RequestDeliveryClient, type RequestDeliveryJournal,
  type RequestDeliveryQuestionResult, type RequestDeliveryResult } from '../src/broker/requestDelivery.js';
import { turnBodyDigest } from '../src/broker/reconciliation.js';
import type { EligibleContextClient, EligiblePhoneCall } from '../src/broker/eligibleContext.js';
import type { BrokerLiveSession, BrokerLiveSessionOptions, LiveBackendChannel, LiveDelegationRequest, LiveTranscriptSnapshot } from '../src/live/types.js';

// Actual generation controller, selected adapter, backend freezer, publication
// journal and BrokerStore. Provider connections and Primary HTTP are injected.
const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const owner = uuid(1), primaryId = uuid(2), threadId = uuid(3), bindingId = uuid(4);
const originalRequest = uuid(5), executionBinding = uuid(6), runId = uuid(7), q1 = uuid(8), q2 = uuid(9);
const callSid = `CA${'a'.repeat(32)}`;
const speech = Buffer.alloc(160, 0).toString('base64'), silence = Buffer.alloc(160, 255).toString('base64');
const pause = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
async function until(predicate: () => boolean) {
  for (let n = 0; n < 500 && !predicate(); n++) await pause(10);
  assert.equal(predicate(), true, 'expected selected generation boundary');
}
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; }); return { promise, resolve };
}
const empty = (): LiveTranscriptSnapshot => ({ revision: 0, callerRevision: 0, fragments: [] });

async function fixture(t: test.TestContext, initialHooks: { firstDelivery?: Promise<void> } = {}) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-selected-generation-'));
  let store = new BrokerStore(directory);
  const call = store.reserve({ requestId: uuid(10), number: '+12025550123', direction: 'outbound',
    purpose: 'An unrelated purpose must never become a new task.', callSid,
    continuation: { originalRequestId: originalRequest, questionId: q1, questionRevision: 1 } }, 'authenticated_broker_control').call;
  const createdAt = new Date().toISOString(), expiresAt = new Date(Date.now() + 120_000).toISOString();
  const deadline = new Date(Date.now() + 180_000).toISOString();
  const ref = { assignmentId: call.requestId, revision: 1 as const, digest: 'a'.repeat(64) };
  let epoch = 1, questionId = q1, completed = false, ordinarySubmits = 0, loseCapture = false, loseReply = false;
  const captures: PrimaryTurn[] = [], posts: RequestDeliveryJournal[] = [], associations: string[] = [], problems: string[] = [];
  const answered = new Map<string, RequestDeliveryJournal>();
  const hooks: { firstDelivery?: Promise<void>; beforeQuestion?: () => Promise<void> } = { ...initialHooks };
  const version = () => ({ ownerPrincipalId: owner, primaryDexterId: primaryId, threadId,
    contextEpoch: String(epoch), threadEventSequence: String(epoch), digest: String(epoch).repeat(64) });
  const context = (): EligiblePhoneCall => ({ bindingId, callSid, state: 'open', callAssignmentRef: ref,
    context: { primaryDexterId: primaryId, threadId, displayName: 'Dexter', messages: [], history: {
      version: version(), pageDigest: 'b'.repeat(64), localContext: '', nextBeforeSequence: null, hasMore: false, omittedForBudget: false } } });
  store.recordPrimaryBinding(call.id, context());
  store.update(call.id, { status: 'in-progress', mediaStartedAt: createdAt });
  const association = (id: string): RequestDeliveryAssociation => ({ id: id === q1 ? uuid(11) : uuid(12),
    v: PHONE_REQUEST_DELIVERY_VERSION, delivery: { bindingId, callSid },
    execution: { bindingId: executionBinding, requestId: originalRequest, runId },
    question: { id, revision: 1 }, createdAt });
  const response = (journal: RequestDeliveryJournal): RequestDeliveryQuestionResult => {
    const saved = answered.get(journal.target.question.id);
    return { bindingId: executionBinding, requestId: originalRequest, deliveryAssociation: journal.association!,
      state: saved ? 'running' : 'waiting_for_reply', question: {
        id: journal.target.question.id, revision: 1, runId, threadId, agent: { id: primaryId, name: 'Dexter' },
        state: saved ? 'answered' : 'pending', runState: saved ? 'running' : 'waiting_for_reply',
        question: journal.target.question.id === q1 ? 'Continue the comparison?' : 'Include the second comparison?',
        context: 'Research only.', proposedAnswer: 'Compare both stocks.', yesMeaning: 'Compare both.', noMeaning: 'Stop the comparison.',
        createdAt, expiresAt, reply: saved ? { ...saved.reply!.input.reply,
          interpretedAnswer: 'Compare both.', receivedAt: new Date().toISOString() } : null } };
  };
  const requests: RequestDeliveryClient = {
    async associate() { throw new Error('Unexpected alias association'); },
    async question(journal) { await hooks.beforeQuestion?.(); return response(journal); },
    async result(journal): Promise<RequestDeliveryResult> {
      if (completed) return { bindingId: executionBinding, requestId: originalRequest, deliveryAssociation: journal.association!,
        state: 'completed', assistant: { messageId: uuid(20), eventId: uuid(23), sequence: '1', text: 'The comparison is saved.' } };
      if (answered.has(questionId)) return { bindingId: executionBinding, requestId: originalRequest,
        deliveryAssociation: journal.association!, state: 'running' };
      const next = response({ ...journal, target: { ...journal.target, question: { id: questionId, revision: 1 } } });
      return { ...next, deliveryAssociation: journal.association!, state: 'waiting_for_reply' };
    },
    async reply(journal) {
      posts.push(structuredClone(journal));
      if (loseReply) throw new PrimaryApiError('request_failed');
      answered.set(journal.target.question.id, structuredClone(journal));
      return { ...response(journal), replayed: false };
    },
  };
  const primary = { async submit() { ordinarySubmits++; throw new Error('Unexpected ordinary submission'); },
    async captureTurn(receivedBinding: string, turn: PrimaryTurn) {
      assert.equal(receivedBinding, bindingId); captures.push(structuredClone(turn));
      if (loseCapture) throw new PrimaryApiError('request_failed');
      return { bindingId, sourceEventId: turn.sourceEventId, revision: turn.revision, recordId: turn.sourceEventId,
        revisionRecordId: brokerIdentity('capture', turn.sourceEventId, String(turn.revision)), bodyDigest: turnBodyDigest(turn), replayed: false };
    } } as unknown as PrimaryApiClient;
  let deliveryCount = 0;
  const eligible: EligibleContextClient = {
    async fetch() { return context(); },
    async assert(_binding, expected) {
      if (expected.digest !== version().digest) throw new PrimaryApiError('http_error', 409, { code: 'context_changed', retryable: false });
      return { bindingId, callSid, current: true, version: expected };
    },
    async delivery(_binding, id, input, journal) {
      assert.ok(journal?.association); assert.equal(id, originalRequest);
      assert.deepEqual(journal.association.execution, association(q1).execution);
      if (deliveryCount++ === 0) await hooks.firstDelivery;
      const source = { messageId: input.source.kind === 'question' ? (input.source.questionId === q1 ? uuid(21) : uuid(22)) : uuid(20),
        eventSequence: String(epoch), provenance: { messageCreatedAt: createdAt, eventId: uuid(23), eventOccurredAt: createdAt, runId } };
      return { bindingId, callSid, requestId: id, version: input.version, deliveryAssociation: journal.association,
        delivery: input.source.kind === 'question' ? { state: 'ready', source: { ...source, kind: 'question' },
          question: { ...response(journal).question, state: 'pending', reply: null },
          text: `${response(journal).question.question} ${'The comparison remains research only. '.repeat(22)}Yes compares both. No stops the comparison.` }
          : { state: 'ready', source: { ...source, kind: 'assistant' }, text: 'The comparison is saved.' } };
    },
  };
  const voices: Array<{ options: BrokerLiveSessionOptions; caption: LiveTranscriptSnapshot;
    channel: LiveBackendChannel; comments: string[]; instructions: string[]; input: string[]; closed: boolean }> = [];
  const live = createGenerationVoice({ call: store.get(call.id)!, callDeadlineAt: deadline, apiKey: 'fixture-key', voice: 'marin',
    store, primary, context: eligible, selectedQuestion: { requests, pollIntervalMs: 50,
      association: { async associate(seed) {
        associations.push(seed.request.questionId); const saved = association(seed.request.questionId);
        return retainRequestDeliveryAssociation(createRequestDeliveryJournal({ deliveryCall: seed.deliveryCall,
          scope: seed.scope, request: seed.request, target: { delivery: seed.delivery, execution: saved.execution, question: saved.question } }),
        { association: saved, replayed: false });
      } } }, onReady() {}, onAudio() {}, onPlaybackClear() {}, onClosed() {}, onProblem: code => { problems.push(code); },
    voiceFactory(options) {
      const sessionId = `selected_generation_${voices.length + 1}`, abort = new AbortController();
      const listeners = new Set<(value: LiveTranscriptSnapshot) => void>();
      const value = { options, caption: empty(), channel: undefined as unknown as LiveBackendChannel,
        comments: [] as string[], instructions: [] as string[], input: [] as string[], closed: false };
      value.channel = { signal: abort.signal, getContext: () => structuredClone(value.caption),
        subscribeContext(listener) { listeners.add(listener); return () => { listeners.delete(listener); }; },
        async publish() { throw new Error('Unexpected ordinary channel publication'); } };
      const receipt = () => ({ eventId: `append_${sessionId}_${value.comments.length}`, status: 'accepted' as const,
        observedCallerRevision: value.caption.callerRevision, observedTranscriptSequence: value.caption.revision });
      const voice: BrokerLiveSession = {
        appendAudio(payload) { value.input.push(payload); return true; },
        async appendContext() { throw new Error('Unexpected flattened context'); },
        async appendInstructions(text) { value.instructions.push(text); return receipt(); },
        async appendCommentary(text) { value.comments.push(text); return receipt(); },
        async interruptPlayback() { return receipt(); }, resumePlayback() {}, getTranscript: () => structuredClone(value.caption),
        async close(reason = 'closed') {
          value.closed = true; abort.abort(); const result = { callId: call.id, sessionId, reason,
            finalized: true, transcriptPersisted: true, outputAudioDiscarded: true };
          options.onClosed(result); return result;
        },
      };
      voices.push(value); queueMicrotask(() => options.onReady({ callId: call.id, sessionId })); return voice;
    },
  });
  const currentCall = () => store.get(call.id)!;
  const say = async (text = ' Yes.\t', startMs = 0) => {
    await pause(5); const voice = voices.at(-1)!, sessionId = currentCall().liveSessionId!;
    while (voice.input.length * 20 < startMs) assert.equal(live.appendAudio(silence), true);
    assert.equal(live.appendAudio(speech), true);
    const sequence = voice.caption.revision + 1, eventId = `caller_${sequence}`;
    voice.caption = { revision: sequence, callerRevision: voice.caption.callerRevision + 1,
      fragments: [...voice.caption.fragments, { eventId, sequence, role: 'user', text, startMs, endMs: startMs + 20 }] };
    await voice.options.onTranscript(structuredClone(voice.caption));
    const request: LiveDelegationRequest = { callId: call.id, sessionId, requestId: `request_${sessionId}_${sequence}`,
      delegationId: `delegate_${sessionId}_${sequence}`, offsetMs: startMs + 20, transcript: structuredClone(voice.caption) };
    return { request, voice, delegate: () => voice.options.backend.delegate(request, voice.channel) };
  };
  t.after(async () => { await live.close('fixture_done'); store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  return { call, live, voices, captures, posts, associations, problems, hooks, requests, primary, say, currentCall,
    setQuestion: (id: string) => { questionId = id; }, complete: () => { completed = true; },
    rotate: async () => { epoch++; await live.appendInstructions('Continue the saved question.'); },
    loseCapture: (value: boolean) => { loseCapture = value; }, loseReply: (value: boolean) => { loseReply = value; },
    acceptSavedReply: () => { const value = posts.at(-1)!; answered.set(value.target.question.id, structuredClone(value)); },
    restartStore: () => { store.close(); store = new BrokerStore(directory); return store; },
    ordinarySubmits: () => ordinarySubmits,
  };
}

test('mounted generation publishes exact selected chunks and answers q1 then q2 on the original execution', async t => {
  const f = await fixture(t);
  await until(() => (f.currentCall().selectedQuestionVoice?.presentations.length ?? 0) === 1);
  assert.equal(f.voices[0]!.options.instructions.includes(f.call.purpose), false);
  assert.equal(f.voices[0]!.options.instructions.includes('registered task'), false);
  assert.equal(f.voices[0]!.instructions.length, 0);
  const first = f.currentCall().selectedQuestionVoice!.presentations[0]!;
  assert.ok(first.publicationReceipts.length > 1);
  assert.equal(f.currentCall().publications!.records.filter(r => r.kind === 'result').every(r => r.selectedDelivery), true);
  const answer = await f.say(); await answer.delegate(); await answer.delegate();
  assert.equal(f.posts.length, 1); assert.equal(f.captures[0]!.text, ' Yes.\t');
  for (let n = 0; n < 3; n++) assert.equal(f.live.appendAudio(silence), true);
  f.setQuestion(q2);
  await until(() => f.currentCall().selectedQuestionVoice?.presentations.some(p => p.response.deliveryAssociation.question.id === q2) === true);
  const secondPresentation = f.currentCall().selectedQuestionVoice!.presentations.find(p => p.response.deliveryAssociation.question.id === q2)!;
  assert.equal(secondPresentation.inputThroughMs, 80, 'publication persists the streamed input frontier before establishing its barrier');
  const second = await f.say('No.', 1400); await second.delegate();
  assert.equal(f.posts.length, 2); assert.deepEqual(f.associations, [q1, q2]);
  assert.equal(f.posts[0]!.association!.id === f.posts[1]!.association!.id, false);
  assert.deepEqual(f.posts.map(p => p.target.execution), [f.posts[0]!.target.execution, f.posts[0]!.target.execution]);
  f.complete(); await until(() => f.voices[0]!.comments.includes('The comparison is saved.'));
  assert.deepEqual(f.currentCall().delegations, {}); assert.equal(f.ordinarySubmits(), 0);
  assert.deepEqual(f.problems, []);
});

test('rotation while the actual source freezer waits retains g1 capture and refuses its new admission on g2', async t => {
  const f = await fixture(t);
  await until(() => (f.currentCall().selectedQuestionVoice?.presentations.length ?? 0) === 1);
  const answer = await f.say(), outcome = answer.delegate().then(() => undefined, error => error);
  const clocks = structuredClone(f.currentCall().liveGenerations!.clocks);
  await f.rotate();
  assert.equal(f.currentCall().liveSessionId, 'selected_generation_2');
  assert.match(String(await outcome), /reply_unqualified/);
  const source = f.currentCall().selectedQuestionVoice!.sources[0]!;
  assert.equal(source.frozen.sessionId, 'selected_generation_1'); assert.equal(f.posts.length, 0);
  await f.live.close('caller_disconnected');
  assert.equal((await f.live.prepareSelectedCloseout()).ready, true);
  assert.deepEqual(f.captures, [source.frozen.turn]);
  assert.deepEqual(f.currentCall().liveGenerations!.clocks, clocks);
  assert.equal(f.ordinarySubmits(), 0);
});

test('restart closeout recovers exact raw capture and refuses a definitely unsent selected reply', async t => {
  const f = await fixture(t);
  await until(() => (f.currentCall().selectedQuestionVoice?.presentations.length ?? 0) === 1);
  f.loseCapture(true); const answer = await f.say(); await assert.rejects(answer.delegate(), /request_failed/);
  const source = structuredClone(f.currentCall().selectedQuestionVoice!.sources[0]!.frozen.turn);
  await f.live.close('caller_disconnected'); f.loseCapture(false);
  const recovery = createSelectedQuestionRecovery({ callId: f.call.id, store: f.restartStore(), primary: f.primary, requests: f.requests });
  assert.deepEqual(await recovery.prepareCloseout(), { pendingAdmissions: 0, pendingCaptures: 0, pendingReplies: 0, ready: true });
  assert.ok(f.captures.length >= 2);
  for (const captured of f.captures) assert.deepEqual(captured, source);
  assert.equal(f.posts.length, 0);
  const anchors = selectedQuestionSourceAnchors(f.currentCall());
  assert.equal(anchors[0]!.replyPending, false); assert.deepEqual(anchors[0]!.frozen.turn, source);
  assert.ok(anchors[0]!.capture);
});

test('restart closeout leaves an unknown selected reply pending until GET proves its exact saved answer', async t => {
  const f = await fixture(t);
  await until(() => (f.currentCall().selectedQuestionVoice?.presentations.length ?? 0) === 1);
  f.loseReply(true); const answer = await f.say(); await answer.delegate();
  assert.equal(f.posts.length, 1); assert.equal(selectedQuestionSourceAnchors(f.currentCall())[0]!.replyPending, true);
  await f.live.close('caller_disconnected');
  const recovery = createSelectedQuestionRecovery({ callId: f.call.id, store: f.restartStore(), primary: f.primary, requests: f.requests });
  assert.deepEqual(await recovery.prepareCloseout(), { pendingAdmissions: 0, pendingCaptures: 0, pendingReplies: 1, ready: false });
  f.acceptSavedReply();
  assert.equal((await recovery.prepareCloseout()).ready, true);
  assert.equal(f.posts.length, 1); assert.equal(selectedQuestionSourceAnchors(f.currentCall())[0]!.replyPending, false);
});

test('a greeting before selected question publication cannot trigger the ordinary assignment', async t => {
  const gate = deferred<void>(), f = await fixture(t, { firstDelivery: gate.promise });
  try {
    await until(() => f.voices.length === 1 && f.currentCall().selectedQuestion?.questions[0]?.record !== undefined);
    const voice = f.voices[0]!;
    await assert.rejects(voice.options.backend.delegate({ callId: f.call.id, sessionId: 'selected_generation_1',
      delegationId: 'hello', requestId: 'hello_request', offsetMs: 0, transcript: empty() }, voice.channel), /question_not_presented/);
    assert.equal(f.posts.length, 0); assert.equal(f.captures.length, 0); assert.equal(f.ordinarySubmits(), 0);
    assert.deepEqual(f.currentCall().delegations, {});
  } finally { gate.resolve(); }
});
