import assert from 'node:assert/strict';
import test, { type TestContext } from 'node:test';
import { writeSync } from 'node:fs';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import path from 'node:path';
import { tmpdir } from 'node:os';
import { BrokerStore } from '../src/broker/store.js';
import { brokerIdentity, createPrimaryBackend, type BrokerDelegationRecord, type PrimaryBackend,
  type PrimaryBackendOptions } from '../src/broker/backend.js';
import type { LiveGenerationJournal } from '../src/broker/liveGeneration.js';
import { PrimaryApiError, type PrimaryQuestion, type PrimaryQuestionResult, type PrimaryReplyInput,
  type PrimaryResult, type PrimarySubmission, type PrimaryTurn } from '../src/broker/primaryApi.js';
import { beginBrokerPublication, settleBrokerPublication, rejectBrokerPublication, type BrokerPublicationReceipt } from '../src/broker/publications.js';
import { turnBodyDigest } from '../src/broker/reconciliation.js';
import type { LiveBackendChannel, LiveBackendUpdate, LiveDelegationRequest, LiveTranscriptSnapshot } from '../src/live/types.js';

// Actual backend and durable store; API and current-generation publication are injected boundaries.
const bindingId = '11111111-1111-4111-8111-111111111111';
const ownerPrincipalId = '22222222-2222-4222-8222-222222222222';
const primaryDexterId = '33333333-3333-4333-8333-333333333333';
const threadId = '44444444-4444-4444-8444-444444444444';
const runId = '55555555-5555-4555-8555-555555555555';
const questionId = '66666666-6666-4666-8666-666666666666';
const oldGenerationId = '77777777-7777-4777-8777-777777777777';
const newGenerationId = '88888888-8888-4888-8888-888888888888';
const messageId = '99999999-9999-4999-8999-999999999999';
const eventId = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
const oldSession = 'generation_old';
const newSession = 'generation_new';
const callSid = `CA${'1'.repeat(32)}`;
const wait = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
}
async function until(predicate: () => boolean) {
  for (let attempt = 0; attempt < 500; attempt++) {
    if (predicate()) return;
    await wait(2);
  }
  assert.ok(predicate(), 'expected bounded backend transition was not reached');
}
async function boundedStep<T>(label: string, work: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([work, new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`Generation fixture step timed out: ${label}`)), 2000);
    })]);
  } finally { clearTimeout(timer); }
}
type Diagnostic = (stage: string, event: 'before' | 'after' | 'rejected') => void;
function diagnosticSync<T>(diagnostic: Diagnostic | undefined, stage: string, start: () => T): T {
  diagnostic?.(stage, 'before');
  try {
    const value = start(); diagnostic?.(stage, 'after'); return value;
  } catch (error) { diagnostic?.(stage, 'rejected'); throw error; }
}
function diagnosticAwait<T>(diagnostic: Diagnostic | undefined, stage: string, start: () => Promise<T>): Promise<T> {
  if (!diagnostic) return start();
  diagnostic(stage, 'before');
  let work: Promise<T>;
  try { work = start(); }
  catch (error) { diagnostic(stage, 'rejected'); throw error; }
  return work.then(value => { diagnostic(stage, 'after'); return value; }, error => {
    diagnostic(stage, 'rejected'); throw error;
  });
}
function voiceContext(sessionId: string) {
  const controller = new AbortController();
  const listeners = new Set<(snapshot: LiveTranscriptSnapshot) => void>();
  const directPublications: LiveBackendUpdate[] = [];
  const snapshot: LiveTranscriptSnapshot = { revision: 0, callerRevision: 0, fragments: [] };
  let inputThroughMs = 0;
  const channel: LiveBackendChannel = {
    signal: controller.signal, getContext: () => structuredClone(snapshot),
    subscribeContext(listener) { listeners.add(listener); return () => { listeners.delete(listener); }; },
    async publish(update) {
      directPublications.push(update);
      return { eventId: update.id, status: controller.signal.aborted ? 'closed' : 'accepted',
        observedCallerRevision: snapshot.callerRevision, observedTranscriptSequence: snapshot.revision };
    },
  };
  const add = (text: string, startMs: number, endMs: number, eventId = `${sessionId}_caller_${snapshot.revision + 1}`) => {
    snapshot.revision++; snapshot.callerRevision++;
    snapshot.fragments.push({ eventId, role: 'user', text, startMs, endMs, sequence: snapshot.revision });
    for (const listener of listeners) listener(structuredClone(snapshot));
  };
  return { controller, channel, add, directPublications,
    advanceAudio: (offsetMs: number) => { inputThroughMs = Math.max(inputThroughMs, offsetMs); },
    inputThroughMs: () => inputThroughMs };
}

async function fixture(t: TestContext, diagnostic?: Diagnostic) {
  const directory = await diagnosticAwait(diagnostic, 'setup.mkdtemp', () => mkdtemp(path.join(tmpdir(), 'broker-generation-backend-')));
  const stores: BrokerStore[] = [];
  let store = diagnosticSync(diagnostic, 'setup.store', () => new BrokerStore(directory)); stores.push(store);
  const callLifetime = new AbortController();
  const cleanups: Array<() => void> = [];
  const tasks: Promise<void>[] = [];
  const contexts = new Map([[oldSession, voiceContext(oldSession)], [newSession, voiceContext(newSession)]]);
  const call = diagnosticSync(diagnostic, 'setup.reserve', () => store.reserve({ requestId: 'synthetic-generation-call', number: '+15550000001',
    direction: 'inbound', purpose: '', callSid }).call);
  diagnosticSync(diagnostic, 'setup.binding', () => store.recordPrimaryBinding(call.id, { bindingId, callSid, state: 'open',
    context: { primaryDexterId, threadId, displayName: 'Dexter', messages: [] } }));
  const version = { ownerPrincipalId, primaryDexterId, threadId, contextEpoch: '1', threadEventSequence: '1', digest: 'a'.repeat(64) };
  const generations: LiveGenerationJournal = { version: 1, callId: call.id,
    binding: { bindingId, callSid, ownerPrincipalId, primaryDexterId, threadId },
    clocks: { callDeadlineAt: new Date(Date.parse(call.createdAt) + 300_000).toISOString(), mediaStartedAt: call.createdAt },
    phase: 'active', inputNextSample: 0, deliveryAttachment: { generationId: oldGenerationId, sessionId: oldSession },
    segments: [{ generationId: oldGenerationId, ordinal: 1, createdAt: call.createdAt, readyAt: call.createdAt,
      state: 'active', sessionId: oldSession, version, pageDigest: 'b'.repeat(64), inputFirstSample: 0,
      transcript: contexts.get(oldSession)!.channel.getContext(), outputDiscarded: false }] };
  diagnosticSync(diagnostic, 'setup.update', () => store.update(call.id, { status: 'in-progress', liveSessionId: oldSession, mediaStartedAt: call.createdAt,
    liveGenerations: generations }));
  const posts: Array<{ binding: string; id: string; input: PrimarySubmission }> = [];
  const reads: string[] = [];
  const captures: PrimaryTurn[] = [];
  const replies: Array<{ binding: string; requestId: string; questionId: string; input: PrimaryReplyInput }> = [];
  const questionReads: Array<{ requestId: string; questionId: string }> = [];
  const publications: Array<{ sourceSessionId: string; deliverySessionId: string; entry: BrokerDelegationRecord; update?: LiveBackendUpdate }> = [];
  const admissions: Array<{ request: LiveDelegationRequest; entries: BrokerDelegationRecord[] }> = [];
  const completed = (id: string): PrimaryResult => ({ requestId: id, bindingId, state: 'completed', assistant: {
    messageId, eventId, sequence: '12', text: 'The saved account result is ready.' } });
  const behavior = {
    submit: async (id: string): Promise<PrimaryResult> => ({ bindingId, requestId: id, state: 'queued', replayed: false }),
    result: async (id: string): Promise<PrimaryResult> => completed(id),
    question: async (_id: string, _questionId: string): Promise<PrimaryQuestionResult> => { throw new Error('No synthetic question configured'); },
    reply: async (_id: string, _questionId: string, _input: PrimaryReplyInput): Promise<PrimaryQuestionResult> => { throw new Error('No synthetic reply configured'); },
    publicationStatus: 'accepted' as 'accepted' | 'rejected',
    rejectBeforeReceipt: false,
  };
  const api: PrimaryBackendOptions['api'] = {
    async submit(binding, id, input) {
      const saved = JSON.parse(await readFile(path.join(directory, 'calls.json'), 'utf8')).calls[call.id].delegations[id];
      assert.equal(saved.status, 'dispatching'); assert.deepEqual(saved.submission, input);
      posts.push({ binding, id, input: structuredClone(input) });
      return behavior.submit(id);
    },
    async result(_binding, id) { reads.push(id); return behavior.result(id); },
    async captureTurn(binding, turn) {
      captures.push(structuredClone(turn));
      return { bindingId: binding, sourceEventId: turn.sourceEventId, revision: turn.revision, recordId: turn.sourceEventId,
        revisionRecordId: eventId, bodyDigest: turnBodyDigest(turn), replayed: false };
    },
    async question(_binding, id, questionId) { questionReads.push({ requestId: id, questionId }); return behavior.question(id, questionId); },
    async reply(binding, requestId, questionId, input) {
      const saved = JSON.parse(await readFile(path.join(directory, 'calls.json'), 'utf8')).calls[call.id].delegations[requestId].questions[questionId].reply;
      assert.equal(saved.status, 'dispatching'); assert.deepEqual(saved.input, input);
      replies.push({ binding, requestId, questionId, input: structuredClone(input) });
      return behavior.reply(requestId, questionId, input);
    },
  };
  const observation: NonNullable<PrimaryBackendOptions['observation']> = {
    signal: callLifetime.signal,
    async publish(entry, update) {
      diagnostic?.('observer.publish', 'before');
      try {
        const deliverySessionId = store.get(call.id)!.liveSessionId!;
        const snapshot = contexts.get(deliverySessionId)!.channel.getContext();
        publications.push({ sourceSessionId: entry.sessionId, deliverySessionId, entry: structuredClone(entry), ...(update ? { update } : {}) });
        const identity = { callId: call.id, callSid, bindingId, sessionId: deliverySessionId,
          ...(entry.sessionId !== deliverySessionId ? { sourceSessionId: entry.sessionId } : {}) };
        const receipts: BrokerPublicationReceipt[] = [];
        let delivery = { eventId: '', status: behavior.publicationStatus,
          observedCallerRevision: snapshot.callerRevision, observedTranscriptSequence: snapshot.revision };
        for (let chunk = 0; chunk < (update ? 1 : 2); chunk++) {
          const input = update ? { ...update, expectedCallerRevision: snapshot.callerRevision } : { id: `publication_${publications.length}_${chunk}`, kind: 'result' as const,
            speak: true, text: `Owned question chunk ${chunk}`, expectedCallerRevision: snapshot.callerRevision };
          const intent = beginBrokerPublication(store, identity, entry.requestId, input, snapshot);
          assert.ok(intent, 'Fixture publication requires an admitted request and the current caller revision');
          delivery = { ...delivery, eventId: `synthetic_publication_${publications.length}_${chunk}` };
          if (behavior.rejectBeforeReceipt) rejectBrokerPublication(store, identity, delivery.eventId, 'provider_error');
          if (intent.created) settleBrokerPublication(store, intent.record, delivery, snapshot, false);
          receipts.push({ publicationSequence: intent.record.publicationSequence, sessionId: deliverySessionId, eventId: delivery.eventId });
        }
        const receipt = { sessionId: deliverySessionId, snapshot, inputThroughMs: contexts.get(deliverySessionId)!.inputThroughMs(),
          delivery, publicationReceipts: receipts };
        diagnostic?.('observer.publish', 'after');
        return receipt;
      } catch (error) { diagnostic?.('observer.publish', 'rejected'); throw error; }
    },
  };
  const backend = (sourceSessionId: string): PrimaryBackend => createPrimaryBackend({
    callId: call.id, callSid, bindingId, api, store, sourceSessionId, observation,
    admissionSettled: request => { admissions.push({ request: structuredClone(request),
      entries: structuredClone(Object.values(store.get(call.id)!.delegations)) as BrokerDelegationRecord[] }); },
    stabilityMs: 5, maxWaitMs: 100, pollIntervalMs: 5, maxPollMs: 30, progressIntervalMs: 60_000,
  });
  const request = (sessionId: string, delegationId: string): LiveDelegationRequest => ({
    callId: call.id, sessionId, delegationId, requestId: `live_${sessionId}_${delegationId}`,
    offsetMs: Math.max(0, ...contexts.get(sessionId)!.channel.getContext().fragments.map(fragment => fragment.endMs)) + 200,
    transcript: contexts.get(sessionId)!.channel.getContext(),
  });
  const delegate = (instance: PrimaryBackend, sessionId: string, delegationId: string) => {
    const work = diagnosticAwait(diagnostic, 'worker.delegate', () => instance.delegate(request(sessionId, delegationId), contexts.get(sessionId)!.channel));
    tasks.push(work); return work;
  };
  const speak = (instance: PrimaryBackend, sessionId: string, text: string, startMs = 800, endMs = 1000) => {
    const context = contexts.get(sessionId)!; context.add(text, startMs, endMs);
    context.advanceAudio(endMs);
    const latestPresentation = Math.max(0, ...Object.values(store.get(call.id)!.delegations).flatMap(entry =>
      Object.values((entry as BrokerDelegationRecord).questions ?? {}).map(question => Date.parse(question.presentation?.acceptedAt ?? '') || 0)));
    // Ordinary fixture speech is a distinct host observation after the latest ACK.
    const observedAt = Math.max(Date.now(), latestPresentation + 1);
    instance.noteCallerSpeech(observedAt, startMs); instance.noteCallerSpeech(observedAt, endMs);
    const snapshot = context.channel.getContext(); instance.observeTranscript(snapshot);
    const current = store.get(call.id)!;
    store.update(call.id, { liveGenerations: { ...current.liveGenerations!, segments: current.liveGenerations!.segments.map(segment =>
      segment.sessionId === sessionId ? { ...segment, transcript: snapshot } : segment) } });
  };
  const rotate = () => {
    const current = store.get(call.id)!;
    const timestamp = new Date().toISOString();
    store.update(call.id, { liveSessionId: newSession, liveGenerations: { ...current.liveGenerations!, phase: 'active',
      inputNextSample: 80_000, deliveryAttachment: { generationId: newGenerationId, sessionId: newSession },
      segments: [...current.liveGenerations!.segments.map(segment => ({ ...segment, state: 'closed' as const,
        closedAt: timestamp, providerFinalized: true, outputDiscarded: true })),
      { generationId: newGenerationId, ordinal: 2, createdAt: timestamp, readyAt: timestamp, state: 'active', sessionId: newSession,
        version: { ...version, contextEpoch: '2', digest: 'c'.repeat(64) }, pageDigest: 'd'.repeat(64), inputFirstSample: 80_000,
        transcript: contexts.get(newSession)!.channel.getContext(), outputDiscarded: false }] } });
    contexts.get(oldSession)!.controller.abort();
  };
  const saved = () => Object.values(store.get(call.id)!.delegations) as BrokerDelegationRecord[];
  const reopen = () => {
    diagnosticSync(diagnostic, 'reopen.store_close', () => store.close());
    store = diagnosticSync(diagnostic, 'reopen.store', () => new BrokerStore(directory)); stores.push(store); return store;
  };
  t.after(() => diagnosticAwait(diagnostic, 'teardown', async () => {
    diagnosticSync(diagnostic, 'teardown.call_abort', () => callLifetime.abort());
    diagnosticSync(diagnostic, 'teardown.update', () => {
      if (!store.get(call.id)!.endedAt) store.update(call.id, { endedAt: new Date().toISOString(), status: 'completed' });
    });
    diagnosticSync(diagnostic, 'teardown.cleanups', () => { for (const cleanup of cleanups) cleanup(); });
    diagnosticSync(diagnostic, 'teardown.voice_abort', () => { for (const context of contexts.values()) context.controller.abort(); });
    let timeout: ReturnType<typeof setTimeout> | undefined;
    try {
      await diagnosticAwait(diagnostic, 'teardown.task_drain', () => Promise.race([Promise.allSettled(tasks), new Promise<never>((_resolve, reject) => {
        timeout = setTimeout(() => reject(new Error('backend fixture tasks did not settle after call abort')), 2_000);
      })]));
    } finally {
      diagnostic?.('teardown.finally', 'before');
      if (timeout) clearTimeout(timeout);
      for (const saved of stores) diagnosticSync(diagnostic, 'teardown.store_close', () => saved.close());
      await diagnosticAwait(diagnostic, 'teardown.remove', () => boundedStep('remove owned fixture directory', rm(directory, { recursive: true, force: true })));
      diagnostic?.('teardown.finally', 'after');
    }
  }));
  return { directory, call, contexts, posts, reads, captures, replies, questionReads, publications, admissions, behavior, completed,
    callLifetime, backend, request, delegate, speak, rotate, saved, reopen, store: () => store,
    onCleanup: (fn: () => void) => { cleanups.push(fn); } };
}

function pendingQuestion(): PrimaryQuestion {
  return { id: questionId, revision: 1, expiresAt: new Date(Date.now() + 60_000).toISOString(),
    runState: 'waiting_for_reply', runId, threadId, agent: { id: primaryDexterId, name: 'Dexter' },
    question: 'Should I compare NVIDIA and Apple?', context: 'You requested a comparison.', proposedAnswer: 'Compare both stocks.',
    yesMeaning: 'Compare both stocks.', noMeaning: 'Do not continue the comparison.', state: 'pending',
    createdAt: new Date().toISOString(), reply: null };
}
const questionResult = (id: string, question: PrimaryQuestion): PrimaryQuestionResult => ({
  requestId: id, bindingId, state: question.runState, question,
});
const answeredQuestion = (question: PrimaryQuestion, input: PrimaryReplyInput): PrimaryQuestion => ({ ...question,
  state: 'answered', runState: 'running', reply: { ...input.reply,
    interpretedAnswer: input.reply.answer === 'yes' ? question.yesMeaning : input.reply.answer === 'no' ? question.noMeaning : input.reply.text!,
    receivedAt: new Date().toISOString() } });

async function currentQuestionFixture(t: TestContext) {
  const f = await fixture(t); const question = pendingQuestion();
  f.behavior.result = async id => questionResult(id, question);
  f.behavior.question = async id => questionResult(id, question);
  const old = f.backend(oldSession); f.speak(old, oldSession, 'Compare these stocks.');
  await f.delegate(old, oldSession, 'comparison');
  f.rotate(); const current = f.backend(newSession);
  f.speak(current, newSession, 'Please repeat the question.');
  await f.delegate(current, newSession, 'repeat');
  assert.equal(f.saved()[0].questions![questionId].presentation!.sessionId, newSession);
  return { ...f, question, current };
}

test('accepted task observation survives old voice abort and delivers through the current generation', async t => {
  const f = await fixture(t); const old = f.backend(oldSession);
  const held = deferred<PrimaryResult>();
  f.behavior.result = async () => held.promise;
  f.onCleanup(() => { const id = f.reads[0]; if (id) held.resolve(f.completed(id)); });
  f.speak(old, oldSession, ' Read my portfolio.\t');
  const work = f.delegate(old, oldSession, 'portfolio');
  await until(() => f.reads.length === 1);
  const original = structuredClone(f.saved()[0]);
  f.rotate();
  assert.equal(f.contexts.get(oldSession)!.channel.signal.aborted, true);
  assert.equal(f.callLifetime.signal.aborted, false);
  held.resolve(f.completed(original.requestId)); await work;
  const result = f.saved()[0];
  assert.equal(result.status, 'completed');
  assert.equal(result.sessionId, oldSession);
  assert.deepEqual(result.submission, original.submission);
  assert.equal(result.requestId, original.requestId);
  assert.equal(result.delivery, 'accepted');
  assert.equal(f.posts.length, 1);
  assert.equal(f.captures.length, 0);
  assert.deepEqual(f.publications.filter(value => !value.update).map(value => [value.sourceSessionId, value.deliverySessionId]),
    [[oldSession, newSession]]);
  assert.deepEqual(f.contexts.get(oldSession)!.directPublications, []);
});

test('call-owned abort stops observation even when the new voice session remains open', async t => {
  const f = await fixture(t); const old = f.backend(oldSession);
  const held = deferred<PrimaryResult>(); f.behavior.result = async () => held.promise;
  f.onCleanup(() => { const id = f.reads[0]; if (id) held.resolve(f.completed(id)); });
  f.speak(old, oldSession, 'Read my portfolio.');
  const work = f.delegate(old, oldSession, 'portfolio');
  await until(() => f.reads.length === 1);
  const id = f.posts[0].id;
  f.rotate(); f.callLifetime.abort(); held.resolve(f.completed(id)); await work;
  assert.equal(f.contexts.get(newSession)!.channel.signal.aborted, false);
  assert.deepEqual(f.publications, []);
  assert.equal(f.posts.length, 1);
});

test('new-generation delegated instruction queues behind accepted work and keeps its own raw source at promotion', async t => {
  const f = await fixture(t); const old = f.backend(oldSession);
  const held = deferred<PrimaryResult>(); let firstId: string | undefined;
  f.behavior.result = async id => { firstId ??= id; return id === firstId ? held.promise : f.completed(id); };
  f.onCleanup(() => { if (firstId) held.resolve(f.completed(firstId)); });
  f.speak(old, oldSession, 'Read my portfolio.');
  const firstWork = f.delegate(old, oldSession, 'portfolio');
  await until(() => f.reads.length === 1);
  const original = structuredClone(f.saved()[0]);
  f.rotate(); const current = f.backend(newSession);
  const raw = ' Also compare NVIDIA with Apple.\t'; f.speak(current, newSession, raw);
  const secondWork = f.delegate(current, newSession, 'comparison');
  await until(() => f.saved()[0].deferredTurns?.some(turn => turn.status === 'queued') === true);
  assert.equal(f.posts.length, 1, 'queued instruction is not a competing submission');
  const queued = structuredClone(f.saved()[0].deferredTurns![0]);
  assert.equal(queued.sourceSessionId, newSession);
  assert.equal(queued.text, raw);
  const admission = f.admissions.filter(value => value.request.sessionId === newSession && value.request.delegationId === 'comparison');
  assert.equal(admission.length, 1);
  assert.equal(admission[0].entries[0].deferredTurns![0].status, 'queued', 'rotation drain sees the durable follow-up before result observation completes');
  assert.equal(queued.queuedRequest!.submission.source.receivedAt, new Date(Date.parse(f.call.createdAt) + 10_000 + 800).toISOString());
  held.resolve(f.completed(original.requestId)); await Promise.all([firstWork, secondWork]);
  assert.equal(f.posts.length, 2);
  const [parent, child] = f.saved();
  assert.equal(parent.sessionId, oldSession); assert.deepEqual(parent.submission, original.submission);
  assert.equal(parent.deferredTurns![0].status, 'promoted');
  assert.deepEqual(parent.delegationAliases, [{ sessionId: newSession, delegationId: 'comparison' }]);
  assert.equal(child.sessionId, newSession); assert.equal(child.predecessorRequestId, original.requestId);
  assert.equal(child.requestId, brokerIdentity('request', f.call.id, newSession, 'comparison'));
  assert.deepEqual(child.submission, queued.queuedRequest!.submission);
  assert.equal(f.posts[1].input.text, raw);
  assert.notEqual(f.posts[1].input.source.sourceEventId, f.posts[0].input.source.sourceEventId);
  await f.delegate(current, newSession, 'comparison');
  assert.equal(f.posts.length, 2, 'exact delegate retry cannot repeat execution');
});

test('old-generation question must be presented in the current generation before a later explicit reply qualifies', async t => {
  const f = await fixture(t); const question = pendingQuestion();
  let answer: PrimaryQuestion | undefined;
  f.behavior.result = async id => answer ? f.completed(id) : questionResult(id, question);
  f.behavior.question = async id => questionResult(id, answer ?? question);
  f.behavior.reply = async (id, _questionId, input) => { answer = answeredQuestion(question, input); return questionResult(id, answer); };
  const old = f.backend(oldSession); f.speak(old, oldSession, 'Compare these stocks.');
  await f.delegate(old, oldSession, 'comparison');
  const original = structuredClone(f.saved()[0]);
  assert.equal(original.questions![questionId].presentation!.sessionId, oldSession);
  f.rotate(); const current = f.backend(newSession);
  f.speak(current, newSession, 'Yes.');
  await f.delegate(current, newSession, 'early_yes');
  assert.equal(f.replies.length, 0, 'old-session presentation cannot establish this new reply boundary');
  assert.equal(f.posts.length, 1);
  const represented = f.saved()[0].questions![questionId];
  assert.equal(represented.presentation!.sessionId, newSession);
  assert.equal(represented.presentation!.sequence, 1);
  assert.equal(f.saved()[0].deferredTurns![0].status, 'recorded_not_submitted');
  const raw = ' Yes.\t'; f.speak(current, newSession, raw, 3000, 3200);
  await f.delegate(current, newSession, 'answer_after_question');
  assert.equal(f.replies.length, 1); assert.equal(f.posts.length, 1);
  const sent = f.replies[0];
  assert.equal(sent.requestId, original.requestId); assert.equal(sent.questionId, questionId); assert.equal(sent.binding, bindingId);
  assert.equal(sent.input.text, raw); assert.equal(sent.input.reply.answer, 'yes'); assert.equal(sent.input.reply.text, null);
  const saved = f.saved()[0];
  assert.equal(saved.sessionId, oldSession); assert.deepEqual(saved.submission, original.submission);
  assert.equal(saved.questions![questionId].question.runId, runId);
  assert.equal(saved.questions![questionId].reply!.sourceSessionId, newSession);
  assert.equal(saved.questions![questionId].reply!.status, 'accepted');
  assert.equal(sent.input.source.sourceEventId, brokerIdentity('source', f.call.id, newSession, `${newSession}_caller_2`));
  assert.equal(sent.input.source.receivedAt, new Date(Date.parse(f.call.createdAt) + 10_000 + 3000).toISOString());
  const canonical = f.store().get(f.call.id)!.canonicalTurns![sent.input.source.sourceEventId] as { sourceSessionId: string; text: string };
  assert.equal(canonical.sourceSessionId, newSession); assert.equal(canonical.text, raw);
  await f.delegate(current, newSession, 'answer_after_question');
  assert.equal(f.replies.length, 1); assert.equal(f.posts.length, 1);
});

test('rejected current-generation question publication never qualifies a spoken answer', async t => {
  const f = await fixture(t); const question = pendingQuestion();
  f.behavior.result = async id => questionResult(id, question);
  f.behavior.question = async id => questionResult(id, question);
  const old = f.backend(oldSession); f.speak(old, oldSession, 'Compare these stocks.');
  await f.delegate(old, oldSession, 'comparison');
  f.rotate(); f.behavior.publicationStatus = 'rejected'; const current = f.backend(newSession);
  f.speak(current, newSession, 'Yes.'); await f.delegate(current, newSession, 'first_yes');
  f.speak(current, newSession, 'Yes.', 3000, 3200); await f.delegate(current, newSession, 'second_yes');
  assert.equal(f.replies.length, 0); assert.equal(f.posts.length, 1);
  assert.equal(f.saved()[0].questions![questionId].presentation!.sessionId, oldSession);
  assert.equal(f.saved()[0].deferredTurns!.every(turn => turn.status === 'recorded_not_submitted'), true);
});

test('restart recovers an uncertain new-generation answer by the original question without replaying its POST', { timeout: 15_000 }, async t => {
  // Fixed stage identifiers only: no caller text, error body, IDs or journal bytes.
  let markerCount = 0;
  const diagnostic: Diagnostic = (stage, event) => {
    if (markerCount < 256) {
      markerCount++; writeSync(2, `generation_backend_sixth ${stage} ${event}\n`);
    } else if (markerCount === 256) {
      markerCount++; writeSync(2, 'generation_backend_sixth marker_limit_reached\n');
    }
  };
  return diagnosticAwait(diagnostic, 'callback', async () => {
    const f = await diagnosticAwait(diagnostic, 'setup', () => fixture(t, diagnostic)); const question = pendingQuestion();
    let remoteAnswer: PrimaryQuestion | undefined;
    let readsAvailable = true;
    f.behavior.result = async id => remoteAnswer && readsAvailable ? f.completed(id) : questionResult(id, question);
    f.behavior.question = async id => {
      if (!readsAvailable) throw new PrimaryApiError('request_timeout');
      return questionResult(id, remoteAnswer ?? question);
    };
    f.behavior.reply = async (_id, _questionId, input) => {
      remoteAnswer = answeredQuestion(question, input); readsAvailable = false;
      throw new PrimaryApiError('request_timeout');
    };
    const old = diagnosticSync(diagnostic, 'backend.old', () => f.backend(oldSession));
    diagnosticSync(diagnostic, 'speak.initial', () => f.speak(old, oldSession, 'Compare these stocks.'));
    await diagnosticAwait(diagnostic, 'delegate.initial', () => boundedStep('initial comparison and pending question', f.delegate(old, oldSession, 'comparison')));
    diagnosticSync(diagnostic, 'rotate', () => f.rotate());
    const current = diagnosticSync(diagnostic, 'backend.current', () => f.backend(newSession));
    diagnosticSync(diagnostic, 'speak.repeat', () => f.speak(current, newSession, 'Please repeat the question.'));
    await diagnosticAwait(diagnostic, 'delegate.repeat', () => boundedStep('current-generation question presentation', f.delegate(current, newSession, 'repeat')));
    const initialPresentation = structuredClone(f.saved()[0].questions![questionId].presentation);
    const priorPublications = f.publications.length;
    diagnosticSync(diagnostic, 'speak.answer', () => f.speak(current, newSession, 'No.', 3000, 3200));
    await diagnosticAwait(diagnostic, 'delegate.answer', () => boundedStep('first answer reaches durable unknown outcome', f.delegate(current, newSession, 'answer')));
    const original = structuredClone(f.saved()[0]);
    const uncertain = original.questions![questionId].reply!;
    assert.equal(uncertain.status, 'unknown'); assert.equal(uncertain.sourceSessionId, newSession);
    assert.equal(f.replies.length, 1); assert.equal(f.posts.length, 1);
    assert.equal(f.publications.slice(priorPublications).filter(value => !value.update).length, 0,
      'unknown reply cannot present the question as newly unanswered');
    assert.ok(f.publications.slice(priorPublications).some(value => value.update?.id.endsWith('_reply_unknown')));
    assert.deepEqual(original.questions![questionId].presentation, initialPresentation);
    const before = await diagnosticAwait(diagnostic, 'read.before_restart', () => boundedStep('read durable unknown reply before restart', readFile(path.join(f.directory, 'calls.json'), 'utf8')));
    const durableReply = JSON.parse(before).calls[f.call.id].delegations[original.requestId].questions[questionId].reply;
    assert.equal(durableReply.status, 'unknown');
    assert.deepEqual(durableReply.input, uncertain.input, 'exact unknown reply is present in the durable journal');
    diagnosticSync(diagnostic, 'reopen', () => f.reopen());
    assert.equal(await diagnosticAwait(diagnostic, 'read.after_restart', () => boundedStep('verify unchanged journal after restart', readFile(path.join(f.directory, 'calls.json'), 'utf8'))), before);
    readsAvailable = true;
    const recovered = diagnosticSync(diagnostic, 'backend.recovered', () => f.backend(newSession));
    await diagnosticAwait(diagnostic, 'delegate.recovery', () => boundedStep('read-only original-question recovery', f.delegate(recovered, newSession, 'answer')));
    const saved = f.saved()[0];
    assert.equal(saved.status, 'completed');
    assert.equal(saved.questions![questionId].reply!.status, 'accepted');
    assert.deepEqual(saved.questions![questionId].reply!.input, uncertain.input);
    assert.equal(saved.questions![questionId].question.runId, runId);
    assert.deepEqual(saved.submission, original.submission); assert.equal(saved.sessionId, oldSession);
    assert.equal(f.replies.length, 1); assert.equal(f.posts.length, 1);
    assert.equal(f.questionReads.at(-1)!.requestId, original.requestId);
    await diagnosticAwait(diagnostic, 'delegate.replay', () => boundedStep('exact admitted answer replay', f.delegate(recovered, newSession, 'answer')));
    assert.equal(f.replies.length, 1); assert.equal(f.posts.length, 1);
  });
});

test('terminal call prevents queued next-generation instruction promotion after a late result', async t => {
  const f = await fixture(t); const old = f.backend(oldSession);
  const held = deferred<PrimaryResult>(); f.behavior.result = async () => held.promise;
  f.onCleanup(() => { const id = f.reads[0]; if (id) held.resolve(f.completed(id)); });
  f.speak(old, oldSession, 'Read my portfolio.');
  const firstWork = f.delegate(old, oldSession, 'portfolio');
  await until(() => f.reads.length === 1);
  const original = structuredClone(f.saved()[0]);
  f.rotate(); const current = f.backend(newSession);
  f.speak(current, newSession, 'Then compare these stocks.');
  const nextWork = f.delegate(current, newSession, 'comparison');
  await until(() => f.saved()[0].deferredTurns?.some(turn => turn.status === 'queued') === true);
  f.store().update(f.call.id, { status: 'completed', endedAt: new Date().toISOString() });
  f.callLifetime.abort(); held.resolve(f.completed(original.requestId));
  await Promise.all([firstWork, nextWork]);
  assert.equal(f.posts.length, 1); assert.equal(f.saved().length, 1);
  assert.equal(f.saved()[0].deferredTurns![0].status, 'queued', 'saved request remains unexecuted after call termination');
  assert.deepEqual(f.publications, []);
});

test('delayed pre-question yes caption cannot become a reply; later fresh speech answers the original run', async t => {
  const f = await fixture(t); const question = pendingQuestion();
  let answer: PrimaryQuestion | undefined;
  f.behavior.result = async id => answer ? f.completed(id) : questionResult(id, question);
  f.behavior.question = async id => questionResult(id, answer ?? question);
  f.behavior.reply = async (id, _questionId, input) => { answer = answeredQuestion(question, input); return questionResult(id, answer); };
  const old = f.backend(oldSession); f.speak(old, oldSession, 'Compare these stocks.');
  await f.delegate(old, oldSession, 'comparison');
  const original = structuredClone(f.saved()[0]); f.rotate(); const current = f.backend(newSession);
  f.speak(current, newSession, 'Repeat the question.');
  const context = f.contexts.get(newSession)!;
  // This audio precedes the question ACK, but its caption has not arrived.
  context.advanceAudio(2700);
  current.noteCallerSpeech(Date.now(), 2500); current.noteCallerSpeech(Date.now(), 2700);
  await f.delegate(current, newSession, 'repeat');
  const presentation = f.saved()[0].questions![questionId].presentation!;
  assert.equal(presentation.inputThroughMs, 2700);
  context.add('Yes.', 2500, 2700, 'delayed_pre_question_yes');
  current.observeTranscript(context.channel.getContext());
  // A later unrelated sound must not make the older caption a new answer.
  context.advanceAudio(3000);
  current.noteCallerSpeech(Math.max(Date.now(), Date.parse(presentation.acceptedAt) + 1), 3000);
  await f.delegate(current, newSession, 'delayed_yes');
  assert.equal(f.replies.length, 0); assert.equal(f.posts.length, 1);
  const retained = f.saved()[0].deferredTurns!.find(turn => turn.text === 'Yes.')!;
  assert.equal(retained.status, 'recorded_not_submitted');
  assert.equal(retained.sourceEventId, brokerIdentity('source', f.call.id, newSession, 'delayed_pre_question_yes'));
  f.speak(current, newSession, ' Yes. ', 5000, 5200);
  await f.delegate(current, newSession, 'fresh_yes');
  assert.equal(f.replies.length, 1); assert.equal(f.posts.length, 1);
  assert.equal(f.replies[0].requestId, original.requestId);
  assert.equal(f.replies[0].input.text, ' Yes. ');
  assert.notEqual(f.replies[0].input.source.sourceEventId, retained.sourceEventId);
  assert.equal(f.saved()[0].questions![questionId].question.runId, runId);
  assert.deepEqual(f.saved()[0].submission, original.submission);
});

test('a retained unavailable question notice gives status only and preserves the last exact presentation', async t => {
  const f = await currentQuestionFixture(t);
  const presentation = structuredClone(f.saved()[0].questions![questionId].presentation);
  const entry = f.saved()[0];
  f.store().update(f.call.id, { delegations: { ...f.store().get(f.call.id)!.delegations,
    [entry.requestId]: { ...entry, questionNotice: 'question_unavailable' } } });
  const start = f.publications.length;
  await f.delegate(f.current, newSession, 'repeat');
  assert.equal(f.saved()[0].questionNotice, 'question_unavailable');
  assert.equal(f.replies.length, 0);
  assert.ok(f.publications.slice(start).some(value => value.update?.id.endsWith('_unavailable')));
  assert.equal(f.publications.slice(start).filter(value => !value.update).length, 0);
  assert.deepEqual(f.saved()[0].questions![questionId].presentation, presentation);
});

test('expired question is never published as eligible or given a presentation receipt', async t => {
  const f = await fixture(t); const question = { ...pendingQuestion(), expiresAt: new Date(Date.now() - 1000).toISOString() };
  f.behavior.result = async id => questionResult(id, question);
  const old = f.backend(oldSession); f.speak(old, oldSession, 'Compare these stocks.');
  await f.delegate(old, oldSession, 'comparison');
  assert.equal(f.saved()[0].questions![questionId].presentation, undefined);
  assert.equal(f.publications.filter(value => !value.update).length, 0);
  assert.ok(f.publications.some(value => value.update?.id.endsWith('_unavailable')));
  assert.equal(f.replies.length, 0);
});

test('waiting state without a selected question produces only a status notice', async t => {
  const f = await currentQuestionFixture(t); const entry = f.saved()[0];
  const { currentQuestionId: _removed, ...withoutSelectedQuestion } = entry;
  f.store().update(f.call.id, { delegations: { ...f.store().get(f.call.id)!.delegations,
    [entry.requestId]: withoutSelectedQuestion } });
  f.behavior.result = async () => { throw new PrimaryApiError('request_timeout'); };
  const start = f.publications.length;
  await f.delegate(f.current, newSession, 'repeat');
  assert.equal(f.publications.slice(start).filter(value => !value.update).length, 0);
  assert.ok(f.publications.slice(start).some(value => value.update?.id.endsWith('_unavailable')));
  assert.equal(f.replies.length, 0); assert.equal(f.posts.length, 1);
});

test('overlong answer needs an accepted notice and separate eligible question before advancing presentation', async t => {
  const f = await currentQuestionFixture(t);
  const presentation = structuredClone(f.saved()[0].questions![questionId].presentation);
  f.behavior.publicationStatus = 'rejected';
  const start = f.publications.length;
  const raw = 'x'.repeat(2001); f.speak(f.current, newSession, raw, 3000, 3200);
  await f.delegate(f.current, newSession, 'long_answer');
  assert.equal(f.saved()[0].questionNotice, 'answer_too_long');
  assert.equal(f.replies.length, 0);
  assert.equal(f.publications.slice(start).filter(value => !value.update).length, 0);
  assert.ok(f.publications.slice(start).some(value => value.update?.id.endsWith('_shorter_answer')));
  assert.deepEqual(f.saved()[0].questions![questionId].presentation, presentation);
  assert.equal(f.saved()[0].deferredTurns!.at(-1)!.text, raw);
  f.behavior.publicationStatus = 'accepted';
  const retryStart = f.publications.length;
  await f.delegate(f.current, newSession, 'long_answer');
  const retry = f.publications.slice(retryStart);
  assert.equal(retry.length, 2);
  assert.ok(retry[0].update?.id.endsWith('_shorter_answer'));
  assert.equal(retry[1].update, undefined, 'the fixed notice itself is not a question presentation');
  assert.equal(f.saved()[0].questions![questionId].presentation!.sequence, 2);
  assert.equal(f.saved()[0].questions![questionId].presentation!.inputThroughMs, 3200);
  assert.equal(f.replies.length, 0); assert.equal(f.posts.length, 1);
});

test('proved refused answer remains immutable while a freshly presented question admits a different explicit source', async t => {
  const f = await currentQuestionFixture(t);
  const presentation = structuredClone(f.saved()[0].questions![questionId].presentation);
  f.behavior.reply = async () => { throw new PrimaryApiError('http_error', 400, { code: 'invalid_request', retryable: false }); };
  f.behavior.publicationStatus = 'rejected';
  const start = f.publications.length;
  f.speak(f.current, newSession, 'Yes.', 3000, 3200);
  await f.delegate(f.current, newSession, 'refused_answer');
  const refused = structuredClone(f.saved()[0].questions![questionId].reply!);
  assert.equal(refused.status, 'refused'); assert.equal(f.replies.length, 1);
  assert.equal(f.publications.slice(start).filter(value => !value.update).length, 0);
  assert.ok(f.publications.slice(start).some(value => value.update?.id.endsWith('_reply_refused')));
  assert.deepEqual(f.saved()[0].questions![questionId].presentation, presentation);
  f.behavior.publicationStatus = 'accepted'; const retryStart = f.publications.length;
  await f.delegate(f.current, newSession, 'refused_answer');
  const retry = f.publications.slice(retryStart);
  assert.equal(retry.length, 2); assert.ok(retry[0].update?.id.endsWith('_reply_refused')); assert.equal(retry[1].update, undefined);
  assert.equal(f.replies.length, 1, 'an exact delegate retry does not repeat a refused POST');
  let answered: PrimaryQuestion | undefined;
  f.behavior.question = async id => questionResult(id, answered ?? f.question);
  f.behavior.result = async id => answered ? f.completed(id) : questionResult(id, f.question);
  f.behavior.reply = async (id, _questionId, input) => { answered = answeredQuestion(f.question, input); return questionResult(id, answered); };
  f.speak(f.current, newSession, 'No.', 5000, 5200);
  await f.delegate(f.current, newSession, 'new_answer');
  const saved = f.saved()[0].questions![questionId];
  assert.deepEqual(saved.refusedReplies, [refused]); assert.equal(saved.reply!.status, 'accepted');
  assert.notEqual(saved.reply!.input.source.sourceEventId, refused.input.source.sourceEventId);
  assert.notEqual(saved.reply!.input.reply.clientReplyId, refused.input.reply.clientReplyId);
  assert.equal(f.replies.length, 2); assert.equal(f.posts.length, 1);
  assert.equal(f.replies[1].requestId, f.replies[0].requestId);
});

test('admission drain sees exact durable source before held submission HTTP and does not await result observation', async t => {
  const f = await fixture(t); const old = f.backend(oldSession);
  const held = deferred<PrimaryResult>(); f.behavior.submit = async () => held.promise;
  f.onCleanup(() => { const id = f.posts[0]?.id; if (id) held.resolve({ bindingId, requestId: id, state: 'queued' }); });
  const raw = ' Read my portfolio.\t'; f.speak(old, oldSession, raw);
  const work = f.delegate(old, oldSession, 'portfolio');
  await until(() => f.admissions.length === 1 && f.posts.length === 1);
  const admission = f.admissions[0]; const prepared = admission.entries[0];
  assert.equal(prepared.status, 'prepared');
  assert.equal(prepared.sessionId, oldSession); assert.equal(prepared.submission.text, raw);
  assert.equal(admission.request.delegationId, 'portfolio');
  const disk = JSON.parse(await readFile(path.join(f.directory, 'calls.json'), 'utf8')).calls[f.call.id];
  assert.deepEqual(disk.delegations[prepared.requestId].submission, prepared.submission);
  assert.equal(disk.delegations[prepared.requestId].status, 'dispatching');
  assert.equal(f.reads.length, 0, 'submit HTTP is still held after source admission has settled');
  f.rotate();
  held.resolve({ bindingId, requestId: prepared.requestId, state: 'queued' });
  await work;
  assert.equal(f.admissions.length, 1, 'the finally fallback does not fire admission twice');
  assert.equal(f.posts.length, 1); assert.equal(f.saved()[0].status, 'completed');
  assert.deepEqual(f.saved()[0].submission, prepared.submission);
  assert.equal(f.publications.at(-1)!.deliverySessionId, newSession);
});

test('definitely-undispatched answer survives a failed question read and later sends the exact saved source once', async t => {
  const f = await currentQuestionFixture(t);
  let available = false; let answer: PrimaryQuestion | undefined;
  f.behavior.question = async id => {
    if (!available) throw new PrimaryApiError('request_timeout');
    return questionResult(id, answer ?? f.question);
  };
  f.behavior.result = async id => answer ? f.completed(id) : questionResult(id, f.question);
  f.behavior.reply = async (id, _questionId, input) => { answer = answeredQuestion(f.question, input); return questionResult(id, answer); };
  f.speak(f.current, newSession, ' Yes. ', 3000, 3200);
  await f.delegate(f.current, newSession, 'saved_answer');
  const prepared = structuredClone(f.saved()[0].questions![questionId].reply!);
  assert.equal(prepared.status, 'prepared'); assert.equal(f.replies.length, 0);
  assert.ok(prepared.admissionPresentation!.publicationReceipts!.length > 1);
  f.reopen(); available = true;
  await f.delegate(f.backend(newSession), newSession, 'saved_answer');
  assert.equal(f.replies.length, 1); assert.deepEqual(f.replies[0].input, prepared.input);
  assert.equal(f.saved()[0].questions![questionId].reply!.status, 'accepted');
  await f.delegate(f.backend(newSession), newSession, 'saved_answer');
  assert.equal(f.replies.length, 1); assert.equal(f.posts.length, 1);
});

test('rejection of any question chunk before presentation persistence refuses the barrier', async t => {
  const f = await fixture(t); const question = pendingQuestion();
  f.behavior.result = async id => questionResult(id, question);
  f.behavior.rejectBeforeReceipt = true;
  const old = f.backend(oldSession); f.speak(old, oldSession, 'Compare these stocks.');
  await f.delegate(old, oldSession, 'comparison');
  assert.equal(f.saved()[0].questions![questionId].presentation, undefined);
  assert.ok(f.store().get(f.call.id)!.publications!.rejections.length >= 2);
  assert.equal(f.replies.length, 0);
});

test('late rejection invalidates the exact saved question and does not consume a later yes as an answer', async t => {
  const f = await currentQuestionFixture(t);
  const original = structuredClone(f.saved()[0].questions![questionId].presentation!);
  const receipt = original.publicationReceipts![0];
  rejectBrokerPublication(f.store(), { callId: f.call.id, callSid, bindingId, sessionId: newSession }, receipt.eventId, 'provider_error');
  f.speak(f.current, newSession, 'Yes.', 3000, 3200);
  await f.delegate(f.current, newSession, 'answer_to_rejected_question');
  assert.equal(f.replies.length, 0); assert.equal(f.posts.length, 1);
  assert.equal(f.saved()[0].deferredTurns!.at(-1)!.status, 'recorded_not_submitted');
  const represented = f.saved()[0].questions![questionId].presentation!;
  assert.notDeepEqual(represented.publicationReceipts, original.publicationReceipts);
  assert.ok(f.store().get(f.call.id)!.publications!.records.some(record => record.outcome?.eventId === receipt.eventId
    && record.outcome.status === 'accepted'), 'accepted-before-rejection history remains');
});

test('late rejection during the pre-dispatch read refuses the saved answer without a POST', async t => {
  const f = await currentQuestionFixture(t);
  const presentation = structuredClone(f.saved()[0].questions![questionId].presentation!);
  f.behavior.question = async id => {
    rejectBrokerPublication(f.store(), { callId: f.call.id, callSid, bindingId, sessionId: newSession },
      presentation.publicationReceipts![1].eventId, 'provider_error');
    return questionResult(id, f.question);
  };
  f.speak(f.current, newSession, 'Yes.', 3000, 3200);
  await f.delegate(f.current, newSession, 'answer');
  const refused = f.saved()[0].questions![questionId].reply!;
  assert.equal(refused.status, 'refused'); assert.equal(refused.refusal!.code, 'question_publication_rejected');
  assert.deepEqual(refused.admissionPresentation, presentation);
  assert.equal(refused.input.text, 'Yes.'); assert.equal(f.replies.length, 0); assert.equal(f.posts.length, 1);
});

test('first post-publication audio at exclusive frontier zero can answer when host speech is later', async t => {
  const f = await fixture(t); const question = pendingQuestion(); let answer: PrimaryQuestion | undefined;
  f.behavior.result = async id => answer ? f.completed(id) : questionResult(id, question);
  f.behavior.question = async id => questionResult(id, answer ?? question);
  f.behavior.reply = async (id, _questionId, input) => { answer = answeredQuestion(question, input); return questionResult(id, answer); };
  const old = f.backend(oldSession); f.speak(old, oldSession, 'Compare these stocks.');
  await f.delegate(old, oldSession, 'comparison'); f.rotate();
  await f.delegate(old, oldSession, 'comparison');
  const presentation = f.saved()[0].questions![questionId].presentation!;
  assert.equal(presentation.sessionId, newSession); assert.equal(presentation.inputThroughMs, 0); assert.equal(presentation.sequence, 0);
  // ISO Date/Date.now observations resolve whole milliseconds. A later observation is required,
  // while the exclusive sample frontier legitimately permits first sample/caption offset zero.
  const current = f.backend(newSession); f.speak(current, newSession, 'Yes.', 0, 200);
  await f.delegate(current, newSession, 'first_sample_answer');
  assert.equal(f.replies.length, 1); assert.equal(f.replies[0].input.text, 'Yes.');
  assert.equal(f.replies[0].input.source.receivedAt, new Date(Date.parse(f.call.createdAt) + 10_000).toISOString());
});


test('retired-generation question rejection still fences a prepared answer after a failed pre-dispatch read', async t => {
  const f = await fixture(t); const question = pendingQuestion(); let readsAvailable = false;
  f.behavior.result = async id => questionResult(id, question);
  f.behavior.question = async id => {
    if (!readsAvailable) throw new PrimaryApiError('request_timeout');
    return questionResult(id, question);
  };
  const old = f.backend(oldSession); f.speak(old, oldSession, 'Compare these stocks.');
  await f.delegate(old, oldSession, 'comparison');
  f.speak(old, oldSession, 'Yes.', 3000, 3200); await f.delegate(old, oldSession, 'answer');
  const prepared = structuredClone(f.saved()[0].questions![questionId].reply!);
  assert.equal(prepared.status, 'prepared'); f.rotate();
  rejectBrokerPublication(f.store(), { callId: f.call.id, callSid, bindingId, sessionId: oldSession },
    prepared.admissionPresentation!.publicationReceipts![0]!.eventId, 'provider_error');
  readsAvailable = true; await f.delegate(old, oldSession, 'answer');
  const refused = f.saved()[0].questions![questionId].reply!;
  assert.equal(refused.status, 'refused'); assert.equal(refused.refusal!.code, 'question_publication_rejected');
  assert.deepEqual(refused.input, prepared.input); assert.equal(f.replies.length, 0); assert.equal(f.posts.length, 1);
});


test('ended-call closeout settles a definitely-undispatched answer locally and permits its source capture', async t => {
  const f = await currentQuestionFixture(t);
  f.behavior.question = async () => { throw new PrimaryApiError('request_timeout'); };
  f.speak(f.current, newSession, ' Yes. ', 3000, 3200);
  await f.delegate(f.current, newSession, 'saved_answer');
  const prepared = structuredClone(f.saved()[0].questions![questionId].reply!);
  assert.equal(prepared.status, 'prepared'); assert.equal(f.replies.length, 0);
  f.store().update(f.call.id, { status: 'completed', endedAt: new Date().toISOString() });
  f.callLifetime.abort();
  const readsBefore = f.questionReads.length;
  const closeoutFor = (sourceSessionId: string) => createPrimaryBackend({ callId: f.call.id, callSid, bindingId,
    store: f.store(), sourceSessionId, stabilityMs: 5,
    api: { async submit() { throw new Error('No new request on closeout'); },
      async result(idBinding, id) { assert.equal(idBinding, bindingId); return questionResult(id, f.question); },
      async question() { throw new Error('Unsent local answer requires no remote question read'); },
      async reply() { throw new Error('No answer POST after hangup'); },
      async captureTurn(idBinding, turn) { f.captures.push(structuredClone(turn)); return {
        bindingId: idBinding, sourceEventId: turn.sourceEventId, recordId: turn.sourceEventId, revision: turn.revision,
        revisionRecordId: eventId, bodyDigest: turnBodyDigest(turn), replayed: false }; } },
  });
  const originalCloseout = closeoutFor(oldSession);
  await boundedStep('original-session ended-call capture flush', originalCloseout.flushPendingCaptures());
  const closeout = closeoutFor(newSession);
  const refused = f.saved()[0].questions![questionId].reply!;
  assert.equal(refused.status, 'refused'); assert.equal(refused.refusal!.code, 'call_ended_before_reply_dispatch');
  assert.deepEqual(refused.input, prepared.input); assert.equal(f.questionReads.length, readsBefore);
  const originalTurn: PrimaryTurn = { sourceEventId: prepared.input.source.sourceEventId, revision: 1,
    itemId: prepared.input.source.itemId, role: 'user', text: prepared.input.text,
    occurredAt: prepared.input.source.receivedAt };
  assert.equal(originalTurn.text, ' Yes. ');
  assert.deepEqual(f.store().get(f.call.id)!.canonicalTurns?.[originalTurn.sourceEventId], {
    ...originalTurn, state: 'dispatching', sourceSessionId: newSession },
  'old-session closeout must leave the new-session raw source unacknowledged');
  // Production closeout flushes each source session before completed transcript capture.
  const flushed = await boundedStep('new-session ended-call capture flush', closeout.flushPendingCaptures());
  assert.deepEqual(flushed, { pending: 0, unsupported: [] });
  assert.deepEqual(f.captures.filter(turn => turn.sourceEventId === originalTurn.sourceEventId), [originalTurn],
    'the new session must acknowledge the original raw revision exactly once');
  assert.deepEqual(f.store().get(f.call.id)!.canonicalTurns?.[originalTurn.sourceEventId], {
    ...originalTurn, state: 'captured', sourceSessionId: newSession, recordId: originalTurn.sourceEventId });
  const capture = await boundedStep('ended-call completed transcript capture',
    closeout.captureCompletedTurns(f.contexts.get(newSession)!.channel.getContext(), {
      sessionFinalized: true, assistantPlaybackComplete: false }));
  assert.equal(capture.unsupported.includes('reply_outcome_unknown'), false);
  assert.equal(capture.unsupported.includes('capture_outcome_unknown'), false);
  assert.deepEqual(f.captures.filter(turn => turn.sourceEventId === originalTurn.sourceEventId), [originalTurn],
    'completed transcript capture must not duplicate or replace the acknowledged raw revision');
  assert.equal(originalCloseout.closeoutPending(), 0); assert.equal(closeout.closeoutPending(), 0);
  assert.deepEqual(f.saved()[0].questions![questionId].reply!.input, prepared.input);
  assert.equal(f.questionReads.length, readsBefore);
  assert.equal(f.replies.length, 0); assert.equal(f.posts.length, 1);
});
