import assert from 'node:assert/strict';
import test, { type TestContext } from 'node:test';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { BrokerStore } from '../src/broker/store.js';
import { brokerIdentity, createPrimaryBackend, type PrimaryBackend, type PrimaryBackendOptions,
  type SelectedQuestionSourceAnchor } from '../src/broker/backend.js';
import { turnBodyDigest } from '../src/broker/reconciliation.js';
import type { LiveBackendChannel, LiveDelegationRequest, LiveTranscriptSnapshot } from '../src/live/types.js';
import type { PrimarySubmission, PrimaryTurn, PrimaryTurnReceipt } from '../src/broker/primaryApi.js';

const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const bindingId = uuid(1), ownerPrincipalId = uuid(2), primaryDexterId = uuid(3), threadId = uuid(4);
const callSid = `CA${'a'.repeat(32)}`, sessionId = 'selected_source_session';
const version = { ownerPrincipalId, primaryDexterId, threadId, contextEpoch: '1', threadEventSequence: '1', digest: 'a'.repeat(64) };
const receipt = (turn: PrimaryTurn): PrimaryTurnReceipt => ({ bindingId, sourceEventId: turn.sourceEventId,
  revision: turn.revision, recordId: turn.sourceEventId, revisionRecordId: uuid(5), bodyDigest: turnBodyDigest(turn), replayed: false });

async function fixture(t: TestContext) {
  const directory = await mkdtemp(path.join(tmpdir(), 'broker-selected-freezer-'));
  let store = new BrokerStore(directory);
  const controller = new AbortController(), abort = () => controller.abort();
  let timedOut = false;
  const timer = setTimeout(() => { timedOut = true; abort(); }, 3_000);
  t.signal.addEventListener('abort', abort, { once: true });
  t.after(async () => {
    clearTimeout(timer); t.signal.removeEventListener('abort', abort); controller.abort();
    try { store.close(); } finally { await rm(directory, { recursive: true, force: true }); }
    assert.equal(timedOut, false, 'Fixture exceeded its 3s lifetime');
  });
  const call = store.reserve({ requestId: 'selected-freezer-fixture', direction: 'inbound', number: '+15550000001', purpose: '', callSid }).call;
  store.recordPrimaryBinding(call.id, { bindingId, callSid, state: 'open', context: { primaryDexterId, threadId, displayName: 'Dexter', messages: [] } });
  store.update(call.id, { status: 'in-progress', mediaStartedAt: call.createdAt, liveSessionId: sessionId,
    liveGenerations: { version: 1, callId: call.id, binding: { bindingId, callSid, ownerPrincipalId, primaryDexterId, threadId },
      clocks: { mediaStartedAt: call.createdAt, callDeadlineAt: new Date(Date.parse(call.createdAt) + 60_000).toISOString() },
      phase: 'active', inputNextSample: 0, deliveryAttachment: { generationId: uuid(6), sessionId },
      segments: [{ generationId: uuid(6), ordinal: 1, sessionId, createdAt: call.createdAt, readyAt: call.createdAt,
        state: 'active', version, pageDigest: 'b'.repeat(64), inputFirstSample: 0, inputLastSampleExclusive: 0,
        transcript: { revision: 0, callerRevision: 0, fragments: [] }, outputDiscarded: false }] } });
  const snapshot: LiveTranscriptSnapshot = { revision: 0, callerRevision: 0, fragments: [] };
  const listeners = new Set<(value: LiveTranscriptSnapshot) => void>();
  let sources: SelectedQuestionSourceAnchor[] = [];
  const posts: PrimarySubmission[] = [], captures: PrimaryTurn[] = [];
  const channel: LiveBackendChannel = { signal: controller.signal, getContext: () => structuredClone(snapshot),
    subscribeContext(listener) { listeners.add(listener); return () => { listeners.delete(listener); }; },
    async publish(update) { return { eventId: update.id, status: 'accepted', observedCallerRevision: snapshot.callerRevision,
      observedTranscriptSequence: snapshot.revision }; } };
  const api: PrimaryBackendOptions['api'] = {
    async submit(_binding, requestId, input) { posts.push(structuredClone(input)); return { bindingId, requestId, state: 'completed',
      assistant: { messageId: uuid(7), eventId: uuid(8), sequence: '2', text: 'The result is saved.' } }; },
    async result() { throw new Error('No request read expected'); },
    async question() { throw new Error('Freezing must not read a question'); },
    async reply() { throw new Error('Freezing must not post a reply'); },
    async captureTurn(_binding, turn) { captures.push(structuredClone(turn)); return receipt(turn); },
  };
  const create = (selected = true, source = sessionId): PrimaryBackend => createPrimaryBackend({ callId: call.id,
    callSid, bindingId, api, store, sourceSessionId: source, stabilityMs: 2, maxWaitMs: 100,
    maxPollMs: 100, progressIntervalMs: 60_000, ...(selected ? { selectedQuestionSources: () => structuredClone(sources) } : {}) });
  let backend = create();
  const add = (text: string, startMs = 25, endMs = 200, eventId = `caption_${snapshot.revision + 1}`) => {
    snapshot.revision++; snapshot.callerRevision++;
    snapshot.fragments.push({ eventId, role: 'user', text, startMs, endMs, sequence: snapshot.revision });
    const saved = store.get(call.id)!, generation = saved.liveGenerations!;
    generation.segments.find(value => value.sessionId === saved.liveSessionId)!.transcript = structuredClone(snapshot);
    store.update(call.id, { liveGenerations: generation });
    for (const listener of listeners) listener(structuredClone(snapshot));
  };
  const speech = (offsetMs = 300, target = backend) => {
    const saved = store.get(call.id)!, generation = saved.liveGenerations!;
    const segment = generation.segments.find(value => value.sessionId === saved.liveSessionId)!;
    segment.inputLastSampleExclusive = segment.inputFirstSample! + offsetMs * 8;
    generation.inputNextSample = Math.max(generation.inputNextSample, segment.inputLastSampleExclusive);
    store.update(call.id, { liveGenerations: generation }); target.noteCallerSpeech(Date.now() - 20, offsetMs);
  };
  const request = (offsetMs = 100): LiveDelegationRequest => ({ callId: call.id, sessionId: store.get(call.id)!.liveSessionId!,
    delegationId: 'explicit_answer', requestId: 'explicit_answer_request', offsetMs, transcript: structuredClone(snapshot) });
  return { call, add, speech, request, channel, controller, posts, captures,
    backend: () => backend, create, store: () => store, setSources(value: SelectedQuestionSourceAnchor[]) { sources = structuredClone(value); },
    sources: () => structuredClone(sources), snapshot: () => structuredClone(snapshot),
    disk: async () => JSON.parse(await readFile(path.join(directory, 'calls.json'), 'utf8')).calls[call.id],
    restart() { backend.persistAcousticEvidence(); store.close(); store = new BrokerStore(directory); backend = create(); },
  };
}

test('actual backend freezer preserves raw text and source clock without HTTP or a journalled ordinary request', { timeout: 6_000 }, async t => {
  const h = await fixture(t); h.add('  Yes, compare both.\t'); h.speech();
  const before = await h.disk();
  const frozen = await h.backend().freezeCallerTurn(h.request(), h.channel, []);
  assert.ok(frozen); assert.equal(frozen.turn.text, '  Yes, compare both.\t');
  assert.equal(frozen.turn.sourceEventId, brokerIdentity('source', h.call.id, sessionId, 'caption_1'));
  assert.equal(frozen.turn.occurredAt, new Date(Date.parse(h.call.createdAt) + 25).toISOString());
  assert.equal(frozen.endMs, 200); assert.equal(h.request().offsetMs, 100, 'caption end may follow explicit delegation offset');
  assert.deepEqual(await h.disk(), before); assert.deepEqual(h.posts, []); assert.deepEqual(h.captures, []);
});

test('selected port refuses the ordinary delegate path before any new Primary run', { timeout: 6_000 }, async t => {
  const h = await fixture(t); h.add('Hello'); h.speech();
  await assert.rejects(h.backend().delegate(h.request(), h.channel), /selected_question_requires_answer_port/);
  assert.deepEqual(h.posts, []); assert.deepEqual((await h.disk()).delegations, {});
});

test('ordinary admission uses the same extracted candidate and preserves its raw submission', { timeout: 6_000 }, async t => {
  const h = await fixture(t); h.add('  Please compare NVIDIA and Apple.\n'); h.speech();
  const frozen = await h.backend().freezeCallerTurn(h.request(), h.channel, []); assert.ok(frozen);
  h.backend().persistAcousticEvidence();
  const ordinary = h.create(false); await ordinary.delegate(h.request(), h.channel);
  assert.equal(h.posts.length, 1); const input = h.posts[0]!;
  assert.equal(input.text, frozen.turn.text); assert.equal(input.source.sourceEventId, frozen.turn.sourceEventId);
  assert.equal(input.source.receivedAt, frozen.turn.occurredAt); assert.equal(input.source.itemId, frozen.turn.itemId);
  assert.equal(input.source.revision, 1); assert.equal(input.callAssignmentRef, undefined);
});

test('two completed caller groups choose the explicitly delegated group and preserve earlier accepted source', { timeout: 6_000 }, async t => {
  const h = await fixture(t); h.add('Yes.', 25, 200); h.speech();
  const first = await h.backend().freezeCallerTurn(h.request(), h.channel, []); assert.ok(first);
  h.setSources([{ frozen: first, capture: receipt(first.turn), replyPending: false }]);
  h.add('Then compare the risk.', 1800, 2100); h.speech(2200);
  const second = await h.backend().freezeCallerTurn(h.request(1900), h.channel, [first.turn.sourceEventId]);
  assert.ok(second); assert.equal(second.turn.text, 'Then compare the risk.');
  assert.equal(second.firstSequence, 2); assert.notEqual(second.turn.sourceEventId, first.turn.sourceEventId);
  assert.deepEqual(h.sources()[0]!.frozen, first);
});

test('later caption extension retains its accepted source and cannot create another answer', { timeout: 6_000 }, async t => {
  const h = await fixture(t); h.add('Yes.', 25, 200); h.speech();
  const frozen = await h.backend().freezeCallerTurn(h.request(), h.channel, []); assert.ok(frozen);
  h.setSources([{ frozen, capture: receipt(frozen.turn), replyPending: false }]);
  h.add(' Compare both.', 210, 500); h.speech(600);
  assert.equal(await h.backend().freezeCallerTurn(h.request(400), h.channel, []), undefined);
  assert.equal(h.sources()[0]!.frozen.turn.text, 'Yes.');
});

test('accepted original capture prevents duplicate revision1 and later final text becomes revision2', { timeout: 6_000 }, async t => {
  const h = await fixture(t); h.add('Yes.', 25, 200); h.speech();
  const frozen = await h.backend().freezeCallerTurn(h.request(), h.channel, []); assert.ok(frozen);
  h.setSources([{ frozen, capture: receipt(frozen.turn), replyPending: false }]);
  assert.equal((await h.backend().captureCompletedTurns(h.snapshot(), { sessionFinalized: true, assistantPlaybackComplete: false })).captured, 0);
  assert.equal(h.captures.length, 0);
  h.add(' Compare both.', 210, 500); h.speech(600);
  const result = await h.backend().captureCompletedTurns(h.snapshot(), { sessionFinalized: true, assistantPlaybackComplete: false });
  assert.equal(result.captured, 1); assert.equal(h.captures.length, 1);
  assert.deepEqual(h.captures[0], { ...frozen.turn, revision: 2, text: 'Yes. Compare both.' });
  assert.deepEqual(h.sources()[0]!.frozen, frozen); assert.deepEqual(h.posts, []);
});

test('unresolved selected reply or missing raw capture ACK blocks final source revision and closeout', { timeout: 6_000 }, async t => {
  const h = await fixture(t); h.add('Yes.', 25, 200); h.speech();
  const frozen = await h.backend().freezeCallerTurn(h.request(), h.channel, []); assert.ok(frozen);
  h.setSources([{ frozen, capture: receipt(frozen.turn), replyPending: true }]);
  h.add(' Compare both.', 210, 500); h.speech(600);
  let result = await h.backend().captureCompletedTurns(h.snapshot(), { sessionFinalized: true, assistantPlaybackComplete: false });
  assert.ok(result.unsupported.includes('reply_outcome_unknown')); assert.equal(h.backend().closeoutPending(), 1);
  h.setSources([{ frozen, replyPending: false }]);
  result = await h.backend().captureCompletedTurns(h.snapshot(), { sessionFinalized: true, assistantPlaybackComplete: false });
  assert.ok(result.unsupported.includes('capture_outcome_unknown')); assert.equal(h.backend().closeoutPending(), 1);
  assert.equal(h.captures.length, 0);
});

test('restart uses persisted acoustic source evidence and skips an already acknowledged selected answer', { timeout: 6_000 }, async t => {
  const h = await fixture(t); h.add('Yes.'); h.speech();
  const frozen = await h.backend().freezeCallerTurn(h.request(), h.channel, []); assert.ok(frozen);
  h.setSources([{ frozen, capture: receipt(frozen.turn), replyPending: false }]); h.restart();
  assert.equal(await h.backend().freezeCallerTurn(h.request(), h.channel, []), undefined);
  assert.equal((await h.backend().captureCompletedTurns(h.snapshot(), { sessionFinalized: true, assistantPlaybackComplete: false })).captured, 0);
  assert.equal(h.captures.length, 0); assert.equal(h.backend().closeoutPending(), 0);
});

test('caller audio is required but noise bursts are not counted as transcript identities', { timeout: 6_000 }, async t => {
  const h = await fixture(t); h.add('Yes.');
  assert.equal(await h.backend().freezeCallerTurn(h.request(), h.channel, []), undefined);
  h.speech(100); h.speech(200); h.speech(300);
  const frozen = await h.backend().freezeCallerTurn(h.request(), h.channel, []);
  assert.ok(frozen); assert.equal(frozen.turn.text, 'Yes.'); assert.equal(frozen.firstEventId, 'caption_1');
});

test('abort and call stop refuse a source freeze without a new request or capture', { timeout: 6_000 }, async t => {
  const h = await fixture(t); h.add('Yes.'); h.speech(); h.controller.abort();
  assert.equal(await h.backend().freezeCallerTurn(h.request(), h.channel, []), undefined);
  assert.deepEqual(h.posts, []); assert.deepEqual(h.captures, []);
  const next = await fixture(t); next.add('Yes.'); next.speech();
  next.store().update(next.call.id, { status: 'completed', endedAt: new Date().toISOString() });
  assert.equal(await next.backend().freezeCallerTurn(next.request(), next.channel, []), undefined);
  assert.deepEqual(next.posts, []); assert.deepEqual(next.captures, []);
});

test('foreign call/session and changed selected capture identity refuse before execution', { timeout: 6_000 }, async t => {
  const h = await fixture(t); h.add('Yes.'); h.speech();
  await assert.rejects(h.backend().freezeCallerTurn({ ...h.request(), callId: uuid(99) }, h.channel, []), /identity_mismatch/);
  await assert.rejects(h.backend().freezeCallerTurn({ ...h.request(), sessionId: 'foreign' }, h.channel, []), /identity_mismatch/);
  const frozen = await h.backend().freezeCallerTurn(h.request(), h.channel, []); assert.ok(frozen);
  h.setSources([{ frozen, capture: { ...receipt(frozen.turn), bodyDigest: 'c'.repeat(64) }, replyPending: false }]);
  await assert.rejects(h.backend().freezeCallerTurn(h.request(), h.channel, []), /capture_acknowledgement_mismatch/);
  assert.deepEqual(h.posts, []);
});
