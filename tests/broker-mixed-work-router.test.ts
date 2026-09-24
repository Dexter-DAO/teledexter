import assert from 'node:assert/strict';
import test, { type TestContext } from 'node:test';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { BrokerStore } from '../src/broker/store.js';
import { brokerIdentity } from '../src/broker/backend.js';
import { createMixedWorkRouter, recoverCallerRouteCaptures, type CallerIntentPort } from '../src/broker/mixedWork.js';
import { turnBodyDigest } from '../src/broker/reconciliation.js';
import type { FrozenCallerTurn } from '../src/broker/callerTurn.js';
import type { CallerRouteDecision } from '../src/broker/callerRouting.js';
import type { LiveBackendChannel, LiveDelegationRequest } from '../src/live/types.js';

const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const sid = `CA${'a'.repeat(32)}`, sessionId = 'mixed_session', bindingId = uuid(2), generationId = uuid(3);
function deferred<T>() { let resolve!: (value: T) => void; const promise = new Promise<T>(done => { resolve = done; }); return { promise, resolve }; }
async function until(condition: () => boolean) {
  for (let index = 0; index < 100 && !condition(); index++) await Promise.resolve();
  assert.ok(condition(), 'expected source boundary');
}
function fixture(t: TestContext, resolve: CallerIntentPort['resolve'], timeoutMs = 1000,
  onAdmit?: (request: LiveDelegationRequest) => Promise<void>) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-mixed-router-'));
  let store = new BrokerStore(dir);
  const reserved = store.reserve({ requestId: uuid(1), direction: 'outbound', number: '+12025550123', purpose: 'Continue saved work.', callSid: sid }).call;
  const at = new Date(Date.now() - 10000).toISOString(), version = { ownerPrincipalId: uuid(4), primaryDexterId: uuid(5),
    threadId: uuid(6), contextEpoch: '1', threadEventSequence: '1', digest: 'a'.repeat(64) };
  store.recordPrimaryBinding(reserved.id, { bindingId, callSid: sid, state: 'open', context: {
    primaryDexterId: version.primaryDexterId, threadId: version.threadId, displayName: 'Dexter', messages: [] } });
  const transcript = { revision: 1, callerRevision: 1, fragments: [{ eventId: 'user1', role: 'user' as const,
    text: ' Research Apple.\t', startMs: 0, endMs: 100, sequence: 1 }] };
  store.update(reserved.id, { status: 'in-progress', mediaStartedAt: at, liveSessionId: sessionId,
    liveGenerations: { version: 1, callId: reserved.id, binding: { bindingId, callSid: sid,
      ownerPrincipalId: version.ownerPrincipalId, primaryDexterId: version.primaryDexterId, threadId: version.threadId },
      phase: 'active', clocks: { mediaStartedAt: at, callDeadlineAt: new Date(Date.now() + 60000).toISOString() }, inputNextSample: 8000,
      deliveryAttachment: { generationId, sessionId }, segments: [{ generationId, ordinal: 1, state: 'active', sessionId,
        createdAt: at, readyAt: at, version, pageDigest: 'b'.repeat(64), transcript, inputFirstSample: 0,
        inputLastSampleExclusive: 8000, outputDiscarded: false }] } });
  const sourceId = brokerIdentity('source', reserved.id, sessionId, 'user1');
  const frozen: FrozenCallerTurn = { sessionId, firstEventId: 'user1', callerRevision: 1, firstSequence: 1, lastSequence: 1,
    startMs: 0, endMs: 100, audioThroughMs: 1000, observedSpeechAtMs: Date.parse(at) + 100,
    turn: { sourceEventId: sourceId, revision: 1, itemId: `live-turn-${sourceId}`, role: 'user', text: transcript.fragments[0]!.text, occurredAt: at } };
  const request: LiveDelegationRequest = { callId: reserved.id, sessionId, delegationId: 'delegation1', requestId: 'live_request1',
    offsetMs: 100, transcript };
  const abort = new AbortController();
  const channel: LiveBackendChannel = { signal: abort.signal, getContext: () => transcript, subscribeContext: () => () => {},
    async publish() { throw new Error('No direct Live output from router'); } };
  let freezes = 0, resolutions = 0, captures = 0;
  const decisions: CallerRouteDecision[] = [], notices: string[] = [], settled: LiveDelegationRequest[] = [];
  const primary = { async captureTurn(receivedBinding: string, turn: typeof frozen.turn) {
    assert.equal(receivedBinding, bindingId); assert.deepEqual(turn, frozen.turn); captures++;
    return { bindingId, sourceEventId: sourceId, revision: 1, recordId: sourceId,
      revisionRecordId: uuid(7), bodyDigest: turnBodyDigest(turn), replayed: false };
  } };
  const read = () => store.get(reserved.id)!;
  const router = createMixedWorkRouter({ callId: reserved.id, store, intent: { timeoutMs,
    resolve(input, signal) { resolutions++; assert.equal(read().callerRouting!.records[0]!.inputDigest, input.record.inputDigest);
      assert.equal(input.record.frozen.turn.text, frozen.turn.text); return resolve(input, signal); } },
    freezer: { async freeze() { freezes++; return frozen; } },
    active: () => !abort.signal.aborted && !read().endedAt,
    resolutionInput: () => ({ version, publicationSequence: 0, questions: [], references: [] }),
    async admit(request, ref, decision) {
      assert.deepEqual(read().callerRouting!.records.find(r => r.inputDigest === ref.inputDigest)?.decision, decision);
      decisions.push(decision);
      await onAdmit?.(request);
    },
    admissionSettled(request) { settled.push(request); },
    async clarify(_request, reason) { notices.push(reason); }, capture: primary,
  });
  t.after(async () => { abort.abort(); await router.prepareCloseout(); store.close(); fs.rmSync(dir, { recursive: true, force: true }); });
  return { router, request, channel, frozen, decisions, notices, settled, read, primary, store: () => store,
    counts: () => ({ freezes, resolutions, captures }), abort,
    restart() { store.close(); store = new BrokerStore(dir); return store; } };
}

test('duplicate in-flight delegation freezes and resolves once before its exact persisted route dispatches', async t => {
  const wait = deferred<void>();
  const f = fixture(t, async input => { await wait.promise; return { inputDigest: input.record.inputDigest, decision: { kind: 'task' } }; });
  const first = f.router.delegate(f.request, f.channel), second = f.router.delegate(f.request, f.channel);
  assert.equal(first, second); await until(() => f.counts().resolutions === 1);
  assert.equal(f.decisions.length, 0); assert.equal(f.read().callerRouting!.records[0]!.decision, undefined);
  wait.resolve(); await first;
  assert.deepEqual(f.decisions, [{ kind: 'task' }]); assert.equal(f.counts().freezes, 1);
});

test('wrong digest, unavailable inference and mixed speech never dispatch and preserve raw capture', async t => {
  for (const kind of ['wrong_digest', 'unavailable', 'mixed'] as const) {
    const f = fixture(t, async input => {
      if (kind === 'unavailable') throw new Error('upstream unavailable');
      return { inputDigest: kind === 'wrong_digest' ? 'f'.repeat(64) : input.record.inputDigest,
        decision: kind === 'mixed' ? { kind: 'clarify', reason: 'mixed_intent' } : { kind: 'task' } };
    });
    await f.router.delegate(f.request, f.channel); await f.router.delegate(f.request, f.channel);
    assert.equal(f.decisions.length, 0); assert.equal(f.counts().resolutions, 1); assert.equal(f.counts().freezes, 1);
    assert.equal(f.counts().captures, 1); assert.ok(f.read().callerRouting!.records[0]!.capture);
    assert.equal(f.read().callerRouting!.records[0]!.frozen.turn.text, f.frozen.turn.text);
  }
});

test('call stop defeats a late task decision and restart captures the same source without resolver or action', async t => {
  const wait = deferred<void>();
  const f = fixture(t, async input => { await wait.promise; return { inputDigest: input.record.inputDigest, decision: { kind: 'task' } }; });
  const result = f.router.delegate(f.request, f.channel);
  await until(() => f.counts().resolutions === 1);
  f.router.stop(); f.abort.abort(); wait.resolve(); await result;
  f.store().update(f.request.callId, { status: 'completed', endedAt: new Date().toISOString() });
  const resumed = f.restart();
  assert.equal(await recoverCallerRouteCaptures({ callId: f.request.callId, store: resumed, primary: f.primary }), 0);
  assert.equal(f.decisions.length, 0); assert.equal(f.counts().resolutions, 1); assert.equal(f.counts().captures, 1);
});

test('changed duplicate identity is refused during the first pending resolution', async t => {
  const wait = deferred<void>();
  const f = fixture(t, async input => { await wait.promise; return { inputDigest: input.record.inputDigest, decision: { kind: 'conversation' } }; });
  const result = f.router.delegate(f.request, f.channel); await until(() => f.counts().resolutions === 1);
  await assert.rejects(f.router.delegate({ ...f.request, offsetMs: 101 }, f.channel), /identity_changed/);
  wait.resolve(); await result; assert.equal(f.counts().resolutions, 1); assert.equal(f.decisions.length, 0);
});

test('duplicate handoffs share admission completion without waiting for the Primary result', async t => {
  const observed = deferred<void>();
  t.after(() => observed.resolve());
  const f = fixture(t, async input => ({ inputDigest: input.record.inputDigest, decision: { kind: 'task' } }),
    1000, async () => observed.promise);
  const first = f.router.delegate(f.request, f.channel);
  await until(() => f.decisions.length === 1);
  const beforeAdmission = { ...f.request };
  const second = f.router.delegate(beforeAdmission, f.channel);
  assert.equal(first, second);
  assert.equal(f.settled.length, 0);
  f.router.admissionSettled(f.request);
  assert.ok(f.settled.includes(f.request));
  assert.ok(f.settled.includes(beforeAdmission));
  assert.equal(f.router.pendingSourceCount(), 0);
  const afterAdmission = { ...f.request };
  const third = f.router.delegate(afterAdmission, f.channel);
  assert.equal(first, third);
  assert.ok(f.settled.includes(afterAdmission));
  assert.equal(f.counts().resolutions, 1);
  observed.resolve();
  await first;
});
