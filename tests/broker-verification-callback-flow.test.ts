import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { BrokerStore } from '../src/broker/store.js';
import { createVerificationCallbackFlow, type VerificationCallbackProviderObservation } from '../src/broker/verificationCallbackFlow.js';
import { phoneVerificationCallbackCommandRef, type PhoneVerificationCallbackPermit, type PhoneVerificationCallbackState,
  type PhoneVerificationCallbackCompletionRecovery, type PhoneVerificationCallbackCompletion } from '../src/broker/verificationCallbackContract.js';
import type { VerificationCallbackClient } from '../src/broker/verificationCallbackClient.js';
import { qualifyVerificationCallbackTurn } from '../src/broker/verificationCallbackTurn.js';
import { brokerIdentity } from '../src/broker/backend.js';
import type { FrozenCallerTurn } from '../src/broker/callerTurn.js';
import type { LiveDelegationRequest } from '../src/live/types.js';

const id = (n: number) => `00000000-0000-4000-8000-${n.toString().padStart(12, '0')}`;
const start = Date.parse('2026-09-25T06:00:00.000Z'), sid = 'CA' + 'b'.repeat(32);
const ref = { requestId: id(1), requestDigest: 'c'.repeat(64) };
const permit: PhoneVerificationCallbackPermit = { v: 1, request: ref, purpose: 'enrollment', providerAccountSid: 'AC' + 'a'.repeat(32),
  from: '+15555550001', to: '+15555550002', answerUrl: `https://broker.example.test/broker/verification-callback/${id(1)}`,
  responseUrl: `https://broker.example.test/broker/verification-callback-response/${id(1)}`, createdAt: new Date(start).toISOString(),
  expiresAt: new Date(start + 300000).toISOString(), maxDurationSeconds: 180,
  billing: { intentId: id(1), eventId: `phone-verification:${id(1)}`, mandateId: id(2), unitPriceMicro: 1000, freeSeconds: 5 } };
const observed: VerificationCallbackProviderObservation = { accountSid: permit.providerAccountSid,
  from: permit.from, to: permit.to, callSid: sid, status: 'queued' };
function deferred<T>() { let resolve!: (value: T) => void; const promise = new Promise<T>(yes => { resolve = yes; }); return { promise, resolve }; }
function fixture(t: test.TestContext) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-callback-flow-'));
  let store = new BrokerStore(directory), clock = start + 10;
  let remote: PhoneVerificationCallbackState = { v: 1, request: ref, state: 'reserved', permit };
  let completionResult: PhoneVerificationCallbackCompletionRecovery | undefined;
  let dispatchGate: ReturnType<typeof deferred<void>> | undefined, createGate: ReturnType<typeof deferred<VerificationCallbackProviderObservation>> | undefined;
  let loseCreate = false, loseCompletion = false;
  const counts = { get: 0, dispatch: 0, create: 0, answer: 0, complete: 0, completionGet: 0, cancel: 0, hangup: 0 };
  const api: VerificationCallbackClient = {
    async prepareAccess() { throw new Error('not used for direct-owner enrollment kickoff'); },
    async recover() { counts.get++; return structuredClone(remote); },
    async dispatch(input) {
      counts.dispatch++; assert.equal(store.get(id(1))?.verificationCallback?.dispatch?.attemptId, input.dispatchAttemptId);
      if (dispatchGate) await dispatchGate.promise;
      remote = { v: 1, request: ref, state: 'dispatching', dispatchAttemptId: input.dispatchAttemptId, expiresAt: permit.expiresAt };
      return { v: 1, state: 'dispatch_granted', dispatchAttemptId: input.dispatchAttemptId, permit };
    },
    async answer(input) { counts.answer++; assert.equal(store.get(id(1))?.verificationCallback?.answer?.proof.signature, input.providerProof.signature);
      remote = { v: 1, request: ref, state: 'answered', dispatchAttemptId: input.dispatchAttemptId,
        callSid: sid, purpose: 'enrollment', expiresAt: permit.expiresAt }; return structuredClone(remote); },
    async complete(input) {
      counts.complete++; assert.ok(store.get(id(1))?.verificationCallback?.completion);
      assert.equal(fs.readFileSync(path.join(directory, 'calls.json'), 'utf8').includes('01234567'), false);
      const result = { v: 1 as const, command: phoneVerificationCallbackCommandRef(input), state: 'rejected' as const, reason: 'challenge_invalid' as const };
      completionResult = result; if (loseCompletion) throw new Error('lost response'); return result;
    },
    async recoverCompletion(command) { counts.completionGet++; return completionResult ?? { v: 1, command, state: 'absent' }; },
    async cancel() { counts.cancel++; remote = { v: 1, request: ref, state: 'cancelled', cancelledAt: new Date(clock).toISOString() }; return structuredClone(remote); },
  };
  const provider = {
    async create(input: { permit: PhoneVerificationCallbackPermit; dispatchAttemptId: string }) {
      counts.create++; assert.deepEqual(input.permit, permit);
      assert.ok(store.get(id(1))?.verificationCallback?.dispatch?.providerStartedAt);
      if (createGate) return createGate.promise;
      if (loseCreate) throw new Error('ambiguous provider create'); return structuredClone(observed);
    },
    async hangup(callSid: string) { counts.hangup++; assert.equal(callSid, sid);
      assert.ok(store.get(id(1))?.verificationCallback?.endDispatchedAt);
      return { ...observed, status: 'completed' as const }; },
  };
  const build = () => createVerificationCallbackFlow({ store, api, provider, providerTimeoutMs: 1000,
    verifyAnswer: proof => proof.signature === 'actual-synthetic-signature', now: () => clock });
  let flow = build(); t.after(() => { store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  return { get store() { return store; }, get flow() { return flow; }, counts, api,
    setRemote(value: PhoneVerificationCallbackState) { remote = value; },
    loseCreate() { loseCreate = true; }, loseCompletion() { loseCompletion = true; },
    holdDispatch() { dispatchGate = deferred<void>(); return dispatchGate; },
    holdCreate() { createGate = deferred<VerificationCallbackProviderObservation>(); return createGate; },
    restart() { store.close(); store = new BrokerStore(directory); flow = build(); },
    advance(ms: number) { clock += ms; }, now: () => clock,
    proof: { url: permit.answerUrl, signature: 'actual-synthetic-signature', params: {
      AccountSid: permit.providerAccountSid, CallSid: sid, Direction: 'outbound-api', From: permit.from, To: permit.to, CallStatus: 'in-progress' } },
  };
}
async function until(check: () => boolean): Promise<void> {
  for (let n = 0; n < 100; n++) { if (check()) return; await new Promise<void>(yes => setImmediate(yes)); }
  throw new Error('Expected bounded handshake did not arrive');
}

test('fresh kickoff uses exact API permit and persisted fences; repeated and restarted kickoff never redials', async t => {
  const f = fixture(t); assert.equal((await f.flow.kickoff(ref)).state, 'dispatching');
  await f.flow.kickoff(ref); f.restart(); await f.flow.kickoff(ref);
  assert.equal(f.counts.dispatch, 1); assert.equal(f.counts.create, 1); assert.equal(f.store.get(id(1))?.callSid, sid);
});

test('unknown provider create with no SID and absent API read retain the original fence', async t => {
  const f = fixture(t); f.loseCreate(); await assert.rejects(f.flow.kickoff(ref));
  f.setRemote({ v: 1, request: ref, state: 'absent' }); f.restart();
  assert.equal((await f.flow.kickoff(ref)).state, 'absent');
  assert.equal(f.counts.create, 1); assert.equal(f.counts.dispatch, 1);
  assert.equal(f.store.get(id(1))?.callSid, undefined);
});

test('simultaneous kickoff coalesces exact identity and rejects changed digest', async t => {
  const f = fixture(t), gate = f.holdDispatch(); const first = f.flow.kickoff(ref);
  try {
    await until(() => f.counts.dispatch === 1);
    const second = f.flow.kickoff(ref); assert.equal(first, second);
    assert.throws(() => f.flow.kickoff({ ...ref, requestDigest: 'f'.repeat(64) }));
  } finally { gate.resolve(); await first; }
  assert.equal(f.counts.create, 1);
});

test('provider callback before create response retains terminal and exact SID', async t => {
  const f = fixture(t), gate = f.holdCreate(); const pending = f.flow.kickoff(ref);
  try {
    await until(() => f.counts.create === 1);
    f.flow.observeProvider(ref, { ...observed, status: 'completed' }, 'signed_callback');
    gate.resolve(observed); await pending;
    assert.equal(f.store.get(id(1))?.verificationCallback?.providerTerminal?.status, 'completed');
  } finally { gate.resolve(observed); await pending.catch(() => {}); }
});

test('stop during held grant prevents provider contact without waiting for that grant', async t => {
  const f = fixture(t), gate = f.holdDispatch(), pending = f.flow.kickoff(ref);
  try { await until(() => f.counts.dispatch === 1); await f.flow.stop(ref); assert.equal(f.counts.create, 0); }
  finally { gate.resolve(); await assert.rejects(pending); }
  assert.equal(f.counts.create, 0);
});

test('stop before lost create resolves hangs up exact late SID once without a second create', async t => {
  const f = fixture(t), gate = f.holdCreate(), pending = f.flow.kickoff(ref);
  try {
    await until(() => f.counts.create === 1); await f.flow.stop(ref);
    assert.equal(f.counts.hangup, 0); gate.resolve(observed); await pending;
    await until(() => f.counts.hangup === 1);
    await f.flow.endLeg(ref); assert.equal(f.counts.hangup, 1); assert.equal(f.counts.create, 1);
  } finally { gate.resolve(observed); await pending.catch(() => {}); }
});

function qualified(f: ReturnType<typeof fixture>) {
  const sessionId = 'restricted-session', firstEventId = 'caption-1';
  const source = brokerIdentity('source', id(1), sessionId, firstEventId);
  const frozen: FrozenCallerTurn = { sessionId, firstEventId, callerRevision: 1, firstSequence: 1, lastSequence: 1,
    startMs: 10, endMs: 20, audioThroughMs: 40, observedSpeechAtMs: f.now(),
    turn: { sourceEventId: source, itemId: `live-turn-${source}`, role: 'user', revision: 1,
      text: 'Yes, complete this phone setup.', occurredAt: new Date(f.now()).toISOString() } };
  const request: LiveDelegationRequest = { callId: id(1), sessionId, delegationId: 'delegation-1', requestId: 'request-1', offsetMs: 30,
    transcript: { revision: 1, callerRevision: 1, fragments: [{ eventId: firstEventId, role: 'user', sequence: 1,
      text: frozen.turn.text, startMs: 10, endMs: 20 }] } };
  const journal = f.store.get(id(1))?.verificationCallback; assert.ok(journal);
  const turn = qualifyVerificationCallbackTurn({ toolCallId: 'tool_complete', journal, request, frozen, current: {
    sessionId, audioThroughMs: 40, observedSpeechAtMs: f.now(), transcript: request.transcript },
    acceptedAtMs: f.now(), receiptSecret: 'synthetic-receipt-secret-at-least-32-chars', execution: 'complete_verification', isCurrent: () => true });
  const input: PhoneVerificationCallbackCompletion = { v: 1, request: ref, brokerCallId: id(1), callSid: sid,
    purpose: 'enrollment', accepted: true, turn: turn.turn, challenge: '01234567' };
  return { turn, input, request, frozen, journal };
}
test('accepted same-leg turn completion persists safe receipt first; uncertain POST recovers GET then ends enrollment leg', async t => {
  const f = fixture(t); await f.flow.kickoff(ref); f.advance(10); await f.flow.answer(ref, f.proof);
  const q = qualified(f); f.loseCompletion(); await assert.rejects(f.flow.complete(q.input, q.turn));
  f.restart(); await f.flow.recover(ref);
  assert.equal(f.counts.complete, 1); assert.equal(f.counts.completionGet, 1); assert.equal(f.counts.hangup, 1);
  assert.equal(f.store.get(id(1))?.primary, undefined);
});

test('turn receipt rejects changed audio before captions and carries no raw transcript/challenge', async t => {
  const f = fixture(t); await f.flow.kickoff(ref); await f.flow.answer(ref, f.proof); const q = qualified(f);
  assert.equal(JSON.stringify(q.turn).includes(q.frozen.turn.text), false);
  assert.throws(() => qualifyVerificationCallbackTurn({ toolCallId: 'tool_complete', journal: q.journal, request: q.request, frozen: q.frozen,
    current: { sessionId: q.frozen.sessionId, audioThroughMs: 80, observedSpeechAtMs: f.now() + 1, transcript: q.request.transcript },
    acceptedAtMs: f.now(), receiptSecret: 'synthetic-receipt-secret-at-least-32-chars', execution: 'complete_verification', isCurrent: () => true }), /acoustic_changed/);
  await assert.rejects(f.flow.complete(q.input, structuredClone(q.turn)), /unqualified/);
  assert.equal(f.counts.complete, 0);
});
