import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { BrokerStore } from '../src/broker/store.js';
import { createRestrictedSetupHost } from '../src/broker/restrictedSetupHost.js';
import { createRestrictedCallerTurnFreezer } from '../src/broker/restrictedCallerTurn.js';
import { createVerificationCallbackFlow } from '../src/broker/verificationCallbackFlow.js';
import { prepareVerificationCallbackDispatch, recordVerificationCallbackGrant, fenceVerificationCallbackProvider,
  prepareVerificationCallbackAnswer, recordVerificationCallbackRemote } from '../src/broker/verificationCallbackJournal.js';
import { phoneVerificationCallbackCommandRef, type PhoneVerificationCallbackPermit } from '../src/broker/verificationCallbackContract.js';
import type { VerificationCallbackClient } from '../src/broker/verificationCallbackClient.js';
import type { LiveDelegationRequest } from '../src/live/types.js';
import type { RestrictedSetupTool } from '../src/live/restrictedSetupTools.js';
import type { PrimaryCall } from '../src/broker/primaryApi.js';

const id = (n: number) => `00000000-0000-4000-8000-${n.toString().padStart(12, '0')}`;
function fixture(t: test.TestContext, purpose: 'enrollment' | 'account_access' = 'enrollment') {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'restricted-host-')), store = new BrokerStore(directory);
  t.after(() => { store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  const start = Date.now(), now = () => start + 10, at = (n: number) => new Date(start + n).toISOString();
  const ref = { requestId: id(1), requestDigest: 'a'.repeat(64) }, sid = 'CA' + 'a'.repeat(32);
  const permit: PhoneVerificationCallbackPermit = { v: 1, request: ref, purpose, providerAccountSid: 'AC' + 'a'.repeat(32),
    from: '+15555550001', to: '+15555550002', answerUrl: `https://broker.example.test/broker/verification-callback/${id(1)}`,
    responseUrl: `https://broker.example.test/broker/verification-callback-response/${id(1)}`, createdAt: at(0), expiresAt: at(300000),
    maxDurationSeconds: 180, billing: { intentId: id(1), eventId: `phone-verification:${id(1)}`, mandateId: id(2), unitPriceMicro: 100, freeSeconds: 0 } };
  const proof = { url: permit.answerUrl, signature: 'synthetic-answer', params: { AccountSid: permit.providerAccountSid,
    CallSid: sid, From: permit.from, To: permit.to, Direction: 'outbound-api', CallStatus: 'in-progress' } };
  const call = store.reserveVerificationCallback(permit, at(1)).call; assert.ok(call.verificationCallback);
  let journal = prepareVerificationCallbackDispatch(call.verificationCallback, id(3), at(2));
  journal = recordVerificationCallbackGrant(journal, { v: 1, state: 'dispatch_granted', dispatchAttemptId: id(3), permit });
  journal = fenceVerificationCallbackProvider(journal, at(3)); journal = prepareVerificationCallbackAnswer(journal, proof, at(4));
  journal = recordVerificationCallbackRemote(journal, { v: 1, request: ref, state: 'answered', dispatchAttemptId: id(3),
    callSid: sid, purpose, expiresAt: permit.expiresAt }); store.recordVerificationCallback(id(1), journal);
  let active = true, challenge: string | undefined = purpose === 'enrollment' ? '01234567' : undefined, audio = 40;
  const counts = { access: 0, dispatch: 0, complete: 0, recover: 0, hangup: 0, capture: 0 };
  const api: VerificationCallbackClient = {
    async prepareAccess() { counts.access++; throw new Error('Unexpected access'); },
    async recover() { counts.recover++; return { v: 1, request: ref, state: 'absent' }; },
    async dispatch() { counts.dispatch++; throw new Error('Unexpected dispatch'); },
    async answer() { throw new Error('Already answered'); },
    async complete(input) {
      counts.complete++; assert.ok(store.get(id(1))?.verificationCallback?.completion);
      const bytes = fs.readFileSync(path.join(directory, 'calls.json'), 'utf8');
      assert.equal(bytes.includes('01234567'), false); assert.equal(bytes.includes('Yes, complete'), false);
      if (input.purpose === 'enrollment') assert.equal(input.challenge, '01234567');
      return input.purpose === 'account_access'
        ? { v: 1, state: 'admitted', command: phoneVerificationCallbackCommandRef(input), admission: { id: id(9), digest: 'b'.repeat(64), expiresAt: at(60000) } }
        : { v: 1, state: 'rejected', command: phoneVerificationCallbackCommandRef(input), reason: 'challenge_invalid' };
    },
    async recoverCompletion(command) { counts.recover++; return { v: 1, state: 'absent', command }; },
    async cancel() { return { v: 1, request: ref, state: 'cancelled', cancelledAt: at(10) }; },
  };
  const flow = createVerificationCallbackFlow({ store, api, now, verifyAnswer: () => true, providerTimeoutMs: 1000,
    provider: { async create() { throw new Error('Unexpected provider contact'); }, async hangup(callSid) {
      counts.hangup++; return { accountSid: permit.providerAccountSid, from: permit.from, to: permit.to, callSid, status: 'completed' };
    } } });
  const scope = { callId: id(1), callSid: sid, sessionId: 'setup-session' }, controller = new AbortController();
  const request: LiveDelegationRequest = { callId: id(1), sessionId: scope.sessionId, requestId: 'request-1', delegationId: 'delegate-1', offsetMs: 30,
    transcript: { revision: 1, callerRevision: 1, fragments: [
      { eventId: 'caption-1', role: 'user', text: 'Yes, complete this verification.', startMs: 0, endMs: 20, sequence: 1 } ] } };
  const frontier = () => ({ sessionId: scope.sessionId, audioThroughMs: audio, observedSpeechAtMs: start - 50 });
  const freezer = createRestrictedCallerTurnFreezer({ callId: id(1), sessionId: scope.sessionId, mediaStartedAtMs: start - 100,
    deadlineAtMs: start + 60000, stabilityMs: 5, maxWaitMs: 100, frontier, isCurrent: () => active, subscribeSpeech: () => () => {} });
  const host = createRestrictedSetupHost({ store, api, flow, now, receiptSecret: 'synthetic-secret-longer-than-thirty-two-characters',
    isCurrent: value => active && value.callId === scope.callId && value.callSid === sid && value.sessionId === scope.sessionId,
    frontier, freeze: (_scope, source, channel, used) => freezer.freeze(source, channel, used),
    takeChallenge() { const result = challenge; challenge = undefined; return result; },
    async requestProtectedInput() { counts.capture++; }, isProtectedInputCurrent: () => active, stopOriginal() { active = false; } });
  const invoke = (tool: RestrictedSetupTool, toolCallId = 'function-1') => host.handleCaller({ ...scope, request, tool, toolCallId }, controller.signal,
    { getContext: () => structuredClone(request.transcript), subscribeContext: () => () => {} });
  return { store, api, host, flow, scope, request, proof, ref, counts, invoke,
    noChallenge() { challenge = undefined; }, supplyChallenge() { challenge = '01234567'; }, newAudio() { audio += 20; }, stop() { active = false; controller.abort(); } };
}
const complete = { name: 'complete_phone_setup', arguments: { accepted: true } } as const;

test('status and greetings create no completion, dispatch or account read', async t => {
  const f = fixture(t); assert.equal((await f.host.observe(f.scope, new AbortController().signal)).phase, 'verification_available');
  await f.invoke({ name: 'phone_setup_status', arguments: {} });
  assert.deepEqual(f.counts, { access: 0, dispatch: 0, complete: 0, recover: 0, hangup: 0, capture: 0 });
  assert.equal(f.store.get(f.scope.callId)?.primary, undefined);
});
test('named completion uses the actual context-free freezer and journals only the safe receipt', async t => {
  const f = fixture(t); assert.equal((await f.invoke(complete)).phase, 'unavailable');
  assert.equal(f.counts.complete, 1); assert.equal(f.counts.hangup, 1); assert.ok(f.store.get(f.scope.callId)?.verificationCallback?.completion);
  assert.equal(f.store.get(f.scope.callId)?.transcript, undefined); assert.equal(f.store.get(f.scope.callId)?.primary, undefined);
});
test('missing protected input opens capture only after explicit accepted source, without completion POST', async t => {
  const f = fixture(t); f.noChallenge(); assert.equal((await f.invoke(complete)).phase, 'verification_available');
  assert.equal(f.counts.capture, 1); assert.equal(f.counts.complete, 0);
  assert.equal((await f.invoke(complete, 'function-2')).phase, 'checking', 'same raw source cannot admit a second effect');
});
test('new acoustic input without a caption and stopped scope prevent completion', async t => {
  const f = fixture(t), pending = f.invoke(complete); f.newAudio(); await pending;
  assert.equal(f.counts.complete, 0); f.stop(); await assert.rejects(f.invoke(complete)); assert.equal(f.counts.complete, 0);
});
test('callback request cannot turn an enrollment callback into another outbound call', async t => {
  const f = fixture(t); assert.equal((await f.invoke({ name: 'request_verified_callback', arguments: { explicitlyRequested: true } })).phase, 'unavailable');
  assert.equal(f.counts.access, 0); assert.equal(f.counts.dispatch, 0);
});
test('private consumer requires admitted account_access and retired setup, then uses exact existing /calls proof only', async t => {
  const f = fixture(t, 'account_access'); assert.equal((await f.invoke(complete)).phase, 'complete');
  assert.equal(f.counts.hangup, 0); assert.equal(f.store.get(f.scope.callId)?.primary, undefined);
  let requests = 0;
  const opened: PrimaryCall = { bindingId: id(20), callSid: f.scope.callSid, state: 'open', context: {
    primaryDexterId: id(21), threadId: id(22), displayName: 'Dexter', messages: [] } };
  const primary = { async openCall(...args: Parameters<import('../src/broker/primaryApi.js').PrimaryApiClient['openCall']>) {
    requests++; assert.deepEqual(args, [f.proof]); return opened;
  } };
  assert.throws(() => f.flow.privateCall(f.ref, primary, () => false)); assert.equal(requests, 0);
  assert.deepEqual(await f.flow.privateCall(f.ref, primary, () => true), opened); assert.equal(requests, 1);
  assert.equal(f.store.get(f.scope.callId)?.primary?.bindingId, opened.bindingId);
  assert.equal(f.host.isCurrent(f.scope), false); assert.equal(f.store.get(f.scope.callId)?.callAssignment, undefined);
});
test('enrollment callback cannot use the private consumer', t => {
  const f = fixture(t); assert.throws(() => f.flow.privateCall(f.ref, { async openCall() { throw new Error('Forbidden'); } }, () => true));
});

test('accepted setup instruction survives protected capture without reusing generic delegation or persisting raw code', async t => {
  const f = fixture(t); f.noChallenge(); await f.invoke(complete); assert.equal(f.counts.complete, 0);
  f.supplyChallenge(); assert.equal(await f.host.completeProtectedInput(f.scope), 'unavailable');
  assert.equal(f.counts.complete, 1); assert.equal(f.counts.hangup, 1);
  await assert.rejects(f.host.completeProtectedInput(f.scope)); assert.equal(f.counts.complete, 1);
});
test('wrong protected attachment and abandoned source cannot complete setup', async t => {
  const f = fixture(t); f.noChallenge(); await f.invoke(complete); f.supplyChallenge();
  await assert.rejects(f.host.completeProtectedInput({ ...f.scope, sessionId: 'another-session' }));
  f.host.abandonProtectedInput(f.scope); await assert.rejects(f.host.completeProtectedInput(f.scope));
  assert.equal(f.counts.complete, 0);
});
