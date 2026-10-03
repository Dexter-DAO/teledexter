import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import Twilio from 'twilio';
import { BrokerStore } from '../src/broker/store.js';
import { createInboundFlow } from '../src/broker/inbound.js';
import { PrimaryApiError, type PrimaryApiClient } from '../src/broker/primaryApi.js';
import { inboundBeginRef, type InboundOutcome } from '../src/broker/inboundContract.js';
import { enrollmentCommand, type EnrollmentCommand, type EnrollmentCompletionInput,
  type EnrollmentCompletionResult, type EnrollmentCompletionRecovery } from '../src/broker/inboundEnrollment.js';

function deferred<T>() {
  let resolve: (value: T) => void = () => { throw new Error('uninitialized fixture'); };
  const promise = new Promise<T>(r => { resolve = r; });
  return { promise, resolve };
}
async function bounded<T>(promise: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try { return await Promise.race([promise, new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error('fixture handshake exceeded2s')), 2000);
  })]); } finally { if (timer) clearTimeout(timer); }
}
const id = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
function activated(command: EnrollmentCommand): EnrollmentCompletionResult {
  return { v: 1, command, state: 'activated', receipt: { enrollmentId: id, operationId: id,
    requestDigest: '1'.repeat(64), state: 'activated', activatedAt: '2026-09-25T00:00:01.000Z',
    enrollmentRef: { id, revision: 1, digest: '2'.repeat(64) }, possessionRef: { id, digest: '3'.repeat(64) },
    standingConsentRef: { id, revision: 1, digest: '4'.repeat(64) } } };
}
function fixture(t: test.TestContext, enabled = true) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-enrollment-flow-'));
  let store = new BrokerStore(directory); let clock = Date.parse('2026-09-25T00:00:00.000Z');
  const config = { twilioAccountSid: 'AC' + 'a'.repeat(32), twilioAuthToken: 'synthetic-provider-token',
    publicOrigin: 'https://broker.example.test', from: '+15555550202' };
  const params: Record<string, string> = { AccountSid: config.twilioAccountSid, CallSid: 'CA' + 'b'.repeat(32),
    Direction: 'inbound', From: '+15555550101', To: config.from, StirVerstat: 'TN-Validation-Passed-A' };
  function proof(route: string, values = params) {
    const url = config.publicOrigin + route;
    return { url, params: { ...values }, signature: Twilio.getExpectedTwilioSignature(config.twilioAuthToken, url, values) };
  }
  const original = proof('/broker/incoming-call');
  const digit = (digits = '01234567') => proof('/broker/inbound/enrollment-response', {
    AccountSid: params.AccountSid, CallSid: params.CallSid, Direction: 'inbound', From: params.From, To: params.To, Digits: digits });
  const counts = { post: 0, get: 0, end: 0, begin: 0, recover: 0, status: 0, cancel: 0, consume: 0, context: 0, provider: 0, close: 0 };
  const completions = new Map<string, EnrollmentCompletionResult>();
  let complete: (input: EnrollmentCompletionInput) => Promise<EnrollmentCompletionResult> = async input => {
    const result: EnrollmentCompletionResult = { v: 1, command: enrollmentCommand(input), state: 'rejected', reason: 'challenge_unavailable' };
    completions.set(result.command.commandDigest, result); return result;
  };
  let recover: (command: EnrollmentCommand) => Promise<EnrollmentCompletionRecovery> = async command =>
    completions.get(command.commandDigest) ?? { v: 1, command, state: 'absent' };
  const releases: (() => void)[] = [];
  let beginGate: Promise<void> | undefined;
  const beginEntered = deferred<void>();
  const unexpected = async (): Promise<never> => { throw new Error('unexpected ordinary API method'); };
  const primary: PrimaryApiClient = {
    readiness: unexpected, openCall: unexpected, submit: unexpected, result: unexpected, reconcile: unexpected,
    question: unexpected, reply: unexpected, captureTurn: unexpected, close: unexpected,
    async getContext() { counts.context++; return unexpected(); },
    async beginInbound(begin) {
      counts.begin++; beginEntered.resolve(); if (beginGate) await beginGate;
      const remote: InboundOutcome = { state: 'pending', attachmentId: id, beginRef: inboundBeginRef(begin),
        expiresAt: new Date(clock + 300_000).toISOString() }; return remote;
    },
    async recoverInbound(begin) { counts.recover++; return { state: 'absent', beginRef: inboundBeginRef(begin) }; },
    async inboundStatus() { counts.status++; return unexpected(); },
    async cancelInbound(begin) { counts.cancel++; return { state: 'cancelled', attachmentId: id,
      beginRef: inboundBeginRef(begin), cancelledAt: new Date(clock).toISOString(), expiresAt: null }; },
    async openInboundCall() { counts.consume++; return unexpected(); },
    async completeInboundEnrollment(input) {
      counts.post++;
      const saved = store.get(input.begin.brokerCallId)!;
      assert.equal(saved.inbound?.mode?.kind, 'enrollment-only');
      assert.equal(saved.inbound?.beginAttemptedAt, undefined);
      assert.ok(fs.readFileSync(path.join(directory, 'calls.json'), 'utf8').includes(enrollmentCommand(input).commandDigest));
      return complete(input);
    },
    async recoverInboundEnrollment(command) { counts.get++; return recover(command); },
  };
  function build() {
    return createInboundFlow({ config, store, primary, now: () => clock,
      async fetchCall() { counts.provider++; return { sid: params.CallSid, accountSid: params.AccountSid,
        from: params.From, to: params.To, direction: 'inbound', status: 'in-progress' }; },
      async closeBound() { counts.close++; throw new Error('unexpected binding close'); },
      ...(enabled ? { enrollment: { async endLeg() { counts.end++; } } } : {}) });
  }
  let flow = build();
  t.after(async () => { for (const release of releases) release(); await flow.drain(); store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  return { counts, original, digit, proof, primary, completions, directory,
    get store() { return store; }, get flow() { return flow; },
    async reserve() { return flow.accept(original); },
    at: () => new Date(clock).toISOString(), advance: (ms: number) => { clock += ms; },
    setComplete(next: typeof complete) { complete = next; }, setRecover(next: typeof recover) { recover = next; },
    async restart() { await flow.drain(); store.close(); store = new BrokerStore(directory); flow = build(); },
    holdBegin() { const gate = deferred<void>(); beginGate = gate.promise; releases.push(() => gate.resolve());
      return { entered: beginEntered.promise, release: () => gate.resolve() }; },
    holdPost() { const gate = deferred<void>(); const entered = deferred<void>(); releases.push(() => gate.resolve());
      complete = async input => { entered.resolve(); await gate.promise; const result: EnrollmentCompletionResult = {
        v: 1, command: enrollmentCommand(input), state: 'rejected', reason: 'challenge_unavailable' };
        completions.set(result.command.commandDigest, result); return result; };
      return { entered: entered.promise, release: () => gate.resolve() }; },
    continue(id: string) { return flow.resume(id, proof(`/broker/inbound/${id}/continue`)); },
  };
}
function noOrdinary(counts: ReturnType<typeof fixture>['counts']) {
  for (const field of ['begin', 'recover', 'status', 'cancel', 'consume', 'context', 'provider', 'close'] as const)
    assert.equal(counts[field], 0, field);
}

test('opt-in reservation, replay, maintenance and restart remain undecided until an explicit continuation', async t => {
  const f = fixture(t); const call = await f.reserve(); const bytes = fs.readFileSync(path.join(f.directory, 'calls.json'));
  assert.deepEqual(call.inbound?.mode, { kind: 'undecided' });
  await f.flow.tick(call.id); await f.reserve(); await f.restart(); await f.flow.tick(call.id);
  noOrdinary(f.counts); assert.equal(f.counts.post, 0);
  assert.deepEqual(fs.readFileSync(path.join(f.directory, 'calls.json')), bytes);
  await f.continue(call.id); assert.equal(f.counts.begin, 1); assert.equal(f.store.get(call.id)?.inbound?.mode?.kind, 'ordinary');
});

test('historical absent mode stays ordinary with no enrollment creation on replay', async t => {
  const f = fixture(t, false); const call = await f.reserve();
  assert.equal(call.inbound?.mode, undefined); await f.continue(call.id); assert.equal(f.counts.begin, 1);
  const before = f.store.get(call.id);
  await assert.rejects(f.flow.completeEnrollment(call.id, f.digit()), /mode_conflict/);
  assert.deepEqual(f.store.get(call.id), before); assert.equal(f.counts.end, 0); assert.equal(f.counts.post, 0);
});

test('ordinary begin wins while in flight; late valid enrollment input neither switches nor hangs up', async t => {
  const f = fixture(t); const call = await f.reserve(); const gate = f.holdBegin();
  const ordinary = f.continue(call.id); await bounded(gate.entered);
  const late = assert.rejects(f.flow.completeEnrollment(call.id, f.digit()), /mode_conflict/);
  assert.equal(f.store.get(call.id)?.inbound?.mode?.kind, 'ordinary'); assert.equal(f.counts.end, 0);
  gate.release(); await bounded(ordinary); await late;
  assert.equal(f.counts.post, 0); assert.equal(f.counts.end, 0); assert.equal(f.store.get(call.id)?.inbound?.localStop, undefined);
});

test('enrollment fence wins before POST; concurrent continuation and tick cannot enter ordinary API', async t => {
  const f = fixture(t); const call = await f.reserve(); const gate = f.holdPost();
  const enrollment = f.flow.completeEnrollment(call.id, f.digit()); await bounded(gate.entered);
  const ordinary = f.continue(call.id); const tick = f.flow.tick(call.id);
  gate.release(); await Promise.all([enrollment, ordinary, tick]); noOrdinary(f.counts);
  assert.equal(f.counts.post, 1); assert.equal(f.counts.get, 0); assert.equal(f.flow.mediaAllowed(f.store.get(call.id)!), false);
  const raw = fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8');
  assert.equal(raw.includes('01234567'), false); assert.equal(raw.includes(f.digit().signature), false);
});

test('activated response ends the enrollment leg without inventing provider terminal status or Primary context', async t => {
  const f = fixture(t); const call = await f.reserve();
  f.setComplete(async input => activated(enrollmentCommand(input)));
  const result = await f.flow.completeEnrollment(call.id, f.digit());
  noOrdinary(f.counts); assert.equal(f.counts.end, 1); assert.equal(result.inbound?.localStop?.reason, 'enrollment_activated');
  assert.equal(result.status, 'prepared'); assert.equal(result.endedAt, undefined); assert.equal(result.primary, undefined);
  assert.equal(result.inbound?.cancelRequestedAt, undefined);
  await f.restart(); await f.flow.tick(call.id); assert.equal(f.counts.post, 1); assert.equal(f.counts.end, 1);
});

test('lost completion response recovers immutable activation by exact GET after restart', async t => {
  const f = fixture(t); const call = await f.reserve();
  f.setComplete(async input => { const result = activated(enrollmentCommand(input));
    f.completions.set(result.command.commandDigest, result); throw new Error('lost HTTP'); });
  await assert.rejects(f.flow.completeEnrollment(call.id, f.digit()), /completion_unavailable/);
  await f.restart(); const recovered = await f.flow.tick(call.id);
  assert.equal(recovered.inbound?.mode?.kind, 'enrollment-only'); assert.equal(f.counts.get, 1);
  assert.equal(f.counts.post, 1); assert.equal(f.counts.end, 1); noOrdinary(f.counts);
  await f.flow.completeEnrollment(call.id, f.digit()); assert.equal(f.counts.post, 1);
});

test('absent or unavailable GET keeps the fence across retries and restart with no repeated POST', async t => {
  const f = fixture(t); const call = await f.reserve(); f.setComplete(async () => { throw new Error('unknown dispatch'); });
  await assert.rejects(f.flow.completeEnrollment(call.id, f.digit()), /completion_unavailable/);
  const mode = f.store.get(call.id)!.inbound!.mode;
  await f.flow.tick(call.id); await f.restart(); await f.flow.completeEnrollment(call.id, f.digit());
  f.setRecover(async () => { throw new PrimaryApiError('http_error', 503, { code: 'inbound_enrollment_unavailable', retryable: true }); });
  await assert.rejects(f.flow.tick(call.id), /recovery_unavailable/);
  assert.equal(f.counts.post, 1); assert.equal(f.counts.get, 3); noOrdinary(f.counts);
  const latest = f.store.get(call.id)!.inbound!.mode;
  assert.equal(latest?.kind, 'enrollment-only');
  if (latest?.kind !== 'enrollment-only' || mode?.kind !== 'enrollment-only') throw new Error('missing fence');
  assert.equal(latest.dispatchStartedAt, mode.dispatchStartedAt); assert.deepEqual(latest.command, mode.command);
});

test('restart after durable fence but before any HTTP still uses only GET on absent', async t => {
  const f = fixture(t); const call = await f.reserve();
  const command = enrollmentCommand({ begin: call.inbound!.begin, digitProof: f.digit() });
  f.store.transitionInbound(call.id, { type: 'enrollment_dispatch', command, observedAt: f.at() });
  await f.restart(); await f.flow.tick(call.id); await f.flow.completeEnrollment(call.id, f.digit());
  assert.equal(f.counts.post, 0); assert.equal(f.counts.get, 2); noOrdinary(f.counts);
});

test('a different signed code cannot overwrite the saved command or create another completion POST', async t => {
  const f = fixture(t); const call = await f.reserve(); await f.flow.completeEnrollment(call.id, f.digit());
  const bytes = fs.readFileSync(path.join(f.directory, 'calls.json'));
  await assert.rejects(f.flow.completeEnrollment(call.id, f.digit('76543210')), /command_conflict/);
  assert.equal(f.counts.post, 1); assert.deepEqual(fs.readFileSync(path.join(f.directory, 'calls.json')), bytes);
});

test('stop contacts end-leg independently of a held completion POST, then recovers without ordinary cancellation', async t => {
  const f = fixture(t); const call = await f.reserve(); const gate = f.holdPost();
  const post = f.flow.completeEnrollment(call.id, f.digit()); await bounded(gate.entered);
  const stopped = f.flow.stop(call.id, 'provider_terminal');
  assert.equal(f.counts.end, 1); assert.equal(f.counts.cancel, 0);
  assert.equal(f.store.get(call.id)?.inbound?.localStop?.reason, 'provider_terminal');
  gate.release(); await bounded(Promise.all([post, stopped])); noOrdinary(f.counts); assert.equal(f.counts.end, 1);
});

test('enrollment deadline is the original120s and ends the leg before an unavailable recovery', async t => {
  const f = fixture(t); const call = await f.reserve(); f.setComplete(async () => { throw new Error('unknown'); });
  f.advance(119_000); await assert.rejects(f.flow.completeEnrollment(call.id, f.digit()));
  f.advance(1000); f.setRecover(async () => { assert.equal(f.counts.end, 1); throw new Error('unavailable'); });
  await assert.rejects(f.flow.tick(call.id), /recovery_unavailable/);
  assert.equal(f.store.get(call.id)?.inbound?.waitUntil, call.inbound?.waitUntil);
  assert.equal(f.store.get(call.id)?.inbound?.localStop?.reason, 'broker_cancelled'); noOrdinary(f.counts);
});

test('exact finite403 completion refusal persists safe transport classification and ends without normal cancel', async t => {
  const f = fixture(t); const call = await f.reserve();
  f.setComplete(async () => { throw new PrimaryApiError('http_error', 403, { code: 'inbound_carrier_attestation_required', retryable: false }); });
  await assert.rejects(f.flow.completeEnrollment(call.id, f.digit()), /completion_unavailable/);
  const mode = f.store.get(call.id)?.inbound?.mode; assert.equal(mode?.kind, 'enrollment-only');
  if (mode?.kind !== 'enrollment-only') throw new Error('missing mode');
  assert.equal(mode.transportError?.code, 'inbound_carrier_attestation_required'); assert.equal(mode.observation, undefined);
  assert.equal(f.counts.end, 1); await f.restart(); await f.flow.tick(call.id);
  assert.equal(f.counts.post, 1); noOrdinary(f.counts);
});

test('invalid same-leg/signature and raw Digits on continue refuse before mode mutation or contact', async t => {
  const f = fixture(t); const call = await f.reserve(); const bytes = fs.readFileSync(path.join(f.directory, 'calls.json'));
  assert.throws(() => f.flow.completeEnrollment(call.id, { ...f.digit(), signature: 'wrong' }), /identity_mismatch/);
  const foreign = f.digit(); foreign.params.From = '+15555550303';
  assert.throws(() => f.flow.completeEnrollment(call.id, foreign), /identity_mismatch/);
  const misplaced = f.proof(`/broker/inbound/${call.id}/continue`, { ...f.original.params, Digits: '01234567' });
  assert.throws(() => f.flow.resume(call.id, misplaced), /route_mismatch/);
  assert.deepEqual(fs.readFileSync(path.join(f.directory, 'calls.json')), bytes); noOrdinary(f.counts); assert.equal(f.counts.post, 0);
});

test('failure to persist the dispatch fence prevents provider completion contact', async t => {
  const f = fixture(t); const call = await f.reserve(); f.store.close();
  await assert.rejects(f.flow.completeEnrollment(call.id, f.digit()), /store is closed/);
  assert.equal(f.counts.post, 0); assert.equal(f.counts.end, 0);
  // The fixture reopens only the unchanged durable reservation; no dispatch was attempted.
  await f.restart(); assert.deepEqual(f.store.get(call.id)?.inbound?.mode, { kind: 'undecided' });
});


test('after-response early admission stays excluded from undecided and enrollment-only legs', async t => {
  const f = fixture(t), call = await f.reserve();
  await f.flow.beginEarly(call.id); noOrdinary(f.counts);
  assert.equal(f.store.get(call.id)!.inbound!.earlyAdmission, undefined);
  await f.flow.completeEnrollment(call.id, f.digit());
  assert.equal(f.store.get(call.id)!.inbound!.mode?.kind, 'enrollment-only');
  await f.flow.beginEarly(call.id); noOrdinary(f.counts);
  assert.equal(f.store.get(call.id)!.inbound!.earlyAdmission, undefined);
});
