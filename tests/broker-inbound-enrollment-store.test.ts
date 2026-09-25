import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { BrokerStore, type BrokerCall } from '../src/broker/store.js';
import { inboundRequestId, validateInboundJournal } from '../src/broker/inboundJournal.js';
import { inboundBeginRef } from '../src/broker/inboundContract.js';
import { enrollmentCommand, type EnrollmentCompletionResult } from '../src/broker/inboundEnrollment.js';

const start = '2026-09-25T00:00:00.000Z';
const at = (ms: number) => new Date(Date.parse(start) + ms).toISOString();
const original = { url: 'https://broker.example.test/broker/incoming-call', signature: 'original-proof-retained',
  params: { AccountSid: 'AC' + 'a'.repeat(32), CallSid: 'CA' + 'b'.repeat(32), From: '+15555550101',
    To: '+15555550202', Direction: 'inbound', StirVerstat: 'TN-Validation-Passed-A' } };
const policy = { version: 1 as const, waitMs: 120_000, mediaGraceMs: 30_000 };
function fixture(t: test.TestContext, enrollment = true) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-enrollment-store-'));
  let store = new BrokerStore(directory);
  t.after(() => { store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  const input = { requestId: inboundRequestId(original.params.CallSid), purpose: 'The person called Dexter.', providerProof: original };
  const call = store.reserveInbound(input, policy, start, enrollment).call;
  const digitProof = { url: 'https://broker.example.test/broker/inbound/enrollment-response', signature: 'ephemeral-digit-signature',
    params: { AccountSid: original.params.AccountSid, CallSid: original.params.CallSid, From: original.params.From,
      To: original.params.To, Direction: 'inbound', Digits: '01234567' } };
  const command = enrollmentCommand({ begin: call.inbound!.begin, digitProof });
  const file = path.join(directory, 'calls.json');
  return { call, command, digitProof, file, input, get store() { return store; },
    restart() { store.close(); store = new BrokerStore(directory); return store.get(call.id)!; },
    dispatch() { return store.transitionInbound(call.id, { type: 'enrollment_dispatch', command, observedAt: at(1000) }); },
    continue() { return store.transitionInbound(call.id, { type: 'continuation', observedAt: at(1000),
      providerProof: { ...original, url: `https://broker.example.test/broker/inbound/${call.id}/continue` } }); } };
}

test('new opt-in reservation saves undecided; historical absent mode keeps exact bytes across read/replay', t => {
  const f = fixture(t, false); const bytes = fs.readFileSync(f.file);
  assert.equal(f.call.inbound!.mode, undefined); assert.deepEqual(f.restart(), f.call);
  const replay = f.store.reserveInbound(f.input, policy, at(99_000), true);
  assert.equal(replay.existing, true); assert.equal(replay.call.inbound!.mode, undefined);
  assert.deepEqual(fs.readFileSync(f.file), bytes);
  const newCall = { ...f.call, inbound: { ...f.call.inbound!, mode: { kind: 'undecided' as const } } };
  assert.throws(() => validateInboundJournal(newCall, f.call), /Historical inbound mode/);
});

test('mode and safe POST fence persist atomically; original proof and fixed deadline survive restart', t => {
  const f = fixture(t); assert.deepEqual(f.call.inbound!.mode, { kind: 'undecided' });
  const saved = f.dispatch(); const mode = saved.inbound!.mode;
  assert.equal(mode?.kind, 'enrollment-only');
  if (mode?.kind !== 'enrollment-only') throw new Error('missing enrollment mode');
  assert.equal(mode.dispatchStartedAt, mode.selectedAt); assert.deepEqual(mode.command, f.command);
  assert.deepEqual(saved.inbound!.begin.providerProof, original); assert.equal(saved.inbound!.waitUntil, at(120_000));
  assert.deepEqual(f.restart(), saved);
  const raw = fs.readFileSync(f.file, 'utf8');
  assert.equal(raw.includes('01234567'), false); assert.equal(raw.includes(f.digitProof.signature), false);
  assert.equal(raw.includes('original-proof-retained'), true);
  assert.throws(() => f.store.update(f.call.id, { inbound: f.call.inbound } as Partial<BrokerCall>), /explicit transition/);
});

test('ordinary continuation wins atomically; later enrollment refusal leaves exact ordinary bytes', t => {
  const f = fixture(t); const saved = f.continue(); const bytes = fs.readFileSync(f.file);
  assert.deepEqual(saved.inbound!.mode, { kind: 'ordinary', selectedAt: at(1000) });
  assert.throws(() => f.dispatch(), /Enrollment mode unavailable/);
  assert.deepEqual(fs.readFileSync(f.file), bytes); assert.deepEqual(f.restart(), saved);
});

test('enrollment winner cannot enter ordinary continuation, attachment, cancellation or media paths', t => {
  const f = fixture(t); const saved = f.dispatch(); const bytes = fs.readFileSync(f.file);
  assert.throws(() => f.continue(), /Enrollment leg/);
  for (const type of ['begin_attempted', 'consume_attempted', 'cancel_requested', 'close_requested'] as const)
    assert.throws(() => f.store.transitionInbound(f.call.id, { type, observedAt: at(2000) }));
  assert.throws(() => f.store.transitionInbound(f.call.id, { type: 'outcome', observedAt: at(2000),
    outcome: { state: 'absent', beginRef: inboundBeginRef(f.call.inbound!.begin) } }), /ordinary admission evidence/);
  assert.throws(() => f.store.update(f.call.id, { mediaStartedAt: at(2000) }), /media start/);
  assert.throws(() => f.store.update(f.call.id, { liveSessionId: 'unexpected-live-session' }));
  assert.deepEqual(fs.readFileSync(f.file), bytes); assert.deepEqual(f.restart(), saved);
});

test('absent remains observational; neither restart nor a fresh dispatch transition can clear its fence', t => {
  const f = fixture(t); f.dispatch();
  const saved = f.store.transitionInbound(f.call.id, { type: 'enrollment_observation', observedAt: at(2000),
    result: { v: 1, command: f.command, state: 'absent' } });
  assert.deepEqual(f.restart(), saved); assert.throws(() => f.dispatch(), /Enrollment mode unavailable/);
  const modified = structuredClone(saved);
  if (modified.inbound!.mode?.kind !== 'enrollment-only') throw new Error('missing enrollment mode');
  modified.inbound!.mode.dispatchStartedAt = at(3000);
  assert.throws(() => validateInboundJournal(modified, saved), /identity changed|fence/);
});

test('committed rejection and matching command are immutable; foreign echoes and raw extra fields refuse', t => {
  const f = fixture(t); f.dispatch();
  const result: EnrollmentCompletionResult = { v: 1, command: f.command, state: 'rejected', reason: 'challenge_unavailable' };
  const saved = f.store.transitionInbound(f.call.id, { type: 'enrollment_observation', result, observedAt: at(2000) });
  const bytes = fs.readFileSync(f.file);
  for (const next of [{ ...result, reason: 'expired' as const }, { v: 1 as const, command: f.command, state: 'absent' as const },
    { ...result, Digits: '01234567' }])
    assert.throws(() => f.store.transitionInbound(f.call.id, { type: 'enrollment_observation', result: next, observedAt: at(3000) }));
  assert.deepEqual(fs.readFileSync(f.file), bytes); assert.deepEqual(f.restart(), saved);
  const changed = enrollmentCommand({ begin: f.call.inbound!.begin,
    digitProof: { ...f.digitProof, signature: 'different-ephemeral-signature' } });
  assert.throws(() => f.store.transitionInbound(f.call.id, { type: 'enrollment_observation', observedAt: at(3000),
    result: { ...result, command: changed } }), /identity changed/);
});

test('enrollment local stop and hangup acknowledgment do not invent cancellation or provider terminal state', t => {
  const f = fixture(t); f.dispatch();
  const stopped = f.store.transitionInbound(f.call.id, { type: 'stop', reason: 'broker_cancelled', observedAt: at(120_000) });
  assert.equal(stopped.inbound!.cancelRequestedAt, undefined); assert.equal(stopped.status, 'prepared');
  f.store.transitionInbound(f.call.id, { type: 'enrollment_end_requested', observedAt: at(120_000) });
  const ended = f.store.transitionInbound(f.call.id, { type: 'enrollment_end_acknowledged', observedAt: at(120_001) });
  assert.equal(ended.endedAt, undefined); assert.equal(ended.status, 'prepared');
  assert.deepEqual(f.restart(), ended); assert.equal(ended.inbound!.waitUntil, at(120_000));
});

test('stopped or expired undecided source cannot be switched into enrollment', t => {
  const f = fixture(t);
  assert.throws(() => f.store.transitionInbound(f.call.id, { type: 'enrollment_dispatch', command: f.command,
    observedAt: at(120_000) }), /mode unavailable/);
  f.store.transitionInbound(f.call.id, { type: 'stop', reason: 'provider_terminal', observedAt: at(1000) });
  assert.throws(() => f.dispatch(), /mode unavailable/); assert.equal(f.restart().inbound!.mode?.kind, 'undecided');
});

test('raw digit proof cannot be persisted by ordinary continuation even when caller uses the old route', t => {
  const f = fixture(t); const bytes = fs.readFileSync(f.file);
  assert.throws(() => f.store.transitionInbound(f.call.id, { type: 'continuation', observedAt: at(1000),
    providerProof: { ...f.digitProof, url: `https://broker.example.test/broker/inbound/${f.call.id}/continue` } }), /Digit form/);
  assert.deepEqual(fs.readFileSync(f.file), bytes);
});

test('startup refuses malformed safe command, added raw digit fields and ordinary evidence on enrollment', t => {
  const f = fixture(t); const saved = f.dispatch(); const originalBytes = fs.readFileSync(f.file);
  f.store.close();
  for (const change of [
    (call: BrokerCall) => { const mode = call.inbound!.mode;
      if (mode?.kind === 'enrollment-only') mode.command.commandDigest = '0'.repeat(64); },
    (call: BrokerCall) => { Object.assign(call.inbound!.mode!, { digitProof: f.digitProof }); },
    (call: BrokerCall) => { call.inbound!.beginAttemptedAt = at(1000); },
  ]) {
    const value = structuredClone(saved); change(value);
    fs.writeFileSync(f.file, JSON.stringify({ version: 1, calls: { [value.id]: value } }));
    assert.throws(() => new BrokerStore(path.dirname(f.file)));
  }
  fs.writeFileSync(f.file, originalBytes); assert.deepEqual(f.restart(), saved);
});
