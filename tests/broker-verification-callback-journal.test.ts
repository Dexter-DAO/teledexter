import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { BrokerStore, type BrokerCall } from '../src/broker/store.js';
import { phoneVerificationCallbackPermitSchema, phoneVerificationCallbackCompletionSchema,
  phoneVerificationCallbackCommandRef, callbackProviderProofSchema,
  type PhoneVerificationCallbackPermit, type PhoneVerificationCallbackCompletion } from '../src/broker/verificationCallbackContract.js';
import { reserveVerificationCallback, prepareVerificationCallbackDispatch, recordVerificationCallbackGrant,
  fenceVerificationCallbackProvider, recordVerificationCallbackProvider, prepareVerificationCallbackAnswer,
  recordVerificationCallbackRemote, prepareVerificationCallbackCompletion, recordVerificationCallbackCompletion,
  stopVerificationCallback, endVerificationCallback, fenceVerificationCallbackEnd, parseVerificationCallbackJournal,
  validateVerificationCallbackUpdate } from '../src/broker/verificationCallbackJournal.js';
import { inboundBeginRef } from '../src/broker/inboundContract.js';
import { inboundRequestId } from '../src/broker/inboundJournal.js';
import { prepareRestrictedSetup, prepareRestrictedAccess, recordRestrictedAccessRefusal } from '../src/broker/restrictedSetupJournal.js';

const id = (n: number) => `00000000-0000-4000-8000-${n.toString().padStart(12, '0')}`;
const start = '2026-09-25T06:00:00.000Z';
const at = (ms: number) => new Date(Date.parse(start) + ms).toISOString();
const sid = 'CA' + 'b'.repeat(32), account = 'AC' + 'a'.repeat(32);
const ref = { requestId: id(1), requestDigest: 'c'.repeat(64) };
function permit(purpose: 'enrollment' | 'account_access' = 'enrollment'): PhoneVerificationCallbackPermit {
  return { v: 1, request: ref, purpose, providerAccountSid: account, from: '+15555550001', to: '+15555550002',
    answerUrl: `https://broker.example.test/broker/verification-callback/${id(1)}`,
    responseUrl: `https://broker.example.test/broker/verification-callback-response/${id(1)}`,
    createdAt: start, expiresAt: at(300_000), maxDurationSeconds: 180,
    billing: { intentId: id(1), eventId: `phone-verification:${id(1)}`, mandateId: id(2), unitPriceMicro: 1000, freeSeconds: 5 } };
}
function dispatched(purpose: 'enrollment' | 'account_access' = 'enrollment') {
  let j = reserveVerificationCallback(permit(purpose), at(1));
  j = prepareVerificationCallbackDispatch(j, id(3), at(2));
  j = recordVerificationCallbackGrant(j, { v: 1, state: 'dispatch_granted', dispatchAttemptId: id(3), permit: j.permit });
  return fenceVerificationCallbackProvider(j, at(3));
}
function proof() {
  return { url: permit().answerUrl, signature: 'signed-answer-only', params: { AccountSid: account, CallSid: sid,
    From: permit().from, To: permit().to, Direction: 'outbound-api', CallStatus: 'in-progress' } };
}
function answered(purpose: 'enrollment' | 'account_access' = 'enrollment') {
  const j = prepareVerificationCallbackAnswer(dispatched(purpose), proof(), at(4));
  return recordVerificationCallbackRemote(j, { v: 1, state: 'answered', request: ref,
    dispatchAttemptId: id(3), callSid: sid, purpose, expiresAt: at(300_000) });
}
function completion(purpose: 'enrollment' | 'account_access' = 'enrollment'): PhoneVerificationCallbackCompletion {
  const base = { v: 1 as const, request: ref, brokerCallId: id(1), callSid: sid,
    turn: { id: id(4), digest: 'd'.repeat(64) }, accepted: true as const };
  return purpose === 'enrollment' ? { ...base, purpose, challenge: '01234567' } : { ...base, purpose };
}
function storeFixture(t: test.TestContext) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-verification-callback-'));
  let store = new BrokerStore(directory); const file = path.join(directory, 'calls.json');
  t.after(() => { store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  return { get store() { return store; }, file,
    restart() { store.close(); store = new BrokerStore(directory); return store; } };
}

test('strict permit binds request, financial reservation, exact callback routes and finite duration', () => {
  assert.deepEqual(phoneVerificationCallbackPermitSchema.parse(permit()), permit());
  for (const mutate of [
    (p: PhoneVerificationCallbackPermit) => { p.to = 'caller-picked'; },
    (p: PhoneVerificationCallbackPermit) => { p.answerUrl += '?owner=1'; },
    (p: PhoneVerificationCallbackPermit) => { p.responseUrl = p.answerUrl; },
    (p: PhoneVerificationCallbackPermit) => { p.billing.intentId = id(99); },
    (p: PhoneVerificationCallbackPermit) => { p.expiresAt = at(300_001); },
  ]) { const value = permit(); mutate(value); assert.equal(phoneVerificationCallbackPermitSchema.safeParse(value).success, false); }
  assert.equal(phoneVerificationCallbackPermitSchema.safeParse({ ...permit(), ownerId: id(99) }).success, false);
});

test('generic callback proof accepts outbound form; answer reducer requires exact answered identity', () => {
  assert.deepEqual(callbackProviderProofSchema.parse(proof()), proof());
  for (const params of [ { CallStatus: 'ringing' }, { Direction: 'inbound' }, { AccountSid: 'AC' + 'c'.repeat(32) },
    { From: '+15555550999' }, { To: '+15555550999' }, { Digits: '01234567' }, { SpeechResult: 'code' } ])
    assert.throws(() => prepareVerificationCallbackAnswer(dispatched(), { ...proof(), params: { ...proof().params, ...params } }, at(4)));
});

test('provider fence requires exact API grant and prevents repeat contact after restart or absent recovery', () => {
  let j = reserveVerificationCallback(permit(), at(1));
  assert.throws(() => fenceVerificationCallbackProvider(j, at(2)));
  j = prepareVerificationCallbackDispatch(j, id(3), at(2));
  assert.throws(() => recordVerificationCallbackGrant(j, { v: 1, state: 'dispatch_granted', dispatchAttemptId: id(99), permit: permit() }));
  j = dispatched(); const restored = parseVerificationCallbackJournal(JSON.parse(JSON.stringify(j)));
  const absent = recordVerificationCallbackRemote(restored, { v: 1, state: 'absent', request: ref });
  assert.throws(() => fenceVerificationCallbackProvider(absent, at(7)));
  assert.throws(() => prepareVerificationCallbackDispatch(absent, id(3), at(7)));
  assert.equal(absent.dispatch?.providerStartedAt, at(3));
});

test('actual BrokerStore persists fence before any external work and keeps replay bytes after expiry', t => {
  const f = storeFixture(t); f.store.reserveVerificationCallback(permit(), at(1));
  const call = f.store.recordVerificationCallback(id(1), dispatched());
  const bytes = fs.readFileSync(f.file);
  assert.deepEqual(f.restart().get(id(1)), call);
  const replay = f.store.reserveVerificationCallback(permit(), at(900_000));
  assert.equal(replay.existing, true); assert.deepEqual(fs.readFileSync(f.file), bytes);
  assert.throws(() => f.store.update(id(1), { verificationCallback: undefined } as Partial<BrokerCall>), /explicit transition/);
  assert.throws(() => f.store.reserveVerificationCallback({ ...permit(), to: '+15555550999' }, at(9)));
});

test('actual store refuses operator collision and private transcript/account state on restricted callback', t => {
  const f = storeFixture(t);
  f.store.reserve({ requestId: id(1), direction: 'outbound', purpose: 'Existing task', number: permit().to });
  assert.throws(() => f.store.reserveVerificationCallback(permit(), at(1)));
  const other = permit(); other.request = { requestId: id(7), requestDigest: 'e'.repeat(64) };
  other.answerUrl = other.answerUrl.replace(id(1), id(7)); other.responseUrl = other.responseUrl.replace(id(1), id(7));
  other.billing.intentId = id(7); other.billing.eventId = `phone-verification:${id(7)}`;
  f.store.reserveVerificationCallback(other, at(1));
  assert.throws(() => f.store.update(id(7), { transcript: { text: '01234567' } }));
  assert.throws(() => f.store.update(id(7), { delegations: { unsafe: { requestId: id(9), status: 'prepared', updatedAt: at(2) } } }));
});

test('callback before create response preserves exact SID and first terminal despite later local observation', () => {
  const input = { accountSid: account, from: permit().from, to: permit().to, callSid: sid,
    status: 'completed' as const, source: 'signed_callback' as const, observedAt: at(20) };
  const ended = recordVerificationCallbackProvider(dispatched(), input);
  const late = recordVerificationCallbackProvider(ended, { ...input, status: 'queued', source: 'create', observedAt: at(21) });
  assert.deepEqual(late, ended);
  assert.throws(() => recordVerificationCallbackProvider(ended, { ...input, callSid: 'CA' + 'c'.repeat(32) }));
  assert.throws(() => recordVerificationCallbackProvider(ended, { ...input, accountSid: 'AC' + 'c'.repeat(32) }));
  assert.throws(() => recordVerificationCallbackProvider(ended, { ...input, to: '+15555550999' }));
});

test('local stop and hangup ACK are not provider-terminal evidence', () => {
  let j = stopVerificationCallback(answered(), at(10)); j = endVerificationCallback(j, at(10));
  j = fenceVerificationCallbackEnd(j, at(10));
  j = endVerificationCallback(j, at(11), true);
  assert.equal(j.providerTerminal, undefined); assert.equal(j.endAcknowledgedAt, at(11));
  assert.throws(() => prepareVerificationCallbackAnswer(j, proof(), at(12)));
});

test('answer proof and API answered receipt precede completion; envelope purpose cannot switch', () => {
  assert.throws(() => prepareVerificationCallbackCompletion(dispatched(), completion(), at(10)));
  const j = answered();
  assert.throws(() => prepareVerificationCallbackCompletion(j, completion('account_access'), at(10)));
  assert.equal(phoneVerificationCallbackCompletionSchema.safeParse({ ...completion('account_access'), challenge: '01234567' }).success, false);
  assert.throws(() => recordVerificationCallbackRemote(j, { v: 1, request: ref, state: 'answered',
    dispatchAttemptId: id(3), callSid: 'CA' + 'c'.repeat(32), purpose: 'enrollment', expiresAt: at(300_000) }));
});

test('completion command excludes challenge and unsafe fields; lost response only preserves GET identity', t => {
  const f = storeFixture(t); f.store.reserveVerificationCallback(permit(), at(1));
  let j = prepareVerificationCallbackCompletion(answered(), completion(), at(10));
  assert.ok(j.completion);
  j = recordVerificationCallbackCompletion(j, { v: 1, command: j.completion.command, state: 'absent' });
  f.store.recordVerificationCallback(id(1), j); const saved = f.restart().get(id(1));
  assert.equal(JSON.stringify(saved).includes('01234567'), false);
  assert.equal(saved?.verificationCallback?.completion?.command.commandDigest, phoneVerificationCallbackCommandRef(completion()).commandDigest);
  assert.throws(() => prepareVerificationCallbackCompletion(j, completion(), at(11)));
  assert.throws(() => parseVerificationCallbackJournal({ ...j, challenge: '01234567' }));
  const modified = structuredClone(j); Object.assign(modified.completion!, { transcript: 'secret' });
  assert.throws(() => parseVerificationCallbackJournal(modified));
});

test('committed completion is immutable and cannot change rejected to admitted or purpose', () => {
  let j = prepareVerificationCallbackCompletion(answered(), completion(), at(10)); assert.ok(j.completion);
  const command = j.completion.command;
  j = recordVerificationCallbackCompletion(j, { v: 1, command, state: 'rejected', reason: 'challenge_invalid' });
  assert.throws(() => recordVerificationCallbackCompletion(j, { v: 1, command, state: 'absent' }));
  assert.throws(() => recordVerificationCallbackCompletion(j, { v: 1, command, state: 'admitted',
    admission: { id: id(20), digest: 'f'.repeat(64), expiresAt: at(40) } }));
  const changed = structuredClone(j); changed.dispatch!.providerStartedAt = at(4);
  assert.throws(() => validateVerificationCallbackUpdate(j, changed));
});

test('expiry and original answer duration are fixed through replay', () => {
  const j = answered();
  assert.throws(() => prepareVerificationCallbackCompletion(j, completion(), at(180_004)));
  assert.throws(() => fenceVerificationCallbackProvider(recordVerificationCallbackGrant(
    prepareVerificationCallbackDispatch(reserveVerificationCallback(permit(), at(1)), id(3), at(2)),
    { v: 1, state: 'dispatch_granted', dispatchAttemptId: id(3), permit: permit() }), at(300_000)));
});

function refusedIncoming(f: ReturnType<typeof storeFixture>) {
  const params = { AccountSid: account, CallSid: 'CA' + 'd'.repeat(32), From: '+15555550002', To: '+15555550001',
    Direction: 'inbound', StirVerstat: 'TN-Validation-Passed-C' };
  let call = f.store.reserveInbound({ requestId: inboundRequestId(params.CallSid), purpose: 'The person called Dexter.',
    providerProof: { url: 'https://broker.example.test/broker/incoming-call', signature: 'original-signed-proof', params } },
    { version: 1, waitMs: 120_000, mediaGraceMs: 30_000 }, start).call;
  call = f.store.transitionInbound(call.id, { type: 'continuation', observedAt: at(1), providerProof: {
    ...call.inbound!.begin.providerProof, url: `https://broker.example.test/broker/inbound/${call.id}/continue` } });
  call = f.store.transitionInbound(call.id, { type: 'begin_attempted', observedAt: at(2) });
  call = f.store.transitionInbound(call.id, { type: 'admission_refused', code: 'inbound_carrier_attestation_required', observedAt: at(3) });
  return call;
}
test('finite 403 alone cannot start setup; exact settled cancellation enables additive disposition', t => {
  const f = storeFixture(t); let call = refusedIncoming(f);
  assert.throws(() => prepareRestrictedSetup(call, at(10), at(180_000)));
  call = f.store.transitionInbound(call.id, { type: 'outcome', observedAt: at(4), outcome: {
    attachmentId: id(30), state: 'cancelled', beginRef: inboundBeginRef(call.inbound!.begin), cancelledAt: at(4), expiresAt: null } });
  const before = structuredClone(call.inbound);
  call = f.store.recordRestrictedSetup(call.id, prepareRestrictedSetup(call, at(10), at(180_000)));
  assert.deepEqual(call.inbound, before); assert.ok(call.inbound?.localStop);
  const recovered = f.restart().get(call.id); assert.deepEqual(recovered?.restrictedSetup, call.restrictedSetup);
  assert.throws(() => f.store.update(call.id, { mediaStartedAt: at(11) }), /media start/);
  assert.throws(() => f.store.update(call.id, { transcript: { text: 'private or code' } }));
});

test('access request fence is bound to original proof; replay cannot create a second callback operation', t => {
  const f = storeFixture(t); let call = refusedIncoming(f);
  call = f.store.transitionInbound(call.id, { type: 'outcome', observedAt: at(4), outcome: {
    attachmentId: id(30), state: 'cancelled', beginRef: inboundBeginRef(call.inbound!.begin), cancelledAt: at(4), expiresAt: null } });
  call = f.store.recordRestrictedSetup(call.id, prepareRestrictedSetup(call, at(10), at(180_000)));
  const request = { v: 1 as const, requestId: id(40), original: call.inbound!.begin, explicitlyRequested: true as const };
  const saved = f.store.recordRestrictedSetup(call.id, prepareRestrictedAccess(call, request, at(11)));
  assert.equal(saved.restrictedSetup?.access?.request.requestId, id(40));
  assert.throws(() => prepareRestrictedAccess(saved, request, at(12)));
  assert.throws(() => f.store.recordRestrictedSetup(call.id, { ...saved.restrictedSetup!, access: undefined }));
});

test('finite consent or finance refusal stays durable without clearing the access POST fence', t => {
  const f = storeFixture(t); let call = refusedIncoming(f);
  call = f.store.transitionInbound(call.id, { type: 'outcome', observedAt: at(4), outcome: {
    attachmentId: id(30), state: 'cancelled', beginRef: inboundBeginRef(call.inbound!.begin), cancelledAt: at(4), expiresAt: null } });
  call = f.store.recordRestrictedSetup(call.id, prepareRestrictedSetup(call, at(10), at(180000)));
  const request = { v: 1 as const, requestId: id(40), original: call.inbound!.begin, explicitlyRequested: true as const };
  call = f.store.recordRestrictedSetup(call.id, prepareRestrictedAccess(call, request, at(11)));
  call = f.store.recordRestrictedSetup(call.id, recordRestrictedAccessRefusal(call, {
    observedAt: at(12), status: 409, code: 'phone_tab_required', retryable: false }));
  assert.equal(f.restart().get(call.id)?.restrictedSetup?.access?.refusal?.code, 'phone_tab_required');
  assert.throws(() => prepareRestrictedAccess(call, request, at(13)));
  assert.throws(() => f.store.recordRestrictedSetup(call.id, { ...call.restrictedSetup!, access: {
    ...call.restrictedSetup!.access!, refusal: undefined } }));
});
