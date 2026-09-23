import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { BrokerStore, type BrokerCall } from '../src/broker/store.js';
import { inboundRequestId, inboundBeginBodyDigest, type InboundJournalEvent } from '../src/broker/inboundJournal.js';
import { inboundBeginRef, type InboundOutcome, type ConsumedInboundClosureReceipt } from '../src/broker/inboundContract.js';
import type { PrimaryCall } from '../src/broker/primaryApi.js';

const start = '2026-09-23T22:00:00.000Z';
const after = (ms: number) => new Date(Date.parse(start) + ms).toISOString();
const sid = `CA${'1'.repeat(32)}`;
const proof = { url: 'https://phone.dexter.cash/broker/incoming-call', signature: 'saved-original-signature',
  params: { AccountSid: `AC${'2'.repeat(32)}`, CallSid: sid, Direction: 'inbound', From: '+12025550123', To: '+12025550124', CallStatus: 'ringing' } };
const input = { requestId: inboundRequestId(sid), purpose: 'The person called Dexter.', providerProof: proof };
const policy = { version: 1 as const, waitMs: 120_000, mediaGraceMs: 30_000 };
const attachmentId = '11111111-1111-4111-8111-111111111111';
const approvalRef = { attachmentId, approvalId: '22222222-2222-4222-8222-222222222222', digest: 'a'.repeat(64) };
const bindingId = '33333333-3333-4333-8333-333333333333';

function fixture(t: test.TestContext) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-inbound-store-'));
  let store = new BrokerStore(directory);
  const file = path.join(directory, 'calls.json');
  t.after(() => { store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  const call = store.reserveInbound(input, policy, start).call;
  const beginRef = inboundBeginRef(call.inbound!.begin);
  const pending: InboundOutcome = { state: 'pending', attachmentId, beginRef, expiresAt: after(100_000) };
  const approved: InboundOutcome = { ...pending, state: 'approved', approvalRef };
  const bound: InboundOutcome = { state: 'bound', attachmentId, beginRef, approvalRef, bindingId,
    consumedAt: after(10_000), callState: 'open' };
  const closure: ConsumedInboundClosureReceipt = { attachmentId, beginRef, approvalRef, bindingId, callSid: sid,
    state: 'closed', closedAt: after(20_000) };
  const context: PrimaryCall = { bindingId, callSid: sid, state: 'open', context: {
    primaryDexterId: '44444444-4444-4444-8444-444444444444', threadId: '55555555-5555-4555-8555-555555555555',
    displayName: 'Dexter', messages: [{ id: 'm1', role: 'user', text: 'Previous conversation' }] } };
  return { directory, file, call, beginRef, pending, approved, bound, closure, context,
    get store() { return store; },
    send(event: InboundJournalEvent) { return store.transitionInbound(call.id, event); },
    restart() { store.close(); store = new BrokerStore(directory); return store.get(call.id)!; },
    approve() { store.transitionInbound(call.id, { type: 'begin_attempted', observedAt: start });
      store.transitionInbound(call.id, { type: 'outcome', outcome: approved, observedAt: after(1000) }); },
    bind() { this.approve(); this.send({ type: 'consume_attempted', observedAt: after(2000) });
      return this.send({ type: 'outcome', outcome: bound, observedAt: after(11_000) }); } };
}

test('reservation atomically preserves original proof, original request hash, caller and fixed budgets', t => {
  const f = fixture(t); const saved = f.call;
  assert.equal(saved.requestId, input.requestId); assert.equal(saved.number, proof.params.From);
  assert.equal(saved.callSid, sid); assert.equal(saved.direction, 'inbound');
  assert.equal(saved.inbound!.begin.brokerCallId, saved.id);
  assert.deepEqual(saved.inbound!.begin.providerProof, proof);
  assert.deepEqual(saved.inbound!.policy, policy); assert.equal(saved.inbound!.waitUntil, after(120_000));
  assert.equal(saved.primary, undefined); assert.equal(saved.primaryClosed, undefined);
  const bytes = fs.readFileSync(f.file, 'utf8');
  assert.deepEqual(f.restart(), saved);
  assert.deepEqual(f.store.reserveInbound(input, { ...policy, waitMs: 600_000, mediaGraceMs: 60_000 }, after(400_000)), { call: saved, existing: true });
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
});

test('repeated original webhook cannot change proof, caller, target, purpose or request ID', t => {
  const f = fixture(t); const bytes = fs.readFileSync(f.file, 'utf8');
  for (const key of ['From', 'To', 'AccountSid', 'Direction', 'CallStatus'] as const) {
    const changed = { ...input, providerProof: { ...proof, params: { ...proof.params, [key]: `${proof.params[key]}x` } } };
    assert.throws(() => f.store.reserveInbound(changed, policy, start));
  }
  for (const changed of [{ ...input, purpose: 'Other task' }, { ...input, requestId: 'c'.repeat(64) },
    { ...input, providerProof: { ...proof, signature: 'new signature' } },
    { ...input, providerProof: { ...proof, url: 'https://phone.dexter.cash/broker/other' } }])
    assert.throws(() => f.store.reserveInbound(changed, policy, start));
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
});

test('protocol-equivalent proof does not replace the saved raw envelope', t => {
  const f = fixture(t); const bytes = fs.readFileSync(f.file, 'utf8');
  const original = f.call.inbound!.begin;
  const equivalent = { ...original, brokerCallId: original.brokerCallId.toUpperCase(),
    providerProof: { ...proof, signature: 'different-signature', params: Object.fromEntries(Object.entries(proof.params).reverse()) } };
  assert.deepEqual(inboundBeginRef(equivalent), inboundBeginRef(original));
  assert.notEqual(inboundBeginBodyDigest(equivalent), inboundBeginBodyDigest(original));
  assert.throws(() => f.store.reserveInbound({ ...input, providerProof: equivalent.providerProof }, policy, start), /conflicts/);
  assert.deepEqual(f.store.reserveInbound({ ...input, providerProof: { ...proof, params: equivalent.providerProof.params } }, policy, start),
    { call: f.call, existing: true });
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  assert.deepEqual(f.restart().inbound!.begin, original);
});

test('the first remote response must carry the exact shared begin digest for the original signed parameters', t => {
  const f = fixture(t); const bytes = fs.readFileSync(f.file, 'utf8');
  const wrongBeginRef = { ...f.beginRef, digest: 'c'.repeat(64) };
  for (const outcome of [{ state: 'absent' as const, beginRef: wrongBeginRef },
    { ...f.pending, beginRef: wrongBeginRef }, { ...f.bound, beginRef: wrongBeginRef }]) {
    assert.throws(() => f.send({ type: 'outcome', outcome }), /call identity mismatch/);
  }
  const otherProofRef = inboundBeginRef({ ...f.call.inbound!.begin,
    providerProof: { ...proof, params: { ...proof.params, CallStatus: 'in-progress' } } });
  assert.throws(() => f.send({ type: 'outcome', outcome: { ...f.pending, beginRef: otherProofRef } }), /call identity mismatch/);
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  assert.deepEqual(f.send({ type: 'outcome', outcome: f.pending }).inbound!.remote, f.pending);
});

test('an existing callback or operator record cannot become a new inbound attachment', t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-inbound-legacy-'));
  const store = new BrokerStore(directory); t.after(() => { store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  const call = store.reserve({ requestId: input.requestId, direction: 'inbound', purpose: input.purpose, number: proof.params.From, callSid: sid }).call;
  store.update(call.id, { callback: { requestId: 'saved-callback', consentedAt: start, state: 'requested' } });
  const bytes = fs.readFileSync(path.join(directory, 'calls.json'), 'utf8');
  assert.throws(() => store.reserveInbound(input, policy, start), /conflicts/);
  assert.equal(fs.readFileSync(path.join(directory, 'calls.json'), 'utf8'), bytes);
});

test('begin intent survives a lost response and exact remote outcome cannot renew expiry', t => {
  const f = fixture(t); f.send({ type: 'begin_attempted', observedAt: start }); f.restart();
  f.send({ type: 'outcome', outcome: { state: 'absent', beginRef: f.beginRef }, observedAt: after(1000) });
  f.send({ type: 'outcome', outcome: f.pending, observedAt: after(2000) });
  const saved = f.restart(); const bytes = fs.readFileSync(f.file, 'utf8');
  assert.equal(saved.inbound!.beginAttemptedAt, start);
  assert.deepEqual(f.send({ type: 'outcome', outcome: f.pending, observedAt: after(3000) }), saved);
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  assert.throws(() => f.send({ type: 'outcome', outcome: { ...f.pending, expiresAt: after(110_000) } }), /deadline changed/);
  assert.throws(() => f.send({ type: 'outcome', outcome: { state: 'absent', beginRef: f.beginRef } }), /cannot disappear/);
});

test('local stop before begin durably requests cancellation and absent never settles it', t => {
  const f = fixture(t);
  const stopped = f.send({ type: 'stop', reason: 'provider_terminal', observedAt: after(1000) });
  assert.equal(stopped.inbound!.cancelRequestedAt, after(1000));
  assert.equal(stopped.inbound!.remote, undefined); assert.equal(stopped.status, 'prepared');
  f.restart();
  f.send({ type: 'outcome', outcome: { state: 'absent', beginRef: f.beginRef }, observedAt: after(2000) });
  assert.throws(() => f.send({ type: 'begin_attempted', observedAt: after(2000) }), /stopped/);
  const cancelled: InboundOutcome = { state: 'cancelled', attachmentId, beginRef: f.beginRef, expiresAt: null, cancelledAt: after(2000) };
  f.send({ type: 'outcome', outcome: cancelled });
  const saved = f.restart(); const bytes = fs.readFileSync(f.file, 'utf8');
  assert.deepEqual(f.send({ type: 'stop', reason: 'service_shutdown', observedAt: after(4000) }), saved);
  assert.deepEqual(f.send({ type: 'outcome', outcome: cancelled }), saved);
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  assert.throws(() => f.send({ type: 'outcome', outcome: f.pending }), /terminal outcome changed/);
});

test('approval and consumed receipt are monotonic across delayed responses', t => {
  const f = fixture(t); f.approve();
  f.send({ type: 'outcome', outcome: f.pending });
  assert.equal(f.store.get(f.call.id)!.inbound!.remote!.state, 'approved');
  f.send({ type: 'consume_attempted', observedAt: after(2000) });
  f.send({ type: 'outcome', outcome: f.bound });
  f.send({ type: 'outcome', outcome: f.approved });
  const saved = f.restart(); assert.equal(saved.inbound!.remote!.state, 'bound');
  assert.equal(saved.inbound!.mediaStartBy, after(40_000));
  assert.deepEqual(saved.inbound!.approvalRef, approvalRef);
  assert.throws(() => f.send({ type: 'outcome', outcome: { state: 'expired', attachmentId, beginRef: f.beginRef, expiresAt: after(100_000) } }), /cannot become unbound/);
});

test('stop during uncertain consume preserves the winning binding without releasing private context', t => {
  const f = fixture(t); f.approve(); f.send({ type: 'consume_attempted', observedAt: after(2000) });
  f.send({ type: 'stop', reason: 'broker_cancelled', observedAt: after(3000) });
  const bound = f.send({ type: 'outcome', outcome: f.bound, observedAt: after(80_000) });
  assert.equal(bound.inbound!.localStop!.reason, 'broker_cancelled');
  assert.equal(bound.inbound!.mediaStartBy, after(40_000));
  assert.equal(bound.primary, undefined); assert.equal(bound.primaryClosed, undefined);
  f.restart(); assert.throws(() => f.send({ type: 'context', context: f.context, observedAt: after(20_000) }), /unavailable/);
});

test('bound recovery after deadline preserves original consumed time and refuses new context', t => {
  const f = fixture(t); f.bind(); const before = f.restart().inbound!.mediaStartBy;
  f.send({ type: 'outcome', outcome: f.bound, observedAt: after(300_000) });
  assert.equal(f.store.get(f.call.id)!.inbound!.mediaStartBy, before);
  assert.throws(() => f.send({ type: 'context', context: f.context, observedAt: after(40_000) }), /unavailable/);
  assert.throws(() => f.send({ type: 'outcome', outcome: { ...f.bound, consumedAt: after(15_000) } }), /binding changed/);
});

test('identity-qualified closure works without Primary context or invented transcript completion', t => {
  const f = fixture(t); f.bind(); f.send({ type: 'stop', reason: 'authority_unavailable', observedAt: after(15_000) });
  f.send({ type: 'close_requested', observedAt: after(16_000) });
  f.restart(); assert.equal(f.store.get(f.call.id)!.primaryClosed, undefined);
  const closed = f.send({ type: 'closure', receipt: f.closure, observedAt: after(21_000) });
  assert.equal(closed.primary, undefined); assert.equal(closed.transcriptCapture, undefined); assert.equal(closed.finalized, undefined);
  assert.deepEqual(closed.primaryClosed, { bindingId, callSid: sid, state: 'closed' });
  assert.deepEqual(closed.inbound!.closure, f.closure);
  const bytes = fs.readFileSync(f.file, 'utf8'); f.restart();
  assert.deepEqual(f.send({ type: 'closure', receipt: f.closure, observedAt: after(60_000) }), closed);
  assert.deepEqual(f.send({ type: 'outcome', outcome: f.bound }), closed);
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  assert.throws(() => f.send({ type: 'closure', receipt: { ...f.closure, closedAt: after(22_000) } }), /receipt changed/);
});

for (const field of ['attachmentId', 'bindingId', 'callSid', 'beginRef', 'approvalRef', 'closedAt'] as const) {
  test(`closure refuses changed ${field} and leaves the original record`, t => {
    const f = fixture(t); f.bind();
    const receipt = structuredClone(f.closure);
    if (field === 'beginRef') receipt.beginRef.digest = 'c'.repeat(64);
    else if (field === 'approvalRef') receipt.approvalRef.digest = 'c'.repeat(64);
    else if (field === 'callSid') receipt.callSid = `CA${'3'.repeat(32)}`;
    else if (field === 'closedAt') receipt.closedAt = start;
    else receipt[field] = '77777777-7777-4777-8777-777777777777';
    const bytes = fs.readFileSync(f.file, 'utf8'); assert.throws(() => f.send({ type: 'closure', receipt }));
    assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  });
}

test('valid context attaches only the exact bound open call and preserves existing capture state on closure', t => {
  const f = fixture(t); f.bind();
  const bound = f.send({ type: 'context', context: f.context, observedAt: after(12_000) });
  assert.deepEqual(bound.primary, { bindingId, name: 'Dexter', context: 'user: Previous conversation' });
  f.store.update(f.call.id, { primaryClosePending: true, transcriptCapture: { state: 'pending', pending: 1,
    unsupported: ['capture_outcome_unknown'], sessionFinalized: false, assistantPlaybackComplete: false } });
  const closed = f.send({ type: 'closure', receipt: f.closure });
  assert.equal(closed.primaryClosePending, true); assert.equal(closed.transcriptCapture!.state, 'pending');
  assert.throws(() => f.send({ type: 'context', context: f.context, observedAt: after(22_000) }), /unavailable/);
});

test('local and API deadlines prevent a new consume; stop remains separate from expired outcome', t => {
  const f = fixture(t); f.approve();
  assert.throws(() => f.send({ type: 'consume_attempted', observedAt: after(100_000) }), /unavailable/);
  f.send({ type: 'stop', reason: 'broker_cancelled', observedAt: after(120_000) });
  assert.equal(f.store.get(f.call.id)!.inbound!.remote!.state, 'approved');
  f.send({ type: 'outcome', outcome: { state: 'expired', attachmentId, beginRef: f.beginRef, expiresAt: after(100_000) } });
  assert.equal(f.restart().inbound!.remote!.state, 'expired');
});

test('general update cannot synthesize inbound closure even with matching private context', t => {
  const f = fixture(t); f.bind(); f.send({ type: 'context', context: f.context, observedAt: after(12_000) });
  const before = fs.readFileSync(f.file, 'utf8');
  assert.throws(() => f.store.update(f.call.id, { primaryClosed: { bindingId, callSid: sid, state: 'closed' } }), /explicit receipt transition/);
  assert.equal(fs.readFileSync(f.file, 'utf8'), before);
  const closed = f.send({ type: 'closure', receipt: f.closure });
  assert.deepEqual(f.store.update(f.call.id, { primaryClosed: { ...closed.primaryClosed! } }).primaryClosed, closed.primaryClosed);
  assert.throws(() => f.store.update(f.call.id, { primaryClosed: undefined }), /explicit receipt transition/);
  assert.equal(closed.finalized, undefined); assert.equal(closed.transcriptCapture, undefined);
});

test('exact existing inbound closure remains recoverable without private context', t => {
  const f = fixture(t); f.bind(); const closed = f.send({ type: 'closure', receipt: f.closure });
  const recovered = f.store.update(f.call.id, { primaryClosed: { ...closed.primaryClosed! } });
  assert.deepEqual(recovered.primaryClosed, closed.primaryClosed);
  assert.equal(recovered.primary, undefined); assert.equal(recovered.transcriptCapture, undefined);
  assert.deepEqual(f.restart().inbound!.closure, f.closure);
});

for (const callState of ['open', 'closed'] as const) {
  test(`restart refuses a matching primaryClosed field without a v3 receipt when remote is ${callState}`, t => {
    const f = fixture(t); f.bind(); f.send({ type: 'context', context: f.context, observedAt: after(12_000) }); f.store.close();
    const state = JSON.parse(fs.readFileSync(f.file, 'utf8')) as { calls: Record<string, BrokerCall> };
    const call = state.calls[f.call.id]!;
    call.primaryClosed = { bindingId, callSid: sid, state: 'closed' };
    if (call.inbound!.remote!.state === 'bound') call.inbound!.remote!.callState = callState;
    const bytes = JSON.stringify(state); fs.writeFileSync(f.file, bytes);
    assert.throws(() => new BrokerStore(f.directory), /lacks exact closure receipt/);
    assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  });
}

test('ordinary updates cannot replace journal, inject private context, change leg or reopen terminal call', t => {
  const f = fixture(t); const bytes = fs.readFileSync(f.file, 'utf8');
  assert.throws(() => f.store.update(f.call.id, { inbound: undefined } as never), /explicit transition/);
  assert.throws(() => f.store.update(f.call.id, { inbound: { ...f.call.inbound, waitUntil: after(300_000) } } as never), /explicit transition/);
  assert.throws(() => f.store.update(f.call.id, { primary: { bindingId, name: 'Fake', context: 'not verified' } }), /explicit transition/);
  assert.throws(() => f.store.update(f.call.id, { callSid: `CA${'3'.repeat(32)}` }), /cannot change/);
  assert.throws(() => f.store.update(f.call.id, { callback: { requestId: 'other', consentedAt: start, state: 'requested' } }), /identity mismatch/);
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  f.store.update(f.call.id, { status: 'completed' });
  assert.equal(f.store.update(f.call.id, { status: 'in-progress' }).status, 'completed');
});

test('media start requires bound open context, fixed grace and an immutable first timestamp', t => {
  const f = fixture(t);
  assert.throws(() => f.store.update(f.call.id, { mediaStartedAt: after(12_000) }), /unavailable/);
  const consumedAt = new Date().toISOString();
  const bound = { ...f.bound, consumedAt };
  f.send({ type: 'outcome', outcome: bound, observedAt: consumedAt });
  f.send({ type: 'context', context: f.context, observedAt: consumedAt });
  const started = f.store.update(f.call.id, { mediaStartedAt: consumedAt });
  assert.equal(started.mediaStartedAt, consumedAt);
  f.restart();
  f.send({ type: 'stop', reason: 'service_shutdown', observedAt: consumedAt });
  assert.equal(f.store.update(f.call.id, { mediaStartedAt: consumedAt }).mediaStartedAt, consumedAt);
  assert.throws(() => f.store.update(f.call.id, { mediaStartedAt: undefined }), /cannot change/);
  assert.throws(() => f.store.update(f.call.id, { mediaStartedAt: new Date(Date.parse(consumedAt) + 1).toISOString() }), /cannot change/);
});

for (const corruption of ['proof', 'grace', 'wait', 'caller', 'attachment', 'closure', 'extra'] as const) {
  test(`restart refuses ${corruption} corruption without rewriting journal`, t => {
    const f = fixture(t); f.bind(); f.send({ type: 'closure', receipt: f.closure }); f.store.close();
    const state = JSON.parse(fs.readFileSync(f.file, 'utf8')) as { calls: Record<string, BrokerCall> };
    const call = state.calls[f.call.id]!;
    if (corruption === 'proof') call.inbound!.begin.providerProof.signature += 'changed';
    if (corruption === 'grace') call.inbound!.mediaStartBy = after(80_000);
    if (corruption === 'wait') call.inbound!.waitUntil = after(200_000);
    if (corruption === 'caller') call.number = '+12025550129';
    if (corruption === 'attachment' && call.inbound!.remote!.state !== 'absent') call.inbound!.remote!.attachmentId = bindingId;
    if (corruption === 'closure') call.primaryClosed = undefined;
    if (corruption === 'extra') (call.inbound as unknown as Record<string, unknown>).extra = true;
    const bytes = JSON.stringify(state); fs.writeFileSync(f.file, bytes);
    assert.throws(() => new BrokerStore(f.directory)); assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  });
}

test('legacy inbound and outbound calls load without attachment fields or mutation', t => {
  const f = fixture(t);
  const legacy = f.store.reserve({ requestId: 'old-inbound', purpose: 'Existing call', direction: 'inbound', number: '+12025550125', callSid: `CA${'4'.repeat(32)}` }).call;
  const outbound = f.store.reserve({ requestId: 'old-outbound', purpose: 'Existing outbound', direction: 'outbound', number: '+12025550125' }).call;
  const bytes = fs.readFileSync(f.file, 'utf8'); f.restart();
  assert.deepEqual(f.store.get(legacy.id), legacy); assert.deepEqual(f.store.get(outbound.id), outbound);
  assert.equal(f.store.get(legacy.id)!.inbound, undefined); assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
});
