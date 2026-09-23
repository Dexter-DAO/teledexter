import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { BrokerStore, type BrokerCall } from '../src/broker/store.js';
import { harkObservedStart, type HarkJournalEvent, type HarkDeliveryOutcome } from '../src/broker/harkJournal.js';
import { harkSourceDigest } from '../src/broker/harkAssignment.js';
import { makeResolvedHarkAssignment } from './fixtures/broker-hark.js';

const queuedAt = '2026-09-23T19:31:00.000Z';
const answeredAt = '2026-09-23T19:31:05.250Z';
const endedAt = '2026-09-23T19:32:05.750Z';
const timing = { startedAt: answeredAt, endedAt, durationSeconds: 60 };
const sid = `CA${'1'.repeat(32)}`;
function fixture(t: test.TestContext) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-hark-journal-'));
  let store = new BrokerStore(directory);
  t.after(() => { store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  const call = store.reserveHark(makeResolvedHarkAssignment()).call;
  return { directory, id: call.id, get store() { return store; },
    restart() { store.close(); store = new BrokerStore(directory); return store; },
    bind() { return store.update(call.id, { callSid: sid }); } };
}
function delivered(item: HarkJournalEvent): HarkDeliveryOutcome {
  if (item.kind === 'outbound_status') return { state: 'delivered', purpose: 'voice.outbound-status.v2', httpStatus: 204 };
  if (item.kind === 'call_ended') return { state: 'delivered', purpose: 'voice.call-ended.v2', httpStatus: 200,
    receipt: { ok: true, id: '99999999-9999-4999-8999-999999999999', status: 'completed' } };
  const { caseId, eventId, deliveryId, intentId, callSid, originTaskId, sourceEventId } = item.body;
  return { state: 'delivered', httpStatus: 200, receipt: { v: 'hark.phone-callback-result.v1', ok: true,
    replay: false, caseId, eventId, deliveryId, intentId, callSid, originTaskId, sourceEventId, caseState: 'open' } };
}
function settle(store: BrokerStore, id: string, item: HarkJournalEvent, outcome = delivered(item)) {
  store.recordHarkAttempt(id, item.id, item.bodyDigest, endedAt);
  return store.settleHarkEvent(id, item.id, item.bodyDigest, outcome);
}

test('reservation and local dispatch failures cannot manufacture provider or billing events', t => {
  const f = fixture(t);
  for (const status of ['prepared', 'dialing', 'unknown', 'failed'] as const) f.store.update(f.id, { status });
  assert.deepEqual(f.store.pendingHarkEvents(), []);
  assert.equal(f.store.get(f.id)?.harkJournal, undefined);
  assert.throws(() => f.store.observeHarkProvider(f.id, 'completed', endedAt, timing), /binding unavailable/);
  f.bind();
  for (const status of ['prepared', 'dialing', 'unknown', 'acknowledged'] as const)
    assert.throws(() => f.store.observeHarkProvider(f.id, status as never, queuedAt), /Invalid Hark provider status/);
  assert.deepEqual(f.store.pendingHarkEvents(), []);
});

test('provider transition and both outbox bodies persist together and recover exact first observation', t => {
  const f = fixture(t); f.bind();
  const call = f.store.observeHarkProvider(f.id, 'queued', queuedAt);
  assert.equal(call.status, 'queued');
  assert.equal(Object.values(call.harkJournal!.events).length, 2);
  const saved = JSON.parse(fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8')).calls[f.id];
  assert.deepEqual(saved, call);
  f.restart();
  const replay = f.store.observeHarkProvider(f.id, 'queued', endedAt);
  assert.deepEqual(replay.harkJournal, call.harkJournal);
  assert.equal(replay.harkJournal!.events[`voice-status:${f.id}:queued`]!.observedAt, queuedAt);
});

test('answer and media evidence use separate states and retain first observed start', t => {
  const f = fixture(t); f.bind();
  const answer = f.store.observeHarkProvider(f.id, 'in-progress', answeredAt);
  assert.equal(answer.harkJournal?.providerStatus, 'answered');
  assert.equal(harkObservedStart(answer), answeredAt);
  const streaming = f.store.observeHarkMediaStarted(f.id, '2026-09-23T19:31:06.000Z');
  assert.equal(streaming.harkJournal?.providerStatus, 'streaming');
  assert.equal(harkObservedStart(streaming), answeredAt);
  assert.equal(streaming.mediaStartedAt, '2026-09-23T19:31:06.000Z');
  const repeat = f.store.observeHarkMediaStarted(f.id, endedAt);
  assert.deepEqual(repeat.harkJournal, streaming.harkJournal);
  assert.equal(repeat.mediaStartedAt, streaming.mediaStartedAt);
  const lateRinging = f.store.observeHarkProvider(f.id, 'ringing', endedAt);
  assert.equal(lateRinging.status, 'in-progress');
  assert.deepEqual(lateRinging.harkJournal, streaming.harkJournal);
});

test('validated media supplies start evidence when no answered callback was saved', t => {
  const f = fixture(t); f.bind();
  const call = f.store.observeHarkMediaStarted(f.id, answeredAt);
  assert.equal(harkObservedStart(call), answeredAt);
});

test('provider termination without qualified timing remains explicit and uncharged through restart', t => {
  const f = fixture(t); f.bind();
  const closed = f.store.observeHarkProvider(f.id, 'no-answer', endedAt);
  assert.deepEqual(closed.harkJournal?.ended, { providerStatus: 'no-answer', observedAt: endedAt });
  assert.equal(Object.values(closed.harkJournal!.events).some(item => item.kind === 'call_ended'), false);
  assert.deepEqual(f.restart().get(f.id)?.harkJournal, closed.harkJournal);
  assert.equal(f.store.pendingHarkEvents(f.id).some(item => item.event.kind === 'call_ended'), false);
});

test('call-ended has frozen exact receipt fields and waits for every outbound status204', t => {
  const f = fixture(t); f.bind();
  f.store.observeHarkProvider(f.id, 'queued', queuedAt);
  f.store.observeHarkProvider(f.id, 'in-progress', answeredAt);
  const closed = f.store.observeHarkProvider(f.id, 'completed', endedAt, timing);
  const end = closed.harkJournal!.events[`voice-ended:${f.id}`]!;
  assert.equal(end.kind, 'call_ended');
  assert.deepEqual(end.body, { source: 'phone', direction: 'outbound-alert', alert_intent_id: f.id,
    alert_event_id: `hark:${f.id}`, external_sid: sid, started_at: answeredAt, ended_at: endedAt,
    duration_seconds: 60, caller_user_id: makeResolvedHarkAssignment().callAssignment.hark.ownerSupabaseUserId });
  assert.equal(f.store.pendingHarkEvents(f.id).some(item => item.event.kind === 'call_ended'), false);
  assert.throws(() => f.store.recordHarkAttempt(f.id, end.id, end.bodyDigest, endedAt), /not ready/);
  for (const state of ['queued', 'answered', 'completed']) {
    const pending = f.store.pendingHarkEvents(f.id).find(item => item.event.kind === 'outbound_status')!.event;
    assert.equal(pending.kind === 'outbound_status' && pending.body.state, state);
    settle(f.store, f.id, pending);
  }
  const pendingEnd = f.restart().pendingHarkEvents(f.id).find(item => item.event.kind === 'call_ended')!.event;
  assert.deepEqual(pendingEnd, end);
  const receipt = settle(f.store, f.id, pendingEnd);
  assert.equal(receipt.state, 'delivered');
  assert.deepEqual(f.store.settleHarkEvent(f.id, pendingEnd.id, pendingEnd.bodyDigest, delivered(pendingEnd)), receipt);
  assert.equal(f.restart().get(f.id)?.harkJournal?.events[end.id]?.state, 'delivered');
});

test('missing timing may be completed later once, while changed timing or terminal outcome refuses', t => {
  const f = fixture(t); f.bind();
  f.store.observeHarkProvider(f.id, 'in-progress', answeredAt);
  f.store.observeHarkProvider(f.id, 'completed', endedAt);
  const completed = f.store.observeHarkProvider(f.id, 'completed', '2026-09-23T19:35:00.000Z', timing);
  assert.equal(completed.harkJournal?.ended?.observedAt, endedAt);
  assert.deepEqual(completed.harkJournal?.ended?.timing, timing);
  assert.throws(() => f.store.observeHarkProvider(f.id, 'completed', endedAt, { ...timing, durationSeconds: 61 }), /timing cannot change/);
  assert.throws(() => f.store.observeHarkProvider(f.id, 'failed', endedAt), /terminal status conflicts/);
  const late = f.store.observeHarkMediaStarted(f.id, '2026-09-23T19:36:00.000Z');
  assert.equal(late.status, 'completed');
  assert.equal(late.mediaStartedAt, undefined);
});

test('attempts and retryable outcomes survive restart without changing event identity or body', t => {
  const f = fixture(t); f.bind();
  f.store.observeHarkProvider(f.id, 'queued', queuedAt);
  const item = f.store.pendingHarkEvents(f.id).find(row => row.event.kind === 'outbound_status')!.event;
  settle(f.store, f.id, item, { state: 'pending', reason: 'request_failed' });
  const restored = f.restart().pendingHarkEvents(f.id).find(row => row.event.id === item.id)!.event;
  assert.equal(restored.attempts, 1);
  assert.equal(restored.lastAttemptAt, endedAt);
  assert.deepEqual(restored.body, item.body);
  assert.equal(restored.bodyDigest, item.bodyDigest);
  const done = settle(f.store, f.id, restored);
  assert.equal(done.attempts, 2);
  assert.equal(done.lastOutcome, undefined);
});

test('permanent status rejection survives restart and blocks later billing rather than recharging', t => {
  const f = fixture(t); f.bind();
  f.store.observeHarkProvider(f.id, 'completed', endedAt, timing);
  const item = f.store.pendingHarkEvents(f.id).find(row => row.event.kind === 'outbound_status')!.event;
  settle(f.store, f.id, item, { state: 'rejected', reason: 'http_refusal', httpStatus: 409 });
  const restored = f.restart();
  assert.equal(restored.get(f.id)?.harkJournal?.events[item.id]?.state, 'rejected');
  assert.equal(restored.pendingHarkEvents(f.id).some(row => row.event.kind !== 'hark_status'), false);
  assert.throws(() => restored.recordHarkAttempt(f.id, item.id, item.bodyDigest, endedAt), /not ready/);
});

test('settlement checks exact event digest, transport kind, receipt identity and prior attempt', t => {
  const f = fixture(t); f.bind(); f.store.observeHarkProvider(f.id, 'queued', queuedAt);
  const rows = f.store.pendingHarkEvents(f.id);
  const item = rows.find(row => row.event.kind === 'hark_status')!.event;
  assert.throws(() => f.store.settleHarkEvent(f.id, item.id, item.bodyDigest, delivered(item)), /not recorded/);
  assert.throws(() => f.store.recordHarkAttempt(f.id, item.id, '0'.repeat(64), endedAt), /identity/);
  f.store.recordHarkAttempt(f.id, item.id, item.bodyDigest, endedAt);
  assert.throws(() => f.store.settleHarkEvent(f.id, item.id, item.bodyDigest,
    { state: 'delivered', purpose: 'voice.outbound-status.v2', httpStatus: 204 }), /Wrong/);
  const wrong = delivered(item);
  assert.ok(wrong.state === 'delivered' && 'receipt' in wrong);
  (wrong.receipt as { callSid?: string }).callSid = `CA${'2'.repeat(32)}`;
  assert.throws(() => f.store.settleHarkEvent(f.id, item.id, item.bodyDigest, wrong), /identity mismatch/);
  assert.equal(f.store.get(f.id)?.harkJournal?.events[item.id]?.state, 'pending');
});

test('runtime patch cannot replace outbox or assignment and exact reordered reservation retains saved bytes', t => {
  const f = fixture(t); f.bind(); f.store.observeHarkProvider(f.id, 'queued', queuedAt);
  const before = fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8');
  assert.throws(() => f.store.update(f.id, { harkJournal: undefined } as never), /requires an observed/);
  assert.throws(() => f.store.update(f.id, { callAssignment: undefined } as never), /assignment cannot change/);
  const resolved = makeResolvedHarkAssignment();
  resolved.callAssignment.hark.event.facts = Object.fromEntries(Object.entries(resolved.callAssignment.hark.event.facts).reverse());
  assert.equal(f.store.reserveHark(resolved).existing, true);
  assert.equal(fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8'), before);
  resolved.callAssignment.hark.event.summary = 'Another saved observation';
  const { sourceDigest: _digest, ...source } = resolved.callAssignment.hark;
  resolved.callAssignment.hark.sourceDigest = harkSourceDigest(source);
  assert.throws(() => f.store.reserveHark(resolved), /conflicts/);
});

for (const corruption of ['digest', 'identity', 'timestamp', 'missing-pair', 'receipt', 'assignment'] as const) {
  test(`restart refuses corrupted ${corruption} while preserving the evidence file`, t => {
    const f = fixture(t); f.bind(); f.store.observeHarkProvider(f.id, 'queued', queuedAt); f.store.close();
    const file = path.join(f.directory, 'calls.json');
    const state = JSON.parse(fs.readFileSync(file, 'utf8')) as { calls: Record<string, BrokerCall> };
    const call = state.calls[f.id]!;
    const item = Object.values(call.harkJournal!.events).find(entry => entry.kind === 'hark_status')!;
    if (corruption === 'digest') item.bodyDigest = '0'.repeat(64);
    if (corruption === 'identity' && item.kind === 'hark_status') (item.body as { callSid: string }).callSid = `CA${'2'.repeat(32)}`;
    if (corruption === 'timestamp') item.observedAt = answeredAt;
    if (corruption === 'missing-pair') delete call.harkJournal!.events[`voice-status:${f.id}:queued`];
    if (corruption === 'receipt') { item.state = 'delivered'; item.receipt = {}; }
    if (corruption === 'assignment' && call.callAssignment?.origin === 'authenticated_hark_delivery') call.callAssignment.value.hark.billing.unitPriceMicro++;
    const corrupted = JSON.stringify(state); fs.writeFileSync(file, corrupted);
    assert.throws(() => new BrokerStore(f.directory));
    assert.equal(fs.readFileSync(file, 'utf8'), corrupted);
  });
}

test('ordinary legacy call restart performs no journal rewrite and lifecycle methods require Hark assignment', t => {
  const f = fixture(t);
  const legacy = f.store.reserve({ requestId: 'legacy-call', direction: 'outbound', purpose: 'Old call', number: '+12025550124' }).call;
  f.store.update(legacy.id, { status: 'completed', callSid: `CA${'3'.repeat(32)}` });
  const before = fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8');
  f.restart();
  assert.equal(fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8'), before);
  assert.equal(f.store.get(legacy.id)?.harkJournal, undefined);
  assert.throws(() => f.store.observeHarkProvider(legacy.id, 'completed', endedAt, timing), /binding unavailable/);
});
