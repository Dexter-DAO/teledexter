import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { BrokerStore, type BrokerCall } from '../src/broker/store.js';
import { makeResolvedHarkAssignment } from './fixtures/broker-hark.js';

const digest = 'a'.repeat(64);
const changedDigest = 'b'.repeat(64);
const startedAt = '2026-09-23T20:00:00.000Z';
const acceptedAt = '2026-09-23T20:00:01.000Z';
const later = '2026-09-24T20:00:00.000Z';
const sid = `CA${'1'.repeat(32)}`;
function fixture(t: test.TestContext) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-hark-dispatch-'));
  let store = new BrokerStore(directory);
  const file = path.join(directory, 'calls.json');
  t.after(() => { store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  return { file, directory, get store() { return store; },
    restart() { store.close(); store = new BrokerStore(directory); return store; },
    reserve() { return store.reserveHark(makeResolvedHarkAssignment(), digest).call; } };
}

test('notification digest and safely undispatched marker are committed with first reservation', t => {
  const f = fixture(t);
  const call = f.reserve();
  assert.deepEqual(call.harkDispatch, { version: 1, notificationDigest: digest,
    state: 'not_dispatched', reservedAt: call.createdAt });
  assert.equal(call.callSid, undefined);
  assert.deepEqual(JSON.parse(fs.readFileSync(f.file, 'utf8')).calls[call.id], call);
  assert.deepEqual(f.restart().byHarkNotification(call.id, digest), call);
  assert.equal(f.store.byHarkNotification('99999999-9999-4999-8999-999999999999', digest), undefined);
});

test('preflight failure can be resumed after restart without changing assignment or original notification', t => {
  const f = fixture(t);
  const call = f.reserve();
  const failed = f.store.update(call.id, { status: 'failed', failure: { code: 'live_unavailable', message: 'Voice preflight unavailable' } });
  f.restart();
  const retried = f.store.reserveHark(makeResolvedHarkAssignment(), digest);
  assert.equal(retried.existing, true);
  assert.equal(retried.call.harkDispatch?.state, 'not_dispatched');
  const started = f.store.beginHarkDispatch(call.id, startedAt);
  assert.equal(started.status, 'dialing');
  assert.equal(started.harkDispatch?.state, 'dispatch_started');
  assert.equal(started.harkDispatch?.dispatchStartedAt, startedAt);
  assert.deepEqual(started.callAssignment, call.callAssignment);
  assert.equal(started.harkDispatch?.notificationDigest, digest);
  const history = { observedAt: failed.updatedAt, ...failed.failure! };
  assert.equal(started.failure, undefined, 'the recovered preflight failure is no longer current');
  assert.deepEqual(started.harkDispatch?.preDispatchFailure, history);
  assert.deepEqual(JSON.parse(fs.readFileSync(f.file, 'utf8')).calls[call.id], started);
  f.restart();
  f.store.acceptHarkDispatch(call.id, sid, acceptedAt);
  const queued = f.store.observeHarkProvider(call.id, 'queued', acceptedAt);
  assert.equal(queued.failure, undefined);
  assert.deepEqual(queued.harkDispatch?.preDispatchFailure, history);
  const bytes = fs.readFileSync(f.file, 'utf8');
  f.restart();
  assert.deepEqual(f.store.acceptHarkDispatch(call.id, sid, later), queued);
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
});

test('dispatch archives only the last preflight failure and preserves later current voice errors', t => {
  const f = fixture(t); const call = f.reserve();
  f.store.update(call.id, { status: 'failed', failure: { code: 'runtime_unavailable', message: 'Primary is unavailable' } });
  const failed = f.store.update(call.id, { status: 'failed', failure: { code: 'live_unavailable', message: 'Voice preflight unavailable' } });
  const started = f.store.beginHarkDispatch(call.id, startedAt);
  const history = { observedAt: failed.updatedAt, ...failed.failure! };
  assert.deepEqual(started.harkDispatch?.preDispatchFailure, history);
  f.store.acceptHarkDispatch(call.id, sid, acceptedAt);
  const failure = { code: 'voice_disconnected', message: 'Voice connection ended unexpectedly' };
  f.store.update(call.id, { failure });
  f.store.observeHarkProvider(call.id, 'in-progress', acceptedAt);
  f.restart();
  const current = f.store.get(call.id)!;
  assert.deepEqual(current.failure, failure);
  assert.deepEqual(current.harkDispatch?.preDispatchFailure, history);
  assert.throws(() => f.store.update(call.id, { harkDispatch: { ...current.harkDispatch,
    preDispatchFailure: { ...history, code: 'rewritten' } } } as never), /explicit transition/);
  const bytes = fs.readFileSync(f.file, 'utf8');
  assert.deepEqual(f.store.acceptHarkDispatch(call.id, sid, later), current);
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
});

for (const corruption of ['extra-key', 'invalid-time', 'long-code', 'long-message', 'empty-code', 'empty-message'] as const) {
  test(`restart rejects pre-dispatch failure history ${corruption}`, t => {
    const f = fixture(t); const call = f.reserve();
    f.store.update(call.id, { status: 'failed', failure: { code: 'live_unavailable', message: 'Voice preflight unavailable' } });
    f.store.beginHarkDispatch(call.id, startedAt); f.store.close();
    const state = JSON.parse(fs.readFileSync(f.file, 'utf8')) as { calls: Record<string, BrokerCall> };
    const history = state.calls[call.id]!.harkDispatch!.preDispatchFailure!;
    if (corruption === 'extra-key') (history as unknown as Record<string, unknown>).other = 'not allowed';
    if (corruption === 'invalid-time') history.observedAt = 'yesterday';
    if (corruption === 'long-code') history.code = 'x'.repeat(81);
    if (corruption === 'long-message') history.message = 'x'.repeat(401);
    if (corruption === 'empty-code') history.code = '';
    if (corruption === 'empty-message') history.message = '';
    const bytes = JSON.stringify(state); fs.writeFileSync(f.file, bytes);
    assert.throws(() => new BrokerStore(f.directory));
    assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  });
}

test('crash after dispatch-start fence remains uncertain and never grants a second dispatch', t => {
  const f = fixture(t); const call = f.reserve();
  f.store.beginHarkDispatch(call.id, startedAt);
  f.restart();
  assert.equal(f.store.byHarkNotification(call.id, digest)?.harkDispatch?.state, 'dispatch_started');
  assert.equal(f.store.byHarkNotification(call.id, digest)?.callSid, undefined);
  assert.throws(() => f.store.beginHarkDispatch(call.id, later), /already started/);
  f.store.update(call.id, { status: 'unknown', failure: { code: 'request_timeout', message: 'Provider response unknown' } });
  f.restart();
  assert.throws(() => f.store.beginHarkDispatch(call.id, later), /already started/);
  assert.equal(f.store.reserveHark(makeResolvedHarkAssignment(), digest).call.harkDispatch?.state, 'dispatch_started');
});

test('a post-start provider refusal remains fenced even when local status says failed', t => {
  const f = fixture(t); const call = f.reserve();
  f.store.beginHarkDispatch(call.id, startedAt);
  f.store.update(call.id, { status: 'failed', failure: { code: 'provider_refused', message: 'Provider returned refusal' } });
  f.restart();
  assert.equal(f.store.byHarkNotification(call.id, digest)?.harkDispatch?.state, 'dispatch_started');
  assert.throws(() => f.store.beginHarkDispatch(call.id, later), /already started/);
});

test('provider acceptance and SID are atomic and exact response replay preserves all saved bytes', t => {
  const f = fixture(t); const call = f.reserve();
  f.store.beginHarkDispatch(call.id, startedAt);
  const accepted = f.store.acceptHarkDispatch(call.id, sid, acceptedAt);
  assert.equal(accepted.callSid, sid);
  assert.deepEqual(accepted.harkDispatch, { version: 1, notificationDigest: digest, reservedAt: call.createdAt,
    state: 'provider_accepted', dispatchStartedAt: startedAt, providerAcceptedAt: acceptedAt });
  const bytes = fs.readFileSync(f.file, 'utf8');
  f.restart();
  assert.deepEqual(f.store.byHarkNotification(call.id, digest), accepted);
  assert.deepEqual(f.store.acceptHarkDispatch(call.id, sid, later), accepted);
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  assert.throws(() => f.store.beginHarkDispatch(call.id, later), /already started/);
  assert.throws(() => f.store.acceptHarkDispatch(call.id, `CA${'2'.repeat(32)}`, later), /SID cannot change/);
});

test('signed callback acceptance before create response keeps first provider receipt and later observations', t => {
  const f = fixture(t); const call = f.reserve();
  f.store.beginHarkDispatch(call.id, startedAt);
  f.store.acceptHarkDispatch(call.id, sid, acceptedAt);
  f.store.observeHarkProvider(call.id, 'in-progress', acceptedAt);
  const answered = f.store.get(call.id)!;
  const bytes = fs.readFileSync(f.file, 'utf8');
  assert.deepEqual(f.store.acceptHarkDispatch(call.id, sid, later), answered);
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  assert.equal(answered.harkDispatch?.providerAcceptedAt, acceptedAt);
  assert.equal(answered.harkJournal?.providerStatus, 'answered');
});

test('missing start, invalid SID and shared SID refuse without partially accepting the call', t => {
  const f = fixture(t); const call = f.reserve();
  assert.throws(() => f.store.acceptHarkDispatch(call.id, sid, acceptedAt), /not started/);
  f.store.beginHarkDispatch(call.id, startedAt);
  assert.throws(() => f.store.acceptHarkDispatch(call.id, 'wrong', acceptedAt), /Invalid call SID/);
  const legacy = f.store.reserve({ requestId: 'legacy-provider-binding', direction: 'outbound', purpose: 'Legacy call', number: '+12025550124' }).call;
  f.store.update(legacy.id, { callSid: sid });
  assert.throws(() => f.store.acceptHarkDispatch(call.id, sid, acceptedAt), /already bound/);
  assert.equal(f.store.get(call.id)?.harkDispatch?.state, 'dispatch_started');
  assert.equal(f.store.get(call.id)?.callSid, undefined);
});

test('different or omitted notification digest cannot replay a marked reservation', t => {
  const f = fixture(t); const call = f.reserve();
  const bytes = fs.readFileSync(f.file, 'utf8');
  assert.throws(() => f.store.byHarkNotification(call.id, changedDigest), /notification conflicts/);
  assert.throws(() => f.store.reserveHark(makeResolvedHarkAssignment(), changedDigest), /notification conflicts/);
  assert.throws(() => f.store.reserveHark(makeResolvedHarkAssignment()), /notification conflicts/);
  assert.throws(() => f.store.reserveHark(makeResolvedHarkAssignment(), 'not-a-digest'));
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
});

test('markerless legacy Hark calls remain ambiguous and never acquire a retry marker', t => {
  const f = fixture(t);
  const legacy = f.store.reserveHark(makeResolvedHarkAssignment()).call;
  const bytes = fs.readFileSync(f.file, 'utf8');
  f.restart();
  assert.equal(f.store.reserveHark(makeResolvedHarkAssignment()).existing, true);
  assert.throws(() => f.store.byHarkNotification(legacy.id, digest), /identity unavailable/);
  assert.throws(() => f.store.reserveHark(makeResolvedHarkAssignment(), digest), /notification conflicts/);
  assert.throws(() => f.store.beginHarkDispatch(legacy.id, startedAt), /history unavailable/);
  assert.throws(() => f.store.acceptHarkDispatch(legacy.id, sid, acceptedAt), /history unavailable/);
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  f.store.update(legacy.id, { callSid: sid });
  assert.throws(() => f.store.byHarkNotification(legacy.id, digest), /identity unavailable/);
  assert.equal(f.store.get(legacy.id)?.harkDispatch, undefined);
});

test('non-Hark request ID collision cannot expose or convert another call', t => {
  const f = fixture(t);
  const id = makeResolvedHarkAssignment().callAssignment.assignmentId;
  const operator = f.store.reserve({ requestId: id, direction: 'outbound', purpose: 'Existing operator call', number: '+12025550123' }, 'authenticated_broker_control').call;
  assert.throws(() => f.store.byHarkNotification(id, digest), /delivery conflicts/);
  assert.throws(() => f.store.reserveHark(makeResolvedHarkAssignment(), digest), /delivery conflicts/);
  assert.equal(f.store.get(operator.id)?.harkDispatch, undefined);
});

test('ordinary updates cannot change, clear or invent the dispatch marker or bypass atomic SID acceptance', t => {
  const f = fixture(t); const call = f.reserve();
  const bytes = fs.readFileSync(f.file, 'utf8');
  for (const marker of [undefined, { ...call.harkDispatch, state: 'dispatch_started', dispatchStartedAt: startedAt },
    { ...call.harkDispatch, notificationDigest: changedDigest }])
    assert.throws(() => f.store.update(call.id, { harkDispatch: marker } as never), /explicit transition/);
  assert.throws(() => f.store.update(call.id, { callSid: sid }), /provider evidence/);
  assert.throws(() => f.store.update(call.id, { status: 'dialing' }), /dispatched status/);
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  f.store.beginHarkDispatch(call.id, startedAt);
  assert.throws(() => f.store.update(call.id, { callSid: sid }), /provider evidence/);
});

for (const corruption of ['marker-shape', 'digest', 'reservation-time', 'missing-start', 'premature-sid', 'premature-status', 'accepted-without-sid', 'accepted-without-time'] as const) {
  test(`restart rejects ${corruption} without rewriting the saved file`, t => {
    const f = fixture(t); const call = f.reserve(); f.store.close();
    const state = JSON.parse(fs.readFileSync(f.file, 'utf8')) as { calls: Record<string, BrokerCall> };
    const saved = state.calls[call.id]!;
    if (corruption === 'marker-shape') (saved.harkDispatch as unknown as Record<string, unknown>).other = 'not allowed';
    if (corruption === 'digest') saved.harkDispatch!.notificationDigest = 'invalid';
    if (corruption === 'reservation-time') saved.harkDispatch!.reservedAt = startedAt;
    if (corruption === 'missing-start') { saved.harkDispatch!.state = 'dispatch_started'; saved.status = 'dialing'; }
    if (corruption === 'premature-sid') saved.callSid = sid;
    if (corruption === 'premature-status') saved.status = 'in-progress';
    if (corruption === 'accepted-without-sid') saved.harkDispatch = { ...saved.harkDispatch!, state: 'provider_accepted', dispatchStartedAt: startedAt, providerAcceptedAt: acceptedAt };
    if (corruption === 'accepted-without-time') { saved.callSid = sid; saved.harkDispatch = { ...saved.harkDispatch!, state: 'provider_accepted', dispatchStartedAt: startedAt }; }
    const modified = JSON.stringify(state); fs.writeFileSync(f.file, modified);
    assert.throws(() => new BrokerStore(f.directory));
    assert.equal(fs.readFileSync(f.file, 'utf8'), modified);
  });
}
