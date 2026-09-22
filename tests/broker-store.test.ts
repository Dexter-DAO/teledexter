import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { BrokerStore, type BrokerCall } from '../src/broker/store.js';

function fixture(t: test.TestContext) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-store-test-'));
  const store = new BrokerStore(directory);
  t.after(() => { store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  return { store, directory };
}
const input = () => ({ requestId: randomUUID(), direction: 'outbound' as const, purpose: 'Test the account connection', number: '+12025550123' });

test('call reservation and uncertain dispatch survive restart without redial', t => {
  const { store, directory } = fixture(t);
  const request = input();
  const first = store.reserve(request).call;
  store.update(first.id, { status: 'dialing' });
  store.update(first.id, { status: 'unknown' });
  store.close();
  const resumed = new BrokerStore(directory);
  t.after(() => resumed.close());
  const replay = resumed.reserve(request);
  assert.equal(replay.existing, true);
  assert.equal(replay.call.id, first.id);
  assert.equal(replay.call.status, 'unknown');
  assert.throws(() => resumed.reserve({ ...request, number: '+12025550124' }), /identity conflicts/);
});

test('provider status cannot regress and exact CallSid cannot move to another call', t => {
  const { store } = fixture(t);
  const a = store.reserve(input()).call;
  const b = store.reserve(input()).call;
  const callSid = 'CA' + '1'.repeat(32);
  store.update(a.id, { callSid, status: 'in-progress' });
  assert.equal(store.update(a.id, { status: 'ringing' }).status, 'in-progress');
  assert.throws(() => store.update(b.id, { callSid }), /already bound/);
  store.update(a.id, { status: 'completed' });
  assert.equal(store.update(a.id, { status: 'queued' }).status, 'completed');
});

test('a second writer cannot dispatch against the same call store', t => {
  const { directory } = fixture(t);
  assert.throws(() => new BrokerStore(directory), /Another broker process/);
});

test('an old transcript checkpoint cannot replace a newer revision', t => {
  const { store } = fixture(t);
  const call = store.reserve(input()).call;
  store.update(call.id, { transcriptRevision: 2, transcript: { text: 'corrected' } });
  assert.deepEqual(store.update(call.id, { transcriptRevision: 1, transcript: { text: 'old' } }).transcript, { text: 'corrected' });
});

for (const [field, value] of [
  ['id', 'another-call'], ['requestId', 'another-request'], ['number', '+12025550124'],
  ['direction', 'inbound'], ['purpose', 'Execute a different task'], ['createdAt', '2000-01-01T00:00:00.000Z'],
  ['purpose', undefined],
] as const) {
  test(`runtime patch cannot replace ${field} with ${String(value)}`, t => {
    const { store, directory } = fixture(t);
    const call = store.reserve(input()).call;
    const before = fs.readFileSync(path.join(directory, 'calls.json'), 'utf8');
    const patch = { [field]: value, status: 'completed', delegations: { injected: {
      requestId: 'injected', status: 'completed', updatedAt: call.createdAt,
    } } } as unknown as Parameters<BrokerStore['update']>[1];
    assert.throws(() => store.update(call.id, patch), /Call identity cannot change/);
    assert.deepEqual(store.get(call.id), call);
    assert.equal(fs.readFileSync(path.join(directory, 'calls.json'), 'utf8'), before);
  });
}

test('exact identity repeats preserve valid status and delegation writes through restart', t => {
  const { store, directory } = fixture(t);
  const request = input();
  const call = store.reserve(request).call;
  const { id, requestId, number, direction, purpose, createdAt } = call;
  const identity = { id, requestId, number, direction, purpose, createdAt };
  const delegation = { requestId: randomUUID(), status: 'running', updatedAt: createdAt };
  // Identity fields can occur in an untyped runtime object despite Omit.
  const patch = { ...identity, status: 'in-progress', delegations: { work: delegation } } as unknown as Parameters<BrokerStore['update']>[1];
  const changed = store.update(call.id, patch);
  const replay = store.update(call.id, patch);
  for (const [key, value] of Object.entries(identity)) assert.equal(replay[key as keyof BrokerCall], value);
  assert.equal(changed.status, 'in-progress');
  assert.equal(replay.status, 'in-progress');
  assert.deepEqual(replay.delegations, { work: delegation });
  store.close();
  const resumed = new BrokerStore(directory);
  t.after(() => resumed.close());
  assert.deepEqual(resumed.get(call.id), replay);
  assert.equal(resumed.reserve(request).call.id, call.id);
});

test('identity validation and persistence consume one own-property snapshot', t => {
  const { store } = fixture(t);
  const call = store.reserve(input()).call;
  let reads = 0;
  const patch = Object.defineProperty({ status: 'ringing' }, 'purpose', {
    enumerable: true, get: () => ++reads === 1 ? call.purpose : 'Changed after validation',
  }) as Parameters<BrokerStore['update']>[1];
  const result = store.update(call.id, patch);
  assert.equal(reads, 1);
  assert.equal(result.purpose, call.purpose);
  assert.equal(result.status, 'ringing');
});

for (const sid of [undefined, null, '', 'CA' + '2'.repeat(32)] as const) {
  test(`an assigned CallSid cannot be cleared or changed to ${String(sid)}`, t => {
    const { store, directory } = fixture(t);
    const originalSid = 'CA' + '1'.repeat(32);
    const call = store.reserve(input()).call;
    const bound = store.update(call.id, { callSid: originalSid, status: 'queued' });
    const before = fs.readFileSync(path.join(directory, 'calls.json'), 'utf8');
    const patch = { callSid: sid, status: 'completed' } as unknown as Parameters<BrokerStore['update']>[1];
    assert.throws(() => store.update(call.id, patch), /Call SID cannot change/);
    assert.deepEqual(store.get(call.id), bound);
    assert.deepEqual(store.bySid(originalSid), bound);
    if (typeof sid === 'string') assert.equal(store.bySid(sid), undefined);
    assert.equal(fs.readFileSync(path.join(directory, 'calls.json'), 'utf8'), before);
    store.close();
    const resumed = new BrokerStore(directory);
    t.after(() => resumed.close());
    assert.deepEqual(resumed.bySid(originalSid), bound);
  });
}

test('first valid CallSid assignment and its exact repeat retain normal updates and lookup', t => {
  const { store } = fixture(t);
  const call = store.reserve(input()).call;
  assert.equal(store.update(call.id, { callSid: undefined }).callSid, undefined);
  const callSid = 'CA' + 'A1'.repeat(16);
  store.update(call.id, { callSid, status: 'queued' });
  const repeated = store.update(call.id, { callSid, status: 'ringing' });
  assert.equal(repeated.callSid, callSid);
  assert.equal(repeated.status, 'ringing');
  assert.deepEqual(store.bySid(callSid), repeated);
});

for (const callSid of ['', 'not-a-provider-id', null, 1] as const) {
  test(`invalid first CallSid ${String(callSid)} cannot alter the call`, t => {
    const { store, directory } = fixture(t);
    const call = store.reserve(input()).call;
    const before = fs.readFileSync(path.join(directory, 'calls.json'), 'utf8');
    assert.throws(() => store.update(call.id, { callSid, status: 'ringing' } as unknown as Parameters<BrokerStore['update']>[1]), /Invalid call SID/);
    assert.deepEqual(store.get(call.id), call);
    assert.equal(fs.readFileSync(path.join(directory, 'calls.json'), 'utf8'), before);
  });
}
