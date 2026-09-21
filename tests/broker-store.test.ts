import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { BrokerStore } from '../src/broker/store.js';

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
