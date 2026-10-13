import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { BrokerStore } from '../src/broker/store.js';

function fixture(t: test.TestContext) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-store-recovery-'));
  const store = new BrokerStore(directory);
  t.after(() => { fs.chmodSync(directory, 0o700); store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  return { store, directory };
}
const input = () => ({ requestId: randomUUID(), direction: 'outbound' as const, purpose: 'Test the account connection', number: '+12025550123' });

test('a failed journal write that left the previous journal intact does not wedge the store', t => {
  if (process.getuid?.() === 0) return t.skip('root ignores directory permissions');
  const { store, directory } = fixture(t);
  const call = store.reserve({ ...input(), callSid: `CA${'1'.repeat(32)}` }).call;
  const file = path.join(directory, 'calls.json');
  const before = fs.readFileSync(file, 'utf8');
  fs.chmodSync(directory, 0o500);
  assert.throws(() => store.update(call.id, { status: 'dialing' } as never), /EACCES/);
  assert.equal(fs.readFileSync(file, 'utf8'), before);
  assert.equal(store.get(call.id)?.status, call.status);
  fs.chmodSync(directory, 0o700);
  const updated = store.update(call.id, { status: 'dialing' } as never);
  assert.equal(updated.status, 'dialing');
  assert.equal(JSON.parse(fs.readFileSync(file, 'utf8')).calls[call.id].status, 'dialing');
  assert.notEqual(store.readCallRequestJournal().state, 'uncertain');
});

test('unknown journal content after a failed write keeps the store uncertain', t => {
  if (process.getuid?.() === 0) return t.skip('root ignores directory permissions');
  const { store, directory } = fixture(t);
  const call = store.reserve({ ...input(), callSid: `CA${'2'.repeat(32)}` }).call;
  const file = path.join(directory, 'calls.json');
  fs.chmodSync(directory, 0o500);
  // Another writer replaced the journal content while this write failed.
  fs.chmodSync(file, 0o600);
  fs.writeFileSync(file, JSON.stringify({ version: 1, calls: {} }));
  assert.throws(() => store.update(call.id, { status: 'dialing' } as never), /EACCES/);
  fs.chmodSync(directory, 0o700);
  assert.throws(() => store.update(call.id, { status: 'dialing' } as never), /write outcome is uncertain/);
  assert.equal(store.readCallRequestJournal().state, 'uncertain');
});
