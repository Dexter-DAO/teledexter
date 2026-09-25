import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { BrokerStore, type BrokerCall } from '../src/broker/store.js';

const selected = () => ({ originalRequestId: randomUUID(), questionId: randomUUID(), questionRevision: 1 as const });
const input = () => ({ requestId: randomUUID(), direction: 'outbound' as const,
  purpose: 'Continue the saved question.', number: '+12025550123' });
function fixture(t: test.TestContext) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-continuation-store-'));
  const store = new BrokerStore(directory);
  t.after(() => { store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  return { store, directory, file: path.join(directory, 'calls.json') };
}

test('selected task persists before provider dispatch and exact replay/restart preserves every byte', t => {
  const f = fixture(t);
  const request = { ...input(), continuation: selected() };
  const first = f.store.reserve(request, 'authenticated_broker_control');
  assert.equal(first.call.status, 'prepared');
  assert.equal(first.call.callSid, undefined);
  assert.deepEqual(first.call.continuation, request.continuation);
  const bytes = fs.readFileSync(f.file, 'utf8');
  assert.equal(f.store.reserve(request, 'authenticated_broker_control').existing, true);
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  f.store.close();
  const restarted = new BrokerStore(f.directory); t.after(() => restarted.close());
  assert.deepEqual(restarted.get(first.call.id), first.call);
  assert.equal(restarted.reserve(request, 'authenticated_broker_control').existing, true);
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
});

test('same request cannot omit or switch the selected question or original task', t => {
  const f = fixture(t); const request = { ...input(), continuation: selected() };
  f.store.reserve(request, 'authenticated_broker_control');
  const bytes = fs.readFileSync(f.file, 'utf8');
  for (const continuation of [undefined, selected(), { ...request.continuation, questionId: randomUUID() },
    { ...request.continuation, originalRequestId: randomUUID() }]) {
    assert.throws(() => f.store.reserve({ ...request, continuation }, 'authenticated_broker_control'), /call_continuation_changed/);
  }
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
});

test('legacy absence remains absent and cannot acquire a selector on replay or runtime update', t => {
  const f = fixture(t); const request = input();
  const call = f.store.reserve(request, 'authenticated_broker_control').call;
  assert.equal(Object.hasOwn(call, 'continuation'), false);
  const bytes = fs.readFileSync(f.file, 'utf8');
  assert.throws(() => f.store.reserve({ ...request, continuation: selected() }, 'authenticated_broker_control'), /call_continuation_changed/);
  assert.throws(() => f.store.update(call.id, { continuation: selected() } as never), /call_continuation_changed/);
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  const updated = f.store.update(call.id, { continuation: undefined } as never);
  assert.equal(Object.hasOwn(updated, 'continuation'), false);
});

test('runtime patches cannot remove or rewrite a saved selection', t => {
  const f = fixture(t); const continuation = selected();
  const call = f.store.reserve({ ...input(), continuation }, 'authenticated_broker_control').call;
  const bytes = fs.readFileSync(f.file, 'utf8');
  for (const value of [undefined, selected(), null]) {
    assert.throws(() => f.store.update(call.id, { continuation: value } as never));
  }
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  const next = f.store.update(call.id, { continuation: { ...continuation }, status: 'dialing' } as never);
  assert.deepEqual(next.continuation, continuation);
});

test('unassigned or inbound reservations refuse a continuation before persisting', t => {
  const f = fixture(t);
  const emptyJournal = fs.readFileSync(f.file, 'utf8');
  assert.deepEqual(JSON.parse(emptyJournal), { version: 1, calls: {} });
  assert.throws(() => f.store.reserve({ ...input(), continuation: selected() }), /call_continuation_not_allowed/);
  assert.throws(() => f.store.reserve({ ...input(), direction: 'inbound', continuation: selected() }, 'authenticated_broker_control'));
  assert.deepEqual(f.store.list(), []);
  assert.equal(fs.readFileSync(f.file, 'utf8'), emptyJournal);
});

test('restart refuses a tampered saved selector without upgrading or rewriting it', t => {
  const f = fixture(t);
  const call = f.store.reserve({ ...input(), continuation: selected() }, 'authenticated_broker_control').call;
  f.store.close();
  const bad = { ...call, continuation: { ...call.continuation!, questionRevision: 2 } } as unknown as BrokerCall;
  const bytes = JSON.stringify({ version: 1, calls: { [bad.id]: bad } });
  fs.writeFileSync(f.file, bytes);
  assert.throws(() => new BrokerStore(f.directory), /invalid_call_continuation/);
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
});
