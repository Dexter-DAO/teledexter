// Prepared source only. Install under tests/ after the separately pinned
// CALL-REQUEST-RECEIPT-INTEGRATION.v1.patch is applied and qualification is assigned.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { BrokerStore } from '../src/broker/store.js';
import { createBrokerServer } from '../src/broker/server.js';
import type { BrokerConfig } from '../src/broker/config.js';
import { CallRequestReceiptError, PHONE_OWNER_CALL_PURPOSE, readCallRequestReceipt } from '../src/broker/callRequestReceipt.js';

const input = () => ({ requestId: randomUUID(), direction: 'outbound' as const,
  purpose: PHONE_OWNER_CALL_PURPOSE, number: '+12025550123',
  continuation: { originalRequestId: randomUUID(), questionId: randomUUID(), questionRevision: 1 as const } });
const unavailable = (error: unknown) => error instanceof CallRequestReceiptError && error.code === 'call_request_unavailable';
function fixture(t: test.TestContext) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-receipt-store-'));
  const store = new BrokerStore(directory);
  t.after(() => { store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  return { directory, file: path.join(directory, 'calls.json'), lock: path.join(directory, 'writer.lock'), store };
}

test('fresh empty storage is durably initialized and exact lookup is read-only', t => {
  const f = fixture(t);
  const original = fs.readFileSync(f.file, 'utf8');
  assert.deepEqual(JSON.parse(original), { version: 1, calls: {} });
  assert.throws(() => readCallRequestReceipt(f.store, randomUUID()), error =>
    error instanceof CallRequestReceiptError && error.code === 'call_request_not_found');
  assert.equal(fs.readFileSync(f.file, 'utf8'), original);
  f.store.close();
  assert.throws(() => readCallRequestReceipt(f.store, randomUUID()), unavailable);
});

test('post-rename sync failure refuses lookup and later writes until restart recovers the saved reservation', t => {
  const f = fixture(t); const request = input(); const sync = fs.fsyncSync;
  const mocked = t.mock.method(fs, 'fsyncSync', (fd: number) => {
    if (fs.fstatSync(fd).isDirectory()) throw new Error('injected directory sync failure');
    sync(fd);
  });
  assert.throws(() => f.store.reserve(request, 'authenticated_broker_control'), /injected directory sync failure/);
  mocked.mock.restore();
  const bytes = fs.readFileSync(f.file, 'utf8');
  const records = Object.values(JSON.parse(bytes).calls) as Array<{ id: string; requestId: string }>;
  assert.equal(records.length, 1); assert.equal(records[0].requestId, request.requestId);
  assert.equal(f.store.get(records[0].id), undefined);
  assert.equal(f.store.readCallRequestJournal().state, 'uncertain');
  assert.throws(() => readCallRequestReceipt(f.store, request.requestId), unavailable);
  assert.throws(() => readCallRequestReceipt(f.store, randomUUID()), unavailable);
  assert.throws(() => f.store.reserve(input(), 'authenticated_broker_control'), /write outcome is uncertain/);
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  f.store.close();
  const restarted = new BrokerStore(f.directory); t.after(() => restarted.close());
  const receipt = readCallRequestReceipt(restarted, request.requestId);
  assert.equal(receipt.call.status, 'prepared'); assert.equal(receipt.call.id, records[0].id);
  assert.equal(restarted.reserve(request, 'authenticated_broker_control').existing, true);
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
});

for (const failure of ['missing-file', 'malformed-file', 'cache-mismatch', 'foreign-lock', 'missing-lock'] as const) {
  test(`exact lookup returns unavailable for ${failure} and leaves storage alone`, t => {
    const f = fixture(t); const request = input(); f.store.reserve(request, 'authenticated_broker_control');
    const bytes = fs.readFileSync(f.file, 'utf8'); const lock = fs.readFileSync(f.lock, 'utf8');
    if (failure === 'missing-file') fs.unlinkSync(f.file);
    if (failure === 'malformed-file') fs.writeFileSync(f.file, '{bad-json');
    if (failure === 'cache-mismatch') fs.writeFileSync(f.file, JSON.stringify({ version: 1, calls: {} }));
    if (failure === 'foreign-lock') fs.writeFileSync(f.lock, JSON.stringify({ pid: process.pid, id: 'foreign' }));
    if (failure === 'missing-lock') fs.unlinkSync(f.lock);
    const fileBefore = fs.existsSync(f.file) ? fs.readFileSync(f.file, 'utf8') : undefined;
    const lockBefore = fs.existsSync(f.lock) ? fs.readFileSync(f.lock, 'utf8') : undefined;
    assert.throws(() => readCallRequestReceipt(f.store, request.requestId), unavailable);
    assert.throws(() => readCallRequestReceipt(f.store, randomUUID()), unavailable);
    assert.equal(fs.existsSync(f.file) ? fs.readFileSync(f.file, 'utf8') : undefined, fileBefore);
    assert.equal(fs.existsSync(f.lock) ? fs.readFileSync(f.lock, 'utf8') : undefined, lockBefore);
    fs.writeFileSync(f.file, bytes, { mode: 0o600 }); fs.writeFileSync(f.lock, lock, { mode: 0o600 });
  });
}

test('restarted duplicate request keys remain unavailable rather than choosing either call', t => {
  const f = fixture(t); const request = input(); const first = f.store.reserve(request, 'authenticated_broker_control').call;
  f.store.close();
  const second = structuredClone(first); second.id = randomUUID();
  second.callAssignment!.value.brokerCallId = second.id;
  fs.writeFileSync(f.file, JSON.stringify({ version: 1, calls: { [first.id]: first, [second.id]: second } }), { mode: 0o600 });
  const restarted = new BrokerStore(f.directory); t.after(() => restarted.close());
  assert.throws(() => readCallRequestReceipt(restarted, request.requestId), unavailable);
  assert.throws(() => readCallRequestReceipt(restarted, randomUUID()), unavailable);
});

test('actual HTTP receipt recovers historical calls outside recipient policy without calling any external port', async t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-receipt-http-'));
  const f = { directory, file: path.join(directory, 'calls.json'), store: new BrokerStore(directory) };
  let closeBroker: (() => Promise<void>) | undefined;
  t.after(async () => {
    try { if (closeBroker) await closeBroker(); else f.store.close(); }
    finally { fs.rmSync(directory, { recursive: true, force: true }); }
  });
  const request = input(); const saved = f.store.reserve(request, 'authenticated_broker_control').call;
  f.store.update(saved.id, { status: 'completed', endedAt: new Date().toISOString() });
  let externalCalls = 0;
  const forbidden = () => { externalCalls++; throw new Error('external port forbidden'); };
  const config: BrokerConfig = { host: '127.0.0.1', port: 3213, publicOrigin: 'https://phone.example.test',
    stateDir: f.directory, controlToken: 'c'.repeat(40), apiKey: 'fixture-key', model: 'gpt-live-1', voice: 'marin',
    twilioAccountSid: 'AC' + '1'.repeat(32), twilioAuthToken: 'fixture-twilio-secret', from: '+12025550100',
    allowedTo: ['+12025550999'], apiBaseUrl: 'https://api.example.test', phoneServiceSecret: 's'.repeat(40), maxCallSeconds: 60 };
  const broker = createBrokerServer({ config, store: f.store, primary: { readiness: forbidden } as never,
    provider: { calls: Object.assign(forbidden, { create: forbidden }) } as never,
    probe: forbidden, voiceFactory: forbidden, harkPrimaryReceiptSender: forbidden });
  await new Promise<void>(resolve => broker.server.listen(0, '127.0.0.1', resolve));
  closeBroker = () => broker.close();
  const base = `http://127.0.0.1:${(broker.server.address() as { port: number }).port}`;
  const headers = { authorization: `Bearer ${config.controlToken}` };
  const route = `/internal/broker/call-requests/${request.requestId}`;
  const before = fs.readFileSync(f.file, 'utf8');
  for (const suffix of ['', '']) {
    const response = await fetch(base + route + suffix, { headers });
    assert.equal(response.status, 200); assert.equal(response.headers.get('cache-control'), 'private, no-store');
    const receipt = await response.json() as { call: { id: string; status: string } };
    assert.equal(receipt.call.id, saved.id); assert.equal(receipt.call.status, 'completed');
    assert.equal(fs.readFileSync(f.file, 'utf8'), before);
  }
  for (const [url, auth, status, error] of [
    [route, {}, 401, 'unauthorized'], [route + '?refresh=1', headers, 400, 'invalid_call_request'],
    [`/internal/broker/call-requests/${randomUUID()}`, headers, 404, 'call_request_not_found'],
  ] as const) {
    const response = await fetch(base + url, { headers: auth });
    assert.equal(response.status, status); assert.deepEqual(await response.json(), { error });
    assert.equal(response.headers.get('cache-control'), 'private, no-store');
  }
  assert.equal(externalCalls, 0); assert.equal(fs.readFileSync(f.file, 'utf8'), before);
});
