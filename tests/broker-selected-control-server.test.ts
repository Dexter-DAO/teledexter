import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { createBrokerServer, type BrokerServerOptions } from '../src/broker/server.js';
import { BrokerStore } from '../src/broker/store.js';
import { PHONE_OWNER_CALL_PURPOSE } from '../src/broker/callRequestReceipt.js';
import type { BrokerConfig } from '../src/broker/config.js';

const callSid = `CA${'3'.repeat(32)}`;
const selectedRequest = () => ({ requestId: randomUUID(), purpose: PHONE_OWNER_CALL_PURPOSE, to: '+12025550123',
  continuation: { originalRequestId: randomUUID(), questionId: randomUUID(), questionRevision: 1 as const } });

async function fixture(t: test.TestContext, mode: 'ready' | 'missing-context' | 'missing-selected' | 'unknown' = 'ready') {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-selected-control-'));
  const store = new BrokerStore(directory);
  const config: BrokerConfig = { host: '127.0.0.1', port: 3213, publicOrigin: 'https://phone.example.test',
    stateDir: directory, controlToken: 'c'.repeat(40), apiKey: 'fixture-key', model: 'gpt-live-1', voice: 'marin',
    twilioAccountSid: `AC${'1'.repeat(32)}`, twilioAuthToken: 'fixture-token', from: '+12025550100',
    allowedTo: ['+12025550123'], apiBaseUrl: 'https://api.example.test', phoneServiceSecret: 's'.repeat(40), maxCallSeconds: 60 };
  const seen = { readiness: 0, probe: 0, dial: 0, association: 0, primaryWork: 0 };
  const forbidden = async () => { seen.primaryWork++; throw new Error('no Primary work before answered-call admission'); };
  let expected: ReturnType<typeof selectedRequest> | undefined;
  const persisted = () => {
    assert.ok(expected);
    const saved = store.list().find(call => call.requestId === expected.requestId)!;
    assert.ok(saved);
    assert.deepEqual(saved.continuation, expected.continuation);
    const disk = JSON.parse(fs.readFileSync(path.join(directory, 'calls.json'), 'utf8'));
    assert.deepEqual(disk.calls[saved.id].continuation, expected.continuation);
    assert.equal(disk.calls[saved.id].callAssignment.origin, 'authenticated_broker_control');
  };
  const providerIdentity = { sid: callSid, accountSid: config.twilioAccountSid, from: config.from, to: config.allowedTo[0]! };
  const calls = Object.assign(() => ({ async update() { return { ...providerIdentity, status: 'completed' }; }, fetch: forbidden }), {
    async create() { seen.dial++; persisted(); if (mode === 'unknown') throw new Error('lost provider reply');
      return { ...providerIdentity, status: 'queued' }; },
  });
  const options: BrokerServerOptions = { config, store,
    primary: { async readiness() { seen.readiness++; persisted(); return { eligible: true }; },
      submit: forbidden, result: forbidden, reply: forbidden, openCall: forbidden, captureTurn: forbidden } as never,
    provider: { calls } as never,
    probe: async () => { seen.probe++; persisted(); return {} as never; }, voiceFactory: () => { throw new Error('no voice before provider answer'); },
    harkPrimaryReceiptSender: forbidden,
    ...(mode !== 'missing-context' ? { eligibleContext: { fetch: forbidden, assert: forbidden, delivery: forbidden } } : {}),
    ...(mode !== 'missing-selected' ? { selectedQuestion: {
      association: { async associate() { seen.association++; throw new Error('not answered yet'); } },
      requests: { associate: forbidden, result: forbidden, question: forbidden, reply: forbidden },
    } } : {}),
  };
  const broker = createBrokerServer(options);
  await new Promise<void>(resolve => broker.server.listen(0, '127.0.0.1', resolve));
  const url = `http://127.0.0.1:${(broker.server.address() as { port: number }).port}`;
  t.after(async () => {
    try { await broker.close(); }
    finally { store.close(); fs.rmSync(directory, { recursive: true, force: true }); }
  });
  return { store, seen, config, directory, broker,
    setExpected(value: ReturnType<typeof selectedRequest>) { expected = value; },
    post(value: unknown, authenticated = true) { return fetch(url + '/internal/broker/calls', { method: 'POST',
      headers: { 'content-type': 'application/json', ...(authenticated ? { authorization: `Bearer ${config.controlToken}` } : {}) },
      body: JSON.stringify(value) }); },
    lookup(id: string) { return fetch(url + `/internal/broker/call-requests/${id}`, { headers: { authorization: `Bearer ${config.controlToken}` } }); },
  };
}

for (const mode of ['missing-context', 'missing-selected'] as const) {
  test(`new selected call refuses before reservation when ${mode}`, async t => {
    const f = await fixture(t, mode); const request = selectedRequest(); f.setExpected(request);
    const response = await f.post(request);
    assert.equal(response.status, 503); assert.deepEqual(await response.json(), { error: 'call_continuation_unavailable' });
    assert.deepEqual(f.store.list(), []);
    assert.deepEqual(f.seen, { readiness: 0, probe: 0, dial: 0, association: 0, primaryWork: 0 });
  });
}

test('actual selected control HTTP persists exact selector before readiness and dial; replay performs no new work', async t => {
  const f = await fixture(t); const request = selectedRequest(); f.setExpected(request);
  const first = await f.post(request); assert.equal(first.status, 201);
  const body = await first.json() as { replayed: boolean; call: { id: string; requestId: string; continuation: unknown; status: string } };
  assert.equal(body.replayed, false); assert.equal(body.call.requestId, request.requestId);
  assert.deepEqual(body.call.continuation, request.continuation); assert.equal(body.call.status, 'queued');
  const before = fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8');
  const replay = await f.post(request); assert.equal(replay.status, 200);
  assert.deepEqual(await replay.json(), { ...body, replayed: true });
  assert.equal(fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8'), before);
  assert.deepEqual(f.seen, { readiness: 1, probe: 1, dial: 1, association: 0, primaryWork: 0 });
  const receipt = await f.lookup(request.requestId); assert.equal(receipt.status, 200);
  const recovered = await receipt.json() as { call: { id: string; continuation: unknown } };
  assert.equal(recovered.call.id, body.call.id); assert.deepEqual(recovered.call.continuation, request.continuation);
});

test('selected control changed question, original request, or omitted selector conflicts before another dial', async t => {
  const f = await fixture(t); const request = selectedRequest(); f.setExpected(request);
  assert.equal((await f.post(request)).status, 201);
  const { continuation: _selected, ...ordinary } = request;
  for (const changed of [ordinary, { ...request, continuation: { ...request.continuation, questionId: randomUUID() } },
    { ...request, continuation: { ...request.continuation, originalRequestId: randomUUID() } }]) {
    const response = await f.post(changed); assert.equal(response.status, 409);
    assert.deepEqual(await response.json(), { error: 'call_continuation_changed' });
  }
  assert.equal(f.seen.dial, 1); assert.equal(f.seen.readiness, 1);
});

test('unknown selected dispatch recovers the same receipt without a second provider request', async t => {
  const f = await fixture(t, 'unknown'); const request = selectedRequest(); f.setExpected(request);
  assert.equal((await f.post(request)).status, 503);
  const saved = f.store.list()[0]!;
  assert.equal(saved.status, 'unknown'); assert.equal(saved.callSid, undefined);
  const before = fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8');
  const replay = await f.post(request); assert.equal(replay.status, 200);
  const receipt = await replay.json() as { replayed: boolean; call: { id: string; status: string } };
  assert.equal(receipt.replayed, true); assert.equal(receipt.call.id, saved.id); assert.equal(receipt.call.status, 'unknown');
  assert.equal(fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8'), before);
  assert.deepEqual(f.seen, { readiness: 1, probe: 1, dial: 1, association: 0, primaryWork: 0 });
});

test('historical selected receipt survives disabled new-call composition', async t => {
  const f = await fixture(t, 'missing-selected'); const request = selectedRequest(); f.setExpected(request);
  const saved = f.store.reserve({ requestId: request.requestId, direction: 'outbound', purpose: request.purpose,
    number: request.to, continuation: request.continuation }, 'authenticated_broker_control').call;
  f.store.update(saved.id, { status: 'completed', endedAt: new Date().toISOString() });
  const response = await f.post(request); assert.equal(response.status, 200);
  const receipt = await response.json() as { replayed: boolean; call: { id: string; status: string } };
  assert.equal(receipt.replayed, true); assert.equal(receipt.call.id, saved.id); assert.equal(receipt.call.status, 'completed');
  assert.deepEqual(f.seen, { readiness: 0, probe: 0, dial: 0, association: 0, primaryWork: 0 });
});

test('unauthenticated or malformed selection cannot create a call', async t => {
  const f = await fixture(t); const request = selectedRequest(); f.setExpected(request);
  assert.equal((await f.post(request, false)).status, 401);
  const response = await f.post({ ...request, continuation: { ...request.continuation, questionRevision: 2 } });
  assert.equal(response.status, 400); assert.deepEqual(await response.json(), { error: 'invalid_call_continuation' });
  assert.deepEqual(f.store.list(), []); assert.equal(f.seen.dial, 0);
});
