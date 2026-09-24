import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import Twilio from 'twilio';
import { createBrokerServer, type BrokerServerOptions } from '../src/broker/server.js';
import { BrokerStore, type BrokerCall } from '../src/broker/store.js';
import type { BrokerConfig } from '../src/broker/config.js';
import { PHONE_OWNER_CALL_PURPOSE, callRequestReceiptSchema } from '../src/broker/callRequestReceipt.js';
import { outboundSettlementFromObservation } from '../src/broker/outboundObservation.js';
import { type OutboundRetryOf, type SettledOutboundSettlement } from '../src/broker/outboundSettlement.js';

const selected = () => ({ requestId: randomUUID(), purpose: PHONE_OWNER_CALL_PURPOSE, to: '+12025550123',
  continuation: { originalRequestId: randomUUID(), questionId: randomUUID(), questionRevision: 1 as const } });
type SelectedRequest = ReturnType<typeof selected> & { retryOf?: OutboundRetryOf };
type ProviderRecord = { sid: string; accountSid: string; from: string; to: string; status: string };
function bounded<T>(promise: Promise<T>, label: string, milliseconds = 2000): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  return Promise.race([promise, new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} exceeded ${milliseconds}ms`)), milliseconds);
  })]).finally(() => { if (timer !== undefined) clearTimeout(timer); });
}
function deferred() {
  let release = () => {};
  const promise = new Promise<void>(resolve => { release = resolve; });
  return { promise, release };
}

async function fixture(t: test.TestContext) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-outbound-selected-'));
  const file = path.join(directory, 'calls.json');
  const config: BrokerConfig = { host: '127.0.0.1', port: 3213, publicOrigin: 'https://phone.example.test',
    stateDir: directory, controlToken: 'c'.repeat(40), apiKey: 'fixture-key', model: 'gpt-live-1', voice: 'marin',
    twilioAccountSid: `AC${'1'.repeat(32)}`, twilioAuthToken: 'fixture-signing-key', from: '+12025550100',
    allowedTo: ['+12025550123'], apiBaseUrl: 'https://api.example.test', phoneServiceSecret: 's'.repeat(40), maxCallSeconds: 60 };
  let store = new BrokerStore(directory);
  let broker: ReturnType<typeof createBrokerServer> | undefined;
  let closePromise: Promise<void> | undefined;
  let origin = '';
  const holds: Array<ReturnType<typeof deferred>> = [];
  const records = new Map<string, ProviderRecord>();
  const seen = { readiness: 0, probe: 0, create: 0, fetch: 0, update: 0, forbidden: 0 };
  const controls: { eligible: boolean; beforeReadiness?: () => Promise<void>; beforeProbe?: () => Promise<void>;
    create?: (call: BrokerCall, record: ProviderRecord) => Promise<Partial<ProviderRecord>>;
    fetch?: (record: ProviderRecord) => Promise<Partial<ProviderRecord>>;
    update?: (record: ProviderRecord) => Promise<Partial<ProviderRecord>> } = { eligible: true };
  const disk = (): Record<string, BrokerCall> => JSON.parse(fs.readFileSync(file, 'utf8')).calls;
  const forbidden = () => { seen.forbidden++; throw new Error('No answered-call work in transport qualification'); };
  const calls = Object.assign((sid: string) => ({
    async fetch() {
      seen.fetch++; const record = records.get(sid); assert.ok(record);
      return controls.fetch ? controls.fetch(structuredClone(record)) : structuredClone(record);
    },
    async update() {
      seen.update++; const record = records.get(sid); assert.ok(record);
      const terminal = { ...record, status: 'completed' };
      const observed = controls.update ? await controls.update(terminal) : terminal;
      return observed;
    },
  }), {
    async create(input: { url: string; from: string; to: string }) {
      seen.create++;
      const id = input.url.split('/').at(-1)!;
      const saved = disk()[id]; assert.ok(saved);
      assert.equal(saved.status, 'dialing');
      assert.ok(saved.outboundObservation?.dispatchStartedAt, 'the fence must reach disk before create');
      assert.equal(saved.outboundObservation.callId, saved.id);
      assert.equal(saved.outboundObservation.requestId, saved.requestId);
      assert.equal(saved.outboundObservation.reservedAt, saved.createdAt);
      assert.deepEqual(saved.outboundObservation.providerIdentity, { accountSid: config.twilioAccountSid,
        from: input.from, to: input.to });
      assert.equal(saved.callSid, undefined);
      const record: ProviderRecord = { sid: `CA${seen.create.toString(16).padStart(32, '0')}`,
        accountSid: config.twilioAccountSid, from: input.from, to: input.to, status: 'queued' };
      records.set(record.sid, record);
      return controls.create ? controls.create(saved, structuredClone(record)) : record;
    },
  });
  function start() {
    broker = createBrokerServer({ config, store,
      primary: { async readiness() {
        seen.readiness++; await controls.beforeReadiness?.();
        return { eligible: controls.eligible, reason: controls.eligible ? 'ready' : 'runtime_unavailable' };
      }, openCall: forbidden, submit: forbidden, result: forbidden, reply: forbidden, captureTurn: forbidden,
      close: forbidden } as never,
      provider: { calls } as never,
      probe: async () => { seen.probe++; await controls.beforeProbe?.(); return {} as never; },
      voiceFactory: forbidden, harkPrimaryReceiptSender: forbidden,
      eligibleContext: { fetch: forbidden, assert: forbidden, delivery: forbidden },
      selectedQuestion: { association: { associate: forbidden }, requests: {
        associate: forbidden, result: forbidden, question: forbidden, reply: forbidden,
      } },
    } satisfies BrokerServerOptions);
    closePromise = undefined;
    const server = broker.server;
    return bounded(new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.listen(0, '127.0.0.1', () => { server.off('error', reject);
        const address = server.address(); assert.ok(address && typeof address !== 'string');
        origin = `http://127.0.0.1:${address.port}`; resolve(); });
    }), 'listen');
  }
  function close() {
    for (const hold of holds) hold.release();
    if (closePromise) return closePromise;
    if (!broker) { store.close(); return Promise.resolve(); }
    closePromise = broker.close(); return closePromise;
  }
  t.after(async () => {
    try { await bounded(close(), 'broker close'); }
    finally { broker?.server.closeAllConnections(); broker?.server.close(); store.close(); fs.rmSync(directory, { recursive: true, force: true }); }
  });
  await start();
  async function request(route: string, value?: unknown, headers: Record<string, string> = {}) {
    const response = await fetch(origin + route, { method: value === undefined ? 'GET' : 'POST',
      headers: { authorization: `Bearer ${config.controlToken}`, ...(value === undefined ? {} : { 'content-type': 'application/json' }), ...headers },
      body: value === undefined ? undefined : JSON.stringify(value), signal: AbortSignal.timeout(2000) });
    return { status: response.status, body: await response.json() as unknown, cacheControl: response.headers.get('cache-control') };
  }
  function saved(requestId: string): BrokerCall {
    const call = store.list().find(value => value.requestId === requestId); assert.ok(call); return call;
  }
  async function callback(callId: string, sid: string, status: string, patch: Record<string, string> = {}, signature?: string) {
    const route = `/broker/status/${callId}`;
    const params = { AccountSid: config.twilioAccountSid, CallSid: sid, From: config.from, To: config.allowedTo[0],
      Direction: 'outbound-api', CallStatus: status, ...patch };
    const response = await fetch(origin + route, { method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded',
      'x-twilio-signature': signature ?? Twilio.getExpectedTwilioSignature(config.twilioAuthToken, config.publicOrigin + route, params) },
      body: new URLSearchParams(params), signal: AbortSignal.timeout(2000) });
    return { status: response.status, body: await response.json() as unknown };
  }
  return { config, controls, seen, file, directory, disk, saved, callback, records,
    get store() { return store; }, get broker() { assert.ok(broker); return broker; },
    hold() { const hold = deferred(); holds.push(hold); return hold; },
    post(value: unknown) { return request('/internal/broker/calls', value); },
    lookup(requestId: string) { return request(`/internal/broker/call-requests/${requestId}`); },
    reconcile(id: string) { return request('/internal/broker/reconcile', { id }); },
    end(id: string) { return request('/internal/broker/end', { id }); },
    close,
    async restart() { await bounded(close(), 'restart close'); broker?.server.closeAllConnections();
      store = new BrokerStore(directory); await start(); },
  };
}

function settlement(call: BrokerCall): SettledOutboundSettlement {
  const value = outboundSettlementFromObservation(call); assert.ok(value?.state === 'settled'); return value;
}
function retry(request: SelectedRequest, call: BrokerCall): SelectedRequest {
  return { ...request, requestId: randomUUID(), retryOf: {
    callId: call.id, requestId: call.requestId, settlementDigest: settlement(call).digest,
  } };
}

test('readiness refusal seals never dispatched; exact HTTP lookup/replay/restart performs no dial', async t => {
  const f = await fixture(t), request = selected(); f.controls.eligible = false;
  f.controls.beforeReadiness = async () => {
    const saved = f.saved(request.requestId), persisted = f.disk()[saved.id];
    assert.deepEqual(persisted.continuation, request.continuation);
    assert.ok(persisted.outboundObservation);
    assert.equal(persisted.outboundObservation.dispatchStartedAt, undefined);
  };
  const first = await f.post(request);
  assert.equal(first.status, 503); assert.deepEqual(first.body, { error: 'call_request_unavailable' });
  const saved = f.saved(request.requestId), receipt = settlement(saved);
  assert.equal(saved.status, 'failed'); assert.equal(saved.callSid, undefined);
  assert.equal(receipt.kind, 'never_dispatched');
  if (receipt.kind === 'never_dispatched') assert.equal(receipt.reason, 'preflight_refused');
  const bytes = fs.readFileSync(f.file, 'utf8');
  for (let i = 0; i < 2; i++) {
    const lookup = await f.lookup(request.requestId); assert.equal(lookup.status, 200);
    const parsed = callRequestReceiptSchema.parse(lookup.body);
    assert.deepEqual(parsed.call.outboundSettlement, receipt);
    assert.equal(lookup.cacheControl, 'private, no-store');
    const replay = await f.post(request); assert.equal(replay.status, 200);
    assert.equal((replay.body as { replayed: boolean }).replayed, true);
    assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  }
  await f.restart();
  assert.equal((await f.post(request)).status, 200);
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  assert.deepEqual(f.seen, { readiness: 1, probe: 0, create: 0, fetch: 0, update: 0, forbidden: 0 });
});

test('ambiguous create retains the disk fence/no SID; exact recovery never redials and new retry refuses', async t => {
  const f = await fixture(t), request = selected();
  f.controls.create = async () => { throw new Error('lost create response'); };
  assert.equal((await f.post(request)).status, 503);
  const saved = f.saved(request.requestId);
  assert.equal(saved.status, 'unknown'); assert.equal(saved.callSid, undefined);
  assert.ok(saved.outboundObservation?.dispatchStartedAt);
  assert.equal(saved.outboundObservation.settlement, undefined);
  const bytes = fs.readFileSync(f.file, 'utf8');
  assert.equal((await f.post(request)).status, 200);
  const lookup = await f.lookup(request.requestId); assert.equal(lookup.status, 200);
  assert.equal(callRequestReceiptSchema.parse(lookup.body).call.outboundSettlement?.state, 'unresolved');
  const refused = await f.post({ ...request, requestId: randomUUID(), retryOf: {
    callId: saved.id, requestId: saved.requestId, settlementDigest: 'a'.repeat(64),
  } });
  assert.equal(refused.status, 409); assert.deepEqual(refused.body, { error: 'call_retry_unavailable' });
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  await f.restart(); assert.equal((await f.post(request)).status, 200);
  assert.equal(f.seen.create, 1); assert.equal(f.seen.readiness, 1); assert.equal(f.seen.probe, 1);
  assert.equal(f.seen.fetch + f.seen.update + f.seen.forbidden, 0);
});

test('signed terminal callback before failed create reply preserves accepted SID and first settlement', async t => {
  const f = await fixture(t), request = selected();
  f.controls.create = async (call, record) => {
    const response = await f.callback(call.id, record.sid, 'no-answer'); assert.equal(response.status, 200);
    const saved = f.disk()[call.id];
    assert.equal(saved.callSid, record.sid);
    assert.equal(saved.outboundObservation?.accepted?.callSid, record.sid);
    assert.equal(saved.outboundObservation?.settlement?.kind, 'provider_terminal');
    throw new Error('create response lost after callback');
  };
  const response = await f.post(request); assert.equal(response.status, 200);
  assert.equal((response.body as { replayed: boolean }).replayed, true);
  const saved = f.saved(request.requestId), first = settlement(saved);
  assert.equal(saved.status, 'no-answer');
  assert.equal(first.kind, 'provider_terminal');
  if (first.kind === 'provider_terminal') assert.equal(first.source, 'signed_status_callback');
  assert.ok(saved.callSid);
  assert.equal((await f.callback(saved.id, saved.callSid, 'ringing')).status, 200);
  assert.equal((await f.callback(saved.id, saved.callSid, 'completed')).status, 200);
  assert.deepEqual(settlement(f.saved(request.requestId)), first);
  assert.equal(f.saved(request.requestId).status, 'no-answer');
  assert.equal((await f.post(request)).status, 200);
  assert.equal(f.seen.create, 1); assert.equal(f.seen.forbidden, 0);
});

test('wrong callback account/from/to/SID or signature cannot alter saved transport evidence', async t => {
  const f = await fixture(t), request = selected(); assert.equal((await f.post(request)).status, 201);
  const saved = f.saved(request.requestId); assert.ok(saved.callSid);
  const bytes = fs.readFileSync(f.file, 'utf8');
  const patches: Array<Record<string, string>> = [{ AccountSid: `AC${'e'.repeat(32)}` }, { From: '+12025550998' },
    { To: '+12025550999' }, { CallSid: `CA${'f'.repeat(32)}` }];
  for (const patch of patches) {
    assert.equal((await f.callback(saved.id, saved.callSid, 'completed', patch)).status, 403);
    assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  }
  assert.equal((await f.callback(saved.id, saved.callSid, 'completed', {}, 'invalid')).status, 403);
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  assert.equal(outboundSettlementFromObservation(f.saved(request.requestId))?.state, 'unresolved');
});

test('terminal callback after create catch records local failure proves provider outcome independently', async t => {
  const f = await fixture(t), request = selected();
  f.controls.create = async () => { throw Object.assign(new Error('provider error without retained refusal proof'), { status: 400 }); };
  assert.equal((await f.post(request)).status, 503);
  const call = f.saved(request.requestId);
  assert.equal(call.status, 'failed'); assert.equal(call.callSid, undefined);
  const sid = [...f.records.keys()][0]; assert.ok(sid);
  assert.equal(outboundSettlementFromObservation(f.saved(request.requestId))?.state, 'unresolved');
  assert.equal((await f.callback(call.id, sid, 'no-answer')).status, 200);
  const receipt = settlement(f.saved(request.requestId));
  assert.equal(f.saved(request.requestId).status, 'failed');
  assert.ok(receipt.kind === 'provider_terminal'); assert.equal(receipt.providerStatus, 'no-answer');
  assert.equal(receipt.source, 'signed_status_callback');
  assert.deepEqual(callRequestReceiptSchema.parse((await f.lookup(request.requestId)).body).call.outboundSettlement, receipt);
});

test('maintenance still fetches an aged locally failed selected call and persists exact terminal evidence', async t => {
  const f = await fixture(t), request = selected(); assert.equal((await f.post(request)).status, 201);
  const call = f.saved(request.requestId);
  f.store.update(call.id, { status: 'failed', endedAt: new Date().toISOString(), finalized: true });
  f.controls.fetch = async record => ({ ...record, status: 'busy' });
  const clock = t.mock.method(Date, 'now', () => Date.parse(call.createdAt) + 61_000);
  try { await bounded(f.broker.maintain(), 'maintenance'); } finally { clock.mock.restore(); }
  assert.equal(f.seen.fetch, 1); assert.equal(f.seen.update, 0);
  const receipt = settlement(f.saved(request.requestId));
  assert.ok(receipt.kind === 'provider_terminal'); assert.equal(receipt.providerStatus, 'busy');
  assert.equal(receipt.source, 'authenticated_fetch');
  assert.equal(f.saved(request.requestId).status, 'failed');
  assert.equal(f.seen.forbidden, 0);
});

test('authenticated end needs the returned provider terminal observation, not its local endedAt write', async t => {
  const f = await fixture(t), request = selected(); assert.equal((await f.post(request)).status, 201);
  const call = f.saved(request.requestId);
  f.controls.update = async record => ({ ...record, status: 'in-progress' });
  assert.equal((await f.end(call.id)).status, 200);
  assert.ok(f.saved(request.requestId).endedAt);
  assert.equal(outboundSettlementFromObservation(f.saved(request.requestId))?.state, 'unresolved');
  f.controls.update = async record => record;
  assert.equal((await f.end(call.id)).status, 200);
  const receipt = settlement(f.saved(request.requestId));
  assert.ok(receipt.kind === 'provider_terminal'); assert.equal(receipt.providerStatus, 'completed');
  assert.equal(receipt.source, 'authenticated_update');
});

test('authenticated fetch refuses every missing or wrong returned provider identity field', async t => {
  const f = await fixture(t), request = selected(); assert.equal((await f.post(request)).status, 201);
  const call = f.saved(request.requestId), before = structuredClone(call.outboundObservation);
  for (const key of ['sid', 'accountSid', 'from', 'to'] as const) {
    f.controls.fetch = async record => { const partial: Partial<ProviderRecord> = { ...record, status: 'completed' }; delete partial[key]; return partial; };
    assert.equal((await f.reconcile(call.id)).status, 503);
    assert.deepEqual(f.saved(request.requestId).outboundObservation, before);
  }
  for (const patch of [{ sid: `CA${'e'.repeat(32)}` }, { accountSid: `AC${'e'.repeat(32)}` },
    { from: '+12025550998' }, { to: '+12025550999' }]) {
    f.controls.fetch = async record => ({ ...record, status: 'completed', ...patch });
    assert.equal((await f.reconcile(call.id)).status, 503);
    assert.deepEqual(f.saved(request.requestId).outboundObservation, before);
  }
  assert.equal((await f.lookup(request.requestId)).status, 200);
  assert.equal(f.seen.fetch, 8); assert.equal(f.seen.forbidden, 0);
});

test('malformed create identity remains uncertain despite returned SID; exact replay cannot dial again', async t => {
  const f = await fixture(t), request = selected();
  f.controls.create = async () => ({ sid: `CA${'e'.repeat(32)}`, status: 'queued' });
  assert.equal((await f.post(request)).status, 503);
  const call = f.saved(request.requestId);
  assert.equal(call.callSid, undefined); assert.ok(call.outboundObservation?.dispatchStartedAt);
  assert.equal(call.outboundObservation.accepted, undefined);
  assert.equal((await f.post(request)).status, 200); assert.equal(f.seen.create, 1);
});

test('retry child and reciprocal edge persist together before readiness; concurrent different child is refused', async t => {
  const f = await fixture(t), original = selected(); f.controls.eligible = false;
  assert.equal((await f.post(original)).status, 503);
  const parent = f.saved(original.requestId), childA = retry(original, parent), childB = retry(original, parent);
  f.controls.eligible = true;
  const entered = f.hold(), release = f.hold();
  f.controls.beforeReadiness = async () => {
    const child = f.saved(childA.requestId), rows = f.disk();
    assert.deepEqual(rows[child.id].retryOf, childA.retryOf);
    assert.deepEqual(rows[parent.id].retrySuccessor, { callId: child.id, requestId: child.requestId,
      settlementDigest: childA.retryOf!.settlementDigest });
    assert.equal(rows[child.id].outboundObservation?.dispatchStartedAt, undefined);
    entered.release(); await release.promise;
  };
  const pending = f.post(childA);
  void pending.catch(() => {}); // The bounded handshake may fail before its response is awaited.
  try {
    await bounded(entered.promise, 'child reservation');
    const refused = await f.post(childB);
    assert.equal(refused.status, 409); assert.deepEqual(refused.body, { error: 'call_request_conflict' });
    assert.equal(f.store.list().length, 2); assert.equal(f.seen.create, 0);
  } finally { release.release(); }
  assert.equal((await pending).status, 201);
  const child = f.saved(childA.requestId); assert.ok(child.callSid);
  assert.equal((await f.callback(child.id, child.callSid, 'no-answer')).status, 200);
  const bytes = fs.readFileSync(f.file, 'utf8');
  assert.equal((await f.post(childA)).status, 200);
  assert.equal((await f.post({ ...childA, retryOf: undefined })).status, 409);
  const { continuation: _continuation, retryOf: _retry, ...omittedSelection } = childA;
  for (const changed of [omittedSelection,
    { ...childA, continuation: { ...childA.continuation, questionId: randomUUID() } },
    { ...childA, continuation: { ...childA.continuation, originalRequestId: randomUUID() } }]) {
    const response = await f.post(changed);
    assert.equal(response.status, 409); assert.deepEqual(response.body, { error: 'call_request_conflict' });
  }
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  await f.restart();
  assert.equal((await f.post(childA)).status, 200);
  const recovered = callRequestReceiptSchema.parse((await f.lookup(childA.requestId)).body);
  assert.deepEqual(recovered.call.retryOf, childA.retryOf);
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  assert.equal(f.seen.create, 1); assert.equal(f.seen.readiness, 2); assert.equal(f.seen.forbidden, 0);
});

test('a completed provider call is not eligible for missed-call retry even if local status says failed', async t => {
  const f = await fixture(t), request = selected(); assert.equal((await f.post(request)).status, 201);
  const call = f.saved(request.requestId); assert.ok(call.callSid);
  f.store.update(call.id, { status: 'failed' });
  assert.equal((await f.callback(call.id, call.callSid, 'completed')).status, 200);
  const before = fs.readFileSync(f.file, 'utf8');
  const response = await f.post(retry(request, f.saved(request.requestId)));
  assert.equal(response.status, 409); assert.deepEqual(response.body, { error: 'call_retry_unavailable' });
  assert.equal(fs.readFileSync(f.file, 'utf8'), before);
  assert.equal(f.seen.create, 1); assert.equal(f.seen.readiness, 1);
});

test('a post-rename fence failure prevents create and makes exact GET/retry unavailable until restart', async t => {
  const f = await fixture(t), request = selected(); const fsync = fs.fsyncSync;
  let injected = false;
  const mocked = t.mock.method(fs, 'fsyncSync', (fd: number) => {
    if (!injected && fs.fstatSync(fd).isDirectory()
      && Object.values(f.disk()).some(call => call.requestId === request.requestId && call.outboundObservation?.dispatchStartedAt)) {
      injected = true; throw new Error('injected post-rename fence sync failure');
    }
    fsync(fd);
  });
  try { assert.equal((await f.post(request)).status, 503); } finally { mocked.mock.restore(); }
  assert.equal(injected, true); assert.equal(f.seen.create, 0);
  const bytes = fs.readFileSync(f.file, 'utf8');
  const diskCall = Object.values(f.disk()).find(call => call.requestId === request.requestId); assert.ok(diskCall);
  assert.ok(diskCall.outboundObservation?.dispatchStartedAt);
  assert.equal(f.store.readCallRequestJournal().state, 'uncertain');
  assert.equal((await f.lookup(request.requestId)).status, 503);
  assert.equal((await f.post(request)).status, 503);
  const child = { ...request, requestId: randomUUID(), retryOf: {
    callId: diskCall.id, requestId: diskCall.requestId, settlementDigest: 'a'.repeat(64),
  } };
  assert.equal((await f.post(child)).status, 503);
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  await f.restart();
  const restored = callRequestReceiptSchema.parse((await f.lookup(request.requestId)).body);
  assert.equal(restored.call.outboundSettlement?.state, 'unresolved');
  assert.equal((await f.post(request)).status, 200);
  assert.equal(fs.readFileSync(f.file, 'utf8'), bytes);
  assert.equal(f.seen.create, 0); assert.equal(f.seen.readiness, 1);
});
