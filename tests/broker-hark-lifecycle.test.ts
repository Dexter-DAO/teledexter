import assert from 'node:assert/strict';
import { createHash, createHmac } from 'node:crypto';
import fs from 'node:fs';
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { createHarkLifecycleDelivery } from '../src/broker/harkLifecycle.js';
import { harkObservedStart, type HarkJournalEvent } from '../src/broker/harkJournal.js';
import { BrokerStore } from '../src/broker/store.js';
import { canonicalJson } from '../src/outbound/canonical.js';
import { makeResolvedHarkAssignment } from './fixtures/broker-hark.js';

const secret = 'offline-hark-lifecycle-callback-secret';
const queuedAt = '2026-09-23T19:31:00.000Z';
const answeredAt = '2026-09-23T19:31:05.250Z';
const endedAt = '2026-09-23T19:32:05.750Z';
const sid = `CA${'1'.repeat(32)}`;
interface Request {
  path: string; raw: string; body: Record<string, unknown>; digest: string;
  nonce: string; signature: string; kind: HarkJournalEvent['kind'];
}
type ResponseHook = (request: Request, response: ServerResponse) => boolean | Promise<boolean>;
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}
async function fixture(t: test.TestContext, hook?: ResponseHook) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-hark-lifecycle-'));
  let store = new BrokerStore(directory);
  const requests: Request[] = [];
  const errors: unknown[] = [];
  const nonces = new Set<string>();
  async function handle(req: IncomingMessage, res: ServerResponse) {
    assert.equal(req.method, 'POST');
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    const raw = Buffer.concat(chunks).toString('utf8');
    const body = JSON.parse(raw) as Record<string, unknown>;
    const timestamp = String(req.headers['x-dexter-alert-timestamp']);
    const nonce = String(req.headers['x-dexter-alert-nonce']);
    const digest = createHash('sha256').update(canonicalJson(body)).digest('hex');
    assert.match(timestamp, /^\d{10}$/);
    assert.ok(Math.abs(Date.now() - Number(timestamp) * 1000) < 10_000);
    assert.match(nonce, /^[A-Za-z0-9_-]{16,160}$/);
    assert.equal(nonces.has(nonce), false, 'Each transport attempt must have a fresh signature nonce');
    nonces.add(nonce);
    const requestPath = req.url!;
    const kind = requestPath === '/internal/voice/outbound-status' ? 'outbound_status'
      : requestPath === '/internal/voice/call-ended' ? 'call_ended' : 'hark_status';
    if (kind === 'hark_status') assert.equal(requestPath, `/internal/hark/v1/deliveries/${body.deliveryId}/status`);
    const signedPayload = kind === 'hark_status' ? `v1\n${timestamp}\n${nonce}\n${digest}`
      : `v2\n${requestPath}\n${timestamp}\n${nonce}\n${digest}`;
    const signature = String(req.headers[kind === 'hark_status' ? 'x-dexter-alert-signature' : 'x-dexter-callback-signature']);
    assert.equal(signature, `${kind === 'hark_status' ? 'v1' : 'v2'}=${createHmac('sha256', secret).update(signedPayload).digest('hex')}`);
    const request: Request = { path: requestPath, raw, body, digest, nonce, signature, kind };
    requests.push(request);
    if (await hook?.(request, res)) return;
    if (kind === 'outbound_status') { res.writeHead(204); res.end(); return; }
    res.setHeader('content-type', 'application/json');
    if (kind === 'call_ended') {
      res.end(JSON.stringify({ ok: true, id: '99999999-9999-4999-8999-999999999999', status: 'completed' }));
      return;
    }
    const { caseId, eventId, deliveryId, intentId, callSid, originTaskId, sourceEventId } = body;
    res.end(JSON.stringify({ v: 'hark.phone-callback-result.v1', ok: true, replay: false,
      caseId, eventId, deliveryId, intentId, callSid, originTaskId, sourceEventId, caseState: 'open' }));
  }
  const server = createServer((req, res) => {
    void handle(req, res).catch(error => { errors.push(error); res.writeHead(500); res.end(); });
  });
  await new Promise<void>((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
  const address = server.address();
  assert.ok(address && typeof address === 'object');
  const apiBaseUrl = `http://127.0.0.1:${address.port}`;
  const delivery = () => createHarkLifecycleDelivery({ store, apiBaseUrl, signingSecret: secret });
  t.after(async () => {
    store.close();
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
    fs.rmSync(directory, { recursive: true, force: true });
    assert.deepEqual(errors, [], 'The local API must accept the actual signed HTTP messages');
  });
  const call = store.reserveHark(makeResolvedHarkAssignment()).call;
  return { id: call.id, requests, errors, directory, delivery,
    get store() { return store; },
    restart() { store.close(); store = new BrokerStore(directory); return delivery(); },
    bound() { store.update(call.id, { callSid: sid }); },
    complete() {
      store.update(call.id, { callSid: sid });
      store.observeHarkProvider(call.id, 'queued', queuedAt);
      store.observeHarkProvider(call.id, 'in-progress', answeredAt);
      store.observeHarkMediaStarted(call.id, '2026-09-23T19:31:05.500Z');
      const startedAt = harkObservedStart(store.get(call.id)!);
      assert.equal(startedAt, answeredAt);
      return store.observeHarkProvider(call.id, 'completed', endedAt,
        { startedAt: startedAt!, endedAt, durationSeconds: Math.floor((Date.parse(endedAt) - Date.parse(startedAt!)) / 1000) });
    } };
}

test('actual signed HTTP drains queued, answered, streaming and terminal records before the ended receipt', async t => {
  const terminalReached = deferred();
  const releaseTerminal = deferred();
  const f = await fixture(t, async (request, res) => {
    if (request.kind !== 'outbound_status' || request.body.state !== 'completed') return false;
    terminalReached.resolve();
    await releaseTerminal.promise;
    res.writeHead(204); res.end(); return true;
  });
  f.complete();
  const lifecycle = f.delivery();
  const flush = lifecycle.flush();
  assert.equal(lifecycle.flush(), flush, 'Concurrent maintenance reuses one drain');
  await terminalReached.promise;
  try {
    assert.equal(f.requests.some(request => request.kind === 'call_ended'), false);
    assert.equal(f.store.get(f.id)?.harkJournal?.events[`voice-ended:${f.id}`]?.attempts, 0);
  } finally { releaseTerminal.resolve(); }
  await flush;
  assert.deepEqual(f.requests.filter(request => request.kind === 'hark_status').map(request => request.body.status),
    ['queued', 'answered', 'streaming', 'completed']);
  assert.deepEqual(f.requests.filter(request => request.kind === 'outbound_status').map(request => request.body.state),
    ['queued', 'answered', 'streaming', 'completed']);
  assert.equal(f.requests.at(-1)?.kind, 'call_ended');
  assert.equal(f.requests.at(-1)?.body.started_at, answeredAt);
  assert.equal(f.requests.at(-1)?.body.ended_at, endedAt);
  assert.equal(f.requests.at(-1)?.body.duration_seconds, 60);
  assert.equal(f.requests.at(-1)?.body.caller_user_id, makeResolvedHarkAssignment().callAssignment.hark.ownerSupabaseUserId);
  assert.deepEqual(f.store.pendingHarkEvents(), []);
  assert.ok(Object.values(f.store.get(f.id)!.harkJournal!.events).every(event => event.state === 'delivered' && event.attempts === 1));
  const count = f.requests.length;
  await lifecycle.flush();
  await f.restart().flush();
  assert.equal(f.requests.length, count, 'Acknowledged entries are not resent after restart');
});

for (const [kind, failure] of [['hark_status', 401], ['outbound_status', 503], ['call_ended', 401]] as const) {
  test(`${kind} HTTP${failure} retries once after restart with exact saved body and new signature nonce`, async t => {
    let rejected = false;
    const f = await fixture(t, (request, res) => {
      if (request.kind !== kind || rejected) return false;
      rejected = true;
      res.writeHead(failure); res.end('temporarily unavailable'); return true;
    });
    f.complete();
    await f.delivery().flush();
    const first = f.requests.find(request => request.kind === kind)!;
    const saved = Object.values(f.store.get(f.id)!.harkJournal!.events).find(event => event.kind === kind && event.bodyDigest === first.digest)!;
    assert.equal(saved.state, 'pending');
    assert.equal(saved.attempts, 1);
    assert.equal(saved.lastOutcome?.httpStatus, failure);
    const before = fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8');
    const restarted = f.restart();
    assert.equal(fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8'), before);
    await restarted.flush();
    const repeated = f.requests.filter(request => request.kind === kind && request.digest === first.digest);
    assert.equal(repeated.length, 2);
    assert.equal(repeated[1].raw, first.raw);
    assert.equal(repeated[1].digest, saved.bodyDigest);
    assert.notEqual(repeated[1].nonce, first.nonce);
    assert.notEqual(repeated[1].signature, first.signature);
    const accepted = f.store.get(f.id)!.harkJournal!.events[saved.id]!;
    assert.equal(accepted.state, 'delivered');
    assert.equal(accepted.attempts, 2);
    assert.deepEqual(accepted.body, saved.body);
    assert.deepEqual(f.store.pendingHarkEvents(), []);
    assert.equal(f.requests.filter(request => request.kind === 'call_ended').length, kind === 'call_ended' ? 2 : 1);
  });
}

test('HTTP409 billing refusal is durable and cannot unlock call-ended on subsequent flush or restart', async t => {
  const f = await fixture(t, (request, res) => {
    if (request.kind !== 'outbound_status') return false;
    res.writeHead(409); res.end('identity conflict'); return true;
  });
  f.complete();
  await f.delivery().flush();
  const statuses = f.requests.filter(request => request.kind === 'outbound_status');
  assert.equal(statuses.length, 1);
  assert.equal(statuses[0].body.state, 'queued');
  const item = f.store.get(f.id)!.harkJournal!.events[`voice-status:${f.id}:queued`]!;
  assert.equal(item.state, 'rejected');
  assert.equal(item.lastOutcome?.httpStatus, 409);
  await f.delivery().flush();
  await f.restart().flush();
  assert.equal(f.requests.filter(request => request.kind === 'outbound_status').length, 1);
  assert.equal(f.requests.some(request => request.kind === 'call_ended'), false);
  assert.equal(f.store.get(f.id)?.harkJournal?.events[`voice-ended:${f.id}`]?.attempts, 0);
});

test('unexpected status200 remains pending and never qualifies as status204 acknowledgment', async t => {
  const f = await fixture(t, (request, res) => {
    if (request.kind !== 'outbound_status') return false;
    res.setHeader('content-type', 'application/json'); res.end(JSON.stringify({ ok: true })); return true;
  });
  f.complete();
  await f.delivery().flush();
  const item = f.store.get(f.id)!.harkJournal!.events[`voice-status:${f.id}:queued`]!;
  assert.equal(item.state, 'pending');
  assert.equal(item.lastOutcome?.reason, 'invalid_response');
  assert.equal(f.requests.some(request => request.kind === 'call_ended'), false);
});

test('missing SID, unknown dispatch and an ordinary legacy call produce no fabricated callbacks', async t => {
  const f = await fixture(t);
  f.store.update(f.id, { status: 'unknown' });
  const legacy = f.store.reserve({ requestId: 'retained-legacy-request', direction: 'outbound', purpose: 'Existing call', number: '+12025550124' }).call;
  f.store.update(legacy.id, { status: 'completed', callSid: `CA${'2'.repeat(32)}`, endedAt });
  const originalLegacy = f.store.get(legacy.id);
  await f.delivery().flush();
  assert.deepEqual(f.requests, []);
  await f.restart().flush();
  assert.deepEqual(f.requests, []);
  assert.deepEqual(f.store.get(legacy.id), originalLegacy);
  assert.equal(f.store.get(f.id)?.status, 'unknown');
});

test('untimed no-answer sends exact terminal status while retaining the missing billing timing', async t => {
  const f = await fixture(t); f.bound();
  f.store.observeHarkProvider(f.id, 'queued', queuedAt);
  f.store.observeHarkProvider(f.id, 'no-answer', endedAt);
  await f.delivery().flush();
  assert.deepEqual(f.requests.filter(request => request.kind === 'outbound_status').map(request => request.body.state), ['queued', 'no-answer']);
  assert.deepEqual(f.requests.filter(request => request.kind === 'hark_status').map(request => request.body.status), ['queued', 'no-answer']);
  assert.equal(f.requests.some(request => request.kind === 'call_ended'), false);
  const closed = f.store.get(f.id)!;
  assert.equal(closed.harkJournal?.ended?.timing, undefined);
  assert.equal(harkObservedStart(closed), undefined);
  const count = f.requests.length;
  await f.restart().flush();
  assert.equal(f.requests.length, count);
  assert.deepEqual(f.store.get(f.id)?.harkJournal, closed.harkJournal);
});
