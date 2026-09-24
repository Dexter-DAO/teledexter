import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createServer } from 'node:http';
import test from 'node:test';
import { BrokerStore } from '../src/broker/store.js';
import { createBrokerServer } from '../src/broker/server.js';
import { createPrimaryBackend, type BrokerDelegationRecord, type PrimaryBackendOptions } from '../src/broker/backend.js';
import { type PrimaryResult } from '../src/broker/primaryApi.js';
import { createPrimaryHarkReceiptDispatcher, createPrimaryHarkReceiptSender } from '../src/broker/harkPrimaryReceiptDelivery.js';
import { primaryHarkReceiptBindingFromCall, reducePrimaryHarkReceiptOutbox,
  PRIMARY_HARK_CALL_RECEIPT_ASSOCIATION_PENDING, type PrimaryHarkReceiptEntry } from '../src/broker/harkPrimaryReceiptOutbox.js';
import { makeResolvedHarkAssignment } from './fixtures/broker-hark.js';

const bindingId = '11111111-1111-4111-8111-111111111111';
const requestId = '22222222-2222-4222-8222-222222222222';
const messageId = '88888888-8888-4888-8888-888888888888';
const eventId = '99999999-9999-4999-8999-999999999999';
const aliasId = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
const sourceEventId = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';
const runId = 'cccccccc-cccc-4ccc-8ccc-cccccccccccc';
const receiptId = 'dddddddd-dddd-4ddd-8ddd-dddddddddddd';
const callSid = `CA${'1'.repeat(32)}`;
const observedAt = '2026-09-24T07:30:00.000Z';
const secret = 'offline-primary-hark-phone-service-secret';
const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status,
  headers: { 'content-type': 'application/json' } });

function fixture(t: test.TestContext, options: { alias?: boolean; hark?: boolean; state?: 'completed' | 'failed' } = {}) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-primary-hark-receipts-'));
  let store = new BrokerStore(directory);
  t.after(() => { store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  const hark = options.hark !== false;
  const call = hark ? store.reserveHark(makeResolvedHarkAssignment()).call
    : store.reserve({ requestId: 'legacy-call', number: '+12025550123', direction: 'outbound', purpose: 'Read my portfolio.' }).call;
  const assignmentRef = { assignmentId: call.id, revision: 1 as const, digest: 'a'.repeat(64) };
  store.update(call.id, { callSid, liveSessionId: 'session-receipts',
    primary: { bindingId, name: null, context: '', ...(hark ? { callAssignmentRef: assignmentRef } : {}) } });
  const originalRequestId = options.alias ? aliasId : requestId;
  const entry: BrokerDelegationRecord = {
    version: 1, requestId: originalRequestId, callId: call.id, callSid, bindingId, sessionId: 'session-receipts',
    delegationIds: ['delegation-receipt'], status: 'dispatch_unknown', updatedAt: observedAt,
    submission: { source: { callSid, itemId: 'caller-1', sourceEventId, revision: 1, receivedAt: observedAt, role: 'user' },
      text: 'Please explain the watch result.', ...(hark ? { callAssignmentRef: assignmentRef } : {}) },
    frozenCallerRevision: 1, firstSequence: 1, lastSequence: 1, completionBasis: 'speech_and_transcript_stability',
    sourceRevision: 1, capturedText: 'Please explain the watch result.',
    ...(options.alias ? { reconciliation: { outcome: 'admitted' as const, requestId: originalRequestId, bindingId,
      canonicalRequestId: requestId, result: { state: 'queued' as const, bindingId, requestId } } } : {}),
  };
  store.update(call.id, { delegations: { [originalRequestId]: entry } });
  const counters = { reads: 0, forbidden: 0, completedWrites: 0 };
  const savedResult: PrimaryResult = options.state === 'failed'
    ? { state: 'failed', bindingId, requestId, error: { code: 'test_failed', retryable: false } }
    : { state: 'completed', bindingId, requestId, assistant: { messageId, eventId, sequence: '3', text: 'The saved watch threshold was crossed.' } };
  const forbidden = async (): Promise<never> => { counters.forbidden++; throw new Error('Unexpected submission or caller capture'); };
  const api: PrimaryBackendOptions['api'] = { submit: forbidden, captureTurn: forbidden, question: forbidden, reply: forbidden,
    result: async (actualBinding, actualRequest) => {
      assert.equal(actualBinding, bindingId); assert.equal(actualRequest, requestId); counters.reads++;
      return structuredClone(savedResult);
    } };
  const recover = async () => {
    const backend = createPrimaryBackend({ callId: call.id, callSid, bindingId, api,
      store: { get: id => store.get(id), update: (id, patch) => {
        const next = patch.delegations?.[originalRequestId] as BrokerDelegationRecord | undefined;
        if (next?.primaryResult?.state === 'completed') {
          counters.completedWrites++;
          if (hark) assert.ok(patch.harkReceiptOutbox, 'Terminal result and outbox must be passed to the same write');
        }
        return store.update(id, patch);
      } } });
    return backend.flushPendingCaptures();
  };
  const item = (): PrimaryHarkReceiptEntry => Object.values(store.get(call.id)!.harkReceiptOutbox!.entries)[0]!;
  const result = (replay = false) => ({ receipt: { id: receiptId, kind: 'primary_outcome', bindingId,
    deliveryId: call.id, caseId: makeResolvedHarkAssignment().callAssignment.hark.caseId, requestId, runId, state: 'recorded' }, replay });
  return { directory, call, originalRequestId, counters, recover, item, result,
    get store() { return store; },
    restart() { store.close(); store = new BrokerStore(directory); },
    binding: () => primaryHarkReceiptBindingFromCall(store.get(call.id)!)!,
    dispatcher: (send: ReturnType<typeof createPrimaryHarkReceiptSender>) => createPrimaryHarkReceiptDispatcher({ store, send, now: () => observedAt }),
  };
}

test('real backend terminal recovery persists result and outbox atomically, including the canonical alias', async t => {
  const f = fixture(t, { alias: true });
  const recovery = await f.recover();
  assert.deepEqual(recovery.unsupported, []);
  assert.equal(f.counters.reads, 1);
  assert.equal(f.counters.forbidden, 0);
  assert.equal(f.counters.completedWrites, 1);
  const disk = JSON.parse(fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8')).calls[f.call.id];
  assert.equal(disk.delegations[aliasId].primaryResult.state, 'completed');
  assert.equal(f.item().body.evidence.kind, 'primary_outcome');
  assert.deepEqual(f.item().body.evidence, { kind: 'primary_outcome', requestId });
  const saved = structuredClone(disk);
  f.restart();
  assert.deepEqual(f.store.get(f.call.id), saved);
});

test('ordinary calls and failed Primary results retain no receipt outbox', async t => {
  for (const options of [{ hark: false }, { state: 'failed' as const }]) {
    const f = fixture(t, options);
    await f.recover();
    assert.equal(f.store.get(f.call.id)?.harkReceiptOutbox, undefined);
    assert.deepEqual(f.store.listPrimaryHarkReceiptOutboxes(), []);
    const bytes = fs.readFileSync(path.join(f.directory, 'calls.json'));
    f.restart();
    assert.deepEqual(fs.readFileSync(path.join(f.directory, 'calls.json')), bytes);
  }
});

test('v2 temporarily_unavailable503 and authentication failures remain pending without raw error retention', async t => {
  for (const [status, code] of [[503, 'temporarily_unavailable'], [401, 'invalid_service_credential'], [403, 'binding_unauthorized']] as const) {
    const f = fixture(t); await f.recover();
    const send = createPrimaryHarkReceiptSender({ apiBaseUrl: 'https://api.example.test', phoneServiceSecret: secret,
      fetch: async () => json({ error: { code, retryable: true } }, status) });
    await f.dispatcher(send).flush();
    assert.deepEqual(f.item().lastOutcome, { state: 'pending', reason: 'http_retry', httpStatus: status });
  }
});

test('actual HTTP uses service auth, exact body and200 result; typed409 recovers after restart', async t => {
  const f = fixture(t);
  await f.recover();
  f.store.update(f.call.id, { primaryClosed: { bindingId, callSid, state: 'closed' }, endedAt: observedAt });
  const requests: Array<{ path: string; body: unknown }> = [];
  const failures: unknown[] = [];
  const server = createServer((req, res) => { void (async () => {
    assert.equal(req.method, 'POST');
    assert.equal(req.headers.authorization, `Bearer ${createHmac('sha256', secret).update('dexter-broker/primary-ingress/v1').digest('hex')}`);
    assert.equal(req.headers['x-dexter-binding-id'], bindingId);
    assert.equal(req.headers['x-dexter-phone-context-version'], undefined);
    assert.equal(req.url, `/internal/primary-dexter/channels/phone/calls/${bindingId}/hark-receipts`);
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(Buffer.from(chunk));
    requests.push({ path: req.url!, body: JSON.parse(Buffer.concat(chunks).toString('utf8')) });
    const disk = JSON.parse(fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8')).calls[f.call.id];
    assert.equal(disk.harkReceiptOutbox.entries[f.item().id].attempts, requests.length, 'Attempt must be durable before HTTP');
    const pending = requests.length === 1;
    res.writeHead(pending ? 409 : 200, { 'content-type': 'application/json' });
    res.end(JSON.stringify(pending ? { error: { code: PRIMARY_HARK_CALL_RECEIPT_ASSOCIATION_PENDING, retryable: true } } : f.result()));
  })().catch(error => { failures.push(error); res.writeHead(500); res.end(); }); });
  await new Promise<void>((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
  t.after(async () => { server.closeAllConnections(); await new Promise<void>(resolve => server.close(() => resolve())); });
  const address = server.address(); assert.ok(address && typeof address === 'object');
  const send = createPrimaryHarkReceiptSender({ apiBaseUrl: `http://127.0.0.1:${address.port}`, phoneServiceSecret: secret });
  await f.dispatcher(send).flush();
  assert.equal(f.item().state, 'pending');
  assert.equal(f.item().lastOutcome?.reason, 'association_pending');
  const original = structuredClone(f.item().body);
  f.restart();
  await f.dispatcher(send).flush();
  assert.equal(f.item().state, 'delivered');
  assert.deepEqual(f.item().result, f.result());
  assert.deepEqual(requests.map(value => value.body), [original, original]);
  f.restart();
  await f.dispatcher(send).flush();
  assert.equal(requests.length, 2);
  assert.deepEqual(failures, []);
});

test('other409s and an association409 without retryabletrue stop automatic retries', async t => {
  for (const [code, retryable] of [['primary_hark_assignment_mismatch', true], ['primary_hark_terminal_outcome_required', false],
    [PRIMARY_HARK_CALL_RECEIPT_ASSOCIATION_PENDING, false], ['arbitrary_private_error', true]] as const) {
    const f = fixture(t); await f.recover(); let requests = 0;
    const send = createPrimaryHarkReceiptSender({ apiBaseUrl: 'https://api.example.test', phoneServiceSecret: secret,
      fetch: async () => { requests++; return json({ error: { code, retryable } }, 409); } });
    await f.dispatcher(send).flush();
    assert.equal(f.item().state, 'rejected');
    assert.equal(JSON.stringify(f.item().lastOutcome).includes(code), false);
    f.restart(); await f.dispatcher(send).flush();
    assert.equal(requests, 1);
  }
});

test('HTTP identity mismatch refuses, malformed success stays pending, and exact retry recovers lost acceptance', async t => {
  const f = fixture(t); await f.recover(); let requests = 0;
  const send = createPrimaryHarkReceiptSender({ apiBaseUrl: 'https://api.example.test', phoneServiceSecret: secret,
    fetch: async () => { requests++; return requests === 1 ? json({ receipt: { id: receiptId } }) : json(f.result(true)); } });
  await f.dispatcher(send).flush();
  assert.equal(f.item().state, 'pending');
  f.restart(); await f.dispatcher(send).flush();
  assert.equal(f.item().result?.replay, true);
  const g = fixture(t); await g.recover();
  const bad = createPrimaryHarkReceiptSender({ apiBaseUrl: 'https://api.example.test', phoneServiceSecret: secret,
    fetch: async () => json({ ...g.result(), receipt: { ...g.result().receipt, requestId: aliasId } }) });
  await g.dispatcher(bad).flush();
  assert.equal(g.item().state, 'rejected');
  assert.equal(g.item().lastOutcome?.reason, 'identity_mismatch');
});

test('store refuses removal, manual delivery mutation, changed terminal answer, or receipts without a saved outcome', async t => {
  const f = fixture(t); await f.recover();
  const original = f.store.get(f.call.id)!;
  assert.throws(() => f.store.update(f.call.id, { harkReceiptOutbox: undefined }), /cannot be removed/);
  const box = structuredClone(original.harkReceiptOutbox!);
  Object.values(box.entries)[0]!.attempts++;
  Object.values(box.entries)[0]!.lastAttemptAt = observedAt;
  assert.throws(() => f.store.update(f.call.id, { harkReceiptOutbox: box }), /explicit transition/);
  const changed = structuredClone(original.delegations);
  const delegation = changed[f.originalRequestId] as BrokerDelegationRecord;
  if (delegation.primaryResult?.state === 'completed') delegation.primaryResult.assistant.text = 'A different saved outcome.';
  assert.throws(() => f.store.update(f.call.id, { delegations: changed }), /terminal result cannot change/);
  assert.throws(() => f.store.update(f.call.id, { delegations: {} }), /saved completed request/);
  assert.deepEqual(f.store.get(f.call.id), original);
  f.restart(); assert.deepEqual(f.store.get(f.call.id), original);
});

test('restart rejects a persisted receipt whose binding no longer matches the saved call', async t => {
  const f = fixture(t); await f.recover();
  const file = path.join(f.directory, 'calls.json');
  const bytes = fs.readFileSync(file);
  f.store.close();
  const damaged = JSON.parse(bytes.toString('utf8'));
  damaged.calls[f.call.id].harkReceiptOutbox.binding.callSid = `CA${'2'.repeat(32)}`;
  fs.writeFileSync(file, JSON.stringify(damaged));
  assert.throws(() => new BrokerStore(f.directory), /binding_changed/);
  fs.writeFileSync(file, bytes);
  f.restart();
  assert.deepEqual(fs.readFileSync(file), bytes);
});

test('dispatcher records once per flush, shares concurrent drain, and stops before HTTP if the attempt cannot persist', async t => {
  const f = fixture(t); await f.recover();
  let release!: () => void; const gate = new Promise<void>(resolve => { release = resolve; });
  let sent = 0;
  const dispatcher = createPrimaryHarkReceiptDispatcher({ store: f.store, send: async () => {
    sent++; await gate; return { state: 'pending', reason: 'request_failed' };
  } });
  const active = dispatcher.flush();
  assert.equal(dispatcher.flush(), active);
  release(); await active;
  assert.equal(sent, 1); assert.equal(f.item().attempts, 1);
  const failed = createPrimaryHarkReceiptDispatcher({ store: {
    listPrimaryHarkReceiptOutboxes: () => f.store.listPrimaryHarkReceiptOutboxes(),
    transitionPrimaryHarkReceiptOutbox: () => { throw new Error('private storage failure'); },
  }, send: async () => { sent++; return { state: 'delivered', result: f.result() as never }; } });
  await assert.rejects(failed.flush(), /^Error: primary_hark_receipt_dispatch_state_unavailable$/);
  assert.equal(sent, 1);
});

test('sender rejects owner-decision evidence without issuing HTTP', async t => {
  const f = fixture(t); await f.recover(); const binding = f.binding();
  const box = reducePrimaryHarkReceiptOutbox(undefined, binding, { kind: 'enqueue', observedAt,
    body: { v: 'hark.primary-call-receipt.v1', assignmentRef: binding.assignmentRef,
      evidence: { kind: 'owner_decision', toolInvocationId: receiptId } } });
  const item = Object.values(box.entries)[0]!;
  const attempted = reducePrimaryHarkReceiptOutbox(box, binding, { kind: 'attempt', id: item.id,
    bodyDigest: item.bodyDigest, expectedAttempts: 0, observedAt });
  let requests = 0;
  const send = createPrimaryHarkReceiptSender({ apiBaseUrl: 'https://api.example.test', phoneServiceSecret: secret,
    fetch: async () => { requests++; return json({}); } });
  assert.deepEqual(await send(binding, attempted.entries[item.id]!), { state: 'rejected', reason: 'unsupported_evidence' });
  assert.equal(requests, 0);
});

test('actual server maintenance drains existing status and billing before the saved Primary receipt', async t => {
  const f = fixture(t); await f.recover();
  f.store.observeHarkProvider(f.call.id, 'completed', observedAt, {
    startedAt: observedAt, endedAt: observedAt, durationSeconds: 0,
  });
  const order: string[] = [];
  const broker = createBrokerServer({ store: f.store,
    config: { host: '127.0.0.1', port: 3213, publicOrigin: 'https://phone.example.test', stateDir: f.directory,
      controlToken: 'offline-control-token-'.repeat(3), apiKey: 'offline-key', model: 'gpt-live-1', voice: 'marin',
      twilioAccountSid: `AC${'1'.repeat(32)}`, twilioAuthToken: 'offline-twilio-secret', from: '+12025550100',
      allowedTo: ['+12025550123'], apiBaseUrl: 'https://api.example.test', phoneServiceSecret: secret, maxCallSeconds: 60,
      harkApiBaseUrl: 'http://127.0.0.1:3032', harkCallbackSigningSecret: 'offline-hark-callback-signature-key',
      harkInternalToken: 'offline-hark-internal-token' },
    primary: {} as never,
    provider: { calls: () => { throw new Error('Unexpected provider call'); } } as never,
    harkStatusSender: async ({ body }) => {
      order.push('status');
      const { caseId, eventId, deliveryId, intentId, callSid, originTaskId, sourceEventId } = body;
      return { state: 'delivered', httpStatus: 200, receipt: { v: 'hark.phone-callback-result.v1', ok: true, replay: false,
        caseId, eventId, deliveryId, intentId, callSid, originTaskId, sourceEventId, caseState: 'open' } };
    },
    harkBillingSender: async input => {
      order.push(input.kind);
      return input.kind === 'outbound-status'
        ? { state: 'delivered', purpose: 'voice.outbound-status.v2', httpStatus: 204 }
        : { state: 'delivered', purpose: 'voice.call-ended.v2', httpStatus: 200, receipt: { ok: true, id: receiptId, status: 'completed' } };
    },
    harkPrimaryReceiptSender: async () => { order.push('primary-receipt'); return { state: 'delivered', result: f.result() as never }; },
  });
  await new Promise<void>(resolve => broker.server.listen(0, '127.0.0.1', resolve));
  t.after(() => broker.close());
  await broker.maintain();
  assert.deepEqual(order, ['status', 'outbound-status', 'call-ended', 'primary-receipt']);
  assert.equal(f.item().state, 'delivered');
  await broker.maintain();
  assert.equal(order.length, 4);
});
