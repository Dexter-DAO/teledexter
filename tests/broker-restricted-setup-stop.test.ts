import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import type Twilio from 'twilio';
import type { BrokerConfig } from '../src/broker/config.js';
import { inboundBeginRef } from '../src/broker/inboundContract.js';
import { inboundRequestId } from '../src/broker/inboundJournal.js';
import type { PrimaryApiClient } from '../src/broker/primaryApi.js';
import { prepareRestrictedSetup, stopRestrictedSetup } from '../src/broker/restrictedSetupJournal.js';
import { createRestrictedSetupServer } from '../src/broker/restrictedSetupServer.js';
import { createBrokerServer } from '../src/broker/server.js';
import { BrokerStore } from '../src/broker/store.js';
import type { VerificationCallbackClient } from '../src/broker/verificationCallbackClient.js';

async function fixture(t: test.TestContext, missing?: 'session' | 'media' | 'ended' | 'terminal', withMedia = true) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'restricted-stop-'));
  const store = new BrokerStore(directory), file = path.join(directory, 'calls.json');
  const start = Date.now() - 1000, at = (offset: number) => new Date(start + offset).toISOString();
  const sid = 'CA' + '1'.repeat(32), accountSid = 'AC' + '2'.repeat(32);
  const proof = { url: 'https://broker.example.test/broker/incoming-call', signature: 'synthetic-original',
    params: { AccountSid: accountSid, CallSid: sid, From: '+15555550002', To: '+15555550001',
      Direction: 'inbound', CallStatus: 'ringing', StirVerstat: 'TN-Validation-Passed-C' } };
  const call = store.reserveInbound({ requestId: inboundRequestId(sid), purpose: 'The person called Dexter.', providerProof: proof },
    { version: 1, waitMs: 120000, mediaGraceMs: 30000 }, at(0)).call;
  store.transitionInbound(call.id, { type: 'continuation', observedAt: at(1),
    providerProof: { ...proof, url: `https://broker.example.test/broker/inbound/${call.id}/continue` } });
  store.transitionInbound(call.id, { type: 'begin_attempted', observedAt: at(2) });
  store.transitionInbound(call.id, { type: 'admission_refused', code: 'inbound_carrier_attestation_required', observedAt: at(3) });
  store.transitionInbound(call.id, { type: 'outcome', observedAt: at(4), outcome: { state: 'cancelled',
    attachmentId: '00000000-0000-4000-8000-000000000001', beginRef: inboundBeginRef(call.inbound!.begin),
    expiresAt: null, cancelledAt: at(4) } });
  let setup = prepareRestrictedSetup(store.get(call.id)!, at(5), call.inbound!.waitUntil);
  if (withMedia) setup = { ...setup, media: { admittedAt: at(5), deadlineAt: setup.deadlineAt,
    ...(missing === 'media' ? {} : { stoppedAt: at(6) }) } };
  store.recordRestrictedSetup(call.id, setup);
  if (missing !== 'session') store.recordRestrictedSetup(call.id, stopRestrictedSetup(store.get(call.id)!, at(7)));
  store.update(call.id, { status: missing === 'terminal' ? 'in-progress' : 'completed',
    ...(missing === 'ended' ? {} : { endedAt: at(8) }) });
  const config: BrokerConfig = { host: '127.0.0.1', port: 3213, publicOrigin: 'https://broker.example.test', stateDir: directory,
    controlToken: 'c'.repeat(40), apiKey: 'synthetic', model: 'gpt-live-1', voice: 'marin', twilioAccountSid: accountSid,
    twilioAuthToken: 'synthetic', from: proof.params.To, allowedTo: [], apiBaseUrl: 'http://127.0.0.1:3031', phoneServiceSecret: 'p'.repeat(40), maxCallSeconds: 1800 };
  const counts = { complete: 0, fetch: 0, remote: 0, close: 0, activeStop: 0 };
  let failCompletion = false;
  const forbidden = async (): Promise<never> => { counts.remote++; throw new Error('Unexpected external operation'); };
  const api: VerificationCallbackClient = { prepareAccess: forbidden, recover: forbidden, dispatch: forbidden,
    answer: forbidden, complete: forbidden, recoverCompletion: forbidden, cancel: forbidden };
  const calls = Object.assign((callSid: string) => ({ fetch: async () => { counts.fetch++; throw new Error('Unexpected provider fetch'); },
    update: async (input: unknown) => { assert.equal(callSid, sid); assert.deepEqual(input, { status: 'completed' });
      counts.complete++; if (failCompletion) throw new Error('Synthetic completion unavailable');
      return { sid, accountSid, from: proof.params.From, to: proof.params.To, direction: 'inbound', status: 'completed' }; } }),
    { create: forbidden });
  const provider = { calls } as unknown as ReturnType<typeof Twilio>;
  const primary = {} as PrimaryApiClient;
  const voiceFactory = () => { throw new Error('Unexpected voice creation'); };
  const active = new Map<string, { stop(reason: string): Promise<void> }>(), claimed = new Set<string>();
  const receiptTransport = { apiBaseUrl: 'http://127.0.0.1:3031', signingSecret: 'r'.repeat(40), fetchImpl: forbidden };
  const restricted = createRestrictedSetupServer({ config, store, primary, provider, voiceFactory, api, active, claimed,
    accepting: () => true, model: 'gpt-5.4-mini', receiptTransport, async closePrimary() { counts.close++; } });
  let broker: ReturnType<typeof createBrokerServer> | undefined, closed = false;
  async function closeBroker() { if (broker && !closed) { closed = true; await broker.close(); } }
  t.after(async () => { try { await closeBroker(); } finally { store.close(); fs.rmSync(directory, { recursive: true, force: true }); } });
  return { store, file, callId: call.id, counts, active, claimed, restricted, closeBroker,
    current: () => store.get(call.id)!, bytes: () => fs.readFileSync(file), failCompletion: () => { failCompletion = true; },
    async startBroker() {
      broker = createBrokerServer({ config, store, primary, provider, voiceFactory, harkPrimaryReceiptSender: forbidden,
        restrictedSetup: { model: 'gpt-5.4-mini', api, receiptTransport } });
      await new Promise<void>(resolve => broker!.server.listen(0, '127.0.0.1', resolve));
      return broker;
    } };
}

for (const withMedia of [true, false]) test(`terminal restricted setup keeps exact journal across maintenance and shutdown; media=${withMedia}`, async t => {
  const f = await fixture(t, undefined, withMedia), broker = await f.startBroker(), before = f.bytes();
  await broker.maintain(); await broker.maintain(); await f.closeBroker();
  assert.deepEqual(f.bytes(), before);
  assert.deepEqual(f.counts, { complete: 0, fetch: 0, remote: 0, close: 0, activeStop: 0 });
});

for (const missing of ['session', 'media', 'ended', 'terminal'] as const) test(`unfinished restricted teardown still runs when ${missing} evidence is missing`, async t => {
  const f = await fixture(t, missing), before = f.bytes();
  await f.restricted.stop(f.current(), 'service_shutdown');
  assert.equal(f.counts.complete, 1); assert.equal(f.counts.close, 1);
  assert.ok(f.current().endedAt); assert.ok(f.current().restrictedSetup?.stoppedAt);
  assert.ok(f.current().restrictedSetup?.media?.stoppedAt); assert.notDeepEqual(f.bytes(), before);
});

test('pending closeout is not skipped by the completed-provider/session guard', async t => {
  const f = await fixture(t); f.store.update(f.callId, { primaryClosePending: true });
  await f.restricted.maintain(f.current());
  assert.ok(f.counts.close > 0); assert.equal(f.counts.complete, 1);
  assert.equal(f.current().primaryClosePending, true, 'only the real closeout owner may clear its fence');
});

test('active and claimed hosts keep ownership; shutdown delegates to the active host', async t => {
  const f = await fixture(t), before = f.bytes();
  f.active.set(f.callId, { async stop(reason) { assert.equal(reason, 'service_shutdown'); f.counts.activeStop++; } });
  await f.restricted.maintain(f.current()); await f.restricted.stop(f.current(), 'service_shutdown');
  assert.equal(f.counts.activeStop, 1); assert.equal(f.counts.complete, 0); assert.deepEqual(f.bytes(), before);
  f.active.clear(); f.claimed.add(f.callId); await f.restricted.maintain(f.current());
  assert.deepEqual(f.bytes(), before); assert.equal(f.counts.complete, 0);
  await f.restricted.stop(f.current(), 'service_shutdown');
  assert.equal(f.counts.complete, 1, 'an unfinished claimed attachment is not treated as fully drained');
});

test('failed provider completion preserves missing end evidence and remains retryable', async t => {
  const f = await fixture(t, 'ended'); f.failCompletion();
  await assert.rejects(f.restricted.stop(f.current(), 'service_shutdown'), /Synthetic completion unavailable/);
  assert.equal(f.current().endedAt, undefined); assert.equal(f.counts.close, 0);
  await assert.rejects(f.restricted.stop(f.current(), 'service_shutdown'), /Synthetic completion unavailable/);
  assert.equal(f.counts.complete, 2); assert.equal(f.current().endedAt, undefined);
});
