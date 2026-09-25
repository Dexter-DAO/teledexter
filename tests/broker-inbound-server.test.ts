import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import Twilio from 'twilio';
import { WebSocket } from 'ws';
import { createBrokerServer } from '../src/broker/server.js';
import { inboundBeginRef, type InboundBeginIdentity, type InboundOutcome } from '../src/broker/inboundContract.js';
import { BrokerStore, type BrokerCall } from '../src/broker/store.js';
import type { BrokerConfig } from '../src/broker/config.js';
import { PrimaryApiError, type PrimaryApiClient } from '../src/broker/primaryApi.js';
import { createInboundHoldAdapter } from '../src/broker/inboundHold.js';
import { streamToken } from '../src/broker/phone.js';

async function fixture(t: test.TestContext, options: { selected?: boolean; retained?: BrokerCall; initialRemote?: InboundOutcome } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-inbound-http-'));
  if (options.retained) fs.writeFileSync(path.join(dir, 'calls.json'), JSON.stringify({ version: 1, calls: { [options.retained.id]: options.retained } }));
  const store = new BrokerStore(dir);
  const config: BrokerConfig = { host: '127.0.0.1', port: 3213, publicOrigin: 'https://phone.example.test', stateDir: dir,
    controlToken: 'c'.repeat(40), apiKey: 'fixture-key', model: 'gpt-live-1', voice: 'marin',
    twilioAccountSid: 'AC' + '1'.repeat(32), twilioAuthToken: 'fixture-twilio-secret', from: '+12025550100',
    allowedTo: ['+12025550123'], apiBaseUrl: 'https://api.example.test', phoneServiceSecret: 's'.repeat(40), maxCallSeconds: 60 };
  const params = { AccountSid: config.twilioAccountSid, CallSid: 'CA' + '2'.repeat(32), From: config.allowedTo[0], To: config.from,
    Direction: 'inbound', CallStatus: 'ringing', StirVerstat: 'TN-Validation-Passed-A' };
  const attachmentId = options.initialRemote?.attachmentId ?? randomUUID();
  const approvalRef = options.initialRemote && 'approvalRef' in options.initialRemote ? options.initialRemote.approvalRef
    : { attachmentId, approvalId: randomUUID(), digest: 'd'.repeat(64) };
  const bindingId = options.initialRemote?.state === 'bound' ? options.initialRemote.bindingId : randomUUID();
  const primaryDexterId = randomUUID(), threadId = randomUUID();
  let remote = options.initialRemote;
  let loseConsume = false, lostBegin = false, authority = true, cancellationUnavailable = false;
  const lifecycle: string[] = [];
  let providerGate: Promise<void> | undefined; let releaseProvider: (() => void) | undefined;
  let providerEntered: (() => void) | undefined; let providerState = 'in-progress';
  let beginError: Error | undefined;
  let statusGate: Promise<void> | undefined; let releaseStatus: (() => void) | undefined;
  const seen = { dial: 0, end: 0, voice: 0, begin: 0, consume: 0, context: 0, cancel: 0, close: 0, legacyClose: 0 };
  const context = () => {
    seen.context++;
    if (!authority) throw new Error('authority_unavailable');
    return { bindingId, callSid: params.CallSid, state: 'open' as const,
      context: { primaryDexterId, threadId, displayName: 'Branch', messages: [] } };
  };
  const primary = {
    async beginInbound(begin: InboundBeginIdentity) {
      seen.begin++;
      assert.ok(store.list()[0].inbound!.continuation);
      assert.deepEqual(begin.providerProof, store.list()[0].inbound!.begin.providerProof);
      if (beginError) throw beginError;
      assert.equal(store.list()[0].inbound!.beginAttemptedAt !== undefined, true);
      remote ??= { attachmentId, beginRef: inboundBeginRef(begin), state: 'pending', expiresAt: new Date(Date.now() + 300_000).toISOString() };
      if (lostBegin) { lostBegin = false; throw new Error('lost_begin'); }
      return structuredClone(remote);
    },
    async recoverInbound(begin: InboundBeginIdentity) { return remote ? structuredClone(remote) : { state: 'absent' as const, beginRef: inboundBeginRef(begin) }; },
    async inboundStatus(id: string) { assert.equal(id, attachmentId); if (statusGate) await statusGate; return structuredClone(remote!); },
    async cancelInbound(begin: InboundBeginIdentity) {
      seen.cancel++;
      if (cancellationUnavailable) throw new Error('cleanup_unavailable');
      if (remote?.state !== 'bound' && remote?.state !== 'cancelled') remote = { attachmentId, beginRef: inboundBeginRef(begin), state: 'cancelled',
        cancelledAt: new Date().toISOString(), expiresAt: remote && 'expiresAt' in remote ? remote.expiresAt : null };
      return structuredClone(remote!);
    },
    async openInboundCall(proof: { params: Record<string, string> }, ref: typeof approvalRef) {
      seen.consume++; assert.deepEqual(ref, approvalRef); assert.equal(proof.params.Direction, 'inbound');
      assert.equal(proof.params.CallSid, params.CallSid);
      assert.equal(store.list()[0].inbound!.consumeAttemptedAt !== undefined, true);
      remote = { attachmentId, beginRef: remote!.beginRef, state: 'bound', approvalRef, bindingId,
        consumedAt: new Date().toISOString(), callState: 'open' };
      if (loseConsume) { loseConsume = false; throw new Error('lost_consume'); }
      return context();
    },
    async getContext() { return context(); },
    async closeInbound(id: string, input: { begin: InboundBeginIdentity; bindingId: string; approvalRef: typeof approvalRef }) {
      seen.close++; lifecycle.push('binding-close'); assert.equal(id, attachmentId); assert.equal(input.bindingId, bindingId); assert.deepEqual(input.approvalRef, approvalRef);
      assert.deepEqual(input.begin, store.list()[0].inbound!.begin);
      assert.equal(remote?.state, 'bound');
      remote = { ...remote!, state: 'bound', approvalRef, bindingId, consumedAt: remote?.state === 'bound' ? remote.consumedAt : '', callState: 'closed' };
      return { attachmentId, beginRef: remote.beginRef, approvalRef, bindingId, callSid: params.CallSid,
        state: 'closed' as const, closedAt: new Date().toISOString() };
    },
    async close() { seen.legacyClose++; throw new Error('must_use_exact_inbound_closure'); },
  } as unknown as PrimaryApiClient;
  const provider = Object.assign((sid: string) => ({
    async fetch() { if (providerGate) { providerEntered?.(); await providerGate; } return { sid, accountSid: config.twilioAccountSid, direction: 'inbound', from: params.From, to: params.To, status: providerState }; },
    async update() { seen.end++; return { sid, status: 'completed' }; },
  }), { async create() { seen.dial++; throw new Error('inbound_must_not_dial'); } });
  const broker = createBrokerServer({ config, store, primary, provider: { calls: provider } as unknown as ReturnType<typeof Twilio>,
    ...(options.selected === false ? {} : { inboundAttachment: createInboundHoldAdapter({ publicOrigin: config.publicOrigin, store }) }),
    voiceFactory: settings => {
      seen.voice++;
      queueMicrotask(() => settings.onReady({ callId: settings.callId, sessionId: 'inbound-fixture' }));
      return { appendAudio() { return true; }, async appendContext() { return { eventId: 'c', status: 'accepted' }; },
        async appendInstructions() { return { eventId: 'i', status: 'accepted' }; }, async interruptPlayback() { return { eventId: 'p', status: 'accepted' }; },
        resumePlayback() {}, getTranscript() { return { revision: 0, callerRevision: 0, fragments: [] }; },
        async close(reason) { lifecycle.push('live-close'); return { callId: settings.callId, sessionId: 'inbound-fixture', finalized: true,
          transcriptPersisted: true, outputAudioDiscarded: false, reason: reason ?? 'closed' }; } };
    },
  });
  await new Promise<void>(resolve => broker.server.listen(0, '127.0.0.1', resolve));
  const base = `http://127.0.0.1:${(broker.server.address() as { port: number }).port}`;
  const webhook = (route: string, patch: Record<string, string> = {}, valid = true) => {
    const body = { ...params, ...patch };
    return fetch(base + route, { method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded',
      'x-twilio-signature': valid ? Twilio.getExpectedTwilioSignature(config.twilioAuthToken, config.publicOrigin + route, body) : 'wrong' }, body: new URLSearchParams(body) });
  };
  let closed = false;
  const close = async () => { if (!closed) { closed = true; await broker.close(); } };
  t.after(async () => { await close(); fs.rmSync(dir, { recursive: true, force: true }); });
  return { broker, store, base, config, params, seen, lifecycle, webhook, close, remote: () => remote,
    async waitForAdmission() { const call = store.list()[0]; await (await webhook(`/broker/inbound/${call.id}/continue`)).text(); },
    beginError(error?: Error) { beginError = error; },
    approve() { assert.ok(remote?.state === 'pending'); remote = { ...remote, state: 'approved', approvalRef }; },
    loseBegin() { lostBegin = true; }, loseConsume() { loseConsume = true; }, refuseContext() { authority = false; },
    refuseCancellation() { cancellationUnavailable = true; },
    gateProvider() {
      providerGate = new Promise<void>(resolve => { releaseProvider = resolve; });
      const entered = new Promise<void>(resolve => { providerEntered = resolve; });
      return { entered, release(status: string) { providerState = status; releaseProvider!(); } };
    },
    gateStatus() { statusGate = new Promise<void>(resolve => { releaseStatus = resolve; }); return () => releaseStatus!(); },
  };
}

test('actual HTTP incoming waits on same leg and approved continuation enters existing voice path', async t => {
  const f = await fixture(t);
  const incoming = await f.webhook('/broker/incoming-call');
  const wait = await incoming.text();
  assert.equal(incoming.status, 200); assert.match(wait, /<Redirect/); assert.doesNotMatch(wait, /<Gather|<Stream/);
  assert.equal(f.seen.dial, 0); assert.equal(f.seen.context, 0); assert.equal(f.seen.voice, 0);
  const call = f.store.list()[0]; await f.waitForAdmission(); f.approve();
  const response = await f.webhook(`/broker/inbound/${call.id}/continue`);
  assert.equal(response.status, 200); assert.match(await response.text(), /<Stream/);
  assert.equal(f.seen.consume, 1); assert.equal(f.seen.dial, 0);
  const route = `/broker/media/${call.id}`;
  const ws = new WebSocket(f.base.replace(/^http/, 'ws') + route, { headers: {
    'x-twilio-signature': Twilio.getExpectedTwilioSignature(f.config.twilioAuthToken, f.config.publicOrigin + route, {}),
  } });
  await new Promise<void>((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
  ws.send(JSON.stringify({ event: 'start', start: { accountSid: f.config.twilioAccountSid, callSid: f.params.CallSid,
    streamSid: 'MZ' + '4'.repeat(32), customParameters: { token: streamToken(f.config.controlToken, call.id, f.params.CallSid) },
    mediaFormat: { encoding: 'audio/x-mulaw', sampleRate: 8000, channels: 1 } } }));
  await new Promise(resolve => setTimeout(resolve, 20));
  assert.equal(f.seen.voice, 1); assert.ok(f.store.get(call.id)!.mediaStartedAt);
  const repeat = await f.webhook(`/broker/inbound/${call.id}/continue`);
  assert.equal(repeat.status, 409); await repeat.text();
  assert.equal(f.seen.voice, 1);
  ws.close(); await new Promise<void>(resolve => ws.once('close', () => resolve()));
});

test('lost registration followed by terminal callback closes exact inbound binding after authority loss', async t => {
  const f = await fixture(t); await (await f.webhook('/broker/incoming-call')).text();
  const call = f.store.list()[0]; await f.waitForAdmission(); f.approve(); f.loseConsume();
  const lost = await f.webhook(`/broker/inbound/${call.id}/continue`);
  assert.match(await lost.text(), /<Redirect/);
  assert.equal(f.store.get(call.id)!.primary, undefined); f.refuseContext();
  const terminal = await f.webhook('/broker/status', { CallStatus: 'completed' });
  assert.equal(terminal.status, 200); await terminal.text();
  const ended = f.store.get(call.id)!;
  assert.equal(ended.inbound!.closure?.state, 'closed');
  assert.equal(ended.primary, undefined); assert.equal(ended.transcriptCapture, undefined);
  assert.equal(f.seen.legacyClose, 0); assert.equal(f.seen.close, 1); assert.equal(f.seen.context, 0);
});

test('lost begin stays a recoverable wait and terminal callback cancels it', async t => {
  const f = await fixture(t); f.loseBegin();
  const first = await f.webhook('/broker/incoming-call'); assert.match(await first.text(), /<Redirect/);
  const call = f.store.list()[0]; assert.equal(f.seen.begin, 0); await f.waitForAdmission();
  assert.equal(f.seen.begin, 1); assert.equal(f.store.get(call.id)!.inbound!.remote, undefined);
  const terminal = await f.webhook('/broker/status', { CallStatus: 'completed' }); await terminal.text();
  assert.equal(f.store.get(call.id)!.inbound!.remote?.state, 'cancelled');
  assert.equal(f.seen.context, 0); assert.equal(f.seen.consume, 0);
});

test('same-call route refuses a different signed caller and legacy callback request', async t => {
  const f = await fixture(t); await (await f.webhook('/broker/incoming-call')).text(); const call = f.store.list()[0];
  const foreign = await f.webhook(`/broker/inbound/${call.id}/continue`, { From: '+12025550999' });
  assert.equal(foreign.status, 403); await foreign.text();
  const callback = await f.webhook(`/broker/callback/${call.id}`, { Digits: '1' });
  assert.equal(callback.status, 409); await callback.text();
  assert.equal(f.seen.consume, 0); assert.equal(f.seen.dial, 0); assert.equal(f.seen.context, 0);
});

test('unselected adapter preserves existing callback behavior for new incoming calls', async t => {
  const f = await fixture(t, { selected: false });
  const response = await f.webhook('/broker/incoming-call'); assert.match(await response.text(), /<Gather/);
  assert.equal(f.store.list()[0].inbound, undefined); assert.equal(f.seen.begin, 0);
});

test('shutdown fences all saved waiting calls, including restored calls absent from this process sessions', async t => {
  const first = await fixture(t); await (await first.webhook('/broker/incoming-call')).text(); await first.waitForAdmission(); first.approve();
  const saved = first.store.list()[0]; const remote = first.remote()!;
  const restarted = await fixture(t, { selected: false, retained: saved, initialRemote: remote });
  await restarted.close();
  const disk = JSON.parse(fs.readFileSync(path.join(restarted.config.stateDir, 'calls.json'), 'utf8')).calls[saved.id];
  assert.equal(disk.inbound.localStop.reason, 'service_shutdown'); assert.equal(disk.inbound.remote.state, 'cancelled');
  assert.equal(restarted.seen.consume, 0); assert.equal(restarted.seen.context, 0); assert.equal(restarted.seen.end, 1);
});

test('disabled adapter cannot consume a retained approved call through a continuation', async t => {
  const first = await fixture(t); await (await first.webhook('/broker/incoming-call')).text(); await first.waitForAdmission(); first.approve();
  const saved = first.store.list()[0];
  const restarted = await fixture(t, { selected: false, retained: saved, initialRemote: first.remote()! });
  const result = await restarted.webhook(`/broker/inbound/${saved.id}/continue`);
  assert.match(await result.text(), /<Hangup/);
  assert.equal(restarted.seen.consume, 0); assert.equal(restarted.seen.context, 0);
  assert.equal(restarted.store.get(saved.id)!.inbound!.remote?.state, 'cancelled');
});


test('shutdown stop is saved before an in-flight approved status can consume', async t => {
  const f = await fixture(t); await (await f.webhook('/broker/incoming-call')).text(); await f.waitForAdmission(); f.approve();
  const call = f.store.list()[0]; const release = f.gateStatus();
  const continuing = f.webhook(`/broker/inbound/${call.id}/continue`);
  await new Promise(resolve => setTimeout(resolve, 20));
  const closing = f.close();
  assert.equal(f.store.get(call.id)!.inbound!.localStop?.reason, 'service_shutdown');
  release();
  const response = await continuing; await response.text(); await closing;
  assert.equal(f.seen.consume, 0); assert.equal(f.seen.context, 0);
});

test('stopped wait terminates provider leg even while API cancellation fails', async t => {
  const f = await fixture(t); await (await f.webhook('/broker/incoming-call')).text();
  const call = f.store.list()[0]; await f.waitForAdmission(); f.refuseCancellation();
  f.store.transitionInbound(call.id, { type: 'stop', observedAt: new Date().toISOString(), reason: 'broker_cancelled' });
  await f.broker.maintain();
  assert.equal(f.seen.end, 1);
  assert.equal(f.store.get(call.id)!.status, 'completed');
  assert.equal(f.store.get(call.id)!.inbound!.remote?.state, 'pending');
  assert.equal(f.store.get(call.id)!.inbound!.closure, undefined);
});

test('enabling direct attachment leaves historical callback webhook retries in their original mode', async t => {
  const previous = await fixture(t, { selected: false }); await (await previous.webhook('/broker/incoming-call')).text();
  const saved = previous.store.list()[0];
  const next = await fixture(t, { selected: true, retained: saved });
  const retry = await next.webhook('/broker/incoming-call');
  assert.equal(retry.status, 200); assert.match(await retry.text(), /<Gather/);
  assert.equal(next.store.get(saved.id)!.inbound, undefined); assert.equal(next.seen.begin, 0);
});


test('terminal provider result arriving after media starts finalizes voice before binding closure', async t => {
  const f = await fixture(t); await (await f.webhook('/broker/incoming-call')).text(); await f.waitForAdmission(); f.approve();
  const call = f.store.list()[0]; await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text();
  const gate = f.gateProvider(); const maintaining = f.broker.maintain(); await gate.entered;
  const route = `/broker/media/${call.id}`;
  const ws = new WebSocket(f.base.replace(/^http/, 'ws') + route, { headers: {
    'x-twilio-signature': Twilio.getExpectedTwilioSignature(f.config.twilioAuthToken, f.config.publicOrigin + route, {}),
  } });
  await new Promise<void>((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
  ws.send(JSON.stringify({ event: 'start', start: { accountSid: f.config.twilioAccountSid, callSid: f.params.CallSid,
    streamSid: 'MZ' + '4'.repeat(32), customParameters: { token: streamToken(f.config.controlToken, call.id, f.params.CallSid) },
    mediaFormat: { encoding: 'audio/x-mulaw', sampleRate: 8000, channels: 1 } } }));
  await new Promise(resolve => setTimeout(resolve, 20)); assert.equal(f.seen.voice, 1);
  gate.release('completed'); await maintaining;
  assert.deepEqual(f.lifecycle, ['live-close', 'binding-close']);
  assert.equal(f.store.get(call.id)!.inbound!.closure?.state, 'closed');
  assert.ok(f.store.get(call.id)!.transcriptCapture);
  if (ws.readyState !== WebSocket.CLOSED) await new Promise<void>(resolve => ws.once('close', () => resolve()));
});

test('actual HTTP hold answers before redirect; restart and maintenance cannot create fresh admission', async t => {
  const f = await fixture(t);
  const first = await f.webhook('/broker/incoming-call'); const xml = await first.text();
  assert.match(xml, /<Say>One moment\.<\/Say><Pause length="1"\/><Redirect method="POST">/);
  assert.equal(f.seen.begin, 0); assert.equal(f.seen.context, 0);
  const saved = f.store.list()[0]; const waitUntil = saved.inbound!.waitUntil;
  await f.broker.maintain(); assert.equal(f.seen.begin, 0);
  const restored = await fixture(t, { retained: saved });
  await restored.broker.maintain(); assert.equal(restored.seen.begin, 0);
  const bad = await restored.webhook(`/broker/inbound/${saved.id}/continue`, {}, false);
  assert.equal(bad.status, 403); await bad.text();
  assert.equal(restored.store.get(saved.id)!.inbound!.continuation, undefined);
  const continued = await restored.webhook(`/broker/inbound/${saved.id}/continue`, { CallStatus: 'in-progress' });
  const silent = await continued.text(); assert.match(silent, /<Pause/); assert.doesNotMatch(silent, /<Say|<Stream|<Gather/);
  assert.equal(restored.seen.begin, 1); assert.equal(restored.seen.context, 0);
  assert.equal(restored.store.get(saved.id)!.inbound!.waitUntil, waitUntil);
  assert.deepEqual(restored.store.get(saved.id)!.inbound!.begin.providerProof, saved.inbound!.begin.providerProof);
  assert.equal(restored.seen.dial, 0);
});

for (const code of ['inbound_carrier_attestation_required', 'inbound_enrollment_required'] as const) {
  test(`actual HTTP ${code} hangs up while unavailable cancellation remains pending`, async t => {
    const f = await fixture(t); await (await f.webhook('/broker/incoming-call')).text();
    const call = f.store.list()[0];
    f.beginError(new PrimaryApiError('http_error', 403, { code, retryable: false }));
    f.refuseCancellation();
    const response = await f.webhook(`/broker/inbound/${call.id}/continue`); const xml = await response.text();
    assert.match(xml, /<Hangup/); assert.doesNotMatch(xml, /<Stream|<Gather|enrollment|attestation/);
    const saved = f.store.get(call.id)!;
    assert.equal(saved.inbound!.admissionRefusal?.code, code);
    assert.equal(saved.inbound!.localStop?.reason, code);
    assert.ok(saved.inbound!.cancelRequestedAt); assert.equal(saved.inbound!.remote, undefined);
    assert.equal(saved.inbound!.closure, undefined); assert.equal(f.seen.context, 0); assert.equal(f.seen.consume, 0);
    f.beginError(); await f.broker.maintain();
    assert.equal(f.seen.begin, 1); assert.equal(f.seen.end, 1);
    assert.equal(f.store.get(call.id)!.inbound!.remote, undefined);
  });
}

test('actual HTTP retryable carrier503 continues silently and preserves one original begin', async t => {
  const f = await fixture(t); await (await f.webhook('/broker/incoming-call')).text();
  const call = f.store.list()[0];
  f.beginError(new PrimaryApiError('http_error', 503, { code: 'inbound_carrier_admission_unavailable', retryable: true }));
  const wait = await f.webhook(`/broker/inbound/${call.id}/continue`);
  assert.match(await wait.text(), /<Redirect/);
  const attempted = f.store.get(call.id)!;
  assert.equal(attempted.inbound!.localStop, undefined); assert.equal(attempted.inbound!.admissionRefusal, undefined);
  f.beginError(); await f.broker.maintain();
  assert.equal(f.seen.begin, 2); assert.equal(f.store.list().length, 1);
  assert.deepEqual(f.store.get(call.id)!.inbound!.begin, attempted.inbound!.begin);
  assert.equal(f.store.get(call.id)!.inbound!.waitUntil, call.inbound!.waitUntil);
  assert.equal(f.seen.context, 0); assert.equal(f.seen.dial, 0);
});

test('hold adapter refuses a different continuation destination', async t => {
  const f = await fixture(t); await (await f.webhook('/broker/incoming-call')).text();
  const call = f.store.list()[0]; const adapter = createInboundHoldAdapter({ publicOrigin: f.config.publicOrigin, store: f.store });
  assert.throws(() => adapter.waitingResponse({ callId: call.id, continuationUrl: 'https://other.example/broker/inbound/' + call.id + '/continue' }), /mismatch/);
  assert.equal(f.seen.begin, 0);
});
