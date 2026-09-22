import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import Twilio from 'twilio';
import { WebSocket } from 'ws';
import { createBrokerServer } from '../src/broker/server.js';
import { streamToken } from '../src/broker/phone.js';
import type { BrokerConfig } from '../src/broker/config.js';
import type { PrimaryApiClient } from '../src/broker/primaryApi.js';
import type { BrokerLiveSessionOptions } from '../src/live/session.js';

async function fixture(t: test.TestContext, overrides: { eligibility?: boolean; failVoice?: boolean; ambiguousDial?: boolean } = {}) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-server-test-'));
  const config: BrokerConfig = { host: '127.0.0.1', port: 3213, publicOrigin: 'https://phone.example.test', stateDir: directory,
    controlToken: 'c'.repeat(40), apiKey: 'fixture-key', model: 'gpt-live-1', voice: 'marin',
    twilioAccountSid: 'AC' + '1'.repeat(32), twilioAuthToken: 'fixture-twilio-secret', from: '+12025550100',
    allowedTo: ['+12025550123'], apiBaseUrl: 'https://api.example.test', phoneServiceSecret: 's'.repeat(40), maxCallSeconds: 60 };
  const sid = 'CA' + '2'.repeat(32);
  const callbackSid = 'CA' + '4'.repeat(32);
  let inboundStatus = 'in-progress';
  const bindingId = randomUUID();
  const calls = { create: 0, update: 0, preflight: 0, readiness: 0, bind: 0, close: 0, audio: 0, fetch: 0 };
  let voiceOptions: BrokerLiveSessionOptions | undefined;
  const primary = {
    async readiness() { calls.readiness++; return { eligible: overrides.eligibility !== false, reason: overrides.eligibility === false ? 'authority_unavailable' : 'ready' }; },
    async openCall(proof: { params: Record<string, string> }) {
      calls.bind++;
      assert.equal(proof.params.CallSid, sid);
      return { bindingId, callSid: sid, state: 'open', context: { primaryDexterId: randomUUID(), threadId: randomUUID(), displayName: 'Branch', messages: [] } };
    },
    async close() { calls.close++; return { bindingId, callSid: sid, state: 'closed' }; },
  } as unknown as PrimaryApiClient;
  const provider = Object.assign((callSid: string) => ({
    async update() { calls.update++; return { sid: callSid, status: 'completed' }; },
    async fetch() {
      calls.fetch++;
      const inbound = broker.store.bySid(callSid)?.direction === 'inbound';
      return { sid: callSid, accountSid: config.twilioAccountSid, direction: inbound ? 'inbound' : 'outbound-api',
        status: inbound ? inboundStatus : 'completed', from: inbound ? config.allowedTo[0] : config.from, to: inbound ? config.from : config.allowedTo[0] };
    },
  }), { async create() {
    calls.create++;
    if (overrides.ambiguousDial) throw new Error('Provider response timed out');
    return { sid: broker.store.bySid(sid)?.direction === 'inbound' ? callbackSid : sid, status: 'queued' };
  } });
  const broker = createBrokerServer({ config, primary, provider: { calls: provider } as unknown as ReturnType<typeof Twilio>,
    probe: async () => {
      calls.preflight++;
      if (overrides.failVoice) throw Object.assign(new Error('Voice unavailable'), { code: 'credit_balance_exhausted' });
      return { model: 'gpt-live-1', audioBytes: 800, sessionId: 'fixture', finalized: true };
    },
    voiceFactory: options => {
      voiceOptions = options;
      queueMicrotask(() => options.onReady({ callId: options.callId, sessionId: 'fixture-live' }));
      return {
        appendAudio() { calls.audio++; options.onAudio(Buffer.alloc(160, 255).toString('base64'), { deliveryId: 'audio-1' }); return true; },
        async appendContext() { return { eventId: 'c', status: 'accepted' }; },
        async appendInstructions() { return { eventId: 'i', status: 'accepted' }; },
        async interruptPlayback() { return { eventId: 'p', status: 'accepted' }; },
        resumePlayback() {},
        getTranscript() { return { revision: 0, callerRevision: 0, fragments: [] }; },
        async close(reason) { const result = { callId: options.callId, sessionId: 'fixture-live', finalized: true, transcriptPersisted: true, outputAudioDiscarded: false, reason: reason || 'closed' }; options.onClosed(result); return result; },
      };
    },
  });
  await new Promise<void>(resolve => broker.server.listen(0, '127.0.0.1', resolve));
  const address = broker.server.address() as { port: number };
  const base = `http://127.0.0.1:${address.port}`;
  t.after(async () => { await broker.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  const post = (route: string, value: unknown) => fetch(base + route, { method: 'POST', headers: { authorization: `Bearer ${config.controlToken}`, 'content-type': 'application/json' }, body: JSON.stringify(value) });
  const webhook = (route: string, params: Record<string, string>, valid = true) => fetch(base + route, {
    method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded', 'x-twilio-signature': valid ? Twilio.getExpectedTwilioSignature(config.twilioAuthToken, config.publicOrigin + route, params) : 'bad' }, body: new URLSearchParams(params),
  });
  return { broker, config, calls, sid, bindingId, base, post, webhook, voiceOptions: () => voiceOptions,
    setInboundStatus: (status: string) => { inboundStatus = status; } };
}

test('outbound calls require account eligibility and generated voice before dialing', async t => {
  const f = await fixture(t, { eligibility: false });
  const response = await f.post('/internal/broker/calls', { requestId: randomUUID(), purpose: 'Test call' });
  assert.equal(response.status, 503);
  assert.equal(f.calls.preflight, 0);
  assert.equal(f.calls.create, 0);
  assert.equal(f.broker.store.list()[0].status, 'failed');
});

test('a voice credit error prevents the phone from ringing', async t => {
  const f = await fixture(t, { failVoice: true });
  assert.equal((await f.post('/internal/broker/calls', { requestId: randomUUID(), purpose: 'Test call' })).status, 503);
  assert.equal(f.calls.preflight, 1);
  assert.equal(f.calls.create, 0);
});

test('ambiguous call creation is recorded and an exact retry cannot ring twice', async t => {
  const f = await fixture(t, { ambiguousDial: true });
  const input = { requestId: randomUUID(), purpose: 'Test call' };
  assert.equal((await f.post('/internal/broker/calls', input)).status, 503);
  const replay = await (await f.post('/internal/broker/calls', input)).json() as { replayed: boolean; call: { status: string } };
  assert.equal(replay.replayed, true);
  assert.equal(replay.call.status, 'unknown');
  assert.equal(f.calls.create, 1);
  assert.equal((await f.post('/internal/broker/calls', { ...input, requestId: randomUUID() })).status, 409);
  assert.equal(f.calls.create, 1);
});

test('signed inbound call offers callback without account reads, voice preflight or dialing', async t => {
  const f = await fixture(t);
  const params = { AccountSid: f.config.twilioAccountSid, CallSid: f.sid, From: f.config.allowedTo[0], To: f.config.from, Direction: 'inbound', CallStatus: 'ringing' };
  assert.equal((await f.webhook('/broker/incoming-call', params, false)).status, 403);
  const response = await f.webhook('/broker/incoming-call', params);
  assert.equal(response.status, 200);
  const call = f.broker.store.list()[0];
  const offered = await response.text();
  assert.match(offered, /<Gather/);
  assert.match(offered, new RegExp(`/broker/callback/${call.id}`));
  assert.doesNotMatch(offered, /<Stream/);
  assert.equal(call.primary, undefined);
  const callback = `/broker/callback/${call.id}`;
  assert.equal((await f.webhook(callback, { ...params, Digits: '1' }, false)).status, 403);
  const consent = await f.webhook(callback, { ...params, CallStatus: 'in-progress', Digits: '1' });
  assert.equal(consent.status, 200);
  assert.match(await consent.text(), /<Hangup\/>/);
  assert.equal(f.broker.store.get(call.id)!.callback!.state, 'requested');
  assert.equal(f.calls.fetch, 1);
  assert.equal(f.calls.bind, 0);
  assert.equal(f.calls.readiness, 0);
  assert.equal(f.calls.preflight, 0);
  assert.equal(f.calls.create, 0);
});

test('signed outbound answer reaches Primary, then authenticated media exchanges audio', async t => {
  const f = await fixture(t);
  assert.equal((await f.post('/internal/broker/calls', { requestId: randomUUID(), purpose: 'A requested callback.' })).status, 201);
  const call = f.broker.store.list()[0];
  const params = { AccountSid: f.config.twilioAccountSid, CallSid: f.sid, From: f.config.from, To: f.config.allowedTo[0], Direction: 'outbound-api', CallStatus: 'in-progress' };
  const response = await f.webhook(`/broker/answer/${call.id}`, params);
  assert.equal(response.status, 200);
  assert.match(await response.text(), /<Stream/);
  const route = `/broker/media/${call.id}`;
  const signature = Twilio.getExpectedTwilioSignature(f.config.twilioAuthToken, f.config.publicOrigin + route, {});
  const ws = new WebSocket(f.base.replace('http:', 'ws:') + route, { headers: { 'x-twilio-signature': signature } });
  const audio = new Promise<Record<string, unknown>>((resolve, reject) => {
    ws.on('error', reject);
    ws.on('message', bytes => { const frame = JSON.parse(bytes.toString()); if (frame.event === 'media') resolve(frame); });
  });
  await new Promise<void>((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
  const streamSid = 'MZ' + '3'.repeat(32);
  ws.send(JSON.stringify({ event: 'start', start: { accountSid: f.config.twilioAccountSid, callSid: f.sid, streamSid,
    customParameters: { token: streamToken(f.config.controlToken, call.id, f.sid) }, mediaFormat: { encoding: 'audio/x-mulaw', sampleRate: 8000, channels: 1 } } }));
  ws.send(JSON.stringify({ event: 'media', streamSid, media: { track: 'inbound', payload: Buffer.alloc(160, 255).toString('base64') } }));
  assert.equal((await audio).event, 'media');
  assert.equal(f.calls.audio, 1);
  assert.match(f.voiceOptions()!.instructions, /called the person/);
  assert.equal(f.broker.store.get(call.id)!.primary!.bindingId, f.bindingId);
  assert.equal(f.broker.store.get(call.id)!.voiceOpening!.status, 'accepted');
  ws.close();
  await new Promise<void>(resolve => ws.once('close', () => resolve()));
});

for (const eligibility of [true, false]) {
  test(`HTTP callback checks current readiness after inbound ends (eligible=${eligibility})`, { timeout: 5_000 }, async t => {
    const originalInterval = globalThis.setInterval;
    let maintenance: (() => void) | undefined;
    t.mock.method(globalThis, 'setInterval', (callback: (...args: unknown[]) => void, delay?: number, ...args: unknown[]) => {
      if (delay === 10_000) maintenance = () => callback(...args);
      return originalInterval(callback, delay, ...args);
    });
    const f = await fixture(t, { eligibility });
    const params = { AccountSid: f.config.twilioAccountSid, CallSid: f.sid, From: f.config.allowedTo[0], To: f.config.from,
      Direction: 'inbound', CallStatus: 'in-progress' };
    await (await f.webhook('/broker/incoming-call', params)).text();
    const inbound = f.broker.store.list()[0];
    const route = `/broker/callback/${inbound.id}`;
    const consentBody = await (await f.webhook(route, { ...params, Digits: '1' })).text();
    assert.match(consentBody, /If phone access is ready/);
    assert.equal(f.calls.create, 0); assert.equal(f.calls.readiness, 0); assert.equal(f.calls.bind, 0);
    f.setInboundStatus('completed');
    assert.ok(maintenance); maintenance();
    const deadline = Date.now() + 1_000;
    while (f.broker.store.get(inbound.id)!.callback!.state === 'requested' && Date.now() < deadline) {
      await new Promise(resolve => setImmediate(resolve));
    }
    assert.equal(f.broker.store.get(inbound.id)!.status, 'completed');
    assert.equal(f.broker.store.get(inbound.id)!.callback!.state, eligibility ? 'started' : 'unavailable');
    assert.equal(f.calls.readiness, 1);
    assert.equal(f.calls.preflight, eligibility ? 1 : 0);
    assert.equal(f.calls.create, eligibility ? 1 : 0);
    assert.equal(f.calls.bind, 0);
    const replay = await (await f.webhook(route, { ...params, Digits: '1' })).text();
    assert.equal(replay, consentBody);
    maintenance(); await new Promise(resolve => setImmediate(resolve));
    assert.equal(f.calls.create, eligibility ? 1 : 0);
    assert.equal(f.calls.readiness, 1);
  });
}
