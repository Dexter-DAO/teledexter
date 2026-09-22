import test from 'node:test';
import assert from 'node:assert/strict';
import type { PhoneCallAssignment } from '../src/broker/assignment.js';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import Twilio from 'twilio';
import { WebSocket } from 'ws';
import { createBrokerServer } from '../src/broker/server.js';
import { streamToken } from '../src/broker/phone.js';
import { brokerOpening } from '../src/broker/instructions.js';
import { beginBrokerPublication, settleBrokerPublication } from '../src/broker/publications.js';
import type { BrokerConfig } from '../src/broker/config.js';
import type { PrimaryApiClient, PrimaryContextMessage } from '../src/broker/primaryApi.js';
import type { BrokerLiveSessionOptions } from '../src/live/session.js';
import { factFreshnessScenarios, historicalAccountMessages } from './fixtures/broker-fact-freshness.js';

async function fixture(t: test.TestContext, overrides: { eligibility?: boolean; failVoice?: boolean; ambiguousDial?: boolean; deferVoiceReady?: boolean; history?: PrimaryContextMessage[]; assignmentResponse?: 'missing' | 'wrong' } = {}) {
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
  let readyVoice: (() => void) | undefined;
  const openingCommands: string[] = [];
  const primary = {
    async readiness() { calls.readiness++; return { eligible: overrides.eligibility !== false, reason: overrides.eligibility === false ? 'authority_unavailable' : 'ready' }; },
    async openCall(proof: { params: Record<string, string> }, assignment?: PhoneCallAssignment) {
      calls.bind++;
      assert.equal(proof.params.CallSid, sid);
      assert.deepEqual(assignment, broker.store.bySid(sid)?.callAssignment?.value);
      assert.equal(broker.store.bySid(sid)?.voiceOpening, undefined);
      return { bindingId, callSid: sid, state: 'open',
        ...(assignment && overrides.assignmentResponse !== 'missing' ? { callAssignmentRef: { assignmentId: overrides.assignmentResponse === 'wrong' ? randomUUID() : assignment.assignmentId, revision: 1, digest: 'a'.repeat(64) } } : {}), context: { primaryDexterId: randomUUID(), threadId: randomUUID(), displayName: 'Branch', messages: overrides.history ?? [] } };
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
      readyVoice = () => options.onReady({ callId: options.callId, sessionId: 'fixture-live' });
      if (!overrides.deferVoiceReady) queueMicrotask(readyVoice);
      return {
        appendAudio() { calls.audio++; options.onAudio(Buffer.alloc(160, 255).toString('base64'), { deliveryId: 'audio-1' }); return true; },
        async appendContext() { return { eventId: 'c', status: 'accepted' }; },
        async appendInstructions(text: string) { openingCommands.push(text); return { eventId: 'i', status: 'accepted' }; },
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
    readyVoice: () => { assert.ok(readyVoice); readyVoice(); }, openingCommands,
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

test('signed outbound answer preserves the repair brief and speaks its role-aware opening only after Live is ready', async t => {
  const f = await fixture(t, { deferVoiceReady: true, history: historicalAccountMessages });
  const purpose = 'Verify the account request repair with Branch: answer one current portfolio question through Primary Dexter, then discuss his feedback. Explain any returned failure plainly.';
  assert.equal((await f.post('/internal/broker/calls', { requestId: randomUUID(), purpose })).status, 201);
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
  assert.deepEqual(f.openingCommands, []);
  assert.equal(f.broker.store.get(call.id)!.voiceOpening, undefined);
  f.readyVoice();
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(f.openingCommands, [brokerOpening('outbound', true)]);
  assert.doesNotMatch(f.openingCommands[0], /verify|Primary|Branch/);
  assert.match(f.voiceOptions()!.instructions, /called the person/);
  assert.ok(f.voiceOptions()!.instructions.includes(`Internal call brief (an assignment for you): ${JSON.stringify(purpose)}`));
  assert.equal(f.broker.store.get(call.id)!.purpose, purpose);
  assert.deepEqual(call.callAssignment, { origin: 'authenticated_broker_control', value: { assignmentId: call.requestId, brokerCallId: call.id, revision: 1, purpose } });
  assert.equal(f.broker.store.get(call.id)!.primary!.callAssignmentRef!.assignmentId, call.requestId);
  assert.equal(f.broker.store.get(call.id)!.primary!.bindingId, f.bindingId);
  assert.equal(f.broker.store.get(call.id)!.voiceOpening!.status, 'accepted');
  // Exercise the actual openCall -> persisted context -> Live options path.
  // A voiceFactory fixture cannot establish how the model will speak these facts.
  const history = f.voiceOptions()!.history!;
  assert.equal(history.length, 1);
  const prior = JSON.parse(history[0].text.split('\n')[1]);
  assert.equal(prior.source, 'prior_primary_conversation');
  assert.equal(prior.freshness, 'historical_unverified');
  assert.equal(prior.observationTime, null);
  assert.equal(prior.context, historicalAccountMessages.map(message => `${message.role}: ${message.text}`).join('\n'));
  assert.ok(prior.context.includes('$41.53'));
  assert.ok(!history[0].text.includes(factFreshnessScenarios.currentPortfolio.backendText));
  assert.match(f.voiceOptions()!.instructions, /Obtain a result for that request during this call/);
  const requestId = randomUUID();
  const current = f.broker.store.get(call.id)!;
  const identity = { callId: call.id, callSid: f.sid, bindingId: f.bindingId, sessionId: 'fixture-live' };
  f.broker.store.update(call.id, { delegations: { ...current.delegations, [requestId]: {
    ...identity, requestId, status: 'completed', updatedAt: current.updatedAt,
    primaryResult: { requestId, bindingId: f.bindingId, state: 'completed' },
  } as typeof current.delegations[string] } });
  const snapshot = { revision: 0, callerRevision: 0, fragments: [] };
  const publication = beginBrokerPublication(f.broker.store, identity, requestId,
    { id: 'synthetic_result', kind: 'result', speak: true, text: 'Saved result', expectedCallerRevision: 0 }, snapshot)!;
  settleBrokerPublication(f.broker.store, publication.record, { eventId: 'broker_17', status: 'accepted',
    observedCallerRevision: 0, observedTranscriptSequence: 0 }, snapshot, false);
  f.voiceOptions()!.onEvent!({ type: 'live.append.late_rejection', eventId: 'foreign_command', code: 'bad' });
  assert.deepEqual(f.broker.store.get(call.id)!.publications!.rejections, []);
  f.voiceOptions()!.onEvent!({ type: 'live.append.late_rejection', eventId: 'broker_17', code: 'late_rejection' });
  assert.deepEqual(f.broker.store.get(call.id)!.publications!.rejections,
    [{ sessionId: 'fixture-live', eventId: 'broker_17', code: 'late_rejection' }]);
  // Remove the synthetic incomplete delegation fixture before ordinary closeout.
  f.broker.store.update(call.id, { delegations: current.delegations });
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
    for (const call of f.broker.store.list()) assert.equal(call.callAssignment, undefined, 'callback is not operator control provenance');
    const replay = await (await f.webhook(route, { ...params, Digits: '1' })).text();
    assert.equal(replay, consentBody);
    maintenance(); await new Promise(resolve => setImmediate(resolve));
    assert.equal(f.calls.create, eligibility ? 1 : 0);
    assert.equal(f.calls.readiness, 1);
  });
}

for (const assignmentResponse of ['missing', 'wrong'] as const) {
  test(`an assigned call cannot open its media without its exact returned reference (${assignmentResponse})`, async t => {
    const f = await fixture(t, { assignmentResponse });
    assert.equal((await f.post('/internal/broker/calls', { requestId: randomUUID(), purpose: ' Read the current portfolio. ' })).status, 201);
    const call = f.broker.store.list()[0];
    assert.equal(call.callAssignment?.value.purpose, 'Read the current portfolio.');
    const params = { AccountSid: f.config.twilioAccountSid, CallSid: f.sid, From: f.config.from,
      To: f.config.allowedTo[0], Direction: 'outbound-api', CallStatus: 'in-progress' };
    const response = await f.webhook(`/broker/answer/${call.id}`, params);
    assert.doesNotMatch(await response.text(), /<Stream/);
    assert.equal(f.broker.store.get(call.id)?.primary, undefined);
    assert.deepEqual(f.openingCommands, []);
  });
}

test('caller cannot inject assignment provenance through unauthenticated or extra control fields', async t => {
  const f = await fixture(t);
  const input = { requestId: randomUUID(), purpose: 'Read my portfolio.' };
  const response = await fetch(f.base + '/internal/broker/calls', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(input) });
  assert.equal(response.status, 401);
  assert.equal((await f.post('/internal/broker/calls', { ...input, callAssignment: { origin: 'owner' } })).status, 400);
  assert.equal(f.broker.store.list().length, 0);
  assert.equal(f.calls.create, 0);
});
