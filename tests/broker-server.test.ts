import test from 'node:test';
import assert from 'node:assert/strict';
import { phoneCallAssignmentSchema, type PhoneCallAssignment } from '../src/broker/assignment.js';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import Twilio from 'twilio';
import { WebSocket } from 'ws';
import { createBrokerServer } from '../src/broker/server.js';
import { formatCallTranscript } from '../src/broker/callTranscript.js';
import { streamToken } from '../src/broker/phone.js';
import { brokerOpening } from '../src/broker/instructions.js';
import { beginBrokerPublication, settleBrokerPublication } from '../src/broker/publications.js';
import type { BrokerConfig } from '../src/broker/config.js';
import type { BrokerCall } from '../src/broker/store.js';
import type { PrimaryApiClient, PrimaryContextMessage } from '../src/broker/primaryApi.js';
import type { BrokerLiveSessionOptions } from '../src/live/session.js';
import type { BrokerDependencyHealth } from '../src/broker/dependencyHealth.js';
import { factFreshnessScenarios, historicalAccountMessages } from './fixtures/broker-fact-freshness.js';
import { loadSharedAssignmentParser, sharedAssignmentParserConfigured } from './helpers/shared-assignment-parser.js';

test('repeated terminal callbacks log one transcript for a call without media', async t => {
  const logs: string[] = [];
  t.mock.method(console, 'info', (message: string) => { if (message.startsWith('CALL ')) logs.push(message); });
  const f = await fixture(t);
  const response = await f.post('/internal/broker/calls', { requestId: randomUUID(), purpose: 'Fixture' });
  assert.equal(response.status, 201);
  await response.text();
  const call = f.broker.store.list()[0];
  const params = { AccountSid: f.config.twilioAccountSid, CallSid: f.sid, CallStatus: 'completed',
    From: f.config.from, To: f.config.allowedTo[0] };
  for (let i = 0; i < 2; i++) {
    const status = await f.webhook(`/broker/status/${call.id}`, params);
    assert.equal(status.status, 200);
    await status.text();
    if (i === 0) assert.deepEqual(logs, [formatCallTranscript(f.broker.store.get(call.id)!)]);
  }
  assert.equal(logs.length, 1);
});

async function fixture(t: test.TestContext, overrides: { eligibility?: boolean; failVoice?: boolean; ambiguousDial?: boolean; deferVoiceReady?: boolean; history?: PrimaryContextMessage[]; assignmentResponse?: 'missing' | 'wrong'; assignmentParser?: { parse(value: unknown): unknown }; retainedCall?: BrokerCall; dependencyHealth?: () => Promise<BrokerDependencyHealth>; greetingClip?: Buffer; gateBind?: boolean } = {}) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-server-test-'));
  const config: BrokerConfig = { host: '127.0.0.1', port: 3213, publicOrigin: 'https://phone.example.test', stateDir: directory,
    controlToken: 'c'.repeat(40), apiKey: 'fixture-key', model: 'gpt-live-1', voice: 'marin',
    twilioAccountSid: 'AC' + '1'.repeat(32), twilioAuthToken: 'fixture-twilio-secret', from: '+12025550100',
    allowedTo: ['+12025550123'], apiBaseUrl: 'https://api.example.test', phoneServiceSecret: 's'.repeat(40), maxCallSeconds: 60 };
  const sid = 'CA' + '2'.repeat(32);
  const callbackSid = 'CA' + '4'.repeat(32);
  let inboundStatus = 'in-progress';
  const bindingId = randomUUID();
  const primaryDexterId = randomUUID();
  const threadId = randomUUID();
  const calls = { create: 0, update: 0, preflight: 0, readiness: 0, bind: 0, close: 0, audio: 0, fetch: 0, portfolio: 0 };
  let voiceOptions: BrokerLiveSessionOptions | undefined;
  let readyVoice: (() => void) | undefined;
  const openingCommands: string[] = [];
  const appended: string[] = [];
  const providerUpdates: Array<Record<string, unknown>> = [];
  let releaseBind: ((failure?: Error) => void) | undefined;
  const bindGate = overrides.gateBind ? new Promise<void>((resolve, reject) => {
    releaseBind = failure => { if (failure) reject(failure); else resolve(); };
  }) : undefined;
  void bindGate?.catch(() => {});
  const primary = {
    async portfolio() { calls.portfolio++; throw new Error('Outbound must not read portfolio in the background'); },
    async readiness() { calls.readiness++; return { eligible: overrides.eligibility !== false, reason: overrides.eligibility === false ? 'authority_unavailable' : 'ready' }; },
    async openCall(proof: { params: Record<string, string> }, assignment?: PhoneCallAssignment) {
      calls.bind++;
      assert.equal(proof.params.CallSid, sid);
      assert.deepEqual(assignment, broker.store.bySid(sid)?.callAssignment?.value);
      if (overrides.assignmentParser) assert.deepEqual(overrides.assignmentParser.parse(assignment), assignment);
      assert.equal(broker.store.bySid(sid)?.voiceOpening, undefined);
      await bindGate;
      return { bindingId, callSid: sid, state: 'open',
        ...(assignment && overrides.assignmentResponse !== 'missing' ? { callAssignmentRef: { assignmentId: overrides.assignmentResponse === 'wrong' ? randomUUID() : assignment.assignmentId, revision: 1, digest: 'a'.repeat(64) } } : {}), context: { primaryDexterId, threadId, displayName: 'Avery Example', messages: overrides.history ?? [] } };
    },
    async close() { calls.close++; return { bindingId, callSid: sid, state: 'closed' }; },
  } as unknown as PrimaryApiClient;
  const provider = Object.assign((callSid: string) => ({
    async update(input: Record<string, unknown>) { calls.update++; providerUpdates.push(input); return { sid: callSid, status: 'completed' }; },
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
  if (overrides.retainedCall) fs.writeFileSync(path.join(directory, 'calls.json'),
    JSON.stringify({ version: 1, calls: { [overrides.retainedCall.id]: overrides.retainedCall } }, null, 2) + '\n');
  const broker = createBrokerServer({ config, primary, provider: { calls: provider } as unknown as ReturnType<typeof Twilio>,
    dependencyHealth: overrides.dependencyHealth,
    ...(overrides.greetingClip ? { greetingClip: overrides.greetingClip } : {}),
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
        appendAudio(payload: string) { calls.audio++; appended.push(payload); options.onAudio(Buffer.alloc(160, 255).toString('base64'), { deliveryId: 'audio-1' }); return true; },
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
  return { broker, config, calls, sid, bindingId, primaryDexterId, threadId, base, post, webhook, voiceOptions: () => voiceOptions,
    readyVoice: () => { assert.ok(readyVoice); readyVoice(); }, openingCommands, appended, providerUpdates,
    releaseBind: (failure?: Error) => { assert.ok(releaseBind); releaseBind(failure); },
    setInboundStatus: (status: string) => { inboundStatus = status; } };
}

async function preCallAssignmentCases(t: test.TestContext, parser: { safeParse(value: unknown): { success: boolean }; parse(value: unknown): unknown }) {
  const canonicalId = 'abcdefab-cdef-4abc-8abc-abcdefabcdef';
  const cases = [
    { name: 'uppercase UUID', requestId: canonicalId.toUpperCase(), purpose: 'Read my portfolio', allowed: false },
    { name: 'NUL', requestId: canonicalId, purpose: 'Read\u0000portfolio', allowed: false },
    { name: 'lone high surrogate', requestId: canonicalId, purpose: 'Read\uD800portfolio', allowed: false },
    { name: 'lone low surrogate', requestId: canonicalId, purpose: 'Read\uDC00portfolio', allowed: false },
    { name: 'ordinary text', requestId: canonicalId, purpose: 'Read my portfolio', allowed: true },
    { name: 'trimmed route input', requestId: canonicalId, purpose: '  Read my portfolio  ', allowed: true },
    { name: 'valid emoji at UTF-16 limit', requestId: canonicalId, purpose: '😀'.repeat(750), allowed: true },
    { name: 'one UTF-16 unit over limit', requestId: canonicalId, purpose: '😀'.repeat(750) + 'a', allowed: false },
  ];
  for (const item of cases) await t.test(item.name, async child => {
    const f = await fixture(child, { assignmentParser: parser });
    const journalFile = path.join(f.config.stateDir, 'calls.json');
    const emptyJournal = fs.readFileSync(journalFile, 'utf8');
    assert.deepEqual(JSON.parse(emptyJournal), { version: 1, calls: {} });
    const input = { requestId: item.requestId, purpose: item.purpose };
    assert.equal(parser.safeParse({ assignmentId: input.requestId, brokerCallId: canonicalId,
      revision: 1, purpose: input.purpose.trim() }).success, item.allowed);
    const response = await f.post('/internal/broker/calls', input);
    assert.equal(response.status, item.allowed ? 201 : 400);
    await response.text();
    if (!item.allowed) {
      assert.deepEqual(f.broker.store.list(), []);
      assert.equal(fs.readFileSync(journalFile, 'utf8'), emptyJournal);
      assert.deepEqual(f.calls, { create: 0, update: 0, preflight: 0, readiness: 0, bind: 0, close: 0, audio: 0, fetch: 0, portfolio: 0 });
      return;
    }
    const saved = f.broker.store.list()[0];
    assert.equal(saved.requestId, input.requestId);
    assert.equal(saved.purpose, input.purpose.trim());
    assert.deepEqual(parser.parse(saved.callAssignment!.value), saved.callAssignment!.value);
    const params = { AccountSid: f.config.twilioAccountSid, CallSid: f.sid, From: f.config.from,
      To: f.config.allowedTo[0], Direction: 'outbound-api', CallStatus: 'in-progress' };
    const answer = await f.webhook(`/broker/answer/${saved.id}`, params);
    assert.equal(answer.status, 200);
    assert.match(await answer.text(), /<Stream/);
    assert.equal(f.calls.bind, 1);
    const journal = fs.readFileSync(path.join(f.config.stateDir, 'calls.json'));
    const replay = await f.post('/internal/broker/calls', input);
    assert.equal(replay.status, 200);
    assert.equal((await replay.json() as { replayed: boolean }).replayed, true);
    assert.deepEqual(fs.readFileSync(path.join(f.config.stateDir, 'calls.json')), journal);
    assert.equal(f.calls.create, 1);
    assert.equal(f.calls.preflight, 1);
    assert.equal(f.calls.readiness, 1);
  });
}

test('unmounted selected-question continuation refuses before reservation or any call operation', async t => {
  const f = await fixture(t);
  const response = await f.post('/internal/broker/calls', { requestId: randomUUID(), purpose: 'Continue my saved question.',
    continuation: { originalRequestId: randomUUID(), questionId: randomUUID(), questionRevision: 1 } });
  assert.equal(response.status, 503);
  assert.deepEqual(await response.json(), { error: 'call_continuation_unavailable' });
  assert.deepEqual(f.broker.store.list(), []);
  assert.equal(f.calls.readiness, 0); assert.equal(f.calls.preflight, 0);
  assert.equal(f.calls.create, 0); assert.equal(f.calls.bind, 0);
});

test('invalid selected-question identity refuses before reservation or dialing', async t => {
  const f = await fixture(t);
  for (const continuation of [null, {}, { originalRequestId: randomUUID(), questionId: randomUUID(), questionRevision: 2 },
    { originalRequestId: randomUUID(), questionId: randomUUID(), questionRevision: 1, purpose: 'Extra authority' }]) {
    const response = await f.post('/internal/broker/calls', { requestId: randomUUID(), purpose: 'Continue my saved question.', continuation });
    assert.equal(response.status, 400); await response.text();
  }
  assert.deepEqual(f.broker.store.list(), []);
  assert.equal(f.calls.readiness, 0); assert.equal(f.calls.preflight, 0); assert.equal(f.calls.create, 0);
});

test('ordinary call replay cannot acquire a saved-question selector', async t => {
  const f = await fixture(t);
  const request = { requestId: randomUUID(), purpose: 'Read my portfolio.' };
  const first = await f.post('/internal/broker/calls', request);
  assert.equal(first.status, 201); await first.text();
  const before = f.broker.store.list();
  const changed = await f.post('/internal/broker/calls', { ...request,
    continuation: { originalRequestId: randomUUID(), questionId: randomUUID(), questionRevision: 1 } });
  assert.equal(changed.status, 409);
  assert.deepEqual(await changed.json(), { error: 'call_continuation_changed' });
  assert.deepEqual(f.broker.store.list(), before);
  assert.equal(f.calls.create, 1);
});

test('new call HTTP admission matches registration before reservation or dialing', async t => {
  await preCallAssignmentCases(t, phoneCallAssignmentSchema);
});

test('HTTP admission and persisted assignments cross the pinned shared API parser with API Zod 3', {
  skip: !sharedAssignmentParserConfigured,
}, async t => {
  const shared = loadSharedAssignmentParser();
  t.diagnostic(`API parser blob ${shared.blob}; Zod ${shared.zodVersion}`);
  await preCallAssignmentCases(t, shared.phoneCallAssignmentSchema);
});

test('historical uppercase control retry recovers exact saved call without new admission or journal rewrite', async t => {
  const requestId = 'ABCDEFAB-CDEF-4ABC-8ABC-ABCDEFABCDEF';
  const id = randomUUID();
  const purpose = 'Read\u0000portfolio\uD800';
  const retainedCall: BrokerCall = { id, requestId, direction: 'outbound', purpose,
    number: '+12025550123', status: 'unknown', createdAt: '2026-09-22T10:00:00.000Z',
    updatedAt: '2026-09-22T10:00:00.000Z', delegations: {},
    callAssignment: { origin: 'authenticated_broker_control', value: { assignmentId: requestId,
      brokerCallId: id, revision: 1, purpose } } };
  const f = await fixture(t, { retainedCall });
  const file = path.join(f.config.stateDir, 'calls.json');
  const original = fs.readFileSync(file);
  const response = await f.post('/internal/broker/calls', { requestId, purpose });
  assert.equal(response.status, 200);
  assert.equal((await response.json() as { replayed: boolean }).replayed, true);
  assert.deepEqual(f.broker.store.get(id), retainedCall);
  assert.deepEqual(fs.readFileSync(file), original);
  assert.deepEqual(f.calls, { create: 0, update: 0, preflight: 0, readiness: 0, bind: 0, close: 0, audio: 0, fetch: 0, portfolio: 0 });
  assert.notEqual((await f.post('/internal/broker/calls', { requestId, purpose: 'Different purpose' })).status, 200);
  assert.deepEqual(fs.readFileSync(file), original);
  assert.equal(f.calls.create, 0);
});

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
  t.mock.timers.enable({ apis: ['setInterval'] });
  const purpose = 'Verify the account request repair with Avery Example: answer one current portfolio question through Primary Dexter, then discuss his feedback. Explain any returned failure plainly.';
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
  assert.doesNotMatch(f.openingCommands[0], /verify|Primary|Avery/);
  assert.equal(f.calls.portfolio, 0);
  t.mock.timers.tick(90_000);
  assert.equal(f.calls.portfolio, 0, 'outbound has no periodic portfolio reader');
  t.mock.timers.reset();
  assert.match(f.voiceOptions()!.instructions, /called the person/);
  assert.ok(f.voiceOptions()!.instructions.includes(`Internal call brief (an assignment for you): ${JSON.stringify(purpose)}`));
  assert.equal(f.broker.store.get(call.id)!.purpose, purpose);
  assert.deepEqual(call.callAssignment, { origin: 'authenticated_broker_control', value: { assignmentId: call.requestId, brokerCallId: call.id, revision: 1, purpose } });
  assert.equal(f.broker.store.get(call.id)!.primary!.callAssignmentRef!.assignmentId, call.requestId);
  assert.equal(f.broker.store.get(call.id)!.primary!.bindingId, f.bindingId);
  assert.equal(f.broker.store.get(call.id)!.primary!.primaryDexterId, f.primaryDexterId);
  assert.equal(f.broker.store.get(call.id)!.primary!.threadId, f.threadId);
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

async function answeredStream(f: Awaited<ReturnType<typeof fixture>>, call: BrokerCall) {
  const params = { AccountSid: f.config.twilioAccountSid, CallSid: f.sid, From: f.config.from,
    To: f.config.allowedTo[0], Direction: 'outbound-api', CallStatus: 'in-progress' };
  const response = await f.webhook(`/broker/answer/${call.id}`, params);
  assert.equal(response.status, 200);
  const twiml = await response.text();
  const route = `/broker/media/${call.id}`;
  const signature = Twilio.getExpectedTwilioSignature(f.config.twilioAuthToken, f.config.publicOrigin + route, {});
  const connect = async () => {
    const ws = new WebSocket(f.base.replace('http:', 'ws:') + route, { headers: { 'x-twilio-signature': signature } });
    const frames: Array<{ event: string; payload?: string; name?: string }> = [];
    ws.on('message', bytes => { const frame = JSON.parse(bytes.toString());
      frames.push({ event: frame.event, ...(frame.media ? { payload: frame.media.payload } : {}), ...(frame.mark ? { name: frame.mark.name } : {}) }); });
    await new Promise<void>((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
    const streamSid = 'MZ' + '3'.repeat(32);
    ws.send(JSON.stringify({ event: 'start', start: { accountSid: f.config.twilioAccountSid, callSid: f.sid, streamSid,
      customParameters: { token: streamToken(f.config.controlToken, call.id, f.sid) }, mediaFormat: { encoding: 'audio/x-mulaw', sampleRate: 8000, channels: 1 } } }));
    const caller = (bytes: Buffer) => ws.send(JSON.stringify({ event: 'media', streamSid, media: { track: 'inbound', payload: bytes.toString('base64') } }));
    const ack = (name: string) => ws.send(JSON.stringify({ event: 'mark', streamSid, mark: { name } }));
    return { ws, frames, caller, ack };
  };
  return { twiml, connect };
}
const until = async (predicate: () => boolean, ms = 2_000) => {
  for (let i = 0; i < ms / 5 && !predicate(); i++) await new Promise(resolve => setTimeout(resolve, 5));
  assert.equal(predicate(), true);
};

for (const assignmentResponse of ['missing', 'wrong'] as const) {
  test(`an assigned call cannot open its media without its exact returned reference (${assignmentResponse})`, async t => {
    const f = await fixture(t, { assignmentResponse });
    assert.equal((await f.post('/internal/broker/calls', { requestId: randomUUID(), purpose: ' Read the current portfolio. ' })).status, 201);
    const call = f.broker.store.list()[0];
    assert.equal(call.callAssignment?.value.purpose, 'Read the current portfolio.');
    // The answer carries only the greeting stream; the binding is checked behind it.
    const answered = await answeredStream(f, call);
    assert.match(answered.twiml, /<Connect><Stream/);
    await until(() => Boolean(f.broker.store.get(call.id)!.failure));
    assert.equal(f.broker.store.get(call.id)?.primary, undefined);
    const media = await answered.connect(); t.after(() => media.ws.close());
    await until(() => f.calls.update === 1);
    assert.match(String(f.providerUpdates[0].twiml), /I could not reach your Dexter account just now/);
    assert.equal(f.voiceOptions(), undefined, 'no Live session or account context');
    assert.deepEqual(f.openingCommands, []);
    assert.equal(f.broker.store.get(call.id)?.primary, undefined);
    assert.equal(f.broker.store.get(call.id)?.mediaStartedAt, undefined);
  });
}

test('outbound answer streams the greeting before the binding exists; speech over it is dropped and later speech reaches Live in order', async t => {
  const clip = Buffer.alloc(2_400, 0x20); // 300 ms
  const f = await fixture(t, { gateBind: true, greetingClip: clip });
  assert.equal((await f.post('/internal/broker/calls', { requestId: randomUUID(), purpose: 'Read the current portfolio.' })).status, 201);
  const call = f.broker.store.list()[0];
  const answered = await answeredStream(f, call);
  assert.match(answered.twiml, /^<\?xml[^>]*><Response><Connect><Stream url="wss:\/\/phone\.example\.test\/broker\/media\/[0-9a-f-]{36}">.*<\/Connect><Hangup\/><\/Response>$/);
  assert.equal(f.calls.bind, 1); assert.equal(f.broker.store.get(call.id)!.primary, undefined, 'answered before the binding exists');
  const media = await answered.connect(); t.after(() => media.ws.close());
  await until(() => media.frames.filter(frame => frame.event === 'media').length === 15);
  assert.equal(f.voiceOptions(), undefined, 'the greeting needs no Live session');
  const overGreeting = Buffer.alloc(800, 0x10); media.caller(overGreeting);
  for (const frame of media.frames.filter(frame => frame.event === 'mark')) media.ack(frame.name!);
  await until(() => Boolean(f.broker.store.get(call.id)!.journey?.greetingClipPlayedAt));
  const first = Buffer.alloc(800, 0x21), second = Buffer.alloc(800, 0x22);
  media.caller(first); media.caller(second);
  await new Promise(resolve => setTimeout(resolve, 30));
  assert.equal(f.voiceOptions(), undefined);
  f.releaseBind();
  await until(() => f.appended.length === 2);
  assert.deepEqual(f.appended, [first.toString('base64'), second.toString('base64')], 'held speech, in order, without the words spoken over the greeting');
  const third = Buffer.alloc(800, 0x23); media.caller(third);
  await until(() => f.appended.length === 3);
  assert.equal(f.appended[2], third.toString('base64'));
  f.readyVoice(); await until(() => f.openingCommands.length === 1);
  assert.equal(f.openingCommands[0], brokerOpening('outbound', true, false, false, true));
  const saved = f.broker.store.get(call.id)!;
  assert.ok(saved.journey!.streamStartedAt! <= saved.journey!.greetingClipStartedAt!);
  assert.ok(saved.mediaStartedAt! >= saved.journey!.greetingClipStartedAt!, 'Live input starts at the greeting end');
  assert.equal(saved.primary!.bindingId, f.bindingId);
});

test('Live attached while the greeting still plays hears silence in place of the words spoken over it', async t => {
  const clip = Buffer.alloc(16_000, 0x20); // 2 s
  const f = await fixture(t, { greetingClip: clip });
  assert.equal((await f.post('/internal/broker/calls', { requestId: randomUUID(), purpose: 'Read the current portfolio.' })).status, 201);
  const call = f.broker.store.list()[0];
  const answered = await answeredStream(f, call);
  await until(() => Boolean(f.broker.store.get(call.id)!.primary));
  const media = await answered.connect(); t.after(() => media.ws.close());
  await until(() => Boolean(f.voiceOptions()));
  const spoken = Buffer.alloc(800, 0x10); media.caller(spoken);
  await until(() => f.appended.length === 1);
  assert.equal(f.appended[0], Buffer.alloc(800, 0xff).toString('base64'));
  assert.equal(f.broker.store.get(call.id)!.journey!.greetingClipPlayedAt, undefined);
});

test('a call that ends while Primary is binding it closes that binding once it lands', async t => {
  const f = await fixture(t, { gateBind: true });
  assert.equal((await f.post('/internal/broker/calls', { requestId: randomUUID(), purpose: 'Read the current portfolio.' })).status, 201);
  const call = f.broker.store.list()[0];
  const answered = await answeredStream(f, call);
  assert.match(answered.twiml, /<Stream/);
  const params = { AccountSid: f.config.twilioAccountSid, CallSid: f.sid, From: f.config.from,
    To: f.config.allowedTo[0], Direction: 'outbound-api', CallStatus: 'completed' };
  await (await f.webhook(`/broker/status/${call.id}`, params)).text();
  assert.equal(f.calls.close, 0);
  f.releaseBind();
  await until(() => f.calls.close === 1);
  assert.equal(f.voiceOptions(), undefined);
  assert.ok(f.broker.store.get(call.id)!.primaryClosed);
});

test('a stream that hangs up while Primary is binding closes that binding after its stop', async t => {
  const f = await fixture(t, { gateBind: true });
  assert.equal((await f.post('/internal/broker/calls', { requestId: randomUUID(), purpose: 'Read the current portfolio.' })).status, 201);
  const call = f.broker.store.list()[0];
  const answered = await answeredStream(f, call);
  const media = await answered.connect();
  await until(() => Boolean(f.broker.store.get(call.id)!.streamSid));
  media.ws.send(JSON.stringify({ event: 'stop', streamSid: 'MZ' + '3'.repeat(32) }));
  await until(() => Boolean(f.broker.store.get(call.id)!.endedAt));
  f.releaseBind();
  await until(() => f.calls.close === 1);
  assert.equal(f.voiceOptions(), undefined);
});

for (const spokeAfterGreeting of [true, false]) {
  test(`the journal records how long the caller waited after speaking for Dexter's first words (${spokeAfterGreeting ? 'spoke after the greeting' : 'spoke only over it'})`, async t => {
    const clip = Buffer.alloc(2_400, 0x20); // 300 ms
    const f = await fixture(t, { greetingClip: clip });
    assert.equal((await f.post('/internal/broker/calls', { requestId: randomUUID(), purpose: 'Read the current portfolio.' })).status, 201);
    const call = f.broker.store.list()[0];
    const answered = await answeredStream(f, call);
    await until(() => Boolean(f.broker.store.get(call.id)!.primary));
    const media = await answered.connect(); t.after(() => media.ws.close());
    await until(() => media.frames.filter(frame => frame.event === 'media').length === 15);
    const speech = Buffer.alloc(800, 0x21), silence = Buffer.alloc(800, 0xff);
    media.caller(speech); // Over the greeting: never the caller's question.
    for (const frame of media.frames.filter(frame => frame.event === 'mark')) media.ack(frame.name!);
    await until(() => Boolean(f.broker.store.get(call.id)!.journey?.greetingClipPlayedAt));
    if (spokeAfterGreeting) media.caller(speech);
    media.caller(silence);
    await until(() => f.appended.length === (spokeAfterGreeting ? 3 : 2));
    await new Promise(resolve => setTimeout(resolve, 40));
    f.voiceOptions()!.onAudio(Buffer.alloc(1_600, 0x21).toString('base64'), { deliveryId: 'first-words' });
    await until(() => Boolean(f.broker.store.get(call.id)!.journey?.firstSpeechAudioSentAt));
    const journey = f.broker.store.get(call.id)!.journey!;
    assert.ok(journey.firstAudioSentAt! <= journey.firstSpeechAudioSentAt!);
    if (!spokeAfterGreeting) { assert.equal(journey.callerSpeechEndedAt, undefined); return; }
    const waited = Date.parse(journey.firstSpeechAudioSentAt!) - Date.parse(journey.callerSpeechEndedAt!);
    assert.ok(waited >= 30 && waited < 2_000, `caller waited ${waited} ms`);
  });
}

for (const streamConnected of [false, true]) {
  test(`a broker shutdown waits for an in-flight outbound binding and closes it (${streamConnected ? 'stream connected' : 'no stream yet'})`, async t => {
    const f = await fixture(t, { gateBind: true });
    assert.equal((await f.post('/internal/broker/calls', { requestId: randomUUID(), purpose: 'Read the current portfolio.' })).status, 201);
    const call = f.broker.store.list()[0];
    const answered = await answeredStream(f, call);
    assert.equal(f.calls.bind, 1);
    if (streamConnected) {
      const media = await answered.connect(); t.after(() => media.ws.close());
      await until(() => Boolean(f.broker.store.get(call.id)!.streamSid));
    }
    let closed = false;
    const closing = f.broker.close().then(() => { closed = true; });
    await new Promise(resolve => setTimeout(resolve, 50));
    assert.equal(closed, false, 'shutdown waits for the binding Primary is still opening');
    f.releaseBind();
    await closing;
    assert.equal(f.calls.close, 1, 'the API binding that landed during shutdown is closed');
    const saved = f.broker.store.get(call.id)!;
    assert.equal(saved.primary?.bindingId, f.bindingId, 'the journal names the binding');
    assert.equal(saved.primaryClosed?.state, 'closed');
    assert.equal(saved.primaryClosePending, false);
    assert.equal(f.voiceOptions(), undefined, 'no Live session during shutdown');
  });
}

test('a broker shutdown whose in-flight outbound binding fails still finishes', async t => {
  const f = await fixture(t, { gateBind: true });
  assert.equal((await f.post('/internal/broker/calls', { requestId: randomUUID(), purpose: 'Read the current portfolio.' })).status, 201);
  const call = f.broker.store.list()[0];
  await answeredStream(f, call);
  const closing = f.broker.close();
  await new Promise(resolve => setTimeout(resolve, 20));
  f.releaseBind(new Error('primary unavailable'));
  await closing;
  assert.equal(f.calls.close, 0);
  assert.equal(f.broker.store.get(call.id)!.primary, undefined);
});

test('caller cannot inject assignment provenance through unauthenticated or extra control fields', async t => {
  const f = await fixture(t);
  const input = { requestId: randomUUID(), purpose: 'Read my portfolio.' };
  const response = await fetch(f.base + '/internal/broker/calls', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(input) });
  assert.equal(response.status, 401);
  assert.equal((await f.post('/internal/broker/calls', { ...input, callAssignment: { origin: 'owner' } })).status, 400);
  assert.equal(f.broker.store.list().length, 0);
  assert.equal(f.calls.create, 0);
});

test('HTTP health exposes an unavailable API and recovers without a call or provider probe', async t => {
  let health: BrokerDependencyHealth = { ok: false, checkedAt: new Date().toISOString(),
    primaryApi: { ok: false, reason: 'unavailable' } };
  const f = await fixture(t, { dependencyHealth: async () => health });
  const before = await fetch(f.base + '/broker/health');
  assert.equal(before.status, 503);
  const body = await before.json() as { ok: boolean; dependencies: BrokerDependencyHealth };
  assert.equal(body.ok, false);
  assert.deepEqual(body.dependencies, health);
  health = { ok: true, checkedAt: new Date().toISOString(),
    primaryApi: { ok: true, releaseId: 'repaired-release' } };
  const after = await fetch(f.base + '/broker/health');
  assert.equal(after.status, 200);
  assert.equal((await after.json() as { ok: boolean }).ok, true);
  assert.equal(f.calls.create, 0);
  assert.equal(f.calls.preflight, 0);
  assert.equal(f.calls.readiness, 0);
  assert.equal(f.calls.bind, 0);
});
