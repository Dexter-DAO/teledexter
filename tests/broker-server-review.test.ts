import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import Twilio from 'twilio';
import { WebSocket } from 'ws';
import { createBrokerServer } from '../src/broker/server.js';
import { streamToken } from '../src/broker/phone.js';
import type { BrokerConfig } from '../src/broker/config.js';
import type { PhoneCallAssignment } from '../src/broker/assignment.js';
import type { PrimaryApiClient, PrimaryProviderProof, PrimaryTurn, PrimarySubmission } from '../src/broker/primaryApi.js';
import type { BrokerLiveSessionOptions, LiveTranscriptSnapshot, LiveBackendChannel } from '../src/live/session.js';

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}

interface Controls {
  bindGate?: ReturnType<typeof deferred>;
  createGate?: ReturnType<typeof deferred>;
  updateGate?: ReturnType<typeof deferred>;
  fetchGate?: ReturnType<typeof deferred>;
  fetchStatus?: string;
  closeFailures?: number;
  captureFailures?: number;
  resultFailures?: number;
  onSubmit?: () => void;
  transcript?: LiveTranscriptSnapshot;
}

async function fixture(t: test.TestContext, controls: Controls = {}) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-server-review-'));
  const config: BrokerConfig = {
    host: '127.0.0.1', port: 3213, publicOrigin: 'https://broker.example.test', stateDir: directory,
    controlToken: 'c'.repeat(40), apiKey: 'synthetic-key', model: 'gpt-live-1', voice: 'marin',
    twilioAccountSid: 'AC' + '1'.repeat(32), twilioAuthToken: 'synthetic-twilio-secret',
    from: '+12025550100', allowedTo: ['+12025550123'], apiBaseUrl: 'https://api.example.test',
    phoneServiceSecret: 's'.repeat(40), maxCallSeconds: 60,
  };
  const sid = 'CA' + '2'.repeat(32);
  const bindingId = randomUUID();
  const primaryDexterId = randomUUID();
  const threadId = randomUUID();
  const bindStarted = deferred();
  const createStarted = deferred();
  const updateStarted = deferred();
  const fetchStarted = deferred();
  const voiceStarted = deferred();
  const updates: { sid: string; args: Record<string, unknown> }[] = [];
  const closures: string[] = [];
  const proofs: PrimaryProviderProof[] = [];
  const captures: PrimaryTurn[] = [];
  const submissions: { bindingId: string; requestId: string; input: PrimarySubmission }[] = [];
  const reads: { bindingId: string; requestId: string }[] = [];
  let created = 0;
  let voices = 0;
  let voiceOptions: BrokerLiveSessionOptions | undefined;
  let closeFailures = controls.closeFailures ?? 0;
  let captureFailures = controls.captureFailures ?? 0;
  let resultFailures = controls.resultFailures ?? 0;
  let voiceFailed = false;
  const primary = {
    async readiness() { return { eligible: true, reason: 'ready' }; },
    async openCall(proof: PrimaryProviderProof, assignment?: PhoneCallAssignment) {
      proofs.push(proof); bindStarted.resolve();
      await controls.bindGate?.promise;
      return { bindingId, callSid: sid, state: 'open', context: { primaryDexterId, threadId, displayName: 'Dexter', messages: [] },
        ...(assignment === undefined ? {} : { callAssignmentRef: {
          assignmentId: assignment.assignmentId, revision: assignment.revision, digest: 'd'.repeat(64),
        } }) };
    },
    async close(id: string) {
      closures.push(id);
      if (closeFailures-- > 0) throw new Error('synthetic_primary_unavailable');
      return { bindingId: id, callSid: sid, state: 'closed' };
    },
    async captureTurn(_binding: string, turn: PrimaryTurn) {
      captures.push(structuredClone(turn));
      if (captureFailures-- > 0) throw new Error('synthetic_capture_response_lost');
      return { bindingId, sourceEventId: turn.sourceEventId, revision: turn.revision, recordId: turn.sourceEventId, replayed: false };
    },
    async submit(binding: string, requestId: string, input: PrimarySubmission) {
      submissions.push({ bindingId: binding, requestId, input: structuredClone(input) });
      controls.onSubmit?.();
      throw new Error('synthetic_admission_response_lost');
    },
    async result(binding: string, requestId: string) {
      reads.push({ bindingId: binding, requestId });
      if (resultFailures-- > 0) throw new Error('synthetic_admission_not_visible');
      return { bindingId: binding, requestId, state: 'running' };
    },
  } as unknown as PrimaryApiClient;
  const calls = Object.assign((callSid: string) => ({
    async update(args: Record<string, unknown>) {
      updates.push({ sid: callSid, args }); updateStarted.resolve();
      await controls.updateGate?.promise;
      return { sid: callSid, status: args.status === 'completed' ? 'completed' : 'in-progress' };
    },
    async fetch() {
      fetchStarted.resolve();
      await controls.fetchGate?.promise;
      return { sid: callSid, status: controls.fetchStatus ?? 'completed', from: config.from, to: config.allowedTo[0] };
    },
  }), {
    async create() {
      created++; createStarted.resolve();
      await controls.createGate?.promise;
      return { sid, status: 'queued' };
    },
  });
  const provider = { calls } as unknown as ReturnType<typeof Twilio>;
  const broker = createBrokerServer({
    config, primary, provider,
    probe: async () => ({ model: 'gpt-live-1', audioBytes: 160, sessionId: 'synthetic-probe', finalized: true }),
    voiceFactory(options) {
      voices++; voiceOptions = options; voiceStarted.resolve();
      queueMicrotask(() => options.onReady({ callId: options.callId, sessionId: 'synthetic-live' }));
      return {
        appendAudio() { return true; },
        async appendContext() { return { eventId: 'context', status: 'accepted' }; },
        async appendInstructions() { return { eventId: 'instructions', status: 'accepted' }; },
        async interruptPlayback() { return { eventId: 'interrupt', status: 'accepted' }; },
        resumePlayback() {},
        getTranscript() { return structuredClone(controls.transcript ?? { revision: 0, callerRevision: 0, fragments: [] }); },
        async close(reason) {
          const result = { callId: options.callId, sessionId: 'synthetic-live', finalized: !voiceFailed,
            transcriptPersisted: true, outputAudioDiscarded: false, reason: reason ?? 'closed' };
          options.onClosed(result);
          return result;
        },
      };
    },
  });
  await new Promise<void>(resolve => broker.server.listen(0, '127.0.0.1', resolve));
  const address = broker.server.address() as { port: number };
  const base = `http://127.0.0.1:${address.port}`;
  let closePromise: Promise<void> | undefined;
  const close = () => closePromise ??= broker.close();
  t.after(async () => {
    controls.bindGate?.resolve(); controls.createGate?.resolve(); controls.updateGate?.resolve(); controls.fetchGate?.resolve();
    await close();
    fs.rmSync(directory, { recursive: true, force: true });
  });
  const post = (route: string, value: unknown) => fetch(base + route, {
    method: 'POST', headers: { authorization: `Bearer ${config.controlToken}`, 'content-type': 'application/json' }, body: JSON.stringify(value),
  });
  const webhook = (route: string, params: Record<string, string>) => fetch(base + route, {
    method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded',
      'x-twilio-signature': Twilio.getExpectedTwilioSignature(config.twilioAuthToken, config.publicOrigin + route, params) }, body: new URLSearchParams(params),
  });
  const params = (direction: 'inbound' | 'outbound-api', status = 'in-progress') => ({
    AccountSid: config.twilioAccountSid, CallSid: sid, Direction: direction, CallStatus: status,
    From: direction === 'inbound' ? config.allowedTo[0] : config.from,
    To: direction === 'inbound' ? config.from : config.allowedTo[0],
  });
  const socketFor = (id: string) => {
    const route = `/broker/media/${id}`;
    return new WebSocket(base.replace('http:', 'ws:') + route, {
      headers: { 'x-twilio-signature': Twilio.getExpectedTwilioSignature(config.twilioAuthToken, config.publicOrigin + route, {}) },
    });
  };
  const media = async (id: string) => {
    const ws = socketFor(id);
    await new Promise<void>((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
    ws.send(JSON.stringify({ event: 'start', start: {
      accountSid: config.twilioAccountSid, callSid: sid, streamSid: 'MZ' + '3'.repeat(32),
      customParameters: { token: streamToken(config.controlToken, id, sid) },
      mediaFormat: { encoding: 'audio/x-mulaw', sampleRate: 8000, channels: 1 },
    } }));
    await voiceStarted.promise;
    return ws;
  };
  const dial = async () => {
    const response = await post('/internal/broker/calls', { requestId: randomUUID(), purpose: 'An authorized test conversation.' });
    assert.equal(response.status, 201);
    return (await response.json() as { call: { id: string } }).call.id;
  };
  return { broker, directory, config, sid, bindingId, bindStarted, createStarted, updateStarted, fetchStarted,
    post, webhook, params, socketFor, media, dial, close, updates, closures, proofs,
    primary, provider, captures, submissions, reads,
    created: () => created, voices: () => voices,
    async delegate(id: string, channel: LiveBackendChannel) {
      assert.ok(voiceOptions);
      await voiceOptions.backend.delegate({ callId: id, sessionId: 'synthetic-live', delegationId: 'test-delegation',
        requestId: 'a'.repeat(64), offsetMs: 20, transcript: channel.getContext() }, channel);
    },
    async playedAudio(ws: WebSocket) {
      assert.ok(voiceOptions);
      const receipt = new Promise<void>(resolve => {
        const listener = (raw: import('ws').RawData) => {
          const frame = JSON.parse(raw.toString());
          if (frame.event === 'mark') {
            ws.off('message', listener);
            ws.send(JSON.stringify(frame), () => resolve());
          }
        };
        ws.on('message', listener);
      });
      voiceOptions.onAudio(Buffer.alloc(160, 0xff).toString('base64'), { eventId: 'synthetic-audio' });
      await receipt;
      // A subsequent inbound message is processed after this mark acknowledgment.
      await new Promise<void>(resolve => setTimeout(resolve, 20));
    },
    failVoice() {
      voiceFailed = true;
      assert.ok(voiceOptions);
      voiceOptions.onEvent?.({ type: 'live.error', code: 'startup_timeout' });
      voiceOptions.onClosed({ callId: voiceOptions.callId, sessionId: 'synthetic-live', finalized: false, transcriptPersisted: true, outputAudioDiscarded: false, reason: 'startup_timeout' });
    },
  };
}

test('terminal callback during account binding closes the late binding without starting media', { timeout: 5000 }, async t => {
  const bindGate = deferred();
  const f = await fixture(t, { bindGate });
  const id = await f.dial();
  const pendingAnswer = f.webhook(`/broker/answer/${id}`, f.params('outbound-api'));
  await f.bindStarted.promise;
  assert.equal((await f.webhook(`/broker/status/${id}`, f.params('outbound-api', 'completed'))).status, 200);
  bindGate.resolve();
  const answer = await pendingAnswer;
  assert.doesNotMatch(await answer.text(), /<Stream/);
  assert.deepEqual(f.closures, [f.bindingId]);
  assert.equal(f.broker.store.get(id)!.status, 'completed');
  assert.equal(f.created(), 1);
  assert.equal(f.proofs[0].url, `${f.config.publicOrigin}/broker/answer/${id}`);
  assert.deepEqual(f.proofs[0].params, f.params('outbound-api'));
});

test('media stays claimed while provider stop is pending, preventing a second voice session', { timeout: 5000 }, async t => {
  const updateGate = deferred();
  const f = await fixture(t, { updateGate });
  const id = await f.dial();
  await f.webhook(`/broker/answer/${id}`, f.params('outbound-api'));
  const ws = await f.media(id);
  ws.close();
  await f.updateStarted.promise;
  const retry = f.socketFor(id);
  const status = await new Promise<number>((resolve, reject) => {
    retry.on('error', () => {});
    retry.once('unexpected-response', (_request, response) => { response.resume(); resolve(response.statusCode!); });
    retry.once('open', () => { retry.close(); reject(new Error('Second media session was admitted during stop')); });
  });
  assert.equal(status, 409);
  assert.equal(f.voices(), 1);
  updateGate.resolve();
  await f.close();
  assert.deepEqual(f.closures, [f.bindingId]);
});

test('ending an inactive call blocks first media while the provider hangup is pending', { timeout: 5000 }, async t => {
  const updateGate = deferred();
  const f = await fixture(t, { updateGate });
  const id = await f.dial();
  await f.webhook(`/broker/answer/${id}`, f.params('outbound-api'));
  const ending = f.post('/internal/broker/end', { id });
  await f.updateStarted.promise;
  assert.ok(f.broker.store.get(id)!.endedAt);
  const ws = f.socketFor(id);
  const status = await new Promise<number>((resolve, reject) => {
    ws.on('error', () => {});
    ws.once('unexpected-response', (_request, response) => { response.resume(); resolve(response.statusCode!); });
    ws.once('open', () => { ws.close(); reject(new Error('Media was admitted while provider hangup was pending')); });
  });
  assert.equal(status, 409);
  assert.equal(f.voices(), 0);
  assert.equal(f.closures.length, 0);
  updateGate.resolve();
  assert.equal((await ending).status, 200);
  assert.deepEqual(f.closures, [f.bindingId]);
  assert.equal(f.broker.store.get(id)!.status, 'completed');
});

test('maintenance leaves newly connected media active after a delayed nonterminal provider lookup', { timeout: 15000 }, async t => {
  const fetchGate = deferred();
  const f = await fixture(t, { fetchGate, fetchStatus: 'in-progress' });
  const id = await f.dial();
  await f.webhook(`/broker/answer/${id}`, f.params('outbound-api'));
  // Age only the maintenance eligibility clock; real network and call timers run normally.
  const now = Date.now();
  t.mock.method(Date, 'now', () => now + 61_000);
  await f.fetchStarted.promise;
  const ws = await f.media(id);
  fetchGate.resolve();
  await new Promise<void>(resolve => setTimeout(resolve, 30));
  assert.equal(f.updates.length, 0);
  assert.equal(f.closures.length, 0);
  assert.equal(f.broker.store.get(id)!.status, 'in-progress');
  assert.equal(f.broker.store.get(id)!.endedAt, undefined);
  assert.equal(ws.readyState, WebSocket.OPEN);
  assert.equal(f.voices(), 1);
});

test('shutdown drains an uncertain in-flight provider creation before closing its durable store', { timeout: 5000 }, async t => {
  const createGate = deferred();
  const f = await fixture(t, { createGate });
  const pending = f.post('/internal/broker/calls', { requestId: randomUUID(), purpose: 'A controlled call.' });
  await f.createStarted.promise;
  const closing = f.close();
  createGate.resolve();
  const response = await pending;
  await response.text();
  await closing;
  const state = JSON.parse(fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8'));
  const saved = Object.values(state.calls) as { callSid: string; status: string }[];
  assert.equal(saved[0].callSid, f.sid);
  assert.equal(saved[0].status, 'completed');
  assert.equal(f.created(), 1);
  assert.ok(f.updates.some(update => update.sid === f.sid && update.args.status === 'completed'));
});

test('a terminal call with failed Primary close is recoverable through explicit reconciliation', { timeout: 5000 }, async t => {
  const f = await fixture(t, { closeFailures: 1 });
  const id = await f.dial();
  await f.webhook(`/broker/answer/${id}`, f.params('outbound-api'));
  await f.webhook(`/broker/status/${id}`, f.params('outbound-api', 'completed'));
  assert.equal(f.closures.length, 1);
  assert.equal(f.broker.store.get(id)!.status, 'completed');
  const durable = JSON.parse(fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8'));
  assert.equal(durable.calls[id].primaryClosePending, true);
  assert.equal((await f.post('/internal/broker/reconcile', { id })).status, 200);
  assert.deepEqual(f.closures, [f.bindingId, f.bindingId]);
  assert.equal(f.broker.store.get(id)!.primaryClosePending, false);
  assert.equal(f.created(), 1);
});

test('signed inbound completion ends the public leg without creating an account binding', { timeout: 5000 }, async t => {
  const f = await fixture(t);
  await f.webhook('/broker/incoming-call', f.params('inbound'));
  const id = f.broker.store.list()[0].id;
  const wrongCaller = { ...f.params('inbound', 'completed'), From: '+12025550199' };
  assert.equal((await f.webhook('/broker/status', wrongCaller)).status, 403);
  assert.equal(f.closures.length, 0);
  assert.equal((await f.webhook('/broker/status', f.params('inbound', 'completed'))).status, 200);
  assert.equal(f.broker.store.get(id)!.status, 'completed');
  assert.deepEqual(f.closures, []);
  assert.equal(f.proofs.length, 0);
  assert.equal(f.broker.store.get(id)!.primary, undefined);
  assert.equal(f.voices(), 0);
  assert.equal(f.created(), 0);
});

test('voice startup failure uses audible TwiML on the same call before provider hangup', { timeout: 5000 }, async t => {
  const updateGate = deferred();
  const f = await fixture(t, { updateGate });
  const id = await f.dial();
  await f.webhook(`/broker/answer/${id}`, f.params('outbound-api'));
  const ws = await f.media(id);
  let replacementAccepted = false;
  let closedBeforeReplacement = false;
  const closed = new Promise<void>(resolve => ws.once('close', () => {
    closedBeforeReplacement = !replacementAccepted;
    resolve();
  }));
  f.failVoice();
  await f.updateStarted.promise;
  const firstUpdate = f.updates[0];
  assert.equal(firstUpdate.sid, f.sid);
  assert.equal(typeof firstUpdate.args.twiml, 'string');
  assert.match(String(firstUpdate.args.twiml), /<Say/);
  assert.match(String(firstUpdate.args.twiml), /<Hangup/);
  assert.equal(f.created(), 1);
  // The provider update is deliberately unresolved. Closing this stream now
  // would resume the original TwiML's Hangup before the replacement can speak.
  await new Promise<void>(resolve => setTimeout(resolve, 40));
  replacementAccepted = true;
  updateGate.resolve();
  await closed;
  assert.equal(closedBeforeReplacement, false);
});

test('failed final source capture keeps Primary open until the exact source replay succeeds', { timeout: 5000 }, async t => {
  const transcript: LiveTranscriptSnapshot = { revision: 1, callerRevision: 0, fragments: [
    { eventId: 'assistant-1', role: 'assistant', text: 'Hello there.', startMs: 0, endMs: 20, sequence: 1 },
  ] };
  const f = await fixture(t, { captureFailures: 1, transcript });
  const id = await f.dial();
  await f.webhook(`/broker/answer/${id}`, f.params('outbound-api'));
  const ws = await f.media(id);
  await f.playedAudio(ws);
  assert.equal((await f.post('/internal/broker/end', { id })).status, 200);
  assert.equal(f.captures.length, 1);
  const frozen = structuredClone(f.captures[0]);
  assert.equal(frozen.text, 'Hello there.');
  assert.equal(f.closures.length, 0);
  const saved = JSON.parse(fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8')).calls[id];
  assert.equal(saved.status, 'completed');
  assert.equal(saved.primaryClosePending, true);
  assert.equal(saved.transcriptCapture.state, 'pending');
  assert.equal(saved.transcriptCapture.pending, 1);
  assert.equal(saved.canonicalTurns[frozen.sourceEventId].state, 'unknown');
  assert.equal((await f.post('/internal/broker/reconcile', { id })).status, 200);
  assert.deepEqual(f.captures, [frozen, frozen]);
  assert.deepEqual(f.closures, [f.bindingId]);
  assert.equal(f.broker.store.get(id)!.primaryClosePending, false);
  assert.equal(f.broker.store.get(id)!.transcriptCapture!.state, 'complete');
  assert.equal(f.submissions.length, 0);
  assert.equal(f.created(), 1);
});

test('restart closeout restores an uncertain capture from disk before closing the saved binding', { timeout: 5000 }, async t => {
  const f = await fixture(t);
  const id = await f.dial();
  await f.webhook(`/broker/answer/${id}`, f.params('outbound-api'));
  const frozen: PrimaryTurn = { sourceEventId: randomUUID(), revision: 2, itemId: 'saved-turn', role: 'user',
    text: 'Please read the receipt.', occurredAt: '2026-09-21T12:34:56.000Z' };
  f.broker.store.update(id, { status: 'completed', endedAt: frozen.occurredAt, liveSessionId: 'synthetic-live',
    primaryClosePending: true, canonicalTurns: { [frozen.sourceEventId]: { ...frozen, state: 'unknown' } } });
  // Equivalent persisted state after an exit between remote capture and its acknowledgment.
  await f.close();
  assert.equal(f.closures.length, 0);
  const recovered = createBrokerServer({ config: f.config, primary: f.primary, provider: f.provider });
  try {
    await new Promise<void>(resolve => recovered.server.listen(0, '127.0.0.1', resolve));
    const address = recovered.server.address() as { port: number };
    const response = await fetch(`http://127.0.0.1:${address.port}/internal/broker/reconcile`, {
      method: 'POST', headers: { authorization: `Bearer ${f.config.controlToken}`, 'content-type': 'application/json' },
      body: JSON.stringify({ id }),
    });
    assert.equal(response.status, 200);
    await response.json();
    assert.deepEqual(f.captures, [frozen]);
    assert.deepEqual(f.closures, [f.bindingId]);
    assert.equal(recovered.store.get(id)!.primaryClosePending, false);
    const durable = JSON.parse(fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8')).calls[id];
    assert.equal(durable.canonicalTurns[frozen.sourceEventId].state, 'captured');
    assert.equal(f.created(), 1);
  } finally { await recovered.close(); }
});

test('aborted uncertain admission is recovered by the original result GET without another submit', { timeout: 5000 }, async t => {
  const controller = new AbortController();
  const transcript: LiveTranscriptSnapshot = { revision: 1, callerRevision: 1, fragments: [
    { eventId: 'caller-1', role: 'user', text: 'Read my last receipt.', startMs: 0, endMs: 20, sequence: 1 },
  ] };
  const f = await fixture(t, { transcript, resultFailures: 1, onSubmit: () => controller.abort() });
  const id = await f.dial();
  await f.webhook(`/broker/answer/${id}`, f.params('outbound-api'));
  const ws = await f.media(id);
  ws.send(JSON.stringify({ event: 'media', streamSid: 'MZ' + '3'.repeat(32),
    media: { track: 'inbound', payload: Buffer.alloc(160, 0).toString('base64') } }));
  // The real backend's 700 ms completion window requires observed quiet after audio.
  await new Promise<void>(resolve => setTimeout(resolve, 750));
  const channel: LiveBackendChannel = { getContext: () => structuredClone(transcript),
    subscribeContext: () => () => {}, signal: controller.signal,
    publish: async update => ({ eventId: update.id, status: 'closed' }) };
  await f.delegate(id, channel);
  assert.equal(f.submissions.length, 1);
  const requestId = f.submissions[0].requestId;
  assert.equal((await f.post('/internal/broker/end', { id })).status, 200);
  assert.equal(f.broker.store.get(id)!.primaryClosePending, true);
  assert.equal(f.closures.length, 0);
  assert.deepEqual(f.reads, [{ bindingId: f.bindingId, requestId }]);
  assert.equal((await f.post('/internal/broker/reconcile', { id })).status, 200);
  assert.deepEqual(f.reads, [{ bindingId: f.bindingId, requestId }, { bindingId: f.bindingId, requestId }]);
  assert.equal(f.submissions.length, 1);
  assert.equal(f.captures.length, 0);
  assert.equal(f.broker.store.get(id)!.delegations[requestId].status, 'running');
  assert.deepEqual(f.closures, [f.bindingId]);
  assert.equal(f.broker.store.get(id)!.primaryClosePending, false);
});
