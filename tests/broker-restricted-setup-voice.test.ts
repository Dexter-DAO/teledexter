import assert from 'node:assert/strict';
import test from 'node:test';
import { createRestrictedSetupVoice, type RestrictedSetupHost, type RestrictedSetupPhase,
  type RestrictedSetupVoiceOptions, type RestrictedSetupResolution } from '../src/broker/restrictedSetupVoice.js';
import { createBrokerLiveSession } from '../src/live/session.js';
import type { BrokerLiveConnection, BrokerLiveSessionOptions, LiveBackendChannel,
  LiveDelegationRequest, LiveSessionClosed, LiveBackendUpdate } from '../src/live/types.js';
import type { ClientEvent } from 'openai/resources/live/live';

const callId = '11111111-1111-4111-8111-111111111111';
const callSid = 'CA' + '1'.repeat(32);
const sessionId = 'sess_setup';
const scope = { callId, callSid, sessionId };
const raw = 'Yes, please arrange the verification call.';
const request: LiveDelegationRequest = { callId, sessionId, delegationId: 'delegate_1', requestId: 'request_1',
  offsetMs: 1500, transcript: { revision: 1, callerRevision: 1, fragments: [
    { eventId: 'raw_1', role: 'user', text: raw, sequence: 1, startMs: 0, endMs: 1400 },
  ] } };
const closed = (reason = 'close_requested'): LiveSessionClosed => ({ callId, sessionId,
  finalized: true, reason, transcriptPersisted: true, outputAudioDiscarded: false });
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(yes => { resolve = yes; });
  return { promise, resolve };
}
const flush = () => new Promise<void>(resolve => setImmediate(resolve));
function fixture(t: { after(fn: () => Promise<void>): void }, hostPatch: Partial<RestrictedSetupHost> = {},
  patch: Partial<RestrictedSetupVoiceOptions> = {}) {
  const { voiceFactory: providedFactory, ...configuration } = patch;
  let options: BrokerLiveSessionOptions | undefined;
  let active = true;
  const heard: string[] = [], outputs: string[] = [], stops: string[] = [], intents: string[] = [];
  const requests: LiveDelegationRequest[] = [];
  let reads = 0;
  const host: RestrictedSetupHost = {
    isCurrent: () => active,
    async observe(value) { reads++; return { ...value, phase: 'account_authentication_required' }; },
    async handleCaller(value) {
      requests.push(structuredClone(value.request));
      return { ...scope, requestId: value.request.requestId, publicationId: 'setup_operation_1', phase: 'verification_requested' };
    }, ...hostPatch,
  };
  const voice = createRestrictedSetupVoice({ apiKey: 'synthetic', setupModel: 'gpt-5-mini', callId, callSid,
    deadlineAtMs: Date.now() + 60_000, hostTimeoutMs: 1000, host,
    voiceFactory(value) {
      options = value;
      if (providedFactory) return providedFactory(value);
      return { appendAudio() { return true; }, async appendContext() { throw new Error('private context forbidden'); },
        async appendInstructions() { throw new Error('unexpected instruction append'); },
        async appendCommentary(text) { heard.push(text); return { eventId: 'commentary', status: 'accepted' }; },
        async interruptPlayback() { return { eventId: 'interrupt', status: 'accepted' }; }, resumePlayback() {},
        getTranscript: () => request.transcript,
        async close(reason) { const result = closed(reason); value.onClosed(result); return result; } };
    }, onAudio: value => outputs.push(value), onStopRequested: reason => stops.push(reason),
    onClosed: (_value, intent) => intents.push(intent), ...configuration });
  assert.ok(options);
  const captured = options;
  const start = () => captured.onReady({ callId, sessionId });
  t.after(async () => { await voice.close(); });
  return { voice, options: captured, start, host, heard, outputs, stops, intents, requests,
    reads: () => reads, revoke() { active = false; } };
}
function channel() {
  const controller = new AbortController(); const published: LiveBackendUpdate[] = [];
  const value: LiveBackendChannel = { getContext: () => structuredClone(request.transcript),
    subscribeContext: () => () => {}, signal: controller.signal,
    async publish(update) { published.push(update); return { eventId: 'published', status: 'accepted' }; } };
  return { value, controller, published };
}

async function execute(options: BrokerLiveSessionOptions, source: LiveDelegationRequest, c: ReturnType<typeof channel>, toolCallId = 'tool_1') {
  assert.ok(options.restrictedSetupTools);
  return options.restrictedSetupTools.execute({ request: source, toolCallId,
    tool: { name: 'request_verified_callback', arguments: { explicitlyRequested: true } } }, c.value);
}

test('construction supplies only restricted instructions, empty history and a setup backend', async t => {
  const f = fixture(t); f.start(); await f.voice.refresh();
  assert.deepEqual(f.options.history, []); assert.equal(f.options.eligibleHistory, undefined);
  assert.match(f.options.instructions, /setup assistance only/);
  assert.match(f.options.instructions, /separate account admission/);
  assert.equal(f.options.onEvent, undefined, 'raw model/error event logging is not attached');
  assert.equal(f.requests.length, 0, 'an opening cannot request verification or a callback');
  assert.equal(f.reads(), 1);
  assert.match(f.heard[0], /signed-in Dexter account/);
});

test('exact named function reaches the narrow host once and returns only its finite phase', async t => {
  const f = fixture(t); f.start(); await f.voice.refresh(); const c = channel();
  assert.deepEqual(await execute(f.options, request, c), { phase: 'verification_requested' });
  assert.deepEqual(await execute(f.options, structuredClone(request), c), { phase: 'verification_requested' });
  assert.deepEqual(f.requests, [request]); assert.equal(c.published.length, 0);
  await assert.rejects(f.options.backend.delegate(request, c.value), /named_tool_required/);
  assert.equal(f.requests.length, 1);
});

test('changed raw source under an existing delegation identity refuses without a second host call', async t => {
  const f = fixture(t); f.start(); await f.voice.refresh(); const c = channel();
  await execute(f.options, request, c);
  const changed = structuredClone(request); changed.transcript.fragments[0].text = 'Cancel that.';
  await assert.rejects(execute(f.options, changed, c), /request_changed/);
  assert.equal(f.requests.length, 1); assert.equal(c.published.length, 0);
});

test('foreign source session or call and an aborted subscription never reach the host', async t => {
  const f = fixture(t); f.start(); await f.voice.refresh(); const c = channel();
  await execute(f.options, { ...request, sessionId: 'old_session' }, c);
  await execute(f.options, { ...request, callId: 'another_call' }, c);
  c.controller.abort(); await execute(f.options, request, c);
  assert.equal(f.requests.length, 0); assert.equal(c.published.length, 0);
});

test('host receives current caption context separately from the immutable original delegation', async t => {
  const entered = deferred<void>(), allowRead = deferred<void>();
  let observedRevision = 0;
  const f = fixture(t, { async handleCaller(input, _signal, caller) {
    assert.deepEqual(input.request, request); entered.resolve(); await allowRead.promise;
    observedRevision = caller.getContext().callerRevision;
    return { ...scope, requestId: request.requestId, publicationId: 'clarify_changed_turn', phase: 'checking' };
  } });
  f.start(); await f.voice.refresh(); const c = channel(); let latest = structuredClone(request.transcript);
  c.value.getContext = () => structuredClone(latest);
  const pending = execute(f.options, request, c); await entered.promise;
  latest = { ...latest, revision: 2, callerRevision: 2 };
  allowRead.resolve(); await pending;
  assert.equal(observedRevision, 2); assert.equal(request.transcript.callerRevision, 1);
});

test('unknown host effect stays pending and duplicate delegation does not retry it', async t => {
  let attempts = 0;
  const f = fixture(t, { async handleCaller() { attempts++; throw new Error('lost response after possible dispatch'); } });
  f.start(); await f.voice.refresh(); const c = channel();
  assert.deepEqual(await execute(f.options, request, c), { phase: 'checking' });
  assert.deepEqual(await execute(f.options, request, c), { phase: 'checking' });
  assert.equal(attempts, 1); assert.equal(c.published.length, 0);
  await f.voice.refresh(); assert.equal(attempts, 1, 'status recovery does not call the effect port');
});

test('foreign or arbitrary host result cannot inject private information into speech', async t => {
  const privateText = 'private account balance and owner name';
  const f = fixture(t, { async handleCaller(value) {
    return { ...scope, sessionId: 'foreign_session', requestId: value.request.requestId,
      publicationId: privateText, phase: 'verification_requested' };
  } });
  f.start(); await f.voice.refresh(); const c = channel();
  const result = await execute(f.options, request, c);
  assert.deepEqual(result, { phase: 'checking' });
  assert.equal(c.published.length, 0); assert.doesNotMatch(JSON.stringify(result), /private account/);
});

test('host revocation gates output and stops on the next input without upgrading the original leg', async t => {
  const f = fixture(t); f.start(); await f.voice.refresh(); f.revoke();
  f.options.onAudio('private-output', { deliveryId: 'late' });
  assert.equal(f.voice.appendAudio('AAAA'), false);
  assert.deepEqual(f.outputs, []); assert.deepEqual(f.stops, ['host_closed']);
  assert.deepEqual(f.intents, ['stop']);
});

test('sensitive capture closes this generation and permanently gates both audio directions', async t => {
  const f = fixture(t); f.start(); await f.voice.refresh();
  const finish = f.voice.suspendForSensitiveInput();
  assert.equal(f.voice.appendAudio('raw-code-audio'), false);
  f.options.onAudio('late-output', { deliveryId: 'late' }); await finish;
  assert.deepEqual(f.outputs, []); assert.deepEqual(f.stops, [], 'host owns continuing provider leg');
  assert.deepEqual(f.intents, ['sensitive_input']);
  assert.equal('resume' in f.voice, false);
});

test('stop aborts a held host request and prevents late success speech', async t => {
  const entered = deferred<void>(), result = deferred<RestrictedSetupResolution>(); let signal: AbortSignal | undefined;
  const f = fixture(t, { async handleCaller(_input, abort) { signal = abort; entered.resolve(); return result.promise; } });
  f.start(); await f.voice.refresh(); const c = channel();
  const waiting = execute(f.options, request, c); await entered.promise;
  await f.voice.close(); assert.equal(signal?.aborted, true);
  result.resolve({ ...scope, requestId: request.requestId, publicationId: 'late', phase: 'complete' });
  await waiting; assert.deepEqual(c.published, []);
});

test('deadline requests provider stop before a held Live close or host effect can finish', async t => {
  const stop = deferred<void>(), liveClose = deferred<LiveSessionClosed>();
  let captured: BrokerLiveSessionOptions | undefined;
  const f = fixture(t, { async handleCaller() { return new Promise(() => {}); } }, {
    deadlineAtMs: Date.now() + 100,
    onStopRequested() { stop.resolve(); },
    voiceFactory(options) {
      captured = options;
      return { appendAudio: () => true, appendContext: async () => ({ eventId: 'x', status: 'accepted' }),
        appendInstructions: async () => ({ eventId: 'x', status: 'accepted' }),
        appendCommentary: async () => ({ eventId: 'x', status: 'accepted' }),
        interruptPlayback: async () => ({ eventId: 'x', status: 'accepted' }), resumePlayback() {},
        getTranscript: () => request.transcript, close: () => liveClose.promise };
    },
  });
  assert.ok(captured); captured.onReady({ callId, sessionId });
  const c = channel(); const action = execute(captured, request, c);
  try { await stop.promise; assert.equal(f.voice.appendAudio('AAAA'), false); assert.equal(c.published.length, 0); }
  finally { liveClose.resolve(closed()); await action; }
});

test('existing Live session sends empty full startup input and setup-only named function delegation', async t => {
  const events: ClientEvent[] = [];
  let opened = () => {}, receive = (_event: unknown) => {}, openedSocket = false;
  const connection: BrokerLiveConnection = { onOpen(fn) { opened = fn; }, onEvent(fn) { receive = fn; },
    onError() {}, onClose() {}, isOpen: () => openedSocket,
    send(event) {
      events.push(event);
      if (event.type === 'session.commentary.append') queueMicrotask(() => receive({
        type: 'session.commentary.appended', client_event_id: event.event_id, event_id: 'ack', start_ms: 0, end_ms: 0 }));
      if (event.type === 'session.close') queueMicrotask(() => receive({ type: 'session.closed', reason: 'close_requested',
        session: { id: sessionId }, usage: { seconds: 1 } }));
    }, close() { openedSocket = false; }, terminate() { openedSocket = false; } };
  let observed = 0;
  const host: RestrictedSetupHost = { isCurrent: () => true,
    async observe(s) { observed++; return { ...s, phase: 'account_authentication_required' }; },
    async handleCaller() { throw new Error('unexpected effect'); } };
  const voice = createRestrictedSetupVoice({ apiKey: 'synthetic', setupModel: 'gpt-5-mini', callId, callSid,
    deadlineAtMs: Date.now() + 60_000, hostTimeoutMs: 1000, host,
    voiceFactory: options => createBrokerLiveSession({ ...options, connectionFactory: () => connection }),
    onAudio() {}, onClosed() {}, onStopRequested() {} });
  t.after(async () => { await voice.close(); });
  openedSocket = true; opened();
  const start = events.find(event => event.type === 'session.start');
  assert.ok(start && start.type === 'session.start');
  assert.deepEqual(start.session.input, []); assert.equal(start.session.delegation?.type, 'responses');
  if (start.session.delegation?.type !== 'responses') throw new Error('Expected restricted Responses delegation');
  assert.equal(start.session.delegation.responses.parallel_tool_calls, false);
  assert.equal(start.session.delegation.responses.tools?.length, 4);
  assert.equal(observed, 0, 'socket open does not qualify model startup');
  receive({ type: 'session.started', session: { id: sessionId, model: 'gpt-live-1', delegation: { type: 'responses' } } });
  await flush(); await voice.refresh(); assert.ok(observed >= 1);
  assert.equal(events.some(event => event.type === 'session.thinking.append'), false);
});

test('only the closed set of host phases can produce spoken setup outcomes', async t => {
  for (const phase of ['verification_available', 'standing_callback_available', 'checking', 'verification_requested',
    'callback_requested', 'complete', 'declined', 'unavailable'] satisfies RestrictedSetupPhase[]) {
    const f = fixture(t, { async observe(s) { return { ...s, phase }; } });
    f.start(); await f.voice.refresh(); assert.equal(f.heard.length, 1); await f.voice.close();
  }
});
