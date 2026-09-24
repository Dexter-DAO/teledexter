import assert from 'node:assert/strict';
import test from 'node:test';
import { createLiveGenerationController, LiveGenerationError,
  LIVE_GENERATION_TRANSITION_MS,
  liveGenerationJournalSchema, type GenerationCallbacks, type GenerationHandle, type LiveGenerationJournal,
  type LiveGenerationOptions } from '../src/broker/liveGeneration.js';
import { PrimaryApiError } from '../src/broker/primaryApi.js';
import type { EligibleContextClient, EligiblePhoneCall, HistoryVersion } from '../src/broker/eligibleContext.js';
import type { LiveSessionClosed, LiveTranscriptSnapshot } from '../src/live/types.js';

const callId = '11111111-1111-4111-8111-111111111111';
const bindingId = '22222222-2222-4222-8222-222222222222';
const ownerPrincipalId = '33333333-3333-4333-8333-333333333333';
const primaryDexterId = '44444444-4444-4444-8444-444444444444';
const threadId = '55555555-5555-4555-8555-555555555555';
const requestId = '66666666-6666-4666-8666-666666666666';
const messageId = '77777777-7777-4777-8777-777777777777';
const eventId = '88888888-8888-4888-8888-888888888888';
const runId = '99999999-9999-4999-8999-999999999999';
const callSid = `CA${'1'.repeat(32)}`;
const startTime = Date.parse('2026-09-24T14:00:00.000Z');
const when = new Date(startTime).toISOString();
const binding = { bindingId, callSid, ownerPrincipalId, primaryDexterId, threadId };
const clocks = { callDeadlineAt: new Date(startTime + 300_000).toISOString(),
  consumedAt: new Date(startTime - 60_000).toISOString(), mediaStartBy: new Date(startTime - 30_000).toISOString(),
  mediaStartedAt: new Date(startTime - 45_000).toISOString() };
const versionFor = (number: number): HistoryVersion => ({ ownerPrincipalId, primaryDexterId, threadId,
  contextEpoch: String(number), threadEventSequence: String(number * 3), digest: String(number).repeat(64) });
function contextFor(version: HistoryVersion): EligiblePhoneCall {
  return { bindingId, callSid, state: 'open', context: { primaryDexterId, threadId, displayName: 'Dexter',
    messages: [], history: { version, pageDigest: 'a'.repeat(64), localContext: '', nextBeforeSequence: null,
      hasMore: false, omittedForBudget: false } } };
}
const changed = () => new PrimaryApiError('http_error', 409, { code: 'context_changed', retryable: false });
const fragment = (role: 'user' | 'assistant', text: string): LiveTranscriptSnapshot => ({
  revision: 1, callerRevision: role === 'user' ? 1 : 0,
  fragments: [{ eventId: 'fragment_1', role, text, startMs: 0, endMs: 20, sequence: 1 }],
});
function pending<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
}
async function until(predicate: () => boolean) {
  for (let count = 0; count < 30 && !predicate(); count++) await Promise.resolve();
  assert.equal(predicate(), true, 'expected asynchronous boundary was not reached');
}
function harness(initial?: LiveGenerationJournal, overrides: Partial<LiveGenerationOptions> = {}) {
  let time = startTime;
  let version = versionFor(1);
  let ordinal = initial?.segments.length ?? 0;
  const persisted: LiveGenerationJournal[] = [];
  const problems: string[] = [];
  const audio: Array<{ payload: string; sessionId: string }> = [];
  let clears = 0;
  const operations: Array<{ name: string; requestId?: string; version?: HistoryVersion }> = [];
  const sessions: Array<{ input: Parameters<LiveGenerationOptions['open']>[0]; handle: GenerationHandle;
    audio: Array<{ payload: string; callInputStartSample: number }>; publications: Array<Parameters<GenerationHandle['publish']>[0]>;
    closes: string[]; refuseAudio: boolean }> = [];
  const api: EligibleContextClient = {
    async fetch() { operations.push({ name: 'fetch' }); return contextFor(structuredClone(version)); },
    async assert(_binding, requested) {
      operations.push({ name: 'assert', version: structuredClone(requested) });
      if (requested.digest !== version.digest) throw changed();
      return { bindingId, callSid, current: true, version: structuredClone(requested) };
    },
    async delivery(_binding, id, input) {
      operations.push({ name: 'delivery', requestId: id, version: structuredClone(input.version) });
      if (input.version.digest !== version.digest) throw changed();
      return { bindingId, callSid, requestId: id, version: structuredClone(input.version), delivery: {
        state: 'ready', source: { kind: 'assistant', messageId, eventSequence: input.version.threadEventSequence,
          provenance: { messageCreatedAt: when, eventId, eventOccurredAt: when, runId } }, text: 'The eligible saved answer.',
      } };
    },
  };
  const options: LiveGenerationOptions = { callId, binding, clocks, initial, context: api,
    now: () => time,
    newId: () => `aaaaaaaa-aaaa-4aaa-8aaa-${String(++ordinal).padStart(12, '0')}`,
    persist: value => { persisted.push(structuredClone(value)); },
    onProblem: code => { problems.push(code); },
    onAudio: (payload, attached) => { audio.push({ payload, sessionId: attached.sessionId }); },
    clearPlayback: () => { clears++; },
    async open(input) {
      const sid = `session_${input.ordinal}`;
      const saved = { input, audio: [] as Array<{ payload: string; callInputStartSample: number }>,
        publications: [] as Array<Parameters<GenerationHandle['publish']>[0]>, closes: [] as string[], refuseAudio: false,
        handle: undefined as unknown as GenerationHandle };
      saved.handle = { sessionId: sid,
        appendAudio(frame) { saved.audio.push(frame); return !saved.refuseAudio; },
        async publish(value) { saved.publications.push(structuredClone(value)); return { eventId: `command_${saved.publications.length}`, status: 'accepted' }; },
        async close(reason) {
          saved.closes.push(reason);
          const result: LiveSessionClosed = { callId, sessionId: sid, finalized: true, reason,
            transcriptPersisted: true, outputAudioDiscarded: true };
          input.callbacks.onClosed(result);
          return result;
        },
      };
      sessions.push(saved);
      return saved.handle;
    },
    ...overrides,
  };
  const controller = createLiveGenerationController(options);
  return { controller, options, sessions, persisted, problems, audio, api, operations,
    get clears() { return clears; }, setTime: (value: number) => { time = value; },
    advanceVersion: () => { version = versionFor(Number(version.contextEpoch) + 1); } };
}

test('generation start pins the current eligible version and original clocks before enabling output', async () => {
  const h = harness();
  const attachment = await h.controller.start();
  assert.equal(attachment.sessionId, 'session_1');
  assert.deepEqual(h.operations.map(value => value.name), ['fetch', 'assert']);
  assert.deepEqual(h.controller.snapshot().clocks, clocks, 'the host already admitted media before its fixed grace expired');
  assert.equal(h.sessions[0].input.allowOpening, true);
  assert.equal(h.persisted.some(value => value.segments[0]?.state === 'starting'), true);
  h.sessions[0].input.callbacks.onAudio('audible');
  assert.deepEqual(h.audio, [{ payload: 'audible', sessionId: 'session_1' }]);
  await h.controller.stop();
});

test('first inbound media requires a saved host observation within the original grace', async () => {
  const { mediaStartedAt: _started, ...unobserved } = clocks;
  for (const invalid of [unobserved,
    { ...clocks, mediaStartedAt: clocks.mediaStartBy },
    { ...clocks, mediaStartedAt: new Date(Date.parse(clocks.consumedAt) - 1).toISOString() },
  ]) assert.throws(() => harness(undefined, { clocks: invalid }));
  assert.throws(() => harness(undefined, { clocks: { ...clocks,
    consumedAt: when, mediaStartBy: new Date(startTime + 30_000).toISOString(),
    mediaStartedAt: new Date(startTime + 1).toISOString() } }),
  (error: unknown) => error instanceof LiveGenerationError && error.code === 'media_start_unobserved');
  const outbound = harness(undefined, { clocks: { callDeadlineAt: clocks.callDeadlineAt } });
  await outbound.controller.start();
  assert.equal(outbound.controller.snapshot().clocks.consumedAt, undefined, 'old outbound calls have no inbound grace');
  await outbound.controller.stop();
});

test('observed version change replaces only Live, preserving call identity, clocks and original result request', async () => {
  const h = harness();
  const first = await h.controller.start();
  h.sessions[0].input.callbacks.onTranscript(fragment('user', 'Read my portfolio.'));
  h.advanceVersion();
  const delivered = await h.controller.deliver(requestId, { kind: 'assistant', messageId });
  assert.equal(delivered.attachment.sessionId, 'session_2');
  assert.notEqual(delivered.attachment.generationId, first.generationId);
  assert.equal(h.sessions[0].closes[0], 'context_replaced');
  assert.equal(h.sessions[1].input.allowOpening, false, 'replacement does not repeat opening or automatic mission');
  assert.equal(h.sessions[1].publications[0].requestId, requestId);
  assert.equal(h.sessions[1].publications[0].delivery.text, 'The eligible saved answer.');
  assert.equal(h.sessions[0].publications.length, 0);
  assert.deepEqual(h.controller.snapshot().binding, binding);
  assert.deepEqual(h.controller.snapshot().clocks, clocks);
  assert.deepEqual(h.controller.snapshot().segments[0].transcript, fragment('user', 'Read my portfolio.'));
  assert.equal(h.problems.includes('generation_closed'), false, 'intentional Live close is not a provider-call hangup');
  assert.equal(h.operations.some(value => value.name === 'submit'), false);
  await h.controller.stop();
});

test('old callbacks cannot overwrite a new segment, produce audio, or close its delivery attachment', async () => {
  const h = harness(); await h.controller.start();
  const old = h.sessions[0].input.callbacks;
  old.onTranscript(fragment('assistant', 'Previous fragment.'));
  h.advanceVersion(); const second = await h.controller.beforeReuse();
  h.sessions[1].input.callbacks.onTranscript(fragment('user', 'New session fragment.'));
  old.onAudio('old audio'); old.onTranscript(fragment('assistant', 'Changed old fragment.'));
  old.onClosed({ callId, sessionId: 'session_1', finalized: true, reason: 'late_close', transcriptPersisted: true, outputAudioDiscarded: false });
  assert.equal(h.audio.length, 0);
  assert.deepEqual(h.controller.snapshot().deliveryAttachment, second);
  const segments = h.controller.snapshot().segments;
  assert.deepEqual(segments.map(value => value.transcript.fragments[0].text), ['Previous fragment.', 'New session fragment.']);
  assert.equal(segments[0].outputDiscarded, true);
  assert.equal('assistantPlaybackComplete' in segments[0], false);
  await h.controller.stop();
});

test('rotation buffers only unsent caller frames and retains their observed call-sample positions', async () => {
  const h = harness(); await h.controller.start();
  const sent = { payload: Buffer.from([1, 2]).toString('base64'), callInputStartSample: 0 };
  assert.equal(h.controller.appendAudio(sent), 'sent');
  const blocked = pending<void>();
  const originalOpen = h.options.open;
  h.options.open = async input => { if (input.ordinal === 2) await blocked.promise; return originalOpen(input); };
  h.advanceVersion();
  const replacing = h.controller.beforeReuse();
  await until(() => h.controller.snapshot().segments.length === 2);
  const buffered = { payload: Buffer.from([3, 4]).toString('base64'), callInputStartSample: 2 };
  assert.equal(h.controller.appendAudio(buffered), 'buffered');
  assert.equal(h.controller.bufferedAudioBytes(), 2);
  blocked.resolve(); await replacing;
  assert.deepEqual(h.sessions[0].audio, [sent]);
  assert.deepEqual(h.sessions[1].audio, [buffered], 'previously sent audio is never replayed into replacement');
  assert.equal(h.controller.bufferedAudioBytes(), 0);
  const [old, current] = h.controller.snapshot().segments;
  assert.deepEqual([old.inputFirstSample, old.inputLastSampleExclusive], [0, 2]);
  assert.deepEqual([current.inputFirstSample, current.inputLastSampleExclusive], [2, 4]);
  await h.controller.stop();
});

test('buffer overflow refuses explicitly and prevents a pending generation from quietly becoming active', async () => {
  const waiting = pending<EligiblePhoneCall>();
  const h = harness(undefined, { maxBufferedAudioBytes: 3 });
  h.api.fetch = async () => waiting.promise;
  const failed = assert.rejects(h.controller.start(), (error: unknown) => error instanceof LiveGenerationError && error.code === 'input_unavailable');
  await until(() => h.controller.snapshot().phase === 'replacing');
  assert.equal(h.controller.appendAudio({ payload: 'AQI=', callInputStartSample: 0 }), 'buffered');
  assert.equal(h.controller.appendAudio({ payload: 'AwQ=', callInputStartSample: 2 }), 'refused');
  assert.equal(h.controller.bufferedAudioBytes(), 2, 'accepted buffer contents remain intact');
  assert.equal(h.controller.snapshot().inputGap, 'buffer_overflow');
  assert.equal(h.problems.includes('input_buffer_overflow'), true);
  waiting.resolve(contextFor(versionFor(1))); await failed;
  assert.equal(h.sessions.length, 0);
  assert.equal(h.controller.snapshot().deliveryAttachment, undefined);
  await h.controller.stop();
});

test('an uncertain audio send is never requeued or replayed', async () => {
  const h = harness(); await h.controller.start();
  h.sessions[0].refuseAudio = true;
  const frame = { payload: 'AQI=', callInputStartSample: 0 };
  assert.equal(h.controller.appendAudio(frame), 'refused');
  assert.equal(h.controller.appendAudio(frame), 'refused');
  assert.equal(h.sessions[0].audio.length, 1);
  assert.equal(h.controller.snapshot().inputGap, 'transport_refused');
  assert.equal(h.controller.bufferedAudioBytes(), 0);
  await h.controller.stop();
});

test('an audio adapter throw after a possible send retains the sample gap and never retries that frame', async () => {
  const h = harness(); await h.controller.start();
  const sent: Array<{ payload: string; callInputStartSample: number }> = [];
  h.sessions[0].handle.appendAudio = frame => { sent.push(frame); throw new Error('connection_lost_after_send'); };
  const frame = { payload: 'AQI=', callInputStartSample: 0 };
  assert.equal(h.controller.appendAudio(frame), 'refused');
  assert.equal(h.controller.appendAudio(frame), 'refused');
  assert.deepEqual(sent, [frame]);
  assert.equal(h.controller.snapshot().inputNextSample, 2);
  assert.equal(h.controller.snapshot().inputGap, 'transport_refused');
  assert.equal(h.controller.snapshot().phase, 'unavailable');
  assert.equal(h.controller.bufferedAudioBytes(), 0);
  assert.equal(h.problems.includes('input_transport_refused'), true);
  await h.controller.stop();
});

test('stop during an outstanding context read prevents a later session start and reports buffered input', async () => {
  const waiting = pending<EligiblePhoneCall>();
  const h = harness(); h.api.fetch = async () => waiting.promise;
  const starting = assert.rejects(h.controller.start(), (error: unknown) => error instanceof LiveGenerationError && error.code === 'call_stopped');
  await until(() => h.controller.snapshot().phase === 'replacing');
  h.controller.appendAudio({ payload: 'AQI=', callInputStartSample: 0 });
  const stopping = h.controller.stop();
  waiting.resolve(contextFor(versionFor(1)));
  await starting; await stopping;
  assert.equal(h.sessions.length, 0);
  assert.equal(h.controller.snapshot().phase, 'stopped');
  assert.equal(h.problems.includes('undelivered_buffered_audio'), true);
});

test('two fresh startup assertion refusals suppress both candidates and end the bounded retry', async () => {
  const h = harness();
  const open = h.options.open;
  h.options.open = async input => { input.callbacks.onAudio('early'); return open(input); };
  h.api.assert = async () => { throw changed(); };
  await assert.rejects(h.controller.start(), (error: unknown) => error instanceof PrimaryApiError && error.remoteCode === 'context_changed');
  assert.equal(h.audio.length, 0);
  assert.equal(h.controller.snapshot().phase, 'unavailable');
  assert.equal(h.controller.snapshot().segments[0].outputDiscarded, true);
  assert.equal(h.sessions.length, 2, 'startup permits one fresh candidate without an endless replacement loop');
  assert.equal(h.sessions.every(value => value.audio.length === 0 && value.publications.length === 0), true);
  await h.controller.stop();
});

test('successor input and publication do not wait for retired admission or provider finalization', async () => {
  const admission = pending<void>(); const finalization = pending<void>();
  const h = harness(undefined, { beforeRetire: async id => {
    if (id.endsWith('000000000001')) await admission.promise;
  } });
  await h.controller.start();
  const oldClose = h.sessions[0].handle.close;
  h.sessions[0].handle.close = async reason => { await finalization.promise; return oldClose(reason); };
  h.advanceVersion();
  const next = await h.controller.beforeReuse();
  assert.equal(next.sessionId, 'session_2');
  assert.equal(h.sessions[0].closes.length, 0);
  assert.equal(h.controller.snapshot().segments[0].state, 'retiring');
  assert.equal(h.controller.appendAudio({ payload: 'AQI=', callInputStartSample: 0 }), 'sent');
  await h.controller.deliver(requestId, { kind: 'assistant', messageId });
  assert.equal(h.sessions[1].publications.length, 1);
  admission.resolve();
  h.advanceVersion();
  const third = await h.controller.beforeReuse();
  finalization.resolve();
  await until(() => h.controller.snapshot().segments[0].state === 'closed');
  assert.deepEqual(h.controller.snapshot().deliveryAttachment, third);
  assert.equal(h.controller.snapshot().segments[2].state, 'active', 'old finalization never selects the latest segment');
  assert.deepEqual(h.controller.snapshot().clocks, clocks);
  await h.controller.stop();
});

test('default unsent capacity covers delayed replacement beyond eight seconds and preserves observation times', async () => {
  const h = harness(); await h.controller.start();
  const sent = { payload: 'AQI=', callInputStartSample: 0, observedAt: startTime };
  assert.equal(h.controller.appendAudio(sent), 'sent');
  const fetched = pending<EligiblePhoneCall>();
  h.advanceVersion(); h.api.fetch = async () => fetched.promise;
  const replacing = h.controller.beforeReuse();
  await until(() => h.controller.snapshot().phase === 'replacing');
  const queued = Array.from({ length: 1_600 }, (_, index) => ({ payload: Buffer.alloc(160, index % 255).toString('base64'),
    callInputStartSample: 2 + index * 160, observedAt: startTime + index * 20 }));
  for (const frame of queued) { h.setTime(frame.observedAt); assert.equal(h.controller.appendAudio(frame), 'buffered'); }
  assert.equal(h.controller.bufferedAudioBytes(), 256_000);
  fetched.resolve(contextFor(versionFor(2))); await replacing;
  assert.deepEqual(h.sessions[0].audio, [sent]);
  assert.deepEqual(h.sessions[1].audio, queued);
  assert.equal(h.controller.snapshot().inputNextSample, 256_002);
  assert.deepEqual(h.controller.snapshot().clocks, clocks);
  await h.controller.stop();
});

test('a stale startup candidate gets no caller input and one fresh candidate drains before publication', async () => {
  const h = harness(); const open = h.options.open;
  h.options.open = async input => {
    const result = await open(input);
    input.callbacks.onAudio('before_assertion');
    if (input.ordinal === 1) h.advanceVersion();
    return result;
  };
  const frame = { payload: 'AQI=', callInputStartSample: 0, observedAt: startTime };
  h.controller.appendAudio(frame);
  const starting = h.controller.start();
  const delivered = h.controller.deliver(requestId, { kind: 'assistant', messageId });
  assert.equal((await starting).sessionId, 'session_2');
  await delivered;
  assert.equal(h.sessions[0].audio.length, 0);
  assert.equal(h.sessions[0].publications.length, 0);
  assert.deepEqual(h.sessions[1].audio, [frame]);
  assert.equal(h.sessions[1].publications.length, 1);
  assert.equal(h.controller.snapshot().segments[1].inputLastSampleExclusive, 2);
  assert.equal(h.audio.length, 0);
  assert.equal(h.sessions[1].input.allowOpening, true, 'an unexposed initial candidate does not consume the initial opening');
  await h.controller.stop();
});

test('candidate retry shares one transition budget instead of restarting its timeout', async () => {
  const h = harness(); const fetch = h.api.fetch; const open = h.options.open;
  let reads = 0;
  h.api.fetch = async scope => {
    reads++; h.setTime(startTime + (reads === 1 ? 20_000 : LIVE_GENERATION_TRANSITION_MS + 1));
    return fetch(scope);
  };
  h.options.open = async input => { const value = await open(input); h.setTime(startTime + 30_000); h.advanceVersion(); return value; };
  await assert.rejects(h.controller.start(), (error: unknown) => error instanceof LiveGenerationError
    && error.code === 'generation_transition_timeout');
  assert.equal(reads, 2); assert.equal(h.sessions.length, 1);
  assert.equal(h.controller.snapshot().deliveryAttachment, undefined);
  assert.deepEqual(h.controller.snapshot().clocks, clocks);
  await h.controller.stop();
});

test('a call deadline shorter than the transition budget still prevents startup', async () => {
  const shortClocks = { ...clocks, callDeadlineAt: new Date(startTime + 500).toISOString() };
  const h = harness(undefined, { clocks: shortClocks });
  h.api.fetch = async () => { h.setTime(startTime + 501); return contextFor(versionFor(1)); };
  await assert.rejects(h.controller.start(), (error: unknown) => error instanceof LiveGenerationError && error.code === 'call_deadline');
  assert.equal(h.sessions.length, 0);
  assert.deepEqual(h.controller.snapshot().clocks, shortClocks);
  await h.controller.stop();
});

test('stop tracks a late opening result through its exact close without attaching or replaying input', async () => {
  const opened = pending<GenerationHandle>(); const closed = pending<void>();
  const h = harness(); const open = h.options.open;
  h.options.open = async input => { await open(input); return opened.promise; };
  const starting = assert.rejects(h.controller.start(), (error: unknown) => error instanceof LiveGenerationError && error.code === 'call_stopped');
  await until(() => h.sessions.length === 1);
  h.controller.appendAudio({ payload: 'AQI=', callInputStartSample: 0 });
  const handle = h.sessions[0].handle; const close = handle.close;
  handle.close = async reason => { await closed.promise; return close(reason); };
  let settled = false;
  const stopping = h.controller.stop().then(() => { settled = true; });
  await starting;
  assert.equal(settled, false);
  opened.resolve(handle);
  await Promise.resolve(); await Promise.resolve();
  assert.equal(settled, false);
  closed.resolve(); await stopping;
  assert.equal(h.sessions[0].closes.length, 1);
  assert.equal(h.sessions[0].audio.length, 0);
  assert.equal(h.controller.snapshot().deliveryAttachment, undefined);
  assert.equal(h.controller.snapshot().phase, 'stopped');
});

test('failed retired finalization stays with its source and does not fail the successor', async () => {
  const h = harness(); await h.controller.start();
  h.sessions[0].handle.close = async () => { throw new Error('retired transport failed'); };
  h.advanceVersion(); const next = await h.controller.beforeReuse();
  await until(() => h.controller.snapshot().segments[0].state === 'abandoned');
  assert.deepEqual(h.controller.snapshot().deliveryAttachment, next);
  assert.equal(h.controller.snapshot().segments[0].providerFinalized, false);
  assert.equal(h.controller.snapshot().segments[0].transcriptPersisted, false);
  assert.deepEqual(h.problems, []);
  await h.controller.stop();
});

test('a second delivery-version refusal ends the bounded retry without publishing any result', async () => {
  const h = harness(); await h.controller.start();
  let attempts = 0;
  h.api.delivery = async () => { attempts++; throw changed(); };
  await assert.rejects(h.controller.deliver(requestId, { kind: 'assistant', messageId }), (error: unknown) => error instanceof PrimaryApiError);
  assert.equal(attempts, 2);
  assert.equal(h.sessions.length, 2);
  assert.equal(h.sessions.reduce((count, value) => count + value.publications.length, 0), 0);
  assert.equal(h.controller.snapshot().phase, 'unavailable');
  await h.controller.stop();
});

test('unavailable eligible delivery returns the reason and never injects a raw saved answer', async () => {
  const h = harness(); await h.controller.start();
  h.api.delivery = async (_scope, id, input) => ({ bindingId, callSid, requestId: id, version: input.version,
    delivery: { state: 'unavailable', reason: 'source_not_in_projection' } });
  const result = await h.controller.deliver(requestId, { kind: 'assistant', messageId });
  assert.equal(result.response.delivery.state, 'unavailable');
  assert.equal(result.publication, undefined);
  assert.equal(h.sessions[0].publications.length, 0);
  await h.controller.stop();
});

test('restart preserves raw transcript segments, reports the input gap and fetches fresh context', async () => {
  const original = harness(); await original.controller.start();
  original.sessions[0].input.callbacks.onTranscript(fragment('user', 'Saved source text.'));
  const saved = original.controller.snapshot();
  const h = harness(saved);
  assert.equal(h.problems.includes('restart_input_unconfirmed'), true);
  assert.equal(h.controller.snapshot().segments[0].state, 'abandoned');
  assert.equal(h.controller.snapshot().segments[0].providerFinalized, false);
  await h.controller.start();
  assert.deepEqual(h.operations.map(value => value.name), ['fetch', 'assert']);
  assert.equal(h.sessions[0].input.allowOpening, false);
  assert.deepEqual(h.sessions[0].input.context.context.messages, [], 'old transcript is not restored as eligible startup history');
  assert.deepEqual(h.controller.snapshot().segments[0].transcript, saved.segments[0].transcript);
  assert.deepEqual(h.controller.snapshot().clocks, clocks);
  await h.controller.stop(); await original.controller.stop();
});

test('new clocks, call SID and scope cannot rebind an existing journal', async () => {
  const h = harness(); await h.controller.start(); const saved = h.controller.snapshot();
  for (const changes of [
    { clocks: { ...clocks, callDeadlineAt: new Date(startTime + 600_000).toISOString() } },
    { clocks: { ...clocks, mediaStartBy: new Date(startTime + 30_000).toISOString() } },
    { binding: { ...binding, callSid: `CA${'2'.repeat(32)}` } },
    { binding: { ...binding, threadId: requestId } },
  ]) assert.throws(() => createLiveGenerationController({ ...h.options, initial: saved, ...changes }),
    (error: unknown) => error instanceof LiveGenerationError && error.code === 'generation_identity_changed');
  h.setTime(startTime + 300_000);
  await assert.rejects(h.controller.beforeReuse(), (error: unknown) => error instanceof LiveGenerationError && error.code === 'call_deadline');
  h.sessions[0].input.callbacks.onAudio('after deadline');
  assert.equal(h.audio.length, 0);
  await h.controller.stop();
});

test('unexpected active Live closure gates output but does not manufacture canonical turn completion', async () => {
  const h = harness(); await h.controller.start();
  h.sessions[0].input.callbacks.onClosed({ callId, sessionId: 'session_1', finalized: false, reason: 'transport_lost',
    transcriptPersisted: false, outputAudioDiscarded: true });
  assert.equal(h.controller.snapshot().phase, 'unavailable');
  assert.equal(h.controller.snapshot().deliveryAttachment, undefined);
  assert.equal(h.problems.includes('generation_closed'), true);
  assert.equal(h.controller.snapshot().segments[0].providerFinalized, false);
  assert.equal('canonicalTurns' in h.controller.snapshot(), false);
  await h.controller.stop();
});

test('invalid retired callbacks preserve the last valid source without disabling its successor', async () => {
  const failures: Array<(callbacks: GenerationCallbacks) => void> = [
    callbacks => callbacks.onTranscript({ revision: -1, callerRevision: 0, fragments: [] }),
    callbacks => callbacks.onTranscript(fragment('user', 'Rewritten source.')),
    callbacks => callbacks.onClosed({ callId: bindingId, sessionId: 'session_1', finalized: true,
      reason: 'context_replaced', transcriptPersisted: true, outputAudioDiscarded: true }),
  ];
  for (const fail of failures) {
    const admission = pending<void>();
    const h = harness(undefined, { beforeRetire: () => admission.promise });
    try {
      await h.controller.start();
      const saved = fragment('user', 'Original source.');
      h.sessions[0].input.callbacks.onTranscript(saved);
      h.advanceVersion();
      const successor = await h.controller.beforeReuse();
      assert.equal(h.controller.snapshot().segments[0].state, 'retiring');
      fail(h.sessions[0].input.callbacks);
      const old = h.controller.snapshot().segments[0];
      assert.equal(old.state, 'abandoned');
      assert.equal(old.providerFinalized, false);
      assert.equal(old.transcriptPersisted, false);
      assert.deepEqual(old.transcript, saved);
      assert.equal(h.controller.snapshot().phase, 'active');
      assert.deepEqual(h.controller.snapshot().deliveryAttachment, successor);
      assert.deepEqual(h.problems, []);
      h.sessions[1].input.callbacks.onAudio('successor remains audible');
      assert.deepEqual(h.audio, [{ payload: 'successor remains audible', sessionId: 'session_2' }]);
      const delivered = await h.controller.deliver(requestId, { kind: 'assistant', messageId });
      assert.deepEqual(delivered.attachment, successor);
      // A later valid close must not turn the rejected source into confirmed evidence.
      admission.resolve();
      await until(() => h.sessions[0].closes.length === 1);
      assert.equal(h.controller.snapshot().segments[0].state, 'abandoned');
      assert.equal(h.controller.snapshot().segments[0].providerFinalized, false);
    } finally {
      admission.resolve();
      await h.controller.stop();
    }
  }
});


test('replacement refuses a repeated provider session ID and restart refuses a duplicate saved session', async () => {
  const f = harness(); await f.controller.start();
  const existing = f.sessions[0]!.handle.sessionId;
  f.options.open = async () => ({ sessionId: existing, appendAudio: () => true,
    publish: async () => ({ eventId: 'unused', status: 'accepted' }),
    close: async reason => ({ callId, reason, finalized: false, transcriptPersisted: false, outputAudioDiscarded: true }) });
  f.advanceVersion();
  await assert.rejects(f.controller.beforeReuse(), (error: unknown) => error instanceof LiveGenerationError && error.code === 'generation_session_reused');
  const saved = f.controller.snapshot();
  saved.segments.at(-1)!.sessionId = existing;
  assert.equal(liveGenerationJournalSchema.safeParse(saved).success, false);
});
