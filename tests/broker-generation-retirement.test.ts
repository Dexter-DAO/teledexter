import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { createGenerationVoice } from '../src/broker/generationVoice.js';
import { brokerIdentity } from '../src/broker/backend.js';
import { BrokerStore } from '../src/broker/store.js';
import { PrimaryApiError, type PrimaryApiClient, type PrimarySubmission } from '../src/broker/primaryApi.js';
import type { EligibleContextClient, EligiblePhoneCall, HistoryVersion } from '../src/broker/eligibleContext.js';
import type { BrokerLiveSession, BrokerLiveSessionOptions, LiveBackendChannel, LiveTranscriptSnapshot } from '../src/live/types.js';

const bindingId = '11111111-1111-4111-8111-111111111111';
const primaryDexterId = '22222222-2222-4222-8222-222222222222';
const threadId = '33333333-3333-4333-8333-333333333333';
const ownerPrincipalId = '44444444-4444-4444-8444-444444444444';
const callSid = `CA${'1'.repeat(32)}`;
const speech = Buffer.alloc(160, 0).toString('base64');
const pause = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
async function until(predicate: () => boolean) {
  for (let count = 0; count < 300 && !predicate(); count++) await pause(10);
  assert.equal(predicate(), true, 'expected generation boundary was not reached');
}
const version = (ordinal: number): HistoryVersion => ({ ownerPrincipalId, primaryDexterId, threadId,
  contextEpoch: String(ordinal), threadEventSequence: String(ordinal), digest: String(ordinal).repeat(64) });
function context(ordinal: number): EligiblePhoneCall {
  return { bindingId, callSid, state: 'open', context: { primaryDexterId, threadId, displayName: 'Dexter', messages: [],
    history: { version: version(ordinal), pageDigest: 'a'.repeat(64), localContext: '',
      nextBeforeSequence: null, hasMore: false, omittedForBudget: false } } };
}
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}

async function fixture(t: test.TestContext) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-generation-retirement-'));
  const store = new BrokerStore(directory);
  const call = store.reserve({ requestId: randomUUID(), number: '+12025550123', direction: 'outbound',
    purpose: 'Read my portfolio.', callSid }).call;
  store.recordPrimaryBinding(call.id, context(1));
  store.update(call.id, { mediaStartedAt: new Date().toISOString() });
  let epoch = 1; let clears = 0;
  let beforeFetch: (() => Promise<void>) | undefined;
  let beforeReady: (() => Promise<void>) | undefined;
  let beforeAssert: (() => Promise<void>) | undefined;
  const problems: string[] = [], audio: string[] = [], posts: PrimarySubmission[] = [];
  const voices: Array<{ options: BrokerLiveSessionOptions; voice: BrokerLiveSession; input: string[];
    closes: string[]; caption: LiveTranscriptSnapshot; channel: LiveBackendChannel; closeGate?: Promise<void>; playbackPaused: boolean }> = [];
  const primary = {
    async submit(_binding: string, requestId: string, input: PrimarySubmission) {
      posts.push(structuredClone(input)); return { bindingId, requestId, state: 'queued', replayed: false };
    },
    async result(_binding: string, requestId: string) { return { bindingId, requestId, state: 'running' }; },
    async captureTurn(_binding: string, turn: { sourceEventId: string; revision: number }) {
      return { bindingId, sourceEventId: turn.sourceEventId, recordId: turn.sourceEventId, revision: turn.revision, replayed: false };
    },
  } as unknown as PrimaryApiClient;
  const eligible: EligibleContextClient = {
    async fetch() { await beforeFetch?.(); return context(epoch); },
    async assert(_binding, expected) {
      await beforeAssert?.();
      if (expected.digest !== version(epoch).digest) throw new PrimaryApiError('http_error', 409, { code: 'context_changed' });
      return { bindingId, callSid, current: true, version: expected };
    },
    async delivery() { throw new Error('Unexpected result publication'); },
  };
  const live = createGenerationVoice({ call: store.get(call.id)!, callDeadlineAt: new Date(Date.now() + 60_000).toISOString(),
    apiKey: 'fixture-key', voice: 'marin', store, primary, context: eligible,
    onAudio: value => { audio.push(value); }, onReady() {}, onClosed() {},
    onPlaybackClear: () => { clears++; }, onProblem: code => { problems.push(code); },
    voiceFactory(options) {
      const sessionId = `retirement_${voices.length + 1}`;
      const abort = new AbortController();
      const saved = { options, input: [] as string[], closes: [] as string[], closeGate: undefined as Promise<void> | undefined, playbackPaused: false,
        caption: { revision: 0, callerRevision: 0, fragments: [] } as LiveTranscriptSnapshot,
        voice: undefined as unknown as BrokerLiveSession, channel: undefined as unknown as LiveBackendChannel };
      saved.channel = { signal: abort.signal, getContext: () => structuredClone(saved.caption),
        subscribeContext: () => () => {}, publish: async () => { throw new Error('Unexpected legacy publication'); } };
      let closing: ReturnType<BrokerLiveSession['close']> | undefined;
      saved.voice = { appendAudio(payload) { saved.input.push(payload); return true; },
        appendContext: async () => { throw new Error('Unexpected flattened context'); },
        appendInstructions: async () => ({ eventId: 'instructions', status: 'accepted' }),
        appendCommentary: async () => ({ eventId: 'commentary', status: 'accepted' }),
        interruptPlayback: async () => { saved.playbackPaused = true; return { eventId: 'interrupt', status: 'accepted' }; },
        pausePlayback() { saved.playbackPaused = true; options.onPlaybackClear?.('application_interrupt'); },
        resumePlayback() { saved.playbackPaused = false; },
        getTranscript: () => structuredClone(saved.caption),
        close(reason = 'closed') {
          if (closing) return closing;
          saved.closes.push(reason); abort.abort();
          closing = (async () => {
            await saved.closeGate;
            options.onPlaybackClear?.('session_closed');
            const result = { callId: call.id, sessionId, finalized: true, transcriptPersisted: true,
              outputAudioDiscarded: true, reason };
            options.onClosed(result); return result;
          })();
          return closing;
        },
      };
      voices.push(saved);
      void Promise.resolve().then(async () => { await beforeReady?.(); options.onReady({ callId: call.id, sessionId }); });
      return saved.voice;
    },
  });
  t.after(async () => { await live.close(); store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  await until(() => store.get(call.id)!.liveGenerations?.phase === 'active');
  return { live, store, callId: call.id, voices, problems, audio, posts, get clears() { return clears; },
    advance() { epoch++; }, fetching(fn?: () => Promise<void>) { beforeFetch = fn; }, starting(fn?: () => Promise<void>) { beforeReady = fn; },
    checking(fn?: () => Promise<void>) { beforeAssert = fn; } };
}

test('caller interruption clears queued and new playback before a delayed context assertion completes', async t => {
  const f = await fixture(t), gate = deferred();
  f.checking(() => gate.promise);
  const before = f.clears;
  const interruption = f.live.interruptPlayback('Listen to the caller.');
  try {
    assert.equal(f.clears, before + 1, 'the caller must not wait for database I/O to silence playback');
    f.voices[0].options.onAudio('old playback', { deliveryId: 'during_assert' });
    assert.deepEqual(f.audio, []);
    gate.resolve(); await interruption;
    f.voices[0].options.onAudio('still paused', { deliveryId: 'before_resume' });
    assert.deepEqual(f.audio, []);
    f.live.resumePlayback();
    f.voices[0].options.onAudio('resumed', { deliveryId: 'after_resume' });
    assert.deepEqual(f.audio, ['resumed']);
  } finally { gate.resolve(); await interruption.catch(() => {}); }
});

test('an interruption finishing after the caller resumes cannot pause provider playback again', async t => {
  const f = await fixture(t), gate = deferred();
  f.checking(() => gate.promise);
  const interruption = f.live.interruptPlayback('Listen to the caller.');
  try {
    f.live.resumePlayback();
    gate.resolve(); await interruption;
    assert.equal(f.voices[0].playbackPaused, false);
  } finally { gate.resolve(); await interruption.catch(() => {}); }
});

test('retired voice completion, playback clear and transport errors cannot disturb its active successor', async t => {
  const f = await fixture(t); const gate = deferred();
  f.voices[0].closeGate = gate.promise;
  try {
    f.advance(); await f.live.appendInstructions('Use the current context.');
    const before = f.clears;
    f.voices[0].options.onPlaybackClear?.('session_failed');
    f.voices[0].options.onEvent?.({ type: 'live.error', code: 'retired_error' });
    f.voices[0].options.onAudio('stale', { deliveryId: 'stale' });
    assert.equal(f.clears, before); assert.deepEqual(f.problems, []); assert.deepEqual(f.audio, []);
    gate.resolve();
    await until(() => f.store.get(f.callId)!.liveGenerations!.segments[0].state === 'closed');
    assert.equal(f.clears, before);
    assert.equal(f.store.get(f.callId)!.liveSessionId, 'retirement_2');
    f.voices[1].options.onAudio('current', { deliveryId: 'current' });
    assert.deepEqual(f.audio, ['current']);
    f.voices[1].options.onPlaybackClear?.('application_interrupt');
    assert.equal(f.clears, before + 1);
  } finally { gate.resolve(); }
});

test('successor speech does not prolong an already observed retired-source admission', async t => {
  const f = await fixture(t);
  assert.equal(f.live.appendAudio(speech), true);
  const source = f.voices[0];
  source.caption = { revision: 1, callerRevision: 1, fragments: [{ eventId: 'original_raw_caller', role: 'user',
    text: 'Read my portfolio.', startMs: 1, endMs: 20, sequence: 1 }] };
  await source.options.onTranscript(source.caption);
  const work = source.options.backend.delegate({ callId: f.callId, sessionId: 'retirement_1', delegationId: 'original',
    requestId: 'provider-original', offsetMs: 20, transcript: structuredClone(source.caption) }, source.channel);
  f.advance(); await f.live.appendInstructions('Use the current context.');
  assert.equal(f.voices.length, 2);
  assert.equal(source.closes.length, 0, 'source remains open until its admitted raw source is frozen');
  const ongoing = setInterval(() => { f.live.appendAudio(speech); }, 40);
  try {
    await until(() => f.posts.length === 1);
    assert.equal(f.posts[0].text, 'Read my portfolio.');
    assert.equal(f.posts[0].source.itemId, `live-turn-${brokerIdentity('source', f.callId, 'retirement_1', 'original_raw_caller')}`);
    await until(() => source.closes.length === 1);
    assert.equal(f.store.get(f.callId)!.liveSessionId, 'retirement_2');
    assert.deepEqual(f.problems, []);
  } finally {
    clearInterval(ongoing);
    await f.live.close(); await work;
  }
});

test('buffered speech retains its original host time rather than its later replay time', async t => {
  const f = await fixture(t); const gate = deferred();
  let observedAt = Date.now();
  t.mock.method(Date, 'now', () => observedAt);
  f.fetching(() => gate.promise); f.advance();
  const replacing = f.live.appendInstructions('Use the current context.');
  await until(() => f.store.get(f.callId)!.liveGenerations!.phase === 'replacing');
  const originalTime = observedAt;
  assert.equal(f.live.appendAudio(speech), true);
  observedAt += 2_000;
  gate.resolve(); await replacing;
  await f.live.close();
  const spans = f.store.get(f.callId)!.sourceAcousticSpans!['retirement_2'] as Array<{ startMs: number; endMs: number; lastSpeechAt: number }>;
  assert.equal(spans.length, 1);
  assert.equal(spans[0].lastSpeechAt, originalTime);
  assert.deepEqual([spans[0].startMs, spans[0].endMs], [0, 20]);
});

test('stop during a successor startup closes it without accepting its late ready callback', async t => {
  const f = await fixture(t); const ready = deferred();
  f.starting(() => ready.promise); f.advance();
  const replacing = assert.rejects(f.live.appendInstructions('Use the current context.'));
  await until(() => f.voices.length === 2);
  await f.live.close(); await replacing;
  ready.resolve(); await pause(0);
  const call = f.store.get(f.callId)!;
  assert.equal(call.liveGenerations!.phase, 'stopped');
  assert.equal(call.liveGenerations!.deliveryAttachment, undefined);
  assert.equal(f.voices[1].closes.length, 1);
  assert.equal(f.voices[1].input.length, 0);
  assert.equal(f.posts.length, 0);
});
