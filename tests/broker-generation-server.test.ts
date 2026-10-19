import assert from 'node:assert/strict';
import test from 'node:test';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import Twilio from 'twilio';
import { WebSocket } from 'ws';
import { createBrokerServer } from '../src/broker/server.js';
import { liveResultChunks } from '../src/broker/backend.js';
import { createPrimaryApiClient, PrimaryApiError, type PrimaryApiClient, type PrimarySubmission } from '../src/broker/primaryApi.js';
import { eligiblePhoneCallSchema, type EligibleContextClient, type EligiblePhoneCall, type HistoryVersion } from '../src/broker/eligibleContext.js';
import { BrokerStore } from '../src/broker/store.js';
import { streamToken } from '../src/broker/phone.js';
import { createGenerationVoice, REPEATED_RESULT_NOTE } from '../src/broker/generationVoice.js';
import type { LiveGenerationJournal } from '../src/broker/liveGeneration.js';
import type { BrokerConfig } from '../src/broker/config.js';
import type { BrokerDelegationRecord } from '../src/broker/backend.js';
import type { BrokerLiveSession, BrokerLiveSessionOptions, LiveBackendChannel, LiveTranscriptSnapshot } from '../src/live/types.js';

const sid = `CA${'1'.repeat(32)}`;
const accountSid = `AC${'2'.repeat(32)}`;
const bindingId = '11111111-1111-4111-8111-111111111111';
const primaryDexterId = '22222222-2222-4222-8222-222222222222';
const threadId = '33333333-3333-4333-8333-333333333333';
const ownerPrincipalId = '44444444-4444-4444-8444-444444444444';
const messageId = '55555555-5555-4555-8555-555555555555';
const eventId = '66666666-6666-4666-8666-666666666666';
const runId = '77777777-7777-4777-8777-777777777777';
const at = '2026-09-24T12:00:00.000Z';
const version = (value: number): HistoryVersion => ({ ownerPrincipalId, primaryDexterId, threadId,
  contextEpoch: String(value), threadEventSequence: String(value), digest: String(value).repeat(64) });
const context = (value = 1): EligiblePhoneCall => ({ bindingId, callSid: sid, state: 'open', context: {
  primaryDexterId, threadId, displayName: 'Dexter', messages: [], history: { version: version(value),
    pageDigest: 'a'.repeat(64), localContext: '', nextBeforeSequence: null, hasMore: false, omittedForBudget: false },
} });
const proof = { url: 'https://phone.example.test/broker/answer/call', signature: 'fixture-signature',
  params: { AccountSid: accountSid, CallSid: sid, Direction: 'outbound-api', From: '+12025550100', To: '+12025550123' } };
const wait = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
async function until(predicate: () => boolean, limit = 6000) {
  const end = Date.now() + limit;
  while (!predicate() && Date.now() < end) await wait(10);
  assert.equal(predicate(), true, 'expected composed boundary was not reached');
}

test('successor registration/context crosses actual HTTP with negotiated full metadata; legacy header remains absent', async t => {
  const requests: Array<{ url: string; headers: Record<string, unknown> }> = [];
  let body: unknown = context();
  const server = createServer((request, response) => {
    requests.push({ url: request.url!, headers: { ...request.headers } });
    response.writeHead(200, { 'content-type': 'application/json' }); response.end(JSON.stringify(body));
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise<void>(resolve => { server.closeAllConnections(); server.close(() => resolve()); }));
  const options = { apiBaseUrl: `http://127.0.0.1:${(server.address() as { port: number }).port}`, phoneServiceSecret: 's'.repeat(40) };
  const client = createPrimaryApiClient({ ...options, contextProtocol: { version: 'memory-eligible-v1', parse: value => eligiblePhoneCallSchema.parse(value) } });
  assert.deepEqual(await client.openCall(proof), body);
  assert.deepEqual(await client.getContext(bindingId), body);
  for (const request of requests) {
    assert.equal(request.headers['x-dexter-phone-context-version'], 'memory-eligible-v1');
    assert.match(String(request.headers.authorization), /^Bearer [a-f0-9]{64}$/);
  }
  const wrongOwner = context(); wrongOwner.context.history.version.ownerPrincipalId = randomUUID(); body = wrongOwner;
  await assert.rejects(client.getContext(bindingId), (error: unknown) => error instanceof PrimaryApiError && error.code === 'identity_mismatch');
  const { history: _history, ...legacyContext } = context().context;
  body = { ...context(), context: legacyContext };
  await assert.rejects(client.getContext(bindingId), (error: unknown) => error instanceof PrimaryApiError && error.code === 'invalid_response');
  const legacy = createPrimaryApiClient(options);
  await legacy.openCall(proof);
  assert.equal(requests.at(-1)!.headers['x-dexter-phone-context-version'], undefined);
  assert.throws(() => createPrimaryApiClient({ ...options, contextProtocol: { version: 'unknown', parse: (value: unknown) => value } } as never));
});

async function host(t: test.TestContext, slowClose = false, questionMode = false, boundedCleanup = false,
  settings: { admitted?: boolean; failed?: boolean; failureCode?: string; cancelled?: boolean; sameEpoch?: boolean; resultText?: string; callerText?: string;
    openingStatus?: 'accepted' | 'rejected' | 'unconfirmed'; greetingClip?: Buffer; unconfirmedSpeech?: boolean;
    /** The first delivery of this text fails; with replace, the context also moves on, so the re-send needs a new voice session. */
    failedSpeech?: { text: string; status: 'rejected' | 'unconfirmed'; replace?: boolean } } = {}) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-generation-server-'));
  const config: BrokerConfig = { host: '127.0.0.1', port: 3213, publicOrigin: 'https://phone.example.test', stateDir: directory,
    controlToken: 'c'.repeat(40), apiKey: 'fixture-key', model: 'gpt-live-1', voice: 'marin',
    twilioAccountSid: accountSid, twilioAuthToken: 'fixture-twilio', from: proof.params.From,
    allowedTo: [proof.params.To], apiBaseUrl: 'https://api.example.test', phoneServiceSecret: 's'.repeat(40), maxCallSeconds: 60 };
  const store = new BrokerStore(directory);
  const reserved = store.reserve({ requestId: randomUUID(), number: proof.params.To, direction: 'outbound', purpose: 'Discuss my portfolio', callSid: sid }).call;
  store.recordPrimaryBinding(reserved.id, context());
  let generation = 1, released = false, providerEnds = 0, cursor = 280, fetches=0, assertions=0;
  const providerUpdates: Array<Record<string, unknown>> = [];
  const currentContext = () => { const value = context(generation);
    if(settings.admitted) value.context.history.version.contextEpoch='1';
    if(settings.admitted) value.context.history.admission={invalidationDigest:"a".repeat(64),messages:[],memories:[]};
    if (settings.sameEpoch) value.context.history.version = { ...value.context.history.version,
      threadEventSequence: String(cursor), digest: createHash('sha256').update(String(cursor)).digest('hex') };
    return value; };
  let beforeResult: (() => void) | undefined;
  const posts: Array<{ requestId: string; input: PrimarySubmission }> = [];
  const questionId = '88888888-8888-4888-8888-888888888888';
  const question = { id: questionId, revision: 1 as const, runId, threadId, agent: { id: primaryDexterId, name: 'Dexter' },
    state: 'pending' as const, reply: null, runState: 'waiting_for_reply' as const, createdAt: at,
    expiresAt: '2099-01-01T00:00:00.000Z', question: 'Would you like me to research this stock?',
    context: 'This request concerns research only. '.repeat(20), proposedAnswer: 'Research the stock.',
    yesMeaning: 'Research the stock and explain the findings.', noMeaning: 'Leave this request waiting.' };
  const questionText = [question.question, question.context, question.proposedAnswer, question.yesMeaning, question.noMeaning].join('\n');
  const eligibleText = questionMode ? questionText : settings.resultText ?? 'Your account result includes the complete position and its observation time. '.repeat(16);
  const completed = (requestId: string) => ({ bindingId, requestId, state: 'completed' as const,
    assistant: { messageId, eventId, sequence: '2', text: 'RAW RESULT MUST NOT BE SENT DIRECTLY' } });
  const primary = {
    async submit(_binding: string, requestId: string, input: PrimarySubmission) {
      posts.push({ requestId, input: structuredClone(input) }); return { bindingId, requestId, state: 'queued', replayed: false };
    },
    async result(_binding: string, requestId: string) {
      if (released) { const callback = beforeResult; beforeResult = undefined; callback?.(); generation = Math.max(generation, 2); if (settings.cancelled) return { bindingId, requestId, state: 'cancelled', error: { code: 'cancelled', retryable: false } }; if (settings.failed) return { bindingId, requestId, state: 'failed', error: { code: settings.failureCode ?? 'primary_financial_result_unknown', retryable: false } }; return questionMode ? { bindingId, requestId, state: 'waiting_for_reply', question } : completed(requestId); }
      return { bindingId, requestId, state: 'running' };
    },
    async close() { return { bindingId, callSid: sid, state: 'closed' }; },
    async captureTurn(_binding: string, turn: { sourceEventId: string; revision: number }) {
      return { bindingId, sourceEventId: turn.sourceEventId, recordId: turn.sourceEventId, revision: turn.revision, replayed: false };
    },
  } as unknown as PrimaryApiClient;
  const eligible: EligibleContextClient = {
    async fetch() { fetches++;return currentContext(); },
    async assert(_binding, requested, admission) {
      assertions++;
      if(settings.admitted && admission) return {bindingId,callSid:sid,current:true,version:currentContext().context.history.version,admission};
      if (requested.digest !== currentContext().context.history.version.digest) throw new PrimaryApiError('http_error', 409, { code: 'context_changed', retryable: false });
      return { bindingId, callSid: sid, current: true, version: requested };
    },
    async delivery(_binding, requestId, input) {
      assert.equal(input.source.kind, questionMode ? 'question' : 'assistant');
      if (questionMode) return { bindingId, callSid: sid, requestId, version: input.version,...(input.admission?{admission:input.admission}:{}), delivery: { state: 'ready',
        source: { kind: 'question', messageId, eventSequence: '2', provenance: { messageCreatedAt: at, eventId, eventOccurredAt: at, runId } },
        text: eligibleText, question } };
      return { bindingId, callSid: sid, requestId, version: input.version,...(input.admission?{admission:input.admission}:{}), delivery: { state: 'ready',
        source: { kind: 'assistant', messageId, eventSequence: '2', provenance: {
          messageCreatedAt: at, eventId, eventOccurredAt: at, runId } }, text: eligibleText } };
    },
  };
  const voices: Array<{ options: BrokerLiveSessionOptions; voice: BrokerLiveSession; captions: LiveTranscriptSnapshot;
    channel: LiveBackendChannel; abort: AbortController; spoken: string[]; quiet: string[]; published:string[]; native:Array<{id:string;kind:string;speak:boolean}>; inputs: string[]; closed: string[] }> = [];
  const opening: string[] = [];
  let speechAttempts = 0, failedSpeechUsed = false;
  let finishSlowClose: (() => void) | undefined;
  let finishQuestion: (() => void) | undefined;
  const broker = createBrokerServer({ config, store, primary, eligibleContext: eligible,
    ...(settings.greetingClip ? { greetingClip: settings.greetingClip } : {}),
    provider: { calls: () => ({ async update(input: Record<string, unknown>) { providerEnds++; providerUpdates.push(input); return { status: 'completed' }; } }) } as unknown as ReturnType<typeof Twilio>,
    voiceFactory(options) {
      const sessionId = `generation_${voices.length + 1}`;
      const abort = new AbortController();
      const captions: LiveTranscriptSnapshot = { revision: 0, callerRevision: 0, fragments: [] };
      const spoken: string[] = [], quiet:string[]=[],published:string[]=[],native:Array<{id:string;kind:string;speak:boolean}>=[], inputs: string[] = [], closed: string[] = [];
      const listeners = new Set<(value: LiveTranscriptSnapshot) => void>();
      const channel: LiveBackendChannel = { signal: abort.signal, getContext: () => structuredClone(captions),
        subscribeContext(listener) { listeners.add(listener); return () => listeners.delete(listener); },
        async publish() { throw new Error('Raw source channel publication must not be used'); } };
      const voice: BrokerLiveSession = {
        appendAudio(payload) { inputs.push(payload); return true; },
        async appendContext(text) {quiet.push(text);published.push(text);return {eventId:`quiet_${sessionId}_${quiet.length}`,status:'accepted'};},
        ...(settings.admitted?{async publishDelegation(id:string,update:import('../src/live/types.js').LiveBackendUpdate) {
          native.push({id,kind:update.kind,speak:update.speak??update.kind==='result'});
          return update.speak===false?voice.appendContext(update.text):voice.appendCommentary!(update.text);
        }}:{}),
        async appendInstructions(text) { opening.push(text); return { eventId: `open_${opening.length}`, status: settings.openingStatus ?? 'accepted' }; },
        async appendCommentary(text) { spoken.push(text);published.push(text);
          if (settings.failedSpeech && text === settings.failedSpeech.text && !failedSpeechUsed) {
            failedSpeechUsed = true;
            if (settings.failedSpeech.replace) generation++;
            return { eventId: `comment_${sessionId}_${spoken.length}`, status: settings.failedSpeech.status,
              observedCallerRevision: captions.callerRevision, observedTranscriptSequence: captions.revision };
          }
          if (settings.unconfirmedSpeech && text === eligibleText && ++speechAttempts === 1)
            return { eventId: `comment_${sessionId}_${spoken.length}`, status: 'unconfirmed', reason: 'speech_not_confirmed',
              observedCallerRevision: captions.callerRevision, observedTranscriptSequence: captions.revision };
          if (questionMode && spoken.join('') === eligibleText) await new Promise<void>(resolve => { finishQuestion = resolve; });
          return { eventId: `comment_${sessionId}_${spoken.length}`, status: 'accepted',
          observedCallerRevision: captions.callerRevision, observedTranscriptSequence: captions.revision }; },
        async interruptPlayback() { return { eventId: '', status: 'accepted' }; }, resumePlayback() {},
        getTranscript: () => structuredClone(captions),
        async close(reason = 'closed') {
          closed.push(reason); abort.abort();
          if (slowClose) await new Promise<void>(resolve => { finishSlowClose = resolve; });
          const result = { callId: options.callId, sessionId, finalized: true, transcriptPersisted: true, outputAudioDiscarded: true, reason };
          options.onClosed(result); return result;
        },
      };
      voices.push({ options, voice, captions, channel, abort, spoken, quiet, published, native, inputs, closed });
      queueMicrotask(() => options.onReady({ callId: options.callId, sessionId }));
      return voice;
    },
  });
  await new Promise<void>(resolve => broker.server.listen(0, '127.0.0.1', resolve));
  const route = `/broker/media/${reserved.id}`;
  const ws = new WebSocket(`ws://127.0.0.1:${(broker.server.address() as { port: number }).port}${route}`, {
    headers: { 'x-twilio-signature': Twilio.getExpectedTwilioSignature(config.twilioAuthToken, config.publicOrigin + route, {}) },
  });
  await new Promise<void>((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
  const streamSid = `MZ${'3'.repeat(32)}`;
  ws.send(JSON.stringify({ event: 'start', start: { accountSid, callSid: sid, streamSid,
    customParameters: { token: streamToken(config.controlToken, reserved.id, sid) },
    mediaFormat: { encoding: 'audio/x-mulaw', sampleRate: 8000, channels: 1 } } }));
  await until(() => store.get(reserved.id)?.liveGenerations?.phase === 'active');
  t.after(async () => {
    finishQuestion?.(); finishSlowClose?.(); ws.terminate();
    const closing = broker.close();
    if (boundedCleanup) {
      const cleanup = observedWork(closing);
      await boundary('broker cleanup', cleanup.settled, [cleanup]);
    } else await closing;
    fs.rmSync(directory, { recursive: true, force: true });
  });
  const audio = (silent = false) => ws.send(JSON.stringify({ event: 'media', streamSid, media: { track: 'inbound', payload: Buffer.alloc(160, silent ? 255 : 0).toString('base64') } }));
  const delegate = async (index = 0) => {
    audio(); await until(() => voices[index]!.inputs.length > 0);
    const voice = voices[index]!;
    voice.captions.revision = 1; voice.captions.callerRevision = 1;
    voice.captions.fragments = [{ eventId: 'caller_original', role: 'user', text: settings.callerText ?? ' Read my portfolio.', startMs: 1, endMs: 20, sequence: 1 }];
    await voice.options.onTranscript(voice.captions);
    return voice.options.backend.delegate({ callId: reserved.id, sessionId: `generation_${index + 1}`,
      delegationId: 'explicit_request', requestId: 'provider_delegate', offsetMs: 20, transcript: structuredClone(voice.captions) }, voice.channel);
  };
  return { broker, store, primary, callId: reserved.id, ws, streamSid, voices, posts, opening, eligible, eligibleText, delegate,
    advanceWork: () => { cursor++; },
    release: (callback?: () => void) => { beforeResult = callback; released = true; }, providerEnds: () => providerEnds, providerUpdates,
    fetches:()=>fetches,assertions:()=>assertions,finishSlow: () => finishSlowClose?.(), finishQuestion: () => finishQuestion?.(), questionId, audio };
}

test('actual server preserves accepted work through replacement and speaks only the complete eligible result', async t => {
  const f = await host(t);
  const work = f.delegate();
  await until(() => f.posts.length === 1);
  const original = structuredClone(f.posts[0]);
  f.release();
  await until(() => f.voices.length === 2 && f.voices[1]!.spoken.join('') === f.eligibleText);
  await work;
  assert.equal(f.providerEnds(), 0, 'closing the old voice cannot end the telephone call');
  assert.equal(f.voices[0]!.abort.signal.aborted, true);
  assert.deepEqual(f.posts, [original]);
  assert.equal(f.opening.length, 1);
  assert.ok(f.voices[1]!.spoken.length > 1);
  assert.ok(f.voices[1]!.spoken.every(chunk => Buffer.byteLength(chunk) <= 500));
  assert.ok(f.voices.every(voice => voice.options.history === undefined && voice.options.eligibleHistory));
  const call = f.store.get(f.callId)!;
  assert.equal(call.liveSessionId, 'generation_2');
  assert.equal((call.delegations[original.requestId] as BrokerDelegationRecord).sessionId, 'generation_1');
  assert.equal(call.liveGenerations!.segments[0]!.transcript.fragments[0]!.text, original.input.text);
  assert.equal(call.liveGenerations!.clocks.mediaStartedAt, call.mediaStartedAt);
  assert.equal(call.publications!.records.at(-1)!.sourceSessionId, 'generation_1');
});

test('the repeat note is conditional: an unheard first delivery is still given as news', () => {
  assert.match(REPEATED_RESULT_NOTE, /If you already told them, refer back to it briefly; if not, give it as news\./);
  assert.doesNotMatch(REPEATED_RESULT_NOTE, /as I mentioned/i);
});

test('a whole result re-sent after an unheard later chunk carries the repeat note, since its first part was heard', async t => {
  const resultText = Array.from({ length: 12 }, (_, index) => `Line ${index + 1}: you sold about $2.00 of Amazon stock and the sale is confirmed.`).join(' ');
  const chunks = liveResultChunks(resultText);
  assert.ok(chunks.length >= 2);
  const f = await host(t, false, false, false, { resultText, failedSpeech: { text: chunks[1]!, status: 'rejected' } });
  const work = observedWork(f.delegate());
  await boundary('accepted request', () => f.posts.length === 1, [work]);
  f.release();
  await boundary('result re-sent', () => f.voices.some(voice => voice.spoken.filter(text => text === chunks[0]).length === 2), [work]);
  const voice = f.voices.find(value => value.spoken.filter(text => text === chunks[0]).length === 2)!;
  const second = voice.published.lastIndexOf(chunks[0]!);
  assert.equal(voice.published[second - 1], REPEATED_RESULT_NOTE, 'the heard first chunk is not announced as news again');
  assert.equal(f.posts.length, 1);
});

test('a result whose only delivery was rejected (never heard) is re-sent plainly, without the repeat note', async t => {
  const resultText = 'You sold about $2.00 of Amazon stock. The sale is confirmed.';
  const f = await host(t, false, false, false, { resultText, failedSpeech: { text: resultText, status: 'rejected' } });
  const work = observedWork(f.delegate());
  await boundary('accepted request', () => f.posts.length === 1, [work]);
  f.release();
  await boundary('result re-sent', () => f.voices.some(voice => voice.spoken.filter(text => text === resultText).length === 2), [work]);
  const voice = f.voices.find(value => value.spoken.filter(text => text === resultText).length === 2)!;
  assert.ok(!voice.published.includes(REPEATED_RESULT_NOTE));
});

test('a result cut off after a heard chunk and re-sent on a replacement voice session carries the repeat note', async t => {
  const resultText = Array.from({ length: 12 }, (_, index) => `Line ${index + 1}: you sold about $2.00 of Amazon stock and the sale is confirmed.`).join(' ');
  const chunks = liveResultChunks(resultText);
  const f = await host(t, false, false, false, { resultText, failedSpeech: { text: chunks[1]!, status: 'rejected', replace: true } });
  const work = observedWork(f.delegate());
  await boundary('accepted request', () => f.posts.length === 1, [work]);
  f.release();
  await boundary('result re-sent on a replacement session', () => f.voices.length === 3 && f.voices[2]!.spoken.includes(chunks.at(-1)!), [work]);
  const [, cut, successor] = f.voices;
  assert.deepEqual(cut!.spoken.slice(0, 2), chunks.slice(0, 2), 'the first chunk was heard before the second failed and the session was replaced');
  const note = successor!.published.indexOf(REPEATED_RESULT_NOTE), first = successor!.published.indexOf(chunks[0]!);
  assert.ok(note >= 0 && note < first, 'the replacement is told the result may repeat what the caller heard');
  assert.ok(successor!.published.slice(note + 1, first).every(text => successor!.quiet.includes(text)), 'nothing is spoken between the note and the result');
  assert.equal(f.posts.length, 1);
});

test('a result never heard on its first session is re-sent plainly on the replacement voice session', async t => {
  const resultText = Array.from({ length: 12 }, (_, index) => `Line ${index + 1}: you sold about $2.00 of Amazon stock and the sale is confirmed.`).join(' ');
  const chunks = liveResultChunks(resultText);
  const f = await host(t, false, false, false, { resultText, failedSpeech: { text: chunks[0]!, status: 'rejected', replace: true } });
  const work = observedWork(f.delegate());
  await boundary('accepted request', () => f.posts.length === 1, [work]);
  f.release();
  await boundary('result re-sent on a replacement session', () => f.voices.length === 3 && f.voices[2]!.spoken.includes(chunks.at(-1)!), [work]);
  assert.deepEqual(f.voices[1]!.spoken, [chunks[0]], 'refused at its first chunk, so nothing was heard');
  assert.ok(!f.voices[2]!.published.includes(REPEATED_RESULT_NOTE));
});

test('a result whose first delivery went unconfirmed is re-sent with the conditional repeat note', async t => {
  const f = await host(t, false, false, false, { resultText: 'You sold about $2.00 of Amazon stock. The sale is confirmed.', unconfirmedSpeech: true });
  const work = observedWork(f.delegate());
  await boundary('accepted request', () => f.posts.length === 1, [work]);
  f.release();
  await boundary('result re-sent', () => f.voices.some(voice => voice.spoken.filter(text => text === f.eligibleText).length === 2), [work]);
  const voice = f.voices.find(value => value.spoken.filter(text => text === f.eligibleText).length === 2)!;
  const first = voice.published.indexOf(f.eligibleText), second = voice.published.lastIndexOf(f.eligibleText);
  assert.ok(first < second);
  assert.equal(voice.published[second - 1], REPEATED_RESULT_NOTE, 'the re-send is introduced as a possible repeat');
  assert.ok(!voice.published.slice(0, first).includes(REPEATED_RESULT_NOTE), 'the first delivery is plain');
  assert.equal(f.posts.length, 1, 'a re-send never re-submits the request');
});

test('a Dexter API failure that ends the call says so plainly and never blames the voice', async t => {
  const f = await host(t);
  const first = observedWork(f.delegate());
  await boundary('original accepted request', () => f.posts.length === 1, [first]);
  f.eligible.fetch = async () => { throw new PrimaryApiError('http_error', 401, { code: 'binding_revoked', retryable: false }); };
  f.eligible.delivery = async () => { throw new PrimaryApiError('http_error', 401, { code: 'binding_revoked', retryable: false }); };
  f.eligible.assert = async () => { throw new PrimaryApiError('http_error', 401, { code: 'binding_revoked', retryable: false }); };
  f.release();
  await boundary('the call is ended with a spoken reason', () => f.providerUpdates.some(update => typeof update.twiml === 'string'), [first]);
  const spoken = f.providerUpdates.map(update => String(update.twiml ?? '')).join(' ');
  assert.match(spoken, /I lost the connection to your Dexter account, so I have to end this call\. A result I hadn't given you yet is in the Dexter app\./,
    'built from the request that finished but was never delivered; nothing claims queued work keeps running');
  assert.doesNotMatch(spoken, /voice connection|Anything you already asked for/);
  assert.equal(f.store.get(f.callId)!.endReason, 'account_unavailable');
});

test('a temporary context failure during a completed request preserves the call, result and next caller request', async t => {
  const f = await host(t);
  const first = observedWork(f.delegate());
  await boundary('original accepted request', () => f.posts.length === 1, [first]);
  const original = structuredClone(f.posts[0]);
  const fetch = f.eligible.fetch; let attempts = 0;
  f.eligible.fetch = async scope => {
    if (++attempts === 1) throw new PrimaryApiError('http_error', 503, { code: 'temporarily_unavailable', retryable: true });
    return fetch(scope);
  };
  f.release();
  await boundary('recovered result', () => f.voices[1]?.spoken.join('') === f.eligibleText && first.settled(), [first]);
  assert.equal(attempts, 2);
  assert.equal(f.providerEnds(), 0);
  assert.equal(f.ws.readyState, WebSocket.OPEN);
  assert.deepEqual(f.posts, [original], 'a read retry never submits the work a second time');
  assert.equal(f.store.get(f.callId)!.contextReadFailures?.some(value => value.stage === 'fetch'
    && value.status === 503 && value.remoteCode === 'temporarily_unavailable'), true);
  const next = observedWork(f.delegate(1));
  await boundary('next caller request completed', () => f.posts.length === 2 && next.settled(), [next]);
  assert.notEqual(f.posts[1].requestId, original.requestId);
  assert.equal(f.providerEnds(), 0);
  assert.equal(f.opening.length, 1);
});

test('call transcript logs once after generation cleanup, including its final fragments', async t => {
  const logs: string[] = [];
  t.mock.method(console, 'info', (message: string) => { if (message.startsWith('CALL ')) logs.push(message); });
  const f = await host(t, true);
  f.ws.close();
  await until(() => f.providerEnds() === 1);
  assert.deepEqual(logs, [], 'the final transcript is not saved while cleanup is stalled');
  const voice = f.voices[0]!;
  voice.captions.revision = 1;
  voice.captions.fragments = [{ eventId: 'last-words', role: 'assistant', text: ' Goodbye.', startMs: 1, endMs: 20, sequence: 1 }];
  voice.options.onTranscript(voice.captions);
  f.finishSlow();
  await until(() => logs.length === 1);
  const { formatCallTranscript } = await import('../src/broker/callTranscript.js');
  assert.equal(logs[0], formatCallTranscript(f.store.get(f.callId)!));
  assert.match(logs[0], /\nDEXTER: Goodbye\.$/);
  await f.broker.maintain();
  assert.equal(logs.length, 1);
});

test('actual host hangup starts before a deliberately stalled generation cleanup', async t => {
  const f = await host(t, true);
  f.ws.close();
  await until(() => f.providerEnds() === 1);
  assert.equal(f.voices[0]!.closed.length, 1);
  assert.ok(f.store.get(f.callId)!.endedAt);
  f.finishSlow();
});


test('question presentation waits for every eligible commentary chunk in the replacement session', async t => {
  const f = await host(t, false, true);
  const work = f.delegate();
  await until(() => f.posts.length === 1);
  f.release();
  await until(() => f.voices.length === 2 && f.voices[1]!.spoken.join('') === f.eligibleText);
  const entry = () => f.store.get(f.callId)!.delegations[f.posts[0]!.requestId] as BrokerDelegationRecord;
  assert.equal(entry().questions![f.questionId]!.presentation, undefined, 'last unacknowledged chunk cannot establish question presentation');
  assert.ok(f.voices[1]!.spoken.length > 1);
  assert.ok(f.voices[1]!.spoken.every(chunk => Buffer.byteLength(chunk) <= 500));
  f.finishQuestion();
  await until(() => entry().questions![f.questionId]!.presentation?.sessionId === 'generation_2');
  assert.equal(f.posts.length, 1);
  assert.equal(f.providerEnds(), 0);
  f.ws.close();
  await work;
});

test('generation host resumes the saved sample clock and preserves the original deadline on restart', async t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-generation-restart-'));
  const store = new BrokerStore(directory);
  const call = store.reserve({ requestId: randomUUID(), number: proof.params.To, direction: 'outbound', purpose: 'Continue', callSid: sid }).call;
  store.recordPrimaryBinding(call.id, context());
  const startedAt = new Date(Date.now() - 1000).toISOString();
  const deadline = new Date(Date.now() + 30_000).toISOString();
  const journal: LiveGenerationJournal = { version: 1, callId: call.id,
    binding: { bindingId, callSid: sid, primaryDexterId, threadId, ownerPrincipalId },
    clocks: { callDeadlineAt: deadline, mediaStartedAt: startedAt }, phase: 'active', inputNextSample: 160,
    deliveryAttachment: { generationId: messageId, sessionId: 'previous_process' },
    segments: [{ generationId: messageId, ordinal: 1, createdAt: startedAt, state: 'active', sessionId: 'previous_process',
      readyAt: startedAt, version: version(1), pageDigest: 'a'.repeat(64), outputDiscarded: false,
      inputFirstSample: 0, inputLastSampleExclusive: 160, transcript: { revision: 0, callerRevision: 0, fragments: [] } }],
  };
  store.update(call.id, { mediaStartedAt: startedAt, liveSessionId: 'previous_process', liveGenerations: journal });
  const problems: string[] = [], received: string[] = [];
  let ready = false;
  const voice = createGenerationVoice({ call: store.get(call.id)!, callDeadlineAt: new Date(Date.now() + 300_000).toISOString(),
    apiKey: 'fixture', voice: 'marin', store, primary: {} as PrimaryApiClient,
    context: { async fetch() { return context(); }, async assert(_scope, expected) { return { bindingId, callSid: sid, version: expected, current: true }; },
      async delivery() { throw new Error('No task was requested'); } },
    onAudio() {}, onReady() { ready = true; }, onClosed() {}, onPlaybackClear() {}, onProblem(code) { problems.push(code); },
    voiceFactory(options) {
      queueMicrotask(() => options.onReady({ callId: call.id, sessionId: 'replacement_process' }));
      return { appendAudio(payload) { received.push(payload); return true; },
        async appendContext() { throw new Error('No context append'); }, async appendInstructions() { throw new Error('Opening cannot repeat'); },
        async interruptPlayback() { return { eventId: '', status: 'accepted' }; }, resumePlayback() {},
        getTranscript() { return { revision: 0, callerRevision: 0, fragments: [] }; },
        async close(reason = 'closed') { const result = { callId: call.id, sessionId: 'replacement_process', reason,
          finalized: true, transcriptPersisted: true, outputAudioDiscarded: false }; options.onClosed(result); return result; } };
    },
  });
  t.after(async () => { await voice.close(); store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  await until(() => ready);
  assert.deepEqual(problems, ['restart_input_unconfirmed']);
  assert.equal(voice.appendAudio(Buffer.alloc(160, 255).toString('base64')), true);
  const saved = store.get(call.id)!.liveGenerations!;
  assert.equal(saved.inputNextSample, 320);
  assert.equal(saved.segments[1]!.inputFirstSample, 160);
  assert.equal(saved.clocks.callDeadlineAt, deadline);
  assert.equal(saved.clocks.mediaStartedAt, startedAt);
  assert.equal(saved.inputGap, 'restart_during_generation', 'unknown previous transport gap remains explicit');
  assert.equal(received.length, 1, 'only newly observed input is sent');
});


function observedWork(work: Promise<void>) {
  let settled = false, failed = false;
  let failure: unknown;
  void work.then(() => { settled = true; }, error => { settled = true; failed = true; failure = error; });
  return { settled: () => settled, check() { if (failed) throw failure; } };
}

async function boundary(label: string, predicate: () => boolean, work: ReturnType<typeof observedWork>[], limit = 6000) {
  const end = Date.now() + limit;
  for (;;) {
    for (const operation of work) operation.check();
    if (predicate()) return;
    assert.ok(Date.now() < end, `timed out waiting for ${label}`);
    await wait(10);
  }
}

async function preparedFollowup(f: Awaited<ReturnType<typeof host>>, work: ReturnType<typeof observedWork>[],
  text = ' Then compare Apple.') {
  const first = f.voices[0]!;
  for (let index = 0; index < 69; index++) f.audio(true);
  f.audio(); await boundary('follow-up audio', () => first.inputs.length === 71, work);
  first.captions.revision = 2; first.captions.callerRevision = 2;
  first.captions.fragments.push({ eventId: 'explicit_followup', role: 'user', text, startMs: 1400, endMs: 1420, sequence: 2 });
  first.options.onTranscript(first.captions);
  const snapshot = structuredClone(first.captions);
  const listeners = new Set<(snapshot: LiveTranscriptSnapshot) => void>();
  let entered = false, released = false;
  // Only this delegation sees delayed captions. The original worker keeps its
  // normal channel, so its stable result observation can trigger replacement.
  const channel: LiveBackendChannel = { ...first.channel,
    getContext: () => released ? structuredClone(snapshot) : { revision: 0, callerRevision: 0, fragments: [] },
    subscribeContext(listener) {
      entered = true; listeners.add(listener);
      return () => { listeners.delete(listener); };
    },
  };
  return {
    start: () => first.options.backend.delegate({ callId: f.callId, sessionId: 'generation_1', delegationId: 'followup',
      requestId: 'provider_followup', offsetMs: 1420, transcript: structuredClone(snapshot) }, channel),
    entered: () => entered,
    pending: () => entered && !released && listeners.size > 0,
    release() {
      if (released) return;
      released = true;
      for (const listener of listeners) listener(structuredClone(snapshot));
    },
  };
}

test('actual host retains the old follow-up admission while a delayed fresh-context fetch buffers only unsent audio', async t => {
  const f = await host(t, false, false, true);
  const work = [observedWork(f.delegate())];
  let followup: Awaited<ReturnType<typeof preparedFollowup>> | undefined;
  let releaseFetch = () => {};
  try {
    await boundary('original submission', () => f.posts.length === 1, work);
    const original = structuredClone(f.posts[0]!);
    followup = await preparedFollowup(f, work);
    const next = observedWork(followup.start()); work.push(next);
    await boundary('pending follow-up caption observation', followup.entered, work);
    assert.equal(followup.pending(), true); assert.equal(next.settled(), false); assert.equal(f.posts.length, 1);
    const fetch = f.eligible.fetch;
    const fetching = new Promise<void>(resolve => { releaseFetch = resolve; });
    let fetchEntered = false;
    f.eligible.fetch = async scope => { fetchEntered = true; await fetching; return fetch(scope); };
    f.release();
    await boundary('replacement fetch', () => fetchEntered && f.store.get(f.callId)!.liveGenerations!.phase === 'replacing', work);
    assert.equal(followup.pending(), true, 'caption observation still holds source admission');
    assert.equal(next.settled(), false); assert.equal(f.posts.length, 1);
    assert.equal(f.voices[0]!.closed.length, 0, 'source admission still owns the old transcript');
    const before = f.store.get(f.callId)!.liveGenerations!.inputNextSample;
    const marker = Buffer.from('buffered_audio_ingress');
    let received = false, pingError: Error | undefined;
    const onPong = (payload: Buffer) => { if (payload.equals(marker)) received = true; };
    f.ws.on('pong', onPong);
    try {
      f.audio(true);
      // The matching pong follows the synchronous server media handler. Queued
      // input stays in memory until a later generation journal save.
      f.ws.ping(marker, undefined, error => { if (error) pingError = error; });
      await boundary('buffered audio ingress', () => { if (pingError) throw pingError; return received; }, work);
    } finally { f.ws.off('pong', onPong); }
    assert.equal(f.voices[0]!.inputs.length, 71, 'new frame remains buffered after the barrier');
    followup.release();
    await boundary('follow-up submission', () => f.posts.length === 2, work);
    assert.equal(f.voices[0]!.closed.length, 0, 'keep the gated old transport until fresh context is available');
    releaseFetch();
    await boundary('replacement attachment', () => f.voices.length === 2 && f.voices[1]!.inputs.length === 1, work);
    await boundary('old source retirement', () => f.voices[0]!.closed.length === 1, work);
    await boundary('both saved results', () => work.every(operation => operation.settled()), work);
    assert.deepEqual(f.posts[0], original); assert.equal(f.posts[1]!.input.text, ' Then compare Apple.');
    assert.notEqual(f.posts[1]!.input.source.sourceEventId, original.input.source.sourceEventId);
    const saved = f.store.get(f.callId)!.delegations[f.posts[1]!.requestId] as BrokerDelegationRecord;
    assert.equal(saved.sessionId, 'generation_1'); assert.deepEqual(saved.submission, f.posts[1]!.input);
    assert.equal(saved.status, 'completed'); assert.ok(f.voices[1]!.spoken.join('').includes(f.eligibleText));
    assert.equal(f.voices[1]!.inputs[0], Buffer.alloc(160, 255).toString('base64'));
    assert.equal(f.providerEnds(), 0); assert.equal(f.opening.length, 1);
    assert.equal(f.store.get(f.callId)!.liveGenerations!.inputNextSample, before + 160);
  } finally { followup?.release(); releaseFetch(); }
});

test('actual call stop cancels an admission drain while follow-up captions and replacement fetch are held', async t => {
  const f = await host(t, false, false, true);
  const work = [observedWork(f.delegate())];
  let followup: Awaited<ReturnType<typeof preparedFollowup>> | undefined;
  let releaseFetch = () => {};
  try {
    await boundary('original submission', () => f.posts.length === 1, work);
    followup = await preparedFollowup(f, work);
    const next = observedWork(followup.start()); work.push(next);
    await boundary('pending follow-up caption observation', followup.entered, work);
    assert.equal(followup.pending(), true); assert.equal(next.settled(), false); assert.equal(f.posts.length, 1);
    const fetch = f.eligible.fetch;
    const fetching = new Promise<void>(resolve => { releaseFetch = resolve; });
    let fetchEntered = false;
    f.eligible.fetch = async scope => { fetchEntered = true; await fetching; return fetch(scope); };
    f.release();
    await boundary('replacement fetch', () => fetchEntered && f.store.get(f.callId)!.liveGenerations!.phase === 'replacing', work);
    assert.equal(followup.pending(), true, 'source admission remains unresolved before stop');
    assert.equal(next.settled(), false); assert.equal(f.posts.length, 1); assert.equal(f.voices[0]!.closed.length, 0);
    f.ws.close();
    // Stop may reject the interrupted work. Observe its settlement without
    // treating an expected cancellation as a failed pre-stop boundary.
    await boundary('provider hangup and source cancellation', () => f.providerEnds() === 1 && next.settled(), []);
    assert.ok(f.store.get(f.callId)!.endedAt);
    assert.equal(f.voices[0]!.closed.length, 1); assert.equal(followup.pending(), false);
    followup.release(); releaseFetch();
    await boundary('stopped result observation', () => work.every(operation => operation.settled()), []);
    assert.equal(f.posts.length, 1, 'ending the actual call prevents a new source submission');
    assert.equal(f.voices.length, 1, 'stop does not wait to start another provider generation');
  } finally { followup?.release(); releaseFetch(); }
});

test('late question-chunk rejection crosses actual host callbacks and forces a different publication attempt', async t => {
  const f = await host(t, false, true); const work = f.delegate(); await until(() => f.posts.length === 1);
  f.release(); await until(() => f.voices.length === 2 && f.voices[1]!.spoken.join('') === f.eligibleText);
  f.finishQuestion();
  const entry = () => f.store.get(f.callId)!.delegations[f.posts[0]!.requestId] as BrokerDelegationRecord;
  await until(() => !!entry().questions![f.questionId]!.presentation);
  const previous = structuredClone(entry().questions![f.questionId]!.presentation!);
  assert.ok(previous.publicationReceipts!.length > 1);
  f.voices[1]!.options.onEvent?.({ type: 'live.append.late_rejection', eventId: previous.publicationReceipts![0]!.eventId, code: 'provider_error' });
  await until(() => entry().questions![f.questionId]!.presentation!.publicationReceipts![0]!.eventId !== previous.publicationReceipts![0]!.eventId);
  const call = f.store.get(f.callId)!;
  assert.equal(call.publications!.rejections[0]!.eventId, previous.publicationReceipts![0]!.eventId);
  const attempts = call.publications!.records.filter(record => record.kind === 'result').map(record => record.updateId);
  assert.ok(attempts.some(id => id.includes('_attempt_1_'))); assert.ok(attempts.some(id => id.includes('_attempt_2_')));
  assert.equal(f.posts.length, 1); f.ws.close(); await work;
});


test('actual server waits for carrier playback acknowledgement before rotating and presenting the result', async t => {
  const f = await host(t); const work = f.delegate();
  await until(() => f.posts.length === 1);
  const messages: Array<{ event: string; mark?: { name: string } }> = [];
  f.ws.on('message', value => { messages.push(JSON.parse(String(value))); });
  f.voices[0].options.onAudio(Buffer.alloc(160, 0).toString('base64'), { deliveryId: 'synthetic_active_playback' });
  await until(() => messages.some(message => message.event === 'mark'));
  f.release(); await wait(1000);
  assert.equal(f.voices.length, 1); assert.deepEqual(f.voices[0].spoken, []);
  const mark = messages.find(message => message.event === 'mark')!.mark!;
  f.ws.send(JSON.stringify({ event: 'mark', streamSid: f.streamSid, mark }));
  await until(() => f.voices[1]?.spoken.join('') === f.eligibleText); await work;
  assert.equal(f.posts.length, 1); assert.equal(f.providerEnds(), 0);
});

test('actual cleared carrier playback releases a queued result without waiting for a cleared mark', async t => {
  const f = await host(t); const work = f.delegate();
  await until(() => f.posts.length === 1);
  f.voices[0].options.onAudio(Buffer.alloc(160, 0).toString('base64'), { deliveryId: 'cleared_playback' });
  f.release(); await wait(1000); assert.equal(f.voices.length, 1);
  f.voices[0].options.onPlaybackClear?.('application_interrupt');
  await until(() => f.voices[1]?.spoken.join('') === f.eligibleText); await work;
  assert.equal(f.posts.length, 1);
});

test('carrier disconnect cancels waiting result presentation and retains the result without another voice', async t => {
  const f = await host(t); const work = f.delegate();
  await until(() => f.posts.length === 1);
  f.voices[0].options.onAudio(Buffer.alloc(160, 0).toString('base64'), { deliveryId: 'disconnect_playback' });
  f.release(); await wait(1000); f.ws.close();
  await until(() => Boolean(f.store.get(f.callId)!.endedAt)); await work;
  assert.equal(f.voices.length, 1); assert.deepEqual(f.voices[0].spoken, []);
  assert.equal((f.store.get(f.callId)!.delegations[f.posts[0].requestId] as BrokerDelegationRecord).status, 'completed');
});

test('a caller turn during context replacement defers then presents the saved result at a fresh quiet boundary', async t => {
  const f = await host(t); const work = f.delegate();
  await until(() => f.posts.length === 1);
  const fetch = f.eligible.fetch;
  f.eligible.fetch = async scope => {
    const source = f.voices[0];
    source.captions.revision++; source.captions.callerRevision++;
    source.captions.fragments.push({ eventId: 'new_topic', role: 'user', text: 'What are your suggestions?',
      startMs: 21, endMs: 40, sequence: source.captions.revision });
    source.options.onTranscript(source.captions);
    return fetch(scope);
  };
  f.release(); await work;
  assert.equal(f.voices.length, 2, 'a genuine eligibility change still requires strict replacement');
  assert.equal(f.voices.flatMap(value => value.spoken).join(''), 'For your earlier request: ' + f.eligibleText);
  const saved = f.store.get(f.callId)!;
  assert.equal((saved.delegations[f.posts[0].requestId] as BrokerDelegationRecord).delivery, 'accepted');
  assert.equal(f.posts.length, 1, 'presentation recovery cannot resubmit the original work');
  assert.equal((saved.delegations[f.posts[0].requestId] as BrokerDelegationRecord).primaryResult?.state, 'completed');
  assert.equal(saved.liveGenerations!.segments[0].transcript.fragments.at(-1)!.text, 'What are your suggestions?');
});

test('actual generation retains the failed financial request and speaks bounded uncertainty without a new submit', async t => {
  const f = await host(t, false, false, false, { failed: true }); const work = f.delegate();
  await until(() => f.posts.length === 1); f.release(); await work;
  assert.deepEqual(f.voices.flatMap(value => value.spoken), [
    "I couldn't confirm the trade request. We need to check that same request before continuing.",
  ]);
  const saved = f.store.get(f.callId)!.delegations[f.posts[0].requestId] as BrokerDelegationRecord;
  assert.equal(saved.status, 'failed'); assert.equal(saved.primaryResult?.state, 'failed');
  assert.equal(saved.primaryResult?.error?.code, 'primary_financial_result_unknown');
  assert.equal(f.posts.length, 1); assert.equal(saved.primaryResult?.assistant, undefined);
});

test('pending work keeps one voice, and the task list rather than a waiting line reaches it', async t => {
  const f = await host(t, false, false, false, { sameEpoch: true }); const work = f.delegate();
  await until(() => f.posts.length === 1);
  const original = structuredClone(f.store.get(f.callId)!.liveGenerations!.segments[0].version);
  f.advanceWork();
  await until(() => f.voices[0].quiet.some(text => /^Task list \d+, task 1: In progress, no result yet\./.test(text)));
  await wait(400);
  assert.equal(f.voices.length, 1); assert.equal(f.voices[0].closed.length, 0);
  assert.deepEqual(f.voices[0].spoken, []);
  assert.ok(!f.voices[0].published.some(text => /Still working on it/.test(text)), 'no waiting lines');
  assert.deepEqual(f.store.get(f.callId)!.liveGenerations!.segments[0].version, original);
  assert.equal(f.posts.length, 1);
  const source = f.voices[0];
  source.captions.revision++; source.captions.callerRevision++;
  source.captions.fragments.push({ eventId: 'suggestion_topic', role: 'user', text: 'What are your suggestions?',
    startMs: 21, endMs: 40, sequence: source.captions.revision });
  source.options.onTranscript(source.captions); f.advanceWork();
  await wait(1500);
  assert.equal(f.voices.length, 1, 'later conversation cannot be reset by pending work');
  assert.deepEqual(source.spoken, []);
  f.ws.close(); await work;
});


const incidentBuy = " Alright, let's buy five dollars worth of Apple";
const incidentResult = 'Bought $5 of Apple stock, confirmed on-chain.';
for (const followup of [' Can you give me something, please', ' What are your suggestions?', ' Cancel that order.']) {
  test(`completed original request reaches voice before queued later speech: ${followup.trim()}`, async t => {
    const f = await host(t, false, false, false, { callerText: incidentBuy, resultText: incidentResult });
    const originalWork = observedWork(f.delegate());
    await boundary('original buy submission', () => f.posts.length === 1, [originalWork]);
    const original = structuredClone(f.posts[0]);
    const first = f.voices[0];
    for (let i = 0; i < 69; i++) f.audio(true);
    f.audio(); await boundary('later caller audio', () => first.inputs.length === 71, [originalWork]);
    first.captions.revision++; first.captions.callerRevision++;
    first.captions.fragments.push({ eventId: 'incident_followup', role: 'user', text: followup,
      startMs: 1400, endMs: 1420, sequence: first.captions.revision });
    first.options.onTranscript(first.captions);
    const request = { callId: f.callId, sessionId: 'generation_1', delegationId: 'incident_followup',
      requestId: 'provider_followup', offsetMs: 1420, transcript: structuredClone(first.captions) };
    const laterWork = observedWork(first.options.backend.delegate(request, first.channel));
    await boundary('saved queued follow-up', () => (f.store.get(f.callId)!.delegations[original.requestId] as BrokerDelegationRecord)
      .deferredTurns?.some(turn => turn.status === 'queued') === true, [originalWork, laterWork]);
    const submit = f.primary.submit;
    f.primary.submit = async (...args) => {
      assert.equal((f.store.get(f.callId)!.delegations[original.requestId] as BrokerDelegationRecord).delivery, 'accepted');
      assert.equal(f.voices.flatMap(v => v.spoken).join(''), 'For your earlier request: ' + incidentResult);
      return submit(...args);
    };
    // Follow-up handling is distinct work. It cannot alter the completed result
    // or replay the original purchase, even when the caller asked to cancel it.
    const result = f.primary.result;
    f.primary.result = async (binding, requestId) => requestId === original.requestId ? result(binding, requestId)
      : { bindingId, requestId, state: 'cancelled', error: { code: 'fixture_followup_closed', retryable: false } };
    f.release();
    await boundary('both workers settled', () => originalWork.settled() && laterWork.settled(), [originalWork, laterWork]);
    assert.equal(f.posts.length, 2); assert.deepEqual(f.posts[0], original);
    assert.equal(f.posts[1].input.text, followup); assert.notEqual(f.posts[1].requestId, original.requestId);
    assert.equal(f.voices.flatMap(v => v.spoken).filter(text => text === 'For your earlier request: ' + incidentResult).length, 1);
    assert.equal((f.store.get(f.callId)!.delegations[original.requestId] as BrokerDelegationRecord).delivery, 'accepted');
    assert.ok(f.store.get(f.callId)!.publications!.records.some(record => record.ordinaryDelivery
      && record.requestId === original.requestId && record.outcome?.status === 'accepted'));
    assert.equal(f.providerEnds(), 0);
  });
}

test('carrier playback beyond the first wait defers a completed answer without losing it', async t => {
  const f = await host(t, false, false, false, { resultText: incidentResult });
  const work = observedWork(f.delegate());
  await boundary('original submission', () => f.posts.length === 1, [work]);
  f.voices[0].options.onAudio(Buffer.alloc(160, 0).toString('base64'), { deliveryId: 'long_existing_playback' });
  f.release();
  await boundary('first completed presentation deferred', () => (f.store.get(f.callId)!.delegations[f.posts[0].requestId] as BrokerDelegationRecord).delivery === 'stale', [work], 8000);
  assert.deepEqual(f.voices.flatMap(v => v.spoken), []);
  f.voices[0].options.onPlaybackClear?.('application_interrupt');
  await boundary('same result presented after playback clears', () => work.settled(), [work]);
  assert.deepEqual(f.voices.flatMap(v => v.spoken), [incidentResult]);
  assert.equal(f.posts.length, 1);
});

test('call 039b39cd: caller speech during the result read prompts delivery instead of deferring it as stale', async t => {
  const f = await host(t, false, false, false, { admitted: true, resultText: incidentResult });
  const work = observedWork(f.delegate());
  await boundary('original submission', () => f.posts.length === 1, [work]);
  const voice = f.voices[0];
  const delivery = f.eligible.delivery;
  const prompts = [' Hello?', ' What are you on?'];
  let reads = 0;
  // The caller is audible during every delivery read, as on the production call. The
  // former frontier/quiet recheck marked each read stale and the answer was never spoken.
  f.eligible.delivery = async (...args) => {
    reads++;
    const before = voice.inputs.length; f.audio();
    await until(() => voice.inputs.length > before);
    voice.captions.revision++; voice.captions.callerRevision++;
    voice.captions.fragments.push({ eventId: `caller_prompt_${reads}`, role: 'user', text: prompts[(reads - 1) % prompts.length],
      startMs: 1400 + reads * 100, endMs: 1450 + reads * 100, sequence: voice.captions.revision });
    voice.options.onTranscript(voice.captions);
    return delivery(...args);
  };
  f.release();
  await boundary('completed result accepted after caller prompt', () =>
    (f.store.get(f.callId)!.delegations[f.posts[0].requestId] as BrokerDelegationRecord).delivery === 'accepted', [work], 10000);
  assert.equal(reads, 1, 'the first read at a pause is delivered; caller speech during it does not force another wait');
  assert.deepEqual(voice.spoken, ['For your earlier request: ' + incidentResult]);
  assert.deepEqual(voice.native.filter(value => value.kind === 'result').map(value => value.id),
    (f.store.get(f.callId)!.delegations[f.posts[0].requestId] as BrokerDelegationRecord).delegationIds,
    'the result is published only on its own request delegation');
  assert.equal(f.posts.length, 1);
  assert.equal(voice.captions.fragments.at(-1)!.text, ' Hello?');
  await wait(1500);
  assert.deepEqual(voice.spoken, ['For your earlier request: ' + incidentResult], 'an accepted result is not repeated');
  f.ws.close(); await boundary('worker closed', work.settled, [work]);
});

test('canonical cancellation is announced without attempting a completed-result delivery', async t => {
  const f = await host(t, false, false, false, { cancelled: true });
  let resultDeliveries = 0;
  const delivery = f.eligible.delivery;
  f.eligible.delivery = async (...args) => { resultDeliveries++; return delivery(...args); };
  const work = f.delegate(); await until(() => f.posts.length === 1); f.release(); await work;
  assert.deepEqual(f.voices.flatMap(v => v.spoken), ['The request was cancelled.']);
  assert.equal(resultDeliveries, 0); assert.equal(f.posts.length, 1);
  assert.equal((f.store.get(f.callId)!.delegations[f.posts[0].requestId] as BrokerDelegationRecord).status, 'cancelled');
});

test('a pending question still yields to queued new work and hangup creates no reply', async t => {
  const f = await host(t, false, true);
  const work = observedWork(f.delegate());
  await boundary('original request', () => f.posts.length === 1, [work]);
  const later = await preparedFollowup(f, [work]);
  const followup = observedWork(later.start());
  await boundary('follow-up capture starts', later.entered, [work, followup]); later.release();
  await boundary('new work queued', () => Object.values(f.store.get(f.callId)!.delegations)
    .some(value => (value as BrokerDelegationRecord).deferredTurns?.some(turn => turn.status === 'queued')), [work, followup]);
  f.release();
  await boundary('question saved', () => (f.store.get(f.callId)!.delegations[f.posts[0].requestId] as BrokerDelegationRecord).status === 'waiting_for_reply', [work, followup]);
  await wait(1000);
  assert.deepEqual(f.voices.flatMap(v => v.spoken), []);
  const question = (f.store.get(f.callId)!.delegations[f.posts[0].requestId] as BrokerDelegationRecord).questions![f.questionId];
  assert.equal(question.presentation, undefined); assert.equal(question.reply, undefined);
  f.ws.close(); await boundary('call workers closed', () => work.settled() && followup.settled(), [work, followup]);
  assert.equal(f.posts.length, 1);
});


test('one contextualized result waits for actual playback and survives caller interruption without a separate prefix', async t => {
  const f = await host(t, false, false, false, { admitted: true, resultText: incidentResult });
  const work = observedWork(f.delegate());
  await boundary('original request', () => f.posts.length === 1, [work]);
  const voice = f.voices[0];
  voice.options.onAudio(Buffer.alloc(160, 0).toString('base64'), { deliveryId: 'prior_speech' });
  voice.captions.revision++; voice.captions.callerRevision++;
  voice.captions.fragments.push({ eventId: 'later_topic', role: 'user', text: 'What else can you do?',
    startMs: 1400, endMs: 1420, sequence: voice.captions.revision });
  voice.options.onTranscript(voice.captions);
  f.release(); await wait(1000);
  assert.deepEqual(voice.spoken, [], 'actual speech remains blocked before playback clears');
  voice.captions.revision++; voice.captions.callerRevision++;
  voice.captions.fragments.push({ eventId: 'interruption', role: 'user', text: 'Yes?',
    startMs: 1420, endMs: 1440, sequence: voice.captions.revision });
  voice.options.onTranscript(voice.captions);
  voice.options.onPlaybackClear?.('application_interrupt');
  await boundary('completed result after interruption', work.settled, [work], 10000);
  assert.deepEqual(voice.spoken, ['For your earlier request: ' + incidentResult]);
  assert.equal(voice.native.filter(value => value.kind === 'result').length, 1);
  assert.equal(f.posts.length, 1);
  assert.equal(f.voices.length, 1);
  const saved = f.store.get(f.callId)!;
  assert.equal((saved.delegations[f.posts[0].requestId] as BrokerDelegationRecord).delivery, 'accepted');
  assert.equal(saved.publications!.records.filter(record => record.kind === 'progress' && record.updateId.includes('_earlier_')).length, 0);
});

test('a forgotten completed source is not spoken or retried from the raw saved answer', async t => {
  const f = await host(t, false, false, false, { resultText: incidentResult });
  let reads = 0;
  f.eligible.delivery = async (_binding, requestId, input) => {
    reads++;
    return { bindingId, callSid: sid, requestId, version: input.version,
      delivery: { state: 'unavailable', reason: 'source_not_in_projection' } };
  };
  const work = f.delegate(); await until(() => f.posts.length === 1); f.release(); await work;
  assert.equal(reads, 1); assert.equal(f.posts.length, 1);
  assert.deepEqual(f.voices.flatMap(value => value.spoken), []);
  assert.equal((f.store.get(f.callId)!.delegations[f.posts[0].requestId] as BrokerDelegationRecord).delivery, 'unconfirmed');
});


for (const outcome of ['cancelled', 'failed', 'financial_unknown'] as const) {
  test(`saved ${outcome} outcome remains reportable after a queued progress question`, async t => {
    const f = await host(t, false, false, false, {
      callerText: incidentBuy, cancelled: outcome === 'cancelled', failed: outcome !== 'cancelled',
      failureCode: outcome === 'financial_unknown' ? 'primary_financial_result_unknown' : 'fixture_request_failed',
    });
    const work = observedWork(f.delegate());
    await boundary('original request submitted', () => f.posts.length === 1, [work]);
    const later = await preparedFollowup(f, [work]);
    const followup = observedWork(later.start());
    await boundary('later capture started', later.entered, [work, followup]); later.release();
    await boundary('later request queued', () => (f.store.get(f.callId)!.delegations[f.posts[0].requestId] as BrokerDelegationRecord)
      .deferredTurns?.some(turn => turn.status === 'queued') === true, [work, followup]);
    const queueSpeech = /queued|later request|not started/i;
    await wait(500);
    assert.ok(!f.voices.flatMap(voice => voice.spoken).some(text => queueSpeech.test(text)), 'queue mechanics stay out of speech');
    const expected = 'For your earlier request: ' + (outcome === 'cancelled' ? 'The request was cancelled.'
      : outcome === 'financial_unknown' ? "I couldn't confirm the trade request. We need to check that same request before continuing."
      : 'I could not get an answer to that request.');
    const submit = f.primary.submit;
    let beforeSecondSubmit: { speech: string[]; delivery: string | undefined } | undefined;
    f.primary.submit = async (...args) => {
      // Capture the boundary; assertions inside this transport would fabricate
      // a dispatch-unknown response if legitimate progress had also been spoken.
      beforeSecondSubmit = { speech: f.voices.flatMap(voice => voice.spoken),
        delivery: (f.store.get(f.callId)!.delegations[f.posts[0].requestId] as BrokerDelegationRecord).delivery };
      return submit(...args);
    };
    f.release();
    await boundary('terminal answer and later request completed', () => work.settled() && followup.settled(), [work, followup]);
    assert.equal(f.posts.length, 2);
    assert.ok(beforeSecondSubmit, 'second submission must be observed');
    assert.equal(beforeSecondSubmit.delivery, 'accepted');
    assert.equal(beforeSecondSubmit.speech.filter(text => text === expected).length, 1);
    assert.ok(!beforeSecondSubmit.speech.some(text => queueSpeech.test(text)), 'queue mechanics stay internal');
    assert.equal(beforeSecondSubmit.speech.at(-1), expected);
    assert.equal((f.store.get(f.callId)!.delegations[f.posts[0].requestId] as BrokerDelegationRecord).delivery, 'accepted');
    assert.equal(f.voices.flatMap(voice => voice.spoken).filter(text => text === expected).length, 1);
    assert.ok(!f.voices.flatMap(voice => voice.spoken).some(text => text.includes('Bought')));
  });
}


test('recorded pending sale and queued buy keep truthful task state through the actual voice path', async t => {
  const f = await host(t, false, false, false, { sameEpoch: true,
    callerText: 'Um, I probably want to sell four dollars of the Apple stock that I just bought recently' });
  const work = observedWork(f.delegate());
  await boundary('sale submitted', () => f.posts.length === 1, [work]);
  const later = await preparedFollowup(f, [work], 'How about you buy a dollar worth');
  const next = observedWork(later.start());
  await boundary('buy admission started', later.entered, [work, next]); later.release();
  await boundary('buy queued', () => (f.store.get(f.callId)!.delegations[f.posts[0].requestId] as BrokerDelegationRecord)
    .deferredTurns?.some(turn => turn.status === 'queued') === true, [work, next]);
  await boundary('task list shows the buy waiting behind the sale', () => f.voices.flatMap(v => v.quiet)
    .some(text => /task 2: Not started yet; it starts after the earlier request finishes/.test(text)), [work, next]);
  assert.equal(f.posts.length, 1, 'queueing does not submit the buy');
  f.primary.result = async () => { throw new PrimaryApiError('http_error', 503,
    { code: 'temporarily_unavailable', retryable: true }); };
  await boundary('read error reaches the task list', () => f.voices.flatMap(v => v.quiet)
    .some(text => /task 1: In progress.*status check failed/.test(text)), [work, next]);
  const call = f.store.get(f.callId)!;
  assert.deepEqual(f.voices.flatMap(v => v.spoken), [], 'pending status is quiet task state, never an unsolicited spoken claim');
  assert.equal((call.delegations[f.posts[0].requestId] as BrokerDelegationRecord).resultReadUnavailable, true);
  assert.equal(call.liveGenerations!.segments.length, 1);
  assert.equal((call.delegations[f.posts[0].requestId] as BrokerDelegationRecord).primaryResult?.state, 'running');
  f.ws.close(); await boundary('hangup closes both observers', () => work.settled() && next.settled(), [work, next]);
  assert.equal(f.posts.length, 1);
});

test('continuous digital silence does not starve a saved result, while audible frames and late marks still block it', async t => {
  const f = await host(t, false, false, false, { resultText: incidentResult });
  const work = observedWork(f.delegate());
  await boundary('request submitted', () => f.posts.length === 1, [work]);
  const source = f.voices[0];
  const marks: string[] = [];
  f.ws.on('message', data => { const event = JSON.parse(data.toString()); if (event.event === 'mark') marks.push(event.mark.name); });
  source.options.onAudio(Buffer.alloc(160, 0xfe).toString('base64'), { deliveryId: 'audible' });
  await boundary('audible mark received', () => marks.length === 1, [work]);
  const audibleMark = marks[0];
  const silence = setInterval(() => {
    source.options.onAudio(Buffer.alloc(160, 255).toString('base64'), { deliveryId: 'digital_silence' });
    // Keep only digital silence acknowledged late; actual speech remains outstanding.
    for (const name of marks.splice(0).filter(name => name !== audibleMark))
      f.ws.send(JSON.stringify({ event: 'mark', streamSid: f.streamSid, mark: { name } }));
  }, 20);
  t.after(() => clearInterval(silence));
  f.release(); await wait(1100);
  assert.deepEqual(f.voices.flatMap(v => v.spoken), [], 'audible mark is mandatory even with a transcript result');
  // Admit caller audio before releasing the playback fence. A timer scheduled
  // afterward leaves a real quiet boundary where delivery is permitted.
  const inputCount = f.voices.reduce((count, voice) => count + voice.inputs.length, 0);
  const callerSpeech = setInterval(() => f.audio(), 20);
  t.after(() => clearInterval(callerSpeech));
  f.audio();
  await boundary('caller interruption admitted', () =>
    f.voices.reduce((count, voice) => count + voice.inputs.length, 0) > inputCount, [work]);
  f.ws.send(JSON.stringify({ event: 'mark', streamSid: f.streamSid, mark: { name: audibleMark } }));
  source.captions.revision++; source.captions.callerRevision++;
  source.captions.fragments.push({ eventId: 'caller_interrupts_silence', role: 'user', text: 'Wait, what happened?',
    startMs: 1400, endMs: 1420, sequence: source.captions.revision });
  await source.options.onTranscript(source.captions);
  try { await wait(500); assert.deepEqual(f.voices.flatMap(v => v.spoken), [], 'caller speech still owns the boundary'); }
  finally { clearInterval(callerSpeech); }
  await boundary('saved result delivered through continuous silence', work.settled, [work], 8000);
  clearInterval(silence);
  assert.deepEqual(f.voices.flatMap(v => v.spoken), ['For your earlier request: '+incidentResult]);
  assert.equal(f.posts.length, 1);
});


test('latest account dialogue keeps one generation and publishes one contextualized answer on its original delegation',async t=>{
  const f=await host(t,false,false,false,{admitted:true,resultText:'Your account total is $45.35.',callerText:"Let's check my account."});
  const work=observedWork(f.delegate());await boundary('account submitted',()=>f.posts.length===1,[work]);
  const voice=f.voices[0];
  voice.captions.revision++;voice.captions.callerRevision++;
  voice.captions.fragments.push({eventId:'latency_complaint',role:'user',text:'Does it always take over thirty seconds to check my account?',
    startMs:21,endMs:40,sequence:voice.captions.revision});
  voice.options.onTranscript(voice.captions);
  f.release();await boundary('original answer delivered',work.settled,[work]);
  assert.equal(f.voices.length,1);assert.equal(f.fetches(),1);assert.equal(f.posts.length,1);
  assert.deepEqual(voice.spoken,['For your earlier request: Your account total is $45.35.']);
  assert.deepEqual(voice.native.filter(update=>update.kind==='result'),[{id:'explicit_request',kind:'result',speak:true}]);
  const saved=f.store.get(f.callId)!;
  assert.equal((saved.delegations[f.posts[0].requestId] as BrokerDelegationRecord).delivery,'accepted');
  assert.ok(saved.statusContextRefreshes?.some(refresh=>refresh.kind==='admitted_sources'));
  assert.equal(saved.liveGenerations!.segments.length,1);
  assert.ok(!JSON.stringify(voice.options.eligibleHistory?.initialItems).includes('Your account total is $45.35.'));
});

test('a genuine replacement keeps an accepted earlier answer and omits only the pending answer',async t=>{
  const f=await host(t,false,false,false,{admitted:true,resultText:'Your account total is $45.35.'});
  const first=observedWork(f.delegate());await boundary('first submitted',()=>f.posts.length===1,[first]);
  f.release();await boundary('first accepted',first.settled,[first]);
  const nextMessageId='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
  f.primary.result=async(_binding,requestId)=>({bindingId,requestId,state:'completed',assistant:{messageId:nextMessageId,eventId,sequence:'3',text:'Your next answer.'}});
  f.eligible.assert=async(_binding,version,admission)=>{
    if(version.contextEpoch==='1')throw new PrimaryApiError('http_error',409,{code:'context_changed',retryable:false});
    return {bindingId,callSid:sid,current:true,version,admission};
  };
  f.eligible.fetch=async()=>{
    const value=context(2);value.context.history.admission={invalidationDigest:'b'.repeat(64),messages:[],memories:[]};
    value.context.history.version.threadEventSequence='3';
    value.context.messages=[{id:messageId,role:'assistant',text:'Your account total is $45.35.',eventSequence:'2',provenance:{messageCreatedAt:at,eventId,eventOccurredAt:at,runId}},
      {id:nextMessageId,role:'assistant',text:'Your next answer.',eventSequence:'3',provenance:{messageCreatedAt:at,eventId,eventOccurredAt:at,runId}}];
    return value;
  };
  f.eligible.delivery=async(_binding,requestId,input)=>({bindingId,callSid:sid,requestId,version:input.version,admission:input.admission,
    delivery:{state:'ready',source:{kind:'assistant',messageId:nextMessageId,eventSequence:'3',provenance:{messageCreatedAt:at,eventId,eventOccurredAt:at,runId}},text:'Your next answer.'}});
  const later=await preparedFollowup(f,[],'Check my next account question.');const second=observedWork(later.start());
  await boundary('second capture',later.entered,[second]);later.release();await boundary('second accepted',second.settled,[second]);
  assert.equal(f.voices.length,2);
  const seeded=JSON.stringify(f.voices[1].options.eligibleHistory!.initialItems);
  assert.ok(seeded.includes('Your account total is $45.35.'));
  assert.ok(!seeded.includes('Your next answer.'));
  assert.deepEqual(f.voices[1].spoken,['Your next answer.']);
  assert.deepEqual(f.voices[1].native,[],'old-session task IDs are never addressed in a replacement session');
  assert.equal(f.posts.length,2);
});

test('failed result after a genuine replacement keeps its own ambiguous caller source and successful tool facts', async t => {
  const original = ' What about the big one?';
  const f = await host(t, false, false, false, { failed: true, failureCode: 'provider_busy', callerText: original });
  const fetch = f.eligible.fetch;
  f.eligible.fetch = async (...args) => {
    const context = await fetch(...args), input = f.posts[0]?.input;
    if (input) context.context.messages = [{ id: messageId, role: 'user', text: input.text, eventSequence: '1',
      provenance: { messageCreatedAt: at, eventId, eventOccurredAt: at, runId },
      canonicalSource: { sourceEventId: input.source.sourceEventId, revision: input.source.revision,
        occurredAt: input.source.receivedAt, storedProjectionKind: 'derived' } }];
    return context;
  };
  const result = f.primary.result;
  const partialResult = { kind: 'prepared_action' as const, text: 'An unsigned Apple purchase was prepared. It has not executed.',
    toolInvocationId: eventId, observedAt: at };
  f.primary.result = async (...args) => {
    const response = await result(...args);
    return response.state === 'failed' ? { ...response, partialResult, failure: { kind: 'provider_error' as const } } : response;
  };
  const work = f.delegate(); await until(() => f.posts.length === 1);
  f.release(); await work;
  assert.equal(f.voices.length, 2);
  const target = f.voices[1];
  const contexts = target.quiet.filter(text => text.startsWith('{')).map(text => JSON.parse(text));
  assert.deepEqual(contexts.map(value => value.requestId), [f.posts[0].requestId]);
  assert.equal(contexts.map(value => value.requestText).join(''), original);
  assert.ok(target.published.indexOf(JSON.stringify(contexts[0])) < target.published.indexOf(target.spoken[0]));
  // The failure leads; the quiet preparation follows once, clearly not an execution.
  assert.match(target.spoken.join(''), /^The AI response failed/);
  assert.match(target.spoken.join(''), /The last observed step before that: An unsigned Apple purchase was prepared\. It has not executed\./);
  assert.equal(f.posts.length, 1); assert.equal(f.posts[0].input.text, original);
  assert.equal((f.store.get(f.callId)!.delegations[f.posts[0].requestId] as BrokerDelegationRecord).status, 'failed');
});

test('a tool fact during provider cooldown is quiet progress, and the final failure carries it once after replacement', async t => {
  const f = await host(t, false, false, false, { failed: true, failureCode: 'provider_request_budget_exceeded' });
  const result = f.primary.result;
  const partialResult = { kind: 'portfolio' as const, text: 'Your portfolio observation lists 8 SOL.', toolInvocationId: eventId, observedAt: at };
  f.primary.result = async (...args) => {
    const response = await result(...args);
    return { ...response, partialResult, failure: response.state === 'running'
      ? { kind: 'provider_cooldown' as const, retryAt: '2099-01-01T00:00:00.000Z', retryAfterSeconds: 60 }
      : { kind: 'provider_request_budget' as const } };
  };
  const work = f.delegate();
  await until(() => f.voices.flatMap(voice => voice.quiet).some(text => text.startsWith('Progress on this task, not its result: ' + partialResult.text)));
  await wait(850);
  assert.equal(f.posts.length, 1); assert.equal(f.voices.length, 1);
  assert.deepEqual(f.voices.flatMap(voice => voice.spoken), [], 'progress is never spoken on arrival');
  assert.equal((f.store.get(f.callId)!.delegations[f.posts[0].requestId] as BrokerDelegationRecord).status, 'running');
  f.release(); await work;
  const speech = f.voices.flatMap(voice => voice.spoken);
  assert.match(speech.at(-1)!, /^The AI response exceeded the processing limit/);
  assert.equal(speech.filter(text => text.includes(partialResult.text)).length, 1);
  assert.equal(f.posts.length, 1); assert.equal(f.voices.length, 2);
  assert.equal((f.store.get(f.callId)!.delegations[f.posts[0].requestId] as BrokerDelegationRecord).partialDelivery?.status, 'accepted');
});

test('replacement cannot reinsert a caller source absent from its eligible history', async t => {
  const original = 'My old private request must remain removed.';
  const f = await host(t, false, false, false, { failed: true, failureCode: 'provider_busy', callerText: original });
  const work = f.delegate(); await until(() => f.posts.length === 1);
  f.release(); await work;
  assert.equal(f.voices.length, 2);
  const target = f.voices[1], contexts = target.quiet.filter(text => text.startsWith('{')).map(text => JSON.parse(text));
  assert.deepEqual(contexts, [{ kind: 'task_result_context', requestId: f.posts[0].requestId, state: 'failed', sourceAvailable: false }]);
  assert.ok(target.published.every(text => !text.includes(original)));
  assert.equal(f.posts.length, 1); assert.match(target.spoken.join(''), /AI service is busy/);
});

for (const openingStatus of ['accepted', 'rejected', 'unconfirmed'] as const) test(`initial greeting records ${openingStatus} without another eligibility round trip or replacement replay`, async t => {
  const f = await host(t, false, false, false, { failed: true, failureCode: 'provider_busy', openingStatus });
  await until(() => f.store.get(f.callId)?.voiceOpening?.status === openingStatus);
  assert.equal(f.fetches(), 1); assert.equal(f.assertions(), 1, 'fresh context still has its post-open assertion');
  assert.equal(f.opening.length, 1);
  const work = f.delegate(); await until(() => f.posts.length === 1); f.release(); await work;
  assert.equal(f.voices.length, 2); assert.equal(f.opening.length, 1);
  assert.equal(f.store.get(f.callId)!.voiceOpening!.status, openingStatus);
  assert.ok(f.assertions() > 1, 'ordinary request publication still qualifies current context');
});

test('an outbound call with the greeting clip opens once, continuing after "Hey, it\'s Dexter."', async t => {
  const f = await host(t, false, false, false, { greetingClip: Buffer.alloc(2_400, 0x20) });
  await until(() => f.store.get(f.callId)?.voiceOpening?.status === 'accepted');
  assert.equal(f.opening.length, 1);
  assert.match(f.opening[0], /^The caller already heard you say "Hey, it's Dexter\." Do not greet or say your name again\. Immediately continue with one short question/);
  assert.ok(f.store.get(f.callId)!.journey?.greetingClipStartedAt);
});

test('a confirmed financial receipt survives a later model failure and voice replacement', async t => {
  const f = await host(t, false, false, false, { failed: true, failureCode: 'provider_busy', callerText: 'Did my Apple purchase finish?' });
  const read = f.primary.result;
  const partialResult = { kind: 'action_status' as const, intentId: messageId, actionState: 'confirmed' as const,
    text: 'The saved Apple purchase is confirmed. The receipt reports 0.02 shares received.', toolInvocationId: eventId, observedAt: at };
  f.primary.result = async (...args) => {
    const result = await read(...args);
    return result.state === 'failed' ? { ...result, partialResult, failure: { kind: 'provider_error' as const } } : result;
  };
  const work = f.delegate(); await until(() => f.posts.length === 1); f.release(); await work;
  const speech = f.voices.flatMap(voice => voice.spoken).join('');
  assert.ok(speech.startsWith(partialResult.text)); assert.match(speech, /AI response failed/);
  assert.doesNotMatch(speech, /trade failed|couldn't confirm the trade|not executed|not submitted/);
  assert.equal(f.voices.length, 2); assert.equal(f.posts.length, 1);
  const saved = f.store.get(f.callId)!.delegations[f.posts[0].requestId] as BrokerDelegationRecord;
  assert.equal(saved.status, 'failed'); assert.deepEqual(saved.primaryResult?.partialResult, partialResult);
  assert.equal(saved.primaryResult?.assistant, undefined);
});
