import assert from 'node:assert/strict';
import test, { type TestContext } from 'node:test';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { BrokerStore } from '../src/broker/store.js';
import { createPrimaryBackend, type PrimaryBackendOptions, type BrokerDelegationRecord } from '../src/broker/backend.js';
import type { PrimaryResult, PrimarySubmission, PrimaryTurn } from '../src/broker/primaryApi.js';
import type { LiveBackendChannel, LiveBackendUpdate, LiveDelegationRequest, LiveTranscriptSnapshot } from '../src/live/session.js';

const bindingId = '11111111-1111-5111-a111-111111111111';
const otherBinding = '22222222-2222-5222-a222-222222222222';
const wait = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
function context() {
  let snapshot: LiveTranscriptSnapshot = { revision: 0, callerRevision: 0, fragments: [] };
  const listeners = new Set<(snapshot: LiveTranscriptSnapshot) => void>();
  const updates: LiveBackendUpdate[] = [];
  const controller = new AbortController();
  const channel: LiveBackendChannel = {
    getContext: () => structuredClone(snapshot),
    subscribeContext: listener => { listeners.add(listener); return () => { listeners.delete(listener); }; },
    publish: async update => {
      updates.push(update);
      return { eventId: update.id, status: controller.signal.aborted ? 'closed'
        : update.expectedCallerRevision !== undefined && update.expectedCallerRevision !== snapshot.callerRevision ? 'stale' : 'accepted' };
    },
    signal: controller.signal,
  };
  const add = (text: string, startMs = 800, endMs = 1000, role: 'user' | 'assistant' = 'user') => {
    snapshot.revision++;
    if (role === 'user') snapshot.callerRevision++;
    snapshot.fragments.push({ eventId: `event_${snapshot.revision}`, role, text, startMs, endMs, sequence: snapshot.revision });
    for (const listener of listeners) listener(structuredClone(snapshot));
  };
  return { channel, add, updates, controller };
}

async function fixture(t: TestContext, overrides: Partial<PrimaryBackendOptions> = {}) {
  const directory = await mkdtemp(path.join(tmpdir(), 'broker-backend-'));
  const store = new BrokerStore(directory);
  t.after(async () => { store.close(); await rm(directory, { recursive: true, force: true }); });
  const call = store.reserve({ requestId: 'test-call', number: '+15550000001', direction: 'inbound', purpose: 'test', callSid: 'CA_test' }).call;
  store.update(call.id, { primary: { bindingId, name: null, context: '' }, liveSessionId: 'sess_test', mediaStartedAt: call.createdAt });
  const c = context();
  const posts: Array<{ bindingId: string; requestId: string; input: PrimarySubmission }> = [];
  const reads: string[] = [];
  const captures: PrimaryTurn[] = [];
  let resultText = 'Your balance is $10.25 USDC.';
  const completed = (requestId: string): PrimaryResult => ({ bindingId, requestId, state: 'completed', assistant: {
    messageId: '33333333-3333-5333-a333-333333333333', eventId: '44444444-4444-5444-a444-444444444444', sequence: '1', text: resultText,
  } });
  const behavior = {
    submit: async (id: string): Promise<PrimaryResult & { replayed: boolean }> => ({ bindingId, requestId: id, state: 'queued', replayed: false }),
    result: async (id: string): Promise<PrimaryResult> => completed(id),
    capture: async (_turn: PrimaryTurn) => {},
  };
  const api: PrimaryBackendOptions['api'] = {
    submit: async (binding, id, input) => {
      // Inspect the actual durable journal before the fake remote admission.
      const saved = JSON.parse(await readFile(path.join(directory, 'calls.json'), 'utf8'));
      assert.equal(saved.calls[call.id].delegations[id].status, 'dispatching');
      assert.deepEqual(saved.calls[call.id].delegations[id].submission, input);
      posts.push({ bindingId: binding, requestId: id, input });
      return behavior.submit(id);
    },
    result: async (_binding, id) => { reads.push(id); return behavior.result(id); },
    captureTurn: async (binding, turn) => {
      captures.push(structuredClone(turn)); await behavior.capture(turn);
      return { bindingId: binding, sourceEventId: turn.sourceEventId, revision: turn.revision, recordId: turn.sourceEventId, replayed: false };
    },
  };
  const options: PrimaryBackendOptions = { callId: call.id, callSid: 'CA_test', bindingId, api, store,
    stabilityMs: 5, maxWaitMs: 50, pollIntervalMs: 5, maxPollMs: 30, ...overrides };
  const backend = createPrimaryBackend(options);
  const speech = (startMs = 800, endMs = 1000, instance = backend) => {
    for (let offset = startMs; offset <= endMs; offset += 5) instance.noteCallerSpeech(Date.now(), offset);
  };
  const request = (delegationId = 'del_1'): LiveDelegationRequest => ({ callId: call.id, sessionId: 'sess_test', delegationId,
    requestId: `live_${delegationId}`, offsetMs: 1200, transcript: c.channel.getContext() });
  const saved = () => Object.values(store.get(call.id)!.delegations) as BrokerDelegationRecord[];
  return { ...c, directory, store, call, api, options, backend, speech, request, posts, reads, captures, behavior, completed,
    saved, result: (text: string) => { resultText = text; } };
}

test('waits for stable acoustic caller text and journals one verbatim canonical source before POST', async t => {
  const f = await fixture(t);
  f.add('What is my'); f.speech();
  const pending = f.backend.delegate(f.request(), f.channel);
  await wait(1);
  f.add(' USDC balance?', 1000, 1200); f.speech(1000, 1200);
  await pending;
  assert.equal(f.posts.length, 1); assert.equal(f.posts[0].input.text, 'What is my USDC balance?');
  assert.match(f.posts[0].requestId, /^[a-f0-9-]{36}$/);
  assert.equal(f.posts[0].input.source.role, 'user'); assert.equal(f.posts[0].input.source.revision, 1);
  assert.match(f.posts[0].input.source.itemId, /^[A-Za-z0-9_-]{1,128}$/);
  assert.equal(f.saved()[0].completionBasis, 'speech_and_transcript_stability');
  assert.equal(f.saved()[0].status, 'completed');
});

test('duplicate and overlapping delegation delivery cannot produce a second submission', async t => {
  const f = await fixture(t); f.add('Read my balance.'); f.speech();
  await Promise.all([f.backend.delegate(f.request(), f.channel), f.backend.delegate(f.request(), f.channel)]);
  await f.backend.delegate(f.request('del_repeated'), f.channel);
  assert.equal(f.posts.length, 1); assert.equal(f.saved().length, 1);
  assert.ok(f.saved()[0].delegationIds.includes('del_repeated'));
});

test('an uncertain POST is reconciled using GET of the original request, never resubmitted', async t => {
  const f = await fixture(t); f.add('Read my balance.'); f.speech();
  f.behavior.submit = async () => { throw new Error('request timeout'); };
  await f.backend.delegate(f.request(), f.channel);
  assert.equal(f.posts.length, 1); assert.deepEqual(f.reads, [f.posts[0].requestId]);
  assert.equal(f.saved()[0].status, 'completed');
});

test('a restarted backend recovers a journalled unknown admission under the same request ID', async t => {
  const f = await fixture(t, { maxPollMs: 10 }); f.add('Read my balance.'); f.speech();
  f.behavior.submit = async () => { throw new Error('timeout'); };
  f.behavior.result = async () => { throw new Error('not found yet'); };
  await f.backend.delegate(f.request(), f.channel);
  assert.equal(f.saved()[0].status, 'dispatch_unknown');
  assert.match(f.updates.at(-1)!.text, /could not confirm/);
  f.behavior.result = async id => f.completed(id);
  await createPrimaryBackend(f.options).delegate(f.request(), f.channel);
  assert.equal(f.posts.length, 1); assert.equal(f.saved()[0].status, 'completed');
  assert.ok(f.reads.every(id => id === f.posts[0].requestId));
});

test('late same-span caller correction becomes one source revision and cannot become another action', async t => {
  const f = await fixture(t); f.add('Read the balance'); f.speech();
  let entered!: () => void;
  const began = new Promise<void>(resolve => { entered = resolve; });
  let release!: () => void;
  const pending = new Promise<void>(resolve => { release = resolve; });
  f.behavior.result = async id => { entered(); await pending; return f.completed(id); };
  const work = f.backend.delegate(f.request(), f.channel); await began;
  f.add(' of the other wallet.', 1000, 1200); f.speech(1000, 1200);
  release(); await work;
  assert.equal(f.posts.length, 1); assert.equal(f.posts[0].input.text, 'Read the balance');
  assert.equal(f.captures.length, 1); assert.equal(f.captures[0].revision, 2);
  assert.equal(f.captures[0].sourceEventId, f.posts[0].input.source.sourceEventId);
  assert.equal(f.captures[0].text, 'Read the balance of the other wallet.');
  assert.equal(f.saved()[0].correctionState, 'recorded_not_applied');
  assert.match(f.updates.at(-1)!.text, /correction has not changed/);
  await f.backend.delegate(f.request('del_correction'), f.channel);
  assert.equal(f.posts.length, 1);
});

test('a later independent caller span excludes the already submitted turn', async t => {
  const f = await fixture(t); f.add('Read my balance.'); f.speech();
  await f.backend.delegate(f.request(), f.channel);
  f.add('What was my last payment?', 3000, 3200); f.speech(3000, 3200);
  await f.backend.delegate(f.request('del_2'), f.channel);
  assert.equal(f.posts.length, 2);
  assert.equal(f.posts[1].input.text, 'What was my last payment?');
  assert.notEqual(f.posts[0].input.source.sourceEventId, f.posts[1].input.source.sourceEventId);
});

test('identity mismatch prevents dispatch and a mismatched response cannot be spoken as success', async t => {
  const f = await fixture(t); f.add('Read my balance.'); f.speech();
  f.store.update(f.call.id, { primary: { bindingId: otherBinding, name: null, context: '' } });
  await assert.rejects(f.backend.delegate(f.request(), f.channel), /identity_mismatch/);
  assert.equal(f.posts.length, 0);
  f.store.update(f.call.id, { primary: { bindingId, name: null, context: '' } });
  f.behavior.result = async id => ({ ...f.completed(id), bindingId: otherBinding });
  await f.backend.delegate(f.request(), f.channel);
  assert.equal(f.saved()[0].status, 'queued');
  assert.ok(!f.updates.some(update => update.text.includes('$10.25')));
});

test('long results retain every character and exact amounts in bounded context chunks', async t => {
  const f = await fixture(t); f.add('Read my portfolio.'); f.speech();
  const text = 'Your balance is $12,345.67 USDC. ' + 'The portfolio includes 12.345678 shares. '.repeat(35);
  f.result(text); await f.backend.delegate(f.request(), f.channel);
  const chunks = f.updates.filter(update => update.kind === 'progress');
  assert.equal(chunks.map(update => update.text).join(''), text);
  assert.ok(chunks.every(update => Buffer.byteLength(update.text) <= 500 && update.speak === false));
  assert.equal(f.updates.at(-1)!.text, 'Your balance is $12,345.67 USDC.');
  assert.equal(f.saved()[0].result, text);
});

test('stable transcript alone cannot establish a completed acoustic caller turn', async t => {
  const f = await fixture(t); f.add('Read my balance.');
  await f.backend.delegate(f.request(), f.channel);
  assert.equal(f.posts.length, 0); assert.match(f.updates.at(-1)!.text, /finish your request/);
});

test('final capture uses canonical IDs once for ordinary caller and fully played assistant turns', async t => {
  const f = await fixture(t); f.add('Hello there.'); f.speech();
  f.add('Hello. How can I help?', 1500, 1800, 'assistant'); await wait(7);
  const snapshot = f.channel.getContext();
  const proof = { assistantPlaybackComplete: true, sessionFinalized: true };
  const first = await f.backend.captureCompletedTurns(snapshot, proof);
  assert.deepEqual(first, { captured: 2, pending: 0, unsupported: [] });
  assert.equal(f.captures[0].role, 'user'); assert.equal(f.captures[1].role, 'assistant');
  assert.equal(f.captures[0].text, 'Hello there.');
  assert.deepEqual(await f.backend.captureCompletedTurns(snapshot, proof), { captured: 0, pending: 0, unsupported: [] });
  assert.equal(f.captures.length, 2);
});

test('submitted caller source is reused during final capture; no second source or financial submission', async t => {
  const f = await fixture(t); f.add('Read my balance.'); f.speech();
  await f.backend.delegate(f.request(), f.channel);
  const result = await f.backend.captureCompletedTurns(f.channel.getContext(), { assistantPlaybackComplete: true, sessionFinalized: true });
  assert.deepEqual(result, { captured: 0, pending: 0, unsupported: [] });
  assert.equal(f.captures.length, 0); assert.equal(f.posts.length, 1);
});

test('terminal speech and cleared assistant audio remain unqualified', async t => {
  const f = await fixture(t); f.add('Let me just'); f.speech(); f.add('A partial answer', 1500, 1800, 'assistant');
  const result = await f.backend.captureCompletedTurns(f.channel.getContext(), { assistantPlaybackComplete: false, sessionFinalized: true });
  assert.ok(result.unsupported.includes('unqualified_caller_turn'));
  assert.ok(result.unsupported.includes('assistant_playback_unconfirmed'));
  assert.equal(f.captures.length, 0);
});

test('uncertain source capture retains its frozen revision and never retries changed content', async t => {
  const f = await fixture(t); f.add('Hello there.'); f.speech(); await wait(7);
  f.behavior.capture = async () => { throw new Error('unknown capture outcome'); };
  const proof = { assistantPlaybackComplete: true, sessionFinalized: true };
  assert.ok((await f.backend.captureCompletedTurns(f.channel.getContext(), proof)).unsupported.includes('capture_outcome_unknown'));
  f.add(' Welcome.', 1000, 1200); f.speech(1000, 1200); await wait(7);
  await f.backend.captureCompletedTurns(f.channel.getContext(), proof);
  assert.equal(f.captures.length, 1);
  assert.equal(f.captures[0].revision, 1); assert.equal(f.captures[0].text, 'Hello there.');
});

test('voice disconnection stops result delivery without cancelling durable Primary work', async t => {
  const f = await fixture(t); f.add('Read my balance.'); f.speech();
  f.behavior.result = async id => { f.controller.abort(); return { bindingId, requestId: id, state: 'running' }; };
  await f.backend.delegate(f.request(), f.channel);
  assert.equal(f.posts.length, 1); assert.equal(f.saved()[0].status, 'running');
  assert.equal(f.updates.length, 0);
});

test('recovery replays an unknown ordinary capture byte for byte after call end', async t => {
  const f = await fixture(t); f.add('Hello there.'); f.speech(); await wait(7);
  f.behavior.capture = async () => { throw new Error('capture response lost'); };
  const result = await f.backend.captureCompletedTurns(f.channel.getContext(), { assistantPlaybackComplete: true, sessionFinalized: true });
  assert.equal(result.pending, 1);
  const frozen = structuredClone(f.captures[0]);
  f.store.update(f.call.id, { endedAt: new Date().toISOString() });
  f.add('A later transcript fragment.', 1000, 1200);
  f.behavior.capture = async () => {};
  const restored = createPrimaryBackend(f.options);
  assert.deepEqual(await restored.flushPendingCaptures(), { pending: 0, unsupported: [] });
  assert.deepEqual(f.captures[1], frozen);
  assert.equal(f.posts.length, 0);
  assert.deepEqual(await restored.flushPendingCaptures(), { pending: 0, unsupported: [] });
  assert.equal(f.captures.length, 2);
});

test('delegated correction recovery replays its exact frozen revision and never submits again', async t => {
  const f = await fixture(t); f.add('Read my balance'); f.speech();
  f.behavior.result = async id => {
    f.add(' please.', 1000, 1200); f.speech(1000, 1200);
    return f.completed(id);
  };
  f.behavior.capture = async () => { throw new Error('capture timeout'); };
  await f.backend.delegate(f.request(), f.channel);
  assert.equal(f.saved()[0].captureState, 'unknown');
  const frozen = structuredClone(f.captures[0]);
  f.store.update(f.call.id, { endedAt: new Date().toISOString() });
  f.behavior.capture = async () => {};
  assert.deepEqual(await createPrimaryBackend(f.options).flushPendingCaptures(), { pending: 0, unsupported: [] });
  assert.deepEqual(f.captures[1], frozen);
  assert.equal(f.saved()[0].sourceRevision, 2); assert.equal(f.posts.length, 1);
});

test('unresolved admission keeps closeout pending and recovery only reads the original request', async t => {
  const f = await fixture(t, { maxPollMs: 10 }); f.add('Read my balance.'); f.speech();
  f.behavior.submit = async () => { throw new Error('submit timeout'); };
  f.behavior.result = async () => { throw new Error('not found yet'); };
  await f.backend.delegate(f.request(), f.channel);
  f.store.update(f.call.id, { endedAt: new Date().toISOString() });
  const restored = createPrimaryBackend(f.options);
  assert.deepEqual(await restored.flushPendingCaptures(), { pending: 1, unsupported: ['source_admission_unknown'] });
  f.behavior.result = async id => f.completed(id);
  assert.deepEqual(await restored.flushPendingCaptures(), { pending: 0, unsupported: [] });
  assert.equal(f.posts.length, 1); assert.equal(f.captures.length, 0);
  assert.ok(f.reads.every(id => id === f.posts[0].requestId));
});

test('a separate caller turn during a running request remains available after completion', async t => {
  const f = await fixture(t); f.add('Read my balance.'); f.speech();
  let entered!: () => void; const began = new Promise<void>(resolve => { entered = resolve; });
  let release!: () => void; const blocked = new Promise<void>(resolve => { release = resolve; });
  f.behavior.result = async id => { entered(); await blocked; return f.completed(id); };
  const first = f.backend.delegate(f.request(), f.channel); await began;
  f.add('Read my last payment.', 3000, 3200); f.speech(3000, 3200);
  release(); await first;
  assert.equal(f.saved()[0].lastSequence, 1);
  assert.equal(f.saved()[0].correctionState, undefined);
  assert.equal(f.captures.length, 0);
  assert.ok(!f.updates.at(-1)!.text.includes('correction'));
  await f.backend.delegate(f.request('del_2'), f.channel);
  assert.equal(f.posts.length, 2); assert.equal(f.posts[1].input.text, 'Read my last payment.');
});

test('waiting after disconnect cannot turn unfinished terminal speech into a completed source', async t => {
  const f = await fixture(t); f.add('Let me think'); f.speech();
  f.store.update(f.call.id, { endedAt: new Date().toISOString() });
  await wait(10);
  const result = await f.backend.captureCompletedTurns(f.channel.getContext(), { assistantPlaybackComplete: false, sessionFinalized: true });
  assert.equal(result.captured, 0); assert.ok(result.unsupported.includes('unqualified_caller_turn'));
  assert.throws(() => f.backend.noteCallerSpeech(), /call_ended/);
  await assert.rejects(f.backend.delegate(f.request('late'), f.channel), /call_ended/);
  assert.equal(f.posts.length, 0);
});

test('canonical occurrence time uses the media origin rather than call reservation or ringing', async t => {
  const f = await fixture(t); const origin = new Date(Date.parse(f.call.createdAt) + 30_000).toISOString();
  f.store.update(f.call.id, { mediaStartedAt: origin });
  f.add('Read my balance.'); f.speech(); await f.backend.delegate(f.request(), f.channel);
  assert.equal(f.posts[0].input.source.receivedAt, new Date(Date.parse(origin) + 800).toISOString());
});

test('caller source preserves delta delivery order even when approximate intervals overlap or regress', async t => {
  const f = await fixture(t); f.add('Read ', 850, 1000); f.add('my balance.', 800, 980); f.speech();
  await f.backend.delegate(f.request(), f.channel);
  assert.equal(f.posts[0].input.text, 'Read my balance.');
});

test('delegation during active work durably records separate speech and explicitly requires a new request', async t => {
  const f = await fixture(t); f.add('Read my balance.'); f.speech();
  let entered!: () => void; const began = new Promise<void>(resolve => { entered = resolve; });
  let release!: () => void; const blocked = new Promise<void>(resolve => { release = resolve; });
  f.behavior.result = async id => { entered(); await blocked; return f.completed(id); };
  const first = f.backend.delegate(f.request(), f.channel); await began;
  f.add('Read my last payment.', 3000, 3200); f.speech(3000, 3200);
  const second = f.backend.delegate(f.request('del_followup'), f.channel);
  await wait(12);
  assert.equal(f.saved()[0].deferredTurns?.[0].text, 'Read my last payment.');
  release(); await Promise.all([first, second]);
  assert.equal(f.posts.length, 1);
  assert.match(f.updates.at(-1)!.text, /recorded what you said afterward/);
  await f.backend.delegate(f.request('del_repeat_no_new_speech'), f.channel);
  assert.equal(f.posts.length, 1);
  f.add('Please read my last payment now.', 5000, 5200); f.speech(5000, 5200);
  await f.backend.delegate(f.request('del_fresh'), f.channel);
  assert.equal(f.posts.length, 2); assert.equal(f.posts[1].input.text, 'Please read my last payment now.');
});

test('reconstructed closeout waits for an in-flight capture before deciding whether it needs replay', async t => {
  const f = await fixture(t); f.add('Hello there.'); f.speech(); await wait(7);
  let entered!: () => void; const began = new Promise<void>(resolve => { entered = resolve; });
  let release!: () => void; const blocked = new Promise<void>(resolve => { release = resolve; });
  f.behavior.capture = async () => { entered(); await blocked; };
  const capture = f.backend.captureCompletedTurns(f.channel.getContext(), { assistantPlaybackComplete: true, sessionFinalized: true });
  await began;
  f.backend.persistAcousticEvidence();
  f.store.update(f.call.id, { endedAt: new Date().toISOString() });
  const recovered = createPrimaryBackend(f.options).flushPendingCaptures();
  await wait(5); assert.equal(f.captures.length, 1);
  release(); await capture;
  assert.deepEqual(await recovered, { pending: 0, unsupported: [] });
  assert.equal(f.captures.length, 1);
});

test('an unsplittable numeric token is saved in full and produces an explicit voice-format limitation', async t => {
  const f = await fixture(t); f.add('Read my portfolio.'); f.speech();
  const text = 'The exact amount is ' + '1'.repeat(510) + ' USDC.';
  f.result(text); await f.backend.delegate(f.request(), f.channel);
  assert.equal(f.saved()[0].result, text);
  assert.match(f.updates.at(-1)!.text, /cannot read that format accurately/);
  assert.ok(!f.updates.at(-1)!.text.includes('111'));
});
