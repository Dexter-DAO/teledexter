import assert from 'node:assert/strict';
import test, { type TestContext } from 'node:test';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { BrokerStore } from '../src/broker/store.js';
import { createPrimaryBackend, literalPhoneAnswer, type PrimaryBackendOptions, type BrokerDelegationRecord } from '../src/broker/backend.js';
import type { PrimaryQuestion, PrimaryQuestionResult, PrimaryReplyInput, PrimaryResult, PrimarySubmission, PrimaryTurn } from '../src/broker/primaryApi.js';
import { PrimaryApiError } from '../src/broker/primaryApi.js';
import { createPortfolioContext } from '../src/broker/portfolioContext.js';
import { turnBodyDigest } from '../src/broker/reconciliation.js';
import { rejectBrokerPublication } from '../src/broker/publications.js';
import type { LiveBackendChannel, LiveBackendUpdate, LiveDelegationRequest, LiveTranscriptSnapshot } from '../src/live/session.js';

const bindingId = '11111111-1111-5111-a111-111111111111';
const otherBinding = '22222222-2222-5222-a222-222222222222';
const wait = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
// Exact caller fragments from the 2026-09-22 minimal-instructions Live trial.
// The raw acoustic span was 8320..10900; the last word starts at 11400.
const recordedCallerFragments = [
  { eventId: 'event_EQr8OSLiWsYCwPQ0LiYKS', text: ' Please', startMs: 9000, endMs: 9200 },
  { eventId: 'event_EQr8P8mJEuiPd8Y1Ovk6s', text: ' ask', startMs: 9400, endMs: 9600 },
  { eventId: 'event_EQr8PJcJmdUNNY2so4RY8', text: ' Primary', startMs: 10000, endMs: 10200 },
  { eventId: 'event_EQr8Q8H2rxyhpLt1MirfO', text: ' for my', startMs: 10400, endMs: 10600 },
  { eventId: 'event_EQr8QHfHWO6XbHGjMZmmB', text: ' test', startMs: 10800, endMs: 11000 },
  { eventId: 'event_EQr8RweQWbbV0Skp6fnYS', text: ' phrase', startMs: 11400, endMs: 11600 },
];
const recordedCallerText = recordedCallerFragments.map(fragment => fragment.text).join('');
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
      return { eventId: update.id, observedCallerRevision: snapshot.callerRevision, observedTranscriptSequence: snapshot.revision,
        status: controller.signal.aborted ? 'closed'
        : update.expectedCallerRevision !== undefined && update.expectedCallerRevision !== snapshot.callerRevision ? 'stale' : 'accepted' };
    },
    signal: controller.signal,
  };
  const add = (text: string, startMs = 800, endMs = 1000, role: 'user' | 'assistant' = 'user', eventId?: string) => {
    snapshot.revision++;
    if (role === 'user') snapshot.callerRevision++;
    snapshot.fragments.push({ eventId: eventId ?? `event_${snapshot.revision}`, role, text, startMs, endMs, sequence: snapshot.revision });
    for (const listener of listeners) listener(structuredClone(snapshot));
  };
  return { channel, add, updates, controller };
}

async function fixture(t: TestContext, overrides: Partial<PrimaryBackendOptions> = {}, assigned = false) {
  const directory = await mkdtemp(path.join(tmpdir(), 'broker-backend-'));
  const store = new BrokerStore(directory);
  t.after(async () => { store.close(); await rm(directory, { recursive: true, force: true }); });
  const call = store.reserve({ requestId: assigned ? 'abababab-abab-5bab-abab-abababababab' : 'test-call',
    number: '+15550000001', direction: assigned ? 'outbound' : 'inbound', purpose: 'test', callSid: 'CA_test' },
  assigned ? 'authenticated_broker_control' : undefined).call;
  store.update(call.id, { primary: { bindingId, name: null, context: '', ...(assigned ? { callAssignmentRef: {
    assignmentId: call.requestId, revision: 1 as const, digest: 'a'.repeat(64),
  } } : {}) }, liveSessionId: 'sess_test', mediaStartedAt: call.createdAt });
  const c = context();
  const posts: Array<{ bindingId: string; requestId: string; input: PrimarySubmission }> = [];
  const reads: string[] = [];
  const captures: PrimaryTurn[] = [];
  const replies: Array<{ bindingId: string; requestId: string; questionId: string; input: PrimaryReplyInput }> = [];
  const questionReads: Array<{ bindingId: string; requestId: string; questionId: string }> = [];
  let resultText = 'Your balance is $10.25 USDC.';
  const completed = (requestId: string): PrimaryResult => ({ bindingId, requestId, state: 'completed', assistant: {
    messageId: '33333333-3333-5333-a333-333333333333', eventId: '44444444-4444-5444-a444-444444444444', sequence: '1', text: resultText,
  } });
  const behavior = {
    submit: async (id: string): Promise<PrimaryResult & { replayed: boolean }> => ({ bindingId, requestId: id, state: 'queued', replayed: false }),
    result: async (id: string): Promise<PrimaryResult> => completed(id),
    capture: async (_turn: PrimaryTurn) => {},
    question: async (_id: string, _questionId: string): Promise<PrimaryQuestionResult> => { throw new Error('No question configured'); },
    reply: async (_id: string, _questionId: string, _input: PrimaryReplyInput): Promise<PrimaryQuestionResult> => { throw new Error('No reply configured'); },
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
    question: async (binding, id, questionId) => {
      questionReads.push({ bindingId: binding, requestId: id, questionId });
      return behavior.question(id, questionId);
    },
    reply: async (binding, id, questionId, input) => {
      const saved = JSON.parse(await readFile(path.join(directory, 'calls.json'), 'utf8'));
      const reply = saved.calls[call.id].delegations[id].questions[questionId].reply;
      assert.equal(reply.status, 'dispatching');
      assert.deepEqual(reply.input, input);
      replies.push({ bindingId: binding, requestId: id, questionId, input: structuredClone(input) });
      return behavior.reply(id, questionId, input);
    },
  };
  const options: PrimaryBackendOptions = { callId: call.id, callSid: 'CA_test', bindingId, api, store,
    stabilityMs: 5, maxWaitMs: 50, pollIntervalMs: 5, maxPollMs: 30, ...overrides };
  const backend = createPrimaryBackend(options);
  const speech = (startMs = 800, endMs = 1000, instance = backend) => {
    for (let offset = startMs; offset <= endMs; offset += 5) instance.noteCallerSpeech(Date.now(), offset);
  };
  const request = (delegationId = 'del_1'): LiveDelegationRequest => ({ callId: call.id, sessionId: 'sess_test', delegationId,
    requestId: `live_${delegationId}`, offsetMs: Math.max(0, ...c.channel.getContext().fragments.map(fragment => fragment.endMs)) + 200, transcript: c.channel.getContext() });
  const saved = () => Object.values(store.get(call.id)!.delegations) as BrokerDelegationRecord[];
  return { ...c, directory, store, call, api, options, backend, speech, request, posts, reads, captures, replies, questionReads, behavior, completed,
    saved, result: (text: string) => { resultText = text; } };
}

test('a saved confirmed action and final completion refresh call portfolio without changing Primary delegation', async t => {
  let reads = 0;
  const portfolio = createPortfolioContext('synthetic', bindingId, { portfolio: async () => {
    reads++; return { version: 'phone.portfolio.v1', bindingId, observedAt: new Date().toISOString(), totalUsd: '10',
      holdingsComplete: true, holdingCount: 0, unpricedCount: 0, omittedCount: 0, holdings: [] };
  } }); t.after(() => portfolio.stop());
  // This checks the completed result as well as repeated partials, so leave
  // time for all three polls and their durable writes under suite contention.
  const f = await fixture(t, { maxPollMs: 5000, onResult: result => portfolio.observe(result) });
  let polls = 0;
  const partialResult = { kind: 'action_status' as const, actionState: 'confirmed' as const,
    intentId: '55555555-5555-4555-8555-555555555555', toolInvocationId: '66666666-6666-4666-8666-666666666666',
    observedAt: new Date().toISOString(), text: 'The purchase was confirmed.' };
  f.behavior.result = async requestId => ++polls < 3 ? { bindingId, requestId, state: 'running', partialResult }
    : f.completed(requestId);
  f.add('Buy one dollar of Apple.'); f.speech();
  await f.backend.delegate(f.request(), f.channel);
  assert.equal(f.posts.length, 1);
  assert.equal(polls, 3);
  assert.equal(f.saved()[0].status, 'completed');
  assert.equal(reads, 3, 'initial read, confirmed-action refresh and final refresh despite repeated polls');
});

test('accepted publication binds the next unchanged caller request and exact retry retains its pair', async t => {
  const f = await fixture(t);
  f.add('Show my portfolio.'); f.speech();
  const publish = f.channel.publish;
  f.channel.publish = async update => {
    const intent = f.store.get(f.call.id)!.publications?.records.find(record => record.updateId === update.id);
    assert.ok(intent, 'intent must be durable before commentary dispatch');
    assert.equal(intent.requestId, f.posts[0].requestId);
    const result = await publish(update);
    return { ...result, eventId: `broker_${f.updates.length}` };
  };
  await f.backend.delegate(f.request(), f.channel);
  f.channel.publish = publish;
  const original = f.posts[0].requestId;
  f.add(' Then review it.\t', 4000, 4300); f.speech(4000, 4300);
  await f.backend.delegate(f.request('followup'), f.channel);
  assert.equal(f.posts.length, 2);
  assert.equal(f.posts[1].input.text, ' Then review it.\t');
  assert.equal(f.posts[1].input.relatedPrimaryRequestId, original);
  assert.equal(f.posts[1].input.referenceOrigin, 'broker_status_publication');
  const frozen = structuredClone(f.posts[1].input);
  const firstPublication = f.store.get(f.call.id)!.publications!.records[0];
  rejectBrokerPublication(f.store, firstPublication, firstPublication.outcome!.eventId, 'late_rejection');
  await f.backend.delegate(f.request('followup'), f.channel);
  assert.equal(f.posts.length, 2);
  assert.deepEqual(f.saved()[1].submission, frozen, 'late evidence cannot rewrite an already submitted source');
});

test('request timing retains first dispatch, saved answer, and delivery acceptance through repeated observations', async t => {
  const f = await fixture(t);
  f.add('Show my portfolio.'); f.speech();
  await f.backend.delegate(f.request(), f.channel);
  const timing = structuredClone(f.saved()[0].timing);
  assert.ok(timing?.firstSavedAt); assert.ok(timing.firstDispatchAt);
  assert.ok(timing.firstAnswerAvailableAt); assert.ok(timing.firstDeliveryAcceptedAt);
  assert.ok(Date.parse(timing.firstSavedAt) <= Date.parse(timing.firstDispatchAt));
  assert.ok(Date.parse(timing.firstDispatchAt) <= Date.parse(timing.firstAnswerAvailableAt));
  assert.ok(Date.parse(timing.firstAnswerAvailableAt) <= Date.parse(timing.firstDeliveryAcceptedAt));
  await f.backend.delegate(f.request(), f.channel);
  assert.deepEqual(f.saved()[0].timing, timing); assert.equal(f.posts.length, 1);
});

test('late rejection before the next caller source excludes that publication reference', async t => {
  const f = await fixture(t);
  f.add('Show my portfolio.'); f.speech();
  await f.backend.delegate(f.request(), f.channel);
  const publication = f.store.get(f.call.id)!.publications!.records[0];
  rejectBrokerPublication(f.store, publication, publication.outcome!.eventId, 'late_rejection');
  f.add(' Then review it.', 4000, 4300); f.speech(4000, 4300);
  await f.backend.delegate(f.request('followup'), f.channel);
  assert.equal(f.posts.length, 2);
  assert.equal(f.posts[1].input.relatedPrimaryRequestId, undefined);
});

test('missing provider frontier leaves accepted commentary ineligible for a caller reference', async t => {
  const f = await fixture(t);
  const publish = f.channel.publish;
  f.channel.publish = async update => { const value = await publish(update); return { eventId: value.eventId, status: value.status }; };
  f.add('Show my portfolio.'); f.speech();
  await f.backend.delegate(f.request(), f.channel);
  f.add(' Then review it.', 4000, 4300); f.speech(4000, 4300);
  await f.backend.delegate(f.request('followup'), f.channel);
  assert.equal(f.posts[1].input.relatedPrimaryRequestId, undefined);
});

test('unknown admission and restarted GET-only recovery retain the original referenced input', async t => {
  const f = await fixture(t);
  f.add('Show my portfolio.'); f.speech();
  await f.backend.delegate(f.request(), f.channel);
  f.behavior.submit = async () => { throw new PrimaryApiError('request_timeout'); };
  f.behavior.result = async () => { throw new PrimaryApiError('http_error', 404, { code: 'request_not_found', retryable: false }); };
  f.add(' Then review it.\t', 4000, 4300); f.speech(4000, 4300);
  await f.backend.delegate(f.request('followup'), f.channel);
  const submitted = structuredClone(f.posts[1]);
  assert.equal(submitted.input.relatedPrimaryRequestId, f.posts[0].requestId);
  assert.equal(f.saved()[1].status, 'dispatch_unknown');
  const recovered = createPrimaryBackend(f.options);
  f.behavior.result = async id => f.completed(id);
  await recovered.delegate(f.request('followup'), f.channel);
  assert.equal(f.posts.length, 2);
  assert.deepEqual(f.saved()[1].submission, submitted.input);
  assert.equal(f.saved()[1].status, 'completed');
  assert.equal(f.reads.at(-1), submitted.requestId);
});

test('waits for stable acoustic caller text and journals one verbatim canonical source before POST', async t => {
  const f = await fixture(t);
  f.add('What is my'); f.speech();
  const subscribe = f.channel.subscribeContext;
  let captionDelivered = false;
  f.channel.subscribeContext = listener => {
    const unsubscribe = subscribe(listener);
    if (!captionDelivered) queueMicrotask(() => {
      if (captionDelivered) return;
      captionDelivered = true;
      assert.equal(f.posts.length, 0);
      f.add(' USDC balance?', 1000, 1200); f.speech(1000, 1200);
    });
    return unsubscribe;
  };
  await f.backend.delegate(f.request(), f.channel);
  assert.equal(captionDelivered, true);
  assert.equal(f.posts.length, 1); assert.equal(f.posts[0].input.text, 'What is my USDC balance?');
  assert.match(f.posts[0].requestId, /^[a-f0-9-]{36}$/);
  assert.equal(f.posts[0].input.source.role, 'user'); assert.equal(f.posts[0].input.source.revision, 1);
  assert.match(f.posts[0].input.source.itemId, /^[A-Za-z0-9_-]{1,128}$/);
  assert.equal(f.saved()[0].completionBasis, 'speech_and_transcript_stability');
  assert.equal(f.saved()[0].status, 'completed');
});

test('recorded Hello delegates the assigned mission across distinct Live and phone audio clocks', async t => {
  const f = await fixture(t, {}, true);
  // Retained 14:51 rehearsal: Live caption 10600..10800, local audio 9620..10120.
  f.add(' Hello', 10600, 10800, 'user', 'recorded_hello'); f.speech(9640, 10120);
  await f.backend.delegate(f.request('item_EQw7mnC2ABitLPfHNrYAX'), f.channel);
  assert.equal(f.posts.length, 1);
  assert.equal(f.posts[0].input.text, ' Hello');
  assert.deepEqual(f.posts[0].input.callAssignmentRef, f.store.get(f.call.id)!.primary!.callAssignmentRef);
  assert.equal(f.saved()[0].audioThroughMs, 10120);
  assert.equal(f.posts[0].input.source.receivedAt, new Date(Date.parse(f.call.createdAt) + 10600).toISOString());
  const original = structuredClone(f.posts[0]);
  await f.backend.delegate(f.request('item_EQw7mnC2ABitLPfHNrYAX'), f.channel);
  assert.equal(f.posts.length, 1); assert.deepEqual(f.posts[0], original);
});

test('ordered caller turns retain their sources when clock distance changes without a fixed offset', async t => {
  const f = await fixture(t);
  f.add('Read my portfolio.', 50, 100); f.speech(8000, 8500);
  await f.backend.delegate(f.request(), f.channel);
  f.add('Read my last payment.', 3000, 3400); f.speech(20000, 20500);
  await f.backend.delegate(f.request('second'), f.channel);
  assert.deepEqual(f.posts.map(post => post.input.text), ['Read my portfolio.', 'Read my last payment.']);
  assert.equal(f.saved()[1].audioThroughMs, 20500);
  assert.equal(f.posts[1].input.source.receivedAt, new Date(Date.parse(f.call.createdAt) + 3000).toISOString());
  f.store.update(f.call.id, { endedAt: new Date().toISOString() });
  const restored = createPrimaryBackend(f.options);
  assert.deepEqual(await restored.captureCompletedTurns(f.channel.getContext(), { sessionFinalized: true, assistantPlaybackComplete: true }),
    { captured: 0, pending: 0, unsupported: [] });
});

test('noise without a caption neither submits a task nor prevents later caller speech after restart', async t => {
  const f = await fixture(t);
  f.speech(200, 400); // A sound burst without recognized words.
  f.add('Read my balance.', 10600, 10800); f.speech(9640, 10120);
  await f.backend.delegate(f.request(), f.channel);
  assert.equal(f.posts.length, 1);
  const original = structuredClone(f.saved()[0].submission);
  f.speech(15000, 15400); f.backend.persistAcousticEvidence();
  const restored = createPrimaryBackend(f.options);
  await restored.delegate(f.request(), f.channel);
  assert.equal(f.posts.length, 1); assert.deepEqual(f.saved()[0].submission, original);
  f.add('Read my last payment.', 16000, 16300); f.speech(20000, 20400, restored);
  await restored.delegate(f.request('payment'), f.channel);
  assert.deepEqual(f.posts.map(post => post.input.text), ['Read my balance.', 'Read my last payment.']);
  assert.deepEqual(f.saved()[0].submission, original);
});

test('legacy accepted sources retain original timestamps and exact request bytes when a late caption is captured', async t => {
  const f = await fixture(t);
  f.add('Read my balance.'); f.speech(); await f.backend.delegate(f.request(), f.channel);
  const original = structuredClone(f.saved()[0]); delete original.audioThroughMs;
  f.store.update(f.call.id, { delegations: { [original.requestId]: original } });
  f.add(' please.', 1000, 1200);
  const restored = createPrimaryBackend(f.options);
  assert.equal((await restored.captureCompletedTurns(f.channel.getContext(), { sessionFinalized: true, assistantPlaybackComplete: true })).captured, 1);
  assert.equal(f.captures[0].sourceEventId, original.submission.source.sourceEventId);
  assert.equal(f.captures[0].occurredAt, original.submission.source.receivedAt);
  assert.equal(f.captures[0].revision, 2);
  assert.deepEqual(f.saved()[0].submission, original.submission);
  assert.equal(f.saved()[0].audioThroughMs, undefined);
  await restored.delegate(f.request(), f.channel); assert.equal(f.posts.length, 1);
});

test('late captions cannot reorder an accepted source even when caption and acoustic counts match', async t => {
  const f = await fixture(t);
  f.add('Read my balance.', 3000, 3200); f.speech(800, 1000);
  await f.backend.delegate(f.request(), f.channel);
  const original = structuredClone(f.saved()[0]);
  f.add('An earlier caption.', 100, 200); f.speech(4000, 4200);
  const result = await f.backend.captureCompletedTurns(f.channel.getContext(), { sessionFinalized: true, assistantPlaybackComplete: true });
  assert.equal(result.captured, 0); assert.ok(result.unsupported.includes('unqualified_caller_turn'));
  assert.deepEqual(f.saved()[0], original);
});

test('recorded Live phrase includes all six available fragments despite the late caption boundary', async t => {
  const f = await fixture(t);
  for (const fragment of recordedCallerFragments) f.add(fragment.text, fragment.startMs, fragment.endMs, 'user', fragment.eventId);
  f.speech(8340, 10900);
  await f.backend.delegate(f.request(), f.channel);
  assert.equal(f.posts.length, 1);
  assert.equal(f.posts[0].input.text, recordedCallerText);
  assert.equal(f.saved()[0].frozenCallerRevision, 6);
  assert.equal(f.saved()[0].lastSequence, 6);
  assert.deepEqual((f.store.get(f.call.id)!.acousticSpans as Array<{ startMs: number; endMs: number }>).map(span => [span.startMs, span.endMs]), [[8320, 10900]]);
  assert.deepEqual(await f.backend.captureCompletedTurns(f.channel.getContext(), { assistantPlaybackComplete: true, sessionFinalized: true }),
    { captured: 0, pending: 0, unsupported: [] });
  assert.equal(f.captures.length, 0);
});

test('a later caption outside the raw acoustic boundary revises the original source without resubmitting', async t => {
  const f = await fixture(t);
  for (const fragment of recordedCallerFragments.slice(0, 5)) f.add(fragment.text, fragment.startMs, fragment.endMs, 'user', fragment.eventId);
  f.speech(8340, 10900);
  let entered!: () => void;
  const began = new Promise<void>(resolve => { entered = resolve; });
  let release!: () => void;
  const pending = new Promise<void>(resolve => { release = resolve; });
  f.behavior.result = async id => { entered(); await pending; return f.completed(id); };
  const work = f.backend.delegate(f.request(), f.channel);
  await began;
  const last = recordedCallerFragments[5];
  f.add(last.text, last.startMs, last.endMs, 'user', last.eventId);
  release(); await work;
  assert.equal(f.posts.length, 1);
  assert.equal(f.posts[0].input.text, ' Please ask Primary for my test');
  assert.equal(f.captures.length, 1);
  assert.equal(f.captures[0].sourceEventId, f.posts[0].input.source.sourceEventId);
  assert.equal(f.captures[0].revision, 2);
  assert.equal(f.captures[0].text, recordedCallerText);
  assert.equal(f.saved()[0].correctionState, 'recorded_not_applied');
  await f.backend.delegate(f.request('del_repeat'), f.channel);
  assert.equal(f.posts.length, 1);
});

test('closeout recovers a late phrase caption on the saved source after the request completed', async t => {
  const f = await fixture(t);
  for (const fragment of recordedCallerFragments.slice(0, 5)) f.add(fragment.text, fragment.startMs, fragment.endMs, 'user', fragment.eventId);
  f.speech(8340, 10900);
  await f.backend.delegate(f.request(), f.channel);
  const last = recordedCallerFragments[5];
  f.add(last.text, last.startMs, last.endMs, 'user', last.eventId);
  f.store.update(f.call.id, { endedAt: new Date().toISOString() });
  const restored = createPrimaryBackend(f.options);
  const proof = { assistantPlaybackComplete: true, sessionFinalized: true };
  assert.deepEqual(await restored.captureCompletedTurns(f.channel.getContext(), proof), { captured: 1, pending: 0, unsupported: [] });
  assert.equal(f.captures[0].sourceEventId, f.posts[0].input.source.sourceEventId);
  assert.equal(f.captures[0].revision, 2);
  assert.equal(f.captures[0].text, recordedCallerText);
  assert.equal(f.saved()[0].correctionState, 'recorded_not_applied');
  assert.deepEqual(await restored.captureCompletedTurns(f.channel.getContext(), proof), { captured: 0, pending: 0, unsupported: [] });
  assert.equal(f.posts.length, 1);
});

test('a paused caller phrase stays whole across multiple local sound bursts', async t => {
  const f = await fixture(t);
  f.add('Read my balance', 800, 1000); f.speech(800, 1000);
  f.add(' and send it.', 1700, 1900); f.speech(1700, 1900);
  await f.backend.delegate(f.request(), f.channel);
  assert.equal(f.posts.length, 1);
  assert.equal(f.posts[0].input.text, 'Read my balance and send it.');
  const result = await f.backend.captureCompletedTurns(f.channel.getContext(), { assistantPlaybackComplete: true, sessionFinalized: true });
  assert.deepEqual(result, { captured: 0, pending: 0, unsupported: [] });
  assert.equal(f.captures.length, 0);
});

test('the whole stable phrase survives a final caption ending after the delegation position', async t => {
  const f = await fixture(t);
  f.add('And please read my most recent ', 17000, 19000); f.add('payment', 19400, 19600); f.speech(16480, 18840);
  const delegated = { ...f.request(), offsetMs: 19400 };
  await f.backend.delegate(delegated, f.channel);
  assert.equal(f.posts.length, 1); assert.equal(f.posts[0].input.text, 'And please read my most recent payment');
});

test('a later distinct phrase cannot borrow an earlier delegation', async t => {
  const f = await fixture(t);
  f.add('Read my balance.', 800, 1000); f.speech();
  const delegated = f.request();
  f.add('Read my payment.', 3000, 3200); f.speech(3000, 3200);
  await f.backend.delegate(delegated, f.channel);
  assert.deepEqual(f.posts.map(post => post.input.text), ['Read my balance.']);
  await f.backend.delegate(f.request('payment'), f.channel);
  assert.deepEqual(f.posts.map(post => post.input.text), ['Read my balance.', 'Read my payment.']);
});

test('two spoken turns before the first delayed delegation remain two requests', async t => {
  const f = await fixture(t);
  f.add('Read my balance.', 800, 1000); f.speech();
  const firstDelegation = f.request();
  f.add('Read my payment.', 3000, 3200); f.speech(3000, 3200);
  await f.backend.delegate(firstDelegation, f.channel);
  assert.equal(f.saved()[0].audioThroughMs, 3200);
  await f.backend.delegate(f.request('payment'), f.channel);
  assert.equal(f.saved()[1].audioThroughMs, 3200);
  assert.deepEqual(f.posts.map(post => post.input.text), ['Read my balance.', 'Read my payment.']);
});

test('a quiet-time caller completion survives a later hangup click and process restart', async t => {
  const f = await fixture(t);
  f.add('Please remember our conversation.'); f.speech(); f.backend.observeTranscript(f.channel.getContext());
  await wait(15);
  assert.equal(Object.keys(f.store.get(f.call.id)!.callerCompletions ?? {}).length, 1);
  assert.equal(f.captures.length, 0, 'checkpoint must not upload provisional memory');
  f.speech(4000, 4020); f.backend.persistAcousticEvidence();
  f.store.update(f.call.id, { endedAt: new Date().toISOString() });
  const restored = createPrimaryBackend(f.options);
  const result = await restored.captureCompletedTurns(f.channel.getContext(), { sessionFinalized: true, assistantPlaybackComplete: false });
  assert.equal(result.captured, 1); assert.equal(f.captures[0].text, 'Please remember our conversation.');
  assert.equal(f.posts.length, 0);
});

test('earlier quiet speech is captured while unfinished final speech stays unqualified', async t => {
  const f = await fixture(t);
  f.add('Hello there.'); f.speech(); f.backend.observeTranscript(f.channel.getContext()); await wait(15);
  f.add('Let me think', 4000, 4200); f.speech(4000, 4200); f.backend.observeTranscript(f.channel.getContext());
  const endedAt = new Date().toISOString();
  f.backend.persistAcousticEvidence(); f.store.update(f.call.id, { endedAt });
  await wait(15);
  const result = await createPrimaryBackend(f.options).captureCompletedTurns(f.channel.getContext(), { sessionFinalized: true, assistantPlaybackComplete: false });
  assert.equal(result.captured, 1); assert.ok(result.unsupported.includes('unqualified_caller_turn'));
  assert.deepEqual(f.captures.map(turn => turn.text), ['Hello there.']);
  assert.equal(Object.keys(f.store.get(f.call.id)!.callerCompletions ?? {}).length, 1);
});

test('a changed late caption cannot reuse an earlier text completion checkpoint', async t => {
  const f = await fixture(t);
  f.add('Read my balance'); f.speech(); f.backend.observeTranscript(f.channel.getContext()); await wait(15);
  f.add(' and send it.', 1000, 1200); f.speech(1000, 1200); f.backend.observeTranscript(f.channel.getContext());
  const endedAt = new Date().toISOString();
  f.backend.persistAcousticEvidence(); f.store.update(f.call.id, { endedAt });
  await wait(15);
  const result = await createPrimaryBackend(f.options).captureCompletedTurns(f.channel.getContext(), { sessionFinalized: true, assistantPlaybackComplete: false });
  assert.equal(result.captured, 0); assert.ok(result.unsupported.includes('unqualified_caller_turn'));
  assert.equal(f.captures.length, 0);
});

test('a failed optional completion write logs a safe cause without claiming a saved checkpoint', async t => {
  const f = await fixture(t);
  const errors: unknown[][] = []; const log = console.error;
  console.error = (...args: unknown[]) => { errors.push(args); };
  t.after(() => { console.error = log; });
  const backend = createPrimaryBackend({ ...f.options, store: { get: id => f.store.get(id),
    update: () => { throw new Error('private transcript and storage details'); } } });
  f.add('A private caller remark.'); f.speech(800, 1000, backend); backend.observeTranscript(f.channel.getContext());
  await wait(15);
  assert.deepEqual(errors, [['broker_caller_completion_failed', { callId: f.call.id }]]);
  assert.equal(f.store.get(f.call.id)!.callerCompletions, undefined); assert.equal(f.captures.length, 0);
});

test('late captions cannot merge two already accepted sources', async t => {
  const f = await fixture(t);
  f.add('Read my balance.', 800, 1000); f.speech(); await f.backend.delegate(f.request(), f.channel);
  f.add('Read my payment.', 3000, 3200); f.speech(3000, 3200); await f.backend.delegate(f.request('second'), f.channel);
  const originals = f.saved().map(entry => structuredClone(entry.submission));
  f.add(' bridging caption ', 1500, 2400);
  const captured = await f.backend.captureCompletedTurns(f.channel.getContext(), { assistantPlaybackComplete: true, sessionFinalized: true });
  assert.equal(captured.captured, 0); assert.ok(captured.unsupported.includes('unqualified_caller_turn'));
  assert.deepEqual(f.saved().map(entry => entry.submission), originals);
  assert.equal(f.posts.length, 2);
});

test('second speech arriving before first admission keeps its own delegated task', async t => {
  const f = await fixture(t);
  f.add('Read my balance.', 800, 1000); f.speech();
  let entered!: () => void; const began = new Promise<void>(resolve => { entered = resolve; });
  let release!: () => void; const blocked = new Promise<void>(resolve => { release = resolve; });
  f.behavior.result = async id => { entered(); await blocked; return f.completed(id); };
  const first = f.backend.delegate(f.request(), f.channel);
  f.add('Read my last payment.', 3000, 3200); f.speech(3000, 3200);
  const second = f.backend.delegate(f.request('second'), f.channel);
  await began; await wait(12);
  const queued = f.saved()[0].deferredTurns?.[0];
  assert.equal(queued?.status, 'queued'); assert.equal(queued?.text, 'Read my last payment.');
  assert.equal(f.saved()[0].audioThroughMs, 1000); assert.equal(queued?.queuedRequest?.audioThroughMs, 3200);
  release(); await Promise.all([first, second]);
  assert.deepEqual(f.posts.map(post => post.input.text), ['Read my balance.', 'Read my last payment.']);
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

for (const error of [
  new PrimaryApiError('http_error', 503, { code: 'primary_unavailable', retryable: true }),
  new PrimaryApiError('http_error', 504, { code: 'upstream_timeout', retryable: true }),
  new PrimaryApiError('invalid_response', 202),
  new PrimaryApiError('request_timeout'),
]) test(`submit ${error.status ?? error.code} is explained before blocked recovery and remains GET-only`, async t => {
  const f = await fixture(t); f.add('Read my balance.'); f.speech();
  Object.assign(error, { message: 'PRIVATE_RAW_ERROR', body: 'PRIVATE_BODY', stack: 'PRIVATE_STACK' });
  f.behavior.submit = async () => { throw error; };
  let entered!: () => void, release!: () => void, acknowledge!: () => void;
  const began = new Promise<void>(resolve => { entered = resolve; });
  const gate = new Promise<void>(resolve => { release = resolve; });
  const noticeAck = new Promise<void>(resolve => { acknowledge = resolve; });
  const publish = f.channel.publish;
  f.channel.publish = async update => {
    const result = await publish(update);
    if (update.kind === 'progress') await noticeAck;
    return result;
  };
  f.behavior.result = async id => { entered(); await gate; return f.completed(id); };
  const work = f.backend.delegate(f.request(), f.channel);
  try {
    await began;
    assert.equal(f.saved()[0].status, 'dispatch_unknown');
    assert.deepEqual(f.saved()[0].submitError, {
      code: error.code, ...(error.status === undefined ? {} : { status: error.status }),
      ...(error.remoteCode === undefined ? {} : { remoteCode: error.remoteCode }),
      ...(error.retryable === undefined ? {} : { retryable: error.retryable }),
      observedAt: f.saved()[0].submitError!.observedAt,
    });
    assert.ok(Number.isFinite(Date.parse(f.saved()[0].submitError!.observedAt)));
    assert.equal(f.updates.length, 1);
    assert.equal(f.updates[0].kind, 'progress'); assert.equal(f.updates[0].speak, true);
    assert.equal(f.updates[0].expectedCallerRevision, f.saved()[0].frozenCallerRevision);
    assert.match(f.updates[0].text, error.code === 'http_error' ? /returned an error/ : /not confirmed/);
    assert.ok(!JSON.stringify({ saved: f.saved(), speech: f.updates }).includes('PRIVATE_'));
  } finally { release(); acknowledge(); await work; }
  assert.equal(f.posts.length, 1); assert.deepEqual(f.reads, [f.posts[0].requestId]);
  assert.equal(f.saved()[0].status, 'completed');
  assert.equal(f.updates.at(-1)!.text, 'Your balance is $10.25 USDC.');
});

test('first submit diagnostics survive successive read errors, closeout recovery and eventual completion', async t => {
  const f = await fixture(t, { maxPollMs: 10 }); f.add('Read my balance.'); f.speech();
  f.behavior.submit = async () => { throw new PrimaryApiError('http_error', 503, { code: 'primary_unavailable', retryable: true }); };
  f.behavior.result = async () => { throw new PrimaryApiError('http_error', 404, { code: 'request_not_found', retryable: false }); };
  await f.backend.delegate(f.request(), f.channel);
  const original = structuredClone(f.saved()[0]);
  assert.equal(original.status, 'dispatch_unknown');
  assert.equal(original.lastResultReadError!.status, 404);
  assert.equal(original.lastResultReadError!.remoteCode, 'request_not_found');
  const restored = createPrimaryBackend(f.options);
  f.behavior.result = async () => { throw new PrimaryApiError('http_error', 503, { code: 'primary_unavailable', retryable: true }); };
  await restored.flushPendingCaptures();
  assert.deepEqual(f.saved()[0].submitError, original.submitError);
  assert.equal(f.saved()[0].lastResultReadError!.status, 503);
  assert.equal(f.saved()[0].lastResultReadError!.retryable, true);
  f.behavior.result = async id => f.completed(id);
  await restored.delegate(f.request(), f.channel);
  assert.equal(f.saved()[0].status, 'completed');
  assert.equal(f.saved()[0].lastResultReadError!.status, 503); // Retained last failure, not the current outcome.
  assert.deepEqual(f.saved()[0].submitError, original.submitError);
  assert.deepEqual(f.saved()[0].submission, original.submission);
  assert.equal(f.posts.length, 1); assert.ok(f.reads.every(id => id === f.posts[0].requestId));
  assert.equal(f.updates.filter(update => update.id.endsWith('_submit_unknown')).length, 1);
});

test('untyped exceptions cannot put raw error details into the saved request or spoken notice', async t => {
  const f = await fixture(t); f.add('Read my balance.'); f.speech();
  f.behavior.submit = async () => { throw new Error('PRIVATE_SECRET_AND_REQUEST_BODY'); };
  await f.backend.delegate(f.request(), f.channel);
  assert.equal(f.saved()[0].submitError, undefined);
  assert.ok(!JSON.stringify({ saved: f.saved(), speech: f.updates }).includes('PRIVATE_'));
  assert.match(f.updates[0].text, /not confirmed/);
  assert.equal(f.updates.at(-1)!.text, 'Your balance is $10.25 USDC.');
});

test('spoken progress is given once during a blocked result read and is bounded across duplicate delegates', async t => {
  const f = await fixture(t, { progressIntervalMs: 15, maxPollMs: 5000 });
  f.add('Read my balance.'); f.speech();
  let entered!: () => void, release!: () => void;
  const began = new Promise<void>(resolve => { entered = resolve; });
  const gate = new Promise<void>(resolve => { release = resolve; });
  f.behavior.result = async id => { entered(); await gate; return f.completed(id); };
  const work = Promise.all([f.backend.delegate(f.request(), f.channel), f.backend.delegate(f.request(), f.channel)]);
  try {
    await began;
    const end = Date.now() + 5000;
    while (f.updates.filter(update => update.kind === 'progress').length < 1 && Date.now() < end) await wait(10);
    await wait(90); // Keep the blocked read open for several more progress intervals.
    const progress = f.updates.filter(update => update.kind === 'progress');
    assert.equal(progress.length, 1, 'one waiting line; repeats only interrupt the caller');
    assert.equal(progress[0].text, 'Still working on it.');
    assert.ok(progress.every(update => update.speak === true && update.expectedCallerRevision === 1));
    assert.equal(f.saved()[0].status, 'queued');
  } finally { release(); await work; }
  assert.equal(f.posts.length, 1); assert.ok(f.reads.every(id => id === f.posts[0].requestId));
  assert.equal(f.saved()[0].status, 'completed');
  assert.equal(f.updates.at(-1)!.text, 'Your balance is $10.25 USDC.');
});

test('pending status notices are periodic rather than emitted by every result poll', async t => {
  let clock: number | undefined;
  const f = await fixture(t, { progressIntervalMs: 25, pollIntervalMs: 1, watchUntilCallEnds: true,
    now: () => clock ?? Date.now() });
  f.add('Read my balance.'); f.speech();
  // Let the existing quiet/admission gate finish, then freeze the clock before
  // the observer starts. Poll count cannot consume the notice's time budget.
  const submit = f.behavior.submit;
  f.behavior.submit = async id => { clock = Date.now() + 10; return submit(id); };
  f.behavior.result = async id => ({ bindingId, requestId: id, state: 'running' });
  let failure: unknown;
  const work = f.backend.delegate(f.request(), f.channel);
  void work.catch(error => { failure = error; });
  const progress = () => f.updates.filter(update => update.kind === 'progress');
  const until = async (label: string, predicate: () => boolean) => {
    const deadline = Date.now() + 6000;
    while (!predicate()) {
      if (failure) throw failure;
      assert.ok(Date.now() < deadline, `timed out waiting for ${label}; reads=${f.reads.length}, progress=${progress().length}`);
      await wait(5);
    }
  };
  try {
    await until('five initial polls', () => f.reads.length >= 5);
    assert.equal(progress().length, 0, 'polls alone must not produce a periodic notice');
    clock! += 25;
    await until('one due notice', () => progress().length === 1);
    const readsAtNotice = f.reads.length;
    await until('five more polls', () => f.reads.length >= readsAtNotice + 5);
    assert.equal(progress().length, 1, 'more polls at the same time must not repeat the notice');
    assert.ok(progress().length <= 4, 'the notice budget remains bounded');
    assert.ok(progress().every(update => update.speak === true), 'periodic notices must be spoken');
    assert.equal(f.posts.length, 1);
  } finally { f.controller.abort(); await work; }
});

for (const change of ['abort', 'call_ended'] as const)
  test(`blocked recovery suppresses attempted progress after ${change}`, async t => {
    const f = await fixture(t, { progressIntervalMs: 10, maxPollMs: 200 });
    f.add('Read my balance.'); f.speech();
    let entered!: () => void, release!: () => void;
    const began = new Promise<void>(resolve => { entered = resolve; });
    const gate = new Promise<void>(resolve => { release = resolve; });
    f.behavior.result = async id => { entered(); await gate; return f.completed(id); };
    const work = f.backend.delegate(f.request(), f.channel);
    try {
      await began;
      if (change === 'abort') f.controller.abort();
      else f.store.update(f.call.id, { endedAt: new Date().toISOString() });
      await wait(35);
      assert.deepEqual(f.updates, []);
    } finally { release(); await work; }
    assert.equal(f.posts.length, 1);
  });

test('a caller interjection does not repeat the spoken pending status', async t => {
  const f = await fixture(t, { progressIntervalMs: 10, maxPollMs: 500 });
  f.add('Read my portfolio.'); f.speech();
  let entered!: () => void, release!: () => void;
  const began = new Promise<void>(resolve => { entered = resolve; });
  const gate = new Promise<void>(resolve => { release = resolve; });
  f.behavior.result = async id => { entered(); await gate; return f.completed(id); };
  const work = f.backend.delegate(f.request(), f.channel);
  const progress = () => f.updates.filter(value => value.kind === 'progress');
  try {
    await began;
    const end = Date.now() + 2000;
    while (!progress().length && Date.now() < end) await wait(5);
    assert.equal(progress().length, 1, 'the pending request gets one spoken status');
    f.add('Okay, do you need anything else?', 3000, 3200); f.speech(3000, 3200);
    await wait(150); // Many progress intervals after the interjection.
    assert.equal(progress().length, 1, 'caller speech alone must not trigger another waiting line');
    assert.equal(progress()[0].text, 'Still working on it.');
    assert.equal(progress()[0].speak, true);
    assert.equal(f.saved()[0].status, 'queued');
    assert.equal(f.posts.length, 1);
  } finally { release(); await work; }
  assert.equal(f.updates.at(-1)!.text, 'For your earlier request: Your balance is $10.25 USDC.');
  assert.equal(f.posts[0].input.text, 'Read my portfolio.');
  assert.equal(f.posts.length, 1);
});

for (const interjection of ['Okay, do you need anything else?', 'What does repair mean?'])
  test(`completion after "${interjection}" speaks the original portfolio result`, async t => {
    const f = await fixture(t); f.add('Read my portfolio.'); f.speech();
    const answer = 'Your portfolio holds $12,345.67 USDC and 12.345678 shares. The last price was $24.91.';
    f.result(answer);
    let entered!: () => void, release!: () => void;
    const began = new Promise<void>(resolve => { entered = resolve; });
    const gate = new Promise<void>(resolve => { release = resolve; });
    f.behavior.result = async id => { entered(); await gate; return f.completed(id); };
    const work = f.backend.delegate(f.request(), f.channel);
    await began;
    f.add(interjection, 3000, 3200); f.speech(3000, 3200);
    release(); await work;
    const spoken = f.updates.filter(update => update.kind === 'result');
    assert.equal(spoken.map(update => update.text).join(''), `For your earlier request: ${answer}`);
    assert.ok(spoken.every(update => update.speak === true && update.expectedCallerRevision === 2));
    assert.equal(f.saved()[0].result, answer);
    assert.equal(f.saved()[0].delivery, 'accepted');
    assert.equal(f.posts.length, 1);
    assert.equal(f.posts[0].input.text, 'Read my portfolio.');
    assert.equal(f.captures.length, 0);
  });

test('fast completion and a pending Primary question receive no extra progress chatter', async t => {
  for (const state of ['completed', 'waiting_for_reply'] as const) {
    const f = await fixture(t, { progressIntervalMs: 10 }); f.add('Read my balance.'); f.speech();
    if (state === 'waiting_for_reply') f.behavior.result = async id => waiting(id, question());
    await f.backend.delegate(f.request(), f.channel);
    assert.equal(f.updates.filter(update => update.kind === 'progress').length, 0);
    assert.equal(f.saved()[0].status, state);
  }
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
  assert.equal(f.updates.at(-1)!.text, 'For your earlier request: Your balance is $10.25 USDC.');
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
  const before = await readFile(path.join(f.directory, 'calls.json'), 'utf8');
  assert.throws(() => f.store.update(f.call.id, { primary: { bindingId: otherBinding, name: null, context: '' } }), /Primary binding cannot change/);
  assert.equal(await readFile(path.join(f.directory, 'calls.json'), 'utf8'), before);
  const mismatchedBackend = createPrimaryBackend({ ...f.options, bindingId: otherBinding });
  await assert.rejects(mismatchedBackend.delegate(f.request(), f.channel), /identity_mismatch/);
  assert.equal(f.posts.length, 0);
  assert.equal(f.store.get(f.call.id)!.primary!.bindingId, bindingId);
  f.behavior.result = async id => ({ ...f.completed(id), bindingId: otherBinding });
  await f.backend.delegate(f.request(), f.channel);
  assert.equal(f.saved()[0].status, 'queued');
  assert.ok(!f.updates.some(update => update.text.includes('$10.25')));
});

test('long results speak every character and exact amount in bounded commentary chunks', async t => {
  const f = await fixture(t); f.add('Read my portfolio.'); f.speech();
  const text = 'Your balance is $12,345.67 USDC. ' + 'The portfolio includes 12.345678 shares. '.repeat(35);
  const publish = f.channel.publish;
  f.channel.publish = async update => {
    if (update.kind === 'result') assert.equal(f.saved()[0].delivery, 'dispatching');
    return publish(update);
  };
  f.result(text); await f.backend.delegate(f.request(), f.channel);
  const chunks = f.updates.filter(update => update.kind === 'result');
  assert.equal(chunks.map(update => update.text).join(''), text);
  assert.ok(chunks.length > 1);
  assert.ok(chunks.every(update => Buffer.byteLength(update.text) <= 500 && update.speak === true));
  assert.ok(chunks.some(update => update.text.includes('12.345678')));
  assert.equal(f.saved()[0].result, text);
  assert.equal(f.saved()[0].delivery, 'accepted');
});

for (const interjection of [false, true])
  test(`a failed request without an assistant answer makes no review promise (interjection=${interjection})`, async t => {
    const f = await fixture(t); f.add('Read my portfolio.'); f.speech();
    f.behavior.result = async id => {
      if (interjection) { f.add('Do you have it yet?', 3000, 3200); f.speech(3000, 3200); }
      return { bindingId, requestId: id, state: 'failed', error: { code: 'primary_run_failed', retryable: false } };
    };
    await f.backend.delegate(f.request(), f.channel);
    assert.equal(f.updates.at(-1)!.text, `${interjection ? 'For your earlier request: ' : ''}I could not get an answer to that request.`);
    assert.equal(f.updates.at(-1)!.speak, true);
    assert.equal(f.saved()[0].status, 'failed');
    assert.equal(f.saved()[0].primaryResult?.error?.code, 'primary_run_failed');
    assert.equal(f.saved()[0].result, undefined);
    assert.equal(f.posts.length, 1);
  });

for (const interruption of ['abort', 'caller_revision', 'acoustic_speech', 'call_ended', 'rejected'] as const)
  test(`result chunks stop after ${interruption} without losing the saved answer or executing again`, async t => {
    const f = await fixture(t); f.add('Read my portfolio.'); f.speech();
    const answer = 'Your portfolio includes 12.345678 shares. '.repeat(20) + 'Total value: $12,345.67.';
    f.result(answer);
    const publish = f.channel.publish;
    let interrupted = false;
    f.channel.publish = async update => {
      const outcome = await publish(update);
      if (update.kind === 'result' && !interrupted) {
        interrupted = true;
        if (interruption === 'abort') f.controller.abort();
        else if (interruption === 'caller_revision') { f.add('Hold on.', 3000, 3200); f.speech(3000, 3200); }
        else if (interruption === 'acoustic_speech') f.speech(3000, 3200);
        else if (interruption === 'call_ended') f.store.update(f.call.id, { endedAt: new Date().toISOString() });
        else return { ...outcome, status: 'rejected' };
      }
      return outcome;
    };
    await f.backend.delegate(f.request(), f.channel);
    assert.equal(f.updates.filter(update => update.kind === 'result').length, 1);
    assert.equal(f.saved()[0].delivery, interruption === 'abort' || interruption === 'call_ended' ? 'closed'
      : interruption === 'rejected' ? 'rejected' : 'stale');
    assert.equal(f.saved()[0].status, 'completed');
    assert.equal(f.saved()[0].result, answer);
    assert.equal(f.posts.length, 1);
    if (interruption === 'caller_revision') {
      const count = f.updates.length;
      await f.backend.delegate(f.request(), f.channel);
      assert.equal(f.updates.slice(count).map(update => update.text).join(''), `For your earlier request: ${answer}`);
      assert.equal(f.saved()[0].delivery, 'accepted');
      assert.equal(f.posts.length, 1);
    }
  });

test('stable transcript alone cannot establish a completed acoustic caller turn', async t => {
  const f = await fixture(t); f.add('Read my balance.');
  await f.backend.delegate(f.request(), f.channel);
  assert.equal(f.posts.length, 0); assert.equal(f.updates.at(-1)!.speak, false);
  assert.equal(f.updates.at(-1)!.kind, 'progress');
  f.speech(); await f.backend.delegate(f.request('completed'), f.channel);
  assert.equal(f.posts.length, 1); assert.equal(f.posts[0].input.text, 'Read my balance.');
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

test('finalized unknown submission captures original raw source before qualified later transcript revisions', async t => {
  const f = await fixture(t, { maxPollMs: 10 }); f.add(' Read my'); f.speech();
  f.behavior.submit = async () => { throw new Error('submit timeout'); };
  f.behavior.result = async () => { throw new Error('not found yet'); };
  await f.backend.delegate(f.request(), f.channel);
  const original = structuredClone(f.saved()[0].submission);
  f.add(' balance.', 1000, 1200); f.speech(1000, 1200);
  await wait(8);
  f.backend.persistAcousticEvidence();
  const proof = { assistantPlaybackComplete: false, sessionFinalized: true };
  f.store.update(f.call.id, { status: 'completed', endedAt: new Date().toISOString(), finalized: true,
    transcriptCapture: { state: 'pending', pending: 1, unsupported: [], ...proof } });
  f.api.reconcile = async () => { throw new Error('not yet closed'); };
  const ordinaryCapture = f.api.captureTurn;
  f.api.captureTurn = async (binding, turn) => ({ ...await ordinaryCapture(binding, turn),
    revisionRecordId: '77777777-7777-4777-8777-777777777777', bodyDigest: turnBodyDigest(turn) });
  const restored = createPrimaryBackend(f.options);
  await restored.prepareCloseout();
  const captured = await restored.captureCompletedTurns(f.channel.getContext(), proof);
  assert.equal(captured.captured, 1);
  assert.equal(restored.closeoutPending(), 0);
  assert.deepEqual(f.captures.map(turn => [turn.revision, turn.text]), [[1, ' Read my'], [2, ' Read my balance.']]);
  assert.deepEqual(f.saved()[0].submission, original);
  assert.equal(f.saved()[0].originalCapture?.turn.text, ' Read my');
  assert.equal(f.posts.length, 1);
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

test('the interrupted caller request is submitted verbatim after the active result without caller repetition', async t => {
  const f = await fixture(t); f.add('Read my balance.'); f.speech();
  let entered!: () => void; const began = new Promise<void>(resolve => { entered = resolve; });
  let release!: () => void; const blocked = new Promise<void>(resolve => { release = resolve; });
  f.behavior.result = async id => { entered(); await blocked; return f.completed(id); };
  const first = f.backend.delegate(f.request(), f.channel); await began;
  const followup = ' No, answer my question first. Do you- you don\'t even have that included in the data that got returned, before you go pull anything? And you can pull it if you want, but I want both of those things';
  f.add(followup, 3000, 3200); f.speech(3000, 3200);
  const second = f.backend.delegate(f.request('del_followup'), f.channel);
  await wait(12);
  const queued = structuredClone(f.saved()[0].deferredTurns![0]);
  assert.equal(queued.text, followup);
  assert.equal(queued.status, 'queued');
  assert.equal(queued.queuedRequest!.audioThroughMs, 3200);
  assert.equal(f.posts.length, 1);
  release(); await Promise.all([first, second]);
  assert.equal(f.posts.length, 2);
  assert.deepEqual(f.posts[1].input, queued.queuedRequest!.submission);
  assert.equal(f.posts[1].input.text, followup);
  assert.equal(f.posts[1].requestId, queued.queuedRequest!.requestId);
  assert.equal(f.posts[1].input.source.receivedAt, new Date(Date.parse(f.call.createdAt) + 3000).toISOString());
  assert.equal(f.saved()[1].predecessorRequestId, f.posts[0].requestId);
  assert.equal(f.saved()[1].audioThroughMs, queued.queuedRequest!.audioThroughMs);
  assert.equal(f.posts[0].input.text, 'Read my balance.');
  assert.equal(f.updates.at(-1)!.text, 'Your balance is $10.25 USDC.');
  assert.equal(f.updates.at(-1)!.speak, true);
  assert.equal(f.saved()[0].deferredTurns?.[0].status, 'promoted');
  await f.backend.delegate(f.request('del_repeat_no_new_speech'), f.channel);
  assert.equal(f.posts.length, 2);
  f.add('Please read my last payment now.', 5000, 5200); f.speech(5000, 5200);
  await f.backend.delegate(f.request('del_fresh'), f.channel);
  assert.equal(f.posts.length, 3); assert.equal(f.posts[2].input.text, 'Please read my last payment now.');
});

async function until(check: () => boolean) {
  const deadline = Date.now() + 1000;
  while (!check()) {
    if (Date.now() >= deadline) throw new Error('fixture condition timed out');
    await wait(2);
  }
}

async function blockedQueueFixture(t: TestContext) {
  const f = await fixture(t, { maxPollMs: 50 });
  let release!: () => void;
  const blocked = new Promise<void>(resolve => { release = resolve; });
  let entered = false;
  f.behavior.result = async id => {
    if (id === f.posts[0].requestId) { entered = true; await blocked; }
    return f.completed(id);
  };
  f.add(' Read my portfolio.'); f.speech();
  const first = f.backend.delegate(f.request(), f.channel);
  await until(() => entered);
  f.add(' Read my last payment. ', 3000, 3200); f.speech(3000, 3200);
  const second = f.backend.delegate(f.request('del_followup'), f.channel);
  await until(() => f.saved()[0].deferredTurns?.length === 1);
  return { ...f, first, second, release };
}

test('concurrent duplicate delegates drain one frozen child in the shared call worker', async t => {
  const f = await blockedQueueFixture(t);
  const duplicate = f.backend.delegate(f.request('del_followup'), f.channel);
  const overlapping = createPrimaryBackend(f.options).delegate(f.request('del_overlap'), f.channel);
  f.release(); await Promise.all([f.first, f.second, duplicate, overlapping]);
  assert.equal(f.posts.length, 2);
  assert.equal(f.saved()[0].deferredTurns!.length, 1);
  assert.equal(f.saved()[1].predecessorRequestId, f.saved()[0].requestId);
  assert.equal(f.updates.filter(update => update.kind === 'result').length, 2);
});

test('queued follow-up freezes its accepted publication before promotion and unknown recovery', async t => {
  const f = await fixture(t, { maxPollMs: 40, progressIntervalMs: 5 });
  let release!: () => void;
  const blocked = new Promise<void>(resolve => { release = resolve; });
  f.behavior.result = async id => {
    if (id === f.posts[0].requestId) { await blocked; return f.completed(id); }
    throw new PrimaryApiError('request_timeout');
  };
  f.add(' Read my portfolio.'); f.speech();
  const first = f.backend.delegate(f.request(), f.channel);
  await until(() => Boolean(f.store.get(f.call.id)!.publications?.records.some(record => record.outcome?.status === 'accepted')));
  const originalRequestId = f.posts[0].requestId;
  f.add(' Then review it.\t', 3000, 3200); f.speech(3000, 3200);
  const second = f.backend.delegate(f.request('queued_review'), f.channel);
  await until(() => f.saved()[0].deferredTurns?.length === 1);
  const queued = structuredClone(f.saved()[0].deferredTurns![0].queuedRequest!);
  assert.equal(queued.submission.relatedPrimaryRequestId, originalRequestId);
  assert.equal(queued.submission.referenceOrigin, 'broker_status_publication');
  assert.equal(queued.submission.text, ' Then review it.\t');
  assert.equal(f.posts.length, 1);
  const publication = f.store.get(f.call.id)!.publications!.records.find(record => record.outcome?.status === 'accepted')!;
  rejectBrokerPublication(f.store, publication, publication.outcome!.eventId, 'late_rejection');
  f.behavior.submit = async () => { throw new PrimaryApiError('request_timeout'); };
  release(); await Promise.all([first, second]);
  assert.equal(f.posts.length, 2);
  assert.deepEqual(f.posts[1].input, queued.submission, 'promotion must use the original caller frontier');
  assert.equal(f.posts[1].requestId, queued.requestId);
  assert.equal(f.saved()[1].status, 'dispatch_unknown');
  f.store.close();
  const restored = new BrokerStore(f.directory); t.after(() => restored.close());
  f.behavior.result = async id => f.completed(id);
  await createPrimaryBackend({ ...f.options, store: restored }).delegate(f.request('queued_review'), f.channel);
  assert.equal(f.posts.length, 2, 'recovery must not resubmit the queued source');
  const recovered = restored.get(f.call.id)!.delegations[queued.requestId] as BrokerDelegationRecord;
  assert.deepEqual(recovered.submission, queued.submission);
  assert.equal(recovered.status, 'completed');
});

test('live call observes a slow predecessor beyond the initial window and drains its queued task', async t => {
  const f = await fixture(t, { maxPollMs: 15, watchUntilCallEnds: true });
  let ready = false;
  f.behavior.result = async id => ready || id !== f.posts[0].requestId
    ? f.completed(id) : { bindingId, requestId: id, state: 'running' };
  f.add('Read my portfolio.'); f.speech();
  const first = f.backend.delegate(f.request(), f.channel);
  await until(() => f.posts.length === 1);
  f.add('Then read my last payment.', 3000, 3200); f.speech(3000, 3200);
  const second = f.backend.delegate(f.request('slow_followup'), f.channel);
  await until(() => f.saved()[0].deferredTurns?.length === 1);
  await wait(45);
  const previousReads = f.reads.length;
  await wait(20);
  assert.ok(f.reads.length > previousReads, 'GET observation must continue while the call remains open');
  assert.equal(f.posts.length, 1);
  ready = true;
  await Promise.all([first, second]);
  assert.equal(f.posts.length, 2);
  assert.equal(f.posts[1].input.text, 'Then read my last payment.');
  assert.deepEqual(f.saved().map(entry => entry.status), ['completed', 'completed']);
});

test('live call observer stops on media abort and never dispatches the remaining queue', async t => {
  const f = await fixture(t, { maxPollMs: 10, watchUntilCallEnds: true });
  f.behavior.result = async id => ({ bindingId, requestId: id, state: 'running' });
  f.add('Read my portfolio.'); f.speech();
  const first = f.backend.delegate(f.request(), f.channel);
  await until(() => f.posts.length === 1);
  f.add('Read my last payment.', 3000, 3200); f.speech(3000, 3200);
  const second = f.backend.delegate(f.request('abort_followup'), f.channel);
  await until(() => f.saved()[0].deferredTurns?.length === 1);
  await wait(30);
  f.controller.abort();
  await Promise.all([first, second]);
  const reads = f.reads.length;
  await wait(20);
  assert.equal(f.reads.length, reads);
  assert.equal(f.posts.length, 1);
  assert.equal(f.saved()[0].deferredTurns![0].status, 'queued');
});

test('assigned call freezes its reference alongside the queued publication and preserves raw speech', async t => {
  const f = await fixture(t, { maxPollMs: 40, progressIntervalMs: 5 }, true);
  let release!: () => void;
  const blocked = new Promise<void>(resolve => { release = resolve; });
  f.behavior.result = async id => {
    if (id === f.posts[0].requestId) await blocked;
    return f.completed(id);
  };
  f.add(' Hello.\t'); f.speech();
  const first = f.backend.delegate(f.request(), f.channel);
  await until(() => Boolean(f.store.get(f.call.id)!.publications?.records.some(record => record.outcome?.status === 'accepted')));
  const assignmentRef = f.store.get(f.call.id)!.primary!.callAssignmentRef;
  assert.deepEqual(f.posts[0].input.callAssignmentRef, assignmentRef);
  assert.equal(f.posts[0].input.text, ' Hello.\t');
  f.add(' Then review it.', 3000, 3200); f.speech(3000, 3200);
  const second = f.backend.delegate(f.request('assigned_followup'), f.channel);
  await until(() => f.saved()[0].deferredTurns?.length === 1);
  const queued = structuredClone(f.saved()[0].deferredTurns![0].queuedRequest!.submission);
  assert.deepEqual(queued.callAssignmentRef, assignmentRef);
  assert.equal(queued.relatedPrimaryRequestId, f.posts[0].requestId);
  assert.equal(queued.referenceOrigin, 'broker_status_publication');
  assert.equal(queued.text, ' Then review it.');
  release(); await Promise.all([first, second]);
  assert.equal(f.posts.length, 2);
  assert.deepEqual(f.posts[1].input, queued);
  assert.equal('purpose' in f.posts[0].input, false);
});

test('a known assignment without its bound registration reference refuses before caller submission', async t => {
  const f = await fixture(t, {}, true);
  const backend = createPrimaryBackend({ ...f.options, store: {
    get: id => {
      const call = f.store.get(id)!;
      delete call.primary!.callAssignmentRef;
      return call;
    },
    update: f.store.update.bind(f.store),
  } });
  f.add('Hello.'); f.speech(800, 1000, backend);
  await assert.rejects(backend.delegate(f.request(), f.channel), /call_assignment_binding_mismatch/);
  assert.equal(f.posts.length, 0);
  assert.equal(f.saved().length, 0);
});

test('multiple completed deferred turns retain FIFO source order and predecessor identities', async t => {
  const f = await blockedQueueFixture(t);
  f.add(' Read the payment before that. ', 5000, 5200); f.speech(5000, 5200);
  const third = f.backend.delegate(f.request('del_third'), f.channel);
  await until(() => f.saved()[0].deferredTurns?.length === 2);
  const queued = structuredClone(f.saved()[0].deferredTurns!);
  f.release(); await Promise.all([f.first, f.second, third]);
  assert.deepEqual(f.posts.map(post => post.input.text), [' Read my portfolio.', ' Read my last payment. ', ' Read the payment before that. ']);
  assert.deepEqual(f.posts.slice(1).map(post => post.input), queued.map(turn => turn.queuedRequest!.submission));
  assert.equal(f.saved()[1].predecessorRequestId, f.posts[0].requestId);
  assert.equal(f.saved()[2].predecessorRequestId, f.posts[1].requestId);
  assert.deepEqual(f.saved()[0].deferredTurns!.map(turn => turn.status), ['promoted', 'promoted']);
});

test('a disk-restored queue resumes the frozen request after predecessor recovery without new speech', async t => {
  const f = await blockedQueueFixture(t);
  const queued = structuredClone(f.saved()[0].deferredTurns![0]);
  f.controller.abort(); f.release(); await Promise.all([f.first, f.second]);
  assert.equal(f.posts.length, 1);
  f.store.close();
  const restored = new BrokerStore(f.directory); t.after(() => restored.close());
  const channel: LiveBackendChannel = { ...f.channel, signal: new AbortController().signal,
    publish: async update => ({ eventId: update.id, status: 'accepted' }) };
  const backend = createPrimaryBackend({ ...f.options, store: restored });
  await backend.delegate(f.request('del_followup'), channel);
  assert.equal(f.posts.length, 2);
  assert.deepEqual(f.posts[1], { bindingId, requestId: queued.queuedRequest!.requestId, input: queued.queuedRequest!.submission });
  const saved = Object.values(restored.get(f.call.id)!.delegations) as BrokerDelegationRecord[];
  assert.equal(saved[1].predecessorRequestId, saved[0].requestId);
  await backend.delegate(f.request('del_followup'), channel);
  assert.equal(f.posts.length, 2);
});

test('an uncertain deferred POST is recovered with the exact request ID and never submitted again', async t => {
  const f = await blockedQueueFixture(t);
  const originalResult = f.behavior.result;
  f.behavior.submit = async id => { throw new Error(`synthetic unknown ${id}`); };
  f.behavior.result = async id => {
    if (id === f.posts[0].requestId) return originalResult(id);
    throw new Error('synthetic result unavailable');
  };
  f.release(); await Promise.all([f.first, f.second]);
  assert.equal(f.posts.length, 2);
  assert.equal(f.saved()[1].status, 'dispatch_unknown');
  const frozen = structuredClone(f.posts[1]);
  f.store.close();
  const restored = new BrokerStore(f.directory); t.after(() => restored.close());
  f.behavior.result = async id => f.completed(id);
  const backend = createPrimaryBackend({ ...f.options, store: restored });
  await backend.delegate(f.request('del_followup'), f.channel);
  assert.equal(f.posts.length, 2);
  assert.equal(f.reads.at(-1), frozen.requestId);
  assert.deepEqual((restored.get(f.call.id)!.delegations[frozen.requestId] as BrokerDelegationRecord).submission, frozen.input);
});

test('hangup preserves queued speech but closeout and restart cannot submit it', async t => {
  const f = await blockedQueueFixture(t);
  f.store.update(f.call.id, { status: 'completed', endedAt: new Date().toISOString() });
  f.release(); await Promise.all([f.first, f.second]);
  assert.equal(f.posts.length, 1);
  assert.equal(f.saved()[0].deferredTurns![0].status, 'queued');
  await createPrimaryBackend(f.options).flushPendingCaptures();
  await createPrimaryBackend(f.options).delegate(f.request('del_followup'), f.channel);
  assert.equal(f.posts.length, 1);
});

test('unknown predecessor admission blocks the queue until a GET confirms its terminal result', async t => {
  const f = await fixture(t, { maxPollMs: 35 });
  f.behavior.submit = async () => { throw new Error('synthetic submit timeout'); };
  f.behavior.result = async () => { throw new Error('synthetic unknown admission'); };
  f.add(' Read my portfolio.'); f.speech();
  const first = f.backend.delegate(f.request(), f.channel);
  await until(() => f.posts.length === 1);
  f.add(' Read my last payment. ', 3000, 3200); f.speech(3000, 3200);
  const second = f.backend.delegate(f.request('del_followup'), f.channel);
  await Promise.all([first, second]);
  assert.equal(f.posts.length, 1);
  const queued = structuredClone(f.saved()[0].deferredTurns![0]);
  assert.equal(queued.status, 'queued');
  f.behavior.submit = async id => ({ ...f.completed(id), replayed: false });
  f.behavior.result = async id => f.completed(id);
  await createPrimaryBackend(f.options).delegate(f.request('del_followup'), f.channel);
  assert.equal(f.posts.length, 2);
  assert.deepEqual(f.posts[1].input, queued.queuedRequest!.submission);
  assert.ok(f.reads.includes(f.posts[0].requestId));
});

test('interrupted result presentation preserves and processes the separately queued request once', async t => {
  const f = await blockedQueueFixture(t);
  const publish = f.channel.publish;
  let interrupted = false;
  f.channel.publish = async update => {
    if (update.kind === 'result' && !interrupted) {
      interrupted = true; f.add('Wait.', 5000, 5200); f.speech(5000, 5200);
    }
    return publish(update);
  };
  f.release(); await Promise.all([f.first, f.second]);
  assert.equal(f.posts.length, 2);
  assert.equal(f.saved()[0].delivery, 'stale');
  assert.equal(f.posts[1].input.text, ' Read my last payment. ');
  assert.equal(f.saved()[1].status, 'completed');
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

const firstQuestionId = '55555555-5555-5555-a555-555555555555';
const secondQuestionId = '66666666-6666-5666-a666-666666666666';
const question = (id = firstQuestionId, overrides: Partial<PrimaryQuestion> = {}): PrimaryQuestion => ({
  id, revision: 1, expiresAt: new Date(Date.now() + 60_000).toISOString(),
  runState: 'waiting_for_reply', runId: '77777777-7777-5777-a777-777777777777',
  threadId: '88888888-8888-5888-a888-888888888888', agent: { id: '99999999-9999-5999-a999-999999999999', name: 'Dexter' },
  question: 'Would you like to buy $10.25 of this stock?', context: 'This continues the purchase you requested.',
  proposedAnswer: 'Buy $10.25.', yesMeaning: 'Continue with the $10.25 purchase.', noMeaning: 'Keep the current position.',
  state: 'pending', createdAt: new Date(Date.now() - 1_000).toISOString(), reply: null, ...overrides,
});
const questionResult = (requestId: string, value: PrimaryQuestion): PrimaryQuestionResult => ({
  requestId, bindingId, state: value.runState, question: structuredClone(value),
});
const waiting = (requestId: string, value: PrimaryQuestion): PrimaryResult => ({
  requestId, bindingId, state: 'waiting_for_reply', question: structuredClone(value),
});
const answered = (value: PrimaryQuestion, input: PrimaryReplyInput): PrimaryQuestion => ({
  ...value, state: 'answered', runState: 'running', reply: { ...input.reply,
    interpretedAnswer: input.reply.answer === 'yes' ? value.yesMeaning : input.reply.answer === 'no' ? value.noMeaning : input.reply.text!,
    receivedAt: new Date().toISOString(),
  },
});

async function waitingFixture(t: TestContext, value = question()) {
  const f = await fixture(t);
  let current = value;
  f.behavior.result = async id => current.state === 'pending' ? waiting(id, current) : f.completed(id);
  f.behavior.question = async (id, questionId) => {
    assert.equal(questionId, current.id);
    return questionResult(id, current);
  };
  f.behavior.reply = async (id, questionId, input) => {
    assert.equal(questionId, current.id);
    current = answered(current, input);
    return { ...questionResult(id, current), replayed: false };
  };
  f.add('Please buy $10.25 of this stock.'); f.speech();
  await f.backend.delegate(f.request(), f.channel);
  return { ...f, currentQuestion: () => current, setQuestion: (next: PrimaryQuestion) => { current = next; } };
}

test('a presented question takes a literal reply on the original run and captures its source once', async t => {
  const f = await waitingFixture(t);
  assert.equal(f.saved()[0].status, 'waiting_for_reply');
  assert.equal(f.saved()[0].questions![firstQuestionId].presentation!.callerRevision, 1);
  const presentation = f.updates.map(update => update.text).join('');
  for (const field of ['question', 'context', 'proposedAnswer', 'yesMeaning', 'noMeaning'] as const)
    assert.ok(presentation.includes(f.currentQuestion()[field]));
  f.add('Yes.', 3000, 3200); f.speech(3000, 3200);
  await f.backend.delegate(f.request('answer'), f.channel);
  assert.equal(f.posts.length, 1); assert.equal(f.replies.length, 1);
  assert.equal(f.replies[0].requestId, f.posts[0].requestId);
  assert.equal(f.replies[0].questionId, firstQuestionId);
  assert.equal(f.replies[0].input.text, 'Yes.');
  assert.equal(f.replies[0].input.reply.answer, 'yes'); assert.equal(f.replies[0].input.reply.text, null);
  assert.equal(f.saved()[0].status, 'completed');
  assert.equal(f.updates.at(-1)!.text, 'Your balance is $10.25 USDC.');
  assert.deepEqual(await f.backend.captureCompletedTurns(f.channel.getContext(), { assistantPlaybackComplete: true, sessionFinalized: true }),
    { captured: 0, pending: 0, unsupported: [] });
  assert.equal(f.captures.length, 0);
  await f.backend.delegate(f.request('duplicate_answer'), f.channel);
  assert.equal(f.posts.length, 1); assert.equal(f.replies.length, 1);
});

test('live observer keeps the original question presentation while caller speech is awaiting admission', async t => {
  const f = await fixture(t, { maxPollMs: 10, watchUntilCallEnds: true });
  let current = question();
  f.behavior.result = async id => current.state === 'pending' ? waiting(id, current) : f.completed(id);
  f.behavior.question = async id => questionResult(id, current);
  f.behavior.reply = async (id, _questionId, input) => {
    current = answered(current, input);
    return { ...questionResult(id, current), replayed: false };
  };
  f.add('Please buy $10.25 of this stock.'); f.speech();
  const first = f.backend.delegate(f.request(), f.channel);
  await until(() => Boolean(f.saved()[0]?.questions?.[firstQuestionId].presentation));
  const presentation = structuredClone(f.saved()[0].questions![firstQuestionId].presentation!);
  const prompts = f.updates.length;
  f.add('Yes.', 3000, 3200); f.speech(3000, 3200);
  await wait(30);
  assert.deepEqual(f.saved()[0].questions![firstQuestionId].presentation, presentation);
  assert.equal(f.updates.length, prompts, 'polling must not re-present the question against the answer frontier');
  const reply = f.backend.delegate(f.request('live_answer'), f.channel);
  await Promise.all([first, reply]);
  assert.equal(f.posts.length, 1);
  assert.equal(f.replies.length, 1);
  assert.equal(f.replies[0].requestId, f.posts[0].requestId);
  assert.equal(f.replies[0].input.reply.answer, 'yes');
  assert.equal(f.saved()[0].status, 'completed');
});

test('a queued second task waits for a same-run question answer and follows the original terminal result', async t => {
  const f = await fixture(t);
  let current = question();
  let release!: () => void;
  const blocked = new Promise<void>(resolve => { release = resolve; });
  let entered = false;
  f.behavior.result = async id => {
    if (id !== f.posts[0].requestId) return f.completed(id);
    entered = true; await blocked;
    return current.state === 'pending' ? waiting(id, current) : f.completed(id);
  };
  f.behavior.question = async id => questionResult(id, current);
  f.behavior.reply = async (id, _questionId, input) => {
    current = answered(current, input);
    return { ...questionResult(id, current), replayed: false };
  };
  f.add('Read my balance.'); f.speech();
  const first = f.backend.delegate(f.request(), f.channel);
  await until(() => entered);
  f.add('Read my last payment.', 3000, 3200); f.speech(3000, 3200);
  const second = f.backend.delegate(f.request('del_followup'), f.channel);
  await until(() => f.saved()[0].deferredTurns?.length === 1);
  release(); await Promise.all([first, second]);
  assert.equal(f.posts.length, 1);
  assert.equal(f.saved()[0].status, 'waiting_for_reply');
  assert.equal(f.saved()[0].deferredTurns![0].status, 'queued');
  f.add('Yes.', 5000, 5200); f.speech(5000, 5200);
  await f.backend.delegate(f.request('answer'), f.channel);
  assert.equal(f.replies.length, 1);
  assert.equal(f.replies[0].requestId, f.posts[0].requestId);
  assert.equal(f.posts.length, 2);
  assert.equal(f.posts[1].input.text, 'Read my last payment.');
  assert.equal(f.saved()[1].predecessorRequestId, f.posts[0].requestId);
  assert.equal(f.saved()[0].deferredTurns!.length, 1);
});

test('literal reply mapping preserves canonical yes and no meanings and leaves qualified speech as other', () => {
  for (const text of ['yes', ' Yes. ', 'YES!']) assert.deepEqual(literalPhoneAnswer(text), { answer: 'yes', text: null });
  for (const text of ['no', ' No. ', 'NO!']) assert.deepEqual(literalPhoneAnswer(text), { answer: 'no', text: null });
  for (const text of ['yes?', 'yes, but buy $20 instead', 'no thanks', 'sure', 'I guess yes'])
    assert.deepEqual(literalPhoneAnswer(text), { answer: 'other', text });
  assert.throws(() => literalPhoneAnswer('é'.repeat(1001)), /answer_too_long/);
});

test('a no reply carries no even when the canonical no meaning continues the request', async t => {
  const f = await waitingFixture(t, question(firstQuestionId, {
    question: 'Would you like to stop this purchase?', yesMeaning: 'Stop the purchase.', noMeaning: 'Continue buying $10.25.',
  }));
  f.add('No!', 3000, 3200); f.speech(3000, 3200);
  await f.backend.delegate(f.request('no_answer'), f.channel);
  assert.equal(f.replies[0].input.reply.answer, 'no');
  assert.equal(f.currentQuestion().reply!.interpretedAnswer, 'Continue buying $10.25.');
  assert.equal(f.posts.length, 1);
});

test('qualified caller text is preserved as other without inventing an approval', async t => {
  const f = await waitingFixture(t);
  const text = 'Yes, but buy $20 instead.';
  f.add(text, 3000, 3200); f.speech(3000, 3200);
  await f.backend.delegate(f.request('qualified_answer'), f.channel);
  assert.equal(f.replies[0].input.text, text);
  assert.deepEqual(f.replies[0].input.reply, { questionRevision: 1,
    clientReplyId: f.replies[0].input.reply.clientReplyId, answer: 'other', text });
  assert.equal(f.posts.length, 1);
});

test('a lost reply response is recovered through the exact old question after a later question appears', async t => {
  const f = await waitingFixture(t);
  const old = f.currentQuestion();
  const next = question(secondQuestionId, { question: 'Would you also like the receipt read aloud?' });
  let accepted: PrimaryQuestion | undefined;
  f.behavior.reply = async (_id, questionId, input) => {
    assert.equal(questionId, firstQuestionId);
    accepted = answered(old, input);
    throw new Error('reply response lost');
  };
  f.behavior.question = async (id, questionId) => {
    assert.equal(questionId, firstQuestionId);
    return questionResult(id, accepted ? { ...accepted, runState: 'waiting_for_reply' } : old);
  };
  f.behavior.result = async id => waiting(id, accepted ? next : old);
  f.add('Yes.', 3000, 3200); f.speech(3000, 3200);
  await f.backend.delegate(f.request('answer'), f.channel);
  assert.equal(f.saved()[0].questions![firstQuestionId].reply!.status, 'accepted');
  assert.ok(f.saved()[0].questions![secondQuestionId].presentation);
  assert.equal(f.replies.length, 1); assert.equal(f.posts.length, 1);
  assert.ok(f.questionReads.every(read => read.questionId === firstQuestionId));
  assert.ok(f.questionReads.every(read => read.requestId === f.posts[0].requestId));
});

test('media abort during question recovery prevents a new result GET after that recovery settles', async t => {
  const f = await waitingFixture(t);
  f.behavior.reply = async () => { throw new PrimaryApiError('request_timeout'); };
  f.behavior.question = async id => questionResult(id, f.currentQuestion());
  f.add('Yes.', 3000, 3200); f.speech(3000, 3200);
  await f.backend.delegate(f.request('unconfirmed_answer'), f.channel);
  assert.equal(f.saved()[0].questions![firstQuestionId].reply!.status, 'unknown');
  let entered = false;
  let release!: () => void;
  const blocked = new Promise<void>(resolve => { release = resolve; });
  f.behavior.question = async id => { entered = true; await blocked; return questionResult(id, f.currentQuestion()); };
  const resumed = createPrimaryBackend({ ...f.options, watchUntilCallEnds: true })
    .delegate(f.request('unconfirmed_answer'), f.channel);
  await until(() => entered);
  const reads = f.reads.length;
  f.controller.abort(); release();
  await resumed;
  assert.equal(f.reads.length, reads);
  assert.equal(f.posts.length, 1);
  assert.equal(f.replies.length, 1);
});

test('two sequential questions use separate reply identities and one original Primary run', async t => {
  const f = await waitingFixture(t);
  const first = f.currentQuestion();
  const next = question(secondQuestionId, { question: 'Should I read the saved receipt?' });
  let step = 0;
  const questions: Record<string, PrimaryQuestion> = { [firstQuestionId]: first, [secondQuestionId]: next };
  f.behavior.question = async (id, questionId) => questionResult(id, questions[questionId]);
  f.behavior.reply = async (id, questionId, input) => {
    questions[questionId] = answered(questions[questionId], input); step++;
    return { ...questionResult(id, questions[questionId]), replayed: false };
  };
  f.behavior.result = async id => step < 2 ? waiting(id, questions[step ? secondQuestionId : firstQuestionId]) : f.completed(id);
  f.add('Yes.', 3000, 3200); f.speech(3000, 3200);
  await f.backend.delegate(f.request('answer_one'), f.channel);
  assert.equal(f.saved()[0].currentQuestionId, secondQuestionId);
  f.add('No.', 5000, 5200); f.speech(5000, 5200);
  await f.backend.delegate(f.request('answer_two'), f.channel);
  assert.equal(f.posts.length, 1); assert.equal(f.replies.length, 2);
  assert.equal(f.replies[0].requestId, f.replies[1].requestId);
  assert.notEqual(f.replies[0].input.reply.clientReplyId, f.replies[1].input.reply.clientReplyId);
  assert.notEqual(f.replies[0].input.source.sourceEventId, f.replies[1].input.source.sourceEventId);
  assert.equal(f.saved()[0].status, 'completed');
  assert.equal((await f.backend.captureCompletedTurns(f.channel.getContext(), { assistantPlaybackComplete: true, sessionFinalized: true })).captured, 0);
  assert.equal(f.captures.length, 0);
});

test('reply recovery stays pending across call end until the exact client reply is observed', async t => {
  const f = await waitingFixture(t);
  const original = f.currentQuestion();
  let accepted: PrimaryQuestion | undefined;
  f.behavior.reply = async () => { throw new Error('reply outcome unknown'); };
  f.behavior.question = async id => questionResult(id, accepted ?? original);
  f.add('Yes.', 3000, 3200); f.speech(3000, 3200);
  await f.backend.delegate(f.request('answer_unknown'), f.channel);
  const input = structuredClone(f.replies[0].input);
  f.store.update(f.call.id, { endedAt: new Date().toISOString() });
  const restored = createPrimaryBackend(f.options);
  assert.deepEqual(await restored.flushPendingCaptures(), { pending: 1, unsupported: ['reply_outcome_unknown'] });
  assert.equal(restored.closeoutPending(), 1);
  const capture = await restored.captureCompletedTurns(f.channel.getContext(), { assistantPlaybackComplete: true, sessionFinalized: true });
  assert.equal(capture.pending, 1); assert.ok(capture.unsupported.includes('reply_outcome_unknown'));
  assert.equal(f.captures.length, 0);
  accepted = answered(original, input);
  assert.deepEqual(await restored.flushPendingCaptures(), { pending: 0, unsupported: [] });
  assert.equal(restored.closeoutPending(), 0);
  assert.equal(f.saved()[0].questions![firstQuestionId].reply!.status, 'accepted');
  assert.equal(f.store.get(f.call.id)!.canonicalTurns![input.source.sourceEventId] &&
    (f.store.get(f.call.id)!.canonicalTurns![input.source.sourceEventId] as { text: string }).text, 'Yes.');
  assert.equal(f.replies.length, 1); assert.equal(f.posts.length, 1); assert.equal(f.captures.length, 0);
});

test('caller speech arriving before the complete question presentation cannot answer it', async t => {
  const f = await fixture(t);
  const value = question();
  f.behavior.result = async id => waiting(id, value);
  f.behavior.question = async id => questionResult(id, value);
  const originalPublish = f.channel.publish;
  let interrupt = true;
  f.channel.publish = async update => {
    if (interrupt) { interrupt = false; f.add('Yes.', 3000, 3200); f.speech(3000, 3200); }
    return originalPublish(update);
  };
  f.add('Buy the stock.'); f.speech();
  await f.backend.delegate(f.request(), f.channel);
  assert.equal(f.saved()[0].questions![firstQuestionId].presentation, undefined);
  await f.backend.delegate(f.request('early_yes'), f.channel);
  assert.equal(f.replies.length, 0);
  assert.equal(f.posts.length, 1);
  assert.equal(f.saved()[0].questions![firstQuestionId].presentation!.callerRevision, 2);
  assert.equal(f.saved()[0].deferredTurns![0].text, 'Yes.');
  assert.equal(f.saved()[0].deferredTurns![0].status, 'recorded_not_submitted');
  assert.equal(f.saved()[0].deferredTurns![0].queuedRequest, undefined);
  f.behavior.result = async id => f.completed(id);
  await f.backend.delegate(f.request('original_finished'), f.channel);
  assert.equal(f.posts.length, 1);
  assert.equal(f.replies.length, 0);
});

test('an overlong answer asks for a shorter reply and stays on the waiting request', async t => {
  const f = await waitingFixture(t);
  f.add('é'.repeat(1001), 3000, 3200); f.speech(3000, 3200);
  await f.backend.delegate(f.request('long_answer'), f.channel);
  assert.equal(f.posts.length, 1); assert.equal(f.replies.length, 0);
  assert.equal(f.saved()[0].status, 'waiting_for_reply');
  assert.ok(f.updates.some(update => /shorter answer/.test(update.text)));
  assert.equal(f.saved()[0].deferredTurns![0].queuedRequest, undefined);
});

test('expired questions remain waiting without admitting a reply or a second run', async t => {
  const f = await fixture(t);
  const value = question(firstQuestionId, { state: 'expired', expiresAt: new Date(Date.now() - 1).toISOString() });
  f.behavior.result = async id => waiting(id, value);
  f.add('Buy the stock.'); f.speech();
  await f.backend.delegate(f.request(), f.channel);
  f.add('Yes.', 3000, 3200); f.speech(3000, 3200);
  await f.backend.delegate(f.request('expired_answer'), f.channel);
  assert.equal(f.replies.length, 0); assert.equal(f.posts.length, 1);
  assert.match(f.updates.at(-1)!.text, /no longer available/);
  assert.equal(f.saved()[0].questions![firstQuestionId].presentation, undefined);
});

test('an exact question read with a foreign identity refuses the reply', async t => {
  for (const change of [{ id: secondQuestionId }, { runId: secondQuestionId }, { threadId: secondQuestionId }]) {
    await t.test(Object.keys(change)[0], async sub => {
      const f = await waitingFixture(sub);
      f.behavior.question = async id => questionResult(id, { ...f.currentQuestion(), ...change });
      f.add('Yes.', 3000, 3200); f.speech(3000, 3200);
      await f.backend.delegate(f.request('foreign_answer'), f.channel);
      assert.equal(f.replies.length, 0); assert.equal(f.posts.length, 1);
      assert.equal(f.saved()[0].questions![firstQuestionId].reply, undefined);
    });
  }
});

test('a revised answer is captured as a later revision of the original source without changing the reply', async t => {
  const f = await waitingFixture(t);
  f.add('Yes', 3000, 3200); f.speech(3000, 3200);
  await f.backend.delegate(f.request('answer'), f.channel);
  const input = structuredClone(f.replies[0].input);
  f.add(', actually let me think.', 3200, 3400); f.speech(3200, 3400); await wait(7);
  const captured = await f.backend.captureCompletedTurns(f.channel.getContext(), { assistantPlaybackComplete: true, sessionFinalized: true });
  assert.equal(captured.captured, 1);
  assert.equal(f.captures[0].sourceEventId, input.source.sourceEventId);
  assert.equal(f.captures[0].revision, 2); assert.equal(f.captures[0].text, 'Yes, actually let me think.');
  assert.deepEqual(f.saved()[0].questions![firstQuestionId].reply!.input, input);
  assert.equal(f.saved()[0].correctionState, 'recorded_not_applied');
  assert.equal(f.replies.length, 1); assert.equal(f.posts.length, 1);
});

test('a different saved client reply cannot be claimed as this caller answer', async t => {
  const f = await waitingFixture(t);
  const value = f.currentQuestion();
  let other: PrimaryQuestion | undefined;
  f.behavior.reply = async (_id, _questionId, input) => {
    other = answered(value, { ...input, reply: { ...input.reply, clientReplyId: secondQuestionId } });
    throw new Error('reply conflicted');
  };
  f.behavior.question = async id => questionResult(id, other ?? value);
  f.behavior.result = async id => other ? f.completed(id) : waiting(id, value);
  f.add('Yes.', 3000, 3200); f.speech(3000, 3200);
  await f.backend.delegate(f.request('conflicting_answer'), f.channel);
  assert.equal(f.saved()[0].questions![firstQuestionId].reply!.status, 'refused');
  const capture = f.store.get(f.call.id)!.canonicalTurns![f.replies[0].input.source.sourceEventId] as PrimaryTurn & { state: string };
  assert.equal(capture.state, 'dispatching');
  assert.equal(capture.text, f.replies[0].input.text);
  assert.equal(f.replies.length, 1); assert.equal(f.posts.length, 1);
});

test('question chunks preserve all context and exact numeric meanings before enabling a reply', async t => {
  const value = question(firstQuestionId, {
    context: 'Your current position contains 12.345678 shares at $123.456789 each. '.repeat(10).trim(),
    yesMeaning: 'Spend exactly $10.25 with a maximum price of $123.456789 per share.',
  });
  const f = await waitingFixture(t, value);
  const chunks = f.updates.filter(update => update.id.includes('_prompt_'));
  assert.ok(chunks.length > 1);
  assert.ok(chunks.every(update => Buffer.byteLength(update.text) <= 500 && update.speak === true));
  assert.equal(chunks.map(chunk => chunk.text).join(''), [value.question, value.context,
    `Proposed answer: ${value.proposedAnswer}`, `If you say yes: ${value.yesMeaning}`, `If you say no: ${value.noMeaning}`].join('\n'));
  assert.ok(f.saved()[0].questions![firstQuestionId].presentation);
});

test('a Primary display-name change leaves exact question recovery available', async t => {
  const f = await waitingFixture(t);
  const original = f.currentQuestion();
  let accepted: PrimaryQuestion | undefined;
  f.behavior.reply = async (_id, _questionId, input) => {
    accepted = answered({ ...original, agent: { ...original.agent, name: 'My renamed Dexter' } }, input);
    throw new Error('reply response lost');
  };
  f.behavior.question = async id => questionResult(id, accepted ?? { ...original, agent: { ...original.agent, name: 'My renamed Dexter' } });
  f.behavior.result = async id => accepted ? { ...f.completed(id), question: accepted } : waiting(id, original);
  f.add('Yes.', 3000, 3200); f.speech(3000, 3200);
  await f.backend.delegate(f.request('renamed_answer'), f.channel);
  assert.equal(f.saved()[0].questions![firstQuestionId].reply!.status, 'accepted');
  assert.equal(f.saved()[0].questions![firstQuestionId].question.agent.name, 'My renamed Dexter');
  assert.equal(f.saved()[0].status, 'completed');
  assert.equal(f.replies.length, 1);
});

test('definite question expiry resolves an unknown reply and captures the caller turn without claiming an answer', async t => {
  const f = await waitingFixture(t);
  const original = f.currentQuestion();
  let expired = false;
  f.behavior.reply = async () => { throw new Error('reply response lost'); };
  f.behavior.question = async id => questionResult(id, expired ? { ...original, state: 'expired' } : original);
  f.add('Yes.', 3000, 3200); f.speech(3000, 3200);
  await f.backend.delegate(f.request('unknown_answer'), f.channel);
  const input = structuredClone(f.replies[0].input);
  f.backend.persistAcousticEvidence();
  f.store.update(f.call.id, { endedAt: new Date().toISOString() });
  expired = true;
  const restored = createPrimaryBackend(f.options);
  assert.deepEqual(await restored.flushPendingCaptures(), { pending: 0, unsupported: [] });
  assert.equal(f.saved()[0].questions![firstQuestionId].reply!.status, 'refused');
  const captured = await restored.captureCompletedTurns(f.channel.getContext(), { assistantPlaybackComplete: true, sessionFinalized: true });
  assert.equal(captured.captured, 0); assert.equal(captured.pending, 0);
  assert.equal(f.captures[0].sourceEventId, input.source.sourceEventId);
  assert.equal(f.captures[0].text, input.text); assert.equal(f.captures[0].revision, 1);
  assert.equal(f.posts.length, 1); assert.equal(f.replies.length, 1);
});

test('first-attempt explicit API non-admission and local validation refuse the reply without unresolved closeout', async t => {
  const errors = [
    new PrimaryApiError('invalid_request'),
    ...([ [400, 'invalid_request'], [401, 'invalid_service_credential'], [403, 'binding_unauthorized'],
      [404, 'question_not_found'], [409, 'call_closed'], [409, 'idempotency_conflict'],
      [410, 'question_expired'], [415, 'unsupported_media_type'], [503, 'phone_questions_unavailable'],
      [503, 'primary_unavailable'] ] as const).map(([status, code]) => new PrimaryApiError('http_error', status, { code, retryable: status === 503 })),
  ];
  for (const error of errors) await t.test(error.remoteCode ?? error.code, async sub => {
    const f = await waitingFixture(sub);
    f.behavior.reply = async () => { throw error; };
    f.add('Yes.', 3000, 3200); f.speech(3000, 3200);
    await f.backend.delegate(f.request('refused'), f.channel);
    const saved = f.saved()[0].questions![firstQuestionId].reply!;
    assert.equal(saved.status, 'refused');
    assert.equal(saved.refusal!.code, error.remoteCode ?? error.code);
    assert.deepEqual(saved.input, f.replies[0].input);
    assert.deepEqual(await createPrimaryBackend(f.options).flushPendingCaptures(), { pending: 0, unsupported: [] });
    assert.equal(f.replies.length, 1); assert.equal(f.posts.length, 1);
    await f.backend.delegate(f.request('repeat_without_new_speech'), f.channel);
    assert.equal(f.replies.length, 1); assert.equal(f.posts.length, 1);
  });
});

test('ambiguous errors retain the exact unresolved reply and do not admit a replacement', async t => {
  const errors = [
    new PrimaryApiError('request_timeout'), new PrimaryApiError('request_failed'), new PrimaryApiError('invalid_response'),
    new PrimaryApiError('http_error', 400),
    ...([ [403, 'unrecognized_remote_error'], [409, 'question_unavailable'], [500, 'request_inconsistent'],
      [503, 'temporarily_unavailable'], [503, 'reply_too_large'] ] as const)
      .map(([status, code]) => new PrimaryApiError('http_error', status, { code, retryable: true })),
  ];
  for (const error of errors) await t.test(error.remoteCode ?? error.code, async sub => {
    const f = await waitingFixture(sub);
    f.behavior.reply = async () => { throw error; };
    f.add('Yes.', 3000, 3200); f.speech(3000, 3200);
    await f.backend.delegate(f.request('unknown'), f.channel);
    const input = structuredClone(f.replies[0].input);
    assert.equal(f.saved()[0].questions![firstQuestionId].reply!.status, 'unknown');
    assert.deepEqual(await createPrimaryBackend(f.options).flushPendingCaptures(), { pending: 1, unsupported: ['reply_outcome_unknown'] });
    f.add('No.', 5000, 5200); f.speech(5000, 5200);
    await f.backend.delegate(f.request('corrected_but_unknown'), f.channel);
    assert.equal(f.replies.length, 1); assert.equal(f.posts.length, 1);
    assert.deepEqual(f.saved()[0].questions![firstQuestionId].reply!.input, input);
    assert.equal(f.saved()[0].questions![firstQuestionId].refusedReplies!.length, 0);
  });
});

test('a fresh corrected answer follows definite refusal on the same request and preserves both source histories', async t => {
  const f = await waitingFixture(t);
  const accept = f.behavior.reply;
  f.behavior.reply = async () => { throw new PrimaryApiError('http_error', 400, { code: 'invalid_request', retryable: false }); };
  f.add('Yes, buy $20 instead.', 3000, 3200); f.speech(3000, 3200);
  await f.backend.delegate(f.request('bad_answer'), f.channel);
  const refused = structuredClone(f.saved()[0].questions![firstQuestionId].reply!);
  f.behavior.reply = accept;
  f.add('No.', 5000, 5200); f.speech(5000, 5200);
  await f.backend.delegate(f.request('corrected_answer'), f.channel);
  const record = f.saved()[0].questions![firstQuestionId];
  assert.deepEqual(record.refusedReplies, [refused]);
  assert.equal(record.reply!.status, 'accepted');
  assert.notEqual(record.reply!.input.source.sourceEventId, refused.input.source.sourceEventId);
  assert.notEqual(record.reply!.input.reply.clientReplyId, refused.input.reply.clientReplyId);
  assert.equal(f.replies[0].requestId, f.replies[1].requestId);
  assert.equal(f.posts.length, 1); assert.equal(f.replies.length, 2);
  f.backend.persistAcousticEvidence();
  f.store.update(f.call.id, { endedAt: new Date().toISOString() });
  const restored = createPrimaryBackend(f.options);
  assert.deepEqual(await restored.flushPendingCaptures(), { pending: 0, unsupported: [] });
  const capture = await restored.captureCompletedTurns(f.channel.getContext(), { assistantPlaybackComplete: true, sessionFinalized: true });
  assert.equal(capture.captured, 0); assert.equal(capture.pending, 0);
  assert.equal(f.captures[0].sourceEventId, refused.input.source.sourceEventId);
  assert.equal(f.captures[0].text, refused.input.text); assert.equal(f.captures[0].revision, 1);
  assert.deepEqual(f.saved()[0].questions![firstQuestionId].refusedReplies, [refused]);
  assert.deepEqual(f.saved()[0].questions![firstQuestionId].reply!.input, f.replies[1].input);
});

test('a refused source preserves its original first revision until capture acknowledgment before a late correction', async t => {
  const f = await waitingFixture(t);
  f.behavior.reply = async () => { throw new PrimaryApiError('http_error', 400, { code: 'invalid_request', retryable: false }); };
  f.add('Yes', 3000, 3200); f.speech(3000, 3200);
  await f.backend.delegate(f.request('refused_answer'), f.channel);
  const refused = structuredClone(f.saved()[0].questions![firstQuestionId].reply!);
  const sourceId = refused.input.source.sourceEventId;
  const initial = f.store.get(f.call.id)!.canonicalTurns![sourceId] as PrimaryTurn & { state: string };
  assert.equal(initial.revision, 1); assert.equal(initial.text, 'Yes'); assert.equal(initial.state, 'dispatching');
  f.add(', actually let me think.', 3200, 3400); f.speech(3200, 3400); await wait(7);
  f.backend.persistAcousticEvidence();
  f.store.update(f.call.id, { endedAt: new Date().toISOString() });
  const restored = createPrimaryBackend(f.options);
  const proof = { assistantPlaybackComplete: true, sessionFinalized: true };
  const finalSnapshot = f.channel.getContext();
  assert.deepEqual(await restored.captureCompletedTurns(finalSnapshot, proof),
    { captured: 0, pending: 1, unsupported: ['capture_outcome_unknown'] });
  assert.equal(f.captures.length, 0);
  f.behavior.capture = async () => { throw new PrimaryApiError('request_timeout'); };
  assert.deepEqual(await restored.flushPendingCaptures(), { pending: 1, unsupported: ['capture_outcome_unknown'] });
  assert.equal(f.captures[0].revision, 1); assert.equal(f.captures[0].text, 'Yes');
  assert.deepEqual(await restored.captureCompletedTurns(finalSnapshot, proof),
    { captured: 0, pending: 1, unsupported: ['capture_outcome_unknown'] });
  assert.equal(f.captures.length, 1);
  f.behavior.capture = async () => {};
  assert.deepEqual(await createPrimaryBackend(f.options).flushPendingCaptures(), { pending: 0, unsupported: [] });
  assert.deepEqual(f.captures[1], f.captures[0]);
  assert.deepEqual(await restored.captureCompletedTurns(finalSnapshot, proof), { captured: 1, pending: 0, unsupported: [] });
  assert.equal(f.captures[2].sourceEventId, sourceId);
  assert.equal(f.captures[2].revision, 2); assert.equal(f.captures[2].text, 'Yes, actually let me think.');
  assert.deepEqual(f.saved()[0].questions![firstQuestionId].reply, refused);
  assert.equal(f.posts.length, 1); assert.equal(f.replies.length, 1);
});

test('a pre-admission-looking error during GET cannot turn an older unknown POST into a refusal', async t => {
  const f = await waitingFixture(t);
  let posted = false;
  const originalRead = f.behavior.question;
  f.behavior.reply = async () => { posted = true; throw new PrimaryApiError('request_timeout'); };
  f.behavior.question = async (id, questionId) => {
    if (posted) throw new PrimaryApiError('http_error', 503, { code: 'phone_questions_unavailable', retryable: true });
    return originalRead(id, questionId);
  };
  f.add('Yes.', 3000, 3200); f.speech(3000, 3200);
  await f.backend.delegate(f.request('unknown'), f.channel);
  assert.deepEqual(await createPrimaryBackend(f.options).flushPendingCaptures(), { pending: 1, unsupported: ['reply_outcome_unknown'] });
  assert.equal(f.saved()[0].questions![firstQuestionId].reply!.status, 'unknown');
  assert.equal(f.replies.length, 1); assert.equal(f.posts.length, 1);
});


test('duplicate saved delegation cannot republish an already accepted completed answer', async t => {
  const f = await fixture(t);
  let publications = 0;
  f.options.observation = { signal: f.controller.signal, async publish() {
    publications++; return { delivery: { eventId: 'accepted_original', status: 'accepted' } };
  } };
  f.add('Read my balance.'); f.speech();
  await f.backend.delegate(f.request(), f.channel);
  assert.equal(f.saved()[0].delivery, 'accepted');
  await f.backend.delegate(f.request('duplicate_completed_source'), f.channel);
  assert.equal(publications, 1); assert.equal(f.posts.length, 1);
});


test('completed presentation survives more than three interruptions without another action', async t => {
  const f = await fixture(t, { watchUntilCallEnds: true });
  let publications = 0;
  f.options.observation = { signal: f.controller.signal, async publish() {
    publications++;
    return { delivery: { eventId: publications < 8 ? '' : 'accepted_original',
      status: publications < 8 ? 'stale' : 'accepted', reason: 'caller_context_changed' } };
  } };
  f.add('Read my balance.'); f.speech();
  await f.backend.delegate(f.request(), f.channel);
  assert.equal(publications, 8); assert.equal(f.posts.length, 1); assert.equal(f.reads.length, 1);
  assert.equal(f.saved()[0].delivery, 'accepted');
});

test('hangup preserves an undelivered completed result without another action', async t => {
  const f = await fixture(t, { watchUntilCallEnds: true });
  let publications = 0;
  f.options.observation = { signal: f.controller.signal, async publish() {
    publications++;
    if (publications === 3) f.controller.abort();
    return { delivery: { eventId: '', status: 'stale' } };
  } };
  f.add('Read my balance.'); f.speech();
  await f.backend.delegate(f.request(), f.channel);
  assert.equal(publications, 3); assert.equal(f.posts.length, 1); assert.equal(f.reads.length, 1);
  assert.equal(f.saved()[0].status, 'completed'); assert.equal(f.saved()[0].primaryResult?.state, 'completed');
});

test('a deferred completed answer yields to queued work and resumes during its running polls', async t => {
  const f = await blockedQueueFixture(t);
  f.options.watchUntilCallEnds = true;
  const presented: string[] = [];
  let attempts = 0;
  f.options.observation = { signal: f.controller.signal, async publish(entry) {
    if (entry.requestId === f.posts[0].requestId) {
      attempts++;
      if (attempts < 6) return { delivery: { eventId: '', status: 'stale' } };
      assert.equal(f.posts.length, 2, 'the later request must not starve behind repeated interruptions');
    }
    presented.push(entry.requestId);
    return { delivery: { eventId: `accepted_${entry.requestId}`, status: 'accepted' } };
  } };
  const result = f.behavior.result;
  f.behavior.result = async id => {
    if (id === f.posts[0].requestId) return result(id);
    return presented.includes(f.posts[0].requestId) ? f.completed(id) : { bindingId, requestId: id, state: 'running' };
  };
  f.release(); await Promise.all([f.first, f.second]);
  assert.equal(attempts, 6); assert.equal(f.posts.length, 2);
  assert.deepEqual(presented, f.posts.map(post => post.requestId));
  assert.equal(f.saved()[0].delivery, 'accepted');
});

async function awaitStatus(predicate: () => boolean) {
  for (let i = 0; i < 300; i++) { if (predicate()) return; await wait(5); }
  assert.ok(predicate(), 'expected bounded status observation');
}

test('recorded sale, progress questions, queued buy and check retain truthful status through 503 and hangup', async t => {
  const f = await fixture(t, { watchUntilCallEnds: true, progressIntervalMs: 15 });
  f.add('Um, I probably want to sell four dollars of the Apple stock that I just bought recently'); f.speech();
  let unavailable = false;
  f.behavior.result = async id => {
    if (unavailable) throw new PrimaryApiError('http_error', 503, { code: 'temporarily_unavailable', retryable: true });
    return { bindingId, requestId: id, state: 'running' };
  };
  const first = f.backend.delegate(f.request(), f.channel);
  await awaitStatus(() => f.reads.length > 0);
  f.add("Okay, it's been thirty seconds. How much longer is it going to be", 3000, 3500); f.speech(3000, 3500);
  await awaitStatus(() => f.updates.some(u => u.text === 'Still working on it.'));
  f.add('How about you buy a dollar worth', 6000, 6400); f.speech(6000, 6400);
  const buy = f.backend.delegate(f.request('buy_later'), f.channel);
  await awaitStatus(() => f.saved()[0].deferredTurns?.length === 1);
  await wait(60); // Several progress intervals: a queued turn is not spoken news.
  assert.equal(f.updates.filter(u => u.kind === 'progress').length, 1);
  assert.equal(f.posts.length, 1);
  unavailable = true;
  f.add('Check again', 9000, 9400); f.speech(9000, 9400);
  const check = f.backend.delegate(f.request('check_later'), f.channel);
  await awaitStatus(() => f.saved()[0].deferredTurns?.length === 2);
  await awaitStatus(() => f.updates.some(u => u.text === "I can't confirm how that's going yet, so I don't know the outcome."));
  assert.equal(f.saved()[0].lastResultReadError?.status, 503);
  assert.equal(f.saved()[0].primaryResult?.state, 'running');
  assert.equal(f.posts.length, 1);
  assert.ok(f.updates.every(u => !/switch|cancelled|buying|selling|checking again|queued|later request|not started/i.test(u.text)));
  f.store.update(f.call.id, { endedAt: new Date().toISOString(), status: 'completed' }); f.controller.abort();
  await Promise.all([first, buy, check]);
  assert.equal(f.posts.length, 1);
  assert.deepEqual(f.saved()[0].deferredTurns?.map(turn => turn.status), ['queued', 'queued']);
});

test('repeated deferred notices retain their budget and a recovered GET retains diagnostic without stale error speech', async t => {
  const f = await fixture(t, { watchUntilCallEnds: true, progressIntervalMs: 5 });
  let attempts = 0, recovered = false, done = false;
  const accepted: string[] = [];
  f.options.observation = { signal: f.controller.signal, async publish(_entry, update) {
    if (!update) return { delivery: { eventId: 'terminal', status: 'accepted' } };
    attempts++;
    if (attempts < 8) return { delivery: { eventId: '', status: 'stale' } };
    accepted.push(update.text); return { delivery: { eventId: `accepted_${attempts}`, status: 'accepted' } };
  } };
  f.behavior.result = async id => {
    if (done) return f.completed(id);
    if (!recovered) throw new PrimaryApiError('http_error', 503, { code: 'temporarily_unavailable', retryable: true });
    return { bindingId, requestId: id, state: 'running' };
  };
  f.add('Sell four dollars of Apple.'); f.speech();
  const work = f.backend.delegate(f.request(), f.channel);
  await awaitStatus(() => accepted.length > 0);
  assert.ok(attempts >= 8); assert.match(accepted[0], /can't confirm how that's going/);
  recovered = true;
  await awaitStatus(() => accepted.some(text => text === 'Still working on it.'));
  assert.equal(f.saved()[0].resultReadUnavailable, false);
  assert.equal(f.saved()[0].lastResultReadError?.status, 503, 'keep original bounded failure evidence');
  done = true; await work;
  assert.equal(f.posts.length, 1); assert.equal(f.saved()[0].delivery, 'accepted');
});

test('uncertain status append is not repeated without a new caller or observed state', async t => {
  const f = await fixture(t, { watchUntilCallEnds: true, progressIntervalMs: 5 });
  let notices = 0, done = false;
  f.options.observation = { signal: f.controller.signal, async publish(_entry, update) {
    if (update) { notices++; return { delivery: { eventId: '', status: 'unconfirmed' } }; }
    return { delivery: { eventId: 'result', status: 'accepted' } };
  } };
  f.behavior.result = async id => done ? f.completed(id) : { bindingId, requestId: id, state: 'running' };
  f.add('Sell four dollars of Apple.'); f.speech(); const work = f.backend.delegate(f.request(), f.channel);
  await awaitStatus(() => notices === 1); await wait(60); assert.equal(notices, 1);
  done = true; await work; assert.equal(f.posts.length, 1);
});

test('a saved tool observation is delivered once while the same request waits and resumes', async t => {
  const f = await fixture(t, { maxPollMs: 5000 });
  const partialResult = { kind: 'portfolio' as const, text: 'The portfolio observation lists 8 SOL.',
    toolInvocationId: '77777777-7777-4777-8777-777777777777', observedAt: '2026-10-02T21:00:00.000Z' };
  let polls = 0;
  f.behavior.result = async id => ++polls < 4 ? { bindingId, requestId: id, state: 'running', partialResult,
    failure: { kind: 'provider_cooldown', retryAt: '2026-10-02T21:01:00.000Z', retryAfterSeconds: 60 - polls } }
    : { bindingId, requestId: id, state: 'failed', partialResult, failure: { kind: 'provider_request_budget' },
      error: { code: 'provider_request_budget_exceeded', retryable: false } };
  f.add('Show my portfolio.'); f.speech();
  await f.backend.delegate(f.request(), f.channel);
  assert.equal(f.updates.filter(update => update.text === partialResult.text).length, 1);
  assert.equal(f.saved()[0].partialDelivery?.status, 'accepted');
  assert.equal(f.posts.length, 1); assert.equal(new Set(f.reads).size, 1);
  assert.equal(f.saved()[0].status, 'failed'); assert.equal(f.saved()[0].primaryResult?.assistant, undefined);
  assert.match(f.updates.at(-1)!.text, /processing limit/);
  assert.doesNotMatch(f.updates.at(-1)!.text, /8 SOL/);
  const backend = createPrimaryBackend(f.options);
  await backend.delegate(f.request(), f.channel);
  assert.equal(f.updates.filter(update => update.text === partialResult.text).length, 1);
  assert.equal(f.posts.length, 1);
});

test('a lost partial append receipt cannot replay the observation or resubmit the request', async t => {
  const f = await fixture(t);
  const partialResult = { kind: 'portfolio' as const, text: 'The portfolio observation lists 8 SOL.',
    toolInvocationId: '77777777-7777-4777-8777-777777777777', observedAt: '2026-10-02T21:00:00.000Z' };
  f.behavior.result = async id => ({ bindingId, requestId: id, state: 'running', partialResult });
  let appends = 0;
  const publish = f.channel.publish;
  f.channel.publish = async update => {
    if (update.text === partialResult.text) { appends++; throw new Error('receipt lost'); }
    return publish(update);
  };
  f.add('Show my portfolio.'); f.speech();
  await f.backend.delegate(f.request(), f.channel);
  const backend = createPrimaryBackend(f.options);
  await backend.delegate(f.request(), f.channel);
  assert.equal(appends, 1); assert.equal(f.posts.length, 1);
  assert.equal(f.saved()[0].partialDelivery?.status, 'unconfirmed');
});

test('a saved action observation cannot silently change its financial intent on a later poll', async t => {
  const f = await fixture(t, { maxPollMs: 5000 });
  const partialResult = { kind: 'action_status' as const, text: 'The saved action is submitted and awaits its final outcome.',
    toolInvocationId: '77777777-7777-4777-8777-777777777777', observedAt: '2026-10-02T21:00:00.000Z',
    intentId: '88888888-8888-4888-8888-888888888888', actionState: 'submitted' as const };
  let reads = 0;
  f.behavior.result = async id => {
    reads++;
    if (reads === 3) assert.equal(f.saved()[0].resultReadUnavailable, true);
    if (reads === 2) return { bindingId, requestId: id, state: 'failed',
      partialResult: { ...partialResult, intentId: '99999999-9999-4999-8999-999999999999' },
      error: { code: 'provider_busy', retryable: false } };
    if (reads >= 3) return { bindingId, requestId: id, state: 'failed', partialResult,
      error: { code: 'provider_busy', retryable: false } };
    return { bindingId, requestId: id, state: 'running', partialResult };
  };
  f.add('Check my saved purchase.'); f.speech(); await f.backend.delegate(f.request(), f.channel);
  assert.equal(reads, 3); assert.deepEqual(f.saved()[0].primaryResult?.partialResult, partialResult);
  assert.equal(f.posts.length, 1);
  assert.equal(f.updates.filter(update => update.text === partialResult.text).length, 1);
});

test('call truth: legacy question speech remains pending and only the last terminal chunk releases the gate', async t => {
  const f = await waitingFixture(t);
  assert.ok(f.updates.filter(update => update.id.includes('_prompt_')).every(update => update.pending === true));
  const g = await fixture(t, { maxPollMs: 1000 }); g.add('Sell $1 of SpaceX.'); g.speech();
  g.result('Nothing was sold. ' + 'Your SpaceX stock and cash are unchanged. '.repeat(20));
  await g.backend.delegate(g.request(), g.channel);
  const chunks = g.updates.filter(update => update.kind === 'result');
  assert.ok(chunks.length > 1);
  assert.ok(chunks.every(update => update.pending === false));
  assert.ok(chunks.slice(0, -1).every(update => update.terminal === false));
  assert.equal(chunks.at(-1)!.terminal, true);
});

test('RV gate leak2: early yes and question reply delegations are tracked', async t => {
  const seen: BrokerDelegationRecord[] = [];
  const f = await fixture(t, { onRequest: entry => { seen.push(structuredClone(entry)); } });
  const value = question();
  f.behavior.result = async id => waiting(id, value);
  f.behavior.question = async id => questionResult(id, value);
  const originalPublish = f.channel.publish;
  let interrupt = true;
  f.channel.publish = async update => {
    if (interrupt) { interrupt = false; f.add('Yes.', 3000, 3200); f.speech(3000, 3200); }
    return originalPublish(update);
  };
  f.add('Buy the stock.'); f.speech();
  await f.backend.delegate(f.request(), f.channel);
  await f.backend.delegate(f.request('early_yes'), f.channel);
  f.behavior.result = async id => f.completed(id);
  await f.backend.delegate(f.request('original_finished'), f.channel);
  const tracked = new Set(seen.flatMap(e => [...e.delegationIds, ...(e.delegationAliases ?? []).map(a => a.delegationId)]));
  t.diagnostic(`tracked=${JSON.stringify([...tracked])} last status=${seen.at(-1)?.status}`);
  assert.ok(tracked.has('early_yes'), 'early_yes delegation is never re-keyed: the gate keeps it pending after the request ends');
  assert.ok(tracked.has('original_finished'), 'original_finished delegation is never re-keyed');
});

test('RV gate leak3: duplicate delegation on same turn and status question delegation', async t => {
  const seen: BrokerDelegationRecord[] = [];
  const f = await fixture(t, { onRequest: entry => { seen.push(structuredClone(entry)); } });
  f.behavior.result = async id => ({ requestId: id, bindingId: f.saved()[0]?.bindingId ?? 'x', state: 'running' } as any);
  f.add('Check my balance.'); f.speech();
  await f.backend.delegate(f.request(), f.channel).catch(e => t.diagnostic('d1 ' + e));
  await f.backend.delegate(f.request('dup_same_turn'), f.channel).catch(e => t.diagnostic('d2 ' + e));
  f.add('Is it done yet?', 3000, 3400); f.speech(3000, 3400);
  await f.backend.delegate(f.request('status_q'), f.channel).catch(e => t.diagnostic('d3 ' + e));
  const tracked = new Set(seen.flatMap(e => [...e.delegationIds, ...(e.delegationAliases ?? []).map(a => a.delegationId)]));
  t.diagnostic(`tracked=${JSON.stringify([...tracked])} saved=${JSON.stringify(f.saved().map(e => [e.requestId.slice(0,12), e.status, e.delegationIds, (e.deferredTurns ?? []).map(d => [d.delegationId, d.status])]))}`);
  assert.ok(tracked.has('dup_same_turn'), 'dup_same_turn never tracked');
  assert.ok(tracked.has('status_q'), 'status_q never tracked');
});

test('RV gate leak4: delegation with no caller speech', async t => {
  const seen: BrokerDelegationRecord[] = [];
  const f = await fixture(t, { onRequest: entry => { seen.push(structuredClone(entry)); } });
  let released = 0; f.channel.release = () => { released++; };
  const pubs: any[] = []; const orig = f.channel.publish;
  f.channel.publish = async u => { pubs.push(u); return orig(u); };
  await f.backend.delegate(f.request('no_speech'), f.channel).catch(e => t.diagnostic('d ' + e));
  const tracked = new Set(seen.flatMap(e => e.delegationIds));
  t.diagnostic(`tracked=${JSON.stringify([...tracked])} pubs=${JSON.stringify(pubs.map(p => [p.id, p.kind, p.speak]))}`);
  assert.equal(released, 1, 'unadmitted delegation explicitly releases its provisional gate');
  assert.equal(pubs.some(p => p.kind === 'result'), false, 'release is not a fabricated Primary result');
});


test('a duplicate delegation for an already spoken result releases only its provisional gate', async t => {
  const f = await fixture(t); f.add('Check my balance.'); f.speech(); f.result('Your cash is unchanged.');
  await f.backend.delegate(f.request(), f.channel);
  assert.equal(f.saved()[0].delivery, 'accepted');
  let released = 0; f.channel.release = () => { released++; };
  await f.backend.delegate(f.request('duplicate_after_result'), f.channel);
  assert.equal(released, 1); assert.equal(f.posts.length, 1);
});
