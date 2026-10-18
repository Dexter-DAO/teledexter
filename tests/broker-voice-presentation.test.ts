import assert from 'node:assert/strict';
import test from 'node:test';
import { carrierPlaybackState, callerPresentationFrontier, hasLaterCallerSpeech, hasNewerCallerWork,
  failedRequestSpeech, requestPresentationContext } from '../src/broker/voicePresentation.js';
import { callTasks } from '../src/broker/voiceTasks.js';
import { carriesSpeechPcmu, hasAudiblePcmu, PlaybackReceipts } from '../src/broker/phone.js';
import type { BrokerCall } from '../src/broker/store.js';
import type { BrokerDelegationRecord } from '../src/broker/backend.js';

// Only presentation coordinates are relevant here. Actual admission and store
// composition are covered by broker-generation-server.test.ts.
function fixture() {
  const first = { generationId: 'generation_a', ordinal: 1, sessionId: 'session_a',
    transcript: { callerRevision: 1, fragments: [{ role: 'user', sequence: 1 }] } };
  const next = { generationId: 'generation_b', ordinal: 2, sessionId: 'session_b',
    transcript: { callerRevision: 0, fragments: [] } };
  const entry = { requestId: 'request_a', sessionId: first.sessionId, firstSequence: 1, lastSequence: 1, status: 'running' } as BrokerDelegationRecord;
  const call = { liveGenerations: { segments: [first, next] }, delegations: { request_a: entry } } as unknown as BrokerCall;
  return { first, next, entry, call };
}

test('carrier receipts distinguish drained, cleared, disconnected and invalid counters', () => {
  assert.equal(carrierPlaybackState(true, 320, 160, 0), 'pending');
  assert.equal(carrierPlaybackState(true, 320, 320, 0), 'idle');
  assert.equal(carrierPlaybackState(true, 320, 160, 160), 'idle');
  assert.equal(carrierPlaybackState(false, 0, 0, 0), 'unavailable');
  for (const counters of [[160, 160, 160], [-1, 0, 0], [1.5, 0, 0], [Infinity, 0, 0], [0, NaN, 0]])
    assert.equal(carrierPlaybackState(true, ...counters as [number, number, number]), 'unavailable');
});

test('an empty successor does not erase prior caller frontier or make a late task current', () => {
  const f = fixture(), initial = callerPresentationFrontier(f.call);
  assert.equal(hasLaterCallerSpeech(f.call, f.entry), false);
  f.first.transcript.callerRevision++;
  f.first.transcript.fragments.push({ role: 'user', sequence: 3 });
  assert.notEqual(callerPresentationFrontier(f.call), initial);
  assert.equal(hasLaterCallerSpeech(f.call, f.entry), true);
  assert.equal(hasNewerCallerWork(f.call, f.entry), false, 'conversation alone is not a new financial request');
});

test('queued later caller work suppresses an earlier result across provider sessions', () => {
  const f = fixture();
  f.entry.deferredTurns = [{ status: 'queued', sourceSessionId: 'session_b', firstSequence: 1 }] as BrokerDelegationRecord['deferredTurns'];
  assert.equal(hasNewerCallerWork(f.call, f.entry), true);
  f.entry.deferredTurns![0].status = 'promoted';
  assert.equal(hasNewerCallerWork(f.call, f.entry), false);
  f.call.delegations.request_b = { requestId: 'request_b', sessionId: 'session_b', firstSequence: 1 };
  assert.equal(hasNewerCallerWork(f.call, f.entry), true);
});

test('an accepted answer advances its own task presentation source but an unaccepted answer cannot', () => {
  const f = fixture();
  f.first.transcript.fragments.push({ role: 'user', sequence: 3 }); f.first.transcript.callerRevision++;
  f.entry.questions = { question: { reply: { status: 'prepared', lastSequence: 3 } } } as BrokerDelegationRecord['questions'];
  assert.equal(hasLaterCallerSpeech(f.call, f.entry), true);
  f.entry.questions!.question.reply!.status = 'accepted';
  assert.equal(hasLaterCallerSpeech(f.call, f.entry), false);
});

test('financial uncertainty speech reports uncertainty without a completion or ongoing-check claim', () => {
  const { entry } = fixture();
  entry.status = 'failed'; entry.primaryResult = { state: 'failed', error: { code: 'primary_financial_result_unknown', retryable: false } } as BrokerDelegationRecord['primaryResult'];
  assert.equal(failedRequestSpeech(entry), "I couldn't confirm the trade request. We need to check that same request before continuing.");
  assert.equal(entry.primaryResult!.state, 'failed');
});


test('late transcript correction cannot move the original task past new caller speech', () => {
  const f = fixture(); f.entry.frozenCallerRevision = 1;
  f.first.transcript.fragments.push({ role: 'user', sequence: 3 }); f.first.transcript.callerRevision++;
  f.entry.lastSequence = 3;
  assert.equal(hasLaterCallerSpeech(f.call, f.entry), true);
});

test('digital silence keeps total accounting without holding audible playback; even one quiet sample needs its mark', () => {
  const receipts = new PlaybackReceipts();
  const silence = Buffer.from([0xff, 0x7f, 0xff]);
  assert.equal(hasAudiblePcmu(silence), false);
  assert.equal(hasAudiblePcmu(Buffer.from([0xff, 0xfe, 0x7f])), true);
  assert.equal(hasAudiblePcmu(Buffer.alloc(0)), true);
  const quiet = receipts.sent(silence.length, hasAudiblePcmu(silence));
  const speech = receipts.sent(160, true);
  receipts.acknowledge(quiet);
  assert.equal(receipts.audiblePlayedBytes, 0);
  assert.equal(carrierPlaybackState(true, receipts.audibleSentBytes, receipts.audiblePlayedBytes, receipts.audibleClearedBytes), 'pending');
  receipts.acknowledge(speech);
  assert.equal(carrierPlaybackState(true, receipts.audibleSentBytes, receipts.audiblePlayedBytes, receipts.audibleClearedBytes), 'idle');
  receipts.sent(320, true); receipts.clear(); receipts.acknowledge(speech);
  assert.equal(receipts.sentBytes, 483); assert.equal(receipts.playedBytes, 163); assert.equal(receipts.clearedBytes, 320);
  assert.equal(receipts.audibleSentBytes, 480); assert.equal(receipts.audiblePlayedBytes, 160); assert.equal(receipts.audibleClearedBytes, 320);
});


test('pre-speech drop never discards speech or counts as a speech clear, and keeps audible accounting balanced', () => {
  // A -58 dBFS blip (max |sample| ~42/32768, as recorded in production) is audible PCMU but not speech.
  assert.equal(hasAudiblePcmu(Buffer.alloc(800, 0xf0)), true);
  assert.equal(carriesSpeechPcmu(Buffer.alloc(800, 0xf0)), false);
  assert.equal(carriesSpeechPcmu(Buffer.from([0xff, 0x7f])), false);
  assert.equal(carriesSpeechPcmu(Buffer.alloc(800, 0x20)), true);
  const receipts = new PlaybackReceipts();
  const silent = receipts.sent(800, false), blip = receipts.sent(800, true), played = receipts.sent(800, false);
  receipts.acknowledge(played);
  assert.equal(receipts.backlogMs, 200);
  assert.equal(receipts.dropPreSpeech(), 1_600);
  assert.equal(receipts.dropPreSpeech(), 0, 'nothing left to drop');
  receipts.acknowledge(silent); receipts.acknowledge(blip);
  assert.equal(receipts.playedBytes, 800); assert.equal(receipts.clearedBytes, 0); assert.equal(receipts.preSpeechDroppedBytes, 1_600);
  assert.equal(receipts.backlogMs, 0);
  assert.equal(carrierPlaybackState(true, receipts.audibleSentBytes, receipts.audiblePlayedBytes, receipts.audibleClearedBytes), 'idle');
  const quiet = receipts.sent(800, true), speech = receipts.sent(800, true, true);
  assert.equal(receipts.dropPreSpeech(), 0, 'a pending speech chunk blocks the drop');
  assert.equal(receipts.isPending(quiet), true);
  receipts.acknowledge(quiet); receipts.acknowledge(speech);
  assert.equal(receipts.speechSentBytes, 800); assert.equal(receipts.speechPlayedBytes, 800);
});


test('known provider failures explain service availability without asserting a financial outcome',()=>{
  const {entry}=fixture();
  for(const code of ['provider_busy','provider_request_budget_exceeded','provider_credit_limit','provider_credits_unavailable','provider_rate_limited']) {
    entry.primaryResult={state:'failed',error:{code,retryable:false}} as BrokerDelegationRecord['primaryResult'];
    const text=failedRequestSpeech(entry);assert.match(text,/AI service/);assert.doesNotMatch(text,/trade failed|not submitted|nothing was sold|out of service credits/i);
  }
});

test('partial facts survive model failure without implying trade execution or erasing uncertainty', () => {
  const { entry } = fixture(); entry.status = 'failed';
  entry.primaryResult = { state: 'failed', error: { code: 'primary_financial_result_unknown', retryable: false },
    partialResult: { kind: 'prepared_action', text: 'An unsigned Apple purchase was prepared. It has not executed.',
      toolInvocationId: '11111111-1111-4111-8111-111111111111', observedAt: '2026-10-02T21:00:00.000Z' } } as BrokerDelegationRecord['primaryResult'];
  // Outcome first; the preparation was quiet progress, so the caller hears it once, after the outcome.
  const expected = "I couldn't confirm the trade request. We need to check that same request before continuing. The last observed step before that: An unsigned Apple purchase was prepared. It has not executed.";
  assert.equal(failedRequestSpeech(entry), expected);
  entry.partialDelivery = { ...entry.primaryResult!.partialResult!, status: 'accepted' };
  assert.equal(failedRequestSpeech(entry), expected, 'quiet delivery is not the caller hearing it');
  entry.primaryResult!.partialResult = { kind: 'action_status', actionState: 'confirmed', intentId: '22222222-2222-4222-8222-222222222222',
    text: 'The Apple purchase was confirmed.', toolInvocationId: '11111111-1111-4111-8111-111111111111', observedAt: '2026-10-02T21:00:00.000Z' };
  assert.match(failedRequestSpeech(entry), /^The Apple purchase was confirmed\. I couldn't confirm the trade request/, 'a settled action is the outcome and leads');
});

test('a provider pause is task-list progress for the voice, never a completion', () => {
  const { entry } = fixture();
  entry.submission = { text: 'Sell four dollars of Apple.', source: { sourceEventId: 'source', revision: 1 } } as BrokerDelegationRecord['submission'];
  entry.primaryResult = { state: 'running', failure: { kind: 'provider_cooldown', retryAt: '2026-10-02T21:01:00.000Z',
    retryAfterSeconds: 60 } } as BrokerDelegationRecord['primaryResult'];
  const [task] = callTasks({ delegations: { request_a: entry } } as unknown as BrokerCall);
  assert.equal(task.status, 'pending');
  assert.match(task.progress!, /same request is scheduled to continue/);
  assert.doesNotMatch(task.progress!, /completed|executed|failed/);
});

test('replacement context carries the eligible source in bounded quoted fragments', () => {
  const { entry } = fixture();
  const text = ' Please review "that one" 🔍, with the earlier figures.\n'.repeat(50);
  entry.submission = { text: 'An old local copy that must not replace eligible text.',
    source: { sourceEventId: 'source', revision: 1 } } as BrokerDelegationRecord['submission'];
  const messages = [{ role: 'user', text, canonicalSource: { sourceEventId: 'source', revision: 1 } }] as Parameters<typeof requestPresentationContext>[1];
  const chunks = requestPresentationContext(entry, messages);
  assert.ok(chunks.length > 1);
  const parsed = chunks.map(chunk => { assert.ok(Buffer.byteLength(chunk) <= 500); return JSON.parse(chunk); });
  assert.equal(parsed.map(value => value.requestText).join(''), text);
  for (const [index, value] of parsed.entries()) {
    assert.equal(value.kind, 'task_result_context'); assert.equal(value.requestId, entry.requestId);
    assert.equal(value.state, 'running'); assert.equal(value.part, index + 1);
  }
  const absent = requestPresentationContext(entry, []);
  assert.deepEqual(JSON.parse(absent[0]), { kind: 'task_result_context', requestId: entry.requestId, state: 'running', sourceAvailable: false });
  const revised = structuredClone(messages); revised[0].canonicalSource!.revision = 2;
  assert.deepEqual(requestPresentationContext(entry, revised), absent);
});

