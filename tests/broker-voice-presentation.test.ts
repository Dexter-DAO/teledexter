import assert from 'node:assert/strict';
import test from 'node:test';
import { carrierPlaybackState, callerPresentationFrontier, hasLaterCallerSpeech, hasNewerCallerWork,
  isFixedPendingStatus, failedRequestSpeech } from '../src/broker/voicePresentation.js';
import { hasAudiblePcmu, PlaybackReceipts } from '../src/broker/phone.js';
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

test('only a fixed local pending notice can use content-equivalent status context', () => {
  const { entry } = fixture();
  const update = { id: 'request_a_1_pending_1', kind: 'progress' as const, speak: true,
    text: 'Your request is still being handled. I am waiting for its result.' };
  assert.equal(isFixedPendingStatus(entry, update), true);
  assert.equal(isFixedPendingStatus(entry, { ...update, text: 'Buying five dollars of Apple.' }), false);
  assert.equal(isFixedPendingStatus(entry, { ...update, kind: 'result' }), false);
  assert.equal(isFixedPendingStatus(entry, { ...update, id: 'different_request' }), false);
  entry.status = 'completed'; assert.equal(isFixedPendingStatus(entry, update), false);
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

test('fixed status must agree with the current queued and unavailable observation', () => {
  const { entry } = fixture(); entry.resultReadUnavailable = true;
  const old = { id: 'request_a_pending_1', kind: 'progress' as const,
    text: 'Your request is still being handled. I am waiting for its result.' };
  assert.equal(isFixedPendingStatus(entry, old), false);
  assert.equal(isFixedPendingStatus(entry, { ...old,
    text: 'I cannot get the latest status of your request. Its outcome is still unconfirmed.' }), true);
  entry.deferredTurns = [{ status: 'queued', sourceEventId: 'new_buy' }] as BrokerDelegationRecord['deferredTurns'];
  assert.equal(isFixedPendingStatus(entry, { ...old,
    text: 'I cannot get the latest status of the earlier request. Your later requests are queued and have not started.' }), true);
  entry.resultReadUnavailable = false;
  assert.equal(isFixedPendingStatus(entry, { ...old,
    text: 'Your later requests are queued behind the earlier request. They have not started.' }), true);
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


test('known provider failures explain service availability without asserting a financial outcome',()=>{
  const {entry}=fixture();
  for(const code of ['provider_busy','provider_request_budget_exceeded','provider_credit_limit','provider_credits_unavailable','provider_rate_limited']) {
    entry.primaryResult={state:'failed',error:{code,retryable:false}} as BrokerDelegationRecord['primaryResult'];
    const text=failedRequestSpeech(entry);assert.match(text,/AI service/);assert.doesNotMatch(text,/trade failed|not submitted|nothing was sold|out of service credits/i);
  }
});
