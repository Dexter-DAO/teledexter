import assert from 'node:assert/strict';
import test from 'node:test';
import { createRestrictedCallerTurnFreezer } from '../src/broker/restrictedCallerTurn.js';
import type { LiveDelegationRequest, LiveTranscriptSnapshot } from '../src/live/types.js';

function fixture() {
  const controller = new AbortController(), sessionId = 'setup-session';
  let snapshot: LiveTranscriptSnapshot = { revision: 1, callerRevision: 1, fragments: [
    { eventId: 'event-1', role: 'user', text: 'Call my verified number.', sequence: 1, startMs: 0, endMs: 20 } ] };
  let audioThroughMs = 40, speechAt = Date.now() - 50, active = true;
  const listeners = new Set<() => void>(), speech = new Set<() => void>();
  const request: LiveDelegationRequest = { callId: 'call-1', sessionId, delegationId: 'delegate-1', requestId: 'request-1', offsetMs: 30,
    transcript: structuredClone(snapshot) };
  const freezer = createRestrictedCallerTurnFreezer({ callId: request.callId, sessionId, mediaStartedAtMs: Date.now() - 1000,
    deadlineAtMs: Date.now() + 1000, stabilityMs: 10, maxWaitMs: 100, isCurrent: () => active,
    frontier: () => ({ sessionId, audioThroughMs, observedSpeechAtMs: speechAt }),
    subscribeSpeech(fn) { speech.add(fn); return () => { speech.delete(fn); }; } });
  const channel = { signal: controller.signal, getContext: () => structuredClone(snapshot),
    subscribeContext(fn: (value: LiveTranscriptSnapshot) => void) { const listener = () => fn(structuredClone(snapshot));
      listeners.add(listener); return () => { listeners.delete(listener); }; } };
  return { freezer, channel, request, abort: () => controller.abort(), revoke: () => { active = false; },
    audio() { audioThroughMs += 20; speechAt = Date.now(); for (const fn of speech) fn(); },
    caption(text: string, startMs = 25) { snapshot = { revision: 2, callerRevision: 2, fragments: [...snapshot.fragments,
      { eventId: 'event-2', role: 'user', text, sequence: 2, startMs, endMs: startMs + 10 }] }; for (const fn of listeners) fn(); },
    listenerCount: () => listeners.size + speech.size };
}
test('context-free source retains exact raw caption and source identity without Primary or persistence', async () => {
  const f = fixture(), value = await f.freezer.freeze(f.request, f.channel, []); assert.ok(value);
  assert.equal(value.turn.text, 'Call my verified number.'); assert.equal(value.sessionId, f.request.sessionId);
  assert.equal(value.firstEventId, 'event-1'); assert.equal(f.listenerCount(), 0);
  assert.equal(await f.freezer.freeze(f.request, f.channel, [value.turn.sourceEventId]), undefined);
});
test('new acoustic input during a pending freeze refuses even without captions', async () => {
  const f = fixture(), pending = f.freezer.freeze(f.request, f.channel, []); f.audio();
  assert.equal(await pending, undefined); assert.equal(f.listenerCount(), 0);
});
test('delayed caption on the same captured audio retains raw fragments without trimming', async () => {
  const f = fixture(), pending = f.freezer.freeze(f.request, f.channel, []); f.caption(' Please.');
  const value = await pending; assert.equal(value?.turn.text, 'Call my verified number. Please.');
  assert.equal(value?.firstEventId, 'event-1'); assert.equal(value?.lastSequence, 2);
});
test('a later independent caller turn cannot be folded into an older delegation', async () => {
  const f = fixture(), pending = f.freezer.freeze(f.request, f.channel, []); f.caption('Wait, do not call.', 2000);
  assert.equal(await pending, undefined);
});
test('stop, retired session and wrong source identity cannot admit a setup effect', async () => {
  const f = fixture(), pending = f.freezer.freeze(f.request, f.channel, []); f.abort(); assert.equal(await pending, undefined);
  assert.equal(f.listenerCount(), 0);
  const g = fixture(); g.revoke(); assert.equal(await g.freezer.freeze(g.request, g.channel, []), undefined);
  const h = fixture(); assert.equal(await h.freezer.freeze({ ...h.request, sessionId: 'old' }, h.channel, []), undefined);
});
