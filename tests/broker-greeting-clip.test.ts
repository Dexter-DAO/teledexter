import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { GREETING_CLIP_ASSET, GREETING_CLIP_TEXT, INBOUND_GREETING_CLIP_ASSET, INBOUND_GREETING_CLIP_TEXT, greetingClipEligible, loadGreetingClip, playGreetingClip } from '../src/broker/greetingClip.js';
import { brokerOpening } from '../src/broker/instructions.js';
import { LIVE_APPEND_MAX_BYTES } from '../src/live/session.js';
import { carriesSpeechPcmu } from '../src/broker/phone.js';
import type { BrokerCall } from '../src/broker/store.js';

test('the shipped clip is plausible 8 kHz PCMU speech of about one second', () => {
  const clip = loadGreetingClip();
  assert.ok(clip, `missing ${GREETING_CLIP_ASSET.pathname}`);
  assert.ok(clip.length >= 6_400 && clip.length <= 16_000, `${clip.length / 8} ms`);
  assert.ok(carriesSpeechPcmu(clip), 'the clip carries speech energy');
  assert.equal(GREETING_CLIP_TEXT, "Hey, it's Dexter.");
  const inbound = loadGreetingClip(INBOUND_GREETING_CLIP_ASSET);
  assert.ok(inbound, `missing ${INBOUND_GREETING_CLIP_ASSET.pathname}`);
  assert.ok(inbound.length > clip.length && inbound.length <= 28_000, `${inbound.length / 8} ms`);
  assert.ok(carriesSpeechPcmu(inbound));
  assert.equal(INBOUND_GREETING_CLIP_TEXT, "Hey, it's Dexter. What can I do for you?");
});

test('a missing or implausible clip falls back to the Live-spoken greeting', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'greeting-clip-'));
  try {
    assert.equal(loadGreetingClip(path.join(dir, 'absent.pcmu')), undefined);
    fs.writeFileSync(path.join(dir, 'short.pcmu'), Buffer.alloc(100, 0x20));
    assert.equal(loadGreetingClip(path.join(dir, 'short.pcmu')), undefined);
    fs.writeFileSync(path.join(dir, 'long.pcmu'), Buffer.alloc(40_000, 0x20));
    assert.equal(loadGreetingClip(path.join(dir, 'long.pcmu')), undefined);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});

test('the clip streams in order as 20 ms frames at real-time pace with a 100 ms lead', () => {
  const clip = Buffer.from(Array.from({ length: 2_000 }, (_value, index) => index % 251));
  let clock = 1_000;
  const timers: Array<{ fn: () => void; at: number }> = [];
  const sent: Array<{ at: number; bytes: Buffer }> = [];
  let done = 0;
  const playback = playGreetingClip(clip, {
    send(frame) { sent.push({ at: clock, bytes: Buffer.from(frame) }); return true; },
    onSent() { done++; },
    now: () => clock,
    setTimer(fn, ms) { const timer = { fn, at: clock + ms }; timers.push(timer); return timer as unknown as ReturnType<typeof setTimeout>; },
    clearTimer() {},
  });
  assert.equal(sent.length, 5, '100 ms lead sent at once');
  assert.ok(sent.every(entry => entry.bytes.length === 160));
  while (timers.length) { const timer = timers.shift()!; clock = timer.at; timer.fn(); }
  assert.equal(done, 1); assert.equal(playback.sending, false);
  assert.deepEqual(Buffer.concat(sent.map(entry => entry.bytes)), clip, 'every byte once, in order');
  // Each later frame is released no earlier than real time minus the 100 ms lead.
  let offset = 0;
  for (const entry of sent) {
    assert.ok(entry.at - 1_000 >= (offset + entry.bytes.length - 800) / 8 - 0.001, `frame at byte ${offset} sent too early`);
    offset += entry.bytes.length;
  }
  assert.ok(clock - 1_000 <= clip.length / 8, 'sending finishes within the clip duration');
});

test('a refused frame or cancel stops the clip without reporting it sent', () => {
  let done = 0, count = 0;
  const refused = playGreetingClip(Buffer.alloc(4_000, 0x20), { send() { return ++count < 3; }, onSent() { done++; } });
  assert.equal(refused.sending, false); assert.equal(count, 3); assert.equal(done, 0);
  const cancelled = playGreetingClip(Buffer.alloc(4_000, 0x20), { send() { return true; }, onSent() { done++; } });
  assert.equal(cancelled.sending, true); cancelled.cancel(); assert.equal(cancelled.sending, false); assert.equal(done, 0);
});

const baseCall = { id: 'c', direction: 'outbound', status: 'in-progress', mediaStartedAt: '2026-10-13T04:16:00.154Z' } as unknown as BrokerCall;
test('the clip plays only where brokerOpening opens the call, once', () => {
  assert.equal(greetingClipEligible(baseCall), true);
  assert.equal(greetingClipEligible({ ...baseCall, direction: 'inbound' } as BrokerCall), true);
  for (const patch of [
    { continuation: {} }, { verificationCallback: {} }, { restrictedSetup: {} },
    { voiceOpening: { status: 'accepted' } }, { journey: { firstAudioSentAt: '2026-10-13T04:16:00.200Z' } },
    { journey: { greetingClipStartedAt: '2026-10-13T04:16:00.200Z' } }, { liveGenerations: { segments: [{}] } },
  ]) assert.equal(greetingClipEligible({ ...baseCall, ...patch } as unknown as BrokerCall), false, JSON.stringify(patch));
});

test('after the clip, every outbound opening continues without greeting or naming Dexter again and fits one append', () => {
  for (const direction of ['outbound'] as const)
    for (const registered of [false, true]) for (const watch of [false, true]) for (const notification of [false, true]) {
      const opening = brokerOpening(direction, registered, watch, notification, true);
      const label = `${direction}/${registered}/${watch}/${notification}`;
      assert.ok(Buffer.byteLength(opening) <= LIVE_APPEND_MAX_BYTES, `${label}: ${Buffer.byteLength(opening)} bytes`);
      assert.match(opening, /^The caller already heard you say "Hey, it's Dexter\." Do not greet or say your name again\./, label);
      assert.doesNotMatch(opening.replace(/^The caller already heard you say "Hey, it's Dexter\."/, ''), /Hey, it's Dexter|Hi, this is Dexter/, label);
      assert.notEqual(opening, brokerOpening(direction, registered, watch, notification), label);
    }
  assert.match(brokerOpening('outbound', true, false, false, true), /"Want me to pull up your portfolio\?"/);
  // Inbound: the clip is the whole opening; Live neither greets nor asks again.
  for (const registered of [false, true]) {
    const inbound = brokerOpening('inbound', registered, false, false, true);
    assert.equal(inbound, 'The caller already heard you say "Hey, it\'s Dexter. What can I do for you?" Do not greet or ask again. Wait for the caller and respond to what they say.');
    assert.ok(Buffer.byteLength(inbound) <= LIVE_APPEND_MAX_BYTES);
    assert.doesNotMatch(inbound, /Immediately/);
  }
  assert.match(brokerOpening('outbound', true, true, false, true), /watch name, event summary and observation time/);
  assert.match(brokerOpening('outbound', true, false, true, true), /title, summary and observation time/);
  // Without the clip, Live still speaks the full greeting.
  assert.match(brokerOpening('outbound', true), /Immediately say in English "Hey, it's Dexter\."/);
});
