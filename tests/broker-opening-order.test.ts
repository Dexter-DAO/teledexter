import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import test, { type TestContext } from 'node:test';
import { createGenerationVoice } from '../src/broker/generationVoice.js';
import { brokerOpening } from '../src/broker/instructions.js';
import { BrokerStore } from '../src/broker/store.js';
import type { PrimaryApiClient } from '../src/broker/primaryApi.js';
import type { EligibleContextClient, EligiblePhoneCall } from '../src/broker/eligibleContext.js';
import { createBrokerLiveSession } from '../src/live/session.js';
import { PreconnectTransport } from './helpers/preconnectTransport.js';

const bindingId = '11111111-1111-4111-8111-111111111111';
const primaryDexterId = '22222222-2222-4222-8222-222222222222';
const threadId = '33333333-3333-4333-8333-333333333333';
const ownerPrincipalId = '44444444-4444-4444-8444-444444444444';
const callSid = `CA${'1'.repeat(32)}`;
// 20 ms caller frames: digital silence, and a frame with speech energy (the threshold every caller-speech check uses).
const silence = Buffer.alloc(160, 0xff).toString('base64');
const speech = Buffer.alloc(160, 0x21).toString('base64');

/** Live started while the person was talking: the frames are queued in the generation controller,
 * then replayed at 1.5x once Live is live. Returns the order of what reached the Live socket. */
async function opening(t: TestContext, direction: 'inbound' | 'outbound', held: string[], greeted = true, onFrame?: () => void) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-opening-order-'));
  const store = new BrokerStore(directory);
  const reserved = direction === 'outbound'
    ? store.reserve({ requestId: randomUUID(), number: '+12025550123', direction: 'outbound', purpose: 'Check the portfolio', callSid }).call
    : store.reserve({ requestId: randomUUID(), number: '+12025550123', direction: 'inbound', purpose: 'The person called Dexter.', callSid }).call;
  const page: EligiblePhoneCall = { bindingId, callSid, state: 'open', context: {
    primaryDexterId, threadId, displayName: 'Dexter', messages: [], history: {
      version: { primaryDexterId, threadId, ownerPrincipalId, contextEpoch: '1', threadEventSequence: '1', digest: 'a'.repeat(64) },
      pageDigest: 'b'.repeat(64), localContext: '', hasMore: false, omittedForBudget: false, nextBeforeSequence: null,
    },
  } };
  store.recordPrimaryBinding(reserved.id, page);
  const startedAt = new Date().toISOString();
  store.update(reserved.id, { mediaStartedAt: startedAt, ...(greeted ? { journey: { streamStartedAt: startedAt, greetingClipStartedAt: startedAt } } : {}) });
  const client: EligibleContextClient = {
    async fetch() { return page; },
    async assert(_binding, version) { return { bindingId, callSid, current: true, version }; },
    async delivery() { throw new Error('no_delivery'); },
  };
  const transport = new PreconnectTransport();
  const timeline: Array<{ type: string; audio?: string; content?: string; at: number }> = [];
  const send = transport.send.bind(transport);
  transport.send = event => {
    if (event.type === 'session.input_audio.append') onFrame?.();
    const record = event as { type: string; audio?: string; content?: string };
    timeline.push({ type: record.type, ...(record.audio ? { audio: record.audio } : {}), ...(record.content ? { content: record.content } : {}), at: Date.now() });
    send(event);
  };
  const logs: Array<Record<string, unknown>> = [];
  t.mock.method(console, 'info', (value: string) => { try { logs.push(JSON.parse(value)); } catch { /* Not an event. */ } });
  const voice = createGenerationVoice({ call: store.get(reserved.id)!, callDeadlineAt: new Date(Date.now() + 60_000).toISOString(),
    apiKey: 'synthetic-key', voice: 'marin', store, primary: {} as PrimaryApiClient, context: client,
    voiceFactory(options) { return createBrokerLiveSession({ ...options, connectionFactory: () => transport, closeTimeoutMs: 10 }); },
    onAudio() {}, onReady() {}, onClosed() {}, onPlaybackClear() {}, onProblem() {},
  });
  t.after(async () => { await voice.close(); store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  for (const frame of held) assert.equal(voice.appendAudio(frame), true);
  transport.open();
  for (let i = 0; i < 400 && !store.get(reserved.id)!.voiceOpening?.status; i++) await new Promise(resolve => setTimeout(resolve, 5));
  for (let i = 0; i < 400 && timeline.filter(event => event.type === 'session.input_audio.append').length < held.length; i++)
    await new Promise(resolve => setTimeout(resolve, 5));
  const instruction = timeline.findIndex(event => event.type === 'session.instructions.append');
  const audio = timeline.filter(event => event.type === 'session.input_audio.append');
  assert.equal(audio.length, held.length, 'every held frame reaches Live, in order');
  assert.deepEqual(audio.map(event => event.audio), held);
  return { store, callId: reserved.id, timeline, instruction, logs,
    /** Held frames that reached Live before the opening instruction. */
    framesBefore: timeline.slice(0, instruction).filter(event => event.type === 'session.input_audio.append').length,
    instructions: timeline.filter(event => event.type === 'session.instructions.append') };
}

test('outbound: a "Hello?" said while Live started reaches Live before the opening, which answers it inside the offer', async t => {
  // 0.3 s of silence after the greeting, a 0.4 s "Hello?", then 0.6 s of silence: all queued before Live went live.
  const held = [...Array(15).fill(silence), ...Array(20).fill(speech), ...Array(30).fill(silence)];
  const f = await opening(t, 'outbound', held);
  assert.equal(f.instructions.length, 1, 'one opening, so one Live turn');
  assert.equal(f.instructions[0].content, brokerOpening('outbound', false, false, false, true, true));
  assert.ok(f.framesBefore >= 35, `the whole "Hello?" was delivered first (${f.framesBefore} frames)`);
  assert.ok(f.framesBefore < held.length, 'the trailing silence is not waited for');
  assert.match(f.instructions[0].content!, /may have answered or said hello\. If so, acknowledge it briefly and, in that same reply,/);
  assert.equal(f.store.get(f.callId)!.voiceOpening?.status, 'accepted');
  const log = f.logs.find(event => event.event === 'broker_opening_after_caller_speech');
  assert.ok(log); assert.equal(log.heldSpeechInputMs, 700);
});

test('outbound: held silence never delays the opening, and the opening keeps its plain continuation', async t => {
  const held = Array(50).fill(silence); // 1 s of silence at 1.5x would hold the offer about 0.67 s.
  const f = await opening(t, 'outbound', held);
  assert.equal(f.instructions.length, 1);
  assert.equal(f.instructions[0].content, brokerOpening('outbound', false, false, false, true));
  assert.ok(f.framesBefore <= 3, `the opening went out at activation (${f.framesBefore} frames before it)`);
  assert.equal(f.logs.some(event => event.event === 'broker_opening_after_caller_speech'), false);
});

test('slow synchronous frame handling cannot consume the silence backlog before the opening', async t => {
  const realNow = Date.now; let frameTime = 0;
  t.mock.method(Date, 'now', () => realNow() + frameTime);
  const f = await opening(t, 'outbound', Array(50).fill(silence), true, () => { frameTime += 50; });
  assert.ok(f.framesBefore <= 3, `opening preceded by ${f.framesBefore} frames`);
  assert.equal(f.instructions.length, 1);
});

test('outbound without the clip: held speech still reaches Live first, and the spoken greeting answers it', async t => {
  const held = [...Array(10).fill(speech), ...Array(10).fill(silence)];
  const f = await opening(t, 'outbound', held, false);
  assert.equal(f.instructions[0].content, brokerOpening('outbound'));
  assert.ok(f.framesBefore >= 10);
});

test('inbound: the wait-for-the-caller opening goes first; the caller\'s held question follows it', async t => {
  const held = [...Array(10).fill(silence), ...Array(40).fill(speech)];
  const f = await opening(t, 'inbound', held);
  assert.equal(f.instructions[0].content, brokerOpening('inbound', false, false, false, true));
  assert.ok(f.framesBefore <= 3, `the inbound opening is not held back (${f.framesBefore} frames before it)`);
});
