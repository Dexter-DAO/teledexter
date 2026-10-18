import assert from 'node:assert/strict';
import test from 'node:test';
import { PlaybackReceipts } from '../src/broker/phone.js';

test('call truth: clear resets queued receipts and late marks never become played audio', () => {
  const receipts = new PlaybackReceipts();
  const marks = Array.from({ length: 301 }, () => receipts.sent(160, true, true));
  assert.ok(receipts.backlogMs > 6000);
  receipts.clear();
  assert.equal(receipts.pendingCount, 0);
  assert.equal(receipts.backlogMs, 0);
  for (const mark of marks) receipts.acknowledge(mark);
  assert.equal(receipts.playedBytes, 0);
  const next = receipts.sent(160);
  receipts.acknowledge(next);
  assert.equal(receipts.playedBytes, 160);
  assert.equal(receipts.backlogMs, 0);
});

import { readFileSync } from 'node:fs';

test('call truth: toilet follow-up carries both sides of this call, within six turns and ninety seconds', async () => {
  const { conversationContext } = await import('../src/broker/conversationContext.js');
  const call = JSON.parse(readFileSync(new URL('./fixtures/call-truth-oct18.json', import.meta.url), 'utf8')).toilet;
  const request = call.requests.at(-1);
  const first = call.fragments.find((value: { sequence: number }) => value.sequence === request.firstSequence);
  const context = conversationContext(call.fragments, first.sequence, first.startMs)!;
  assert.ok(context.recentSpeech.some(value => value.role === 'dexter' && /toilet/i.test(value.text)), 'Dexter toilet discussion is context');
  assert.ok(context.recentSpeech.some(value => value.role === 'caller'), 'caller side is retained');
  assert.ok(context.recentSpeech.length <= 6, 'turn bound');
  assert.equal(context.recentSpeech.some(value => /SpaceX/.test(value.text)), false);
  const many = Array.from({ length: 12 }, (_, i) => ({ eventId: `e${i}`, sequence: i + 1,
    role: (i % 2 ? 'user' : 'assistant') as 'user' | 'assistant', text: '😀'.repeat(2500), startMs: i * 20000, endMs: i * 20000 + 100 }));
  const bounded = conversationContext(many, 13, 240000)!;
  assert.ok(bounded.recentSpeech.length <= 5, 'ninety-second bound');
  assert.ok(bounded.recentSpeech.reduce((n, turn) => n + turn.text.length, 0) <= 6000, 'total character bound');
});

test('call truth: reflected PCM conversion preserves silence, polarity and phone sample count', async () => {
  const { pcmToPcmu } = await import('../src/live/pcm.js');
  const pcm = Buffer.alloc(18);
  for (let i = 6; i < 12; i += 2) pcm.writeInt16LE(1000, i);
  for (let i = 12; i < 18; i += 2) pcm.writeInt16LE(-1000, i);
  assert.deepEqual([...pcmToPcmu(pcm)], [255, 206, 78]);
});




test('context strips NUL characters on both sides before bounding and transport', async () => {
  const { conversationContext } = await import('../src/broker/conversationContext.js');
  const context = conversationContext([
    { eventId: 'a', sequence: 1, role: 'user', text: 'Find\0 it', startMs: 0, endMs: 100 },
    { eventId: 'b', sequence: 2, role: 'assistant', text: 'The\0 toilet', startMs: 100, endMs: 200 },
  ], 3, 300);
  assert.deepEqual(context?.recentSpeech.map(turn => turn.text), ['Find it', 'The toilet']);
});

test('a persistently blocked media socket stops after ten seconds; draining and close clear the watchdog', async t => {
  const { watchMediaSocket } = await import('../src/broker/phone.js');
  t.mock.timers.enable({ apis: ['setInterval', 'Date'], now: 0 });
  const socket = { bufferedAmount: 300000 }; let stopped = 0;
  const close = watchMediaSocket(socket, () => stopped++);
  const tick = (ms: number) => { for (let at = 0; at < ms; at += 250) t.mock.timers.tick(250); };
  tick(9000); assert.equal(stopped, 0);
  socket.bufferedAmount = 0; tick(250); socket.bufferedAmount = 300000;
  tick(10000); assert.equal(stopped, 0); tick(250); assert.equal(stopped, 1);
  close(); tick(20000); assert.equal(stopped, 1);
  const closeSecond = watchMediaSocket(socket, () => stopped++); tick(250); closeSecond(); tick(20000); assert.equal(stopped, 1);
});


test('context strips lone surrogates and preserves paired emoji', async () => {
  const { conversationContext } = await import('../src/broker/conversationContext.js');
  const context = conversationContext([
    { eventId: 'a', sequence: 1, role: 'user', text: 'Find\uD800 it\0 😀', startMs: 0, endMs: 100 },
    { eventId: 'b', sequence: 2, role: 'assistant', text: 'The\uDC00 toilet 🚽', startMs: 100, endMs: 200 },
  ], 3, 300);
  assert.deepEqual(context?.recentSpeech.map(turn => turn.text), ['Find it 😀', 'The toilet 🚽']);
});
