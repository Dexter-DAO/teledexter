import { readFileSync } from 'node:fs';
import assert from 'node:assert/strict';
import test from 'node:test';
import { createSpeechGate } from '../src/live/speechGate.js';

function fixture(t: test.TestContext) {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'], now: 0 });
  const releases: Array<{ audio: Buffer; played(heard: boolean): void }> = [];
  const events: Array<Record<string, unknown>> = [], steering: string[] = []; let clears = 0;
  const gate = createSpeechGate({ release(audio, played) { releases.push({ audio, played }); },
    clear() { clears++; }, steer(text) { steering.push(text); }, event(value) { events.push(value); } });
  t.after(() => gate.close());
  const pcm = Buffer.alloc(960); for (let n = 0; n < pcm.length; n += 2) pcm.writeInt16LE(1000, n);
  let serial = 0;
  function segment(text?: string, ms = 400) {
    const start = Date.now();
    if (text) gate.transcript(String(++serial), text, start, start + ms);
    for (let o = 0; o < ms + 300; o += 20) { gate.audio(o < ms ? pcm : Buffer.alloc(960), start + o, start + o + 20); t.mock.timers.tick(20); }
  }
  return { gate, releases, events, steering, segment, pcm, clears: () => clears, next: () => String(++serial) };
}

test('PROBE coverage: a late tail transcript must hold the whole segment', t => {
  const f = fixture(t); f.gate.pending('sale'); f.gate.deliver('sale', 'Nothing was sold.', true);
  const start = Date.now();
  f.gate.transcript('a', 'Nothing was sold.', start, start + 400);
  for (let o = 0; o < 1100; o += 20) { f.gate.audio(o < 800 ? f.pcm : Buffer.alloc(960), start + o, start + o + 20); t.mock.timers.tick(20); }
  t.mock.timers.tick(1200); // New quiet close deadline; tail still must be covered.
  const releasedBeforeTail = f.releases.length;
  f.gate.transcript('b', ' Buying it back now.', start + 400, start + 800);
  t.mock.timers.tick(2000);
  assert.equal(releasedBeforeTail, 0, 'segment with untranscribed tail released');
  assert.equal(f.releases.length, 0);
});

test('PROBE alias: dispatch re-key does not cut an approved hold', t => {
  const f = fixture(t); f.gate.pending('del_1'); t.mock.timers.tick(5000);
  f.segment('On it.'); assert.equal(f.releases.length, 1);
  const before = f.clears(); f.gate.pending('req_1', ['del_1']);
  assert.equal(f.clears(), before, `clear() called ${f.clears() - before}x on alias re-key`);
});

test('PROBE two requests: B partial while A terminal plays', async t => {
  const f = fixture(t); f.gate.pending('A'); f.gate.pending('B');
  const a = f.gate.deliver('A', 'Nothing was sold.', true); f.segment('Nothing was sold.');
  assert.equal(f.releases.length, 1);
  const b = f.gate.deliver('B', 'The send is being attempted.', false);
  f.releases[0].played(true);
  const heardA = await a;
  t.diagnostic(`A heard=${heardA}; A still pending=${f.gate.has('A')}; clears=${f.clears()}`);
  assert.equal(heardA, true, 'A terminal invalidated by B partial; A must be retried');
  void b;
});

test('PROBE ignored steering: silence after terminal result is unbounded', t => {
  const f = fixture(t); f.gate.pending('sale'); f.gate.deliver('sale', 'The SpaceX sale did not go through.', true);
  for (let i = 0; i < 12; i++) { f.segment('Unfortunately the SpaceX sale did not go through.', 1500); t.mock.timers.tick(3200); }
  const holdSteers = f.steering.filter(s => /Say exactly/.test(s)).length;
  t.diagnostic(`after ~${Date.now()/1000}s: releases=${f.releases.length}, steers=${f.steering.length}, holdLines=${holdSteers}`);
  assert.ok(f.releases.length > 0, 'caller heard nothing');
});

test('PROBE case: case-only transcript difference', t => {
  const f = fixture(t); f.gate.pending('sale'); f.gate.deliver('sale', 'Your SpaceX stock is unchanged.', true);
  f.segment('Your Spacex stock is unchanged.');
  assert.equal(f.releases.length, 1, 'case difference discarded');
});


test('continuous multi-sentence final, including case changes, releases once and opens after playback', async t => {
  const f = fixture(t); f.gate.pending('research');
  const text = 'The SpaceX sale did not go through. Nothing was sold. Your cash is unchanged.';
  const heard = f.gate.deliver('research', text, true);
  f.segment('Unfortunately, the Spacex sale did not go through. Nothing was sold. Your cash is unchanged.', 1500);
  assert.equal(f.releases.length, 1);
  f.releases[0].played(true); assert.equal(await heard, true); assert.equal(f.gate.active(), false);
  f.releases[0].played(true); assert.equal(f.releases.length, 1);
});

test('two failed final attempts restore alternating holds, log counts and keep retrying the result', t => {
  const f = fixture(t); f.gate.pending('sale'); f.gate.deliver('sale', 'Nothing was sold.', true);
  for (let i = 0; i < 2; i++) { f.segment('The sale is done.', 2000); t.mock.timers.tick(200); }
  f.segment('On it.', 400);
  assert.equal(f.releases.length, 1);
  assert.ok(f.events.some(e => e.type === 'broker_speech_relay_failed' && e.discardedAttempts === 2));
  f.releases[0].played(true); t.mock.timers.tick(5000); f.segment('Still working on it.', 400);
  assert.equal(f.releases.length, 2);
  assert.match(f.steering.join(' '), /Nothing was sold/);
  assert.equal(f.gate.active(), true);
  t.diagnostic(`two discarded 2-second attempts: first approved hold at ${f.events.find(e => e.type === 'live.speech_gate.released')?.latencyMs}ms gate latency, 5400ms after result; next hold after 5s idle plus 400ms speech buffer. Scripted provider obeys hold steering.`);
});

test('status during pending work immediately permits a neutral response and steers latest progress', t => {
  const f = fixture(t); f.gate.pending('research'); t.mock.timers.tick(2000);
  f.gate.callerSpoke("How's it going?"); assert.match(f.steering.at(-1)!, /Say exactly/);
  f.segment('Still working on it.', 600); assert.equal(f.releases.length, 1);
  f.gate.deliver('research', 'I found the toilet guide.', false);
  f.gate.statusRequested(); assert.match(f.steering.at(-1)!, /I found the toilet guide/);
  f.segment('I found the toilet guide.'); assert.equal(f.releases.length, 2);
});

test('ten-second uninterrupted answer releases at aligned sentence boundaries without 300ms pauses', t => {
  const f = fixture(t); f.gate.pending('research');
  const parts = ['The SpaceX sale did not go through.', 'Nothing was sold.', 'The buy was not started.', 'Your cash is unchanged.', 'Your SpaceX stock is unchanged.'];
  f.gate.deliver('research', parts.join(' '), true);
  // Separate reflected transcript ranges supply actual sentence boundaries.
  parts.forEach((text, i) => f.gate.transcript(String(i), text, i * 2000, (i + 1) * 2000));
  for (let at = 0; at < 10020; at += 20) { f.gate.audio(at < 10000 ? f.pcm : Buffer.alloc(960), at, at + 20); t.mock.timers.tick(20); }
  assert.equal(f.releases.length, 5);
  const released = f.events.filter(e => e.type === 'live.speech_gate.released');
  assert.deepEqual(released.map(e => e.latencyMs), [2000, 2000, 2000, 2000, 2000]);
  t.diagnostic(`10s continuous answer: per-sentence buffer latency ${released.map(e => e.latencyMs)}ms; first release 2000ms; no pause penalty. Whole-answer buffering previously 10300ms.`);
});

test('fact and polarity changes are never licensed by other clauses', t => {
  const f = fixture(t); f.gate.pending('sale');
  f.gate.deliver('sale', 'The SpaceX sale did not go through. Nothing was sold. The Apple buy was confirmed for $1.00 on 2026-10-18.', true);
  for (const text of ['The SpaceX sale did go through.', 'SpaceX was sold.', 'The Apple buy failed.', 'The Apple buy was confirmed for $10.00 on 2026-10-18.', 'The Apple buy was confirmed for $1.00 on 2026-10-19.', 'The BONK buy was confirmed for $1.00 on 2026-10-18.']) f.segment(text);
  assert.equal(f.releases.length, 0);
});


test('fact matching accepts a failure paraphrase without admitting its opposite', t => {
  const f = fixture(t); f.gate.pending('sale');
  f.gate.deliver('sale', "The SpaceX sale didn't go through.", true);
  f.segment('The Spacex sale was successful.'); assert.equal(f.releases.length, 0);
  f.segment('Unfortunately, the Spacex sale failed.'); assert.equal(f.releases.length, 1);
});

test('one whole-answer timestamp range remains buffered until its audio is covered', t => {
  const f = fixture(t); f.gate.pending('sale');
  const text = JSON.parse(readFileSync(new URL('./fixtures/call-truth-oct18.json', import.meta.url), 'utf8')).sale.requests[1].result;
  f.gate.deliver('sale', text, true); f.segment(text, 12000);
  assert.equal(f.releases.length, 1);
  const released = f.events.find(e => e.type === 'live.speech_gate.released')!;
  assert.equal(released.latencyMs, 12000);
  t.diagnostic('Whole-answer range, 12s audio: approved at 12000ms. The original review latency target of <2000ms requires finer alignment; buffering that complete range remains necessary for the coverage guard.');
});


test('outcome polarity stays bound to its action and company across a continuous relay', t => {
  const f = fixture(t); f.gate.pending('trades');
  f.gate.deliver('trades', 'The SpaceX sale failed. The Apple sale was done.', true);
  f.segment('The SpaceX sale was done. The Apple sale failed.'); assert.equal(f.releases.length, 0);
  f.segment('The Spacex sale failed. The Apple sale was done.'); assert.equal(f.releases.length, 1);
});

test('PROBE original coverage: an unsafe late tail cannot release a terminal prefix', t => {
  const f = fixture(t); f.gate.pending('sale'); f.gate.deliver('sale', 'Nothing was sold.', true);
  const start = Date.now();
  f.gate.transcript('a', 'Nothing was sold.', start, start + 400);
  for (let o = 0; o < 1100; o += 20) { f.gate.audio(o < 800 ? f.pcm : Buffer.alloc(960), start + o, start + o + 20); t.mock.timers.tick(20); }
  const releasedBeforeTail = f.releases.length;
  f.gate.transcript('b', ' Buying it back now.', start + 400, start + 800);
  t.mock.timers.tick(2000);
  assert.equal(releasedBeforeTail, 0, 'segment with untranscribed tail released');
  assert.equal(f.releases.length, 0);
});
