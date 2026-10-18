import { readFileSync } from 'node:fs';
import assert from 'node:assert/strict';
import test from 'node:test';
import { createSpeechGate } from '../src/live/speechGate.js';

const sale = JSON.parse(readFileSync(new URL('./fixtures/call-truth-oct18.json', import.meta.url), 'utf8')).sale;
const words = sale.fragments.filter((f: any) => f.role !== 'user' && f.startMs >= 58000 && f.startMs < 62100);
const delivered = words.map((f: any) => f.text).join('').trim();

function run(t: test.TestContext, voicedAt: (ms: number) => boolean, label: string, textLag = 0) {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'], now: 0 });
  const rel: Array<{ at: number; text?: string }> = [], ev: any[] = [];
  const gate = createSpeechGate({ release(_a, played) { rel.push({ at: Date.now() }); played(true); }, clear() {}, steer() {}, event(e) { ev.push(e); } });
  t.after(() => gate.close());
  t.mock.timers.tick(57000);
  gate.pending('r'); gate.deliver('r', delivered, false);
  const pcm = Buffer.alloc(960); for (let n = 0; n < 960; n += 2) pcm.writeInt16LE(1000, n);
  for (let ms = 58000; ms < 63500; ms += 20) {
    t.mock.timers.tick(20);
    gate.audio(voicedAt(ms) ? pcm : Buffer.alloc(960), ms, ms + 20);
    for (const w of words) if (w.endMs + textLag === ms + 20) gate.transcript(w.eventId, w.text, w.startMs, w.endMs);
  }
  t.mock.timers.tick(3000);
  const released = ev.filter(e => e.type === 'live.speech_gate.released').map(e => `${JSON.stringify(e.text)}@${e.latencyMs}ms`);
  const discarded = ev.filter(e => e.type === 'live.speech_gate.discarded').map(e => `${e.reason}[${e.startMs}-${e.endMs}]`);
  t.diagnostic(`${label}: delivered=${JSON.stringify(delivered)} released=${JSON.stringify(released)} discarded=${JSON.stringify(discarded)}`);
  return { released, discarded };
}

test('RV cadence A: real word-delta timing, continuous voiced audio per sentence', t => {
  const r = run(t, ms => (ms >= 58000 && ms < 60000) || (ms >= 60400 && ms < 62000), 'A');
  assert.equal(r.released.length, 2, 'faithful word-level sentences not released');
});
test('RV cadence B: real word-delta timing, audio voiced only inside delta ranges', t => {
  const r = run(t, ms => words.some((w: any) => ms >= w.startMs && ms < w.endMs), 'B');
  assert.ok(r.released.length >= 1, 'nothing released');
});
test('RV cadence C: like A, transcript deltas lag audio by 200 ms', t => {
  const r = run(t, ms => (ms >= 58000 && ms < 60000) || (ms >= 60400 && ms < 62000), 'C', 200);
  assert.equal(r.released.length, 2);
});


for (const [start, secondStart, expected] of [[0, 1000, 1], [200, 1000, 0], [0, 1400, 0]]) {
  test(`cadence retains start coverage and caps internal gaps: ${start}/${secondStart}`, t => {
    t.mock.timers.enable({ apis: ['setTimeout', 'Date'], now: 0 });
    let released = 0;
    const gate = createSpeechGate({ release() { released++; }, clear() {}, steer() {}, event() {} });
    t.after(() => gate.close()); gate.pending('r'); gate.deliver('r', 'Nothing was sold.', true);
    gate.transcript('a', 'Nothing ', start, start + 200);
    gate.transcript('b', 'was sold.', secondStart, secondStart + 200);
    const pcm = Buffer.alloc(960); for (let n = 0; n < 960; n += 2) pcm.writeInt16LE(1000, n);
    for (let ms = 0; ms < secondStart + 500; ms += 20) {
      gate.audio(ms < secondStart + 200 ? pcm : Buffer.alloc(960), ms, ms + 20); t.mock.timers.tick(20);
    }
    t.mock.timers.tick(1200); t.mock.timers.tick(1500);
    assert.equal(released, expected);
  });
}
