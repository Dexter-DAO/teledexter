import assert from 'node:assert/strict';
import test from 'node:test';
import { createSpeechGate } from '../src/live/speechGate.js';

test('RV M3: stubborn wrong relay -> holds; worst-case silence', t => {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'], now: 0 });
  const rel: Array<{ at: number; played(h: boolean): void }> = [], ev: any[] = [], steer: Array<[number, string]> = [];
  const gate = createSpeechGate({ release(_a, played) { rel.push({ at: Date.now(), played }); setTimeout(() => played(true), 50); }, clear() {}, steer(s) { steer.push([Date.now(), s]); }, event(e) { ev.push(e); } });
  t.after(() => gate.close());
  const pcm = Buffer.alloc(960); for (let n = 0; n < 960; n += 2) pcm.writeInt16LE(1000, n);
  let serial = 0;
  const say = (text: string, ms: number) => { const s = Date.now(); gate.transcript(String(++serial), text, s, s + ms);
    for (let o = 0; o < ms + 300; o += 20) { gate.audio(o < ms ? pcm : Buffer.alloc(960), s + o, s + o + 20); t.mock.timers.tick(20); } };
  gate.pending('sale'); t.mock.timers.tick(3000);
  gate.deliver('sale', "The SpaceX sale didn't go through. Nothing was sold.", true);
  const t0 = Date.now(); let seen = 0;
  while (Date.now() - t0 < 40_000) {
    t.mock.timers.tick(500);
    const hold = steer.slice(seen).find(([, s]) => /Say exactly "([^"]+)"/.test(s)); seen = steer.length;
    if (hold) say(/Say exactly "([^"]+)"/.exec(hold[1])![1], 600); else say("The sale's done.", 1500);
  }
  const onsets = rel.map(r => r.at - t0);
  const gaps = [onsets[0], ...onsets.slice(1).map((v, i) => v - onsets[i])];
  const failed = ev.filter(e => e.type === 'broker_speech_relay_failed');
  t.diagnostic(`releases at ${JSON.stringify(onsets)} ms after result; max silence=${Math.max(...gaps)} ms; relay_failed events=${failed.length} first=${JSON.stringify(failed[0])}; steer kinds=${JSON.stringify(steer.map(([a, s]) => [a - t0, s.slice(0, 40)]))}`);
  assert.ok(rel.length > 0);
  assert.equal(failed.length, 1, 'metric is once per result, not once per attempt');
  assert.ok(Math.max(...gaps) <= 6000, 'neutral holds remain bounded in this probe');
});
