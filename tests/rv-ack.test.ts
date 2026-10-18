import test from 'node:test';
import assert from 'node:assert/strict';
import { createSpeechGate } from '../src/live/speechGate.js';
test('RV ack: spontaneous "On it." right after delegation', t => {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'], now: 0 });
  const rel: number[] = [], ev: any[] = [], steer: Array<[number, string]> = [];
  const gate = createSpeechGate({ release() { rel.push(Date.now()); }, clear() {}, steer(s) { steer.push([Date.now(), s]); }, event(e) { ev.push(e); } });
  t.after(() => gate.close());
  const pcm = Buffer.alloc(960); for (let n = 0; n < 960; n += 2) pcm.writeInt16LE(1000, n);
  gate.pending('del'); t.mock.timers.tick(200);
  const s = Date.now(); gate.transcript('a', 'On it.', s, s + 400);
  for (let o = 0; o < 700; o += 20) { gate.audio(o < 400 ? pcm : Buffer.alloc(960), s + o, s + o + 20); t.mock.timers.tick(20); }
  t.mock.timers.tick(6000);
  assert.equal(rel[0], 600);
  assert.equal(rel.length, 1);
  t.diagnostic(`released=${JSON.stringify(rel)} discarded=${JSON.stringify(ev.filter(e => e.type.endsWith('discarded')).map(e => e.reason))} first hold steer at ${steer[0]?.[0]} ms`);
});
