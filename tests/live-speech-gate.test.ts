import assert from 'node:assert/strict';
import test from 'node:test';
import { createSpeechGate } from '../src/live/speechGate.js';

function fixture(t: test.TestContext) {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'], now: 0 });
  const releases: Array<{ audio: Buffer; played(heard: boolean): void }> = [];
  const events: Array<Record<string, unknown>> = [], steering: string[] = [];
  const gate = createSpeechGate({ release(audio, played) { releases.push({ audio, played }); },
    clear() {}, steer(text) { steering.push(text); }, event(value) { events.push(value); } });
  t.after(() => gate.close());
  let serial = 0;
  function segment(text?: string, transcriptEnd?: number) {
    const start = Date.now(), pcm = Buffer.alloc(960);
    for (let n = 0; n < pcm.length; n += 2) pcm.writeInt16LE(1000, n);
    if (text) gate.transcript(String(++serial), text, start, transcriptEnd ?? start + 400);
    for (let offset = 0; offset < 700; offset += 20) {
      gate.audio(offset < 400 ? pcm : Buffer.alloc(960), start + offset, start + offset + 20);
      t.mock.timers.tick(20);
    }
  }
  return { gate, releases, events, steering, segment };
}

test('money speech waits for all terminal sentences and playback; another money request keeps its gate', async t => {
  const f = fixture(t); f.gate.pending('provisional'); f.gate.pending('sale', ['provisional']); f.gate.pending('send');
  const heard = f.gate.deliver('sale', 'Nothing was sold. Your cash is unchanged.', true);
  f.segment('Nothing was sold.'); f.segment('Your cash is unchanged.');
  assert.equal(f.releases.length, 2);
  assert.equal(f.releases[0].audio.subarray(0, 3200).every(byte => byte === 206), true, 'release is original Live PCM converted to PCMU');
  f.releases[1].played(true); assert.equal(f.gate.has('sale'), true, 'last sentence alone cannot prove the first played');
  f.releases[0].played(true); assert.equal(await heard, true);
  assert.equal(f.gate.has('sale'), false); assert.equal(f.gate.active(), true);
  const sent = f.gate.deliver('send', 'Nothing was sent.', true); f.segment('Nothing was sent.');
  f.releases.at(-1)!.played(true); assert.equal(await sent, true); assert.equal(f.gate.active(), false);
});

test('new truth and cleared playback cannot be mistaken for a spoken terminal result', async t => {
  const f = fixture(t); f.gate.pending('sale');
  const old = f.gate.deliver('sale', 'The sale is being placed.', false); f.segment('The sale is being placed.');
  const final = f.gate.deliver('sale', 'Nothing was sold.', true);
  assert.equal(await old, false); f.releases[0].played(true);
  f.segment('The sale is being placed.'); assert.equal(f.releases.length, 1, 'superseded partial cannot play');
  f.segment('Nothing was sold.'); f.releases[1].played(false);
  assert.equal(await final, false); assert.equal(f.gate.active(), true);
  f.releases[1].played(true); assert.equal(f.gate.active(), true, 'late mark after clear cannot reopen speech');
  const retry = f.gate.deliver('sale', 'Nothing was sold.', true); f.segment('Nothing was sold.');
  f.releases[2].played(true); assert.equal(await retry, true); assert.equal(f.gate.active(), false);
});

test('a transcript extending beyond an audio segment never licenses that incomplete speech', t => {
  const f = fixture(t); f.gate.pending('sale'); f.gate.deliver('sale', 'Nothing was sold.', true);
  f.segment('Nothing was sold.', 1600); t.mock.timers.tick(1200); t.mock.timers.tick(1500);
  assert.equal(f.releases.length, 0); assert.equal(f.gate.active(), true);
  assert.ok(f.events.some(event => event.reason === 'transcript_timeout'));
});

test('all delegated work blocks unrelated speech and unobserved stages but allows its initial hold', t => {
  const f = fixture(t); f.gate.pending('sale');
  f.segment('Hello!'); assert.equal(f.releases.length, 0);
  f.segment('On it.'); assert.equal(f.releases.length, 1);
  f.segment('On it.'); f.segment('I found SpaceX.'); f.segment('Done.');
  assert.equal(f.releases.length, 1); assert.equal(f.gate.active(), true);
  f.gate.close(); const before = f.steering.length; t.mock.timers.tick(20_000);
  assert.equal(f.steering.length, before, 'hangup clears holding timer');
});


test('money speech preserves signed amounts and punctuated asset names exactly', t => {
  const f = fixture(t); f.gate.pending('trade');
  f.gate.deliver('trade', 'ABC-X changed by -$1.00.', false);
  f.segment('ABC-X changed by $1.00.'); f.segment('ABC X changed by -$1.00.');
  assert.equal(f.releases.length, 0);
  f.segment('ABC-X changed by -$1.00.'); assert.equal(f.releases.length, 1);
});


test('a waiting terminal result is steered again instead of replaced by a holding line', t => {
  const f = fixture(t); f.gate.pending('sale'); t.mock.timers.tick(5000);
  assert.match(f.steering.at(-1)!, /On it/);
  f.gate.deliver('sale', 'Nothing was sold.', true); t.mock.timers.tick(5000);
  assert.match(f.steering.at(-1)!, /Read the latest supplied Primary result exactly/);
  f.segment('On it.'); assert.equal(f.releases.length, 0, 'old holding permission was withdrawn by the result');
  f.segment('Nothing was sold.'); assert.equal(f.releases.length, 1);
});


for (const [spoken, delivered] of [
  ['Your cash.', 'Your cash is unchanged.'],
  ['Sold BONK.', 'Sold BONK, but did not sell BONK.'],
]) test(`a relay cannot omit delivered polarity or become a fragment: ${spoken}`, t => {
  const f = fixture(t); f.gate.pending('r'); f.gate.deliver('r', delivered, true);
  f.segment(spoken); assert.equal(f.releases.length, 0);
  assert.equal(f.gate.active(), true);
});

test('release of an unadmitted delegation leaves other pending playback intact', async t => {
  const f = fixture(t); f.gate.pending('sale'); f.gate.pending('unadmitted');
  const result = f.gate.deliver('sale', 'Nothing was sold.', true); f.segment('Nothing was sold.');
  f.gate.release('unadmitted'); assert.equal(f.gate.has('sale'), true);
  f.releases[0].played(true); assert.equal(await result, true); assert.equal(f.gate.active(), false);
});
