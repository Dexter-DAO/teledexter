import assert from 'node:assert/strict';
import test from 'node:test';
import type { ClientEvent } from 'openai/resources/live/live';
import { LiveProbeError, probeLive, type LiveProbeConnection } from '../src/broker/preflight.js';

class FakeConnection implements LiveProbeConnection {
  open = () => {};
  event: (event: unknown) => void = () => {};
  error: (error: unknown) => void = () => {};
  closed = () => {};
  commands: ClientEvent[] = [];
  closes = 0;
  terminations = 0;
  onOpen(listener: () => void) { this.open = listener; }
  onEvent(listener: (event: unknown) => void) { this.event = listener; }
  onError(listener: (error: unknown) => void) { this.error = listener; }
  onClose(listener: () => void) { this.closed = listener; }
  send(event: ClientEvent) { this.commands.push(event); }
  close() { this.closes++; }
  terminate() { this.terminations++; }
  start() {
    this.open();
    this.event({ type: 'session.started', session: { id: 'live_probe_test', model: 'gpt-live-1' } });
  }
  audio() { this.event({ type: 'session.output_audio.delta', delta: Buffer.alloc(160, 42).toString('base64') }); }
  finalize() {
    this.event({ type: 'session.closed', reason: 'close_requested', session: { id: 'live_probe_test', model: 'gpt-live-1' }, usage: {} });
  }
}

function probe(connection: FakeConnection, timeoutMs = 1000) {
  return probeLive({ apiKey: 'synthetic-private-key', voice: 'marin', timeoutMs, connectionFactory: () => connection });
}

const hasCode = (code: string) => (error: unknown) => error instanceof LiveProbeError && error.code === code;

test('requires non-silent audio and a final session receipt; client mode has no backend', async () => {
  const connection = new FakeConnection();
  const result = probe(connection);
  connection.start();
  assert.deepEqual(connection.commands[0], {
    type: 'session.start', event_id: 'probe_start',
    session: {
      model: 'gpt-live-1',
      instructions: 'This is a brief voice connection check. Say "Voice connection ready" once now. Do not delegate or ask a question.',
      audio: { format: { type: 'audio/pcmu', rate: 8000 }, output: { voice: 'marin' } },
      delegation: { type: 'client' },
    },
  });
  assert.equal(connection.commands.length, 1);
  connection.audio();
  assert.equal(connection.commands[1]?.type, 'session.close');
  assert.equal(connection.closes, 0);
  connection.audio();
  assert.equal(connection.commands.length, 2);
  connection.finalize();
  assert.deepEqual(await result, { model: 'gpt-live-1', sessionId: 'live_probe_test', audioBytes: 320, finalized: true });
  assert.equal(connection.closes, 1);
  assert.equal(connection.terminations, 1);
});

test('paces silent input after startup and stops it before closing on audible output', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval'] });
  const connection = new FakeConnection();
  let frames = 0;
  connection.send = (event) => {
    connection.commands.push(event);
    if (event.type !== 'session.input_audio.append') return;
    frames++;
    assert.deepEqual(Buffer.from(event.audio, 'base64'), Buffer.alloc(160, 0xff));
    // Emulate a peer whose processing depends on incoming media frames. A
    // leading silence chunk must not cause an early close of the probe.
    if (frames === 2) connection.event({ type: 'session.output_audio.delta', delta: event.audio });
    if (frames === 3) connection.audio();
  };
  const result = probe(connection);
  connection.open();
  t.mock.timers.tick(40);
  assert.equal(frames, 0);
  connection.event({ type: 'session.started', session: { id: 'live_probe_test', model: 'gpt-live-1' } });
  t.mock.timers.tick(19);
  assert.equal(frames, 0);
  t.mock.timers.tick(1);
  assert.equal(frames, 1);
  t.mock.timers.tick(20);
  assert.equal(frames, 2);
  assert.equal(connection.commands.some((event) => event.type === 'session.close'), false);
  t.mock.timers.tick(20);
  assert.equal(frames, 3);
  assert.equal(connection.commands.at(-1)?.type, 'session.close');
  assert.equal(connection.commands.some((event) => event.type === 'session.instructions.append'), false);
  t.mock.timers.tick(200);
  assert.equal(frames, 3);
  assert.equal(connection.closes, 0);
  connection.finalize();
  assert.equal((await result).audioBytes, 320);
  const finalCommandCount = connection.commands.length;
  t.mock.timers.tick(2000);
  connection.start(); connection.audio();
  assert.equal(connection.commands.length, finalCommandCount);
  assert.equal(connection.terminations, 1);
});

test('silence and low-energy output do not qualify or request an early close', async () => {
  const connection = new FakeConnection();
  const result = probe(connection);
  connection.start();
  for (const value of [0xff, 0x7f, 0xfe]) {
    connection.event({ type: 'session.output_audio.delta', delta: Buffer.alloc(160, value).toString('base64') });
  }
  connection.event({ type: 'session.output_transcript.delta', delta: 'Voice connection ready' });
  assert.equal(connection.commands.some((event) => event.type === 'session.close'), false);
  connection.finalize();
  await assert.rejects(result, hasCode('live_no_audio'));
});

test('deadline stops active input pacing and never reconnects or restarts', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval'] });
  const connection = new FakeConnection();
  const result = probe(connection, 65);
  connection.start();
  t.mock.timers.tick(20);
  t.mock.timers.tick(20);
  t.mock.timers.tick(20);
  assert.equal(connection.commands.filter((event) => event.type === 'session.input_audio.append').length, 3);
  t.mock.timers.tick(5);
  await assert.rejects(result, hasCode('live_preflight_timeout'));
  const count = connection.commands.length;
  t.mock.timers.tick(500);
  connection.start(); connection.audio(); connection.finalize();
  assert.equal(connection.commands.length, count);
  assert.equal(connection.terminations, 1);
});

test('a session-start acknowledgment arriving after timeout cannot create an input timer', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval'] });
  const intervals = t.mock.method(globalThis, 'setInterval');
  const connection = new FakeConnection();
  const result = probe(connection, 25);
  connection.open();
  t.mock.timers.tick(25);
  await assert.rejects(result, hasCode('live_preflight_timeout'));
  connection.event({ type: 'session.started', session: { id: 'live_probe_test', model: 'gpt-live-1' } });
  assert.equal(intervals.mock.callCount(), 0);
  t.mock.timers.tick(200);
  assert.equal(connection.commands.length, 1);
  assert.equal(connection.terminations, 1);
});

test('provider and transport failures stop active input pacing', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval'] });
  for (const failure of ['provider', 'transport', 'send'] as const) {
    const connection = new FakeConnection();
    const result = probe(connection);
    connection.start();
    t.mock.timers.tick(20);
    if (failure === 'provider') connection.event({ type: 'error', error: { code: 'server_error' } });
    else if (failure === 'transport') connection.closed();
    else {
      connection.send = () => { throw new Error('synthetic-private-key'); };
      t.mock.timers.tick(20);
    }
    await assert.rejects(result, hasCode(failure === 'provider' ? 'server_error'
      : failure === 'transport' ? 'live_closed_without_finalization' : 'live_transport_error'));
    const count = connection.commands.length;
    connection.send = (event) => { connection.commands.push(event); };
    t.mock.timers.tick(2000);
    assert.equal(connection.commands.length, count);
    assert.equal(connection.terminations, 1);
  }
});

test('opening and closing a quiet session is insufficient', async () => {
  const connection = new FakeConnection();
  const result = probe(connection);
  connection.start();
  connection.finalize();
  await assert.rejects(result, hasCode('live_no_audio'));
});

test('audio without finalization fails on transport closure', async () => {
  const connection = new FakeConnection();
  const result = probe(connection);
  connection.start(); connection.audio(); connection.closed();
  await assert.rejects(result, hasCode('live_closed_without_finalization'));
});

test('a final receipt for another session cannot qualify the probe', async () => {
  const connection = new FakeConnection();
  const result = probe(connection);
  connection.start(); connection.audio();
  connection.event({ type: 'session.closed', session: { id: 'live_other' }, usage: {} });
  await assert.rejects(result, hasCode('live_session_mismatch'));
});

test('deadline terminates startup and unfinalized audio without retries', async () => {
  for (const started of [false, true]) {
    const connection = new FakeConnection();
    const result = probe(connection, 15);
    if (started) { connection.start(); connection.audio(); }
    await assert.rejects(result, hasCode('live_preflight_timeout'));
    assert.equal(connection.terminations, 1);
    connection.open(); connection.event({ type: 'session.closed' });
    assert.equal(connection.commands.filter((event) => event.type === 'session.start').length, started ? 1 : 0);
  }
});

test('billing failure retains a useful code but never provider details or credentials', async () => {
  const connection = new FakeConnection();
  const result = probe(connection);
  connection.error({ code: 'credit_balance_exhausted', message: 'synthetic-private-key +15555550123', cause: 'secret' });
  await assert.rejects(result, (error: unknown) => {
    assert.ok(error instanceof LiveProbeError);
    assert.equal(error.code, 'credit_balance_exhausted');
    assert.doesNotMatch(String(error.stack), /synthetic-private-key|15555550123|secret/);
    assert.equal(error.cause, undefined);
    return true;
  });
});

test('provider-controlled unknown codes and send failures cannot leak details', async () => {
  const connection = new FakeConnection();
  const result = probe(connection);
  connection.event({ type: 'error', error: { code: 'synthetic-private-key', message: 'private' } });
  await assert.rejects(result, hasCode('live_provider_error'));
  const failing = new FakeConnection();
  failing.send = () => { throw new Error('synthetic-private-key'); };
  const failed = probe(failing); failing.open();
  await assert.rejects(failed, hasCode('live_transport_error'));
});

test('rejects malformed or premature audio and an unexpected resolved model', async () => {
  for (const delta of ['', '%%%%', 'AB==']) {
    const connection = new FakeConnection();
    const result = probe(connection); connection.start();
    connection.event({ type: 'session.output_audio.delta', delta });
    await assert.rejects(result, hasCode('live_invalid_audio'));
  }
  const early = new FakeConnection();
  const earlyResult = probe(early); early.audio();
  await assert.rejects(earlyResult, hasCode('live_audio_before_session'));
  const mismatch = new FakeConnection();
  const mismatchResult = probe(mismatch); mismatch.open();
  mismatch.event({ type: 'session.started', session: { id: 'live_test', model: 'other-model' } });
  await assert.rejects(mismatchResult, hasCode('live_session_mismatch'));
});

test('invalid configuration never constructs a connection', async () => {
  let connections = 0;
  const connectionFactory = () => { connections++; return new FakeConnection(); };
  for (const timeoutMs of [0, -1, 15_001, NaN]) {
    await assert.rejects(probeLive({ apiKey: 'synthetic', voice: 'marin', timeoutMs, connectionFactory }), hasCode('live_preflight_invalid_configuration'));
  }
  await assert.rejects(probeLive({ apiKey: '', voice: 'marin', connectionFactory }), hasCode('live_preflight_invalid_configuration'));
  assert.equal(connections, 0);
});
