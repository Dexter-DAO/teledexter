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

test('requires generated audio and a final session receipt; client mode has no backend', async () => {
  const connection = new FakeConnection();
  const result = probe(connection);
  connection.start();
  assert.deepEqual(connection.commands[0], {
    type: 'session.start', event_id: 'probe_start',
    session: {
      model: 'gpt-live-1',
      instructions: 'This is a brief voice connection check. Follow the application instruction to speak once. No backend work is needed.',
      audio: { format: { type: 'audio/pcmu', rate: 8000 }, output: { voice: 'marin' } },
      delegation: { type: 'client' },
    },
  });
  assert.equal(connection.commands[1]?.type, 'session.instructions.append');
  connection.event({ type: 'session.instructions.appended', client_event_id: 'probe_speak' });
  assert.equal(connection.commands.length, 2);
  connection.audio();
  assert.equal(connection.commands[2]?.type, 'session.close');
  assert.equal(connection.closes, 0);
  connection.audio();
  assert.equal(connection.commands.length, 3);
  connection.finalize();
  assert.deepEqual(await result, { model: 'gpt-live-1', sessionId: 'live_probe_test', audioBytes: 320, finalized: true });
  assert.equal(connection.closes, 1);
  assert.equal(connection.terminations, 1);
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
