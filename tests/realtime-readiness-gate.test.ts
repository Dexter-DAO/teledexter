import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import test from 'node:test';
import {
  connectRealtimeSession,
  RealtimeReadinessGate,
  probeRealtimeAudioGeneration,
  type RealtimeSocketFactory,
} from '../src/outbound/RealtimeReadinessGate.js';

class FakeSocket extends EventEmitter {
  sent: unknown[] = [];
  closed = false;

  send(data: string): void {
    this.sent.push(JSON.parse(data));
  }

  close(): void {
    this.closed = true;
  }

  terminate(): void {
    this.closed = true;
  }
}

test('generation probe requires actual audio and never exposes the API key', async () => {
  const socket = new FakeSocket();
  let authorization = '';
  const factory: RealtimeSocketFactory = (url, options) => {
    assert.match(url, /model=gpt-realtime-2/u);
    assert.equal(options.headers['OpenAI-Beta'], undefined);
    authorization = options.headers.Authorization;
    queueMicrotask(() => socket.emit('message', JSON.stringify({ type: 'session.created' })));
    return socket as any;
  };
  const resultPromise = probeRealtimeAudioGeneration({
    apiKey: 'secret-key-value',
    model: 'gpt-realtime-2',
    voice: 'sage',
    socketFactory: factory,
  });
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(socket.sent, [{
    type: 'response.create',
    response: {
      conversation: 'none',
      input: [{
        type: 'message',
        role: 'user',
        content: [{ type: 'input_text', text: 'Say only: ready.' }],
      }],
      instructions: 'Say only the word ready.',
      max_output_tokens: 32,
      output_modalities: ['audio'],
      audio: { output: { voice: 'sage' } },
    },
  }]);
  socket.emit('message', JSON.stringify({
    type: 'response.output_audio.delta',
    delta: 'AQID',
  }));
  socket.emit('message', JSON.stringify({
    type: 'response.done',
    response: { status: 'completed' },
  }));
  const result = await resultPromise;
  assert.equal(result.ok, true);
  assert.match(result.checkedAt, /^\d{4}-\d{2}-\d{2}T/u);
  assert.equal(authorization, 'Bearer secret-key-value');
  assert.equal(socket.closed, true);
});

test('generation probe classifies quota failure and rejects text-only success', async () => {
  const quotaSocket = new FakeSocket();
  const quota = probeRealtimeAudioGeneration({
    apiKey: 'key',
    model: 'gpt-realtime-2',
    voice: 'sage',
    socketFactory: () => {
      queueMicrotask(() => {
        quotaSocket.emit('message', JSON.stringify({ type: 'session.created' }));
        quotaSocket.emit('message', JSON.stringify({
          type: 'error',
          error: { code: 'insufficient_quota', message: 'quota exhausted' },
        }));
      });
      return quotaSocket as any;
    },
  });
  const quotaResult = await quota;
  assert.equal(quotaResult.ok, false);
  if (!quotaResult.ok) assert.equal(quotaResult.reason, 'openai_quota_exhausted');

  const noAudioSocket = new FakeSocket();
  const noAudio = probeRealtimeAudioGeneration({
    apiKey: 'key',
    model: 'gpt-realtime-2',
    voice: 'sage',
    socketFactory: () => {
      queueMicrotask(() => {
        noAudioSocket.emit('message', JSON.stringify({ type: 'session.created' }));
        noAudioSocket.emit('message', JSON.stringify({
          type: 'response.done',
          response: { status: 'completed' },
        }));
      });
      return noAudioSocket as any;
    },
  });
  const noAudioResult = await noAudio;
  assert.equal(noAudioResult.ok, false);
  if (!noAudioResult.ok) assert.equal(noAudioResult.reason, 'openai_audio_missing');
});

test('readiness probes single-flight, cache, expire, and trip immediately', async () => {
  let nowMs = Date.parse('2026-08-31T00:00:00.000Z');
  let probes = 0;
  let release: (() => void) | null = null;
  const gate = new RealtimeReadinessGate({
    now: () => nowMs,
    successTtlMs: 1_000,
    failureTtlMs: 100,
    probe: async () => {
      probes += 1;
      await new Promise<void>((resolve) => { release = resolve; });
      return { ok: true, checkedAt: new Date(nowMs).toISOString() };
    },
  });
  const first = gate.check();
  const concurrent = gate.check();
  assert.equal(probes, 1);
  release?.();
  assert.equal((await first).status, 'ready');
  assert.equal((await concurrent).status, 'ready');
  assert.equal((await gate.check()).status, 'ready');
  assert.equal(probes, 1);

  nowMs += 1_001;
  const expired = gate.check();
  release?.();
  assert.equal((await expired).status, 'ready');
  assert.equal(probes, 2);

  gate.trip('openai_rate_limited');
  assert.deepEqual(gate.current(), {
    status: 'unavailable',
    checkedAt: new Date(nowMs).toISOString(),
    expiresAt: new Date(nowMs + 100).toISOString(),
    reason: 'openai_rate_limited',
  });
});

test('a stale successful probe cannot erase a fatal error from an actual call', async () => {
  let release: (() => void) | null = null;
  const gate = new RealtimeReadinessGate({
    failureTtlMs: 10_000,
    probe: async () => {
      await new Promise<void>((resolve) => { release = resolve; });
      return { ok: true, checkedAt: '2026-08-31T00:00:00.000Z' };
    },
  });

  const staleProbe = gate.check();
  gate.trip('openai_quota_exhausted');
  const tripped = gate.current();
  release?.();

  assert.deepEqual(await staleProbe, tripped);
  assert.deepEqual(gate.current(), tripped);
});

test('a post-probe session connection failure selects the Twilio fallback once', async () => {
  const providerError = new Error('socket closed after the readiness probe');
  let connectAttempts = 0;
  const result = await connectRealtimeSession(
    async () => {
      connectAttempts++;
      throw providerError;
    },
    () => null,
  );

  assert.equal(connectAttempts, 1);
  assert.deepEqual(result, {
    ok: false,
    reason: 'openai_connection_failed',
    error: providerError,
  });
});
