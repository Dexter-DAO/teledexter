import assert from 'node:assert/strict';
import test from 'node:test';
import type { TransportLayerAudio } from '@openai/agents/realtime';
import { EchoGuardedTwilioTransport } from '../src/session/EchoGuardedTransport.js';
import { classifyUserTurn } from '../src/session/echoTurnSuppressor.js';

class FakeTwilioSocket {
  readonly sent: any[] = [];
  readonly listeners = new Map<string, Array<(event: any) => void>>();

  addEventListener(type: string, listener: (event: any) => void): void {
    const listeners = this.listeners.get(type) ?? [];
    listeners.push(listener);
    this.listeners.set(type, listeners);
  }

  send(data: string): void {
    this.sent.push(JSON.parse(data));
  }

  receive(message: unknown): void {
    for (const listener of this.listeners.get('message') ?? []) {
      listener({ data: JSON.stringify(message) });
    }
  }
}

class TestTransport extends EchoGuardedTwilioTransport {
  feedAudio(responseId: string, bytes = 800): void {
    this._onAudio({
      type: 'audio',
      responseId,
      data: new ArrayBuffer(bytes),
    } satisfies TransportLayerAudio);
  }
}

function clearMessages(socket: FakeTwilioSocket): any[] {
  return socket.sent.filter((message) => message.event === 'clear');
}

test('low-energy opening echo is guarded but a loud early caller clears Twilio', () => {
  const socket = new FakeTwilioSocket();
  let rms = 0.08;
  const suppressed: Array<{ ms: number; rms: number }> = [];
  const stopped: string[] = [];
  const transport = new TestTransport({
    twilioWebSocket: socket,
    guardMs: 1_800,
    maxGuardRms: 0.14,
    currentInboundRms: () => rms,
    onSuppressed: (ms, level) => suppressed.push({ ms, rms: level }),
    onPlaybackStopped: (reason) => stopped.push(reason),
  });

  socket.receive({ event: 'start', start: { streamSid: 'MZ-test' } });
  transport.feedAudio('response-1');
  assert.equal(transport.isPlaybackPending(), true);

  transport.interrupt();
  assert.equal(suppressed.length, 1);
  assert.equal(suppressed[0].rms, 0.08);
  assert.equal(clearMessages(socket).length, 0);
  assert.equal(transport.isPlaybackPending(), true);

  rms = 0.31;
  transport.interrupt();
  assert.deepEqual(clearMessages(socket), [
    { event: 'clear', streamSid: 'MZ-test' },
  ]);
  assert.equal(transport.isPlaybackPending(), false);
  assert.deepEqual(stopped, ['interrupted']);
});

test('a caller can clear queued Twilio audio after OpenAI generation state is gone', () => {
  const socket = new FakeTwilioSocket();
  const transport = new TestTransport({
    twilioWebSocket: socket,
    guardMs: 0,
    maxGuardRms: 0.14,
    currentInboundRms: () => 0.4,
    onSuppressed: () => assert.fail('loud caller must not be suppressed'),
  });

  socket.receive({ event: 'start', start: { streamSid: 'MZ-tail' } });
  // A test-fed response has no SDK currentItemId, reproducing the exact tail
  // state after response.output_audio.done reset it.
  transport.feedAudio('response-tail', 1_600);
  transport.interrupt();

  assert.deepEqual(clearMessages(socket), [
    { event: 'clear', streamSid: 'MZ-tail' },
  ]);
  assert.equal(transport.isPlaybackPending(), false);
});

test('Twilio done mark, not OpenAI audio generation, ends playback state', () => {
  const socket = new FakeTwilioSocket();
  const stopped: string[] = [];
  const transport = new TestTransport({
    twilioWebSocket: socket,
    guardMs: 1_800,
    maxGuardRms: 0.14,
    currentInboundRms: () => 0,
    onSuppressed: () => undefined,
    onPlaybackStopped: (reason) => stopped.push(reason),
  });

  transport.feedAudio('response-done');
  assert.equal(transport.isPlaybackPending(), true);
  socket.receive({ event: 'mark', mark: { name: 'done:response-done' } });

  assert.equal(transport.isPlaybackPending(), false);
  assert.deepEqual(stopped, ['played']);
});

test('text overlap cannot suppress a loud real correction such as Faster', () => {
  assert.deepEqual(
    classifyUserTurn('Faster', 'Yes, I will speak faster.', {
      beganDuringPlayback: true,
      rms: 0.4283,
      maxEchoRms: 0.14,
    }),
    { echo: false },
  );

  assert.deepEqual(
    classifyUserTurn('Faster', 'Yes, I will speak faster.', {
      beganDuringPlayback: true,
      rms: 0.08,
      maxEchoRms: 0.14,
    }),
    { echo: true, reason: 'low_energy_matches_agent_speech' },
  );

  assert.deepEqual(
    classifyUserTurn('Faster', 'Yes, I will speak faster.', {
      beganDuringPlayback: false,
      rms: 0.08,
      maxEchoRms: 0.14,
    }),
    { echo: false },
  );
});
