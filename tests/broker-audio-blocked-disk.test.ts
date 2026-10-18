import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { monitorEventLoopDelay } from 'node:perf_hooks';
import Twilio from 'twilio';
import { WebSocket } from 'ws';
import { createBrokerServer } from '../src/broker/server.js';
import { BrokerStore } from '../src/broker/store.js';
import { nodeOverlayFileSystem, type OverlayFileSystem } from '../src/broker/journalOverlay.js';
import { streamToken } from '../src/broker/phone.js';
import type { BrokerConfig } from '../src/broker/config.js';
import type { PrimaryApiClient } from '../src/broker/primaryApi.js';
import type { BrokerLiveSessionOptions } from '../src/live/session.js';

/**
 * Call of Oct 17: the journal rewrote ~1.8 MB with fsync for every transcript fragment on the
 * event loop, unplayed audio piled up and the call was hung up for audio backpressure. Here the
 * disk that holds transcripts never completes a write, and a speaking call still relays every
 * frame both ways without delay and stays up.
 */
test('a blocked transcript disk delays no call audio and never ends the call', async t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'blocked-disk-'));
  const writes: string[] = [];
  let release!: () => void;
  const blocked = new Promise<void>(resolve => { release = resolve; });
  const disk: OverlayFileSystem = { ...nodeOverlayFileSystem, async writeFile(file, data) { writes.push(file); await blocked; await nodeOverlayFileSystem.writeFile(file, data); } };
  const store = new BrokerStore(directory, { observationFs: disk, observationIntervalMs: 0 });
  const config: BrokerConfig = { host: '127.0.0.1', port: 3213, publicOrigin: 'https://phone.example.test', stateDir: directory,
    controlToken: 'c'.repeat(40), apiKey: 'fixture-key', model: 'gpt-live-1', voice: 'marin',
    twilioAccountSid: 'AC' + '1'.repeat(32), twilioAuthToken: 'fixture-twilio-secret', from: '+12025550100',
    allowedTo: ['+12025550123'], apiBaseUrl: 'https://api.example.test', phoneServiceSecret: 's'.repeat(40), maxCallSeconds: 60 };
  const sid = 'CA' + '2'.repeat(32);
  const primary = {
    async portfolio() { throw new Error('unused'); },
    async readiness() { return { eligible: true, reason: 'ready' }; },
    async openCall(_proof: unknown, assignment?: { assignmentId: string }) {
      return { bindingId: randomUUID(), callSid: sid, state: 'open',
        ...(assignment ? { callAssignmentRef: { assignmentId: assignment.assignmentId, revision: 1, digest: 'a'.repeat(64) } } : {}),
        context: { primaryDexterId: randomUUID(), threadId: randomUUID(), displayName: 'Avery Example', messages: [] } };
    },
    async close() { return { state: 'closed' }; },
  } as unknown as PrimaryApiClient;
  const provider = Object.assign((callSid: string) => ({
    async update() { return { sid: callSid, status: 'completed' }; },
    async fetch() { return { sid: callSid, status: 'in-progress', from: config.from, to: config.allowedTo[0] }; },
  }), { async create() { return { sid, status: 'queued' }; } });
  const received: number[] = [];
  let voice: BrokerLiveSessionOptions | undefined;
  const broker = createBrokerServer({ config, primary, store, provider: { calls: provider } as unknown as ReturnType<typeof Twilio>,
    probe: async () => ({ model: 'gpt-live-1', audioBytes: 800, sessionId: 'fixture', finalized: true }),
    voiceFactory: options => {
      voice = options;
      queueMicrotask(() => options.onReady({ callId: options.callId, sessionId: 'fixture-live' }));
      let revision = 0;
      const fragments: Array<{ eventId: string; role: 'user' | 'assistant'; text: string; startMs: number; endMs: number; sequence: number }> = [];
      return {
        appendAudio(payload: string) {
          // Every caller frame is answered by an assistant frame and a new transcript fragment:
          // far more transcript traffic than a real call produces.
          received.push(performance.now());
          revision++;
          fragments.push({ eventId: `f${revision}`, role: 'assistant', text: ` word${revision}`, startMs: revision * 20, endMs: revision * 20 + 20, sequence: revision });
          options.onTranscript({ revision, callerRevision: 0, fragments: [...fragments] });
          options.onAudio(Buffer.alloc(160, 0x20).toString('base64'), { deliveryId: `out-${revision}` });
          void payload; return true;
        },
        async appendContext() { return { eventId: 'c', status: 'accepted' }; },
        async appendInstructions() { return { eventId: 'i', status: 'accepted' }; },
        async interruptPlayback() { return { eventId: 'p', status: 'accepted' }; },
        resumePlayback() {},
        getTranscript() { return { revision, callerRevision: 0, fragments: [...fragments] }; },
        async close(reason) { const result = { callId: options.callId, sessionId: 'fixture-live', finalized: true, transcriptPersisted: true, outputAudioDiscarded: false, reason: reason || 'closed' }; options.onClosed(result); return result; },
      };
    },
  });
  await new Promise<void>(resolve => broker.server.listen(0, '127.0.0.1', resolve));
  const base = `http://127.0.0.1:${(broker.server.address() as { port: number }).port}`;
  t.after(async () => { release(); await broker.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  const created = await fetch(base + '/internal/broker/calls', { method: 'POST', headers: { authorization: `Bearer ${config.controlToken}`, 'content-type': 'application/json' },
    body: JSON.stringify({ requestId: randomUUID(), purpose: 'Read the current portfolio.' }) });
  assert.equal(created.status, 201);
  const call = store.list()[0]!;
  const params = { AccountSid: config.twilioAccountSid, CallSid: sid, From: config.from, To: config.allowedTo[0], Direction: 'outbound-api', CallStatus: 'in-progress' };
  const route = `/broker/answer/${call.id}`;
  const answer = await fetch(base + route, { method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded',
    'x-twilio-signature': Twilio.getExpectedTwilioSignature(config.twilioAuthToken, config.publicOrigin + route, params) }, body: new URLSearchParams(params) });
  assert.equal(answer.status, 200);
  const media = `/broker/media/${call.id}`;
  const ws = new WebSocket(base.replace('http:', 'ws:') + media, { headers: { 'x-twilio-signature': Twilio.getExpectedTwilioSignature(config.twilioAuthToken, config.publicOrigin + media, {}) } });
  const outbound: number[] = [];
  const streamSid = 'MZ' + '3'.repeat(32);
  ws.on('message', bytes => {
    const frame = JSON.parse(bytes.toString());
    if (frame.event === 'media') outbound.push(performance.now());
    // A healthy carrier acknowledges playback.
    if (frame.event === 'mark') ws.send(JSON.stringify({ event: 'mark', streamSid, mark: frame.mark }));
  });
  await new Promise<void>((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
  ws.send(JSON.stringify({ event: 'start', start: { accountSid: config.twilioAccountSid, callSid: sid, streamSid,
    customParameters: { token: streamToken(config.controlToken, call.id, sid) }, mediaFormat: { encoding: 'audio/x-mulaw', sampleRate: 8000, channels: 1 } } }));
  for (let i = 0; i < 200 && !voice; i++) await new Promise(resolve => setTimeout(resolve, 5));
  assert.ok(voice, 'Live attached');
  const loop = monitorEventLoopDelay({ resolution: 1 }); loop.enable();
  const sent: number[] = [];
  // Four seconds of caller speech at the carrier's 20 ms cadence.
  for (let frame = 0; frame < 200; frame++) {
    sent.push(performance.now());
    ws.send(JSON.stringify({ event: 'media', streamSid, media: { track: 'inbound', payload: Buffer.alloc(160, 0x10 + (frame % 16)).toString('base64') } }));
    await new Promise(resolve => setTimeout(resolve, 20));
  }
  for (let i = 0; i < 200 && (received.length < 200 || outbound.length < 200); i++) await new Promise(resolve => setTimeout(resolve, 5));
  loop.disable();
  assert.equal(received.length, 200, 'every caller frame reached Live');
  assert.ok(outbound.length >= 200, 'every assistant frame reached the carrier');
  const inbound = received.map((at, index) => at - sent[index]!).sort((a, b) => a - b);
  const p99 = inbound[Math.floor(inbound.length * 0.99)]!;
  assert.ok(p99 < 100, `caller audio relay p99 ${p99.toFixed(1)} ms`);
  assert.ok(loop.max / 1e6 < 250, `event loop stalled ${(loop.max / 1e6).toFixed(0)} ms`);
  const saved = store.get(call.id)!;
  assert.equal(saved.endedAt, undefined, 'the call is still up');
  assert.equal(saved.endReason, undefined);
  assert.equal(saved.transcriptRevision, 200, 'transcripts are current in memory');
  const disk_ = JSON.parse(fs.readFileSync(path.join(directory, 'calls.json'), 'utf8')).calls[call.id];
  assert.ok((disk_.transcriptRevision ?? 0) < 200, 'and were not written into calls.json one by one');
  assert.equal(writes.length, 1, 'one observation write is in flight on the blocked disk; the rest coalesce behind it');
  ws.close();
});
