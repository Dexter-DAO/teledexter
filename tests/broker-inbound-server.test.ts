import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import { createPhoneApiCallbackSignature } from '../src/outbound/PhoneApiCallback.js';
import { inboundBillingSummary, inboundCallEndedSchema, inboundStatusEndedSchema, type InboundStatusEnded, type InboundCallEnded } from '../src/broker/inboundBilling.js';
import { createInboundBillingLifecycle } from '../src/broker/inboundBillingLifecycle.js';
import Twilio from 'twilio';
import { WebSocket } from 'ws';
import { createBrokerServer, inboundAdmissionPollDelay } from '../src/broker/server.js';
import { inboundBeginRef, type InboundBeginIdentity, type InboundOutcome } from '../src/broker/inboundContract.js';
import { BrokerStore, type BrokerCall } from '../src/broker/store.js';
import type { BrokerConfig } from '../src/broker/config.js';
import { PrimaryApiError, type PrimaryApiClient } from '../src/broker/primaryApi.js';
import { createInboundHoldAdapter } from '../src/broker/inboundHold.js';
import { streamToken } from '../src/broker/phone.js';
import { brokerOpening } from '../src/broker/instructions.js';
import { createBrokerLiveSession, type BrokerLiveSessionOptions } from '../src/live/session.js';
import { PreconnectTransport } from './helpers/preconnectTransport.js';
import type { EligiblePhoneCall } from '../src/broker/eligibleContext.js';

async function billingReceiver(t: test.TestContext, loseFirstReceipt = false) {
  const bodies: InboundCallEnded[] = [], encodedBodies: string[] = [];
  const receipt = { ok: true as const, id: randomUUID(), status: 'completed' as const };
  const server = createServer(async (request, response) => {
    const chunks = []; for await (const chunk of request) chunks.push(Buffer.from(chunk));
    const encoded = Buffer.concat(chunks).toString('utf8');
    const body = inboundCallEndedSchema.parse(JSON.parse(encoded));
    assert.equal(request.url, '/internal/voice/call-ended');
    assert.equal(request.headers['x-dexter-callback-signature'], createPhoneApiCallbackSignature('b'.repeat(40),
      '/internal/voice/call-ended', String(request.headers['x-dexter-alert-timestamp']), String(request.headers['x-dexter-alert-nonce']), body));
    bodies.push(body); encodedBodies.push(encoded);
    if (loseFirstReceipt && bodies.length === 1) { response.destroy(); return; }
    response.writeHead(200, { 'content-type': 'application/json' }); response.end(JSON.stringify(receipt));
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())));
  return { bodies, encodedBodies, receipt, url: `http://127.0.0.1:${(server.address() as { port: number }).port}` };
}

async function connectIncomingMedia(f: Awaited<ReturnType<typeof fixture>>, onMessage?: (raw: unknown) => void) {
  await (await f.webhook('/broker/incoming-call')).text();
  // The continuation answers with the stream at once; the owner approves while it waits.
  const call = f.store.list()[0]; await f.waitForAdmission(); f.approve();
  const route = `/broker/media/${call.id}`;
  const ws = new WebSocket(f.base.replace(/^http/, 'ws') + route, { headers: {
    'x-twilio-signature': Twilio.getExpectedTwilioSignature(f.config.twilioAuthToken, f.config.publicOrigin + route, {}),
  } });
  await new Promise<void>((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
  if (onMessage) ws.on('message', onMessage);
  ws.send(JSON.stringify({ event: 'start', start: { accountSid: f.config.twilioAccountSid, callSid: f.params.CallSid,
    streamSid: 'MZ' + '4'.repeat(32), customParameters: { token: streamToken(f.config.controlToken, call.id, f.params.CallSid) },
    mediaFormat: { encoding: 'audio/x-mulaw', sampleRate: 8000, channels: 1 } } }));
  for (let count = 0; count < 100 && f.seen.voice === 0; count++) await new Promise(resolve => setTimeout(resolve, 10));
  assert.equal(f.seen.voice, 1);
  return { call, ws };
}

for (const duration of ['0', '93', '121', '180', '181']) test(`incoming HTTP/media closes with exact provider duration ${duration} in signed financial callback`, async t => {
  const receiver = await billingReceiver(t);
  const f = await fixture(t, { billing: true, billingUrl: receiver.url, duration });
  const { call, ws } = await connectIncomingMedia(f); t.after(() => ws.close());
  assert.equal(receiver.bodies.length, 0);
  assert.deepEqual(f.store.get(call.id)!.inboundBilling?.ref, f.billing);
  f.providerEnded();
  assert.equal((await f.webhook('/broker/status', { CallStatus: 'completed' })).status, 200);
  await f.broker.maintain();
  assert.equal(receiver.bodies.length, 1);
  const body = receiver.bodies[0];
  assert.equal(body.duration_seconds, Number(duration));
  assert.equal(body.provider_terminal.durationSeconds, Number(duration));
  assert.equal(body.broker_call_id, call.id); assert.equal(body.billing_terms_digest, f.billing!.termsDigest);
  assert.equal(body.phone_binding_id, f.billing!.bindingId); assert.equal(body.external_sid, f.params.CallSid);
  assert.equal(f.store.get(call.id)!.inboundBilling?.event?.state, 'delivered');
  assert.deepEqual(f.store.get(call.id)!.inboundBilling?.event?.receipt, receiver.receipt);
  await f.broker.maintain(); assert.equal(receiver.bodies.length, 1);
  assert.equal(f.seen.dial, 0);
});

test('incoming billing retries identical body after accepted callback loses reply and Broker restarts', async t => {
  const receiver = await billingReceiver(t, true);
  const f = await fixture(t, { billing: true, billingUrl: receiver.url, duration: '93' });
  const { call, ws } = await connectIncomingMedia(f); t.after(() => ws.close());
  f.providerEnded(); await (await f.webhook('/broker/status', { CallStatus: 'completed' })).text();
  await f.broker.maintain();
  assert.equal(receiver.bodies.length, 1);
  assert.equal(f.store.get(call.id)!.inboundBilling?.event?.state, 'pending');
  await f.close();
  const restarted = new BrokerStore(f.config.stateDir); t.after(() => restarted.close());
  const recovery = createInboundBillingLifecycle({ store: restarted, apiBaseUrl: receiver.url, signingSecret: 'b'.repeat(40),
    now: () => Date.now() + 60_000, fetchCall: async () => { throw new Error('immutable timing must not be fetched again'); } });
  await Promise.all([recovery.reconcile(call.id), recovery.reconcile(call.id)]);
  assert.equal(receiver.bodies.length, 2); assert.equal(receiver.encodedBodies[0], receiver.encodedBodies[1]);
  assert.deepEqual(restarted.get(call.id)!.inboundBilling?.event?.receipt, receiver.receipt);
  await recovery.reconcile(call.id); assert.equal(receiver.bodies.length, 2);
});

test('incoming completed callback without provider duration remains pending and recovers timing without inventing a free call', async t => {
  const receiver = await billingReceiver(t), f = await fixture(t, { billing: true, billingUrl: receiver.url });
  const { call, ws } = await connectIncomingMedia(f); t.after(() => ws.close());
  f.providerEnded(); await (await f.webhook('/broker/status', { CallStatus: 'completed' })).text();
  await f.broker.maintain();
  assert.equal(receiver.bodies.length, 0); assert.equal(f.store.get(call.id)!.inboundBilling?.timingError, 'provider_timing_unavailable');
  f.providerEnded('93');
  const recovery = createInboundBillingLifecycle({ store: f.store, apiBaseUrl: receiver.url, signingSecret: 'b'.repeat(40),
    now: () => Date.now() + 60_000, fetchCall: sid => f.provider(sid).fetch() });
  await recovery.reconcile(call.id); assert.equal(receiver.bodies.length, 1);
  assert.equal(receiver.bodies[0].duration_seconds, 93);
});

test('incomplete provider duration remains refetchable instead of freezing an API-rejected financial event', async t => {
  const receiver = await billingReceiver(t), f = await fixture(t, { billing: true, billingUrl: receiver.url, duration: '93' });
  const { call, ws } = await connectIncomingMedia(f); t.after(() => ws.close());
  f.providerEnded(); await (await f.webhook('/broker/status', { CallStatus: 'completed' })).text();
  let observedAt = Date.now(), duration = '0';
  const recovery = createInboundBillingLifecycle({ store: f.store, apiBaseUrl: receiver.url, signingSecret: 'b'.repeat(40),
    now: () => observedAt, fetchCall: async sid => ({ ...await f.provider(sid).fetch(), duration }) });
  for (const invalid of ['0', '-1', 'malformed']) {
    duration = invalid; observedAt += 31_000; await recovery.reconcile(call.id);
    assert.equal(f.store.get(call.id)!.inboundBilling?.event, undefined);
    assert.equal(f.store.get(call.id)!.inboundBilling?.timingError, 'provider_timing_unavailable');
    assert.equal(receiver.bodies.length, 0);
  }
  duration = '93'; observedAt += 31_000; await recovery.reconcile(call.id);
  assert.equal(receiver.bodies.length, 1); assert.equal(receiver.bodies[0].duration_seconds, 93);
});

async function statusReceiver(t: test.TestContext, loseFirst = false) {
  const bodies: InboundStatusEnded[] = [], encodedBodies: string[] = [];
  const server = createServer(async (request, response) => {
    const chunks = []; for await (const chunk of request) chunks.push(Buffer.from(chunk));
    const encoded = Buffer.concat(chunks).toString('utf8'), body = inboundStatusEndedSchema.parse(JSON.parse(encoded));
    assert.equal(request.url, '/internal/voice/call-ended');
    assert.equal(request.headers['x-dexter-callback-signature'], createPhoneApiCallbackSignature('b'.repeat(40),
      '/internal/voice/call-ended', String(request.headers['x-dexter-alert-timestamp']), String(request.headers['x-dexter-alert-nonce']), body));
    bodies.push(body); encodedBodies.push(encoded);
    if (loseFirst && bodies.length === 1) { response.destroy(); return; }
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end(JSON.stringify({ ok: true, id: body.phone_binding_id, status: body.provider_terminal.status }));
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())));
  return { bodies, encodedBodies, url: `http://127.0.0.1:${(server.address() as { port: number }).port}` };
}

for (const terminal of ['failed', 'busy', 'no-answer', 'canceled']) test(`incoming ${terminal} persists authenticated nonfinancial outcome once with unknown duration`, async t => {
  const receiver = await statusReceiver(t), f = await fixture(t, { billing: true, billingUrl: receiver.url });
  const { call, ws } = await connectIncomingMedia(f); t.after(() => ws.close());
  await (await f.webhook('/broker/status', { CallStatus: terminal })).text();
  let reads = 0;
  const recovery = createInboundBillingLifecycle({ store: f.store, apiBaseUrl: receiver.url, signingSecret: 'b'.repeat(40),
    now: () => Date.now() + 60_000, fetchCall: async sid => { reads++; return { ...await f.provider(sid).fetch(), status: terminal }; } });
  await recovery.reconcile(call.id); await recovery.reconcile(call.id);
  assert.equal(reads, 1); assert.equal(receiver.bodies.length, 1);
  assert.equal(f.store.get(call.id)!.inboundBilling?.event, undefined);
  assert.deepEqual(inboundBillingSummary(f.store.get(call.id)!), { state: 'not_billable', receiptId: f.billing!.bindingId,
    providerStatus: terminal, durationSeconds: null, problem: null });
  assert.equal(receiver.bodies[0].provider_terminal.startedAt, null);
  assert.equal(receiver.bodies[0].provider_terminal.endedAt, null);
  assert.equal('duration_seconds' in receiver.bodies[0], false);
});

test('noncompleted provider status preserves actual timing and retries identical saved report after restart and lost ACK', async t => {
  const receiver = await statusReceiver(t, true), f = await fixture(t, { billing: true, billingUrl: receiver.url, duration: '93' });
  const { call, ws } = await connectIncomingMedia(f); t.after(() => ws.close());
  await (await f.webhook('/broker/status', { CallStatus: 'failed' })).text();
  const recovery = createInboundBillingLifecycle({ store: f.store, apiBaseUrl: receiver.url, signingSecret: 'b'.repeat(40),
    now: () => Date.now() + 60_000, fetchCall: async sid => ({ ...await f.provider(sid).fetch(), status: 'failed' }) });
  await recovery.reconcile(call.id);
  assert.equal(f.store.get(call.id)!.inboundBilling?.statusEvent?.state, 'pending');
  assert.equal(receiver.bodies[0].provider_terminal.durationSeconds, 93);
  await f.close();
  const restarted = new BrokerStore(f.config.stateDir); t.after(() => restarted.close());
  const retry = createInboundBillingLifecycle({ store: restarted, apiBaseUrl: receiver.url, signingSecret: 'b'.repeat(40),
    now: () => Date.now() + 120_000, fetchCall: async () => { throw new Error('saved status must not refetch'); } });
  await Promise.all([retry.reconcile(call.id), retry.reconcile(call.id)]);
  assert.equal(receiver.bodies.length, 2); assert.equal(receiver.encodedBodies[0], receiver.encodedBodies[1]);
  assert.equal(restarted.get(call.id)!.inboundBilling?.statusEvent?.state, 'delivered');
  assert.equal(restarted.get(call.id)!.inboundBilling?.event, undefined);
});

test('noncompleted status with wrong leg or malformed timing remains refetchable', async t => {
  const receiver = await statusReceiver(t), f = await fixture(t, { billing: true, billingUrl: receiver.url, duration: '93' });
  const { call, ws } = await connectIncomingMedia(f); t.after(() => ws.close());
  await (await f.webhook('/broker/status', { CallStatus: 'failed' })).text();
  let now = Date.now() + 60_000, patch: Record<string, unknown> = { to: '+12025550999' };
  const recovery = createInboundBillingLifecycle({ store: f.store, apiBaseUrl: receiver.url, signingSecret: 'b'.repeat(40),
    now: () => now, fetchCall: async sid => ({ ...await f.provider(sid).fetch(), status: 'failed', ...patch }) });
  for (patch of [{ to: '+12025550999' }, { status: 'completed' }, { duration: '-1' }, { duration: '0' },
    { startTime: new Date(now + 600_000) }, { endTime: new Date(now + 600_000) }]) {
    now += 31_000; await recovery.reconcile(call.id);
    assert.equal(f.store.get(call.id)!.inboundBilling?.statusEvent, undefined);
    assert.equal(receiver.bodies.length, 0);
  }
  patch = {}; now += 31_000; await recovery.reconcile(call.id);
  assert.equal(receiver.bodies.length, 1);
});

test('incoming provider leg mismatch cannot create financial body', async t => {
  const receiver = await billingReceiver(t), f = await fixture(t, { billing: true, billingUrl: receiver.url, duration: '93' });
  const { call, ws } = await connectIncomingMedia(f); t.after(() => ws.close());
  f.providerEnded(); f.wrongProviderIdentity(); await (await f.webhook('/broker/status', { CallStatus: 'completed' })).text();
  await f.broker.maintain();
  assert.equal(receiver.bodies.length, 0); assert.equal(f.store.get(call.id)!.inboundBilling?.timingError, 'provider_identity_mismatch');
});

test('incoming audio timing separates sent audio, cleared marks, and confirmed playback', async t => {
  const f = await fixture(t), { call, ws } = await connectIncomingMedia(f); t.after(() => ws.close());
  const marks: string[] = [];
  ws.on('message', raw => { const frame = JSON.parse(String(raw)); if (frame.event === 'mark') marks.push(frame.mark.name); });
  assert.ok(f.store.get(call.id)!.journey?.liveReadyAt);
  assert.equal(f.store.get(call.id)!.journey?.firstAudioSentAt, undefined);
  f.emitAudio();
  for (let i = 0; i < 100 && !marks.length; i++) await new Promise(resolve => setTimeout(resolve, 5));
  const sentAt = f.store.get(call.id)!.journey!.firstAudioSentAt; assert.ok(sentAt);
  assert.equal(f.store.get(call.id)!.journey?.firstPlaybackConfirmedAt, undefined);
  f.clearAudio();
  ws.send(JSON.stringify({ event: 'mark', streamSid: 'MZ' + '4'.repeat(32), mark: { name: marks[0] } }));
  await new Promise(resolve => setTimeout(resolve, 10));
  assert.equal(f.store.get(call.id)!.journey?.firstPlaybackConfirmedAt, undefined);
  f.emitAudio();
  for (let i = 0; i < 100 && marks.length < 2; i++) await new Promise(resolve => setTimeout(resolve, 5));
  ws.send(JSON.stringify({ event: 'mark', streamSid: 'MZ' + '4'.repeat(32), mark: { name: marks[1] } }));
  for (let i = 0; i < 100 && !f.store.get(call.id)!.journey?.firstPlaybackConfirmedAt; i++) await new Promise(resolve => setTimeout(resolve, 5));
  assert.ok(f.store.get(call.id)!.journey?.firstPlaybackConfirmedAt);
  assert.equal(f.store.get(call.id)!.journey?.firstAudioSentAt, sentAt);
});

test('audible timing excludes silence, empty audio, cleared and duplicate marks; only first speech reaches the journal', async t => {
  const events: Array<{ event: string; callId: string; stage: string; observedAt: string }> = [];
  t.mock.method(console, 'info', (value: string) => { const event = JSON.parse(value); if (event.event === 'broker_audio_timing') events.push(event); });
  const f = await fixture(t), { call, ws } = await connectIncomingMedia(f); t.after(() => ws.close());
  const marks: string[] = [];
  ws.on('message', raw => { const frame = JSON.parse(String(raw)); if (frame.event === 'mark') marks.push(frame.mark.name); });
  const send = async (bytes: Buffer) => {
    const count = marks.length; f.emitAudio(bytes);
    for (let i = 0; i < 100 && marks.length === count; i++) await new Promise(resolve => setTimeout(resolve, 5));
    assert.equal(marks.length, count + 1); return marks.at(-1)!;
  };
  const mark = async (name: string) => {
    ws.send(JSON.stringify({ event: 'mark', streamSid: 'MZ' + '4'.repeat(32), mark: { name } }));
    await new Promise(resolve => setTimeout(resolve, 10));
  };
  await mark('not-issued');
  await mark(await send(Buffer.alloc(0)));
  await mark(await send(Buffer.from([0xff, 0x7f])));
  assert.equal(events.length, 0);
  assert.ok(f.store.get(call.id)!.journey?.firstPlaybackConfirmedAt, 'original all-frame timing stays intact');
  const cleared = await send(Buffer.from([0xfe]));
  assert.deepEqual(events.map(event => event.stage), ['firstAudibleAudioSentAt']);
  f.clearAudio(); await mark(cleared);
  assert.equal(events.length, 1, 'a cleared audible frame cannot count as played');
  const audible = await send(Buffer.from([0xfe, 0xff]));
  await mark(audible); await mark(audible);
  assert.deepEqual(events.map(event => event.stage), ['firstAudibleAudioSentAt', 'firstAudiblePlaybackAcknowledgedAt'],
    'quiet nonzero PCMU is audible for playback accounting but never counts as speech');
  await mark(await send(Buffer.from([0x80])));
  assert.deepEqual(events.map(event => event.stage), ['firstAudibleAudioSentAt', 'firstAudiblePlaybackAcknowledgedAt',
    'firstSpeechAudioSentAt', 'firstSpeechPlaybackAcknowledgedAt']);
  assert.ok(events.every(event => event.callId === call.id && Number.isFinite(Date.parse(event.observedAt))));
  assert.deepEqual(Object.keys(events[0]).sort(), ['callId', 'event', 'observedAt', 'stage', 'version']);
  // No caller spoke, so no callerSpeechEndedAt: the first words answer nothing the caller said.
  assert.deepEqual(Object.keys(f.store.get(call.id)!.journey!).sort(), ['firstAudioSentAt', 'firstPlaybackConfirmedAt', 'firstSpeechAudioSentAt', 'liveReadyAt', 'streamStartedAt']);
});

// Production evidence (calls 60616be7 / 6b2ef8d1): the first "audible" chunk was a
// -58 dBFS Live breath/noise blip; the greeting itself was heard ~3.9 s / ~4.4 s later.
const nearSilent = (length = 800) => Buffer.alloc(length, 0xf0); // nonzero PCMU, RMS ~0.004
const spoken = (length = 800) => Buffer.alloc(length, 0x20); // RMS well above -40 dBFS

test('a queued pre-speech Live backlog is cleared before the first speech; its last 200 ms is re-queued first', async t => {
  const timing: Array<{ stage: string; queuedAheadMs?: number }> = [];
  const clears: Array<{ backlogMs: number; droppedMs: number; requeuedMs: number }> = [];
  t.mock.method(console, 'info', (value: string) => { const event = JSON.parse(value);
    if (event.event === 'broker_audio_timing') timing.push(event); if (event.event === 'broker_pre_speech_clear') clears.push(event); });
  const f = await fixture(t), { call, ws } = await connectIncomingMedia(f); t.after(() => ws.close());
  const frames: Array<{ event: string; payload?: string; name?: string }> = [];
  ws.on('message', raw => { const frame = JSON.parse(String(raw));
    frames.push({ event: frame.event, ...(frame.media ? { payload: frame.media.payload } : {}), ...(frame.mark ? { name: frame.mark.name } : {}) }); });
  const waitFor = async (count: number) => { for (let i = 0; i < 200 && frames.length < count; i++) await new Promise(resolve => setTimeout(resolve, 5)); };
  const leading = Array.from({ length: 6 }, (_value, index) => Buffer.alloc(800, 0xf0 + index)); // 600 ms, unplayed
  for (const chunk of leading) f.emitAudio(chunk);
  await waitFor(12);
  assert.deepEqual(timing.map(event => event.stage), ['firstAudibleAudioSentAt']);
  const greeting = spoken();
  f.emitAudio(greeting);
  await waitFor(12 + 1 + 4 + 2);
  const after = frames.slice(12);
  assert.deepEqual(after.map(frame => frame.event), ['clear', 'media', 'mark', 'media', 'mark', 'media', 'mark']);
  assert.deepEqual(after.filter(frame => frame.event === 'media').map(frame => frame.payload),
    [leading[4].toString('base64'), leading[5].toString('base64'), greeting.toString('base64')], 'onset context then the greeting');
  assert.deepEqual(clears.map(({ backlogMs, droppedMs, requeuedMs }) => ({ backlogMs, droppedMs, requeuedMs })), [{ backlogMs: 600, droppedMs: 600, requeuedMs: 200 }]);
  assert.deepEqual(timing.map(event => event.stage), ['firstAudibleAudioSentAt', 'firstSpeechAudioSentAt']);
  assert.equal(timing[1].queuedAheadMs, 200, 'only the re-queued onset context remains ahead of the greeting');
  // Twilio returns the dropped marks after a clear; none of them counts as played.
  const send = (name: string) => ws.send(JSON.stringify({ event: 'mark', streamSid: 'MZ' + '4'.repeat(32), mark: { name } }));
  for (const frame of frames.slice(0, 12)) if (frame.event === 'mark') send(frame.name!);
  await new Promise(resolve => setTimeout(resolve, 20));
  assert.equal(f.store.get(call.id)!.journey?.firstPlaybackConfirmedAt, undefined, 'dropped pre-speech audio was never heard');
  for (const frame of after) if (frame.event === 'mark') send(frame.name!);
  for (let i = 0; i < 100 && timing.length < 4; i++) await new Promise(resolve => setTimeout(resolve, 5));
  assert.deepEqual(timing.map(event => event.stage), ['firstAudibleAudioSentAt', 'firstSpeechAudioSentAt',
    'firstAudiblePlaybackAcknowledgedAt', 'firstSpeechPlaybackAcknowledgedAt']);
  assert.ok(f.store.get(call.id)!.journey?.firstPlaybackConfirmedAt);
  // Later speech is never cleared by this path, even with a long backlog.
  for (let index = 0; index < 8; index++) f.emitAudio(nearSilent());
  f.emitAudio(spoken());
  await waitFor(frames.length + 18);
  assert.equal(frames.filter(frame => frame.event === 'clear').length, 1);
});

test('a short pre-speech backlog is ordinary jitter and is not cleared', async t => {
  const clears: unknown[] = [];
  t.mock.method(console, 'info', (value: string) => { if (JSON.parse(value).event === 'broker_pre_speech_clear') clears.push(value); });
  const f = await fixture(t), { ws } = await connectIncomingMedia(f); t.after(() => ws.close());
  const events: string[] = [];
  ws.on('message', raw => { events.push(JSON.parse(String(raw)).event); });
  f.emitAudio(nearSilent()); f.emitAudio(nearSilent(1_200)); f.emitAudio(spoken()); // 250 ms queued
  for (let i = 0; i < 100 && events.length < 6; i++) await new Promise(resolve => setTimeout(resolve, 5));
  await new Promise(resolve => setTimeout(resolve, 20));
  assert.deepEqual(events, ['media', 'mark', 'media', 'mark', 'media', 'mark']);
  assert.equal(clears.length, 0);
});

// The pre-rendered "Hey, it's Dexter." plays at media start, before Live is ready.
const greetingFixtureClip = Buffer.alloc(2_400, 0x20); // 300 ms of speech-energy PCMU
async function greetingCall(t: test.TestContext, options: { eligible?: boolean } = {}) {
  const timing: Array<{ stage: string; heldLiveMs?: number; incomplete?: number }> = [];
  t.mock.method(console, 'info', (value: string) => { const event = JSON.parse(value); if (event.event === 'broker_audio_timing') timing.push(event); });
  const f = await fixture(t, { greetingClip: greetingFixtureClip, ...options });
  const frames: Array<{ event: string; payload?: string; name?: string }> = [];
  const { call, ws } = await connectIncomingMedia(f, raw => { const frame = JSON.parse(String(raw));
    frames.push({ event: frame.event, ...(frame.media ? { payload: frame.media.payload } : {}), ...(frame.mark ? { name: frame.mark.name } : {}) }); });
  t.after(() => ws.close());
  const waitFor = async (predicate: () => boolean, ms = 2_000) => { for (let i = 0; i < ms / 5 && !predicate(); i++) await new Promise(resolve => setTimeout(resolve, 5)); };
  const ack = (name: string) => ws.send(JSON.stringify({ event: 'mark', streamSid: 'MZ' + '4'.repeat(32), mark: { name } }));
  const clipMedia = () => frames.filter(frame => frame.event === 'media' && Buffer.from(frame.payload!, 'base64').every(byte => byte === 0x20));
  return { f, call, ws, frames, timing, waitFor, ack, clipMedia };
}

for (const eligible of [false, true]) test(`greeting clip plays first at media start; Live waits behind it and opens without re-greeting (${eligible ? 'generation' : 'legacy'} path)`, async t => {
  const g = await greetingCall(t, { eligible });
  await g.waitFor(() => g.f.seen.opening === 1);
  assert.equal(g.f.openings.length, 1, 'exactly one opening');
  assert.equal(g.f.openings[0], 'The caller already heard you say "Hey, it\'s Dexter. What can I do for you?" Do not greet or ask again. Wait for the caller and respond to what they say.');
  // Live audio that arrives during the clip is held, never interleaved.
  const live = [Buffer.alloc(800, 0xf0), spoken(1_600)];
  for (const chunk of live) g.f.emitAudio(chunk);
  await g.waitFor(() => g.clipMedia().length === 15);
  assert.equal(g.clipMedia().length, 15, '300 ms clip as fifteen 20 ms frames');
  assert.deepEqual(Buffer.concat(g.clipMedia().map(frame => Buffer.from(frame.payload!, 'base64'))), greetingFixtureClip);
  await new Promise(resolve => setTimeout(resolve, 30));
  assert.equal(g.frames.filter(frame => frame.event === 'media').length, 15, 'no Live audio while the clip is unplayed');
  const journey = g.f.store.get(g.call.id)!.journey!;
  assert.ok(journey.greetingClipStartedAt && journey.firstAudioSentAt);
  assert.equal(journey.greetingClipPlayedAt, undefined);
  // Acknowledge all but the last clip mark: Live is still held.
  const clipMarks = g.frames.filter(frame => frame.event === 'mark').map(frame => frame.name!);
  for (const name of clipMarks.slice(0, -1)) g.ack(name);
  await new Promise(resolve => setTimeout(resolve, 30));
  assert.equal(g.frames.filter(frame => frame.event === 'media').length, 15);
  g.ack(clipMarks.at(-1)!);
  await g.waitFor(() => g.frames.filter(frame => frame.event === 'media').length >= 17);
  const after = g.frames.slice(30);
  assert.ok(!g.frames.some(frame => frame.event === 'clear'), 'the clip is never cleared');
  assert.deepEqual(after.filter(frame => frame.event === 'media').map(frame => frame.payload), live.map(chunk => chunk.toString('base64')), 'held Live audio follows the clip in order');
  assert.ok(g.f.store.get(g.call.id)!.journey!.greetingClipPlayedAt);
  const stages = g.timing.map(event => event.stage);
  assert.deepEqual(stages.filter(stage => stage.startsWith('greetingClip')), ['greetingClipStartedAt', 'greetingClipFirstPlaybackAcknowledgedAt', 'greetingClipPlayedAt']);
  assert.equal(g.timing.find(event => event.stage === 'greetingClipPlayedAt')!.heldLiveMs, 300);
  assert.ok(stages.indexOf('firstSpeechAudioSentAt') > stages.indexOf('greetingClipPlayedAt'), 'first-speech timing still measures Live');
  assert.equal(stages.filter(stage => stage === 'firstSpeechAudioSentAt').length, 1);
  assert.ok(!stages.includes('firstSpeechPlaybackAcknowledgedAt'), 'clip acknowledgements never time Live speech playback');
  for (const frame of after) if (frame.event === 'mark') g.ack(frame.name!);
  await g.waitFor(() => g.timing.some(event => event.stage === 'firstSpeechPlaybackAcknowledgedAt'));
  assert.ok(g.timing.some(event => event.stage === 'firstSpeechPlaybackAcknowledgedAt'));
});

test('a Live playback clear during the clip drops held Live audio and leaves the clip playing', async t => {
  const g = await greetingCall(t);
  const stale = spoken(800), fresh = spoken(1_200);
  g.f.emitAudio(stale); g.f.clearAudio(); g.f.emitAudio(fresh);
  await g.waitFor(() => g.clipMedia().length === 15);
  for (const frame of g.frames.filter(frame => frame.event === 'mark')) g.ack(frame.name!);
  await g.waitFor(() => g.frames.filter(frame => frame.event === 'media').length >= 16);
  await new Promise(resolve => setTimeout(resolve, 20));
  assert.ok(!g.frames.some(frame => frame.event === 'clear'));
  assert.deepEqual(g.frames.slice(30).filter(frame => frame.event === 'media').map(frame => frame.payload), [fresh.toString('base64')]);
});

test('Live audio is released after the clip duration plus grace even when the carrier never acknowledges marks', async t => {
  const g = await greetingCall(t);
  g.f.emitAudio(spoken(800));
  await g.waitFor(() => g.frames.filter(frame => frame.event === 'media').length >= 16, 3_000);
  assert.equal(g.frames.filter(frame => frame.event === 'media').length, 16);
  const played = g.timing.find(event => event.stage === 'greetingClipPlayedAt')!;
  assert.equal(played.incomplete, 1);
  assert.equal(g.f.store.get(g.call.id)!.journey!.greetingClipPlayedAt, undefined, 'unacknowledged playback is not recorded as heard');
});

test('an inbound call never plays the outbound-only clip; Live speaks the whole inbound greeting', async t => {
  const f = await fixture(t, { outboundClipOnly: Buffer.alloc(2_400, 0x20) });
  const media: string[] = [];
  const { call, ws } = await connectIncomingMedia(f, raw => { if (JSON.parse(String(raw)).event === 'media') media.push('media'); }); t.after(() => ws.close());
  for (let i = 0; i < 100 && f.seen.opening === 0; i++) await new Promise(resolve => setTimeout(resolve, 5));
  await new Promise(resolve => setTimeout(resolve, 30));
  assert.equal(media.length, 0);
  assert.equal(f.openings[0], 'Immediately say: "Hey, it\'s Dexter. What can I do for you?" Then pause to listen.');
  assert.equal(f.store.get(call.id)!.journey?.greetingClipStartedAt, undefined);
});

test('without a clip the Live opening still speaks the whole greeting', async t => {
  const f = await fixture(t);
  const { call, ws } = await connectIncomingMedia(f); t.after(() => ws.close());
  for (let i = 0; i < 100 && f.seen.opening === 0; i++) await new Promise(resolve => setTimeout(resolve, 5));
  assert.equal(f.openings[0], 'Immediately say: "Hey, it\'s Dexter. What can I do for you?" Then pause to listen.');
  assert.equal(f.store.get(call.id)!.journey?.greetingClipStartedAt, undefined);
});

test('audible timing logger failure cannot interrupt sent audio or carrier playback', async t => {
  let failures = 0;
  t.mock.method(console, 'info', (value: string) => { if (JSON.parse(value).event === 'broker_audio_timing') { failures++; throw new Error('sink_unavailable'); } });
  const f = await fixture(t), { call, ws } = await connectIncomingMedia(f); t.after(() => ws.close());
  ws.on('message', raw => { const frame = JSON.parse(String(raw)); if (frame.event === 'mark') ws.send(JSON.stringify(frame)); });
  f.emitAudio();
  for (let i = 0; i < 100 && !f.store.get(call.id)!.journey?.firstPlaybackConfirmedAt; i++) await new Promise(resolve => setTimeout(resolve, 5));
  // Audible and speech stages (sent + acknowledged) all failed to log; audio still played.
  for (let i = 0; i < 100 && failures < 4; i++) await new Promise(resolve => setTimeout(resolve, 5));
  assert.ok(f.store.get(call.id)!.journey?.firstPlaybackConfirmedAt); assert.equal(failures, 4);
  assert.equal(f.store.get(call.id)!.endedAt, undefined);
});

test('lost incoming context reply recovers billing reference from bound outcome after caller hangs up', async t => {
  const receiver = await billingReceiver(t), f = await fixture(t, { billing: true, billingUrl: receiver.url, duration: '93' });
  await (await f.webhook('/broker/incoming-call')).text();
  const call = f.store.list()[0]; await f.waitForAdmission(); f.approve(); f.loseConsume();
  await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text();
  assert.equal(f.store.get(call.id)!.primary, undefined);
  f.refuseContext(); f.providerEnded(); await (await f.webhook('/broker/status', { CallStatus: 'completed' })).text();
  await f.broker.maintain();
  assert.deepEqual(f.store.get(call.id)!.inboundBilling?.ref, f.billing);
  assert.equal(receiver.bodies.length, 1);
  assert.equal(f.seen.voice, 0); assert.equal(f.seen.dial, 0);
});

test('incoming legacy admission without frozen billing reference never posts a new financial event', async t => {
  const receiver = await billingReceiver(t), f = await fixture(t, { billingUrl: receiver.url, duration: '181' });
  const { call, ws } = await connectIncomingMedia(f); t.after(() => ws.close());
  f.providerEnded(); await (await f.webhook('/broker/status', { CallStatus: 'completed' })).text();
  await f.broker.maintain(); assert.equal(receiver.bodies.length, 0); assert.equal(f.store.get(call.id)!.inboundBilling, undefined);
  const remote = f.store.get(call.id)!.inbound!.remote;
  assert.equal(remote?.state, 'bound');
  assert.throws(() => f.store.transitionInbound(call.id, { type: 'outcome', observedAt: new Date().toISOString(),
    outcome: { ...remote!, state: 'bound', billing: { version: 1, bindingId: remote!.state === 'bound' ? remote!.bindingId : '',
      callSid: call.callSid!, termsDigest: 'a'.repeat(64) } } as InboundOutcome }), /binding changed/);
});

async function fixture(t: test.TestContext, options: { portfolio?: PrimaryApiClient['portfolio']; selected?: boolean; retained?: BrokerCall; initialRemote?: InboundOutcome; eligible?: boolean; billing?: boolean; billingUrl?: string; duration?: string; preconnect?: boolean; replaceFirstGeneration?: boolean; greetingClip?: Buffer; outboundClipOnly?: Buffer; startupPageFreshMs?: number; defaultAdmissionPoll?: boolean } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-inbound-http-'));
  if (options.retained) fs.writeFileSync(path.join(dir, 'calls.json'), JSON.stringify({ version: 1, calls: { [options.retained.id]: options.retained } }));
  const store = new BrokerStore(dir);
  const config: BrokerConfig = { host: '127.0.0.1', port: 3213, publicOrigin: 'https://phone.example.test', stateDir: dir,
    controlToken: 'c'.repeat(40), apiKey: 'fixture-key', model: 'gpt-live-1', voice: 'marin',
    twilioAccountSid: 'AC' + '1'.repeat(32), twilioAuthToken: 'fixture-twilio-secret', from: '+12025550100',
    allowedTo: ['+12025550123'], apiBaseUrl: 'https://api.example.test', phoneServiceSecret: 's'.repeat(40), maxCallSeconds: 60 };
  const params = { AccountSid: config.twilioAccountSid, CallSid: 'CA' + '2'.repeat(32), From: config.allowedTo[0], To: config.from,
    Direction: 'inbound', CallStatus: 'ringing', StirVerstat: 'TN-Validation-Passed-A' };
  const attachmentId = options.initialRemote?.attachmentId ?? randomUUID();
  const approvalRef = options.initialRemote && 'approvalRef' in options.initialRemote ? options.initialRemote.approvalRef
    : { attachmentId, approvalId: randomUUID(), digest: 'd'.repeat(64) };
  const bindingId = options.initialRemote?.state === 'bound' ? options.initialRemote.bindingId : randomUUID();
  const primaryDexterId = randomUUID(), threadId = randomUUID();
  const billing = options.billing ? { version: 1 as const, bindingId, callSid: params.CallSid, termsDigest: 'f'.repeat(64) } : undefined;
  if (options.billingUrl) { config.harkApiBaseUrl = options.billingUrl; config.harkCallbackSigningSecret = 'b'.repeat(40); }
  const eligiblePage: EligiblePhoneCall = { billing, bindingId, callSid: params.CallSid, state: 'open', context: {
    primaryDexterId, threadId, displayName: 'Avery Example', messages: [{ id: randomUUID(), role: 'user', text: 'The earlier caller request.',
      eventSequence: '1', provenance: { messageCreatedAt: '2026-09-26T12:00:00.000Z', eventId: randomUUID(),
        eventOccurredAt: '2026-09-26T12:00:00.000Z', runId: randomUUID() } }], history: {
      version: { ownerPrincipalId: randomUUID(), primaryDexterId, threadId, contextEpoch: '1', threadEventSequence: '2', digest: 'a'.repeat(64) },
      pageDigest: 'b'.repeat(64), localContext: 'The caller has an earlier conversation.', nextBeforeSequence: null, hasMore: false, omittedForBudget: false,
    },
  } };
  const startupPages: EligiblePhoneCall[] = [];
  const transports: PreconnectTransport[] = [];
  let consumeGate: Promise<void> | undefined, releaseConsume: (() => void) | undefined;
  let assertGate: Promise<void> | undefined, releaseAssert: (() => void) | undefined;
  let voiceOptions: BrokerLiveSessionOptions | undefined;
  let remote = options.initialRemote;
  let loseConsume = false, lostBegin = false, authority = true, cancellationUnavailable = false, generationReplaced = false;
  const lifecycle: string[] = [];
  let providerGate: Promise<void> | undefined; let releaseProvider: (() => void) | undefined;
  let providerEntered: (() => void) | undefined; let providerState = 'in-progress';
  let providerDuration = options.duration; let providerWrongIdentity = false; let providerFailures = 0;
  let beginError: Error | undefined, pendingBeginError: Error | undefined;
  let beginGate: Promise<void> | undefined, releaseBegin: (() => void) | undefined;
  let statusGate: Promise<void> | undefined; let releaseStatus: (() => void) | undefined;
  const seen = { dial: 0, end: 0, voice: 0, begin: 0, consume: 0, context: 0, cancel: 0, close: 0, legacyClose: 0,
    startupFetch: 0, contextAssert: 0, opening: 0 };
  const openings: string[] = [];
  const appended: string[] = [];
  const beginTimes: number[] = [];
  const context = () => {
    seen.context++;
    if (!authority) throw new Error('authority_unavailable');
    if (options.eligible) return structuredClone(eligiblePage);
    return { billing, bindingId, callSid: params.CallSid, state: 'open' as const,
      context: { primaryDexterId, threadId, displayName: 'Avery Example', messages: [] } };
  };
  const primary = {
    ...(options.portfolio ? { portfolio: options.portfolio } : {}),
    async beginInbound(begin: InboundBeginIdentity) {
      seen.begin++; beginTimes.push(Date.now());
      if (beginGate) await beginGate;
      assert.ok(store.list()[0].inbound!.continuation || store.list()[0].inbound!.earlyAdmission);
      assert.deepEqual(begin.providerProof, store.list()[0].inbound!.begin.providerProof);
      if (beginError) throw beginError;
      assert.equal(store.list()[0].inbound!.beginAttemptedAt !== undefined, true);
      remote ??= { attachmentId, beginRef: inboundBeginRef(begin), state: 'pending', expiresAt: new Date(Date.now() + 300_000).toISOString() };
      if (pendingBeginError) throw pendingBeginError;
      if (lostBegin) { lostBegin = false; throw new Error('lost_begin'); }
      return structuredClone(remote);
    },
    async recoverInbound(begin: InboundBeginIdentity) { return remote ? structuredClone(remote) : { state: 'absent' as const, beginRef: inboundBeginRef(begin) }; },
    async inboundStatus(id: string) { assert.equal(id, attachmentId); if (statusGate) await statusGate; return structuredClone(remote!); },
    async cancelInbound(begin: InboundBeginIdentity) {
      seen.cancel++;
      if (cancellationUnavailable) throw new Error('cleanup_unavailable');
      if (remote?.state !== 'bound' && remote?.state !== 'cancelled') remote = { attachmentId, beginRef: inboundBeginRef(begin), state: 'cancelled',
        cancelledAt: new Date().toISOString(), expiresAt: remote && 'expiresAt' in remote ? remote.expiresAt : null };
      return structuredClone(remote!);
    },
    async openInboundCall(proof: { params: Record<string, string> }, ref: typeof approvalRef) {
      seen.consume++; assert.deepEqual(ref, approvalRef); assert.equal(proof.params.Direction, 'inbound');
      assert.equal(proof.params.CallSid, params.CallSid);
      assert.equal(store.list()[0].inbound!.consumeAttemptedAt !== undefined, true);
      remote = { attachmentId, beginRef: remote!.beginRef, state: 'bound', approvalRef, bindingId, billing,
        consumedAt: new Date().toISOString(), callState: 'open' };
      if (consumeGate) await consumeGate;
      if (loseConsume) { loseConsume = false; throw new Error('lost_consume'); }
      return context();
    },
    async getContext() { return context(); },
    async closeInbound(id: string, input: { begin: InboundBeginIdentity; bindingId: string; approvalRef: typeof approvalRef }) {
      seen.close++; lifecycle.push('binding-close'); assert.equal(id, attachmentId); assert.equal(input.bindingId, bindingId); assert.deepEqual(input.approvalRef, approvalRef);
      assert.deepEqual(input.begin, store.list()[0].inbound!.begin);
      assert.equal(remote?.state, 'bound');
      remote = { ...remote!, state: 'bound', approvalRef, bindingId, consumedAt: remote?.state === 'bound' ? remote.consumedAt : '', callState: 'closed' };
      return { attachmentId, beginRef: remote.beginRef, approvalRef, bindingId, callSid: params.CallSid,
        state: 'closed' as const, closedAt: new Date().toISOString() };
    },
    async close() { seen.legacyClose++; throw new Error('must_use_exact_inbound_closure'); },
  } as unknown as PrimaryApiClient;
  const provider = Object.assign((sid: string) => ({
    async fetch() { inflight++; try { if (providerGate) { providerEntered?.(); await providerGate; } } finally { inflight--; }
      if (providerFailures > 0) { providerFailures--; throw new Error('provider_read_unavailable'); }
      return { sid, accountSid: config.twilioAccountSid, direction: 'inbound', from: params.From, to: params.To, status: providerState, ...(providerWrongIdentity ? { to: '+12025550999' } : {}),
      ...(providerDuration === undefined ? {} : { duration: providerDuration, startTime: new Date('2020-01-01T10:00:00Z'), endTime: new Date(1577872800000 + Number(providerDuration) * 1000) }) }; },
    async update() { seen.end++; return { sid, status: 'completed' }; },
  }), { async create() { seen.dial++; throw new Error('inbound_must_not_dial'); } });
  // Background admission (after the continuation and while a greeting stream waits) is
  // observed through these doubles: settle() waits until none of their calls is in flight.
  let inflight = 0;
  for (const key of Object.keys(primary) as Array<keyof PrimaryApiClient>) {
    const method = (primary as unknown as Record<string, (...args: unknown[]) => Promise<unknown>>)[key];
    (primary as unknown as Record<string, unknown>)[key] = async (...args: unknown[]) => {
      inflight++; try { return await method(...args); } finally { inflight--; }
    };
  }
  const broker = createBrokerServer({ config, store, primary, provider: { calls: provider } as unknown as ReturnType<typeof Twilio>,
    ...(options.defaultAdmissionPoll ? {} : { inboundAdmissionPollMs: 5 }),
    ...(options.startupPageFreshMs === undefined ? {} : { startupPageFreshMs: options.startupPageFreshMs }),
    ...(options.greetingClip ? { inboundGreetingClip: options.greetingClip } : {}),
    ...(options.outboundClipOnly ? { greetingClip: options.outboundClipOnly } : {}),
    ...(options.selected === false ? {} : { inboundAttachment: createInboundHoldAdapter({ publicOrigin: config.publicOrigin, store }) }),
    ...(options.preconnect ? { inboundPreconnect: () => {
      const connection = new PreconnectTransport(`preconnect_${transports.length + 1}`); transports.push(connection); return connection;
    } } : {}),
    ...(options.eligible ? { eligibleContext: {
      async fetch() { seen.startupFetch++; return structuredClone(eligiblePage); },
      async assert(binding, version) {
        seen.contextAssert++; assert.equal(binding.bindingId, bindingId); assert.equal(binding.callSid, params.CallSid);
        if (assertGate) await assertGate;
        if (!authority) throw new PrimaryApiError('http_error', 403, { code: 'binding_revoked', retryable: false });
        // The first assertion made after the first Live session opened (a pre-load recheck runs before it).
        if (options.replaceFirstGeneration && seen.voice === 1 && !generationReplaced) {
          generationReplaced = true;
          eligiblePage.context.history.version = { ...eligiblePage.context.history.version, contextEpoch: '2', digest: 'c'.repeat(64) };
          throw new PrimaryApiError('http_error', 409, { code: 'context_changed', retryable: false });
        }
        assert.deepEqual(version, eligiblePage.context.history.version);
        return { bindingId, callSid: params.CallSid, current: true as const, version };
      },
      async delivery() { throw new Error('no_request_delivery_expected'); },
    } } : {}),
    voiceFactory: settings => {
      seen.voice++; voiceOptions = settings;
      if (settings.eligibleHistory) startupPages.push(structuredClone(settings.eligibleHistory.source));
      if (options.preconnect) return createBrokerLiveSession({ ...settings, closeTimeoutMs: 100,
        connectionFactory: settings.connectionFactory ?? (() => {
          const connection = new PreconnectTransport(`preconnect_${transports.length + 1}`); transports.push(connection); queueMicrotask(() => connection.open()); return connection;
        }),
      });
      queueMicrotask(() => settings.onReady({ callId: settings.callId, sessionId: 'inbound-fixture' }));
      return { appendAudio(payload: string) { appended.push(payload); return true; }, async appendContext() { return { eventId: 'c', status: 'accepted' }; },
        async appendInstructions(text) { seen.opening++; openings.push(text); return { eventId: 'i', status: 'accepted' }; }, async interruptPlayback() { return { eventId: 'p', status: 'accepted' }; },
        resumePlayback() {}, getTranscript() { return { revision: 0, callerRevision: 0, fragments: [] }; },
        async close(reason) { lifecycle.push('live-close'); return { callId: settings.callId, sessionId: 'inbound-fixture', finalized: true,
          transcriptPersisted: true, outputAudioDiscarded: false, reason: reason ?? 'closed' }; } };
    },
  });
  await new Promise<void>(resolve => broker.server.listen(0, '127.0.0.1', resolve));
  const base = `http://127.0.0.1:${(broker.server.address() as { port: number }).port}`;
  const webhook = (route: string, patch: Record<string, string> = {}, valid = true) => {
    const body = { ...params, ...patch };
    return fetch(base + route, { method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded',
      'x-twilio-signature': valid ? Twilio.getExpectedTwilioSignature(config.twilioAuthToken, config.publicOrigin + route, body) : 'wrong' }, body: new URLSearchParams(body) });
  };
  let closed = false;
  const close = async () => { if (!closed) { closed = true; await broker.close(); } };
  async function settle() {
    for (let quiet = 0, n = 0; quiet < 3 && n < 400; n++) {
      await new Promise(resolve => setTimeout(resolve, 5)); quiet = inflight === 0 ? quiet + 1 : 0;
    }
  }
  t.after(async () => { releaseBegin?.(); await close(); fs.rmSync(dir, { recursive: true, force: true }); });
  return { broker, store, base, config, params, seen, openings, appended, lifecycle, webhook, close, provider, billing, transports, beginTimes,
    gateBegin() { beginGate = new Promise<void>(resolve => { releaseBegin = resolve; }); return () => releaseBegin!(); },
    gateConsume() { consumeGate = new Promise<void>(resolve => { releaseConsume = resolve; }); return () => releaseConsume!(); },
    gateAssert() { assertGate = new Promise<void>(resolve => { releaseAssert = resolve; }); return () => releaseAssert!(); },
    providerEnded(duration = providerDuration) { providerState = 'completed'; providerDuration = duration; },
    wrongProviderIdentity() { providerWrongIdentity = true; },
    failProviderReads(count: number) { providerFailures = count; },
    emitAudio(bytes = Buffer.alloc(800, 0)) { voiceOptions!.onAudio(bytes.toString('base64')); },
    clearAudio() { voiceOptions!.onPlaybackClear?.(); }, eligiblePage, startupPages, remote: () => remote,
    /** The signed continuation answers with the greeting stream at once; its admission runs behind it. */
    async waitForAdmission() {
      const call = store.list()[0];
      assert.match(await (await webhook(`/broker/inbound/${call.id}/continue`)).text(), /<Connect><Stream/);
      await settle();
    },
    settle,
    beginError(error?: Error) { beginError = error; },
    pendingBeginError(error?: Error) { pendingBeginError = error; },
    approve() { assert.ok(remote?.state === 'pending'); remote = { ...remote, state: 'approved', approvalRef }; },
    loseBegin() { lostBegin = true; }, loseConsume() { loseConsume = true; }, refuseContext() { authority = false; },
    refuseCancellation() { cancellationUnavailable = true; },
    gateProvider() {
      providerGate = new Promise<void>(resolve => { releaseProvider = resolve; });
      const entered = new Promise<void>(resolve => { providerEntered = resolve; });
      return { entered, release(status: string) { providerState = status; releaseProvider!(); } };
    },
    gateStatus() { statusGate = new Promise<void>(resolve => { releaseStatus = resolve; }); return () => releaseStatus!(); },
  };
}

test('actual HTTP incoming waits on same leg and approved continuation enters existing voice path', async t => {
  const f = await fixture(t);
  const incoming = await f.webhook('/broker/incoming-call');
  const wait = await incoming.text();
  assert.equal(incoming.status, 200); assert.match(wait, /<Redirect/); assert.doesNotMatch(wait, /<Gather|<Stream/);
  assert.equal(f.seen.dial, 0); assert.equal(f.seen.context, 0); assert.equal(f.seen.voice, 0);
  const call = f.store.list()[0]; await f.waitForAdmission(); f.approve();
  const response = await f.webhook(`/broker/inbound/${call.id}/continue`);
  assert.equal(response.status, 200); assert.match(await response.text(), /<Stream/);
  assert.equal(f.seen.consume, 1); assert.equal(f.seen.dial, 0);
  const route = `/broker/media/${call.id}`;
  const ws = new WebSocket(f.base.replace(/^http/, 'ws') + route, { headers: {
    'x-twilio-signature': Twilio.getExpectedTwilioSignature(f.config.twilioAuthToken, f.config.publicOrigin + route, {}),
  } });
  await new Promise<void>((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
  ws.send(JSON.stringify({ event: 'start', start: { accountSid: f.config.twilioAccountSid, callSid: f.params.CallSid,
    streamSid: 'MZ' + '4'.repeat(32), customParameters: { token: streamToken(f.config.controlToken, call.id, f.params.CallSid) },
    mediaFormat: { encoding: 'audio/x-mulaw', sampleRate: 8000, channels: 1 } } }));
  await new Promise(resolve => setTimeout(resolve, 20));
  assert.equal(f.seen.voice, 1); assert.ok(f.store.get(call.id)!.mediaStartedAt);
  // Twilio runs the trailing redirect only after the stream ends: the leg is hung up, never re-streamed.
  const repeat = await f.webhook(`/broker/inbound/${call.id}/continue`);
  assert.equal(repeat.status, 200); assert.equal(await repeat.text(), '<Response><Hangup/></Response>');
  assert.equal(f.seen.voice, 1);
  ws.close(); await new Promise<void>(resolve => ws.once('close', () => resolve()));
});

test('the trailing redirect after a restart that lost an attached call hangs up instead of an application error', async t => {
  const f = await fixture(t);
  const { call, ws } = await connectIncomingMedia(f); t.after(() => ws.close());
  const attached = f.store.get(call.id)!;
  assert.ok(attached.mediaStartedAt); assert.equal(attached.endedAt, undefined);
  // The process crashed with Live attached: a new broker reads the journal, with no stream of its own.
  const restarted = await fixture(t, { retained: attached, initialRemote: attached.inbound!.remote });
  const response = await restarted.webhook(`/broker/inbound/${call.id}/continue`);
  assert.equal(response.status, 200);
  assert.equal(response.headers.get('content-type'), 'text/xml');
  assert.equal(await response.text(), '<Response><Hangup/></Response>');
  assert.equal(restarted.seen.voice, 0); assert.equal(restarted.seen.begin, 0); assert.equal(restarted.seen.consume, 0);
  // The ended leg's status callback then closes the attachment, as for any ended call.
  assert.equal((await restarted.webhook('/broker/status', { CallStatus: 'completed' })).status, 200);
  await restarted.settle();
  const closed = restarted.store.get(call.id)!;
  assert.equal(closed.status, 'completed'); assert.equal(closed.inbound!.closure?.state, 'closed');
});

for (const fresh of [true, false])
test(`admitted eligible history reaches first voice startup without a duplicate GET (${fresh ? 'fresh binding response: post-open assertion only' : 'older page: pre-load and post-open assertions'})`, async t => {
  const f = await fixture(t, { eligible: true, ...(fresh ? {} : { startupPageFreshMs: 0 }) });
  await (await f.webhook('/broker/incoming-call')).text();
  const call = f.store.list()[0]; await f.waitForAdmission(); f.approve();
  assert.match(await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text(), /<Stream/);
  assert.equal(f.seen.context, 1);
  assert.equal('history' in f.store.get(call.id)!.primary!, false);
  assert.doesNotMatch(fs.readFileSync(path.join(f.config.stateDir, 'calls.json'), 'utf8'), /"history":|"pageDigest":/);
  const route = `/broker/media/${call.id}`;
  const ws = new WebSocket(f.base.replace(/^http/, 'ws') + route, { headers: {
    'x-twilio-signature': Twilio.getExpectedTwilioSignature(f.config.twilioAuthToken, f.config.publicOrigin + route, {}),
  } });
  t.after(() => ws.close());
  await new Promise<void>((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
  ws.send(JSON.stringify({ event: 'start', start: { accountSid: f.config.twilioAccountSid, callSid: f.params.CallSid,
    streamSid: 'MZ' + '4'.repeat(32), customParameters: { token: streamToken(f.config.controlToken, call.id, f.params.CallSid) },
    mediaFormat: { encoding: 'audio/x-mulaw', sampleRate: 8000, channels: 1 } } }));
  for (let count = 0; count < 100 && f.seen.opening === 0; count++) await new Promise(resolve => setTimeout(resolve, 10));
  assert.equal(f.seen.opening, 1);
  assert.equal(f.seen.contextAssert, fresh ? 1 : 2, fresh ? 'the binding response qualifies the page; the post-open check qualifies the greeting'
    : 'pre-load and post-open checks also qualify the initial greeting');
  assert.equal(f.seen.startupFetch, 0); assert.equal(f.seen.context, 1);
  assert.deepEqual(f.startupPages, [f.eligiblePage]);
  ws.close(); await new Promise<void>(resolve => ws.once('close', () => resolve()));
});

test('lost registration followed by terminal callback closes exact inbound binding after authority loss', async t => {
  const f = await fixture(t); await (await f.webhook('/broker/incoming-call')).text();
  const call = f.store.list()[0]; await f.waitForAdmission(); f.approve(); f.loseConsume();
  const lost = await f.webhook(`/broker/inbound/${call.id}/continue`);
  assert.match(await lost.text(), /<Redirect/);
  assert.equal(f.store.get(call.id)!.primary, undefined); f.refuseContext();
  const terminal = await f.webhook('/broker/status', { CallStatus: 'completed' });
  assert.equal(terminal.status, 200); await terminal.text();
  const ended = f.store.get(call.id)!;
  assert.equal(ended.inbound!.closure?.state, 'closed');
  assert.equal(ended.primary, undefined); assert.equal(ended.transcriptCapture, undefined);
  assert.equal(f.seen.legacyClose, 0); assert.equal(f.seen.close, 1); assert.equal(f.seen.context, 0);
});

test('lost begin stays a recoverable wait and terminal callback cancels it', async t => {
  const f = await fixture(t); f.loseBegin();
  const first = await f.webhook('/broker/incoming-call'); assert.match(await first.text(), /<Redirect/);
  const call = f.store.list()[0]; assert.equal(f.seen.begin, 0, 'no admission contact before the signed continuation');
  await f.waitForAdmission();
  assert.equal(f.seen.begin, 1); assert.equal(f.store.get(call.id)!.inbound!.remote, undefined);
  await f.broker.maintain();
  assert.equal(f.seen.begin, 2); assert.equal(f.store.get(call.id)!.inbound!.remote?.state, 'pending');
  const terminal = await f.webhook('/broker/status', { CallStatus: 'completed' }); await terminal.text();
  assert.equal(f.store.get(call.id)!.inbound!.remote?.state, 'cancelled');
  assert.equal(f.seen.context, 0); assert.equal(f.seen.consume, 0);
});

test('same-call route refuses a different signed caller and legacy callback request', async t => {
  const f = await fixture(t); await (await f.webhook('/broker/incoming-call')).text(); const call = f.store.list()[0];
  const foreign = await f.webhook(`/broker/inbound/${call.id}/continue`, { From: '+12025550999' });
  assert.equal(foreign.status, 403); await foreign.text();
  const callback = await f.webhook(`/broker/callback/${call.id}`, { Digits: '1' });
  assert.equal(callback.status, 409); await callback.text();
  assert.equal(f.seen.consume, 0); assert.equal(f.seen.dial, 0); assert.equal(f.seen.context, 0);
});

test('unselected adapter preserves existing callback behavior for new incoming calls', async t => {
  const f = await fixture(t, { selected: false });
  const response = await f.webhook('/broker/incoming-call'); assert.match(await response.text(), /<Gather/);
  assert.equal(f.store.list()[0].inbound, undefined); assert.equal(f.seen.begin, 0);
});

test('shutdown fences all saved waiting calls, including restored calls absent from this process sessions', async t => {
  const first = await fixture(t); await (await first.webhook('/broker/incoming-call')).text(); await first.waitForAdmission(); first.approve();
  const saved = first.store.list()[0]; const remote = first.remote()!;
  const restarted = await fixture(t, { selected: false, retained: saved, initialRemote: remote });
  await restarted.close();
  const disk = JSON.parse(fs.readFileSync(path.join(restarted.config.stateDir, 'calls.json'), 'utf8')).calls[saved.id];
  assert.equal(disk.inbound.localStop.reason, 'service_shutdown'); assert.equal(disk.inbound.remote.state, 'cancelled');
  assert.equal(restarted.seen.consume, 0); assert.equal(restarted.seen.context, 0); assert.equal(restarted.seen.end, 1);
});

test('disabled adapter cannot consume a retained approved call through a continuation', async t => {
  const first = await fixture(t); await (await first.webhook('/broker/incoming-call')).text(); await first.waitForAdmission(); first.approve();
  const saved = first.store.list()[0];
  const restarted = await fixture(t, { selected: false, retained: saved, initialRemote: first.remote()! });
  const result = await restarted.webhook(`/broker/inbound/${saved.id}/continue`);
  assert.match(await result.text(), /<Hangup/);
  assert.equal(restarted.seen.consume, 0); assert.equal(restarted.seen.context, 0);
  assert.equal(restarted.store.get(saved.id)!.inbound!.remote?.state, 'cancelled');
});


test('shutdown stop is saved before an in-flight approved status can consume', async t => {
  const f = await fixture(t); await (await f.webhook('/broker/incoming-call')).text(); await f.waitForAdmission(); f.approve();
  const call = f.store.list()[0]; const release = f.gateStatus();
  const continuing = f.webhook(`/broker/inbound/${call.id}/continue`);
  await new Promise(resolve => setTimeout(resolve, 20));
  const closing = f.close();
  assert.equal(f.store.get(call.id)!.inbound!.localStop?.reason, 'service_shutdown');
  release();
  const response = await continuing; await response.text(); await closing;
  assert.equal(f.seen.consume, 0); assert.equal(f.seen.context, 0);
});

test('stopped wait terminates provider leg even while API cancellation fails', async t => {
  const f = await fixture(t); await (await f.webhook('/broker/incoming-call')).text();
  const call = f.store.list()[0]; await f.waitForAdmission(); f.refuseCancellation();
  f.store.transitionInbound(call.id, { type: 'stop', observedAt: new Date().toISOString(), reason: 'broker_cancelled' });
  await f.broker.maintain();
  assert.equal(f.seen.end, 1);
  assert.equal(f.store.get(call.id)!.status, 'completed');
  assert.equal(f.store.get(call.id)!.inbound!.remote?.state, 'pending');
  assert.equal(f.store.get(call.id)!.inbound!.closure, undefined);
});

test('enabling direct attachment leaves historical callback webhook retries in their original mode', async t => {
  const previous = await fixture(t, { selected: false }); await (await previous.webhook('/broker/incoming-call')).text();
  const saved = previous.store.list()[0];
  const next = await fixture(t, { selected: true, retained: saved });
  const retry = await next.webhook('/broker/incoming-call');
  assert.equal(retry.status, 200); assert.match(await retry.text(), /<Gather/);
  assert.equal(next.store.get(saved.id)!.inbound, undefined); assert.equal(next.seen.begin, 0);
});


test('terminal provider result arriving after media starts finalizes voice before binding closure', async t => {
  const f = await fixture(t); await (await f.webhook('/broker/incoming-call')).text(); await f.waitForAdmission(); f.approve();
  const call = f.store.list()[0]; await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text(); await f.settle();
  assert.equal(f.store.get(call.id)!.inbound!.remote?.state, 'bound');
  const gate = f.gateProvider(); const maintaining = f.broker.maintain(); await gate.entered;
  const route = `/broker/media/${call.id}`;
  const ws = new WebSocket(f.base.replace(/^http/, 'ws') + route, { headers: {
    'x-twilio-signature': Twilio.getExpectedTwilioSignature(f.config.twilioAuthToken, f.config.publicOrigin + route, {}),
  } });
  await new Promise<void>((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
  ws.send(JSON.stringify({ event: 'start', start: { accountSid: f.config.twilioAccountSid, callSid: f.params.CallSid,
    streamSid: 'MZ' + '4'.repeat(32), customParameters: { token: streamToken(f.config.controlToken, call.id, f.params.CallSid) },
    mediaFormat: { encoding: 'audio/x-mulaw', sampleRate: 8000, channels: 1 } } }));
  await new Promise(resolve => setTimeout(resolve, 20)); assert.equal(f.seen.voice, 1);
  gate.release('completed'); await maintaining;
  assert.deepEqual(f.lifecycle, ['live-close', 'binding-close']);
  assert.equal(f.store.get(call.id)!.inbound!.closure?.state, 'closed');
  assert.ok(f.store.get(call.id)!.transcriptCapture);
  if (ws.readyState !== WebSocket.CLOSED) await new Promise<void>(resolve => ws.once('close', () => resolve()));
});

test('the first leg is a bare redirect; only the signed continuation starts admission, and it answers with the stream', async t => {
  const f = await fixture(t);
  const first = await f.webhook('/broker/incoming-call'); const xml = await first.text();
  // No verb answers the leg: Twilio fetches the continuation while the caller still hears ringing.
  assert.match(xml, /^<\?xml[^>]*><Response><Redirect method="POST">https:\/\/phone\.example\.test\/broker\/inbound\/[0-9a-f-]{36}\/continue<\/Redirect><\/Response>$/);
  assert.equal(f.seen.begin, 0); assert.equal(f.seen.context, 0);
  const saved = f.store.list()[0]; const waitUntil = saved.inbound!.waitUntil;
  await f.broker.maintain(); assert.equal(f.seen.begin, 0, 'maintenance cannot begin admission before the continuation');
  const restored = await fixture(t, { retained: saved });
  await restored.broker.maintain(); assert.equal(restored.seen.begin, 0);
  const bad = await restored.webhook(`/broker/inbound/${saved.id}/continue`, {}, false);
  assert.equal(bad.status, 403); await bad.text();
  assert.equal(restored.store.get(saved.id)!.inbound!.continuation, undefined);
  const continued = await restored.webhook(`/broker/inbound/${saved.id}/continue`);
  const stream = await continued.text();
  assert.match(stream, /^<\?xml[^>]*><Response><Connect><Stream url="wss:\/\/phone\.example\.test\/broker\/media\/[0-9a-f-]{36}"><Parameter name="token" value="[^"]+"\/><\/Stream><\/Connect><Redirect method="POST">https:\/\/phone\.example\.test\/broker\/inbound\/[0-9a-f-]{36}\/continue<\/Redirect><\/Response>$/);
  assert.doesNotMatch(stream, /<Say|<Play|<Pause|<Gather|<Hangup/);
  await restored.settle();
  assert.equal(restored.seen.begin, 1, 'admission starts once the stream answered the leg'); assert.equal(restored.seen.context, 0);
  assert.ok(restored.store.get(saved.id)!.inbound!.continuation);
  assert.equal(restored.store.get(saved.id)!.inbound!.waitUntil, waitUntil);
  assert.deepEqual(restored.store.get(saved.id)!.inbound!.begin.providerProof, saved.inbound!.begin.providerProof);
  assert.equal(restored.seen.dial, 0);
});

for (const code of ['inbound_carrier_attestation_required', 'inbound_enrollment_required'] as const) {
  test(`actual HTTP ${code} ends the wait and the next continuation hangs up while unavailable cancellation remains pending`, async t => {
    const f = await fixture(t);
    f.beginError(new PrimaryApiError('http_error', 403, { code, retryable: false }));
    f.refuseCancellation();
    await (await f.webhook('/broker/incoming-call')).text();
    const call = f.store.list()[0];
    await f.waitForAdmission();
    const saved = f.store.get(call.id)!;
    assert.equal(saved.inbound!.admissionRefusal?.code, code);
    assert.equal(saved.inbound!.localStop?.reason, code);
    assert.ok(saved.inbound!.cancelRequestedAt); assert.equal(saved.inbound!.remote, undefined);
    assert.equal(saved.inbound!.closure, undefined); assert.equal(f.seen.context, 0); assert.equal(f.seen.consume, 0);
    // The greeting stream hands the leg back; its trailing redirect now receives the hangup.
    const response = await f.webhook(`/broker/inbound/${call.id}/continue`); const xml = await response.text();
    assert.match(xml, /<Hangup/); assert.doesNotMatch(xml, /<Stream|<Gather|enrollment|attestation/);
    f.beginError(); await f.broker.maintain();
    assert.equal(f.seen.begin, 1); assert.equal(f.seen.end, 1);
    assert.equal(f.store.get(call.id)!.inbound!.remote, undefined);
  });
}

test('actual HTTP retryable carrier503 keeps the wait and preserves one original begin', async t => {
  const f = await fixture(t); await (await f.webhook('/broker/incoming-call')).text();
  const call = f.store.list()[0];
  f.beginError(new PrimaryApiError('http_error', 503, { code: 'inbound_carrier_admission_unavailable', retryable: true }));
  await f.waitForAdmission();
  const attempted = f.store.get(call.id)!;
  assert.ok(attempted.failure, 'ordinary admission failure remains reported');
  assert.equal(attempted.inbound!.localStop, undefined); assert.equal(attempted.inbound!.admissionRefusal, undefined);
  f.beginError(); await f.broker.maintain();
  assert.equal(f.seen.begin, 2); assert.equal(f.store.list().length, 1);
  assert.equal(f.store.get(call.id)!.inbound!.remote?.state, 'pending');
  assert.deepEqual(f.store.get(call.id)!.inbound!.begin, attempted.inbound!.begin);
  assert.equal(f.store.get(call.id)!.inbound!.waitUntil, call.inbound!.waitUntil);
  assert.equal(f.seen.context, 0); assert.equal(f.seen.dial, 0);
});

test('hold adapter refuses a different continuation destination', async t => {
  const f = await fixture(t); await (await f.webhook('/broker/incoming-call')).text();
  const call = f.store.list()[0]; const adapter = createInboundHoldAdapter({ publicOrigin: f.config.publicOrigin, store: f.store });
  const before = f.seen.begin;
  assert.throws(() => adapter.waitingResponse({ callId: call.id, continuationUrl: 'https://other.example/broker/inbound/' + call.id + '/continue' }), /mismatch/);
  assert.equal(f.seen.begin, before);
});

async function preconnectApproved(f: Awaited<ReturnType<typeof fixture>>) {
  await (await f.webhook('/broker/incoming-call')).text();
  await f.waitForAdmission(); f.approve(); return f.store.list()[0];
}
async function preconnectUntil(predicate: () => boolean) {
  for (let n = 0; n < 150 && !predicate(); n++) await new Promise(resolve => setTimeout(resolve, 10));
  assert.equal(predicate(), true, 'expected preconnect boundary');
}
async function preconnectMedia(f: Awaited<ReturnType<typeof fixture>>, call: BrokerCall, token?: string) {
  const route = `/broker/media/${call.id}`;
  const ws = new WebSocket(f.base.replace(/^http/, 'ws') + route, { headers: {
    'x-twilio-signature': Twilio.getExpectedTwilioSignature(f.config.twilioAuthToken, f.config.publicOrigin + route, {}),
  } });
  await new Promise<void>((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
  ws.send(JSON.stringify({ event: 'start', start: { accountSid: f.config.twilioAccountSid, callSid: f.params.CallSid,
    streamSid: 'MZ' + '4'.repeat(32), customParameters: { token: token ?? streamToken(f.config.controlToken, call.id, f.params.CallSid) },
    mediaFormat: { encoding: 'audio/x-mulaw', sampleRate: 8000, channels: 1 } } }));
  return ws;
}

test('bound inbound fetch runs before Live, leaves opening free, and delivers quiet portfolio context before a question', async t => {
  let reads = 0, release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  const f = await fixture(t, { eligible: true, preconnect: true, portfolio: async bindingId => {
    reads++;
    assert.equal(f.store.list()[0].primary?.bindingId, bindingId);
    await gate;
    return { version: 'phone.portfolio.v1', bindingId, observedAt: '2026-10-17T12:52:00.000Z', totalUsd: '28.86',
      holdingsComplete: true, holdingCount: 1, unpricedCount: 0, omittedCount: 0,
      holdings: [{ name: 'USD Coin', symbol: 'USDC', assetClass: 'token', amount: '28.86', valueUsd: '28.86', change24hPercent: null }] };
  } }); t.after(release);
  const call = await preconnectApproved(f);
  assert.equal(reads, 0, 'no portfolio access before a bound account');
  await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text();
  await preconnectUntil(() => reads === 1);
  assert.equal(f.seen.voice, 0, 'fetch starts at binding, before voice construction');
  const transport = f.transports[0]; transport.open();
  const ws = await preconnectMedia(f, call); t.after(() => ws.close());
  await preconnectUntil(() => f.store.get(call.id)!.voiceOpening?.status === 'accepted');
  assert.equal(transport.sent.some(event => event.type === 'session.thinking.append'), false, 'opening completed while fetch is pending');
  release();
  await preconnectUntil(() => transport.sent.some(event => event.type === 'session.thinking.append' && event.content.includes('only current portfolio note')));
  const notes = transport.sent.filter(event => event.type === 'session.thinking.append');
  assert.ok(notes.every(event => event.delegation_id === null && Buffer.byteLength(event.content) <= 500));
  assert.match(notes.map(event => event.content).join(' '), /as of 2026-10-17T12:52:00.000Z: Your portfolio totals about \$28.86/);
  assert.equal(transport.sent.some(event => event.type === 'session.commentary.append'), false);
  transport.receive({ type: 'session.input_transcript.delta', event_id: 'portfolio_question', delta: "What's in my portfolio?",
    start_ms: 1000, end_ms: 1500 });
  await preconnectUntil(() => f.store.get(call.id)!.liveGenerations!.segments.some(segment =>
    segment.transcript.fragments.some(fragment => fragment.eventId === 'portfolio_question')));
  assert.equal(Object.keys(f.store.get(call.id)!.delegations).length, 0, 'a transcript alone still does not submit work');
  assert.equal(reads, 1);
});

for (const fresh of [true, false])
test(`approved inbound transport overlaps context I/O and stays command-free through media and ${fresh ? 'the post-open' : 'the pre-start'} assertion`, async t => {
  const f = await fixture(t, { eligible: true, preconnect: true, ...(fresh ? {} : { startupPageFreshMs: 0 }) });
  const call = await preconnectApproved(f), release = f.gateConsume();
  const response = f.webhook(`/broker/inbound/${call.id}/continue`);
  await preconnectUntil(() => f.seen.consume === 1);
  assert.equal(f.transports.length, 1); const transport = f.transports[0];
  transport.open(); // The handshake finishes while the ordinary account lookup is still pending.
  assert.equal(f.seen.context, 0); assert.deepEqual(transport.sent, []);
  release(); assert.match(await (await response).text(), /<Stream/);
  assert.deepEqual(transport.sent, []);
  const releaseAssert = f.gateAssert(), ws = await preconnectMedia(f, call); t.after(() => ws.close());
  await preconnectUntil(() => f.seen.contextAssert === 1);
  if (fresh) {
    // The binding response just qualified the page: Live opens on it at once, but neither the
    // opening nor any caller audio passes until the post-open assertion succeeds.
    assert.equal(f.seen.voice, 1);
    assert.deepEqual(transport.sent.map(event => event.type), ['session.start']);
    assert.equal(f.store.get(call.id)!.voiceOpening, undefined);
  } else { assert.deepEqual(transport.sent, []); assert.equal(f.seen.voice, 0); }
  releaseAssert();
  await preconnectUntil(() => f.store.get(call.id)!.voiceOpening?.status === 'accepted');
  assert.equal(f.transports.length, 1); assert.equal(f.seen.contextAssert, fresh ? 1 : 2, 'opening shares the validated activation');
  assert.equal(f.seen.startupFetch, 0); assert.equal(f.seen.context, 1);
  assert.equal(transport.sent.filter(event => event.type === 'session.start').length, 1);
  assert.deepEqual(f.startupPages, [f.eligiblePage]);
});

test('duplicate approved continuations share the original transport and account consume', async t => {
  const f = await fixture(t, { eligible: true, preconnect: true });
  const call = await preconnectApproved(f), release = f.gateConsume();
  const first = f.webhook(`/broker/inbound/${call.id}/continue`);
  await preconnectUntil(() => f.seen.consume === 1);
  const duplicate = f.webhook(`/broker/inbound/${call.id}/continue`);
  await new Promise(resolve => setTimeout(resolve, 20));
  assert.equal(f.transports.length, 1); assert.equal(f.seen.consume, 1);
  release(); await (await first).text(); await (await duplicate).text();
  assert.equal(f.transports.length, 1); assert.deepEqual(f.transports[0].sent, []);
  await f.close(); assert.equal(f.transports[0].terminated, 1);
});

test('the signed continuation opens the Live socket while admission runs, and Live starts on it without a second connect', async t => {
  const transportEvents: Array<{ stage: string; at: number }> = [];
  t.mock.method(console, 'info', (value: string) => {
    const event = JSON.parse(value); if (event.event === 'broker_inbound_transport') transportEvents.push({ stage: event.stage, at: Date.now() });
  });
  const f = await fixture(t, { eligible: true, preconnect: true });
  await (await f.webhook('/broker/incoming-call')).text();
  const call = f.store.list()[0];
  assert.equal(f.transports.length, 0, 'the unanswered first leg opens nothing');
  const release = f.gateBegin(); t.after(release);
  assert.match(await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text(), /<Connect><Stream/);
  assert.equal(f.transports.length, 1, 'opened with the stream response, before admission answers');
  const transport = f.transports[0]; transport.open();
  assert.deepEqual(transport.sent, [], 'only the socket: no session, history or audio');
  assert.equal(f.seen.context, 0);
  release(); await f.settle(); f.approve();
  const ws = await preconnectMedia(f, call); t.after(() => ws.close());
  await preconnectUntil(() => f.store.get(call.id)!.voiceOpening?.status === 'accepted');
  assert.equal(f.transports.length, 1, 'no second connect');
  assert.equal(transport.sent[0]?.type, 'session.start');
  assert.equal(transport.sent.filter(event => event.type === 'session.start').length, 1);
  assert.deepEqual(transportEvents.map(event => event.stage), ['connecting', 'open', 'adopted']);
});

test('a continuation socket that failed before approval is replaced at approval and Live starts on the replacement', async t => {
  const f = await fixture(t, { eligible: true, preconnect: true });
  await (await f.webhook('/broker/incoming-call')).text(); await f.waitForAdmission();
  const call = f.store.list()[0];
  assert.equal(f.transports.length, 1); f.transports[0].fail(); // For example, the provider closed an idle socket.
  assert.equal(f.transports[0].terminated, 1);
  f.approve();
  const ws = await preconnectMedia(f, call); t.after(() => ws.close());
  await preconnectUntil(() => f.transports.length === 2);
  f.transports[1].open();
  await preconnectUntil(() => f.store.get(call.id)!.voiceOpening?.status === 'accepted');
  assert.equal(f.transports.length, 2); assert.deepEqual(f.transports[0].sent, []);
  assert.equal(f.transports[1].sent[0]?.type, 'session.start');
});

for (const refusal of ['unapproved', 'wrong-provider', 'provider-ended'] as const)
  test(`an ${refusal} admission records no context and sends no command to the Live socket`, async t => {
    const f = await fixture(t, { eligible: true, preconnect: true });
    await (await f.webhook('/broker/incoming-call')).text(); await f.waitForAdmission();
    if (refusal !== 'unapproved') f.approve();
    if (refusal === 'wrong-provider') f.wrongProviderIdentity();
    if (refusal === 'provider-ended') f.providerEnded();
    const call = f.store.list()[0]; await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text();
    await f.settle();
    // The broker's provider check runs alongside the API's consume; context needs both to pass.
    assert.equal(f.seen.consume, refusal === 'unapproved' ? 0 : 1);
    const saved = f.store.get(call.id)!;
    assert.equal(saved.primary, undefined, 'no account context is recorded');
    assert.equal(f.seen.voice, 0);
    assert.equal(f.transports.length, 1, 'only the socket the continuation opened');
    assert.deepEqual(f.transports[0].sent, [], 'no command on the Live socket');
    if (refusal === 'unapproved') assert.equal(f.transports[0].terminated, 0, 'a waiting admission keeps it');
    else {
      // A leg that ended, or that Twilio describes differently from the signed one, ends the wait at once.
      assert.equal(saved.inbound!.localStop?.reason, refusal === 'provider-ended' ? 'provider_terminal' : 'provider_identity_mismatch');
      assert.equal(saved.inbound!.closure?.state, 'closed', 'the binding made alongside the check is closed');
      assert.equal(f.transports[0].terminated, 1);
    }
  });

for (const failure of ['media-token', 'revoked', 'hangup', 'shutdown'] as const)
  test(`inbound preconnect cleans ${failure} without private commands or automatic replacement`, async t => {
    // Revocation between an older admission and media: the pre-load recheck refuses before Live opens.
    const f = await fixture(t, { eligible: true, preconnect: true, ...(failure === 'revoked' ? { startupPageFreshMs: 0 } : {}) });
    const call = await preconnectApproved(f);
    await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text();
    assert.equal(f.transports.length, 1); const transport = f.transports[0]; transport.open();
    if (failure === 'revoked') f.refuseContext();
    if (failure === 'hangup') await (await f.webhook('/broker/status', { CallStatus: 'completed' })).text();
    else if (failure === 'shutdown') await f.close();
    else {
      const ws = await preconnectMedia(f, call, failure === 'media-token' ? 'wrong' : undefined);
      t.after(() => ws.close());
      await preconnectUntil(() => ws.readyState === WebSocket.CLOSED);
    }
    assert.equal(transport.terminated, 1); assert.deepEqual(transport.sent, []);
    assert.equal(f.transports.length, 1);
  });

test('a lost consume reply recovers the binding and Live still starts on the continuation socket', async t => {
  const f = await fixture(t, { eligible: true, preconnect: true });
  const call = await preconnectApproved(f);
  f.loseConsume();
  await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text();
  await f.settle();
  assert.equal(f.transports.length, 1); const transport = f.transports[0];
  assert.equal(transport.terminated, 0, 'a step the next poll retries keeps the socket'); transport.open();
  const ws = await preconnectMedia(f, call); t.after(() => ws.close()); // Its admission loop recovers the committed binding.
  await preconnectUntil(() => f.store.get(call.id)!.voiceOpening?.status === 'accepted');
  assert.equal(f.transports.length, 1); assert.equal(f.seen.consume, 1);
  assert.equal(transport.sent[0]?.type, 'session.start');
});

for (const opened of ['continuation', 'approval'] as const)
  test(`two failed provider reads after the consume keep the socket opened at the ${opened}, and Live starts on it once a read passes`, async t => {
    const f = await fixture(t, { eligible: true, preconnect: true });
    await (await f.webhook('/broker/incoming-call')).text(); await f.waitForAdmission();
    const call = f.store.list()[0];
    // A socket opened at approval (the continuation's had failed) is the one held to the original rule.
    if (opened === 'approval') f.transports[0].fail();
    f.approve(); f.failProviderReads(2);
    await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text();
    await f.settle();
    // The consume committed alongside the first failed read: bound, with no context recorded.
    assert.equal(f.store.get(call.id)!.inbound!.remote?.state, 'bound'); assert.equal(f.store.get(call.id)!.primary, undefined);
    const transport = f.transports.at(-1)!; transport.open();
    assert.equal(f.transports.length, opened === 'approval' ? 2 : 1);
    const ws = await preconnectMedia(f, call); t.after(() => ws.close()); // The next poll's read fails too; the one after passes.
    await preconnectUntil(() => f.store.get(call.id)!.voiceOpening?.status === 'accepted');
    assert.equal(f.transports.length, opened === 'approval' ? 2 : 1); assert.equal(transport.terminated, 0);
    assert.equal(transport.sent[0]?.type, 'session.start'); assert.equal(f.seen.consume, 1);
  });

for (const loss of ['error', 'close'] as const)
  test(`a continuation socket lost (${loss}) after approval leaves Live its own connection`, async t => {
    const f = await fixture(t, { eligible: true, preconnect: true });
    const call = await preconnectApproved(f);
    await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text();
    assert.equal(f.transports.length, 1); const transport = f.transports[0]; transport.open();
    if (loss === 'error') transport.fail(); else transport.end();
    assert.equal(transport.terminated, 1);
    const ws = await preconnectMedia(f, call); t.after(() => ws.close());
    await preconnectUntil(() => f.store.get(call.id)!.voiceOpening?.status === 'accepted');
    assert.equal(f.transports.length, 2, 'one ordinary connection, no warm replacement loop');
    assert.deepEqual(transport.sent, []); assert.equal(f.transports[1].sent[0]?.type, 'session.start');
  });

test('a socket opened at approval because the continuation socket failed keeps the original rule: lost, Live cannot start', async t => {
  const f = await fixture(t, { eligible: true, preconnect: true });
  await (await f.webhook('/broker/incoming-call')).text(); await f.waitForAdmission();
  const call = f.store.list()[0];
  f.transports[0].fail(); f.approve();
  await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text();
  await preconnectUntil(() => f.transports.length === 2);
  const strict = f.transports[1]; strict.open(); strict.fail();
  const ws = await preconnectMedia(f, call); t.after(() => ws.close());
  await preconnectUntil(() => ws.readyState === WebSocket.CLOSED);
  assert.equal(f.transports.length, 2, 'no automatic replacement'); assert.deepEqual(strict.sent, []);
  assert.equal(f.seen.opening, 0);
});

test('a begin refused while Twilio picks up keeps the continuation socket: Live starts on it with no second connect', async t => {
  const f = await fixture(t, { eligible: true, preconnect: true });
  await (await f.webhook('/broker/incoming-call')).text();
  const call = f.store.list()[0];
  f.beginError(new PrimaryApiError('http_error', 403, { code: 'call_not_active', retryable: false }));
  assert.match(await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text(), /<Stream/);
  await f.settle();
  assert.equal(f.transports.length, 1); assert.equal(f.transports[0].terminated, 0);
  f.beginError(); f.transports[0].open();
  const ws = await preconnectMedia(f, call); t.after(() => ws.close());
  await preconnectUntil(() => f.remote()?.state === 'pending'); f.approve();
  await preconnectUntil(() => f.store.get(call.id)!.voiceOpening?.status === 'accepted');
  assert.equal(f.transports.length, 1, 'the socket from the continuation is the one Live starts on');
  assert.equal(f.transports[0].sent[0]?.type, 'session.start');
});

test('an admission refusal ends the account wait and closes the continuation socket unused', async t => {
  const f = await fixture(t, { eligible: true, preconnect: true });
  f.beginError(new PrimaryApiError('http_error', 403, { code: 'inbound_carrier_attestation_required', retryable: false }));
  await (await f.webhook('/broker/incoming-call')).text(); await f.waitForAdmission();
  assert.equal(f.store.list()[0].inbound!.admissionRefusal?.code, 'inbound_carrier_attestation_required');
  assert.equal(f.transports.length, 1); assert.equal(f.transports[0].terminated, 1); assert.deepEqual(f.transports[0].sent, []);
});

test('authority revoked just after a fresh binding response: Live opens but no opening, audio or caller input passes', async t => {
  const f = await fixture(t, { eligible: true, preconnect: true });
  const call = await preconnectApproved(f);
  await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text();
  const transport = f.transports[0]; transport.open();
  f.refuseContext();
  const ws = await preconnectMedia(f, call); t.after(() => ws.close());
  await preconnectUntil(() => ws.readyState === WebSocket.CLOSED);
  assert.equal(f.seen.contextAssert, 1, 'the post-open assertion refused');
  assert.deepEqual(transport.sent.filter(event => event.type !== 'session.close').map(event => event.type), ['session.start'],
    'no opening, instruction or caller audio reached the session');
  assert.equal(f.seen.opening, 0); assert.equal(f.store.get(call.id)!.voiceOpening, undefined);
  assert.equal(f.transports.length, 1, 'no automatic replacement');
  assert.ok(transport.closed + transport.terminated >= 1);
});

test('inbound preconnect adoption preserves a managed generation replacement after context changes', async t => {
  const f = await fixture(t, { eligible: true, preconnect: true, replaceFirstGeneration: true });
  const call = await preconnectApproved(f);
  await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text(); f.transports[0].open();
  const ws = await preconnectMedia(f, call); t.after(() => ws.close());
  await preconnectUntil(() => f.store.get(call.id)!.voiceOpening?.status === 'accepted');
  assert.equal(f.transports.length, 2); assert.equal(f.seen.voice, 2);
  assert.equal(f.seen.consume, 1); assert.equal(f.seen.startupFetch, 1);
  assert.ok(f.transports.every(transport => transport.sent.filter(event => event.type === 'session.start').length === 1));
  assert.ok(f.transports[0].closed >= 1);
  const generations = f.store.get(call.id)!.liveGenerations!;
  assert.equal(generations.segments.length, 2); assert.equal(generations.segments[1].state, 'active');
});

for (const cancellation of ['revoked', 'hangup', 'shutdown'] as const)
  test(`inbound preconnect cleans ${cancellation} while account consumption is in flight`, async t => {
    const f = await fixture(t, { eligible: true, preconnect: true });
    const call = await preconnectApproved(f), release = f.gateConsume();
    const continuation = f.webhook(`/broker/inbound/${call.id}/continue`);
    await preconnectUntil(() => f.seen.consume === 1);
    const transport = f.transports[0]; transport.open();
    let stopping: Promise<unknown> | undefined;
    if (cancellation === 'revoked') f.refuseContext();
    if (cancellation === 'hangup') {
      stopping = f.webhook('/broker/status', { CallStatus: 'completed' }).then(response => response.text());
      await preconnectUntil(() => !!f.store.get(call.id)!.endedAt);
    }
    if (cancellation === 'shutdown') stopping = f.close();
    release(); await (await continuation).text(); await stopping;
    if (cancellation === 'revoked') {
      // The failed context read is a step the next poll retries: the socket waits, unused, until the wait ends.
      await f.settle();
      assert.equal(transport.terminated, 0); assert.deepEqual(transport.sent, []);
      await (await f.webhook('/broker/status', { CallStatus: 'completed' })).text();
    }
    assert.equal(transport.terminated, 1); assert.equal(f.transports.length, 1);
    assert.deepEqual(transport.sent, []); assert.equal(f.seen.voice, 0);
  });


test('duplicate first webhooks contact nothing; only the continuation releases admission, context and transport', async t => {
  const f = await fixture(t, { eligible: true, preconnect: true });
  const first = await f.webhook('/broker/incoming-call'); const redirect = await first.text();
  assert.match(redirect, /<Response><Redirect method="POST">/);
  const call = f.store.list()[0]; assert.equal(f.seen.begin, 0);
  const duplicate = await f.webhook('/broker/incoming-call');
  assert.equal(await duplicate.text(), redirect);
  assert.equal(f.seen.begin, 0); assert.equal(f.seen.context, 0); assert.equal(f.seen.consume, 0); assert.equal(f.transports.length, 0);
  assert.equal((await f.webhook(`/broker/inbound/${call.id}/continue`, {}, false)).status, 403);
  const release = f.gateBegin(); t.after(release);
  const continued = await f.webhook(`/broker/inbound/${call.id}/continue`);
  assert.match(await continued.text(), /<Stream/);
  await preconnectUntil(() => f.seen.begin === 1);
  // The continuation opens only the Live socket: no session, history or audio while admission runs.
  assert.equal(f.seen.consume, 0); assert.equal(f.transports.length, 1); assert.deepEqual(f.transports[0].sent, []);
  release(); await f.settle();
  assert.equal(f.store.get(call.id)!.inbound!.remote?.state, 'pending');
  f.approve(); await f.broker.maintain();
  assert.equal(f.seen.begin, 1); assert.equal(f.seen.consume, 1);
  assert.equal(f.transports.length, 1, 'approval keeps the socket the continuation opened');
  assert.deepEqual(f.transports[0].sent, []); assert.equal(f.transports[0].terminated, 0);
});

test('shutdown during the first admission contact settles its same attachment without starting voice', async t => {
  const f = await fixture(t, { eligible: true, preconnect: true }), release = f.gateBegin();
  t.after(release);
  await (await f.webhook('/broker/incoming-call')).text();
  const call = f.store.list()[0];
  assert.match(await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text(), /<Stream/);
  await preconnectUntil(() => f.seen.begin === 1);
  const closing = f.close();
  await preconnectUntil(() => Boolean(f.store.get(call.id)!.inbound!.localStop));
  release(); await closing;
  assert.equal(f.seen.begin, 1); assert.equal(f.seen.cancel, 1);
  assert.equal(f.store.get(call.id)!.inbound!.remote?.state, 'cancelled');
  assert.equal(f.seen.voice, 0); assert.equal(f.seen.context, 0);
  assert.equal(f.transports.length, 1, 'only the continuation socket');
  assert.deepEqual(f.transports[0].sent, []); assert.equal(f.transports[0].terminated, 1);
});

async function greetingMedia(f: Awaited<ReturnType<typeof fixture>>, call: BrokerCall) {
  const route = `/broker/media/${call.id}`;
  const ws = new WebSocket(f.base.replace(/^http/, 'ws') + route, { headers: {
    'x-twilio-signature': Twilio.getExpectedTwilioSignature(f.config.twilioAuthToken, f.config.publicOrigin + route, {}),
  } });
  const frames: Array<{ event: string; payload?: string; name?: string }> = [];
  ws.on('message', raw => { const frame = JSON.parse(String(raw));
    frames.push({ event: frame.event, ...(frame.media ? { payload: frame.media.payload } : {}), ...(frame.mark ? { name: frame.mark.name } : {}) }); });
  await new Promise<void>((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
  const streamSid = 'MZ' + '4'.repeat(32);
  ws.send(JSON.stringify({ event: 'start', start: { accountSid: f.config.twilioAccountSid, callSid: f.params.CallSid, streamSid,
    customParameters: { token: streamToken(f.config.controlToken, call.id, f.params.CallSid) },
    mediaFormat: { encoding: 'audio/x-mulaw', sampleRate: 8000, channels: 1 } } }));
  const caller = (bytes: Buffer) => ws.send(JSON.stringify({ event: 'media', streamSid, media: { track: 'inbound', payload: bytes.toString('base64') } }));
  const ackClip = () => { for (const frame of frames.filter(value => value.event === 'mark')) ws.send(JSON.stringify({ event: 'mark', streamSid, mark: { name: frame.name } })); };
  const clipFrames = () => frames.filter(frame => frame.event === 'media').length;
  return { ws, frames, caller, ackClip, clipFrames };
}

test('the greeting plays at stream start while admission waits; words over it are dropped and later words reach Live first, in order', async t => {
  const media: Array<Record<string, unknown>> = [];
  t.mock.method(console, 'info', (value: string) => { const event = JSON.parse(value); if (event.event === 'broker_account_media') media.push(event); });
  const f = await fixture(t, { greetingClip: greetingFixtureClip });
  await (await f.webhook('/broker/incoming-call')).text();
  const call = f.store.list()[0]; await f.waitForAdmission();
  assert.equal(f.remote()?.state, 'pending', 'the owner has not approved yet');
  const g = await greetingMedia(f, call); t.after(() => g.ws.close());
  for (let i = 0; i < 200 && g.clipFrames() < 15; i++) await new Promise(resolve => setTimeout(resolve, 5));
  assert.equal(g.clipFrames(), 15, 'the whole clip is sent before admission binds');
  let saved = f.store.get(call.id)!;
  assert.ok(saved.journey!.streamStartedAt && saved.journey!.greetingClipStartedAt);
  assert.equal(saved.mediaStartedAt, undefined); assert.equal(saved.primary, undefined); assert.equal(f.seen.voice, 0);
  g.caller(Buffer.alloc(800, 0x10)); // "Hello?" over the greeting
  await new Promise(resolve => setTimeout(resolve, 20));
  g.ackClip();
  for (let i = 0; i < 100 && !f.store.get(call.id)!.journey?.greetingClipPlayedAt; i++) await new Promise(resolve => setTimeout(resolve, 5));
  const first = Buffer.alloc(800, 0x21), second = Buffer.alloc(800, 0x22);
  g.caller(first); g.caller(second);
  await new Promise(resolve => setTimeout(resolve, 30));
  assert.equal(f.seen.voice, 0); assert.equal(f.seen.consume, 0);
  f.approve();
  for (let i = 0; i < 200 && f.appended.length < 2; i++) await new Promise(resolve => setTimeout(resolve, 5));
  assert.deepEqual(f.appended, [first.toString('base64'), second.toString('base64')]);
  const third = Buffer.alloc(800, 0x23); g.caller(third);
  for (let i = 0; i < 100 && f.appended.length < 3; i++) await new Promise(resolve => setTimeout(resolve, 5));
  assert.equal(f.appended[2], third.toString('base64'));
  for (let i = 0; i < 100 && f.seen.opening === 0; i++) await new Promise(resolve => setTimeout(resolve, 5));
  assert.deepEqual(f.openings, [brokerOpening('inbound', false, false, false, true)]);
  saved = f.store.get(call.id)!;
  assert.equal(saved.inbound!.remote?.state, 'bound');
  assert.ok(saved.mediaStartedAt! >= saved.journey!.greetingClipPlayedAt!);
  assert.ok(Date.parse(saved.mediaStartedAt!) >= Date.parse((saved.inbound!.remote as { consumedAt: string }).consumedAt));
  assert.deepEqual(media.map(({ heldInputMs, droppedInputMs, greetingInputMs }) => ({ heldInputMs, droppedInputMs, greetingInputMs })),
    [{ heldInputMs: 200, droppedInputMs: 0, greetingInputMs: 100 }]);
});

test('in the greeting-stream order Live opens on the binding response with only its post-open assertion; the wait after the caller\'s words is journaled', async t => {
  const f = await fixture(t, { eligible: true, greetingClip: greetingFixtureClip });
  await (await f.webhook('/broker/incoming-call')).text();
  const call = f.store.list()[0]; await f.waitForAdmission();
  const g = await greetingMedia(f, call); t.after(() => g.ws.close());
  for (let i = 0; i < 200 && g.clipFrames() < 15; i++) await new Promise(resolve => setTimeout(resolve, 5));
  g.ackClip();
  for (let i = 0; i < 100 && !f.store.get(call.id)!.journey?.greetingClipPlayedAt; i++) await new Promise(resolve => setTimeout(resolve, 5));
  g.caller(Buffer.alloc(800, 0x21)); g.caller(Buffer.alloc(800, 0xff)); // A question, then silence, while admission runs.
  await new Promise(resolve => setTimeout(resolve, 20));
  assert.equal(f.seen.voice, 0);
  f.approve();
  for (let i = 0; i < 200 && f.seen.opening === 0; i++) await new Promise(resolve => setTimeout(resolve, 5));
  assert.equal(f.seen.opening, 1);
  assert.equal(f.seen.contextAssert, 1, 'no second eligibility read between the binding response and Live');
  assert.equal(f.seen.startupFetch, 0); assert.equal(f.seen.context, 1);
  assert.deepEqual(f.startupPages, [f.eligiblePage]);
  f.emitAudio(Buffer.alloc(800, 0x20)); // Dexter's first words.
  for (let i = 0; i < 100 && !f.store.get(call.id)!.journey?.firstSpeechAudioSentAt; i++) await new Promise(resolve => setTimeout(resolve, 5));
  const journey = f.store.get(call.id)!.journey!;
  assert.ok(journey.callerSpeechEndedAt && journey.firstSpeechAudioSentAt);
  assert.ok(journey.greetingClipPlayedAt! <= journey.callerSpeechEndedAt && journey.callerSpeechEndedAt <= journey.firstSpeechAudioSentAt);
});

test('held caller audio keeps the newest 15 s while admission waits', async t => {
  const f = await fixture(t);
  await (await f.webhook('/broker/incoming-call')).text();
  const call = f.store.list()[0]; await f.waitForAdmission();
  const g = await greetingMedia(f, call); t.after(() => g.ws.close());
  for (let i = 0; i < 100 && !f.store.get(call.id)!.streamSid; i++) await new Promise(resolve => setTimeout(resolve, 5));
  const frames = Array.from({ length: 160 }, (_value, index) => Buffer.alloc(800, index)); // 16 s
  for (const frame of frames) g.caller(frame);
  await new Promise(resolve => setTimeout(resolve, 50));
  f.approve();
  for (let i = 0; i < 400 && f.appended.length < 150; i++) await new Promise(resolve => setTimeout(resolve, 5));
  assert.equal(f.appended.length, 150);
  assert.deepEqual(f.appended, frames.slice(10).map(frame => frame.toString('base64')));
});

test('a refusal while the greeting plays hands the leg back after the greeting; the continuation then hangs up', async t => {
  const f = await fixture(t, { greetingClip: greetingFixtureClip }), release = f.gateBegin(); t.after(release);
  await (await f.webhook('/broker/incoming-call')).text();
  const call = f.store.list()[0];
  const answer = await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text();
  assert.match(answer, /<\/Connect><Redirect method="POST">https:\/\/phone\.example\.test\/broker\/inbound\/[0-9a-f-]{36}\/continue<\/Redirect>/);
  const g = await greetingMedia(f, call);
  for (let i = 0; i < 200 && g.clipFrames() < 15; i++) await new Promise(resolve => setTimeout(resolve, 5));
  f.beginError(new PrimaryApiError('http_error', 403, { code: 'inbound_carrier_attestation_required', retryable: false }));
  release(); await f.settle();
  assert.equal(f.store.get(call.id)!.inbound!.admissionRefusal?.code, 'inbound_carrier_attestation_required');
  await new Promise(resolve => setTimeout(resolve, 30));
  assert.equal(g.ws.readyState, WebSocket.OPEN, 'the caller hears the whole greeting first');
  const closed = new Promise<void>(resolve => g.ws.once('close', () => resolve()));
  g.ackClip(); await closed;
  const released = f.store.get(call.id)!;
  assert.equal(released.endedAt, undefined, 'releasing the stream does not end the call');
  assert.equal(f.seen.end, 0); assert.equal(f.seen.voice, 0); assert.equal(f.seen.consume, 0);
  const next = await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text();
  assert.match(next, /<Hangup/); assert.doesNotMatch(next, /<Stream/);
});


test('a stream that starts after a refusal plays no greeting and hands the leg straight back', async t => {
  const f = await fixture(t, { greetingClip: greetingFixtureClip });
  f.beginError(new PrimaryApiError('http_error', 403, { code: 'inbound_enrollment_required', retryable: false }));
  await (await f.webhook('/broker/incoming-call')).text();
  const call = f.store.list()[0]; await f.waitForAdmission();
  assert.equal(f.store.get(call.id)!.inbound!.admissionRefusal?.code, 'inbound_enrollment_required');
  const g = await greetingMedia(f, call);
  await new Promise<void>(resolve => { if (g.ws.readyState === WebSocket.CLOSED) resolve(); else g.ws.once('close', () => resolve()); });
  assert.equal(g.clipFrames(), 0);
  assert.equal(f.store.get(call.id)!.endedAt, undefined); assert.equal(f.seen.end, 0); assert.equal(f.seen.voice, 0);
  assert.equal(f.store.get(call.id)!.failure, undefined);
  const next = await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text();
  assert.match(next, /<Hangup/);
});

test('a begin that races the pickup is retried after 100 ms, then 200 and 400 ms, then every second', async t => {
  assert.deepEqual([0, 1, 2, 3, 4, 9].map(inboundAdmissionPollDelay), [100, 200, 400, 1_000, 1_000, 1_000]);
  const f = await fixture(t, { greetingClip: greetingFixtureClip, defaultAdmissionPoll: true });
  await (await f.webhook('/broker/incoming-call')).text();
  const call = f.store.list()[0];
  // The API's begin refuses a leg Twilio has not picked up yet (call 28a563f6's continuation arrived while it rang).
  f.beginError(new PrimaryApiError('http_error', 403, { code: 'call_not_active', retryable: false }));
  assert.match(await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text(), /<Connect><Stream/);
  const g = await greetingMedia(f, call); t.after(() => g.ws.close());
  for (let i = 0; i < 800 && f.beginTimes.length < 5; i++) await new Promise(resolve => setTimeout(resolve, 5));
  f.beginError();
  assert.equal(f.beginTimes.length, 5, 'one refused begin from the continuation, then one per poll');
  const gaps = f.beginTimes.slice(1).map((at, index) => at - f.beginTimes[index]);
  // Each gap is the poll wait (plus, for the first, the media start that begins the loop).
  // Upper bounds leave room for a loaded box; a fixed 1 s poll (the old pace) could never meet the first two.
  for (const [index, [low, high]] of [[95, 900], [195, 950], [395, 1_300], [995, 2_000]].entries())
    assert.ok(gaps[index] >= low && gaps[index] <= high, `gap ${index + 1} was ${gaps[index]} ms`);
  for (let i = 0; i < 400 && f.remote()?.state !== 'pending'; i++) await new Promise(resolve => setTimeout(resolve, 5));
  assert.equal(f.remote()?.state, 'pending', 'the next begin admits the answered leg');
  assert.equal(f.store.get(call.id)!.inbound!.localStop, undefined);
});
