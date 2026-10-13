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
import { createBrokerServer } from '../src/broker/server.js';
import { inboundBeginRef, type InboundBeginIdentity, type InboundOutcome } from '../src/broker/inboundContract.js';
import { BrokerStore, type BrokerCall } from '../src/broker/store.js';
import type { BrokerConfig } from '../src/broker/config.js';
import { PrimaryApiError, type PrimaryApiClient } from '../src/broker/primaryApi.js';
import { createInboundHoldAdapter } from '../src/broker/inboundHold.js';
import { streamToken } from '../src/broker/phone.js';
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

async function connectIncomingMedia(f: Awaited<ReturnType<typeof fixture>>) {
  await (await f.webhook('/broker/incoming-call')).text();
  const call = f.store.list()[0]; await f.waitForAdmission(); f.approve();
  assert.match(await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text(), /<Stream/);
  const route = `/broker/media/${call.id}`;
  const ws = new WebSocket(f.base.replace(/^http/, 'ws') + route, { headers: {
    'x-twilio-signature': Twilio.getExpectedTwilioSignature(f.config.twilioAuthToken, f.config.publicOrigin + route, {}),
  } });
  await new Promise<void>((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
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

test('audible timing excludes silence, empty audio, cleared and duplicate marks without changing the journal', async t => {
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
  assert.deepEqual(Object.keys(f.store.get(call.id)!.journey!).sort(), ['firstAudioSentAt', 'firstPlaybackConfirmedAt', 'liveReadyAt']);
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

async function fixture(t: test.TestContext, options: { selected?: boolean; retained?: BrokerCall; initialRemote?: InboundOutcome; eligible?: boolean; billing?: boolean; billingUrl?: string; duration?: string; preconnect?: boolean; replaceFirstGeneration?: boolean } = {}) {
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
  let loseConsume = false, lostBegin = false, authority = true, cancellationUnavailable = false;
  const lifecycle: string[] = [];
  let providerGate: Promise<void> | undefined; let releaseProvider: (() => void) | undefined;
  let providerEntered: (() => void) | undefined; let providerState = 'in-progress';
  let providerDuration = options.duration; let providerWrongIdentity = false;
  let beginError: Error | undefined, pendingBeginError: Error | undefined;
  let beginGate: Promise<void> | undefined, releaseBegin: (() => void) | undefined;
  let statusGate: Promise<void> | undefined; let releaseStatus: (() => void) | undefined;
  const seen = { dial: 0, end: 0, voice: 0, begin: 0, consume: 0, context: 0, cancel: 0, close: 0, legacyClose: 0,
    startupFetch: 0, contextAssert: 0, opening: 0 };
  const context = () => {
    seen.context++;
    if (!authority) throw new Error('authority_unavailable');
    if (options.eligible) return structuredClone(eligiblePage);
    return { billing, bindingId, callSid: params.CallSid, state: 'open' as const,
      context: { primaryDexterId, threadId, displayName: 'Avery Example', messages: [] } };
  };
  const primary = {
    async beginInbound(begin: InboundBeginIdentity) {
      seen.begin++;
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
    async fetch() { if (providerGate) { providerEntered?.(); await providerGate; } return { sid, accountSid: config.twilioAccountSid, direction: 'inbound', from: params.From, to: params.To, status: providerState, ...(providerWrongIdentity ? { to: '+12025550999' } : {}),
      ...(providerDuration === undefined ? {} : { duration: providerDuration, startTime: new Date('2020-01-01T10:00:00Z'), endTime: new Date(1577872800000 + Number(providerDuration) * 1000) }) }; },
    async update() { seen.end++; return { sid, status: 'completed' }; },
  }), { async create() { seen.dial++; throw new Error('inbound_must_not_dial'); } });
  const broker = createBrokerServer({ config, store, primary, provider: { calls: provider } as unknown as ReturnType<typeof Twilio>,
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
        if (options.replaceFirstGeneration && seen.voice === 1 && seen.contextAssert === 2) {
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
      return { appendAudio() { return true; }, async appendContext() { return { eventId: 'c', status: 'accepted' }; },
        async appendInstructions() { seen.opening++; return { eventId: 'i', status: 'accepted' }; }, async interruptPlayback() { return { eventId: 'p', status: 'accepted' }; },
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
  t.after(async () => { releaseBegin?.(); await close(); fs.rmSync(dir, { recursive: true, force: true }); });
  return { broker, store, base, config, params, seen, lifecycle, webhook, close, provider, billing, transports,
    gateBegin() { beginGate = new Promise<void>(resolve => { releaseBegin = resolve; }); return () => releaseBegin!(); },
    gateConsume() { consumeGate = new Promise<void>(resolve => { releaseConsume = resolve; }); return () => releaseConsume!(); },
    gateAssert() { assertGate = new Promise<void>(resolve => { releaseAssert = resolve; }); return () => releaseAssert!(); },
    providerEnded(duration = providerDuration) { providerState = 'completed'; providerDuration = duration; },
    wrongProviderIdentity() { providerWrongIdentity = true; },
    emitAudio(bytes = Buffer.alloc(800, 0)) { voiceOptions!.onAudio(bytes.toString('base64')); },
    clearAudio() { voiceOptions!.onPlaybackClear?.(); }, eligiblePage, startupPages, remote: () => remote,
    async waitForAdmission() { const call = store.list()[0]; await (await webhook(`/broker/inbound/${call.id}/continue`)).text(); },
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
  const repeat = await f.webhook(`/broker/inbound/${call.id}/continue`);
  assert.equal(repeat.status, 409); await repeat.text();
  assert.equal(f.seen.voice, 1);
  ws.close(); await new Promise<void>(resolve => ws.once('close', () => resolve()));
});

test('admitted eligible history reaches first voice startup without a duplicate GET and keeps both original assertions', async t => {
  const f = await fixture(t, { eligible: true });
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
  assert.equal(f.seen.opening, 1); assert.equal(f.seen.contextAssert, 2, 'pre-load and post-open checks also qualify the initial greeting');
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
  const call = f.store.list()[0]; assert.equal(f.seen.begin, 1);
  assert.equal(f.store.get(call.id)!.inbound!.remote, undefined); await f.waitForAdmission();
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
  const call = f.store.list()[0]; await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text();
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

test('actual HTTP hold overlaps one admission; restart and maintenance cannot repeat it before continuation', async t => {
  const f = await fixture(t);
  const first = await f.webhook('/broker/incoming-call'); const xml = await first.text();
  assert.match(xml, /<Say>One moment\.<\/Say><Redirect method="POST">/);
  assert.doesNotMatch(xml, /<Pause/);
  assert.equal(f.seen.begin, 1); assert.equal(f.seen.context, 0);
  const saved = f.store.list()[0]; const waitUntil = saved.inbound!.waitUntil;
  assert.ok(saved.inbound!.earlyAdmission);
  await f.broker.maintain(); assert.equal(f.seen.begin, 1);
  const restored = await fixture(t, { retained: saved, initialRemote: f.remote() });
  await restored.broker.maintain(); assert.equal(restored.seen.begin, 0);
  const bad = await restored.webhook(`/broker/inbound/${saved.id}/continue`, {}, false);
  assert.equal(bad.status, 403); await bad.text();
  assert.equal(restored.store.get(saved.id)!.inbound!.continuation, undefined);
  const continued = await restored.webhook(`/broker/inbound/${saved.id}/continue`, { CallStatus: 'in-progress' });
  const silent = await continued.text();
  assert.match(silent, /<Pause length="1"\/><Redirect method="POST">/);
  assert.doesNotMatch(silent, /<Say|<Stream|<Gather/);
  assert.equal(restored.seen.begin, 1); assert.equal(restored.seen.context, 0);
  assert.equal(restored.store.get(saved.id)!.inbound!.waitUntil, waitUntil);
  assert.deepEqual(restored.store.get(saved.id)!.inbound!.begin.providerProof, saved.inbound!.begin.providerProof);
  assert.equal(restored.seen.dial, 0);
});

for (const code of ['inbound_carrier_attestation_required', 'inbound_enrollment_required'] as const) {
  test(`actual HTTP ${code} hangs up while unavailable cancellation remains pending`, async t => {
    const f = await fixture(t);
    f.beginError(new PrimaryApiError('http_error', 403, { code, retryable: false }));
    f.refuseCancellation();
    await (await f.webhook('/broker/incoming-call')).text();
    const call = f.store.list()[0];
    const response = await f.webhook(`/broker/inbound/${call.id}/continue`); const xml = await response.text();
    assert.match(xml, /<Hangup/); assert.doesNotMatch(xml, /<Stream|<Gather|enrollment|attestation/);
    const saved = f.store.get(call.id)!;
    assert.equal(saved.inbound!.admissionRefusal?.code, code);
    assert.equal(saved.inbound!.localStop?.reason, code);
    assert.ok(saved.inbound!.cancelRequestedAt); assert.equal(saved.inbound!.remote, undefined);
    assert.equal(saved.inbound!.closure, undefined); assert.equal(f.seen.context, 0); assert.equal(f.seen.consume, 0);
    f.beginError(); await f.broker.maintain();
    assert.equal(f.seen.begin, 1); assert.equal(f.seen.end, 1);
    assert.equal(f.store.get(call.id)!.inbound!.remote, undefined);
  });
}

test('actual HTTP retryable carrier503 continues silently and preserves one original begin', async t => {
  const f = await fixture(t); await (await f.webhook('/broker/incoming-call')).text();
  const call = f.store.list()[0];
  f.beginError(new PrimaryApiError('http_error', 503, { code: 'inbound_carrier_admission_unavailable', retryable: true }));
  const wait = await f.webhook(`/broker/inbound/${call.id}/continue`);
  assert.match(await wait.text(), /<Redirect/);
  const attempted = f.store.get(call.id)!;
  assert.ok(attempted.failure, 'ordinary continuation failure remains reported');
  assert.equal(attempted.inbound!.localStop, undefined); assert.equal(attempted.inbound!.admissionRefusal, undefined);
  f.beginError(); await f.broker.maintain();
  assert.equal(f.seen.begin, 3); assert.equal(f.store.list().length, 1);
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

test('approved inbound transport overlaps context I/O and stays command-free through media and pre-start assertion', async t => {
  const f = await fixture(t, { eligible: true, preconnect: true });
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
  assert.deepEqual(transport.sent, []); assert.equal(f.seen.voice, 0);
  releaseAssert();
  await preconnectUntil(() => f.store.get(call.id)!.voiceOpening?.status === 'accepted');
  assert.equal(f.transports.length, 1); assert.equal(f.seen.contextAssert, 2, 'opening shares the validated activation');
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

for (const refusal of ['unapproved', 'wrong-provider', 'provider-ended'] as const)
  test(`inbound preconnect refuses ${refusal} before opening any socket`, async t => {
    const f = await fixture(t, { eligible: true, preconnect: true });
    await (await f.webhook('/broker/incoming-call')).text(); await f.waitForAdmission();
    if (refusal !== 'unapproved') f.approve();
    if (refusal === 'wrong-provider') f.wrongProviderIdentity();
    if (refusal === 'provider-ended') f.providerEnded();
    const call = f.store.list()[0]; await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text();
    assert.equal(f.transports.length, 0); assert.equal(f.seen.consume, 0);
  });

for (const failure of ['consume', 'early-error', 'early-close', 'media-token', 'revoked', 'hangup', 'shutdown'] as const)
  test(`inbound preconnect cleans ${failure} without private commands or automatic replacement`, async t => {
    const f = await fixture(t, { eligible: true, preconnect: true });
    const call = await preconnectApproved(f);
    if (failure === 'consume') f.loseConsume();
    await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text();
    assert.equal(f.transports.length, 1); const transport = f.transports[0]; transport.open();
    if (failure === 'early-error') transport.fail();
    if (failure === 'early-close') transport.end();
    if (failure === 'revoked') f.refuseContext();
    if (failure === 'hangup') await (await f.webhook('/broker/status', { CallStatus: 'completed' })).text();
    else if (failure === 'shutdown') await f.close();
    else {
      if (failure === 'consume') await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text();
      const ws = await preconnectMedia(f, call, failure === 'media-token' ? 'wrong' : undefined);
      t.after(() => ws.close());
      await preconnectUntil(() => ws.readyState === WebSocket.CLOSED);
    }
    assert.equal(transport.terminated, 1); assert.deepEqual(transport.sent, []);
    assert.equal(f.transports.length, 1);
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
    assert.equal(transport.terminated, 1); assert.equal(f.transports.length, 1);
    assert.deepEqual(transport.sent, []); assert.equal(f.seen.voice, 0);
  });


test('HTTP hold and duplicate return while one early admission is delayed; continuation alone releases context and transport', async t => {
  const f = await fixture(t, { eligible: true, preconnect: true }), release = f.gateBegin();
  t.after(release);
  const first = await f.webhook('/broker/incoming-call');
  assert.match(await first.text(), /<Say>One moment\.<\/Say><Redirect method="POST">/);
  const call = f.store.list()[0]; assert.equal(f.seen.begin, 1);
  assert.equal(f.seen.context, 0); assert.equal(f.seen.consume, 0); assert.equal(f.transports.length, 0);
  const duplicate = await f.webhook('/broker/incoming-call');
  assert.match(await duplicate.text(), /<Say>One moment\.<\/Say><Redirect method="POST">/);
  assert.equal(f.seen.begin, 1);
  release(); await preconnectUntil(() => f.store.get(call.id)!.inbound!.remote?.state === 'pending');
  f.approve(); await f.broker.maintain();
  assert.equal(f.seen.context, 0); assert.equal(f.seen.consume, 0); assert.equal(f.transports.length, 0);
  assert.equal((await f.webhook(`/broker/inbound/${call.id}/continue`, {}, false)).status, 403);
  assert.equal(f.seen.consume, 0);
  const continued = await f.webhook(`/broker/inbound/${call.id}/continue`);
  assert.match(await continued.text(), /<Stream/);
  assert.equal(f.seen.begin, 1); assert.equal(f.seen.consume, 1); assert.equal(f.transports.length, 1);
  assert.deepEqual(f.transports[0].sent, []);
});

test('shutdown during the early contact settles its same attachment without starting voice', async t => {
  const f = await fixture(t, { eligible: true, preconnect: true }), release = f.gateBegin();
  t.after(release);
  await (await f.webhook('/broker/incoming-call')).text();
  const call = f.store.list()[0]; assert.equal(f.seen.begin, 1);
  const closing = f.close();
  await preconnectUntil(() => Boolean(f.store.get(call.id)!.inbound!.localStop));
  release(); await closing;
  assert.equal(f.seen.begin, 1); assert.equal(f.seen.cancel, 1);
  assert.equal(f.store.get(call.id)!.inbound!.remote?.state, 'cancelled');
  assert.equal(f.seen.voice, 0); assert.equal(f.seen.context, 0); assert.equal(f.transports.length, 0);
});


for (const failure of ['ringing', 'lost-reply'] as const) test(`early ${failure} then successful continuation has no persisted failed-call diagnostic`, async t => {
  const f = await fixture(t);
  if (failure === 'ringing') f.pendingBeginError(new PrimaryApiError('http_error', 403, { code: 'call_not_active', retryable: false }));
  else f.loseBegin();
  const first = await f.webhook('/broker/incoming-call');
  assert.match(await first.text(), /<Say>One moment\.<\/Say><Redirect/);
  const call = f.store.list()[0]; assert.equal(f.seen.begin, 1);
  assert.ok(call.inbound!.earlyAdmission); assert.equal(call.inbound!.remote, undefined);
  assert.equal(f.remote()?.state, 'pending'); assert.equal(call.failure, undefined);
  f.pendingBeginError(); f.approve();
  assert.match(await (await f.webhook(`/broker/inbound/${call.id}/continue`)).text(), /<Stream/);
  assert.equal(f.seen.begin, 1); assert.equal(f.seen.consume, 1);
  const saved = f.store.get(call.id)!;
  assert.equal(saved.inbound!.remote?.state, 'bound'); assert.equal(saved.failure, undefined);
  const response = await fetch(f.base + '/internal/broker/calls', { headers: { authorization: `Bearer ${f.config.controlToken}` } });
  assert.equal(response.status, 200);
  const result = await response.json() as { calls: Array<{ id: string; failure?: unknown }> };
  assert.equal(result.calls.find(value => value.id === call.id)?.failure, undefined);
  assert.ok(result.calls.some(value => value.id === call.id));
});
