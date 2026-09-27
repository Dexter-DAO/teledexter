import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Twilio from 'twilio';
import { WebSocket } from 'ws';
import { createBrokerServer } from '../../src/broker/server.js';
import { BrokerStore, type BrokerCall } from '../../src/broker/store.js';
import type { BrokerConfig } from '../../src/broker/config.js';
import { createInboundHoldAdapter } from '../../src/broker/inboundHold.js';
import { streamToken } from '../../src/broker/phone.js';
import { deliverHarkBilling } from '../../src/broker/harkBillingDelivery.js';
import { createInboundBillingLifecycle } from '../../src/broker/inboundBillingLifecycle.js';

/** Exercises actual Broker HTTP, Primary client, media, provider recovery and signed billing transport.
 * API and Postgres are supplied by the caller. Twilio and Live are local test doubles.
 */
export async function runIncomingBillingJourney(options: {
  apiBaseUrl: string; phoneServiceSecret: string; callbackSigningSecret: string;
  accountSid: string; authToken: string; caller: string; phoneNumber: string; callSid: string;
  durationSeconds: number | null;
  terminalStatus?: 'completed' | 'failed' | 'busy' | 'no-answer' | 'canceled';
  /** Called after the actual attachment-begin POST has returned. Approve its saved attachment. */
  afterWaiting(call: BrokerCall): Promise<void>;
  publicOrigin?: string;
  loseFirstFinancialReply?: boolean;
}) {
  const base = new URL(options.apiBaseUrl);
  assert.equal(base.protocol, 'http:'); assert.ok(['127.0.0.1', 'localhost', '[::1]'].includes(base.hostname));
  assert.ok(options.durationSeconds === null || (Number.isSafeInteger(options.durationSeconds) && options.durationSeconds >= 0));
  const terminalStatus = options.terminalStatus ?? 'completed';
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-real-api-billing-'));
  const store = new BrokerStore(directory);
  const config: BrokerConfig = { host: '127.0.0.1', port: 3213, publicOrigin: options.publicOrigin ?? 'https://broker.fixture.test',
    stateDir: directory, controlToken: 'test-control-secret-'.repeat(3), apiKey: 'local-fixture', model: 'gpt-live-1', voice: 'marin',
    twilioAccountSid: options.accountSid, twilioAuthToken: options.authToken, from: options.phoneNumber, allowedTo: [options.caller],
    apiBaseUrl: options.apiBaseUrl, phoneServiceSecret: options.phoneServiceSecret, maxCallSeconds: 60,
    harkApiBaseUrl: options.apiBaseUrl, harkCallbackSigningSecret: options.callbackSigningSecret };
  let completed = false, voiceStarted = false, dialCount = 0, lostReply = false;
  let endedAt: Date | undefined, startedAt: Date | undefined;
  const complete = () => {
    if (completed) return;
    completed = true;
    if (options.durationSeconds !== null) { endedAt = new Date(); startedAt = new Date(endedAt.getTime() - options.durationSeconds * 1000); }
  };
  const provider = Object.assign((sid: string) => ({
    async fetch() { return { sid, accountSid: options.accountSid, from: options.caller, to: options.phoneNumber,
      direction: 'inbound', status: completed ? terminalStatus : 'in-progress',
      ...(completed ? { startTime: startedAt ?? null, endTime: endedAt ?? null,
        duration: options.durationSeconds === null ? null : String(options.durationSeconds) } : {}) }; },
    async update() { complete(); return { sid, status: 'completed' }; },
  }), { async create() { dialCount++; throw new Error('Incoming fixture must not dial'); } });
  const broker = createBrokerServer({ config, store, provider: { calls: provider } as unknown as ReturnType<typeof Twilio>,
    inboundAttachment: createInboundHoldAdapter({ publicOrigin: config.publicOrigin, store }),
    inboundBillingSender: input => deliverHarkBilling({ ...input, fetchImpl: async (url, init) => {
      const response = await fetch(url, init);
      if (options.loseFirstFinancialReply && !lostReply && response.ok) {
        lostReply = true; await response.text(); throw new Error('Simulated accepted receipt lost in transit');
      }
      return response;
    } }),
    voiceFactory: settings => {
      voiceStarted = true;
      queueMicrotask(() => settings.onReady({ callId: settings.callId, sessionId: 'incoming-integration' }));
      return { appendAudio() { return true; }, async appendContext() { return { eventId: 'context', status: 'accepted' }; },
        async appendInstructions() { settings.onAudio(Buffer.alloc(800, 0).toString('base64')); return { eventId: 'opening', status: 'accepted' }; },
        async interruptPlayback() { return { eventId: 'interrupt', status: 'accepted' }; }, resumePlayback() {},
        getTranscript() { return { revision: 0, callerRevision: 0, fragments: [] }; },
        async close(reason) { return { callId: settings.callId, sessionId: 'incoming-integration', finalized: true,
          transcriptPersisted: true, outputAudioDiscarded: false, reason: reason ?? 'closed' }; } };
    } });
  let brokerClosed = false, socket: WebSocket | undefined, reopened: BrokerStore | undefined;
  try {
    await new Promise<void>(resolve => broker.server.listen(0, '127.0.0.1', resolve));
    const brokerBase = `http://127.0.0.1:${(broker.server.address() as { port: number }).port}`;
    const params = { AccountSid: options.accountSid, CallSid: options.callSid, From: options.caller, To: options.phoneNumber,
      Direction: 'inbound', CallStatus: 'in-progress', StirVerstat: 'TN-Validation-Passed-A' };
    const webhook = (route: string, patch: Record<string, string> = {}) => {
      const body = { ...params, ...patch };
      return fetch(brokerBase + route, { method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded',
        'x-twilio-signature': Twilio.getExpectedTwilioSignature(options.authToken, config.publicOrigin + route, body) },
      body: new URLSearchParams(body) });
    };
    const incoming = await webhook('/broker/incoming-call'); assert.equal(incoming.status, 200); await incoming.text();
    const id = store.list()[0].id;
    const continuation = `/broker/inbound/${id}/continue`;
    const waiting = await webhook(continuation); assert.equal(waiting.status, 200); await waiting.text();
    await options.afterWaiting(store.get(id)!);
    const admitted = await webhook(continuation); assert.equal(admitted.status, 200); assert.match(await admitted.text(), /<Stream/);
    assert.ok(store.get(id)!.inboundBilling, 'New admission must carry frozen billing reference from actual API');
    const route = `/broker/media/${id}`;
    socket = new WebSocket(brokerBase.replace(/^http/, 'ws') + route, { headers: {
      'x-twilio-signature': Twilio.getExpectedTwilioSignature(options.authToken, config.publicOrigin + route, {}),
    } });
    const mediaSocket = socket;
    mediaSocket.on('message', raw => { const frame = JSON.parse(String(raw)); if (frame.event === 'mark')
      mediaSocket.send(JSON.stringify({ event: 'mark', streamSid: frame.streamSid, mark: frame.mark })); });
    await new Promise<void>((resolve, reject) => { mediaSocket.once('open', resolve); mediaSocket.once('error', reject); });
    mediaSocket.send(JSON.stringify({ event: 'start', start: { accountSid: options.accountSid, callSid: options.callSid,
      streamSid: 'MZ' + '9'.repeat(32), customParameters: { token: streamToken(config.controlToken, id, options.callSid) },
      mediaFormat: { encoding: 'audio/x-mulaw', sampleRate: 8000, channels: 1 } } }));
    for (let i = 0; i < 200 && !store.get(id)!.journey?.firstPlaybackConfirmedAt; i++) await new Promise(resolve => setTimeout(resolve, 5));
    assert.ok(voiceStarted); assert.ok(store.get(id)!.journey?.firstPlaybackConfirmedAt);
    complete();
    const terminal = await webhook('/broker/status', { CallStatus: terminalStatus }); assert.equal(terminal.status, 200); await terminal.text();
    await broker.maintain();
    const firstOutcome = store.get(id)!;
    await broker.close(); brokerClosed = true;
    // Always reopen the durable journal. A lost acknowledgment retries exactly
    // the same body through the real API; a delivered event performs no POST.
    reopened = new BrokerStore(directory);
    const recovery = createInboundBillingLifecycle({ store: reopened, apiBaseUrl: options.apiBaseUrl,
      signingSecret: options.callbackSigningSecret, now: () => Date.now() + 60_000,
      fetchCall: async () => { throw new Error('Saved timing must not be fetched again'); } });
    await recovery.reconcile(id); await recovery.reconcile(id);
    assert.equal(dialCount, 0);
    return { firstOutcome, call: reopened.get(id)!, lostReply, dialCount };
  } finally {
    socket?.close(); if (!brokerClosed) await broker.close(); reopened?.close();
    fs.rmSync(directory, { recursive: true, force: true });
  }
}
