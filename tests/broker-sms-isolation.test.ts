import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { Worker } from 'node:worker_threads';
import { createHmac, randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import Twilio from 'twilio';
import { createBrokerSmsBridge } from '../src/broker/sms.js';
import { createBrokerServer } from '../src/broker/server.js';
import type { BrokerConfig } from '../src/broker/config.js';
import { SmsService, SMS_DISCLOSURE_VERSION } from '../src/sms/SmsService.js';

const PHONE = '+15555550123', SENDER = '+15555550124', BASE = 'https://phone.example.test';
const ACCOUNT = 'AC' + '1'.repeat(32), TOKEN = 'fixture-sms-webhook';
async function fixture(t: test.TestContext, large = false, sendEnabled = false, verifyEnabled = false) {
  const stateHome = path.join(os.homedir(), '.local/state');
  fs.mkdirSync(stateHome, { recursive: true });
  const dir = fs.mkdtempSync(path.join(stateHome, 'broker-sms-isolation-'));
  const statePath = path.join(dir, 'sms.json');
  if (large || sendEnabled) fs.writeFileSync(statePath, JSON.stringify({ version: 1,
    consents: sendEnabled ? [{ phone: PHONE, enrolled: true, carrierOptedOut: false, disclosureVersion: SMS_DISCLOSURE_VERSION,
      updatedAt: new Date().toISOString(), verifiedAt: new Date().toISOString(), withdrawnAt: null }] : [], challenges: [], proofs: [],
    events: [], messages: [], rates: [], webhooks: [],
    inbox: Array.from({ length: large ? 50_000 : 0 }, (_, i) => ({ sid: 'SM' + i.toString(16).padStart(32, '0'),
      phone: PHONE, body: 'Synthetic retained text. '.repeat(20), received: Date.now(), state: 'suppressed' })),
  }), { mode: 0o600 });
  const gate = new Int32Array(new SharedArrayBuffer(4));
  let worker!: Worker;
  const env: NodeJS.ProcessEnv = {
    BROKER_CONTROL_TOKEN: 'c'.repeat(40), DEXTER_SMS_STATE_PATH: statePath, DEXTER_SMS_INTERNAL_SECRET: 'fixture-secret-'.repeat(4),
    DEXTER_SMS_ENROLLMENT_ENABLED: 'true', DEXTER_SMS_SEND_ENABLED: String(sendEnabled),
    DEXTER_SMS_CAMPAIGN_APPROVED: 'true', DEXTER_SMS_ADVANCED_OPT_OUT_CONFIGURED: 'true',
    TWILIO_VERIFY_SID: 'VA' + '3'.repeat(32), TWILIO_ACCOUNT_SID: ACCOUNT, TWILIO_AUTH_TOKEN: TOKEN,
    TWILIO_PHONE_NUMBER: SENDER, TWILIO_MESSAGING_SERVICE_SID: 'MG' + '4'.repeat(32), DEXTER_PHONE_PUBLIC_URL: BASE,
  };
  let sends = 0, starts = 0, checks = 0;
  const clients = { verify: { verify: { v2: { services: () => ({
    verifications: { async create() { starts++; if (!verifyEnabled) throw new Error('unexpected_verify'); } },
    verificationChecks: { async create({ code }: { code: string }) {
      checks++; if (!verifyEnabled) throw new Error('unexpected_verify');
      return { status: code === '123456' ? 'approved' : 'pending' };
    } },
  }) } } }, messaging: { messages: { async create() {
    sends++; if (!sendEnabled) throw new Error('unexpected_send');
    return { sid: 'SM'+'d'.repeat(32), status: 'queued' };
  } } } } as any;
  const sms = await createBrokerSmsBridge(env, path.join(dir, 'release'), clients, undefined, (url, options) => {
    worker = new Worker(`
      const { workerData } = require('node:worker_threads');
      const fs = require('node:fs');
      const gate = new Int32Array(workerData.testGate);
      const fsync = fs.fsyncSync;
      fs.fsyncSync = function(fd) {
        if (Atomics.compareExchange(gate, 0, 1, 2) === 1) {
          Atomics.notify(gate, 0);
          Atomics.wait(gate, 0, 2, 10000);
        }
        return fsync(fd);
      };
      require('tsx/cjs'); require(${JSON.stringify(url.pathname)});
    `, { ...options, eval: true, workerData: { ...options!.workerData, testGate: gate.buffer } });
    return worker;
  });
  assert.ok(sms);
  const api = createServer((_request, response) => {
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify({ ok: true, service: 'dexter-api', release: { releaseId: 'synthetic-sms-isolation' } }));
  });
  await new Promise<void>(resolve => api.listen(0, '127.0.0.1', resolve));
  const apiBaseUrl = `http://127.0.0.1:${(api.address() as { port: number }).port}`;
  const config: BrokerConfig = { host: '127.0.0.1', port: 0, publicOrigin: BASE, stateDir: path.join(dir, 'calls'),
    controlToken: 'c'.repeat(40), apiKey: 'fixture', model: 'fixture', voice: 'marin',
    twilioAccountSid: ACCOUNT, twilioAuthToken: TOKEN, from: SENDER, allowedTo: [PHONE],
    apiBaseUrl, phoneServiceSecret: 's'.repeat(40), maxCallSeconds: 60 };
  const broker = createBrokerServer({ config, sms });
  await new Promise<void>(resolve => broker.server.listen(0, '127.0.0.1', resolve));
  const origin = `http://127.0.0.1:${(broker.server.address() as { port: number }).port}`;
  t.after(async () => { Atomics.store(gate, 0, 0); Atomics.notify(gate, 0); await broker.close(); await new Promise<void>((resolve, reject) => api.close(error => error ? reject(error) : resolve())); fs.rmSync(dir, { recursive: true, force: true }); });
  const webhook = (route: string, body: Record<string, string>, timeout = 5000) => fetch(origin + route, {
    method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded',
      'x-twilio-signature': Twilio.getExpectedTwilioSignature(TOKEN, BASE + route, body) },
    body: new URLSearchParams(body), signal: AbortSignal.timeout(timeout),
  });
  const enrollment = (action: string, body: unknown, bearer = config.controlToken) => fetch(origin+'/internal/broker/sms/enrollment/'+action, {
    method: 'POST', headers: { authorization: `Bearer ${bearer}`, 'content-type': 'application/json' }, body: JSON.stringify(body),
  });
  async function voice() {
    const start = performance.now();
    const health = await fetch(origin + '/broker/health', { signal: AbortSignal.timeout(1000) });
    assert.equal(health.status, 200);
    const call = await webhook('/broker/incoming-call', { AccountSid: ACCOUNT, CallSid: 'CA' + '9'.repeat(32),
      From: PHONE, To: SENDER, Direction: 'inbound', CallStatus: 'ringing' }, 1000);
    assert.equal(call.status, 200); assert.match(await call.text(), /<Response>/);
    return performance.now() - start;
  }
  return { sms, gate, worker, webhook, voice, statePath, enrollment, sends: () => sends, verifies: () => ({ starts, checks }) };
}

test('blocked fsync on a 50k-entry SMS ledger leaves voice health and signed call handling responsive', async t => {
  const f = await fixture(t, true);
  Atomics.store(f.gate, 0, 1);
  let completed = false;
  const incoming = f.webhook('/sms/incoming', { AccountSid: ACCOUNT, MessageSid: 'SM' + 'a'.repeat(32),
    From: PHONE, To: SENDER, Body: 'Synthetic hello' }).then(result => { completed = true; return result; });
  for (let i = 0; i < 500 && Atomics.load(f.gate, 0) !== 2; i++) await new Promise(resolve => setTimeout(resolve, 5));
  assert.equal(Atomics.load(f.gate, 0), 2, 'SMS is blocked inside the actual durable write');
  try {
    const elapsed = await f.voice();
    assert.equal(completed, false, 'voice completes while SMS fsync is still blocked');
    t.diagnostic(`Voice health plus signed incoming request with blocked SMS disk: ${elapsed.toFixed(1)}ms`);
  } finally { Atomics.store(f.gate, 0, 0); Atomics.notify(f.gate, 0); }
  assert.equal((await incoming).status, 200);
  const saved = JSON.parse(fs.readFileSync(f.statePath, 'utf8'));
  assert.equal(saved.inbox.some((item: any) => item.sid === 'SM' + 'a'.repeat(32)), true);
});

test('SMS worker exit fails SMS closed and preserves voice HTTP and signed calls', async t => {
  const f = await fixture(t);
  Atomics.store(f.gate, 0, 1);
  const pending = f.webhook('/sms/incoming', { AccountSid: ACCOUNT, MessageSid: 'SM' + 'c'.repeat(32),
    From: PHONE, To: SENDER, Body: 'Synthetic in-flight hello' });
  for (let i = 0; i < 500 && Atomics.load(f.gate, 0) !== 2; i++) await new Promise(resolve => setTimeout(resolve, 5));
  assert.equal(Atomics.load(f.gate, 0), 2);
  await f.worker.terminate();
  assert.equal((await pending).status, 503);
  await assert.rejects(f.sms.requestedEligibility(PHONE), /sms_unavailable/);
  const response = await f.webhook('/sms/incoming', { AccountSid: ACCOUNT, MessageSid: 'SM' + 'b'.repeat(32),
    From: PHONE, To: SENDER, Body: 'Synthetic hello' });
  assert.equal(response.status, 503);
  assert.equal((await response.json() as any).error, 'sms_unavailable');
  await f.voice();
});

test('expiry or shutdown while worker eligibility is pending cannot reserve a new result SMS', async () => {
  const { randomUUID } = await import('node:crypto');
  const { dispatchResultSms } = await import('../src/broker/resultSmsDispatch.js');
  const { phoneResultSmsIntentDigest, phoneResultSmsPayload } = await import('../src/broker/resultSmsContract.js');
  for (const reason of ['expiry', 'shutdown']) {
    let now = Date.now(), accepting = true, sends = 0;
    const identity = { v: 'phone.result-sms.v1' as const, intentId: randomUUID(), bindingId: randomUUID(),
      callSid: 'CA' + '1'.repeat(32), requestId: randomUUID(), runId: randomUUID(), ownerPrincipalId: randomUUID(),
      primaryDexterId: randomUUID(), threadId: randomUUID(), phoneNumber: PHONE,
      source: { callerIntentOperationId: randomUUID(), sourceEventId: randomUUID(), sourceItemId: 'synthetic-turn',
        sourceRevision: 1 as const, sourceDigest: 'b'.repeat(64) },
      admittedAt: new Date(now).toISOString(), expiresAt: new Date(now + 60_000).toISOString() };
    const resolved = { identity, intentDigest: phoneResultSmsIntentDigest(identity), payload: phoneResultSmsPayload(identity,
      { eventId: randomUUID(), messageId: randomUUID(), sequence: '12', contentDigest: 'c'.repeat(64) }) };
    const result = await dispatchResultSms({ v: identity.v, intentId: identity.intentId,
      intentDigest: resolved.intentDigest, messageId: resolved.payload.messageId }, {
      client: { resolveIdentity: async () => resolved, admitDispatch: async () => resolved },
      accepting: () => accepting, now: () => now,
      sms: {
        requestedReceipt: async () => null,
        async requestedEligibility() {
          await new Promise(resolve => setImmediate(resolve));
          if (reason === 'expiry') now = Date.parse(identity.expiresAt); else accepting = false;
          return { ok: true };
        },
        async sendRequested() { sends++; throw new Error('must not send'); },
        async handle() { return false; }, stop() {}, async close() {},
      },
    });
    assert.deepEqual(result, { ok: false, error: reason === 'expiry' ? 'sms_intent_expired' : 'sms_unavailable' });
    assert.equal(sends, 0);
  }
});

test('a send queued behind a blocked ledger commit cannot reserve after its frozen deadline', async t => {
  const f = await fixture(t, false, true);
  Atomics.store(f.gate, 0, 1);
  const incoming = f.webhook('/sms/incoming', { AccountSid: ACCOUNT, MessageSid: 'SM' + 'e'.repeat(32),
    From: PHONE, To: SENDER, Body: 'Synthetic hello' });
  for (let i = 0; i < 500 && Atomics.load(f.gate, 0) !== 2; i++) await new Promise(resolve => setTimeout(resolve, 5));
  assert.equal(Atomics.load(f.gate, 0), 2);
  const send = f.sms.sendRequested(PHONE, 'CA'+'a'.repeat(32), 'Synthetic requested result', new Date(Date.now()+50).toISOString());
  await new Promise(resolve => setTimeout(resolve, 100));
  Atomics.store(f.gate, 0, 0); Atomics.notify(f.gate, 0);
  assert.equal((await incoming).status, 200);
  assert.deepEqual(await send, { ok: false, error: 'sms_intent_expired', enrollmentUrl: 'https://dexter.cash/sms' });
  assert.equal(f.sends(), 0);
  assert.deepEqual(JSON.parse(fs.readFileSync(f.statePath, 'utf8')).messages, []);
});

test('account Verify HTTP binds approved phone proof to its authenticated owner and never enrolls messaging consent', async t => {
  const f = await fixture(t, false, false, true), ownerId = randomUUID(), otherOwnerId = randomUUID();
  assert.equal((await f.enrollment('start', { ownerId, phone: PHONE }, 'wrong-token')).status, 401);
  assert.equal((await f.enrollment('start', { ownerId, phone: PHONE, client: 'attacker-selected' })).status, 400);
  assert.equal((await f.enrollment('start', { ownerId: 'not-a-uuid', phone: PHONE })).status, 400);
  assert.deepEqual(f.verifies(), { starts: 0, checks: 0 });
  const started = await f.enrollment('start', { ownerId, phone: PHONE });
  assert.equal(started.status, 200);
  const { challengeId } = await started.json() as { challengeId: string };
  assert.equal((await f.enrollment('check', { ownerId: otherOwnerId, challengeId, code: '123456' })).status, 400);
  assert.equal((await f.enrollment('check', { ownerId, challengeId, code: '000000' })).status, 400);
  assert.deepEqual(f.verifies(), { starts: 1, checks: 1 }, 'wrong owner never reaches Verify; pending code grants no proof');
  const approved = await f.enrollment('check', { ownerId, challengeId, code: '123456' });
  assert.equal(approved.status, 200);
  const proof = await approved.json() as { ok: true; phone: string; verifiedAt: string };
  assert.deepEqual(Object.keys(proof).sort(), ['ok', 'phone', 'verifiedAt']);
  assert.equal(proof.ok, true); assert.equal(proof.phone, PHONE);
  assert.equal(new Date(proof.verifiedAt).toISOString(), proof.verifiedAt);
  assert.equal((await f.enrollment('check', { ownerId: otherOwnerId, challengeId, code: '123456' })).status, 400);
  const recovered = await f.enrollment('check', { ownerId, challengeId, code: '999999' });
  assert.equal(recovered.status, 200); assert.deepEqual(await recovered.json(), proof);
  assert.deepEqual(f.verifies(), { starts: 1, checks: 2 }, 'same owner can recover lost response without another Verify call');
  const saved = JSON.parse(fs.readFileSync(f.statePath, 'utf8'));
  assert.equal(saved.challenges[0].client, createHmac('sha256', 'c'.repeat(40)).update(`sms-account:${ownerId}`).digest('hex'));
  assert.equal(typeof saved.challenges[0].verified, 'number');
  assert.equal('code' in saved.challenges[0], false);
  assert.deepEqual(saved.consents, []); assert.deepEqual(saved.events, []); assert.deepEqual(saved.messages, []);
  assert.equal(f.sends(), 0);
});

test('account challenge recovery excludes pending, failed, wrong-owner and expired verification', async t => {
  const dir = fs.mkdtempSync(path.join(os.homedir(), '.local/state/sms-challenge-recovery-'));
  let now = Date.now(), failStart = false;
  const service = new SmsService({ statePath: path.join(dir, 'sms.json'), now: () => now,
    enrollmentEnabled: true, sendEnabled: false, campaignApproved: false, advancedOptOutConfigured: false,
    messagingServiceSid: '', publicBaseUrl: BASE,
    provider: { async startVerification() { if (failStart) throw new Error('synthetic_verification_outage'); },
      async checkVerification() { return true; }, async send() { throw new Error('must not send'); } },
  });
  t.after(() => { service.close(); fs.rmSync(dir, { recursive: true, force: true }); });
  const client = 'a'.repeat(64), challenge = await service.start(PHONE, client);
  assert.equal(service.verifiedChallenge(challenge.challengeId, client), null);
  const approved = await service.check(challenge.challengeId, '123456', client);
  assert.deepEqual(service.verifiedChallenge(challenge.challengeId, client), service.verifiedPhone(approved.proofToken, client));
  assert.equal(service.verifiedChallenge(challenge.challengeId, 'b'.repeat(64)), null);
  now += 10*60_000;
  assert.equal(service.verifiedChallenge(challenge.challengeId, client), null);
  failStart = true;
  await assert.rejects(service.start(SENDER, client), /verification_unavailable/);
  const saved = JSON.parse(fs.readFileSync(path.join(dir, 'sms.json'), 'utf8'));
  const failed = saved.challenges.find((entry: any) => entry.state === 'failed');
  assert.ok(failed); assert.equal(service.verifiedChallenge(failed.id, client), null);
  assert.deepEqual(saved.consents, []);
});
