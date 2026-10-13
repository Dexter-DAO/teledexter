import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { pathToFileURL } from 'node:url';
import Twilio from 'twilio';
import { createBrokerSmsBridge } from '../src/broker/sms.js';
import { createBrokerServer } from '../src/broker/server.js';
import { SmsService, SMS_DISCLOSURE_VERSION, type SmsOptions } from '../src/sms/SmsService.js';
import type { BrokerConfig } from '../src/broker/config.js';
const PHONE = '+15555550123', SENDER = '+15555550124', CLIENT = 'a'.repeat(64);
const SECRET = 'internal-test-secret-'.repeat(4), TOKEN = 'fixture-sms-webhook';
const ACCOUNT = 'AC' + '1'.repeat(32), BASE = 'https://phone.example.test', MESSAGE = 'SM' + '2'.repeat(32);
function settings(t: test.TestContext) {
  // The real durable-state validator rejects temporary roots and release-contained state.
  const stateHome = path.join(os.homedir(), '.local/state');
  fs.mkdirSync(stateHome, { recursive: true });
  const dir = fs.mkdtempSync(path.join(stateHome, 'broker-sms-test-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const env: NodeJS.ProcessEnv = {
    DEXTER_SMS_STATE_PATH: path.join(dir, 'sms.json'), DEXTER_SMS_INTERNAL_SECRET: SECRET,
    DEXTER_SMS_ENROLLMENT_ENABLED: 'true', DEXTER_SMS_SEND_ENABLED: 'false',
    DEXTER_SMS_CAMPAIGN_APPROVED: 'false', DEXTER_SMS_ADVANCED_OPT_OUT_CONFIGURED: 'false',
    TWILIO_VERIFY_SID: 'VA' + '3'.repeat(32), TWILIO_ACCOUNT_SID: ACCOUNT,
    TWILIO_AUTH_TOKEN: TOKEN, TWILIO_PHONE_NUMBER: SENDER,
    TWILIO_MESSAGING_SERVICE_SID: 'MG' + '4'.repeat(32), DEXTER_PHONE_PUBLIC_URL: BASE,
  };
  const calls = { starts: 0, checks: 0, sends: 0 };
  const clients = { verify: { verify: { v2: { services: () => ({
    verifications: { async create() { calls.starts++; } },
    verificationChecks: { async create({ code }: { code: string }) { calls.checks++; return { status: code === '123456' ? 'approved' : 'pending' }; } },
  }) } } }, messaging: { messages: { async create() { calls.sends++; throw Error('unexpected_send'); } } } } as unknown as { verify: ReturnType<typeof Twilio>; messaging: ReturnType<typeof Twilio> };
  const options: SmsOptions = { statePath: env.DEXTER_SMS_STATE_PATH!, enrollmentEnabled: true,
    sendEnabled: false, campaignApproved: false, advancedOptOutConfigured: false,
    messagingServiceSid: env.TWILIO_MESSAGING_SERVICE_SID!, publicBaseUrl: BASE,
    provider: { async startVerification() {}, async checkVerification() { return true; }, async send() { throw Error('unexpected_send'); } },
  };
  return { dir, env, calls, clients, options };
}
async function serve(t: test.TestContext, f: ReturnType<typeof settings>) {
  const sms = await createBrokerSmsBridge(f.env, path.join(f.dir, 'release'), f.clients);
  assert.ok(sms);
  const config: BrokerConfig = { host: '127.0.0.1', port: 0, publicOrigin: BASE, stateDir: path.join(f.dir, 'calls'),
    controlToken: 'c'.repeat(40), apiKey: 'fixture', model: 'fixture', voice: 'marin',
    twilioAccountSid: ACCOUNT, twilioAuthToken: TOKEN, from: SENDER,
    allowedTo: [PHONE], apiBaseUrl: 'http://127.0.0.1:1', phoneServiceSecret: 's'.repeat(40), maxCallSeconds: 60 };
  const broker = createBrokerServer({ config, sms });
  await new Promise<void>(resolve => broker.server.listen(0, '127.0.0.1', resolve));
  const origin = `http://127.0.0.1:${(broker.server.address() as { port: number }).port}`;
  t.after(async () => { await broker.close(); });
  const request = (action: string, body?: unknown, client = CLIENT, secret = SECRET) => fetch(origin + '/internal/sms/' + action, {
    method: body === undefined ? 'GET' : 'POST', headers: { 'content-type': 'application/json',
      'x-dexter-sms-internal-secret': secret, 'x-dexter-sms-client-id': client },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  const webhook = (route: string, body: Record<string, string>, signed = true) => fetch(origin + route, {
    method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded',
      'x-twilio-signature': signed ? Twilio.getExpectedTwilioSignature(TOKEN, BASE + route, body) : 'bad' }, body: new URLSearchParams(body),
  });
  return { sms, broker, origin, request, webhook };
}
async function verify(f: Awaited<ReturnType<typeof serve>>) {
  const start = await f.request('verify/start', { phone: PHONE }); assert.equal(start.status, 200);
  const { challengeId } = await start.json() as { challengeId: string };
  const checked = await f.request('verify/check', { challengeId, code: '123456' }); assert.equal(checked.status, 200);
  return await checked.json() as { proofToken: string; status: { enrolled: boolean } };
}

test('SMS is opt-in to Broker startup, and partial configuration fails admission', async t => {
  assert.equal(await createBrokerSmsBridge({}, '/unused'), undefined);
  const f = settings(t);
  await assert.rejects(createBrokerSmsBridge({ ...f.env, DEXTER_SMS_STATE_PATH: undefined }, '/unused', f.clients), /sms_state_admission_failed/);
  assert.equal(fs.existsSync(f.env.DEXTER_SMS_STATE_PATH!), false);
});
test('existing writer and corrupt journal prevent startup; existing owner lock is preserved', async t => {
  const f = settings(t), old = new SmsService(f.options);
  const before = fs.readFileSync(f.options.statePath + '.lock');
  await assert.rejects(createBrokerSmsBridge(f.env, '/unused', f.clients), /sms_state_admission_failed/);
  assert.deepEqual(fs.readFileSync(f.options.statePath + '.lock'), before);
  old.close();
  fs.writeFileSync(f.options.statePath, '{corrupt', { mode: 0o600 });
  await assert.rejects(createBrokerSmsBridge(f.env, '/unused', f.clients), /sms_state_admission_failed/);
  assert.equal(fs.existsSync(f.options.statePath + '.lock'), false);
});
test('actual Broker HTTP preserves enrollment, proof binding, withdrawal and disabled sending', async t => {
  const f = settings(t), server = await serve(t, f);
  const denied = await server.request('config', undefined, CLIENT, 'wrong'); assert.equal(denied.status, 404);
  const config = await server.request('config'); assert.equal(config.status, 200); assert.equal(config.headers.get('cache-control'), 'no-store');
  assert.deepEqual(await config.json(), { ok: true, enrollmentAvailable: true, deliveryActive: false, disclosureVersion: SMS_DISCLOSURE_VERSION });
  assert.equal((await server.request('verify/start', { phone: PHONE }, '')).status, 400);
  const proof = await verify(server); assert.equal(proof.status.enrolled, false);
  assert.equal((await server.request('status', { proofToken: proof.proofToken }, 'b'.repeat(64))).status, 401);
  assert.equal((await server.request('enroll', { proofToken: proof.proofToken, consent: true, disclosureVersion: 'old' })).status, 400);
  const enrolled = await server.request('enroll', { proofToken: proof.proofToken, consent: true, disclosureVersion: SMS_DISCLOSURE_VERSION });
  assert.equal(enrolled.status, 200); assert.equal((await enrolled.json() as any).status.enrolled, true);
  const status = await server.request('status', { proofToken: proof.proofToken }); assert.equal((await status.json() as any).status.maskedPhone, '+1 *** *** 0123');
  const withdrawn = await server.request('withdraw', { proofToken: proof.proofToken }); assert.equal((await withdrawn.json() as any).status.enrolled, false);
  const limited = await server.request('verify/start', { phone: PHONE }); assert.equal(limited.status, 429); assert.ok(limited.headers.get('retry-after'));
  assert.equal((await server.request('verify/start', { phone: PHONE, extra: 'x'.repeat(2100) })).status, 413);
  assert.deepEqual(f.calls, { starts: 1, checks: 1, sends: 0 });
  assert.deepEqual(server.broker.store.list(), []);
});
test('current disabled flags still allow an existing proof to read and withdraw consent', async t => {
  const f = settings(t), old = new SmsService(f.options);
  const start = await old.start(PHONE, CLIENT), proof = await old.check(start.challengeId, '123456', CLIENT);
  await old.enroll(proof.proofToken, CLIENT, true, SMS_DISCLOSURE_VERSION); old.close();
  f.env.DEXTER_SMS_ENROLLMENT_ENABLED = 'false';
  const server = await serve(t, f);
  const config = await server.request('config');
  assert.deepEqual(await config.json(), { ok: true, enrollmentAvailable: false, deliveryActive: false, disclosureVersion: SMS_DISCLOSURE_VERSION });
  assert.equal((await server.request('verify/start', { phone: PHONE })).status, 503);
  assert.equal((await server.request('enroll', { proofToken: proof.proofToken, consent: true, disclosureVersion: SMS_DISCLOSURE_VERSION })).status, 503);
  const status = await server.request('status', { proofToken: proof.proofToken }); assert.equal((await status.json() as any).status.enrolled, true);
  const withdrawal = await server.request('withdraw', { proofToken: proof.proofToken }); assert.equal((await withdrawal.json() as any).status.enrolled, false);
  assert.deepEqual(f.calls, { starts: 0, checks: 0, sends: 0 });
});
test('signed provider STOP and delivery status retain exact URL, account and recipient checks', async t => {
  const f = settings(t), server = await serve(t, f), proof = await verify(server);
  await server.request('enroll', { proofToken: proof.proofToken, consent: true, disclosureVersion: SMS_DISCLOSURE_VERSION });
  const inbound = { AccountSid: ACCOUNT, MessageSid: MESSAGE, To: SENDER, From: PHONE, Body: 'STOP', OptOutType: 'STOP' };
  assert.equal((await server.webhook('/sms/incoming', inbound, false)).status, 403);
  assert.equal((await server.webhook('/sms/incoming', { ...inbound, To: PHONE })).status, 400);
  assert.equal((await server.webhook('/sms/incoming', inbound)).status, 200);
  const stopped = await server.request('status', { proofToken: proof.proofToken }); assert.equal((await stopped.json() as any).status.carrierOptedOut, true);
  const reenroll = await server.request('enroll', { proofToken: proof.proofToken, consent: true, disclosureVersion: SMS_DISCLOSURE_VERSION }); assert.equal(reenroll.status, 409);
  assert.equal((await server.webhook('/sms/status/' + 'a'.repeat(64), { AccountSid: ACCOUNT, MessageSid: MESSAGE, To: PHONE, MessageStatus: 'delivered' }, false)).status, 403);
  assert.equal(f.calls.sends, 0);
});
test('legacy journal reopens under Broker and back with consent, suppression, proof and cooldown intact', async t => {
  const f = settings(t);
  const legacyRoot = process.env.BROKER_SMS_LEGACY_RELEASE;
  const LegacySmsService: typeof SmsService = legacyRoot
    ? (await import(pathToFileURL(path.join(legacyRoot, 'src/sms/SmsService.ts')).href)).SmsService : SmsService;
  const oldOptions = { ...f.options, sendEnabled: true, campaignApproved: true, advancedOptOutConfigured: true,
    provider: { ...f.options.provider, async send() { return { sid: MESSAGE, status: 'queued' }; } } };
  const old = new LegacySmsService(oldOptions);
  const challenge = await old.start(PHONE, CLIENT), proof = await old.check(challenge.challengeId, '123456', CLIENT);
  await old.enroll(proof.proofToken, CLIENT, true, SMS_DISCLOSURE_VERSION);
  old.inbound({ phone: PHONE, sid: MESSAGE, body: 'STOP', optOutType: 'STOP' });
  old.close();
  const saved = JSON.parse(fs.readFileSync(f.options.statePath, 'utf8'));
  const server = await serve(t, f);
  assert.deepEqual(JSON.parse(fs.readFileSync(f.options.statePath, 'utf8')), saved);
  assert.equal(saved.messages.length, 1);
  const delivered = await server.webhook('/sms/status/' + saved.messages[0].id, { AccountSid: ACCOUNT, MessageSid: MESSAGE, To: PHONE, MessageStatus: 'delivered' });
  assert.equal(delivered.status, 204);
  const status = await server.request('status', { proofToken: proof.proofToken });
  assert.equal((await status.json() as any).status.carrierOptedOut, true);
  assert.equal((await server.request('verify/start', { phone: PHONE })).status, 429);
  await server.request('withdraw', { proofToken: proof.proofToken });
  await server.broker.close();
  const reopened = new LegacySmsService(f.options);
  t.after(() => reopened.close());
  assert.equal(reopened.status(proof.proofToken, CLIENT).enrolled, false);
  assert.equal(reopened.status(proof.proofToken, CLIENT).carrierOptedOut, true);
  assert.equal(JSON.parse(fs.readFileSync(f.options.statePath, 'utf8')).messages[0].status, 'delivered');
  assert.equal(f.calls.sends, 0);
});
test('shutdown retains ownership through an in-flight provider verification', async t => {
  const f = settings(t); let finish!: () => void, started!: () => void;
  const began = new Promise<void>(resolve => { started = resolve; });
  f.clients.verify.verify.v2.services = (() => ({ verifications: { create: async () => { started(); await new Promise<void>(resolve => { finish = resolve; }); } } })) as any;
  const server = await serve(t, f);
  const response = server.request('verify/start', { phone: PHONE }); await began;
  let closed = false;
  const closing = server.broker.close().then(() => { closed = true; });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(closed, false);
  await assert.rejects(createBrokerSmsBridge(f.env, '/unused', f.clients), /sms_state_admission_failed/);
  finish(); assert.equal((await response).status, 200); await closing;
  const next = await createBrokerSmsBridge(f.env, '/unused', f.clients); assert.ok(next); await next.close();
});
