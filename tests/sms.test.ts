import assert from 'node:assert/strict';
import test from 'node:test';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { pathToFileURL } from 'node:url';
import Fastify from 'fastify';
import formBody from '@fastify/formbody';
import Twilio from 'twilio';
import { SmsService, SmsError, SMS_DISCLOSURE_VERSION, SMS_DISCLOSURE, type SmsOptions } from '../src/sms/SmsService.js';
import { registerSmsRoutes } from '../src/sms/registerSmsRoutes.js';
import { createCallerSmsSender } from '../src/sms/createSmsTool.js';
import { configureSms } from '../src/sms/configureSms.js';

const PHONE = '+15555550123';
const OTHER = '+15555550124';
const CLIENT = 'a'.repeat(64);
const CALL = `CA${'1'.repeat(32)}`;
const SID = `SM${'2'.repeat(32)}`;
const SERVICE = `MG${'3'.repeat(32)}`;
const ACCOUNT = `AC${'4'.repeat(32)}`;
const SECRET = 'internal-test-secret-'.repeat(4);
const WEBHOOK_TOKEN = 'webhook-test-token';
const BASE = 'https://phone.example.test';

function fixture(t: any, overrides: Partial<SmsOptions> = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dexter-sms-test-'));
  let now = Date.parse('2026-09-16T12:00:00Z');
  const sends: Array<{ to: string; body: string; statusCallback: string }> = [];
  const starts: string[] = [];
  let checks = 0;
  const options: SmsOptions = {
    statePath: path.join(dir, 'sms.json'), enrollmentEnabled: true, sendEnabled: false,
    campaignApproved: true, advancedOptOutConfigured: true, messagingServiceSid: SERVICE, publicBaseUrl: BASE, now: () => now,
    provider: {
      startVerification: async phone => { starts.push(phone); },
      checkVerification: async () => { checks++; return true; },
      send: async input => { sends.push(input); return { sid: SID, status: 'queued' }; },
    }, ...overrides,
  };
  let service = new SmsService(options);
  t.after(() => { service.close(); fs.rmSync(dir, { recursive: true, force: true }); });
  return {
    get service() { return service; }, options, dir, sends, starts, get checks() { return checks; },
    tick(ms: number) { now += ms; },
    reopen() { service.close(); service = new SmsService(options); },
    persisted() { return JSON.parse(fs.readFileSync(options.statePath, 'utf8')); },
  };
}
async function verify(f: ReturnType<typeof fixture>, phone = PHONE, client = CLIENT) {
  const start = await f.service.start(phone, client);
  return (await f.service.check(start.challengeId, '123456', client)).proofToken;
}
async function enroll(f: ReturnType<typeof fixture>, phone = PHONE) {
  const proof = await verify(f, phone);
  await f.service.enroll(proof, CLIENT, true, SMS_DISCLOSURE_VERSION);
  return proof;
}
const error = (code: string) => (e: unknown) => e instanceof SmsError && e.code === code;

test('OTP proves ownership without enrolling; explicit versioned consent persists separately', async t => {
  const f = fixture(t);
  const proof = await verify(f);
  assert.equal(f.service.status(proof, CLIENT).enrolled, false);
  await assert.rejects(f.service.enroll(proof, CLIENT, false, SMS_DISCLOSURE_VERSION), error('invalid_consent'));
  await assert.rejects(f.service.enroll(proof, CLIENT, true, 'old'), error('invalid_consent'));
  await f.service.enroll(proof, CLIENT, true, SMS_DISCLOSURE_VERSION);
  f.reopen();
  assert.equal(f.service.status(proof, CLIENT).enrolled, true);
  assert.equal(f.service.status(proof, CLIENT).maskedPhone, '+1 *** *** 0123');
  assert.equal(f.sends.length, 0);
  const evidence = f.persisted().events[0];
  assert.match(evidence.disclosure, /Source: https:\/\/dexter.cash\/sms/);
  assert.ok(evidence.disclosure.includes(SMS_DISCLOSURE));
  assert.ok(evidence.verifiedAt);
  assert.equal(fs.statSync(f.options.statePath).mode & 0o777, 0o600);
  assert.equal(fs.readFileSync(f.options.statePath, 'utf8').includes(proof), false);
});

test('enrollment and sending are separate off switches; welcome is once per enrollment', async t => {
  const f = fixture(t, { enrollmentEnabled: false });
  await assert.rejects(f.service.start(PHONE, CLIENT), error('sms_unavailable'));
  assert.equal(f.starts.length, 0);
  f.options.enrollmentEnabled = true;
  f.options.sendEnabled = true;
  f.options.advancedOptOutConfigured = false;
  const proof = await enroll(f);
  await assert.rejects(f.service.sendRequested(PHONE, CALL, 'Requested info'), error('sms_delivery_pending'));
  assert.equal(f.sends.length, 0);
  f.service.withdraw(proof, CLIENT);
  f.options.advancedOptOutConfigured = true;
  await f.service.enroll(proof, CLIENT, true, SMS_DISCLOSURE_VERSION);
  await f.service.enroll(proof, CLIENT, true, SMS_DISCLOSURE_VERSION);
  assert.equal(f.sends.length, 1);
  assert.match(f.sends[0].body, /You are subscribed/);
});

test('proofs are client-bound, expire, and cannot authorize another number', async t => {
  const f = fixture(t);
  const proof = await verify(f);
  assert.throws(() => f.service.status(proof, 'b'.repeat(64)), error('proof_expired'));
  assert.throws(() => f.service.status('not-a-token', CLIENT), error('invalid_proof'));
  f.tick(15 * 60_000 + 1);
  await assert.rejects(f.service.enroll(proof, CLIENT, true, SMS_DISCLOSURE_VERSION), error('proof_expired'));
});

test('OTP cooldown and phone hourly limits survive restart', async t => {
  const f = fixture(t);
  await f.service.start(PHONE, CLIENT);
  await assert.rejects(f.service.start(PHONE, CLIENT), error('rate_limited'));
  f.reopen();
  await assert.rejects(f.service.start(PHONE, CLIENT), error('rate_limited'));
  f.tick(60_001); await f.service.start(PHONE, CLIENT);
  f.tick(60_001); await f.service.start(PHONE, CLIENT);
  f.tick(60_001); await assert.rejects(f.service.start(PHONE, 'b'.repeat(64)), error('rate_limited'));
  assert.equal(f.starts.length, 3);
});

test('OTP wrong-code attempts stop at five and concurrent checks cannot bypass the cap', async t => {
  const f = fixture(t);
  let resolve: (value: boolean) => void = () => {};
  let checkCalls = 0;
  f.options.provider.checkVerification = async () => { checkCalls++; return new Promise(r => { resolve = r; }); };
  const c = await f.service.start(PHONE, CLIENT);
  const pending = f.service.check(c.challengeId, '111111', CLIENT);
  await assert.rejects(f.service.check(c.challengeId, '222222', CLIENT), error('invalid_verification'));
  resolve(false);
  await assert.rejects(pending, error('invalid_verification'));
  f.options.provider.checkVerification = async () => { checkCalls++; return false; };
  for (let n = 0; n < 4; n++) await assert.rejects(f.service.check(c.challengeId, '111111', CLIENT), error('invalid_verification'));
  f.reopen();
  await assert.rejects(f.service.check(c.challengeId, '111111', CLIENT), error('invalid_verification'));
  assert.equal(checkCalls, 5);
});

test('STOP survives restart, denies sends and web enrollment; YES resubscribes prior consent', async t => {
  const f = fixture(t);
  const proof = await enroll(f);
  f.options.sendEnabled = true;
  assert.equal(f.service.inbound({ phone: PHONE, sid: SID, body: 'STOPALL', optOutType: 'STOP' }), '');
  f.reopen();
  assert.equal(f.service.status(proof, CLIENT).carrierOptedOut, true);
  await assert.rejects(f.service.sendRequested(PHONE, CALL, 'Requested info'), error('sms_consent_required'));
  await assert.rejects(f.service.enroll(proof, CLIENT, true, SMS_DISCLOSURE_VERSION), error('carrier_opted_out'));
  f.service.inbound({ phone: PHONE, sid: `SM${'5'.repeat(32)}`, body: 'YES', optOutType: 'START' });
  assert.equal(f.service.status(proof, CLIENT).enrolled, true);
  f.service.inbound({ phone: PHONE, sid: SID, body: 'STOPALL', optOutType: 'STOP' });
  assert.equal(f.service.status(proof, CLIENT).enrolled, true, 'duplicate earlier STOP must not undo later START');
  assert.equal(f.persisted().events.at(-1).type, 'sms_start');
});

test('START never invents consent; old disclosure needs reenrollment; ordinary inbound is stored without dispatch', async t => {
  const f = fixture(t);
  const proof = await verify(f);
  f.service.inbound({ phone: PHONE, sid: SID, body: 'START' });
  assert.equal(f.service.status(proof, CLIENT).enrolled, false);
  f.service.inbound({ phone: PHONE, sid: `SM${'5'.repeat(32)}`, body: 'Please help with my request' });
  assert.equal(f.persisted().inbox[0].state, 'suppressed');
  await f.service.enroll(proof, CLIENT, true, SMS_DISCLOSURE_VERSION);
  f.service.inbound({ phone: PHONE, sid: `SM${'6'.repeat(32)}`, body: 'Please follow up' });
  assert.equal(f.persisted().inbox[1].state, 'pending_dispatch');
  const state = f.persisted();
  f.service.close();
  state.consents[0].disclosureVersion = 'previous-program';
  fs.writeFileSync(f.options.statePath, JSON.stringify(state));
  f.reopen();
  f.service.inbound({ phone: PHONE, sid: `SM${'7'.repeat(32)}`, body: 'START' });
  assert.equal(f.service.status(proof, CLIENT).enrolled, false);
  assert.equal(f.sends.length, 0);
});

test('sender closure binds the recipient, refuses missing consent, and deduplicates accepted requests', async t => {
  const f = fixture(t);
  await enroll(f);
  f.options.sendEnabled = true;
  const send = createCallerSmsSender(f.service, PHONE, CALL);
  const result = await send('The link you requested: https://dexter.cash/sms');
  assert.equal(result.ok, true);
  await send('The link you requested: https://dexter.cash/sms');
  assert.equal(f.sends.length, 1);
  assert.equal(f.sends[0].to, PHONE);
  assert.match(f.sends[0].body, /^Dexter: /);
  assert.match(f.sends[0].body, /Reply STOP/);
  await assert.rejects(f.service.sendRequested(OTHER, CALL, 'Requested info'), error('sms_consent_required'));
  await assert.rejects(f.service.sendRequested(PHONE, 'bad-call', 'Requested info'), error('invalid_request'));
});

test('uncertain provider sends are persisted and never auto-retried; immediate 21610 suppresses recipient', async t => {
  const f = fixture(t);
  const proof = await enroll(f);
  f.options.sendEnabled = true;
  let calls = 0;
  f.options.provider.send = async () => { calls++; throw new Error('transport interrupted'); };
  await assert.rejects(f.service.sendRequested(PHONE, CALL, 'First'), error('sms_dispatch_uncertain'));
  f.reopen();
  const replay = await f.service.sendRequested(PHONE, CALL, 'First');
  assert.equal(replay.status, 'uncertain'); assert.equal(calls, 1);
  f.options.provider.send = async () => { throw Object.assign(new Error('blocked'), { code: 21610 }); };
  await assert.rejects(f.service.sendRequested(PHONE, CALL, 'Second'), error('carrier_opted_out'));
  assert.equal(f.service.status(proof, CLIENT).carrierOptedOut, true);
});

test('delivery receipt binds sid and destination and cannot regress after delivered', async t => {
  const f = fixture(t); await enroll(f); f.options.sendEnabled = true;
  const sent = await f.service.sendRequested(PHONE, CALL, 'Requested info');
  assert.throws(() => f.service.delivery(sent.messageId, OTHER, SID, 'delivered', null), error('unknown_message'));
  assert.throws(() => f.service.delivery(sent.messageId, PHONE, `SM${'9'.repeat(32)}`, 'delivered', null), error('unknown_message'));
  f.service.delivery(sent.messageId, PHONE, SID, 'delivered', null);
  f.service.delivery(sent.messageId, PHONE, SID, 'sent', null);
  assert.equal(f.persisted().messages[0].status, 'delivered');
});

test('exclusive writer lock prevents candidate sharing; corruption fails closed; suppression outlives evidence retention', async t => {
  const f = fixture(t, { consentRetentionDays: 1, messageRetentionDays: 1 });
  assert.throws(() => new SmsService(f.options), /EEXIST/);
  const proof = await enroll(f);
  f.service.inbound({ phone: PHONE, sid: SID, body: 'STOP' });
  f.tick(2 * 24 * 60 * 60_000);
  f.service.inbound({ phone: PHONE, sid: `SM${'7'.repeat(32)}`, body: 'HELP' });
  assert.equal(f.persisted().events.length, 0);
  assert.equal(f.persisted().consents[0].carrierOptedOut, true);
  assert.equal(f.persisted().consents[0].disclosureVersion, null);
  f.service.close();
  fs.writeFileSync(f.options.statePath, '{invalid');
  assert.throws(() => new SmsService(f.options));
  assert.equal(fs.existsSync(`${f.options.statePath}.lock`), false);
});

test('failed durable write prevents provider OTP request', async t => {
  const f = fixture(t);
  const renamed = `${f.dir}-moved`;
  fs.renameSync(f.dir, renamed);
  t.after(() => fs.rmSync(renamed, { recursive: true, force: true }));
  await assert.rejects(f.service.start(PHONE, CLIENT), error('sms_unavailable'));
  assert.equal(f.starts.length, 0);
});

test('internal routes require secret and client identity; enrollment rejects extra recipient and affirmative omission', async t => {
  const f = fixture(t);
  const app = Fastify(); app.register(formBody);
  registerSmsRoutes(app, { service: f.service, internalSecret: SECRET, twilioAuthToken: WEBHOOK_TOKEN, twilioAccountSid: ACCOUNT, incomingNumber: OTHER, messagingServiceSid: SERVICE, publicBaseUrl: BASE });
  t.after(() => app.close());
  assert.equal((await app.inject({ method: 'GET', url: '/internal/sms/config' })).statusCode, 404);
  const headers = { 'x-dexter-sms-internal-secret': SECRET, 'x-dexter-sms-client-id': CLIENT };
  const start = await app.inject({ method: 'POST', url: '/internal/sms/verify/start', headers, payload: { phone: PHONE } });
  assert.equal(start.statusCode, 200);
  const checked = await app.inject({ method: 'POST', url: '/internal/sms/verify/check', headers, payload: { challengeId: start.json().challengeId, code: '123456' } });
  const proofToken = checked.json().proofToken;
  assert.ok(proofToken); assert.equal(checked.json().status.enrolled, false);
  for (const payload of [{ proofToken, disclosureVersion: SMS_DISCLOSURE_VERSION }, { proofToken, consent: true, disclosureVersion: SMS_DISCLOSURE_VERSION, phone: OTHER }]) {
    assert.equal((await app.inject({ method: 'POST', url: '/internal/sms/enroll', headers, payload })).statusCode, 400);
  }
  assert.equal((await app.inject({ method: 'POST', url: '/internal/sms/enroll', headers, payload: { proofToken, consent: true, disclosureVersion: SMS_DISCLOSURE_VERSION } })).json().status.enrolled, true);
});

test('signed webhook verifies exact form, account, recipient and URL; controls never generate duplicate TwiML', async t => {
  const f = fixture(t); const proof = await enroll(f);
  const app = Fastify(); app.register(formBody);
  registerSmsRoutes(app, { service: f.service, internalSecret: SECRET, twilioAuthToken: WEBHOOK_TOKEN, twilioAccountSid: ACCOUNT, incomingNumber: OTHER, messagingServiceSid: SERVICE, publicBaseUrl: BASE });
  t.after(() => app.close());
  const body = { AccountSid: ACCOUNT, MessagingServiceSid: SERVICE, MessageSid: SID, From: PHONE, To: OTHER, Body: 'STOP', OptOutType: 'STOP' };
  const signature = Twilio.getExpectedTwilioSignature(WEBHOOK_TOKEN, `${BASE}/sms/incoming`, body);
  const headers = { 'content-type': 'application/x-www-form-urlencoded', 'x-twilio-signature': signature };
  assert.equal((await app.inject({ method: 'POST', url: '/sms/incoming', headers: { ...headers, 'x-twilio-signature': 'bad' }, payload: new URLSearchParams(body).toString() })).statusCode, 403);
  assert.equal(f.service.status(proof, CLIENT).enrolled, true);
  assert.equal((await app.inject({ method: 'POST', url: '/sms/incoming', headers, payload: new URLSearchParams({ ...body, Body: 'START' }).toString() })).statusCode, 403);
  const valid = await app.inject({ method: 'POST', url: '/sms/incoming', headers, payload: new URLSearchParams(body).toString() });
  assert.equal(valid.statusCode, 200); assert.equal(valid.body.includes('<Message>'), false);
  assert.equal(f.service.status(proof, CLIENT).carrierOptedOut, true);
});

test('unconfigured service fails closed without opening Twilio clients or storage', () => {
  const fakeClients = { verify: {}, messaging: {} } as any;
  assert.equal(configureSms({}, fakeClients, '/release'), null);
});

test('closing on SIGTERM releases ledger ownership and preserves bytes for next process', async t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'sms-signal-test-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const statePath = path.join(directory, 'sms.json');
  const serviceModule = pathToFileURL(path.resolve('src/sms/SmsService.ts')).href;
  const code = `import { SmsService } from ${JSON.stringify(serviceModule)};
    const service = new SmsService({ statePath: ${JSON.stringify(statePath)}, enrollmentEnabled: false, sendEnabled: false, campaignApproved: false, advancedOptOutConfigured: false, messagingServiceSid: '', publicBaseUrl: 'https://test.invalid', provider: { startVerification: async () => {}, checkVerification: async () => false, send: async () => { throw new Error('disabled'); } } });
    process.once('SIGTERM', () => { service.close(); process.exit(0); });
    console.log('READY'); setInterval(() => {}, 1000);`;
  const child = spawn(process.execPath, ['--import', 'tsx', '--input-type=module', '-e', code], { stdio: ['ignore', 'pipe', 'pipe'] });
  t.after(() => { if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL'); });
  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('child_start_timeout')), 10_000);
    child.stdout.on('data', chunk => { if (String(chunk).includes('READY')) { clearTimeout(timeout); resolve(); } });
    child.once('error', reject);
  });
  assert.equal(fs.existsSync(`${statePath}.lock`), true);
  const before = fs.readFileSync(statePath, 'utf8');
  const exited = once(child, 'exit');
  child.kill('SIGTERM');
  const [exitCode, signal] = await exited;
  assert.equal(exitCode, 0); assert.equal(signal, null);
  assert.equal(fs.existsSync(`${statePath}.lock`), false);
  assert.equal(fs.readFileSync(statePath, 'utf8'), before);
});
