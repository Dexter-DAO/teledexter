/** Local-only PostgreSQL/API -> Broker -> durable SMS -> fake carrier crossing. */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { createInterface } from 'node:readline';
import { once } from 'node:events';
import type Twilio from 'twilio';
import { createBrokerServer } from '../../src/broker/server.js';
import { createBrokerSmsBridge } from '../../src/broker/sms.js';
import type { BrokerConfig } from '../../src/broker/config.js';
import { SmsService, SMS_DISCLOSURE_VERSION } from '../../src/sms/SmsService.js';

const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
const [line] = await once(lines, 'line');
lines.close();
assert.equal(typeof line, 'string');
assert.ok(Buffer.byteLength(line) <= 8192, 'fixture_input_too_large');
const input: unknown = JSON.parse(line);
assert.ok(input && typeof input === 'object' && !Array.isArray(input), 'fixture_input_invalid');
const configInput = input as Record<string, unknown>;
assert.deepEqual(Object.keys(configInput).sort(),
  ['stateDir', 'apiBaseUrl', 'phoneServiceSecret', 'controlToken', 'phoneNumber'].sort());
for (const key of Object.keys(configInput)) assert.equal(typeof configInput[key], 'string');
const { stateDir, apiBaseUrl, phoneServiceSecret, controlToken, phoneNumber } = configInput as Record<string, string>;
assert.ok(path.isAbsolute(stateDir!), 'fixture_state_must_be_absolute');
assert.ok(fs.lstatSync(stateDir!).isDirectory() && !fs.lstatSync(stateDir!).isSymbolicLink(), 'fixture_state_directory_required');
assert.match(apiBaseUrl!, /^http:\/\/127\.0\.0\.1:[1-9][0-9]{0,4}$/);
assert.ok(new URL(apiBaseUrl!).port, 'fixture_api_port_required');
assert.match(phoneNumber!, /^\+1[2-9][0-9]{9}$/);
assert.ok(phoneServiceSecret!.length >= 32 && phoneServiceSecret!.length <= 4096);
assert.ok(controlToken!.length >= 32 && controlToken!.length <= 4096);

// Every HTTP request made by a default client stays inside the parent's local fixture.
const realFetch = globalThis.fetch;
globalThis.fetch = (request, init) => {
  const url = new URL(request instanceof Request ? request.url : request.toString());
  assert.equal(url.origin, apiBaseUrl, 'unexpected_fixture_network');
  return realFetch(request, init);
};
const emit = (value: unknown) => process.stdout.write(`${JSON.stringify(value)}\n`);
const BASE = 'https://phone.example.test', TOKEN = 'fixture-webhook', CLIENT = 'a'.repeat(64);
const ACCOUNT = `AC${'4'.repeat(32)}`, SERVICE = `MG${'2'.repeat(32)}`, SID = `SM${'3'.repeat(32)}`;
const SENDER = '+12025550124', statePath = path.join(stateDir!, 'sms.json');
const seed = new SmsService({ statePath, enrollmentEnabled: true, sendEnabled: false,
  campaignApproved: true, advancedOptOutConfigured: true, messagingServiceSid: SERVICE, publicBaseUrl: BASE,
  provider: { async startVerification() {}, async checkVerification() { return true; },
    async send() { throw new Error('unexpected_enrollment_send'); } } });
try {
  const challenge = await seed.start(phoneNumber!, CLIENT);
  const proof = await seed.check(challenge.challengeId, '123456', CLIENT);
  await seed.enroll(proof.proofToken, CLIENT, true, SMS_DISCLOSURE_VERSION);
} finally { seed.close(); }

const env: NodeJS.ProcessEnv = {
  DEXTER_SMS_STATE_PATH: statePath, DEXTER_SMS_INTERNAL_SECRET: 'i'.repeat(40),
  DEXTER_SMS_ENROLLMENT_ENABLED: 'true', DEXTER_SMS_SEND_ENABLED: 'true', DEXTER_SMS_CAMPAIGN_APPROVED: 'true',
  DEXTER_SMS_ADVANCED_OPT_OUT_CONFIGURED: 'true', TWILIO_VERIFY_SID: `VA${'5'.repeat(32)}`,
  TWILIO_ACCOUNT_SID: ACCOUNT, TWILIO_AUTH_TOKEN: TOKEN, TWILIO_PHONE_NUMBER: SENDER,
  TWILIO_MESSAGING_SERVICE_SID: SERVICE, DEXTER_PHONE_PUBLIC_URL: BASE,
};
const unexpected = async () => { throw new Error('unexpected_voice_or_verify_operation'); };
const clients = {
  verify: { verify: { v2: { services: () => ({ verifications: { create: unexpected }, verificationChecks: { create: unexpected } }) } } },
  messaging: { messages: { async create(value: { to: string; body: string; messagingServiceSid: string; statusCallback: string }) {
    assert.equal(value.to, phoneNumber);
    emit({ kind: 'send', input: value, sid: SID });
    return { sid: SID, status: 'queued' };
  } } },
} as unknown as { verify: ReturnType<typeof Twilio>; messaging: ReturnType<typeof Twilio> };
const sms = await createBrokerSmsBridge(env, path.join(stateDir!, 'release'), clients);
assert.ok(sms, 'fixture_sms_bridge_unavailable');
const config: BrokerConfig = { host: '127.0.0.1', port: 0, publicOrigin: BASE, stateDir: path.join(stateDir!, 'calls'),
  controlToken: controlToken!, apiKey: 'fixture', model: 'fixture', voice: 'marin',
  twilioAccountSid: ACCOUNT, twilioAuthToken: TOKEN, from: SENDER, allowedTo: [phoneNumber!],
  apiBaseUrl: apiBaseUrl!, phoneServiceSecret: phoneServiceSecret!, maxCallSeconds: 60 };
const calls = Object.assign(() => ({ fetch: unexpected, update: unexpected }), { create: unexpected });
const broker = createBrokerServer({ config, sms,
  provider: { calls } as unknown as ReturnType<typeof Twilio>,
  voiceFactory: () => { throw new Error('unexpected_voice_session'); },
  dependencyHealth: async () => ({ ok: true, checkedAt: new Date().toISOString(),
    primaryApi: { ok: true, releaseId: 'result-sms-local-fixture' } }),
});
let stopped = false;
async function stop() {
  if (stopped) return;
  stopped = true;
  await broker.close();
  process.exit(0);
}
process.once('SIGTERM', () => { void stop(); });
process.once('SIGINT', () => { void stop(); });
await new Promise<void>((resolve, reject) => {
  broker.server.once('error', reject);
  broker.server.listen(0, '127.0.0.1', resolve);
});
assert.equal(broker.store.list().length, 0, 'fixture_must_have_no_live_calls');
emit({ kind: 'ready', origin: `http://127.0.0.1:${(broker.server.address() as { port: number }).port}` });
