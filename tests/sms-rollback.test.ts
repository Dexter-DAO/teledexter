import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';
import ts from 'typescript';
import Twilio from 'twilio';
import { SmsService, SMS_DISCLOSURE_VERSION } from '../src/sms/SmsService.js';
import type { BrokerConfig } from '../src/broker/config.js';

// Exercise the actual pre-SMS-fix reader and bridge, rather than a copied schema.
async function previousModule(dir: string, relative: string) {
  const source = execFileSync('git', ['show', `2c69df0:${relative}`], { encoding: 'utf8' });
  const compiled = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext } }).outputText;
  const resolved = compiled.replace(/from (['"])(\.[^'"]+)\1/g, (_match, quote, specifier) =>
    `from ${quote}${pathToFileURL(path.resolve(path.dirname(relative), specifier.replace(/\.js$/, '.ts'))).href}${quote}`);
  const file = path.join(dir, path.basename(relative, '.ts') + '.mjs');
  fs.writeFileSync(file, resolved);
  return import(pathToFileURL(file).href);
}

test('rollback to actual old release removes every SMS variable and keeps signed voice admission available', async t => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sms-rollback-'));
  let broker: { server: import('node:http').Server; close(): Promise<void> } | undefined;
  t.after(async () => { await broker?.close(); fs.rmSync(dir, { recursive: true, force: true }); });
  fs.symlinkSync(path.resolve('node_modules'), path.join(dir, 'node_modules'), 'dir');
  const phone = '+15555550123', number = '+15555550124', client = 'a'.repeat(64), sid = 'SM' + '1'.repeat(32);
  const statePath = path.join(dir, 'sms.json');
  const options = { statePath, enrollmentEnabled: true, sendEnabled: true, campaignApproved: true,
    advancedOptOutConfigured: true, messagingServiceSid: 'MG' + '2'.repeat(32), publicBaseUrl: 'https://phone.example.test',
    provider: { async startVerification() {}, async checkVerification() { return true; }, async send() { return { sid, status: 'queued' }; } } };
  const service = new SmsService(options);
  const start = await service.start(phone, client), proof = await service.check(start.challengeId, '123456', client);
  await service.enroll(proof.proofToken, client, true, SMS_DISCLOSURE_VERSION);
  service.inbound({ phone, sid, body: 'balance?' });
  await service.sendThreadReply(phone, sid, 'Portfolio value: $42.'); service.finishText(sid, 'replied');
  const signupSid = 'SM' + '3'.repeat(32);
  service.inbound({ phone: number, sid: signupSid, body: 'hello' });
  await service.sendSignupReply(number, signupSid); service.finishText(signupSid, 'replied');
  service.close();
  const retained = fs.readFileSync(statePath);
  assert.ok(JSON.parse(retained.toString()).consents.some((c: any) => c.signupReplyAttempted));
  const previous = await previousModule(dir, 'src/sms/SmsService.ts');
  assert.equal(JSON.parse(retained.toString()).inbox[0].state, 'replied');
  assert.throws(() => new previous.SmsService(options), (e: any) => e.name === 'ZodError'
    && e.issues.some((issue: any) => issue.code === 'invalid_value' && issue.path.join('.') === 'inbox.0.state'));
  const legacy = await previousModule(dir, 'src/broker/sms.ts');
  const configured: NodeJS.ProcessEnv = { DEXTER_SMS_STATE_PATH: statePath, DEXTER_SMS_SEND_ENABLED: 'false',
    DEXTER_SMS_ENROLLMENT_ENABLED: 'false', DEXTER_SMS_INTERNAL_SECRET: 's'.repeat(40), TWILIO_AUTH_TOKEN: 'test-token' };
  const rollbackEnv = Object.fromEntries(Object.entries(configured).filter(([key]) => !key.startsWith('DEXTER_SMS_')));
  const sms = await legacy.createBrokerSmsBridge(rollbackEnv, '/unused');
  assert.equal(sms, undefined);
  assert.deepEqual(fs.readFileSync(statePath), retained, 'rollback does not strip one-time markers or carrier reservations');
  assert.equal(fs.existsSync(statePath + '.lock'), false);
  const account = 'AC' + '4'.repeat(32), origin = 'https://phone.example.test';
  const config: BrokerConfig = { host: '127.0.0.1', port: 0, publicOrigin: origin, stateDir: path.join(dir, 'voice'),
    controlToken: 'c'.repeat(40), apiKey: 'fixture', model: 'fixture', voice: 'marin', twilioAccountSid: account,
    twilioAuthToken: 'test-token', from: number, allowedTo: [phone], apiBaseUrl: 'http://127.0.0.1:1',
    phoneServiceSecret: 'p'.repeat(40), maxCallSeconds: 60 };
  const oldServer = await previousModule(dir, 'src/broker/server.ts');
  broker = oldServer.createBrokerServer({ config, sms, dependencyHealth: async () => ({
    ok: true, checkedAt: new Date().toISOString(), primaryApi: { ok: true, releaseId: 'disposable-test-api' },
  }) });
  await new Promise<void>(resolve => broker!.server.listen(0, '127.0.0.1', resolve));
  const local = `http://127.0.0.1:${(broker.server.address() as { port: number }).port}`;
  assert.equal((await fetch(local + '/broker/health')).status, 200);
  const body = { AccountSid: account, CallSid: 'CA' + '5'.repeat(32), From: phone, To: number, Direction: 'inbound', CallStatus: 'ringing' };
  const response = await fetch(local + '/broker/incoming-call', { method: 'POST', headers: {
    'content-type': 'application/x-www-form-urlencoded',
    'x-twilio-signature': Twilio.getExpectedTwilioSignature('test-token', origin + '/broker/incoming-call', body),
  }, body: new URLSearchParams(body) });
  assert.equal(response.status, 200); assert.match(await response.text(), /<Response>/);
});
