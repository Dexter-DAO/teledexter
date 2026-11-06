import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { Worker } from 'node:worker_threads';
import { SmsService, SMS_DISCLOSURE_VERSION } from '../src/sms/SmsService.js';
import { createBrokerSmsBridge, type BrokerSmsBridge } from '../src/broker/sms.js';

for (const stopDuringStartup of [true, false]) {
  test(`saved inbox ${stopDuringStartup ? 'stays paused when shutdown begins' : 'drains after admission'} during SMS startup`, async t => {
    const dir = fs.mkdtempSync(path.join(os.homedir(), '.local/state/sms-startup-stop-'));
    const statePath = path.join(dir, 'sms.json'), phone = '+15555550123', sid = 'SM' + '2'.repeat(32);
    const provider = { async startVerification() {}, async checkVerification() { return true; },
      async send(): Promise<never> { throw new Error('unexpected_carrier_send'); } };
    const service = new SmsService({ statePath, enrollmentEnabled: true, sendEnabled: true,
      campaignApproved: true, advancedOptOutConfigured: true, messagingServiceSid: 'MG' + '4'.repeat(32),
      publicBaseUrl: 'https://phone.example.test', provider });
    const challenge = await service.start(phone, 'a'.repeat(64));
    const proof = await service.check(challenge.challengeId, '123456', 'a'.repeat(64));
    await service.enroll(proof.proofToken, 'a'.repeat(64), true, SMS_DISCLOSURE_VERSION);
    service.inbound({ phone, sid, body: 'Balance?' }); service.close();
    const before = fs.readFileSync(statePath, 'utf8');
    const env: NodeJS.ProcessEnv = { DEXTER_SMS_STATE_PATH: statePath, DEXTER_SMS_INTERNAL_SECRET: 's'.repeat(40),
      DEXTER_SMS_ENROLLMENT_ENABLED: 'true', DEXTER_SMS_SEND_ENABLED: 'true', DEXTER_SMS_CAMPAIGN_APPROVED: 'true',
      DEXTER_SMS_ADVANCED_OPT_OUT_CONFIGURED: 'true', TWILIO_VERIFY_SID: 'VA' + '3'.repeat(32),
      TWILIO_ACCOUNT_SID: 'AC' + '1'.repeat(32), TWILIO_AUTH_TOKEN: 'fake', TWILIO_PHONE_NUMBER: '+15555550124',
      TWILIO_MESSAGING_SERVICE_SID: 'MG' + '4'.repeat(32), DEXTER_PHONE_PUBLIC_URL: 'https://phone.example.test' };
    let bridge: BrokerSmsBridge | undefined, turns = 0, sends = 0;
    const clients = { verify: { verify: { v2: { services: () => ({
      verifications: { async create() {} }, verificationChecks: { async create() { return { status: 'approved' }; } },
    }) } } }, messaging: { messages: { async create() { sends++; throw new Error('unexpected_carrier_send'); } } } } as any;
    t.after(async () => { await bridge?.close(); fs.rmSync(dir, { recursive: true, force: true }); });
    bridge = await createBrokerSmsBridge(env, path.join(dir, 'release'), clients, {
      async turn(input) { assert.equal(input.messageSid, sid); turns++; return { state: 'running' }; },
    }, (url, options) => {
      const worker = new Worker(`require('tsx/cjs'); require(${JSON.stringify(url.pathname)});`, { ...options, execArgv: [], eval: true });
      const on = worker.on;
      worker.on = function(event: string, listener: (...args: any[]) => void) {
        return on.call(this, event, event === 'message' ? (message: any) => {
          // Let the worker finish admission while the parent is still waiting for its ready reply.
          if (message.id === 1 && !message.method) setTimeout(() => listener(message), 100);
          else listener(message);
        } : listener);
      } as typeof worker.on;
      return worker;
    });
    assert.ok(bridge);
    // This is main.ts's pending shutdown check as soon as startup returns its bridge.
    if (stopDuringStartup) bridge.stop();
    else {
      for (let i = 0; i < 100 && !turns; i++) await new Promise(resolve => setTimeout(resolve, 10));
    }
    await bridge.close();
    assert.equal(turns, stopDuringStartup ? 0 : 1);
    assert.equal(sends, 0);
    const saved = JSON.parse(fs.readFileSync(statePath, 'utf8'));
    assert.equal(saved.inbox[0].sid, sid); assert.equal(saved.inbox[0].state, 'pending_dispatch');
    assert.equal(fs.readFileSync(statePath, 'utf8'), before, 'no pending text, send reservation or retry was changed');
  });
}
