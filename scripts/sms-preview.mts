// Isolated UI contract harness. Uses no environment credentials and never contacts Twilio.
import Fastify from 'fastify';
import formBody from '@fastify/formbody';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { SmsService } from '../src/sms/SmsService.js';
import { registerSmsRoutes } from '../src/sms/registerSmsRoutes.js';

const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'dexter-sms-preview-'));
const service = new SmsService({
  statePath: path.join(directory, 'sms.json'),
  enrollmentEnabled: true, sendEnabled: false, campaignApproved: false, advancedOptOutConfigured: false,
  messagingServiceSid: '', publicBaseUrl: 'https://sms-preview.invalid',
  provider: {
    startVerification: async () => {},
    checkVerification: async (_phone, code) => code === '123456',
    send: async () => { throw new Error('preview_cannot_send'); },
  },
});
const app = Fastify({ logger: false });
app.register(formBody);
registerSmsRoutes(app, {
  service, internalSecret: 'sms-preview-secret-never-use-in-production-2026',
  twilioAuthToken: '', twilioAccountSid: '', incomingNumber: '', messagingServiceSid: '',
  publicBaseUrl: 'https://sms-preview.invalid',
});
app.addHook('onClose', async () => { fs.rmSync(directory, { recursive: true, force: true }); });
for (const signal of ['SIGINT', 'SIGTERM'] as const) process.once(signal, () => { void app.close(); });
await app.listen({ host: '127.0.0.1', port: 34118 });
console.log('SMS preview backend on 127.0.0.1:34118. Fixed test code 123456. Provider calls disabled.');
