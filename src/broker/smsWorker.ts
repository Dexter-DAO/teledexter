import { parentPort, workerData } from 'node:worker_threads';
import { createHmac } from 'node:crypto';
import Fastify from 'fastify';
import formBody from '@fastify/formbody';
import Twilio from 'twilio';
import { configureSms } from '../sms/configureSms.js';
import { registerSmsRoutes } from '../sms/registerSmsRoutes.js';
import { createCallerSmsSender } from '../sms/createSmsTool.js';
import { SmsError } from '../sms/SmsService.js';
import { createPrimarySmsClient, type PrimarySmsClient } from './primaryApi.js';
import { dispatchSmsInbox } from '../sms/dispatch.js';
import { safeFailure } from './phone.js';
import { smsRpc } from './smsRpc.js';

const { env, releaseRoot, proxyClients, proxyPrimary } = workerData as {
  env: NodeJS.ProcessEnv; releaseRoot: string; proxyClients: boolean; proxyPrimary: boolean;
};
let accepting = true;
const pending = new Set<Promise<unknown>>();
let draining: Promise<void> | undefined;
let timer: ReturnType<typeof setInterval> | undefined;
const app = Fastify({ logger: false });
const rpc = smsRpc(parentPort!, async (method, args) => {
  if (method === 'ready') return ready;
  await ready;
  if (method === 'stop') { accepting = false; clearInterval(timer); return; }
  if (method === 'close') {
    accepting = false; clearInterval(timer);
    await Promise.allSettled([...pending]);
    await draining;
    await app.close();
    return;
  }
  if (method === 'drain') { drain(); return; }
  if (method === 'receipt') return service!.requestedReceipt(args[0], args[1], args[2], args[3]);
  if (!accepting) throw new SmsError('sms_unavailable', 503);
  if (method === 'eligibility') return service!.requestedEligibility(args[0]);
  // A request can wait behind another ledger commit after the parent's deadline check.
  if (method === 'send' && args[3] !== undefined && Date.parse(args[3]) <= Date.now())
    return { ok: false, error: 'sms_intent_expired', enrollmentUrl: 'https://dexter.cash/sms' };
  const task = method === 'accountStart' ? service!.start(args[1], accountClient(args[0]))
    : method === 'accountCheck' ? (async () => {
      const client = accountClient(args[0]);
      const saved = service!.verifiedChallenge(args[1], client);
      if (saved) return { ok: true, ...saved };
      const checked = await service!.check(args[1], args[2], client);
      return { ok: true, ...service!.verifiedPhone(checked.proofToken, client) };
    })()
    : method === 'send' ? createCallerSmsSender(service, args[0], args[1])(args[2])
    : method === 'http' ? app.inject({ ...args[0], payload: Buffer.from(args[0].payload) }).then(result => ({
      statusCode: result.statusCode, headers: result.headers, payload: result.rawPayload,
    })) : Promise.reject(new Error('invalid_sms_method'));
  pending.add(task);
  try { return await task; } finally { pending.delete(task); }
});
const clients = proxyClients ? {
  verify: { verify: { v2: { services: (sid: string) => ({
    verifications: { create: (input: unknown) => rpc.call('verifyStart', sid, input) },
    verificationChecks: { create: (input: unknown) => rpc.call('verifyCheck', sid, input) },
  }) } } },
  messaging: { messages: { create: (input: unknown) => rpc.call('send', input) } },
} as unknown as { verify: ReturnType<typeof Twilio>; messaging: ReturnType<typeof Twilio> } : {
  verify: Twilio(env.TWILIO_VERIFY_ACCOUNT_SID || env.TWILIO_ACCOUNT_SID,
    env.TWILIO_VERIFY_AUTH_TOKEN || env.TWILIO_AUTH_TOKEN, { autoRetry: false, timeout: 15_000 }),
  messaging: Twilio(env.TWILIO_ACCOUNT_SID, env.TWILIO_AUTH_TOKEN, { autoRetry: false, timeout: 15_000 }),
};
const service = configureSms(env, clients, releaseRoot);
const primary: PrimarySmsClient | undefined = proxyPrimary ? { turn: input => rpc.call('turn', input) }
  : env.DEXTER_API_URL && env.PHONE_SERVICE_SECRET
    ? createPrimarySmsClient({ apiBaseUrl: env.DEXTER_API_URL, phoneServiceSecret: env.PHONE_SERVICE_SECRET }) : undefined;
function accountClient(ownerId: string) {
  const token = env.BROKER_CONTROL_TOKEN?.trim();
  if (!token || token.length < 32) throw new SmsError('sms_unavailable', 503);
  return createHmac('sha256', token).update(`sms-account:${ownerId}`).digest('hex');
}
function drain() {
  if (!accepting || draining || pending.size || !primary) return;
  const failure = (error: unknown) => console.error(JSON.stringify({ service: 'sms', event: 'dispatch_failed', ...safeFailure(error) }));
  draining = dispatchSmsInbox(service!, primary, failure).catch(failure).finally(() => { draining = undefined; });
}
const ready = (async () => {
  if (!service) throw new Error('sms_state_admission_failed');
  try {
    await app.register(formBody);
    registerSmsRoutes(app, { service, internalSecret: env.DEXTER_SMS_INTERNAL_SECRET || '',
      twilioAuthToken: env.TWILIO_AUTH_TOKEN || '', twilioAccountSid: env.TWILIO_ACCOUNT_SID || '',
      incomingNumber: env.TWILIO_PHONE_NUMBER || '', messagingServiceSid: env.TWILIO_MESSAGING_SERVICE_SID || '',
      publicBaseUrl: (env.DEXTER_PHONE_PUBLIC_URL || '').replace(/\/+$/, ''),
    });
    await app.ready();
    timer = setInterval(drain, 2000); timer.unref();
    setImmediate(drain);
  } catch (error) { await app.close().catch(() => {}); service.close(); throw error; }
})();
// The ready RPC owns reporting; avoid an unhandled rejection before its message arrives.
void ready.catch(() => {});
