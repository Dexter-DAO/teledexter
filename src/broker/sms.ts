import type { IncomingMessage, ServerResponse } from 'node:http';
import { Worker } from 'node:worker_threads';
import type Twilio from 'twilio';
import type { createCallerSmsSender } from '../sms/createSmsTool.js';
import { SmsError, type SmsService } from '../sms/SmsService.js';
import type { PrimarySmsClient } from './primaryApi.js';
import { safeFailure } from './phone.js';
import { smsRpc } from './smsRpc.js';

export interface BrokerSmsBridge {
  requestedEligibility(phone: string): ReturnType<SmsService['requestedEligibility']> | Promise<ReturnType<SmsService['requestedEligibility']>>;
  requestedReceipt(phone: string, callSid: string, message: string, messageId: string): ReturnType<SmsService['requestedReceipt']> | Promise<ReturnType<SmsService['requestedReceipt']>>;
  startAccountVerification?(ownerId: string, phone: string): Promise<Awaited<ReturnType<SmsService['start']>>>;
  checkAccountVerification?(ownerId: string, challengeId: string, code: string): Promise<{ ok: true; phone: string; verifiedAt: string }>;
  sendRequested(phone: string, callSid: string, message: string, expiresAt?: string): ReturnType<ReturnType<typeof createCallerSmsSender>>;
  handle(request: IncomingMessage, response: ServerResponse): Promise<boolean>;
  stop(): void;
  close(): Promise<void>;
}

/** Keep all ledger cloning, serialization and disk work off the voice event loop. */
export async function createBrokerSmsBridge(env: NodeJS.ProcessEnv, releaseRoot: string,
  clients?: { verify: ReturnType<typeof Twilio>; messaging: ReturnType<typeof Twilio> },
  primary?: PrimarySmsClient,
  createWorker?: (url: URL, options: ConstructorParameters<typeof Worker>[1]) => Worker,
): Promise<BrokerSmsBridge | undefined> {
  if (!Object.keys(env).some(key => key.startsWith('DEXTER_SMS_') && env[key])) return undefined;
  const source = import.meta.url.endsWith('.ts');
  const url = new URL(source ? './smsWorker.ts' : './smsWorker.js', import.meta.url);
  // Source runs register tsx inside the worker; built releases load plain JavaScript.
  const options = { env, workerData: { env, releaseRoot, proxyClients: !!clients, proxyPrimary: !!primary } };
  const worker = createWorker ? createWorker(url, options) : source
    ? new Worker(`require('tsx/cjs'); require(${JSON.stringify(url.pathname)});`, { ...options, eval: true })
    : new Worker(url, options);
  let accepting = true, dead = false, initialized = false, closing: Promise<void> | undefined;
  const pending = new Set<Promise<unknown>>();
  const rpc = smsRpc(worker, async (method, args) => {
    if ((method === 'turn' && !primary) || (['verifyStart', 'verifyCheck', 'send'].includes(method) && !clients))
      throw new SmsError('sms_unavailable', 503);
    switch (method) {
      case 'verifyStart': await clients!.verify.verify.v2.services(args[0]).verifications.create(args[1]); return;
      case 'verifyCheck': return { status: (await clients!.verify.verify.v2.services(args[0]).verificationChecks.create(args[1])).status };
      case 'send': { const message = await clients!.messaging.messages.create(args[0]); return { sid: message.sid, status: message.status }; }
      case 'turn': return primary!.turn(args[0]);
      default: throw new Error('invalid_sms_dependency');
    }
  });
  const failed = (error: Error) => {
    if (!dead && initialized && !closing) console.error(JSON.stringify({ service: 'sms', state: 'disabled', ...safeFailure(error) }));
    dead = true; accepting = false; rpc.fail(error);
  };
  worker.once('error', failed);
  worker.once('exit', () => failed(new SmsError('sms_unavailable', 503)));
  let startupTimer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([rpc.call('ready'), new Promise((_, reject) => {
      startupTimer = setTimeout(() => reject(new Error('sms_state_admission_failed')), 10_000);
    })]);
  } catch (error) { void worker.terminate(); throw error; }
  finally { clearTimeout(startupTimer); }
  initialized = true;
  function failure(response: ServerResponse, status: number, error: string) {
    if (response.destroyed) return;
    response.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    response.end(JSON.stringify({ ok: false, error }));
  }
  async function dispatch(request: IncomingMessage, response: ServerResponse) {
    const limit = request.url?.startsWith('/internal/') ? 2048 : 16_384;
    const chunks: Buffer[] = []; let size = 0;
    for await (const chunk of request) {
      size += chunk.length;
      if (size > limit) { failure(response, 413, 'invalid_request'); return; }
      chunks.push(Buffer.from(chunk));
    }
    try {
      const result = await rpc.call('http', { method: request.method, url: request.url || '/',
        headers: request.headers, payload: Buffer.concat(chunks) });
      if (!response.destroyed) {
        response.once('finish', () => { void rpc.call('drain').catch(() => {}); });
        response.writeHead(result.statusCode, result.headers); response.end(Buffer.from(result.payload));
      }
    } catch { failure(response, 503, 'sms_unavailable'); }
  }
  async function accountVerification<T>(method: string, ...args: string[]): Promise<T> {
    if (!accepting) throw new SmsError('sms_unavailable', 503);
    const task = rpc.call<T>(method, ...args);
    pending.add(task);
    try { return await task; } finally { pending.delete(task); }
  }
  return {
    startAccountVerification: (ownerId, phone) => accountVerification('accountStart', ownerId, phone),
    checkAccountVerification: (ownerId, challengeId, code) => accountVerification('accountCheck', ownerId, challengeId, code),
    requestedEligibility(phone) {
      if (!accepting) return Promise.reject(new SmsError('sms_unavailable', 503));
      return rpc.call('eligibility', phone);
    },
    requestedReceipt(phone, callSid, message, messageId) { return rpc.call('receipt', phone, callSid, message, messageId); },
    async sendRequested(phone, callSid, message, expiresAt) {
      if (!accepting) return { ok: false, error: 'sms_unavailable', enrollmentUrl: 'https://dexter.cash/sms' };
      const task = rpc.call('send', phone, callSid, message, expiresAt);
      pending.add(task);
      try { return await task; } finally { pending.delete(task); }
    },
    async handle(request, response) {
      const pathname = (request.url || '').split('?')[0];
      if (!pathname.startsWith('/internal/sms/') && pathname !== '/sms/incoming' && !pathname.startsWith('/sms/status/')) return false;
      if (!accepting) { failure(response, 503, 'sms_unavailable'); return true; }
      if (!['GET', 'POST', 'HEAD'].includes(request.method || '')) { failure(response, 404, 'not_found'); return true; }
      const task = dispatch(request, response); pending.add(task);
      try { await task; } finally { pending.delete(task); }
      return true;
    },
    stop() { accepting = false; void rpc.call('stop').catch(() => {}); },
    close() {
      return closing ??= (async () => {
        accepting = false;
        if (dead) return;
        try {
          await rpc.call('stop');
          await Promise.allSettled([...pending]);
          if (!dead) await rpc.call('close');
        } catch (error) { if (!dead) throw error; }
        finally { await worker.terminate(); }
      })();
    },
  };
}

/** A broken SMS dependency must leave the voice broker available. */
export async function startBrokerSmsBridge(...args: Parameters<typeof createBrokerSmsBridge>) {
  try { return await createBrokerSmsBridge(...args); }
  catch (error) {
    console.error(JSON.stringify({ service: 'sms', state: 'disabled', ...safeFailure(error) }));
    return undefined;
  }
}
