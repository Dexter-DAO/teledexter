import type { IncomingMessage, ServerResponse } from 'node:http';
import Fastify from 'fastify';
import formBody from '@fastify/formbody';
import Twilio from 'twilio';
import { configureSms } from '../sms/configureSms.js';
import { registerSmsRoutes } from '../sms/registerSmsRoutes.js';

export interface BrokerSmsBridge {
  handle(request: IncomingMessage, response: ServerResponse): Promise<boolean>;
  stop(): void;
  close(): Promise<void>;
}

/** Preserve the existing SMS service and ledger as one explicitly configured writer. */
export async function createBrokerSmsBridge(env: NodeJS.ProcessEnv, releaseRoot: string,
  clients?: { verify: ReturnType<typeof Twilio>; messaging: ReturnType<typeof Twilio> },
): Promise<BrokerSmsBridge | undefined> {
  if (!Object.keys(env).some(key => key.startsWith('DEXTER_SMS_') && env[key])) return undefined;
  const service = configureSms(env, clients ?? {
    verify: Twilio(env.TWILIO_VERIFY_ACCOUNT_SID || env.TWILIO_ACCOUNT_SID,
      env.TWILIO_VERIFY_AUTH_TOKEN || env.TWILIO_AUTH_TOKEN, { autoRetry: false, timeout: 15_000 }),
    messaging: Twilio(env.TWILIO_ACCOUNT_SID, env.TWILIO_AUTH_TOKEN, { autoRetry: false, timeout: 15_000 }),
  }, releaseRoot);
  // Misconfiguration, unreadable state, or an existing writer must prevent listen/readiness.
  if (!service) throw new Error('sms_state_admission_failed');
  const app = Fastify({ logger: false });
  let accepting = true;
  const pending = new Set<Promise<void>>();
  try {
    await app.register(formBody);
    registerSmsRoutes(app, { service, internalSecret: env.DEXTER_SMS_INTERNAL_SECRET || '',
      twilioAuthToken: env.TWILIO_AUTH_TOKEN || '', twilioAccountSid: env.TWILIO_ACCOUNT_SID || '',
      incomingNumber: env.TWILIO_PHONE_NUMBER || '', messagingServiceSid: env.TWILIO_MESSAGING_SERVICE_SID || '',
      publicBaseUrl: (env.DEXTER_PHONE_PUBLIC_URL || '').replace(/\/+$/, ''),
    });
    await app.ready();
  } catch (error) { await app.close().catch(() => {}); service.close(); throw error; }
  function failure(response: ServerResponse, status: number, error: string) {
    response.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    response.end(JSON.stringify({ ok: false, error }));
  }
  async function dispatch(request: IncomingMessage, response: ServerResponse) {
    const limit = request.url?.startsWith('/internal/') ? 2048 : 16_384;
    const chunks: Buffer[] = []; let size = 0;
    // Broker's HTTP request timeout still bounds incomplete uploads. Injection lets
    // shutdown await the actual handler, including a provider request after disconnect.
    for await (const chunk of request) {
      size += chunk.length;
      if (size > limit) { failure(response, 413, 'invalid_request'); return; }
      chunks.push(Buffer.from(chunk));
    }
    const result = await app.inject({ method: request.method as 'GET' | 'POST' | 'HEAD', url: request.url || '/',
      headers: request.headers, payload: Buffer.concat(chunks),
    });
    if (!response.destroyed) { response.writeHead(result.statusCode, result.headers); response.end(result.rawPayload); }
  }
  return {
    async handle(request, response) {
      const pathname = (request.url || '').split('?')[0];
      if (!pathname.startsWith('/internal/sms/') && pathname !== '/sms/incoming' && !pathname.startsWith('/sms/status/')) return false;
      if (!accepting) { failure(response, 503, 'sms_unavailable'); return true; }
      if (!['GET', 'POST', 'HEAD'].includes(request.method || '')) { failure(response, 404, 'not_found'); return true; }
      const task = dispatch(request, response);
      pending.add(task);
      try { await task; } finally { pending.delete(task); }
      return true;
    },
    stop() { accepting = false; },
    async close() {
      accepting = false;
      await Promise.allSettled([...pending]);
      await app.close();
    },
  };
}
