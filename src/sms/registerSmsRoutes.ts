import { createHash, timingSafeEqual } from 'node:crypto';
import type { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import Twilio from 'twilio';
import { z } from 'zod';
import { validateTwilioWebhook } from '../outbound/twilioWebhook.js';
import { SMS_DISCLOSURE_VERSION, SmsError, type SmsService } from './SmsService.js';

interface RouteOptions {
  service: SmsService | null;
  internalSecret: string;
  twilioAuthToken: string;
  twilioAccountSid: string;
  incomingNumber: string;
  messagingServiceSid: string;
  publicBaseUrl: string;
}
const digest = (value: string) => createHash('sha256').update(value).digest();
const token = z.string().min(1).max(128);
const startSchema = z.object({ phone: z.string().max(20) }).strict();
const checkSchema = z.object({ challengeId: z.string().uuid(), code: z.string().regex(/^\d{6}$/) }).strict();
const proofSchema = z.object({ proofToken: token }).strict();
const enrollSchema = z.object({ proofToken: token, consent: z.literal(true), disclosureVersion: z.literal(SMS_DISCLOSURE_VERSION) }).strict();

export function registerSmsRoutes(app: FastifyInstance, options: RouteOptions) {
  app.addHook('onClose', async () => options.service?.close());
  function authorize(request: FastifyRequest, needClient = true) {
    const supplied = request.headers['x-dexter-sms-internal-secret'];
    if (options.internalSecret.length < 32 || typeof supplied !== 'string' || !timingSafeEqual(digest(supplied), digest(options.internalSecret))) throw new SmsError('not_found', 404);
    const client = request.headers['x-dexter-sms-client-id'];
    if (needClient && (typeof client !== 'string' || !/^[0-9a-f]{64}$/.test(client))) throw new SmsError('invalid_request');
    return typeof client === 'string' ? client : '';
  }
  function service() {
    if (!options.service) throw new SmsError('sms_unavailable', 503);
    return options.service;
  }
  const errorResponse = (error: unknown, reply: FastifyReply) => {
    const e = error instanceof SmsError ? error : error instanceof z.ZodError ? new SmsError('invalid_request') : new SmsError('sms_unavailable', 503);
    if (e.retryAfterSeconds) reply.header('Retry-After', String(e.retryAfterSeconds));
    return reply.code(e.status).send({ ok: false, error: e.code, ...(e.retryAfterSeconds ? { retryAfterSeconds: e.retryAfterSeconds } : {}) });
  };
  app.get('/internal/sms/config', async (request, reply) => {
    reply.header('Cache-Control', 'no-store');
    try {
      authorize(request, false);
      return options.service?.config() ?? { ok: true, enrollmentAvailable: false, deliveryActive: false, disclosureVersion: SMS_DISCLOSURE_VERSION };
    } catch (e) { return errorResponse(e, reply); }
  });
  const actions = {
    '/internal/sms/verify/start': async (body: unknown, client: string) => service().start(startSchema.parse(body).phone, client),
    '/internal/sms/verify/check': async (body: unknown, client: string) => { const b = checkSchema.parse(body); return service().check(b.challengeId, b.code, client); },
    '/internal/sms/status': async (body: unknown, client: string) => ({ ok: true, status: service().status(proofSchema.parse(body).proofToken, client) }),
    '/internal/sms/enroll': async (body: unknown, client: string) => { const b = enrollSchema.parse(body); return { ok: true, status: await service().enroll(b.proofToken, client, b.consent, b.disclosureVersion) }; },
    '/internal/sms/withdraw': async (body: unknown, client: string) => ({ ok: true, status: service().withdraw(proofSchema.parse(body).proofToken, client) }),
  };
  for (const [url, action] of Object.entries(actions)) {
    app.post(url, { bodyLimit: 2048 }, async (request, reply) => {
      reply.header('Cache-Control', 'no-store');
      try {
        const client = authorize(request);
        if (!request.headers['content-type']?.startsWith('application/json')) throw new SmsError('invalid_request');
        return await action(request.body, client);
      } catch (e) { return errorResponse(e, reply); }
    });
  }
  function signed(request: FastifyRequest): Record<string, string> {
    if (!options.twilioAuthToken || !options.publicBaseUrl.startsWith('https://') || !request.headers['content-type']?.startsWith('application/x-www-form-urlencoded') || typeof request.headers['x-twilio-signature'] !== 'string') throw new SmsError('invalid_signature', 403);
    if (!validateTwilioWebhook(options.twilioAuthToken, options.publicBaseUrl, { headers: request.headers, rawUrl: request.raw.url || request.url, body: request.body })) throw new SmsError('invalid_signature', 403);
    const body = z.record(z.string(), z.string()).parse(request.body);
    if (body.AccountSid !== options.twilioAccountSid || !/^SM[0-9a-f]{32}$/i.test(body.MessageSid ?? '') || (body.MessagingServiceSid && body.MessagingServiceSid !== options.messagingServiceSid)) throw new SmsError('invalid_request');
    return body;
  }
  app.post('/sms/incoming', { bodyLimit: 16_384 }, async (request, reply) => {
    try {
      const b = signed(request);
      if (b.To !== options.incomingNumber || !b.From || !b.Body || b.Body.length > 1600 || (b.OptOutType && !['STOP', 'START', 'HELP'].includes(b.OptOutType))) throw new SmsError('invalid_request');
      const response = service().inbound({ phone: b.From, sid: b.MessageSid, body: b.Body, optOutType: b.OptOutType });
      const twiml = new Twilio.twiml.MessagingResponse();
      if (response) twiml.message(response);
      return reply.type('text/xml').send(twiml.toString());
    } catch (e) { return errorResponse(e, reply); }
  });
  app.post('/sms/status/:id', { bodyLimit: 16_384 }, async (request, reply) => {
    try {
      const b = signed(request);
      const { id } = request.params as { id: string };
      if (!/^[0-9a-f]{64}$/.test(id) || !b.To || !b.MessageStatus || (b.ErrorCode && !/^\d+$/.test(b.ErrorCode))) throw new SmsError('invalid_request');
      service().delivery(id, b.To, b.MessageSid, b.MessageStatus, b.ErrorCode || null);
      return reply.code(204).send();
    } catch (e) { return errorResponse(e, reply); }
  });
}
