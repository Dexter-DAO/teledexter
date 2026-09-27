import { randomBytes } from 'node:crypto';
import { z } from 'zod';
import { inboundCallEndedSchema, inboundStatusEndedSchema, inboundStatusReceiptSchema, type InboundStatusEnded, type InboundCallEnded } from './inboundBilling.js';
import { createPhoneApiCallbackSignature } from '../outbound/PhoneApiCallback.js';

const uuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i);
const eventId = z.string().regex(/^[A-Za-z0-9._:-]{8,160}$/);
const callSid = z.string().min(1).max(64);

export const HarkOutboundStatusBodySchema = z.object({
  intentId: uuid,
  eventId,
  callSid,
  state: z.enum(['reserved', 'dialing', 'queued', 'ringing', 'answered', 'streaming',
    'completed', 'failed', 'busy', 'no-answer', 'canceled', 'expired']),
}).strict();

export const HarkCallEndedBodySchema = z.object({
  source: z.literal('phone'),
  direction: z.literal('outbound-alert'),
  alert_intent_id: uuid,
  alert_event_id: eventId,
  external_sid: callSid,
  started_at: z.string().datetime({ offset: true }),
  ended_at: z.string().datetime({ offset: true }),
  duration_seconds: z.number().int().safe().nonnegative(),
  caller_user_id: uuid.optional(),
}).strict();

export const HarkCallEndedReceiptSchema = z.object({
  ok: z.literal(true),
  id: uuid,
  status: z.enum(['completed', 'short', 'silent', 'error', 'tool_failure']),
}).strict();

export type HarkOutboundStatusBody = Readonly<z.infer<typeof HarkOutboundStatusBodySchema>>;
export type HarkCallEndedBody = Readonly<z.infer<typeof HarkCallEndedBodySchema>>;
export type HarkCallEndedReceipt = Readonly<z.infer<typeof HarkCallEndedReceiptSchema>>;
export type HarkBillingDeliveryResult =
  | { state: 'delivered'; purpose: 'voice.outbound-status.v2'; httpStatus: 204 }
  | { state: 'delivered'; purpose: 'voice.call-ended.v2'; httpStatus: 200; receipt: HarkCallEndedReceipt | z.infer<typeof inboundStatusReceiptSchema> }
  | { state: 'pending'; reason: 'configuration_unavailable' | 'request_failed' | 'http_retry' | 'invalid_response'; httpStatus?: number }
  | { state: 'rejected'; reason: 'invalid_callback_body' | 'http_refusal'; httpStatus?: number };

export type HarkBillingDeliveryOptions = {
  apiBaseUrl: string;
  signingSecret: string;
  fetchImpl?: typeof fetch;
  now?: () => number;
  nonce?: () => string;
  timeoutMs?: number;
} & (
  | { kind: 'outbound-status'; body: HarkOutboundStatusBody }
  | { kind: 'call-ended'; body: HarkCallEndedBody }
  | { kind: 'inbound-call-ended'; body: InboundCallEnded }
  | { kind: 'inbound-status-ended'; body: InboundStatusEnded }
);

const MAX_RECEIPT_BYTES = 16 * 1024;

async function readResponseText(response: Response): Promise<string | undefined> {
  const length = response.headers.get('content-length');
  if (length && /^\d+$/.test(length) && Number(length) > MAX_RECEIPT_BYTES) {
    await response.body?.cancel().catch(() => {});
    return undefined;
  }
  if (!response.body) return '';
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const next = await reader.read();
      if (next.done) break;
      size += next.value.byteLength;
      if (size > MAX_RECEIPT_BYTES) {
        await reader.cancel().catch(() => {});
        return undefined;
      }
      chunks.push(next.value);
    }
    return Buffer.concat(chunks).toString('utf8');
  } catch { return undefined; }
  finally { reader.releaseLock(); }
}

/** One attempt with a saved body. The journal owns ordering, timestamps, and retries. */
export async function deliverHarkBilling(options: HarkBillingDeliveryOptions): Promise<HarkBillingDeliveryResult> {
  let encodedBody: string;
  let body: unknown;
  try {
    const schema = options.kind === 'outbound-status' ? HarkOutboundStatusBodySchema
      : options.kind === 'call-ended' ? HarkCallEndedBodySchema
      : options.kind === 'inbound-call-ended' ? inboundCallEndedSchema
      : options.kind === 'inbound-status-ended' ? inboundStatusEndedSchema : undefined;
    if (!schema || !schema.safeParse(options.body).success) {
      return { state: 'rejected', reason: 'invalid_callback_body' };
    }
    // Preserve saved strings and field order; never rederive financial receipt fields.
    encodedBody = JSON.stringify(options.body);
    body = JSON.parse(encodedBody) as unknown;
    if (!schema.safeParse(body).success) return { state: 'rejected', reason: 'invalid_callback_body' };
  } catch { return { state: 'rejected', reason: 'invalid_callback_body' }; }

  const path = options.kind === 'outbound-status' ? '/internal/voice/outbound-status' : '/internal/voice/call-ended';
  let url: string;
  let secret: string;
  let timestamp: string;
  let nonce: string;
  let timeoutMs: number;
  try {
    const base = new URL(options.apiBaseUrl);
    secret = options.signingSecret.trim();
    timeoutMs = options.timeoutMs ?? 5000;
    if (base.username || base.password || base.search || base.hash
      || (base.protocol !== 'https:' && !(base.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(base.hostname)))
      || secret.length < 32 || !Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 30_000) {
      return { state: 'pending', reason: 'configuration_unavailable' };
    }
    url = `${base.href.replace(/\/+$/, '')}${path}`;
    const now = (options.now ?? Date.now)();
    if (!Number.isFinite(now) || now < 0) return { state: 'pending', reason: 'configuration_unavailable' };
    timestamp = String(Math.floor(now / 1000));
    nonce = (options.nonce ?? (() => randomBytes(18).toString('base64url')))();
    if (!Number.isSafeInteger(Number(timestamp)) || !/^[A-Za-z0-9_-]{16,160}$/.test(nonce)) {
      return { state: 'pending', reason: 'configuration_unavailable' };
    }
  } catch { return { state: 'pending', reason: 'configuration_unavailable' }; }

  try {
    const response = await (options.fetchImpl ?? fetch)(url, {
      method: 'POST', redirect: 'error', signal: AbortSignal.timeout(timeoutMs),
      headers: {
        'content-type': 'application/json',
        'x-dexter-alert-timestamp': timestamp,
        'x-dexter-alert-nonce': nonce,
        'x-dexter-callback-signature': createPhoneApiCallbackSignature(secret, path, timestamp, nonce, body),
      },
      body: encodedBody,
    });
    const httpStatus = response.status;
    if (!response.ok) {
      await response.body?.cancel().catch(() => {});
      return [401, 403, 408, 429].includes(httpStatus) || httpStatus >= 500
        ? { state: 'pending', reason: 'http_retry', httpStatus }
        : { state: 'rejected', reason: 'http_refusal', httpStatus };
    }
    const text = await readResponseText(response);
    if (options.kind === 'outbound-status' && httpStatus === 204 && text === '') {
      return { state: 'delivered', purpose: 'voice.outbound-status.v2', httpStatus: 204 };
    }
    if ((options.kind === 'call-ended' || options.kind === 'inbound-call-ended' || options.kind === 'inbound-status-ended') && httpStatus === 200 && text !== undefined) {
      let parsed: unknown;
      try { parsed = JSON.parse(text) as unknown; } catch { /* Invalid receipt stays pending. */ }
      const receipt = (options.kind === 'inbound-status-ended' ? inboundStatusReceiptSchema : HarkCallEndedReceiptSchema).safeParse(parsed);
      if (receipt.success && (options.kind !== 'inbound-status-ended'
        || (receipt.data.id === options.body.phone_binding_id && receipt.data.status === options.body.provider_terminal.status))) {
        return { state: 'delivered', purpose: 'voice.call-ended.v2', httpStatus: 200, receipt: Object.freeze(receipt.data) };
      }
    }
    return { state: 'pending', reason: 'invalid_response', httpStatus };
  } catch { return { state: 'pending', reason: 'request_failed' }; }
}
