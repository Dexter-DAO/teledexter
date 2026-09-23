import { randomBytes } from 'node:crypto';
import { z } from 'zod';
import { createSignedAlertSignature } from '../outbound/auth.js';
import { canonicalJson, sha256Hex } from '../outbound/canonical.js';
import {
  HarkPhoneDeliveryStatusSchema,
  HarkPhoneStatusCallbackResultSchema,
  type HarkPhoneDeliveryStatus,
  type HarkPhoneBinding,
} from '../outbound/contract.js';

export type HarkStatusBody = Readonly<HarkPhoneDeliveryStatus>;
export type HarkStatusInput = Omit<HarkPhoneDeliveryStatus, 'v' | 'sourceEventId'>;
export type HarkStatusReceipt = Readonly<z.infer<typeof HarkPhoneStatusCallbackResultSchema>>;
export interface HarkStatusEvent {
  readonly sourceId: string;
  readonly bodyDigest: string;
  readonly body: HarkStatusBody;
}
export interface HarkStatusBinding {
  intentId: string;
  callSid: string;
  hark: HarkPhoneBinding;
}
export type HarkStatusDeliveryResult =
  | { state: 'delivered'; httpStatus: number; receipt: HarkStatusReceipt }
  | { state: 'pending'; reason: 'configuration_unavailable' | 'request_failed' | 'http_retry' | 'invalid_response'; httpStatus?: number }
  | { state: 'rejected'; reason: 'invalid_status_body' | 'identity_mismatch' | 'http_refusal'; httpStatus?: number };
export interface HarkStatusDeliveryOptions {
  apiBaseUrl: string;
  signingSecret: string;
  body: HarkStatusBody;
  fetchImpl?: typeof fetch;
  now?: () => number;
  nonce?: () => string;
  timeoutMs?: number;
}

function sourceId(body: HarkStatusInput): string {
  // Keep the existing Hark status identity; observation time belongs to the saved body.
  return `phone-status:${sha256Hex(canonicalJson({
    caseId: body.caseId, deliveryId: body.deliveryId, intentId: body.intentId,
    callSid: body.callSid, originTaskId: body.originTaskId, status: body.status,
  }))}`;
}

/** Save this event with the call transition before sending. Reuse its body on every retry. */
export function createHarkStatusEvent(input: HarkStatusInput): HarkStatusEvent {
  const parsed = HarkPhoneDeliveryStatusSchema.safeParse({ ...input,
    v: 'hark.phone-status.v1', sourceEventId: sourceId(input) });
  if (!parsed.success) throw new Error('invalid_hark_status_input');
  const body = Object.freeze(parsed.data);
  return Object.freeze({ sourceId: body.sourceEventId, bodyDigest: sha256Hex(canonicalJson(body)), body });
}

export function buildHarkStatusBody(
  binding: HarkStatusBinding,
  status: HarkPhoneDeliveryStatus['status'],
  observedAt: string,
): HarkStatusBody {
  return createHarkStatusEvent({
    caseId: binding.hark.caseId, eventId: binding.hark.eventId,
    deliveryId: binding.hark.deliveryId, originTaskId: binding.hark.originTaskId,
    intentId: binding.intentId, callSid: binding.callSid, status, observedAt,
  }).body;
}

const RECEIPT_BINDING_FIELDS = [
  'caseId', 'eventId', 'deliveryId', 'intentId', 'callSid', 'originTaskId', 'sourceEventId',
] as const;
const MAX_RECEIPT_BYTES = 16 * 1024;

async function readReceipt(response: Response): Promise<unknown> {
  const length = response.headers.get('content-length');
  if (length && /^\d+$/.test(length) && Number(length) > MAX_RECEIPT_BYTES) {
    await response.body?.cancel().catch(() => {});
    return undefined;
  }
  if (!response.body) return undefined;
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
    return JSON.parse(Buffer.concat(chunks).toString('utf8')) as unknown;
  } catch { return undefined; }
  finally { reader.releaseLock(); }
}

/** One status HTTP attempt. The caller owns durable storage and retry scheduling. */
export async function deliverHarkStatus(options: HarkStatusDeliveryOptions): Promise<HarkStatusDeliveryResult> {
  const parsed = HarkPhoneDeliveryStatusSchema.safeParse(options.body);
  if (!parsed.success || parsed.data.sourceEventId !== sourceId(parsed.data)) {
    return { state: 'rejected', reason: 'invalid_status_body' };
  }
  // Zod creates a detached snapshot before an asynchronous caller can mutate its input.
  const body = parsed.data;
  let url: string;
  let timestamp: string;
  let nonce: string;
  let timeoutMs: number;
  try {
    const base = new URL(options.apiBaseUrl);
    timeoutMs = options.timeoutMs ?? 3000;
    if (base.username || base.password || base.search || base.hash
      || (base.protocol !== 'https:' && !(base.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(base.hostname)))
      || typeof options.signingSecret !== 'string' || options.signingSecret.length < 32
      || !Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 30_000) {
      return { state: 'pending', reason: 'configuration_unavailable' };
    }
    url = `${base.href.replace(/\/+$/, '')}/internal/hark/v1/deliveries/${encodeURIComponent(body.deliveryId)}/status`;
    const now = (options.now ?? Date.now)();
    if (!Number.isFinite(now) || now < 0) return { state: 'pending', reason: 'configuration_unavailable' };
    timestamp = String(Math.floor(now / 1000));
    nonce = (options.nonce ?? (() => randomBytes(18).toString('base64url')))();
    if (!/^\d{10}$/.test(timestamp) || !/^[A-Za-z0-9_-]{16,160}$/.test(nonce)) {
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
        'x-dexter-alert-signature': createSignedAlertSignature(options.signingSecret, timestamp, nonce, body),
      },
      body: JSON.stringify(body),
    });
    const httpStatus = response.status;
    if (!response.ok) {
      await response.body?.cancel().catch(() => {});
      return [401, 403, 408, 429].includes(httpStatus) || httpStatus >= 500
        ? { state: 'pending', reason: 'http_retry', httpStatus }
        : { state: 'rejected', reason: 'http_refusal', httpStatus };
    }
    const receipt = HarkPhoneStatusCallbackResultSchema.safeParse(await readReceipt(response));
    if (!receipt.success) return { state: 'pending', reason: 'invalid_response', httpStatus };
    if (RECEIPT_BINDING_FIELDS.some(key => receipt.data[key] !== body[key])) {
      return { state: 'rejected', reason: 'identity_mismatch', httpStatus };
    }
    return { state: 'delivered', httpStatus, receipt: Object.freeze(receipt.data) };
  } catch { return { state: 'pending', reason: 'request_failed' }; }
}
