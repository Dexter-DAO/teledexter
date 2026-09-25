import { randomBytes } from 'node:crypto';
import { z } from 'zod';
import { createPhoneApiCallbackSignature } from '../outbound/PhoneApiCallback.js';
import { HarkCallEndedReceiptSchema, HarkOutboundStatusBodySchema, deliverHarkBilling } from './harkBillingDelivery.js';
import type { PhoneVerificationCallbackPermit } from './verificationCallbackContract.js';
const timestamp = z.string().datetime().refine(value => new Date(value).toISOString() === value);
export const callbackEndedTimingSchema = z.object({ startedAt: timestamp, endedAt: timestamp,
  durationSeconds: z.number().int().safe().nonnegative() }).strict().refine(value => Date.parse(value.endedAt) >= Date.parse(value.startedAt)
    && value.durationSeconds <= Math.ceil((Date.parse(value.endedAt) - Date.parse(value.startedAt)) / 1000));
export function verificationCallbackProviderTiming(raw: unknown): CallbackEndedTiming | undefined {
  if (!raw || typeof raw !== 'object') return;
  const value = raw as { status?: unknown; startTime?: unknown; endTime?: unknown; duration?: unknown };
  if (!['completed', 'busy', 'failed', 'no-answer', 'canceled'].includes(String(value.status))
    || !(value.startTime instanceof Date) || !(value.endTime instanceof Date) || typeof value.duration !== 'string' || !/^\d+$/.test(value.duration)) return;
  try { const parsed = callbackEndedTimingSchema.safeParse({ startedAt: value.startTime.toISOString(), endedAt: value.endTime.toISOString(), durationSeconds: Number(value.duration) });
    return parsed.success ? parsed.data : undefined; } catch { return; }
}
export type CallbackEndedTiming = z.output<typeof callbackEndedTimingSchema>;
export const callbackEndedBodySchema = z.object({ source: z.literal('phone'), direction: z.literal('outbound-verification'),
  purpose: z.literal('phone_verification_v1'), alert_intent_id: z.string().uuid(), alert_event_id: z.string(),
  external_sid: z.string().regex(/^CA[0-9a-fA-F]{32}$/), started_at: timestamp, ended_at: timestamp,
  duration_seconds: z.number().int().safe().nonnegative() }).strict();
export const callbackReceiptSchema = z.discriminatedUnion('kind', [
  z.object({ kind: z.literal('status'), body: HarkOutboundStatusBodySchema, acknowledgedAt: timestamp.optional() }).strict(),
  z.object({ kind: z.literal('ended'), body: callbackEndedBodySchema, acknowledgedAt: timestamp.optional(),
    receipt: HarkCallEndedReceiptSchema.optional() }).strict(),
]);
export type CallbackVoiceReceipt = z.output<typeof callbackReceiptSchema>;
export function validateCallbackReceipts(permit: PhoneVerificationCallbackPermit, sid: string | undefined,
  next: CallbackVoiceReceipt[], previous: CallbackVoiceReceipt[] = []): void {
  if (next.length > 9 || next.length < previous.length) throw new Error('callback_receipt_changed');
  const keys = new Set<string>();
  next.forEach((raw, index) => {
    const value = callbackReceiptSchema.parse(raw), key = value.kind === 'status' ? value.body.state : 'ended';
    if (keys.has(key)) throw new Error('callback_receipt_changed'); keys.add(key);
    if (value.kind === 'status' ? value.body.intentId !== permit.billing.intentId || value.body.eventId !== permit.billing.eventId
      || value.body.callSid !== sid : value.body.alert_intent_id !== permit.billing.intentId || value.body.alert_event_id !== permit.billing.eventId
      || value.body.external_sid !== sid) throw new Error('callback_receipt_identity_changed');
    if (value.kind === 'ended') {
      callbackEndedTimingSchema.parse({ startedAt: value.body.started_at, endedAt: value.body.ended_at, durationSeconds: value.body.duration_seconds });
      if (!!value.acknowledgedAt !== !!value.receipt) throw new Error('callback_receipt_changed');
    }
    const old = previous[index];
    if (old && (old.kind !== value.kind || JSON.stringify(old.body) !== JSON.stringify(value.body)
      || (old.acknowledgedAt !== undefined && JSON.stringify(old) !== JSON.stringify(value)))) throw new Error('callback_receipt_changed');
  });
}
export function callbackStatusReceipt(permit: PhoneVerificationCallbackPermit, callSid: string, status: string): CallbackVoiceReceipt {
  return callbackReceiptSchema.parse({ kind: 'status', body: { intentId: permit.billing.intentId, eventId: permit.billing.eventId,
    callSid, state: status === 'in-progress' ? 'answered' : status } });
}
export function callbackEndedReceipt(permit: PhoneVerificationCallbackPermit, callSid: string, timing: CallbackEndedTiming): CallbackVoiceReceipt {
  callbackEndedTimingSchema.parse(timing);
  return callbackReceiptSchema.parse({ kind: 'ended', body: { source: 'phone', direction: 'outbound-verification', purpose: 'phone_verification_v1',
    alert_intent_id: permit.billing.intentId, alert_event_id: permit.billing.eventId, external_sid: callSid,
    started_at: timing.startedAt, ended_at: timing.endedAt, duration_seconds: timing.durationSeconds } });
}
/** Same durable body on every attempt. Authentication receives a fresh nonce/time. */
export async function deliverCallbackReceipt(options: { apiBaseUrl: string; signingSecret: string; value: CallbackVoiceReceipt;
  fetchImpl?: typeof fetch; now?: () => number; nonce?: () => string; timeoutMs?: number }): Promise<CallbackVoiceReceipt | undefined> {
  const value = callbackReceiptSchema.parse(options.value);
  if (value.acknowledgedAt) return value;
  const base = new URL(options.apiBaseUrl), timeout = options.timeoutMs ?? 5000;
  if (base.protocol !== 'http:' || !['127.0.0.1', 'localhost', '[::1]'].includes(base.hostname) || base.pathname !== '/'
    || base.username || base.password || base.search || base.hash || options.signingSecret.trim().length < 32
    || !Number.isSafeInteger(timeout) || timeout < 1 || timeout > 30000) throw new Error('callback_receipt_configuration_invalid');
  const now = options.now ?? Date.now;
  if (value.kind === 'status') {
    const result = await deliverHarkBilling({ kind: 'outbound-status', body: value.body, apiBaseUrl: options.apiBaseUrl,
      signingSecret: options.signingSecret, timeoutMs: timeout, fetchImpl: options.fetchImpl, now, nonce: options.nonce });
    return result.state === 'delivered' ? { ...value, acknowledgedAt: new Date(now()).toISOString() } : undefined;
  }
  const path = '/internal/voice/call-ended', timestamp = String(Math.floor(now() / 1000)), nonce = (options.nonce ?? (() => randomBytes(18).toString('base64url')))();
  if (!/^\d+$/.test(timestamp) || !/^[A-Za-z0-9_-]{16,160}$/.test(nonce)) throw new Error('callback_receipt_configuration_invalid');
  try {
    const response = await (options.fetchImpl ?? fetch)(base.origin + path, { method: 'POST', redirect: 'error', signal: AbortSignal.timeout(timeout),
      headers: { 'content-type': 'application/json', 'x-dexter-alert-timestamp': timestamp, 'x-dexter-alert-nonce': nonce,
        'x-dexter-callback-signature': createPhoneApiCallbackSignature(options.signingSecret.trim(), path, timestamp, nonce, value.body) },
      body: JSON.stringify(value.body) });
    if (response.status !== 200 || !/^application\/json(?:\s*;.*)?$/i.test(response.headers.get('content-type') ?? '')) { await response.body?.cancel(); return; }
    const reader = response.body?.getReader(); if (!reader) return;
    const chunks: Uint8Array[] = []; let bytes = 0;
    try { for (;;) { const chunk = await reader.read(); if (chunk.done) break; bytes += chunk.value.byteLength;
      if (bytes > 16384) { await reader.cancel(); return; } chunks.push(chunk.value); } } finally { reader.releaseLock(); }
    const receipt = HarkCallEndedReceiptSchema.safeParse(JSON.parse(Buffer.concat(chunks).toString('utf8')));
    return receipt.success ? { ...value, receipt: receipt.data, acknowledgedAt: new Date(now()).toISOString() } : undefined;
  } catch { return; }
}
