import { SmsError } from '../sms/SmsService.js';
import { PrimaryApiError } from './primaryApi.js';
import type { ResultSmsClient } from './resultSmsClient.js';
import { phoneResultSmsDispatchRefSchema, phoneResultSmsResolvedSchema, phoneResultSmsReceiptSchema,
  type PhoneResultSmsResolved } from './resultSmsContract.js';
import type { BrokerSmsBridge } from './sms.js';

const safeErrors = new Set(['sms_delivery_pending', 'sms_consent_required', 'carrier_opted_out',
  'sms_intent_expired', 'sms_authority_revoked', 'sms_number_changed', 'sms_unsupported_number',
  'sms_result_unavailable', 'sms_intent_unavailable', 'sms_dispatch_uncertain', 'rate_limited', 'sms_unavailable']);
export function resultSmsFailure(error: unknown): { ok: false; error: string } {
  const code = error instanceof SmsError ? error.code : error instanceof PrimaryApiError ? error.remoteCode : undefined;
  return { ok: false, error: code && safeErrors.has(code) ? code : 'sms_unavailable' };
}

/** Recovery is read-only and precedes every current-authority or send-availability check. */
export async function dispatchResultSms(value: unknown, options: {
  client: ResultSmsClient; sms?: BrokerSmsBridge; accepting(): boolean; now?: () => number;
}) {
  const parsed = phoneResultSmsDispatchRefSchema.safeParse(value);
  if (!parsed.success) return { ok: false as const, error: 'invalid_request' };
  const ref = parsed.data;
  const validate = (value: unknown): PhoneResultSmsResolved => {
    const resolved = phoneResultSmsResolvedSchema.parse(value);
    if (resolved.identity.intentId !== ref.intentId || resolved.intentDigest !== ref.intentDigest
      || resolved.payload.messageId !== ref.messageId) throw new Error('sms_identity_mismatch');
    return resolved;
  };
  try {
    const resolved = validate(await options.client.resolveIdentity(ref));
    const { identity, payload } = resolved;
    if (!options.sms) return { ok: false as const, error: 'sms_delivery_pending' };
    const receipt = () => {
      const value = options.sms!.requestedReceipt(identity.phoneNumber, identity.callSid, payload.message, ref.messageId);
      if (!value) return null;
      const result = phoneResultSmsReceiptSchema.parse(value);
      if (result.messageId !== ref.messageId) throw new Error('sms_receipt_mismatch');
      return result;
    };
    const existing = receipt();
    if (existing) return existing;
    if (!options.accepting()) return { ok: false as const, error: 'sms_unavailable' };
    const expired = () => Date.parse(identity.expiresAt) <= (options.now ?? Date.now)();
    if (expired()) return { ok: false as const, error: 'sms_intent_expired' };
    let admitted: PhoneResultSmsResolved;
    try { admitted = validate(await options.client.admitDispatch(ref)); }
    catch (error) {
      // Another exact attempt may have reserved while this admission was in
      // flight. Its receipt still wins if authority was subsequently revoked.
      const raced = receipt();
      if (raced) return raced;
      throw error;
    }
    // No refreshed destination, body, deadline or answer may replace the frozen one.
    if (JSON.stringify(admitted) !== JSON.stringify(resolved)) throw new Error('sms_identity_mismatch');
    const raced = receipt();
    if (raced) return raced;
    if (!options.accepting()) return { ok: false as const, error: 'sms_unavailable' };
    if (expired()) return { ok: false as const, error: 'sms_intent_expired' };
    options.sms.requestedEligibility(identity.phoneNumber);
    // SmsService fsyncs the same key before carrier I/O. Concurrent and delayed
    // attempts observe dispatching/uncertain rather than making another send.
    const sent = await options.sms.sendRequested(identity.phoneNumber, identity.callSid, payload.message);
    if (!sent.ok) {
      // An uncertain transport or carrier refusal can already have a durable receipt.
      const saved = receipt();
      if (saved) return saved;
      return { ok: false as const, error: 'error' in sent && safeErrors.has(sent.error) ? sent.error : 'sms_unavailable' };
    }
    const saved = phoneResultSmsReceiptSchema.parse(sent);
    if (saved.messageId !== ref.messageId) throw new Error('sms_receipt_mismatch');
    return saved;
  } catch (error) { return resultSmsFailure(error); }
}
