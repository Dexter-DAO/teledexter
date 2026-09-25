import { z } from 'zod';
import { authorizedControl } from './phone.js';
import { phoneVerificationCallbackRefSchema, type PhoneVerificationCallbackRef } from './verificationCallbackContract.js';

/** API server sends only this saved reference; browser and model are not callers. */
export const VERIFICATION_CALLBACK_KICKOFF_PATH = '/internal/broker/verification-callbacks/dispatch';
export const verificationCallbackKickoffSchema = phoneVerificationCallbackRefSchema;
export const verificationCallbackKickoffResultSchema = z.object({
  v: z.literal(1), request: phoneVerificationCallbackRefSchema,
  state: z.enum(['reserved', 'dispatching', 'answered', 'completed', 'cancelled', 'expired', 'absent']),
}).strict();
export type VerificationCallbackKickoffResult = z.output<typeof verificationCallbackKickoffResultSchema>;
export interface VerificationCallbackKickoffPort {
  kickoff(ref: PhoneVerificationCallbackRef): Promise<VerificationCallbackKickoffResult>;
}
export const verificationCallbackKickoffErrorSchema = z.object({ error: z.object({
  code: z.enum(['callback_invalid', 'callback_owner_required', 'callback_unavailable']), retryable: z.boolean(),
}).strict() }).strict().refine(value => value.error.retryable === (value.error.code === 'callback_unavailable'));
export interface VerificationCallbackKickoffResponse {
  status: 200 | 400 | 403 | 413 | 415 | 503;
  body: VerificationCallbackKickoffResult | { error: { code: 'callback_invalid' | 'callback_owner_required' | 'callback_unavailable'; retryable: boolean } };
}
/** Unmounted HTTP handler boundary. The server must enforce the 4096-byte streamed
 * body limit before parsing and reject nonexact route/query/method separately. */
export function createVerificationCallbackKickoff(controlToken: string, port: VerificationCallbackKickoffPort) {
  if (controlToken.length < 32) throw new Error('invalid_callback_control_configuration');
  return async (input: { remoteAddress: string | undefined; authorization?: string; contentType?: string; body: Uint8Array }): Promise<VerificationCallbackKickoffResponse> => {
    const error = (status: VerificationCallbackKickoffResponse['status'], code: 'callback_invalid' | 'callback_owner_required' | 'callback_unavailable'): VerificationCallbackKickoffResponse =>
      ({ status, body: { error: { code, retryable: status === 503 } } });
    if (!['127.0.0.1', '::1', '::ffff:127.0.0.1'].includes(input.remoteAddress ?? '')
      || !authorizedControl(input.authorization, controlToken)) return error(403, 'callback_owner_required');
    if (input.body.byteLength > 4096) return error(413, 'callback_invalid');
    if (!/^application\/json(?:\s*;\s*charset=utf-8)?$/i.test(input.contentType ?? '')) return error(415, 'callback_invalid');
    let ref: PhoneVerificationCallbackRef;
    try { ref = verificationCallbackKickoffSchema.parse(JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(input.body))); }
    catch { return error(400, 'callback_invalid'); }
    try {
      const result = verificationCallbackKickoffResultSchema.parse(await port.kickoff(ref));
      if (result.request.requestId !== ref.requestId || result.request.requestDigest !== ref.requestDigest)
        return error(503, 'callback_unavailable');
      return { status: 200, body: result };
    } catch { return error(503, 'callback_unavailable'); }
  };
}
