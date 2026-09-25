import { z } from 'zod';
import { createPrimaryApiTransport, PrimaryApiError, type PrimaryApiOptions } from './primaryApi.js';
import {
  phoneVerificationCallbackAccessRequestSchema, phoneVerificationCallbackAccessDigest,
  phoneVerificationCallbackRefSchema, phoneVerificationCallbackDispatchSchema, phoneVerificationCallbackDispatchResultSchema,
  phoneVerificationCallbackAnswerSchema, phoneVerificationCallbackCompletionSchema, phoneVerificationCallbackCommandRef,
  phoneVerificationCallbackCommandRefSchema, phoneVerificationCallbackCompletionResultSchema,
  phoneVerificationCallbackCompletionRecoverySchema, phoneVerificationCallbackStateSchema,
  phoneVerificationCallbackCancelSchema, phoneVerificationCallbackRecoveryQuerySchema,
  sameCallbackRef, sameCallbackCommand, callbackCommandForPurpose, assertCallbackStateIdentity,
  type PhoneVerificationCallbackAccessRequest, type PhoneVerificationCallbackRef,
  type PhoneVerificationCallbackDispatch, type PhoneVerificationCallbackDispatchResult, type PhoneVerificationCallbackAnswer,
  type PhoneVerificationCallbackCompletion, type PhoneVerificationCallbackCommandRef,
  type PhoneVerificationCallbackCompletionResult, type PhoneVerificationCallbackCompletionRecovery,
  type PhoneVerificationCallbackPurpose, type PhoneVerificationCallbackState,
} from './verificationCallbackContract.js';

const base = '/verification-callbacks';
function parse<T>(schema: z.ZodType<T>, value: unknown, code: 'invalid_request' | 'invalid_response'): T {
  const result = schema.safeParse(value); if (!result.success) throw new PrimaryApiError(code); return result.data;
}
function identity(ok: boolean): void { if (!ok) throw new PrimaryApiError('identity_mismatch'); }
function state(value: unknown, ref: PhoneVerificationCallbackRef): PhoneVerificationCallbackState {
  const result = parse(phoneVerificationCallbackStateSchema, value, 'invalid_response');
  try { assertCallbackStateIdentity(result, ref); } catch { throw new PrimaryApiError('identity_mismatch'); }
  return result;
}
function purposeResult(value: PhoneVerificationCallbackCompletionRecovery, purpose: PhoneVerificationCallbackPurpose): void {
  identity(callbackCommandForPurpose(value.command, purpose)
    && (value.state !== 'enrolled' || purpose === 'enrollment') && (value.state !== 'admitted' || purpose === 'account_access'));
}
export interface VerificationCallbackClient {
  prepareAccess(input: PhoneVerificationCallbackAccessRequest, signal?: AbortSignal): Promise<PhoneVerificationCallbackState>;
  recover(ref: PhoneVerificationCallbackRef, signal?: AbortSignal): Promise<PhoneVerificationCallbackState>;
  dispatch(input: PhoneVerificationCallbackDispatch, signal?: AbortSignal): Promise<PhoneVerificationCallbackDispatchResult>;
  answer(input: PhoneVerificationCallbackAnswer, purpose: PhoneVerificationCallbackPurpose, signal?: AbortSignal): Promise<PhoneVerificationCallbackState>;
  complete(input: PhoneVerificationCallbackCompletion, signal?: AbortSignal): Promise<PhoneVerificationCallbackCompletionResult>;
  recoverCompletion(command: PhoneVerificationCallbackCommandRef, purpose: PhoneVerificationCallbackPurpose,
    signal?: AbortSignal): Promise<PhoneVerificationCallbackCompletionRecovery>;
  cancel(ref: PhoneVerificationCallbackRef, signal?: AbortSignal): Promise<PhoneVerificationCallbackState>;
}
/** Exact HTTPv4 service transport; each call makes one attempt, with no retry middleware. */
export function createVerificationCallbackClient(options: PrimaryApiOptions): VerificationCallbackClient {
  const request = createPrimaryApiTransport(options);
  return {
    async prepareAccess(raw, signal) {
      const input = parse(phoneVerificationCallbackAccessRequestSchema, raw, 'invalid_request');
      const ref = { requestId: input.requestId, requestDigest: phoneVerificationCallbackAccessDigest(input) };
      const result = state(await request('POST', base, input, undefined, 200, false, { signal }), ref);
      if (result.state === 'reserved') identity(result.permit.purpose === 'account_access');
      if (result.state === 'answered') identity(result.purpose === 'account_access');
      if (result.state === 'completed') purposeResult(result.result, 'account_access');
      return result;
    },
    async recover(raw, signal) {
      const ref = parse(phoneVerificationCallbackRefSchema, raw, 'invalid_request');
      return state(await request('GET', `${base}/${ref.requestId}?requestDigest=${encodeURIComponent(ref.requestDigest)}`,
        undefined, undefined, 200, false, { signal }), ref);
    },
    async dispatch(raw, signal) {
      const input = parse(phoneVerificationCallbackDispatchSchema, raw, 'invalid_request');
      const result = parse(phoneVerificationCallbackDispatchResultSchema,
        await request('POST', `${base}/${input.request.requestId}/dispatch`, input, undefined, 200, false, { signal }), 'invalid_response');
      identity(sameCallbackRef(result.permit.request, input.request) && result.dispatchAttemptId === input.dispatchAttemptId);
      return result;
    },
    async answer(raw, purpose, signal) {
      const input = parse(phoneVerificationCallbackAnswerSchema, raw, 'invalid_request');
      const result = state(await request('POST', `${base}/${input.request.requestId}/answer`, input,
        undefined, 200, false, { signal }), input.request);
      identity(['answered', 'completed', 'cancelled', 'expired'].includes(result.state));
      if (result.state === 'answered') identity(result.callSid === input.providerProof.params.CallSid
        && result.dispatchAttemptId === input.dispatchAttemptId && result.purpose === purpose);
      if (result.state === 'completed') { purposeResult(result.result, purpose);
        identity(result.result.command.callSid === input.providerProof.params.CallSid); }
      return result;
    },
    async complete(raw, signal) {
      const input = parse(phoneVerificationCallbackCompletionSchema, raw, 'invalid_request');
      const expected = phoneVerificationCallbackCommandRef(input);
      const result = parse(phoneVerificationCallbackCompletionResultSchema,
        await request('POST', `${base}/${input.request.requestId}/complete`, input, undefined, 200, false, { signal }), 'invalid_response');
      identity(sameCallbackCommand(result.command, expected)); purposeResult(result, input.purpose); return result;
    },
    async recoverCompletion(raw, purpose, signal) {
      const command = parse(phoneVerificationCallbackCommandRefSchema, raw, 'invalid_request');
      if (!callbackCommandForPurpose(command, purpose)) throw new PrimaryApiError('invalid_request');
      const query = phoneVerificationCallbackRecoveryQuerySchema.parse({ requestDigest: command.request.requestDigest,
        brokerCallId: command.brokerCallId, callSid: command.callSid, turnId: command.turn.id,
        turnDigest: command.turn.digest, commandDigest: command.commandDigest });
      const encoded = Object.entries(query).map(([key, value]) => `${key}=${encodeURIComponent(value)}`).join('&');
      const result = parse(phoneVerificationCallbackCompletionRecoverySchema,
        await request('GET', `${base}/${command.request.requestId}/completions?${encoded}`, undefined,
          undefined, 200, false, { signal }), 'invalid_response');
      identity(sameCallbackCommand(result.command, command)); purposeResult(result, purpose); return result;
    },
    async cancel(raw, signal) {
      const ref = parse(phoneVerificationCallbackRefSchema, raw, 'invalid_request');
      const body = phoneVerificationCallbackCancelSchema.parse({ v: 1, request: ref });
      const result = state(await request('POST', `${base}/${ref.requestId}/cancel`, body, undefined, 200, false, { signal }), ref);
      identity(['completed', 'cancelled', 'expired'].includes(result.state)); return result;
    },
  };
}
