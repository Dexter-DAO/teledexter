import { createHash } from 'node:crypto';
import { z } from 'zod';
import { inboundBeginIdentitySchema, inboundProviderProofSchema, inboundBeginRef, inboundProofDigest } from './inboundContract.js';

export const INBOUND_ENROLLMENT_PATH = '/inbound-enrollment-completions';
export const INBOUND_ENROLLMENT_RESPONSE_PATH = '/broker/inbound/enrollment-response';
export const INBOUND_ENROLLMENT_MAX_REQUEST_BYTES = 262_144;
const uuid = z.string().uuid().refine(value => value === value.toLowerCase());
const digest = z.string().regex(/^[0-9a-f]{64}$/);
const timestamp = z.string().datetime({ offset: true });
const identity = z.object({ brokerCallId: uuid, beginDigest: digest, digitProofDigest: digest }).strict();

export function enrollmentCommandDigest(value: z.infer<typeof identity>): string {
  const input = identity.parse(value);
  return createHash('sha256').update(JSON.stringify([
    'dexter.phone.enrollment-voice-command/v1', input.brokerCallId, input.beginDigest, input.digitProofDigest,
  ]), 'utf8').digest('hex');
}
export const enrollmentCommandSchema = identity.extend({ commandDigest: digest }).strict().refine(value =>
  enrollmentCommandDigest({ brokerCallId: value.brokerCallId, beginDigest: value.beginDigest,
    digitProofDigest: value.digitProofDigest }) === value.commandDigest);
export type EnrollmentCommand = z.infer<typeof enrollmentCommandSchema>;

/** The form and its secret-backed signature are ephemeral. Retain only this digest. */
export function enrollmentDigitProofDigest(proof: z.infer<typeof inboundProviderProofSchema>): string {
  return createHash('sha256').update(JSON.stringify([
    'dexter.phone.enrollment-digit-proof/v1', inboundProofDigest(proof), proof.signature,
  ]), 'utf8').digest('hex');
}

export const enrollmentCompletionInputSchema = z.object({
  begin: inboundBeginIdentitySchema, digitProof: inboundProviderProofSchema,
}).strict().refine(value => {
  const original = value.begin.providerProof;
  const digit = value.digitProof;
  let url: URL, originalUrl: URL;
  try { url = new URL(digit.url); originalUrl = new URL(original.url); } catch { return false; }
  return url.protocol === 'https:' && !url.username && !url.password && !url.search && !url.hash
    && url.origin === originalUrl.origin && url.pathname === INBOUND_ENROLLMENT_RESPONSE_PATH
    && url.href === digit.url && digit.url !== original.url
    && ['AccountSid', 'CallSid', 'From', 'To'].every(key => digit.params[key] === original.params[key])
    && Object.prototype.propertyIsEnumerable.call(digit.params, 'Digits') && /^[0-9]{8}$/.test(digit.params.Digits ?? '')
    && !Object.prototype.hasOwnProperty.call(digit.params, 'SpeechResult');
});
export type EnrollmentCompletionInput = z.infer<typeof enrollmentCompletionInputSchema>;
export function enrollmentCommand(input: EnrollmentCompletionInput): EnrollmentCommand {
  const parsed = enrollmentCompletionInputSchema.parse(input);
  const ref = { brokerCallId: parsed.begin.brokerCallId, beginDigest: inboundBeginRef(parsed.begin).digest,
    digitProofDigest: enrollmentDigitProofDigest(parsed.digitProof) };
  return { ...ref, commandDigest: enrollmentCommandDigest(ref) };
}
const activatedReceipt = z.object({
  enrollmentId: uuid, operationId: uuid, requestDigest: digest, state: z.literal('activated'), activatedAt: timestamp,
  enrollmentRef: z.object({ id: uuid, revision: z.literal(1), digest }).strict(),
  possessionRef: z.object({ id: uuid, digest }).strict(),
  standingConsentRef: z.object({ id: uuid, revision: z.literal(1), digest }).strict(),
}).strict().refine(value => value.enrollmentRef.id === value.enrollmentId);
export const enrollmentCompletionResultSchema = z.discriminatedUnion('state', [
  z.object({ v: z.literal(1), command: enrollmentCommandSchema, state: z.literal('activated'), receipt: activatedReceipt }).strict(),
  z.object({ v: z.literal(1), command: enrollmentCommandSchema, state: z.literal('rejected'),
    reason: z.enum(['challenge_unavailable', 'attempts_exhausted', 'owner_changed', 'expired', 'not_pending']) }).strict(),
]);
export type EnrollmentCompletionResult = z.infer<typeof enrollmentCompletionResultSchema>;
export const enrollmentCompletionRecoverySchema = z.union([
  enrollmentCompletionResultSchema,
  z.object({ v: z.literal(1), command: enrollmentCommandSchema, state: z.literal('absent') }).strict(),
]);
export type EnrollmentCompletionRecovery = z.infer<typeof enrollmentCompletionRecoverySchema>;
export function sameEnrollmentCommand(left: EnrollmentCommand, right: EnrollmentCommand): boolean {
  return left.brokerCallId === right.brokerCallId && left.beginDigest === right.beginDigest
    && left.digitProofDigest === right.digitProofDigest && left.commandDigest === right.commandDigest;
}

export const enrollmentHttpErrorSchema = z.object({ error: z.object({
  code: z.string(), retryable: z.boolean(),
}).strict() }).strict();
export function isEnrollmentHttpFailure(status: number | undefined, error: { code?: string; retryable?: boolean }): boolean {
  const expected: Readonly<Record<string, number | readonly number[]>> = {
    inbound_enrollment_invalid: [400, 413], phone_service_unauthorized: 401,
    inbound_enrollment_voice_invalid: 403, inbound_carrier_attestation_required: 403,
    invalid_provider_proof: 403, call_not_active: 403, inbound_enrollment_voice_stale: 409,
    inbound_enrollment_conflict: 409, inbound_enrollment_attempts_exhausted: 409,
    phone_provider_unavailable: 503, inbound_enrollment_unavailable: 503,
  };
  if (status === undefined || error.code === undefined || !Object.hasOwn(expected, error.code)) return false;
  const allowed = expected[error.code];
  return (typeof allowed === 'number' ? status === allowed : allowed.includes(status)) && error.retryable === (status === 503);
}

/** Only finite transport classifications are durable; raw error text is never accepted. */
export const enrollmentTransportErrorSchema = z.object({
  observedAt: timestamp,
  code: z.enum(['invalid_request', 'invalid_response', 'identity_mismatch', 'request_failed', 'request_timeout',
    'response_too_large', 'invalid_configuration', 'http_error', 'inbound_enrollment_invalid',
    'phone_service_unauthorized', 'inbound_enrollment_voice_invalid', 'inbound_carrier_attestation_required',
    'invalid_provider_proof', 'call_not_active', 'inbound_enrollment_voice_stale', 'inbound_enrollment_conflict',
    'inbound_enrollment_attempts_exhausted', 'phone_provider_unavailable', 'inbound_enrollment_unavailable']),
  status: z.number().int().min(100).max(599).optional(), retryable: z.boolean().optional(),
}).strict();
export type EnrollmentTransportError = z.infer<typeof enrollmentTransportErrorSchema>;
export const inboundModeSchema = z.discriminatedUnion('kind', [
  z.object({ kind: z.literal('undecided') }).strict(),
  z.object({ kind: z.literal('ordinary'), selectedAt: timestamp }).strict(),
  z.object({ kind: z.literal('enrollment-only'), selectedAt: timestamp,
    command: enrollmentCommandSchema, dispatchStartedAt: timestamp,
    observation: z.object({ observedAt: timestamp, result: enrollmentCompletionRecoverySchema }).strict().optional(),
    transportError: enrollmentTransportErrorSchema.optional(),
    endRequestedAt: timestamp.optional(), endAcknowledgedAt: timestamp.optional(),
  }).strict(),
]);
export type InboundMode = z.infer<typeof inboundModeSchema>;
