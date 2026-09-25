import { createHash } from "node:crypto";
import { z } from "zod";
// Frozen APIa314 schema adapted to Broker Zod4. Generic proof intentionally
// permits outbound answer forms; transport authentication remains API-owned.
export const callbackProviderProofSchema = z.object({
  url: z.string().url().max(2048), signature: z.string().max(128),
  params: z.record(z.string().max(128), z.string().max(2048))
    .refine(params => Object.keys(params).length <= 80),
}).strict();
const phoneProviderProofSchema = callbackProviderProofSchema;
const inboundBeginSchema = z.object({
  brokerCallId: z.string().uuid().transform(value => value.toLowerCase()),
  providerProof: phoneProviderProofSchema,
}).strict();


export const PHONE_VERIFICATION_CALLBACK_POLICY = "dexter.phone.verification-callback/v1";
export const PHONE_VERIFICATION_CALLBACK_PATH = "/internal/primary-dexter/channels/phone/verification-callbacks";
export const PHONE_VERIFICATION_CALLBACK_SETUP_PATH = "/api/user/phone/verification-callbacks";
export const PHONE_VERIFICATION_CALLBACK_CONSENT_PATH = "/api/user/phone/verification-callback-consents";
export const PHONE_VERIFICATION_CALLBACK_ANSWER_PATH = "/broker/verification-callback";
export const PHONE_VERIFICATION_CALLBACK_RESPONSE_PATH = "/broker/verification-callback-response";
export const PHONE_VERIFICATION_CALLBACK_MAX_DURATION_SECONDS = 180;
export const PHONE_VERIFICATION_CALLBACK_LIFETIME_MS = 300_000;
export const PHONE_VERIFICATION_CALLBACK_PROOF_MAX_BYTES = 262_144;
export const PHONE_VERIFICATION_CALLBACK_COMMAND_MAX_BYTES = 4_096;

const uuid = z.string().uuid().refine(value => value === value.toLowerCase());
const digest = z.string().regex(/^[0-9a-f]{64}$/);
const phone = z.string().regex(/^\+[1-9][0-9]{7,14}$/);
const timestamp = z.string().datetime().refine(value => new Date(value).toISOString() === value);
const callSid = z.string().regex(/^CA[0-9a-fA-F]{32}$/);
const accountSid = z.string().regex(/^AC[0-9a-fA-F]{32}$/);
const consent = z.object({ enabled: z.literal(true), policyVersion: z.literal(1) }).strict();

/** Direct-owner-session routes only. This is separate from incomingAccess and a payment mandate. */
export const phoneVerificationCallbackConsentRequestSchema = z.object({
  operationId: uuid, enrollmentId: uuid, optionalVerificationCallback: consent,
}).strict();
/** One contact to the saved candidate. This operation does not activate standing access. */
export const phoneVerificationCallbackEnrollmentRequestSchema = z.object({
  operationId: uuid, enrollmentOperationId: uuid, verificationCallback: consent,
}).strict();
/** Service-authenticated restricted call tool. The original signed form selects no caller-supplied owner. */
export const phoneVerificationCallbackAccessRequestSchema = z.object({
  v: z.literal(1), requestId: uuid, original: inboundBeginSchema,
  explicitlyRequested: z.literal(true),
}).strict();
export type PhoneVerificationCallbackAccessRequest = z.output<typeof phoneVerificationCallbackAccessRequestSchema>;

export const phoneVerificationCallbackRefSchema = z.object({ requestId: uuid, requestDigest: digest }).strict();
export type PhoneVerificationCallbackRef = z.output<typeof phoneVerificationCallbackRefSchema>;
export const phoneVerificationCallbackPurposeSchema = z.enum(["enrollment", "account_access"]);
export type PhoneVerificationCallbackPurpose = z.output<typeof phoneVerificationCallbackPurposeSchema>;

/** Broker-only permit. No raw account challenge, account identifiers or private Primary context. */
export const phoneVerificationCallbackPermitSchema = z.object({
  v: z.literal(1), request: phoneVerificationCallbackRefSchema, purpose: phoneVerificationCallbackPurposeSchema,
  providerAccountSid: accountSid, from: phone, to: phone,
  answerUrl: z.string().url(), responseUrl: z.string().url(),
  createdAt: timestamp, expiresAt: timestamp,
  maxDurationSeconds: z.literal(PHONE_VERIFICATION_CALLBACK_MAX_DURATION_SECONDS),
  billing: z.object({ intentId: uuid, eventId: z.string(), mandateId: uuid,
    unitPriceMicro: z.number().int().safe().positive(), freeSeconds: z.number().int().safe().nonnegative() }).strict(),
}).strict().superRefine((value, context) => {
  const invalid = () => context.addIssue({ code: z.ZodIssueCode.custom, message: "callback_permit_invalid" });
  if (value.billing.intentId !== value.request.requestId
    || value.billing.eventId !== `phone-verification:${value.request.requestId}`
    || Date.parse(value.expiresAt) <= Date.parse(value.createdAt)
    || Date.parse(value.expiresAt) - Date.parse(value.createdAt) > PHONE_VERIFICATION_CALLBACK_LIFETIME_MS) invalid();
  try {
    const answer = new URL(value.answerUrl), response = new URL(value.responseUrl);
    if (answer.protocol !== "https:" || response.origin !== answer.origin
      || answer.username || answer.password || answer.search || answer.hash
      || response.username || response.password || response.search || response.hash
      || answer.href !== value.answerUrl || response.href !== value.responseUrl
      || answer.pathname !== `${PHONE_VERIFICATION_CALLBACK_ANSWER_PATH}/${value.request.requestId}`
      || response.pathname !== `${PHONE_VERIFICATION_CALLBACK_RESPONSE_PATH}/${value.request.requestId}`) invalid();
  } catch { invalid(); }
});
export type PhoneVerificationCallbackPermit = z.output<typeof phoneVerificationCallbackPermitSchema>;

/** This receipt is authored by the authenticated Broker from its bound live voice turn, never supplied by a caller/model field. */
export const phoneVerificationCallbackTurnSchema = z.object({ id: uuid, digest }).strict();
export const phoneVerificationCallbackAnswerSchema = z.object({
  v: z.literal(1), request: phoneVerificationCallbackRefSchema, brokerCallId: uuid,
  dispatchAttemptId: uuid, providerProof: phoneProviderProofSchema,
}).strict().refine(value => value.brokerCallId === value.request.requestId);
export type PhoneVerificationCallbackAnswer = z.output<typeof phoneVerificationCallbackAnswerSchema>;
const completion = { v: z.literal(1), request: phoneVerificationCallbackRefSchema,
  brokerCallId: uuid, callSid, turn: phoneVerificationCallbackTurnSchema, accepted: z.literal(true) };
export const phoneVerificationCallbackCompletionSchema = z.discriminatedUnion("purpose", [
  z.object({ ...completion, purpose: z.literal("enrollment"), challenge: z.string().regex(/^[0-9]{8}$/) }).strict(),
  z.object({ ...completion, purpose: z.literal("account_access") }).strict(),
]).refine(value => value.brokerCallId === value.request.requestId);
export type PhoneVerificationCallbackCompletion = z.output<typeof phoneVerificationCallbackCompletionSchema>;
export const phoneVerificationCallbackCommandRefSchema = z.object({
  request: phoneVerificationCallbackRefSchema, brokerCallId: uuid, callSid,
  turn: phoneVerificationCallbackTurnSchema, commandDigest: digest,
}).strict();
export type PhoneVerificationCallbackCommandRef = z.output<typeof phoneVerificationCallbackCommandRefSchema>;

export const phoneVerificationCallbackRefusalSchema = z.enum([
  "expired", "cancelled", "owner_changed", "consent_revoked", "challenge_invalid", "attempts_exhausted",
]);
const activated = z.object({
  enrollmentId: uuid, operationId: uuid, requestDigest: digest,
  state: z.literal("activated"), activatedAt: timestamp,
  enrollmentRef: z.object({ id: uuid, revision: z.number().int().min(1).max(Number.MAX_SAFE_INTEGER), digest }).strict(),
  possessionRef: z.object({ id: uuid, digest }).strict(),
  standingConsentRef: z.object({ id: uuid, revision: z.number().int().min(1).max(Number.MAX_SAFE_INTEGER), digest }).strict(),
}).strict();
export const phoneVerificationCallbackCompletionResultSchema = z.discriminatedUnion("state", [
  z.object({ v: z.literal(1), command: phoneVerificationCallbackCommandRefSchema,
    state: z.literal("enrolled"), receipt: activated }).strict(),
  z.object({ v: z.literal(1), command: phoneVerificationCallbackCommandRefSchema,
    state: z.literal("admitted"), admission: z.object({ id: uuid, digest, expiresAt: timestamp }).strict() }).strict(),
  z.object({ v: z.literal(1), command: phoneVerificationCallbackCommandRefSchema,
    state: z.literal("rejected"), reason: phoneVerificationCallbackRefusalSchema }).strict(),
]);
export type PhoneVerificationCallbackCompletionResult = z.output<typeof phoneVerificationCallbackCompletionResultSchema>;
export const phoneVerificationCallbackCompletionRecoverySchema = z.union([
  phoneVerificationCallbackCompletionResultSchema,
  z.object({ v: z.literal(1), command: phoneVerificationCallbackCommandRefSchema, state: z.literal("absent") }).strict(),
]);

export const phoneVerificationCallbackDispatchSchema = z.object({
  v: z.literal(1), request: phoneVerificationCallbackRefSchema, dispatchAttemptId: uuid,
}).strict();
export const phoneVerificationCallbackCancelSchema = z.object({ v: z.literal(1), request: phoneVerificationCallbackRefSchema }).strict();
export const phoneVerificationCallbackRecoveryQuerySchema = z.object({
  requestDigest: digest, brokerCallId: uuid, callSid, turnId: uuid, turnDigest: digest, commandDigest: digest,
}).strict();
/** A recovered grant never authorizes another provider create. Broker must recover the identical durable attempt. */
export const phoneVerificationCallbackDispatchResultSchema = z.object({
  v: z.literal(1), state: z.literal("dispatch_granted"), dispatchAttemptId: uuid,
  permit: phoneVerificationCallbackPermitSchema,
}).strict();
export const phoneVerificationCallbackStateSchema = z.discriminatedUnion("state", [
  z.object({ v: z.literal(1), request: phoneVerificationCallbackRefSchema,
    state: z.literal("reserved"), permit: phoneVerificationCallbackPermitSchema }).strict(),
  z.object({ v: z.literal(1), request: phoneVerificationCallbackRefSchema,
    state: z.literal("dispatching"), dispatchAttemptId: uuid, expiresAt: timestamp }).strict(),
  z.object({ v: z.literal(1), request: phoneVerificationCallbackRefSchema,
    state: z.literal("answered"), dispatchAttemptId: uuid, callSid, purpose: phoneVerificationCallbackPurposeSchema,
    expiresAt: timestamp }).strict(),
  z.object({ v: z.literal(1), request: phoneVerificationCallbackRefSchema,
    state: z.literal("completed"), result: phoneVerificationCallbackCompletionResultSchema }).strict(),
  z.object({ v: z.literal(1), request: phoneVerificationCallbackRefSchema,
    state: z.literal("cancelled"), cancelledAt: timestamp }).strict(),
  z.object({ v: z.literal(1), request: phoneVerificationCallbackRefSchema,
    state: z.literal("expired"), expiresAt: timestamp }).strict(),
  z.object({ v: z.literal(1), request: phoneVerificationCallbackRefSchema, state: z.literal("absent") }).strict(),
]);
export type PhoneVerificationCallbackState = z.output<typeof phoneVerificationCallbackStateSchema>;

/** Direct-account success envelope. Broker permits and private account context never cross this boundary. */
export const phoneVerificationCallbackOwnerResultSchema = z.object({
  v: z.literal(1), operationId: uuid, request: phoneVerificationCallbackRefSchema,
  purpose: z.literal("enrollment"), state: z.enum(["reserved", "dispatching", "answered", "enrolled", "rejected", "cancelled", "expired"]),
  expiresAt: timestamp,
}).strict();
export type PhoneVerificationCallbackOwnerResult = z.output<typeof phoneVerificationCallbackOwnerResultSchema>;
export const phoneVerificationCallbackConsentResultSchema = z.discriminatedUnion("state", [
  z.object({ v: z.literal(1), operationId: uuid, consentId: uuid, enrollmentId: uuid, digest,
    state: z.literal("active"), createdAt: timestamp }).strict(),
  z.object({ v: z.literal(1), operationId: uuid, consentId: uuid, enrollmentId: uuid, digest,
    state: z.literal("revoked"), createdAt: timestamp, revokedAt: timestamp }).strict(),
]);

export const phoneVerificationCallbackErrorSchema = z.object({
  error: z.object({ code: z.enum(["callback_invalid", "direct_owner_session_required", "callback_owner_required",
    "callback_not_found", "callback_consent_required", "callback_conflict", "callback_rate_limited", "callback_expired",
    "callback_unavailable", "phone_tab_required", "callback_financial_admission_required", "callback_not_answered"]),
    retryable: z.boolean() }).strict(),
}).strict();

export function phoneVerificationCallbackDigest(domain: string, tuple: readonly unknown[]): string {
  return createHash("sha256").update(JSON.stringify([domain, ...tuple]), "utf8").digest("hex");
}
/** Recoverable before POST. Raw provider signatures and raw challenges are never stored in this identity. */
export function phoneVerificationCallbackAccessDigest(raw: PhoneVerificationCallbackAccessRequest): string {
  const value = phoneVerificationCallbackAccessRequestSchema.parse(raw);
  const proof = value.original.providerProof;
  return phoneVerificationCallbackDigest("dexter.phone.callback-access-request/v1", [value.requestId,
    value.original.brokerCallId, proof.url, Object.keys(proof.params).sort().map(key => [key, proof.params[key]]),
    true]);
}

export function phoneVerificationCallbackCommandRef(raw: PhoneVerificationCallbackCompletion): PhoneVerificationCallbackCommandRef {
  const value = phoneVerificationCallbackCompletionSchema.parse(raw);
  const commandDigest = phoneVerificationCallbackDigest("dexter.phone.callback-voice-command/v1", [
    value.request.requestId, value.request.requestDigest, value.brokerCallId, value.callSid,
    value.turn.id, value.turn.digest, value.purpose, true,
  ]);
  return { request: value.request, brokerCallId: value.brokerCallId, callSid: value.callSid, turn: value.turn, commandDigest };
}

export type PhoneVerificationCallbackDispatch = z.output<typeof phoneVerificationCallbackDispatchSchema>;
export type PhoneVerificationCallbackDispatchResult = z.output<typeof phoneVerificationCallbackDispatchResultSchema>;
export type PhoneVerificationCallbackCompletionRecovery = z.output<typeof phoneVerificationCallbackCompletionRecoverySchema>;
export type PhoneVerificationCallbackTurn = z.output<typeof phoneVerificationCallbackTurnSchema>;
export type CallbackProviderProof = z.output<typeof callbackProviderProofSchema>;

/** Additional consumer consistency checks; shared public schemas remain unchanged. */
export function sameCallbackRef(a: PhoneVerificationCallbackRef, b: PhoneVerificationCallbackRef): boolean {
  return a.requestId === b.requestId && a.requestDigest === b.requestDigest;
}
export function sameCallbackCommand(a: PhoneVerificationCallbackCommandRef, b: PhoneVerificationCallbackCommandRef): boolean {
  return sameCallbackRef(a.request, b.request) && a.brokerCallId === b.brokerCallId && a.callSid === b.callSid
    && a.turn.id === b.turn.id && a.turn.digest === b.turn.digest && a.commandDigest === b.commandDigest;
}
export function callbackCommandForPurpose(command: PhoneVerificationCallbackCommandRef,
  purpose: PhoneVerificationCallbackPurpose): boolean {
  return command.brokerCallId === command.request.requestId
    && command.commandDigest === phoneVerificationCallbackDigest("dexter.phone.callback-voice-command/v1", [
      command.request.requestId, command.request.requestDigest, command.brokerCallId, command.callSid,
      command.turn.id, command.turn.digest, purpose, true,
    ]);
}
export function assertCallbackStateIdentity(state: PhoneVerificationCallbackState, ref: PhoneVerificationCallbackRef): void {
  if (!sameCallbackRef(state.request, ref)
    || (state.state === 'reserved' && !sameCallbackRef(state.permit.request, ref))
    || (state.state === 'completed' && (!sameCallbackRef(state.result.command.request, ref)
      || state.result.command.brokerCallId !== ref.requestId))) throw new Error('callback_identity_changed');
}
