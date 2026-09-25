import { z } from 'zod';
import { phoneCallAssignmentRefSchema, type BrokerCallAssignment, type PhoneCallAssignmentRef } from './assignment.js';
import { canonicalJson, sha256Hex } from '../outbound/canonical.js';
import type { PrimaryRequestResult } from './primaryApi.js';

// Mirrors API 9c300195 primaryCallReceiptsContracts.ts. HTTP mapping is deliberately
// supplied by the future route adapter; this module does not send requests.
export const PRIMARY_HARK_CALL_RECEIPT_ASSOCIATION_PENDING = 'primary_hark_delivery_association_pending' as const;
export const primaryHarkCallReceiptInputSchema = z.object({
  v: z.literal('hark.primary-call-receipt.v1'),
  assignmentRef: phoneCallAssignmentRefSchema,
  evidence: z.discriminatedUnion('kind', [
    z.object({ kind: z.literal('owner_decision'), toolInvocationId: z.string().uuid() }).strict(),
    z.object({ kind: z.literal('primary_outcome'), requestId: z.string().uuid() }).strict(),
  ]),
}).strict();
export type PrimaryHarkCallReceiptInput = z.infer<typeof primaryHarkCallReceiptInputSchema>;

const uuid = z.string().uuid();
const digest = z.string().regex(/^[a-f0-9]{64}$/);
const timestamp = z.string().datetime({ offset: true });
const commonReceipt = { id: uuid, bindingId: uuid, deliveryId: uuid, caseId: uuid, runId: uuid };
// The API interface is broad; these are its two actual returned variants.
export const primaryHarkCallReceiptResultSchema = z.object({
  receipt: z.discriminatedUnion('kind', [
    z.object({ ...commonReceipt, kind: z.literal('owner_decision'), state: z.literal('closed') }).strict(),
    z.object({ ...commonReceipt, kind: z.literal('primary_outcome'), requestId: uuid,
      state: z.literal('recorded') }).strict(),
  ]),
  replay: z.boolean(),
}).strict();
export type PrimaryHarkCallReceiptResult = z.infer<typeof primaryHarkCallReceiptResultSchema>;

export const primaryHarkReceiptBindingSchema = z.object({
  origin: z.literal('authenticated_hark_delivery'), callId: uuid, bindingId: uuid,
  callSid: z.string().regex(/^CA[0-9a-fA-F]{32}$/), deliveryId: uuid, caseId: uuid,
  assignmentRef: phoneCallAssignmentRefSchema,
}).strict().refine(value => value.callId === value.deliveryId && value.callId === value.assignmentRef.assignmentId);
export type PrimaryHarkReceiptBinding = z.infer<typeof primaryHarkReceiptBindingSchema>;

/** Existing saved assignment plus its admitted Primary binding supplies identity. */
export function primaryHarkReceiptBindingFromCall(call: {
  id: string; callSid?: string; callAssignment?: BrokerCallAssignment;
  primary?: { bindingId: string; callAssignmentRef?: PhoneCallAssignmentRef };
}): PrimaryHarkReceiptBinding | undefined {
  if (call.callAssignment?.origin !== 'authenticated_hark_delivery' || !call.primary) return undefined;
  return frozen(parse(primaryHarkReceiptBindingSchema, {
    origin: call.callAssignment.origin, callId: call.id, bindingId: call.primary.bindingId,
    callSid: call.callSid, deliveryId: call.callAssignment.value.hark.deliveryId,
    caseId: call.callAssignment.value.hark.caseId, assignmentRef: call.primary.callAssignmentRef,
  }));
}

const pendingReason = z.enum(['association_pending', 'http_retry', 'configuration_unavailable', 'request_failed', 'invalid_response']);
const rejectedReason = z.enum(['http_refusal', 'identity_mismatch', 'invalid_receipt_body', 'unsupported_evidence']);
const httpStatus = z.number().int().min(100).max(599).optional();
const unsettledSchema = z.discriminatedUnion('state', [
  z.object({ state: z.literal('pending'), reason: pendingReason, httpStatus }).strict(),
  z.object({ state: z.literal('rejected'), reason: rejectedReason, httpStatus }).strict(),
]);
export type PrimaryHarkReceiptUnsettled = z.infer<typeof unsettledSchema>;
export type PrimaryHarkReceiptDelivery = PrimaryHarkReceiptUnsettled
  | { state: 'delivered'; result: PrimaryHarkCallReceiptResult };
export type PrimaryHarkReceiptFailure =
  | { kind: 'http'; status: number; code?: string }
  | { kind: 'configuration' | 'transport' | 'invalid_response' | 'identity_mismatch' };

/** Finite diagnostics only. The transport adapter owns parsing the HTTP envelope. */
export function classifyPrimaryHarkReceiptFailure(failure: PrimaryHarkReceiptFailure): PrimaryHarkReceiptUnsettled {
  if (failure.kind === 'http') {
    if (!Number.isInteger(failure.status) || failure.status < 400 || failure.status > 599)
      return { state: 'rejected', reason: 'http_refusal' };
    if (failure.status === 409 && failure.code === PRIMARY_HARK_CALL_RECEIPT_ASSOCIATION_PENDING)
      return { state: 'pending', reason: 'association_pending', httpStatus: 409 };
    return [401, 403, 408, 429].includes(failure.status) || failure.status >= 500
      ? { state: 'pending', reason: 'http_retry', httpStatus: failure.status }
      : { state: 'rejected', reason: 'http_refusal', httpStatus: failure.status };
  }
  if (failure.kind === 'identity_mismatch') return { state: 'rejected', reason: 'identity_mismatch' };
  if (failure.kind === 'configuration') return { state: 'pending', reason: 'configuration_unavailable' };
  return { state: 'pending', reason: failure.kind === 'transport' ? 'request_failed' : 'invalid_response' };
}

const entrySchema = z.object({
  id: z.string().min(1).max(180), body: primaryHarkCallReceiptInputSchema, bodyDigest: digest, observedAt: timestamp,
  state: z.enum(['pending', 'delivered', 'rejected']), attempts: z.number().int().safe().nonnegative(),
  lastAttemptAt: timestamp.optional(), lastOutcome: unsettledSchema.optional(), result: primaryHarkCallReceiptResultSchema.optional(),
}).strict();
const outboxSchema = z.object({ version: z.literal(1), binding: primaryHarkReceiptBindingSchema,
  entries: z.record(z.string(), entrySchema) }).strict();
export type PrimaryHarkReceiptEntry = z.infer<typeof entrySchema>;
export type PrimaryHarkReceiptOutbox = z.infer<typeof outboxSchema>;
export type PrimaryHarkReceiptCommand =
  | { kind: 'enqueue'; body: PrimaryHarkCallReceiptInput; observedAt: string }
  | { kind: 'attempt'; id: string; bodyDigest: string; expectedAttempts: number; observedAt: string }
  | { kind: 'settle'; id: string; bodyDigest: string; attempt: number; outcome: PrimaryHarkReceiptDelivery };

function frozen<T>(value: T): T {
  if (value && typeof value === 'object') {
    for (const child of Object.values(value)) frozen(child);
    Object.freeze(value);
  }
  return value;
}
function parse<T>(schema: z.ZodType<T>, value: unknown): T {
  const result = schema.safeParse(value);
  if (!result.success) throw new Error('invalid_primary_hark_receipt');
  return result.data;
}
function same(a: unknown, b: unknown): boolean { return canonicalJson(a) === canonicalJson(b); }
export function primaryHarkReceiptSourceId(bindingId: string, body: PrimaryHarkCallReceiptInput): string {
  parse(uuid, bindingId);
  const value = parse(primaryHarkCallReceiptInputSchema, body);
  return value.evidence.kind === 'owner_decision' ? `owner_decision:${value.evidence.toolInvocationId}`
    : `primary_outcome:${bindingId}:${value.evidence.requestId}`;
}
export function primaryHarkReceiptBodyDigest(bindingId: string, body: PrimaryHarkCallReceiptInput): string {
  parse(uuid, bindingId);
  return sha256Hex(canonicalJson({ namespace: 'hark.primary-call-receipt.v1',
    value: { bindingId, body: parse(primaryHarkCallReceiptInputSchema, body) } }));
}

export function validatePrimaryHarkReceiptResult(binding: PrimaryHarkReceiptBinding,
  body: PrimaryHarkCallReceiptInput, result: unknown): PrimaryHarkCallReceiptResult {
  const identity = parse(primaryHarkReceiptBindingSchema, binding);
  const request = parse(primaryHarkCallReceiptInputSchema, body);
  const value = parse(primaryHarkCallReceiptResultSchema, result);
  const receipt = value.receipt;
  if (!same(identity.assignmentRef, request.assignmentRef) || receipt.bindingId !== identity.bindingId
    || receipt.deliveryId !== identity.deliveryId || receipt.caseId !== identity.caseId
    || receipt.kind !== request.evidence.kind
    || (request.evidence.kind === 'owner_decision' && receipt.id !== request.evidence.toolInvocationId)
    || (request.evidence.kind === 'primary_outcome'
      && (receipt.kind !== 'primary_outcome' || receipt.requestId !== request.evidence.requestId)))
    throw new Error('primary_hark_receipt_identity_mismatch');
  return frozen(value);
}

/** Validate restored JSON against the call's separately saved immutable binding. */
export function validatePrimaryHarkReceiptOutbox(value: unknown, binding: PrimaryHarkReceiptBinding): PrimaryHarkReceiptOutbox {
  const box = parse(outboxSchema, value);
  const expected = parse(primaryHarkReceiptBindingSchema, binding);
  if (!same(box.binding, expected)) throw new Error('primary_hark_receipt_binding_changed');
  for (const [id, entry] of Object.entries(box.entries)) {
    if (entry.id !== id || id !== primaryHarkReceiptSourceId(expected.bindingId, entry.body)
      || entry.bodyDigest !== primaryHarkReceiptBodyDigest(expected.bindingId, entry.body)
      || !same(entry.body.assignmentRef, expected.assignmentRef)) throw new Error('primary_hark_receipt_body_changed');
    if ((entry.attempts === 0) !== (entry.lastAttemptAt === undefined)
      || (entry.attempts === 0 && (entry.lastOutcome !== undefined || entry.state !== 'pending')))
      throw new Error('invalid_primary_hark_receipt_attempt');
    if (entry.state === 'delivered') {
      if (!entry.result || !entry.attempts || entry.lastOutcome) throw new Error('invalid_primary_hark_receipt_delivery');
      validatePrimaryHarkReceiptResult(expected, entry.body, entry.result);
    } else if (entry.result || (entry.lastOutcome && entry.lastOutcome.state !== entry.state)
      || (entry.state === 'rejected' && entry.lastOutcome?.state !== 'rejected')) {
      throw new Error('invalid_primary_hark_receipt_delivery');
    }
  }
  return frozen(box);
}

/** Caller atomically saves the returned outbox with its accepted Primary result.
 * Only the shared API can establish an owner decision from the supplied tool ID.
 * This reducer performs no HTTP, speech interpretation, call or financial action.
 */
export function reducePrimaryHarkReceiptOutbox(value: PrimaryHarkReceiptOutbox | undefined,
  binding: PrimaryHarkReceiptBinding, command: PrimaryHarkReceiptCommand): PrimaryHarkReceiptOutbox {
  const identity = parse(primaryHarkReceiptBindingSchema, binding);
  const box = value === undefined ? { version: 1 as const, binding: identity, entries: {} as Record<string, PrimaryHarkReceiptEntry> }
    : structuredClone(validatePrimaryHarkReceiptOutbox(value, identity));
  if (command.kind === 'enqueue') {
    const body = parse(primaryHarkCallReceiptInputSchema, command.body);
    parse(timestamp, command.observedAt);
    if (!same(body.assignmentRef, identity.assignmentRef)) throw new Error('primary_hark_receipt_binding_changed');
    const id = primaryHarkReceiptSourceId(identity.bindingId, body);
    const bodyDigest = primaryHarkReceiptBodyDigest(identity.bindingId, body);
    const existing = box.entries[id];
    if (existing) {
      if (existing.bodyDigest !== bodyDigest || !same(existing.body, body)) throw new Error('primary_hark_receipt_body_changed');
    } else box.entries[id] = { id, body, bodyDigest, observedAt: command.observedAt, state: 'pending', attempts: 0 };
  } else {
    const entry = box.entries[command.id];
    if (!entry || entry.bodyDigest !== command.bodyDigest) throw new Error('primary_hark_receipt_entry_mismatch');
    if (command.kind === 'attempt') {
      parse(timestamp, command.observedAt);
      if (entry.state !== 'pending') throw new Error('primary_hark_receipt_not_pending');
      if (!Number.isSafeInteger(command.expectedAttempts) || command.expectedAttempts !== entry.attempts)
        throw new Error('primary_hark_receipt_attempt_mismatch');
      entry.attempts += 1;
      entry.lastAttemptAt = command.observedAt;
    } else {
      if (!Number.isSafeInteger(command.attempt) || command.attempt !== entry.attempts || command.attempt < 1)
        throw new Error('primary_hark_receipt_attempt_mismatch');
      if (command.outcome.state === 'delivered') {
        const result = validatePrimaryHarkReceiptResult(identity, entry.body, command.outcome.result);
        if (entry.state === 'rejected') throw new Error('primary_hark_receipt_already_rejected');
        if (entry.result && !same(entry.result.receipt, result.receipt)) throw new Error('primary_hark_receipt_result_changed');
        entry.result ??= result;
        entry.state = 'delivered';
        delete entry.lastOutcome;
      } else {
        const outcome = parse(unsettledSchema, command.outcome);
        if (entry.state === 'delivered' || (entry.state === 'rejected' && !same(entry.lastOutcome, outcome)))
          throw new Error('primary_hark_receipt_terminal_changed');
        entry.state = outcome.state;
        entry.lastOutcome = outcome;
      }
    }
  }
  return validatePrimaryHarkReceiptOutbox(box, identity);
}

const completedResultIdentitySchema = z.object({ state: z.literal('completed'), requestId: uuid, bindingId: uuid,
  assistant: z.object({ messageId: uuid, eventId: uuid, sequence: z.string().regex(/^\d+$/),
    text: z.string().refine(value => Boolean(value.trim())) }) });

/** Apply after the existing terminal-result immutability check. The store owner
 * commits this patch in one call-record write; this function does not persist it.
 * canonicalRequestId comes from the accepted result/reconciliation, never recency.
 */
export function preparePrimaryHarkOutcomePatch(input: {
  binding: PrimaryHarkReceiptBinding; outbox?: PrimaryHarkReceiptOutbox;
  canonicalRequestId: string; result: PrimaryRequestResult; observedAt: string;
}): { primaryResult: Extract<PrimaryRequestResult, { state: 'completed' }>; harkReceiptOutbox: PrimaryHarkReceiptOutbox } {
  const binding = parse(primaryHarkReceiptBindingSchema, input.binding);
  const identity = parse(completedResultIdentitySchema, input.result);
  parse(uuid, input.canonicalRequestId);
  if (input.result.state !== 'completed' || identity.requestId !== input.canonicalRequestId || identity.bindingId !== binding.bindingId)
    throw new Error('primary_hark_receipt_identity_mismatch');
  const body: PrimaryHarkCallReceiptInput = { v: 'hark.primary-call-receipt.v1', assignmentRef: binding.assignmentRef,
    evidence: { kind: 'primary_outcome', requestId: input.canonicalRequestId } };
  return frozen({ primaryResult: structuredClone(input.result), harkReceiptOutbox: reducePrimaryHarkReceiptOutbox(
    input.outbox, binding, { kind: 'enqueue', body, observedAt: input.observedAt }) });
}
