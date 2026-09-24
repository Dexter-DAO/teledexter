import { z } from 'zod';
import { phoneCallAssignmentRefSchema, sameCallAssignmentRef } from './assignment.js';
import { createPrimaryApiTransport, PrimaryApiError, type PrimaryApiOptions } from './primaryApi.js';
import { canonicalJson } from '../outbound/canonical.js';
import { parseRequestDeliveryJournal, requestDeliveryAssociationSchema, requestDeliveryHeaders,
  type RequestDeliveryJournal } from './requestDelivery.js';

export const ELIGIBLE_CONTEXT_MAX_TEXT_BYTES = 24_000;
export const ELIGIBLE_CONTEXT_MAX_MESSAGES = 20;
export const ELIGIBLE_CONTEXT_MAX_RESPONSE_BYTES = 65_536;
export const PHONE_ELIGIBLE_CONTEXT_HEADER = 'X-Dexter-Phone-Context-Version';
export const PHONE_ELIGIBLE_CONTEXT_VERSION = 'memory-eligible-v1';
const MAX_SEQUENCE = 9_223_372_036_854_775_807n;
const uuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
const callSid = z.string().regex(/^CA[0-9a-fA-F]{32}$/);
const digest = z.string().regex(/^[0-9a-f]{64}$/);
const timestamp = z.iso.datetime({ offset: true });
function sequenceValue(value: string): bigint | undefined {
  if (!/^(0|[1-9][0-9]{0,18})$/.test(value)) return undefined;
  const result = BigInt(value);
  return result <= MAX_SEQUENCE ? result : undefined;
}
export const historySequenceSchema = z.string().regex(/^(0|[1-9][0-9]{0,18})$/)
  .refine(value => sequenceValue(value) !== undefined);

export const historyVersionSchema = z.object({
  ownerPrincipalId: uuid, primaryDexterId: uuid, threadId: uuid,
  contextEpoch: historySequenceSchema, threadEventSequence: historySequenceSchema, digest,
}).strict();
export type HistoryVersion = z.infer<typeof historyVersionSchema>;

export const messageAttributionSchema = z.object({
  messageCreatedAt: timestamp, eventId: uuid, eventOccurredAt: timestamp, runId: uuid,
}).strict();
export const canonicalSourceSchema = z.object({
  sourceEventId: uuid, revision: z.number().int().min(1).max(2_147_483_647),
  occurredAt: timestamp, storedProjectionKind: z.enum(['verbatim', 'derived']),
}).strict();
export const eligiblePhoneMessageSchema = z.object({
  id: uuid, role: z.enum(['user', 'assistant']), text: z.string(),
  eventSequence: historySequenceSchema, provenance: messageAttributionSchema,
  canonicalSource: canonicalSourceSchema.optional(),
}).strict();
export type EligiblePhoneMessage = z.infer<typeof eligiblePhoneMessageSchema>;

export const phoneHistorySchema = z.object({
  version: historyVersionSchema, pageDigest: digest, localContext: z.string(),
  nextBeforeSequence: historySequenceSchema.nullable(), hasMore: z.boolean(), omittedForBudget: z.boolean(),
}).strict();
export const eligiblePhoneContextSchema = z.object({
  primaryDexterId: uuid, threadId: uuid, displayName: z.string().min(1).max(200),
  messages: z.array(eligiblePhoneMessageSchema).max(ELIGIBLE_CONTEXT_MAX_MESSAGES),
  history: phoneHistorySchema,
}).strict().superRefine((value, ctx) => {
  if (value.primaryDexterId !== value.history.version.primaryDexterId
    || value.threadId !== value.history.version.threadId) {
    ctx.addIssue({ code: 'custom', message: 'History scope differs from context' });
  }
  const bytes = Buffer.byteLength(value.history.localContext, 'utf8')
    + value.messages.reduce((total, message) => total + Buffer.byteLength(message.text, 'utf8'), 0);
  if (bytes > ELIGIBLE_CONTEXT_MAX_TEXT_BYTES) {
    ctx.addIssue({ code: 'custom', message: 'Eligible context exceeds text budget' });
  }
  // The reader returns whole eligible messages in ascending event order.
  let previous: bigint | undefined;
  const head = sequenceValue(value.history.version.threadEventSequence);
  const ids = new Set<string>();
  for (const message of value.messages) {
    const sequence = sequenceValue(message.eventSequence);
    if (sequence === undefined || head === undefined) continue;
    if (ids.has(message.id) || (previous !== undefined && sequence <= previous)
      || sequence > head) {
      ctx.addIssue({ code: 'custom', message: 'Invalid projected message identity or sequence' });
    }
    ids.add(message.id);
    previous = sequence;
  }
  const cursor = value.history.nextBeforeSequence === null ? undefined : sequenceValue(value.history.nextBeforeSequence);
  if (cursor !== undefined && head !== undefined && cursor > head) {
    ctx.addIssue({ code: 'custom', message: 'History cursor exceeds thread head' });
  }
});
export const eligiblePhoneCallSchema = z.object({
  bindingId: uuid, callSid, state: z.enum(['open', 'closed']), context: eligiblePhoneContextSchema,
  callAssignmentRef: phoneCallAssignmentRefSchema.optional(),
}).strict();
export type EligiblePhoneCall = z.infer<typeof eligiblePhoneCallSchema>;

export const eligibleContextBindingSchema = z.object({
  bindingId: uuid, callSid, primaryDexterId: uuid, threadId: uuid,
  ownerPrincipalId: uuid.optional(), callAssignmentRef: phoneCallAssignmentRefSchema.optional(),
}).strict();
/** The first authenticated eligible envelope can establish the owner; later reads pin it. */
export type EligibleContextBinding = z.infer<typeof eligibleContextBindingSchema>;

export const contextAssertionRequestSchema = z.object({ version: historyVersionSchema }).strict();
export const contextAssertionSchema = z.object({
  bindingId: uuid, callSid, current: z.literal(true), version: historyVersionSchema,
}).strict();
export type ContextAssertion = z.infer<typeof contextAssertionSchema>;

const questionText = (limit: number) => z.string().min(1).refine(value => value.trim() === value
  && Buffer.byteLength(value, 'utf8') <= limit
  && !/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/u.test(value)
  && Array.from(value).every(character => character.codePointAt(0)! < 0xd800 || character.codePointAt(0)! > 0xdfff));
// Delivery accepts only the pending branch of AgentQuestionV1. Original meanings
// remain structured data; the API proves their saved render survived projection.
export const eligiblePendingQuestionSchema = z.object({
  id: uuid, revision: z.literal(1), expiresAt: timestamp,
  runState: z.enum(['queued', 'running', 'waiting_for_reply', 'completed', 'failed', 'cancelled']),
  runId: uuid, threadId: uuid, agent: z.object({ id: uuid, name: questionText(512) }).strict(),
  question: questionText(400), context: questionText(1000), proposedAnswer: questionText(400),
  yesMeaning: questionText(400), noMeaning: questionText(400),
  state: z.literal('pending'), createdAt: timestamp, reply: z.null(),
}).strict();
export const eligibleDeliveryTargetSchema = z.discriminatedUnion('kind', [
  z.object({ kind: z.literal('assistant'), messageId: uuid }).strict(),
  z.object({ kind: z.literal('question'), questionId: uuid, questionRevision: z.literal(1) }).strict(),
]);
export type EligibleDeliveryTarget = z.infer<typeof eligibleDeliveryTargetSchema>;
export const eligibleDeliveryRequestSchema = z.object({
  version: historyVersionSchema, source: eligibleDeliveryTargetSchema,
}).strict();
export type EligibleDeliveryRequest = z.infer<typeof eligibleDeliveryRequestSchema>;

const readySourceFields = {
  messageId: uuid, eventSequence: historySequenceSchema,
  provenance: messageAttributionSchema, canonicalSource: canonicalSourceSchema.optional(),
};
const deliveredText = z.string().refine(value => Buffer.byteLength(value, 'utf8') <= ELIGIBLE_CONTEXT_MAX_TEXT_BYTES);
const eligibleDeliverySchema = z.union([
  z.object({ state: z.literal('ready'),
    source: z.object({ kind: z.literal('assistant'), ...readySourceFields }).strict(), text: deliveredText,
  }).strict(),
  z.object({ state: z.literal('ready'),
    source: z.object({ kind: z.literal('question'), ...readySourceFields }).strict(), text: deliveredText,
    question: eligiblePendingQuestionSchema,
  }).strict(),
  z.object({ state: z.literal('unavailable'), reason: z.enum(['not_ready', 'source_not_in_projection']) }).strict(),
]);
const deliveryResponseBaseSchema = z.object({
  bindingId: uuid, callSid, requestId: uuid, version: historyVersionSchema, delivery: eligibleDeliverySchema,
}).strict();
function checkDeliveryScope(value: z.infer<typeof deliveryResponseBaseSchema>, ctx: z.RefinementCtx): void {
  if (value.delivery.state !== 'ready') return;
  const sequence = sequenceValue(value.delivery.source.eventSequence);
  const head = sequenceValue(value.version.threadEventSequence);
  if (sequence !== undefined && head !== undefined && sequence > head) {
    ctx.addIssue({ code: 'custom', message: 'Delivery event exceeds thread head' });
  }
  if ('question' in value.delivery && (value.delivery.question.threadId !== value.version.threadId
    || value.delivery.question.runId !== value.delivery.source.provenance.runId)) {
    ctx.addIssue({ code: 'custom', message: 'Question source scope differs from delivery' });
  }
}
export const eligibleDeliveryResponseSchema = deliveryResponseBaseSchema.superRefine(checkDeliveryScope);
export const eligibleAssociatedDeliveryResponseSchema = deliveryResponseBaseSchema.extend({
  deliveryAssociation: requestDeliveryAssociationSchema,
}).strict().superRefine(checkDeliveryScope).superRefine((value, ctx) => {
  const association = value.deliveryAssociation;
  if (association.delivery.bindingId !== value.bindingId || association.delivery.callSid !== value.callSid
    || (value.delivery.state === 'ready' && value.delivery.source.provenance.runId !== association.execution.runId)
    || (value.delivery.state === 'ready' && 'question' in value.delivery
      && (value.delivery.question.id !== association.question.id || value.delivery.question.revision !== association.question.revision)))
    ctx.addIssue({ code: 'custom', message: 'Delivery association scope differs from source' });
});
export type EligibleDeliveryResponse = z.infer<typeof eligibleDeliveryResponseSchema>
  | z.infer<typeof eligibleAssociatedDeliveryResponseSchema>;

function parse<T>(schema: z.ZodType<T>, value: unknown, code: 'invalid_request' | 'invalid_response'): T {
  const parsed = schema.safeParse(value);
  if (!parsed.success) throw new PrimaryApiError(code);
  return parsed.data;
}
function same(actual: unknown, expected: unknown): void {
  if (actual !== expected) throw new PrimaryApiError('identity_mismatch');
}
function checkScope(binding: EligibleContextBinding, version: HistoryVersion): void {
  same(version.primaryDexterId, binding.primaryDexterId);
  same(version.threadId, binding.threadId);
  if (binding.ownerPrincipalId !== undefined) same(version.ownerPrincipalId, binding.ownerPrincipalId);
}
export function sameHistoryVersion(actual: HistoryVersion, expected: HistoryVersion): boolean {
  return actual.ownerPrincipalId === expected.ownerPrincipalId && actual.primaryDexterId === expected.primaryDexterId
    && actual.threadId === expected.threadId && actual.contextEpoch === expected.contextEpoch
    && actual.threadEventSequence === expected.threadEventSequence && actual.digest === expected.digest;
}

/** Use on an authenticated registration envelope before loading any returned text. */
export function parseEligiblePhoneCall(value: unknown, expected: EligibleContextBinding): EligiblePhoneCall {
  const binding = parse(eligibleContextBindingSchema, expected, 'invalid_request');
  const call = parse(eligiblePhoneCallSchema, value, 'invalid_response');
  same(call.bindingId, binding.bindingId);
  same(call.callSid, binding.callSid);
  checkScope(binding, call.context.history.version);
  if (binding.callAssignmentRef !== undefined && !sameCallAssignmentRef(call.callAssignmentRef, binding.callAssignmentRef)) {
    throw new PrimaryApiError('identity_mismatch');
  }
  return call;
}

export interface EligibleContextClient {
  fetch(binding: EligibleContextBinding): Promise<EligiblePhoneCall>;
  assert(binding: EligibleContextBinding, version: HistoryVersion): Promise<ContextAssertion>;
  delivery(binding: EligibleContextBinding, requestId: string, input: EligibleDeliveryRequest,
    laterCall?: RequestDeliveryJournal): Promise<EligibleDeliveryResponse>;
}

export function createEligibleContextClient(options: PrimaryApiOptions): EligibleContextClient {
  const request = createPrimaryApiTransport({ ...options,
    maxBodyBytes: options.maxBodyBytes ?? ELIGIBLE_CONTEXT_MAX_RESPONSE_BYTES });
  const knownBindings = new Map<string, EligibleContextBinding & { ownerPrincipalId: string }>();
  function checkedBinding(value: EligibleContextBinding): EligibleContextBinding {
    const binding = parse(eligibleContextBindingSchema, value, 'invalid_request');
    const known = knownBindings.get(binding.bindingId);
    if (known) {
      same(binding.callSid, known.callSid);
      same(binding.primaryDexterId, known.primaryDexterId);
      same(binding.threadId, known.threadId);
      if (binding.ownerPrincipalId !== undefined) same(binding.ownerPrincipalId, known.ownerPrincipalId);
      if (binding.callAssignmentRef !== undefined && !sameCallAssignmentRef(binding.callAssignmentRef, known.callAssignmentRef)) {
        throw new PrimaryApiError('identity_mismatch');
      }
      return { ...binding, ownerPrincipalId: known.ownerPrincipalId, callAssignmentRef: known.callAssignmentRef };
    }
    return binding;
  }
  function remember(binding: EligibleContextBinding, version: HistoryVersion): void {
    // Recheck after HTTP as concurrent first reads may have pinned the binding.
    const current = checkedBinding(binding);
    checkScope(current, version);
    knownBindings.set(binding.bindingId, { ...current, ownerPrincipalId: version.ownerPrincipalId });
  }
  function checkedVersion(binding: EligibleContextBinding, value: HistoryVersion): HistoryVersion {
    const version = parse(historyVersionSchema, value, 'invalid_request');
    checkScope(binding, version);
    return version;
  }
  const headers = (binding: EligibleContextBinding) => ({
    'X-Dexter-Binding-Id': binding.bindingId,
    // Selected by this authenticated client, never a caller-supplied body field.
    [PHONE_ELIGIBLE_CONTEXT_HEADER]: PHONE_ELIGIBLE_CONTEXT_VERSION,
  });
  return {
    async fetch(expected) {
      const binding = checkedBinding(expected);
      const value = await request('GET', `/calls/${binding.bindingId}/context`, undefined, headers(binding));
      const current = checkedBinding(binding);
      const call = parseEligiblePhoneCall(value, current);
      const observed = { ...current, callAssignmentRef: call.callAssignmentRef };
      remember(observed, call.context.history.version);
      return call;
    },
    async assert(expected, inputVersion) {
      const binding = checkedBinding(expected);
      const version = checkedVersion(binding, inputVersion);
      const value = await request('POST', `/calls/${binding.bindingId}/context/assert`, { version }, headers(binding));
      const result = parse(contextAssertionSchema, value, 'invalid_response');
      same(result.bindingId, binding.bindingId);
      same(result.callSid, binding.callSid);
      if (!sameHistoryVersion(result.version, version)) throw new PrimaryApiError('identity_mismatch');
      remember(binding, version);
      return result;
    },
    async delivery(expected, inputRequestId, input, laterCall) {
      const binding = checkedBinding(expected);
      const requestId = parse(uuid, inputRequestId, 'invalid_request');
      const body = parse(eligibleDeliveryRequestSchema, input, 'invalid_request');
      checkedVersion(binding, body.version);
      const journal = laterCall === undefined ? undefined : parseRequestDeliveryJournal(laterCall);
      if (journal) {
        const association = journal.association;
        if (!association) throw new PrimaryApiError('identity_mismatch');
        same(association.delivery.bindingId, binding.bindingId);
        same(association.delivery.callSid, binding.callSid);
        same(journal.scope.ownerPrincipalId, body.version.ownerPrincipalId);
        same(journal.scope.primaryDexterId, body.version.primaryDexterId);
        same(journal.scope.threadId, body.version.threadId);
        if (requestId !== association.execution.requestId && requestId !== journal.request.originalRequestId)
          throw new PrimaryApiError('identity_mismatch');
        if (body.source.kind === 'question') {
          same(body.source.questionId, association.question.id);
          same(body.source.questionRevision, association.question.revision);
        }
      }
      const value = await request('POST', `/requests/${requestId}/delivery`, body,
        { ...headers(binding), ...(journal ? requestDeliveryHeaders(journal) : {}) });
      const result: EligibleDeliveryResponse = journal
        ? parse(eligibleAssociatedDeliveryResponseSchema, value, 'invalid_response')
        : parse(eligibleDeliveryResponseSchema, value, 'invalid_response');
      if (journal) {
        if (!('deliveryAssociation' in result)
          || canonicalJson(result.deliveryAssociation) !== canonicalJson(journal.association))
          throw new PrimaryApiError('identity_mismatch');
      }
      same(result.bindingId, binding.bindingId);
      same(result.callSid, binding.callSid);
      same(result.requestId, requestId);
      if (!sameHistoryVersion(result.version, body.version)) throw new PrimaryApiError('identity_mismatch');
      if (result.delivery.state === 'ready') {
        same(result.delivery.source.kind, body.source.kind);
        if (body.source.kind === 'assistant') same(result.delivery.source.messageId, body.source.messageId);
        else {
          if (!('question' in result.delivery)) throw new PrimaryApiError('identity_mismatch');
          same(result.delivery.question.id, body.source.questionId);
          same(result.delivery.question.revision, body.source.questionRevision);
        }
      }
      remember(binding, body.version);
      return result;
    },
  };
}
