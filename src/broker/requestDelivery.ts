import { z } from 'zod';
import { canonicalJson, sha256Hex } from '../outbound/canonical.js';
import { createPrimaryApiTransport, PrimaryApiError, type PrimaryApiOptions,
  type PrimaryTurn, type PrimaryTurnReceipt, type PrimaryReplyInput,
  type PrimaryRequestResult, type PrimaryQuestionResult,
  requestSchema, questionResultSchema, replyInputSchema } from './primaryApi.js';
import { acknowledgedTurn, turnBodyDigest } from './reconciliation.js';

export const PHONE_REQUEST_DELIVERY_VERSION = 'phone.primary-request-delivery.v1';
export const PHONE_REQUEST_DELIVERY_HEADER = 'X-Dexter-Phone-Request-Delivery-Id';
const CONTEXT_HEADER = 'X-Dexter-Phone-Context-Version';
const CONTEXT_VERSION = 'memory-eligible-v1';
// Existing Phone wire IDs are already normalized. Never rewrite retained IDs.
const uuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
const callSid = z.string().regex(/^CA[0-9a-fA-F]{32}$/);
const timestamp = z.iso.datetime({ offset: true });
const digest = z.string().regex(/^[0-9a-f]{64}$/);
const itemId = z.string().regex(/^[a-zA-Z0-9_-]{1,128}$/);
const deliverySchema = z.object({ bindingId: uuid, callSid }).strict();
const executionSchema = z.object({ bindingId: uuid, requestId: uuid, runId: uuid }).strict();
const questionSchema = z.object({ id: uuid, revision: z.literal(1) }).strict();

export const requestDeliveryRequestSchema = z.object({
  v: z.literal(PHONE_REQUEST_DELIVERY_VERSION), originalRequestId: uuid,
  questionId: uuid, questionRevision: z.literal(1),
}).strict();
export type RequestDeliveryRequest = z.infer<typeof requestDeliveryRequestSchema>;

export const requestDeliveryAssociationSchema = z.object({
  id: uuid, v: z.literal(PHONE_REQUEST_DELIVERY_VERSION),
  delivery: deliverySchema, execution: executionSchema, question: questionSchema, createdAt: timestamp,
}).strict().refine(value => value.delivery.bindingId !== value.execution.bindingId);
export type RequestDeliveryAssociation = z.infer<typeof requestDeliveryAssociationSchema>;
export const requestDeliveryResponseSchema = z.object({
  association: requestDeliveryAssociationSchema, replayed: z.boolean(),
}).strict();
export type RequestDeliveryResponse = z.infer<typeof requestDeliveryResponseSchema>;
const associatedFields = { deliveryAssociation: requestDeliveryAssociationSchema };
export const requestDeliveryResultSchema = z.discriminatedUnion('state', [
  requestSchema.options[0].extend(associatedFields).strict(),
  requestSchema.options[1].extend(associatedFields).strict(),
  requestSchema.options[2].extend(associatedFields).strict(),
  requestSchema.options[3].extend(associatedFields).strict(),
]);
export const requestDeliveryQuestionSchema = questionResultSchema.extend(associatedFields).strict();
export type RequestDeliveryResult = PrimaryRequestResult & { deliveryAssociation: RequestDeliveryAssociation };
export type RequestDeliveryQuestionResult = PrimaryQuestionResult & { deliveryAssociation: RequestDeliveryAssociation };

const targetSchema = z.object({ delivery: deliverySchema, execution: executionSchema, question: questionSchema })
  .strict().refine(value => value.delivery.bindingId !== value.execution.bindingId);
/** The canonical request must come from saved API evidence, not alias inference. */
export type RequestDeliveryTarget = z.infer<typeof targetSchema>;
const journalInputSchema = z.object({
  deliveryCall: z.object({ callId: uuid, sessionId: z.string().min(1).max(200) }).strict(),
  scope: z.object({ ownerPrincipalId: uuid, primaryDexterId: uuid, threadId: uuid }).strict(),
  target: targetSchema,
  request: requestDeliveryRequestSchema,
}).strict();
export type RequestDeliveryJournalInput = z.infer<typeof journalInputSchema>;
const callerSourceSchema = z.object({
  bindingId: uuid, callSid, sourceEventId: uuid, revision: z.literal(1), itemId,
  role: z.literal('user'), occurredAt: timestamp,
  recordId: uuid, revisionRecordId: uuid, bodyDigest: digest,
}).strict();
export type RequestDeliveryCallerSource = z.infer<typeof callerSourceSchema>;
const journalSchema = journalInputSchema.extend({
  v: z.literal('broker.phone-request-delivery.v1'),
  association: requestDeliveryAssociationSchema.optional(),
  callerSource: callerSourceSchema.optional(),
  reply: z.object({ input: replyInputSchema, localDigest: digest }).strict().optional(),
}).strict();
export type RequestDeliveryJournal = z.infer<typeof journalSchema>;

function parse<T>(schema: z.ZodType<T>, value: unknown, code: 'invalid_request' | 'invalid_response'): T {
  const result = schema.safeParse(value);
  if (!result.success) throw new PrimaryApiError(code);
  return result.data;
}
function same(actual: unknown, expected: unknown): void {
  if (canonicalJson(actual) !== canonicalJson(expected)) throw new PrimaryApiError('identity_mismatch');
}
function checkRequest(input: RequestDeliveryJournalInput): void {
  same(input.request.questionId, input.target.question.id);
  same(input.request.questionRevision, input.target.question.revision);
}
function checkAssociation(target: RequestDeliveryTarget, association: RequestDeliveryAssociation): void {
  same(association.delivery, target.delivery);
  same(association.execution, target.execution);
  same(association.question, target.question);
}
function replyTurn(input: PrimaryReplyInput): PrimaryTurn {
  return { sourceEventId: input.source.sourceEventId, revision: input.source.revision, itemId: input.source.itemId,
    role: input.role, text: input.text, occurredAt: input.source.receivedAt };
}
function checkReplySource(journal: RequestDeliveryJournal, input: PrimaryReplyInput): void {
  if (!journal.association || !journal.callerSource) throw new PrimaryApiError('identity_mismatch');
  same(input.source.callSid, journal.association.delivery.callSid);
  same(input.reply.questionRevision, journal.association.question.revision);
  same({ sourceEventId: input.source.sourceEventId, revision: input.source.revision,
    itemId: input.source.itemId, occurredAt: input.source.receivedAt, role: input.role },
  { sourceEventId: journal.callerSource.sourceEventId, revision: journal.callerSource.revision,
    itemId: journal.callerSource.itemId, occurredAt: journal.callerSource.occurredAt, role: journal.callerSource.role });
  same(turnBodyDigest(replyTurn(input)), journal.callerSource.bodyDigest);
}
function localReplyDigest(associationId: string, input: PrimaryReplyInput): string {
  // Local journal checksum only. The API owns its distinct persisted reply digest.
  return sha256Hex(canonicalJson({ v: 'broker.phone-request-delivery-reply.v1', associationId, input }));
}

/** Persist this separate delivery journal before attempting its association POST. */
export function createRequestDeliveryJournal(input: RequestDeliveryJournalInput): RequestDeliveryJournal {
  const checked = parse(journalInputSchema, input, 'invalid_request');
  checkRequest(checked);
  return { v: 'broker.phone-request-delivery.v1', ...checked };
}

/** Strict restart validation; neither old delegation records nor missing IDs are upgraded here. */
export function parseRequestDeliveryJournal(value: unknown): RequestDeliveryJournal {
  const journal = parse(journalSchema, value, 'invalid_request');
  checkRequest(journal);
  if (journal.association) checkAssociation(journal.target, journal.association);
  if (journal.callerSource) {
    if (!journal.association) throw new PrimaryApiError('identity_mismatch');
    same(journal.callerSource.bindingId, journal.association.delivery.bindingId);
    same(journal.callerSource.callSid, journal.association.delivery.callSid);
    same(journal.callerSource.recordId, journal.callerSource.sourceEventId);
  }
  if (journal.reply) {
    checkReplySource(journal, journal.reply.input);
    same(journal.reply.localDigest, localReplyDigest(journal.association!.id, journal.reply.input));
  }
  return journal;
}

/** Store integration must compare the saved entry before replacing it with a reducer result. */
export function assertRequestDeliveryJournalUpdate(previous: RequestDeliveryJournal,
  next: RequestDeliveryJournal): RequestDeliveryJournal {
  const original = parseRequestDeliveryJournal(previous);
  const updated = parseRequestDeliveryJournal(next);
  same(updated.deliveryCall, original.deliveryCall);
  same(updated.scope, original.scope);
  same(updated.target, original.target);
  same(updated.request, original.request);
  if (original.association) {
    if (!updated.association) throw new PrimaryApiError('identity_mismatch');
    same(updated.association, original.association);
  }
  if (original.callerSource) {
    if (!updated.callerSource) throw new PrimaryApiError('identity_mismatch');
    same(updated.callerSource, original.callerSource);
  }
  if (original.reply) {
    if (!updated.reply) throw new PrimaryApiError('identity_mismatch');
    same(updated.reply, original.reply);
  }
  return updated;
}

/** Association recovery never changes the original execution, supplied alias, or source. */
export function retainRequestDeliveryAssociation(value: RequestDeliveryJournal,
  input: RequestDeliveryResponse): RequestDeliveryJournal {
  const journal = parseRequestDeliveryJournal(value);
  const response = parse(requestDeliveryResponseSchema, input, 'invalid_response');
  checkAssociation(journal.target, response.association);
  if (journal.association) same(response.association, journal.association);
  return assertRequestDeliveryJournalUpdate(journal, { ...journal, association: response.association });
}

const capturedTurnSchema = z.object({
  sourceEventId: uuid, revision: z.literal(1), itemId, role: z.literal('user'),
  text: z.string().refine(value => Boolean(value.trim()) && !value.includes('\0')
    && Array.from(value).length <= 4000
    && Array.from(value).every(c => c.codePointAt(0)! < 0xd800 || c.codePointAt(0)! > 0xdfff)),
  occurredAt: timestamp,
}).strict();
const captureReceiptSchema = z.object({
  bindingId: uuid, sourceEventId: uuid, revision: z.literal(1), recordId: uuid,
  replayed: z.boolean(), revisionRecordId: uuid, bodyDigest: digest,
}).strict();

/** Retain an acknowledged new-call caller source; capture is not answer consent or submission. */
export function retainRequestDeliveryCallerSource(value: RequestDeliveryJournal, input: PrimaryTurn,
  inputReceipt: PrimaryTurnReceipt): RequestDeliveryJournal {
  const journal = parseRequestDeliveryJournal(value);
  if (!journal.association) throw new PrimaryApiError('identity_mismatch');
  const turn = parse(capturedTurnSchema, input, 'invalid_request');
  const receipt = parse(captureReceiptSchema, inputReceipt, 'invalid_response');
  try { acknowledgedTurn(journal.association.delivery.bindingId, turn, receipt); }
  catch { throw new PrimaryApiError('identity_mismatch'); }
  const source: RequestDeliveryCallerSource = {
    bindingId: journal.association.delivery.bindingId, callSid: journal.association.delivery.callSid,
    sourceEventId: turn.sourceEventId, revision: 1, itemId: turn.itemId, role: 'user', occurredAt: turn.occurredAt,
    recordId: receipt.recordId, revisionRecordId: receipt.revisionRecordId, bodyDigest: receipt.bodyDigest,
  };
  if (journal.callerSource) same(source, journal.callerSource);
  return assertRequestDeliveryJournalUpdate(journal, { ...journal, callerSource: source });
}

/** Called for an explicit owner reply, then persisted before the separate reply HTTP attempt. */
export function retainRequestDeliveryReply(value: RequestDeliveryJournal, input: PrimaryReplyInput): RequestDeliveryJournal {
  const journal = parseRequestDeliveryJournal(value);
  const reply = parse(replyInputSchema, input, 'invalid_request');
  checkReplySource(journal, reply);
  const saved = { input: reply, localDigest: localReplyDigest(journal.association!.id, reply) };
  if (journal.reply) same(saved, journal.reply);
  return assertRequestDeliveryJournalUpdate(journal, { ...journal, reply: saved });
}

/** Use only with separately qualified later-call readers; same-call paths omit this header. */
export function requestDeliveryHeaders(value: RequestDeliveryJournal): Record<string, string> {
  const journal = parseRequestDeliveryJournal(value);
  if (!journal.association) throw new PrimaryApiError('identity_mismatch');
  return { 'X-Dexter-Binding-Id': journal.association.delivery.bindingId,
    [CONTEXT_HEADER]: CONTEXT_VERSION, [PHONE_REQUEST_DELIVERY_HEADER]: journal.association.id };
}

export interface RequestDeliveryClient {
  associate(journal: RequestDeliveryJournal): Promise<RequestDeliveryResponse>;
  result(journal: RequestDeliveryJournal): Promise<RequestDeliveryResult>;
  question(journal: RequestDeliveryJournal): Promise<RequestDeliveryQuestionResult>;
  reply(journal: RequestDeliveryJournal): Promise<RequestDeliveryQuestionResult>;
}

function checkResult(journal: RequestDeliveryJournal,
  result: RequestDeliveryResult | RequestDeliveryQuestionResult, exactQuestion = true): void {
  if (!journal.association) throw new PrimaryApiError('identity_mismatch');
  same(result.deliveryAssociation, journal.association);
  same(result.bindingId, journal.association.execution.bindingId);
  same(result.requestId, journal.request.originalRequestId);
  if (result.question) {
    if (exactQuestion) same(result.question.id, journal.association.question.id);
    same(result.question.revision, journal.association.question.revision);
    same(result.question.runId, journal.association.execution.runId);
    same(result.question.threadId, journal.scope.threadId);
    same(result.question.agent.id, journal.scope.primaryDexterId);
    same(result.question.runState, result.state);
  }
}

/** One HTTP attempt; the caller persists the journal and invokes exact recovery explicitly. */
export function createRequestDeliveryClient(options: PrimaryApiOptions): RequestDeliveryClient {
  const request = createPrimaryApiTransport(options);
  return {
    async associate(value) {
      const journal = parseRequestDeliveryJournal(value);
      const bindingId = journal.target.delivery.bindingId;
      const result = await request('POST', `/calls/${bindingId}/request-deliveries`, journal.request,
        { 'X-Dexter-Binding-Id': bindingId, [CONTEXT_HEADER]: CONTEXT_VERSION });
      const response = parse(requestDeliveryResponseSchema, result, 'invalid_response');
      retainRequestDeliveryAssociation(journal, response);
      return response;
    },
    async result(value) {
      const journal = parseRequestDeliveryJournal(value);
      const headers = requestDeliveryHeaders(journal);
      const result = parse(requestDeliveryResultSchema,
        await request('GET', `/requests/${journal.request.originalRequestId}`, undefined, headers), 'invalid_response');
      // The ordinary request reader may discover a later question on this run.
      // Presenting/replying to it still needs its own exact association.
      checkResult(journal, result, false);
      if (result.state === 'waiting_for_reply' && !['pending', 'expired'].includes(result.question.state))
        throw new PrimaryApiError('invalid_response');
      return result;
    },
    async question(value) {
      const journal = parseRequestDeliveryJournal(value);
      const headers = requestDeliveryHeaders(journal);
      const result = parse(requestDeliveryQuestionSchema, await request('GET',
        `/requests/${journal.request.originalRequestId}/questions/${journal.target.question.id}`, undefined, headers), 'invalid_response');
      checkResult(journal, result);
      return result;
    },
    async reply(value) {
      const journal = parseRequestDeliveryJournal(value);
      const headers = requestDeliveryHeaders(journal);
      if (!journal.reply) throw new PrimaryApiError('invalid_request');
      const input = journal.reply.input;
      const result = parse(requestDeliveryQuestionSchema, await request('POST',
        `/requests/${journal.request.originalRequestId}/questions/${journal.target.question.id}/reply`, input, headers), 'invalid_response');
      checkResult(journal, result);
      const reply = result.question.reply;
      if (typeof result.replayed !== 'boolean' || !reply) throw new PrimaryApiError('invalid_response');
      same({ questionRevision: reply.questionRevision, clientReplyId: reply.clientReplyId, answer: reply.answer, text: reply.text }, input.reply);
      return result;
    },
  };
}

const nonAdmissionCodes: Record<number, readonly string[]> = {
  400: ['invalid_request'],
  401: ['invalid_service_credential'],
  403: ['caller_possession_required', 'binding_unauthorized', 'caller_unlinked', 'caller_binding_ambiguous'],
  404: ['request_not_found', 'question_not_found'],
  409: ['delivery_association_mismatch', 'original_call_active', 'call_closed', 'source_conflict', 'idempotency_conflict',
    'question_cancelled', 'already_answered', 'question_already_answered', 'question_expired', 'question_unavailable'],
  410: ['question_expired'],
  415: ['unsupported_media_type'],
};
const refusalSchema = z.object({ kind: z.literal('first_attempt_refusal'),
  origin: z.enum(['local_validation', 'api_non_admission']), code: z.string(), status: z.number().int().optional(), observedAt: timestamp,
}).strict().refine(value => value.origin === 'local_validation'
  ? value.code === 'invalid_request' && value.status === undefined
  : value.status !== undefined && nonAdmissionCodes[value.status]?.includes(value.code) === true);
export const undispatchedReplyRefusalSchema = z.object({ kind: z.literal('never_dispatched'),
  code: z.enum(['call_ended', 'presentation_invalidated', 'question_not_pending', 'question_expired']), observedAt: timestamp,
}).strict();
export type UndispatchedReplyRefusalCode = z.infer<typeof undispatchedReplyRefusalSchema>['code'];
const questionObservationSchema = z.object({ value: requestDeliveryQuestionSchema, observedAt: timestamp }).strict();
const attemptSchema = z.object({
  sequence: z.number().int().safe().positive(), journal: journalSchema,
  state: z.enum(['prepared', 'dispatching', 'unknown', 'refused', 'accepted']),
  dispatches: z.number().int().safe().nonnegative(), recovery: z.boolean(), lastDispatchAt: timestamp.optional(),
  freshQuestion: questionObservationSchema,
  refusal: refusalSchema.optional(), localRefusal: undispatchedReplyRefusalSchema.optional(), terminalQuestion: questionObservationSchema.optional(),
  accepted: questionObservationSchema.optional(),
}).strict();
const questionRecordSchema = z.object({
  v: z.literal('broker.phone-request-delivery-attempts.v1'), base: journalSchema, attempts: z.array(attemptSchema),
}).strict();
export type RequestDeliveryReplyAttempt = z.infer<typeof attemptSchema>;
export type RequestDeliveryQuestionRecord = z.infer<typeof questionRecordSchema>;

function journalBase(journal: RequestDeliveryJournal): RequestDeliveryJournal {
  const { callerSource: _source, reply: _reply, ...base } = journal;
  return base;
}
function exactReply(journal: RequestDeliveryJournal, result: RequestDeliveryQuestionResult): boolean {
  const reply = result.question.reply;
  return Boolean(reply && journal.reply && canonicalJson({ questionRevision: reply.questionRevision,
    clientReplyId: reply.clientReplyId, answer: reply.answer, text: reply.text }) === canonicalJson(journal.reply.input.reply));
}
function checkPendingQuestion(journal: RequestDeliveryJournal, observation: z.infer<typeof questionObservationSchema>): void {
  checkResult(journal, observation.value);
  if (observation.value.question.state !== 'pending' || observation.value.state !== 'waiting_for_reply'
    || Date.parse(observation.observedAt) >= Date.parse(observation.value.question.expiresAt))
    throw new PrimaryApiError('identity_mismatch');
}

/** Each journal remains immutable; only the final unresolved attempt can advance. */
export function parseRequestDeliveryQuestionRecord(value: unknown): RequestDeliveryQuestionRecord {
  const record = parse(questionRecordSchema, value, 'invalid_request');
  const base = parseRequestDeliveryJournal(record.base);
  if (!base.association || base.callerSource || base.reply) throw new PrimaryApiError('identity_mismatch');
  const sources = new Set<string>();
  const replyIds = new Set<string>();
  for (const [index, attempt] of record.attempts.entries()) {
    const journal = parseRequestDeliveryJournal(attempt.journal);
    same(journalBase(journal), base);
    if (attempt.sequence !== index + 1 || !journal.reply || !journal.callerSource
      || sources.has(journal.callerSource.sourceEventId) || replyIds.has(journal.reply.input.reply.clientReplyId))
      throw new PrimaryApiError('identity_mismatch');
    sources.add(journal.callerSource.sourceEventId); replyIds.add(journal.reply.input.reply.clientReplyId);
    checkPendingQuestion(journal, attempt.freshQuestion);
    const previous = record.attempts[index - 1];
    if (previous && (previous.state !== 'refused' || !(previous.refusal || previous.localRefusal) || previous.terminalQuestion
      || Date.parse(attempt.freshQuestion.observedAt) < Date.parse((previous.refusal ?? previous.localRefusal)!.observedAt)))
      throw new PrimaryApiError('identity_mismatch');
    if ((attempt.dispatches === 0) !== (attempt.lastDispatchAt === undefined)
      || (attempt.state === 'prepared' && (attempt.dispatches !== 0 || attempt.recovery))
      || (attempt.state !== 'prepared' && attempt.dispatches === 0 && !attempt.localRefusal)
      || (attempt.dispatches > 1 && !attempt.recovery)
      || (attempt.state === 'unknown' && !attempt.recovery)) throw new PrimaryApiError('identity_mismatch');
    if (attempt.localRefusal && (attempt.state !== 'refused' || attempt.dispatches !== 0 || attempt.recovery
      || attempt.refusal || attempt.terminalQuestion || attempt.accepted
      || Date.parse(attempt.localRefusal.observedAt) < Date.parse(attempt.freshQuestion.observedAt)))
      throw new PrimaryApiError('identity_mismatch');
    if (attempt.refusal && (attempt.state !== 'refused' || attempt.dispatches !== 1 || attempt.recovery))
      throw new PrimaryApiError('identity_mismatch');
    if (attempt.terminalQuestion) {
      checkResult(journal, attempt.terminalQuestion.value);
      if (attempt.state !== 'refused' || !['answered', 'expired', 'cancelled'].includes(attempt.terminalQuestion.value.question.state)
        || exactReply(journal, attempt.terminalQuestion.value)) throw new PrimaryApiError('identity_mismatch');
    }
    if (attempt.accepted) {
      checkResult(journal, attempt.accepted.value);
      if (attempt.state !== 'accepted' || !exactReply(journal, attempt.accepted.value)) throw new PrimaryApiError('identity_mismatch');
    }
    if ((attempt.state === 'refused') !== Boolean(attempt.refusal || attempt.localRefusal || attempt.terminalQuestion)
      || (attempt.state === 'accepted') !== Boolean(attempt.accepted)
      || (attempt.refusal && attempt.terminalQuestion)) throw new PrimaryApiError('identity_mismatch');
  }
  return record;
}

/** The store applies this comparison before persisting an attempt collection. */
export function assertRequestDeliveryQuestionRecordUpdate(previous: RequestDeliveryQuestionRecord,
  next: RequestDeliveryQuestionRecord): RequestDeliveryQuestionRecord {
  const original = parseRequestDeliveryQuestionRecord(previous);
  const updated = parseRequestDeliveryQuestionRecord(next);
  same(updated.base, original.base);
  if (updated.attempts.length < original.attempts.length || updated.attempts.length > original.attempts.length + 1)
    throw new PrimaryApiError('identity_mismatch');
  const appended = updated.attempts.length > original.attempts.length;
  if (appended && updated.attempts.at(-1)!.state !== 'prepared') throw new PrimaryApiError('identity_mismatch');
  for (const [index, before] of original.attempts.entries()) {
    const after = updated.attempts[index]!;
    same(after.journal, before.journal); same(after.sequence, before.sequence); same(after.freshQuestion, before.freshQuestion);
    if (appended || index < original.attempts.length - 1 || ['accepted', 'refused'].includes(before.state)) {
      same(after, before); continue;
    }
    if (canonicalJson(after) === canonicalJson(before)) continue;
    const dispatched = after.state === 'dispatching' && after.dispatches === before.dispatches + 1
      && after.recovery === (before.state !== 'prepared');
    const observed = before.state !== 'prepared' && after.dispatches === before.dispatches
      && after.lastDispatchAt === before.lastDispatchAt && ['unknown', 'accepted', 'refused'].includes(after.state)
      && (!before.recovery || after.recovery)
      && (!after.refusal || (before.state === 'dispatching' && !before.recovery));
    const neverDispatched = before.state === 'prepared' && after.state === 'refused' && Boolean(after.localRefusal)
      && after.dispatches === 0 && !after.recovery && after.lastDispatchAt === undefined;
    if (!dispatched && !observed && !neverDispatched) throw new PrimaryApiError('identity_mismatch');
  }
  return updated;
}

export function createRequestDeliveryQuestionRecord(base: RequestDeliveryJournal): RequestDeliveryQuestionRecord {
  return parseRequestDeliveryQuestionRecord({ v: 'broker.phone-request-delivery-attempts.v1', base, attempts: [] });
}

/** The caller supplies a fresh exact-question read and a new explicit, captured reply. */
export function appendRequestDeliveryReplyAttempt(value: RequestDeliveryQuestionRecord, journal: RequestDeliveryJournal,
  question: RequestDeliveryQuestionResult, observedAt: string): RequestDeliveryQuestionRecord {
  const record = parseRequestDeliveryQuestionRecord(value);
  const attempt: RequestDeliveryReplyAttempt = { sequence: record.attempts.length + 1,
    journal: parseRequestDeliveryJournal(journal), state: 'prepared', dispatches: 0, recovery: false,
    freshQuestion: parse(questionObservationSchema, { value: question, observedAt }, 'invalid_response') };
  return assertRequestDeliveryQuestionRecordUpdate(record, { ...record, attempts: [...record.attempts, attempt] });
}

function updateLastAttempt(record: RequestDeliveryQuestionRecord, attempt: RequestDeliveryReplyAttempt): RequestDeliveryQuestionRecord {
  return assertRequestDeliveryQuestionRecordUpdate(record,
    { ...record, attempts: [...record.attempts.slice(0, -1), attempt] });
}
function lastAttempt(record: RequestDeliveryQuestionRecord): RequestDeliveryReplyAttempt {
  const attempt = record.attempts.at(-1);
  if (!attempt) throw new PrimaryApiError('invalid_request');
  return attempt;
}

/** Persist before POST. A retry after dispatch/uncertainty uses the exact old journal. */
export function beginRequestDeliveryReplyAttempt(value: RequestDeliveryQuestionRecord, observedAt: string): RequestDeliveryQuestionRecord {
  const record = parseRequestDeliveryQuestionRecord(value);
  const last = lastAttempt(record);
  if (['accepted', 'refused'].includes(last.state)) throw new PrimaryApiError('identity_mismatch');
  return updateLastAttempt(record, { ...last, state: 'dispatching', dispatches: last.dispatches + 1,
    recovery: last.state !== 'prepared', lastDispatchAt: parse(timestamp, observedAt, 'invalid_request') });
}

/** Local stop may settle only an attempt whose dispatch marker has never been written. */
export function refuseUndispatchedRequestDeliveryReply(value: RequestDeliveryQuestionRecord,
  code: UndispatchedReplyRefusalCode, observedAt: string): RequestDeliveryQuestionRecord {
  const record = parseRequestDeliveryQuestionRecord(value); const last = lastAttempt(record);
  const localRefusal = parse(undispatchedReplyRefusalSchema, { kind: 'never_dispatched', code, observedAt }, 'invalid_request');
  if (last.localRefusal) { same(last.localRefusal, localRefusal); return record; }
  if (last.state !== 'prepared' || last.dispatches !== 0) throw new PrimaryApiError('identity_mismatch');
  return updateLastAttempt(record, { ...last, state: 'refused', localRefusal });
}

/** A refusal from a retry cannot prove an earlier uncertain POST was uncommitted. */
export function observeRequestDeliveryReplyFailure(value: RequestDeliveryQuestionRecord, error: unknown,
  observedAt: string): RequestDeliveryQuestionRecord {
  const record = parseRequestDeliveryQuestionRecord(value); const last = lastAttempt(record);
  if (last.state !== 'dispatching') throw new PrimaryApiError('identity_mismatch');
  const at = parse(timestamp, observedAt, 'invalid_request');
  let refusal: z.infer<typeof refusalSchema> | undefined;
  if (last.dispatches === 1 && !last.recovery && error instanceof PrimaryApiError) {
    if (error.code === 'invalid_request') refusal = { kind: 'first_attempt_refusal', origin: 'local_validation', code: error.code, observedAt: at };
    else if (error.code === 'http_error' && error.retryable === false && error.status !== undefined
      && error.remoteCode && nonAdmissionCodes[error.status]?.includes(error.remoteCode)) {
      refusal = { kind: 'first_attempt_refusal', origin: 'api_non_admission', code: error.remoteCode, status: error.status, observedAt: at };
    }
  }
  return updateLastAttempt(record, refusal ? { ...last, state: 'refused', refusal }
    : { ...last, state: 'unknown', recovery: true });
}

/** GET or exact POST recovery may prove the saved answer, another answer, or a terminal question. */
export function observeRequestDeliveryReplyResult(value: RequestDeliveryQuestionRecord,
  response: RequestDeliveryQuestionResult, observedAt: string): RequestDeliveryQuestionRecord {
  const record = parseRequestDeliveryQuestionRecord(value); const last = lastAttempt(record);
  const observation = parse(questionObservationSchema, { value: response, observedAt }, 'invalid_response');
  checkResult(last.journal, observation.value);
  if (last.state === 'prepared') throw new PrimaryApiError('identity_mismatch');
  if (last.state === 'accepted') {
    if (!exactReply(last.journal, observation.value)) throw new PrimaryApiError('identity_mismatch');
    return record;
  }
  if (last.state === 'refused') throw new PrimaryApiError('identity_mismatch');
  if (exactReply(last.journal, observation.value)) return updateLastAttempt(record, { ...last, state: 'accepted', accepted: observation });
  if (['answered', 'expired', 'cancelled'].includes(observation.value.question.state)) return updateLastAttempt(record,
    { ...last, state: 'refused', terminalQuestion: observation });
  return updateLastAttempt(record, { ...last, state: 'unknown', recovery: true });
}
