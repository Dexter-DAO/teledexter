import { z } from 'zod';
import { canonicalJson } from '../outbound/canonical.js';
import { createPrimaryApiTransport, PrimaryApiError, type PrimaryApiOptions, type PrimaryReplyInput,
  replyInputSchema, type PrimaryTurn, type PrimaryTurnReceipt } from './primaryApi.js';
import { PHONE_ELIGIBLE_CONTEXT_HEADER, PHONE_ELIGIBLE_CONTEXT_VERSION, type EligibleDeliveryTarget } from './eligibleContext.js';
import { requestDeliveryRequestSchema, requestDeliveryResponseSchema, requestDeliveryQuestionSchema, createRequestDeliveryJournal,
  parseRequestDeliveryJournal, assertRequestDeliveryJournalUpdate, undispatchedReplyRefusalSchema,
  refuseUndispatchedRequestDeliveryReply, type UndispatchedReplyRefusalCode,
  retainRequestDeliveryAssociation, createRequestDeliveryQuestionRecord, parseRequestDeliveryQuestionRecord,
  assertRequestDeliveryQuestionRecordUpdate, retainRequestDeliveryCallerSource, retainRequestDeliveryReply,
  appendRequestDeliveryReplyAttempt, beginRequestDeliveryReplyAttempt, observeRequestDeliveryReplyFailure,
  observeRequestDeliveryReplyResult, type RequestDeliveryJournal, type RequestDeliveryQuestionRecord,
  type RequestDeliveryRequest, type RequestDeliveryClient, type RequestDeliveryResult,
  type RequestDeliveryQuestionResult } from './requestDelivery.js';

const uuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
const seedSchema = z.object({
  v: z.literal('broker.selected-question-seed.v1'),
  deliveryCall: z.object({ callId: uuid, sessionId: z.string().min(1).max(200) }).strict(),
  scope: z.object({ ownerPrincipalId: uuid, primaryDexterId: uuid, threadId: uuid }).strict(),
  delivery: z.object({ bindingId: uuid, callSid: z.string().regex(/^CA[0-9a-fA-F]{32}$/) }).strict(),
  request: requestDeliveryRequestSchema,
  callDeadlineAt: z.iso.datetime({ offset: true }),
}).strict();
export type SelectedQuestionSeed = z.infer<typeof seedSchema>;
const same = (a: unknown, b: unknown) => {
  if (canonicalJson(a) !== canonicalJson(b)) throw new PrimaryApiError('identity_mismatch');
};
function parse<T>(schema: z.ZodType<T>, value: unknown, code: 'invalid_request' | 'invalid_response' = 'invalid_request'): T {
  const parsed = schema.safeParse(value);
  if (!parsed.success) throw new PrimaryApiError(code);
  return parsed.data;
}
export const parseSelectedQuestionSeed = (value: unknown): SelectedQuestionSeed => parse(seedSchema, value);

export interface SelectedQuestionAssociationClient {
  /** Seed is durably saved first. This new selector path requires the owner's canonical request ID. */
  associate(seed: SelectedQuestionSeed): Promise<RequestDeliveryJournal>;
}

/** Existing alias-aware associate(journal) remains unchanged. Only this canonical-selector bootstrap lacks a prior run ID. */
export function createSelectedQuestionAssociationClient(options: PrimaryApiOptions): SelectedQuestionAssociationClient {
  const request = createPrimaryApiTransport(options);
  return {
    async associate(value) {
      const seed = parseSelectedQuestionSeed(value);
      const response = parse(requestDeliveryResponseSchema, await request('POST',
        `/calls/${seed.delivery.bindingId}/request-deliveries`, seed.request,
        { 'X-Dexter-Binding-Id': seed.delivery.bindingId, [PHONE_ELIGIBLE_CONTEXT_HEADER]: PHONE_ELIGIBLE_CONTEXT_VERSION }), 'invalid_response');
      same(response.association.delivery, seed.delivery);
      same(response.association.question, { id: seed.request.questionId, revision: seed.request.questionRevision });
      // Owner selector v1 returns canonical identity. A different request is not accepted as an inferred alias.
      same(response.association.execution.requestId, seed.request.originalRequestId);
      const journal = createRequestDeliveryJournal({ deliveryCall: seed.deliveryCall, scope: seed.scope,
        request: seed.request, target: { delivery: seed.delivery, execution: response.association.execution,
          question: response.association.question } });
      return retainRequestDeliveryAssociation(journal, response);
    },
  };
}

const answerSchema = z.object({ questionId: uuid, input: replyInputSchema,
  sourceSessionId: z.string().min(1).max(200), delegationId: z.string().min(1).max(200),
  presentationId: z.string().min(1).max(200) }).strict();
/** Supplied only by the host's explicit-delegation/completed-source admission path. */
export type SelectedQuestionAnswer = z.infer<typeof answerSchema>;
export interface SelectedQuestionAdmission {
  sequence: number;
  answer: SelectedQuestionAnswer;
  admittedAt: string;
  captured?: RequestDeliveryJournal;
  attemptSequence?: number;
  localRefusal?: z.infer<typeof undispatchedReplyRefusalSchema>;
}
export interface SelectedQuestionSlot {
  seed: SelectedQuestionSeed;
  record?: RequestDeliveryQuestionRecord;
  admissions: SelectedQuestionAdmission[];
}
export interface SelectedQuestionJournal {
  v: 'broker.selected-question-host.v1';
  initial: SelectedQuestionSeed;
  questions: SelectedQuestionSlot[];
  stoppedAt?: string;
}
const admissionSchema = z.object({ sequence: z.number().int().safe().positive(), answer: answerSchema,
  admittedAt: z.iso.datetime({ offset: true }), captured: z.unknown().optional(),
  attemptSequence: z.number().int().safe().positive().optional(), localRefusal: undispatchedReplyRefusalSchema.optional() }).strict();
function withoutSource(journal: RequestDeliveryJournal): RequestDeliveryJournal {
  const { callerSource: _source, reply: _reply, ...base } = journal; return base;
}
function admissionAttempt(slot: SelectedQuestionSlot, admission: SelectedQuestionAdmission) {
  return admission.attemptSequence === undefined ? undefined : slot.record?.attempts[admission.attemptSequence - 1];
}
function unresolved(slot: SelectedQuestionSlot): boolean {
  return slot.admissions.some(admission => !admission.captured || (!admission.localRefusal
    && !['accepted', 'refused'].includes(admissionAttempt(slot, admission)?.state ?? 'prepared')));
}
export function parseSelectedQuestionJournal(value: unknown): SelectedQuestionJournal {
  const parsed = parse(z.object({ v: z.literal('broker.selected-question-host.v1'), initial: seedSchema,
    stoppedAt: z.iso.datetime({ offset: true }).optional(),
    questions: z.array(z.object({ seed: seedSchema, record: z.unknown().optional(), admissions: z.array(admissionSchema) }).strict()).min(1),
  }).strict(), value);
  const questionIds = new Set<string>(), sourceIds = new Set<string>(), replyIds = new Set<string>();
  const questions: SelectedQuestionSlot[] = [];
  for (const [index, slot] of parsed.questions.entries()) {
    const seed = slot.seed;
    same(seed.deliveryCall, parsed.initial.deliveryCall); same(seed.delivery, parsed.initial.delivery);
    same(seed.scope, parsed.initial.scope); same(seed.callDeadlineAt, parsed.initial.callDeadlineAt);
    same(seed.request.originalRequestId, parsed.initial.request.originalRequestId);
    if (index === 0) same(seed, parsed.initial);
    if (questionIds.has(seed.request.questionId) || (index > 0 && (!questions[index - 1]!.record || unresolved(questions[index - 1]!))))
      throw new PrimaryApiError('identity_mismatch');
    questionIds.add(seed.request.questionId);
    const record = slot.record === undefined ? undefined : parseRequestDeliveryQuestionRecord(slot.record);
    if (record) {
      same(record.base.deliveryCall, seed.deliveryCall); same(record.base.scope, seed.scope);
      same(record.base.request, seed.request); same(record.base.target.delivery, seed.delivery);
      same(record.base.target.execution.requestId, seed.request.originalRequestId);
      if (index > 0) same(record.base.target.execution, questions[0]!.record!.base.target.execution);
    }
    const admissions: SelectedQuestionAdmission[] = [];
    const attemptSequences = new Set<number>();
    for (const [admissionIndex, input] of slot.admissions.entries()) {
      if (!record || input.sequence !== admissionIndex + 1 || input.answer.questionId !== seed.request.questionId
        || sourceIds.has(input.answer.input.source.sourceEventId) || replyIds.has(input.answer.input.reply.clientReplyId))
        throw new PrimaryApiError('identity_mismatch');
      same(input.answer.input.source.callSid, seed.delivery.callSid);
      sourceIds.add(input.answer.input.source.sourceEventId); replyIds.add(input.answer.input.reply.clientReplyId);
      const captured = input.captured === undefined ? undefined : parseRequestDeliveryJournal(input.captured);
      if (captured) {
        same(withoutSource(captured), record.base);
        if (!captured.reply || !captured.callerSource) throw new PrimaryApiError('identity_mismatch');
        same(captured.reply.input, input.answer.input);
      }
      const admission: SelectedQuestionAdmission = { ...input, ...(captured ? { captured } : {}) } as SelectedQuestionAdmission;
      const attempt = admission.attemptSequence === undefined ? undefined : record.attempts[admission.attemptSequence - 1];
      if (admission.attemptSequence !== undefined) {
        if (!captured || !attempt || attemptSequences.has(admission.attemptSequence)) throw new PrimaryApiError('identity_mismatch');
        same(attempt.journal, captured); attemptSequences.add(admission.attemptSequence);
      }
      if (admission.localRefusal) {
        if (Date.parse(admission.localRefusal.observedAt) < Date.parse(admission.admittedAt)) throw new PrimaryApiError('identity_mismatch');
        if (attempt) same(attempt.localRefusal, admission.localRefusal);
      } else if (attempt?.localRefusal) throw new PrimaryApiError('identity_mismatch');
      const previous = admissions.at(-1);
      if (previous && (!previous.captured || (!previous.localRefusal
        && admissionAttempt({ seed, record, admissions }, previous)?.state !== 'refused')))
        throw new PrimaryApiError('identity_mismatch');
      admissions.push(admission);
    }
    if (record && attemptSequences.size !== record.attempts.length) throw new PrimaryApiError('identity_mismatch');
    questions.push({ seed, ...(record ? { record } : {}), admissions });
  }
  return { v: parsed.v, initial: parsed.initial, questions, ...(parsed.stoppedAt ? { stoppedAt: parsed.stoppedAt } : {}) };
}
export function assertSelectedQuestionJournalUpdate(previous: SelectedQuestionJournal, next: SelectedQuestionJournal): SelectedQuestionJournal {
  const before = parseSelectedQuestionJournal(previous), after = parseSelectedQuestionJournal(next);
  same(before.initial, after.initial);
  if (before.stoppedAt) same(before.stoppedAt, after.stoppedAt);
  if (after.questions.length < before.questions.length || after.questions.length > before.questions.length + 1
    || (before.stoppedAt && after.questions.length !== before.questions.length)) throw new PrimaryApiError('identity_mismatch');
  for (const [index, slot] of before.questions.entries()) {
    const updated = after.questions[index]!; same(slot.seed, updated.seed);
    if (slot.record) {
      if (!updated.record) throw new PrimaryApiError('identity_mismatch');
      assertRequestDeliveryQuestionRecordUpdate(slot.record, updated.record);
      if (before.stoppedAt && (updated.record.attempts.length !== slot.record.attempts.length
        || updated.record.attempts.some((attempt, i) => attempt.dispatches !== slot.record!.attempts[i]!.dispatches)))
        throw new PrimaryApiError('identity_mismatch');
    }
    if (updated.admissions.length < slot.admissions.length || updated.admissions.length > slot.admissions.length + 1
      || (before.stoppedAt && updated.admissions.length !== slot.admissions.length)) throw new PrimaryApiError('identity_mismatch');
    for (const [i, admission] of slot.admissions.entries()) {
      const changed = updated.admissions[i]!;
      same(admission.sequence, changed.sequence); same(admission.answer, changed.answer); same(admission.admittedAt, changed.admittedAt);
      if (admission.captured) {
        if (!changed.captured) throw new PrimaryApiError('identity_mismatch');
        assertRequestDeliveryJournalUpdate(admission.captured, changed.captured);
      }
      if (admission.attemptSequence !== undefined) same(admission.attemptSequence, changed.attemptSequence);
      if (admission.localRefusal) same(admission.localRefusal, changed.localRefusal);
    }
  }
  return after;
}

export interface SelectedQuestionHostOptions {
  seed: SelectedQuestionSeed;
  load(): SelectedQuestionJournal | undefined;
  persist(value: SelectedQuestionJournal): void;
  active(): boolean;
  now?(): number;
  association: SelectedQuestionAssociationClient;
  requests: RequestDeliveryClient;
  capture(bindingId: string, turn: PrimaryTurn): Promise<PrimaryTurnReceipt>;
  /** Uses controller.deliver(..., journal); no raw question/result fallback. */
  deliver(journal: RequestDeliveryJournal, target: EligibleDeliveryTarget): Promise<void>;
  /** Admission: current publication, source session/sample frontier and explicit delegation. */
  replyIsQualified(journal: RequestDeliveryJournal, answer: SelectedQuestionAnswer): boolean;
  /** Recovery: the already-admitted proof remains valid, including every publication receipt's rejection state. */
  admittedReplyIsValid(journal: RequestDeliveryJournal, answer: SelectedQuestionAnswer): boolean;
}

/** One admitted phone call hosts existing work. This host has no submitMessage or new-run port. */
export function createSelectedQuestionHost(options: SelectedQuestionHostOptions) {
  const initial = parseSelectedQuestionSeed(options.seed), now = options.now ?? Date.now;
  const at = () => new Date(now()).toISOString();
  let state = options.load();
  if (state) { state = parseSelectedQuestionJournal(state); same(state.initial, initial); }
  else { state = { v: 'broker.selected-question-host.v1', initial, questions: [{ seed: initial, admissions: [] }] };
    options.persist(structuredClone(state)); }
  const active = () => !state!.stoppedAt && options.active() && now() < Date.parse(initial.callDeadlineAt);
  let tail: Promise<unknown> = Promise.resolve();
  const serial = <T>(operation: () => Promise<T>): Promise<T> => {
    const next = tail.then(operation); tail = next.catch(() => {}); return next;
  };
  const save = (next: SelectedQuestionJournal) => {
    const checked = assertSelectedQuestionJournalUpdate(state!, next);
    options.persist(structuredClone(checked)); state = checked;
  };
  const slot = () => state!.questions.at(-1)!;
  const saveSlot = (next: SelectedQuestionSlot) => save({ ...state!, questions: [...state!.questions.slice(0, -1), next] });
  const saveRecord = (record: RequestDeliveryQuestionRecord) => saveSlot({ ...slot(), record });
  const updateAdmission = (value: SelectedQuestionAdmission, record = slot().record) => {
    saveSlot({ ...slot(), record, admissions: slot().admissions.map(a => a.sequence === value.sequence ? value : a) });
  };
  const requireActive = () => { if (!active()) throw new Error('selected_question_call_inactive'); };
  const ensureAssociation = async () => {
    requireActive();
    if (!slot().record) {
      // Lost response retains this exact seed. Recovery repeats association only, never a call or run.
      const journal = await options.association.associate(structuredClone(slot().seed));
      saveRecord(createRequestDeliveryQuestionRecord(journal));
    }
    requireActive(); return slot().record!;
  };
  const localRefuse = (admission: SelectedQuestionAdmission, code: UndispatchedReplyRefusalCode) => {
    if (admission.localRefusal) return;
    const attempt = admissionAttempt(slot(), admission);
    if (attempt && attempt.state !== 'prepared') return;
    const observedAt = at();
    const record = attempt ? refuseUndispatchedRequestDeliveryReply(slot().record!, code, observedAt) : slot().record;
    updateAdmission({ ...admission, localRefusal: { kind: 'never_dispatched', code, observedAt } }, record);
  };
  const validityRefusal = (admission: SelectedQuestionAdmission) => {
    if (!active()) return 'call_ended' as const;
    if (!options.admittedReplyIsValid(slot().record!.base, structuredClone(admission.answer))) return 'presentation_invalidated' as const;
    return undefined;
  };
  const exactQuestion = (input: RequestDeliveryQuestionResult, journal: RequestDeliveryJournal) => {
    const result = parse(requestDeliveryQuestionSchema, input, 'invalid_response');
    same(result.deliveryAssociation, journal.association); same(result.bindingId, journal.target.execution.bindingId);
    same(result.requestId, journal.request.originalRequestId); same(result.question.id, journal.target.question.id);
    same(result.question.revision, 1); same(result.question.runId, journal.target.execution.runId);
    same(result.question.threadId, journal.scope.threadId); same(result.question.agent.id, journal.scope.primaryDexterId);
    same(result.question.runState, result.state); return result;
  };
  const dispatch = async () => {
    requireActive();
    const record = beginRequestDeliveryReplyAttempt(slot().record!, at()); saveRecord(record);
    let response: RequestDeliveryQuestionResult;
    try { response = await options.requests.reply(record.attempts.at(-1)!.journal); }
    catch (error) { saveRecord(observeRequestDeliveryReplyFailure(slot().record!, error, at())); return; }
    saveRecord(observeRequestDeliveryReplyResult(slot().record!, response, at()));
  };
  const readRecovery = async () => {
    const record = slot().record!; const last = record.attempts.at(-1);
    if (!last || ['prepared', 'accepted', 'refused'].includes(last.state)) return;
    const observed = await options.requests.question(last.journal);
    saveRecord(observeRequestDeliveryReplyResult(slot().record!, observed, at()));
  };
  const continueAdmission = async () => {
    let admission = slot().admissions.at(-1); if (!admission) return;
    const attempt = admissionAttempt(slot(), admission);
    if (attempt && attempt.state !== 'prepared') return;
    const before = validityRefusal(admission); if (before) localRefuse(admission, before);
    admission = slot().admissions.at(-1)!;
    if (!admission.captured) {
      const input = admission.answer.input;
      const turn: PrimaryTurn = { sourceEventId: input.source.sourceEventId, revision: input.source.revision,
        itemId: input.source.itemId, role: input.role, text: input.text, occurredAt: input.source.receivedAt };
      const receipt = await options.capture(slot().record!.base.target.delivery.bindingId, turn);
      const captured = retainRequestDeliveryReply(retainRequestDeliveryCallerSource(slot().record!.base, turn, receipt), input);
      admission = { ...slot().admissions.at(-1)!, captured }; updateAdmission(admission);
    }
    if (admission.localRefusal) return;
    const beforeRead = validityRefusal(admission); if (beforeRead) { localRefuse(admission, beforeRead); return; }
    const question = exactQuestion(await options.requests.question(admission.captured!), admission.captured!);
    admission = slot().admissions.at(-1)!;
    if (admission.localRefusal) return;
    const afterRead = validityRefusal(admission); if (afterRead) { localRefuse(admission, afterRead); return; }
    if (question.question.state === 'unavailable') throw new Error('selected_question_temporarily_unavailable');
    if (question.question.state !== 'pending' || question.state !== 'waiting_for_reply') {
      localRefuse(admission, 'question_not_pending'); return;
    }
    if (now() >= Date.parse(question.question.expiresAt)) { localRefuse(admission, 'question_expired'); return; }
    if (admission.attemptSequence === undefined) {
      const record = appendRequestDeliveryReplyAttempt(slot().record!, admission.captured!, question, at());
      admission = { ...admission, attemptSequence: record.attempts.length }; updateAdmission(admission, record);
    }
    const beforePost = validityRefusal(admission);
    if (beforePost) { localRefuse(admission, beforePost); return; }
    await dispatch();
  };
  const pending = () => unresolved(slot());
  return {
    snapshot: () => structuredClone(state!),
    start: () => serial(async () => {
      const record = await ensureAssociation();
      if (!pending()) await options.deliver(record.base, { kind: 'question', questionId: record.base.target.question.id, questionRevision: 1 });
    }),
    answer: (value: SelectedQuestionAnswer) => serial(async () => {
      const answer = parse(answerSchema, value);
      const savedSlot = state!.questions.find(q => q.admissions.some(a =>
        a.answer.input.source.sourceEventId === answer.input.source.sourceEventId || a.answer.input.reply.clientReplyId === answer.input.reply.clientReplyId));
      if (savedSlot) {
        const saved = savedSlot.admissions.find(a => a.answer.input.source.sourceEventId === answer.input.source.sourceEventId
          || a.answer.input.reply.clientReplyId === answer.input.reply.clientReplyId)!;
        same(saved.answer, answer);
        if (savedSlot === slot()) { await continueAdmission(); await readRecovery(); return structuredClone(slot().record!); }
        return structuredClone(savedSlot.record!);
      }
      requireActive(); const record = slot().record;
      if (!record || answer.questionId !== record.base.target.question.id
        || !options.replyIsQualified(record.base, structuredClone(answer))) throw new Error('selected_question_reply_unqualified');
      if (pending() || slot().admissions.some(a => admissionAttempt(slot(), a)?.state === 'accepted'))
        throw new Error('selected_question_reply_pending');
      // This save precedes capture and question HTTP. Exact raw source and admission proof survive either lost response.
      saveSlot({ ...slot(), admissions: [...slot().admissions, { sequence: slot().admissions.length + 1, answer, admittedAt: at() }] });
      await continueAdmission(); return structuredClone(slot().record!);
    }),
    recoverReply: () => serial(async () => {
      if (!slot().record) { requireActive(); await ensureAssociation(); }
      await continueAdmission(); await readRecovery(); return structuredClone(slot().record!);
    }),
    /** Bypasses serialized HTTP waits. Any in-flight POST remains unresolved; unsent sources stay capture obligations. */
    stop: () => {
      if (!state!.stoppedAt) save({ ...state!, stoppedAt: at() });
      const last = slot().admissions.at(-1); if (last) localRefuse(last, 'call_ended');
      return structuredClone(state!);
    },
    poll: () => serial(async (): Promise<RequestDeliveryResult> => {
      const record = await ensureAssociation(); await continueAdmission(); await readRecovery();
      const result = await options.requests.result(record.base); requireActive();
      if (result.state === 'waiting_for_reply' && result.question.id !== record.base.target.question.id) {
        if (pending()) throw new Error('selected_question_predecessor_unresolved');
        const previous = exactQuestion(await options.requests.question(record.base), record.base); requireActive();
        if (previous.question.state !== 'answered') throw new Error('selected_question_predecessor_unresolved');
        const request: RequestDeliveryRequest = { ...initial.request, questionId: result.question.id, questionRevision: 1 };
        save({ ...state!, questions: [...state!.questions, { seed: { ...initial, request }, admissions: [] }] });
        const next = await ensureAssociation();
        await options.deliver(next.base, { kind: 'question', questionId: request.questionId, questionRevision: 1 });
      } else if (result.state === 'waiting_for_reply') {
        if (!pending()) await options.deliver(record.base, { kind: 'question', questionId: record.base.target.question.id, questionRevision: 1 });
      } else if (result.state === 'completed') await options.deliver(record.base, { kind: 'assistant', messageId: result.assistant.messageId });
      return result;
    }),
  };
}
