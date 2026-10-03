import { createHash } from 'node:crypto';
import { z } from 'zod';
import { canonicalJson } from '../outbound/canonical.js';
import { frozenCallerTurnSchema, type FrozenCallerTurn } from './callerTurn.js';
import { historyVersionSchema } from './eligibleContext.js';
import { acknowledgedTurn, originalTurn } from './reconciliation.js';
import { brokerPublicationReceiptsAccepted, selectedQuestionPublicationReceiptsAccepted,
  callerIntentResultPublicationCandidates, callerIntentResultPublicationInventory } from './publications.js';
import { callerIntentOperationSchema, callerIntentTaskIdentitySchema, callerIntentInputSchema, callerIntentResultCoverageSchema, validateCallerIntentResponse,
  type CallerIntentOperation, type CallerIntentInput, type CallerIntentQuestion, type CallerIntentResponse } from './callerIntentContract.js';
import type { BrokerCall } from './store.js';
import type { BrokerDelegationRecord } from './backend.js';
import type { PrimaryTurnReceipt } from './primaryApi.js';
import type { LiveDelegationRequest } from '../live/types.js';

const uuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
const digest = z.string().regex(/^[0-9a-f]{64}$/);
const name = z.string().min(1).max(200);
const same = (a: unknown, b: unknown) => canonicalJson(a) === canonicalJson(b);
function fail(code: string): never { throw new Error(code); }
export const callerRoutePresentationDigest = (value: unknown): string => createHash('sha256').update(canonicalJson(value)).digest('hex');
export const callerQuestionPresentationDigest = callerRoutePresentationDigest;
const identity = (kind: string, ...parts: string[]) => {
  const h = createHash('sha256').update(JSON.stringify(['dexter-broker/primary/v1', kind, ...parts])).digest('hex');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-5${h.slice(13, 16)}-a${h.slice(17, 20)}-${h.slice(20, 32)}`;
};

const targetFields = { requestId: uuid, bindingId: uuid, runId: uuid, questionId: uuid,
  questionRevision: z.literal(1), presentationDigest: digest };
export const callerQuestionTargetSchema = z.discriminatedUnion('kind', [
  z.object({ kind: z.literal('ordinary'), ...targetFields }).strict(),
  z.object({ kind: z.literal('selected'), ...targetFields, associationId: uuid }).strict(),
]);
export type CallerQuestionTarget = z.infer<typeof callerQuestionTargetSchema>;
/** Internal candidate identity only. No associated-result HTTP wire is selected here. */
const referenceSchema = z.object({ id: name, digest }).strict();
export type CallerTaskContextReference = z.infer<typeof referenceSchema>;
export const callerResolutionInputSchema = z.object({ version: historyVersionSchema,
  publicationSequence: z.number().int().safe().nonnegative(),
  questions: z.array(callerQuestionTargetSchema), references: z.array(referenceSchema),
  // Absent on retained v1 or explicit local refusal; old bytes stay intact.
  resultCoverage: callerIntentResultCoverageSchema.optional(),
  // Local source preservation when a complete inventory cannot be established.
  // This is never a caller-intent input or a claim of empty result coverage.
  classificationRefusal: z.enum(['caller_result_inventory_inactive', 'caller_result_inventory_invalid',
    'caller_result_inventory_unverifiable']).optional() }).strict().superRefine((value, ctx) => {
    if (value.resultCoverage && value.resultCoverage.count !== value.references.length)
      ctx.addIssue({ code: 'custom', path: ['resultCoverage'], message: 'Full publication inventory differs from coverage' });
    if (value.classificationRefusal && (value.resultCoverage !== undefined || value.references.length !== 0))
      ctx.addIssue({ code: 'custom', path: ['classificationRefusal'], message: 'Unverified inventory cannot carry result coverage' });
  });
export const callerRouteDecisionSchema = z.discriminatedUnion('kind', [
  z.object({ kind: z.literal('task'), contextReference: referenceSchema.optional(), delivery:z.object({kind:z.literal('sms_result')}).strict().optional() }).strict(),
  z.object({kind:z.literal('deliver_result'),target:callerIntentTaskIdentitySchema,delivery:z.object({kind:z.literal('sms_result')}).strict()}).strict(),
  z.object({ kind: z.literal('answer'), target: callerQuestionTargetSchema }).strict(),
  z.object({ kind: z.literal('clarify'), reason: z.enum(['ambiguous_target', 'mixed_intent', 'incomplete',
    'unavailable', 'stale_context', 'invalid_resolution']) }).strict(),
  z.object({ kind: z.literal('conversation') }).strict(),
]);
export type CallerRouteDecision = z.infer<typeof callerRouteDecisionSchema>;
const delegationSchema = z.object({ sessionId: name, delegationId: name, requestId: name,
  offsetMs: z.number().finite().nonnegative() }).strict();
const scopeSchema = z.object({ callId: uuid, bindingId: uuid, callSid: z.string().regex(/^CA[0-9a-fA-F]{32}$/), generationId: uuid }).strict();
const captureSchema = z.object({ bindingId: uuid, sourceEventId: uuid, revision: z.literal(1), recordId: uuid,
  revisionRecordId: uuid, bodyDigest: digest, replayed: z.boolean() }).strict();
const recordSchema = z.object({ scope: scopeSchema, delegation: delegationSchema, frozen: frozenCallerTurnSchema,
  resolutionInput: callerResolutionInputSchema, inputDigest: digest, decision: callerRouteDecisionSchema.optional(),
  intent: callerIntentOperationSchema.optional(), capture: captureSchema.optional() }).strict();
export type CallerRouteRecord = z.infer<typeof recordSchema>;
export interface CallerRoutePreparation { delegation: CallerRouteRecord['delegation']; frozen: FrozenCallerTurn;
  resolutionInput: CallerRouteRecord['resolutionInput'] }
export const callerRouteRefSchema = z.object({ sourceEventId: uuid, inputDigest: digest }).strict();
export type CallerRouteRef = z.infer<typeof callerRouteRefSchema>;
export interface CallerRoutingJournal { v: 'broker.caller-routing.v1'; records: CallerRouteRecord[] }
const journalSchema = z.object({ v: z.literal('broker.caller-routing.v1'), records: z.array(recordSchema) }).strict();
export const callerRouteRef = (record: CallerRouteRecord): CallerRouteRef => ({
  sourceEventId: record.frozen.turn.sourceEventId, inputDigest: record.inputDigest,
});
const inputDigest = (record: Omit<CallerRouteRecord, 'inputDigest' | 'decision' | 'capture' | 'intent'>) =>
  callerRoutePresentationDigest([record.resolutionInput.resultCoverage || record.resolutionInput.classificationRefusal
    ? 'broker.caller-routing-input.v2' : 'broker.caller-routing-input.v1',
    record.scope, record.delegation, record.frozen, record.resolutionInput]);

export function callerIntentQuestionForTarget(target: CallerQuestionTarget): CallerIntentQuestion {
  const execution = { bindingId: target.bindingId, requestId: target.requestId, runId: target.runId };
  const question = { id: target.questionId, revision: target.questionRevision };
  return target.kind === 'selected'
    ? { kind: 'selected', execution, question, presentationId: target.presentationDigest, deliveryAssociationId: target.associationId }
    : { kind: 'ordinary', execution, question, presentationId: target.presentationDigest };
}

function validateIntentRecord(record: CallerRouteRecord): void {
  const intent = record.intent;
  if (!intent) fail('caller_intent_not_prepared');
  const { input } = intent, { turn } = record.frozen;
  if (!same(input.source, { callSid: record.scope.callSid, sourceEventId: turn.sourceEventId,
    itemId: turn.itemId, revision: turn.revision, receivedAt: turn.occurredAt, role: turn.role, text: turn.text })
    || input.generationId !== record.scope.generationId || input.sourceSessionId !== record.frozen.sessionId
    || input.brokerInputDigest !== record.inputDigest
    || input.presentationDigest !== callerRoutePresentationDigest(record.resolutionInput)
    || !same(input.version, record.resolutionInput.version)
    || !same(input.questions, record.resolutionInput.questions.map(callerIntentQuestionForTarget))) fail('caller_intent_input_changed');
  const coverage = record.resolutionInput.resultCoverage;
  if (input.v !== 'phone.caller-intent.v1') {
    if (!coverage || !same(input.resultCoverage, coverage)
      || (coverage.state === 'complete' ? input.results.length !== record.resolutionInput.references.length : input.results.length !== 0))
      fail('caller_intent_input_changed');
  } else if (coverage !== undefined || input.results.length !== record.resolutionInput.references.length)
    fail('caller_intent_input_changed');
  if (intent.response) validateCallerIntentResponse(record.scope.bindingId, intent.operationId, input, intent.response);
}

/** An API receipt chooses the route; local clarification never authorizes work. */
function validateActionReceipt(record: CallerRouteRecord): void {
  const decision = record.decision, response = record.intent?.response;
  if (!decision || !['task', 'answer', 'deliver_result'].includes(decision.kind)) return;
  if (response?.state !== 'resolved') fail('caller_route_intent_unresolved');
  if (decision.kind === 'deliver_result') {
    if(response.v!=='phone.caller-intent.v3' || response.decision.kind!=='deliver_result' || !same(response.decision,decision)
      || decision.target.bindingId!==record.scope.bindingId)fail('caller_route_intent_decision_changed');
  } else if (decision.kind === 'answer') {
    if (response.decision.kind !== 'answer' || !same(response.decision.target, callerIntentQuestionForTarget(decision.target)))
      fail('caller_route_intent_decision_changed');
  } else if (decision.kind === 'task' && (response.decision.kind !== 'task'
    || Boolean(response.decision.reference) !== Boolean(decision.contextReference)
    || !same('delivery' in response.decision ? response.decision.delivery : undefined,decision.delivery))) fail('caller_route_intent_decision_changed');
}

function validateIntentCandidates(call: BrokerCall, record: CallerRouteRecord, historical: boolean): void {
  const intent = record.intent;
  if (!intent) return;
  if (intent.input.callDeadlineAt !== call.liveGenerations?.clocks.callDeadlineAt) fail('caller_intent_deadline_changed');
  const candidates = callerIntentResultPublicationCandidates(call, record.frozen.sessionId, { historical });
  const coverage = record.resolutionInput.resultCoverage;
  if (!historical && intent.input.v !== 'phone.caller-intent.v1') {
    const inventory = callerIntentResultPublicationInventory(call, record.frozen.sessionId);
    if (!same(coverage, inventory.resultCoverage)
      || !same(record.resolutionInput.references, inventory.candidates.map(({ id, digest }) => ({ id, digest }))))
      fail('caller_intent_candidate_coverage_changed');
  }
  let lastIndex = -1;
  const identities = new Set<string>();
  for (const [index, ref] of record.resolutionInput.references.entries()) {
    const found = candidates.findIndex(value => value.id === ref.id && value.digest === ref.digest);
    const candidate = candidates[found];
    if (!candidate || found <= lastIndex
      || (coverage?.state !== 'overflow' && !same(candidate.candidate, intent.input.results[index]))) fail('caller_intent_candidate_changed');
    const c = candidate.candidate;
    const identity = canonicalJson([c.kind, c.execution, c.assistantMessageId, c.kind === 'selected' ? c.deliveryAssociationId : null]);
    if (coverage && identities.has(identity)) fail('caller_intent_candidate_changed');
    identities.add(identity);
    lastIndex = found;
  }
  const decision = record.decision, response = intent.response;
  if (decision?.kind === 'task' && decision.contextReference) {
    const ref = decision.contextReference;
    const candidate = candidates.find(value => value.id === ref.id && value.digest === ref.digest);
    if (!candidate || response?.state !== 'resolved' || response.decision.kind !== 'task'
      || !same(candidate.reference, response.decision.reference)) fail('caller_route_intent_reference_changed');
  }
}

function validateIntentUpdate(before: CallerIntentOperation | undefined, after: CallerIntentOperation | undefined): void {
  if (!before) return;
  if (!after || before.operationId !== after.operationId || !same(before.input, after.input)) fail('caller_intent_operation_changed');
  const previous = before.response, response = after.response;
  if (!previous) return;
  if (!response) fail('caller_intent_response_removed');
  if (previous.state === 'resolved') {
    if (!same(previous, response)) fail('caller_intent_resolution_changed');
  } else {
    const previousEnvelope = { ...previous, state: undefined, decision: undefined };
    const nextEnvelope = { ...response, state: undefined, decision: undefined };
    if (!same(previousEnvelope, nextEnvelope)) fail('caller_intent_reservation_changed');
  }
}

export class CallerRouteExpiredError extends Error {
  constructor(readonly validUntil: string, readonly inputDigest: string) { super('caller_intent_expired'); }
}

/** Only for the first HTTP dispatch. Unknown prior dispatch is reconciled read-only. */
export function requireCallerRouteDispatch(call: BrokerCall, sourceEventId: string, atMs: number): void {
  if (!Number.isFinite(atMs)) fail('caller_route_invalid_dispatch_time');
  validateCallerRouting(call);
  const record = getCallerRouteBySource(call, sourceEventId), response = record?.intent?.response;
  if (!record || !record.decision || !['task', 'answer', 'deliver_result'].includes(record.decision.kind) || response?.state !== 'resolved')
    fail('caller_route_intent_unresolved');
  // This function gates a first HTTP dispatch only. Saved admitted or unknown
  // work is recovered by its existing downstream identity before reaching it.
  if (!record.intent || record.intent.input.v === 'phone.caller-intent.v1' || response.v === 'phone.caller-intent.v1')
    fail('caller_intent_legacy_dispatch_refused');
  validateActionReceipt(record);
  if (call.endedAt || call.finalized || call.primaryClosed || ['completed', 'busy', 'no-answer', 'canceled', 'failed'].includes(call.status)
    || !call.liveGenerations || call.liveGenerations.phase === 'stopped') fail('caller_route_stale_source');
  if (atMs >= Date.parse(response.validUntil) || atMs >= Date.parse(call.liveGenerations.clocks.callDeadlineAt))
    throw new CallerRouteExpiredError(response.validUntil, response.inputDigest);
}

export function parseCallerRoutingJournal(value: unknown): CallerRoutingJournal {
  const result = journalSchema.parse(value), sources = new Set<string>(), delegations = new Set<string>(), requests = new Set<string>(), operations = new Set<string>();
  for (const record of result.records) {
    const key = canonicalJson([record.delegation.sessionId, record.delegation.delegationId]);
    const { scope, delegation, frozen, resolutionInput } = record;
    if (sources.has(frozen.turn.sourceEventId) || delegations.has(key) || requests.has(delegation.requestId) || delegation.sessionId !== frozen.sessionId
      || record.inputDigest !== inputDigest({ scope, delegation, frozen, resolutionInput })
      || frozen.turn.sourceEventId !== identity('source', scope.callId, frozen.sessionId, frozen.firstEventId)
      || frozen.turn.itemId !== `live-turn-${frozen.turn.sourceEventId}`
      || new Set(resolutionInput.questions.map(q => canonicalJson([q.kind, q.requestId, q.questionId]))).size !== resolutionInput.questions.length
      || new Set(resolutionInput.references.map(r => r.id)).size !== resolutionInput.references.length)
      fail('caller_route_identity_changed');
    sources.add(frozen.turn.sourceEventId); delegations.add(key); requests.add(delegation.requestId);
    const decision = record.decision;
    if (resolutionInput.classificationRefusal && (record.intent
      || (decision && (decision.kind !== 'clarify' || decision.reason !== 'unavailable'))))
      fail('caller_route_inventory_refused');
    if (decision?.kind === 'answer' && !resolutionInput.questions.some(q => same(q, decision.target)))
      fail('caller_route_target_not_offered');
    if (decision?.kind === 'task' && decision.contextReference
      && !resolutionInput.references.some(r => same(r, decision.contextReference)))
      fail('caller_route_reference_not_offered');
    if (record.capture) acknowledgedTurn(scope.bindingId, frozen.turn, record.capture);
    if (record.intent) {
      if (operations.has(record.intent.operationId)) fail('caller_intent_operation_reused');
      operations.add(record.intent.operationId);
      validateIntentRecord(record);
    }
    if (decision?.kind === 'task' || decision?.kind === 'answer' || decision?.kind === 'deliver_result') validateActionReceipt(record);
  }
  return result;
}

const stopped = (call: BrokerCall, atMs = Date.now()) => Boolean(call.endedAt || call.finalized || call.primaryClosed
  || ['completed', 'busy', 'no-answer', 'canceled', 'failed'].includes(call.status)
  || call.liveGenerations?.phase === 'stopped'
  || (call.liveGenerations && atMs >= Date.parse(call.liveGenerations.clocks.callDeadlineAt)));
function currentSource(call: BrokerCall, record: CallerRouteRecord): void {
  const g = call.liveGenerations, a = g?.deliveryAttachment;
  if (stopped(call) || !g || !a || g.phase !== 'active' || a.generationId !== record.scope.generationId
    || a.sessionId !== record.frozen.sessionId || call.liveSessionId !== record.frozen.sessionId
    || g.segments.find(s => s.generationId === a.generationId)?.state !== 'active') fail('caller_route_stale_source');
  const segment = g.segments.find(s => s.generationId === a.generationId);
  if (!segment || segment.transcript.callerRevision !== record.frozen.callerRevision
    || (call.publications?.records.at(-1)?.publicationSequence ?? 0) !== record.resolutionInput.publicationSequence
    || !same(segment.version, record.resolutionInput.version))
    fail('caller_route_stale_context');
}
function currentTarget(call: BrokerCall, target: CallerQuestionTarget, sessionId: string): void {
  if (target.kind === 'ordinary') {
    const entry = call.delegations[target.requestId] as BrokerDelegationRecord | undefined;
    const question = entry?.questions?.[target.questionId], p = question?.presentation;
    if (!entry || entry.bindingId !== target.bindingId || entry.primaryResult?.question?.runId !== target.runId
      || entry.currentQuestionId !== target.questionId || entry.status !== 'waiting_for_reply'
      || question?.question.revision !== target.questionRevision || question.question.runId !== target.runId
      || question.question.state !== 'pending' || Date.parse(question.question.expiresAt) <= Date.now()
      || (question.reply && question.reply.status !== 'refused') || !p || p.sessionId !== sessionId
      || callerRoutePresentationDigest(p) !== target.presentationDigest
      || !brokerPublicationReceiptsAccepted(call, entry.primaryResult.requestId, sessionId, p.publicationReceipts)) fail('caller_route_stale_target');
  } else {
    const slot = call.selectedQuestion?.questions.at(-1), base = slot?.record?.base, association = base?.association;
    const p = call.selectedQuestionVoice?.presentations.find(p => callerRoutePresentationDigest(p) === target.presentationDigest);
    if (!slot || !slot.record || !association || association.id !== target.associationId || association.execution.bindingId !== target.bindingId
      || association.execution.requestId !== target.requestId || association.execution.runId !== target.runId
      || association.question.id !== target.questionId || association.question.revision !== target.questionRevision
      || call.selectedQuestion?.stoppedAt || slot.admissions.some(a => !a.localRefusal && (a.attemptSequence === undefined
        || slot.record?.attempts[a.attemptSequence - 1]?.state !== 'refused'))
      || !p || p.sessionId !== sessionId || p.associationId !== target.associationId
      || p.response.delivery.state !== 'ready' || !('question' in p.response.delivery)
      || Date.parse(p.response.delivery.question.expiresAt) <= Date.now()
      || !selectedQuestionPublicationReceiptsAccepted(call, p.response, sessionId, p.publicationReceipts)) fail('caller_route_stale_target');
  }
}

function records(call: BrokerCall): CallerRouteRecord[] { return call.callerRouting ? parseCallerRoutingJournal(call.callerRouting).records : []; }
export function findCallerRoute(call: BrokerCall, request: LiveDelegationRequest): CallerRouteRecord | undefined {
  if (request.callId !== call.id) fail('caller_route_identity_changed');
  validateCallerRouting(call);
  const record = records(call).find(r => (r.delegation.sessionId === request.sessionId && r.delegation.delegationId === request.delegationId)
    || r.delegation.requestId === request.requestId);
  if (record && !same(record.delegation, { sessionId: request.sessionId, delegationId: request.delegationId,
    requestId: request.requestId, offsetMs: request.offsetMs })) fail('caller_route_identity_changed');
  return record && structuredClone(record);
}
export const callerRouteUsedSourceIds = (call: BrokerCall): string[] => records(call).map(r => r.frozen.turn.sourceEventId);
/** Every record, including unresolved/clarification speech, retains a capture obligation. */
export const callerRouteSources = (call: BrokerCall): Array<{ frozen: FrozenCallerTurn; capture?: PrimaryTurnReceipt; replyPending: boolean }> =>
  records(call).map(r => ({ frozen: r.frozen, ...(r.capture ? { capture: r.capture } : {}), replyPending: false }));
export const callerRoutingSourceAnchors = callerRouteSources;
export const getCallerRouteBySource = (call: BrokerCall, sourceEventId: string): CallerRouteRecord | undefined =>
  records(call).find(r => r.frozen.turn.sourceEventId === sourceEventId);
export const callerRoutePendingCaptures = (call: BrokerCall): number => records(call).filter(r => !r.capture).length;

export function requireCallerRoute(call: BrokerCall, request: LiveDelegationRequest, value: CallerRouteRef,
  kind: 'task' | 'ordinary_answer' | 'selected_answer' | 'deliver_result', mode: 'admit' | 'recover' = 'admit'): CallerRouteRecord {
  const ref = callerRouteRefSchema.parse(value), record = findCallerRoute(call, request);
  if (!record || !same(callerRouteRef(record), ref)) fail('caller_route_not_found');
  const decision = record.decision;
  if (kind === 'deliver_result' ? decision?.kind !== 'deliver_result' : kind === 'task' ? decision?.kind !== 'task'
    : decision?.kind !== 'answer' || decision.target.kind !== (kind === 'ordinary_answer' ? 'ordinary' : 'selected'))
    fail('caller_route_decision_mismatch');
  if (mode === 'admit') {
    currentSource(call, record);
    validateIntentCandidates(call, record, false);
    if (decision?.kind === 'answer') currentTarget(call, decision.target, record.frozen.sessionId);
    requireCallerRouteDispatch(call, record.frozen.turn.sourceEventId, Date.now());
  }
  // Recovery only retrieves the old decision. The consuming port must require its
  // own saved downstream submission/admission before any recovery operation.
  return structuredClone(record);
}

function owners(call: BrokerCall, sourceId: string): Array<{ kind: 'task' | 'ordinary' | 'selected'; requestId: string;
  questionId?: string; turn: unknown; sessionId: string; associationId?: string;resultSms?:BrokerDelegationRecord['submission']['resultSms'] }> {
  const found: ReturnType<typeof owners> = [];
  for (const raw of Object.values(call.delegations)) {
    const entry = raw as BrokerDelegationRecord;
    if (entry.submission?.source.sourceEventId === sourceId) found.push({ kind: 'task', requestId: entry.requestId,
      turn: originalTurn(entry.submission), sessionId: entry.sessionId,resultSms:entry.submission.resultSms });
    for (const deferred of entry.deferredTurns ?? []) if (deferred.sourceEventId === sourceId && deferred.queuedRequest)
      found.push({ kind: 'task', requestId: deferred.queuedRequest.requestId, turn: originalTurn(deferred.queuedRequest.submission),
        sessionId: deferred.sourceSessionId ?? entry.sessionId,resultSms:deferred.queuedRequest.submission.resultSms });
    for (const q of Object.values(entry.questions ?? {})) for (const reply of [...(q.refusedReplies ?? []), ...(q.reply ? [q.reply] : [])])
      if (reply.input.source.sourceEventId === sourceId) found.push({ kind: 'ordinary', requestId: entry.requestId,
        questionId: q.question.id, sessionId: reply.sourceSessionId ?? entry.sessionId,
        turn: { sourceEventId: reply.input.source.sourceEventId, revision: reply.input.source.revision,
          itemId: reply.input.source.itemId, role: reply.input.role, text: reply.input.text, occurredAt: reply.input.source.receivedAt } });
  }
  for (const source of call.selectedQuestionVoice?.sources ?? []) if (source.frozen.turn.sourceEventId === sourceId) {
    const p = call.selectedQuestionVoice?.presentations.find(p => p.id === source.answer.presentationId);
    if (!p) fail('caller_route_presentation_missing');
    found.push({ kind: 'selected', requestId: p.response.requestId, questionId: source.answer.questionId,
      turn: source.frozen.turn, sessionId: source.frozen.sessionId, associationId: p.associationId });
  }
  return found;
}

/** Called on startup and every journal write, including unrelated update paths. */
export function validateCallerRouting(call: BrokerCall, previous?: BrokerCall): void {
  if (!call.callerRouting) { if (previous?.callerRouting) fail('caller_routing_removed'); return; }
  const after = records(call), before = previous ? records(previous) : [];
  if (after.length < before.length || after.length > before.length + (previous ? 1 : after.length)) fail('caller_routing_history_changed');
  for (const [index, record] of after.entries()) {
    const old = before[index], { scope, frozen, resolutionInput } = record;
    const segment = call.liveGenerations?.segments.find(s => s.generationId === scope.generationId);
    if (!call.primary || !segment || scope.callId !== call.id || scope.bindingId !== call.primary.bindingId || scope.callSid !== call.callSid
      || segment.sessionId !== frozen.sessionId || !same(segment.version, resolutionInput.version)
      || resolutionInput.version.ownerPrincipalId !== call.liveGenerations?.binding.ownerPrincipalId
      || resolutionInput.version.primaryDexterId !== call.primary?.primaryDexterId || resolutionInput.version.threadId !== call.primary.threadId
      || !call.mediaStartedAt || segment.inputFirstSample === undefined
      || frozen.turn.occurredAt !== new Date(Date.parse(call.mediaStartedAt) + segment.inputFirstSample / 8 + frozen.startMs).toISOString())
      fail('caller_route_scope_changed');
    if (old && (!same({ ...old, decision: undefined, capture: undefined, intent: undefined }, { ...record, decision: undefined, capture: undefined, intent: undefined })
      || (old.decision && !same(old.decision, record.decision)) || (old.capture && !same(old.capture, record.capture))))
      fail('caller_routing_history_changed');
    validateIntentUpdate(old?.intent, record.intent);
    validateIntentCandidates(call, record, true);
    if (previous && !old) {
      if (record.decision || record.capture || record.intent || owners(previous, frozen.turn.sourceEventId).length
        || Object.values(previous.delegations).some(raw => (raw as BrokerDelegationRecord).deferredTurns?.some(t => t.sourceEventId === frozen.turn.sourceEventId))) fail('caller_route_source_already_owned');
      // A finished source may arrive during retirement. Preserve it for capture;
      // currentSource is required separately before the decision can admit work.
    }
    if (previous && old && !old.intent && record.intent) {
      if (old.decision || record.intent.response) fail('caller_intent_input_not_durable');
      if (record.intent.input.v === 'phone.caller-intent.v1') fail('caller_intent_legacy_dispatch_refused');
      currentSource(call, record);
      for (const target of record.resolutionInput.questions) currentTarget(call, target, frozen.sessionId);
      validateIntentCandidates(call, record, false);
    }
    if (previous && old && !old.decision && record.decision && ['task', 'answer', 'deliver_result'].includes(record.decision.kind)) {
      if (old.intent?.response?.state !== 'resolved') fail('caller_intent_receipt_not_durable');
      currentSource(call, record);
      validateIntentCandidates(call, record, false);
      if (record.decision.kind === 'answer') currentTarget(call, record.decision.target, frozen.sessionId);
    }
    const uses = owners(call, frozen.turn.sourceEventId);
    const distinct = new Set(uses.map(use => canonicalJson([use.kind, use.requestId, use.questionId, use.associationId])));
    if (distinct.size > 1) fail('caller_route_double_admission');
    for (const use of uses) {
      if (!same(use.turn, frozen.turn) || use.sessionId !== frozen.sessionId) fail('caller_route_source_changed');
      if (use.kind === 'task') {
        if (record.decision?.kind !== 'task' || use.requestId !== identity('request', call.id, frozen.sessionId, record.delegation.delegationId))
          fail('caller_route_decision_mismatch');
        if(record.decision.delivery ? !same(use.resultSms,{v:'phone.result-sms.v1',callerIntentOperationId:record.intent?.operationId,
          requestId:use.requestId}) : use.resultSms!==undefined)fail('caller_route_delivery_changed');
      } else if (record.decision?.kind !== 'answer' || record.decision.target.kind !== use.kind
        || record.decision.target.requestId !== use.requestId || record.decision.target.questionId !== use.questionId
        || (record.decision.target.kind === 'selected' && record.decision.target.associationId !== use.associationId))
        fail('caller_route_decision_mismatch');
      if (previous && !owners(previous, frozen.turn.sourceEventId).length) {
        if (!before[index]?.decision) fail('caller_route_decision_not_durable');
        if (use.kind !== 'selected') currentSource(call, record);
      }
    }
  }
}

export function prepareCallerRouting(call: BrokerCall, input: CallerRoutePreparation): { journal: CallerRoutingJournal; record: CallerRouteRecord } {
  const segment = call.liveGenerations?.segments.find(s => s.sessionId === input.frozen.sessionId);
  const base = { scope: scopeSchema.parse({ callId: call.id, bindingId: call.primary?.bindingId, callSid: call.callSid, generationId: segment?.generationId }),
    delegation: input.delegation, frozen: input.frozen, resolutionInput: input.resolutionInput };
  const record = recordSchema.parse({ ...base, inputDigest: inputDigest(base) });
  const before = records(call), existing = before.find(r => r.frozen.turn.sourceEventId === record.frozen.turn.sourceEventId
    || r.delegation.requestId === record.delegation.requestId
    || (r.delegation.sessionId === record.delegation.sessionId && r.delegation.delegationId === record.delegation.delegationId));
  if (existing) {
    if (existing.inputDigest !== record.inputDigest) fail('caller_route_identity_changed');
    return { journal: call.callerRouting!, record: existing };
  }
  const journal: CallerRoutingJournal = { v: 'broker.caller-routing.v1', records: [...before, record] };
  validateCallerRouting({ ...call, callerRouting: journal }, call);
  return { journal, record };
}

export function prepareCallerIntent(call: BrokerCall, value: CallerRouteRef,
  operation: { operationId: string; input: CallerIntentInput }): CallerRoutingJournal {
  const ref = callerRouteRefSchema.parse(value), all = records(call), record = all.find(r => same(callerRouteRef(r), ref));
  if (!record) fail('caller_route_not_found');
  if (record.resolutionInput.classificationRefusal) fail('caller_route_inventory_refused');
  const intent = callerIntentOperationSchema.parse(
    z.object({ operationId: uuid, input: callerIntentInputSchema }).strict().parse(operation));
  if (record.intent) {
    if (record.intent.operationId !== intent.operationId || !same(record.intent.input, intent.input)) fail('caller_intent_operation_changed');
    if (!call.callerRouting) fail('caller_route_not_found');
    return call.callerRouting;
  }
  const journal: CallerRoutingJournal = { v: 'broker.caller-routing.v1', records: all.map(r => r === record ? { ...r, intent } : r) };
  validateCallerRouting({ ...call, callerRouting: journal }, call);
  return journal;
}

export function recordCallerIntentResponse(call: BrokerCall, value: CallerRouteRef, body: CallerIntentResponse): CallerRoutingJournal {
  const ref = callerRouteRefSchema.parse(value), all = records(call), record = all.find(r => same(callerRouteRef(r), ref));
  if (!record?.intent) fail('caller_intent_not_prepared');
  const response = validateCallerIntentResponse(record.scope.bindingId, record.intent.operationId, record.intent.input, body);
  const intent = record.intent;
  if (same(record.intent.response, response)) {
    if (!call.callerRouting) fail('caller_route_not_found');
    return call.callerRouting;
  }
  const nextIntent = callerIntentOperationSchema.parse({ ...intent, response });
  const journal: CallerRoutingJournal = { v: 'broker.caller-routing.v1', records: all.map(r => r === record
    ? { ...r, intent: nextIntent } : r) };
  validateCallerRouting({ ...call, callerRouting: journal }, call);
  return journal;
}
export function decideCallerRouting(call: BrokerCall, value: CallerRouteRef,
  response: { inputDigest: string; decision: CallerRouteDecision }): CallerRoutingJournal {
  const ref = callerRouteRefSchema.parse(value), all = records(call), record = all.find(r => same(callerRouteRef(r), ref));
  if (!record || response.inputDigest !== record.inputDigest) fail('caller_route_identity_changed');
  const decision = callerRouteDecisionSchema.parse(response.decision);
  if (record.decision) { if (!same(record.decision, decision)) fail('caller_route_decision_changed'); return call.callerRouting!; }
  const journal: CallerRoutingJournal = { v: 'broker.caller-routing.v1', records: all.map(r => r === record ? { ...r, decision } : r) };
  parseCallerRoutingJournal(journal); validateCallerRouting({ ...call, callerRouting: journal }, call); return journal;
}
export function captureCallerRouting(call: BrokerCall, value: CallerRouteRef, receipt: PrimaryTurnReceipt): CallerRoutingJournal {
  const ref = callerRouteRefSchema.parse(value), all = records(call), record = all.find(r => same(callerRouteRef(r), ref));
  if (!record) fail('caller_route_not_found');
  const capture = captureSchema.parse(acknowledgedTurn(record.scope.bindingId, record.frozen.turn, receipt));
  if (record.capture) {
    if (!same({ ...record.capture, replayed: false }, { ...capture, replayed: false })) fail('caller_route_capture_changed');
    return call.callerRouting!;
  }
  const journal: CallerRoutingJournal = { v: 'broker.caller-routing.v1', records: all.map(r => r === record ? { ...r, capture } : r) };
  validateCallerRouting({ ...call, callerRouting: journal }, call); return journal;
}
