import { createHash } from 'node:crypto';
import { z } from 'zod';
import { historyVersionSchema } from './eligibleContext.js';
import { associatedResultReferenceSchema } from './associatedResult.js';
import { canonicalJson } from '../outbound/canonical.js';

// Frozen shared caller-intent wire6137. This operation chooses a route; existing
// task and question endpoints still own admission and execution authority.
const uuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
const digest = z.string().regex(/^[0-9a-f]{64}$/);
const time = z.iso.datetime({ offset: true });
const opaque = z.string().min(1).max(200);
const execution = z.object({ bindingId: uuid, requestId: uuid, runId: uuid }).strict();
const question = z.object({ id: uuid, revision: z.literal(1) }).strict();
export const callerIntentQuestionSchema = z.discriminatedUnion('kind', [
  z.object({ kind: z.literal('ordinary'), execution, question, presentationId: opaque }).strict(),
  z.object({ kind: z.literal('selected'), execution, question, presentationId: opaque, deliveryAssociationId: uuid }).strict(),
]);
export const callerIntentResultSchema = z.discriminatedUnion('kind', [
  z.object({ kind: z.literal('ordinary'), execution, assistantMessageId: uuid, presentationId: opaque }).strict(),
  z.object({ kind: z.literal('selected'), execution, assistantMessageId: uuid, presentationId: opaque, deliveryAssociationId: uuid }).strict(),
]);
export type CallerIntentQuestion = z.infer<typeof callerIntentQuestionSchema>;
export type CallerIntentResult = z.infer<typeof callerIntentResultSchema>;
const questions = z.array(callerIntentQuestionSchema).max(2);
const results = z.array(callerIntentResultSchema).max(2);
type Candidates = { questions: CallerIntentQuestion[]; results: CallerIntentResult[] };
function checkCandidates(value: Candidates, ctx: z.RefinementCtx): void {
  for (const key of ['questions', 'results'] as const) {
    const seen = new Set<string>();
    value[key].forEach((candidate, index) => {
      const identity = 'question' in candidate ? candidate.question.id : candidate.assistantMessageId;
      if (seen.has(identity)) ctx.addIssue({ code: 'custom', path: [key, index], message: 'Duplicate candidate identity' });
      seen.add(identity);
    });
  }
  const requests = new Map<string, string>(), runs = new Map<string, string>(), associations = new Map<string, string>();
  for (const candidate of [...value.questions, ...value.results]) {
    const execution = candidate.execution;
    const requestIdentity = JSON.stringify([execution.bindingId, execution.runId]);
    const runIdentity = JSON.stringify([execution.bindingId, execution.requestId]);
    if ((requests.has(execution.requestId) && requests.get(execution.requestId) !== requestIdentity)
      || (runs.has(execution.runId) && runs.get(execution.runId) !== runIdentity))
      ctx.addIssue({ code: 'custom', message: 'Candidate execution identities disagree' });
    requests.set(execution.requestId, requestIdentity); runs.set(execution.runId, runIdentity);
    if (candidate.kind === 'selected') {
      const identity = JSON.stringify([execution.bindingId, execution.requestId, execution.runId]);
      if (associations.has(candidate.deliveryAssociationId) && associations.get(candidate.deliveryAssociationId) !== identity)
        ctx.addIssue({ code: 'custom', message: 'Delivery association execution identities disagree' });
      associations.set(candidate.deliveryAssociationId, identity);
    }
  }
}
const ordinaryScopeMatches = (bindingId: string, candidates: Candidates) =>
  [...candidates.questions, ...candidates.results].every(candidate => candidate.kind !== 'ordinary'
    || candidate.execution.bindingId === bindingId);
const callerText = z.string().refine(value => value.trim().length > 0 && [...value].length <= 4000
  && !value.includes('\0') && !/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/u.test(value));
export const callerIntentInputSchema = z.object({
  v: z.literal('phone.caller-intent.v1'),
  source: z.object({ callSid: z.string().regex(/^CA[0-9a-fA-F]{32}$/), sourceEventId: uuid,
    itemId: z.string().regex(/^[a-zA-Z0-9_-]{1,128}$/), revision: z.literal(1), receivedAt: time,
    role: z.literal('user'), text: callerText }).strict(),
  generationId: uuid, sourceSessionId: opaque, brokerInputDigest: digest, presentationDigest: digest,
  callDeadlineAt: time, version: historyVersionSchema, questions, results,
}).strict().superRefine(checkCandidates);
export type CallerIntentInput = z.infer<typeof callerIntentInputSchema>;
const taskReference = z.discriminatedUnion('kind', [
  z.object({ kind: z.literal('ordinary'), relatedPrimaryRequestId: uuid,
    referenceOrigin: z.literal('broker_status_publication') }).strict(),
  z.object({ kind: z.literal('associated'), associatedResultReference: associatedResultReferenceSchema }).strict(),
]);
export const callerIntentDecisionSchema = z.discriminatedUnion('kind', [
  z.object({ kind: z.literal('task'), reference: taskReference.optional() }).strict(),
  z.object({ kind: z.literal('answer'), target: callerIntentQuestionSchema }).strict(),
  z.object({ kind: z.literal('clarify'), reason: z.enum(['ambiguous_target', 'mixed_intent', 'incomplete',
    'context_changed', 'candidate_unavailable', 'budget_exhausted', 'deadline_exceeded',
    'provider_unavailable', 'provider_uncertain', 'invalid_output']) }).strict(),
  z.object({ kind: z.literal('conversation') }).strict(),
]);
export type CallerIntentDecision = z.infer<typeof callerIntentDecisionSchema>;
const responseFields = { v: z.literal('phone.caller-intent.v1'), operationId: uuid, bindingId: uuid,
  inputDigest: digest, brokerInputDigest: digest, presentationDigest: digest, generationId: uuid,
  sourceSessionId: opaque, callDeadlineAt: time, questions, results, reservedAt: time, validUntil: time,
  version: historyVersionSchema };
export const callerIntentResponseSchema = z.discriminatedUnion('state', [
  z.object({ ...responseFields, state: z.literal('pending') }).strict(),
  z.object({ ...responseFields, state: z.literal('resolved'), decision: callerIntentDecisionSchema }).strict(),
]).superRefine((value, ctx) => {
  checkCandidates(value, ctx);
  if (!ordinaryScopeMatches(value.bindingId, value))
    ctx.addIssue({ code: 'custom', path: ['bindingId'], message: 'Ordinary candidate belongs to another call' });
  if (Date.parse(value.validUntil) !== Math.min(Date.parse(value.callDeadlineAt), Date.parse(value.reservedAt) + 10_000))
    ctx.addIssue({ code: 'custom', path: ['validUntil'], message: 'Decision deadline differs from original budget' });
  if (value.state !== 'resolved') return;
  const decision = value.decision;
  if (decision.kind === 'answer' && !value.questions.some(q => canonicalJson(q) === canonicalJson(decision.target)))
    ctx.addIssue({ code: 'custom', path: ['decision', 'target'], message: 'Answer target was not supplied' });
  if (decision.kind === 'task' && decision.reference) {
    const reference = decision.reference;
    const offered = value.results.some(result => reference.kind === 'ordinary'
      ? result.kind === 'ordinary' && result.execution.requestId === reference.relatedPrimaryRequestId
      : result.kind === 'selected' && result.deliveryAssociationId === reference.associatedResultReference.deliveryAssociationId
        && result.execution.requestId === reference.associatedResultReference.originalRequestId
        && result.assistantMessageId === reference.associatedResultReference.assistantMessageId);
    if (!offered) ctx.addIssue({ code: 'custom', path: ['decision', 'reference'], message: 'Task reference was not supplied' });
  }
});
export type CallerIntentResponse = z.infer<typeof callerIntentResponseSchema>;
export const callerIntentOperationSchema = z.object({ operationId: uuid, input: callerIntentInputSchema,
  response: callerIntentResponseSchema.optional() }).strict();
export type CallerIntentOperation = z.infer<typeof callerIntentOperationSchema>;

export function callerIntentInputDigest(operationId: string, bindingId: string, value: CallerIntentInput): string {
  uuid.parse(operationId); uuid.parse(bindingId);
  const input = callerIntentInputSchema.parse(value), s = input.source, v = input.version;
  if (!ordinaryScopeMatches(bindingId, input)) throw new Error('caller_intent_candidate_binding_mismatch');
  const ordered = ['dexter-phone-caller-intent-input/v1', operationId, bindingId,
    [s.callSid, s.sourceEventId, s.itemId, s.revision, s.receivedAt, 'user', s.text],
    input.generationId, input.sourceSessionId, input.brokerInputDigest, input.presentationDigest, input.callDeadlineAt,
    [v.ownerPrincipalId, v.primaryDexterId, v.threadId, v.contextEpoch, v.threadEventSequence, v.digest],
    input.questions.map(q => [q.kind, q.execution.bindingId, q.execution.requestId, q.execution.runId,
      q.question.id, 1, q.presentationId, q.kind === 'selected' ? q.deliveryAssociationId : null]),
    input.results.map(r => [r.kind, r.execution.bindingId, r.execution.requestId, r.execution.runId,
      r.assistantMessageId, r.presentationId, r.kind === 'selected' ? r.deliveryAssociationId : null])];
  return createHash('sha256').update(JSON.stringify(ordered), 'utf8').digest('hex');
}

/** Validate both the transport envelope and its exact immutable submitted input. */
export function validateCallerIntentResponse(bindingId: string, operationId: string,
  value: CallerIntentInput, body: unknown): CallerIntentResponse {
  const input = callerIntentInputSchema.parse(value), response = callerIntentResponseSchema.parse(body);
  const same = (a: unknown, b: unknown) => canonicalJson(a) === canonicalJson(b);
  if (response.bindingId !== bindingId || response.operationId !== operationId
    || response.inputDigest !== callerIntentInputDigest(operationId, bindingId, input)
    || response.brokerInputDigest !== input.brokerInputDigest || response.presentationDigest !== input.presentationDigest
    || response.generationId !== input.generationId || response.sourceSessionId !== input.sourceSessionId
    || response.callDeadlineAt !== input.callDeadlineAt || !same(response.version, input.version)
    || !same(response.questions, input.questions) || !same(response.results, input.results)
    || Date.parse(response.validUntil) !== Math.min(Date.parse(input.callDeadlineAt), Date.parse(response.reservedAt) + 10_000))
    throw new Error('caller_intent_response_identity_changed');
  if (response.state === 'resolved') {
    const d = response.decision;
    if (d.kind === 'answer' && !input.questions.some(q => same(q, d.target)))
      throw new Error('caller_intent_target_not_offered');
    if (d.kind === 'task' && d.reference) {
      const reference = d.reference;
      const offered = reference.kind === 'ordinary'
        ? input.results.some(r => r.kind === 'ordinary' && r.execution.requestId === reference.relatedPrimaryRequestId)
        : input.results.some(r => r.kind === 'selected'
          && r.deliveryAssociationId === reference.associatedResultReference.deliveryAssociationId
          && r.execution.requestId === reference.associatedResultReference.originalRequestId
          && r.assistantMessageId === reference.associatedResultReference.assistantMessageId);
      if (!offered) throw new Error('caller_intent_reference_not_offered');
    }
  }
  return response;
}
