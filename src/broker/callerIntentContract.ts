import { createHash } from 'node:crypto';
import { z } from 'zod';
import { historyVersionSchema } from './eligibleContext.js';
import { associatedResultReferenceSchema } from './associatedResult.js';
import { canonicalJson } from '../outbound/canonical.js';

// Frozen caller-intent v1 and coverage-aware v2 wire. This chooses a route; existing
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
export const callerIntentV1InputSchema = z.object({
  v: z.literal('phone.caller-intent.v1'),
  source: z.object({ callSid: z.string().regex(/^CA[0-9a-fA-F]{32}$/), sourceEventId: uuid,
    itemId: z.string().regex(/^[a-zA-Z0-9_-]{1,128}$/), revision: z.literal(1), receivedAt: time,
    role: z.literal('user'), text: callerText }).strict(),
  generationId: uuid, sourceSessionId: opaque, brokerInputDigest: digest, presentationDigest: digest,
  callDeadlineAt: time, version: historyVersionSchema, questions, results,
}).strict().superRefine(checkCandidates);
export type CallerIntentV1Input = z.infer<typeof callerIntentV1InputSchema>;
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

const responseFields = { v: z.literal('phone.caller-intent.v1'), operationId: uuid, bindingId: uuid,
  inputDigest: digest, brokerInputDigest: digest, presentationDigest: digest, generationId: uuid,
  sourceSessionId: opaque, callDeadlineAt: time, questions, results, reservedAt: time, validUntil: time,
  version: historyVersionSchema };
export const callerIntentV1ResponseSchema = z.discriminatedUnion('state', [
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
export type CallerIntentV1Response = z.infer<typeof callerIntentV1ResponseSchema>;
export const callerIntentResultCoverageSchema = z.discriminatedUnion('state', [
  z.object({ state: z.literal('complete'), count: z.number().int().min(0).max(7) }).strict(),
  z.object({ state: z.literal('overflow'), count: z.number().int().min(8).max(Number.MAX_SAFE_INTEGER) }).strict(),
]);
export type CallerIntentResultCoverage = z.infer<typeof callerIntentResultCoverageSchema>;
export const callerIntentResultContextCoverageSchema = z.discriminatedUnion('state', [
  z.object({ state: z.literal('complete') }).strict(),
  z.object({ state: z.literal('unavailable'), reason: z.enum([
    'inventory_overflow', 'source_unavailable', 'context_budget', 'not_prepared',
  ]) }).strict(),
]);
export type CallerIntentResultContextCoverage = z.infer<typeof callerIntentResultContextCoverageSchema>;
type CoverageCandidates = Candidates & { resultCoverage: CallerIntentResultCoverage };
function checkCoverageCandidates(value: CoverageCandidates, ctx: z.RefinementCtx): void {
  checkCandidates(value, ctx);
  if (value.resultCoverage.state === 'complete'
    ? value.resultCoverage.count !== value.results.length : value.results.length !== 0)
    ctx.addIssue({ code: 'custom', path: ['resultCoverage'], message: 'Result inventory differs from its coverage declaration' });
}
function contextCoverageMatches(value: CoverageCandidates, context: CallerIntentResultContextCoverage): boolean {
  if (context.state === 'complete') return value.resultCoverage.state === 'complete';
  if (value.resultCoverage.state === 'overflow')
    return context.reason === 'inventory_overflow' || context.reason === 'not_prepared';
  return context.reason !== 'inventory_overflow'
    && (context.reason !== 'source_unavailable' || value.results.length > 0);
}
const v2Candidates = { questions, results: z.array(callerIntentResultSchema).max(7),
  resultCoverage: callerIntentResultCoverageSchema };
export const callerIntentV2InputSchema = z.object({ ...callerIntentV1InputSchema.shape,
  v: z.literal('phone.caller-intent.v2'), ...v2Candidates }).strict().superRefine(checkCoverageCandidates);
export type CallerIntentV2Input = z.infer<typeof callerIntentV2InputSchema>;
const v2ResponseFields = { ...responseFields, v: z.literal('phone.caller-intent.v2'), ...v2Candidates,
  resultContextCoverage: callerIntentResultContextCoverageSchema };
export const callerIntentV2ResponseSchema = z.discriminatedUnion('state', [
  z.object({ ...v2ResponseFields, state: z.literal('pending') }).strict(),
  z.object({ ...v2ResponseFields, state: z.literal('resolved'), decision: callerIntentDecisionSchema }).strict(),
]).superRefine((value, ctx) => {
  checkCoverageCandidates(value, ctx);
  if (!ordinaryScopeMatches(value.bindingId, value))
    ctx.addIssue({ code: 'custom', path: ['bindingId'], message: 'Ordinary candidate belongs to another call' });
  if (Date.parse(value.validUntil) !== Math.min(Date.parse(value.callDeadlineAt), Date.parse(value.reservedAt) + 10_000))
    ctx.addIssue({ code: 'custom', path: ['validUntil'], message: 'Decision deadline differs from original budget' });
  if (!contextCoverageMatches(value, value.resultContextCoverage))
    ctx.addIssue({ code: 'custom', path: ['resultContextCoverage'], message: 'Prepared context coverage contradicts supplied inventory' });
  if (value.resultContextCoverage.state === 'unavailable' && value.resultContextCoverage.reason === 'not_prepared'
    && (value.state !== 'resolved' || value.decision.kind !== 'clarify'))
    ctx.addIssue({ code: 'custom', path: ['resultContextCoverage'], message: 'Unprepared context permits only a resolved clarification' });
  if (value.state !== 'resolved') return;
  const decision = value.decision;
  if (decision.kind === 'answer' && !value.questions.some(q => canonicalJson(q) === canonicalJson(decision.target)))
    ctx.addIssue({ code: 'custom', path: ['decision', 'target'], message: 'Answer target was not supplied' });
  if (decision.kind === 'task' && decision.reference) {
    if (value.resultContextCoverage.state !== 'complete')
      ctx.addIssue({ code: 'custom', path: ['decision', 'reference'], message: 'Task reference requires complete prepared context' });
    const reference = decision.reference;
    const offered = value.results.some(result => reference.kind === 'ordinary'
      ? result.kind === 'ordinary' && result.execution.requestId === reference.relatedPrimaryRequestId
      : result.kind === 'selected' && result.deliveryAssociationId === reference.associatedResultReference.deliveryAssociationId
        && result.execution.requestId === reference.associatedResultReference.originalRequestId
        && result.assistantMessageId === reference.associatedResultReference.assistantMessageId);
    if (!offered) ctx.addIssue({ code: 'custom', path: ['decision', 'reference'], message: 'Task reference was not supplied' });
  }
});
export type CallerIntentV2Response = z.infer<typeof callerIntentV2ResponseSchema>;
export const PHONE_CALLER_INTENT_V3_VERSION = 'phone.caller-intent.v3';
export const callerIntentTaskIdentitySchema = z.object({ bindingId: uuid, requestId: uuid, runId: uuid }).strict();
export type CallerIntentTaskIdentity = z.infer<typeof callerIntentTaskIdentitySchema>;
export const callerIntentV3TaskSchema = z.object({ execution: callerIntentTaskIdentitySchema }).strict();
export const callerIntentV3TaskCoverageSchema = z.discriminatedUnion('state', [
  z.object({ state: z.literal('complete'), count: z.number().int().min(0).max(16), inventoryDigest: z.string().regex(/^[a-f0-9]{64}$/) }).strict(),
  z.object({ state: z.literal('partial'), count: z.number().int().min(1).max(Number.MAX_SAFE_INTEGER), inventoryDigest: z.string().regex(/^[a-f0-9]{64}$/) }).strict(),
]);
const delivery = z.object({ kind: z.literal('sms_result') }).strict();
const v3Task = callerIntentDecisionSchema.options[0].extend({ delivery: delivery.optional() });
export const callerIntentV3DecisionSchema = z.discriminatedUnion('kind', [
  v3Task, callerIntentDecisionSchema.options[1], callerIntentDecisionSchema.options[2],
  callerIntentDecisionSchema.options[3],
  z.object({ kind: z.literal('deliver_result'), target: callerIntentTaskIdentitySchema, delivery }).strict(),
]);
export type CallerIntentV3Decision = z.infer<typeof callerIntentV3DecisionSchema>;
const taskFields = { tasks: z.array(callerIntentV3TaskSchema).max(16), taskCoverage: callerIntentV3TaskCoverageSchema };
type V3Tasks = { tasks: Array<z.infer<typeof callerIntentV3TaskSchema>>; taskCoverage: z.infer<typeof callerIntentV3TaskCoverageSchema>;
  questions: Array<{ execution: CallerIntentTaskIdentity }>; results: Array<{ execution: CallerIntentTaskIdentity }> };
const tuple = (value: CallerIntentTaskIdentity) => JSON.stringify([value.bindingId, value.requestId, value.runId]);
function validateTasks(value: V3Tasks, ctx: z.RefinementCtx) {
  if (value.taskCoverage.state === 'complete' ? value.tasks.length !== value.taskCoverage.count : value.tasks.length >= value.taskCoverage.count)
    ctx.addIssue({ code: 'custom', path: ['taskCoverage'], message: 'Task inventory differs from declared coverage.' });
  if (new Set(value.tasks.map(task => task.execution.requestId)).size !== value.tasks.length
    || new Set(value.tasks.map(task => task.execution.runId)).size !== value.tasks.length)
    ctx.addIssue({ code: 'custom', path: ['tasks'], message: 'Task identity is duplicated.' });
  const requests = new Map<string, string>(), runs = new Map<string, string>();
  for (const candidate of [...value.tasks, ...value.questions, ...value.results]) {
    const e = candidate.execution, exact = tuple(e);
    if ((requests.has(e.requestId) && requests.get(e.requestId) !== exact)
      || (runs.has(e.runId) && runs.get(e.runId) !== exact))
      ctx.addIssue({ code: 'custom', path: ['tasks'], message: 'Candidate execution identities disagree.' });
    requests.set(e.requestId, exact); runs.set(e.runId, exact);
  }
}

export function callerIntentV3BaseInput(value: CallerIntentV3Input): CallerIntentV2Input {
  const { tasks: _tasks, taskCoverage: _coverage, ...base } = value;
  return callerIntentV2InputSchema.parse({ ...base, v: 'phone.caller-intent.v2' });
}
export const callerIntentV3InputSchema = z.object({...callerIntentV2InputSchema.shape,
  v: z.literal(PHONE_CALLER_INTENT_V3_VERSION), ...taskFields,
}).strict().superRefine((value, ctx) => {
  validateTasks(value, ctx);
  const { tasks: _tasks, taskCoverage: _coverage, ...base } = value;
  const checked = callerIntentV2InputSchema.safeParse({ ...base, v: 'phone.caller-intent.v2' });
  if (!checked.success) for (const issue of checked.error.issues) ctx.addIssue({code:'custom',path:issue.path,message:issue.message});
});
export type CallerIntentV3Input = z.infer<typeof callerIntentV3InputSchema>;

const previousResponse = callerIntentV2ResponseSchema.options;
const common = { v: z.literal(PHONE_CALLER_INTENT_V3_VERSION), ...taskFields };
export const callerIntentV3ResponseSchema = z.discriminatedUnion('state', [
  previousResponse[0].extend(common).strict(),
  previousResponse[1].extend({ ...common, decision: callerIntentV3DecisionSchema }).strict(),
]).superRefine((value, ctx) => {
  validateTasks(value, ctx);
  if (value.tasks.some(task => task.execution.bindingId !== value.bindingId))
    ctx.addIssue({ code: 'custom', path: ['tasks'], message: 'Task belongs to another call.' });
  const { tasks: _tasks, taskCoverage: _coverage, ...base } = value;
  const decision = value.state === 'resolved' ? value.decision : undefined;
  const oldDecision = decision?.kind === 'deliver_result' ? { kind: 'conversation' }
    : decision?.kind === 'task' ? { kind: 'task', ...(decision.reference ? { reference: decision.reference } : {}) } : decision;
  const checked = callerIntentV2ResponseSchema.safeParse({ ...base, v: 'phone.caller-intent.v2',
    ...(oldDecision ? { decision: oldDecision } : {}) });
  if (!checked.success) for (const issue of checked.error.issues) ctx.addIssue({code:'custom',path:issue.path,message:issue.message});
  if (decision?.kind === 'deliver_result' && (!value.tasks.some(task => tuple(task.execution) === tuple(decision.target))))
    ctx.addIssue({ code: 'custom', path: ['decision'], message: 'Delivery target is not an exact offered task.' });
});
export type CallerIntentV3Response = z.infer<typeof callerIntentV3ResponseSchema>;

export function assertCallerIntentV3Scope(bindingId: string, input: CallerIntentV3Input) {
  uuid.parse(bindingId); callerIntentV3InputSchema.parse(input);
  if (input.tasks.some(task => task.execution.bindingId !== bindingId)) throw new Error('caller_intent_candidate_binding_mismatch');
  // The existing v2 digest checks the ordinary question/result binding scopes.
  callerIntentV2InputDigest(bindingId, bindingId, callerIntentV3BaseInput(input));
}
export function callerIntentV3InputDigest(operationId: string, bindingId: string, input: CallerIntentV3Input): string {
  assertCallerIntentV3Scope(bindingId, input);
  const baseDigest = callerIntentV2InputDigest(operationId, bindingId, callerIntentV3BaseInput(input));
  return createHash('sha256').update(JSON.stringify(['dexter-phone-caller-intent-input/v3', baseDigest,
    input.tasks.map(task => tuple(task.execution)), [input.taskCoverage.state, input.taskCoverage.count, input.taskCoverage.inventoryDigest]])).digest('hex');
}

export type CallerIntentDecision = z.infer<typeof callerIntentV3DecisionSchema>;
export const callerIntentInputSchema = z.discriminatedUnion('v', [callerIntentV1InputSchema, callerIntentV2InputSchema, callerIntentV3InputSchema]);
export const callerIntentResponseSchema = z.union([callerIntentV1ResponseSchema, callerIntentV2ResponseSchema, callerIntentV3ResponseSchema]);
export type CallerIntentInput = CallerIntentV1Input | CallerIntentV2Input | CallerIntentV3Input;
export type CallerIntentResponse = CallerIntentV1Response | CallerIntentV2Response | CallerIntentV3Response;
export const callerIntentV1OperationSchema = z.object({ operationId: uuid, input: callerIntentV1InputSchema,
  response: callerIntentV1ResponseSchema.optional() }).strict();
export const callerIntentV2OperationSchema = z.object({ operationId: uuid, input: callerIntentV2InputSchema,
  response: callerIntentV2ResponseSchema.optional() }).strict();
export const callerIntentV3OperationSchema = z.object({operationId:uuid,input:callerIntentV3InputSchema,response:callerIntentV3ResponseSchema.optional()}).strict();
export const callerIntentOperationSchema = z.union([callerIntentV1OperationSchema, callerIntentV2OperationSchema, callerIntentV3OperationSchema]);
export type CallerIntentOperation = z.infer<typeof callerIntentOperationSchema>;

export function callerIntentV1InputDigest(operationId: string, bindingId: string, value: CallerIntentV1Input): string {
  uuid.parse(operationId); uuid.parse(bindingId);
  const input = callerIntentV1InputSchema.parse(value), s = input.source, v = input.version;
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

export function callerIntentV2InputDigest(operationId: string, bindingId: string, value: CallerIntentV2Input): string {
  uuid.parse(operationId); uuid.parse(bindingId);
  const input = callerIntentV2InputSchema.parse(value), s = input.source, v = input.version;
  if (!ordinaryScopeMatches(bindingId, input)) throw new Error('caller_intent_candidate_binding_mismatch');
  const ordered = ['dexter-phone-caller-intent-input/v2', operationId, bindingId,
    [s.callSid, s.sourceEventId, s.itemId, s.revision, s.receivedAt, 'user', s.text],
    input.generationId, input.sourceSessionId, input.brokerInputDigest, input.presentationDigest, input.callDeadlineAt,
    [v.ownerPrincipalId, v.primaryDexterId, v.threadId, v.contextEpoch, v.threadEventSequence, v.digest],
    input.questions.map(q => [q.kind, q.execution.bindingId, q.execution.requestId, q.execution.runId,
      q.question.id, 1, q.presentationId, q.kind === 'selected' ? q.deliveryAssociationId : null]),
    input.results.map(r => [r.kind, r.execution.bindingId, r.execution.requestId, r.execution.runId,
      r.assistantMessageId, r.presentationId, r.kind === 'selected' ? r.deliveryAssociationId : null]),
    [input.resultCoverage.state, input.resultCoverage.count]];
  return createHash('sha256').update(JSON.stringify(ordered), 'utf8').digest('hex');
}
export function callerIntentInputDigest(operationId: string, bindingId: string, value: CallerIntentInput): string {
  const input = callerIntentInputSchema.parse(value);
  return input.v === 'phone.caller-intent.v1'
    ? callerIntentV1InputDigest(operationId, bindingId, input) : input.v === 'phone.caller-intent.v3'
      ? callerIntentV3InputDigest(operationId,bindingId,input) : callerIntentV2InputDigest(operationId, bindingId, input);
}

/** Validate both the transport envelope and its exact immutable submitted input. */
export function validateCallerIntentResponse(bindingId: string, operationId: string,
  value: CallerIntentInput, body: unknown, previous?: CallerIntentResponse): CallerIntentResponse {
  const input = callerIntentInputSchema.parse(value), response = callerIntentResponseSchema.parse(body);
  const same = (a: unknown, b: unknown) => canonicalJson(a) === canonicalJson(b);
  if (response.v !== input.v || response.bindingId !== bindingId || response.operationId !== operationId
    || response.inputDigest !== callerIntentInputDigest(operationId, bindingId, input)
    || response.brokerInputDigest !== input.brokerInputDigest || response.presentationDigest !== input.presentationDigest
    || response.generationId !== input.generationId || response.sourceSessionId !== input.sourceSessionId
    || response.callDeadlineAt !== input.callDeadlineAt || !same(response.version, input.version)
    || !same(response.questions, input.questions) || !same(response.results, input.results)
    || Date.parse(response.validUntil) !== Math.min(Date.parse(input.callDeadlineAt), Date.parse(response.reservedAt) + 10_000))
    throw new Error('caller_intent_response_identity_changed');
  if (input.v !== 'phone.caller-intent.v1'
    && (response.v !== input.v || !same(response.resultCoverage, input.resultCoverage)))
    throw new Error('caller_intent_response_identity_changed');
  if (input.v === 'phone.caller-intent.v3' && (response.v !== input.v
    || !same(response.tasks,input.tasks) || !same(response.taskCoverage,input.taskCoverage)))
    throw new Error('caller_intent_response_identity_changed');
  if (previous) {
    const saved = validateCallerIntentResponse(bindingId, operationId, input, previous);
    if (saved.reservedAt !== response.reservedAt || saved.validUntil !== response.validUntil
      || (saved.v !== 'phone.caller-intent.v1'
        && (response.v !== saved.v || !same(saved.resultContextCoverage, response.resultContextCoverage)))
      || (saved.state === 'resolved' && !same(saved, response)))
      throw new Error('caller_intent_response_changed');
  }
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
