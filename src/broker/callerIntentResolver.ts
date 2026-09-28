import { canonicalJson } from '../outbound/canonical.js';
import { brokerIdentity, withCallerAdmission } from './backend.js';
import { callerIntentV2InputSchema, callerIntentV3InputSchema, type CallerIntentV2Input, type CallerIntentDecision } from './callerIntentContract.js';
import type { CallerIntentClient } from './callerIntentClient.js';
import { callerIntentQuestionForTarget, callerRoutePresentationDigest, callerRouteRef,
  decideCallerRouting, getCallerRouteBySource, validateCallerRouting,
  type CallerRouteDecision, type CallerRouteRecord } from './callerRouting.js';
import { callerIntentResultPublicationCandidates, callerIntentResultPublicationInventory } from './publications.js';
import type { CallerIntentPort } from './mixedWork.js';
import type { BrokerCall, BrokerStore } from './store.js';

const same = (a: unknown, b: unknown) => canonicalJson(a) === canonicalJson(b);
function refuse(code: string): never { throw new Error(code); }
const immutableRoute = ({ decision: _decision, intent: _intent, capture: _capture, ...input }: CallerRouteRecord) => input;

function current(call: BrokerCall, record: CallerRouteRecord, deadline: string): void {
  const generations = call.liveGenerations, attachment = generations?.deliveryAttachment;
  const segment = generations?.segments.find(value => value.generationId === record.scope.generationId);
  if (call.endedAt || call.finalized || call.primaryClosed
    || ['completed', 'busy', 'no-answer', 'canceled', 'failed'].includes(call.status)
    || !generations || generations.phase !== 'active' || !attachment || !segment || segment.state !== 'active'
    || attachment.generationId !== record.scope.generationId || attachment.sessionId !== record.frozen.sessionId
    || call.liveSessionId !== record.frozen.sessionId) refuse('caller_intent_source_inactive');
  if (deadline !== generations.clocks.callDeadlineAt || Date.now() >= Date.parse(deadline))
    refuse('caller_intent_deadline_exceeded');
  if (segment.transcript.callerRevision !== record.frozen.callerRevision
    || (call.publications?.records.at(-1)?.publicationSequence ?? 0) !== record.resolutionInput.publicationSequence
    || !same(segment.version, record.resolutionInput.version)) refuse('caller_intent_context_changed');
}

/** Build the exact frozen input. Candidate coverage is never shortened to fit. */
export function buildCallerIntentInput(call: BrokerCall, record: CallerRouteRecord): CallerIntentV2Input {
  validateCallerRouting(call);
  const saved = getCallerRouteBySource(call, record.frozen.turn.sourceEventId);
  if (!saved || !same(immutableRoute(saved), immutableRoute(record))) refuse('caller_intent_route_changed');
  if (record.resolutionInput.classificationRefusal) refuse('caller_route_inventory_refused');
  const deadline = call.liveGenerations?.clocks.callDeadlineAt;
  if (!deadline) refuse('caller_intent_deadline_missing');
  current(call, record, deadline);
  const inventory = callerIntentResultPublicationInventory(call, record.frozen.sessionId);
  const { candidates, resultCoverage } = inventory;
  if (!record.resolutionInput.resultCoverage) refuse('caller_intent_legacy_dispatch_refused');
  if (!same(candidates.map(({ id, digest }) => ({ id, digest })), record.resolutionInput.references)
    || !same(resultCoverage, record.resolutionInput.resultCoverage))
    refuse('caller_intent_candidate_coverage_changed');
  const turn = record.frozen.turn;
  return callerIntentV2InputSchema.parse({ v: 'phone.caller-intent.v2',
    source: { callSid: record.scope.callSid, sourceEventId: turn.sourceEventId, itemId: turn.itemId,
      revision: turn.revision, receivedAt: turn.occurredAt, role: turn.role, text: turn.text },
    generationId: record.scope.generationId, sourceSessionId: record.frozen.sessionId,
    brokerInputDigest: record.inputDigest, presentationDigest: callerRoutePresentationDigest(record.resolutionInput),
    callDeadlineAt: deadline, version: record.resolutionInput.version,
    questions: record.resolutionInput.questions.map(callerIntentQuestionForTarget),
    resultCoverage, results: resultCoverage.state === 'complete' ? candidates.map(candidate => candidate.candidate) : [] });
}

function routeDecision(call: BrokerCall, record: CallerRouteRecord, decision: CallerIntentDecision): CallerRouteDecision {
  if (decision.kind === 'conversation') return { kind: 'conversation' };
  if (decision.kind === 'clarify') {
    switch (decision.reason) {
      case 'ambiguous_target': case 'mixed_intent': case 'incomplete': return { kind: 'clarify', reason: decision.reason };
      case 'context_changed': return { kind: 'clarify', reason: 'stale_context' };
      case 'invalid_output': return { kind: 'clarify', reason: 'invalid_resolution' };
      default: return { kind: 'clarify', reason: 'unavailable' };
    }
  }
  if (decision.kind === 'answer') {
    const matches = record.resolutionInput.questions.filter(target => same(callerIntentQuestionForTarget(target), decision.target));
    if (matches.length !== 1 || !matches[0]) refuse('caller_intent_target_not_offered');
    return { kind: 'answer', target: matches[0] };
  }
  if (decision.kind === 'deliver_result') return {kind:'deliver_result',target:decision.target,delivery:decision.delivery};
  const delivery=decision.delivery ? {delivery:decision.delivery} : {};
  if (!decision.reference) return { kind: 'task',...delivery };
  const candidates = callerIntentResultPublicationCandidates(call, record.frozen.sessionId);
  const matches = record.resolutionInput.references.filter(ref => candidates.some(candidate =>
    candidate.id === ref.id && candidate.digest === ref.digest && same(candidate.reference, decision.reference)));
  if (!matches[0] || (decision.reference.kind === 'associated' && matches.length !== 1))
    refuse('caller_intent_reference_not_offered');
  // Ordinary wire references identify the request, not one assistant message.
  // Equivalent matches keep the first frozen offer as the internal receipt
  // anchor; the complete ordered input remains saved and no recency is inferred.
  return { kind: 'task', contextReference: matches[0],...delivery };
}

export interface CallerIntentResolverOptions {
  callId: string;
  store: BrokerStore;
  client: CallerIntentClient;
  /** Explicit host wait, bounded by the fixed decision budget and call deadline. */
  timeoutMs: number;
}

/** Persists inference evidence only. The router separately persists one route
 * decision, then existing task/reply admissions recheck current authority. */
export function createCallerIntentResolver(options: CallerIntentResolverOptions): CallerIntentPort {
  if (!Number.isSafeInteger(options.timeoutMs) || options.timeoutMs < 1 || options.timeoutMs > 10_000)
    refuse('caller_intent_budget_unconfigured');
  const load = (record: CallerRouteRecord) => {
    const call = options.store.get(options.callId);
    if (!call || record.scope.callId !== options.callId) refuse('caller_intent_call_changed');
    validateCallerRouting(call);
    const saved = getCallerRouteBySource(call, record.frozen.turn.sourceEventId);
    if (!saved || !same(immutableRoute(saved), immutableRoute(record))) refuse('caller_intent_route_changed');
    return { call, saved };
  };
  return { timeoutMs: options.timeoutMs, async resolve(input, signal) {
    const expectedOperationId = brokerIdentity('caller-intent', options.callId, input.record.frozen.sessionId,
      input.record.delegation.delegationId, input.record.frozen.turn.sourceEventId);
    if (input.operationId !== expectedOperationId) refuse('caller_intent_operation_changed');
    if (signal.aborted) refuse('caller_intent_interrupted');
    const controller = new AbortController();
    const stopped = () => controller.abort(new Error('caller_intent_interrupted'));
    signal.addEventListener('abort', stopped, { once: true });
    const remaining = Math.min(options.timeoutMs, Date.parse(input.callDeadlineAt) - Date.now());
    if (!Number.isFinite(remaining) || remaining < 1) {
      signal.removeEventListener('abort', stopped); refuse('caller_intent_deadline_exceeded');
    }
    const timer = setTimeout(() => controller.abort(new Error('caller_intent_timeout')), remaining);
    let abortListener: (() => void) | undefined;
    const interrupted = new Promise<never>((_resolve, reject) => {
      abortListener = () => reject(controller.signal.reason);
      controller.signal.addEventListener('abort', abortListener, { once: true });
    });
    const alive = () => { if (controller.signal.aborted) refuse('caller_intent_interrupted'); };
    try {
      const prepared = await Promise.race([withCallerAdmission(options.store, options.callId, async () => {
        alive(); const { call, saved } = load(input.record);
        if (saved.intent && saved.intent.operationId !== input.operationId) refuse('caller_intent_operation_changed');
        if (saved.decision) return { record: saved, submit: false };
        current(call, saved, input.callDeadlineAt);
        if (saved.intent) return { record: saved, submit: false };
        const base=buildCallerIntentInput(call,saved);
        const candidates=await options.client.taskCandidates?.(saved.scope.bindingId,base.source,controller.signal);
        alive();
        current(load(input.record).call,saved,input.callDeadlineAt);
        const body=candidates ? callerIntentV3InputSchema.parse({...base,v:'phone.caller-intent.v3',...candidates}) : base;
        const record = options.store.prepareCallerIntent(options.callId, callerRouteRef(saved), {
          operationId: input.operationId, input: body });
        return { record, submit: true };
      }), interrupted]);
      alive();
      if (prepared.record.decision) return { inputDigest: prepared.record.inputDigest, decision: prepared.record.decision };
      const intent = prepared.record.intent;
      if (!intent) refuse('caller_intent_not_prepared');
      let response = intent.response;
      if (response?.state !== 'resolved') {
        // A saved operation might have reached the API. Even a recovered404
        // cannot cause a second POST or a replacement operation ID.
        alive();
        const beforeHttp = load(input.record);
        if (beforeHttp.saved.decision) return { inputDigest: beforeHttp.saved.inputDigest, decision: beforeHttp.saved.decision };
        current(beforeHttp.call, prepared.record, input.callDeadlineAt);
        if (prepared.submit && intent.input.v === 'phone.caller-intent.v1') refuse('caller_intent_legacy_dispatch_refused');
        const operation = prepared.submit && intent.input.v !== 'phone.caller-intent.v1'
          ? options.client.submit(prepared.record.scope.bindingId, intent.operationId, structuredClone(intent.input), controller.signal)
          : options.client.recover(prepared.record.scope.bindingId, intent.operationId, structuredClone(intent.input), controller.signal);
        response = await Promise.race([operation, interrupted]);
      }
      const exactResponse = response;
      return await Promise.race([withCallerAdmission(options.store, options.callId, () => {
        const loaded = load(input.record);
        const saved = options.store.recordCallerIntentResponse(options.callId, callerRouteRef(loaded.saved), exactResponse);
        // Retain a received envelope even if the call was stopped meanwhile;
        // it cannot replace an existing local clarification or authorize work.
        alive();
        if (saved.decision) return { inputDigest: saved.inputDigest, decision: saved.decision };
        const call = load(input.record).call;
        current(call, saved, input.callDeadlineAt);
        const receipt = saved.intent?.response;
        if (!receipt || receipt.state !== 'resolved') refuse('caller_intent_pending');
        if (!saved.intent || saved.intent.input.v === 'phone.caller-intent.v1' || receipt.v === 'phone.caller-intent.v1')
          refuse('caller_intent_legacy_dispatch_refused');
        if (Date.now() >= Date.parse(receipt.validUntil)) refuse('caller_intent_expired');
        const decision = routeDecision(call, saved, receipt.decision);
        // Reuse the journal's target/receipt/frontier checks without saving a
        // decision here. The caller's next journal write must pass them again.
        decideCallerRouting(call, callerRouteRef(saved), { inputDigest: saved.inputDigest, decision });
        return { inputDigest: saved.inputDigest, decision };
      }), interrupted]);
    } finally {
      clearTimeout(timer); signal.removeEventListener('abort', stopped);
      if (abortListener) controller.signal.removeEventListener('abort', abortListener);
    }
  } };
}
