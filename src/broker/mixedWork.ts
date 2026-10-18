import { brokerIdentity, withCallerAdmission } from './backend.js';
import { callerRouteRef, callerRouteUsedSourceIds, findCallerRoute,
  type CallerRouteDecision, type CallerRouteRecord, type CallerRouteRef, type CallerRoutePreparation } from './callerRouting.js';
import { acknowledgedTurn } from './reconciliation.js';
import type { CallerTurnFreezer } from './callerTurn.js';
import type { BrokerStore } from './store.js';
import type { PrimaryApiClient } from './primaryApi.js';
import type { LiveBackendChannel, LiveDelegationRequest } from '../live/types.js';

/** Internal composition port, not the pending shared HTTP schema. The Primary
 * adapter owns durable inference attempts, policy, accounting and operation
 * recovery. It cannot submit work or reply to a question through this port. */
export interface CallerIntentPort {
  /** Must be supplied by the reviewed Primary policy; no implicit retry budget. */
  timeoutMs: number;
  resolve(input: { operationId: string; record: CallerRouteRecord; callDeadlineAt: string },
    signal: AbortSignal): Promise<{ inputDigest: string; decision: CallerRouteDecision }>;
}

export interface MixedWorkOptions {
  callId: string;
  store: BrokerStore;
  intent: CallerIntentPort;
  freezer: CallerTurnFreezer;
  resolutionInput(request: LiveDelegationRequest): CallerRoutePreparation['resolutionInput'];
  active(sessionId: string): boolean;
  admit(request: LiveDelegationRequest, ref: CallerRouteRef, decision: CallerRouteDecision,
    channel: LiveBackendChannel): Promise<void>;
  clarify(request: LiveDelegationRequest, reason: string): Promise<void>;
  capture: Pick<PrimaryApiClient, 'captureTurn'>;
  admissionSettled?(request: LiveDelegationRequest): void;
}

/** One raw source, one durable decision, one downstream route. Every pending
 * source remains a capture obligation even if inference or call delivery ends. */
export function createMixedWorkRouter(options: MixedWorkOptions) {
  if (!Number.isSafeInteger(options.intent.timeoutMs) || options.intent.timeoutMs < 1)
    throw new Error('caller_intent_budget_unconfigured');
  const inFlight = new Map<string, { request: LiveDelegationRequest; promise: Promise<void>;
    admissionSettled: boolean; waitingAdmissions: Set<LiveDelegationRequest> }>();
  const admitting = new Set<LiveDelegationRequest>();
  const notified = new Set<string>(), resolutionAborts = new Set<AbortController>();
  let preparing: Promise<unknown> = Promise.resolve();
  let stopping = false;
  const call = () => options.store.get(options.callId) ?? (() => { throw new Error('caller_route_call_missing'); })();
  const settle = (request: LiveDelegationRequest) => {
    const entry = inFlight.get(JSON.stringify([request.sessionId, request.delegationId]));
    if (!entry) { admitting.delete(request); options.admissionSettled?.(request); return; }
    entry.admissionSettled = true;
    admitting.delete(entry.request);
    for (const pending of entry.waitingAdmissions) options.admissionSettled?.(pending);
    entry.waitingAdmissions.clear();
  };
  const capture = async (record: CallerRouteRecord) => withCallerAdmission(options.store, options.callId, async () => {
    const current = call().callerRouting?.records.find(r => r.inputDigest === record.inputDigest);
    if (!current) throw new Error('caller_route_not_found');
    if (current.capture) return;
    const receipt = await options.capture.captureTurn(record.scope.bindingId, structuredClone(record.frozen.turn));
    acknowledgedTurn(record.scope.bindingId, record.frozen.turn, receipt);
    options.store.captureCallerRoute(options.callId, callerRouteRef(record), receipt);
  });
  const prepare = (request: LiveDelegationRequest, channel: LiveBackendChannel) => {
    const work = preparing.then(async () => {
      const saved = findCallerRoute(call(), request);
      if (saved) return saved;
      if (stopping || channel.signal.aborted || !options.active(request.sessionId)) throw new Error('caller_route_source_inactive');
      // The freezer owns the same admission lock; do not recursively acquire it.
      const frozen = await options.freezer.freeze(request, channel, callerRouteUsedSourceIds(call()));
      if (!frozen) throw new Error('caller_route_turn_unqualified');
      return options.store.prepareCallerRoute(options.callId, {
        delegation: { sessionId: request.sessionId, delegationId: request.delegationId,
          requestId: request.requestId, offsetMs: request.offsetMs }, frozen,
        resolutionInput: options.resolutionInput(request),
      });
    });
    preparing = work.catch(() => {});
    return work;
  };
  const notice = async (request: LiveDelegationRequest, record: CallerRouteRecord, reason: string) => {
    if (stopping || !options.active(request.sessionId) || notified.has(record.inputDigest)) return;
    notified.add(record.inputDigest);
    await options.clarify(request, reason);
  };
  const run = async (request: LiveDelegationRequest, channel: LiveBackendChannel) => {
    let record = await prepare(request, channel);
    if (!record.decision && record.resolutionInput.classificationRefusal) {
      // Preserve the exact frozen source before any local clarification/capture.
      // An unverified inventory never reaches classification as an empty one.
      record = options.store.decideCallerRoute(options.callId, callerRouteRef(record), {
        inputDigest: record.inputDigest, decision: { kind: 'clarify', reason: 'unavailable' },
      });
    }
    if (!record.decision && !stopping && !channel.signal.aborted && options.active(request.sessionId)) {
      const abort = new AbortController(); resolutionAborts.add(abort);
      const stopped = () => abort.abort(new Error('caller_route_source_inactive'));
      channel.signal.addEventListener('abort', stopped, { once: true });
      const deadline = call().liveGenerations!.clocks.callDeadlineAt;
      const remaining = Math.min(options.intent.timeoutMs, Date.parse(deadline) - Date.now());
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        if (remaining < 1) throw new Error('caller_intent_budget_exhausted');
        const interrupted = new Promise<never>((_, reject) => {
          if (abort.signal.aborted) reject(abort.signal.reason);
          else abort.signal.addEventListener('abort', () => reject(abort.signal.reason), { once: true });
        });
        timer = setTimeout(() => abort.abort(new Error('caller_intent_timeout')), remaining); timer.unref();
        const operationId = brokerIdentity('caller-intent', options.callId, record.frozen.sessionId,
          record.delegation.delegationId, record.frozen.turn.sourceEventId);
        const response = await Promise.race([options.intent.resolve({ operationId, record: structuredClone(record),
          callDeadlineAt: deadline }, abort.signal), interrupted]);
        if (stopping || channel.signal.aborted || !options.active(request.sessionId)) throw new Error('caller_route_source_inactive');
        record = options.store.decideCallerRoute(options.callId, callerRouteRef(record), response);
      } catch {
        // Unknown inference never becomes an inferred action. Persist a final
        // no-dispatch disposition; an exact handoff replay cannot retry it.
        record = options.store.decideCallerRoute(options.callId, callerRouteRef(record), {
          inputDigest: record.inputDigest, decision: { kind: 'clarify', reason: 'unavailable' },
        });
      } finally {
        clearTimeout(timer); channel.signal.removeEventListener('abort', stopped); resolutionAborts.delete(abort);
      }
    }
    if (!record.decision || stopping || channel.signal.aborted || !options.active(request.sessionId)) { channel.release?.(); return; }
    if (record.decision.kind === 'task' || record.decision.kind === 'answer' || record.decision.kind === 'deliver_result') {
      // Each admission port rechecks current presentation/authority or requires
      // its own durable downstream record before recovery. No route is retargeted.
      await options.admit(request, callerRouteRef(record), record.decision, channel);
    } else {
      settle(request);
      // Conversation and clarify decisions start no work: release the speech gate first, so the
      // gate cannot stay on for the call and Live can speak the clarification.
      channel.release?.();
      try { if (record.decision.kind === 'clarify') await notice(request, record, record.decision.reason); }
      finally { try { await capture(record); } catch { /* Closeout retains the exact source. */ } }
    }
  };
  const stop = () => { stopping = true; for (const abort of resolutionAborts) abort.abort(new Error('caller_route_stopped')); };
  return {
    delegate(request: LiveDelegationRequest, channel: LiveBackendChannel): Promise<void> {
      const key = JSON.stringify([request.sessionId, request.delegationId]);
      const previous = inFlight.get(key);
      if (previous) {
        if (previous.request.callId !== request.callId || previous.request.requestId !== request.requestId
          || previous.request.offsetMs !== request.offsetMs) return Promise.reject(new Error('caller_route_identity_changed'));
        // A replay shares durable work observation, but generation retirement
        // waits only for its admission milestone, never for the whole result.
        if (previous.admissionSettled) options.admissionSettled?.(request);
        else previous.waitingAdmissions.add(request);
        return previous.promise;
      }
      admitting.add(request);
      const promise = run(request, channel).finally(() => { settle(request); inFlight.delete(key); });
      inFlight.set(key, { request, promise, admissionSettled: false, waitingAdmissions: new Set([request]) }); return promise;
    },
    pendingSourceCount: () => admitting.size,
    admissionSettled: settle,
    stop,
    async prepareCloseout() {
      stop();
      await Promise.allSettled([...inFlight.values()].map(value => value.promise));
      // No resolver/submit/reply is called from closeout or restart recovery.
      for (const record of call().callerRouting?.records ?? []) {
        try { await capture(record); } catch { /* Keep acknowledged versus pending distinct. */ }
      }
      return { pendingAdmissions: inFlight.size,
        pendingCaptures: (call().callerRouting?.records ?? []).filter(record => !record.capture).length };
    },
  };
}

/** Terminal-call recovery captures original speech only. It performs no intent
 * request, new work, question reply or source rewrite after restart. */
export async function recoverCallerRouteCaptures(options: {
  callId: string; store: BrokerStore; primary: Pick<PrimaryApiClient, 'captureTurn'>;
}): Promise<number> {
  await withCallerAdmission(options.store, options.callId, async () => {
    for (const record of options.store.get(options.callId)?.callerRouting?.records ?? []) {
      if (record.capture) continue;
      try {
        const receipt = await options.primary.captureTurn(record.scope.bindingId, structuredClone(record.frozen.turn));
        acknowledgedTurn(record.scope.bindingId, record.frozen.turn, receipt);
        options.store.captureCallerRoute(options.callId, callerRouteRef(record), receipt);
      } catch { /* Exact original source remains a pending capture obligation. */ }
    }
  });
  return (options.store.get(options.callId)?.callerRouting?.records ?? []).filter(record => !record.capture).length;
}
