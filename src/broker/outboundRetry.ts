import { canonicalJson } from '../outbound/canonical.js';
import { validateCallContinuation } from './callContinuation.js';
import { outboundSettlementFromObservation, validateSelectedOutboundObservation,
  type OutboundObservationCall } from './outboundObservation.js';
import { OutboundRetryError, assertOutboundRetrySettlement, outboundRetryOfSchema,
  outboundRetrySuccessorSchema, type OutboundRetryOf } from './outboundSettlement.js';

export interface OutboundRetryCall extends OutboundObservationCall {
  retryOf?: unknown;
  retrySuccessor?: unknown;
}

type Calls<T extends OutboundRetryCall = OutboundRetryCall> = Readonly<Record<string, T>>;
const same = (left: unknown, right: unknown) => canonicalJson(left) === canonicalJson(right);
function unavailable(): never { throw new OutboundRetryError('call_request_unavailable'); }
function conflict(): never { throw new OutboundRetryError('call_request_conflict'); }
const own = <T extends OutboundRetryCall>(calls: Calls<T>, id: string): T | undefined =>
  Object.hasOwn(calls, id) ? calls[id] : undefined;

function edges(call: OutboundRetryCall) {
  return {
    retryOf: call.retryOf === undefined ? undefined : outboundRetryOfSchema.parse(call.retryOf),
    retrySuccessor: call.retrySuccessor === undefined ? undefined : outboundRetrySuccessorSchema.parse(call.retrySuccessor),
  };
}

function sameSelection(parent: OutboundRetryCall, child: OutboundRetryCall): boolean {
  const before = validateCallContinuation(parent), after = validateCallContinuation(child);
  return before !== undefined && after !== undefined && same(before, after)
    && parent.number === child.number && parent.direction === child.direction && parent.purpose === child.purpose;
}

function identity(call: OutboundRetryCall) {
  return { id: call.id, requestId: call.requestId, number: call.number, direction: call.direction,
    purpose: call.purpose, createdAt: call.createdAt, continuation: call.continuation, callAssignment: call.callAssignment };
}

function requireFreshChild(call: OutboundRetryCall): void {
  const observation = validateSelectedOutboundObservation(call);
  if (!observation || call.callSid !== undefined || observation.dispatchStartedAt !== undefined
    || observation.accepted !== undefined || observation.settlement !== undefined || call.retrySuccessor !== undefined)
    unavailable();
}

/** Validate the complete calls map after the existing call validators. Legacy
 * records without edges keep their identities and absent fields unchanged. */
export function validateOutboundRetries(calls: Calls, previous?: Calls): void {
  try {
    const requests = new Set<string>();
    const parsed = new Map<string, ReturnType<typeof edges>>();
    for (const [id, call] of Object.entries(calls)) {
      if (!call || typeof call !== 'object' || Array.isArray(call) || call.id !== id
        || typeof call.requestId !== 'string' || requests.has(call.requestId)) unavailable();
      requests.add(call.requestId);
      const edge = edges(call); parsed.set(id, edge);
      if (edge.retryOf === undefined && edge.retrySuccessor === undefined) continue;
      if (!validateCallContinuation(call) || !validateSelectedOutboundObservation(call)) unavailable();
    }
    for (const [id, edge] of parsed) {
      const child = calls[id];
      if (edge.retryOf !== undefined) {
        const parent = own(calls, edge.retryOf.callId), successor = parent && parsed.get(parent.id)?.retrySuccessor;
        if (!parent || !successor || parent.id === child.id || parent.requestId === child.requestId
          || parent.requestId !== edge.retryOf.requestId || successor.callId !== child.id
          || successor.requestId !== child.requestId || successor.settlementDigest !== edge.retryOf.settlementDigest
          || !sameSelection(parent, child)) unavailable();
        assertOutboundRetrySettlement({ callId: parent.id, requestId: parent.requestId, callSid: parent.callSid },
          outboundSettlementFromObservation(parent), edge.retryOf);
      }
      if (edge.retrySuccessor !== undefined) {
        const next = own(calls, edge.retrySuccessor.callId), predecessor = next && parsed.get(next.id)?.retryOf;
        if (!next || !predecessor || next.requestId !== edge.retrySuccessor.requestId
          || predecessor.callId !== child.id || predecessor.requestId !== child.requestId
          || predecessor.settlementDigest !== edge.retrySuccessor.settlementDigest) unavailable();
      }
    }
    // One successor per node still permits a closed cycle unless checked across
    // the whole map. Iterative traversal keeps long retained chains bounded.
    const visited = new Set<string>();
    for (const id of parsed.keys()) {
      const path = new Set<string>();
      let cursor: string | undefined = id;
      while (cursor !== undefined && !visited.has(cursor)) {
        if (path.has(cursor)) unavailable();
        path.add(cursor);
        cursor = parsed.get(cursor)?.retrySuccessor?.callId;
      }
      for (const value of path) visited.add(value);
    }
    if (previous === undefined) return;
    validateOutboundRetries(previous);
    for (const [id, before] of Object.entries(previous)) {
      const prior = edges(before), after = own(calls, id);
      if ((prior.retryOf !== undefined || prior.retrySuccessor !== undefined) && !after) unavailable();
      if (!after) continue;
      const next = parsed.get(id)!;
      if (!same(prior.retryOf, next.retryOf)
        || (prior.retrySuccessor !== undefined && !same(prior.retrySuccessor, next.retrySuccessor))) unavailable();
      if ((prior.retryOf !== undefined || prior.retrySuccessor !== undefined || next.retrySuccessor !== undefined)
        && !same(identity(before), identity(after))) unavailable();
      if (prior.retrySuccessor === undefined && next.retrySuccessor !== undefined) {
        // A predecessor can gain an edge only when its exact new child appears
        // in the same replacement. Existing calls cannot be adopted as retries.
        if (own(previous, next.retrySuccessor.callId)) unavailable();
        const child = own(calls, next.retrySuccessor.callId);
        if (!child) unavailable();
        requireFreshChild(child);
        // The settlement must already exist before the child reservation.
        assertOutboundRetrySettlement({ callId: before.id, requestId: before.requestId, callSid: before.callSid },
          outboundSettlementFromObservation(before), edges(child).retryOf!);
      }
    }
    for (const [id, call] of Object.entries(calls)) {
      if (own(previous, id)) continue;
      const edge = parsed.get(id)!;
      if (edge.retrySuccessor !== undefined) unavailable();
      if (edge.retryOf !== undefined) {
        if (!own(previous, edge.retryOf.callId)) unavailable();
        requireFreshChild(call);
      }
    }
  } catch { unavailable(); }
}

/** Fresh reservation only. The store must first recover an exact existing
 * request and verify complete durable storage availability. Persist both
 * returned calls together before readiness or provider contact; a persistence
 * error authorizes neither another child nor a provider create. */
export function prepareOutboundRetry<T extends OutboundRetryCall>(calls: Calls<T>,
  child: T & { retryOf: OutboundRetryOf }): { predecessor: T; child: T } {
  validateOutboundRetries(calls);
  let retryOf: OutboundRetryOf;
  try { retryOf = outboundRetryOfSchema.parse(child.retryOf); }
  catch { unavailable(); }
  const parent = own(calls, retryOf.callId);
  if (!parent) unavailable();
  if (own(calls, child.id) || Object.values(calls).some(call => call.requestId === child.requestId)
    || parent.requestId !== retryOf.requestId || parent.id === child.id || parent.requestId === child.requestId
    || parent.retrySuccessor !== undefined) conflict();
  try { if (!sameSelection(parent, child)) conflict(); }
  catch { conflict(); }
  let settlement;
  try { settlement = outboundSettlementFromObservation(parent); }
  catch { unavailable(); }
  assertOutboundRetrySettlement({ callId: parent.id, requestId: parent.requestId, callSid: parent.callSid }, settlement, retryOf);
  let retrySuccessor;
  try {
    requireFreshChild(child);
    retrySuccessor = outboundRetrySuccessorSchema.parse({ callId: child.id, requestId: child.requestId,
      settlementDigest: retryOf.settlementDigest });
  } catch { unavailable(); }
  const predecessor = { ...structuredClone(parent), retrySuccessor };
  const preparedChild = structuredClone(child);
  validateOutboundRetries({ ...calls, [predecessor.id]: predecessor, [preparedChild.id]: preparedChild }, calls);
  return { predecessor, child: preparedChild };
}
