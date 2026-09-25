import assert from 'node:assert/strict';
import test from 'node:test';
import { prepareOutboundRetry, validateOutboundRetries, type OutboundRetryCall } from '../src/broker/outboundRetry.js';
import { createOutboundObservation, outboundSettlementFromObservation, reduceOutboundObservation,
  type OutboundObservationEvent } from '../src/broker/outboundObservation.js';
import { OutboundRetryError, type OutboundRetryOf } from '../src/broker/outboundSettlement.js';

const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const at = '2026-09-24T10:00:00.000Z';
const after = '2026-09-24T10:01:00.000Z';
const provider = { accountSid: `AC${'a'.repeat(32)}`, from: '+12025550100', to: '+12025550101' };
interface Call extends OutboundRetryCall { status: string; delegations: Record<string, unknown>; marker: { value: number } }
function reserved(n: number): Call {
  const call: Call = { id: uuid(n), requestId: uuid(n + 100), direction: 'outbound', number: provider.to,
    purpose: 'Discuss the selected saved question.', createdAt: at, status: 'prepared', delegations: {}, marker: { value: n },
    continuation: { originalRequestId: uuid(500), questionId: uuid(501), questionRevision: 1 },
    callAssignment: { origin: 'authenticated_broker_control', value: { assignmentId: uuid(n + 100),
      brokerCallId: uuid(n), revision: 1, purpose: 'Discuss the selected saved question.' } } };
  return { ...call, outboundObservation: createOutboundObservation(call, provider) };
}
function apply(call: Call, event: OutboundObservationEvent): Call {
  const outboundObservation = reduceOutboundObservation(call, event);
  return { ...call, outboundObservation,
    ...(outboundObservation.accepted ? { callSid: outboundObservation.accepted.callSid } : {}) };
}
function settled(n = 1, status?: 'completed' | 'busy' | 'no-answer' | 'canceled' | 'failed'): Call {
  let call = reserved(n);
  const event = { callId: call.id, requestId: call.requestId, observedAt: after };
  if (status === undefined) return apply(call, { ...event, type: 'never_dispatched', reason: 'preflight_refused' });
  call = apply(call, { ...event, type: 'dispatch_started' });
  return apply(call, { ...event, type: 'provider_terminal', identity: { ...provider,
    callSid: `CA${n.toString(16).padStart(32, '0')}` }, providerStatus: status,
    source: 'signed_status_callback', evidenceDigest: 'b'.repeat(64) });
}
function edge(parent: Call): OutboundRetryOf {
  const receipt = outboundSettlementFromObservation(parent);
  assert.ok(receipt?.state === 'settled');
  return { callId: parent.id, requestId: parent.requestId, settlementDigest: receipt.digest };
}
function child(parent: Call, n = 2): Call & { retryOf: OutboundRetryOf } {
  return { ...reserved(n), retryOf: edge(parent) };
}
const journal = (...calls: Call[]): Record<string, Call> => Object.fromEntries(calls.map(call => [call.id, call]));
const hasCode = (code: OutboundRetryError['code']) => (error: unknown) => error instanceof OutboundRetryError && error.code === code;
function prepared(parent = settled()) {
  const before = journal(parent), pair = prepareOutboundRetry(before, child(parent));
  return { before, pair, after: { ...before, [pair.predecessor.id]: pair.predecessor, [pair.child.id]: pair.child } };
}

test('fresh retry returns both reciprocal calls without mutating either input or losing generic call fields', () => {
  const parent = settled(), next = child(parent), calls = journal(parent);
  const bytes = JSON.stringify({ calls, next });
  const pair = prepareOutboundRetry(calls, next);
  assert.equal(JSON.stringify({ calls, next }), bytes);
  assert.deepEqual(pair.predecessor.retrySuccessor, { callId: next.id, requestId: next.requestId,
    settlementDigest: edge(parent).settlementDigest });
  assert.deepEqual(pair.child.retryOf, edge(parent));
  assert.equal(pair.child.status, 'prepared');
  assert.equal(pair.child.marker.value, 2);
  assert.deepEqual(pair.child.delegations, {});
  assert.equal(Object.hasOwn(pair.child, 'retrySuccessor'), false);
  pair.child.marker.value = 99;
  assert.equal(next.marker.value, 2);
  validateOutboundRetries(journal(pair.predecessor, pair.child), calls);
});

test('historical calls without edges retain exact mixed-case and legacy IDs without normalization', () => {
  const a = { ...reserved(1), id: 'ABCDEFAB-1234-4234-8234-123456789ABC', requestId: 'legacy-request-A',
    continuation: undefined, callAssignment: undefined, outboundObservation: undefined };
  const b = { ...a, id: 'historical-call', requestId: 'legacy-request-a', direction: 'inbound' };
  const calls = journal(a, b), bytes = JSON.stringify(calls);
  validateOutboundRetries(calls);
  validateOutboundRetries(calls, structuredClone(calls));
  assert.equal(JSON.stringify(calls), bytes);
  assert.equal(Object.hasOwn(a, 'retryOf'), false);
  assert.equal(Object.hasOwn(b, 'retrySuccessor'), false);
});

for (const status of ['busy', 'no-answer', 'canceled', 'failed'] as const) {
  test(`durable provider ${status} permits one fresh child with its exact settlement digest`, () => {
    const parent = settled(1, status), pair = prepareOutboundRetry(journal(parent), child(parent));
    validateOutboundRetries(journal(pair.predecessor, pair.child));
    assert.equal(pair.child.callSid, undefined);
    assert.equal(outboundSettlementFromObservation(pair.child)?.state, 'unresolved');
  });
}

test('missing predecessor differs from a known call with no qualifying settlement', () => {
  const parent = settled(), next = child(parent);
  assert.throws(() => prepareOutboundRetry({}, next), hasCode('call_request_unavailable'));
  for (const prior of [reserved(1), { ...reserved(1), outboundObservation: undefined }])
    assert.throws(() => prepareOutboundRetry(journal(prior), next), hasCode('call_retry_unavailable'));
  const completed = settled(1, 'completed');
  assert.throws(() => prepareOutboundRetry(journal(completed), child(completed)), hasCode('call_retry_unavailable'));
});

test('timeout and local ended/failed status after a dispatch fence do not permit a retry', () => {
  const parent = settled(), next = child(parent), original = reserved(1);
  const fenced = apply(original, { type: 'dispatch_started', callId: original.id, requestId: original.requestId, observedAt: after });
  const local = { ...fenced, status: 'failed', endedAt: after, failure: { code: 'timeout' } };
  assert.throws(() => prepareOutboundRetry(journal(local), next), hasCode('call_retry_unavailable'));
  assert.equal(local.callSid, undefined);
});

test('contradictory private observation is unavailable even when its public-looking digest matches', () => {
  const parent = settled(1, 'no-answer'), next = child(parent);
  const contradictory = { ...parent, callSid: `CA${'f'.repeat(32)}` };
  assert.throws(() => prepareOutboundRetry(journal(contradictory), next), hasCode('call_request_unavailable'));
  const fakeProjection = { ...reserved(1), outboundObservation: undefined, outboundSettlement: outboundSettlementFromObservation(parent) };
  assert.throws(() => prepareOutboundRetry(journal(fakeProjection), next), hasCode('call_retry_unavailable'));
});

test('changed prior request or settlement digest conflicts without modifying the predecessor', () => {
  const parent = settled(), next = child(parent), calls = journal(parent), bytes = JSON.stringify(calls);
  for (const retryOf of [{ ...next.retryOf, requestId: uuid(999) }, { ...next.retryOf, settlementDigest: 'f'.repeat(64) }])
    assert.throws(() => prepareOutboundRetry(calls, { ...next, retryOf }), hasCode('call_request_conflict'));
  assert.equal(JSON.stringify(calls), bytes);
});

test('number, purpose, direction, selector and control origin cannot change across the retry', () => {
  const parent = settled(), next = child(parent);
  const changed = [
    { ...next, number: '+12025550999' }, { ...next, purpose: 'A different mission' }, { ...next, direction: 'inbound' },
    { ...next, continuation: { originalRequestId: uuid(600), questionId: uuid(501), questionRevision: 1 } },
    { ...next, continuation: { originalRequestId: uuid(500), questionId: uuid(601), questionRevision: 1 } },
    { ...next, continuation: undefined }, { ...next, callAssignment: { origin: 'authenticated_hark_delivery' } },
  ];
  for (const value of changed)
    assert.throws(() => prepareOutboundRetry(journal(parent), value), hasCode('call_request_conflict'));
});

test('distinct unused child call and request IDs are required; exact replay belongs to the store', () => {
  const { before, pair, after } = prepared(), parent = before[uuid(1)], next = child(parent);
  for (const value of [{ ...next, id: parent.id }, { ...next, requestId: parent.requestId }])
    assert.throws(() => prepareOutboundRetry(before, value), hasCode('call_request_conflict'));
  assert.throws(() => prepareOutboundRetry(journal(parent, reserved(3)), { ...next, requestId: uuid(103) }), hasCode('call_request_conflict'));
  assert.throws(() => prepareOutboundRetry(after, { ...pair.child, retryOf: edge(parent) }), hasCode('call_request_conflict'));
  assert.throws(() => prepareOutboundRetry(after, child(parent, 3)), hasCode('call_request_conflict'));
});

test('a new child must carry only reserved observation before the atomic replacement', () => {
  const parent = settled(), next = child(parent);
  const fence = apply(next, { type: 'dispatch_started', callId: next.id, requestId: next.requestId, observedAt: after });
  for (const value of [{ ...next, outboundObservation: undefined }, { ...next, callSid: `CA${'c'.repeat(32)}` },
    { ...fence, retryOf: next.retryOf }, { ...settled(2), retryOf: next.retryOf }])
    assert.throws(() => prepareOutboundRetry(journal(parent), value), hasCode('call_request_unavailable'));
});

test('full journal rejects either half of a replacement, orphans and a second child', () => {
  const { before, pair, after } = prepared(), other = { ...reserved(3), retryOf: pair.child.retryOf };
  for (const invalid of [journal(pair.predecessor), { ...before, [pair.child.id]: pair.child },
    { ...after, [other.id]: other }, { ...after, [pair.child.id]: { ...pair.child, retryOf: { ...edge(before[uuid(1)]), callId: uuid(999) } } }])
    assert.throws(() => validateOutboundRetries(invalid), hasCode('call_request_unavailable'));
});

test('malformed edge syntax, duplicate request identity and wrong map key make the journal unavailable', () => {
  const { after, pair } = prepared();
  for (const invalid of [
    { ...after, [pair.child.id]: { ...pair.child, retryOf: null } },
    { ...after, [pair.child.id]: { ...pair.child, retryOf: { ...pair.child.retryOf as OutboundRetryOf, extra: true } } },
    { ...after, [pair.predecessor.id]: { ...pair.predecessor, retrySuccessor: { ...pair.predecessor.retrySuccessor as OutboundRetryOf, settlementDigest: 'A'.repeat(64) } } },
    { ...after, [uuid(3)]: { ...reserved(3), requestId: pair.child.requestId } },
    { ...after, [uuid(3)]: pair.child },
  ]) assert.throws(() => validateOutboundRetries(invalid), hasCode('call_request_unavailable'));
});

test('edge-bearing historical rows require private observation and exact selected identity', () => {
  const { after, pair } = prepared();
  for (const patch of [{ outboundObservation: undefined }, { continuation: undefined }, { direction: 'inbound' },
    { number: '+12025550999' }, { callAssignment: { origin: 'authenticated_hark_delivery' } }])
    assert.throws(() => validateOutboundRetries({ ...after, [pair.child.id]: { ...pair.child, ...patch } }),
      hasCode('call_request_unavailable'));
});

test('previous edges cannot be removed, changed or moved to another child in an otherwise coherent graph', () => {
  const { before, pair, after } = prepared(), alternative = prepareOutboundRetry(before, child(before[uuid(1)], 3));
  const replaced = journal(alternative.predecessor, alternative.child);
  validateOutboundRetries(replaced);
  for (const invalid of [before, replaced, journal(pair.child), journal(pair.predecessor)])
    assert.throws(() => validateOutboundRetries(invalid, after), hasCode('call_request_unavailable'));
});

test('changing both calls to a different selector cannot rewrite the meaning of an existing edge', () => {
  const { after, pair } = prepared();
  const continuation = { originalRequestId: uuid(600), questionId: uuid(601), questionRevision: 1 };
  const changed = journal({ ...pair.predecessor, continuation }, { ...pair.child, continuation });
  validateOutboundRetries(changed);
  assert.throws(() => validateOutboundRetries(changed, after), hasCode('call_request_unavailable'));
});

test('retrofitting an existing selected call as a retry is refused even when both edges are reciprocal', () => {
  const parent = settled(), originalChild = reserved(2), { predecessor, child: next } = prepareOutboundRetry(journal(parent), child(parent));
  assert.throws(() => validateOutboundRetries(journal(predecessor, next), journal(parent, originalChild)),
    hasCode('call_request_unavailable'));
});

test('new predecessor and child cannot be invented together, and settlement must predate reservation', () => {
  const { after } = prepared();
  assert.throws(() => validateOutboundRetries(after, {}), hasCode('call_request_unavailable'));
  assert.throws(() => validateOutboundRetries(after, journal(reserved(1))), hasCode('call_request_unavailable'));
});

test('later attempts reference a newly settled child while preserving the complete earlier chain on restart', () => {
  const first = prepared(), second = apply(first.pair.child, { type: 'never_dispatched', callId: first.pair.child.id,
    requestId: first.pair.child.requestId, observedAt: after, reason: 'service_stopping' });
  const before = { ...first.after, [second.id]: second };
  validateOutboundRetries(before, first.after);
  const pair = prepareOutboundRetry(before, child(second, 3));
  const afterChain = { ...before, [pair.predecessor.id]: pair.predecessor, [pair.child.id]: pair.child };
  validateOutboundRetries(afterChain, before);
  validateOutboundRetries(JSON.parse(JSON.stringify(afterChain)));
  assert.deepEqual(pair.predecessor.retryOf, first.pair.child.retryOf);
  assert.deepEqual(afterChain[first.pair.predecessor.id].retrySuccessor, first.pair.predecessor.retrySuccessor);
});

test('a reciprocal settled cycle is invalid even when each individual settlement digest matches', () => {
  const a = settled(1), b = settled(2);
  const cycle = journal({ ...a, retryOf: edge(b), retrySuccessor: { ...edge(a), callId: b.id, requestId: b.requestId } },
    { ...b, retryOf: edge(a), retrySuccessor: { ...edge(b), callId: a.id, requestId: a.requestId } });
  assert.throws(() => validateOutboundRetries(cycle), hasCode('call_request_unavailable'));
});

test('two preparations against one snapshot cannot both survive validation against the committed winner', () => {
  const parent = settled(), before = journal(parent), first = prepareOutboundRetry(before, child(parent, 2));
  const other = prepareOutboundRetry(before, child(parent, 3));
  const committed = journal(first.predecessor, first.child), stale = journal(other.predecessor, other.child);
  validateOutboundRetries(committed, before);
  assert.throws(() => validateOutboundRetries(stale, committed), hasCode('call_request_unavailable'));
  assert.throws(() => validateOutboundRetries({ ...committed, ...stale }, committed), hasCode('call_request_unavailable'));
  assert.throws(() => prepareOutboundRetry(committed, child(parent, 3)), hasCode('call_request_conflict'));
  // Filesystem uncertainty and the writer latch are exercised by the store mount,
  // not by pretending this pure preparation performs a durable replacement.
});
