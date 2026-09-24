import assert from 'node:assert/strict';
import test from 'node:test';
import { OUTBOUND_OBSERVATION_VERSION, OutboundObservationError, createOutboundObservation,
  outboundObservationEventSchema, outboundObservationSchema, outboundSettlementFromObservation,
  reduceOutboundObservation, validateSelectedOutboundObservation,
  type OutboundAcceptedIdentity, type OutboundObservation, type OutboundObservationCall,
  type OutboundObservationErrorCode, type OutboundObservationEvent } from '../src/broker/outboundObservation.js';
import { OUTBOUND_SETTLEMENT_VERSION, assertOutboundRetrySettlement, createOutboundSettlement } from '../src/broker/outboundSettlement.js';

const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const at = '2026-09-24T10:00:00.000+00:00';
const after = '2026-09-24T10:00:01.000Z';
const later = '2026-09-24T10:01:00.000Z';
const provider = { accountSid: `AC${'a'.repeat(32)}`, from: '+12025550100', to: '+12025550101' };
const identity: OutboundAcceptedIdentity = { ...provider, callSid: `CA${'b'.repeat(32)}` };
function base(): OutboundObservationCall {
  return { id: uuid(1), requestId: uuid(2), number: provider.to, createdAt: at, direction: 'outbound',
    purpose: 'Continue the saved question.',
    continuation: { originalRequestId: uuid(3), questionId: uuid(4), questionRevision: 1 },
    callAssignment: { origin: 'authenticated_broker_control', value: { assignmentId: uuid(2), brokerCallId: uuid(1),
      revision: 1, purpose: 'Continue the saved question.' } } };
}
function reserved(): OutboundObservationCall {
  const call = base(); return { ...call, outboundObservation: createOutboundObservation(call, provider) };
}
const eventIdentity = { callId: uuid(1), requestId: uuid(2), observedAt: after };
const fence = (): Extract<OutboundObservationEvent, { type: 'dispatch_started' }> => ({ ...eventIdentity, type: 'dispatch_started' });
const accept = (observedAt = later): Extract<OutboundObservationEvent, { type: 'provider_accepted' }> => ({ ...eventIdentity, observedAt,
  type: 'provider_accepted', identity });
const terminal = (overrides: Partial<Extract<OutboundObservationEvent, { type: 'provider_terminal' }>> = {}): Extract<OutboundObservationEvent, { type: 'provider_terminal' }> => ({
  ...eventIdentity, observedAt: later, type: 'provider_terminal', identity, providerStatus: 'no-answer',
  source: 'signed_status_callback', evidenceDigest: 'c'.repeat(64), ...overrides });
function apply(call: OutboundObservationCall, event: OutboundObservationEvent): OutboundObservationCall {
  const observation = reduceOutboundObservation(call, event);
  const next = { ...call, outboundObservation: observation,
    ...(observation.accepted === undefined ? {} : { callSid: observation.accepted.callSid }) };
  validateSelectedOutboundObservation(next, call); return next;
}
function observation(call: OutboundObservationCall): OutboundObservation {
  const result = validateSelectedOutboundObservation(call);
  assert.ok(result); return result;
}
function hasCode(code: OutboundObservationErrorCode) {
  return (error: unknown) => error instanceof OutboundObservationError && error.code === code;
}

test('new selected reservation binds exact call/request/provider/destination/time with unresolved public projection', () => {
  const call = reserved();
  assert.deepEqual(observation(call), { v: OUTBOUND_OBSERVATION_VERSION, callId: call.id, requestId: call.requestId,
    providerIdentity: provider, reservedAt: at });
  assert.deepEqual(outboundSettlementFromObservation(call), { v: OUTBOUND_SETTLEMENT_VERSION,
    callId: call.id, requestId: call.requestId, state: 'unresolved' });
  assert.equal(Object.hasOwn(call, 'callSid'), false);
});

test('historical ordinary/Hark/inbound/selected absence is not normalized or adopted', () => {
  for (const call of [base(), { ...base(), continuation: undefined }, { ...base(), direction: 'inbound' },
    { ...base(), callAssignment: { origin: 'authenticated_hark_delivery' } },
    { ...base(), id: 'ABCDEFAB-1234-4234-8234-123456789ABC', requestId: 'legacy-request', callSid: identity.callSid }]) {
    const bytes = JSON.stringify(call);
    assert.equal(validateSelectedOutboundObservation(call), undefined);
    assert.equal(outboundSettlementFromObservation(call), undefined);
    assert.equal(JSON.stringify(call), bytes);
    assert.equal(Object.hasOwn(call, 'outboundObservation'), false);
  }
  assert.throws(() => validateSelectedOutboundObservation(reserved(), base()), hasCode('outbound_observation_changed'));
  assert.throws(() => reduceOutboundObservation(base(), fence()), hasCode('outbound_observation_missing'));
});

test('reservation requires exact selected outbound scope and cannot instrument an accepted or existing record', () => {
  for (const patch of [{ continuation: undefined }, { direction: 'inbound' }, { callAssignment: undefined },
    { inbound: {} }, { harkJournal: {} }, { harkDispatch: {} }, { harkReceiptOutbox: {} }])
    assert.throws(() => createOutboundObservation({ ...base(), ...patch }, provider), hasCode('outbound_observation_scope'));
  assert.throws(() => createOutboundObservation({ ...base(), callSid: identity.callSid }, provider), hasCode('outbound_observation_identity'));
  assert.throws(() => createOutboundObservation(reserved(), provider), hasCode('outbound_observation_changed'));
  assert.throws(() => createOutboundObservation(base(), { ...provider, to: '+12025550999' }), hasCode('outbound_observation_identity'));
});

for (const reason of ['call_already_open', 'preflight_refused', 'service_stopping'] as const) {
  test(`${reason} seals only a pre-dispatch selected reservation and prevents a later create`, () => {
    const event: OutboundObservationEvent = { ...eventIdentity, type: 'never_dispatched', reason };
    const saved = apply(reserved(), event), receipt = outboundSettlementFromObservation(saved);
    assert.deepEqual(receipt, createOutboundSettlement({ v: OUTBOUND_SETTLEMENT_VERSION, callId: saved.id,
      requestId: saved.requestId, state: 'settled', kind: 'never_dispatched', observedAt: after, reason }));
    assert.throws(() => apply(saved, fence()), hasCode('outbound_already_settled'));
    assert.throws(() => apply(saved, accept()), hasCode('outbound_already_settled'));
    assert.throws(() => apply(saved, terminal()), hasCode('outbound_already_settled'));
    assert.deepEqual(apply(saved, { ...event, observedAt: later }), saved);
  });
}

test('a second pre-dispatch reason cannot rewrite the first settlement', () => {
  const saved = apply(reserved(), { ...eventIdentity, type: 'never_dispatched', reason: 'preflight_refused' });
  assert.throws(() => apply(saved, { ...eventIdentity, type: 'never_dispatched', reason: 'service_stopping' }),
    hasCode('outbound_already_settled'));
});

test('dispatch fence precedes provider contact and exact duplicate transition cannot authorize another create', () => {
  const call = apply(reserved(), fence());
  assert.equal(observation(call).dispatchStartedAt, after);
  assert.equal(observation(call).accepted, undefined);
  assert.throws(() => apply(call, fence()), hasCode('outbound_dispatch_already_started'));
  assert.throws(() => apply(call, { ...eventIdentity, type: 'never_dispatched', reason: 'preflight_refused' }),
    hasCode('outbound_dispatch_already_started'));
  const restarted = JSON.parse(JSON.stringify(call));
  assert.deepEqual(outboundSettlementFromObservation(restarted), outboundSettlementFromObservation(call));
  assert.equal(outboundSettlementFromObservation(restarted)?.state, 'unresolved');
});

test('pre-create settlement versus dispatch race has no valid merged or stale-overwrite outcome', () => {
  const original = reserved();
  const dispatched = apply(original, fence());
  const refused = apply(original, { ...eventIdentity, type: 'never_dispatched', reason: 'service_stopping' });
  assert.throws(() => validateSelectedOutboundObservation(refused, dispatched), hasCode('outbound_observation_changed'));
  assert.throws(() => validateSelectedOutboundObservation(dispatched, refused), hasCode('outbound_observation_changed'));
  assert.equal(outboundObservationSchema.safeParse({ ...observation(dispatched), settlement: observation(refused).settlement }).success, false);
});

test('a fence recovered after an uncertain replacement cannot be treated as a never-dispatched call', () => {
  const original = reserved();
  const staged = reduceOutboundObservation(original, fence());
  assert.equal(observation(original).dispatchStartedAt, undefined);
  const diskAfterRename = JSON.parse(JSON.stringify({ ...original, outboundObservation: staged }));
  assert.equal(outboundSettlementFromObservation(diskAfterRename)?.state, 'unresolved');
  assert.throws(() => apply(diskAfterRename, fence()), hasCode('outbound_dispatch_already_started'));
  assert.throws(() => apply(diskAfterRename, { ...eventIdentity, type: 'never_dispatched', reason: 'service_stopping' }),
    hasCode('outbound_dispatch_already_started'));
  // The actual durable writer/failure latch belongs to the separately mounted store tests.
});

test('timeout, local failure, closure and missing SID after the fence never establish settlement', () => {
  const dispatched = apply(reserved(), fence());
  for (const patch of [{ status: 'failed' }, { status: 'unknown' }, { endedAt: later }, { finalized: true },
    { failure: { code: 'HTTP_400', message: 'create refused' } }, { primaryClosed: { state: 'closed' } }]) {
    const call = { ...dispatched, ...patch };
    assert.equal(outboundSettlementFromObservation(call)?.state, 'unresolved');
    assert.equal(observation(call).settlement, undefined);
  }
  for (const type of ['timeout', 'failed', 'hangup_requested', 'socket_closed', 'http_4xx'])
    assert.equal(outboundObservationEventSchema.safeParse({ ...eventIdentity, type }).success, false);
});

test('provider acceptance without durable dispatch is refused, even with a plausible signed callback', () => {
  assert.throws(() => apply(reserved(), accept()), hasCode('outbound_dispatch_required'));
  assert.throws(() => apply(reserved(), terminal()), hasCode('outbound_dispatch_required'));
});

test('terminal callback before create response atomically binds first SID and immutable settlement', () => {
  const fenced = apply(reserved(), fence());
  const saved = apply(fenced, terminal());
  assert.equal(saved.callSid, identity.callSid);
  assert.deepEqual(observation(saved).accepted, { callSid: identity.callSid, observedAt: later });
  assert.deepEqual(apply(saved, accept('2026-09-24T10:02:00.000Z')), saved);
  const receipt = outboundSettlementFromObservation(saved);
  assert.ok(receipt?.state === 'settled' && receipt.kind === 'provider_terminal');
  assert.equal(receipt.source, 'signed_status_callback');
  assert.equal(receipt.observedAt, later);
  assert.throws(() => validateSelectedOutboundObservation({ ...fenced, outboundObservation: observation(saved) }),
    hasCode('outbound_observation_identity'));
});

test('acceptance persists first timestamp and later terminal fetch preserves it', () => {
  const accepted = apply(apply(reserved(), fence()), accept(after));
  const saved = apply(accepted, terminal({ source: 'authenticated_fetch' }));
  assert.deepEqual(observation(saved).accepted, { callSid: identity.callSid, observedAt: after });
  assert.equal(observation(saved).settlement?.observedAt, later);
  assert.deepEqual(apply(accepted, accept(later)), accepted);
});

for (const source of ['signed_status_callback', 'authenticated_fetch', 'authenticated_update'] as const) {
  test(`${source} projects only validated terminal identity; private transport fields stay private`, () => {
    for (const providerStatus of ['completed', 'busy', 'no-answer', 'canceled', 'failed'] as const) {
      const saved = apply(apply(reserved(), fence()), terminal({ source, providerStatus }));
      const receipt = outboundSettlementFromObservation(saved);
      assert.ok(receipt?.state === 'settled' && receipt.kind === 'provider_terminal');
      assert.equal(receipt.providerStatus, providerStatus);
      assert.equal(receipt.source, source);
      assert.equal(JSON.stringify(receipt).includes(provider.accountSid), false);
      assert.equal(JSON.stringify(receipt).includes(provider.from), false);
      assert.equal(JSON.stringify(receipt).includes(provider.to), false);
      assert.deepEqual(outboundSettlementFromObservation(JSON.parse(JSON.stringify(saved))), receipt);
      const retry = { callId: saved.id, requestId: saved.requestId, settlementDigest: receipt.digest };
      if (providerStatus === 'completed') assert.throws(() => assertOutboundRetrySettlement(
        { callId: saved.id, requestId: saved.requestId, callSid: saved.callSid }, receipt, retry), /call_retry_unavailable/);
      else assert.deepEqual(assertOutboundRetrySettlement(
        { callId: saved.id, requestId: saved.requestId, callSid: saved.callSid }, receipt, retry), receipt);
    }
  });
}

test('late valid observations cannot reopen or replace first settlement source/status/time/digest', () => {
  const saved = apply(apply(reserved(), fence()), terminal());
  for (const next of [accept(), terminal(), terminal({ observedAt: after, source: 'authenticated_update',
    providerStatus: 'completed', evidenceDigest: 'd'.repeat(64) })])
    assert.deepEqual(apply(saved, next), saved);
  assert.equal(outboundObservationEventSchema.safeParse({ ...terminal(), providerStatus: 'in-progress' }).success, false);
});

test('wrong account, from, to or SID refuses both late acceptance and terminal settlement', () => {
  const accepted = apply(apply(reserved(), fence()), accept());
  const settled = apply(accepted, terminal());
  for (const call of [accepted, settled]) for (const changed of [{ accountSid: `AC${'d'.repeat(32)}` },
    { from: '+12025550999' }, { to: '+12025550999' }, { callSid: `CA${'d'.repeat(32)}` }]) {
    assert.throws(() => apply(call, { ...accept(), identity: { ...identity, ...changed } }), hasCode('outbound_observation_identity'));
    assert.throws(() => apply(call, terminal({ identity: { ...identity, ...changed } })), hasCode('outbound_observation_identity'));
  }
  for (const key of ['accountSid', 'from', 'to', 'callSid'] as const) {
    const incomplete: Partial<OutboundAcceptedIdentity> = { ...identity }; delete incomplete[key];
    assert.equal(outboundObservationEventSchema.safeParse({ ...terminal(), identity: incomplete }).success, false);
  }
});

test('wrong call or request cannot reuse a valid provider observation', () => {
  const call = apply(reserved(), fence());
  assert.throws(() => apply(call, terminal({ callId: uuid(9) })), hasCode('outbound_observation_identity'));
  assert.throws(() => apply(call, terminal({ requestId: uuid(9) })), hasCode('outbound_observation_identity'));
});

test('restart validation refuses changed call/request/destination/reservation/SID', () => {
  const call = apply(apply(reserved(), fence()), terminal());
  for (const patch of [{ id: uuid(8) }, { requestId: uuid(8) }, { number: '+12025550999' },
    { createdAt: after }, { callSid: `CA${'f'.repeat(32)}` }, { callSid: undefined }])
    assert.throws(() => validateSelectedOutboundObservation({ ...call, ...patch }));
});

test('all previously saved fields remain immutable on every write, including exact timestamp spelling', () => {
  const call = apply(apply(reserved(), fence()), terminal());
  const saved = observation(call);
  for (const patch of [{ dispatchStartedAt: undefined }, { dispatchStartedAt: later }, { accepted: undefined },
    { accepted: { callSid: identity.callSid, observedAt: after } }, { settlement: undefined },
    { providerIdentity: { ...provider, from: '+12025550999' } },
    { reservedAt: '2026-09-24T10:00:00.000Z' }])
    assert.throws(() => validateSelectedOutboundObservation({ ...call, outboundObservation: { ...saved, ...patch } }, call));
  assert.throws(() => validateSelectedOutboundObservation({ ...call, outboundObservation: undefined }, call),
    hasCode('outbound_observation_changed'));
  const settlement = saved.settlement;
  assert.ok(settlement?.kind === 'provider_terminal');
  for (const patch of [{ observedAt: after }, { evidenceDigest: 'd'.repeat(64) }, { source: 'authenticated_fetch' }, { providerStatus: 'failed' }])
    assert.throws(() => validateSelectedOutboundObservation({ ...call,
      outboundObservation: { ...saved, settlement: { ...settlement, ...patch } } }, call), hasCode('outbound_observation_changed'));
});

test('strict observation refuses corrupt/unsupported fields and never coerces null or provider facts', () => {
  const saved = observation(reserved());
  for (const bad of [null, {}, { ...saved, dispatchStartedAt: null }, { ...saved, settlement: null },
    { ...saved, v: 'other' }, { ...saved, providerIdentity: { ...provider, token: 'not-a-secret' } },
    { ...saved, status: 'failed' }, { ...saved, signature: 'not-a-secret' },
    { ...saved, accepted: { callSid: identity.callSid, observedAt: later } }])
    assert.equal(outboundObservationSchema.safeParse(bad).success, false);
  assert.equal(outboundObservationEventSchema.safeParse({ ...terminal(), source: 'local_status' }).success, false);
  assert.equal(outboundObservationEventSchema.safeParse({ ...eventIdentity, type: 'never_dispatched', reason: 'create_timeout' }).success, false);
});

test('pure transitions do not mutate or alias caller-owned saved input', () => {
  const call = reserved(), bytes = JSON.stringify(call), event = fence();
  const next = reduceOutboundObservation(call, event);
  assert.equal(JSON.stringify(call), bytes);
  next.providerIdentity.from = '+12025550999';
  assert.equal(observation(call).providerIdentity.from, provider.from);
  assert.equal(event.observedAt, after);
});
