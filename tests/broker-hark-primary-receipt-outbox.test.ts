import assert from 'node:assert/strict';
import test from 'node:test';
import { canonicalJson, sha256Hex } from '../src/outbound/canonical.js';
import {
  PRIMARY_HARK_CALL_RECEIPT_ASSOCIATION_PENDING, classifyPrimaryHarkReceiptFailure,
  primaryHarkCallReceiptInputSchema, primaryHarkReceiptBodyDigest,
  preparePrimaryHarkOutcomePatch,
  reducePrimaryHarkReceiptOutbox, validatePrimaryHarkReceiptOutbox, validatePrimaryHarkReceiptResult,
  type PrimaryHarkReceiptBinding, type PrimaryHarkCallReceiptInput, type PrimaryHarkCallReceiptResult,
  type PrimaryHarkReceiptOutbox, type PrimaryHarkReceiptDelivery,
} from '../src/broker/harkPrimaryReceiptOutbox.js';

const callId = '11111111-1111-4111-8111-111111111111';
const bindingId = '22222222-2222-4222-8222-222222222222';
const requestId = '33333333-3333-4333-8333-333333333333';
const caseId = '44444444-4444-4444-8444-444444444444';
const runId = '55555555-5555-4555-8555-555555555555';
const receiptId = '66666666-6666-4666-8666-666666666666';
const other = '77777777-7777-4777-8777-777777777777';
const observedAt = '2026-09-24T07:00:00.000Z';
const later = '2026-09-24T07:00:10.000Z';
const binding: PrimaryHarkReceiptBinding = {
  origin: 'authenticated_hark_delivery', callId, bindingId, callSid: `CA${'a'.repeat(32)}`,
  deliveryId: callId, caseId, assignmentRef: { assignmentId: callId, revision: 1, digest: 'a'.repeat(64) },
};
function body(kind: 'primary_outcome' | 'owner_decision' = 'primary_outcome'): PrimaryHarkCallReceiptInput {
  return { v: 'hark.primary-call-receipt.v1', assignmentRef: structuredClone(binding.assignmentRef),
    evidence: kind === 'primary_outcome' ? { kind, requestId } : { kind, toolInvocationId: receiptId } };
}
function initial(input = body()) {
  return reducePrimaryHarkReceiptOutbox(undefined, binding, { kind: 'enqueue', body: input, observedAt });
}
function entry(box: PrimaryHarkReceiptOutbox) { return Object.values(box.entries)[0]!; }
function attempt(box: PrimaryHarkReceiptOutbox) {
  const item = entry(box);
  return reducePrimaryHarkReceiptOutbox(box, binding, { kind: 'attempt', id: item.id, bodyDigest: item.bodyDigest,
    expectedAttempts: item.attempts, observedAt: later });
}
function settle(box: PrimaryHarkReceiptOutbox, outcome: PrimaryHarkReceiptDelivery, attemptNumber = entry(box).attempts) {
  const item = entry(box);
  return reducePrimaryHarkReceiptOutbox(box, binding, { kind: 'settle', id: item.id, bodyDigest: item.bodyDigest,
    attempt: attemptNumber, outcome });
}
function result(kind: 'primary_outcome' | 'owner_decision' = 'primary_outcome'): PrimaryHarkCallReceiptResult {
  const common = { id: receiptId, bindingId, deliveryId: callId, caseId, runId };
  return { replay: false, receipt: kind === 'primary_outcome'
    ? { ...common, kind, requestId, state: 'recorded' } : { ...common, kind, state: 'closed' } };
}

test('enqueue preserves exact input and first observation through detached immutable retry and JSON restart', () => {
  const input = body();
  const first = initial(input);
  input.assignmentRef.digest = 'b'.repeat(64);
  assert.equal(entry(first).body.assignmentRef.digest, 'a'.repeat(64));
  assert.equal(Object.isFrozen(first), true);
  assert.equal(Object.isFrozen(entry(first).body.evidence), true);
  assert.equal(entry(first).id, `primary_outcome:${bindingId}:${requestId}`);
  assert.deepEqual(reducePrimaryHarkReceiptOutbox(first, binding, { kind: 'enqueue', body: body(), observedAt: later }), first);
  assert.deepEqual(validatePrimaryHarkReceiptOutbox(JSON.parse(JSON.stringify(first)), binding), first);
  assert.equal(primaryHarkReceiptBodyDigest(bindingId, body()), sha256Hex(canonicalJson({
    namespace: 'hark.primary-call-receipt.v1', value: { bindingId, body: body() },
  })));
});

test('changed assignment, call identity, body or digest cannot recover an old entry', () => {
  const first = initial();
  assert.throws(() => reducePrimaryHarkReceiptOutbox(first, binding, { kind: 'enqueue',
    body: { ...body(), assignmentRef: { ...binding.assignmentRef, digest: 'b'.repeat(64) } }, observedAt: later }), /binding_changed/);
  assert.throws(() => validatePrimaryHarkReceiptOutbox(first, { ...binding, bindingId: other }), /binding_changed/);
  assert.throws(() => validatePrimaryHarkReceiptOutbox(first, { ...binding, callSid: `CA${'b'.repeat(32)}` }), /binding_changed/);
  const damaged = structuredClone(first);
  entry(damaged).body.evidence = { kind: 'primary_outcome', requestId: other };
  assert.throws(() => validatePrimaryHarkReceiptOutbox(damaged, binding), /body_changed/);
  assert.throws(() => reducePrimaryHarkReceiptOutbox(first, binding, { kind: 'attempt', id: entry(first).id,
    bodyDigest: '0'.repeat(64), expectedAttempts: 0, observedAt: later }), /entry_mismatch/);
});

test('decision input accepts only an explicit tool identity, with no prose or decision-enum authority', () => {
  for (const evidence of [
    { kind: 'owner_decision', decision: 'acknowledge' },
    { kind: 'owner_decision', toolInvocationId: receiptId, decision: 'dismiss' },
    { kind: 'owner_decision', toolInvocationId: 'Caller said yes' },
    { kind: 'primary_outcome', requestId, heard: true },
  ]) assert.equal(primaryHarkCallReceiptInputSchema.safeParse({ ...body(), evidence }).success, false);
  const pending = initial(body('owner_decision'));
  assert.equal(entry(pending).id, `owner_decision:${receiptId}`);
  assert.equal(entry(pending).state, 'pending');
  assert.equal(entry(pending).result, undefined);
  const closed = settle(attempt(pending), { state: 'delivered', result: result('owner_decision') });
  assert.equal(entry(closed).result?.receipt.state, 'closed');
});

test('only the exact association-pending conflict retries; other409 codes remain rejected', () => {
  assert.deepEqual(classifyPrimaryHarkReceiptFailure({ kind: 'http', status: 409,
    code: PRIMARY_HARK_CALL_RECEIPT_ASSOCIATION_PENDING }),
  { state: 'pending', reason: 'association_pending', httpStatus: 409 });
  for (const code of [undefined, 'primary_hark_assignment_mismatch', 'primary_hark_owner_action_required',
    'primary_hark_terminal_outcome_required', 'primary_hark_receipt_conflict', `${PRIMARY_HARK_CALL_RECEIPT_ASSOCIATION_PENDING} `]) {
    assert.deepEqual(classifyPrimaryHarkReceiptFailure({ kind: 'http', status: 409, code }),
      { state: 'rejected', reason: 'http_refusal', httpStatus: 409 });
  }
  for (const status of [401, 403, 408, 429, 500, 503])
    assert.equal(classifyPrimaryHarkReceiptFailure({ kind: 'http', status, code: 'private_error_value' }).state, 'pending');
  assert.equal(JSON.stringify(classifyPrimaryHarkReceiptFailure({ kind: 'http', status: 400, code: 'private_error_value' })).includes('private_error_value'), false);
  assert.deepEqual(classifyPrimaryHarkReceiptFailure({ kind: 'invalid_response' }), { state: 'pending', reason: 'invalid_response' });
});

test('pending association and attempts survive restart, then exact receipt settles once', () => {
  const first = initial();
  const waiting = settle(attempt(first), classifyPrimaryHarkReceiptFailure({ kind: 'http', status: 409,
    code: PRIMARY_HARK_CALL_RECEIPT_ASSOCIATION_PENDING }));
  const restored = validatePrimaryHarkReceiptOutbox(JSON.parse(JSON.stringify(waiting)), binding);
  assert.equal(entry(restored).attempts, 1);
  assert.deepEqual(entry(restored).body, entry(first).body);
  const delivered = settle(attempt(restored), { state: 'delivered', result: result() });
  assert.equal(entry(delivered).attempts, 2);
  assert.equal(entry(delivered).state, 'delivered');
  assert.equal(entry(delivered).lastOutcome, undefined);
  assert.deepEqual(settle(delivered, { state: 'delivered', result: { ...result(), replay: true } }), delivered);
  assert.throws(() => settle(delivered, { state: 'pending', reason: 'request_failed' }), /terminal_changed/);
  assert.throws(() => attempt(delivered), /not_pending/);
  assert.deepEqual(validatePrimaryHarkReceiptOutbox(JSON.parse(JSON.stringify(delivered)), binding), delivered);
});

test('late responses require the exact recorded attempt, and rejection cannot be silently reopened', () => {
  assert.throws(() => settle(initial(), { state: 'delivered', result: result() }), /attempt_mismatch/);
  const second = attempt(attempt(initial()));
  assert.throws(() => settle(second, { state: 'pending', reason: 'request_failed' }, 1), /attempt_mismatch/);
  const rejected = settle(second, { state: 'rejected', reason: 'http_refusal', httpStatus: 409 });
  assert.throws(() => attempt(rejected), /not_pending/);
  assert.throws(() => settle(rejected, { state: 'delivered', result: result() }), /already_rejected/);
  assert.deepEqual(settle(rejected, { state: 'rejected', reason: 'http_refusal', httpStatus: 409 }), rejected);
});

test('success must match the exact binding, delivery, case, request and variant', () => {
  for (const key of ['bindingId', 'deliveryId', 'caseId', 'requestId'] as const) {
    const candidate = result('primary_outcome');
    assert.throws(() => validatePrimaryHarkReceiptResult(binding, body(), {
      ...candidate, receipt: { ...candidate.receipt, [key]: other },
    }), /identity_mismatch/);
  }
  assert.throws(() => validatePrimaryHarkReceiptResult(binding, body(), result('owner_decision')), /identity_mismatch/);
  assert.throws(() => validatePrimaryHarkReceiptResult(binding, body('owner_decision'), {
    ...result('owner_decision'), receipt: { ...result('owner_decision').receipt, id: other },
  }), /identity_mismatch/);
  assert.throws(() => validatePrimaryHarkReceiptResult(binding, body(), {
    ...result(), receipt: { ...result().receipt, state: 'closed' },
  }), /invalid_primary_hark_receipt/);
});

test('a different receipt or run cannot replace the saved successful receipt', () => {
  const delivered = settle(attempt(initial()), { state: 'delivered', result: result() });
  for (const key of ['id', 'runId'] as const)
    assert.throws(() => settle(delivered, { state: 'delivered', result: {
      ...result(), receipt: { ...result().receipt, [key]: other },
    } }), /result_changed/);
});

test('pure result-plus-outbox preparation does not mutate the prior persisted call', () => {
  // The store owner must save both fields in one existing calls.json transaction.
  // This fixture demonstrates the returned value, not file atomicity or API proof.
  const prior = { primaryResult: { state: 'running', requestId, bindingId }, outbox: undefined };
  const completed = { state: 'completed' as const, requestId, bindingId,
    assistant: { messageId: other, eventId: receiptId, sequence: '2', text: 'Recorded result.' } };
  const prepared = preparePrimaryHarkOutcomePatch({ binding, canonicalRequestId: requestId,
    result: completed, observedAt });
  assert.equal(prior.primaryResult.state, 'running');
  assert.equal(prior.outbox, undefined);
  assert.equal(prepared.primaryResult.requestId, entry(prepared.harkReceiptOutbox).body.evidence.kind === 'primary_outcome'
    ? (entry(prepared.harkReceiptOutbox).body.evidence as { requestId: string }).requestId : undefined);
  assert.throws(() => preparePrimaryHarkOutcomePatch({ binding, canonicalRequestId: other,
    result: completed, observedAt }), /identity_mismatch/);
  assert.throws(() => preparePrimaryHarkOutcomePatch({ binding, canonicalRequestId: requestId,
    result: { ...completed, bindingId: other }, observedAt }), /identity_mismatch/);
  for (const state of ['queued', 'running'] as const) assert.throws(() => preparePrimaryHarkOutcomePatch({
    binding, canonicalRequestId: requestId, result: { state, requestId, bindingId }, observedAt }), /invalid_primary_hark_receipt/);
  assert.throws(() => preparePrimaryHarkOutcomePatch({ binding, canonicalRequestId: requestId,
    result: { state: 'failed', requestId, bindingId, error: { code: 'failed', retryable: false } }, observedAt }), /invalid_primary_hark_receipt/);
});
