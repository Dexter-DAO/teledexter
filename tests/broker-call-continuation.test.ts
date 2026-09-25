import assert from 'node:assert/strict';
import test from 'node:test';
import { CallContinuationError, assertCallContinuationReplay, callContinuationPatch, callContinuationSchema,
  parseOptionalCallContinuation, requestDeliveryRequestForContinuation, validateCallContinuation,
  type CallContinuationContext, type CallContinuationErrorCode } from '../src/broker/callContinuation.js';
import { PHONE_REQUEST_DELIVERY_VERSION, requestDeliveryRequestSchema } from '../src/broker/requestDelivery.js';

const callId = '11111111-1111-4111-8111-111111111111';
const controlRequestId = '22222222-2222-4222-8222-222222222222';
const originalRequestId = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
const questionId = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';
const differentId = 'cccccccc-cccc-4ccc-8ccc-cccccccccccc';
const purpose = 'Talk about the saved research question.';
const selector = () => ({ originalRequestId, questionId, questionRevision: 1 as const });
const context = (): CallContinuationContext => ({ id: callId, requestId: controlRequestId, direction: 'outbound', purpose,
  continuation: selector(), callAssignment: { origin: 'authenticated_broker_control',
    value: { assignmentId: controlRequestId, brokerCallId: callId, revision: 1, purpose } } });
const fails = (code: CallContinuationErrorCode) => (error: unknown) => error instanceof CallContinuationError && error.code === code;

test('explicit selector maps only its exact identities to the existing API association contract', () => {
  const input = selector();
  assert.deepEqual(callContinuationSchema.parse(input), input);
  assert.deepEqual(parseOptionalCallContinuation(input), input);
  const mapped = requestDeliveryRequestForContinuation(input);
  assert.deepEqual(mapped, { v: PHONE_REQUEST_DELIVERY_VERSION, ...input });
  assert.deepEqual(requestDeliveryRequestSchema.parse(mapped), mapped);
  assert.deepEqual(Object.keys(mapped), ['v', 'originalRequestId', 'questionId', 'questionRevision']);
  assert.equal('runId' in mapped, false); assert.equal('ownerId' in mapped, false); assert.equal('bindingId' in mapped, false);
});

test('selector rejects null, missing keys, unsupported revision and added authority or text', () => {
  for (const invalid of [null, '', [], {}, { originalRequestId, questionId },
    { ...selector(), questionRevision: 2 }, { ...selector(), questionRevision: '1' },
    ...[{ ownerId: differentId }, { bindingId: differentId }, { runId: differentId }, { text: 'Continue it' },
      { v: PHONE_REQUEST_DELIVERY_VERSION }, { extra: true }].map(extra => ({ ...selector(), ...extra }))]) {
    assert.throws(() => parseOptionalCallContinuation(invalid), fails('invalid_call_continuation'));
    assert.throws(() => requestDeliveryRequestForContinuation(invalid), fails('invalid_call_continuation'));
  }
  assert.throws(() => requestDeliveryRequestForContinuation(undefined), fails('invalid_call_continuation'));
});

test('UUIDs use the exact lower-case API input shape without normalizing retained values', () => {
  for (const key of ['originalRequestId', 'questionId'] as const) {
    for (const invalid of [selector()[key].toUpperCase(), ` ${selector()[key]}`, `${selector()[key]}\n`,
      '../requests', '00000000-0000-0000-0000-000000000000', differentId.replace('-4ccc-', '-9ccc-')]) {
      const input = { ...selector(), [key]: invalid }; const before = JSON.stringify(input);
      assert.throws(() => parseOptionalCallContinuation(input), fails('invalid_call_continuation'));
      assert.equal(JSON.stringify(input), before);
    }
  }
});

test('legacy absence creates no key and does not reinterpret ordinary, Hark or inbound calls', () => {
  assert.equal(parseOptionalCallContinuation(undefined), undefined);
  assert.deepEqual(callContinuationPatch(undefined), {});
  assert.deepEqual(Object.keys(callContinuationPatch(undefined)), []);
  assert.equal(assertCallContinuationReplay(undefined, undefined), undefined);
  for (const call of [
    { id: callId, requestId: controlRequestId, direction: 'outbound', purpose },
    { id: callId, requestId: controlRequestId, direction: 'inbound', purpose, inbound: { retained: true } },
    { id: callId, requestId: controlRequestId, direction: 'outbound', purpose,
      callAssignment: { origin: 'authenticated_hark_delivery', value: { retained: true } }, harkJournal: { retained: true } },
  ]) {
    const before = JSON.stringify(call);
    assert.equal(validateCallContinuation(call), undefined);
    assert.equal(JSON.stringify({ ...call, ...callContinuationPatch(undefined) }), before);
    assert.equal(Object.prototype.hasOwnProperty.call(call, 'continuation'), false);
  }
});

test('a continuation requires the exact saved authenticated operator assignment', () => {
  assert.deepEqual(validateCallContinuation(context()), selector());
  for (const call of [
    { ...context(), direction: 'inbound' }, { ...context(), callAssignment: undefined },
    { ...context(), callAssignment: { origin: 'authenticated_hark_delivery', value: {} } },
    { ...context(), inbound: {} }, { ...context(), harkJournal: {} },
    { ...context(), harkDispatch: {} }, { ...context(), harkReceiptOutbox: {} },
  ]) assert.throws(() => validateCallContinuation(call), fails('call_continuation_not_allowed'));
});

test('saved assignment identity cannot borrow a different call, control request or purpose', () => {
  for (const fields of [{ id: differentId }, { requestId: differentId }, { purpose: 'A different purpose.' }])
    assert.throws(() => validateCallContinuation({ ...context(), ...fields }), fails('call_continuation_identity_mismatch'));
  const original = context();
  original.callAssignment = { origin: 'authenticated_broker_control', value: {
    assignmentId: controlRequestId, brokerCallId: callId, revision: 1, purpose, continuation: selector() } };
  assert.throws(() => validateCallContinuation(original), fails('call_continuation_not_allowed'));
});

test('same saved selector survives exact JSON restart and replay without mutation', () => {
  const saved = context(); const bytes = JSON.stringify(saved);
  const restarted = JSON.parse(bytes) as CallContinuationContext;
  const selected = validateCallContinuation(restarted);
  assert.deepEqual(assertCallContinuationReplay(selected, selector()), selector());
  assert.equal(JSON.stringify(restarted), bytes);
  assert.deepEqual(requestDeliveryRequestForContinuation(selected), requestDeliveryRequestForContinuation(saved.continuation));
});

test('same control request cannot add, change or omit a previously saved selector', () => {
  assert.throws(() => assertCallContinuationReplay(undefined, selector()), fails('call_continuation_changed'));
  assert.throws(() => assertCallContinuationReplay(selector(), undefined), fails('call_continuation_changed'));
  for (const changed of [{ ...selector(), originalRequestId: differentId }, { ...selector(), questionId: differentId }])
    assert.throws(() => assertCallContinuationReplay(selector(), changed), fails('call_continuation_changed'));
  for (const invalid of [null, { ...selector(), questionRevision: 2 }, { ...selector(), extra: true }]) {
    assert.throws(() => assertCallContinuationReplay(selector(), invalid), fails('invalid_call_continuation'));
    assert.throws(() => assertCallContinuationReplay(invalid, selector()), fails('invalid_call_continuation'));
  }
});

test('parser and patch return independent values while errors disclose no arbitrary input', () => {
  const input = selector(); const patch = callContinuationPatch(input);
  input.questionId = differentId;
  assert.equal(patch.continuation!.questionId, questionId);
  const first = selector(); const replay = assertCallContinuationReplay(first, selector())!;
  replay.questionId = differentId;
  assert.equal(first.questionId, questionId);
  const sensitive = 'arbitrary-sensitive-value';
  try { parseOptionalCallContinuation({ ...selector(), originalRequestId: sensitive }); assert.fail('expected refusal'); }
  catch (error) {
    assert.ok(error instanceof CallContinuationError);
    assert.equal(error.message, 'invalid_call_continuation');
    assert.equal(JSON.stringify(error).includes(sensitive), false);
  }
});
