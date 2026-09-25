import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';
import test from 'node:test';
import ts from 'typescript';
import { CALL_REQUEST_RECEIPT_NAMESPACE, PHONE_OWNER_CALL_PURPOSE, CallRequestReceiptError,
  callRequestIdFromLookupPath, callRequestCallSchema, callRequestReceiptSchema, callRequestReceiptFailure,
  callRequestStateSchema, readCallRequestReceipt, type CallRequestJournalPort,
  type CallRequestReceiptErrorCode } from '../src/broker/callRequestReceipt.js';
import { makeResolvedHarkAssignment } from './fixtures/broker-hark.js';

const id = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
const requestId = '11e30788-2d32-5d08-9b60-a196e88775f8';
const originalRequestId = '44444444-4444-4444-8444-444444444444';
const questionId = '33333333-3333-4333-8333-333333333333';
const otherId = '55555555-5555-4555-8555-555555555555';
const continuation = () => ({ originalRequestId, questionId, questionRevision: 1 as const });
const selected = () => ({ id, requestId, direction: 'outbound' as const, purpose: PHONE_OWNER_CALL_PURPOSE,
  continuation: continuation(), status: 'unknown', number: '+12025550123', createdAt: '2026-09-24T00:00:00.000Z',
  updatedAt: '2026-09-24T00:01:00.000Z', delegations: {},
  callAssignment: { origin: 'authenticated_broker_control' as const,
    value: { assignmentId: requestId, brokerCallId: id, revision: 1 as const, purpose: PHONE_OWNER_CALL_PURPOSE } },
  failure: { code: 'private_failure_code', message: 'private provider text' }, transcript: { private: 'saved transcript' } });
const full = (calls: Record<string, unknown>) => ({ version: 1, calls });
const source = (journal: unknown): CallRequestJournalPort => ({
  readCallRequestJournal: () => ({ state: 'available', journal }),
});
const fails = (code: CallRequestReceiptErrorCode) => (error: unknown) =>
  error instanceof CallRequestReceiptError && error.code === code;

test('exact lookup reads the full journal and returns only the saved receipt fields', () => {
  const calls: Record<string, unknown> = {};
  for (let index = 0; index < 75; index++) {
    const legacyId = `retained-call-${index}`;
    calls[legacyId] = { id: legacyId, requestId: `retained-request-${index}` };
  }
  calls[id] = selected();
  const journal = full(calls); const before = JSON.stringify(journal);
  const result = readCallRequestReceipt(source(journal), requestId);
  assert.deepEqual(result, { schemaVersion: 1, namespace: CALL_REQUEST_RECEIPT_NAMESPACE, requestId,
    call: { id, requestId, direction: 'outbound', purpose: PHONE_OWNER_CALL_PURPOSE,
      continuation: continuation(), status: 'unknown' } });
  assert.deepEqual(callRequestReceiptSchema.parse(result), result);
  assert.equal(JSON.stringify(result).includes('private'), false);
  assert.equal('number' in result.call, false); assert.equal('callSid' in result.call, false);
  assert.equal(JSON.stringify(journal), before);
  result.call.continuation.questionId = otherId;
  assert.equal((calls[id] as ReturnType<typeof selected>).continuation.questionId, questionId);
});

for (const status of callRequestStateSchema.options) {
  test(`saved ${status} state is history and is returned unchanged`, () => {
    const call = { ...selected(), status };
    const result = readCallRequestReceipt(source(full({ [id]: call })), requestId);
    assert.equal(result.call.status, status);
    assert.equal(result.call.id, id);
    assert.equal(Object.keys(result.call).length, 6);
  });
}

test('only an available full journal can prove an absent request', () => {
  assert.throws(() => readCallRequestReceipt(source(full({})), requestId), fails('call_request_not_found'));
  assert.throws(() => readCallRequestReceipt(source(full({ [id]: selected() })), otherId), fails('call_request_not_found'));
  for (const state of ['unavailable', 'uncertain'] as const) {
    assert.throws(() => readCallRequestReceipt({ readCallRequestJournal: () => ({ state }) }, requestId),
      fails('call_request_unavailable'));
  }
  assert.throws(() => readCallRequestReceipt({ readCallRequestJournal: () => {
    throw new Error('private disk path and failure detail');
  } }, requestId), fails('call_request_unavailable'));
});

test('unknown snapshot shapes and duplicate exact request keys refuse absence and success', () => {
  for (const journal of [undefined, null, [], { version: 1, calls: [] }, { version: 2, calls: {} },
    { version: 1, calls: {}, recentOnly: true }, full({ [id]: null }), full({ [id]: selected(), broken: {} }),
    full({ wrongKey: selected() }), full({ [id]: { id, requestId: '' } }),
    full({ [id]: selected(), [otherId]: { ...selected(), id: otherId } })]) {
    for (const requested of [requestId, otherId]) {
      assert.throws(() => readCallRequestReceipt(source(journal), requested), fails('call_request_unavailable'));
    }
  }
});

test('legacy shapes and exact UUID casing remain outside selected-call validation', () => {
  const upper = 'BBBBBBBB-BBBB-4BBB-8BBB-BBBBBBBBBBBB';
  const calls = { [id]: selected(),
    [upper]: { id: upper, requestId: upper, direction: 'outbound', purpose: 'Read\u0000my portfolio.' },
    inbound: { id: 'inbound', requestId: 'retained-inbound-request', direction: 'inbound' },
    callback: { id: 'callback', requestId: 'retained-callback-request', direction: 'outbound' },
  };
  const journal = full(calls); const before = JSON.stringify(journal);
  assert.equal(readCallRequestReceipt(source(journal), requestId).call.id, id);
  assert.throws(() => readCallRequestReceipt(source(journal), upper), fails('call_request_conflict'));
  // Existing Broker request keys are exact strings; the lookup never rewrites them.
  assert.throws(() => readCallRequestReceipt(source(journal), upper.toLowerCase()), fails('call_request_not_found'));
  assert.equal(JSON.stringify(journal), before);
});

test('selected receipt preserves historical identifier bytes and validates through normalized consumer schema', () => {
  const call = selected(); call.id = id.toUpperCase(); call.requestId = requestId.toUpperCase();
  call.callAssignment.value.brokerCallId = call.id; call.callAssignment.value.assignmentId = call.requestId;
  const journal = full({ [call.id]: call }); const bytes = JSON.stringify(journal);
  const result = readCallRequestReceipt(source(journal), call.requestId);
  assert.equal(result.call.id, call.id); assert.equal(result.requestId, call.requestId);
  assert.equal(callRequestReceiptSchema.parse(result).requestId, requestId);
  assert.equal(JSON.stringify(journal), bytes);
});

test('valid ordinary, inbound, Hark and wrong-purpose matches return conflict without call details', () => {
  const ordinary = selected(); const { continuation: ignored, ...withoutSelection } = ordinary;
  const hark = makeResolvedHarkAssignment();
  for (const call of [withoutSelection,
    { ...selected(), direction: 'inbound' },
    { ...selected(), purpose: 'Read the portfolio.' },
    { ...selected(), callAssignment: { origin: 'authenticated_hark_delivery', value: hark.callAssignment } },
    { ...selected(), inbound: {} }, { ...selected(), harkJournal: {} }, { ...selected(), harkDispatch: {} },
    { ...selected(), harkReceiptOutbox: {} }]) {
    try { readCallRequestReceipt(source(full({ [id]: call })), requestId); assert.fail('expected conflict'); }
    catch (error) {
      assert.ok(fails('call_request_conflict')(error));
      assert.deepEqual(callRequestReceiptFailure(error), { status: 409, body: { error: 'call_request_conflict' } });
    }
  }
});

test('corrupt selected projection or assignment remains unavailable', () => {
  for (const call of [
    { ...selected(), status: 'made-up' }, { ...selected(), continuation: null },
    { ...selected(), continuation: { ...continuation(), questionRevision: 2 } },
    { ...selected(), continuation: { ...continuation(), rawQuestion: 'private' } },
    { ...selected(), continuation: { ...continuation(), originalRequestId: 'not-a-uuid' } },
    { ...selected(), callAssignment: undefined },
    { ...selected(), callAssignment: { ...selected().callAssignment,
      value: { ...selected().callAssignment.value, brokerCallId: otherId } } },
  ]) assert.throws(() => readCallRequestReceipt(source(full({ [id]: call })), requestId),
    fails('call_request_unavailable'));
});

test('query, encoded paths, trailing segments and malformed UUIDs refuse before storage access', () => {
  let reads = 0; const port: CallRequestJournalPort = { readCallRequestJournal() {
    reads++; return { state: 'available', journal: full({}) };
  } };
  for (const candidate of [undefined, null, '', [], requestId + '?x=1', requestId + '/', 'not-a-uuid',
    '00000000-0000-0000-0000-000000000000', requestId.replace('-5d08-', '-9d08-')])
    assert.throws(() => readCallRequestReceipt(port, candidate), fails('invalid_call_request'));
  for (const route of ['/internal/broker/call-requests', '/internal/broker/call-requests/',
    `/internal/broker/call-requests/${requestId}?x=1`, `/internal/broker/call-requests/${requestId}#fragment`,
    `/internal/broker/call-requests/${requestId}/`, `/internal/broker/call-requests/${requestId}/more`,
    `/internal/broker/call-requests/%31${requestId.slice(1)}`, `/internal/broker/calls/${requestId}`])
    assert.throws(() => callRequestIdFromLookupPath(route), fails('invalid_call_request'));
  assert.equal(callRequestIdFromLookupPath(`/internal/broker/call-requests/${requestId}`), requestId);
  assert.equal(callRequestIdFromLookupPath(`/internal/broker/call-requests/${requestId.toUpperCase()}`), requestId.toUpperCase());
  assert.equal(reads, 0);
});

test('wire schemas reject extra keys, changed identity and unsupported state', () => {
  const receipt = readCallRequestReceipt(source(full({ [id]: selected() })), requestId);
  for (const value of [{ ...receipt, requestId: otherId }, { ...receipt, schemaVersion: 2 },
    { ...receipt, namespace: 'other' }, { ...receipt, extra: true },
    { ...receipt, call: { ...receipt.call, number: '+12025550123' } },
    { ...receipt, call: { ...receipt.call, status: 'answered' } },
    { ...receipt, call: { ...receipt.call, continuation: { ...continuation(), ownerId: otherId } } }])
    assert.equal(callRequestReceiptSchema.safeParse(value).success, false);
});

test('error mapper emits only finite codes and ignores arbitrary or mutated exception details', () => {
  for (const [code, status] of [['invalid_call_request', 400], ['unauthorized', 401],
    ['call_request_not_found', 404], ['call_request_conflict', 409], ['call_request_unavailable', 503]] as const)
    assert.deepEqual(callRequestReceiptFailure(new CallRequestReceiptError(code)), { status, body: { error: code } });
  const mutated = new CallRequestReceiptError('call_request_unavailable');
  Object.assign(mutated, { code: 'private message', status: 200 });
  for (const error of [mutated, new Error('private path'), { code: 'private detail', status: 404 }, undefined])
    assert.deepEqual(callRequestReceiptFailure(error), { status: 503, body: { error: 'call_request_unavailable' } });
});

// Optional coordinated crossing against the exact exported API module. Normal
// Broker tests require neither the API worktree nor its dependency directory.
const sharedSource = process.env.BROKER_API_QUESTION_CALL_CONTRACT_SOURCE;
const dependencyRoot = process.env.BROKER_API_DEPENDENCY_ROOT;
test('exact API source accepts the Broker projection and enforces the same strict wire', {
  skip: !sharedSource,
}, () => {
  assert.ok(sharedSource && dependencyRoot, 'Both API source and dependency paths are required');
  const code = fs.readFileSync(sharedSource, 'utf8');
  assert.equal(createHash('sha256').update(code).digest('hex'),
    'fdbf028e3d8a7118bbc40675ab2d5fedbf1efebbd89db964b408991fd238f4cb');
  const apiRequire = createRequire(path.join(dependencyRoot, 'package.json'));
  assert.match(apiRequire('zod/package.json').version as string, /^3\./);
  const compiled = ts.transpileModule(code, { compilerOptions: {
    target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS,
  } }).outputText;
  type Schema = { parse(value: unknown): unknown; safeParse(value: unknown): { success: boolean } };
  const exports: { brokerQuestionCallLookupSchema: Schema; brokerQuestionCallSchema: Schema;
    phoneOwnerCallRequestId(owner: string, client: string): string } = {} as never;
  new Function('require', 'exports', compiled)(apiRequire, exports);
  const receipt = readCallRequestReceipt(source(full({ [id]: selected() })), requestId);
  assert.deepEqual(exports.brokerQuestionCallLookupSchema.parse(receipt), callRequestReceiptSchema.parse(receipt));
  assert.deepEqual(exports.brokerQuestionCallSchema.parse(receipt.call), callRequestCallSchema.parse(receipt.call));
  assert.equal(exports.phoneOwnerCallRequestId('11111111-1111-4111-8111-111111111111',
    '22222222-2222-4222-8222-222222222222'), requestId);
  for (const changed of [{ ...receipt, requestId: otherId }, { ...receipt, extra: true },
    { ...receipt, call: { ...receipt.call, status: 'answered' } },
    { ...receipt, call: { ...receipt.call, recipientLast4: '0123' } }]) {
    assert.equal(exports.brokerQuestionCallLookupSchema.safeParse(changed).success, false);
    assert.equal(callRequestReceiptSchema.safeParse(changed).success, false);
  }
});
