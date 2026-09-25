import assert from 'node:assert/strict';
import test from 'node:test';
import { OUTBOUND_SETTLEMENT_VERSION, OutboundRetryError, assertOutboundRetryReplay,
  assertOutboundRetrySettlement, createOutboundSettlement, outboundRetryOfPatch, outboundRetryOfSchema,
  outboundRetrySuccessorSchema, outboundSettlementDigest, outboundSettlementForCall, outboundSettlementSchema,
  parseOptionalOutboundRetryOf, type OutboundSettledInput, type SettledOutboundSettlement } from '../src/broker/outboundSettlement.js';

const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const call = { callId: uuid(1), requestId: uuid(2) };
const sid = `CA${'a'.repeat(32)}`;
const at = '2026-09-24T10:00:00.000Z';
const never = (): OutboundSettledInput => ({ v: OUTBOUND_SETTLEMENT_VERSION, ...call,
  state: 'settled', kind: 'never_dispatched', observedAt: at, reason: 'preflight_refused' });
const terminal = (providerStatus: 'completed' | 'busy' | 'no-answer' | 'canceled' | 'failed' = 'no-answer'): OutboundSettledInput => ({
  v: OUTBOUND_SETTLEMENT_VERSION, ...call, state: 'settled', kind: 'provider_terminal', observedAt: at,
  callSid: sid, providerStatus, source: 'signed_status_callback', evidenceDigest: 'b'.repeat(64) });
const edge = (receipt: SettledOutboundSettlement) => ({ ...call, settlementDigest: receipt.digest });
const hasCode = (code: OutboundRetryError['code']) => (error: unknown) => error instanceof OutboundRetryError && error.code === code;

test('ordered digest matches independent contract vectors for each settled arm', () => {
  assert.equal(outboundSettlementDigest(never()), 'b6586347887c076a1add8be0ed604691496a72d10a6e562ddcb4f86d426da33d');
  assert.equal(outboundSettlementDigest(terminal()), '49f9a7c1a9220568619e723d6999d74d4e5075094950d31ce7b057513f335de9');
  assert.deepEqual(outboundSettlementSchema.parse(createOutboundSettlement(terminal())), createOutboundSettlement(terminal()));
});

test('shared API literal dc662014 retains the exact offset timestamp and digest', () => {
  const shared = { v: OUTBOUND_SETTLEMENT_VERSION, state: 'settled' as const, kind: 'provider_terminal' as const,
    callId: uuid(8), requestId: uuid(9), observedAt: '2026-09-24T12:00:00.000+00:00', callSid: sid,
    providerStatus: 'no-answer' as const, source: 'authenticated_fetch' as const, evidenceDigest: 'b'.repeat(64),
    digest: '415e14aca1a7de94a1da34b159b792cac22859326deb5bb8dff46c5ae155965b' };
  assert.deepEqual(outboundSettlementForCall(shared, { callId: uuid(8), requestId: uuid(9), callSid: sid }), shared);
  const { digest: expected, ...input } = shared;
  assert.equal(outboundSettlementDigest(input), expected);
});

test('historical absence remains absent and cannot permit a fresh attempt', () => {
  assert.equal(outboundSettlementForCall(undefined, call), undefined);
  assert.deepEqual(outboundRetryOfPatch(undefined), {});
  assert.equal(Object.hasOwn(outboundRetryOfPatch(undefined), 'retryOf'), false);
  assert.equal(parseOptionalOutboundRetryOf(undefined), undefined);
  const original = JSON.stringify({ ...call, status: 'failed', endedAt: at });
  assert.equal(JSON.stringify({ ...JSON.parse(original), ...outboundRetryOfPatch(undefined) }), original);
  assert.throws(() => assertOutboundRetrySettlement(call, undefined, { ...call, settlementDigest: 'a'.repeat(64) }),
    hasCode('call_retry_unavailable'));
  assert.throws(() => outboundSettlementForCall(null, call));
  assert.throws(() => parseOptionalOutboundRetryOf(null));
});

test('unresolved is a closed arm without a digest or implied settlement', () => {
  const unresolved = { v: OUTBOUND_SETTLEMENT_VERSION, ...call, state: 'unresolved' };
  assert.deepEqual(outboundSettlementForCall(unresolved, call), unresolved);
  for (const extra of [{ digest: 'a'.repeat(64) }, { observedAt: at }, { callSid: sid }, { providerStatus: 'failed' }])
    assert.equal(outboundSettlementSchema.safeParse({ ...unresolved, ...extra }).success, false);
  assert.throws(() => assertOutboundRetrySettlement(call, unresolved, { ...call, settlementDigest: 'a'.repeat(64) }),
    hasCode('call_retry_unavailable'));
});

test('local failed, endedAt, finalized or missing SID cannot manufacture a receipt', () => {
  for (const local of [{ status: 'failed' }, { endedAt: at }, { finalized: true }, { callSid: undefined }])
    assert.equal(outboundSettlementSchema.safeParse({ v: OUTBOUND_SETTLEMENT_VERSION, ...call, ...local }).success, false);
  assert.throws(() => assertOutboundRetrySettlement(call, { status: 'failed', endedAt: at },
    { ...call, settlementDigest: 'a'.repeat(64) }), hasCode('call_request_unavailable'));
});

test('settled projection rejects changed digest and every changed bound identity', () => {
  const receipt = createOutboundSettlement(terminal());
  for (const patch of [{ digest: 'c'.repeat(64) }, { callId: uuid(3) }, { requestId: uuid(4) },
    { callSid: `CA${'d'.repeat(32)}` }, { observedAt: '2026-09-24T10:00:00.001Z' },
    { evidenceDigest: 'c'.repeat(64) }, { source: 'authenticated_fetch' }])
    assert.equal(outboundSettlementSchema.safeParse({ ...receipt, ...patch }).success, false);
  assert.throws(() => outboundSettlementForCall(receipt, { ...call, callSid: `CA${'d'.repeat(32)}` }));
  assert.throws(() => outboundSettlementForCall(receipt, call));
  assert.throws(() => outboundSettlementForCall(createOutboundSettlement({ ...terminal(), callId: uuid(3) }),
    { ...call, callSid: sid }));
  assert.throws(() => outboundSettlementForCall(createOutboundSettlement({ ...terminal(), requestId: uuid(4) }),
    { ...call, callSid: sid }));
});

test('timestamp spelling is retained and contributes to the digest', () => {
  const original = createOutboundSettlement(never());
  const offset = createOutboundSettlement({ ...never(), observedAt: '2026-09-24T10:00:00.000+00:00' });
  assert.notEqual(original.digest, offset.digest);
  assert.equal(offset.observedAt, '2026-09-24T10:00:00.000+00:00');
  assert.deepEqual(outboundSettlementSchema.parse(offset), offset);
});

test('closed union refuses private provider data and unsupported status/source/reason', () => {
  const receipt = createOutboundSettlement(terminal());
  for (const extra of [{ from: '+12025550100' }, { to: '+12025550101' }, { accountSid: `AC${'a'.repeat(32)}` },
    { signature: 'not-a-credential' }, { url: 'https://example.invalid/callback' }, { params: {} }])
    assert.equal(outboundSettlementSchema.safeParse({ ...receipt, ...extra }).success, false);
  for (const providerStatus of ['ringing', 'queued', 'in-progress', 'unknown'])
    assert.equal(outboundSettlementSchema.safeParse({ ...receipt, providerStatus }).success, false);
  assert.equal(outboundSettlementSchema.safeParse({ ...receipt, source: 'local_status' }).success, false);
  assert.equal(outboundSettlementSchema.safeParse({ ...createOutboundSettlement(never()), reason: 'timeout' }).success, false);
});

test('missed or failed transport evidence permits only a fresh admission; completed is excluded', () => {
  for (const status of ['busy', 'no-answer', 'canceled', 'failed'] as const) {
    const receipt = createOutboundSettlement(terminal(status));
    assert.deepEqual(assertOutboundRetrySettlement({ ...call, callSid: sid }, receipt, edge(receipt)), receipt);
  }
  const completed = createOutboundSettlement(terminal('completed'));
  assert.throws(() => assertOutboundRetrySettlement({ ...call, callSid: sid }, completed, edge(completed)),
    hasCode('call_retry_unavailable'));
  const refused = createOutboundSettlement(never());
  assert.deepEqual(assertOutboundRetrySettlement(call, refused, edge(refused)), refused);
  assert.throws(() => assertOutboundRetrySettlement({ ...call, callSid: sid }, refused, edge(refused)),
    hasCode('call_request_unavailable'));
});

test('retry edge binds exact prior call/request/digest and preserves exact restart replay', () => {
  const receipt = createOutboundSettlement(terminal()), prior = edge(receipt);
  const reloaded = JSON.parse(JSON.stringify({ retryOf: prior, retrySuccessor: { ...prior, callId: uuid(8), requestId: uuid(9) } }));
  assert.deepEqual(assertOutboundRetryReplay(reloaded.retryOf, prior), prior);
  assert.deepEqual(outboundRetryOfPatch(prior), { retryOf: prior });
  assert.deepEqual(outboundRetrySuccessorSchema.parse(reloaded.retrySuccessor), reloaded.retrySuccessor);
  for (const changed of [{ ...prior, callId: uuid(3) }, { ...prior, requestId: uuid(4) }, { ...prior, settlementDigest: 'c'.repeat(64) }]) {
    assert.throws(() => assertOutboundRetryReplay(prior, changed), hasCode('call_request_conflict'));
    assert.throws(() => assertOutboundRetrySettlement({ ...call, callSid: sid }, receipt, changed), hasCode('call_request_conflict'));
  }
  assert.throws(() => assertOutboundRetryReplay(prior, undefined), hasCode('call_request_conflict'));
  assert.throws(() => assertOutboundRetryReplay(undefined, prior), hasCode('call_request_conflict'));
  assert.equal(assertOutboundRetryReplay(undefined, undefined), undefined);
});

test('strict retry edge has all three fields and never normalizes retained IDs', () => {
  const prior = edge(createOutboundSettlement(never()));
  for (const invalid of [null, {}, { ...prior, settlementDigest: undefined }, { ...prior, requestId: undefined },
    { ...prior, callId: undefined }, { ...prior, extra: true }, { ...prior, callId: 'ABCDEFAB-1234-4234-8234-123456789abc' },
    { ...prior, settlementDigest: 'A'.repeat(64) }])
    assert.equal(outboundRetryOfSchema.safeParse(invalid).success, false);
});
