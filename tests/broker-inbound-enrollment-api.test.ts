import assert from 'node:assert/strict';
import { createHash, createHmac } from 'node:crypto';
import fs from 'node:fs';
import { createServer } from 'node:http';
import test from 'node:test';
import { createPrimaryApiClient, PrimaryApiError } from '../src/broker/primaryApi.js';
import { inboundBeginRef, inboundProofDigest } from '../src/broker/inboundContract.js';
import { enrollmentCommand, enrollmentCommandDigest, enrollmentCommandSchema, enrollmentDigitProofDigest,
  enrollmentCompletionInputSchema, enrollmentCompletionResultSchema, enrollmentCompletionRecoverySchema,
  type EnrollmentCompletionInput, type EnrollmentCommand } from '../src/broker/inboundEnrollment.js';

const bytes = fs.readFileSync(new URL('./fixtures/inbound-enrollment-completion.json', import.meta.url));
assert.equal(createHash('sha256').update(bytes).digest('hex'), '956cc67ef29b2a6bb198304159e9775eabf60187e265ca506f6fd140de923ee1');
const literal = JSON.parse(bytes.toString());
const input: EnrollmentCompletionInput = literal.input;
const secret = 'synthetic-enrollment-service-secret-32-chars';
const prefix = '/internal/primary-dexter/channels/phone/inbound-enrollment-completions';
const rejected = (command: EnrollmentCommand) => ({ v: 1, command, state: 'rejected', reason: 'challenge_unavailable' });
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status });
const code = (expected: string) => (error: unknown) => error instanceof PrimaryApiError && error.code === expected;
function fixture(responder: (request: RequestInit, url: string) => Promise<Response> | Response) {
  const seen: { init: RequestInit; url: string }[] = [];
  const client = createPrimaryApiClient({ apiBaseUrl: 'https://api.example.test', phoneServiceSecret: secret,
    fetch: async (url, init) => { const request = init ?? {}; seen.push({ init: request, url: String(url) });
      return responder(request, String(url)); } });
  return { client, seen };
}

test('frozen final literals bind the separate signature-backed digit digest and four-field command', () => {
  assert.equal(inboundBeginRef(input.begin).digest, literal.begin.sha256);
  assert.equal(enrollmentDigitProofDigest(input.digitProof), literal.digitProof.sha256);
  assert.deepEqual(enrollmentCommand(input), literal.safeRef);
  for (const key of ['begin', 'digitProof', 'command'])
    assert.equal(createHash('sha256').update(literal[key].preimage).digest('hex'), literal[key].sha256);
  assert.notEqual(inboundProofDigest(input.digitProof), literal.safeRef.digitProofDigest);
  const signatureChanged = structuredClone(input); signatureChanged.digitProof.signature += 'x';
  assert.notEqual(enrollmentCommand(signatureChanged).commandDigest, literal.safeRef.commandDigest);
  const ordered = structuredClone(input); ordered.digitProof.params = Object.fromEntries(Object.entries(ordered.digitProof.params).reverse());
  assert.deepEqual(enrollmentCommand(ordered), literal.safeRef);
  const rawChanged = structuredClone(input); rawChanged.begin.providerProof.params.StirVerstat += ' ';
  assert.notEqual(enrollmentCommand(rawChanged).beginDigest, literal.safeRef.beginDigest);
});

test('strict wire refuses selector, mismatched leg, guessed route and speech fields without contact', async () => {
  const f = fixture(() => { throw new Error('unexpected HTTP'); });
  const values: unknown[] = [
    { ...input, enrollmentId: input.begin.brokerCallId },
    { ...input, digitProof: { ...input.digitProof, url: 'https://broker.example.test/twilio/inbound/enrollment-response' } },
    { ...input, digitProof: { ...input.digitProof, url: input.digitProof.url + '?id=x' } },
    { ...input, digitProof: { ...input.digitProof, url: 'invalid-private-input' } },
    ...['AccountSid', 'CallSid', 'From', 'To', 'Digits', 'SpeechResult'].map(field => ({ ...input,
      digitProof: { ...input.digitProof, params: { ...input.digitProof.params,
        [field]: field === 'Digits' ? '1234567' : field === 'SpeechResult' ? 'spoken' : 'changed' } } })),
  ];
  for (const value of values) {
    assert.equal(enrollmentCompletionInputSchema.safeParse(value).success, false);
    await assert.rejects(f.client.completeInboundEnrollment!(value as EnrollmentCompletionInput), code('invalid_request'));
  }
  assert.equal(f.seen.length, 0);
});

test('actual local HTTP serializes exact POST and safe bodyless/queryless GET using service authentication', async t => {
  const seen: { path: string; body: string; authorization: string | undefined }[] = [];
  const server = createServer(async (req, res) => {
    let body = ''; for await (const chunk of req) body += chunk;
    seen.push({ path: req.url!, body, authorization: req.headers.authorization });
    const command = req.method === 'POST' ? enrollmentCommand(JSON.parse(body)) : literal.safeRef;
    res.setHeader('content-type', 'application/json'); res.end(JSON.stringify(rejected(command)));
  });
  t.after(() => new Promise<void>(resolve => { server.closeAllConnections(); server.close(() => resolve()); }));
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  const address = server.address(); assert.ok(address && typeof address !== 'string');
  const client = createPrimaryApiClient({ apiBaseUrl: `http://127.0.0.1:${address.port}`, phoneServiceSecret: secret });
  assert.deepEqual(await client.completeInboundEnrollment!(input), rejected(literal.safeRef));
  assert.deepEqual(await client.recoverInboundEnrollment!(literal.safeRef), rejected(literal.safeRef));
  assert.deepEqual(JSON.parse(seen[0].body), input); assert.equal(seen[0].path, prefix);
  assert.equal(seen[1].body, '');
  assert.equal(seen[1].path, `${prefix}/${literal.safeRef.brokerCallId}/${literal.safeRef.beginDigest}/${literal.safeRef.digitProofDigest}/${literal.safeRef.commandDigest}`);
  for (const request of seen) assert.equal(request.authorization,
    `Bearer ${createHmac('sha256', secret).update('dexter-broker/primary-ingress/v1').digest('hex')}`);
});

test('POST refuses absent, unknown fields and a valid but foreign command; GET alone accepts absent', async () => {
  const foreign = { ...literal.safeRef, brokerCallId: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa' };
  foreign.commandDigest = enrollmentCommandDigest({ brokerCallId: foreign.brokerCallId,
    beginDigest: foreign.beginDigest, digitProofDigest: foreign.digitProofDigest });
  for (const [result, expected] of [
    [{ v: 1, command: literal.safeRef, state: 'absent' }, 'invalid_response'],
    [{ ...rejected(literal.safeRef), extra: true }, 'invalid_response'],
    [rejected(foreign), 'identity_mismatch'],
  ] as const) {
    const f = fixture(() => json(result));
    await assert.rejects(f.client.completeInboundEnrollment!(input), code(expected));
  }
  const f = fixture(() => json({ v: 1, command: literal.safeRef, state: 'absent' }));
  assert.equal((await f.client.recoverInboundEnrollment!(literal.safeRef)).state, 'absent');
  assert.equal(enrollmentCommandSchema.safeParse({ ...literal.safeRef, commandDigest: '0'.repeat(64) }).success, false);
});

test('activated receipt is strict and bound; pending owner challenge data cannot enter a completion result', () => {
  const id = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
  const receipt = { enrollmentId: id, operationId: id, requestDigest: 'a'.repeat(64), state: 'activated',
    activatedAt: '2026-09-25T00:00:00.000Z', enrollmentRef: { id, revision: 1, digest: 'b'.repeat(64) },
    possessionRef: { id, digest: 'c'.repeat(64) }, standingConsentRef: { id, revision: 1, digest: 'd'.repeat(64) } };
  const result = { v: 1, command: literal.safeRef, state: 'activated', receipt };
  assert.deepEqual(enrollmentCompletionResultSchema.parse(result), result);
  for (const changed of [{ ...receipt, voiceChallenge: '01234567' }, { ...receipt, state: 'pending_voice' },
    { ...receipt, enrollmentRef: { ...receipt.enrollmentRef, id: input.begin.brokerCallId } },
    { ...receipt, standingConsentRef: { ...receipt.standingConsentRef, revision: 2 } }])
    assert.equal(enrollmentCompletionRecoverySchema.safeParse({ ...result, receipt: changed }).success, false);
});

function bodyOfSize(size: number): EnrollmentCompletionInput {
  const value = structuredClone(input);
  const slots: { params: Record<string, string>; key: string }[] = [];
  for (const proof of [value.begin.providerProof, value.digitProof]) for (let index = 0; index < 70; index++) {
    const key = `Padding${index}`; proof.params[key] = ''; slots.push({ params: proof.params, key });
  }
  let remaining = size - Buffer.byteLength(JSON.stringify(value));
  for (const slot of slots) { const length = Math.min(2048, remaining); slot.params[slot.key] = 'x'.repeat(length); remaining -= length; }
  assert.equal(remaining, 0); assert.equal(Buffer.byteLength(JSON.stringify(value)), size);
  assert.equal(enrollmentCompletionInputSchema.safeParse(value).success, true);
  return value;
}
test('completion-only request cap accepts more than24KiB and exact262144, refuses262145 before HTTP', async () => {
  const f = fixture(request => json(rejected(enrollmentCommand(JSON.parse(String(request.body))))));
  for (const size of [24_577, 262_144]) {
    const value = bodyOfSize(size); await f.client.completeInboundEnrollment!(value);
    assert.equal(Buffer.byteLength(String(f.seen.at(-1)!.init.body)), size);
  }
  await assert.rejects(f.client.completeInboundEnrollment!(bodyOfSize(262_145)), code('invalid_request'));
  assert.equal(f.seen.length, 2);
  const legacy = structuredClone(input.begin);
  for (let i = 0; i < 16; i++) legacy.providerProof.params[`Legacy${i}`] = 'y'.repeat(1800);
  await assert.rejects(f.client.beginInbound!(legacy), code('invalid_request'));
  assert.equal(f.seen.length, 2);
});

test('lost POST receives no transport retry; explicit recovery uses exact GET and safe errors', async () => {
  let sent = 0;
  const f = fixture(request => { if (request.method === 'POST') { sent++; throw new Error('private provider detail'); }
    return json({ v: 1, command: literal.safeRef, state: 'absent' }); });
  await assert.rejects(f.client.completeInboundEnrollment!(input), error => error instanceof PrimaryApiError
    && error.code === 'request_failed' && !error.message.includes('private'));
  assert.equal(sent, 1); await f.client.recoverInboundEnrollment!(literal.safeRef); assert.equal(sent, 1);
  const unavailable = fixture(() => json({ error: { code: 'inbound_enrollment_unavailable', retryable: true } }, 503));
  await assert.rejects(unavailable.client.completeInboundEnrollment!(input), error => error instanceof PrimaryApiError
    && error.status === 503 && error.remoteCode === 'inbound_enrollment_unavailable' && error.retryable === true);
  assert.equal(unavailable.seen.length, 1);
});

test('only the exact finite HTTP code/status/retryable envelope is retained for enrollment', async () => {
  for (const body of [
    { error: { code: 'unrecognized_code', retryable: false } },
    { error: { code: 'inbound_carrier_attestation_required', retryable: true } },
    { error: { code: 'inbound_enrollment_unavailable', retryable: false } },
    { error: { code: 'inbound_carrier_attestation_required', retryable: false }, Digits: '01234567' },
  ]) {
    const f = fixture(() => json(body, 403));
    await assert.rejects(f.client.completeInboundEnrollment!(input), error => error instanceof PrimaryApiError
      && error.status === 403 && error.remoteCode === undefined && error.retryable === undefined);
  }
});
