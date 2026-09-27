import assert from 'node:assert/strict';
import { createHash, createHmac } from 'node:crypto';
import fs from 'node:fs';
import { createServer } from 'node:http';
import { createRequire } from 'node:module';
import path from 'node:path';
import test from 'node:test';
import ts from 'typescript';
import { createPrimaryApiClient, PrimaryApiError, type PrimaryApiOptions } from '../src/broker/primaryApi.js';
import {
  inboundBeginRef, inboundProofDigest, inboundBeginIdentitySchema, inboundOutcomeSchema,
  type InboundBeginIdentity, type InboundOutcome, type InboundApprovalRef,
} from '../src/broker/inboundContract.js';

const brokerCallId = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
const attachmentId = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';
const approvalId = 'cccccccc-cccc-4ccc-8ccc-cccccccccccc';
const bindingId = 'dddddddd-dddd-4ddd-8ddd-dddddddddddd';
const primaryDexterId = 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee';
const threadId = 'ffffffff-ffff-4fff-8fff-ffffffffffff';
const foreignId = '11111111-1111-4111-8111-111111111111';
const callSid = 'CA22222222222222222222222222222222';
const begin: InboundBeginIdentity = { brokerCallId, providerProof: {
  url: 'https://broker.example.test/twilio/inbound', signature: 'fixture-signature-not-provider-proof',
  params: { To: '+15550000002', Direction: 'inbound', From: '+15550000001', CallSid: callSid,
    AccountSid: 'AC11111111111111111111111111111111', CallerName: 'Example ☃' },
} };
const beginRef = inboundBeginRef(begin);
const approvalRef: InboundApprovalRef = { attachmentId, approvalId, digest: '3'.repeat(64) };
const expiresAt = '2026-09-23T22:02:00.000Z';
const consumedAt = '2026-09-23T22:01:00.000Z';
const closedAt = '2026-09-23T22:01:30.000Z';
const pending: InboundOutcome = { attachmentId, beginRef, state: 'pending', expiresAt };
const approved: InboundOutcome = { attachmentId, beginRef, state: 'approved', expiresAt, approvalRef };
const bound: InboundOutcome = { attachmentId, beginRef, state: 'bound', approvalRef,
  bindingId, consumedAt, callState: 'open' };
const cancelled: InboundOutcome = { attachmentId, beginRef, state: 'cancelled', cancelledAt: closedAt, expiresAt: null };
const expired: InboundOutcome = { attachmentId, beginRef, state: 'expired', expiresAt };
const closed = { attachmentId, beginRef, approvalRef, bindingId, callSid, state: 'closed', closedAt };
const context = { primaryDexterId, threadId, displayName: 'Dexter', messages: [] };
const call = { bindingId, callSid, state: 'open', context };
const secret = 'synthetic-phone-service-secret-32-characters';
const prefix = '/internal/primary-dexter/channels/phone';
const closure = { begin, approvalRef, bindingId };
const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status,
  headers: { 'content-type': 'application/json' } });
const code = (expected: string) => (error: unknown) => error instanceof PrimaryApiError && error.code === expected;

function harness(replies: (Response | Error)[], options: Partial<PrimaryApiOptions> = {}) {
  const requests: { url: string; init: RequestInit }[] = [];
  const fetcher: typeof fetch = async (url, init) => {
    requests.push({ url: String(url), init: init ?? {} });
    const reply = replies.shift();
    assert.ok(reply, 'unexpected extra HTTP request');
    if (reply instanceof Error) throw reply;
    return reply;
  };
  return { requests, client: createPrimaryApiClient({ apiBaseUrl: 'https://api.example.test',
    phoneServiceSecret: secret, fetch: fetcher, ...options }) };
}

test('ordinary incoming transport retains frozen billing reference in context and bound recovery', async () => {
  const billing = { version: 1, bindingId, callSid, termsDigest: 'a'.repeat(64) };
  const h = harness([json({ ...call, billing }), json({ ...bound, billing }), json({ ...call, billing })]);
  assert.deepEqual((await h.client.openInboundCall!(begin.providerProof, approvalRef)).billing, billing);
  const recovered = await h.client.recoverInbound!(begin);
  assert.equal(recovered.state, 'bound'); if (recovered.state === 'bound') assert.deepEqual(recovered.billing, billing);
  assert.deepEqual((await h.client.getContext(bindingId, callSid)).billing, billing);
  for (const changed of [undefined, { ...billing, termsDigest: 'b'.repeat(64) }, { ...billing, bindingId: foreignId }]) {
    const contextChange = harness([json({ ...call, billing }), json({ ...call, billing: changed })]);
    await contextChange.client.openInboundCall!(begin.providerProof, approvalRef);
    await assert.rejects(contextChange.client.getContext(bindingId, callSid), code('identity_mismatch'));
    const recoveryChange = harness([json({ ...bound, billing }), json({ ...bound, billing: changed })]);
    await recoveryChange.client.recoverInbound!(begin);
    await assert.rejects(recoveryChange.client.recoverInbound!(begin), code('identity_mismatch'));
  }
});

test('exact shared begin identity includes signed values but excludes signature and normalizes only broker UUID', () => {
  assert.equal(inboundProofDigest(begin.providerProof), 'f355b816e711ba068e91ca96a4962c7d412d6bfaedeba9244b5d740505aeb448');
  assert.equal(beginRef.digest, 'be3c70c5048366b0b0a078a00c36c77fc04dd6345f608a9ca90463c05be14ecf');
  assert.deepEqual(inboundBeginRef({ ...begin, brokerCallId: brokerCallId.toUpperCase(), providerProof: {
    ...begin.providerProof, signature: 'different-synthetic-signature',
    params: Object.fromEntries(Object.entries(begin.providerProof.params).reverse()),
  } }), beginRef);
  for (const providerProof of [
    { ...begin.providerProof, url: `${begin.providerProof.url}?changed=1` },
    { ...begin.providerProof, params: { ...begin.providerProof.params, CallerName: 'Example snowman' } },
    { ...begin.providerProof, params: { ...begin.providerProof.params, From: '+15550000003' } },
    { ...begin.providerProof, params: { ...begin.providerProof.params, AccountSid: 'AC' + 'a'.repeat(32) } },
  ]) assert.notEqual(inboundBeginRef({ ...begin, providerProof }).digest, beginRef.digest);
});

const sharedRoot = process.env.BROKER_INBOUND_SHARED_WIRE_ROOT;
test('Broker projection crosses all frozen fixtures and actual shared API exports with API Zod 3',
  { skip: !sharedRoot }, () => {
    const zodPath = process.env.BROKER_INBOUND_API_ZOD;
    assert.ok(sharedRoot && zodPath, 'Both pinned shared source and API Zod paths are required');
    const hashes: Record<string, string> = {
      'phoneInboundContract.ts': '7e66b2cb60c6e8b88311b111633c2fd512daffb1d42033afe024f835076a6dde',
      'phoneProofDigest.ts': '3785281081cc16c123ed35f2cccaf84e0c072a88b45cbaaf38e48051da9f5915',
      'phoneProviderProof.ts': 'f9b94eb71a967d8054ed283dc7082033b32f906da1c75a4b70acce70a3e9bcf2',
      'begin-identity.fixture.json': '39904454eb4216266e725886e3a2ed89c35f6e2b2892451ac886caa619686b22',
    };
    const sources = Object.fromEntries(Object.entries(hashes).map(([file, hash]) => {
      const source = fs.readFileSync(path.join(sharedRoot, file), 'utf8');
      assert.equal(createHash('sha256').update(source).digest('hex'), hash, file);
      return [file, source];
    }));
    const require = createRequire(import.meta.url);
    const apiZod = require(zodPath);
    const version = JSON.parse(fs.readFileSync(path.join(path.dirname(zodPath), 'package.json'), 'utf8')).version;
    assert.equal(version, '3.25.76');
    function compile(file: string, modules: Record<string, unknown>) {
      const exports: Record<string, any> = {};
      const output = ts.transpileModule(sources[file], { compilerOptions: {
        target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS,
      } }).outputText;
      new Function('exports', 'require', output)(exports, (id: string) => {
        if (!(id in modules)) throw new Error(`Unexpected frozen module import: ${id}`);
        return modules[id];
      });
      return exports;
    }
    const digest = compile('phoneProofDigest.ts', { 'node:crypto': require('node:crypto') });
    const api = compile('phoneInboundContract.ts', { 'node:crypto': require('node:crypto'), zod: apiZod,
      './phoneProofDigest.js': digest });
    const fixture = JSON.parse(sources['begin-identity.fixture.json']);
    assert.equal(fixture.cases.length, 7);
    for (const item of fixture.cases) {
      const actual = api.inboundBeginRef(api.projectInboundBeginIdentity(item.input));
      assert.deepEqual(actual, item.expected.beginRef, item.name);
      assert.deepEqual(inboundBeginRef(item.input), actual, item.name);
      assert.equal(inboundProofDigest(item.input.providerProof), item.expected.proofDigest, item.name);
      assert.deepEqual(inboundBeginIdentitySchema.parse(item.input), api.inboundBeginSchema.parse(item.input));
    }
  });

test('actual HTTP client crosses pending, approval, context, bound recovery and committed closure without renewing timestamps', async () => {
  const replies = [pending, approved, call, bound, bound, closed, closed, { ...bound, callState: 'closed' }];
  const requests: { url?: string; method?: string; headers: Record<string, string | string[] | undefined>; body: unknown }[] = [];
  const server = createServer(async (req, res) => {
    let text = '';
    for await (const chunk of req) text += chunk;
    requests.push({ url: req.url, method: req.method, headers: req.headers, body: text ? JSON.parse(text) : undefined });
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify(replies.shift()));
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  try {
    const address = server.address();
    assert.ok(address && typeof address !== 'string');
    const client = createPrimaryApiClient({ apiBaseUrl: `http://127.0.0.1:${address.port}`, phoneServiceSecret: secret });
    assert.deepEqual(await client.beginInbound!(begin), pending);
    assert.deepEqual(await client.inboundStatus!(attachmentId), approved);
    assert.deepEqual(await client.openInboundCall!(begin.providerProof, approvalRef), call);
    assert.deepEqual(await client.recoverInbound!(begin), bound);
    assert.deepEqual(await client.cancelInbound!(begin), bound, 'cancel cannot erase a consumed binding');
    assert.deepEqual(await client.closeInbound!(attachmentId, closure), closed);
    assert.deepEqual(await client.closeInbound!(attachmentId, closure), closed);
    assert.deepEqual(await client.inboundStatus!(attachmentId), { ...bound, callState: 'closed' });
    assert.deepEqual(requests.map(r => [r.method, r.url]), [
      ['POST', `${prefix}/inbound-attachments`], ['GET', `${prefix}/inbound-attachments/${attachmentId}`],
      ['POST', `${prefix}/calls`], ['POST', `${prefix}/inbound-attachments/recover`],
      ['POST', `${prefix}/inbound-attachments/cancel`], ['POST', `${prefix}/inbound-attachments/${attachmentId}/close`],
      ['POST', `${prefix}/inbound-attachments/${attachmentId}/close`], ['GET', `${prefix}/inbound-attachments/${attachmentId}`],
    ]);
    assert.deepEqual(requests.map(r => r.body), [begin, undefined,
      { providerProof: begin.providerProof, inboundApprovalRef: approvalRef }, { begin }, { begin }, closure, closure, undefined]);
    const bearer = createHmac('sha256', secret).update('dexter-broker/primary-ingress/v1').digest('hex');
    for (const request of requests) {
      assert.equal(request.headers.authorization, `Bearer ${bearer}`);
      assert.equal(request.headers['x-dexter-binding-id'], undefined);
      assert.equal(request.headers['idempotency-key'], undefined);
    }
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  }
});

test('lost begin response performs no automatic retry and explicit recovery retains the original tuple', async () => {
  const { client, requests } = harness([new Error('private network detail'), json(pending)]);
  await assert.rejects(client.beginInbound!(begin), code('request_failed'));
  assert.equal(requests.length, 1);
  assert.deepEqual(await client.recoverInbound!(begin), pending);
  assert.deepEqual(JSON.parse(String(requests[1].init.body)), { begin });
  assert.equal(requests[1].init.redirect, 'error');
});

test('absent is observational; only cancel installs a nullable-expiry tombstone and retry keeps it', async () => {
  const { client, requests } = harness([json({ state: 'absent', beginRef }), json(cancelled), json(cancelled), json(pending)]);
  assert.deepEqual(await client.recoverInbound!(begin), { state: 'absent', beginRef });
  assert.deepEqual(await client.cancelInbound!(begin), cancelled);
  assert.deepEqual(await client.beginInbound!(begin), cancelled);
  await assert.rejects(client.inboundStatus!(attachmentId), code('identity_mismatch'));
  assert.deepEqual(JSON.parse(String(requests[1].init.body)), { begin });
  assert.equal('reason' in JSON.parse(String(requests[1].init.body)), false);
});

test('lost context response followed by authority refusal still recovers and closes a binding without account context', async () => {
  const { client, requests } = harness([json(approved), new Error('lost response'),
    json({ error: { code: 'caller_unlinked', retryable: false } }, 403), json(bound), json(closed), json(closed)]);
  await client.beginInbound!(begin);
  await assert.rejects(client.openInboundCall!(begin.providerProof, approvalRef), code('request_failed'));
  await assert.rejects(client.openInboundCall!(begin.providerProof, approvalRef), error =>
    code('http_error')(error) && (error as PrimaryApiError).remoteCode === 'caller_unlinked');
  const recovered = await client.recoverInbound!(begin);
  assert.deepEqual(recovered, bound);
  assert.equal('context' in recovered, false);
  assert.deepEqual(await client.closeInbound!(attachmentId, closure), closed);
  assert.deepEqual(await client.closeInbound!(attachmentId, closure), closed);
  assert.equal(requests.length, 6);
});

test('a fresh client can validate and close the durable bound identity without recovering private context', async () => {
  const { client, requests } = harness([json(closed)]);
  assert.deepEqual(await client.closeInbound!(attachmentId, closure), closed);
  assert.equal(requests.length, 1);
  assert.equal(requests[0].url.endsWith(`/inbound-attachments/${attachmentId}/close`), true);
});

test('bound receipt never fabricates context or media completion; /calls has no fabricated consumedAt', async () => {
  const { client } = harness([json(bound), json(call)]);
  assert.deepEqual(await client.beginInbound!(begin), bound);
  const contextual = await client.openInboundCall!(begin.providerProof, approvalRef);
  assert.deepEqual(contextual, call);
  assert.equal('consumedAt' in contextual, false);
  assert.equal('mediaConnected' in contextual, false);
});

test('malformed state unions and any private context in context-free outcomes are rejected', async () => {
  for (const value of [
    { ...pending, context }, { ...approved, ownerId: foreignId }, { ...bound, consumedAt: undefined },
    { ...bound, expiresAt }, { ...pending, approvalRef }, { ...cancelled, expiresAt: undefined },
    { ...cancelled, approvalRef }, { ...bound, approvalRef: { ...approvalRef, attachmentId: foreignId } },
    { ...pending, beginRef: { ...beginRef, extra: true } }, { state: 'absent', beginRef },
  ]) await assert.rejects(harness([json(value)]).client.beginInbound!(begin), code('invalid_response'));
});

test('cancel refuses absent, pending and approved responses and accepts expired', async () => {
  for (const outcome of [{ state: 'absent', beginRef }, pending, approved])
    await assert.rejects(harness([json(outcome)]).client.cancelInbound!(begin), code('invalid_response'));
  assert.deepEqual(await harness([json(expired)]).client.cancelInbound!(begin), expired);
});

test('begin and recovery reject changed Broker, CallSid or digest identity', async () => {
  for (const badRef of [
    { ...beginRef, brokerCallId: foreignId }, { ...beginRef, callSid: 'CA' + '5'.repeat(32) },
    { ...beginRef, digest: '9'.repeat(64) },
  ]) {
    await assert.rejects(harness([json({ ...pending, beginRef: badRef })]).client.beginInbound!(begin), code('identity_mismatch'));
    await assert.rejects(harness([json({ state: 'absent', beginRef: badRef })]).client.recoverInbound!(begin), code('identity_mismatch'));
  }
});

test('lost-response status lookup still verifies known begin digest and requested attachment ID', async () => {
  for (const bad of [{ ...pending, beginRef: { ...beginRef, digest: '9'.repeat(64) } },
    { ...pending, attachmentId: foreignId }]) {
    const { client } = harness([new Error('lost'), json(bad)]);
    await assert.rejects(client.beginInbound!(begin), code('request_failed'));
    await assert.rejects(client.inboundStatus!(attachmentId), code('identity_mismatch'));
  }
});

test('receipt identity, approval, expiry and consumedAt cannot change across exact recovery', async () => {
  const cases: [InboundOutcome, unknown][] = [
    [pending, { ...pending, attachmentId: foreignId }],
    [pending, { ...pending, expiresAt: closedAt }],
    [approved, { ...approved, approvalRef: { ...approvalRef, digest: '9'.repeat(64) } }],
    [approved, pending], [bound, pending],
    [bound, { ...bound, bindingId: foreignId }],
    [bound, { ...bound, consumedAt: closedAt }],
    [bound, { state: 'absent', beginRef }],
    [expired, pending], [cancelled, expired],
    [cancelled, { ...cancelled, cancelledAt: consumedAt }],
    [{ ...bound, callState: 'closed' }, bound],
  ];
  for (const [first, changed] of cases) {
    const { client } = harness([json(first), json(changed)]);
    await client.beginInbound!(begin);
    await assert.rejects(client.recoverInbound!(begin), code('identity_mismatch'));
  }
});

test('changing saved original proof cannot be hidden behind an unchanged proof digest', async () => {
  for (const change of [
    { ...begin, providerProof: { ...begin.providerProof, signature: 'changed' } },
    { ...begin, providerProof: { ...begin.providerProof, params: { ...begin.providerProof.params, CallerName: 'changed' } } },
  ]) {
    const { client, requests } = harness([json(pending)]);
    await client.beginInbound!(begin);
    await assert.rejects(client.recoverInbound!(change), code('identity_mismatch'));
    assert.equal(requests.length, 1);
  }
});

test('uppercase Broker UUID and parameter-order replay preserve all signed values and one begin identity', async () => {
  const reordered = { ...begin, brokerCallId: brokerCallId.toUpperCase(), providerProof: { ...begin.providerProof,
    params: Object.fromEntries(Object.entries(begin.providerProof.params).reverse()) } };
  const { client, requests } = harness([json(pending), json(pending)]);
  await client.beginInbound!(reordered);
  await client.recoverInbound!(begin);
  const sent = JSON.parse(String(requests[0].init.body));
  assert.equal(sent.brokerCallId, brokerCallId);
  assert.deepEqual(sent.providerProof, reordered.providerProof);
});

test('same CallSid cannot acquire a second attachment or Broker identity in this client', async () => {
  const otherBegin = { ...begin, brokerCallId: foreignId };
  const { client } = harness([json(pending), json({ ...pending, attachmentId: foreignId, beginRef: inboundBeginRef(otherBegin) })]);
  await client.beginInbound!(begin);
  await assert.rejects(client.beginInbound!(otherBegin), code('identity_mismatch'));
});

test('inbound context is strict and cannot include outbound assignment or mismatch known binding', async () => {
  for (const bad of [
    { ...call, callSid: 'CA' + '5'.repeat(32) }, { ...call, bindingId: foreignId },
  ]) {
    const { client } = harness([json(bound), json(bad)]);
    await client.beginInbound!(begin);
    await assert.rejects(client.openInboundCall!(begin.providerProof, approvalRef), code('identity_mismatch'));
  }
  for (const bad of [{ ...call, consumedAt }, { ...call, context: { ...context, ownerId: foreignId } },
    { ...call, callAssignmentRef: { assignmentId: foreignId, revision: 1, digest: '5'.repeat(64) } }])
    await assert.rejects(harness([json(bad)]).client.openInboundCall!(begin.providerProof, approvalRef), code('invalid_response'));
});

test('changed approval, original proof or terminal attachment refuses consumption before HTTP', async () => {
  for (const first of [approved, cancelled, expired]) {
    const { client, requests } = harness([json(first)]);
    await client.beginInbound!(begin);
    await assert.rejects(client.openInboundCall!(begin.providerProof,
      first.state === 'approved' ? { ...approvalRef, digest: '9'.repeat(64) } : approvalRef), code('identity_mismatch'));
    assert.equal(requests.length, 1);
  }
  const { client, requests } = harness([json(approved)]);
  await client.beginInbound!(begin);
  await assert.rejects(client.openInboundCall!({ ...begin.providerProof, signature: 'changed' }, approvalRef), code('identity_mismatch'));
  assert.equal(requests.length, 1);
});

test('closure requires exact path, begin, approval, binding and SID, retaining first closedAt', async () => {
  for (const fields of [
    { attachmentId: foreignId }, { bindingId: foreignId },
    { approvalRef: { ...approvalRef, digest: '9'.repeat(64) } },
    { beginRef: { ...beginRef, digest: '9'.repeat(64) } },
  ]) await assert.rejects(harness([json({ ...closed, ...fields })]).client.closeInbound!(attachmentId, closure),
    fields.attachmentId ? code('invalid_response') : code('identity_mismatch'));
  await assert.rejects(harness([json({ ...closed, callSid: 'CA' + '5'.repeat(32) })]).client.closeInbound!(attachmentId, closure), code('invalid_response'));
  await assert.rejects(harness([json({ ...closed, context })]).client.closeInbound!(attachmentId, closure), code('invalid_response'));
  const { client, requests } = harness([json(closed), json({ ...closed, closedAt: expiresAt }), json(bound)]);
  await client.closeInbound!(attachmentId, closure);
  await assert.rejects(client.closeInbound!(attachmentId, closure), code('identity_mismatch'));
  await assert.rejects(client.recoverInbound!(begin), code('identity_mismatch'));
  await assert.rejects(client.closeInbound!(foreignId, closure), code('identity_mismatch'));
  assert.equal(requests.length, 3);
});

test('known foreign binding or approval never reaches the close endpoint', async () => {
  for (const changed of [{ ...closure, bindingId: foreignId },
    { ...closure, approvalRef: { ...approvalRef, approvalId: foreignId } }]) {
    const { client, requests } = harness([json(bound)]);
    await client.beginInbound!(begin);
    await assert.rejects(client.closeInbound!(attachmentId, changed), code('identity_mismatch'));
    assert.equal(requests.length, 1);
  }
});

test('mutating returned wire objects cannot rewrite retained identity or deadlines', async () => {
  const { client } = harness([json(approved), json(bound), json(closed), json(closed)]);
  const first = await client.beginInbound!(begin);
  first.beginRef.digest = '9'.repeat(64);
  if (first.state === 'approved') first.approvalRef.digest = '8'.repeat(64);
  assert.deepEqual(await client.recoverInbound!(begin), bound);
  const receipt = await client.closeInbound!(attachmentId, closure);
  receipt.closedAt = expiresAt;
  receipt.approvalRef.digest = '9'.repeat(64);
  assert.deepEqual(await client.closeInbound!(attachmentId, closure), closed);
});

test('a late private-context response cannot overwrite closure recovered while /calls was pending', async () => {
  let deliverContext!: (response: Response) => void;
  let contextStarted!: () => void;
  const started = new Promise<void>(resolve => { contextStarted = resolve; });
  const client = createPrimaryApiClient({ apiBaseUrl: 'https://api.example.test', phoneServiceSecret: secret,
    fetch: async url => {
      if (String(url).endsWith('/calls')) {
        contextStarted();
        return new Promise<Response>(resolve => { deliverContext = resolve; });
      }
      if (String(url).endsWith('/close')) return json(closed);
      return json({ ...bound, callState: 'closed' });
    } });
  const opening = client.openInboundCall!(begin.providerProof, approvalRef);
  await started;
  await client.closeInbound!(attachmentId, closure);
  deliverContext(json(call));
  await assert.rejects(opening, code('identity_mismatch'));
  assert.deepEqual(await client.recoverInbound!(begin), { ...bound, callState: 'closed' });
});

test('concurrent closure responses cannot change the first committed receipt timestamp', async () => {
  let deliverFirst!: (response: Response) => void;
  let firstStarted!: () => void;
  const started = new Promise<void>(resolve => { firstStarted = resolve; });
  let count = 0;
  const client = createPrimaryApiClient({ apiBaseUrl: 'https://api.example.test', phoneServiceSecret: secret,
    fetch: async () => {
      count++;
      if (count === 1) {
        firstStarted();
        return new Promise<Response>(resolve => { deliverFirst = resolve; });
      }
      return json(closed);
    } });
  const first = client.closeInbound!(attachmentId, closure);
  await started;
  assert.deepEqual(await client.closeInbound!(attachmentId, closure), closed);
  deliverFirst(json({ ...closed, closedAt: expiresAt }));
  await assert.rejects(first, code('identity_mismatch'));
  assert.deepEqual(await client.closeInbound!(attachmentId, closure), closed);
});

test('invalid identities and unsupported transport URLs refuse before network', async () => {
  const invalid: unknown[] = [
    { ...begin, extra: true }, { ...begin, brokerCallId: 'not-a-uuid' },
    ...['http://broker.example.test/twilio/inbound', `${begin.providerProof.url}?extra=1`,
      `${begin.providerProof.url}#fragment`, 'https://user:pass@broker.example.test/twilio/inbound'].map(url =>
      ({ ...begin, providerProof: { ...begin.providerProof, url } })),
    ...['', 'x'.repeat(129)].map(signature => ({ ...begin, providerProof: { ...begin.providerProof, signature } })),
    ...[{ Direction: 'outbound-api' }, { AccountSid: 'CA' + '1'.repeat(32) }, { CallSid: 'bad' },
      { From: 'anonymous' }, { To: '123' }].map(params => ({ ...begin,
      providerProof: { ...begin.providerProof, params: { ...begin.providerProof.params, ...params } } })),
  ];
  for (const input of invalid) {
    const { client, requests } = harness([]);
    await assert.rejects(client.beginInbound!(input as InboundBeginIdentity), code('invalid_request'));
    assert.equal(requests.length, 0);
  }
});

test('inbound envelopes keep the ordinary 24KiB cap including valid large signed parameter sets', async () => {
  const large = structuredClone(begin);
  for (let i = 0; i < 13; i++) large.providerProof.params[`Extra${i}`] = 'x'.repeat(2048);
  assert.ok(inboundBeginIdentitySchema.safeParse(large).success);
  const { client, requests } = harness([]);
  await assert.rejects(client.beginInbound!(large), code('invalid_request'));
  await assert.rejects(client.openInboundCall!(large.providerProof, approvalRef), code('invalid_request'));
  await assert.rejects(client.closeInbound!(attachmentId, { ...closure, begin: large }), code('invalid_request'));
  assert.equal(requests.length, 0);
});

test('HTTP errors, redirects, wrong success status and oversize response do not become absent or successful closure', async () => {
  for (const [response, expected] of [
    [json({ error: { code: 'cleanup_unavailable', retryable: true } }, 503), 'http_error'],
    [new Response(null, { status: 302, headers: { Location: 'https://foreign.example.test' } }), 'http_error'],
    [json(closed, 201), 'invalid_response'],
    [new Response('x'.repeat(65537)), 'response_too_large'],
  ] as const) {
    const { client, requests } = harness([response, json(closed)]);
    await assert.rejects(client.closeInbound!(attachmentId, closure), code(expected));
    assert.equal(requests.length, 1);
    assert.deepEqual(await client.closeInbound!(attachmentId, closure), closed);
  }
});

test('request timeout is one attempt and remains recoverable by the exact original begin', async () => {
  let count = 0;
  const client = createPrimaryApiClient({ apiBaseUrl: 'https://api.example.test', phoneServiceSecret: secret,
    timeoutMs: 10, fetch: async () => { count++; if (count === 1) return new Promise<Response>(() => {}); return json(bound); } });
  await assert.rejects(client.beginInbound!(begin), code('request_timeout'));
  assert.equal(count, 1);
  assert.deepEqual(await client.recoverInbound!(begin), bound);
});

test('strict outcome validation keeps pending approval separate from provider/media state', () => {
  assert.ok(inboundOutcomeSchema.safeParse(cancelled).success);
  for (const field of ['providerStatus', 'mediaConnected', 'heard', 'context'])
    assert.equal(inboundOutcomeSchema.safeParse({ ...approved, [field]: true }).success, false);
});
