import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { pathToFileURL } from 'node:url';
import { createHmac, randomUUID } from 'node:crypto';
import Twilio from 'twilio';
import { createBrokerServer } from '../src/broker/server.js';
import { createBrokerSmsBridge } from '../src/broker/sms.js';
import { createResultSmsClient, type ResultSmsClient } from '../src/broker/resultSmsClient.js';
import { dispatchResultSms } from '../src/broker/resultSmsDispatch.js';
import { phoneResultSmsIntentDigest, phoneResultSmsPayload, phoneResultSmsResolvedSchema,
  type PhoneResultSmsIdentity, type PhoneResultSmsResolved } from '../src/broker/resultSmsContract.js';
import { PrimaryApiError, type PrimaryApiClient } from '../src/broker/primaryApi.js';
import type { BrokerConfig } from '../src/broker/config.js';
import { SmsService, SMS_DISCLOSURE_VERSION } from '../src/sms/SmsService.js';

const PHONE = '+12025550123', SENDER = '+12025550124', CALL = 'CA' + '1'.repeat(32);
const SERVICE = 'MG' + '2'.repeat(32), MESSAGE = 'SM' + '3'.repeat(32), ACCOUNT = 'AC' + '4'.repeat(32);
const BASE = 'https://phone.example.test', TOKEN = 'fixture-webhook', CLIENT = 'a'.repeat(64);
function value(expiresAt = new Date(Date.now() + 60_000).toISOString()): PhoneResultSmsResolved {
  const identity: PhoneResultSmsIdentity = { v: 'phone.result-sms.v1', intentId: randomUUID(), bindingId: randomUUID(),
    callSid: CALL, requestId: randomUUID(), runId: randomUUID(), ownerPrincipalId: randomUUID(), primaryDexterId: randomUUID(),
    threadId: randomUUID(), phoneNumber: PHONE, source: { callerIntentOperationId: randomUUID(), sourceEventId: randomUUID(),
      sourceItemId: 'original-caller-turn', sourceRevision: 1, sourceDigest: 'b'.repeat(64) },
    admittedAt: new Date(Date.now() - 60_000).toISOString(), expiresAt };
  return { identity, intentDigest: phoneResultSmsIntentDigest(identity), payload: phoneResultSmsPayload(identity,
    { eventId: randomUUID(), messageId: randomUUID(), sequence: '12', contentDigest: 'c'.repeat(64) }) };
}
const ref = (v: PhoneResultSmsResolved) => ({ v: v.identity.v, intentId: v.identity.intentId,
  intentDigest: v.intentDigest, messageId: v.payload.messageId });
function deferred<T>() { let resolve!: (v: T) => void; const promise = new Promise<T>(r => { resolve = r; }); return { promise, resolve }; }

async function fixture(t: test.TestContext, resolved = value()) {
  // The durable-state validator rejects temporary roots, so use a directory under the user's state home.
  const stateHome = path.join(os.homedir(), '.local/state');
  fs.mkdirSync(stateHome, { recursive: true });
  const dir = fs.mkdtempSync(path.join(stateHome, 'broker-result-sms-test-'));
  const statePath = path.join(dir, 'sms.json');
  const old = new SmsService({ statePath, enrollmentEnabled: true, sendEnabled: false, campaignApproved: true,
    advancedOptOutConfigured: true, messagingServiceSid: SERVICE, publicBaseUrl: BASE,
    provider: { async startVerification() {}, async checkVerification() { return true; }, async send() { throw Error('unexpected'); } } });
  const challenge = await old.start(PHONE, CLIENT), proof = await old.check(challenge.challengeId, '123456', CLIENT);
  await old.enroll(proof.proofToken, CLIENT, true, SMS_DISCLOSURE_VERSION); old.close();
  const sends: Array<{ to: string; body: string; statusCallback: string }> = [];
  let send = async () => ({ sid: MESSAGE, status: 'queued' });
  let identityReads = 0, admissions = 0, contextReads = 0, closed = false;
  const client: ResultSmsClient = {
    async resolveIdentity() { identityReads++; return resolved; },
    async admitDispatch() { admissions++; return resolved; },
  };
  const env: NodeJS.ProcessEnv = { DEXTER_SMS_STATE_PATH: statePath, DEXTER_SMS_INTERNAL_SECRET: 'i'.repeat(40),
    DEXTER_SMS_ENROLLMENT_ENABLED: 'true', DEXTER_SMS_SEND_ENABLED: 'true', DEXTER_SMS_CAMPAIGN_APPROVED: 'true',
    DEXTER_SMS_ADVANCED_OPT_OUT_CONFIGURED: 'true', TWILIO_VERIFY_SID: 'VA' + '5'.repeat(32), TWILIO_ACCOUNT_SID: ACCOUNT,
    TWILIO_AUTH_TOKEN: TOKEN, TWILIO_PHONE_NUMBER: SENDER, TWILIO_MESSAGING_SERVICE_SID: SERVICE, DEXTER_PHONE_PUBLIC_URL: BASE };
  const clients = { verify: {}, messaging: { messages: { async create(input: typeof sends[number]) {
    sends.push(input); return send();
  } } } } as unknown as { verify: ReturnType<typeof Twilio>; messaging: ReturnType<typeof Twilio> };
  const config: BrokerConfig = { host: '127.0.0.1', port: 0, publicOrigin: BASE, stateDir: path.join(dir, 'calls'),
    controlToken: 't'.repeat(40), apiKey: 'fixture', model: 'fixture', voice: 'marin',
    twilioAccountSid: ACCOUNT, twilioAuthToken: TOKEN, from: SENDER, allowedTo: [PHONE],
    apiBaseUrl: 'http://127.0.0.1:1', phoneServiceSecret: 'p'.repeat(40), maxCallSeconds: 60 };
  let current = { bindingId: resolved.identity.bindingId, callSid: CALL, state: 'open' as const,
    context: { primaryDexterId: resolved.identity.primaryDexterId, threadId: resolved.identity.threadId, displayName: 'Fixture', messages: [] } };
  const primary = { async getContext() { contextReads++; return current; } } as unknown as PrimaryApiClient;
  const sms = await createBrokerSmsBridge(env, path.join(dir, 'release'), clients); assert.ok(sms);
  const broker = createBrokerServer({ config, sms, resultSms: client, primary });
  await new Promise<void>(r => broker.server.listen(0, '127.0.0.1', r));
  const origin = `http://127.0.0.1:${(broker.server.address() as { port: number }).port}`;
  const post = (body: unknown = ref(resolved), suffix = '', token = config.controlToken) => fetch(origin + '/internal/broker/result-sms' + suffix,
    { method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' }, body: JSON.stringify(body) });
  const webhook = (route: string, body: Record<string, string>) => fetch(origin + route, { method: 'POST', headers: {
    'content-type': 'application/x-www-form-urlencoded', 'x-twilio-signature': Twilio.getExpectedTwilioSignature(TOKEN, BASE + route, body),
  }, body: new URLSearchParams(body) });
  const close = async () => { if (!closed) { closed = true; await broker.close(); } };
  t.after(async () => { await close(); fs.rmSync(dir, { recursive: true, force: true }); });
  return { dir, resolved, sms, broker, env, clients, primary, post, webhook, sends, close, statePath,
    reads: () => ({ identityReads, admissions, contextReads }), setSend(fn: typeof send) { send = fn; }, client,
    eligibility: { bindingId: resolved.identity.bindingId, callSid: CALL, primaryDexterId: resolved.identity.primaryDexterId, threadId: resolved.identity.threadId },
    setContext(fn: PrimaryApiClient['getContext']) { primary.getContext = fn; },
    activateCall() {
      const call = broker.store.reserve({ requestId: randomUUID(), direction: 'outbound', purpose: 'Fixture', number: PHONE, callSid: CALL }).call;
      broker.store.recordPrimaryBinding(call.id, current);
      return broker.store.update(call.id, { status: 'in-progress', mediaStartedAt: new Date().toISOString() });
    },
  };
}

test('deferred HTTP send works after hangup without an active call; exact receipt survives STOP and revoked authority', async t => {
  const f = await fixture(t);
  assert.equal(f.broker.store.list().length, 0);
  const sent = await f.post(); assert.equal(sent.status, 200);
  assert.deepEqual(await sent.json(), { ok: true, messageId: f.resolved.payload.messageId, status: 'queued', duplicate: false });
  assert.equal(f.sends.length, 1); assert.equal(f.sends[0].to, PHONE);
  assert.match(f.sends[0].body, /Your requested answer is ready: https:\/\/dexter.cash\/wallet\/dexter\?/);
  assert.equal((await f.webhook('/sms/incoming', { AccountSid: ACCOUNT, MessageSid: 'SM' + '6'.repeat(32), From: PHONE,
    To: SENDER, Body: 'STOP', OptOutType: 'STOP' })).status, 200);
  f.client.admitDispatch = async () => { throw Error('revoked authority must not be checked for recovery'); };
  assert.deepEqual(await (await f.post()).json(), { ok: true, messageId: f.resolved.payload.messageId, status: 'queued', duplicate: true });
  assert.equal(f.sends.length, 1); assert.equal(f.reads().admissions, 1);
});

test('delayed original and simultaneous retries reserve one durable message before carrier I/O', async t => {
  const f = await fixture(t), entered = deferred<void>(), release = deferred<{ sid: string; status: string }>();
  f.setSend(async () => { entered.resolve(); return release.promise; });
  const original = f.post(); await entered.promise;
  const disk = JSON.parse(fs.readFileSync(f.statePath, 'utf8'));
  assert.equal(disk.messages[0].status, 'dispatching'); assert.equal(disk.messages[0].id, f.resolved.payload.messageId);
  const retries = await Promise.all(Array.from({ length: 8 }, async () => (await f.post()).json()));
  for (const retry of retries) assert.deepEqual(retry, { ok: true, messageId: f.resolved.payload.messageId, status: 'dispatching', duplicate: true });
  assert.equal(f.sends.length, 1);
  release.resolve({ sid: MESSAGE, status: 'queued' }); await original;
});

test('uncertain send is recovered after restart with provider flags disabled and no second send', async t => {
  const f = await fixture(t);
  f.setSend(async () => { throw Error('carrier timed out after accepting'); });
  assert.equal((await (await f.post()).json() as any).status, 'uncertain');
  await f.close();
  f.env.DEXTER_SMS_SEND_ENABLED = 'false';
  const next = await createBrokerSmsBridge(f.env, path.join(f.dir, 'release'), f.clients); assert.ok(next);
  try {
    const result = await dispatchResultSms(ref(f.resolved), { sms: next, accepting: () => false, client: {
      resolveIdentity: async () => f.resolved, admitDispatch: async () => { throw Error('must not admit'); },
    } });
    assert.deepEqual(result, { ok: true, messageId: f.resolved.payload.messageId, status: 'uncertain', duplicate: true });
    assert.equal(f.sends.length, 1);
  } finally { await next.close(); }
});

test('process exit during carrier I/O leaves a durable dispatching record that restarts as uncertain', async t => {
  const f = await fixture(t); await f.close();
  const modulePath = pathToFileURL(path.resolve('src/sms/SmsService.ts')).href;
  const code = `import { SmsService } from ${JSON.stringify(modulePath)};
    const service = new SmsService({ statePath: ${JSON.stringify(f.statePath)}, enrollmentEnabled: false,
      sendEnabled: true, campaignApproved: true, advancedOptOutConfigured: true,
      messagingServiceSid: ${JSON.stringify(SERVICE)}, publicBaseUrl: ${JSON.stringify(BASE)},
      provider: { async startVerification() {}, async checkVerification() { return false; },
        async send() { process.exit(17); } } });
    await service.sendRequested(${JSON.stringify(PHONE)}, ${JSON.stringify(CALL)}, ${JSON.stringify(f.resolved.payload.message)});`;
  const child = spawn(process.execPath, ['--import', 'tsx', '--input-type=module', '-e', code], { stdio: ['ignore', 'ignore', 'pipe'] });
  t.after(() => { if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL'); });
  const [exitCode] = await once(child, 'exit'); assert.equal(exitCode, 17);
  assert.equal(JSON.parse(fs.readFileSync(f.statePath, 'utf8')).messages[0].status, 'dispatching');
  const next = await createBrokerSmsBridge(f.env, path.join(f.dir, 'release'), f.clients); assert.ok(next);
  try {
    const recovered = await dispatchResultSms(ref(f.resolved), { sms: next, client: f.client, accepting: () => true });
    assert.deepEqual(recovered, { ok: true, messageId: f.resolved.payload.messageId, status: 'uncertain', duplicate: true });
    assert.equal(f.sends.length, 0); assert.equal(f.reads().admissions, 0);
  } finally { await next.close(); }
});

test('a receipt created during a delayed admission wins over a later authority refusal', async t => {
  const f = await fixture(t), entered = deferred<void>(), release = deferred<void>();
  f.client.admitDispatch = async () => { entered.resolve(); await release.promise;
    throw new PrimaryApiError('http_error', 409, { code: 'sms_authority_revoked', retryable: false }); };
  const delayed = f.post(); await entered.promise;
  await f.sms.sendRequested(PHONE, CALL, f.resolved.payload.message);
  release.resolve();
  assert.deepEqual(await (await delayed).json(), { ok: true, messageId: f.resolved.payload.messageId, status: 'queued', duplicate: true });
  assert.equal(f.sends.length, 1);
});

test('expired intent with no receipt cannot send, even when the API admission stub would allow it', async t => {
  const f = await fixture(t, value(new Date(Date.now() - 1).toISOString()));
  assert.deepEqual(await (await f.post()).json(), { ok: false, error: 'sms_intent_expired' });
  assert.equal(f.reads().admissions, 0); assert.equal(f.sends.length, 0);
});

test('existing receipt remains recoverable after the frozen first-send deadline', async t => {
  const f = await fixture(t, value(new Date(Date.now() - 1).toISOString()));
  await f.sms.sendRequested(PHONE, CALL, f.resolved.payload.message);
  assert.equal((await (await f.post()).json() as any).duplicate, true);
  assert.equal(f.reads().admissions, 0); assert.equal(f.sends.length, 1);
});

test('expiry or shutdown during current-authority lookup refuses an otherwise new send', async t => {
  for (const reason of ['expiry', 'shutdown']) {
    const f = await fixture(t), entered = deferred<void>(), release = deferred<void>();
    let now = Date.now(), accepting = true;
    f.client.admitDispatch = async () => { entered.resolve(); await release.promise; return f.resolved; };
    const pending = dispatchResultSms(ref(f.resolved), { sms: f.sms, client: f.client, accepting: () => accepting, now: () => now });
    await entered.promise;
    if (reason === 'expiry') now = Date.parse(f.resolved.identity.expiresAt); else accepting = false;
    release.resolve();
    assert.deepEqual(await pending, { ok: false, error: reason === 'expiry' ? 'sms_intent_expired' : 'sms_unavailable' });
    assert.equal(f.sends.length, 0);
  }
});

test('new send rechecks STOP and current API authority; identity mismatch never reaches the carrier', async t => {
  const f = await fixture(t);
  f.client.admitDispatch = async () => { throw new PrimaryApiError('http_error', 409, { code: 'sms_authority_revoked', retryable: false }); };
  assert.deepEqual(await (await f.post()).json(), { ok: false, error: 'sms_authority_revoked' });
  f.client.admitDispatch = async () => value();
  assert.deepEqual(await (await f.post()).json(), { ok: false, error: 'sms_unavailable' });
  f.client.admitDispatch = async () => f.resolved;
  await f.webhook('/sms/incoming', { AccountSid: ACCOUNT, MessageSid: 'SM' + '7'.repeat(32), From: PHONE,
    To: SENDER, Body: 'STOP', OptOutType: 'STOP' });
  assert.deepEqual(await (await f.post()).json(), { ok: false, error: 'carrier_opted_out' });
  assert.equal(f.sends.length, 0);
});

test('control auth and strict reference reject destination, text and identity injection', async t => {
  const f = await fixture(t);
  assert.equal((await f.post(ref(f.resolved), '', 'wrong')).status, 401);
  for (const extra of [{ to: PHONE }, { message: 'send this instead' }, { messageId: '0'.repeat(64) }, { intentId: randomUUID() }]) {
    const result = await (await f.post({ ...ref(f.resolved), ...extra })).json() as any;
    assert.equal(result.ok, false);
  }
  assert.equal(f.sends.length, 0);
});

test('eligibility requires a current private call and uses its saved number, without sending or reserving', async t => {
  const f = await fixture(t);
  assert.equal((await f.post(f.eligibility, '/eligibility')).status, 409);
  const call = f.activateCall();
  const before = fs.readFileSync(f.statePath);
  assert.deepEqual(await (await f.post(f.eligibility, '/eligibility')).json(), { ok: true });
  assert.deepEqual(fs.readFileSync(f.statePath), before); assert.equal(f.sends.length, 0);
  assert.equal((await f.post({ ...f.eligibility, phoneNumber: SENDER }, '/eligibility')).status, 400);
  const entered = deferred<void>(), release = deferred<void>();
  f.setContext(async () => { entered.resolve(); await release.promise; return { ...f.eligibility, state: 'open',
    context: { primaryDexterId: f.eligibility.primaryDexterId, threadId: f.eligibility.threadId, displayName: 'Fixture', messages: [] } }; });
  const pending = f.post(f.eligibility, '/eligibility'); await entered.promise;
  f.broker.store.update(call.id, { status: 'completed', endedAt: new Date().toISOString() }); release.resolve();
  assert.equal((await pending).status, 409);
});

test('Broker shutdown waits for the admitted carrier request and durable receipt before releasing the ledger', async t => {
  const f = await fixture(t), entered = deferred<void>(), release = deferred<{ sid: string; status: string }>();
  f.setSend(async () => { entered.resolve(); return release.promise; });
  const request = f.post(); await entered.promise;
  let finished = false; const closing = f.close().then(() => { finished = true; });
  await new Promise(r => setTimeout(r, 20)); assert.equal(finished, false);
  assert.equal(fs.existsSync(f.statePath + '.lock'), true);
  release.resolve({ sid: MESSAGE, status: 'queued' }); await request; await closing;
  assert.equal(fs.existsSync(f.statePath + '.lock'), false);
  assert.equal(JSON.parse(fs.readFileSync(f.statePath, 'utf8')).messages[0].status, 'queued');
});

test('client binds both HTTP operations to the strict frozen reference and standard Phone bearer', async () => {
  const v = value(), paths: string[] = [], secret = 's'.repeat(40);
  const client = createResultSmsClient({ apiBaseUrl: 'https://api.example.test', phoneServiceSecret: secret,
    fetch: async (url, init) => {
      paths.push(String(url)); assert.equal(init?.method, 'POST'); assert.equal(init?.redirect, 'error');
      assert.deepEqual(JSON.parse(init?.body as string), ref(v));
      assert.equal((init?.headers as any).Authorization, 'Bearer ' + createHmac('sha256', secret).update('dexter-broker/primary-ingress/v1').digest('hex'));
      return Response.json(v);
    } });
  assert.deepEqual(await client.resolveIdentity(ref(v)), v); assert.deepEqual(await client.admitDispatch(ref(v)), v);
  assert.deepEqual(paths.map(url => new URL(url).pathname), ['/internal/primary-dexter/channels/phone/result-sms/identity', '/internal/primary-dexter/channels/phone/result-sms/dispatch']);
});

test('client rejects forged derived payloads, other intent identities, oversized bodies and stalled responses', async () => {
  const v = value();
  for (const response of [{ ...v, payload: { ...v.payload, href: 'https://evil.example' } }, value()]) {
    const client = createResultSmsClient({ apiBaseUrl: 'https://api.example.test', phoneServiceSecret: 's'.repeat(40), fetch: async () => Response.json(response) });
    await assert.rejects(client.resolveIdentity(ref(v)), (e: unknown) => e instanceof PrimaryApiError && e.code === 'invalid_response');
  }
  for (const raw of ['x'.repeat(9000), undefined]) {
    const client = createResultSmsClient({ apiBaseUrl: 'https://api.example.test', phoneServiceSecret: 's'.repeat(40), timeoutMs: 20,
      fetch: async () => raw ? new Response(raw) : new Promise(() => {}) });
    await assert.rejects(client.admitDispatch(ref(v)), (e: unknown) => e instanceof PrimaryApiError && ['response_too_large', 'request_timeout'].includes(e.code));
  }
  const dirty = structuredClone(v); dirty.payload.answer.sequence = 'not-a-number';
  assert.equal(phoneResultSmsResolvedSchema.safeParse(dirty).success, false);
});
