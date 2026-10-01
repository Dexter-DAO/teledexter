import { inboundBeginRef, type InboundBeginIdentity, type InboundOutcome } from '../src/broker/inboundContract.js';
import { createInboundHoldAdapter } from '../src/broker/inboundHold.js';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import Twilio from 'twilio';
import { WebSocket } from 'ws';
import { createBrokerServer } from '../src/broker/server.js';
import { BrokerStore } from '../src/broker/store.js';
import { streamToken } from '../src/broker/phone.js';
import type { BrokerConfig } from '../src/broker/config.js';
import { createEligibleContextClient, eligiblePhoneCallSchema, type EligiblePhoneCall } from '../src/broker/eligibleContext.js';
import { PrimaryApiError, createPrimaryApiClient, type PrimaryApiClient } from '../src/broker/primaryApi.js';
import type { VerificationCallbackClient } from '../src/broker/verificationCallbackClient.js';
import { phoneVerificationCallbackCommandRef, type PhoneVerificationCallbackPermit, type PhoneVerificationCallbackState,
  type PhoneVerificationCallbackCompletionRecovery } from '../src/broker/verificationCallbackContract.js';
import type { BrokerLiveSessionOptions, LiveTranscriptSnapshot } from '../src/live/types.js';
const id = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const sid = 'CA' + '1'.repeat(32), stream = 'MZ' + '2'.repeat(32), sessionId = 'setup_session';
const ref = { requestId: id(1), requestDigest: 'a'.repeat(64) };
async function until(predicate: () => boolean, label: string) {
  const deadline = Date.now() + 2000;
  while (!predicate()) { if (Date.now() >= deadline) throw new Error(label); await new Promise<void>(resolve => setTimeout(resolve, 5)); }
}
async function fixture(t: test.TestContext, purpose: 'enrollment' | 'account_access' = 'enrollment', enabled = true) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'restricted-broker-server-'));
  const store = new BrokerStore(directory), now = Date.now();
  const config: BrokerConfig = { host: '127.0.0.1', port: 3213, publicOrigin: 'https://broker.example.test', stateDir: directory,
    controlToken: 'c'.repeat(40), apiKey: 'synthetic', model: 'gpt-live-1', voice: 'marin', twilioAccountSid: 'AC' + '3'.repeat(32),
    twilioAuthToken: 'synthetic-token', from: '+15555550001', allowedTo: [], apiBaseUrl: 'http://127.0.0.1:3032', phoneServiceSecret: 'p'.repeat(40), maxCallSeconds: 1800 };
  const permit: PhoneVerificationCallbackPermit = { v: 1, request: ref, purpose, providerAccountSid: config.twilioAccountSid,
    from: config.from, to: '+15555550002', answerUrl: config.publicOrigin + `/broker/verification-callback/${id(1)}`,
    responseUrl: config.publicOrigin + `/broker/verification-callback-response/${id(1)}`, createdAt: new Date(now - 1000).toISOString(),
    expiresAt: new Date(now + 299000).toISOString(), maxDurationSeconds: 180,
    billing: { intentId: id(1), eventId: `phone-verification:${id(1)}`, mandateId: id(2), unitPriceMicro: 1000, freeSeconds: 0 } };
  const counts = { get: 0, dispatch: 0, create: 0, complete: 0, completionGet: 0, privateOpen: 0, ordinarySubmit: 0, ended: 0, cancel: 0, providerFetch: 0 };
  const receipts: unknown[] = [], updates: Array<Record<string, unknown>> = [], voices: BrokerLiveSessionOptions[] = [];
  let remote: PhoneVerificationCallbackState = { v: 1, request: ref, state: 'reserved', permit };
  let completion: PhoneVerificationCallbackCompletionRecovery | undefined;
  let loseCreate = false, loseCompletion = false, dropReceipt = false;
  let receiptGate: Promise<void> | undefined, releaseReceipt: (() => void) | undefined;
  let providerStatus = 'queued';
  let transcript: LiveTranscriptSnapshot = { revision: 0, callerRevision: 0, fragments: [] };
  const providerValue = () => ({ sid, accountSid: config.twilioAccountSid, from: permit.from, to: permit.to,
    direction: 'outbound-api', status: providerStatus, ...(providerStatus === 'completed' ? {
      startTime: new Date(now), endTime: new Date(now + 20000), duration: '19' } : {}) });
  const calls = Object.assign((_sid: string) => ({ async fetch() { counts.providerFetch++; assert.equal(_sid, sid); return providerValue(); },
    async update(input: Record<string, unknown>) { updates.push(input); if (input.status === 'completed') providerStatus = 'completed'; return providerValue(); } }), {
    async create(input: Record<string, unknown>) { counts.create++; assert.equal(input.to, permit.to); assert.equal(input.url, permit.answerUrl);
      assert.ok(store.get(id(1))?.verificationCallback?.dispatch?.providerStartedAt);
      if (loseCreate) throw new Error('lost create'); return providerValue(); },
  });
  const api: VerificationCallbackClient = {
    async prepareAccess() { throw new Error('No original-leg request in this fixture'); },
    async recover() { counts.get++; return structuredClone(remote); },
    async dispatch(input) { counts.dispatch++; remote = { v: 1, request: ref, state: 'dispatching', dispatchAttemptId: input.dispatchAttemptId, expiresAt: permit.expiresAt };
      return { v: 1, state: 'dispatch_granted', dispatchAttemptId: input.dispatchAttemptId, permit }; },
    async answer(input) { assert.deepEqual(store.get(id(1))?.verificationCallback?.answer?.proof, input.providerProof);
      remote = { v: 1, request: ref, state: 'answered', dispatchAttemptId: input.dispatchAttemptId, callSid: sid, purpose, expiresAt: permit.expiresAt }; return remote; },
    async complete(input) { counts.complete++;
      assert.equal(fs.readFileSync(path.join(directory, 'calls.json'), 'utf8').includes('01234567'), false);
      completion = purpose === 'enrollment' ? { v: 1, command: phoneVerificationCallbackCommandRef(input), state: 'enrolled', receipt: {
        enrollmentId: id(50), operationId: id(51), requestDigest: 'e'.repeat(64), state: 'activated', activatedAt: new Date().toISOString(),
        enrollmentRef: { id: id(50), revision: 1, digest: 'f'.repeat(64) }, possessionRef: { id: id(52), digest: '1'.repeat(64) },
        standingConsentRef: { id: id(53), revision: 1, digest: '2'.repeat(64) } } }
        : { v: 1, command: phoneVerificationCallbackCommandRef(input), state: 'admitted', admission: { id: id(9), digest: 'b'.repeat(64), expiresAt: permit.expiresAt } };
      remote = { v: 1, request: ref, state: 'completed', result: completion };
      if (loseCompletion) throw new Error('lost completion'); return completion; },
    async recoverCompletion(command) { counts.completionGet++; return completion ?? { v: 1, command, state: 'absent' }; },
    async cancel() { counts.cancel++; if (remote.state !== 'completed') remote = { v: 1, request: ref, state: 'cancelled', cancelledAt: new Date().toISOString() }; return remote; },
  };
  const legacyPrimary = { async openCall() { throw new Error('Ordinary proof cap must not be used'); },
    async openVerifiedCallbackCall(proof: unknown) { counts.privateOpen++;
      assert.deepEqual(proof, store.get(id(1))?.verificationCallback?.answer?.proof);
      assert.equal(store.get(id(1))?.verificationCallback?.media?.privateRequestedAt !== undefined, true);
      return { bindingId: id(20), callSid: sid, state: 'open', context: { primaryDexterId: id(21), threadId: id(22), displayName: 'Dexter', messages: [] } }; },
    async close() { counts.ended++; return { bindingId: id(20), callSid: sid, state: 'closed' }; },
    async submit() { counts.ordinarySubmit++; throw new Error('No automatic mission on verification'); },
    async captureTurn() { throw new Error('Restricted text must not become Primary memory'); },
  } as unknown as PrimaryApiClient;
  const privateRequests: Array<{ method: string; path: string; body: unknown; version: string | null }> = [];
  const eligibleCall: EligiblePhoneCall = { bindingId: id(20), callSid: sid, state: 'open', context: {
    primaryDexterId: id(21), threadId: id(22), displayName: 'Dexter', messages: [{ id: id(60), role: 'assistant',
      text: 'Your earlier saved research is available.', eventSequence: '1', provenance: {
        messageCreatedAt: new Date(now - 10000).toISOString(), eventId: id(61), eventOccurredAt: new Date(now - 10000).toISOString(), runId: id(62) } }],
    history: { version: { ownerPrincipalId: id(23), primaryDexterId: id(21), threadId: id(22),
      contextEpoch: '1', threadEventSequence: '1', digest: 'd'.repeat(64) }, pageDigest: 'e'.repeat(64),
      localContext: 'Use the saved research when it is relevant.', nextBeforeSequence: null, hasMore: false, omittedForBudget: false } } };
  // Real authenticated Primary/eligible clients and the generation host; only the remote API is faked.
  const privateFetch: typeof fetch = async (url, init) => {
    const route = new URL(String(url)).pathname, headers = new Headers(init?.headers);
    const body: unknown = init?.body === undefined ? undefined : JSON.parse(String(init.body));
    privateRequests.push({ method: init?.method ?? 'GET', path: route, body, version: headers.get('x-dexter-phone-context-version') });
    assert.match(headers.get('authorization') ?? '', /^Bearer [a-f0-9]{64}$/);
    let response: unknown;
    if (route.endsWith('/calls') && init?.method === 'POST') {
      counts.privateOpen++;
      assert.deepEqual(body, { providerProof: store.get(id(1))?.verificationCallback?.answer?.proof });
      assert.ok(store.get(id(1))?.verificationCallback?.media?.privateRequestedAt);
      assert.equal(headers.get('x-dexter-phone-context-version'), 'memory-eligible-v1'); response = eligibleCall;
    } else if (route.endsWith(`/calls/${id(20)}/context`) && init?.method === 'GET') {
      assert.equal(body, undefined); assert.equal(headers.get('x-dexter-phone-context-version'), 'memory-eligible-v1'); response = eligibleCall;
    } else if (route.endsWith(`/calls/${id(20)}/context/assert`) && init?.method === 'POST') {
      assert.equal(headers.get('x-dexter-phone-context-version'), 'memory-eligible-v1');
      assert.deepEqual(body, { version: eligibleCall.context.history.version });
      response = { bindingId: id(20), callSid: sid, current: true, version: eligibleCall.context.history.version };
    } else if (route.endsWith(`/calls/${id(20)}/close`) && init?.method === 'POST') {
      counts.ended++; response = { bindingId: id(20), callSid: sid, state: 'closed' };
    } else { counts.ordinarySubmit++; throw new Error('Unexpected private request before caller delegation'); }
    return new Response(JSON.stringify(response), { status: 200, headers: { 'content-type': 'application/json' } });
  };
  const privateOptions = { apiBaseUrl: config.apiBaseUrl, phoneServiceSecret: config.phoneServiceSecret, fetch: privateFetch };
  const primary = purpose === 'account_access' ? createPrimaryApiClient({ ...privateOptions,
    contextProtocol: { version: 'memory-eligible-v1', parse: value => eligiblePhoneCallSchema.parse(value) } }) : legacyPrimary;
  const broker = createBrokerServer({ config, store, primary,
    ...(purpose === 'account_access' ? { eligibleContext: createEligibleContextClient(privateOptions) } : {}), provider: { calls } as never,
    harkPrimaryReceiptSender: async () => { throw new Error('No Hark receipt'); },
    voiceFactory(options) { voices.push(options); const voiceSessionId = options.restrictedSetupTools ? sessionId : 'private_session';
      queueMicrotask(() => options.onReady({ callId: id(1), sessionId: voiceSessionId }));
      return { appendAudio: () => true, async appendContext() { throw new Error('No context append'); },
        async appendInstructions() { return { eventId: 'instruction', status: 'accepted' }; },
        async appendCommentary() { return { eventId: 'commentary', status: 'accepted' }; },
        async interruptPlayback() { return { eventId: 'interrupt', status: 'accepted' }; }, resumePlayback() {},
        getTranscript: () => options.restrictedSetupTools ? structuredClone(transcript) : { revision: 0, callerRevision: 0, fragments: [] },
        async close(reason) { const result = { callId: id(1), sessionId: voiceSessionId,
          reason: reason ?? 'closed', finalized: true, transcriptPersisted: false, outputAudioDiscarded: false }; options.onClosed(result); return result; } }; },
    ...(enabled ? { restrictedSetup: { model: 'gpt-5-mini', api, receiptTransport: { apiBaseUrl: 'http://127.0.0.1:3032', signingSecret: 'r'.repeat(40),
      fetchImpl: async (url: string | URL | Request, init?: RequestInit) => { receipts.push(JSON.parse(String(init?.body)));
        if (receiptGate) await receiptGate;
        if (dropReceipt) throw new Error('lost signed receipt');
        return String(url).endsWith('/outbound-status') ? new Response(null, { status: 204 })
          : new Response(JSON.stringify({ ok: true, id: id(30), status: 'completed' }), { headers: { 'content-type': 'application/json' } }); } } } } : {}),
  });
  await new Promise<void>(resolve => broker.server.listen(0, '127.0.0.1', resolve));
  const origin = `http://127.0.0.1:${(broker.server.address() as { port: number }).port}`, sockets: WebSocket[] = [];
  t.after(async () => { releaseReceipt?.(); try { for (const ws of sockets) ws.close(); await broker.close(); } finally { store.close(); fs.rmSync(directory, { recursive: true, force: true }); } });
  const post = (route: string, params: Record<string, string>, valid = true) => fetch(origin + route, { method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded', 'x-twilio-signature': valid
      ? Twilio.getExpectedTwilioSignature(config.twilioAuthToken, config.publicOrigin + route, params) : 'wrong' }, body: new URLSearchParams(params) });
  const params = () => ({ AccountSid: config.twilioAccountSid, CallSid: sid, From: permit.from, To: permit.to, Direction: 'outbound-api', CallStatus: providerStatus });
  const kickoff = (body: unknown = ref, authorization = `Bearer ${config.controlToken}`) => fetch(origin + '/internal/broker/verification-callbacks/dispatch',
    { method: 'POST', headers: { 'content-type': 'application/json', authorization }, body: JSON.stringify(body) });
  async function answer() { providerStatus = 'in-progress'; return post(`/broker/verification-callback/${id(1)}`, params()); }
  async function connect(beforeStart?: () => void) {
    const route = `/broker/setup-media/${id(1)}`;
    const ws = new WebSocket(origin.replace('http:', 'ws:') + route, { headers: { 'x-twilio-signature': Twilio.getExpectedTwilioSignature(config.twilioAuthToken, config.publicOrigin + route, {}) } });
    sockets.push(ws); await new Promise<void>((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
    beforeStart?.();
    ws.send(JSON.stringify({ event: 'start', start: { streamSid: stream, accountSid: config.twilioAccountSid, callSid: sid,
      mediaFormat: { encoding: 'audio/x-mulaw', sampleRate: 8000, channels: 1 }, customParameters: { token: streamToken(config.controlToken + ':restricted', id(1), sid) } } }));
    await until(() => !!store.get(id(1))?.verificationCallback?.media?.sessionId, 'setup session starts'); return ws;
  }
  async function connectPrivate() {
    const route = `/broker/media/${id(1)}`;
    const ws = new WebSocket(origin.replace('http:', 'ws:') + route, { headers: { 'x-twilio-signature': Twilio.getExpectedTwilioSignature(config.twilioAuthToken, config.publicOrigin + route, {}) } });
    sockets.push(ws); await new Promise<void>((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
    ws.send(JSON.stringify({ event: 'start', start: { streamSid: 'MZ' + '4'.repeat(32), accountSid: config.twilioAccountSid, callSid: sid,
      mediaFormat: { encoding: 'audio/x-mulaw', sampleRate: 8000, channels: 1 }, customParameters: { token: streamToken(config.controlToken, id(1), sid) } } }));
    await until(() => voices.length === 2 && store.get(id(1))?.liveSessionId === 'private_session', 'private session starts'); return ws;
  }
  async function completeFromSpeech(ws: WebSocket, silentDuringWait = false, newSpeechDuringWait = false) {
    ws.send(JSON.stringify({ event: 'media', streamSid: stream, media: { track: 'inbound', payload: Buffer.alloc(8000, 0).toString('base64') } }));
    await new Promise(resolve => setTimeout(resolve, 20));
    transcript = { revision: 1, callerRevision: 1, fragments: [{ eventId: 'yes', role: 'user', text: 'Yes, continue with verification.', sequence: 1, startMs: 0, endMs: 500 }] };
    const channel = { getContext: () => structuredClone(transcript), subscribeContext: () => () => {}, signal: new AbortController().signal,
      async publish() { throw new Error('No ordinary Primary publication'); } };
    const pending = voices[0].restrictedSetupTools!.execute({ toolCallId: 'setup_tool_1', tool: { name: 'complete_phone_setup', arguments: { accepted: true } },
      request: { callId: id(1), sessionId, requestId: 'request_1', delegationId: 'delegate_1', offsetMs: 600, transcript: structuredClone(transcript) } }, channel);
    if (silentDuringWait || newSpeechDuringWait) ws.send(JSON.stringify({ event: 'media', streamSid: stream, media: { track: 'inbound', payload: Buffer.alloc(1600, newSpeechDuringWait ? 0 : 255).toString('base64') } }));
    return pending;
  }
  return { broker, store, directory, config, permit, counts, receipts, updates, voices, privateRequests, eligibleCall, kickoff, answer, connect, connectPrivate, post, params, completeFromSpeech,
    loseCreate() { loseCreate = true; }, loseCompletion() { loseCompletion = true; }, dropReceipt(value: boolean) { dropReceipt = value; },
    setStatus(value: string) { providerStatus = value; },
    holdReceipt() { receiptGate = new Promise<void>(resolve => { releaseReceipt = resolve; }); },
    releaseReceipt() { releaseReceipt?.(); receiptGate = undefined; } };
}

test('unconfigured executable leaves kickoff absent; configured auth precedes reservation', async t => {
  const disabled = await fixture(t, 'enrollment', false); assert.equal((await disabled.kickoff()).status, 404);
  const f = await fixture(t); assert.equal((await f.kickoff(ref, 'Bearer wrong')).status, 403); assert.equal(f.store.list().length, 0);
  assert.equal((await f.kickoff({ ...ref, to: '+15555550999' })).status, 400); assert.equal(f.counts.create, 0);
});
test('actual HTTP kickoff persists before provider create and exact replay never redials', async t => {
  const f = await fixture(t); assert.equal((await f.kickoff()).status, 200); assert.equal((await f.kickoff()).status, 200);
  assert.equal(f.counts.create, 1); assert.equal(f.counts.dispatch, 1);
  assert.equal(f.store.get(id(1))?.verificationCallback?.provider?.callSid, sid);
  assert.equal((await f.kickoff({ ...ref, requestDigest: 'f'.repeat(64) })).status, 503); assert.equal(f.counts.create, 1);
});
test('unknown provider create remains fenced on HTTP replay and maintenance', async t => {
  const f = await fixture(t); f.loseCreate(); assert.equal((await f.kickoff()).status, 503);
  await f.kickoff(); await f.broker.maintain(); assert.equal(f.counts.create, 1); assert.equal(f.counts.dispatch, 1);
  assert.equal(f.store.get(id(1))?.callSid, undefined);
});
test('signed answer mounts context-free media; spoofed identity and signature cannot replace it', async t => {
  const f = await fixture(t); await f.kickoff(); const answer = await f.answer(); assert.equal(answer.status, 200);
  assert.match(await answer.text(), /setup-media/); const ws = await f.connect();
  assert.deepEqual(f.voices[0].history, []); assert.ok(f.voices[0].restrictedSetupTools); assert.equal(f.voices[0].onEvent, undefined);
  assert.equal(f.counts.privateOpen, 0); assert.equal(f.counts.ordinarySubmit, 0);
  assert.equal(f.store.get(id(1))?.transcript, undefined); assert.equal(f.store.get(id(1))?.primary, undefined);
  for (const params of [{ ...f.params(), CallSid: 'CA' + '9'.repeat(32) }, { ...f.params(), AccountSid: 'AC' + '9'.repeat(32) }])
    assert.equal((await f.post(`/broker/verification-callback/${id(1)}`, params)).status, 403);
  assert.equal((await f.post(`/broker/verification-callback/${id(1)}`, f.params(), false)).status, 403); ws.close();
});
test('ordinary silence during source freeze preserves explicit enrollment; protected Digits never enter Live or disk', async t => {
  const f = await fixture(t); await f.kickoff(); await f.answer(); const ws = await f.connect();
  await f.completeFromSpeech(ws, true); await until(() => f.updates.some(value => String(value.twiml).includes('<Gather')), 'protected capture update');
  assert.match(String(f.updates.find(value => value.twiml)?.twiml), /Enter the eight-digit code/);
  const response = await f.post(`/broker/verification-callback-response/${id(1)}`, { ...f.params(), Digits: '01234567' });
  assert.equal(response.status, 200); assert.equal(f.counts.complete, 1); assert.equal(f.counts.privateOpen, 0);
  assert.equal(f.store.get(id(1))!.verificationCallback!.completion!.observation!.state, 'enrolled');
  assert.ok(f.updates.some(value => value.status === 'completed'));
  assert.equal(fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8').includes('01234567'), false);
  assert.equal(f.voices.length, 1); assert.equal(f.store.get(id(1))?.transcript, undefined);
});
async function finishEnrollment(f: Awaited<ReturnType<typeof fixture>>) {
  await f.kickoff(); await f.answer(); const ws = await f.connect();
  await f.completeFromSpeech(ws);
  await until(() => f.updates.some(value => String(value.twiml).includes('<Gather')), 'protected capture');
  assert.equal((await f.post(`/broker/verification-callback-response/${id(1)}`, { ...f.params(), Digits: '01234567' })).status, 200);
  await f.broker.maintain();
  await until(() => ws.readyState === WebSocket.CLOSED && !!f.store.get(id(1))?.verificationCallback?.media?.stoppedAt,
    'completed enrollment media stops');
}

test('completed enrollment with all receipts acknowledged stops remote recovery and leaves journal bytes unchanged', async t => {
  const f = await fixture(t); await finishEnrollment(f);
  await f.broker.maintain();
  const saved = f.store.get(id(1))!;
  assert.equal(saved.verificationCallback!.remote!.state, 'completed');
  assert.equal(saved.verificationCallback!.completion!.observation!.state, 'enrolled');
  assert.ok(saved.verificationCallback!.receipts!.every(receipt => receipt.acknowledgedAt));
  assert.ok(saved.verificationCallback!.receipts!.some(receipt => receipt.kind === 'ended' && receipt.receipt));
  const before = fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8');
  const counts = { ...f.counts }, receiptCount = f.receipts.length, providerUpdates = f.updates.length;
  await new Promise(resolve => setTimeout(resolve, 5));
  await f.broker.maintain(); await f.broker.maintain();
  assert.deepEqual(f.counts, counts, 'no repeated API recovery/cancel or provider request');
  assert.equal(f.receipts.length, receiptCount);
  assert.equal(f.updates.length, providerUpdates);
  assert.equal(fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8'), before);
});

test('completed enrollment keeps delivering an unacknowledged ended receipt, then becomes idle', async t => {
  const f = await fixture(t); f.dropReceipt(true); await finishEnrollment(f);
  const pending = f.store.get(id(1))!.verificationCallback!;
  assert.ok(pending.receipts!.some(receipt => !receipt.acknowledgedAt));
  const attempts = f.receipts.length;
  await f.broker.maintain();
  assert.ok(f.receipts.length > attempts, 'pending callback delivery remains retryable');
  f.dropReceipt(false); await f.broker.maintain();
  assert.ok(f.store.get(id(1))!.verificationCallback!.receipts!.every(receipt => receipt.acknowledgedAt));
  const counts = { ...f.counts }, receipts = f.receipts.length;
  const bytes = fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8');
  await f.broker.maintain();
  assert.deepEqual(f.counts, counts); assert.equal(f.receipts.length, receipts);
  assert.equal(fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8'), bytes);
});

test('protected form without an accepted same-leg tool instruction refuses before completion', async t => {
  const f = await fixture(t); await f.kickoff(); await f.answer(); await f.connect();
  assert.equal((await f.post(`/broker/verification-callback-response/${id(1)}`, { ...f.params(), Digits: '01234567' })).status, 503);
  assert.equal(f.counts.complete, 0); assert.equal(f.counts.privateOpen, 0);
});
test('completed account-access uses exact saved proof after retiring restricted Live and preserves deadline', async t => {
  const timerCalls = t.mock.method(globalThis, 'setTimeout');
  const clearedTimers = t.mock.method(globalThis, 'clearTimeout');
  const f = await fixture(t, 'account_access'); await f.kickoff(); await f.answer();
  const beforeConnect = timerCalls.mock.calls.length;
  let restrictedDeadline: ReturnType<typeof setTimeout> | undefined;
  const ws = await f.connect(() => {
    // The leg deadline exists on socket connection. Live's separate timer is created only after start.
    const restrictedDeadlines = timerCalls.mock.calls.slice(beforeConnect).filter(call => typeof call.arguments[1] === 'number'
      && call.arguments[1] > 170_000 && call.arguments[1] <= 180_000);
    assert.equal(restrictedDeadlines.length, 1);
    restrictedDeadline = restrictedDeadlines[0].result;
  });
  assert.ok(restrictedDeadline);
  const before = f.store.get(id(1))!.verificationCallback!.media!;
  await f.completeFromSpeech(ws); await until(() => f.updates.some(value => String(value.twiml).includes('/broker/media/')), 'private media handoff');
  const call = f.store.get(id(1))!;
  assert.equal(f.counts.privateOpen, 1); assert.equal(f.counts.ordinarySubmit, 0); assert.equal(call.primary?.bindingId, id(20));
  assert.equal(call.verificationCallback!.media!.deadlineAt, before.deadlineAt);
  assert.equal(call.verificationCallback!.media!.startedAt, before.startedAt); assert.equal(call.mediaStartedAt, undefined);
  assert.equal(call.transcript, undefined); assert.ok(call.verificationCallback!.media!.privateDispatchedAt);
  assert.equal(clearedTimers.mock.calls.some(call => call.arguments[0] === restrictedDeadline), false);
  const privateSocket = await f.connectPrivate();
  assert.equal(f.voices[1].restrictedSetupTools, undefined); assert.ok(f.voices[1].backend);
  assert.equal(f.voices[1].history, undefined); assert.ok(f.voices[1].eligibleHistory);
  await until(() => f.store.get(id(1))?.liveGenerations?.phase === 'active', 'strict generation activates after assertion');
  assert.equal(f.store.get(id(1))!.liveGenerations!.clocks.callDeadlineAt, before.deadlineAt);
  assert.equal(f.store.get(id(1))!.liveGenerations!.clocks.mediaStartedAt, f.store.get(id(1))!.mediaStartedAt);
  assert.equal(f.privateRequests.filter(value => value.path.endsWith('/calls')).length, 1);
  assert.ok(f.privateRequests.some(value => value.path.endsWith('/context') && value.method === 'GET'));
  assert.ok(f.privateRequests.some(value => value.path.endsWith('/context/assert') && value.method === 'POST'));
  assert.equal(f.store.get(id(1))!.primary!.primaryDexterId, f.eligibleCall.context.primaryDexterId);
  assert.equal(f.store.get(id(1))!.primary!.threadId, f.eligibleCall.context.threadId);
  assert.deepEqual(f.voices[1].eligibleHistory!.source, f.eligibleCall);
  assert.deepEqual(f.voices[1].eligibleHistory!.version, f.eligibleCall.context.history.version);
  assert.equal(f.voices[1].eligibleHistory!.pageDigest, f.eligibleCall.context.history.pageDigest);
  assert.ok(f.voices[1].eligibleHistory!.history[0].text.includes(f.eligibleCall.context.history.localContext));
  assert.deepEqual(JSON.parse(f.voices[1].eligibleHistory!.history[1].text),
    { kind: 'primary_conversation_message', ...f.eligibleCall.context.messages[0] });
  assert.equal(f.store.get(id(1))!.verificationCallback!.media!.deadlineAt, before.deadlineAt);
  assert.equal(f.store.get(id(1))!.liveSessionId, 'private_session');
  assert.equal(f.counts.privateOpen, 1); assert.equal(f.counts.ordinarySubmit, 0); privateSocket.close();
  await until(() => f.counts.ended > 0, 'private binding closes');
  assert.ok(clearedTimers.mock.calls.some(call => call.arguments[0] === restrictedDeadline));
});
test('lost completion recovers exact GET and never repeats POST or creates private context from absent', async t => {
  const f = await fixture(t); await f.kickoff(); await f.answer(); const ws = await f.connect(); f.loseCompletion();
  await f.completeFromSpeech(ws); await until(() => f.updates.some(value => String(value.twiml).includes('<Gather')), 'protected capture');
  assert.equal((await f.post(`/broker/verification-callback-response/${id(1)}`, { ...f.params(), Digits: '01234567' })).status, 503);
  await f.broker.maintain(); assert.equal(f.counts.complete, 1); assert.equal(f.counts.privateOpen, 0);
});
test('provider terminal receipt timing is saved and retries exact body after response loss', async t => {
  const f = await fixture(t); await f.kickoff(); await f.answer(); f.dropReceipt(true); f.setStatus('completed');
  assert.equal((await f.post(`/broker/status/${id(1)}`, f.params())).status, 200);
  await f.broker.maintain(); const saved = f.store.get(id(1))!.verificationCallback!;
  assert.deepEqual(saved.endedTiming?.durationSeconds, 19); assert.ok(saved.receipts?.some(value => value.kind === 'ended'));
  f.dropReceipt(false); await f.broker.maintain();
  assert.ok(f.store.get(id(1))!.verificationCallback!.receipts!.every(value => value.acknowledgedAt));
  const ended = f.receipts.filter((value: any) => value.direction === 'outbound-verification') as any[];
  assert.ok(ended.length >= 1); assert.equal(ended[0].caller_user_id, undefined); assert.equal(ended[0].duration_seconds, 19);
});


test('terminal signed observation gates active restricted media before held receipt HTTP completes', async t => {
  const f = await fixture(t); await f.kickoff(); await f.answer(); await f.connect(); f.holdReceipt(); f.setStatus('completed');
  let responded = false; const pending = f.post(`/broker/status/${id(1)}`, f.params()).then(value => { responded = true; return value; });
  try {
    await until(() => !!f.store.get(id(1))?.verificationCallback?.media?.stoppedAt, 'terminal media gate before billing');
    assert.equal(responded, false); assert.equal(f.counts.privateOpen, 0);
    assert.equal(f.store.get(id(1))!.verificationCallback!.providerTerminal!.status, 'completed');
  } finally { f.releaseReceipt(); }
  assert.equal((await pending).status, 200);
});

test('new speech without new caption during setup admission blocks completion and protected capture', async t => {
  const f = await fixture(t); await f.kickoff(); await f.answer(); const ws = await f.connect();
  await f.completeFromSpeech(ws, false, true);
  assert.equal(f.counts.complete, 0); assert.equal(f.counts.privateOpen, 0);
  assert.equal(f.updates.some(value => String(value.twiml).includes('<Gather')), false);
});
test('only exact callback form routes receive262144 bound and duplicate signed answer cannot replace active media', async t => {
  const f = await fixture(t); await f.kickoff(); await f.answer(); await f.connect();
  assert.equal((await f.answer()).status, 409); assert.equal(f.voices.length, 1);
  assert.equal((await f.post(`/broker/verification-callback-response/${id(1)}`, { ...f.params(), padding: 'x'.repeat(262145) })).status, 413);
  assert.equal(f.counts.complete, 0); assert.equal(f.counts.privateOpen, 0);
});

test('actual non-A incoming settles ordinary attachment before restricted conversation; no private context or billing fallback', async t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'restricted-inbound-server-')), store = new BrokerStore(directory);
  const config: BrokerConfig = { host: '127.0.0.1', port: 3213, publicOrigin: 'https://broker.example.test', stateDir: directory,
    controlToken: 'c'.repeat(40), apiKey: 'synthetic', model: 'gpt-live-1', voice: 'marin', twilioAccountSid: 'AC' + '3'.repeat(32),
    twilioAuthToken: 'synthetic-token', from: '+15555550001', allowedTo: [], apiBaseUrl: 'http://127.0.0.1:3032', phoneServiceSecret: 'p'.repeat(40), maxCallSeconds: 1800 };
  const params = { AccountSid: config.twilioAccountSid, CallSid: sid, From: '+15555550002', To: config.from, Direction: 'inbound',
    CallStatus: 'ringing', StirVerstat: 'TN-Validation-Passed-C' };
  let remote: InboundOutcome | undefined, voice: BrokerLiveSessionOptions | undefined;
  const counts = { begin: 0, status: 0, cancel: 0, dial: 0, private: 0, receipt: 0 };
  const forbidden = async () => { counts.private++; throw new Error('No account access on original restricted leg'); };
  const primary = { openCall: forbidden, openInboundCall: forbidden, getContext: forbidden, submit: forbidden, captureTurn: forbidden,
    async recoverInbound(begin: InboundBeginIdentity) { return remote ?? { state: 'absent', beginRef: inboundBeginRef(begin) }; },
    async inboundStatus(attachmentId: string) { counts.status++; assert.equal(attachmentId, id(40)); assert.ok(remote); return structuredClone(remote); },
    async beginInbound() { counts.begin++; throw new PrimaryApiError('http_error', 403, { code: 'inbound_carrier_attestation_required', retryable: false }); },
    async cancelInbound(begin: InboundBeginIdentity) { counts.cancel++; remote ??= { attachmentId: id(40), beginRef: inboundBeginRef(begin),
      state: 'cancelled', expiresAt: null, cancelledAt: new Date().toISOString() }; return remote; },
  } as unknown as PrimaryApiClient;
  const calls = Object.assign(() => ({ async fetch() { return { sid, accountSid: config.twilioAccountSid, from: params.From,
    to: params.To, direction: 'inbound', status: 'in-progress' }; }, async update() { return { sid, accountSid: config.twilioAccountSid,
    from: params.From, to: params.To, direction: 'inbound', status: 'completed' }; } }), { async create() { counts.dial++; throw new Error('No callback without request'); } });
  const broker = createBrokerServer({ config, store, primary, provider: { calls } as never,
    inboundAttachment: createInboundHoldAdapter({ publicOrigin: config.publicOrigin, store }),
    harkPrimaryReceiptSender: forbidden, voiceFactory(options) { voice = options; queueMicrotask(() => options.onReady({ callId: options.callId!, sessionId }));
      return { appendAudio: () => true, async appendContext() { throw new Error('No context'); }, async appendInstructions() { return { eventId: 'instructions', status: 'accepted' }; },
        async appendCommentary() { return { eventId: 'opening', status: 'accepted' }; }, async interruptPlayback() { return { eventId: 'interrupt', status: 'accepted' }; }, resumePlayback() {},
        getTranscript: () => ({ revision: 0, callerRevision: 0, fragments: [] }), async close(reason) { const result = { callId: options.callId!, sessionId,
          reason: reason ?? 'closed', finalized: true, transcriptPersisted: false, outputAudioDiscarded: false }; options.onClosed(result); return result; } }; },
    restrictedSetup: { model: 'gpt-5-mini', receiptTransport: { apiBaseUrl: 'http://127.0.0.1:3032', signingSecret: 'r'.repeat(40),
      fetchImpl: async () => { counts.receipt++; throw new Error('Original leg is nonbillable'); } } },
  });
  await new Promise<void>(resolve => broker.server.listen(0, '127.0.0.1', resolve));
  const origin = `http://127.0.0.1:${(broker.server.address() as { port: number }).port}`; let ws: WebSocket | undefined;
  t.after(async () => { try { ws?.close(); await broker.close(); } finally { store.close(); fs.rmSync(directory, { recursive: true, force: true }); } });
  async function post(route: string, body: Record<string, string>) { return fetch(origin + route, { method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded', 'x-twilio-signature': Twilio.getExpectedTwilioSignature(config.twilioAuthToken, config.publicOrigin + route, body) },
    body: new URLSearchParams(body) }); }
  const first = await post('/broker/incoming-call', params); assert.equal(first.status, 200);
  assert.match(await first.text(), /<Say>One moment\.<\/Say><Redirect/);
  await until(() => counts.begin === 1, 'early ordinary admission reaches its current carrier refusal');
  assert.equal(counts.private, 0); assert.equal(voice, undefined);
  const original = store.bySid(sid)!;
  const continuation = await post(`/broker/inbound/${original.id}/continue`, { ...params, CallStatus: 'in-progress' });
  const continuationBody = await continuation.text();
  assert.equal(continuation.status, 200); assert.equal(counts.begin, 1); assert.equal(counts.cancel, 1); assert.equal(counts.status, 0);
  const saved = store.get(original.id)!;
  assert.equal(saved.failure, undefined);
  assert.equal(saved.inbound?.admissionRefusal?.code, 'inbound_carrier_attestation_required');
  assert.match(continuationBody, /setup-media/);
  assert.ok(saved.inbound?.localStop); assert.equal(saved.inbound?.remote?.state, 'cancelled'); assert.ok(saved.restrictedSetup);
  assert.equal(saved.primary, undefined); assert.equal(saved.inbound?.approvalRef, undefined); assert.equal(saved.restrictedSetup!.deadlineAt, saved.inbound!.waitUntil);
  const route = `/broker/setup-media/${saved.id}`;
  ws = new WebSocket(origin.replace('http:', 'ws:') + route, { headers: { 'x-twilio-signature': Twilio.getExpectedTwilioSignature(config.twilioAuthToken, config.publicOrigin + route, {}) } });
  await new Promise<void>((resolve, reject) => { ws!.once('open', resolve); ws!.once('error', reject); });
  ws.send(JSON.stringify({ event: 'start', start: { streamSid: stream, accountSid: config.twilioAccountSid, callSid: sid,
    mediaFormat: { encoding: 'audio/x-mulaw', sampleRate: 8000, channels: 1 }, customParameters: { token: streamToken(config.controlToken + ':restricted', saved.id, sid) } } }));
  await until(() => !!voice, 'restricted incoming Live starts');
  assert.deepEqual(voice!.history, []); assert.ok(voice!.restrictedSetupTools);
  await broker.maintain(); assert.equal(counts.begin, 1); assert.equal(counts.cancel, 1);
  assert.equal(counts.private, 0); assert.equal(counts.dial, 0); assert.equal(counts.receipt, 0);
  assert.deepEqual(store.get(saved.id)!.inbound!.begin.providerProof.params, params);
});
