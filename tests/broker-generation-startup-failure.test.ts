import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import test, { type TestContext } from 'node:test';
import type { ClientEvent } from 'openai/resources/live/live';
import { createGenerationVoice } from '../src/broker/generationVoice.js';
import { createInboundStartupContexts } from '../src/broker/startupContext.js';
import { BrokerStore } from '../src/broker/store.js';
import { PrimaryApiError, type PrimaryApiClient } from '../src/broker/primaryApi.js';
import type { EligibleContextClient, EligiblePhoneCall } from '../src/broker/eligibleContext.js';
import { createBrokerLiveSession, type BrokerLiveConnection } from '../src/live/session.js';

const bindingId = '11111111-1111-4111-8111-111111111111';
const primaryDexterId = '22222222-2222-4222-8222-222222222222';
const threadId = '33333333-3333-4333-8333-333333333333';
const ownerPrincipalId = '44444444-4444-4444-8444-444444444444';
const callSid = `CA${'1'.repeat(32)}`;
const privateMessage = 'Never retain this provider message or credential: synthetic-secret';

class Connection implements BrokerLiveConnection {
  sent: ClientEvent[] = [];
  terminated = 0;
  open = false;
  opened = () => {};
  event: (event: unknown) => void = () => {};
  error: (error: unknown) => void = () => {};
  ended = () => {};
  onOpen(fn: () => void) { this.opened = fn; }
  onEvent(fn: (event: unknown) => void) { this.event = fn; }
  onError(fn: (error: unknown) => void) { this.error = fn; }
  onClose(fn: () => void) { this.ended = fn; }
  isOpen() { return this.open; }
  send(event: ClientEvent) { this.sent.push(structuredClone(event)); }
  close() { this.open = false; }
  terminate() { this.terminated++; this.open = false; }
  connect() { this.open = true; this.opened(); }
  reject(code: string) {
    const event = { type: 'error', error: { code, message: privateMessage } };
    this.event(event);
    this.error({ error: event }); // The SDK repeats protocol errors through its error callback.
  }
}
async function until(predicate: () => boolean) {
  for (let count = 0; count < 100 && !predicate(); count++) await new Promise<void>(resolve => setImmediate(resolve));
  assert.ok(predicate(), 'expected generation boundary');
}
async function fixture(t: TestContext, rejectPostOpenAssert = false) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-startup-failure-'));
  const store = new BrokerStore(directory);
  const call = store.reserve({ requestId: randomUUID(), number: '+12025550123', direction: 'outbound',
    purpose: 'Read my saved context.', callSid }).call;
  const page: EligiblePhoneCall = { bindingId, callSid, state: 'open', context: {
    primaryDexterId, threadId, displayName: 'Dexter', messages: [], history: {
      version: { primaryDexterId, threadId, ownerPrincipalId, contextEpoch: '6', threadEventSequence: '261', digest: 'a'.repeat(64) },
      pageDigest: 'b'.repeat(64), localContext: '', hasMore: false, omittedForBudget: false, nextBeforeSequence: null,
    },
  } };
  store.recordPrimaryBinding(call.id, page);
  store.update(call.id, { mediaStartedAt: new Date().toISOString() });
  let assertions = 0, fetches = 0, ready = 0;
  const problems: string[] = [], audio: string[] = [];
  const client: EligibleContextClient = {
    async fetch() { fetches++; return page; },
    async assert(_binding, version) {
      assertions++;
      if (rejectPostOpenAssert && assertions > 1) throw new PrimaryApiError('http_error', 503);
      return { bindingId, callSid, current: true, version };
    },
    async delivery() { throw new Error('No delivery is authorized'); },
  };
  const startup = createInboundStartupContexts(client);
  startup.remember(store.get(call.id)!, page);
  const connection = new Connection();
  let created = false;
  const voice = createGenerationVoice({ call: store.get(call.id)!, callDeadlineAt: new Date(Date.now() + 60_000).toISOString(),
    apiKey: 'synthetic-key', voice: 'marin', store, primary: {} as PrimaryApiClient,
    context: startup.take(store.get(call.id)!),
    voiceFactory(options) { created = true; return createBrokerLiveSession({ ...options, connectionFactory: () => connection, closeTimeoutMs: 10 }); },
    onAudio(payload) { audio.push(payload); }, onReady() { ready++; }, onClosed() {}, onPlaybackClear() {},
    onProblem(code) { problems.push(code); store.update(call.id, { failure: { code, message: code } }); },
  });
  t.after(async () => { await voice.close(); store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  await until(() => created);
  connection.connect();
  assert.equal(connection.sent[0]?.type, 'session.start');
  assert.equal(assertions, 1, 'cached context retains its mandatory fresh assertion before opening');
  assert.equal(fetches, 0);
  return { store, callId: call.id, connection, problems, audio, get ready() { return ready; }, get assertions() { return assertions; } };
}

for (const [providerCode, expectedCode] of [
  ['credit_balance_exhausted', 'credit_balance_exhausted'],
  ['invalid_request_error', 'invalid_request_error'],
  ['rate_limit_exceeded', 'rate_limit_exceeded'],
  ['unrecognized_synthetic_secret', 'generation_start_failed'],
] as const) {
  test(`actual Live adapter preserves finite startup failure ${expectedCode} through the generation journal`, async t => {
    const f = await fixture(t);
    f.connection.reject(providerCode);
    await until(() => Boolean(f.store.get(f.callId)!.failure));
    const call = f.store.get(f.callId)!;
    const segment = call.liveGenerations!.segments[0];
    assert.equal(Object.hasOwn(segment, 'startupFailure'), false, 'the existing journal schema is unchanged');
    assert.equal(segment.state, 'closed');
    assert.equal(segment.providerFinalized, false);
    assert.equal(segment.sessionId, undefined);
    assert.equal(segment.readyAt, undefined);
    assert.equal(call.failure?.code, expectedCode);
    assert.ok(f.problems.length > 0 && f.problems.every(code => code === expectedCode));
    assert.equal(f.assertions, 1, 'post-open assertion cannot run before a ready session');
    assert.equal(f.ready, 0);
    assert.deepEqual(f.audio, []);
    assert.equal(f.connection.terminated, 1);
    assert.ok(!JSON.stringify(call).includes('synthetic-secret'));
    assert.ok(!JSON.stringify(call).includes('unrecognized_synthetic_secret'));
  });
}

test('connection loss before session.started retains its transport stage and code', async t => {
  const f = await fixture(t);
  f.connection.error(new Error(privateMessage));
  await until(() => Boolean(f.store.get(f.callId)!.failure));
  assert.equal(f.store.get(f.callId)!.failure?.code, 'provider_connection_error');
  assert.deepEqual(f.audio, []);
});

test('post-open authority failure remains separate from Live startup failure', async t => {
  const f = await fixture(t, true);
  f.connection.event({ type: 'session.started', session: { id: 'started_fixture', model: 'gpt-live-1', delegation: { type: 'client' } } });
  await until(() => f.problems.includes('context_unavailable'));
  const segment = f.store.get(f.callId)!.liveGenerations!.segments[0];
  assert.equal(segment.sessionId, 'started_fixture');
  assert.ok(segment.readyAt);
  assert.equal(Object.hasOwn(segment, 'startupFailure'), false);
  assert.equal(f.assertions, 2);
  assert.equal(f.ready, 0);
  assert.deepEqual(f.audio, []);
});
