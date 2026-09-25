import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test, { type TestContext } from 'node:test';
import { WebSocketError } from 'openai/resources/live/internal-base';
import type { ClientEvent, ErrorEvent, ServerEvent, SessionResource } from 'openai/resources/live/live';
import { prepareEligibleHistory, type PreparedEligibleHistory } from '../src/broker/eligibleHistory.js';
import type { EligiblePhoneCall } from '../src/broker/eligibleContext.js';
import { createBrokerLiveSession, type BrokerLiveConnection, type BrokerLiveEvent,
  type BrokerLiveSessionOptions, type LiveSessionClosed } from '../src/live/session.js';

// Fake transport checks the exact startup boundary. It does not measure rendered tokens.
const bindingId = '11111111-1111-4111-8111-111111111111';
const ownerPrincipalId = '22222222-2222-4222-8222-222222222222';
const primaryDexterId = '33333333-3333-4333-8333-333333333333';
const threadId = '44444444-4444-4444-8444-444444444444';
const runId = '55555555-5555-4555-8555-555555555555';
const callSid = `CA${'1'.repeat(32)}`;
const when = '2026-09-24T12:00:00.000Z';
const binding = { bindingId, ownerPrincipalId, primaryDexterId, threadId, callSid };
const sessionSnapshot: SessionResource = { id: 'eligible_startup_session', model: 'gpt-live-1', status: 'active',
  expires_at: 1_800_000_000, delegation: { type: 'client' },
  audio: { format: { type: 'audio/pcmu', rate: 8000 }, output: { voice: 'marin' } } };
const audio = Buffer.alloc(160, 0xff).toString('base64');
const flush = () => new Promise<void>(resolve => setImmediate(resolve));

function source(): EligiblePhoneCall {
  return { bindingId, callSid, state: 'open', context: { primaryDexterId, threadId, displayName: 'Dexter',
    messages: [
      { id: '66666666-6666-4666-8666-666666666666', role: 'user', text: `  ${'A'.repeat(4500)}\nEarlier question.🙂`,
        eventSequence: '9', provenance: { messageCreatedAt: when, eventId: '77777777-7777-4777-8777-777777777777',
          eventOccurredAt: when, runId }, canonicalSource: { sourceEventId: '88888888-8888-4888-8888-888888888888',
          revision: 2, occurredAt: when, storedProjectionKind: 'derived' } },
      { id: '99999999-9999-4999-8999-999999999999', role: 'assistant', text: `${'B'.repeat(4500)}\nEarlier answer.  `,
        eventSequence: '10', provenance: { messageCreatedAt: when, eventId: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
          eventOccurredAt: when, runId } },
    ],
    history: { version: { ownerPrincipalId, primaryDexterId, threadId, contextEpoch: '8', threadEventSequence: '11', digest: 'a'.repeat(64) },
      pageDigest: 'b'.repeat(64), localContext: 'Saved preference: Explain results briefly.',
      hasMore: true, omittedForBudget: false, nextBeforeSequence: '9' } } };
}
const prepared = () => prepareEligibleHistory(source(), binding);

class Connection implements BrokerLiveConnection {
  sent: ClientEvent[] = [];
  open = false;
  terminated = 0;
  opened: () => void = () => {};
  event: (event: unknown) => void = () => {};
  error: (error: unknown) => void = () => {};
  ended: () => void = () => {};
  onOpen(fn: () => void) { this.opened = fn; }
  onEvent(fn: (event: unknown) => void) { this.event = fn; }
  onError(fn: (error: unknown) => void) { this.error = fn; }
  onClose(fn: () => void) { this.ended = fn; }
  isOpen() { return this.open; }
  send(event: ClientEvent) { this.sent.push(JSON.parse(JSON.stringify(event)) as ClientEvent); }
  close() { this.open = false; }
  terminate() { this.terminated++; this.open = false; }
  connect() { this.open = true; this.opened(); }
  receive(event: ServerEvent) {
    this.event(event);
    if (event.type === 'error') this.error(new WebSocketError(event.error.message, event));
  }
  accept() { this.receive({ type: 'session.started', event_id: 'started', session: sessionSnapshot }); }
}

function options(connection: Connection): BrokerLiveSessionOptions {
  return { apiKey: 'synthetic-key', callId: 'eligible_startup_call', instructions: 'Delegate account requests.',
    connectionFactory: () => connection, backend: { async delegate() {} },
    onAudio() {}, onTranscript() {}, onReady() {}, onClosed() {} };
}
function fixture(t: TestContext, overrides: Partial<BrokerLiveSessionOptions> = {}) {
  const connection = new Connection();
  const ready: Array<{ callId: string; sessionId: string }> = [];
  const closed: LiveSessionClosed[] = [];
  const events: BrokerLiveEvent[] = [];
  const session = createBrokerLiveSession({ ...options(connection), eligibleHistory: prepared(),
    onReady: value => ready.push(value), onClosed: value => closed.push(value), onEvent: value => events.push(value), ...overrides });
  t.after(async () => {
    const completion = session.close();
    if (ready.length && !closed.length) connection.receive({ type: 'session.closed', event_id: 'closed',
      session: sessionSnapshot, reason: 'close_requested', usage: { seconds: 0 } });
    await completion;
  });
  return { connection, session, ready, closed, events };
}
function startup(connection: Connection) {
  const event = connection.sent.find(value => value.type === 'session.start');
  assert.ok(event && event.type === 'session.start');
  return event;
}
function assertBeforeConnection(overrides: Partial<BrokerLiveSessionOptions>, expected?: RegExp) {
  let constructed = 0;
  const connection = new Connection();
  const create = () => createBrokerLiveSession({ ...options(connection), ...overrides,
    connectionFactory: () => { constructed++; return connection; } });
  if (expected) assert.throws(create, expected);
  else assert.throws(create);
  assert.equal(constructed, 0);
  assert.deepEqual(connection.sent, []);
}

test('eligible startup sends every complete item and provenance above the legacy 8KB bound unchanged', t => {
  const plan = prepared();
  assert.ok(plan.historyTextBytes > 8000);
  const expected = structuredClone(plan);
  const f = fixture(t, { eligibleHistory: plan });
  f.connection.connect();
  const start = startup(f.connection);
  assert.deepEqual(start.session.input, expected.initialItems);
  assert.equal(JSON.stringify(start.session.input), JSON.stringify(expected.initialItems));
  assert.equal(Buffer.byteLength(JSON.stringify(start.session.input)), expected.initialInputBytes);
  assert.equal(createHash('sha256').update(JSON.stringify(start.session.input)).digest('hex'), expected.initialInputDigest);
  assert.deepEqual(start.session.input!.map(item => item.role), ['developer', 'user', 'assistant']);
  const memoryText = start.session.input![0].content[0].text;
  const memory = JSON.parse(memoryText.slice(memoryText.indexOf('\n') + 1));
  assert.deepEqual(memory.history, expected.source.context.history);
  assert.equal(memory.primaryDexterId, primaryDexterId);
  assert.equal(memory.threadId, threadId);
  for (const [index, message] of expected.source.context.messages.entries()) {
    assert.deepEqual(JSON.parse(start.session.input![index + 1].content[0].text), { kind: 'primary_conversation_message', ...message });
  }
  assert.equal(expected.renderedTokenCount, null);
  assert.deepEqual(f.ready, []);
  f.connection.accept();
  assert.equal(f.ready[0].sessionId, sessionSnapshot.id);
  assert.equal(plan.renderedTokenCount, null, 'provider startup does not supply an actual rendered-token count');
});

test('prepared eligible history is snapshotted before asynchronous socket opening', t => {
  const plan = prepared();
  const expected = JSON.stringify(plan.initialItems);
  const f = fixture(t, { eligibleHistory: plan });
  plan.source.context.messages[0].text = 'Changed after construction.';
  plan.history[1].text = 'Changed flattened history.';
  plan.initialItems[1].content[0].text = 'Changed wire item.';
  f.connection.connect();
  assert.equal(JSON.stringify(startup(f.connection).session.input), expected);
});

test('startup rejects changed digest, byte accounting, source, role or partial items before opening a connection', () => {
  for (const mutate of [
    (plan: PreparedEligibleHistory) => { plan.initialInputDigest = 'f'.repeat(64); },
    (plan: PreparedEligibleHistory) => { plan.initialInputBytes++; },
    (plan: PreparedEligibleHistory) => { plan.historyTextBytes--; },
    (plan: PreparedEligibleHistory) => { plan.pageDigest = 'f'.repeat(64); },
    (plan: PreparedEligibleHistory) => { plan.version = { ...plan.version, contextEpoch: '9' }; },
    (plan: PreparedEligibleHistory) => { plan.initialItems.pop(); },
    (plan: PreparedEligibleHistory) => { plan.history[1].role = 'assistant'; },
    (plan: PreparedEligibleHistory) => { plan.source.context.messages[0].provenance.eventOccurredAt = '2026-09-24T12:01:00.000Z'; },
    (plan: PreparedEligibleHistory) => { plan.source.context.messages[0].canonicalSource!.revision = 3; },
    (plan: PreparedEligibleHistory) => {
      plan.initialItems[1].content[0].text = 'Clipped';
      plan.initialInputDigest = createHash('sha256').update(JSON.stringify(plan.initialItems)).digest('hex');
      plan.initialInputBytes = Buffer.byteLength(JSON.stringify(plan.initialItems));
    },
  ]) {
    const plan = prepared(); mutate(plan);
    assertBeforeConnection({ eligibleHistory: plan });
  }
});

test('forged 129-item or oversized payloads cannot bypass reconstruction and source bounds', () => {
  const itemCount = prepared();
  itemCount.initialItems = Array.from({ length: 129 }, () => structuredClone(itemCount.initialItems[0]));
  itemCount.initialInputDigest = createHash('sha256').update(JSON.stringify(itemCount.initialItems)).digest('hex');
  itemCount.initialInputBytes = Buffer.byteLength(JSON.stringify(itemCount.initialItems));
  assertBeforeConnection({ eligibleHistory: itemCount });
  const wirePayload = prepared();
  wirePayload.initialItems[0].content[0].text = 'x'.repeat(131_073);
  wirePayload.initialInputDigest = createHash('sha256').update(JSON.stringify(wirePayload.initialItems)).digest('hex');
  wirePayload.initialInputBytes = Buffer.byteLength(JSON.stringify(wirePayload.initialItems));
  assertBeforeConnection({ eligibleHistory: wirePayload });
  const escaped = prepared();
  escaped.source.context.messages = [escaped.source.context.messages[0]];
  escaped.source.context.messages[0].text = '\u0001'.repeat(24_000);
  escaped.source.context.history.localContext = '';
  assertBeforeConnection({ eligibleHistory: escaped }, /history_payload_too_large/);
  const excessiveSource = prepared(); excessiveSource.source.context.messages[0].text = 'x'.repeat(24_001);
  assertBeforeConnection({ eligibleHistory: excessiveSource });
});

test('eligible and legacy histories cannot be combined, and legacy 8KB and 128-item behavior remains', t => {
  assertBeforeConnection({ eligibleHistory: prepared(), history: [] }, /ambiguous_live_history/);
  assertBeforeConnection({ history: [{ role: 'user', text: 'x'.repeat(8001) }] }, /invalid_live_history/);
  assertBeforeConnection({ history: Array.from({ length: 129 }, () => ({ role: 'user' as const, text: 'x' })) }, /invalid_live_history/);
  const history = Array.from({ length: 128 }, () => ({ role: 'user' as const, text: 'x' }));
  const f = fixture(t, { eligibleHistory: undefined, history });
  f.connection.connect();
  assert.equal(startup(f.connection).session.input!.length, 128);
  assert.equal(startup(f.connection).session.input!.every(item => item.content[0].text === 'x'), true);
  f.connection.accept();
  assert.equal(f.ready.length, 1);
});

test('socket opening and send completion do not release buffered audio or readiness before session.started', async t => {
  const f = fixture(t);
  assert.equal(f.session.appendAudio(audio), true);
  f.connection.connect();
  assert.equal(f.connection.sent.length, 1);
  assert.equal(f.connection.sent[0].type, 'session.start');
  assert.deepEqual(f.ready, []);
  assert.equal((await f.session.appendContext('A factual update.')).status, 'rejected');
  assert.deepEqual(f.connection.sent.filter(event => event.type === 'session.input_audio.append'), []);
  f.connection.accept();
  assert.equal(f.ready.length, 1);
  assert.deepEqual(f.connection.sent.filter(event => event.type === 'session.input_audio.append').map(event => event.audio), [audio]);
  f.connection.accept();
  assert.equal(f.ready.length, 1);
  assert.equal(f.connection.sent.filter(event => event.type === 'session.start').length, 1);
});

test('generic provider startup refusal retains safe code, discards queued audio and never claims ready', async t => {
  const f = fixture(t);
  f.session.appendAudio(audio); f.connection.connect();
  const command = startup(f.connection);
  const refusal: ErrorEvent = { type: 'error', event_id: 'startup_error', error: { type: 'invalid_request_error',
    code: 'invalid_request_error', message: 'Private diagnostic detail and full input must remain private.',
    param: 'session.input', client_event_id: command.event_id! } };
  f.connection.receive(refusal);
  await flush();
  assert.deepEqual(f.ready, []);
  assert.equal(f.closed.length, 1);
  assert.equal(f.closed[0].finalized, false);
  assert.equal(f.closed[0].reason, 'invalid_request_error');
  assert.equal(f.connection.terminated, 1);
  assert.equal(f.events.filter(event => event.type === 'live.error').length, 1, 'SDK dual callbacks must not duplicate failure');
  assert.equal(JSON.stringify({ events: f.events, closed: f.closed }).includes('Private diagnostic'), false);
  assert.deepEqual(f.connection.sent.filter(event => event.type === 'session.input_audio.append'), []);
  f.connection.accept();
  assert.deepEqual(f.ready, []);
});

test('missing session.started times out without a ready callback or automatic startup retry', async t => {
  const f = fixture(t, { startupTimeoutMs: 5 });
  f.session.appendAudio(audio); f.connection.connect();
  await new Promise<void>(resolve => setTimeout(resolve, 25));
  assert.deepEqual(f.ready, []);
  assert.equal(f.closed.length, 1);
  assert.equal(f.closed[0].reason, 'startup_timeout');
  assert.equal(f.closed[0].finalized, false);
  assert.equal(f.connection.terminated, 1);
  assert.equal(f.connection.sent.filter(event => event.type === 'session.start').length, 1);
  assert.deepEqual(f.connection.sent.filter(event => event.type === 'session.input_audio.append'), []);
  f.connection.accept();
  assert.deepEqual(f.ready, []);
});

test('startup transport failure cannot be mistaken for accepted eligible input', async t => {
  const f = fixture(t); f.connection.connect();
  f.connection.error(new Error('private socket diagnostic'));
  await flush();
  assert.deepEqual(f.ready, []);
  assert.equal(f.closed[0].reason, 'provider_connection_error');
  assert.equal(f.connection.terminated, 1);
  assert.equal(JSON.stringify(f.events).includes('private socket'), false);
});
