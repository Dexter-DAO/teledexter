import assert from 'node:assert/strict';
import test from 'node:test';
import type { ClientEvent } from 'openai/resources/live/live';
import { createBrokerLiveSession } from '../src/live/session.js';
import type { BrokerLiveConnection } from '../src/live/types.js';
import type { RestrictedSetupTools } from '../src/live/restrictedSetupTools.js';

const callId = 'restricted-call', sessionId = 'restricted-session';
const tick = () => new Promise<void>(resolve => setImmediate(resolve));
function fixture(t: test.TestContext, execute?: RestrictedSetupTools['execute']) {
  const sent: ClientEvent[] = [], calls: Parameters<RestrictedSetupTools['execute']>[0][] = [], errors: string[] = [];
  let receive = (_event: unknown) => {}, open = () => {}, socket = false, generic = 0, ready = 0;
  const connection: BrokerLiveConnection = { onOpen(fn) { open = fn; }, onEvent(fn) { receive = fn; },
    onError() {}, onClose() {}, isOpen: () => socket, send(value) { sent.push(value);
      if (value.type === 'session.close') queueMicrotask(() => receive({ type: 'session.closed', session: { id: sessionId }, reason: 'close_requested' }));
    }, close() { socket = false; }, terminate() { socket = false; } };
  const voice = createBrokerLiveSession({ apiKey: 'synthetic', callId, instructions: 'Help the caller verify phone access.', history: [],
    restrictedSetupTools: { model: 'gpt-5-mini', async execute(input, channel) {
      calls.push(structuredClone(input)); return execute ? execute(input, channel) : { phase: 'checking' };
    } }, backend: { async delegate() { generic++; } }, connectionFactory: () => connection,
    onAudio() {}, onTranscript() {}, onClosed() {}, onReady() { ready++; },
    onEvent(value) { if (value.type === 'live.error' && typeof value.code === 'string') errors.push(value.code); } });
  t.after(async () => { await voice.close(); });
  socket = true; open();
  const start = () => receive({ type: 'session.started', session: { id: sessionId, model: 'gpt-live-1', delegation: { type: 'responses' } } });
  const delegate = (id = 'delegation-1') => receive({ type: 'session.delegation.created', offset_ms: 2000,
    delegation: { id, type: 'delegation', target: 'responses', response_id: `response-${id}` } });
  const tool = (patch: Record<string, unknown> = {}, delegationId: string | null = 'delegation-1') => receive({ type: 'response.event', delegation_id: delegationId,
    event: { type: 'response.output_item.done', output_index: 0, sequence_number: 1, item: {
      type: 'function_call', id: 'item-1', call_id: 'function-1', name: 'request_verified_callback',
      arguments: '{"explicitlyRequested":true}', status: 'completed', ...patch } } });
  return { voice, sent, calls, errors, start, delegate, tool, receive, ready: () => ready, generic: () => generic };
}

test('startup sends only the four finite tools and waits for session.started', async t => {
  const f = fixture(t), event = f.sent[0];
  assert.equal(event.type, 'session.start'); if (event.type !== 'session.start') throw new Error('Expected startup');
  assert.deepEqual(event.session.input, []); assert.equal(event.session.store, false);
  const delegation = event.session.delegation; assert.equal(delegation?.type, 'responses');
  if (delegation?.type !== 'responses') throw new Error('Expected named tools');
  assert.equal(delegation.responses.parallel_tool_calls, false);
  assert.deepEqual(delegation.responses.tools?.map(tool => tool.type === 'function' ? tool.name : tool.type),
    ['phone_setup_status', 'request_verified_callback', 'complete_phone_setup', 'decline_phone_setup']);
  assert.equal(f.ready(), 0); f.start(); assert.equal(f.ready(), 1);
});

test('metadata and streamed argument deltas execute no tool; complete named call uses exact raw source', async t => {
  const f = fixture(t); f.start();
  f.receive({ type: 'session.input_transcript.delta', event_id: 'raw-1', delta: 'Please call my verified number.', start_ms: 0, end_ms: 1500 });
  f.delegate(); f.receive({ type: 'response.event', delegation_id: 'delegation-1', event: {
    type: 'response.function_call_arguments.delta', delta: '{"explicitlyRequested":' } });
  await tick(); assert.equal(f.calls.length, 0); assert.equal(f.generic(), 0);
  f.tool(); await tick(); assert.equal(f.calls.length, 1); assert.equal(f.generic(), 0);
  assert.equal(f.calls[0].toolCallId, 'function-1'); assert.equal(f.calls[0].request.sessionId, sessionId);
  assert.equal(f.calls[0].request.transcript.fragments[0].text, 'Please call my verified number.');
  const result = f.sent.find(value => value.type === 'response.item.create');
  assert.ok(result && result.type === 'response.item.create' && result.item.type === 'function_call_output');
  assert.equal(result.item.call_id, 'function-1'); assert.equal(result.item.output, '{"phase":"checking"}');
  assert.equal(f.sent.filter(value => value.type === 'response.create').length, 1);
});

test('exact function replay executes once; changed argument replay refuses', async t => {
  const f = fixture(t); f.start(); f.delegate(); f.tool(); await tick(); f.tool(); await tick();
  assert.equal(f.calls.length, 1); f.tool({ name: 'decline_phone_setup', arguments: '{}' });
  assert.deepEqual(f.errors, ['setup_tool_identity_changed']); assert.equal(f.calls.length, 1);
});

test('a function ID cannot cross delegation identities', async t => {
  const f = fixture(t); f.start(); f.delegate(); f.tool(); await tick(); f.delegate('delegation-2'); f.tool({}, 'delegation-2');
  assert.deepEqual(f.errors, ['setup_tool_identity_changed']); assert.equal(f.calls.length, 1);
});

test('private functions, extra destination or code fields, false consent and malformed JSON execute nothing', async t => {
  for (const patch of [{ name: 'wallet_balance', arguments: '{}' },
    { arguments: '{"explicitlyRequested":true,"to":"+15555550001"}' },
    { name: 'complete_phone_setup', arguments: '{"accepted":true,"challenge":"01234567"}' },
    { arguments: '{"explicitlyRequested":false}' }, { arguments: '{' }]) {
    const f = fixture(t); f.start(); f.delegate(); f.tool(patch);
    assert.equal(f.calls.length, 0); assert.deepEqual(f.errors, ['invalid_setup_tool']);
  }
});

test('unattributed lifecycle events are ignored, but an unattributed function is refused', async t => {
  const f = fixture(t); f.start();
  f.receive({ type: 'response.event', delegation_id: null, event: { type: 'response.created' } });
  assert.equal(f.errors.length, 0); f.tool({}, null);
  assert.deepEqual(f.errors, ['uncorrelated_setup_response']); assert.equal(f.calls.length, 0);
});

test('close aborts a held tool and drops late output without retry', async t => {
  let release!: () => void, entered!: () => void, signal: AbortSignal | undefined;
  const began = new Promise<void>(resolve => { entered = resolve; }), gate = new Promise<void>(resolve => { release = resolve; });
  const f = fixture(t, async (_input, channel) => { signal = channel.signal; entered(); await gate; return { phase: 'complete' }; });
  f.start(); f.delegate(); f.tool();
  try { await began; await f.voice.close(); assert.equal(signal?.aborted, true); }
  finally { release(); await tick(); }
  assert.equal(f.sent.some(event => event.type === 'response.item.create'), false); assert.equal(f.calls.length, 1);
});

test('host errors return only a pending phase without raw error or code text', async t => {
  const f = fixture(t, async () => { throw new Error('private code 01234567'); }); f.start(); f.delegate(); f.tool(); await tick();
  const outputs = f.sent.filter(event => event.type === 'response.item.create');
  assert.equal(outputs.length, 1); assert.doesNotMatch(JSON.stringify(outputs), /01234567|private code/);
  assert.match(JSON.stringify(outputs), /checking/);
});
