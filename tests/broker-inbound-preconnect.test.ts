import test from 'node:test';
import assert from 'node:assert/strict';
import { createInboundPreconnect, type InboundTransportTiming } from '../src/broker/inboundPreconnect.js';
import { createBrokerLiveSession } from '../src/live/session.js';
import type { BrokerCall } from '../src/broker/store.js';
import { PreconnectTransport } from './helpers/preconnectTransport.js';

function call() {
  const approvalRef = { attachmentId: '11111111-1111-4111-8111-111111111111', approvalId: '22222222-2222-4222-8222-222222222222', digest: 'd'.repeat(64) };
  return { id: '33333333-3333-4333-8333-333333333333', callSid: 'CA' + '1'.repeat(32),
    inbound: { waitUntil: new Date(Date.now() + 1000).toISOString(), remote: { state: 'approved', approvalRef,
      expiresAt: new Date(Date.now() + 1000).toISOString() } } } as unknown as BrokerCall;
}
function admit(c: BrokerCall) {
  const approvalRef = c.inbound!.remote!.state === 'approved' ? c.inbound!.remote!.approvalRef : undefined;
  c.inbound!.remote = { ...c.inbound!.remote!, state: 'bound', approvalRef: approvalRef!, bindingId: '44444444-4444-4444-8444-444444444444', callState: 'open', consumedAt: new Date().toISOString() };
  c.primary = { bindingId: c.inbound!.remote.bindingId } as BrokerCall['primary'];
  c.mediaStartedAt = new Date().toISOString(); return c;
}
function session(c: BrokerCall, connectionFactory: () => PreconnectTransport) {
  return createBrokerLiveSession({ callId: c.id, apiKey: 'test', instructions: 'Greet the caller.', connectionFactory,
    backend: { async delegate() { throw new Error('unexpected_delegation'); } }, onAudio() {}, onTranscript() {}, onReady() {}, onClosed() {} });
}

test('initial adopted transport leaves later explicit generation replacements on their normal connector', async () => {
  const first = new PreconnectTransport(), next = new PreconnectTransport(), c = call(); let connects = 0;
  const manager = createInboundPreconnect(() => { connects++; return first; });
  manager.begin(c); first.open(); admit(c);
  const initial = session(c, manager.factory(c)! as () => PreconnectTransport);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(first.sent.filter(event => event.type === 'session.start').length, 1);
  await initial.close();
  assert.equal(manager.factory(c), undefined);
  const replacement = session(c, manager.factory(c) as (() => PreconnectTransport) | undefined ?? (() => next));
  next.open(); await new Promise(resolve => setImmediate(resolve));
  assert.equal(next.sent.filter(event => event.type === 'session.start').length, 1);
  assert.equal(connects, 1); await replacement.close(); manager.close();
});

for (const mismatch of ['approval', 'binding', 'callSid', 'media', 'closed'] as const) test(`adoption rejects ${mismatch} drift without sending a command`, () => {
  const transport = new PreconnectTransport(), c = call(), manager = createInboundPreconnect(() => transport);
  manager.begin(c); admit(c);
  if (mismatch === 'approval' && c.inbound!.remote!.state === 'bound') c.inbound!.remote!.approvalRef.digest = 'e'.repeat(64);
  if (mismatch === 'binding') c.primary!.bindingId = 'wrong';
  if (mismatch === 'callSid') c.callSid = 'wrong';
  if (mismatch === 'media') c.mediaStartedAt = undefined;
  if (mismatch === 'closed') c.endedAt = new Date().toISOString();
  assert.throws(() => manager.factory(c)!(), /inbound_preconnect_unavailable/);
  assert.deepEqual(transport.sent, []); assert.equal(transport.terminated, 1); manager.close();
});

test('admission deadline closes a never-adopted transport; terminal cleanup and shutdown retain no socket', t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'] });
  const transport = new PreconnectTransport(), c = call(), manager = createInboundPreconnect(() => transport);
  manager.begin(c); t.mock.timers.tick(1001);
  assert.equal(transport.terminated, 1); admit(c);
  assert.throws(() => manager.factory(c)!(), /inbound_preconnect_unavailable/);
  c.endedAt = new Date().toISOString(); manager.forget(c.id);
  assert.equal(manager.factory(c), undefined); manager.begin(c); assert.equal(transport.terminated, 1); manager.close();
});

test('open timing is emitted once for an already-open socket and logging failures do not change adoption', () => {
  const transport = new PreconnectTransport(), events: InboundTransportTiming[] = [], c = call(); transport.open();
  const manager = createInboundPreconnect(() => transport, event => { events.push(event); throw new Error('logger unavailable'); });
  manager.begin(c); transport.open(); admit(c); assert.equal(manager.factory(c)!(), transport);
  assert.deepEqual(events.map(event => event.stage), ['connecting', 'open', 'adopted']);
  assert.ok(events.every(event => Number.isFinite(event.elapsedMs) && event.elapsedMs >= 0));
  manager.forget(c.id); assert.equal(transport.terminated, 0); manager.close(); transport.close();
});

test('synchronous connector failure retains one failed initial slot without reconnecting', () => {
  const c = call(); let attempts = 0;
  const manager = createInboundPreconnect(() => { attempts++; throw new Error('connector_failed'); });
  manager.begin(c); manager.begin(c); admit(c);
  assert.throws(() => manager.factory(c)!(), /inbound_preconnect_unavailable/);
  assert.equal(attempts, 1); manager.close();
});

function outboundCall() {
  return { id: '55555555-5555-4555-8555-555555555555', callSid: 'CA' + '2'.repeat(32), direction: 'outbound' } as unknown as BrokerCall;
}
function bindOutbound(c: BrokerCall) {
  c.primary = { bindingId: '66666666-6666-4666-8666-666666666666' } as BrokerCall['primary'];
  c.mediaStartedAt = new Date().toISOString(); return c;
}

test('outbound answer warms only the socket; a bound call adopts it and session.start is the first command', async () => {
  const transport = new PreconnectTransport(), c = outboundCall(); let connects = 0;
  const timings: InboundTransportTiming[] = [];
  const manager = createInboundPreconnect(() => { connects++; return transport; }, event => timings.push(event));
  manager.beginOutbound(c); transport.open();
  assert.deepEqual(transport.sent, [], 'no session, instructions, history or audio before binding and media');
  bindOutbound(c);
  const live = session(c, manager.factory(c)! as () => PreconnectTransport);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(transport.sent[0]?.type, 'session.start');
  assert.equal(connects, 1);
  assert.deepEqual(timings.map(event => event.stage), ['connecting', 'open', 'adopted']);
  await live.close(); manager.close();
});

for (const state of ['unbound', 'no_media', 'ended', 'inbound', 'callSid'] as const) test(`outbound adoption with ${state} discards the warm socket and connects fresh`, () => {
  const warm = new PreconnectTransport(), fresh = new PreconnectTransport(), c = outboundCall();
  let connects = 0;
  const manager = createInboundPreconnect(() => (++connects === 1 ? warm : fresh));
  manager.beginOutbound(c); bindOutbound(c);
  if (state === 'unbound') c.primary = undefined;
  if (state === 'no_media') c.mediaStartedAt = undefined;
  if (state === 'ended') c.endedAt = new Date().toISOString();
  if (state === 'inbound') (c as { direction: string }).direction = 'inbound';
  if (state === 'callSid') c.callSid = 'CA' + '3'.repeat(32);
  if (state === 'inbound') assert.throws(() => manager.factory(c)!(), /inbound_preconnect_unavailable/);
  else assert.equal(manager.factory(c)!(), fresh);
  assert.deepEqual(warm.sent, []); assert.equal(warm.terminated, 1); manager.close();
});

test('outbound warm socket is closed at its TTL when media never starts; an expired slot falls back to a normal connect', t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'] });
  const transport = new PreconnectTransport(), c = outboundCall();
  const manager = createInboundPreconnect(() => transport, undefined, 20_000);
  manager.beginOutbound(c); transport.open();
  t.mock.timers.tick(19_999); assert.equal(transport.terminated, 0);
  t.mock.timers.tick(1); assert.equal(transport.terminated, 1);
  assert.deepEqual(transport.sent, []);
  bindOutbound(c);
  assert.equal(manager.factory(c), undefined, 'the session uses its own connector');
  manager.close();
});

test('outbound warm socket that closes early is never adopted and binding failure cleanup terminates it', () => {
  const closedEarly = new PreconnectTransport(), c = outboundCall();
  const manager = createInboundPreconnect(() => closedEarly);
  manager.beginOutbound(c); closedEarly.open(); closedEarly.fail();
  assert.equal(closedEarly.terminated, 1); bindOutbound(c);
  assert.equal(manager.factory(c), undefined, 'a failed warm socket is never adopted; the session connects normally');
  assert.deepEqual(closedEarly.sent, []);
  const other = new PreconnectTransport(), d = { ...outboundCall(), id: '77777777-7777-4777-8777-777777777777' } as BrokerCall;
  const second = createInboundPreconnect(() => other);
  second.beginOutbound(d); other.open(); second.forget(d.id);
  assert.equal(other.terminated, 1); assert.deepEqual(other.sent, []);
  manager.close(); second.close();
});

function continued() {
  const c = call(), approved = c.inbound!.remote!;
  (c as { direction: string }).direction = 'inbound';
  c.inbound = { ...c.inbound!, waitUntil: new Date(Date.now() + 120_000).toISOString(),
    continuation: { observedAt: new Date().toISOString(), beginBodyDigest: 'e'.repeat(64), providerProof: {} } } as BrokerCall['inbound'];
  delete c.inbound!.remote; // Admission has not answered yet.
  return { c, approve() { c.inbound!.remote = { ...approved, expiresAt: new Date(Date.now() + 60_000).toISOString() }; return c; } };
}

test('the signed continuation opens only the socket; approval keeps it and Live adopts it with session.start first', async () => {
  const transport = new PreconnectTransport(), timings: InboundTransportTiming[] = []; let connects = 0;
  const manager = createInboundPreconnect(() => { connects++; return transport; }, event => timings.push(event));
  const { c, approve } = continued();
  manager.beginInbound(c); manager.beginInbound(c); transport.open();
  assert.equal(connects, 1); assert.deepEqual(transport.sent, [], 'no session, instructions, history or audio while admission runs');
  manager.begin(approve());
  assert.equal(connects, 1, 'approval keeps the continuation socket');
  admit(c);
  const live = session(c, manager.factory(c)! as () => PreconnectTransport);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(transport.sent[0]?.type, 'session.start');
  assert.deepEqual(timings.map(event => event.stage), ['connecting', 'open', 'adopted']);
  await live.close(); manager.close();
});

test('after approval the continuation socket is held to that approval like an approved socket', () => {
  for (const drift of ['approval', 'failed'] as const) {
    const transport = new PreconnectTransport(), manager = createInboundPreconnect(() => transport);
    const { c, approve } = continued();
    manager.beginInbound(c); manager.begin(approve()); admit(c);
    if (drift === 'approval' && c.inbound!.remote!.state === 'bound') c.inbound!.remote!.approvalRef.digest = 'e'.repeat(64);
    if (drift === 'failed') transport.fail();
    assert.throws(() => manager.factory(c)!(), /inbound_preconnect_unavailable/, drift);
    assert.deepEqual(transport.sent, []); assert.equal(transport.terminated, 1); manager.close();
  }
});

test('a continuation socket that fails or expires before approval is replaced by one fresh socket at approval', t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'] });
  for (const loss of ['failed', 'expired'] as const) {
    const sockets: PreconnectTransport[] = [];
    const manager = createInboundPreconnect(() => { const next = new PreconnectTransport(); sockets.push(next); return next; }, undefined, 20_000);
    const { c, approve } = continued();
    manager.beginInbound(c);
    if (loss === 'failed') sockets[0].fail(); else t.mock.timers.tick(20_000); // The warm TTL bounds an unapproved socket.
    assert.equal(sockets[0].terminated, 1, loss);
    manager.begin(approve()); manager.begin(c);
    assert.equal(sockets.length, 2, 'one replacement, opened at approval');
    admit(c); assert.equal(manager.factory(c)!(), sockets[1]);
    assert.deepEqual(sockets[0].sent, []); manager.close();
  }
});

test('a continuation socket never tied to an approval is adopted for the bound admission, or the session connects normally', () => {
  const warm = new PreconnectTransport(), manager = createInboundPreconnect(() => warm);
  const { c, approve } = continued();
  manager.beginInbound(c); approve(); admit(c); // A recovered binding: no approval reached this manager.
  assert.equal(manager.factory(c)!(), warm);
  const failed = new PreconnectTransport(), second = createInboundPreconnect(() => failed);
  const other = continued();
  second.beginInbound(other.c); failed.fail(); other.approve(); admit(other.c);
  assert.equal(second.factory(other.c), undefined, 'a failed warm socket leaves the session its own connector');
  manager.close(); second.close();
});

test('the continuation opens nothing for a leg without a continuation, a stopped, ended or later-mode wait, or after close', () => {
  for (const state of ['no_continuation', 'stopped', 'ended', 'media', 'mode', 'closed_binding', 'closed_manager'] as const) {
    let connects = 0;
    const manager = createInboundPreconnect(() => { connects++; return new PreconnectTransport(); });
    const { c } = continued();
    if (state === 'no_continuation') delete c.inbound!.continuation;
    if (state === 'stopped') c.inbound!.localStop = { observedAt: new Date().toISOString(), reason: 'caller_hangup' };
    if (state === 'ended') c.endedAt = new Date().toISOString();
    if (state === 'media') c.mediaStartedAt = new Date().toISOString();
    if (state === 'mode') c.inbound!.mode = { kind: 'undecided' } as NonNullable<BrokerCall['inbound']>['mode'];
    if (state === 'closed_binding') c.primaryClosed = { bindingId: 'b', callSid: c.callSid!, state: 'closed' };
    if (state === 'closed_manager') manager.close();
    manager.beginInbound(c);
    assert.equal(connects, 0, state);
    manager.close();
  }
});
