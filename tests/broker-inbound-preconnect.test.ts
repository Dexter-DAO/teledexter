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
