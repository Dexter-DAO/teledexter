import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import Twilio from 'twilio';
import { createInboundFlow } from '../src/broker/inbound.js';
import { inboundBeginRef, type InboundBeginIdentity, type InboundOutcome } from '../src/broker/inboundContract.js';
import { BrokerStore } from '../src/broker/store.js';
import type { PrimaryApiClient } from '../src/broker/primaryApi.js';

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
async function fixture(t: test.TestContext) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-inbound-flow-'));
  let store = new BrokerStore(directory);
  let clock = Date.now();
  const at = () => new Date(clock).toISOString();
  const config = { twilioAccountSid: 'AC' + '1'.repeat(32), twilioAuthToken: 'inbound-test-secret',
    publicOrigin: 'https://phone.example.test', from: '+12025550100' };
  const params = { AccountSid: config.twilioAccountSid, CallSid: 'CA' + '2'.repeat(32), From: '+12025550123',
    To: config.from, Direction: 'inbound', CallStatus: 'ringing' };
  const proof = (route: string) => ({ url: config.publicOrigin + route, params: { ...params },
    signature: Twilio.getExpectedTwilioSignature(config.twilioAuthToken, config.publicOrigin + route, params) });
  const original = proof('/broker/incoming-call');
  const bindingId = randomUUID(), attachmentId = randomUUID();
  const approvalRef = { attachmentId, approvalId: randomUUID(), digest: 'b'.repeat(64) };
  let remote: InboundOutcome | undefined;
  let beginIdentity: InboundBeginIdentity | undefined;
  let providerStatus = 'in-progress';
  let contextAllowed = true;
  let loseBegin = false, loseConsume = false, loseClose = false;
  let beginGate: ReturnType<typeof deferred<void>> | undefined;
  let contextGate: ReturnType<typeof deferred<void>> | undefined;
  let consumedAt: string | undefined, closedAt: string | undefined;
  let pendingAfterClose = false;
  const counts = { begin: 0, recover: 0, status: 0, consume: 0, context: 0, cancel: 0, close: 0, provider: 0, closeout: 0 };
  const remember = (begin: InboundBeginIdentity) => {
    if (beginIdentity) assert.deepEqual(begin, beginIdentity); else beginIdentity = structuredClone(begin);
  };
  const primary = {
    async beginInbound(begin: InboundBeginIdentity) {
      counts.begin++; remember(begin);
      if (beginGate) await beginGate.promise;
      remote ??= { attachmentId, beginRef: inboundBeginRef(begin), state: 'pending', expiresAt: new Date(clock + 300_000).toISOString() };
      if (loseBegin) { loseBegin = false; throw new Error('lost_begin_response'); }
      return structuredClone(remote);
    },
    async recoverInbound(begin: InboundBeginIdentity) {
      counts.recover++; remember(begin);
      return remote ? structuredClone(remote) : { state: 'absent' as const, beginRef: inboundBeginRef(begin) };
    },
    async inboundStatus(id: string) { counts.status++; assert.equal(id, attachmentId); assert.ok(remote); return structuredClone(remote); },
    async cancelInbound(begin: InboundBeginIdentity) {
      counts.cancel++; remember(begin);
      if (remote?.state !== 'bound' && remote?.state !== 'cancelled' && remote?.state !== 'expired') remote = {
        attachmentId, beginRef: inboundBeginRef(begin), state: 'cancelled', cancelledAt: at(),
        expiresAt: remote && 'expiresAt' in remote ? remote.expiresAt : null,
      };
      return structuredClone(remote!);
    },
    async openInboundCall(savedProof: typeof original, ref: typeof approvalRef) {
      counts.consume++; assert.deepEqual(savedProof, original); assert.deepEqual(ref, approvalRef);
      assert.ok(remote);
      consumedAt ??= at();
      remote = { attachmentId, beginRef: remote.beginRef, approvalRef, bindingId, state: 'bound', consumedAt, callState: 'open' };
      if (loseConsume) { loseConsume = false; throw new Error('lost_consume_response'); }
      return primary.getContext(bindingId);
    },
    async getContext(id: string) {
      counts.context++; assert.equal(id, bindingId);
      if (contextGate) await contextGate.promise;
      if (!contextAllowed) throw new Error('authority_unavailable');
      return { bindingId, callSid: params.CallSid, state: 'open' as const,
        context: { primaryDexterId: randomUUID(), threadId: randomUUID(), displayName: 'Branch', messages: [] } };
    },
    async closeInbound(id: string, input: { begin: InboundBeginIdentity; approvalRef: typeof approvalRef; bindingId: string }) {
      counts.close++; assert.equal(id, attachmentId); assert.deepEqual(input.begin, beginIdentity);
      assert.deepEqual(input.approvalRef, approvalRef); assert.equal(input.bindingId, bindingId);
      assert.equal(remote?.state, 'bound'); closedAt ??= at();
      remote = { ...remote!, state: 'bound', approvalRef, bindingId, consumedAt: consumedAt!, callState: 'closed' };
      if (loseClose) { loseClose = false; throw new Error('lost_close_response'); }
      return { attachmentId, beginRef: remote.beginRef, approvalRef, bindingId, callSid: params.CallSid, state: 'closed' as const, closedAt };
    },
  };
  function build() {
    return createInboundFlow({ config, store, primary: primary as unknown as PrimaryApiClient, now: () => clock,
      async fetchCall(sid) { counts.provider++; assert.equal(sid, params.CallSid); return { sid, accountSid: config.twilioAccountSid,
        direction: 'inbound', from: params.From, to: params.To, status: providerStatus }; },
      async closeBound(call) {
        counts.closeout++;
        if (call.primaryClosed) { store.update(call.id, { primaryClosePending: false }); return; }
        const bound = call.inbound!.remote;
        assert.equal(bound?.state, 'bound'); if (bound?.state !== 'bound') throw new Error('missing_bound');
        store.transitionInbound(call.id, { type: 'close_requested', observedAt: at() });
        const receipt = await primary.closeInbound(bound.attachmentId, { begin: call.inbound!.begin, approvalRef: bound.approvalRef, bindingId: bound.bindingId });
        store.transitionInbound(call.id, { type: 'closure', receipt, observedAt: at() });
        if (pendingAfterClose) store.update(call.id, { primaryClosePending: true });
      },
    });
  }
  let flow = build();
  t.after(async () => { await flow.drain(); store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  return {
    get flow() { return flow; }, get store() { return store; }, config, params, original, proof, counts, directory,
    clock: () => clock, advance: (ms: number) => { clock += ms; },
    approve() { assert.ok(remote && remote.state === 'pending'); remote = { ...remote, state: 'approved', approvalRef }; },
    remote: () => remote,
    loseBegin() { loseBegin = true; }, loseConsume() { loseConsume = true; }, loseClose() { loseClose = true; },
    refuseContext() { contextAllowed = false; }, pendingAfterClose() { pendingAfterClose = true; },
    gateBegin() { beginGate = deferred<void>(); return beginGate; },
    gateContext() { contextGate = deferred<void>(); return contextGate; },
    providerStatus(status: string) { providerStatus = status; },
    restart() { store.close(); store = new BrokerStore(directory); flow = build(); },
  };
}

test('pending inbound keeps original leg and proof; approval consumes that same call', async t => {
  const f = await fixture(t);
  const first = await f.flow.accept(f.original);
  assert.equal(first.inbound!.remote?.state, 'pending');
  assert.equal(first.primary, undefined);
  assert.equal(f.counts.context, 0);
  f.approve();
  const next = await f.flow.resume(first.id, f.proof(`/broker/inbound/${first.id}/continue`));
  assert.equal(next.id, first.id);
  assert.equal(next.callSid, first.callSid);
  assert.equal(next.inbound!.remote?.state, 'bound');
  assert.ok(f.flow.mediaAllowed(next));
  assert.equal(Date.parse(next.inbound!.mediaStartBy!), f.clock() + 30_000);
  assert.equal(f.counts.consume, 1);
  assert.deepEqual(next.inbound!.begin.providerProof, f.original);
});

test('lost begin response recovers the original attachment without another begin', async t => {
  const f = await fixture(t); f.loseBegin();
  await assert.rejects(f.flow.accept(f.original), /lost_begin/);
  const saved = f.store.list()[0];
  f.advance(15_000);
  const next = await f.flow.tick(saved.id);
  assert.equal(next.inbound!.remote?.state, 'pending');
  assert.equal(next.inbound!.waitUntil, saved.inbound!.waitUntil);
  assert.equal(f.counts.begin, 1); assert.equal(f.counts.recover, 1);
});

test('local stop persists while begin is delayed and fences its eventual reply', async t => {
  const f = await fixture(t); const gate = f.gateBegin();
  const beginning = f.flow.accept(f.original);
  await new Promise(resolve => setImmediate(resolve));
  const id = f.store.list()[0].id;
  const stopping = f.flow.stop(id, 'caller_hangup');
  assert.equal(f.store.get(id)!.inbound!.localStop!.reason, 'caller_hangup');
  gate.resolve();
  await beginning; await stopping;
  assert.equal(f.store.get(id)!.inbound!.remote?.state, 'cancelled');
  assert.equal(f.counts.consume, 0); assert.equal(f.counts.context, 0);
});

test('lost consume reply and authority loss recover and close without private context', async t => {
  const f = await fixture(t); const first = await f.flow.accept(f.original); f.approve(); f.loseConsume();
  await assert.rejects(f.flow.tick(first.id), /lost_consume/);
  f.refuseContext();
  const closed = await f.flow.stop(first.id, 'caller_hangup');
  assert.equal(closed.inbound!.remote?.state, 'bound');
  assert.equal(closed.inbound!.closure?.state, 'closed');
  assert.equal(closed.primary, undefined); assert.equal(closed.transcriptCapture, undefined);
  assert.equal(f.counts.consume, 1); assert.equal(f.counts.context, 0);
  const exact = closed.inbound!.closure;
  f.restart(); await f.flow.stop(first.id, 'service_shutdown');
  assert.deepEqual(f.store.get(first.id)!.inbound!.closure, exact);
  assert.equal(f.counts.close, 1);
});

test('lost closure response stays unresolved until its original receipt returns', async t => {
  const f = await fixture(t); const first = await f.flow.accept(f.original); f.approve(); f.loseConsume();
  await assert.rejects(f.flow.tick(first.id)); f.loseClose();
  await assert.rejects(f.flow.stop(first.id, 'caller_hangup'), /lost_close/);
  assert.equal(f.store.get(first.id)!.primaryClosed, undefined);
  assert.ok(f.store.get(first.id)!.inbound!.closureRequestedAt);
  f.advance(10_000); f.restart(); await f.flow.tick(first.id);
  assert.equal(f.store.get(first.id)!.inbound!.closure?.closedAt, new Date(f.clock() - 10_000).toISOString());
  assert.equal(f.counts.consume, 1);
});

test('delayed context cannot renew server anchored media grace', async t => {
  const f = await fixture(t); const first = await f.flow.accept(f.original); f.approve(); const gate = f.gateContext();
  const consuming = f.flow.tick(first.id); await new Promise(resolve => setImmediate(resolve));
  const consumed = f.clock(); f.advance(31_000); gate.resolve();
  const next = await consuming;
  assert.equal(next.primary, undefined); assert.equal(f.flow.mediaAllowed(next), false);
  assert.equal(next.inbound!.mediaStartBy, new Date(consumed + 30_000).toISOString());
  assert.equal(next.inbound!.localStop?.reason, 'media_start_expired');
  assert.equal(next.inbound!.closure?.state, 'closed');
});

test('restart after local waiting budget cancels rather than issuing a new wait', async t => {
  const f = await fixture(t); const first = await f.flow.accept(f.original); const deadline = first.inbound!.waitUntil;
  f.advance(120_000); f.restart();
  const stopped = await f.flow.tick(first.id);
  assert.equal(stopped.inbound!.waitUntil, deadline);
  assert.equal(stopped.inbound!.localStop?.reason, 'broker_cancelled');
  assert.equal(stopped.inbound!.remote?.state, 'cancelled');
  assert.equal(f.counts.begin, 1); assert.equal(f.counts.consume, 0);
});

test('signed continuation cannot move caller or replace original proof', async t => {
  const f = await fixture(t); const first = await f.flow.accept(f.original);
  const proof = f.proof(`/broker/inbound/${first.id}/continue`);
  proof.params.From = '+12025550999';
  proof.signature = Twilio.getExpectedTwilioSignature(f.config.twilioAuthToken, proof.url, proof.params);
  assert.throws(() => f.flow.resume(first.id, proof), /identity_mismatch/);
  assert.deepEqual(f.store.get(first.id)!.inbound!.begin.providerProof, f.original);
  assert.equal(f.counts.consume, 0);
});

test('provider terminal before consumption closes waiting without account access', async t => {
  const f = await fixture(t); const first = await f.flow.accept(f.original); f.approve(); f.providerStatus('completed');
  const ended = await f.flow.tick(first.id);
  assert.equal(ended.status, 'completed');
  assert.equal(ended.inbound!.remote?.state, 'cancelled');
  assert.equal(f.counts.consume, 0); assert.equal(f.counts.context, 0);
});

test('a local stop during context retrieval wins over the late private reply', async t => {
  const f = await fixture(t); const first = await f.flow.accept(f.original); f.approve(); const gate = f.gateContext();
  const consuming = f.flow.tick(first.id); await new Promise(resolve => setImmediate(resolve));
  const stopping = f.flow.stop(first.id, 'operator_requested'); gate.resolve();
  await consuming; await stopping;
  const ended = f.store.get(first.id)!;
  assert.equal(ended.primary, undefined); assert.equal(ended.inbound!.closure?.state, 'closed');
  assert.equal(f.flow.mediaAllowed(ended), false);
});

test('a started media session cannot be started again after crash recovery', async t => {
  const f = await fixture(t); const first = await f.flow.accept(f.original); f.approve(); const bound = await f.flow.tick(first.id);
  f.store.update(first.id, { mediaStartedAt: new Date(f.clock()).toISOString() });
  f.restart();
  assert.equal(f.flow.mediaAllowed(f.store.get(first.id)!), false);
  assert.equal(bound.inbound!.mediaStartBy, f.store.get(first.id)!.inbound!.mediaStartBy);
});


test('persisted closure still resumes pending reconciliation without another close', async t => {
  const f = await fixture(t); const call = await f.flow.accept(f.original); f.approve();
  await f.flow.tick(call.id); f.pendingAfterClose(); await f.flow.stop(call.id, 'caller_hangup');
  assert.equal(f.store.get(call.id)!.primaryClosePending, true);
  const original = f.store.get(call.id)!.inbound!.closure;
  f.restart(); await f.flow.tick(call.id);
  assert.equal(f.store.get(call.id)!.primaryClosePending, false);
  assert.deepEqual(f.store.get(call.id)!.inbound!.closure, original);
  assert.equal(f.counts.close, 1); assert.equal(f.counts.closeout, 2);
});
