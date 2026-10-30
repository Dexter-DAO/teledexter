import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import Twilio from 'twilio';
import { createInboundFlow } from '../src/broker/inbound.js';
import type { InboundTimingEvent } from '../src/broker/inboundTiming.js';
import { inboundBeginRef, type InboundBeginIdentity, type InboundOutcome } from '../src/broker/inboundContract.js';
import { BrokerStore } from '../src/broker/store.js';
import { PrimaryApiError, type PrimaryApiClient } from '../src/broker/primaryApi.js';

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
    To: config.from, Direction: 'inbound', CallStatus: 'ringing', StirVerstat: 'TN-Validation-Passed-A' };
  const proof = (route: string) => ({ url: config.publicOrigin + route, params: { ...params },
    signature: Twilio.getExpectedTwilioSignature(config.twilioAuthToken, config.publicOrigin + route, params) });
  const original = proof('/broker/incoming-call');
  const bindingId = randomUUID(), attachmentId = randomUUID();
  const primaryDexterId = randomUUID(), threadId = randomUUID();
  const approvalRef = { attachmentId, approvalId: randomUUID(), digest: 'b'.repeat(64) };
  let remote: InboundOutcome | undefined;
  let beginIdentity: InboundBeginIdentity | undefined;
  let providerStatus = 'in-progress';
  let providerGate: ReturnType<typeof deferred<void>> | undefined;
  let providerFailure: Error | undefined;
  let providerMismatch = false;
  let contextAllowed = true;
  let loseBegin = false, loseConsume = false, loseClose = false;
  let includeOutcome = false;
  let beginGate: ReturnType<typeof deferred<void>> | undefined;
  let contextGate: ReturnType<typeof deferred<void>> | undefined;
  let consumedAt: string | undefined, closedAt: string | undefined;
  let pendingAfterClose = false;
  let beginError: Error | undefined, pendingBeginError: Error | undefined;
  let consumeError: Error | undefined, contextError: Error | undefined; let refuseBeforeConsume = false;
  const counts = { begin: 0, recover: 0, status: 0, consume: 0, context: 0, cancel: 0, close: 0, provider: 0, closeout: 0 };
  const timings: InboundTimingEvent[] = [];
  const contexts: Array<{ receivedAt: number; handedAt: number }> = [];
  const remember = (begin: InboundBeginIdentity) => {
    if (beginIdentity) assert.deepEqual(begin, beginIdentity); else beginIdentity = structuredClone(begin);
  };
  const primary = {
    async beginInbound(begin: InboundBeginIdentity) {
      counts.begin++; remember(begin);
      if (beginGate) await beginGate.promise;
      if (beginError) throw beginError;
      remote ??= { attachmentId, beginRef: inboundBeginRef(begin), state: 'pending', expiresAt: new Date(clock + 300_000).toISOString() };
      if (pendingBeginError) throw pendingBeginError;
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
      counts.consume++; assert.deepEqual(savedProof, original);
      if (refuseBeforeConsume && consumeError) throw consumeError;
      assert.deepEqual(ref, approvalRef);
      assert.ok(remote);
      consumedAt ??= at();
      remote = { attachmentId, beginRef: remote.beginRef, approvalRef, bindingId, state: 'bound', consumedAt, callState: 'open' };
      if (consumeError) throw consumeError;
      if (loseConsume) { loseConsume = false; throw new Error('lost_consume_response'); }
      const context = await primary.getContext(bindingId);
      return { ...context, ...(includeOutcome ? { inboundOutcome: structuredClone(remote) } : {}) };
    },
    async getContext(id: string) {
      counts.context++; assert.equal(id, bindingId);
      if (contextGate) await contextGate.promise;
      if (contextError) throw contextError;
      if (!contextAllowed) throw new Error('authority_unavailable');
      return { bindingId, callSid: params.CallSid, state: 'open' as const,
        context: { primaryDexterId, threadId, displayName: 'Avery Example', messages: [] } };
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
      onTiming: event => timings.push(event),
      onContext: (_call, _context, receivedAt) => { contexts.push({ receivedAt, handedAt: clock }); },
      async fetchCall(sid) { counts.provider++; assert.equal(sid, params.CallSid);
        if (providerGate) await providerGate.promise;
        if (providerFailure) { const failure = providerFailure; providerFailure = undefined; throw failure; }
        return { sid, accountSid: config.twilioAccountSid,
        direction: 'inbound', from: params.From, to: providerMismatch ? '+12025550999' : params.To, status: providerStatus }; },
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
  t.after(async () => { beginGate?.resolve(); contextGate?.resolve(); providerGate?.resolve(); await flow.drain(); store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  return {
    get flow() { return flow; }, get store() { return store; }, config, params, original, proof, counts, timings, contexts, directory,
    clock: () => clock, advance: (ms: number) => { clock += ms; },
    approve() { assert.ok(remote && remote.state === 'pending'); remote = { ...remote, state: 'approved', approvalRef }; },
    remote: () => remote,
    async begin() { const first = await flow.accept(original); return flow.resume(first.id, proof(`/broker/inbound/${first.id}/continue`)); },
    beginError(error?: Error) { beginError = error; },
    pendingBeginError(error?: Error) { pendingBeginError = error; },
    consumeError(error: Error, beforeCommit: boolean) { consumeError = error; refuseBeforeConsume = beforeCommit; },
    contextError(error: Error) { contextError = error; },
    restoreHistorical(call: ReturnType<BrokerStore['list']>[number]) {
      store.close(); fs.writeFileSync(path.join(directory, 'calls.json'), JSON.stringify({ version: 1, calls: { [call.id]: call } }));
      store = new BrokerStore(directory); flow = build();
    },
    loseBegin() { loseBegin = true; }, loseConsume() { loseConsume = true; }, loseClose() { loseClose = true; },
    includeOutcome() { includeOutcome = true; },
    refuseContext() { contextAllowed = false; }, pendingAfterClose() { pendingAfterClose = true; },
    gateBegin() { beginGate = deferred<void>(); return beginGate; },
    gateContext() { contextGate = deferred<void>(); return contextGate; },
    providerStatus(status: string) { providerStatus = status; },
    gateProvider() { providerGate = deferred<void>(); return providerGate; },
    failProviderOnce(error: Error) { providerFailure = error; },
    mismatchProvider() { providerMismatch = true; },
    restart() { store.close(); store = new BrokerStore(directory); flow = build(); },
  };
}

test('pending inbound keeps original leg and proof; approval consumes that same call', async t => {
  const f = await fixture(t);
  const first = await f.begin();
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

test('negotiated successful consumption removes one recovery read and preserves the persisted grace anchor', async t => {
  const f = await fixture(t); f.includeOutcome();
  const first = await f.begin(); f.approve();
  const before = f.counts.recover;
  const next = await f.flow.tick(first.id);
  assert.ok(f.flow.mediaAllowed(next));
  assert.equal(f.counts.recover, before);
  assert.equal(f.counts.consume, 1);
  assert.equal(next.inbound!.mediaStartBy, new Date(f.clock() + 30_000).toISOString());
  assert.equal(f.timings.some(event => event.stage === 'recover_binding'), false);
  f.restart(); f.advance(5_000);
  const recovered = await f.flow.tick(first.id);
  assert.equal(recovered.inbound!.mediaStartBy, next.inbound!.mediaStartBy);
  assert.equal(f.counts.consume, 1);
});

test('lost negotiated response still recovers the original consumption after restart', async t => {
  const f = await fixture(t); f.includeOutcome();
  const first = await f.begin(); f.approve(); f.loseConsume();
  await assert.rejects(f.flow.tick(first.id), /lost_consume_response/);
  const consumed = f.clock(), before = f.counts.recover + f.counts.status;
  f.restart(); f.advance(5_000);
  const recovered = await f.flow.tick(first.id);
  assert.ok(f.flow.mediaAllowed(recovered));
  assert.equal(f.counts.recover + f.counts.status, before + 1);
  assert.equal(f.counts.consume, 1);
  assert.equal(recovered.inbound!.mediaStartBy, new Date(consumed + 30_000).toISOString());
});

test('slow negotiated context cannot renew expired media grace', async t => {
  const f = await fixture(t); f.includeOutcome();
  const first = await f.begin(); f.approve(); const gate = f.gateContext();
  const pending = f.flow.tick(first.id);
  while (!f.counts.context) await new Promise(resolve => setImmediate(resolve));
  const consumed = f.clock(); f.advance(31_000); gate.resolve();
  const next = await pending;
  assert.equal(f.flow.mediaAllowed(next), false);
  assert.equal(next.inbound!.mediaStartBy, new Date(consumed + 30_000).toISOString());
  assert.equal(f.counts.close, 1);
});

test('setup timing identifies each real admission step and retains separate failed and recovered attempts', async t => {
  const f = await fixture(t), first = await f.begin(); f.approve(); f.loseConsume();
  await assert.rejects(f.flow.resume(first.id, f.proof(`/broker/inbound/${first.id}/continue`)), /lost_consume_response/);
  const failed = f.timings.find(event => event.stage === 'open_context' && event.phase === 'failed');
  assert.ok(failed);
  f.restart();
  const recovered = await f.flow.tick(first.id);
  assert.ok(f.flow.mediaAllowed(recovered));
  assert.equal(f.counts.consume, 1);
  const refreshed = f.timings.find(event => event.stage === 'refresh_context' && event.phase === 'completed');
  assert.ok(refreshed);
  assert.notEqual(failed.attemptId, refreshed.attemptId);
  assert.ok(f.timings.some(event => event.stage === 'recover_attachment' && event.phase === 'completed')
    || f.timings.some(event => event.stage === 'poll_attachment' && event.phase === 'completed'));
  for (const end of f.timings.filter(event => event.phase !== 'started')) {
    const starts = f.timings.filter(event => event.spanId === end.spanId && event.phase === 'started');
    assert.equal(starts.length, 1); assert.equal(starts[0].callId, first.id);
    assert.ok(Number.isFinite(end.elapsedMs) && end.elapsedMs! >= 0);
  }
  const encoded = JSON.stringify(f.timings);
  for (const secret of [f.params.From, f.params.To, f.params.CallSid, f.original.signature, f.config.twilioAuthToken])
    assert.equal(encoded.includes(secret), false);
});

test('lost begin response recovers a sealed approval without another begin', async t => {
  const f = await fixture(t); f.loseBegin();
  await assert.rejects(f.begin(), /lost_begin/);
  const saved = f.store.list()[0];
  f.approve(); f.advance(15_000);
  const next = await f.flow.tick(saved.id);
  assert.equal(next.inbound!.remote?.state, 'bound');
  assert.equal(next.inbound!.waitUntil, saved.inbound!.waitUntil);
  assert.equal(f.counts.begin, 1); assert.equal(f.counts.recover, 2);
});

test('local stop persists while begin is delayed and fences its eventual reply', async t => {
  const f = await fixture(t); const gate = f.gateBegin();
  const beginning = f.begin();
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
  const f = await fixture(t); const first = await f.begin(); f.approve(); f.loseConsume();
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
  const f = await fixture(t); const first = await f.begin(); f.approve(); f.loseConsume();
  await assert.rejects(f.flow.tick(first.id)); f.loseClose();
  await assert.rejects(f.flow.stop(first.id, 'caller_hangup'), /lost_close/);
  assert.equal(f.store.get(first.id)!.primaryClosed, undefined);
  assert.ok(f.store.get(first.id)!.inbound!.closureRequestedAt);
  f.advance(10_000); f.restart(); await f.flow.tick(first.id);
  assert.equal(f.store.get(first.id)!.inbound!.closure?.closedAt, new Date(f.clock() - 10_000).toISOString());
  assert.equal(f.counts.consume, 1);
});

test('delayed context cannot renew server anchored media grace', async t => {
  const f = await fixture(t); const first = await f.begin(); f.approve(); const gate = f.gateContext();
  const consuming = f.flow.tick(first.id); await new Promise(resolve => setImmediate(resolve));
  const consumed = f.clock(); f.advance(31_000); gate.resolve();
  const next = await consuming;
  assert.equal(next.primary, undefined); assert.equal(f.flow.mediaAllowed(next), false);
  assert.equal(next.inbound!.mediaStartBy, new Date(consumed + 30_000).toISOString());
  assert.equal(next.inbound!.localStop?.reason, 'media_start_expired');
  assert.equal(next.inbound!.closure?.state, 'closed');
});

test('restart after local waiting budget cancels rather than issuing a new wait', async t => {
  const f = await fixture(t); const first = await f.begin(); const deadline = first.inbound!.waitUntil;
  f.advance(120_000); f.restart();
  const stopped = await f.flow.tick(first.id);
  assert.equal(stopped.inbound!.waitUntil, deadline);
  assert.equal(stopped.inbound!.localStop?.reason, 'broker_cancelled');
  assert.equal(stopped.inbound!.remote?.state, 'cancelled');
  assert.equal(f.counts.begin, 1); assert.equal(f.counts.consume, 0);
});

test('signed continuation cannot move caller or replace original proof', async t => {
  const f = await fixture(t); const first = await f.begin();
  const proof = f.proof(`/broker/inbound/${first.id}/continue`);
  proof.params.From = '+12025550999';
  proof.signature = Twilio.getExpectedTwilioSignature(f.config.twilioAuthToken, proof.url, proof.params);
  assert.throws(() => f.flow.resume(first.id, proof), /identity_mismatch/);
  assert.deepEqual(f.store.get(first.id)!.inbound!.begin.providerProof, f.original);
  assert.equal(f.counts.consume, 0);
});

test('a leg that ended while its approval is consumed: the API refuses the consume and the wait is cancelled', async t => {
  const f = await fixture(t); const first = await f.begin(); f.approve(); f.providerStatus('completed');
  // The API's consume makes its own active-call check and refuses before binding.
  f.consumeError(new PrimaryApiError('http_error', 403, { code: 'call_not_active', retryable: false }), true);
  const ended = await f.flow.tick(first.id);
  assert.equal(ended.status, 'completed'); assert.equal(ended.inbound!.localStop?.reason, 'provider_terminal');
  assert.equal(ended.inbound!.remote?.state, 'cancelled');
  assert.equal(f.counts.consume, 1); assert.equal(f.counts.context, 0); assert.equal(ended.primary, undefined);
});

test('a leg that ended while a consume still committed closes that binding without recording its context', async t => {
  const f = await fixture(t); const first = await f.begin(); f.approve(); f.providerStatus('completed');
  const ended = await f.flow.tick(first.id);
  assert.equal(ended.status, 'completed'); assert.equal(ended.inbound!.localStop?.reason, 'provider_terminal');
  assert.equal(ended.inbound!.remote?.state, 'bound'); assert.equal(ended.inbound!.closure?.state, 'closed');
  assert.equal(ended.primary, undefined, 'context is recorded only when the provider check passes too');
  assert.equal(f.flow.mediaAllowed(ended), false);
  assert.equal(f.counts.consume, 1); assert.equal(f.counts.close, 1);
});

test('the provider check runs alongside the consume; context is recorded only once both pass', async t => {
  const f = await fixture(t); f.includeOutcome();
  const first = await f.begin(); f.approve();
  const gate = f.gateProvider();
  const ticking = f.flow.tick(first.id);
  for (let i = 0; i < 100 && f.store.get(first.id)!.inbound!.remote?.state !== 'bound'; i++) await new Promise(resolve => setImmediate(resolve));
  // The consume returned (bound, context in hand) while the provider read is still out.
  assert.equal(f.counts.provider, 1); assert.equal(f.counts.consume, 1);
  assert.equal(f.store.get(first.id)!.inbound!.remote?.state, 'bound');
  assert.equal(f.store.get(first.id)!.primary, undefined);
  assert.equal(f.flow.mediaAllowed(f.store.get(first.id)!), false);
  const consumedAt = f.clock();
  f.advance(1_500); // A slow provider read: the page in hand ages meanwhile.
  gate.resolve();
  const admitted = await ticking;
  assert.ok(f.flow.mediaAllowed(admitted)); assert.equal(admitted.primary?.bindingId, (admitted.inbound!.remote as { bindingId: string }).bindingId);
  assert.equal(f.counts.provider, 1); assert.equal(f.counts.consume, 1); assert.equal(f.counts.context, 1);
  // The startup page's age counts from the consume reply, not from the later handoff.
  assert.deepEqual(f.contexts, [{ receivedAt: consumedAt, handedAt: consumedAt + 1_500 }]);
  const started = f.timings.filter(event => event.phase === 'started').map(event => event.stage);
  assert.ok(started.indexOf('open_context') >= 0 && started.indexOf('provider_identity') >= 0);
  const providerDone = f.timings.findIndex(event => event.stage === 'provider_identity' && event.phase === 'completed');
  const consumeStart = f.timings.findIndex(event => event.stage === 'open_context' && event.phase === 'started');
  assert.ok(consumeStart < providerDone, 'the consume starts before the provider read finishes');
});

test('a provider leg that is not the signed one ends the wait and closes the binding the consume made', async t => {
  const f = await fixture(t); f.includeOutcome();
  const first = await f.begin(); f.approve(); f.mismatchProvider();
  const ended = await f.flow.tick(first.id);
  assert.equal(ended.inbound!.localStop?.reason, 'provider_identity_mismatch');
  assert.equal(ended.inbound!.remote?.state, 'bound'); assert.equal(ended.inbound!.closure?.state, 'closed');
  assert.equal(ended.primary, undefined); assert.equal(f.contexts.length, 0);
  assert.equal(f.counts.consume, 1); assert.equal(f.counts.close, 1);
});

test('a failed provider read after the consume leaves the binding unused until the next poll passes the check', async t => {
  const f = await fixture(t); f.includeOutcome();
  const first = await f.begin(); f.approve();
  f.failProviderOnce(new Error('provider_read_unavailable'));
  const unused = await f.flow.tick(first.id);
  assert.equal(unused.inbound!.remote?.state, 'bound'); assert.equal(unused.primary, undefined);
  assert.equal(f.flow.mediaAllowed(unused), false);
  assert.equal(unused.failure?.message, 'provider_read_unavailable');
  assert.equal(unused.inbound!.localStop, undefined, 'a provider read failure does not end the wait');
  const admitted = await f.flow.tick(first.id);
  assert.ok(f.flow.mediaAllowed(admitted));
  assert.equal(f.counts.consume, 1); assert.equal(f.counts.provider, 2);
  assert.equal(f.counts.context, 2, 'the next poll reads context again after its own provider check');
});

test('a local stop during context retrieval wins over the late private reply', async t => {
  const f = await fixture(t); const first = await f.begin(); f.approve(); const gate = f.gateContext();
  const consuming = f.flow.tick(first.id); await new Promise(resolve => setImmediate(resolve));
  const stopping = f.flow.stop(first.id, 'operator_requested'); gate.resolve();
  await consuming; await stopping;
  const ended = f.store.get(first.id)!;
  assert.equal(ended.primary, undefined); assert.equal(ended.inbound!.closure?.state, 'closed');
  assert.equal(f.flow.mediaAllowed(ended), false);
});

test('a started media session cannot be started again after crash recovery', async t => {
  const f = await fixture(t); const first = await f.begin(); f.approve(); const bound = await f.flow.tick(first.id);
  f.store.update(first.id, { mediaStartedAt: new Date(f.clock()).toISOString() });
  f.restart();
  assert.equal(f.flow.mediaAllowed(f.store.get(first.id)!), false);
  assert.equal(bound.inbound!.mediaStartBy, f.store.get(first.id)!.inbound!.mediaStartBy);
});


test('persisted closure still resumes pending reconciliation without another close', async t => {
  const f = await fixture(t); const call = await f.begin(); f.approve();
  await f.flow.tick(call.id); f.pendingAfterClose(); await f.flow.stop(call.id, 'caller_hangup');
  assert.equal(f.store.get(call.id)!.primaryClosePending, true);
  const original = f.store.get(call.id)!.inbound!.closure;
  f.restart(); await f.flow.tick(call.id);
  assert.equal(f.store.get(call.id)!.primaryClosePending, false);
  assert.deepEqual(f.store.get(call.id)!.inbound!.closure, original);
  assert.equal(f.counts.close, 1); assert.equal(f.counts.closeout, 2);
});

test('reservation and repeated maintenance stay unadmitted until an authenticated same-leg continuation', async t => {
  const f = await fixture(t);
  const first = await f.flow.accept(f.original);
  assert.equal(first.inbound!.remote, undefined);
  assert.equal(first.inbound!.continuation, undefined);
  await Promise.all([f.flow.tick(first.id), f.flow.accept(f.original)]);
  assert.equal(f.counts.begin, 0); assert.equal(f.counts.recover, 0); assert.equal(f.counts.provider, 0);
  f.restart(); await f.flow.tick(first.id);
  assert.equal(f.counts.begin, 0); assert.equal(f.counts.context, 0);
  const continued = await f.flow.resume(first.id, f.proof(`/broker/inbound/${first.id}/continue`));
  assert.equal(f.counts.begin, 1); assert.equal(continued.inbound!.remote?.state, 'pending');
  assert.deepEqual(continued.inbound!.begin, first.inbound!.begin);
  assert.equal(continued.inbound!.continuation?.beginBodyDigest, first.inbound!.beginBodyDigest);
  assert.equal(continued.inbound!.waitUntil, first.inbound!.waitUntil);
  assert.equal(continued.inbound!.begin.providerProof.params.StirVerstat, 'TN-Validation-Passed-A');
});

test('pending GET can observe but original proof and retained continuation are required for fresh pending POST', async t => {
  const f = await fixture(t); const first = await f.begin();
  const historical = structuredClone(first); delete historical.inbound!.continuation;
  f.restoreHistorical(historical);
  const bytes = fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8');
  await f.flow.tick(first.id);
  assert.equal(f.counts.status, 1); assert.equal(f.counts.begin, 1);
  assert.equal(fs.readFileSync(path.join(f.directory, 'calls.json'), 'utf8'), bytes);
  const next = await f.flow.resume(first.id, f.proof(`/broker/inbound/${first.id}/continue`));
  assert.equal(f.counts.begin, 2); assert.equal(next.inbound!.remote?.state, 'pending');
  assert.deepEqual(next.inbound!.begin, historical.inbound!.begin);
  assert.equal(next.inbound!.waitUntil, historical.inbound!.waitUntil);
});

test('historical unknown attempt recovers absent without acquiring fresh authority from its digest', async t => {
  const f = await fixture(t);
  const first = await f.flow.accept(f.original);
  const historical = structuredClone(first); historical.inbound!.beginAttemptedAt = first.createdAt;
  f.restoreHistorical(historical); await f.flow.tick(first.id);
  assert.equal(f.counts.recover, 1); assert.equal(f.counts.begin, 0);
  assert.equal(f.store.get(first.id)!.inbound!.remote?.state, 'absent');
  f.advance(120_000); await f.flow.tick(first.id);
  assert.equal(f.counts.begin, 0); assert.equal(f.store.get(first.id)!.inbound!.remote?.state, 'cancelled');
  assert.equal(f.store.get(first.id)!.inbound!.waitUntil, first.inbound!.waitUntil);
});

test('historical approved and consumed outcomes recover after token rotation with no new marker', async t => {
  const f = await fixture(t); const first = await f.begin(); f.approve();
  const historical = structuredClone(first); delete historical.inbound!.continuation;
  f.restoreHistorical(historical); f.config.twilioAuthToken = 'rotated-test-secret';
  const bound = await f.flow.tick(first.id);
  assert.equal(bound.inbound!.remote?.state, 'bound'); assert.equal(f.counts.begin, 1);
  assert.equal(bound.inbound!.continuation, undefined); assert.equal(f.counts.consume, 1);
  const deadline = bound.inbound!.mediaStartBy;
  f.restart(); await f.flow.tick(first.id);
  assert.equal(f.counts.begin, 1); assert.equal(f.counts.consume, 1);
  assert.equal(f.store.get(first.id)!.inbound!.mediaStartBy, deadline);
  await f.flow.stop(first.id, 'caller_hangup'); f.restart(); await f.flow.tick(first.id);
  assert.equal(f.counts.close, 1); assert.equal(f.store.get(first.id)!.inbound!.closure?.state, 'closed');
});

for (const code of ['inbound_carrier_attestation_required', 'inbound_enrollment_required'] as const) {
  test(`${code} persists exact refusal and cancellation intent across restart`, async t => {
    const f = await fixture(t); f.beginError(new PrimaryApiError('http_error', 403, { code, retryable: false }));
    const stopped = await f.begin();
    assert.equal(stopped.inbound!.admissionRefusal?.code, code);
    assert.equal(stopped.inbound!.admissionRefusal?.beginBodyDigest, stopped.inbound!.beginBodyDigest);
    assert.equal(stopped.inbound!.localStop?.reason, code);
    assert.equal(stopped.inbound!.remote?.state, 'cancelled');
    assert.equal(f.counts.consume, 0); assert.equal(f.counts.context, 0);
    f.beginError(); f.restart(); await f.flow.tick(stopped.id);
    assert.equal(f.counts.begin, 1); assert.equal(f.counts.consume, 0);
  });
}

test('retryable carrier 503 retains original attempted identity and recovers before exact retry', async t => {
  const f = await fixture(t);
  f.beginError(new PrimaryApiError('http_error', 503, { code: 'inbound_carrier_admission_unavailable', retryable: true }));
  await assert.rejects(f.begin(), error => error instanceof PrimaryApiError && error.status === 503);
  const before = f.store.list()[0];
  assert.equal(before.inbound!.admissionRefusal, undefined); assert.equal(before.inbound!.localStop, undefined);
  f.beginError(); f.advance(5000); f.restart();
  const next = await f.flow.tick(before.id);
  assert.equal(f.counts.recover, 1); assert.equal(f.counts.begin, 2);
  assert.deepEqual(next.inbound!.begin, before.inbound!.begin);
  assert.deepEqual(next.inbound!.continuation, before.inbound!.continuation);
  assert.equal(next.inbound!.beginAttemptedAt, before.inbound!.beginAttemptedAt);
  assert.equal(next.inbound!.waitUntil, before.inbound!.waitUntil);
});

for (const committed of [false, true]) test(`Phone Tab refusal stops context retries and reconciles committed=${committed}`, async t => {
  const f = await fixture(t), first = await f.begin(); f.approve();
  f.consumeError(new PrimaryApiError('http_error', 403, { code: 'phone_tab_required', retryable: false }), !committed);
  const stopped = await f.flow.tick(first.id);
  assert.equal(stopped.inbound!.localStop?.reason, 'phone_tab_required');
  assert.equal(stopped.inbound!.remote?.state, committed ? 'bound' : 'cancelled');
  assert.equal(stopped.inbound!.closure?.state, committed ? 'closed' : undefined);
  assert.deepEqual(stopped.failure, { code: 'http_error', message: 'Primary API request failed: http_error (HTTP 403)',
    status: 403, remoteCode: 'phone_tab_required', retryable: false });
  assert.equal(f.counts.consume, 1); assert.equal(f.counts.context, 0);
  assert.equal(stopped.primary, undefined); assert.equal(f.flow.waitingMediaAllowed(stopped), false);
  f.restart(); await f.flow.tick(first.id); await f.flow.tick(first.id);
  assert.equal(f.counts.consume, 1); assert.equal(f.counts.begin, 1);
  assert.equal(f.counts.close, committed ? 1 : 0);
  assert.equal(f.store.get(first.id)!.failure?.remoteCode, 'phone_tab_required');
});

for (const refresh of [false, true]) test(`Phone Tab refusal from ${refresh ? 'refresh' : 'opening'} context closes the committed binding`, async t => {
  const f = await fixture(t), first = await f.begin(); f.approve();
  if (refresh) await f.flow.tick(first.id);
  f.contextError(new PrimaryApiError('http_error', 403, { code: 'phone_tab_required', retryable: false }));
  const stopped = await f.flow.tick(first.id);
  assert.equal(stopped.inbound!.localStop?.reason, 'phone_tab_required');
  assert.equal(stopped.inbound!.remote?.state, 'bound'); assert.equal(stopped.inbound!.closure?.state, 'closed');
  assert.equal(f.counts.context, refresh ? 2 : 1); assert.equal(f.counts.consume, 1); assert.equal(f.counts.close, 1);
  const failure = f.timings.find(event => event.phase === 'failed' && event.stage === (refresh ? 'refresh_context' : 'open_context'));
  assert.equal(failure?.primaryApiRemoteCode, 'phone_tab_required'); assert.equal(failure?.primaryApiRetryable, false);
  f.restart(); await f.flow.tick(first.id);
  assert.equal(f.counts.context, refresh ? 2 : 1); assert.equal(f.counts.consume, 1); assert.equal(f.counts.close, 1);
});

for (const [index, error] of [new PrimaryApiError('http_error', 403, { code: 'inbound_enrollment_required', retryable: true }),
  new PrimaryApiError('http_error', 503, { code: 'inbound_enrollment_required', retryable: false }),
  new PrimaryApiError('http_error', 403, { code: 'phone_tab_required', retryable: true }),
  new PrimaryApiError('http_error', 503, { code: 'phone_tab_required', retryable: false }),
  new PrimaryApiError('http_error', 403, { code: 'other_refusal', retryable: false }),
  Object.assign(new Error('untrusted shape'), { status: 403, remoteCode: 'inbound_enrollment_required', retryable: false })].entries()) {
  test(`nonmatching error ${index} is not converted into carrier refusal: ${error.message}`, async t => {
    const f = await fixture(t); f.beginError(error);
    await assert.rejects(f.begin());
    assert.equal(f.store.list()[0].inbound!.admissionRefusal, undefined);
    assert.equal(f.store.list()[0].inbound!.localStop, undefined);
    assert.equal(f.counts.context, 0);
  });
}

test('changed original attestation or continuation signature cannot create the marker', async t => {
  const f = await fixture(t); const first = await f.flow.accept(f.original);
  const altered = structuredClone(f.original); altered.params.StirVerstat = 'TN-Validation-Passed-B';
  assert.throws(() => f.flow.accept(altered), /identity_mismatch/);
  altered.signature = Twilio.getExpectedTwilioSignature(f.config.twilioAuthToken, altered.url, altered.params);
  assert.throws(() => f.flow.accept(altered), /conflicts/);
  const continuation = f.proof(`/broker/inbound/${first.id}/continue`); continuation.signature = 'invalid';
  assert.throws(() => f.flow.resume(first.id, continuation), /identity_mismatch/);
  assert.equal(f.store.get(first.id)!.inbound!.continuation, undefined); assert.equal(f.counts.begin, 0);
  assert.deepEqual(f.store.get(first.id)!.inbound!.begin.providerProof, f.original);
});

for (const committed of [false, true]) {
  test(`enrollment refusal at consume durably stops; committed winner=${committed} closes independently`, async t => {
    const f = await fixture(t); const first = await f.begin(); f.approve();
    f.consumeError(new PrimaryApiError('http_error', 403, { code: 'inbound_enrollment_required', retryable: false }), !committed);
    const stopped = await f.flow.tick(first.id);
    assert.equal(stopped.inbound!.admissionRefusal?.code, 'inbound_enrollment_required');
    assert.equal(stopped.inbound!.localStop?.reason, 'inbound_enrollment_required');
    assert.equal(stopped.inbound!.remote?.state, committed ? 'bound' : 'cancelled');
    assert.equal(stopped.inbound!.closure?.state, committed ? 'closed' : undefined);
    assert.equal(f.counts.context, 0); assert.equal(stopped.primary, undefined);
    f.restart(); await f.flow.tick(first.id);
    assert.equal(f.counts.consume, 1); assert.equal(f.counts.close, committed ? 1 : 0);
  });
}

test('historical approved receipt remains valid after local refusal and cancellation', async t => {
  const f = await fixture(t); const first = await f.begin(); f.approve();
  const historical = structuredClone(first);
  historical.inbound!.remote = f.remote();
  if (historical.inbound!.remote?.state !== 'approved') throw new Error('fixture approval missing');
  historical.inbound!.approvalRef = historical.inbound!.remote.approvalRef;
  delete historical.inbound!.continuation; delete historical.inbound!.beginAttemptedAt;
  f.restoreHistorical(historical);
  // Current-context refusal can also be returned by an authenticated outcome read.
  f.store.transitionInbound(first.id, { type: 'admission_refused', code: 'inbound_enrollment_required', observedAt: new Date(f.clock()).toISOString() });
  const stopped = await f.flow.tick(first.id);
  assert.equal(stopped.inbound!.remote?.state, 'cancelled');
  assert.equal(stopped.inbound!.beginAttemptedAt, undefined);
  assert.equal(stopped.inbound!.admissionRefusal?.code, 'inbound_enrollment_required');
  f.restart(); assert.equal(f.store.get(first.id)!.inbound!.remote?.state, 'cancelled');
  assert.equal(f.counts.consume, 0);
});

test('enrollment refusal on bound context recovery closes exact consumed receipt without fresh admission', async t => {
  const f = await fixture(t); const first = await f.begin(); f.approve(); await f.flow.tick(first.id);
  const deadline = f.store.get(first.id)!.inbound!.mediaStartBy;
  f.contextError(new PrimaryApiError('http_error', 403, { code: 'inbound_enrollment_required', retryable: false }));
  const closed = await f.flow.tick(first.id);
  assert.equal(closed.inbound!.admissionRefusal?.code, 'inbound_enrollment_required');
  assert.equal(closed.inbound!.closure?.state, 'closed');
  assert.equal(closed.inbound!.mediaStartBy, deadline);
  assert.equal(f.counts.begin, 1); assert.equal(f.counts.consume, 1); assert.equal(f.counts.close, 1);
});

async function until(predicate: () => boolean) {
  for (let i = 0; i < 100 && !predicate(); i++) await new Promise(resolve => setImmediate(resolve));
  assert.ok(predicate(), 'expected admission boundary');
}

test('admission waits for the signed continuation: duplicates, ticks and restart contact nothing before it', async t => {
  const f = await fixture(t), first = await f.flow.accept(f.original);
  assert.equal((await f.flow.accept(f.original)).id, first.id);
  await f.flow.tick(first.id); f.restart(); await f.flow.tick(first.id);
  assert.equal(f.counts.begin, 0); assert.equal(f.counts.status, 0); assert.equal(f.counts.recover, 0);
  assert.equal(f.counts.provider, 0); assert.equal(f.counts.consume, 0); assert.equal(f.counts.context, 0);
  assert.equal(f.flow.waitingMediaAllowed(f.store.get(first.id)!), false, 'no greeting stream before the continuation');
});

test('a recorded continuation admits the greeting stream before any API contact; ticks then bind the same attachment', async t => {
  const f = await fixture(t), first = await f.flow.accept(f.original);
  f.flow.recordContinuation(first.id, f.proof(`/broker/inbound/${first.id}/continue`));
  const continued = f.store.get(first.id)!;
  assert.ok(continued.inbound!.continuation);
  assert.equal(f.counts.begin, 0); assert.equal(f.counts.provider, 0);
  assert.equal(f.flow.waitingMediaAllowed(continued), true); assert.equal(f.flow.mediaAllowed(continued), false);
  assert.throws(() => f.flow.recordContinuation(first.id, { ...f.proof(`/broker/inbound/${first.id}/continue`), signature: 'bad' }), /inbound_call_identity_mismatch/);
  await f.flow.tick(first.id);
  assert.equal(f.counts.begin, 1); assert.equal(f.store.get(first.id)!.inbound!.remote?.state, 'pending');
  assert.equal(f.flow.waitingMediaAllowed(f.store.get(first.id)!), true);
  f.approve(); const bound = await f.flow.tick(first.id);
  assert.equal(f.counts.begin, 1); assert.equal(f.counts.consume, 1);
  assert.ok(f.flow.mediaAllowed(bound)); assert.equal(f.flow.waitingMediaAllowed(bound), true);
  assert.equal(bound.inbound!.waitUntil, first.inbound!.waitUntil);
  assert.deepEqual(bound.inbound!.begin, first.inbound!.begin);
});

for (const failure of ['lost_reply', 'ringing_after_pending', 'ringing_before_pending'] as const) {
  test(`first begin ${failure} recovers on the next tick with the exact original begin`, async t => {
    const f = await fixture(t), first = await f.flow.accept(f.original);
    f.flow.recordContinuation(first.id, f.proof(`/broker/inbound/${first.id}/continue`));
    if (failure === 'ringing_before_pending') f.beginError(new PrimaryApiError('http_error', 403, { code: 'call_not_active', retryable: false }));
    else if (failure === 'ringing_after_pending') f.pendingBeginError(new PrimaryApiError('http_error', 403, { code: 'call_not_active', retryable: false }));
    else f.loseBegin();
    await assert.rejects(f.flow.tick(first.id));
    const attempted = f.store.get(first.id)!;
    assert.ok(attempted.inbound!.beginAttemptedAt); assert.equal(attempted.inbound!.localStop, undefined);
    assert.equal(f.flow.waitingMediaAllowed(attempted), true, 'the greeting stream keeps waiting');
    if (failure === 'ringing_after_pending') assert.equal(f.remote()?.state, 'pending');
    f.beginError(); f.pendingBeginError();
    await f.flow.tick(first.id);
    assert.equal(f.counts.recover, 1); assert.equal(f.counts.begin, 2);
    assert.deepEqual(f.store.get(first.id)!.inbound!.begin, first.inbound!.begin);
    assert.equal(f.store.get(first.id)!.inbound!.beginAttemptedAt, attempted.inbound!.beginAttemptedAt);
    f.approve(); await f.flow.tick(first.id); assert.equal(f.counts.consume, 1);
  });
}

for (const stop of ['hangup', 'shutdown', 'deadline'] as const) {
  test(`admission ${stop} during the first begin settles the same attachment without context`, async t => {
    const f = await fixture(t), first = await f.flow.accept(f.original), gate = f.gateBegin();
    f.flow.recordContinuation(first.id, f.proof(`/broker/inbound/${first.id}/continue`));
    const admitting = f.flow.tick(first.id); await until(() => f.counts.begin === 1);
    assert.equal(f.flow.busy(first.id), true);
    let stopping: Promise<unknown> | undefined;
    if (stop === 'deadline') f.advance(120_000);
    else stopping = f.flow.stop(first.id, stop === 'hangup' ? 'provider_terminal' : 'service_shutdown');
    if (stopping) assert.ok(f.store.get(first.id)!.inbound!.localStop);
    gate.resolve(); await admitting; await stopping; await f.flow.idle(first.id);
    if (stop === 'deadline') await f.flow.tick(first.id);
    assert.equal(f.flow.busy(first.id), false);
    assert.equal(f.counts.begin, 1); assert.equal(f.counts.cancel, 1);
    assert.equal(f.store.get(first.id)!.inbound!.remote?.state, 'cancelled');
    assert.equal(f.flow.waitingMediaAllowed(f.store.get(first.id)!), false);
    assert.equal(f.counts.consume, 0); assert.equal(f.counts.context, 0);
  });
}

for (const code of ['inbound_carrier_attestation_required', 'inbound_enrollment_required'] as const) {
  test(`first begin ${code} retains refusal and cancellation without private work and ends the greeting wait`, async t => {
    const f = await fixture(t), first = await f.flow.accept(f.original);
    f.flow.recordContinuation(first.id, f.proof(`/broker/inbound/${first.id}/continue`));
    f.beginError(new PrimaryApiError('http_error', 403, { code, retryable: false }));
    const stopped = await f.flow.tick(first.id);
    assert.equal(stopped.inbound!.admissionRefusal?.code, code); assert.equal(stopped.inbound!.remote?.state, 'cancelled');
    assert.equal(f.flow.waitingMediaAllowed(stopped), false);
    assert.equal(f.counts.begin, 1); assert.equal(f.counts.cancel, 1); assert.equal(f.counts.consume, 0);
    await f.flow.resume(first.id, f.proof(`/broker/inbound/${first.id}/continue`));
    assert.equal(f.counts.begin, 1); assert.equal(f.counts.context, 0);
  });
}

test('an expired wait records no continuation and admits no greeting stream', async t => {
  const f = await fixture(t), first = await f.flow.accept(f.original);
  f.advance(120_000); f.flow.recordContinuation(first.id, f.proof(`/broker/inbound/${first.id}/continue`));
  assert.equal(f.store.get(first.id)!.inbound!.continuation, undefined);
  assert.equal(f.flow.waitingMediaAllowed(f.store.get(first.id)!), false);
  assert.equal(f.counts.begin, 0);
});

test('revocation after approval is rechecked at consume and cancels the same attachment', async t => {
  const f = await fixture(t), first = await f.flow.accept(f.original);
  f.flow.recordContinuation(first.id, f.proof(`/broker/inbound/${first.id}/continue`));
  await f.flow.tick(first.id); f.approve();
  f.consumeError(new PrimaryApiError('http_error', 403, { code: 'inbound_enrollment_required', retryable: false }), true);
  const result = await f.flow.tick(first.id);
  assert.equal(result.inbound!.admissionRefusal?.code, 'inbound_enrollment_required');
  assert.equal(result.inbound!.remote?.state, 'cancelled'); assert.equal(f.counts.begin, 1);
  assert.equal(f.counts.consume, 1); assert.equal(f.counts.context, 0);
});
