import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import Twilio from 'twilio';
import { BrokerStore, type BrokerCall } from '../src/broker/store.js';
import { createCallbackFlow, callbackRequestId, type CallbackFlowOptions, type ObservedCallbackCall } from '../src/broker/callback.js';
import type { ProviderCallProof } from '../src/broker/phone.js';

function fixture(t: test.TestContext) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-callback-test-'));
  let store = new BrokerStore(directory);
  t.after(() => { store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  const config = { publicOrigin: 'https://phone.example.test', twilioAccountSid: 'AC' + '1'.repeat(32),
    twilioAuthToken: 'fixture-secret', from: '+12025550100', allowedTo: ['+12025550123'] };
  let time = Date.parse('2026-09-21T12:00:00.000Z');
  const observed = new Map<string, ObservedCallbackCall>();
  let reads = 0, dials = 0;
  let fetchOverride: CallbackFlowOptions['fetchCall'] | undefined;
  let executorOverride: CallbackFlowOptions['requestOutbound'] | undefined;
  const outboundRequests: Parameters<CallbackFlowOptions['requestOutbound']>[0][] = [];
  function incoming(number = config.allowedTo[0]) {
    const callSid = 'CA' + randomUUID().replaceAll('-', '');
    const call = store.reserve({ direction: 'inbound', requestId: randomUUID(), number, callSid, purpose: 'The person called Dexter.' }).call;
    observed.set(callSid, { sid: callSid, accountSid: config.twilioAccountSid, from: number, to: config.from, direction: 'inbound', status: 'in-progress' });
    return call;
  }
  function proof(call: BrokerCall, kind: 'offer' | 'consent' = 'consent', patch: Record<string, string> = {}): ProviderCallProof {
    const url = config.publicOrigin + (kind === 'offer' ? '/broker/incoming-call' : `/broker/callback/${call.id}`);
    const params = { AccountSid: config.twilioAccountSid, CallSid: call.callSid!, From: call.number, To: config.from,
      Direction: 'inbound', ...(kind === 'consent' ? { Digits: '1' } : {}), ...patch };
    return { url, params, signature: Twilio.getExpectedTwilioSignature(config.twilioAuthToken, url, params) };
  }
  function flow(overrides: Partial<CallbackFlowOptions> = {}) {
    return createCallbackFlow({ config, store, now: () => time,
      fetchCall: async sid => { reads++; return fetchOverride ? fetchOverride(sid) : observed.get(sid)!; },
      requestOutbound: async input => {
        outboundRequests.push(input);
        if (executorOverride) return executorOverride(input);
        const reserved = store.reserve({ requestId: input.requestId, direction: 'outbound', purpose: input.purpose, number: input.to });
        if (reserved.existing) return reserved.call;
        assert.equal(store.get(input.callbackFor)!.status, 'completed');
        dials++;
        return store.update(reserved.call.id, { status: 'queued', callSid: 'CA' + randomUUID().replaceAll('-', '') });
      }, ...overrides });
  }
  return { config, incoming, proof, flow, observed, directory, outboundRequests,
    store: () => store, reads: () => reads, dials: () => dials,
    advance: (ms: number) => { time += ms; },
    changeFetch: (fetch: CallbackFlowOptions['fetchCall']) => { fetchOverride = fetch; },
    changeExecutor: (execute: CallbackFlowOptions['requestOutbound']) => { executorOverride = execute; },
    restart: () => { store.close(); store = new BrokerStore(directory); },
  };
}

test('inbound offer collects a keypress and is identical for eligible and unknown callers', t => {
  const f = fixture(t); const flow = f.flow();
  const first = f.incoming(), unknown = f.incoming('+12025550199');
  const a = flow.offer(first, f.proof(first, 'offer'));
  const b = flow.offer(unknown, f.proof(unknown, 'offer'));
  assert.match(a, /<Gather[^>]*numDigits="1"/);
  assert.match(a, /press 1/);
  assert.match(a, /<Hangup\/>/);
  assert.equal(a.replaceAll(first.id, 'CALL'), b.replaceAll(unknown.id, 'CALL'));
  assert.equal(f.reads(), 0); assert.equal(f.dials(), 0);
});

test('consent persists before response, waits for terminal inbound, then uses shared outbound once', async t => {
  const f = fixture(t); const flow = f.flow(); const call = f.incoming();
  const response = await flow.consent(call, f.proof(call));
  assert.match(response, /If phone access is ready/);
  assert.equal(f.store().get(call.id)!.callback!.state, 'requested');
  assert.equal(f.outboundRequests.length, 0);
  await flow.dispatchPending();
  assert.equal(f.outboundRequests.length, 0);
  f.observed.get(call.callSid!)!.status = 'completed';
  await Promise.all([flow.dispatchPending(), flow.dispatchPending()]);
  assert.equal(f.dials(), 1);
  assert.equal(f.store().get(call.id)!.callback!.state, 'started');
  assert.equal(f.outboundRequests[0].callbackFor, call.id);
  assert.equal(f.outboundRequests[0].to, call.number);
  await flow.consent(call, f.proof(call)); await flow.dispatchPending();
  assert.equal(f.dials(), 1);
});

test('valid signatures for another source, route, direction, or destination cannot consent', async t => {
  const f = fixture(t); const flow = f.flow(); const call = f.incoming();
  const changes: Record<string, string>[] = [{ From: '+12025550124' }, { To: '+12025550101' }, { Direction: 'outbound-api' }, { CallSid: 'CA' + '9'.repeat(32) }];
  for (const patch of changes) {
    await assert.rejects(flow.consent(call, f.proof(call, 'consent', patch)), /identity_mismatch/);
  }
  await assert.rejects(flow.consent(call, f.proof(call, 'offer')), /identity_mismatch/);
  const forged = f.proof(call); forged.signature = 'invalid';
  await assert.rejects(flow.consent(call, forged), /identity_mismatch/);
  assert.equal(f.reads(), 0); assert.equal(f.store().get(call.id)!.callback, undefined);
});

test('keypress, allowlist and fresh provider identity are required without disclosing the result', async t => {
  const f = fixture(t); const flow = f.flow(); const good = f.incoming();
  const expected = await flow.consent(good, f.proof(good));
  const unknown = f.incoming('+12025550199');
  assert.equal(await flow.consent(unknown, f.proof(unknown)), expected);
  const stale = f.incoming(); f.observed.get(stale.callSid!)!.status = 'completed';
  assert.equal(await flow.consent(stale, f.proof(stale)), expected);
  const wrongAccount = f.incoming(); f.observed.get(wrongAccount.callSid!)!.accountSid = 'AC' + '9'.repeat(32);
  assert.equal(await flow.consent(wrongAccount, f.proof(wrongAccount)), expected);
  const wrongSid = f.incoming(); f.observed.get(wrongSid.callSid!)!.sid = good.callSid!;
  assert.equal(await flow.consent(wrongSid, f.proof(wrongSid)), expected);
  const declined = f.incoming(); await flow.consent(declined, f.proof(declined, 'consent', { Digits: '2' }));
  for (const call of [unknown, stale, wrongAccount, wrongSid, declined]) assert.equal(f.store().get(call.id)!.callback, undefined);
  assert.equal(f.outboundRequests.length, 0);
});

test('a timed-out provider read cannot later admit consent', async t => {
  const f = fixture(t); const call = f.incoming();
  let release!: (value: ObservedCallbackCall) => void;
  f.changeFetch(() => new Promise(resolve => { release = resolve; }));
  const flow = f.flow({ providerTimeoutMs: 5 });
  await flow.consent(call, f.proof(call));
  release(f.observed.get(call.callSid!)!);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(f.store().get(call.id)!.callback, undefined);
});

test('same consent concurrent delivery is admitted once and survives process restart', async t => {
  const f = fixture(t); const call = f.incoming(); const flow = f.flow();
  await Promise.all([flow.consent(call, f.proof(call)), flow.consent(call, f.proof(call))]);
  assert.equal(f.reads(), 1);
  const requestId = f.store().get(call.id)!.callback!.requestId;
  assert.match(requestId, /^[0-9a-f]{8}-[0-9a-f]{4}-8[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  f.restart(); f.observed.get(call.callSid!)!.status = 'completed';
  await f.flow().dispatchPending();
  assert.equal(f.dials(), 1);
  assert.equal(f.outboundRequests[0].requestId, requestId);
});

test('unknown outbound outcome is recovered after restart instead of dialing again', async t => {
  const f = fixture(t); const call = f.incoming(); const flow = f.flow();
  await flow.consent(call, f.proof(call));
  f.changeExecutor(async input => {
    const saved = f.store().reserve({ requestId: input.requestId, direction: 'outbound', purpose: input.purpose, number: input.to }).call;
    f.store().update(saved.id, { status: 'unknown' });
    throw new Error('Lost response after dispatch');
  });
  f.observed.get(call.callSid!)!.status = 'completed';
  await flow.dispatchPending();
  assert.equal(f.outboundRequests.length, 1);
  // Simulate interruption before the callback journal reflected the dispatch.
  f.store().update(call.id, { callback: { ...f.store().get(call.id)!.callback!, state: 'requested' } });
  f.restart(); await f.flow().dispatchPending();
  assert.equal(f.outboundRequests.length, 1);
  assert.equal(f.store().get(call.id)!.callback!.state, 'started');
});

test('permission/readiness failure is terminal for this callback request', async t => {
  const f = fixture(t); const call = f.incoming(); const flow = f.flow();
  await flow.consent(call, f.proof(call));
  f.changeExecutor(async input => {
    const saved = f.store().reserve({ requestId: input.requestId, direction: 'outbound', purpose: input.purpose, number: input.to }).call;
    f.store().update(saved.id, { status: 'failed', failure: { code: 'authority_unavailable', message: 'Unavailable' } });
    throw new Error('Unavailable');
  });
  f.observed.get(call.callSid!)!.status = 'completed';
  await flow.dispatchPending(); await flow.dispatchPending();
  assert.equal(f.outboundRequests.length, 1);
  assert.equal(f.store().get(call.id)!.callback!.state, 'unavailable');
  assert.equal(f.dials(), 0);
});

test('provider identity must still match when observing terminal inbound', async t => {
  const f = fixture(t); const call = f.incoming(); const flow = f.flow();
  await flow.consent(call, f.proof(call));
  Object.assign(f.observed.get(call.callSid!)!, { status: 'completed', from: '+12025550124' });
  await flow.dispatchPending();
  assert.equal(f.outboundRequests.length, 0);
  assert.equal(f.store().get(call.id)!.callback!.state, 'requested');
});

test('cooldown and hourly callback limits survive restart and do not change public speech', async t => {
  const f = fixture(t); const flow = f.flow(); const first = f.incoming();
  const expected = await flow.consent(first, f.proof(first));
  const tooSoon = f.incoming();
  assert.equal(await flow.consent(tooSoon, f.proof(tooSoon)), expected);
  assert.equal(f.store().get(tooSoon.id)!.callback, undefined);
  for (let i = 0; i < 2; i++) {
    f.advance(61_000); const next = f.incoming(); await flow.consent(next, f.proof(next));
    assert.equal(f.store().get(next.id)!.callback!.state, 'requested');
  }
  f.restart(); f.advance(61_000); const fourth = f.incoming();
  assert.equal(await f.flow().consent(fourth, f.proof(fourth)), expected);
  assert.equal(f.store().get(fourth.id)!.callback, undefined);
});

test('expired or newly disallowed callbacks do not ring after restart', async t => {
  const f = fixture(t); const call = f.incoming();
  await f.flow().consent(call, f.proof(call));
  f.observed.get(call.callSid!)!.status = 'completed';
  f.advance(5 * 60_000 + 1); f.restart();
  await f.flow().dispatchPending();
  assert.equal(f.store().get(call.id)!.callback!.state, 'unavailable');
  assert.equal(f.outboundRequests.length, 0);
  const another = f.incoming(); await f.flow().consent(another, f.proof(another));
  f.config.allowedTo = [];
  await f.flow().dispatchPending();
  assert.equal(f.store().get(another.id)!.callback!.state, 'unavailable');
  assert.equal(f.outboundRequests.length, 0);
});

test('consent expiring during the final provider read is not dispatched', async t => {
  const f = fixture(t); const call = f.incoming(); const flow = f.flow();
  await flow.consent(call, f.proof(call));
  f.advance(5 * 60_000 - 1);
  f.changeFetch(async sid => {
    f.advance(2);
    return { ...f.observed.get(sid)!, status: 'completed' };
  });
  await flow.dispatchPending();
  assert.equal(f.store().get(call.id)!.callback!.state, 'unavailable');
  assert.equal(f.outboundRequests.length, 0);
});

test('callback id is scoped to immutable provider account and call identity', () => {
  const account = 'AC' + '1'.repeat(32), sid = 'CA' + '2'.repeat(32);
  assert.equal(callbackRequestId(account, sid), callbackRequestId(account, sid));
  assert.notEqual(callbackRequestId(account, sid), callbackRequestId(account, 'CA' + '3'.repeat(32)));
  assert.notEqual(callbackRequestId(account, sid), callbackRequestId('AC' + '4'.repeat(32), sid));
});
