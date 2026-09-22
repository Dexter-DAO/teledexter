import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test, { type TestContext } from 'node:test';
import { createPrimaryBackend, type BrokerDelegationRecord } from '../src/broker/backend.js';
import { BrokerStore } from '../src/broker/store.js';
import { PrimaryApiError, type PrimaryApiClient, type PrimaryReconciliation, type PrimarySubmission, type PrimaryTurn } from '../src/broker/primaryApi.js';
import { originalTurn, rawSubmissionDigest, turnBodyDigest, validateNonAdmissionProof } from '../src/broker/reconciliation.js';
import { createBrokerServer } from '../src/broker/server.js';
import type { BrokerConfig } from '../src/broker/config.js';

const bindingId = '11111111-1111-4111-8111-111111111111';
const requestId = '22222222-2222-4222-8222-222222222222';
const sourceEventId = '33333333-3333-4333-8333-333333333333';
const revisionRecordId = '44444444-4444-4444-8444-444444444444';
const callSid = 'CA' + 'a'.repeat(32);
const at = '2026-09-22T09:58:00.000Z';
const closedAt = '2026-09-22T10:02:00.000Z';
const input: PrimarySubmission = { source: { callSid, itemId: 'original_turn', sourceEventId, revision: 1,
  role: 'user', receivedAt: at }, text: ' And to what extent can you do all those things\t' };
const closed = { bindingId, callSid, state: 'closed' as const };
const receipt = (turn: PrimaryTurn) => ({ bindingId, sourceEventId: turn.sourceEventId, revision: turn.revision,
  recordId: turn.sourceEventId, revisionRecordId, bodyDigest: turnBodyDigest(turn), replayed: true });
const absent = (): Extract<PrimaryReconciliation, { outcome: 'not_admitted' }> => ({ outcome: 'not_admitted', bindingId, requestId,
  proof: { schemaVersion: 1, kind: 'phone_closed_call_non_admission', callSid,
    source: { itemId: input.source.itemId, sourceEventId, revision: 1, receivedAt: at },
    rawSubmissionDigest: rawSubmissionDigest(bindingId, input),
    turn: { recordId: sourceEventId, revisionRecordId, bodyDigest: turnBodyDigest(originalTurn(input)) }, closedAt } });

async function fixture(t: TestContext) {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'broker-reconciliation-'));
  let store = new BrokerStore(directory);
  t.after(async () => { store.close(); await rm(directory, { recursive: true, force: true }); });
  const call = store.reserve({ requestId: randomUUID(), direction: 'outbound', number: '+12025550123', purpose: 'test', callSid }).call;
  const entry: BrokerDelegationRecord = { version: 1, requestId, callId: call.id, callSid, bindingId,
    sessionId: 'saved-session', delegationIds: ['saved-delegation'], status: 'dispatch_unknown', updatedAt: at,
    submission: structuredClone(input), frozenCallerRevision: 1, firstSequence: 1, lastSequence: 1,
    completionBasis: 'speech_and_transcript_stability', sourceRevision: 1, capturedText: input.text,
    submitError: { code: 'http_error', status: 503, remoteCode: 'temporarily_unavailable', observedAt: at } };
  store.update(call.id, { status: 'completed', endedAt: closedAt, finalized: true,
    primary: { bindingId, name: null, context: '' }, liveSessionId: entry.sessionId,
    mediaStartedAt: at, delegations: { [requestId]: entry }, primaryClosePending: true,
    transcriptCapture: { state: 'pending', pending: 1, unsupported: ['source_admission_unknown'],
      sessionFinalized: true, assistantPlaybackComplete: false } });
  const actions: string[] = [];
  const captures: PrimaryTurn[] = [];
  let captureFailures = 0, reconcileFailures = 0;
  let recovered: PrimaryReconciliation = absent();
  let captureReceipt = receipt;
  const forbidden = async () => { throw new Error('forbidden execution'); };
  const api = {
    submit: forbidden, reply: forbidden, question: forbidden,
    result: async (_binding: string, id: string) => {
      actions.push(`get:${id}`);
      throw new PrimaryApiError('http_error', 404, { code: 'request_not_found', retryable: false });
    },
    captureTurn: async (_binding: string, turn: PrimaryTurn) => {
      actions.push(`capture:${turn.revision}`); captures.push(structuredClone(turn));
      assert.deepEqual((store.get(call.id)!.delegations[requestId] as BrokerDelegationRecord).originalCapture?.turn, originalTurn(input));
      if (captureFailures-- > 0) throw new PrimaryApiError('request_timeout');
      return captureReceipt(turn);
    },
    reconcile: async (binding: string, id: string, submission: PrimarySubmission) => {
      actions.push('reconcile');
      assert.equal(binding, bindingId); assert.equal(id, requestId); assert.deepEqual(submission, input);
      assert.deepEqual(store.get(call.id)!.primaryClosed, closed);
      if (reconcileFailures-- > 0) throw new PrimaryApiError('request_timeout');
      return structuredClone(recovered);
    },
  };
  const backend = () => createPrimaryBackend({ callId: call.id, callSid, bindingId, api, store });
  return { directory, call, entry, api, actions, captures, backend,
    store: () => store, saved: () => store.get(call.id)!.delegations[requestId] as BrokerDelegationRecord,
    restart() { store.close(); store = new BrokerStore(directory); },
    failures(capture: number, reconcile = 0) { captureFailures = capture; reconcileFailures = reconcile; },
    result(value: PrimaryReconciliation) { recovered = value; },
    receipt(value: typeof receipt) { captureReceipt = value; },
    close() { actions.push('close'); store.update(call.id, { primaryClosed: closed }); },
  };
}

test('restart acknowledges exact raw revision1 despite initialized fields, then proves non-admission without submitting', async t => {
  const f = await fixture(t);
  const original = structuredClone(f.saved());
  f.failures(1);
  await f.backend().prepareCloseout();
  assert.equal(f.backend().closeoutPending(), 1);
  assert.equal(f.saved().originalCapture?.state, 'unknown');
  assert.equal(f.saved().status, 'dispatch_unknown');
  f.restart();
  await f.backend().prepareCloseout();
  assert.equal(f.backend().closeoutPending(), 0);
  assert.deepEqual(f.captures, [originalTurn(input), originalTurn(input)]);
  assert.equal((await f.backend().flushPendingCaptures()).pending, 1);
  f.close(); f.failures(0, 1);
  assert.deepEqual(await f.backend().reconcileClosedRequests(closed), { pending: 1 });
  f.restart();
  assert.deepEqual(await f.backend().reconcileClosedRequests(closed), { pending: 0 });
  f.restart();
  assert.equal(f.saved().status, 'not_admitted');
  assert.deepEqual(f.saved().submission, original.submission);
  assert.equal(f.saved().requestId, original.requestId);
  assert.deepEqual(f.saved().submitError, original.submitError);
  assert.equal(f.saved().primaryResult, undefined); assert.equal(f.saved().result, undefined);
  assert.deepEqual(f.saved().reconciliation, absent());
  assert.deepEqual(await f.backend().flushPendingCaptures(), { pending: 0, unsupported: [] });
  assert.equal(f.actions.filter(action => action === 'reconcile').length, 2);
});

test('open or unfinalized calls cannot prepare absence recovery', async t => {
  const f = await fixture(t);
  f.store().update(f.call.id, { endedAt: undefined });
  await f.backend().prepareCloseout();
  assert.equal(f.backend().closeoutPending(), 1); assert.equal(f.actions.length, 0);
  f.store().update(f.call.id, { endedAt: closedAt, transcriptCapture: { ...f.store().get(f.call.id)!.transcriptCapture!, sessionFinalized: false } });
  await f.backend().prepareCloseout();
  assert.equal(f.backend().closeoutPending(), 1); assert.equal(f.actions.length, 0);
});

test('legacy and wrong capture acknowledgements cannot permit closure', async t => {
  const f = await fixture(t);
  for (const field of ['revisionRecordId', 'bodyDigest', 'bindingId', 'sourceEventId', 'recordId', 'revision'] as const) {
    f.receipt(turn => ({ ...receipt(turn), [field]: field === 'revision' ? 2 : field === 'bodyDigest' ? 'a'.repeat(64) : undefined } as ReturnType<typeof receipt>));
    await f.backend().prepareCloseout();
    assert.equal(f.backend().closeoutPending(), 1, field);
    assert.equal(f.saved().originalCapture?.state, 'unknown');
  }
});

test('non-admission proof must match acknowledged row, raw words, source and closed call', async t => {
  const f = await fixture(t);
  await f.backend().prepareCloseout(); f.close();
  const changes = [
    (p: ReturnType<typeof absent>) => { p.requestId = randomUUID(); },
    (p: ReturnType<typeof absent>) => { p.bindingId = randomUUID(); },
    (p: ReturnType<typeof absent>) => { p.proof.callSid = 'CA' + 'b'.repeat(32); },
    (p: ReturnType<typeof absent>) => { p.proof.source.receivedAt = at.replace('.000Z', 'Z'); },
    (p: ReturnType<typeof absent>) => { p.proof.source.itemId = 'other_turn'; },
    (p: ReturnType<typeof absent>) => { p.proof.source.sourceEventId = randomUUID(); },
    (p: ReturnType<typeof absent>) => { p.proof.rawSubmissionDigest = rawSubmissionDigest(bindingId, { ...input, text: input.text.trim() }); },
    (p: ReturnType<typeof absent>) => { p.proof.turn.revisionRecordId = randomUUID(); },
    (p: ReturnType<typeof absent>) => { p.proof.turn.bodyDigest = 'a'.repeat(64); },
    (p: ReturnType<typeof absent>) => { p.proof.closedAt = closedAt.replace('.000Z', '+00:00'); },
  ];
  for (const change of changes) {
    const proof = absent(); change(proof); f.result(proof);
    assert.deepEqual(await f.backend().reconcileClosedRequests(closed), { pending: 1 });
    assert.equal(f.saved().status, 'dispatch_unknown');
    assert.equal(f.saved().reconciliation, undefined);
  }
});

test('admitted canonical run wins without rewriting the original request or creating an alias', async t => {
  const f = await fixture(t);
  await f.backend().prepareCloseout(); f.close();
  const canonicalRequestId = randomUUID();
  f.result({ outcome: 'admitted', requestId, bindingId, canonicalRequestId,
    result: { requestId: canonicalRequestId, bindingId, state: 'running' } });
  assert.deepEqual(await f.backend().reconcileClosedRequests(closed), { pending: 0 });
  f.restart();
  assert.equal(f.saved().requestId, requestId); assert.deepEqual(f.saved().submission, input);
  assert.equal(f.saved().primaryResult?.requestId, canonicalRequestId);
  assert.equal(f.saved().status, 'running');
  assert.deepEqual(await f.backend().reconcileClosedRequests(closed), { pending: 0 });
  assert.equal(f.actions.filter(action => action === 'reconcile').length, 1);
});

test('an already admitted result avoids absence capture and malformed admitted result cannot poison the journal', async t => {
  const f = await fixture(t);
  f.api.result = async (_binding, id) => ({ requestId: id, bindingId, state: 'running' } as never);
  await f.backend().prepareCloseout();
  assert.equal(f.saved().status, 'running'); assert.equal(f.captures.length, 0);
  const g = await fixture(t); await g.backend().prepareCloseout(); g.close();
  const canonicalRequestId = randomUUID();
  g.result({ outcome: 'admitted', requestId, bindingId, canonicalRequestId,
    result: { requestId: canonicalRequestId, bindingId: randomUUID(), state: 'running' } });
  assert.deepEqual(await g.backend().reconcileClosedRequests(closed), { pending: 1 });
  assert.equal(g.saved().reconciliation, undefined);
});

test('later exact capture timeout blocks closure after original acknowledgement', async t => {
  const f = await fixture(t);
  await f.backend().prepareCloseout();
  const revised = { ...originalTurn(input), revision: 2, text: input.text + ' please' };
  const saved = f.saved();
  f.store().update(f.call.id, { delegations: { [requestId]: { ...saved, pendingCapture: revised, captureState: 'unknown' } } });
  assert.equal(f.backend().closeoutPending(), 1);
  f.failures(1);
  await f.backend().flushPendingCaptures();
  assert.equal(f.backend().closeoutPending(), 1);
  f.restart(); await f.backend().flushPendingCaptures();
  assert.equal(f.backend().closeoutPending(), 0);
  assert.deepEqual(f.captures, [originalTurn(input), revised, revised]);
  assert.deepEqual(f.saved().originalCapture?.turn, originalTurn(input));
  assert.equal(f.saved().sourceRevision, 2);
});

test('proof validation is sensitive to raw whitespace and keeps revision1 separate from later evidence', () => {
  const turn = originalTurn(input);
  validateNonAdmissionProof(bindingId, input, receipt(turn), closed, absent().proof);
  assert.throws(() => validateNonAdmissionProof(bindingId, { ...input, text: input.text.trim() }, receipt(turn), closed, absent().proof));
  assert.throws(() => validateNonAdmissionProof(bindingId, input, receipt({ ...turn, revision: 2 }), closed, absent().proof));
});

test('actual server maintenance recovers capture, close and reconciliation response loss across disk restarts', async t => {
  const f = await fixture(t);
  const config: BrokerConfig = { host: '127.0.0.1', port: 3213, publicOrigin: 'https://broker.example.test', stateDir: f.directory,
    controlToken: 'c'.repeat(40), apiKey: 'synthetic-key', model: 'gpt-live-1', voice: 'marin', twilioAccountSid: 'AC' + '1'.repeat(32),
    twilioAuthToken: 'synthetic-secret', from: '+12025550100', allowedTo: ['+12025550123'], apiBaseUrl: 'https://api.example.test',
    phoneServiceSecret: 's'.repeat(40), maxCallSeconds: 60 };
  let closeFailures = 1;
  const primary = { ...f.api, close: async () => {
    f.actions.push('close');
    if (closeFailures-- > 0) throw new PrimaryApiError('request_timeout');
    return closed;
  } } as unknown as PrimaryApiClient;
  const provider = { calls() { throw new Error('provider must not be used for ended call recovery'); } } as never;
  t.mock.timers.enable({ apis: ['setInterval'] });
  f.failures(1, 1);
  let server = createBrokerServer({ config, store: f.store(), primary, provider });
  const settled = async (predicate: () => boolean) => {
    for (let i = 0; i < 100; i++) {
      await new Promise<void>(resolve => setImmediate(resolve));
      if (predicate()) return;
    }
    assert.fail('maintenance did not settle');
  };
  const restart = async () => {
    await server.close(); f.restart();
    server = createBrokerServer({ config, store: f.store(), primary, provider });
  };
  try {
    t.mock.timers.tick(10_000); await settled(() => f.saved().originalCapture?.state === 'unknown');
    assert.equal(f.store().get(f.call.id)!.primaryClosed, undefined);
    assert.deepEqual(f.actions.filter(action => !action.startsWith('get:')), ['capture:1']);
    await restart();
    t.mock.timers.tick(10_000); await settled(() => !!f.store().get(f.call.id)!.failure);
    assert.equal(f.store().get(f.call.id)!.primaryClosed, undefined);
    assert.equal(f.saved().originalCapture?.state, 'accepted');
    await restart();
    t.mock.timers.tick(10_000); await settled(() => !!f.saved().reconciliationError);
    assert.equal(f.store().get(f.call.id)!.primaryClosePending, true);
    assert.deepEqual(f.actions.filter(action => !action.startsWith('get:')), ['capture:1', 'capture:1', 'close', 'close', 'reconcile']);
    await restart();
    t.mock.timers.tick(10_000);
    for (let i = 0; i < 100 && f.store().get(f.call.id)!.primaryClosePending; i++) await new Promise<void>(resolve => setImmediate(resolve));
    assert.equal(f.store().get(f.call.id)!.primaryClosePending, false);
    assert.equal(f.store().get(f.call.id)!.transcriptCapture?.state, 'complete');
    assert.equal(f.saved().status, 'not_admitted');
    assert.deepEqual(f.actions.filter(action => !action.startsWith('get:')), ['capture:1', 'capture:1', 'close', 'close', 'reconcile', 'reconcile']);
    assert.throws(() => f.store().update(f.call.id, { primaryClosed: { ...closed, bindingId: randomUUID() } }));
    assert.throws(() => f.store().update(f.call.id, { primaryClosed: undefined }));
  } finally { await server.close(); }
});
