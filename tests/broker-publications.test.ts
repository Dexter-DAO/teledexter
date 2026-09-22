import assert from 'node:assert/strict';
import test, { type TestContext } from 'node:test';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import path from 'node:path';
import { tmpdir } from 'node:os';
import { BrokerStore } from '../src/broker/store.js';
import { beginBrokerPublication, settleBrokerPublication, selectBrokerPublicationReference, rejectBrokerPublication } from '../src/broker/publications.js';
import type { LiveDelivery, LiveTranscriptSnapshot } from '../src/live/types.js';

const bindingId = '11111111-1111-4111-8111-111111111111';
const requestA = '22222222-2222-4222-8222-222222222222';
const requestB = '33333333-3333-4333-8333-333333333333';
const before: LiveTranscriptSnapshot = { revision: 1, callerRevision: 1, fragments: [
  { eventId: 'caller_1', role: 'user', text: 'Show my portfolio', startMs: 100, endMs: 1000, sequence: 1 },
] };
const after: LiveTranscriptSnapshot = { revision: 2, callerRevision: 2, fragments: [...before.fragments,
  { eventId: 'caller_2', role: 'user', text: ' Then review it.', startMs: 4000, endMs: 5000, sequence: 2 },
] };
const accepted = (eventId: string): LiveDelivery => ({ eventId, status: 'accepted', observedCallerRevision: 1, observedTranscriptSequence: 1 });

async function fixture(t: TestContext) {
  const directory = await mkdtemp(path.join(tmpdir(), 'broker-publications-'));
  const store = new BrokerStore(directory);
  t.after(async () => { store.close(); await rm(directory, { recursive: true, force: true }); });
  const call = store.reserve({ requestId: 'synthetic-call', number: '+15550000001', direction: 'outbound', purpose: 'test', callSid: 'CA_test' }).call;
  const identity = { callId: call.id, callSid: 'CA_test', bindingId, sessionId: 'sess_1' };
  store.update(call.id, { status: 'in-progress', liveSessionId: identity.sessionId, primary: { bindingId, name: null, context: '' },
    delegations: Object.fromEntries([requestA, requestB].map(requestId => [requestId, {
      ...identity, requestId, status: 'completed', updatedAt: '2026-09-22T00:00:00.000Z',
      primaryResult: { requestId, bindingId, state: 'completed' },
    }])) });
  const begin = (requestId = requestA, updateId = 'update_a') => beginBrokerPublication(store, identity, requestId,
    { id: updateId, kind: 'result', speak: true, text: 'Portfolio result', expectedCallerRevision: 1 }, before)!;
  const select = () => selectBrokerPublicationReference(store.get(call.id)!, identity, after, 2);
  return { store, directory, call, identity, begin, select };
}

test('intent is durable before publication, and acceptance selects exact request with a distinct command ID', async t => {
  const f = await fixture(t);
  const intent = f.begin();
  assert.equal(intent.created, true);
  const disk = JSON.parse(await readFile(path.join(f.directory, 'calls.json'), 'utf8'));
  assert.deepEqual(disk.calls[f.call.id].publications.records, [intent.record]);
  assert.equal(f.select(), undefined);
  settleBrokerPublication(f.store, intent.record, accepted('broker_7'), before, false);
  assert.deepEqual(f.select(), { relatedPrimaryRequestId: requestA, referenceOrigin: 'broker_status_publication' });
  assert.equal(f.begin().created, false);
  assert.equal(f.store.get(f.call.id)!.publications!.records.length, 1);
});

test('late earlier acknowledgment cannot replace newer publication order or resolve competing requests', async t => {
  const f = await fixture(t);
  const a = f.begin(); const b = f.begin(requestB, 'update_b');
  settleBrokerPublication(f.store, b.record, accepted('broker_2'), before, false);
  assert.equal(f.select(), undefined, 'older publication still has unknown acceptance');
  settleBrokerPublication(f.store, a.record, accepted('broker_1'), before, false);
  assert.equal(f.select(), undefined, 'two accepted requests are ambiguous');
  assert.deepEqual(f.store.get(f.call.id)!.publications!.records.map(r => [r.publicationSequence, r.outcome!.eventId]), [[1, 'broker_1'], [2, 'broker_2']]);
});

test('same-request chunks can arrive out of order without selecting an unrelated saved request', async t => {
  const f = await fixture(t);
  const first = f.begin(); const second = f.begin(requestA, 'update_b');
  settleBrokerPublication(f.store, second.record, accepted('broker_2'), before, false);
  settleBrokerPublication(f.store, first.record, accepted('broker_1'), before, false);
  assert.equal(f.select()?.relatedPrimaryRequestId, requestA);
});

test('restart preserves publication ordinal and uncertainty until the exact original outcome is known', async t => {
  const f = await fixture(t); const a = f.begin(); const b = f.begin(requestB, 'update_b');
  settleBrokerPublication(f.store, b.record, accepted('broker_2'), before, false);
  const bytes = await readFile(path.join(f.directory, 'calls.json'), 'utf8');
  f.store.close();
  const reopened = new BrokerStore(f.directory); t.after(() => reopened.close());
  assert.equal(await readFile(path.join(f.directory, 'calls.json'), 'utf8'), bytes);
  assert.equal(selectBrokerPublicationReference(reopened.get(f.call.id)!, f.identity, after, 2), undefined);
  settleBrokerPublication(reopened, a.record, accepted('broker_1'), before, false);
  assert.equal(selectBrokerPublicationReference(reopened.get(f.call.id)!, f.identity, after, 2), undefined);
  assert.deepEqual(reopened.get(f.call.id)!.publications!.records.map(value => value.publicationSequence), [1, 2]);
});

test('acknowledgment after raw caller input is ineligible even while durable context is older', async t => {
  const f = await fixture(t); const intent = f.begin();
  settleBrokerPublication(f.store, intent.record, { ...accepted('broker_1'), observedCallerRevision: 2,
    observedTranscriptSequence: 2 }, before, false);
  assert.equal(f.select(), undefined);
  assert.equal(f.store.get(f.call.id)!.publications!.records[0].outcome!.status, 'stale');
});

test('an accepted result with a different canonical request ID cannot be guessed from a local alias', async t => {
  const f = await fixture(t);
  const call = f.store.get(f.call.id)!;
  f.store.update(call.id, { delegations: { ...call.delegations, [requestA]: { ...call.delegations[requestA],
    primaryResult: { bindingId, requestId: requestB, state: 'completed' } } as typeof call.delegations[string] } });
  assert.equal(f.begin(), undefined);
});

for (const status of ['rejected', 'stale', 'closed', 'unconfirmed'] as const) test(`${status} publication cannot establish a reference`, async t => {
  const f = await fixture(t); const intent = f.begin();
  settleBrokerPublication(f.store, intent.record, { eventId: 'broker_1', status }, before, false);
  assert.equal(f.select(), undefined);
});

test('late rejection is matched by call, session and command, including before receipt persistence', async t => {
  const f = await fixture(t); const intent = f.begin();
  rejectBrokerPublication(f.store, { ...f.identity, sessionId: 'foreign' }, 'broker_1', 'bad');
  settleBrokerPublication(f.store, intent.record, accepted('broker_1'), before, false);
  assert.ok(f.select());
  rejectBrokerPublication(f.store, f.identity, 'unrelated_command', 'bad');
  assert.ok(f.select());
  rejectBrokerPublication(f.store, f.identity, 'broker_1', 'bad');
  assert.equal(f.select(), undefined);
  assert.equal(f.store.get(f.call.id)!.publications!.records[0].outcome!.status, 'accepted', 'receipt remains historical evidence');
  const newer = f.begin(requestA, 'update_b');
  rejectBrokerPublication(f.store, f.identity, 'broker_2', 'bad');
  settleBrokerPublication(f.store, newer.record, accepted('broker_2'), before, false);
  assert.equal(f.select(), undefined);
});

test('one command cannot stand for two different publication intents', async t => {
  const f = await fixture(t); const a = f.begin(); const b = f.begin(requestA, 'update_b');
  settleBrokerPublication(f.store, a.record, accepted('broker_1'), before, false);
  settleBrokerPublication(f.store, b.record, accepted('broker_1'), before, false);
  assert.equal(f.select(), undefined);
});

test('caller change, call closure, foreign binding and session changes exclude accepted receipt', async t => {
  const f = await fixture(t); const intent = f.begin();
  settleBrokerPublication(f.store, intent.record, accepted('broker_1'), after, false);
  assert.equal(f.select(), undefined);
  assert.equal(beginBrokerPublication(f.store, { ...f.identity, bindingId: requestB }, requestA,
    { id: 'other', kind: 'result', speak: true, text: 'text', expectedCallerRevision: 1 }, before), undefined);
  f.store.update(f.call.id, { liveSessionId: 'sess_2' });
  assert.equal(f.select(), undefined);
  f.store.update(f.call.id, { liveSessionId: 'sess_1', endedAt: '2026-09-22T00:01:00.000Z' });
  assert.equal(f.select(), undefined);
});

test('quiet thinking, missing request, unknown admission and changed update bodies are refused', async t => {
  const f = await fixture(t);
  const update = { id: 'quiet', kind: 'progress' as const, text: 'thinking', expectedCallerRevision: 1 };
  assert.equal(beginBrokerPublication(f.store, f.identity, requestA, update, before), undefined);
  assert.equal(beginBrokerPublication(f.store, f.identity, bindingId, { ...update, speak: true }, before), undefined);
  f.begin();
  assert.throws(() => beginBrokerPublication(f.store, f.identity, requestA,
    { id: 'update_a', kind: 'result', speak: true, text: 'changed', expectedCallerRevision: 1 }, before), /publication_identity_changed/);
  const call = f.store.get(f.call.id)!;
  f.store.update(f.call.id, { delegations: { ...call.delegations, [requestA]: { ...call.delegations[requestA], status: 'dispatch_unknown' } } });
  assert.equal(f.begin(requestA, 'unknown'), undefined);
});
