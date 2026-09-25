import assert from 'node:assert/strict';
import test, { type TestContext } from 'node:test';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import path from 'node:path';
import { tmpdir } from 'node:os';
import { createHash } from 'node:crypto';
import { BrokerStore, type BrokerCall } from '../src/broker/store.js';
import { beginBrokerPublication, settleBrokerPublication, selectBrokerPublicationReference, rejectBrokerPublication } from '../src/broker/publications.js';
import type { LiveDelivery, LiveTranscriptSnapshot } from '../src/live/types.js';
import type { LiveGenerationJournal } from '../src/broker/liveGeneration.js';

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

async function fixture(t: TestContext, callSid = 'CA_test') {
  const directory = await mkdtemp(path.join(tmpdir(), 'broker-publications-'));
  const store = new BrokerStore(directory);
  t.after(async () => { store.close(); await rm(directory, { recursive: true, force: true }); });
  const call = store.reserve({ requestId: 'synthetic-call', number: '+15550000001', direction: 'outbound', purpose: 'test', callSid }).call;
  const identity = { callId: call.id, callSid, bindingId, sessionId: 'sess_1' };
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

async function generationFixture(t: TestContext) {
  const f = await fixture(t, `CA${'1'.repeat(32)}`);
  const ownerPrincipalId = '44444444-4444-4444-8444-444444444444';
  const primaryDexterId = '55555555-5555-4555-8555-555555555555';
  const threadId = '66666666-6666-4666-8666-666666666666';
  const oldGenerationId = '77777777-7777-4777-8777-777777777777';
  const currentGenerationId = '88888888-8888-4888-8888-888888888888';
  const when = '2026-09-24T12:00:00.000Z';
  const version = { ownerPrincipalId, primaryDexterId, threadId, contextEpoch: '1',
    threadEventSequence: '10', digest: '1'.repeat(64) };
  const liveGenerations: LiveGenerationJournal = {
    version: 1, callId: f.call.id,
    binding: { bindingId, callSid: f.identity.callSid, ownerPrincipalId, primaryDexterId, threadId },
    clocks: { callDeadlineAt: '2026-09-24T12:05:00.000Z' }, phase: 'active', inputNextSample: 0,
    deliveryAttachment: { generationId: currentGenerationId, sessionId: 'sess_2' },
    segments: [
      { generationId: oldGenerationId, ordinal: 1, createdAt: when, state: 'closed', sessionId: 'sess_1',
        closedAt: when, providerFinalized: true, version, pageDigest: 'a'.repeat(64), transcript: before, outputDiscarded: true },
      { generationId: currentGenerationId, ordinal: 2, createdAt: when, state: 'active', sessionId: 'sess_2',
        readyAt: when, version: { ...version, contextEpoch: '2', digest: '2'.repeat(64) },
        pageDigest: 'b'.repeat(64), transcript: before, outputDiscarded: false },
    ],
  };
  f.store.recordPrimaryBinding(f.call.id, { bindingId, callSid: f.identity.callSid, state: 'open',
    context: { primaryDexterId, threadId, displayName: 'Dexter', messages: [] } });
  f.store.update(f.call.id, { liveSessionId: 'sess_2', liveGenerations });
  const deliveryIdentity = { ...f.identity, sessionId: 'sess_2' };
  const publicationIdentity = { ...deliveryIdentity, sourceSessionId: 'sess_1' };
  const update = { id: 'generation_result', kind: 'result' as const, speak: true,
    text: 'The saved comparison is ready.', expectedCallerRevision: 1 };
  const begin = () => beginBrokerPublication(f.store, publicationIdentity, requestA, update, before)!;
  const select = () => selectBrokerPublicationReference(f.store.get(f.call.id)!, deliveryIdentity, after, 2);
  return { ...f, deliveryIdentity, publicationIdentity, update, begin, select };
}

test('accepted current-generation publication retains old request source and supports the next explicit caller reference', async t => {
  const f = await generationFixture(t);
  const original = structuredClone(f.store.get(f.call.id)!.delegations[requestA]);
  const intent = f.begin();
  assert.equal(intent.record.sessionId, 'sess_2');
  assert.equal(intent.record.sourceSessionId, 'sess_1');
  assert.equal(f.select(), undefined);
  settleBrokerPublication(f.store, intent.record, accepted('new_generation_command'), before, false);
  assert.deepEqual(f.select(), { relatedPrimaryRequestId: requestA, referenceOrigin: 'broker_status_publication' });
  assert.equal('sourceSessionId' in f.deliveryIdentity, false, 'new caller does not claim the earlier source session');
  assert.deepEqual(f.store.get(f.call.id)!.delegations[requestA], original);
  const bytes = await readFile(path.join(f.directory, 'calls.json'), 'utf8');
  f.store.close();
  const restarted = new BrokerStore(f.directory); t.after(() => restarted.close());
  assert.equal(await readFile(path.join(f.directory, 'calls.json'), 'utf8'), bytes);
  assert.deepEqual(selectBrokerPublicationReference(restarted.get(f.call.id)!, f.deliveryIdentity, after, 2),
    { relatedPrimaryRequestId: requestA, referenceOrigin: 'broker_status_publication' });
});

test('cross-generation publication needs explicit original source and both registered sessions', async t => {
  const f = await generationFixture(t);
  assert.equal(beginBrokerPublication(f.store, f.deliveryIdentity, requestA, f.update, before), undefined);
  assert.equal(beginBrokerPublication(f.store, { ...f.publicationIdentity, sourceSessionId: 'sess_2' }, requestA, f.update, before), undefined);
  assert.equal(beginBrokerPublication(f.store, { ...f.publicationIdentity, sourceSessionId: 'other' }, requestA, f.update, before), undefined);
  for (const missing of ['all', 'sess_1', 'sess_2']) {
    const call = structuredClone(f.store.get(f.call.id)!);
    if (missing === 'all') delete call.liveGenerations;
    else call.liveGenerations!.segments = call.liveGenerations!.segments.filter(segment => segment.sessionId !== missing);
    const readOnly = { get: () => call, update: () => { throw new Error('unexpected publication write'); } };
    assert.equal(beginBrokerPublication(readOnly, f.publicationIdentity, requestA, f.update, before), undefined);
  }
  assert.equal(f.store.get(f.call.id)!.publications, undefined);
});

test('saved cross-generation selection rejects changed source identity and missing generation evidence', async t => {
  const f = await generationFixture(t);
  const intent = f.begin();
  settleBrokerPublication(f.store, intent.record, accepted('new_generation_command'), before, false);
  for (const mutate of [
    (call: ReturnType<typeof f.store.get>) => { call!.publications!.records[0].sourceSessionId = 'sess_2'; },
    (call: ReturnType<typeof f.store.get>) => { delete call!.publications!.records[0].sourceSessionId; },
    (call: ReturnType<typeof f.store.get>) => { delete call!.liveGenerations; },
    (call: ReturnType<typeof f.store.get>) => { call!.liveGenerations!.segments = call!.liveGenerations!.segments.slice(1); },
    (call: ReturnType<typeof f.store.get>) => { call!.liveGenerations!.segments = call!.liveGenerations!.segments.slice(0, 1); },
    (call: ReturnType<typeof f.store.get>) => {
      (call!.delegations[requestA] as BrokerCall['delegations'][string] & { sessionId: string }).sessionId = 'foreign';
    },
    (call: ReturnType<typeof f.store.get>) => { call!.publications!.records[0].sessionId = 'sess_1'; },
  ]) {
    const call = structuredClone(f.store.get(f.call.id)!); mutate(call);
    assert.equal(selectBrokerPublicationReference(call, f.deliveryIdentity, after, 2), undefined);
  }
  assert.ok(f.select(), 'retained original remains eligible');
});

test('new generation does not infer a request from recency or acceptance in the retired session', async t => {
  const f = await generationFixture(t);
  const call = f.store.get(f.call.id)!;
  assert.equal(f.select(), undefined, 'completed request alone is not an accepted publication');
  assert.equal(selectBrokerPublicationReference(call, f.identity, after, 2), undefined);
  const a = f.begin();
  const b = beginBrokerPublication(f.store, f.publicationIdentity, requestB,
    { ...f.update, id: 'generation_result_b' }, before)!;
  settleBrokerPublication(f.store, a.record, accepted('generation_command_a'), before, false);
  assert.equal(f.select(), undefined, 'second publication remains uncertain');
  settleBrokerPublication(f.store, b.record, accepted('generation_command_b'), before, false);
  assert.equal(f.select(), undefined, 'two accepted requests do not select the newest');
});

test('cross-generation publication exact replay preserves its source and command while changed content is refused', async t => {
  const f = await generationFixture(t);
  const first = f.begin();
  settleBrokerPublication(f.store, first.record, accepted('generation_command'), before, false);
  const record = structuredClone(f.store.get(f.call.id)!.publications!.records[0]);
  assert.equal(f.begin().created, false);
  assert.deepEqual(f.begin().record, record);
  assert.throws(() => beginBrokerPublication(f.store, f.publicationIdentity, requestA,
    { ...f.update, text: 'Altered answer.' }, before), /publication_identity_changed/);
  assert.equal(beginBrokerPublication(f.store, { ...f.publicationIdentity, sourceSessionId: 'other' },
    requestA, f.update, before), undefined);
  assert.deepEqual(f.store.get(f.call.id)!.publications!.records[0], record);
});

test('legacy publication JSON order and text digest stay unchanged with absent source session and generation journal', async t => {
  const f = await fixture(t);
  const intent = f.begin();
  const expected = { ...f.identity, updateId: 'update_a', requestId: requestA, requestState: 'completed',
    kind: 'result', speak: true, textDigest: createHash('sha256').update('Portfolio result', 'utf8').digest('hex'),
    callerRevision: 1, transcriptSequence: 1, publicationSequence: 1 };
  assert.equal(JSON.stringify(intent.record), JSON.stringify(expected));
  assert.equal('sourceSessionId' in intent.record, false);
  assert.equal(f.store.get(f.call.id)!.liveGenerations, undefined);
  settleBrokerPublication(f.store, intent.record, accepted('legacy_command'), before, false);
  assert.equal(JSON.stringify(f.store.get(f.call.id)!.publications!.records[0]), JSON.stringify({ ...expected,
    outcome: { eventId: 'legacy_command', status: 'accepted', callerRevision: 1, transcriptSequence: 1 } }));
  assert.equal(f.select()?.relatedPrimaryRequestId, requestA);
});
