import assert from 'node:assert/strict';
import test from 'node:test';
import { liveResultChunks, type BrokerDelegationRecord } from '../src/broker/backend.js';
import { beginBrokerPublication, beginSelectedQuestionPublication, callerIntentResultPublicationCandidates,
  callerIntentResultPublicationInventory, CallerIntentResultInventoryError, rejectBrokerPublication,
  settleBrokerPublication, validateSelectedQuestionPublications, type BrokerPublicationRecord,
  type OrdinaryPublicationEvidence } from '../src/broker/publications.js';
import type { BrokerCall } from '../src/broker/store.js';
import type { LiveTranscriptSnapshot } from '../src/live/types.js';
import type { eligibleAssociatedDeliveryResponseSchema } from '../src/broker/eligibleContext.js';
import type { z } from 'zod';
import { PHONE_REQUEST_DELIVERY_VERSION, createRequestDeliveryJournal, retainRequestDeliveryAssociation,
  createRequestDeliveryQuestionRecord } from '../src/broker/requestDelivery.js';
import { parseSelectedQuestionJournal, type SelectedQuestionSeed } from '../src/broker/selectedQuestion.js';

// Local fixture uses the existing durable publication reducers, with only storage replaced.
const id = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const at = '2026-09-24T10:00:00.000Z';
const deadline = '2999-01-01T00:00:00.000Z';
const snapshot: LiveTranscriptSnapshot = { revision: 1, callerRevision: 1,
  fragments: [{ eventId: 'caller_1', role: 'user', text: 'Read my portfolio.', startMs: 1, endMs: 10, sequence: 1 }] };

function fixture() {
  const scope = { ownerPrincipalId: id(2), primaryDexterId: id(3), threadId: id(4) };
  const binding = { bindingId: id(5), callSid: `CA${'a'.repeat(32)}`, ...scope };
  const version = { ...scope, contextEpoch: '1', threadEventSequence: '20', digest: 'a'.repeat(64) };
  let call: BrokerCall = { id: id(1), requestId: id(99), direction: 'outbound', number: '+15550000001',
    purpose: 'Read the saved result.', createdAt: at, updatedAt: at, status: 'in-progress',
    callSid: binding.callSid, liveSessionId: 'current', primary: { ...binding, name: null, context: '' },
    delegations: {}, liveGenerations: { version: 1, callId: id(1), binding, clocks: { callDeadlineAt: deadline },
      phase: 'active', inputNextSample: 0, deliveryAttachment: { generationId: id(6), sessionId: 'current' },
      segments: [{ generationId: id(6), ordinal: 1, createdAt: at, readyAt: at, state: 'active', sessionId: 'current',
        version, pageDigest: 'b'.repeat(64), transcript: snapshot, outputDiscarded: false }] } };
  const store = { get: (callId: string) => callId === call.id ? structuredClone(call) : undefined,
    update(callId: string, patch: Partial<BrokerCall>) {
      assert.equal(callId, call.id);
      const next = { ...call, ...structuredClone(patch) };
      validateSelectedQuestionPublications(next, call);
      call = next;
      return structuredClone(call);
    } };
  const identity = () => ({ callId: call.id, callSid: binding.callSid, bindingId: binding.bindingId,
    sessionId: call.liveSessionId! });
  function ordinary(n = 10, text = 'The fetched portfolio value is 41 dollars and 53 cents.', canonical = n) {
    const response: OrdinaryPublicationEvidence['response'] = { bindingId: binding.bindingId,
      callSid: binding.callSid, requestId: id(canonical), version, delivery: { state: 'ready', text,
        source: { kind: 'assistant', messageId: id(n + 100), eventSequence: '10', provenance: {
          messageCreatedAt: at, eventId: id(n + 200), eventOccurredAt: at, runId: id(n + 300) } } } };
    const entry = { ...identity(), version: 1, requestId: id(n), status: 'completed', updatedAt: at,
      primaryResult: { requestId: id(canonical), bindingId: binding.bindingId, state: 'completed', assistant: {
        messageId: id(n + 100), eventId: id(n + 200), sequence: '10', text } } } as BrokerDelegationRecord;
    if (n !== canonical) entry.reconciliation = { outcome: 'admitted', requestId: id(n), bindingId: binding.bindingId,
      canonicalRequestId: id(canonical), result: structuredClone(entry.primaryResult!) };
    call.delegations[id(n)] = entry;
    return { requestId: id(n), response };
  }
  function publish(value: ReturnType<typeof ordinary>, attempt = 1, indices?: number[]) {
    assert.equal(value.response.delivery.state, 'ready');
    if (value.response.delivery.state !== 'ready') throw new Error('fixture_delivery');
    const chunks = liveResultChunks(value.response.delivery.text);
    const records: BrokerPublicationRecord[] = [];
    for (const index of indices ?? chunks.map((_, index) => index)) {
      const entry = call.delegations[value.requestId] as BrokerDelegationRecord;
      const record = beginBrokerPublication(store, { ...identity(),
        ...(entry.sessionId !== call.liveSessionId ? { sourceSessionId: entry.sessionId } : {}) }, value.requestId,
      { id: `result_${value.requestId}_attempt_${attempt}_${index}`, kind: 'result', speak: true,
        text: chunks[index]!, expectedCallerRevision: 1 }, snapshot, { response: value.response, chunkIndex: index })!.record;
      settleBrokerPublication(store, record, { eventId: `command_${record.publicationSequence}`, status: 'accepted',
        observedCallerRevision: 1, observedTranscriptSequence: 1 }, snapshot, false);
      records.push(record);
    }
    return records;
  }
  return { store, identity, ordinary, publish, current: () => structuredClone(call),
    replace: (next: BrokerCall) => { call = structuredClone(next); }, binding, scope, version };
}

function refuses(call: BrokerCall, code: ConstructorParameters<typeof CallerIntentResultInventoryError>[0], sessionId = 'current') {
  const bytes = JSON.stringify(call);
  assert.throws(() => callerIntentResultPublicationInventory(call, sessionId), error =>
    error instanceof CallerIntentResultInventoryError && error.code === code);
  assert.equal(JSON.stringify(call), bytes);
}

for (const count of [0, 3, 7, 8]) test(`inventory certifies all ${count} ordered results without selecting a subset`, () => {
  const f = fixture();
  const order = [17, 10, 16, 11, 15, 12, 14, 13].slice(0, count);
  for (const n of order) f.publish(f.ordinary(n));
  const call = f.current(), bytes = JSON.stringify(call);
  const inventory = callerIntentResultPublicationInventory(call, 'current');
  assert.deepEqual(inventory.resultCoverage, { state: count <= 7 ? 'complete' : 'overflow', count });
  assert.deepEqual(inventory.candidates.map(value => value.candidate.execution.requestId), order.map(id));
  assert.deepEqual(inventory.candidates, callerIntentResultPublicationCandidates(call, 'current'));
  assert.equal(JSON.stringify(call), bytes);
  assert.deepEqual(callerIntentResultPublicationInventory(JSON.parse(bytes), 'current'), inventory);
});

test('an older reference retains its original anchor after later results and duplicate presentations', () => {
  const f = fixture(), older = f.ordinary(10); f.publish(older);
  const first = callerIntentResultPublicationInventory(f.current(), 'current').candidates[0]!;
  f.publish(f.ordinary(12)); f.publish(older, 2); f.publish(f.ordinary(11));
  const inventory = callerIntentResultPublicationInventory(f.current(), 'current');
  assert.deepEqual(inventory.resultCoverage, { state: 'complete', count: 3 });
  assert.deepEqual(inventory.candidates[0], first);
  assert.deepEqual(inventory.candidates[0]!.reference, { kind: 'ordinary', relatedPrimaryRequestId: id(10),
    referenceOrigin: 'broker_status_publication' });
  assert.deepEqual(inventory.candidates.map(value => value.candidate.execution.requestId), [id(10), id(12), id(11)]);
  const historical = callerIntentResultPublicationCandidates(f.current(), 'current', { historical: true });
  assert.equal(historical.length, 4);
  assert.equal(historical[0]!.id, first.id);
  assert.equal(historical[0]!.digest, first.digest);
});

test('fresh inventory refuses inactive or different-generation scope while v1 saved anchors remain recoverable', () => {
  const f = fixture(); f.publish(f.ordinary());
  const original = f.current(), historical = callerIntentResultPublicationCandidates(original, 'current', { historical: true });
  refuses(original, 'caller_result_inventory_inactive', 'foreign');
  for (const change of [
    (call: BrokerCall) => { call.endedAt = at; },
    (call: BrokerCall) => { call.finalized = true; },
    (call: BrokerCall) => { call.status = 'completed'; },
    (call: BrokerCall) => { call.liveGenerations!.clocks.callDeadlineAt = at; },
    (call: BrokerCall) => { call.liveGenerations!.phase = 'replacing'; delete call.liveGenerations!.deliveryAttachment; },
  ]) {
    const call = structuredClone(original); change(call);
    refuses(call, 'caller_result_inventory_inactive');
    assert.deepEqual(callerIntentResultPublicationCandidates(call, 'current', { historical: true }), historical);
  }
  const mismatch = structuredClone(original); mismatch.primary!.bindingId = id(999);
  refuses(mismatch, 'caller_result_inventory_invalid');
});

test('a replacement session counts only its own re-publication and retains the original source identity', () => {
  const f = fixture(), value = f.ordinary(); f.publish(value);
  const call = f.current(), old = call.liveGenerations!.segments[0]!;
  old.state = 'closed'; old.closedAt = at;
  call.liveSessionId = 'next'; call.liveGenerations!.segments.push({ ...old, generationId: id(7), ordinal: 2,
    state: 'active', sessionId: 'next', closedAt: undefined });
  call.liveGenerations!.deliveryAttachment = { generationId: id(7), sessionId: 'next' }; f.replace(call);
  assert.deepEqual(callerIntentResultPublicationInventory(f.current(), 'next').resultCoverage, { state: 'complete', count: 0 });
  f.publish(value);
  const next = callerIntentResultPublicationInventory(f.current(), 'next');
  assert.deepEqual(next.resultCoverage, { state: 'complete', count: 1 });
  assert.equal(f.current().publications!.records.at(-1)!.sourceSessionId, 'current');
  assert.deepEqual(next.candidates[0]!.candidate.execution, { bindingId: id(5), requestId: id(10), runId: id(310) });
  assert.equal(callerIntentResultPublicationCandidates(f.current(), 'current', { historical: true }).length, 1);
});

test('accepted completed legacy output cannot disappear into a complete empty inventory', () => {
  const f = fixture(), value = f.ordinary();
  const record = beginBrokerPublication(f.store, f.identity(), value.requestId, { id: 'legacy_0', kind: 'result',
    speak: true, text: 'An old result.', expectedCallerRevision: 1 }, snapshot)!.record;
  settleBrokerPublication(f.store, record, { eventId: 'legacy_receipt', status: 'accepted', observedCallerRevision: 1,
    observedTranscriptSequence: 1 }, snapshot, false);
  const call = f.current();
  assert.deepEqual(callerIntentResultPublicationCandidates(call, 'current'), []);
  refuses(call, 'caller_result_inventory_unverifiable');
});

test('unconfirmed, provisional and incomplete typed presentations refuse instead of declaring zero results', () => {
  const f = fixture(), value = f.ordinary();
  const record = beginBrokerPublication(f.store, f.identity(), value.requestId, { id: 'provisional_0', kind: 'result',
    speak: true, text: value.response.delivery.state === 'ready' ? value.response.delivery.text : '', expectedCallerRevision: 1 },
  snapshot, { response: value.response, chunkIndex: 0 })!.record;
  refuses(f.current(), 'caller_result_inventory_unverifiable');
  settleBrokerPublication(f.store, record, { eventId: 'uncertain_receipt', status: 'unconfirmed' }, snapshot, false);
  refuses(f.current(), 'caller_result_inventory_unverifiable');
  const chunksFixture = fixture(), long = chunksFixture.ordinary(10, Array(40).fill('A fetched holding is worth twelve dollars.').join(' '));
  if (long.response.delivery.state !== 'ready') throw new Error('fixture_delivery');
  const chunks = liveResultChunks(long.response.delivery.text);
  assert.ok(chunks.length > 1);
  chunksFixture.publish(long, 1, [0]);
  chunksFixture.publish(long, 2, chunks.map((_, index) => index).slice(1));
  refuses(chunksFixture.current(), 'caller_result_inventory_unverifiable');
  chunksFixture.publish(long, 3);
  const inventory = callerIntentResultPublicationInventory(chunksFixture.current(), 'current');
  assert.deepEqual(inventory.resultCoverage, { state: 'complete', count: 1 });
  assert.match(inventory.candidates[0]!.id, new RegExp(`:${chunksFixture.current().publications!.records.find(r => r.updateId.endsWith('_attempt_3_0'))!.publicationSequence}$`));
});

test('rejected commands and late tombstones never manufacture completion', () => {
  const f = fixture(), value = f.ordinary();
  const record = beginBrokerPublication(f.store, f.identity(), value.requestId, { id: 'rejected_0', kind: 'result',
    speak: true, text: value.response.delivery.state === 'ready' ? value.response.delivery.text : '', expectedCallerRevision: 1 },
  snapshot, { response: value.response, chunkIndex: 0 })!.record;
  settleBrokerPublication(f.store, record, { eventId: 'rejected_receipt', status: 'rejected' }, snapshot, false);
  assert.deepEqual(callerIntentResultPublicationInventory(f.current(), 'current'), {
    candidates: [], resultCoverage: { state: 'complete', count: 0 },
  });
  f.publish(value);
  const accepted = f.current().publications!.records.at(-1)!;
  rejectBrokerPublication(f.store, f.identity(), accepted.outcome!.eventId, 'provider_error');
  assert.deepEqual(callerIntentResultPublicationInventory(f.current(), 'current').resultCoverage, { state: 'complete', count: 0 });
  assert.equal(callerIntentResultPublicationCandidates(f.current(), 'current', { historical: true }).length, 1);
});

test('malformed journal, source identity, rendering, receipt and rejection data refuse without mutation', () => {
  const f = fixture(); f.publish(f.ordinary(10)); f.publish(f.ordinary(11));
  const original = f.current();
  for (const change of [
    (call: BrokerCall) => { call.publications!.records[0]!.publicationSequence = 2; },
    (call: BrokerCall) => { call.publications!.records[0]!.callId = id(999); },
    (call: BrokerCall) => { call.publications!.records[0]!.sessionId = 'absent'; },
    (call: BrokerCall) => { call.publications!.records[0]!.requestId = id(999); },
    (call: BrokerCall) => { call.publications!.records[0]!.textDigest = 'f'.repeat(64); },
    (call: BrokerCall) => { call.publications!.records[0]!.outcome!.callerRevision = 2; },
    (call: BrokerCall) => { call.publications!.records[1]!.outcome!.eventId = call.publications!.records[0]!.outcome!.eventId; },
    (call: BrokerCall) => { call.publications!.rejections.push({ sessionId: 'absent', eventId: 'command_1', code: 'provider_error' }); },
    (call: BrokerCall) => { const response = call.publications!.records[0]!.ordinaryDelivery!.response;
      if (response.delivery.state === 'ready') response.delivery.source.provenance.eventId = id(999); },
  ]) {
    const call = structuredClone(original); change(call);
    refuses(call, 'caller_result_inventory_invalid');
  }
  for (const publications of [null, {}, { records: [], rejections: [], extra: true }, { records: 'unreadable', rejections: [] }])
    refuses(JSON.parse(JSON.stringify({ ...original, publications })) as BrokerCall, 'caller_result_inventory_invalid');
  const missing = structuredClone(original); delete missing.publications!.records[0]!.ordinaryDelivery;
  refuses(missing, 'caller_result_inventory_unverifiable');
});

test('six ordinary results and the selected original form seven exact distinct identities', () => {
  const f = fixture(); for (const n of [15, 10, 14, 11, 13, 12]) f.publish(f.ordinary(n));
  const association = { v: PHONE_REQUEST_DELIVERY_VERSION, id: id(40), delivery: { bindingId: id(5), callSid: f.binding.callSid },
    execution: { bindingId: id(41), requestId: id(42), runId: id(43) }, question: { id: id(44), revision: 1 as const }, createdAt: at };
  const call = f.current(); call.continuation = { originalRequestId: id(42), questionId: id(44), questionRevision: 1 };
  const seed: SelectedQuestionSeed = { v: 'broker.selected-question-seed.v1', scope: f.scope,
    deliveryCall: { callId: call.id, sessionId: 'current' }, delivery: association.delivery,
    request: { v: PHONE_REQUEST_DELIVERY_VERSION, ...call.continuation }, callDeadlineAt: deadline };
  const base = retainRequestDeliveryAssociation(createRequestDeliveryJournal({ deliveryCall: seed.deliveryCall,
    scope: seed.scope, request: seed.request, target: { delivery: seed.delivery,
      execution: association.execution, question: association.question } }), { association, replayed: false });
  call.selectedQuestion = parseSelectedQuestionJournal({ v: 'broker.selected-question-host.v1', initial: seed,
    questions: [{ seed, record: createRequestDeliveryQuestionRecord(base), admissions: [] }] });
  f.replace(call);
  const response: z.infer<typeof eligibleAssociatedDeliveryResponseSchema> = { bindingId: id(5), callSid: f.binding.callSid,
    requestId: id(42), version: f.version, deliveryAssociation: association,
    delivery: { state: 'ready', text: 'The earlier comparison is saved.', source: { kind: 'assistant', messageId: id(45),
      eventSequence: '11', provenance: { messageCreatedAt: at, eventId: id(46), eventOccurredAt: at, runId: id(43) } } } };
  const record = beginSelectedQuestionPublication(f.store, f.identity(), response, { id: 'selected_attempt_1_0', kind: 'result',
    speak: true, text: 'The earlier comparison is saved.', expectedCallerRevision: 1 }, snapshot, 0)!.record;
  settleBrokerPublication(f.store, record, { eventId: 'selected_receipt', status: 'accepted', observedCallerRevision: 1,
    observedTranscriptSequence: 1 }, snapshot, false);
  const inventory = callerIntentResultPublicationInventory(f.current(), 'current');
  assert.deepEqual(inventory.resultCoverage, { state: 'complete', count: 7 });
  assert.deepEqual(inventory.candidates.at(-1)!.candidate.execution, association.execution);
  assert.deepEqual(inventory.candidates.at(-1)!.reference, { kind: 'associated', associatedResultReference: {
    v: 'phone.associated-result-reference.v1', deliveryAssociationId: association.id,
    originalRequestId: association.execution.requestId, assistantMessageId: id(45),
  } });
  const malformed = f.current(); malformed.publications!.records.at(-1)!.selectedDelivery!.response.deliveryAssociation.execution.runId = id(999);
  refuses(malformed, 'caller_result_inventory_invalid');
});
