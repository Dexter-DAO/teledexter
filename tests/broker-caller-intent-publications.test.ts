import assert from 'node:assert/strict';
import test from 'node:test';
import { liveResultChunks, type BrokerDelegationRecord } from '../src/broker/backend.js';
import { canonicalJson } from '../src/outbound/canonical.js';
import { beginBrokerPublication, beginSelectedQuestionPublication, callerIntentResultPublicationCandidates,
  rejectBrokerPublication, settleBrokerPublication, validateSelectedQuestionPublications,
  type BrokerPublicationRecord, type OrdinaryPublicationEvidence } from '../src/broker/publications.js';
import type { BrokerCall } from '../src/broker/store.js';
import type { LiveTranscriptSnapshot } from '../src/live/types.js';
import type { eligibleAssociatedDeliveryResponseSchema } from '../src/broker/eligibleContext.js';
import type { z } from 'zod';
import { PHONE_REQUEST_DELIVERY_VERSION, createRequestDeliveryJournal, retainRequestDeliveryAssociation,
  createRequestDeliveryQuestionRecord } from '../src/broker/requestDelivery.js';
import { parseSelectedQuestionJournal, type SelectedQuestionSeed } from '../src/broker/selectedQuestion.js';

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

test('ordinary candidate retains the exact eligible run/message and complete accepted evidence after reload', () => {
  const f = fixture(), value = f.ordinary();
  f.publish(value);
  const saved = f.current(), bytes = JSON.stringify(saved);
  validateSelectedQuestionPublications(JSON.parse(bytes));
  const candidates = callerIntentResultPublicationCandidates(JSON.parse(bytes), 'current');
  assert.equal(candidates.length, 1);
  assert.deepEqual(candidates[0]!.reference, { kind: 'ordinary', relatedPrimaryRequestId: value.requestId,
    referenceOrigin: 'broker_status_publication' });
  assert.deepEqual(candidates[0]!.candidate, { kind: 'ordinary', execution: { bindingId: id(5), requestId: id(10), runId: id(310) },
    assistantMessageId: id(110), presentationId: candidates[0]!.id });
  assert.match(candidates[0]!.digest, /^[0-9a-f]{64}$/);
  assert.equal(JSON.stringify(saved), bytes);
});

test('absent delivery evidence preserves legacy publication bytes and never fabricates a candidate', () => {
  const f = fixture(), value = f.ordinary();
  const intent = beginBrokerPublication(f.store, f.identity(), value.requestId, { id: 'legacy', kind: 'result',
    speak: true, text: 'An old saved answer.', expectedCallerRevision: 1 }, snapshot)!.record;
  settleBrokerPublication(f.store, intent, { eventId: 'legacy_command', status: 'accepted', observedCallerRevision: 1,
    observedTranscriptSequence: 1 }, snapshot, false);
  const bytes = JSON.stringify(f.current());
  assert.equal('ordinaryDelivery' in f.current().publications!.records[0]!, false);
  assert.deepEqual(callerIntentResultPublicationCandidates(f.current(), 'current'), []);
  validateSelectedQuestionPublications(f.current());
  assert.equal(JSON.stringify(f.current()), bytes);
});

test('progress and unconfirmed/rejected publication commands are never result candidates', () => {
  const f = fixture(), value = f.ordinary();
  assert.throws(() => beginBrokerPublication(f.store, f.identity(), value.requestId, { id: 'progress_0', kind: 'progress',
    speak: true, text: 'Checking now.', expectedCallerRevision: 1 }, snapshot, { response: value.response, chunkIndex: 0 }));
  const intent = beginBrokerPublication(f.store, f.identity(), value.requestId, { id: 'result_0', kind: 'result',
    speak: true, text: value.response.delivery.state === 'ready' ? value.response.delivery.text : '', expectedCallerRevision: 1 },
  snapshot, { response: value.response, chunkIndex: 0 })!.record;
  assert.deepEqual(callerIntentResultPublicationCandidates(f.current(), 'current'), []);
  settleBrokerPublication(f.store, intent, { eventId: 'unconfirmed', status: 'unconfirmed' }, snapshot, false);
  assert.deepEqual(callerIntentResultPublicationCandidates(f.current(), 'current'), []);
});

test('changed owner, binding, request, message, event, and rendering fail before publication', () => {
  for (const change of [
    (r: OrdinaryPublicationEvidence['response']) => { r.bindingId = id(999); },
    (r: OrdinaryPublicationEvidence['response']) => { r.requestId = id(999); },
    (r: OrdinaryPublicationEvidence['response']) => { r.version.ownerPrincipalId = id(999); },
    (r: OrdinaryPublicationEvidence['response']) => { r.version.threadId = id(999); },
    (r: OrdinaryPublicationEvidence['response']) => { if (r.delivery.state === 'ready') r.delivery.source.messageId = id(999); },
    (r: OrdinaryPublicationEvidence['response']) => { if (r.delivery.state === 'ready') r.delivery.source.provenance.eventId = id(999); },
  ]) {
    const f = fixture(), value = f.ordinary(); change(value.response);
    assert.throws(() => f.publish(value), /ordinary_publication_identity_changed/);
    assert.equal(f.current().publications, undefined);
  }
  const f = fixture(), value = f.ordinary();
  assert.throws(() => beginBrokerPublication(f.store, f.identity(), value.requestId, { id: 'changed_render_0', kind: 'result',
    speak: true, text: 'An invented answer.', expectedCallerRevision: 1 }, snapshot, { response: value.response, chunkIndex: 0 }),
  /ordinary_publication_render_changed/);
});

test('ordinary evidence is immutable on every write and cannot be grafted onto a legacy record', () => {
  const f = fixture(), value = f.ordinary(); f.publish(value);
  const original = f.current();
  for (const change of [
    (r: BrokerPublicationRecord) => { r.ordinaryDelivery!.response.delivery.state === 'ready'
      && (r.ordinaryDelivery!.response.delivery.source.provenance.runId = id(999)); },
    (r: BrokerPublicationRecord) => { delete r.ordinaryDelivery; },
    (r: BrokerPublicationRecord) => { r.textDigest = 'f'.repeat(64); },
  ]) {
    const changed = structuredClone(original); change(changed.publications!.records[0]!);
    assert.throws(() => validateSelectedQuestionPublications(changed, original));
  }
  const legacy = structuredClone(original); delete legacy.publications!.records[0]!.ordinaryDelivery;
  assert.throws(() => validateSelectedQuestionPublications(original, legacy), /requires_current_intent/);
});

test('partial attempts cannot combine chunks; rejection removes fresh eligibility while historical IDs remain exact', () => {
  const f = fixture(), value = f.ordinary(10, Array(40).fill('A fetched holding is worth twelve dollars.').join(' '));
  const text = value.response.delivery.state === 'ready' ? value.response.delivery.text : '';
  assert.ok(liveResultChunks(text).length > 1);
  f.publish(value, 1, [0]);
  f.publish(value, 2, liveResultChunks(text).map((_, i) => i).slice(1));
  assert.deepEqual(callerIntentResultPublicationCandidates(f.current(), 'current'), []);
  f.publish(value, 3);
  const original = callerIntentResultPublicationCandidates(f.current(), 'current')[0]!;
  const first = f.current().publications!.records.find(r => r.updateId.endsWith('_attempt_3_0'))!;
  rejectBrokerPublication(f.store, f.identity(), first.outcome!.eventId, 'provider_error');
  assert.deepEqual(callerIntentResultPublicationCandidates(f.current(), 'current'), []);
  f.publish(value, 4);
  const later = callerIntentResultPublicationCandidates(f.current(), 'current')[0]!;
  assert.notEqual(later.id, original.id);
  const historical = callerIntentResultPublicationCandidates(JSON.parse(JSON.stringify(f.current())), 'current', { historical: true });
  assert.deepEqual(historical.map(r => [r.id, r.digest]), [[original.id, original.digest], [later.id, later.digest]]);
});

test('all distinct candidates retain original publication order with no two-result truncation', () => {
  const f = fixture();
  for (const n of [12, 10, 11]) f.publish(f.ordinary(n));
  assert.deepEqual(callerIntentResultPublicationCandidates(f.current(), 'current').map(r => r.candidate.execution.requestId),
    [id(12), id(10), id(11)]);
});

test('explicit admitted alias maps to canonical request; an unproved canonical substitution refuses', () => {
  const f = fixture(), value = f.ordinary(10, 'The result is saved.', 20); f.publish(value);
  const result = callerIntentResultPublicationCandidates(f.current(), 'current')[0]!;
  assert.equal(result.candidate.execution.requestId, id(20));
  assert.deepEqual(result.reference, { kind: 'ordinary', relatedPrimaryRequestId: id(20), referenceOrigin: 'broker_status_publication' });
  const changed = f.current(); delete (changed.delegations[id(10)] as BrokerDelegationRecord).reconciliation;
  assert.throws(() => validateSelectedQuestionPublications(changed));
});

test('new generation publication keeps original source session and historical recovery survives retirement', () => {
  const f = fixture(), value = f.ordinary();
  const call = f.current(), old = call.liveGenerations!.segments[0]!;
  old.state = 'closed'; old.closedAt = at;
  call.liveSessionId = 'next'; call.liveGenerations!.segments.push({ ...old, generationId: id(7), ordinal: 2,
    state: 'active', sessionId: 'next', closedAt: undefined });
  call.liveGenerations!.deliveryAttachment = { generationId: id(7), sessionId: 'next' }; f.replace(call);
  f.publish(value);
  assert.equal(f.current().publications!.records[0]!.sourceSessionId, 'current');
  const prior = callerIntentResultPublicationCandidates(f.current(), 'next'); assert.equal(prior.length, 1);
  const ended = f.current(); ended.endedAt = at; ended.status = 'completed'; ended.liveGenerations!.phase = 'stopped';
  delete ended.liveGenerations!.deliveryAttachment; ended.liveGenerations!.segments[1]!.state = 'closed'; f.replace(ended);
  assert.deepEqual(callerIntentResultPublicationCandidates(f.current(), 'next'), []);
  assert.deepEqual(callerIntentResultPublicationCandidates(f.current(), 'next', { historical: true }), prior);
});

test('ordinary and associated results share journal order while retaining separate execution bindings', () => {
  const f = fixture(); f.publish(f.ordinary());
  const association = { v: PHONE_REQUEST_DELIVERY_VERSION, id: id(40), delivery: { bindingId: id(5), callSid: f.binding.callSid },
    execution: { bindingId: id(41), requestId: id(42), runId: id(43) }, question: { id: id(44), revision: 1 as const }, createdAt: at };
  const call = f.current();
  call.continuation = { originalRequestId: id(42), questionId: id(44), questionRevision: 1 };
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
  settleBrokerPublication(f.store, record, { eventId: 'selected_command', status: 'accepted', observedCallerRevision: 1,
    observedTranscriptSequence: 1 }, snapshot, false);
  const candidates = callerIntentResultPublicationCandidates(f.current(), 'current');
  assert.deepEqual(candidates.map(r => r.candidate.kind), ['ordinary', 'selected']);
  assert.deepEqual(candidates[1]!.candidate.execution, association.execution);
  assert.equal(candidates[1]!.reference.kind, 'associated');
  assert.equal(canonicalJson(candidates), canonicalJson(callerIntentResultPublicationCandidates(JSON.parse(JSON.stringify(f.current())), 'current')));
});

test('a forged receipt or missing original generation cannot qualify saved evidence', () => {
  const f = fixture(), value = f.ordinary(); f.publish(value);
  const badReceipt = f.current(); badReceipt.publications!.records[0]!.outcome!.callerRevision = 2;
  assert.throws(() => validateSelectedQuestionPublications(badReceipt));
  assert.deepEqual(callerIntentResultPublicationCandidates(badReceipt, 'current'), []);
  const missingSource = f.current();
  missingSource.publications!.records[0]!.sourceSessionId = 'absent';
  assert.throws(() => validateSelectedQuestionPublications(missingSource));
  assert.deepEqual(callerIntentResultPublicationCandidates(missingSource, 'current'), []);
});

test('exact publication retry preserves the original evidence and sequence; changed delivery refuses', () => {
  const f = fixture(), value = f.ordinary(); f.publish(value);
  const original = JSON.stringify(f.current().publications);
  f.publish(value);
  assert.equal(JSON.stringify(f.current().publications), original);
  if (value.response.delivery.state !== 'ready') throw new Error('fixture_delivery');
  value.response.delivery.source.provenance.runId = id(999);
  assert.throws(() => f.publish(value), /publication_identity_changed/);
  assert.equal(JSON.stringify(f.current().publications), original);
});
