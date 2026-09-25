import assert from 'node:assert/strict';
import test, { type TestContext } from 'node:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { BrokerStore } from '../src/broker/store.js';
import { brokerIdentity, createPrimaryBackend, validateBrokerAdmissionRefusals, type BrokerDelegationRecord, type PrimaryBackendOptions } from '../src/broker/backend.js';
import { callerRouteRef, callerRoutingSourceAnchors, getCallerRouteBySource, callerRoutePresentationDigest,
  callerIntentQuestionForTarget, type CallerRouteRef, type CallerRouteRecord, type CallerQuestionTarget, type CallerRouteDecision } from '../src/broker/callerRouting.js';
import { callerIntentInputDigest, type CallerIntentInput, type CallerIntentDecision, type CallerIntentResponse } from '../src/broker/callerIntentContract.js';
import type { FrozenCallerTurn } from '../src/broker/callerTurn.js';
import { beginBrokerPublication, beginSelectedQuestionPublication, settleBrokerPublication, rejectBrokerPublication,
  callerIntentResultPublicationCandidates, callerIntentResultPublicationInventory } from '../src/broker/publications.js';
import { createRequestDeliveryJournal, retainRequestDeliveryAssociation, createRequestDeliveryQuestionRecord,
  PHONE_REQUEST_DELIVERY_VERSION } from '../src/broker/requestDelivery.js';
import type { SelectedQuestionSeed } from '../src/broker/selectedQuestion.js';
import { PrimaryApiError, type PrimaryCall, type PrimaryQuestion, type PrimaryReplyInput, type PrimaryResult, type PrimarySubmission, type PrimaryTurn } from '../src/broker/primaryApi.js';
import { rawSubmissionDigest, turnBodyDigest } from '../src/broker/reconciliation.js';
import type { LiveBackendChannel, LiveDelegationRequest, LiveTranscriptSnapshot } from '../src/live/types.js';

const id = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;

async function fixture(t: TestContext, selectedResult = false) {
  const directory = await mkdtemp(path.join(tmpdir(), 'broker-mixed-admission-'));
  const store = new BrokerStore(directory);
  const callSid = `CA${'a'.repeat(32)}`, sessionId = 'mixed_g1', bindingId = id(1);
  const continuation = { originalRequestId: id(21), questionId: id(24), questionRevision: 1 as const };
  const initial = store.reserve(selectedResult
    ? { requestId: id(100), callSid, direction: 'outbound', number: '+15550000001', purpose: 'Discuss the saved comparison.', continuation }
    : { requestId: 'mixed-admission-fixture', callSid, direction: 'inbound', number: '+15550000001', purpose: '' },
    selectedResult ? 'authenticated_broker_control' : undefined).call;
  const version = { ownerPrincipalId: id(2), primaryDexterId: id(3), threadId: id(4),
    contextEpoch: '1', threadEventSequence: '1', digest: 'a'.repeat(64) };
  const abort = new AbortController();
  let clock = Date.now(), sequence = 0;
  const assignmentRef = selectedResult ? { assignmentId: initial.requestId, revision: 1 as const, digest: 'c'.repeat(64) } : undefined;
  const snapshot: LiveTranscriptSnapshot = { revision: 0, callerRevision: 0, fragments: [] };
  const binding: PrimaryCall = { bindingId, callSid, state: 'open',
    context: { primaryDexterId: id(3), threadId: id(4), displayName: 'Dexter', messages: [] },
    ...(assignmentRef ? { callAssignmentRef: assignmentRef } : {}) };
  store.recordPrimaryBinding(initial.id, binding);
  assert.deepEqual(store.get(initial.id)!.primary, { bindingId, name: 'Dexter', context: '',
    primaryDexterId: id(3), threadId: id(4), ...(assignmentRef ? { callAssignmentRef: assignmentRef } : {}) });
  store.update(initial.id, { status: 'in-progress', liveSessionId: sessionId, mediaStartedAt: initial.createdAt,
    liveGenerations: { version: 1, callId: initial.id, binding: { bindingId, callSid,
      ownerPrincipalId: id(2), primaryDexterId: id(3), threadId: id(4),
      ...(assignmentRef ? { callAssignmentRef: assignmentRef } : {}) },
      clocks: { mediaStartedAt: initial.createdAt, callDeadlineAt: new Date(clock + 300_000).toISOString() },
      phase: 'active', inputNextSample: 0, deliveryAttachment: { generationId: id(5), sessionId },
      segments: [{ generationId: id(5), ordinal: 1, state: 'active', createdAt: initial.createdAt,
        readyAt: initial.createdAt, sessionId, version, pageDigest: 'b'.repeat(64), inputFirstSample: 0,
        inputLastSampleExclusive: 0, transcript: structuredClone(snapshot), outputDiscarded: false }] } });
  if (selectedResult) {
    const seed: SelectedQuestionSeed = { v: 'broker.selected-question-seed.v1', deliveryCall: { callId: initial.id, sessionId },
      scope: { ownerPrincipalId: id(2), primaryDexterId: id(3), threadId: id(4) }, delivery: { bindingId, callSid },
      request: { v: PHONE_REQUEST_DELIVERY_VERSION, ...continuation },
      callDeadlineAt: store.get(initial.id)!.liveGenerations!.clocks.callDeadlineAt };
    store.recordSelectedQuestion(initial.id, { v: 'broker.selected-question-host.v1', initial: seed,
      questions: [{ seed, admissions: [] }] });
    const association = { id: id(20), v: PHONE_REQUEST_DELIVERY_VERSION, delivery: seed.delivery,
      execution: { bindingId: id(25), requestId: id(21), runId: id(23) },
      question: { id: id(24), revision: 1 as const }, createdAt: initial.createdAt };
    const journal = retainRequestDeliveryAssociation(createRequestDeliveryJournal({ deliveryCall: seed.deliveryCall,
      scope: seed.scope, request: seed.request, target: { delivery: seed.delivery, execution: association.execution, question: association.question } }),
    { association, replayed: false });
    store.recordSelectedQuestion(initial.id, { v: 'broker.selected-question-host.v1', initial: seed,
      questions: [{ seed, admissions: [], record: createRequestDeliveryQuestionRecord(journal) }] });
    const response: Parameters<typeof beginSelectedQuestionPublication>[2] = { bindingId, callSid, requestId: id(21),
      version, deliveryAssociation: association, delivery: { state: 'ready', text: 'The comparison is saved.',
        source: { kind: 'assistant', messageId: id(22), eventSequence: '1', provenance: {
          messageCreatedAt: initial.createdAt, eventId: id(26), eventOccurredAt: initial.createdAt, runId: id(23) } } } };
    const intent = beginSelectedQuestionPublication(store, { callId: initial.id, callSid, bindingId, sessionId }, response,
      { id: 'selected_result_0', kind: 'result', speak: true, text: response.delivery.text, expectedCallerRevision: 0 }, snapshot, 0)!;
    assert.ok(intent);
    settleBrokerPublication(store, intent.record, { eventId: 'selected_append_0', status: 'accepted',
      observedCallerRevision: 0, observedTranscriptSequence: 0 }, snapshot, false);
  }
  const resultCandidate = selectedResult ? callerIntentResultPublicationCandidates(store.get(initial.id)!, sessionId)[0] : undefined;
  if (selectedResult) assert.ok(resultCandidate);
  const posts: PrimarySubmission[] = [], replies: PrimaryReplyInput[] = [], reads: string[] = [];
  const captures: PrimaryTurn[] = [], commentary: string[] = [];
  const results = new Map<string, PrimaryResult>();
  const hooks: { submitError?: Error; readError?: Error; captureFailures?: number } = {};
  let submitQuestion = false, loseReply = false;
  const question = (): PrimaryQuestion => ({ id: id(6), revision: 1, runId: id(7), threadId: id(4),
    agent: { id: id(3), name: 'Dexter' }, question: 'Continue the comparison?', context: 'Research only.',
    proposedAnswer: 'Compare both companies.', yesMeaning: 'Compare both.', noMeaning: 'Stop the comparison.',
    state: 'pending', runState: 'waiting_for_reply', createdAt: initial.createdAt,
    expiresAt: new Date(Date.now() + 120_000).toISOString(), reply: null });
  const channel: LiveBackendChannel = { signal: abort.signal, getContext: () => structuredClone(snapshot),
    subscribeContext: () => () => {}, async publish(update) { return { status: 'accepted', eventId: update.id }; } };
  const options: PrimaryBackendOptions = { callId: initial.id, callSid, bindingId, store, sourceSessionId: sessionId,
    selectedQuestionSources: () => [], callerRouteSources: () => callerRoutingSourceAnchors(store.get(initial.id)!),
    callerRouteCaptureAccepted(sourceEventId, receipt) {
      const route = getCallerRouteBySource(store.get(initial.id)!, sourceEventId);
      if (route) store.captureCallerRoute(initial.id, callerRouteRef(route), receipt);
    },
    stabilityMs: 1, maxWaitMs: 2, pollIntervalMs: 1, maxPollMs: 2, now: () => ++clock,
    api: {
      async submit(_binding, requestId, input) {
        posts.push(structuredClone(input));
        if (hooks.submitError) throw hooks.submitError;
        const result: PrimaryResult = submitQuestion ? { requestId, bindingId, state: 'waiting_for_reply', question: question() }
          : { requestId, bindingId, state: 'completed', assistant: { messageId: id(8), eventId: id(9), sequence: '1', text: 'Research saved.' } };
        results.set(requestId, result); return structuredClone(result);
      },
      async result(_binding, requestId) {
        reads.push(requestId);
        if (hooks.readError) throw hooks.readError;
        if (!results.has(requestId)) throw new PrimaryApiError('http_error', 404, { code: 'request_not_found', retryable: false });
        return structuredClone(results.get(requestId)!);
      },
      async question(_binding, requestId) {
        reads.push(requestId); const result = results.get(requestId)!;
        return { ...structuredClone(result), question: structuredClone(result.question!) };
      },
      async reply(_binding, requestId, _questionId, input) {
        replies.push(structuredClone(input));
        if (loseReply) throw new Error('lost response');
        const before = results.get(requestId)!;
        const result = { requestId, bindingId, state: 'running' as const, question: {
          ...before.question!, state: 'answered' as const, runState: 'running' as const,
          reply: { ...input.reply, interpretedAnswer: 'Compare both.', receivedAt: new Date(clock).toISOString() } } };
        results.set(requestId, result); return result;
      },
      async captureTurn(_binding, turn) {
        captures.push(structuredClone(turn));
        if (hooks.captureFailures) { hooks.captureFailures--; throw new Error('capture response lost'); }
        return { bindingId, sourceEventId: turn.sourceEventId, revision: turn.revision, recordId: turn.sourceEventId,
          revisionRecordId: id(90), bodyDigest: turnBodyDigest(turn), replayed: false };
      },
    },
    observation: { signal: abort.signal, async publish(entry, update) {
      if (['admission_refused', 'admission_expired'].includes(entry.status)) {
        assert.equal(update?.kind, 'progress'); commentary.push(update!.text);
        return { delivery: { eventId: update!.id, status: 'accepted' as const } };
      }
      const current = store.get(initial.id)!;
      const rendering = { ...(update ?? { id: `result_${entry.requestId}_${entry.status}_${entry.currentQuestionId ?? 'none'}_${snapshot.callerRevision}_0`,
        kind: 'result' as const, text: entry.result ?? 'Continue the comparison?' }), speak: true, expectedCallerRevision: snapshot.callerRevision };
      const ordinaryDelivery: Parameters<typeof beginBrokerPublication>[5] = !update && entry.primaryResult?.state === 'completed'
        ? { chunkIndex: 0, response: { bindingId, callSid, requestId: entry.primaryResult.requestId, version,
          delivery: { state: 'ready', text: rendering.text, source: { kind: 'assistant', messageId: entry.primaryResult.assistant.messageId,
            eventSequence: entry.primaryResult.assistant.sequence, provenance: { messageCreatedAt: initial.createdAt,
              eventId: entry.primaryResult.assistant.eventId, eventOccurredAt: initial.createdAt, runId: id(7) } } } } } : undefined;
      const intent = beginBrokerPublication(store, { callId: current.id, callSid, bindingId, sessionId }, entry.requestId,
        rendering, snapshot, ordinaryDelivery)!;
      const delivery = { eventId: `append_${intent.record.publicationSequence}`, status: 'accepted' as const,
        observedCallerRevision: snapshot.callerRevision,
        observedTranscriptSequence: Math.max(0, ...snapshot.fragments.map(fragment => fragment.sequence)) };
      settleBrokerPublication(store, intent.record, delivery, snapshot, false);
      assert.deepEqual(store.get(initial.id)!.publications!.records.find(record =>
        record.publicationSequence === intent.record.publicationSequence)!.outcome,
        { eventId: delivery.eventId, status: 'accepted', callerRevision: delivery.observedCallerRevision,
          transcriptSequence: delivery.observedTranscriptSequence }, 'fixture publication must retain its exact accepted frontier');
      return { delivery, sessionId, snapshot: structuredClone(snapshot), inputThroughMs: snapshot.fragments.at(-1)?.endMs ?? 0,
        publicationReceipts: [{ publicationSequence: intent.record.publicationSequence, sessionId, eventId: delivery.eventId }] };
    } },
  };
  const backend = createPrimaryBackend(options);
  t.after(async () => { abort.abort(); await store.close(); await rm(directory, { recursive: true, force: true }); });
  const resolve = (ref: CallerRouteRef, decision: CallerRouteDecision) => {
    const current = store.get(initial.id)!, record: CallerRouteRecord = getCallerRouteBySource(current, ref.sourceEventId)!;
    const offered = callerIntentResultPublicationCandidates(current, sessionId);
    const selected = record.resolutionInput.references.map(reference => {
      const value = offered.find(value => value.id === reference.id && value.digest === reference.digest);
      assert.ok(value); return value;
    });
    const input: CallerIntentInput = { v: 'phone.caller-intent.v2', source: { callSid,
      sourceEventId: record.frozen.turn.sourceEventId, itemId: record.frozen.turn.itemId, revision: 1,
      receivedAt: record.frozen.turn.occurredAt, role: 'user', text: record.frozen.turn.text },
      generationId: record.scope.generationId, sourceSessionId: sessionId, brokerInputDigest: record.inputDigest,
      presentationDigest: callerRoutePresentationDigest(record.resolutionInput), callDeadlineAt: current.liveGenerations!.clocks.callDeadlineAt,
      version: record.resolutionInput.version, questions: record.resolutionInput.questions.map(callerIntentQuestionForTarget),
      resultCoverage: record.resolutionInput.resultCoverage!,
      results: record.resolutionInput.resultCoverage!.state === 'overflow' ? [] : selected.map(value => value.candidate) };
    const operationId = brokerIdentity('test-caller-intent', initial.id, ref.sourceEventId);
    store.prepareCallerIntent(initial.id, ref, { operationId, input });
    const resolved: CallerIntentDecision = decision.kind === 'answer' ? { kind: 'answer', target: callerIntentQuestionForTarget(decision.target) }
      : decision.kind === 'task' ? { kind: 'task', ...(decision.contextReference ? {
        reference: selected.find(value => value.id === decision.contextReference!.id)!.reference } : {}) }
      : decision.kind === 'conversation' ? { kind: 'conversation' } : { kind: 'clarify', reason: 'invalid_output' };
    const response: CallerIntentResponse = { v: 'phone.caller-intent.v2', operationId, bindingId,
      inputDigest: callerIntentInputDigest(operationId, bindingId, input), brokerInputDigest: record.inputDigest,
      presentationDigest: input.presentationDigest, generationId: input.generationId, sourceSessionId: sessionId,
      callDeadlineAt: input.callDeadlineAt, questions: input.questions, results: input.results, version: input.version,
      resultCoverage: input.resultCoverage, resultContextCoverage: input.resultCoverage.state === 'overflow'
        ? { state: 'unavailable', reason: 'inventory_overflow' } : { state: 'complete' },
      reservedAt: new Date(clock).toISOString(), validUntil: new Date(Math.min(clock + 10_000, Date.parse(input.callDeadlineAt))).toISOString(),
      state: 'resolved', decision: resolved };
    store.recordCallerIntentResponse(initial.id, ref, response);
    store.decideCallerRoute(initial.id, ref, { inputDigest: ref.inputDigest, decision });
  };
  const route = (text: string, decision?: CallerRouteDecision, targets: CallerQuestionTarget[] = [], references?: Array<{ id: string; digest: string }>) => {
    sequence++; const startMs = (sequence - 1) * 1400 + 1, endMs = startMs + 20, firstEventId = `caller_${sequence}`;
    snapshot.fragments.push({ role: 'user', eventId: firstEventId, text, startMs, endMs, sequence });
    snapshot.revision = snapshot.callerRevision = sequence;
    const current = store.get(initial.id)!, generations = structuredClone(current.liveGenerations!);
    generations.inputNextSample = Math.ceil(endMs * 8); generations.segments[0].inputLastSampleExclusive = generations.inputNextSample;
    generations.segments[0].transcript = structuredClone(snapshot); store.update(initial.id, { liveGenerations: generations });
    const sourceEventId = brokerIdentity('source', initial.id, sessionId, firstEventId);
    const observedSpeechAtMs = ++clock;
    backend.noteCallerSpeech(observedSpeechAtMs, endMs);
    const frozen: FrozenCallerTurn = { sessionId, firstEventId, callerRevision: sequence, firstSequence: sequence,
      lastSequence: sequence, startMs, endMs, audioThroughMs: endMs, observedSpeechAtMs,
      turn: { sourceEventId, revision: 1, itemId: `live-turn-${sourceEventId}`, role: 'user', text,
        occurredAt: new Date(Date.parse(initial.createdAt) + startMs).toISOString() } };
    assert.doesNotThrow(() => backend.assertFrozenCallerSpeechCurrent(frozen), 'fixture speech frontier must match its frozen source');
    const request: LiveDelegationRequest = { callId: initial.id, sessionId, delegationId: `delegate_${sequence}`,
      requestId: `provider_request_${sequence}`, offsetMs: endMs, transcript: structuredClone(snapshot) };
    const inventory = callerIntentResultPublicationInventory(current, sessionId);
    const fullReferences = inventory.candidates.map(({ id, digest }) => ({ id, digest }));
    if (references) assert.deepEqual(references, fullReferences, 'the fixture must offer the full current publication inventory');
    const record = store.prepareCallerRoute(initial.id, { frozen,
      delegation: { sessionId, delegationId: request.delegationId, requestId: request.requestId, offsetMs: request.offsetMs },
      resolutionInput: { version, questions: targets, references: fullReferences, resultCoverage: inventory.resultCoverage,
        publicationSequence: current.publications?.records.at(-1)?.publicationSequence ?? 0 } });
    const ref = callerRouteRef(record);
    if (decision) resolve(ref, decision);
    return { request, ref, frozen };
  };
  return { store, directory, callId: initial.id, sessionId, bindingId, options, backend, channel, route, posts, replies, reads, snapshot,
    hooks, captures, commentary, results, resolve,
    reference: resultCandidate && { id: resultCandidate.id, digest: resultCandidate.digest },
    pendingQuestion() { submitQuestion = true; }, completedSubmits() { submitQuestion = false; },
    advance(ms: number) { clock += ms; }, now: () => clock, loseReply() { loseReply = true; },
    entries: () => Object.values(store.get(initial.id)!.delegations) as BrokerDelegationRecord[] };
}

test('explicit task uses the journalled raw source once with both selected and ordinary ports configured', async t => {
  const h = await fixture(t), r = h.route('  Research NVDA and compare it with AMD.\t', { kind: 'task' });
  h.snapshot.fragments[0].text = 'This later channel value must not replace the frozen request.';
  await h.backend.delegateFrozenTask(r.request, r.ref, h.channel);
  await h.backend.delegateFrozenTask(r.request, r.ref, h.channel);
  assert.equal(h.posts.length, 1); assert.equal(h.replies.length, 0);
  assert.equal(h.posts[0].text, r.frozen.turn.text);
  assert.equal(h.posts[0].source.sourceEventId, r.frozen.turn.sourceEventId);
});

test('an undecided source and a changed route reference dispatch neither a task nor an answer', async t => {
  const h = await fixture(t), r = h.route('Buy ten dollars of NVDA.');
  await assert.rejects(h.backend.delegateFrozenTask(r.request, r.ref, h.channel), /caller_route_decision_mismatch/);
  h.resolve(r.ref, { kind: 'task' });
  await assert.rejects(h.backend.delegateFrozenTask(r.request, { ...r.ref, inputDigest: '0'.repeat(64) }, h.channel), /caller_route_not_found/);
  await assert.rejects(h.backend.answerFrozenQuestion(r.request, r.ref, h.channel), /caller_route_decision_mismatch/);
  assert.deepEqual([h.posts.length, h.replies.length], [0, 0]);
});

test('a task during an ordinary pending question queues its exact raw body without becoming consent', async t => {
  const h = await fixture(t); h.pendingQuestion();
  const first = h.route('Compare these companies.', { kind: 'task' });
  await h.backend.delegateFrozenTask(first.request, first.ref, h.channel);
  const next = h.route('Sell five dollars of my other holding.', { kind: 'task' }, h.backend.questionTargets(h.sessionId));
  await h.backend.delegateFrozenTask(next.request, next.ref, h.channel);
  assert.equal(h.posts.length, 1); assert.equal(h.replies.length, 0);
  assert.equal(h.entries()[0].deferredTurns![0].status, 'queued');
  assert.equal(h.entries()[0].deferredTurns![0].queuedRequest!.submission.text, next.frozen.turn.text);
});

test('an exact ordinary answer retains its question and source, and lost reply recovery never POSTs again', async t => {
  const h = await fixture(t); h.pendingQuestion();
  const first = h.route('Compare these companies.', { kind: 'task' });
  await h.backend.delegateFrozenTask(first.request, first.ref, h.channel);
  const targets = h.backend.questionTargets(h.sessionId); assert.equal(targets.length, 1);
  const answer = h.route(' Yes.\t', { kind: 'answer', target: targets[0] }, targets); h.loseReply();
  await h.backend.answerFrozenQuestion(answer.request, answer.ref, h.channel);
  h.advance(11_000);
  await h.backend.answerFrozenQuestion(answer.request, answer.ref, h.channel);
  assert.equal(h.posts.length, 1); assert.equal(h.replies.length, 1);
  assert.equal(h.replies[0].text, answer.frozen.turn.text); assert.equal(h.replies[0].reply.answer, 'yes');
  assert.equal(h.entries()[0].questions![id(6)].reply!.status, 'unknown');
});

test('a late question publication rejection removes the candidate and prevents fresh routed reply', async t => {
  const h = await fixture(t); h.pendingQuestion();
  const first = h.route('Compare these companies.', { kind: 'task' });
  await h.backend.delegateFrozenTask(first.request, first.ref, h.channel);
  const targets = h.backend.questionTargets(h.sessionId);
  assert.equal(targets.length, 1, 'the accepted question must be offered before its late rejection');
  const answer = h.route('Yes.', { kind: 'answer', target: targets[0] }, targets);
  const publication = h.store.get(h.callId)!.publications!.records.at(-1)!;
  rejectBrokerPublication(h.store, { callId: h.callId, callSid: publication.callSid, bindingId: h.bindingId,
    sessionId: h.sessionId }, publication.outcome!.eventId, 'provider_error');
  assert.deepEqual(h.backend.questionTargets(h.sessionId), []);
  await assert.rejects(h.backend.answerFrozenQuestion(answer.request, answer.ref, h.channel), /caller_route_stale_target/);
  assert.equal(h.replies.length, 0);
});

test('an offered associated context reference is required, frozen in the submission and reused on replay', async t => {
  const h = await fixture(t, true), reference = h.reference!;
  const task = h.route('Research the company in that result.', { kind: 'task', contextReference: reference }, [], [reference]);
  await assert.rejects(h.backend.delegateFrozenTask(task.request, task.ref, h.channel), /caller_route_context_reference_unsupported/);
  let resolutions = 0;
  h.options.resolveCallerTaskReference = () => { resolutions++; return { kind: 'associated', associatedResultReference: {
    v: 'phone.associated-result-reference.v1', deliveryAssociationId: id(20), originalRequestId: id(21), assistantMessageId: id(22) } }; };
  await h.backend.delegateFrozenTask(task.request, task.ref, h.channel);
  await h.backend.delegateFrozenTask(task.request, task.ref, h.channel);
  assert.equal(resolutions, 1); assert.equal(h.posts.length, 1);
  assert.equal(h.posts[0].associatedResultReference?.deliveryAssociationId, id(20));
  assert.equal(h.posts[0].relatedPrimaryRequestId, undefined);
});

test('an explicitly selected ordinary result reference is frozen from its complete publication without an associated reference', async t => {
  const h = await fixture(t), first = h.route('Research AMD.', { kind: 'task' });
  await h.backend.delegateFrozenTask(first.request, first.ref, h.channel);
  const candidate = callerIntentResultPublicationCandidates(h.store.get(h.callId)!, h.sessionId)[0];
  assert.ok(candidate, 'the accepted ordinary result must be offered before its explicit reference');
  assert.equal(candidate.reference.kind, 'ordinary');
  const reference = { id: candidate.id, digest: candidate.digest };
  const next = h.route('Explain the risks in that result.', { kind: 'task', contextReference: reference }, [], [reference]);
  h.options.resolveCallerTaskReference = () => candidate.reference;
  await h.backend.delegateFrozenTask(next.request, next.ref, h.channel);
  assert.equal(h.posts.length, 2); assert.equal(h.posts[1].relatedPrimaryRequestId, h.entries()[0].requestId);
  assert.equal(h.posts[1].referenceOrigin, 'broker_status_publication');
  assert.equal(h.posts[1].associatedResultReference, undefined); assert.equal(h.posts[1].text, next.frozen.turn.text);
});

test('a call ending after route persistence refuses fresh dispatch and retains its raw capture obligation', async t => {
  const h = await fixture(t), task = h.route('Research AMD.', { kind: 'task' });
  h.store.update(h.callId, { status: 'completed', endedAt: new Date().toISOString() });
  await assert.rejects(h.backend.delegateFrozenTask(task.request, task.ref, h.channel), /caller_route_stale_source/);
  assert.equal(h.posts.length, 0);
  assert.equal(callerRoutingSourceAnchors(h.store.get(h.callId)!)[0].frozen.turn.text, 'Research AMD.');
  assert.equal(callerRoutingSourceAnchors(h.store.get(h.callId)!)[0].capture, undefined);
});

const associatedReference = { kind: 'associated' as const, associatedResultReference: {
  v: 'phone.associated-result-reference.v1' as const,
  deliveryAssociationId: id(20), originalRequestId: id(21), assistantMessageId: id(22) } };
const associationError = (code: string, status = 409) => new PrimaryApiError('http_error', status, { code, retryable: false });

for (const code of ['delivery_association_mismatch', 'associated_result_unavailable'])
  test(`first associated submit ${code} is a durable refusal with raw capture and exact restart replay`, async t => {
    const h = await fixture(t, true), offeredReference = h.reference!;
    h.options.resolveCallerTaskReference = () => associatedReference;
    h.hooks.submitError = associationError(code);
    const task = h.route('Research the company in that result.', { kind: 'task', contextReference: offeredReference }, [], [offeredReference]);
    await h.backend.delegateFrozenTask(task.request, task.ref, h.channel);
    const saved = h.entries()[0];
    assert.equal(saved.status, 'admission_refused');
    assert.deepEqual(saved.admissionRefusal, { origin: 'api_non_admission', status: 409, code,
      submissionDigest: rawSubmissionDigest(h.bindingId, saved.submission) });
    assert.equal(saved.primaryResult, undefined); assert.equal(saved.result, undefined); assert.equal(saved.reconciliation, undefined);
    assert.equal(saved.originalCapture?.state, 'accepted'); assert.deepEqual(h.captures, [task.frozen.turn]);
    assert.ok(getCallerRouteBySource(h.store.get(h.callId)!, task.ref.sourceEventId)!.capture);
    assert.equal(h.commentary[0], 'I could not start that request from the saved result. Please tell me what you want me to check or do.');
    assert.equal(h.reads.length, 0);
    h.store.close();
    const restarted = new BrokerStore(h.directory); t.after(() => restarted.close());
    const resumed = createPrimaryBackend({ ...h.options, store: restarted,
      callerRouteSources: () => callerRoutingSourceAnchors(restarted.get(h.callId)!) });
    await resumed.delegateFrozenTask(task.request, task.ref, h.channel);
    assert.equal(h.posts.length, 1); assert.equal(h.captures.length, 1); assert.equal(h.reads.length, 0);
    assert.equal((restarted.get(h.callId)!.delegations[saved.requestId] as BrokerDelegationRecord).status, 'admission_refused');
  });

test('refused task capture remains pending across restart and retries only the original turn', async t => {
  const h = await fixture(t, true), offeredReference = h.reference!; h.options.resolveCallerTaskReference = () => associatedReference;
  h.hooks.submitError = associationError('associated_result_unavailable'); h.hooks.captureFailures = 1;
  const task = h.route('Check that saved comparison.', { kind: 'task', contextReference: offeredReference }, [], [offeredReference]);
  await h.backend.delegateFrozenTask(task.request, task.ref, h.channel);
  assert.equal(h.entries()[0].originalCapture?.state, 'unknown'); assert.ok(h.backend.closeoutPending() > 0);
  h.store.close();
  const restarted = new BrokerStore(h.directory); t.after(() => restarted.close());
  const resumed = createPrimaryBackend({ ...h.options, store: restarted,
    callerRouteSources: () => callerRoutingSourceAnchors(restarted.get(h.callId)!),
    callerRouteCaptureAccepted(sourceEventId, receipt) {
      restarted.captureCallerRoute(h.callId, callerRouteRef(getCallerRouteBySource(restarted.get(h.callId)!, sourceEventId)!), receipt);
    } });
  await resumed.flushPendingCaptures();
  assert.equal(h.posts.length, 1); assert.equal(h.reads.length, 0);
  assert.deepEqual(h.captures, [task.frozen.turn, task.frozen.turn]);
  assert.equal(resumed.closeoutPending(), 0);
});

test('503 first submit and a later typed 409 GET retain unknown admission and never POST again', async t => {
  const h = await fixture(t, true), offeredReference = h.reference!; h.options.resolveCallerTaskReference = () => associatedReference;
  h.hooks.submitError = associationError('associated_result_unavailable', 503);
  h.hooks.readError = associationError('delivery_association_mismatch');
  const task = h.route('Research the company in that result.', { kind: 'task', contextReference: offeredReference }, [], [offeredReference]);
  await h.backend.delegateFrozenTask(task.request, task.ref, h.channel);
  h.advance(11_000);
  await h.backend.delegateFrozenTask(task.request, task.ref, h.channel);
  await h.backend.flushPendingCaptures();
  assert.equal(h.posts.length, 1); assert.ok(h.reads.length > 0);
  assert.equal(h.entries()[0].status, 'dispatch_unknown'); assert.equal(h.entries()[0].admissionRefusal, undefined);
  assert.equal(h.entries()[0].submitError?.status, 503); assert.equal(h.entries()[0].lastResultReadError?.status, 409);
});

test('the associated refusal rule cannot narrow an ordinary first-submit 409', async t => {
  const h = await fixture(t); h.hooks.submitError = associationError('delivery_association_mismatch');
  const task = h.route('Research AMD.', { kind: 'task' });
  await h.backend.delegateFrozenTask(task.request, task.ref, h.channel);
  assert.equal(h.entries()[0].status, 'dispatch_unknown'); assert.equal(h.entries()[0].admissionRefusal, undefined);
});

test('a refused associated task allows the already queued ordinary successor to dispatch once', async t => {
  const h = await fixture(t, true), offeredReference = h.reference!; h.pendingQuestion(); h.options.resolveCallerTaskReference = () => associatedReference;
  const first = h.route('Compare these companies.', { kind: 'task' });
  await h.backend.delegateFrozenTask(first.request, first.ref, h.channel);
  const refused = h.route('Research that saved result.', { kind: 'task', contextReference: offeredReference }, [], [offeredReference]);
  await h.backend.delegateFrozenTask(refused.request, refused.ref, h.channel);
  const next = h.route('Research AMD.', { kind: 'task' });
  await h.backend.delegateFrozenTask(next.request, next.ref, h.channel);
  assert.equal(h.posts.length, 1);
  const originalSubmit = h.options.api.submit;
  h.options.api.submit = async (bindingId, requestId, input) => {
    if (input.associatedResultReference) { h.posts.push(structuredClone(input)); throw associationError('associated_result_unavailable'); }
    return originalSubmit(bindingId, requestId, input);
  };
  const parent = h.entries()[0];
  h.completedSubmits();
  h.results.set(parent.requestId, { requestId: parent.requestId, bindingId: h.bindingId, state: 'completed',
    assistant: { messageId: id(70), eventId: id(71), sequence: '1', text: 'Comparison saved.' } });
  await h.backend.delegateFrozenTask(first.request, first.ref, h.channel);
  assert.equal(h.posts.length, 3); assert.deepEqual(h.posts.map(value => value.text), [first.frozen.turn.text, refused.frozen.turn.text, next.frozen.turn.text]);
  assert.equal(h.entries().find(entry => entry.submission.source.sourceEventId === refused.ref.sourceEventId)!.status, 'admission_refused');
  await h.backend.delegateFrozenTask(refused.request, refused.ref, h.channel);
  assert.equal(h.posts.length, 3);
});

test('refusal validation retains the exact submission and never converts prior uncertainty into non-admission', async t => {
  const h = await fixture(t, true), offeredReference = h.reference!; h.options.resolveCallerTaskReference = () => associatedReference;
  h.hooks.submitError = associationError('delivery_association_mismatch');
  const task = h.route('Research that company.', { kind: 'task', contextReference: offeredReference }, [], [offeredReference]);
  await h.backend.delegateFrozenTask(task.request, task.ref, h.channel);
  const call = h.store.get(h.callId)!, requestId = h.entries()[0].requestId;
  const changed = structuredClone(call), changedEntry = changed.delegations[requestId] as BrokerDelegationRecord;
  changedEntry.submission.text = 'Buy that company.';
  assert.throws(() => validateBrokerAdmissionRefusals(changed), /invalid_saved_admission_refusal/);
  changedEntry.admissionRefusal!.submissionDigest = rawSubmissionDigest(h.bindingId, changedEntry.submission);
  assert.throws(() => validateBrokerAdmissionRefusals(changed, call), /admission_refusal_changed/);
  const falseRun = structuredClone(call);
  (falseRun.delegations[requestId] as BrokerDelegationRecord).primaryResult = { requestId, bindingId: h.bindingId,
    state: 'failed', error: { code: 'invented', retryable: false } };
  assert.throws(() => validateBrokerAdmissionRefusals(falseRun), /invalid_saved_admission_refusal/);
  const removed = structuredClone(call); delete removed.delegations[requestId];
  assert.throws(() => validateBrokerAdmissionRefusals(removed, call), /admission_refusal_removed/);
  const uncertain = structuredClone(call), before = uncertain.delegations[requestId] as BrokerDelegationRecord;
  before.status = 'dispatch_unknown'; delete before.admissionRefusal;
  assert.throws(() => validateBrokerAdmissionRefusals(call, uncertain), /admission_refusal_requires_first_dispatch/);
});

test('a task expiring during its prepared write is captured and never reaches the first POST', async t => {
  const h = await fixture(t), task = h.route('Research AMD.', { kind: 'task' });
  const intent = getCallerRouteBySource(h.store.get(h.callId)!, task.ref.sourceEventId)!.intent!.response!;
  const update = h.store.update.bind(h.store); let delayed = false;
  t.mock.method(h.store, 'update', (callId, patch) => {
    const saved = update(callId, patch);
    if (!delayed && Object.values(patch.delegations ?? {}).some(raw => (raw as BrokerDelegationRecord).status === 'prepared')) {
      delayed = true; h.advance(11_000);
    }
    return saved;
  });
  await h.backend.delegateFrozenTask(task.request, task.ref, h.channel);
  assert.equal(h.posts.length, 0); assert.equal(h.reads.length, 0); assert.deepEqual(h.captures, [task.frozen.turn]);
  assert.equal(h.entries()[0].status, 'admission_expired');
  assert.deepEqual(h.entries()[0].admissionExpiry, { origin: 'caller_intent_expiry', validUntil: intent.validUntil, inputDigest: intent.inputDigest });
  assert.equal(h.entries()[0].primaryResult, undefined); assert.equal(h.entries()[0].admissionRefusal, undefined);
  await h.backend.delegateFrozenTask(task.request, task.ref, h.channel);
  assert.equal(h.posts.length, 0); assert.equal(h.reads.length, 0);
});

test('an expired queued source releases a later fresh source without changing either frozen request', async t => {
  const h = await fixture(t); h.pendingQuestion();
  const first = h.route('Compare the companies.', { kind: 'task' });
  await h.backend.delegateFrozenTask(first.request, first.ref, h.channel);
  const expired = h.route('Research AMD.', { kind: 'task' });
  await h.backend.delegateFrozenTask(expired.request, expired.ref, h.channel);
  h.advance(11_000);
  const next = h.route('Research NVDA.', { kind: 'task' });
  await h.backend.delegateFrozenTask(next.request, next.ref, h.channel);
  const parent = h.entries()[0]; h.completedSubmits();
  h.results.set(parent.requestId, { requestId: parent.requestId, bindingId: h.bindingId, state: 'completed',
    assistant: { messageId: id(72), eventId: id(73), sequence: '1', text: 'Comparison saved.' } });
  await h.backend.delegateFrozenTask(first.request, first.ref, h.channel);
  assert.deepEqual(h.posts.map(input => input.text), [first.frozen.turn.text, next.frozen.turn.text]);
  const local = h.entries().find(entry => entry.submission.source.sourceEventId === expired.ref.sourceEventId)!;
  assert.equal(local.status, 'admission_expired'); assert.equal(local.originalCapture?.state, 'accepted');
  assert.equal(local.submission.text, expired.frozen.turn.text); assert.equal(local.primaryResult, undefined);
  h.store.close();
  const restarted = new BrokerStore(h.directory); t.after(() => restarted.close());
  const resumed = createPrimaryBackend({ ...h.options, store: restarted,
    callerRouteSources: () => callerRoutingSourceAnchors(restarted.get(h.callId)!) });
  await resumed.delegateFrozenTask(expired.request, expired.ref, h.channel);
  assert.equal(h.posts.length, 2);
});

test('an ordinary answer expiring during the question read is locally refused before its first reply POST', async t => {
  const h = await fixture(t); h.pendingQuestion();
  const first = h.route('Compare the companies.', { kind: 'task' });
  await h.backend.delegateFrozenTask(first.request, first.ref, h.channel);
  const targets = h.backend.questionTargets(h.sessionId), answer = h.route('Yes.', { kind: 'answer', target: targets[0] }, targets);
  const read = h.options.api.question;
  h.options.api.question = async (bindingId, requestId, questionId) => {
    h.advance(11_000); return read(bindingId, requestId, questionId);
  };
  await h.backend.answerFrozenQuestion(answer.request, answer.ref, h.channel);
  assert.equal(h.replies.length, 0);
  assert.equal(h.entries()[0].questions![id(6)].reply!.status, 'refused');
  assert.equal(h.entries()[0].questions![id(6)].reply!.refusal?.code, 'caller_intent_expired');
  await h.backend.flushPendingCaptures();
  assert.ok(h.captures.some(turn => turn.sourceEventId === answer.ref.sourceEventId && turn.text === answer.frozen.turn.text));
  await h.backend.answerFrozenQuestion(answer.request, answer.ref, h.channel);
  assert.equal(h.replies.length, 0);
});
