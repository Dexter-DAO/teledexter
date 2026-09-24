import assert from 'node:assert/strict';
import test from 'node:test';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Twilio from 'twilio';
import { WebSocket } from 'ws';
import { createBrokerServer } from '../src/broker/server.js';
import { BrokerStore } from '../src/broker/store.js';
import { brokerIdentity, type BrokerDelegationRecord } from '../src/broker/backend.js';
import { createPrimaryCallerIntentClient } from '../src/broker/callerIntentClient.js';
import { callerIntentV2InputSchema, callerIntentInputDigest, type CallerIntentInput, type CallerIntentDecision,
  type CallerIntentResponse } from '../src/broker/callerIntentContract.js';
import { streamToken } from '../src/broker/phone.js';
import { turnBodyDigest } from '../src/broker/reconciliation.js';
import { createRequestDeliveryJournal, retainRequestDeliveryAssociation, PHONE_REQUEST_DELIVERY_VERSION,
  type RequestDeliveryAssociation, type RequestDeliveryClient, type RequestDeliveryJournal,
  type RequestDeliveryQuestionResult, type RequestDeliveryResult } from '../src/broker/requestDelivery.js';
import type { BrokerConfig } from '../src/broker/config.js';
import type { EligibleContextClient, EligiblePhoneCall } from '../src/broker/eligibleContext.js';
import type { PrimaryApiClient, PrimarySubmission, PrimaryTurn, PrimaryQuestionResult,
  PrimaryReplyInput, PrimaryResult } from '../src/broker/primaryApi.js';
import type { BrokerLiveSession, BrokerLiveSessionOptions, LiveBackendChannel,
  LiveDelegationRequest, LiveTranscriptSnapshot } from '../src/live/types.js';

// Real signed media route, Store, generation host, source freezer, HTTP intent
// client, resolver and router. External API responses and voice transport are
// fixtures; this does not exercise the shared API database or its model policy.
const id = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const bindingId = id(1), owner = id(2), primaryId = id(3), threadId = id(4);
const originalRequest = id(5), executionBinding = id(6), runId = id(7), questionId = id(8), associationId = id(9);
const callSid = `CA${'a'.repeat(32)}`, accountSid = `AC${'b'.repeat(32)}`;
const pause = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
async function bounded<T>(work: Promise<T>, label: string, ms = 6000): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try { return await Promise.race([work, new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`Timed out: ${label}`)), ms);
  })]); } finally { clearTimeout(timer); }
}
async function until(predicate: () => boolean, label: string) {
  const deadline = Date.now() + 6000;
  while (!predicate()) { assert.ok(Date.now() < deadline, `Timed out: ${label}`); await pause(10); }
}
type IntentOutcome = 'task' | 'ordinary_answer' | 'selected_answer' | 'ambiguous' | 'lost';

async function fixture(t: test.TestContext, options: { ordinaryQuestion?: boolean } = {}) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-mixed-server-'));
  const store = new BrokerStore(directory);
  let ws: WebSocket | undefined, closeHost: (() => Promise<void>) | undefined;
  const activeDelegations = new Set<Promise<void>>();
  const releases = new Set<() => void>();
  const intentHolds: Array<{ enter(): void; wait: Promise<void> }> = [];
  t.after(async () => {
    for (const release of releases) release();
    ws?.terminate();
    if (closeHost) await bounded(closeHost(), 'broker cleanup');
    await bounded(Promise.allSettled([...activeDelegations]), 'delegation cleanup');
    store.close(); fs.rmSync(directory, { recursive: true, force: true });
  });
  const config: BrokerConfig = { host: '127.0.0.1', port: 3213, publicOrigin: 'https://phone.example.test',
    stateDir: directory, controlToken: 'c'.repeat(40), apiKey: 'fixture-key', model: 'gpt-live-1', voice: 'marin',
    twilioAccountSid: accountSid, twilioAuthToken: 'fixture-token', from: '+12025550100', allowedTo: ['+12025550123'],
    apiBaseUrl: 'https://api.example.test', phoneServiceSecret: 's'.repeat(40), maxCallSeconds: 60 };
  const call = store.reserve({ requestId: id(10), number: config.allowedTo[0]!, direction: 'outbound', callSid,
    purpose: 'Review the saved comparison.', continuation: { originalRequestId: originalRequest, questionId, questionRevision: 1 } },
  'authenticated_broker_control').call;
  const createdAt = new Date().toISOString(), expiresAt = new Date(Date.now() + 120_000).toISOString();
  const assignmentRef = { assignmentId: call.requestId, revision: 1 as const, digest: 'a'.repeat(64) };
  const version = { ownerPrincipalId: owner, primaryDexterId: primaryId, threadId,
    contextEpoch: '1', threadEventSequence: '10', digest: 'b'.repeat(64) };
  const context = (): EligiblePhoneCall => ({ bindingId, callSid, state: 'open', callAssignmentRef: assignmentRef,
    context: { primaryDexterId: primaryId, threadId, displayName: 'Dexter', messages: [], history: {
      version, pageDigest: 'c'.repeat(64), localContext: '', nextBeforeSequence: null, hasMore: false, omittedForBudget: false } } });
  store.recordPrimaryBinding(call.id, context());
  const association: RequestDeliveryAssociation = { id: associationId, v: PHONE_REQUEST_DELIVERY_VERSION,
    delivery: { bindingId, callSid }, execution: { bindingId: executionBinding, requestId: originalRequest, runId },
    question: { id: questionId, revision: 1 }, createdAt };
  const taskPosts: Array<{ requestId: string; input: PrimarySubmission }> = [], replyPosts: RequestDeliveryJournal[] = [];
  const ordinaryReplyPosts: Array<{ requestId: string; questionId: string; input: PrimaryReplyInput }> = [];
  const captures: PrimaryTurn[] = [], intentPosts: Array<{ operationId: string; input: CallerIntentInput; method: string }> = [];
  const outcomes: IntentOutcome[] = [], suppliedOutcomes: IntentOutcome[] = [];
  let answered: RequestDeliveryJournal | undefined, providerEnds = 0;
  const questionResponse = (journal: RequestDeliveryJournal): RequestDeliveryQuestionResult => ({
    bindingId: executionBinding, requestId: originalRequest, deliveryAssociation: journal.association!,
    state: answered ? 'running' : 'waiting_for_reply', question: {
      id: questionId, revision: 1, runId, threadId, agent: { id: primaryId, name: 'Dexter' },
      state: answered ? 'answered' : 'pending', runState: answered ? 'running' : 'waiting_for_reply',
      question: 'Continue the saved comparison?', context: 'Research only.', proposedAnswer: 'Compare both stocks.',
      yesMeaning: 'Compare both stocks.', noMeaning: 'Leave the comparison waiting.', createdAt, expiresAt,
      reply: answered ? { ...answered.reply!.input.reply, interpretedAnswer: 'Compare both stocks.', receivedAt: new Date().toISOString() } : null } });
  const requests: RequestDeliveryClient = {
    async associate() { throw new Error('Unexpected alias association'); },
    async question(journal) { return questionResponse(journal); },
    async result(journal): Promise<RequestDeliveryResult> {
      if (answered) return { bindingId: executionBinding, requestId: originalRequest, deliveryAssociation: journal.association!, state: 'running' };
      return { ...questionResponse(journal), state: 'waiting_for_reply' };
    },
    async reply(journal) {
      assert.deepEqual(journal.target.execution, association.execution);
      assert.deepEqual(journal.association, association);
      replyPosts.push(structuredClone(journal)); answered = structuredClone(journal);
      return { ...questionResponse(journal), replayed: false };
    },
  };
  const forbidden = async () => { throw new Error('Unexpected external API operation'); };
  const ordinaryQuestionId = id(25), ordinaryRunId = id(26);
  let ordinaryAnswer: PrimaryReplyInput | undefined;
  const ordinaryQuestion = (requestId: string): PrimaryQuestionResult => ({ bindingId, requestId,
    state: ordinaryAnswer ? 'running' : 'waiting_for_reply', question: {
      id: ordinaryQuestionId, revision: 1, runId: ordinaryRunId, threadId, agent: { id: primaryId, name: 'Dexter' },
      state: ordinaryAnswer ? 'answered' : 'pending', runState: ordinaryAnswer ? 'running' : 'waiting_for_reply',
      question: 'Include the filing comparison?', context: 'Research only.', proposedAnswer: 'Compare the filings.',
      yesMeaning: 'Compare the filings.', noMeaning: 'Leave out the filings.', createdAt, expiresAt,
      reply: ordinaryAnswer ? { ...ordinaryAnswer.reply, interpretedAnswer: 'Compare the filings.',
        receivedAt: new Date().toISOString() } : null } });
  const completedTask = (requestId: string) => ({ bindingId, requestId, state: 'completed' as const,
    assistant: { messageId: id(20), eventId: id(21), sequence: '2', text: 'The new research is saved.' } });
  const taskResult = (requestId: string): PrimaryResult => options.ordinaryQuestion && !ordinaryAnswer
    ? { ...ordinaryQuestion(requestId), state: 'waiting_for_reply' } : completedTask(requestId);
  const primary: PrimaryApiClient = { readiness: forbidden, openCall: forbidden, getContext: forbidden,
    reconcile: forbidden,
    async question(receivedBinding, requestId, receivedQuestion) {
      assert.ok(options.ordinaryQuestion); assert.equal(receivedBinding, bindingId);
      assert.equal(requestId, taskPosts[0]!.requestId); assert.equal(receivedQuestion, ordinaryQuestionId);
      return ordinaryQuestion(requestId);
    },
    async reply(receivedBinding, requestId, receivedQuestion, input) {
      assert.ok(options.ordinaryQuestion); assert.equal(receivedBinding, bindingId);
      assert.equal(requestId, taskPosts[0]!.requestId); assert.equal(receivedQuestion, ordinaryQuestionId);
      ordinaryReplyPosts.push({ requestId, questionId: receivedQuestion, input: structuredClone(input) });
      ordinaryAnswer = structuredClone(input);
      return { ...ordinaryQuestion(requestId), replayed: false };
    },
    async submit(receivedBinding, requestId, input) {
      assert.equal(receivedBinding, bindingId); assert.notEqual(requestId, originalRequest);
      taskPosts.push({ requestId, input: structuredClone(input) }); return taskResult(requestId);
    },
    async result(receivedBinding, requestId) { assert.equal(receivedBinding, bindingId); return taskResult(requestId); },
    async captureTurn(receivedBinding, turn) {
      assert.equal(receivedBinding, bindingId); captures.push(structuredClone(turn));
      return { bindingId, sourceEventId: turn.sourceEventId, revision: turn.revision, recordId: turn.sourceEventId,
        revisionRecordId: brokerIdentity('capture', turn.sourceEventId, String(turn.revision)), bodyDigest: turnBodyDigest(turn), replayed: false };
    },
    async close(receivedBinding) { assert.equal(receivedBinding, bindingId); return { bindingId, callSid, state: 'closed' }; },
  };
  const eligible: EligibleContextClient = {
    async fetch() { return context(); },
    async assert(_scope, expected) { assert.deepEqual(expected, version); return { bindingId, callSid, version: expected, current: true }; },
    async delivery(_scope, requestId, input, journal) {
      if (journal) {
        assert.equal(requestId, originalRequest); assert.deepEqual(journal.association, association);
        assert.equal(input.source.kind, 'question');
        return { bindingId, callSid, requestId, version: input.version, deliveryAssociation: association,
          delivery: { state: 'ready', text: 'Continue the saved comparison? Yes compares both stocks. No leaves it waiting.',
            question: questionResponse(journal).question, source: { kind: 'question', messageId: id(22), eventSequence: '1',
              provenance: { messageCreatedAt: createdAt, eventId: id(23), eventOccurredAt: createdAt, runId } } } };
      }
      assert.ok(taskPosts.some(post => post.requestId === requestId));
      if (options.ordinaryQuestion && input.source.kind === 'question') return { bindingId, callSid, requestId,
        version: input.version, delivery: { state: 'ready', text: 'Include the filing comparison? Yes compares the filings. No leaves them out.',
          question: ordinaryQuestion(requestId).question, source: { kind: 'question', messageId: id(27), eventSequence: '2',
            provenance: { messageCreatedAt: createdAt, eventId: id(28), eventOccurredAt: createdAt, runId: ordinaryRunId } } } };
      assert.equal(input.source.kind, 'assistant');
      return { bindingId, callSid, requestId, version: input.version,
        delivery: { state: 'ready', text: completedTask(requestId).assistant.text,
          source: { kind: 'assistant', messageId: id(20), eventSequence: '2', provenance: {
            messageCreatedAt: createdAt, eventId: id(21), eventOccurredAt: createdAt,
            runId: options.ordinaryQuestion ? ordinaryRunId : id(24) } } } };
    },
  };
  const client = createPrimaryCallerIntentClient({ apiBaseUrl: config.apiBaseUrl, phoneServiceSecret: config.phoneServiceSecret,
    fetch: async (url, init) => {
      const headers = new Headers(init?.headers), operationId = headers.get('Idempotency-Key');
      assert.equal(String(url), config.apiBaseUrl + `/internal/primary-dexter/channels/phone/calls/${bindingId}/caller-intents`);
      assert.equal(init?.method, 'POST'); assert.ok(operationId);
      assert.equal(headers.get('X-Dexter-Binding-Id'), bindingId);
      assert.equal(headers.get('X-Dexter-Phone-Context-Version'), 'memory-eligible-v1');
      assert.match(headers.get('authorization') ?? '', /^Bearer [a-f0-9]{64}$/);
      const input = callerIntentV2InputSchema.parse(JSON.parse(String(init?.body)));
      intentPosts.push({ operationId, input: structuredClone(input), method: init!.method! });
      const saved = store.get(call.id)!.callerRouting!.records.find(record => record.intent?.operationId === operationId)!;
      assert.ok(saved); assert.deepEqual(saved.intent!.input, input); assert.equal(saved.decision, undefined);
      const outcome = outcomes.shift(); assert.ok(outcome, 'test must explicitly provide the external intent outcome');
      suppliedOutcomes.push(outcome);
      if (outcome === 'lost') throw new Error('connection closed after possible intent acceptance');
      let decision: CallerIntentDecision;
      if (outcome === 'task') decision = { kind: 'task' };
      else if (outcome === 'ambiguous') decision = { kind: 'clarify', reason: 'ambiguous_target' };
      else {
        const target = input.questions.find(candidate => candidate.kind === (outcome === 'ordinary_answer' ? 'ordinary' : 'selected')); assert.ok(target);
        decision = { kind: 'answer', target };
      }
      const reservedAt = new Date().toISOString();
      const response: CallerIntentResponse = { v: input.v, operationId, bindingId,
        inputDigest: callerIntentInputDigest(operationId, bindingId, input), brokerInputDigest: input.brokerInputDigest,
        presentationDigest: input.presentationDigest, generationId: input.generationId, sourceSessionId: input.sourceSessionId,
        callDeadlineAt: input.callDeadlineAt, questions: input.questions, results: input.results, version: input.version,
        resultCoverage: input.resultCoverage, resultContextCoverage: input.resultCoverage.state === 'complete'
          ? { state: 'complete' } : { state: 'unavailable', reason: 'inventory_overflow' },
        reservedAt, validUntil: new Date(Math.min(Date.parse(input.callDeadlineAt), Date.parse(reservedAt) + 10_000)).toISOString(),
        state: 'resolved', decision };
      const hold = intentHolds.shift();
      if (hold) { hold.enter(); await hold.wait; }
      return Response.json(response);
    } });
  const voices: Array<{ options: BrokerLiveSessionOptions; caption: LiveTranscriptSnapshot; channel: LiveBackendChannel;
    input: string[]; comments: string[]; instructions: string[] }> = [];
  const broker = createBrokerServer({ config, store, primary, eligibleContext: eligible,
    callerIntent: { client, timeoutMs: 10_000 },
    provider: { calls: () => ({ async update() { providerEnds++; return { status: 'completed' }; } }) } as unknown as ReturnType<typeof Twilio>,
    selectedQuestion: { requests, pollIntervalMs: 50, association: { async associate(seed) {
      assert.deepEqual(seed.request, { v: PHONE_REQUEST_DELIVERY_VERSION, originalRequestId: originalRequest, questionId, questionRevision: 1 });
      return retainRequestDeliveryAssociation(createRequestDeliveryJournal({ deliveryCall: seed.deliveryCall, scope: seed.scope,
        request: seed.request, target: { delivery: seed.delivery, execution: association.execution, question: association.question } }),
      { association, replayed: false });
    } } },
    voiceFactory(options) {
      const sessionId = `mixed_generation_${voices.length + 1}`, abort = new AbortController();
      const listeners = new Set<(snapshot: LiveTranscriptSnapshot) => void>();
      const value = { options, caption: { revision: 0, callerRevision: 0, fragments: [] } as LiveTranscriptSnapshot,
        channel: undefined as unknown as LiveBackendChannel, input: [] as string[], comments: [] as string[], instructions: [] as string[] };
      value.channel = { signal: abort.signal, getContext: () => structuredClone(value.caption),
        subscribeContext(listener) { listeners.add(listener); return () => { listeners.delete(listener); }; },
        async publish() { throw new Error('Raw channel publication must not replace generation delivery'); } };
      const receipt = () => ({ eventId: `append_${sessionId}_${value.comments.length}`, status: 'accepted' as const,
        observedCallerRevision: value.caption.callerRevision, observedTranscriptSequence: value.caption.revision });
      const voice: BrokerLiveSession = {
        appendAudio(payload) { value.input.push(payload); return true; }, async appendContext() { throw new Error('No flattened context'); },
        async appendInstructions(text) { value.instructions.push(text); return receipt(); },
        async appendCommentary(text) { value.comments.push(text); return receipt(); },
        async interruptPlayback() { return receipt(); }, resumePlayback() {}, getTranscript: () => structuredClone(value.caption),
        async close(reason = 'closed') {
          abort.abort(); const result = { callId: call.id, sessionId, reason, finalized: true, transcriptPersisted: true, outputAudioDiscarded: true };
          options.onClosed(result); return result;
        },
      };
      voices.push(value); queueMicrotask(() => options.onReady({ callId: call.id, sessionId })); return voice;
    },
  });
  closeHost = () => broker.close();
  await bounded(new Promise<void>((resolve, reject) => {
    broker.server.once('error', reject); broker.server.listen(0, '127.0.0.1', resolve);
  }), 'server listen');
  const route = `/broker/media/${call.id}`, streamSid = `MZ${'c'.repeat(32)}`;
  ws = new WebSocket(`ws://127.0.0.1:${(broker.server.address() as { port: number }).port}${route}`, {
    headers: { 'x-twilio-signature': Twilio.getExpectedTwilioSignature(config.twilioAuthToken, config.publicOrigin + route, {}) } });
  await bounded(new Promise<void>((resolve, reject) => { ws!.once('open', resolve); ws!.once('error', reject); }), 'media socket');
  ws.send(JSON.stringify({ event: 'start', start: { accountSid, callSid, streamSid,
    customParameters: { token: streamToken(config.controlToken, call.id, callSid) },
    mediaFormat: { encoding: 'audio/x-mulaw', sampleRate: 8000, channels: 1 } } }));
  const currentCall = () => store.get(call.id)!;
  await until(() => currentCall().selectedQuestionVoice?.presentations.length === 1, 'selected question presentation');
  const audio = (silent: boolean) => ws!.send(JSON.stringify({ event: 'media', streamSid,
    media: { track: 'inbound', payload: Buffer.alloc(160, silent ? 255 : 0).toString('base64') } }));
  const mediaOnly = async (silent: boolean) => {
    const voice = voices.at(-1)!, count = voice.input.length;
    audio(silent); await until(() => voice.input.length === count + 1, 'additional audio accepted');
  };
  const holdNextIntent = () => {
    let enter!: () => void, release!: () => void;
    const entered = new Promise<void>(resolve => { enter = resolve; });
    const wait = new Promise<void>(resolve => { release = resolve; });
    releases.add(release); intentHolds.push({ enter, wait });
    return { entered, release };
  };
  const say = async (text: string, outcome: IntentOutcome, startMs = 0) => {
    await pause(5); const voice = voices.at(-1)!;
    const frames = startMs / 20; assert.equal(Number.isInteger(frames), true);
    for (let index = voice.input.length; index < frames; index++) audio(true);
    audio(false); await until(() => voice.input.length === frames + 1, 'caller audio');
    const sequence = voice.caption.revision + 1;
    voice.caption = { revision: sequence, callerRevision: sequence, fragments: [...voice.caption.fragments,
      { eventId: `caller_${sequence}`, sequence, role: 'user', text, startMs, endMs: startMs + 20 }] };
    await voice.options.onTranscript(structuredClone(voice.caption)); outcomes.push(outcome);
    const request: LiveDelegationRequest = { callId: call.id, sessionId: currentCall().liveSessionId!,
      delegationId: `delegate_${sequence}`, requestId: `provider_${sequence}`, offsetMs: startMs + 20,
      transcript: structuredClone(voice.caption) };
    const start = () => {
      const work = voice.options.backend.delegate(request, voice.channel); activeDelegations.add(work);
      void work.then(() => activeDelegations.delete(work), () => activeDelegations.delete(work));
      return work;
    };
    const delegate = () => bounded(start(), 'explicit caller delegation');
    return { request, delegate, start };
  };
  return { call, currentCall, say, voices, taskPosts, replyPosts, captures, intentPosts, suppliedOutcomes,
    ordinaryReplyPosts, ordinaryQuestionId, holdNextIntent, mediaOnly, providerEnds: () => providerEnds };
}

test('actual selected server routes a new task and then an exact answer without consuming the pending question as task consent', { timeout: 15_000 }, async t => {
  const f = await fixture(t);
  assert.equal(f.voices[0]!.options.instructions.includes(f.call.purpose), false);
  assert.deepEqual(f.voices[0]!.instructions, []);
  const original = structuredClone(f.currentCall().selectedQuestion!.questions[0]!.record!.base);
  const task = await f.say(' Research Apple.\t', 'task'); await task.delegate(); await task.delegate();
  assert.equal(f.intentPosts.length, 1); assert.equal(f.taskPosts.length, 1); assert.equal(f.replyPosts.length, 0);
  assert.deepEqual(f.intentPosts[0]!.input.questions[0]!.execution, associationExecution());
  assert.equal(f.taskPosts[0]!.input.text, ' Research Apple.\t');
  assert.deepEqual(f.taskPosts[0]!.input.callAssignmentRef, f.currentCall().primary!.callAssignmentRef);
  const selected = f.currentCall().selectedQuestion!.questions[0]!;
  assert.deepEqual(selected.record!.base, original); assert.deepEqual(selected.admissions, []);
  const delivery = f.currentCall().selectedQuestionVoice!.presentations[0]!.response.delivery;
  assert.ok(delivery.state === 'ready' && 'question' in delivery); assert.equal(delivery.question.state, 'pending');
  assert.equal(f.voices[0]!.comments.includes('The new research is saved.'), true);
  const ordinary = f.currentCall().delegations[f.taskPosts[0]!.requestId] as BrokerDelegationRecord;
  assert.equal(ordinary.status, 'completed'); assert.notEqual(ordinary.requestId, originalRequest);
  const answer = await f.say(' Yes.\t', 'selected_answer', 1400); await answer.delegate(); await answer.delegate();
  assert.equal(f.intentPosts.length, 2); assert.equal(f.taskPosts.length, 1); assert.equal(f.replyPosts.length, 1);
  assert.deepEqual(f.replyPosts[0]!.target.execution, associationExecution());
  assert.equal(f.replyPosts[0]!.association!.id, associationId); assert.equal(f.replyPosts[0]!.target.question.id, questionId);
  assert.equal(f.replyPosts[0]!.reply!.input.text, ' Yes.\t'); assert.equal(f.replyPosts[0]!.reply!.input.reply.answer, 'yes');
  const records = f.currentCall().callerRouting!.records;
  assert.deepEqual(records.map(record => record.decision?.kind), ['task', 'answer']);
  assert.deepEqual(records.map(record => record.frozen.turn.text), [' Research Apple.\t', ' Yes.\t']);
  assert.notEqual(records[0]!.frozen.turn.sourceEventId, records[1]!.frozen.turn.sourceEventId);
  assert.ok(records.every(record => record.intent?.response?.state === 'resolved'));
  assert.deepEqual(f.suppliedOutcomes, ['task', 'selected_answer']);
  assert.equal(f.providerEnds(), 0);
});

function associationExecution() { return { bindingId: executionBinding, requestId: originalRequest, runId }; }

for (const outcome of ['ambiguous', 'lost'] as const) {
  test(`actual selected server preserves raw speech and dispatches neither task nor answer after ${outcome} intent`, { timeout: 15_000 }, async t => {
    const f = await fixture(t);
    const raw = '  Do that, then check Apple.\t';
    const speech = await f.say(raw, outcome); await speech.delegate(); await speech.delegate();
    assert.equal(f.intentPosts.length, 1, 'possible inference acceptance cannot authorize a second POST on replay');
    assert.deepEqual(f.suppliedOutcomes, [outcome], 'the intended external response boundary must be reached');
    assert.deepEqual(f.taskPosts, []); assert.deepEqual(f.replyPosts, []);
    const record = f.currentCall().callerRouting!.records[0]!;
    assert.equal(record.frozen.turn.text, raw); assert.equal(record.intent!.input.source.text, raw);
    assert.deepEqual(record.decision, { kind: 'clarify', reason: outcome === 'lost' ? 'unavailable' : 'ambiguous_target' });
    assert.equal(record.intent!.response?.state, outcome === 'lost' ? undefined : 'resolved');
    assert.deepEqual(f.captures, [record.frozen.turn]); assert.ok(record.capture);
    assert.deepEqual(f.currentCall().selectedQuestion!.questions[0]!.admissions, []);
    assert.equal(f.currentCall().selectedQuestion!.questions[0]!.record!.attempts.length, 0);
    assert.deepEqual(f.currentCall().delegations, {}); assert.equal(f.providerEnds(), 0);
  });
}

// X-MIXED-001: captions can lag behind accepted caller audio while the HTTP
// intent request is outstanding. Exercise each fresh admission through the
// actual host, then distinguish added silence from another speech frame.
for (const route of ['task', 'ordinary_answer', 'selected_answer'] as const) {
  for (const duringIntent of ['unchanged', 'silence', 'speech'] as const) {
    test(`held intent ${route} with ${duringIntent} audio rechecks the frozen speech before fresh dispatch`,
      { timeout: 20_000 }, async t => {
        const f = await fixture(t, { ordinaryQuestion: route === 'ordinary_answer' });
        let ordinaryRequest: string | undefined;
        if (route === 'ordinary_answer') {
          const seed = await f.say('Compare the filings.', 'task');
          let seedError: unknown;
          void seed.start().catch(error => { seedError = error; });
          await until(() => {
            if (seedError) throw seedError;
            ordinaryRequest = f.taskPosts[0]?.requestId;
            const entry = ordinaryRequest && f.currentCall().delegations[ordinaryRequest] as BrokerDelegationRecord | undefined;
            return Boolean(entry && entry.status === 'waiting_for_reply'
              && entry.questions?.[f.ordinaryQuestionId]?.presentation?.publicationReceipts?.length);
          }, 'ordinary question accepted publication');
          assert.equal(f.taskPosts.length, 1);
          assert.equal(f.ordinaryReplyPosts.length, 0);
          assert.equal(f.voices[0]!.comments.some(text => text.includes('Include the filing comparison?')), true);
        }
        const taskCount = f.taskPosts.length, intentCount = f.intentPosts.length;
        const raw = route === 'task' ? ' Research Apple.\t' : ' Yes.\t';
        const speech = await f.say(raw, route, route === 'ordinary_answer' ? 1400 : 0);
        const held = f.holdNextIntent();
        let completed = false;
        const result = speech.start().then(() => { completed = true; return undefined; }, error => { completed = true; return error; });
        try {
          await bounded(Promise.race([held.entered, result.then(error => {
            throw error ?? new Error('Delegation ended before the held intent HTTP request');
          })]), 'intent HTTP entered with durable frozen source');
          const voice = f.voices.at(-1)!, caption = structuredClone(voice.caption);
          const saved = f.currentCall().callerRouting!.records.at(-1)!;
          const frozen = structuredClone(saved.frozen), input = structuredClone(saved.intent!.input);
          const publicationSequence = f.currentCall().publications!.records.at(-1)!.publicationSequence;
          const inputNextSample = f.currentCall().liveGenerations!.inputNextSample;
          const frames = voice.input.length;
          assert.equal(completed, false);
          assert.equal(saved.intent!.response, undefined);
          assert.equal(saved.decision, undefined);
          assert.equal(f.intentPosts.length, intentCount + 1);
          assert.equal(input.source.text, raw);
          assert.equal(frozen.audioThroughMs, route === 'ordinary_answer' ? 1420 : 20);
          if (route === 'ordinary_answer') {
            assert.equal(input.questions.length, 2);
            const target = input.questions.find(value => value.kind === 'ordinary')!;
            assert.equal(target.execution.bindingId, bindingId);
            assert.equal(target.execution.requestId, ordinaryRequest);
            assert.equal(target.question.id, f.ordinaryQuestionId);
            assert.ok(input.questions.some(value => value.kind === 'selected' && value.question.id === questionId));
          }
          if (duringIntent !== 'unchanged') await f.mediaOnly(duringIntent === 'silence');
          assert.equal(voice.input.length, frames + (duringIntent === 'unchanged' ? 0 : 1));
          assert.equal(f.currentCall().liveGenerations!.inputNextSample,
            inputNextSample + (duringIntent === 'unchanged' ? 0 : 160));
          assert.deepEqual(voice.caption, caption, 'the provider has not emitted a revised caption');
          const segment = f.currentCall().liveGenerations!.segments.find(value => value.sessionId === speech.request.sessionId)!;
          assert.deepEqual(segment.transcript, caption);
          assert.equal(f.currentCall().publications!.records.at(-1)!.publicationSequence, publicationSequence);
          assert.equal(completed, false);
          assert.equal(f.taskPosts.length, taskCount);
          assert.equal(f.replyPosts.length, 0);
          assert.equal(f.ordinaryReplyPosts.length, 0);
          held.release();
          const error = await bounded(result, 'fresh route after held intent response');
          const retained = f.currentCall().callerRouting!.records.find(value => value.inputDigest === saved.inputDigest)!;
          assert.deepEqual(retained.frozen, frozen);
          assert.deepEqual(retained.intent!.input, input);
          assert.equal(retained.intent!.response?.state, 'resolved', 'the acoustic guard sees a completed intent response');
          assert.equal(retained.decision?.kind, route === 'task' ? 'task' : 'answer');
          if (duringIntent === 'speech') {
            assert.ok(error instanceof Error);
            assert.equal(error.message, 'caller_route_acoustic_changed');
            assert.equal(f.taskPosts.length, taskCount);
            assert.equal(f.replyPosts.length, 0);
            assert.equal(f.ordinaryReplyPosts.length, 0);
            assert.equal(Object.values(f.currentCall().delegations).some(value =>
              (value as BrokerDelegationRecord).submission.source.sourceEventId === frozen.turn.sourceEventId), false);
            assert.deepEqual(f.currentCall().selectedQuestion!.questions[0]!.admissions, []);
            assert.deepEqual(f.currentCall().selectedQuestionVoice!.sources, []);
            if (ordinaryRequest) {
              const question = (f.currentCall().delegations[ordinaryRequest] as BrokerDelegationRecord).questions![f.ordinaryQuestionId]!;
              assert.equal(question.reply, undefined);
              assert.deepEqual(question.refusedReplies ?? [], []);
            }
            const replayError = await bounded(speech.start().then(() => undefined, failure => failure), 'stale acoustic replay refusal');
            assert.ok(replayError instanceof Error); assert.equal(replayError.message, 'caller_route_acoustic_changed');
          } else {
            assert.equal(error, undefined);
            assert.equal(f.taskPosts.length, taskCount + (route === 'task' ? 1 : 0));
            assert.equal(f.replyPosts.length, route === 'selected_answer' ? 1 : 0);
            assert.equal(f.ordinaryReplyPosts.length, route === 'ordinary_answer' ? 1 : 0);
            if (route === 'ordinary_answer') {
              assert.equal(f.ordinaryReplyPosts[0]!.requestId, ordinaryRequest);
              assert.equal(f.ordinaryReplyPosts[0]!.questionId, f.ordinaryQuestionId);
              assert.equal(f.ordinaryReplyPosts[0]!.input.text, raw);
              assert.equal(f.ordinaryReplyPosts[0]!.input.source.sourceEventId, frozen.turn.sourceEventId);
              assert.deepEqual(f.currentCall().selectedQuestion!.questions[0]!.admissions, []);
            }
            if (route === 'selected_answer') {
              assert.deepEqual(f.replyPosts[0]!.target.execution, associationExecution());
              assert.equal(f.replyPosts[0]!.reply!.input.text, raw);
            }
            // Once this exact source has a durable downstream admission, later
            // speech cannot turn its replay into another admission or block it.
            await f.mediaOnly(false);
            await speech.delegate();
          }
          assert.equal(f.intentPosts.length, intentCount + 1, 'exact replay never asks for another classification');
          assert.equal(f.taskPosts.length, taskCount + (duringIntent !== 'speech' && route === 'task' ? 1 : 0));
          assert.equal(f.replyPosts.length, duringIntent !== 'speech' && route === 'selected_answer' ? 1 : 0);
          assert.equal(f.ordinaryReplyPosts.length, duringIntent !== 'speech' && route === 'ordinary_answer' ? 1 : 0);
          assert.equal(f.providerEnds(), 0);
        } finally { held.release(); }
      });
  }
}
