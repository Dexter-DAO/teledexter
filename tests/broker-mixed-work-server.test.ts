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
import { callerIntentInputDigest, type CallerIntentInput, type CallerIntentDecision,
  type CallerIntentResponse } from '../src/broker/callerIntentContract.js';
import { streamToken } from '../src/broker/phone.js';
import { turnBodyDigest } from '../src/broker/reconciliation.js';
import { createRequestDeliveryJournal, retainRequestDeliveryAssociation, PHONE_REQUEST_DELIVERY_VERSION,
  type RequestDeliveryAssociation, type RequestDeliveryClient, type RequestDeliveryJournal,
  type RequestDeliveryQuestionResult, type RequestDeliveryResult } from '../src/broker/requestDelivery.js';
import type { BrokerConfig } from '../src/broker/config.js';
import type { EligibleContextClient, EligiblePhoneCall } from '../src/broker/eligibleContext.js';
import type { PrimaryApiClient, PrimarySubmission, PrimaryTurn } from '../src/broker/primaryApi.js';
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
type IntentOutcome = 'task' | 'selected_answer' | 'ambiguous' | 'lost';

async function fixture(t: test.TestContext) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-mixed-server-'));
  const store = new BrokerStore(directory);
  let ws: WebSocket | undefined, closeHost: (() => Promise<void>) | undefined;
  t.after(async () => {
    ws?.terminate();
    if (closeHost) await bounded(closeHost(), 'broker cleanup');
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
  const taskResult = (requestId: string) => ({ bindingId, requestId, state: 'completed' as const,
    assistant: { messageId: id(20), eventId: id(21), sequence: '2', text: 'The new research is saved.' } });
  const primary: PrimaryApiClient = { readiness: forbidden, openCall: forbidden, getContext: forbidden,
    reconcile: forbidden, question: forbidden, reply: forbidden,
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
      assert.ok(taskPosts.some(post => post.requestId === requestId)); assert.equal(input.source.kind, 'assistant');
      return { bindingId, callSid, requestId, version: input.version,
        delivery: { state: 'ready', text: taskResult(requestId).assistant.text,
          source: { kind: 'assistant', messageId: id(20), eventSequence: '2', provenance: {
            messageCreatedAt: createdAt, eventId: id(21), eventOccurredAt: createdAt, runId: id(24) } } } };
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
      const input = JSON.parse(String(init?.body)) as CallerIntentInput;
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
        const target = input.questions.find(candidate => candidate.kind === 'selected'); assert.ok(target);
        decision = { kind: 'answer', target };
      }
      const reservedAt = new Date().toISOString();
      const response: CallerIntentResponse = { v: input.v, operationId, bindingId,
        inputDigest: callerIntentInputDigest(operationId, bindingId, input), brokerInputDigest: input.brokerInputDigest,
        presentationDigest: input.presentationDigest, generationId: input.generationId, sourceSessionId: input.sourceSessionId,
        callDeadlineAt: input.callDeadlineAt, questions: input.questions, results: input.results, version: input.version,
        reservedAt, validUntil: new Date(Math.min(Date.parse(input.callDeadlineAt), Date.parse(reservedAt) + 10_000)).toISOString(),
        state: 'resolved', decision };
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
  const say = async (text: string, outcome: IntentOutcome, startMs = 0) => {
    await pause(5); const voice = voices.at(-1)!;
    const frames = startMs / 20; assert.equal(Number.isInteger(frames), true);
    const audio = (silent: boolean) => ws!.send(JSON.stringify({ event: 'media', streamSid,
      media: { track: 'inbound', payload: Buffer.alloc(160, silent ? 255 : 0).toString('base64') } }));
    for (let index = voice.input.length; index < frames; index++) audio(true);
    audio(false); await until(() => voice.input.length === frames + 1, 'caller audio');
    const sequence = voice.caption.revision + 1;
    voice.caption = { revision: sequence, callerRevision: sequence, fragments: [...voice.caption.fragments,
      { eventId: `caller_${sequence}`, sequence, role: 'user', text, startMs, endMs: startMs + 20 }] };
    await voice.options.onTranscript(structuredClone(voice.caption)); outcomes.push(outcome);
    const request: LiveDelegationRequest = { callId: call.id, sessionId: currentCall().liveSessionId!,
      delegationId: `delegate_${sequence}`, requestId: `provider_${sequence}`, offsetMs: startMs + 20,
      transcript: structuredClone(voice.caption) };
    const delegate = () => bounded(voice.options.backend.delegate(request, voice.channel), 'explicit caller delegation');
    return { request, delegate };
  };
  return { call, currentCall, say, voices, taskPosts, replyPosts, captures, intentPosts, suppliedOutcomes,
    providerEnds: () => providerEnds };
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
