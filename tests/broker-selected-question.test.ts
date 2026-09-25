import assert from 'node:assert/strict';
import test from 'node:test';
import { createServer } from 'node:http';
import { createSelectedQuestionAssociationClient, createSelectedQuestionHost, parseSelectedQuestionJournal,
  assertSelectedQuestionJournalUpdate, type SelectedQuestionAnswer, type SelectedQuestionHostOptions, type SelectedQuestionSeed, type SelectedQuestionJournal } from '../src/broker/selectedQuestion.js';
import { createRequestDeliveryJournal, retainRequestDeliveryAssociation, PHONE_REQUEST_DELIVERY_VERSION,
  type RequestDeliveryAssociation, type RequestDeliveryClient, type RequestDeliveryQuestionResult } from '../src/broker/requestDelivery.js';
import { turnBodyDigest } from '../src/broker/reconciliation.js';
import { PrimaryApiError, type PrimaryTurn, type PrimaryTurnReceipt } from '../src/broker/primaryApi.js';

const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const at = '2026-09-24T10:00:00.000Z';
const seed: SelectedQuestionSeed = { v: 'broker.selected-question-seed.v1',
  deliveryCall: { callId: uuid(1), sessionId: 'current_session' },
  scope: { ownerPrincipalId: uuid(2), primaryDexterId: uuid(3), threadId: uuid(4) },
  delivery: { bindingId: uuid(5), callSid: `CA${'a'.repeat(32)}` },
  request: { v: PHONE_REQUEST_DELIVERY_VERSION, originalRequestId: uuid(6), questionId: uuid(7), questionRevision: 1 },
  callDeadlineAt: '2026-09-24T10:05:00.000Z' };
const association = (questionId = seed.request.questionId): RequestDeliveryAssociation => ({ id: uuid(questionId === uuid(7) ? 8 : 18),
  v: PHONE_REQUEST_DELIVERY_VERSION, delivery: seed.delivery, execution: { bindingId: uuid(9), requestId: uuid(6), runId: uuid(10) },
  question: { id: questionId, revision: 1 }, createdAt: at });
const associated = (value = seed) => {
  const response = { association: association(value.request.questionId), replayed: false };
  return retainRequestDeliveryAssociation(createRequestDeliveryJournal({ deliveryCall: value.deliveryCall, scope: value.scope,
    request: value.request, target: { delivery: value.delivery, execution: response.association.execution, question: response.association.question } }), response);
};
const question = (questionId = uuid(7), state: 'pending' | 'answered' = 'pending'): RequestDeliveryQuestionResult => ({
  bindingId: uuid(9), requestId: uuid(6), state: state === 'pending' ? 'waiting_for_reply' : 'running', deliveryAssociation: association(questionId),
  question: { id: questionId, revision: 1, runId: uuid(10), threadId: uuid(4), agent: { id: uuid(3), name: 'Dexter' },
    state, runState: state === 'pending' ? 'waiting_for_reply' : 'running', question: 'Continue the comparison?', context: 'Research only.',
    proposedAnswer: 'Compare both stocks.', yesMeaning: 'Compare both.', noMeaning: 'Stop the comparison.', createdAt: at,
    expiresAt: '2026-09-24T10:04:00.000Z', reply: state === 'pending' ? null : { questionRevision: 1, clientReplyId: uuid(11),
      answer: 'yes', text: null, interpretedAnswer: 'Compare both.', receivedAt: at } },
});

test('canonical selector bootstrap crosses actual HTTP and derives execution only from exact API association', async t => {
  const received: Array<{ url: string; headers: Record<string, unknown>; body: unknown }> = [];
  let response: unknown = { association: association(), replayed: false };
  const server = createServer(async (request, res) => {
    const chunks: Buffer[] = []; for await (const chunk of request) chunks.push(Buffer.from(chunk));
    received.push({ url: request.url!, headers: { ...request.headers }, body: JSON.parse(Buffer.concat(chunks).toString()) });
    res.writeHead(200, { 'content-type': 'application/json' }); res.end(JSON.stringify(response));
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise<void>(resolve => { server.closeAllConnections(); server.close(() => resolve()); }));
  const client = createSelectedQuestionAssociationClient({ apiBaseUrl: `http://127.0.0.1:${(server.address() as { port: number }).port}`,
    phoneServiceSecret: 'x'.repeat(40) });
  const journal = await client.associate(seed);
  assert.deepEqual(journal, associated());
  assert.equal(received[0]!.url, `/internal/primary-dexter/channels/phone/calls/${seed.delivery.bindingId}/request-deliveries`);
  assert.deepEqual(received[0]!.body, seed.request);
  assert.equal(received[0]!.headers['x-dexter-binding-id'], seed.delivery.bindingId);
  assert.equal(received[0]!.headers['x-dexter-phone-context-version'], 'memory-eligible-v1');
  assert.equal(received[0]!.headers['x-dexter-phone-request-delivery-id'], undefined);
  assert.match(String(received[0]!.headers.authorization), /^Bearer [a-f0-9]{64}$/);
  for (const altered of [
    { ...association(), delivery: { ...seed.delivery, callSid: `CA${'b'.repeat(32)}` } },
    { ...association(), question: { id: uuid(12), revision: 1 } },
    { ...association(), execution: { ...association().execution, requestId: uuid(12) } },
  ]) {
    response = { association: altered, replayed: false };
    await assert.rejects(client.associate(seed), (error: unknown) => error instanceof PrimaryApiError && error.code === 'identity_mismatch');
  }
  response = { association: { ...association(), runId: uuid(13) }, replayed: false };
  await assert.rejects(client.associate(seed), (error: unknown) => error instanceof PrimaryApiError && error.code === 'invalid_response');
});

test('host persists the exact selected seed before association and full association before eligible speech', async () => {
  let saved: SelectedQuestionJournal | undefined; const order: string[] = [];
  const host = createSelectedQuestionHost({ seed, load: () => saved, persist(value) { saved = structuredClone(value); order.push('persist'); },
    active: () => true, now: () => Date.parse(at), requests: {} as RequestDeliveryClient,
    association: { async associate(value) { assert.deepEqual(saved!.questions[0]!.seed, value); order.push('associate'); return associated(value); } },
    async capture() { throw new Error('Selection cannot capture an answer'); }, replyIsQualified: () => false, admittedReplyIsValid: () => false,
    async deliver(journal, target) { assert.deepEqual(saved!.questions[0]!.record!.base, journal);
      assert.deepEqual(target, { kind: 'question', questionId: seed.request.questionId, questionRevision: 1 }); order.push('deliver'); } });
  await host.start(); assert.deepEqual(order, ['persist', 'associate', 'persist', 'deliver']);
  assert.deepEqual(parseSelectedQuestionJournal(saved), host.snapshot());
});

test('lost association response retains exact seed and restart recovers without a new call or run', async () => {
  let saved: SelectedQuestionJournal | undefined; let attempts = 0;
  const options = { seed, load: () => saved, persist(value: SelectedQuestionJournal) { saved = structuredClone(value); },
    active: () => true, now: () => Date.parse(at), requests: {} as RequestDeliveryClient,
    association: { async associate(value: SelectedQuestionSeed) { attempts++; assert.deepEqual(value, seed);
      if (attempts === 1) throw new PrimaryApiError('request_timeout'); return associated(value); } },
    async capture(): Promise<never> { throw new Error('No answer'); }, replyIsQualified: () => false, admittedReplyIsValid: () => false,
    async deliver() {} };
  await assert.rejects(createSelectedQuestionHost(options).start());
  assert.equal(saved!.questions[0]!.record, undefined);
  const restarted = createSelectedQuestionHost(options); await restarted.start(); await restarted.start();
  assert.equal(attempts, 2); assert.deepEqual(saved!.initial, seed);
});

test('a later question keeps the original execution and gets a new association before speech', async () => {
  let saved: SelectedQuestionJournal | undefined;
  const selections: string[] = []; const deliveries: string[] = [];
  const requests = { async question() { return question(uuid(7), 'answered'); }, async result() {
    return { ...question(uuid(17)), deliveryAssociation: association(uuid(7)) }; } } as RequestDeliveryClient;
  const host = createSelectedQuestionHost({ seed, load: () => saved, persist(value) { saved = structuredClone(value); },
    active: () => true, now: () => Date.parse(at), requests,
    association: { async associate(value) { selections.push(value.request.questionId); return associated(value); } },
    async capture() { throw new Error('No implicit answer'); }, replyIsQualified: () => false, admittedReplyIsValid: () => false,
    async deliver(journal, target) { assert.equal(saved!.questions.at(-1)!.record!.base.association!.id, journal.association!.id);
      assert.equal(target.kind, 'question'); deliveries.push(journal.association!.question.id); } });
  await host.start(); const original = structuredClone(host.snapshot().questions[0]); await host.poll();
  assert.deepEqual(selections, [uuid(7), uuid(17)]); assert.deepEqual(deliveries, selections);
  assert.deepEqual(host.snapshot().questions[0], original);
  assert.deepEqual(host.snapshot().questions[1]!.record!.base.target.execution, original.record!.base.target.execution);
});

const answer = (n = 20): SelectedQuestionAnswer => ({ questionId: uuid(7),
  sourceSessionId: 'current_session', delegationId: `delegation_${n}`, presentationId: `presentation_${n}`,
  input: { source: { callSid: seed.delivery.callSid, sourceEventId: uuid(n), revision: 1,
    itemId: `explicit_answer_${n}`, receivedAt: at }, role: 'user', text: ' Yes, compare both.\t',
    reply: { questionRevision: 1, clientReplyId: uuid(n + 1), answer: 'yes', text: null } } });
const receipt = (turn: PrimaryTurn): PrimaryTurnReceipt => ({ bindingId: seed.delivery.bindingId,
  sourceEventId: turn.sourceEventId, revision: turn.revision, recordId: turn.sourceEventId,
  revisionRecordId: uuid(40), bodyDigest: turnBodyDigest(turn), replayed: false });
function deferred<T>() {
  let resolve!: (value: T) => void; let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject };
}
function answeringHost() {
  let saved: SelectedQuestionJournal | undefined, active = true, qualified = true, clock = Date.parse(at);
  const calls = { captures: [] as PrimaryTurn[], questions: 0, posts: [] as import('../src/broker/requestDelivery.js').RequestDeliveryJournal[], deliveries: 0 };
  const hooks: {
    persist?: (value: SelectedQuestionJournal) => void;
    capture?: (turn: PrimaryTurn) => Promise<PrimaryTurnReceipt>;
    question?: () => Promise<RequestDeliveryQuestionResult>;
    reply?: (value: import('../src/broker/requestDelivery.js').RequestDeliveryJournal) => Promise<RequestDeliveryQuestionResult>;
  } = {};
  const options: SelectedQuestionHostOptions = { seed, load: () => saved,
    persist(value) { saved = structuredClone(value); hooks.persist?.(saved); }, active: () => active,
    now: () => clock, association: { async associate(value) { return associated(value); } },
    replyIsQualified: () => qualified, admittedReplyIsValid: () => qualified,
    async capture(bindingId, turn) {
      assert.equal(bindingId, seed.delivery.bindingId);
      assert.equal(saved!.questions.at(-1)!.admissions.at(-1)!.answer.input.text, turn.text, 'raw admission exists before capture');
      assert.equal(saved!.questions.at(-1)!.admissions.at(-1)!.answer.input.source.sourceEventId, turn.sourceEventId);
      calls.captures.push(structuredClone(turn)); return hooks.capture ? hooks.capture(turn) : receipt(turn);
    },
    requests: {
      async associate() { throw new Error('Bootstrap uses its own strict client'); },
      async question() { calls.questions++; return hooks.question ? hooks.question() : question(); },
      async result() { return question(); },
      async reply(journal) {
        assert.equal(saved!.questions.at(-1)!.record!.attempts.at(-1)!.state, 'dispatching', 'dispatch marker precedes HTTP');
        assert.deepEqual(saved!.questions.at(-1)!.record!.attempts.at(-1)!.journal, journal);
        calls.posts.push(structuredClone(journal));
        if (hooks.reply) return hooks.reply(journal);
        const response = question(uuid(7), 'answered');
        return { ...response, replayed: false, question: { ...response.question,
          reply: { ...journal.reply!.input.reply, interpretedAnswer: 'Compare both.', receivedAt: at } } };
      },
    }, async deliver() { calls.deliveries++; },
  };
  return { options, hooks, calls, create: () => createSelectedQuestionHost(options),
    saved: () => structuredClone(saved!), setActive(value: boolean) { active = value; },
    setQualified(value: boolean) { qualified = value; }, setTime(value: string) { clock = Date.parse(value); } };
}

test('explicit answer is durable before capture, then replies under the original execution with an acknowledged new-call source', async () => {
  const h = answeringHost(), host = h.create(); await host.start(); const input = answer();
  const result = await host.answer(input);
  assert.equal(result.attempts[0]!.state, 'accepted'); assert.equal(h.calls.posts.length, 1);
  assert.equal(h.calls.captures.length, 1); assert.equal(h.calls.posts[0]!.target.execution.runId, uuid(10));
  assert.equal(h.calls.posts[0]!.target.execution.bindingId, uuid(9));
  assert.equal(h.calls.posts[0]!.callerSource!.bindingId, seed.delivery.bindingId);
  assert.equal(h.calls.posts[0]!.reply!.input.text, input.input.text);
  assert.deepEqual(h.saved().questions[0]!.admissions[0]!.answer, input);
  assert.deepEqual(parseSelectedQuestionJournal(JSON.parse(JSON.stringify(h.saved()))), h.saved());
  await host.answer(input); assert.equal(h.calls.posts.length, 1); assert.equal(h.calls.captures.length, 1);
  await assert.rejects(host.answer({ ...input, input: { ...input.input, text: input.input.text.trim() } }),
    (error: unknown) => error instanceof PrimaryApiError && error.code === 'identity_mismatch');
});

test('lost capture acknowledgement restarts with exact raw source before the first reply dispatch', async () => {
  const h = answeringHost(), first = h.create(); await first.start(); const input = answer();
  h.hooks.capture = async () => { throw new PrimaryApiError('request_timeout'); };
  await assert.rejects(first.answer(input));
  assert.equal(h.saved().questions[0]!.admissions[0]!.captured, undefined);
  assert.deepEqual(h.saved().questions[0]!.admissions[0]!.answer, input);
  assert.equal(h.calls.posts.length, 0); assert.equal(h.calls.questions, 0);
  h.hooks.capture = async turn => ({ ...receipt(turn), replayed: true });
  const restarted = h.create(); await restarted.recoverReply();
  assert.equal(h.calls.posts.length, 1); assert.equal(h.calls.captures.length, 2);
  assert.deepEqual(h.calls.captures[0], h.calls.captures[1]);
  assert.equal(restarted.snapshot().questions[0]!.record!.attempts[0]!.dispatches, 1);
});

test('failed pre-dispatch question read keeps captured answer and restart performs its first exact dispatch', async () => {
  const h = answeringHost(), host = h.create(); await host.start();
  h.hooks.question = async () => { throw new PrimaryApiError('request_timeout'); };
  await assert.rejects(host.answer(answer()));
  assert.ok(h.saved().questions[0]!.admissions[0]!.captured);
  assert.equal(h.saved().questions[0]!.record!.attempts.length, 0); assert.equal(h.calls.posts.length, 0);
  h.hooks.question = undefined; await h.create().recoverReply();
  assert.equal(h.calls.captures.length, 1); assert.equal(h.calls.posts.length, 1);
  assert.equal(h.saved().questions[0]!.record!.attempts[0]!.recovery, false);
});

test('crash after prepared persistence preserves the exact first-dispatch identity', async () => {
  const h = answeringHost(), host = h.create(); await host.start();
  h.hooks.persist = value => { if (value.questions[0]!.record?.attempts[0]?.state === 'prepared') throw new Error('simulated process exit'); };
  await assert.rejects(host.answer(answer()));
  const before = h.saved().questions[0]!.record!.attempts[0]!.journal;
  assert.equal(h.calls.posts.length, 0); assert.equal(h.saved().questions[0]!.record!.attempts[0]!.dispatches, 0);
  h.hooks.persist = undefined; await h.create().recoverReply();
  assert.deepEqual(h.calls.posts[0], before); assert.equal(h.calls.posts.length, 1);
  assert.equal(h.saved().questions[0]!.record!.attempts[0]!.recovery, false);
});

test('stop bypasses a held capture and preserves its acknowledgement without sending the answer', async () => {
  const h = answeringHost(), host = h.create(); await host.start();
  const entered = deferred<PrimaryTurn>(), held = deferred<PrimaryTurnReceipt>();
  h.hooks.capture = async turn => { entered.resolve(turn); return held.promise; };
  const work = host.answer(answer()); const turn = await entered.promise;
  const stopped = host.stop();
  assert.equal(stopped.stoppedAt, at); assert.equal(stopped.questions[0]!.admissions[0]!.localRefusal!.code, 'call_ended');
  assert.equal(h.calls.posts.length, 0);
  held.resolve(receipt(turn)); await work;
  assert.ok(host.snapshot().questions[0]!.admissions[0]!.captured); assert.equal(h.calls.questions, 0);
  await h.create().recoverReply(); assert.equal(h.calls.posts.length, 0);
});

test('ended call settles a prepared answer at zero dispatch without discarding its transcript receipt', async () => {
  const h = answeringHost(), host = h.create(); await host.start();
  h.hooks.persist = value => { if (value.questions[0]!.record?.attempts[0]?.state === 'prepared') throw new Error('simulated process exit'); };
  await assert.rejects(host.answer(answer())); h.hooks.persist = undefined; h.setActive(false);
  const restarted = h.create(); await restarted.recoverReply();
  const attempt = h.saved().questions[0]!.record!.attempts[0]!;
  assert.equal(attempt.state, 'refused'); assert.equal(attempt.localRefusal!.code, 'call_ended');
  assert.equal(attempt.dispatches, 0); assert.equal(attempt.refusal, undefined); assert.equal(h.calls.posts.length, 0);
  assert.ok(h.saved().questions[0]!.admissions[0]!.captured);
});

test('late invalidation while question read is held refuses unsent answer and requires a fresh explicit source', async () => {
  const h = answeringHost(), host = h.create(); await host.start();
  const entered = deferred<void>(), held = deferred<RequestDeliveryQuestionResult>();
  h.hooks.question = async () => { entered.resolve(); return held.promise; };
  const work = host.answer(answer()); await entered.promise; h.setQualified(false); held.resolve(question()); await work;
  assert.equal(h.calls.posts.length, 0);
  assert.equal(h.saved().questions[0]!.admissions[0]!.localRefusal!.code, 'presentation_invalidated');
  h.setQualified(true); h.hooks.question = undefined;
  await host.answer(answer()); assert.equal(h.calls.posts.length, 0, 'restoring a presentation does not revive its refused old answer');
  await host.answer(answer(30)); assert.equal(h.calls.posts.length, 1);
  assert.equal(h.saved().questions[0]!.admissions.length, 2);
});

test('uncertain POST remains bound to the first answer through stop and read-only recovery', async () => {
  const h = answeringHost(), host = h.create(); await host.start();
  h.hooks.reply = async () => { throw new PrimaryApiError('request_timeout'); };
  await host.answer(answer()); const sent = h.calls.posts[0]!;
  assert.equal(h.saved().questions[0]!.record!.attempts[0]!.state, 'unknown');
  await assert.rejects(host.answer(answer(30)), /selected_question_reply_pending/);
  host.stop(); await h.create().recoverReply();
  assert.equal(h.calls.posts.length, 1); assert.equal(h.saved().questions[0]!.record!.attempts[0]!.state, 'unknown');
  assert.equal(h.saved().questions[0]!.admissions[0]!.localRefusal, undefined);
  h.hooks.question = async () => ({ ...question(uuid(7), 'answered'), question: { ...question(uuid(7), 'answered').question,
    reply: { ...sent.reply!.input.reply, interpretedAnswer: 'Compare both.', receivedAt: at } } });
  await h.create().recoverReply(); assert.equal(h.saved().questions[0]!.record!.attempts[0]!.state, 'accepted');
  assert.equal(h.calls.posts.length, 1);
});

test('stop while a reply POST is in flight leaves its marker uncertain until the exact reply returns', async () => {
  const h = answeringHost(), host = h.create(); await host.start();
  const entered = deferred<void>(), held = deferred<RequestDeliveryQuestionResult>();
  h.hooks.reply = async () => { entered.resolve(); return held.promise; };
  const work = host.answer(answer()); await entered.promise; host.stop();
  assert.equal(h.saved().questions[0]!.record!.attempts[0]!.state, 'dispatching');
  assert.equal(h.saved().questions[0]!.admissions[0]!.localRefusal, undefined);
  const response = question(uuid(7), 'answered');
  held.resolve({ ...response, question: { ...response.question,
    reply: { ...answer().input.reply, interpretedAnswer: 'Compare both.', receivedAt: at } } });
  await work; assert.equal(h.saved().questions[0]!.record!.attempts[0]!.state, 'accepted');
  assert.equal(h.calls.posts.length, 1);
});

test('restart validation refuses dropped raw source, changed proof, rewritten receipt and an invented attempt', async () => {
  const h = answeringHost(), host = h.create(); await host.start(); await host.answer(answer());
  const before = h.saved();
  for (const alter of [
    (value: SelectedQuestionJournal) => { value.questions[0]!.admissions = []; },
    (value: SelectedQuestionJournal) => { value.questions[0]!.admissions[0]!.answer.presentationId = 'other'; },
    (value: SelectedQuestionJournal) => { value.questions[0]!.admissions[0]!.answer.input.text = 'No.'; },
    (value: SelectedQuestionJournal) => { value.questions[0]!.admissions[0]!.captured!.callerSource!.bodyDigest = 'f'.repeat(64); },
    (value: SelectedQuestionJournal) => { value.questions[0]!.admissions[0]!.attemptSequence = 2; },
    (value: SelectedQuestionJournal) => { value.questions[0]!.admissions[0]!.localRefusal = { kind: 'never_dispatched', code: 'call_ended', observedAt: at }; },
  ]) { const changed = structuredClone(before); alter(changed); assert.throws(() => assertSelectedQuestionJournalUpdate(before, changed)); }
});


test('selection and unqualified small talk cannot capture a source or dispatch an answer', async () => {
  const h = answeringHost(), host = h.create(); h.setQualified(false);
  await assert.rejects(host.answer(answer()), /selected_question_reply_unqualified/);
  await host.start(); await assert.rejects(host.answer(answer()), /selected_question_reply_unqualified/);
  assert.equal(h.calls.posts.length, 0); assert.equal(h.calls.captures.length, 0);
  assert.equal(h.saved().questions[0]!.admissions.length, 0);
});

test('restart preserves the original deadline and cannot first-dispatch an answer after expiry', async () => {
  const h = answeringHost(), host = h.create(); await host.start();
  h.hooks.question = async () => { throw new PrimaryApiError('request_timeout'); };
  await assert.rejects(host.answer(answer()));
  h.hooks.question = undefined; h.setTime(seed.callDeadlineAt);
  const restarted = h.create(); await restarted.recoverReply();
  assert.equal(h.calls.posts.length, 0); assert.equal(h.saved().initial.callDeadlineAt, seed.callDeadlineAt);
  assert.equal(h.saved().questions[0]!.admissions[0]!.localRefusal!.code, 'call_ended');
  await assert.rejects(restarted.start(), /selected_question_call_inactive/);
  const changed = h.saved(); changed.initial.callDeadlineAt = '2026-09-24T11:00:00.000Z';
  changed.questions[0]!.seed.callDeadlineAt = changed.initial.callDeadlineAt;
  assert.throws(() => assertSelectedQuestionJournalUpdate(h.saved(), changed));
});
