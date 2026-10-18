import test from 'node:test';
import assert from 'node:assert/strict';
import { callTasks, createVoiceTasks, taskLine, taskListLines, type VoiceTask } from '../src/broker/voiceTasks.js';
import { LIVE_APPEND_MAX_BYTES } from '../src/live/session.js';
import type { BrokerCall } from '../src/broker/store.js';
import type { BrokerLiveSession, LiveDelivery } from '../src/live/types.js';

const wait = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

/** Only the fields the task list reads; the rest of a delegation record is irrelevant here. */
function entry(requestId: string, status: string, extra: Record<string, unknown> = {}): Record<string, unknown> & { requestId: string } {
  return { requestId, sessionId: 'session_a', delegationIds: [`item_${requestId}`], status,
    submission: { text: `Request ${requestId} text.`, source: { sourceEventId: `source_${requestId}` } }, ...extra };
}
function call(...entries: Array<Record<string, unknown> & { requestId: string }>): BrokerCall {
  return { id: 'call_1', delegations: Object.fromEntries(entries.map(value => [value.requestId, value])) } as unknown as BrokerCall;
}
function voice(status: LiveDelivery['status'] = 'accepted') {
  const sent: string[] = [];
  const session = { async appendContext(text: string) { sent.push(text); return { eventId: `e${sent.length}`, status }; } };
  return { sent, session: session as unknown as BrokerLiveSession };
}

test('the task list projects each saved request to one status the voice can state', () => {
  const tasks = callTasks(call(
    entry('a', 'running', { primaryResult: { partialResult: { kind: 'prepared_action', text: 'The sale was being placed.' } } }),
    entry('b', 'failed', { primaryResult: { error: { code: 'provider_busy' } } }),
    entry('c', 'cancelled'),
    entry('d', 'not_admitted'),
    entry('e', 'dispatch_unknown'),
    entry('f', 'running', { deferredTurns: [{ delegationId: 'item_g', sourceEventId: 'source_g', text: 'Then buy it back.', status: 'queued',
      queuedRequest: { requestId: 'g' } }] }),
  ));
  assert.deepEqual(tasks.map(task => [task.requestId, task.status]), [['a', 'pending'], ['b', 'failed'], ['c', 'cancelled'],
    ['d', 'not_started'], ['e', 'pending'], ['f', 'pending'], ['g', 'waiting_to_start']]);
  assert.equal(tasks[0]!.progress, 'The sale was being placed.');
  assert.equal(tasks[0]!.outcome, undefined, 'progress is never an outcome');
  assert.equal(tasks[1]!.outcome, 'The AI service is busy and could not answer this request.');
  assert.equal(tasks[4]!.progress, 'Not yet confirmed that the request was received.');
  assert.equal(tasks[6]!.request, 'Then buy it back.');
});

test('a completed result enters the voice list only once delivered; the truth view has it at once', () => {
  const completed = entry('a', 'completed', { primaryResult: { assistant: { text: 'The sale failed. Nothing was sold. Holdings are unchanged.' },
    partialResult: { kind: 'prepared_action', text: 'The sale was being placed.' } } });
  const voiceView = callTasks(call(completed));
  assert.equal(voiceView[0]!.status, 'pending', 'undelivered: the voice still has no outcome to state');
  assert.equal(voiceView[0]!.outcome, undefined);
  assert.equal(voiceView[0]!.progress, 'The sale was being placed.', 'same as before completion, so no new list is sent');
  const truth = callTasks(call(completed), { truth: true });
  assert.deepEqual([truth[0]!.status, truth[0]!.outcome], ['finished', 'The sale failed. Nothing was sold. Holdings are unchanged.']);
  const forCorrection = callTasks(call(completed), { arriving: true });
  assert.deepEqual([forCorrection[0]!.status, forCorrection[0]!.outcome], ['arriving', undefined],
    'a correction may say the result is on its way, never "in progress" and never its words');
  const delivered = callTasks(call({ ...completed, delivery: 'accepted' }));
  assert.deepEqual([delivered[0]!.status, delivered[0]!.outcome], ['finished', 'The sale failed. Nothing was sold. Holdings are unchanged.']);
});

test('every task line fits one append, outcome first, and hides caller words where the voice may not see them', () => {
  const long: VoiceTask = { requestId: 'a', sessionId: 's', sourceEventId: 'x', status: 'finished', request: 'Sell '.repeat(40),
    outcome: 'You sold about $2.00 of Amazon. '.repeat(20) };
  const line = taskLine(long, 0, 3);
  assert.ok(Buffer.byteLength(line) <= LIVE_APPEND_MAX_BYTES);
  assert.match(line, /^Task list 3, task 1: Finished\. Caller asked: "Sell/);
  assert.match(taskLine(long, 1, 3, false), /^Task list 3, task 2: Finished\. An earlier request\. Result: You sold/);
  const header = taskListLines([long, long, long, long, long, long, long], 4);
  assert.equal(header.length, 7, 'header plus the six most recent tasks');
  assert.match(header[0]!, /^Task list 4 replaces all earlier task lists\. The caller has 7 requests on this call; the 6 most recent follow\. Nothing else has been started\.$/);
});

test('a delegation is in the truth from the moment the voice makes it; the voice hears of it once it is saved', async () => {
  let saved = call();
  const tasks = createVoiceTasks(() => saved);
  const target = voice();
  tasks.attach(target.session);
  assert.deepEqual(target.sent, [], 'no list before the first request: nothing has been started');
  tasks.starting({ sessionId: 'session_a', delegationId: 'item_a', requestId: 'a' });
  tasks.refresh(); await wait(250);
  assert.deepEqual(target.sent, [], 'the voice knows what it just delegated; no list is sent for that moment');
  assert.deepEqual(tasks.tasks(true).map(task => task.status), ['starting'], 'the guardrail judges "On it." against it');
  assert.deepEqual(tasks.tasks(), []);
  saved = call(entry('a', 'running'));
  tasks.refresh(); await wait(250);
  assert.deepEqual(target.sent, ['Task list 1 replaces all earlier task lists. The caller has 1 request on this call. Nothing else has been started.',
    'Task list 1, task 1: In progress, no result yet. Caller asked: "Request a text.".']);
  assert.deepEqual(tasks.tasks(true).map(task => task.status), ['pending'], 'the saved request replaces the delegation');
  tasks.settled({ delegationId: 'item_a' }); tasks.refresh(); await wait(250);
  assert.equal(target.sent.length, 2, 'an unchanged list is not sent again');
  tasks.stop();
});

test('a list the voice did not accept is sent again; a replacement voice sees only what it may', async () => {
  const saved = call(entry('a', 'running'));
  const tasks = createVoiceTasks(() => saved);
  const lost = voice('unconfirmed');
  tasks.attach(lost.session); await wait(20);
  assert.equal(lost.sent.length, 2);
  tasks.refresh(); await wait(250);
  assert.equal(lost.sent.length, 4, 'the unchanged list goes again because the first was not confirmed');
  const next = voice();
  tasks.attach(next.session, () => false); await wait(20);
  assert.equal(next.sent.length, 2);
  assert.match(next.sent[1]!, /task 1: In progress, no result yet\. An earlier request\.$/, 'another session sees no caller words it was not given');
  tasks.stop();
});
