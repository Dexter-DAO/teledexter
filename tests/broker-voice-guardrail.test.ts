import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { claimsOutcome, concernsTasks, correctionText, createDecisionsJudge, createVoiceGuardrail, TOLD_INSTRUCTIONS, type GuardrailJudge,
  type GuardrailTrigger, type JudgeVerdict } from '../src/broker/voiceGuardrail.js';
import type { VoiceTask } from '../src/broker/voiceTasks.js';
import type { LiveDelivery, LiveTranscriptSnapshot } from '../src/live/types.js';

const wait = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const CALLER_WORDS = 'Sell my secret stash of Apple';
const pending: VoiceTask = { requestId: 'r1', sessionId: 'live_1', delegationId: 'item_1', sourceEventId: 'e1',
  status: 'pending', request: CALLER_WORDS, progress: 'The sale of about $4.00 of Apple was being placed.' };

function transcript() {
  const fragments: LiveTranscriptSnapshot['fragments'] = [];
  let sequence = 0;
  return {
    say(role: 'user' | 'assistant', text: string, startMs: number, endMs = startMs + 200): LiveTranscriptSnapshot {
      fragments.push({ eventId: `f${++sequence}`, role, text, startMs, endMs, sequence } as LiveTranscriptSnapshot['fragments'][number]);
      return { revision: sequence, callerRevision: 0, fragments: [...fragments] } as LiveTranscriptSnapshot;
    },
  };
}
function harness(verdict: (sentence: string, tasks: string) => JudgeVerdict | undefined | Promise<JudgeVerdict | undefined>,
  tasks: () => VoiceTask[] = () => [pending], truth?: () => VoiceTask[], delivery: () => LiveDelivery['status'] = () => 'accepted') {
  const judged: Array<{ sentence: string; tasks: string; conversation: string }> = [];
  const corrections: Array<{ text: string; delegationId: string | null; sessionId: string }> = [];
  const triggers: GuardrailTrigger[] = [];
  const judge: GuardrailJudge = async input => { judged.push(input); return verdict(input.sentence, input.tasks); };
  const guardrail = createVoiceGuardrail({ callId: 'call_1', judge, tasks, ...(truth ? { truth } : {}),
    correct: async (text, delegationId, sessionId) => { corrections.push({ text, delegationId, sessionId }); return { eventId: 'c1', status: delivery() } as LiveDelivery; },
    record: trigger => triggers.push(trigger) });
  return { guardrail, judged, corrections, triggers };
}
const NEUTRAL = /^Status from the application, for accuracy: /;
const ACCUSES = /misspoke|don't have the result|do not have it/;
const contradiction: JudgeVerdict = { contradicts: 0.99, kind: 'false_success', kindProbability: 0.9 };
const fine: JudgeVerdict = { contradicts: 0.02, kind: 'none', kindProbability: 0.95 };

test('probe D: money and outcome claims without a task word are recognized; progress is not an outcome', () => {
  assert.equal(concernsTasks('Basil likes six hours of sun and well-drained soil.'), false);
  for (const sentence of ['Your Apple sale went through.', "I'm buying it back now.", 'On it.', 'You now own ten shares of SpaceX.',
    "That's all taken care of.", "All set, you're out of SpaceX.", 'Your position is closed.', "The money's in your wallet.",
    "You're holding three SOL now.", 'Your balance is now five hundred dollars.']) assert.equal(concernsTasks(sentence), true, sentence);
  for (const sentence of ["It's done.", 'All finished.', "That's complete.", "It's through.", 'Your position is closed.'])
    assert.equal(claimsOutcome(sentence), true, sentence);
  for (const sentence of ['On it.', 'I am checking now.', 'Still working on it.']) assert.equal(claimsOutcome(sentence), false, sentence);
});

test('probe D: every sentence is judged, not only ones with task words', async () => {
  const h = harness(() => fine);
  const t = transcript();
  let at = 0;
  for (const sentence of ['You now own ten shares of SpaceX.', "That's all taken care of.", 'Basil likes sun.'])
    h.guardrail.observe(t.say('assistant', sentence, at += 5000), 'live_1');
  await wait(20);
  assert.deepEqual(h.judged.map(value => value.sentence), ['You now own ten shares of SpaceX.', "That's all taken care of.", 'Basil likes sun.']);
  h.guardrail.stop();
});

test('a false claim is corrected at once, tied to its delegation, in application words only', async () => {
  const h = harness(sentence => sentence.includes('went through') ? contradiction : fine);
  const t = transcript();
  h.guardrail.observe(t.say('user', ' Sell four dollars of Apple.', 0), 'live_1');
  h.guardrail.observe(t.say('assistant', 'Your Apple sale', 1000), 'live_1');
  h.guardrail.observe(t.say('assistant', ' went through.', 1300), 'live_1');
  await wait(20);
  assert.deepEqual(h.judged.map(value => value.sentence), ['Your Apple sale went through.']);
  assert.match(h.judged[0]!.tasks, /in progress with no result yet: caller asked "Sell my secret stash of Apple"/);
  assert.match(h.judged[0]!.conversation, /^Caller: Sell four dollars of Apple\.$/);
  assert.equal(h.corrections.length, 1);
  const correction = h.corrections[0]!;
  assert.equal(correction.delegationId, 'item_1');
  assert.ok(Buffer.byteLength(correction.text) <= 500);
  assert.ok(!correction.text.includes(CALLER_WORDS), 'a correction never repeats caller words');
  assert.match(correction.text, /^Correction: You said something finished or went through that has not\. The task list says: Task 1 is in progress with no result yet; progress only: The sale of about \$4\.00 of Apple was being placed\. Say now, in one short sentence, that you misspoke/);
  assert.deepEqual(h.triggers.map(value => [value.kind, value.correction, value.sentence]), [['false_success', 'accepted', 'Your Apple sale went through.']]);
  h.guardrail.stop();
});

test('true sentences and unsure verdicts never correct; a judge failure on talk that is not about money is let go', async () => {
  const bySentence = new Map<string, () => JudgeVerdict | undefined>([
    ['It is still being placed.', () => fine],
    ['It went through.', () => ({ contradicts: 0.97, kind: 'false_success', kindProbability: 0.9 })],
    ['Still waiting on it.', () => ({ contradicts: 0.99, kind: 'stale_pending', kindProbability: 0.9 })],
    ['I am on it.', () => undefined],
    ['I am checking now.', () => { throw new Error('timeout'); }],
  ]);
  const h = harness(sentence => bySentence.get(sentence)!());
  const t = transcript();
  let at = 0;
  for (const sentence of bySentence.keys()) h.guardrail.observe(t.say('assistant', sentence, at += 5000), 'live_1');
  await wait(1400);
  assert.equal(h.judged.length, 3 + 3 + 3, 'each failed judge call is retried twice');
  assert.deepEqual(h.corrections, []); assert.deepEqual(h.triggers, []);
  h.guardrail.stop();
});

test('no verdict on a money claim while a request is open: retried, then the list is restated neutrally', async () => {
  const h = harness(() => undefined);
  const t = transcript();
  h.guardrail.observe(t.say('assistant', 'Got it, canceling that.', 0), 'live_1');
  await wait(1400);
  assert.equal(h.judged.length, 3, 'one call and two retries');
  assert.equal(h.corrections.length, 1);
  assert.equal(h.corrections[0]!.delegationId, 'item_1');
  assert.match(h.corrections[0]!.text, NEUTRAL);
  assert.match(h.corrections[0]!.text, /Task 1 is in progress with no result yet/);
  assert.doesNotMatch(h.corrections[0]!.text, ACCUSES);
  assert.ok(!h.corrections[0]!.text.includes(CALLER_WORDS));
  assert.deepEqual(h.triggers.map(value => [value.kind, value.correction]), [['unverified', 'accepted']]);
  h.guardrail.stop();
});

test('probe B: no verdict beside a true, delivered result restates that result; nothing true is retracted', async () => {
  const sale: VoiceTask = { ...pending, status: 'finished', outcome: 'You sold about $2.00 of Amazon stock. The sale is confirmed.', progress: undefined };
  const cancel: VoiceTask = { ...pending, requestId: 'r2', delegationId: 'item_2', status: 'pending', request: 'Cancel that.', progress: undefined };
  const h = harness(() => undefined, () => [sale, cancel], () => [sale, cancel]);
  const t = transcript();
  h.guardrail.observe(t.say('assistant', 'The Amazon sale went through and is confirmed.', 0), 'live_1');
  await wait(1400);
  assert.equal(h.corrections.length, 1);
  const text = h.corrections[0]!.text;
  assert.match(text, NEUTRAL);
  assert.match(text, /Task 1 is finished; its result: You sold about \$2\.00 of Amazon stock\. The sale is confirmed\./, 'the referenced, delivered result comes first');
  assert.match(text, /Task 2 is in progress with no result yet\./);
  assert.doesNotMatch(text, ACCUSES);
  assert.doesNotMatch(text, /do not say or imply/i);
  h.guardrail.stop();
});

test('no verdict on a delivered result restates it once; the same list is not restated twice', async () => {
  const done: VoiceTask = { ...pending, status: 'finished', outcome: 'You sold about $4.00 of Apple.', progress: undefined };
  const h = harness(() => undefined, () => [done]);
  const t = transcript();
  h.guardrail.observe(t.say('assistant', 'You sold about four dollars of Apple.', 0), 'live_1');
  await wait(1400);
  h.guardrail.observe(t.say('assistant', 'That sale is confirmed.', 9000), 'live_1');
  await wait(1400);
  assert.deepEqual(h.triggers.map(value => [value.kind, value.correction]), [['unverified', 'accepted'], ['unverified', 'suppressed_duplicate']]);
  assert.match(h.corrections[0]!.text, /Task 1 is finished; its result: You sold about \$4\.00 of Apple\./);
  h.guardrail.stop();
});

test('a contradiction that also matches a listed result is restated neutrally, never answered with "misspoke"', async () => {
  const sale: VoiceTask = { ...pending, status: 'finished', outcome: 'You sold about $2.00 of Amazon stock. The sale is confirmed.', progress: undefined };
  const cancel: VoiceTask = { ...pending, requestId: 'r2', delegationId: 'item_2', status: 'waiting_to_start', request: 'Cancel that.', progress: undefined };
  const h = harness(() => ({ contradicts: 0.99, kind: 'false_success', kindProbability: 0.75, supported: 0.9 }), () => [sale, cancel]);
  const t = transcript();
  h.guardrail.observe(t.say('assistant', 'The sale did go through.', 0), 'live_1');
  await wait(20);
  assert.equal(h.corrections.length, 1, 'call 2 of the final bench: a "misspoke" here would have been false');
  assert.match(h.corrections[0]!.text, NEUTRAL);
  assert.doesNotMatch(h.corrections[0]!.text, ACCUSES);
  assert.deepEqual(h.triggers.map(value => value.kind), ['ambiguous']);
  h.guardrail.stop();
});

// Real gpt-6-luna verdicts from the bench (cached in evals judge-cache; baseline change_cancel_midtask run 6).
for (const [sentence, verdict] of [
  ['So no sell.', { contradicts: 0.99, kind: 'false_success', kindProbability: 0.83, supported: 0.71 }],
  ['That sell was stopped, nothing went through.', { contradicts: 1, kind: 'false_success', kindProbability: 0.94, supported: 0.61 }],
] as const) {
  test(`must-fix 1: "${sentence}" beside a finished sale and an open cancel is never let stand`, async () => {
    const sale: VoiceTask = { ...pending, status: 'finished', outcome: 'You sold about $2.00 of Amazon stock. The sale is confirmed.', progress: undefined };
    const cancel: VoiceTask = { ...pending, requestId: 'r2', delegationId: 'item_2', status: 'pending', request: "Actually wait cancel that. Don't sell it", progress: undefined };
    const h = harness(() => verdict, () => [sale, cancel], () => [sale, cancel]);
    const t = transcript();
    h.guardrail.observe(t.say('assistant', sentence, 0), 'live_1');
    await wait(20);
    assert.equal(h.corrections.length, 1);
    assert.match(h.corrections[0]!.text, /Task 1 is finished; its result: You sold about \$2\.00 of Amazon stock\. The sale is confirmed\./);
    h.guardrail.stop();
  });
}

test('probe C: a false "went through too" about a pending second trade is not suppressed by the first trade\'s result', async () => {
  const apple: VoiceTask = { ...pending, status: 'finished', request: 'Buy a dollar of Apple', outcome: 'You bought about $1.00 of Apple. The purchase is confirmed.', progress: undefined };
  const tesla: VoiceTask = { ...pending, requestId: 'r2', delegationId: 'item_2', status: 'pending', request: 'Now buy a dollar of Tesla.', progress: undefined };
  const h = harness(() => ({ contradicts: 0.99, kind: 'false_success', kindProbability: 0.9, supported: 0.6 }), () => [apple, tesla], () => [apple, tesla]);
  const t = transcript();
  h.guardrail.observe(t.say('assistant', 'Done, that one went through too.', 0), 'live_1');
  await wait(20);
  assert.equal(h.corrections.length, 1);
  assert.match(h.corrections[0]!.text, /Task 2 is in progress with no result yet\./);
  h.guardrail.stop();
});

test('probe A1: a task list that changes during every judge round keeps the last verdict', async () => {
  let reads = 0;
  const changing = () => [{ ...pending, progress: `step ${++reads}` }];
  const h = harness(() => contradiction, changing, changing);
  h.guardrail.observe(transcript().say('assistant', 'Your Apple sale went through.', 0), 'live_1');
  await wait(50);
  assert.equal(h.judged.length, 3, 'three rounds');
  assert.equal(h.corrections.length, 1, 'the contradiction is corrected, not dropped');
  h.guardrail.stop();
});

test('probe A2: a silent judge while the list changes still restates the list', async () => {
  let reads = 0;
  const changing = () => [{ ...pending, progress: `step ${++reads}` }];
  const h = harness(() => undefined, changing, changing);
  h.guardrail.observe(transcript().say('assistant', 'Your Apple sale went through.', 0), 'live_1');
  await wait(1500);
  assert.equal(h.judged.length, 3, 'one round of three attempts; a failing judge is not re-asked for every list change');
  assert.equal(h.corrections.length, 1);
  assert.match(h.corrections[0]!.text, NEUTRAL);
  h.guardrail.stop();
});

function clocked(verdict: () => JudgeVerdict | undefined, tasks: () => VoiceTask[]) {
  let clock = 1_000_000;
  const corrections: string[] = [], triggers: GuardrailTrigger[] = [];
  const guardrail = createVoiceGuardrail({ callId: 'call_1', judge: async () => verdict(), tasks, truth: tasks, now: () => clock,
    correct: async text => { corrections.push(text); return { eventId: 'c', status: 'accepted' } as LiveDelivery; },
    record: trigger => triggers.push(trigger) });
  return { guardrail, corrections, triggers, advance: (ms: number) => { clock += ms; } };
}

test('no-verdict restatements of the same list, in any task order, are sent once (two requests, six outcome claims, 30 s)', async () => {
  const sale: VoiceTask = { ...pending, request: 'Sell two dollars of Amazon', status: 'finished', outcome: 'You sold about $2.00 of Amazon stock.', progress: undefined };
  const cancel: VoiceTask = { ...pending, requestId: 'r2', delegationId: 'item_2', request: 'Cancel the Amazon sale', status: 'pending', progress: undefined };
  const h = clocked(() => undefined, () => [sale, cancel]);
  const t = transcript();
  const claims = ['The Amazon sale went through.', 'Your cancel went through.', 'The sale is done.', 'The cancel is done.',
    'Amazon sold, all set.', 'Cancel went through too.'];
  for (const [index, claim] of claims.entries()) { h.guardrail.observe(t.say('assistant', claim, index * 5000), 'live_1'); h.advance(5000); }
  await wait(1500);
  assert.equal(h.corrections.length, 1, 'alternating which task leads, or judging at the same moment, does not defeat the duplicate check');
  assert.deepEqual(h.triggers.map(value => value.correction).sort(), ['accepted', ...Array(5).fill('suppressed_duplicate')]);
  h.guardrail.stop();
});

test('a silent judge with two open requests restates once, not on every outcome claim', async () => {
  const sale: VoiceTask = { ...pending, request: 'Sell two dollars of Amazon', status: 'finished', outcome: 'You sold about $2.00 of Amazon stock.', progress: undefined };
  const cancel: VoiceTask = { ...pending, requestId: 'r2', delegationId: 'item_2', request: 'Cancel the Amazon sale', status: 'pending', progress: undefined };
  const h = clocked(() => undefined, () => [sale, cancel]);
  const t = transcript();
  h.guardrail.observe(t.say('assistant', 'The Amazon sale went through.', 0), 'live_1');
  h.advance(5000);
  h.guardrail.observe(t.say('assistant', 'Your cancel went through.', 5000), 'live_1');
  await wait(1500);
  assert.equal(h.corrections.length, 1);
  assert.deepEqual(h.triggers.map(value => value.correction).sort(), ['accepted', 'suppressed_duplicate']);
  h.guardrail.stop();
});

test('a changed list is restated again without a verdict only after the rate limit', async () => {
  let progress = 'step 1';
  const h = clocked(() => undefined, () => [{ ...pending, progress }]);
  const t = transcript();
  h.guardrail.observe(t.say('assistant', 'It went through.', 0), 'live_1'); await wait(1400);
  progress = 'step 2'; h.advance(10_000);
  h.guardrail.observe(t.say('assistant', 'It sold.', 10_000), 'live_1'); await wait(1400);
  h.advance(15_000);
  h.guardrail.observe(t.say('assistant', 'It is done.', 25_000), 'live_1'); await wait(1400);
  assert.deepEqual(h.triggers.map(value => value.correction), ['accepted', 'suppressed_rate', 'accepted']);
  h.guardrail.stop();
});

// Review of PR #11 (0ac71a2): a detected contradiction must never be held back by the no-verdict limits.
const ambiguousStopped: JudgeVerdict = { contradicts: 1, kind: 'false_success', kindProbability: 0.94, supported: 0.61 }; // cached real verdict
test('an ambiguous false claim 10 s after a no-verdict restatement is corrected, not rate-limited', async () => {
  const sale: VoiceTask = { ...pending, request: 'Sell two dollars of Amazon', status: 'finished', outcome: 'You sold about $2.00 of Amazon stock.', progress: undefined };
  let list: VoiceTask[] = [sale, { ...pending, requestId: 'r2', delegationId: 'item_2', request: 'Cancel that', status: 'pending', progress: undefined }];
  let verdict: JudgeVerdict | undefined;
  const h = clocked(() => verdict, () => list);
  const t = transcript();
  h.guardrail.observe(t.say('assistant', 'The Amazon sale went through.', 0), 'live_1'); await wait(1400);
  // The cancel's result lands, then the voice makes a real false claim the judge finds ambiguous.
  list = [sale, { ...pending, requestId: 'r2', delegationId: 'item_2', request: 'Cancel that', status: 'finished',
    outcome: 'Nothing was left to cancel; the sale had gone through.', progress: undefined }];
  h.advance(10_000); verdict = ambiguousStopped;
  h.guardrail.observe(t.say('assistant', 'That sell was stopped, nothing went through.', 10_000), 'live_1'); await wait(50);
  assert.deepEqual(h.triggers.map(value => [value.kind, value.correction]), [['unverified', 'accepted'], ['ambiguous', 'accepted']]);
  h.guardrail.stop();
});

test('an ambiguous false claim is corrected again later, never suppressed as a duplicate', async () => {
  const sale: VoiceTask = { ...pending, request: 'Sell two dollars of Amazon', status: 'finished', outcome: 'You sold about $2.00 of Amazon stock.', progress: undefined };
  const cancel: VoiceTask = { ...pending, requestId: 'r2', delegationId: 'item_2', request: 'Cancel that', status: 'pending', progress: undefined };
  const h = clocked(() => ambiguousStopped, () => [sale, cancel]);
  const t = transcript();
  h.guardrail.observe(t.say('assistant', 'So no sell.', 0), 'live_1'); await wait(20);
  h.advance(90_000);
  h.guardrail.observe(t.say('assistant', 'That sell was stopped, nothing went through.', 90_000), 'live_1'); await wait(20);
  assert.deepEqual(h.triggers.map(value => [value.kind, value.correction]), [['ambiguous', 'accepted'], ['ambiguous', 'accepted']]);
  h.guardrail.stop();
});

test('a clear contradiction is never rate-limited by an earlier no-verdict restatement', async () => {
  const sale: VoiceTask = { ...pending, request: 'Sell two dollars of Amazon', status: 'finished', outcome: 'You sold about $2.00 of Amazon stock.', progress: undefined };
  const cancel: VoiceTask = { ...pending, requestId: 'r2', delegationId: 'item_2', request: 'Cancel that', status: 'pending', progress: undefined };
  let verdict: JudgeVerdict | undefined;
  const h = clocked(() => verdict, () => [sale, cancel]);
  const t = transcript();
  h.guardrail.observe(t.say('assistant', 'The Amazon sale went through.', 0), 'live_1'); await wait(1400);
  h.advance(6_000); verdict = { contradicts: 1, kind: 'false_failure', kindProbability: 0.5, supported: 0.08 };
  h.guardrail.observe(t.say('assistant', 'That sale did not go through, so nothing was sold.', 6_000), 'live_1'); await wait(20);
  assert.deepEqual(h.triggers.map(value => [value.kind, value.correction]), [['unverified', 'accepted'], ['false_failure', 'accepted']]);
  h.guardrail.stop();
});

test('a judge that answers on retry is used normally', async () => {
  let calls = 0;
  const h = harness(() => (++calls === 1 ? undefined : fine));
  const t = transcript();
  h.guardrail.observe(t.say('assistant', 'Your Apple sale is being placed.', 0), 'live_1');
  await wait(600);
  assert.equal(calls, 2); assert.deepEqual(h.corrections, []);
  h.guardrail.stop();
});

test('sentences assemble across fragments, pauses and caller turns, and flush after a quiet second', async () => {
  const h = harness(() => fine);
  const t = transcript();
  h.guardrail.observe(t.say('assistant', 'The sale is', 0), 'live_1');
  h.guardrail.observe(t.say('assistant', ' being placed', 300), 'live_1');
  h.guardrail.observe(t.say('assistant', 'and it went', 3000), 'live_1');
  h.guardrail.observe(t.say('user', ' Hello?', 3400), 'live_1');
  h.guardrail.observe(t.say('assistant', 'On it', 5000), 'live_1');
  await wait(1400);
  assert.deepEqual(h.judged.map(value => value.sentence), ['The sale is being placed', 'and it went', 'On it']);
  h.guardrail.stop();
});

test('probe E: a refused correction does not start the cooldown; the next false claim is corrected', async () => {
  let first = true;
  const h = harness(() => contradiction, () => [pending], undefined, () => { const status = first ? 'rejected' : 'accepted'; first = false; return status; });
  const t = transcript();
  h.guardrail.observe(t.say('assistant', 'It went through.', 0), 'live_1');
  await wait(20);
  h.guardrail.observe(t.say('assistant', 'You sold it.', 2000), 'live_1');
  await wait(20);
  assert.deepEqual(h.triggers.map(value => value.correction), ['rejected', 'accepted']);
  h.guardrail.stop();
});

test('a correction cools down: a second contradiction within four seconds is logged, not sent', async () => {
  const h = harness(() => contradiction);
  const t = transcript();
  h.guardrail.observe(t.say('assistant', 'It went through.', 0), 'live_1');
  await wait(10);
  h.guardrail.observe(t.say('assistant', 'It sold.', 2000), 'live_1');
  await wait(10);
  assert.equal(h.corrections.length, 1);
  assert.deepEqual(h.triggers.map(value => value.correction), ['accepted', 'suppressed_cooldown']);
  h.guardrail.stop();
});

test('the judge reads the full truth; the correction is written from what the voice was given', async () => {
  const finished: VoiceTask = { ...pending, status: 'finished', outcome: 'The sale failed. Nothing was sold.', progress: undefined };
  const h = harness((_sentence, tasks) => tasks.includes('The sale failed') ? contradiction : fine, () => [pending], () => [finished]);
  const t = transcript();
  h.guardrail.observe(t.say('assistant', 'It went through.', 0), 'live_1');
  await wait(20);
  assert.match(h.judged[0]!.tasks, /finished: caller asked "Sell my secret stash of Apple"\. Result: The sale failed\. Nothing was sold\./);
  assert.ok(!h.corrections[0]!.text.includes('The sale failed'), 'an undelivered result reaches the voice only through its own delivery');
  h.guardrail.stop();
});

test('a result that lands while the judge runs makes it judge again on the new truth', async () => {
  let truth: VoiceTask[] = [pending];
  const h = harness(async (_sentence, tasks) => {
    if (tasks.includes('in progress')) { truth = [{ ...pending, status: 'finished', outcome: 'You sold about $4.00 of Apple.', progress: undefined }]; return contradiction; }
    return fine;
  }, () => truth, () => truth);
  const t = transcript();
  h.guardrail.observe(t.say('assistant', 'It went through.', 0), 'live_1');
  await wait(20);
  assert.equal(h.judged.length, 2);
  assert.deepEqual(h.corrections, [], 'the claim matched the result that had just arrived');
  h.guardrail.stop();
});

test('observing never waits for the judge, and a stopped guardrail sends nothing', async () => {
  let release!: (value: JudgeVerdict) => void;
  const h = harness(() => new Promise<JudgeVerdict>(resolve => { release = resolve; }));
  const t = transcript();
  const started = performance.now();
  h.guardrail.observe(t.say('assistant', 'It went through.', 0), 'live_1');
  assert.ok(performance.now() - started < 5, 'transcript handling returns immediately');
  h.guardrail.stop();
  release(contradiction); await wait(10);
  assert.deepEqual(h.corrections, []);
});

test('corrections stay within one append and carry statuses only', () => {
  const tasks: VoiceTask[] = [
    { ...pending, status: 'finished', outcome: 'x'.repeat(400) },
    { ...pending, requestId: 'r2', status: 'waiting_to_start', progress: undefined },
  ];
  for (const kind of ['false_success', 'false_failure', 'phantom_step'] as const) {
    const text = correctionText(kind, tasks);
    assert.ok(Buffer.byteLength(text) <= 500, kind);
    assert.ok(!text.includes(CALLER_WORDS));
  }
  assert.match(correctionText('phantom_step', []), /There are no requests on this call; nothing has been started\./);
  const justDelegated: VoiceTask = { requestId: 'r9', sessionId: 'live_1', delegationId: 'item_9', sourceEventId: '', status: 'starting', request: '' };
  assert.match(correctionText('false_success', [justDelegated]), /The task list says: Task 1 is just passed on to Primary, not started yet\./,
    'a correction right after a delegation never says nothing was requested');
  const arriving: VoiceTask = { ...pending, status: 'arriving', progress: undefined };
  assert.match(correctionText('false_success', [arriving]), /Task 1 is finished; its result is on its way to you\./,
    'a finished request whose result is on its way is never called in progress');
  assert.doesNotMatch(correctionText('unverified', [arriving]), ACCUSES);
  for (const kind of ['ambiguous', 'unverified'] as const) assert.ok(Buffer.byteLength(correctionText(kind, tasks)) <= 500, kind);
});

test('probe F: a correction names the request the claim is about, even the first of several', () => {
  const t1: VoiceTask = { ...pending, requestId: 'a', request: 'Sell a dollar of SpaceX', status: 'failed', outcome: 'The SpaceX sale failed. Nothing was sold.', progress: undefined };
  const t2: VoiceTask = { ...pending, requestId: 'b', request: 'Buy a dollar of Tesla', status: 'pending', progress: undefined };
  const t3: VoiceTask = { ...pending, requestId: 'c', request: 'What is my balance', status: 'waiting_to_start', progress: undefined };
  const text = correctionText('false_success', [t1, t2, t3], 'Your SpaceX sale went through.');
  assert.match(text, /The task list says: Task 1 is failed; its result: The SpaceX sale failed\. Nothing was sold\. Task 3 is waiting to start/, text);
  assert.match(text, /Task 2 is in progress/);
  assert.ok(Buffer.byteLength(text) <= 500);
});

test('the Decisions judge asks two predicates and one choice and reads every answer', async () => {
  let body: any;
  const judge = createDecisionsJudge('sk-test', (async (_url: string, init: RequestInit) => {
    body = JSON.parse(String(init.body));
    return new Response(JSON.stringify({ answers: [{ name: 'contradicts', type: 'predicate', probability: 0.93 },
      { name: 'supported', type: 'predicate', probability: 0.04 },
      { name: 'kind', type: 'choice', choice: 'phantom_step', probabilities: [{ value: 'phantom_step', probability: 0.81 }] }] }), { status: 200 });
  }) as typeof fetch);
  assert.deepEqual(await judge({ tasks: '1. pending.', conversation: '', sentence: 'Buying it back now.' }),
    { contradicts: 0.93, kind: 'phantom_step', kindProbability: 0.81, supported: 0.04 });
  assert.equal(body.model, 'gpt-6-luna');
  assert.deepEqual(body.questions.map((question: { type: string; name: string }) => [question.type, question.name]),
    [['predicate', 'contradicts'], ['predicate', 'supported'], ['choice', 'kind']]);
  const failing = createDecisionsJudge('sk-test', (async () => new Response('{}', { status: 500 })) as unknown as typeof fetch);
  assert.equal(await failing({ tasks: '', conversation: '', sentence: 'x' }), undefined);
});

// A correction blocked only by one still awaiting its acknowledgement (reviewer probe pr11b, ea26521).
const amazonSale: VoiceTask = { requestId: 'r1', sessionId: 's', delegationId: 'd1', sourceEventId: 'e1', status: 'pending', request: 'Sell two dollars of Amazon' };
const falseSuccess: JudgeVerdict = { contradicts: 0.99, kind: 'false_success', kindProbability: 0.9, supported: 0.1 };
const falseFailure: JudgeVerdict = { contradicts: 0.99, kind: 'false_failure', kindProbability: 0.9, supported: 0.1 };
const byWords = (sentence: string): JudgeVerdict => sentence.includes('went through') ? falseSuccess : sentence.includes('failed') ? falseFailure : fine;
function inFlight(outcomes: Array<{ ms: number; status: LiveDelivery['status'] }>, tasks: () => VoiceTask[] = () => [amazonSale],
  verdict: (sentence: string) => JudgeVerdict | undefined = byWords) {
  const clock = { t: 0 }, sent: string[] = [], judged: string[] = [], triggers: GuardrailTrigger[] = [];
  let next = 0, sequence = 0;
  const guardrail = createVoiceGuardrail({ callId: 'c', tasks, now: () => clock.t,
    judge: async input => { judged.push(input.sentence); return verdict(input.sentence); },
    correct: async text => {
      const outcome = outcomes[next++] ?? { ms: 0, status: 'accepted' as const };
      sent.push(text); await wait(outcome.ms); return { eventId: 'x', status: outcome.status } as LiveDelivery;
    },
    record: trigger => triggers.push(trigger) });
  const fragments: LiveTranscriptSnapshot['fragments'] = [];
  const say = (text: string, at: number) => {
    clock.t = at; sequence++;
    fragments.push({ eventId: `f${sequence}`, role: 'assistant', text, startMs: at, endMs: at + 100, sequence } as LiveTranscriptSnapshot['fragments'][number]);
    guardrail.observe({ revision: sequence, callerRevision: 0, fragments: [...fragments] } as LiveTranscriptSnapshot, 's');
  };
  return { guardrail, sent, judged, triggers, say };
}
const outcomes = (triggers: GuardrailTrigger[]) => triggers.map(trigger => [trigger.sentence.slice(0, 18), trigger.correction]);

test('probe pr11b: a correction blocked only by one in flight is sent when that one is refused', async () => {
  const h = inFlight([{ ms: 400, status: 'rejected' }]);
  h.say('Your sale went through.', 0); await wait(50);           // A: reserved, its append pending 400 ms
  h.say('Actually the sale failed.', 1000); await wait(500);     // B: judged while A is pending, held; A refused, B sent
  h.say('So it went through after all.', 2000); await wait(50);  // C: within B's accepted cooldown
  h.guardrail.stop();
  assert.deepEqual(outcomes(h.triggers), [['Your sale went thr', 'rejected'], ['Actually the sale ', 'accepted'],
    ['So it went through', 'suppressed_cooldown']]);
  assert.equal(h.sent.length, 2);
  assert.match(h.sent[1]!, /^Correction: You said something failed or did not happen/);
  assert.deepEqual(h.judged, ['Your sale went through.', 'Actually the sale failed.', 'So it went through after all.'],
    'the task list did not change while B waited, so B went out on its first verdict');
});

test('probe pr11b: an append that ends unconfirmed (ack timeout) releases the cooldown; no wedge', async () => {
  const h = inFlight([{ ms: 300, status: 'unconfirmed' }]);
  h.say('Your sale went through.', 0); await wait(350);
  h.say('Your sale went through, done.', 1000); await wait(50);
  h.guardrail.stop();
  assert.deepEqual(h.triggers.map(trigger => trigger.correction), ['unconfirmed', 'accepted']);
});

test('a correction held behind one in flight stays suppressed when that one is accepted, and is recorded once', async () => {
  const h = inFlight([{ ms: 300, status: 'accepted' }]);
  h.say('Your sale went through.', 0); await wait(50);
  h.say('Actually the sale failed.', 1000); await wait(400);
  h.guardrail.stop();
  assert.deepEqual(outcomes(h.triggers), [['Your sale went thr', 'accepted'], ['Actually the sale ', 'suppressed_cooldown']]);
  assert.equal(h.sent.length, 1);
});

test('of corrections held behind one in flight the latest is sent, and a restatement never displaces a clear correction', async () => {
  const ambiguous: JudgeVerdict = { contradicts: 0.99, kind: 'false_success', kindProbability: 0.9, supported: 0.7 };
  const h = inFlight([{ ms: 400, status: 'rejected' }], undefined, sentence => sentence.startsWith('Anyway') ? ambiguous : byWords(sentence));
  h.say('Your sale went through.', 0); await wait(50);              // A in flight
  h.say('The sale failed.', 1000); await wait(20);                   // B1 held
  h.say('Nothing sold, the sale failed.', 1500); await wait(20);     // B2, later, replaces B1
  h.say('Anyway, it went through.', 2000); await wait(500);          // B3, a restatement, leaves B2 held; A refused, B2 sent
  h.guardrail.stop();
  assert.deepEqual(outcomes(h.triggers), [['The sale failed.', 'suppressed_cooldown'], ['Anyway, it went th', 'suppressed_cooldown'],
    ['Your sale went thr', 'rejected'], ['Nothing sold, the ', 'accepted']]);
  assert.equal(h.sent.length, 2);
});

test('a held correction is judged again when the task list changed while it waited, and dropped if no longer false', async () => {
  let tasks = [amazonSale];
  const h = inFlight([{ ms: 300, status: 'rejected' }], () => tasks,
    sentence => sentence.includes('went through') && tasks[0]!.status === 'pending' ? falseSuccess : fine);
  h.say('Your sale went through.', 0); await wait(50);   // A in flight
  h.say('Yes, it went through.', 1000); await wait(20);  // B held
  tasks = [{ ...amazonSale, status: 'finished', outcome: 'You sold about $2.00 of Amazon stock.' }];
  await wait(400);
  h.guardrail.stop();
  assert.deepEqual(h.judged, ['Your sale went through.', 'Yes, it went through.', 'Yes, it went through.'], 'judged again on the new list');
  assert.equal(h.sent.length, 1, 'true by then, so nothing more is sent');
  assert.deepEqual(h.triggers.map(trigger => trigger.correction), ['rejected']);
});

// Call 1325b55e, scrubbed (tests/fixtures/voice-told-facts.json).
const call1325 = JSON.parse(readFileSync(new URL('./fixtures/voice-told-facts.json', import.meta.url), 'utf8')) as {
  holdingLabels: string[]; fragments: LiveTranscriptSnapshot['fragments']; requests: Array<{ firstSequence: number; result?: string }> };
const RELAYED = "I haven't mentioned anything called Maple, but Cedar Leaf is five hundred tokens worth about four dollars and eighty cents.";
function replay(guardrail: ReturnType<typeof createVoiceGuardrail>, fragments: LiveTranscriptSnapshot['fragments']) {
  const shown: LiveTranscriptSnapshot['fragments'] = [];
  for (const fragment of fragments) {
    shown.push(fragment);
    guardrail.observe({ revision: shown.length, callerRevision: 0, fragments: [...shown] } as LiveTranscriptSnapshot, 'live_1');
  }
}
function toldHarness(fragments = call1325.fragments) {
  const inputs: Array<{ sentence: string; told: string }> = [], corrections: string[] = [], triggers: GuardrailTrigger[] = [];
  const tasks: VoiceTask[] = call1325.requests.slice(0, 3).map((request, index) => ({ requestId: `r${index}`, sessionId: 'live_1',
    delegationId: `d${index}`, sourceEventId: `e${index}`, status: 'finished' as const, request: `request ${index}`, outcome: request.result }));
  const guardrail = createVoiceGuardrail({ callId: 'c', tasks: () => tasks, labels: () => call1325.holdingLabels,
    // The relayed result matches a listed result, so the task-list questions find nothing wrong with it;
    // only Dexter's own earlier words show the conflict.
    judge: async input => {
      inputs.push({ sentence: input.sentence, told: input.told ?? '' });
      return { contradicts: 0.02, kind: 'none', kindProbability: 0.97, supported: 0.95,
        contradictsTold: input.sentence.startsWith("I haven't mentioned anything called Maple")
          && (input.told ?? '').includes('"MAPLE is about twenty-one forty') ? 0.995 : 0.01 };
    },
    correct: async text => { corrections.push(text); return { eventId: 'x', status: 'accepted' } as LiveDelivery; },
    record: trigger => triggers.push(trigger) });
  replay(guardrail, fragments);
  return { guardrail, inputs, corrections, triggers };
}

test('call 1325b55e: a relayed "I haven\'t mentioned Maple" after Dexter read Maple out gets a neutral restatement of its own words', async () => {
  const h = toldHarness();
  await wait(50); h.guardrail.stop();
  assert.deepEqual(h.triggers.map(trigger => [trigger.kind, trigger.correction, trigger.sentence, trigger.probability]),
    [['told_conflict', 'accepted', RELAYED, 0.995]]);
  assert.equal(h.corrections.length, 1);
  assert.match(h.corrections[0]!, /^For accuracy, this is what you already told the caller earlier on this call, in your own words: "MAPLE is about twenty-one forty, Birch nine oh-five, Cedarleaf four eighty, SpaceX two ninety, /,
    'the statement it contradicts comes first');
  assert.match(h.corrections[0]!, /never deny having said it/);
  assert.doesNotMatch(h.corrections[0]!, ACCUSES);
  assert.ok(Buffer.byteLength(h.corrections[0]!) <= 500);
  const relayed = h.inputs.find(input => input.sentence === RELAYED)!;
  assert.match(relayed.told, /^- "Your portfolio is about forty-one dollars and twenty cents\."$/m);
  assert.match(relayed.told, /^- "MAPLE is about twenty-one forty, .* USDC forty cents,"$/m);
  assert.match(relayed.told, /^- "Right, MAPLE\."$/m, 'a holding named from the portfolio note is a fact');
  assert.doesNotMatch(relayed.told, /haven't mentioned|Let me check/, 'only what Dexter said before this sentence');
  assert.doesNotMatch(relayed.told, /On it|Checking that one|Should I run it|still waiting/, 'progress talk and questions are not facts');
  assert.equal(h.inputs[0]!.told, '', 'nothing was told before the first sentence');
});

test('call 1325b55e: the same sentence passes when Dexter never read Maple out', async () => {
  const h = toldHarness(call1325.fragments.filter(fragment => fragment.sequence < 9 || fragment.sequence > 38));
  await wait(50); h.guardrail.stop();
  assert.deepEqual(h.triggers, []);
  assert.doesNotMatch(h.inputs.find(input => input.sentence === RELAYED)!.told, /twenty-one forty/);
});

test('a sentence the guardrail found false is never offered back to the judge as something Dexter told the caller', async () => {
  const inputs: Array<{ sentence: string; told: string }> = [];
  const h = createVoiceGuardrail({ callId: 'c', tasks: () => [pending],
    judge: async input => { inputs.push({ sentence: input.sentence, told: input.told ?? '' });
      return input.sentence.includes('went through') ? contradiction : fine; },
    correct: async () => ({ eventId: 'x', status: 'accepted' }) as LiveDelivery });
  const t = transcript();
  h.observe(t.say('assistant', 'Your Apple sale went through.', 0), 'live_1'); await wait(20);
  h.observe(t.say('assistant', 'You have about twelve dollars of Apple stock.', 6000), 'live_1'); await wait(20);
  h.observe(t.say('assistant', 'Your balance is about forty dollars.', 12000), 'live_1'); await wait(20);
  h.stop();
  assert.equal(inputs[1]!.told, '', 'the corrected false claim is not a fact');
  assert.equal(inputs[2]!.told, '- "You have about twelve dollars of Apple stock."');
});

test('the told restatement stays within one append however long the earlier statements', () => {
  const told = ['ß'.repeat(900), 'You have about twelve dollars of WALNUT.', '😀 '.repeat(300)];
  const text = correctionText('told_conflict', [], 'I never mentioned WALNUT.', told);
  assert.ok(Buffer.byteLength(text) <= 500, String(Buffer.byteLength(text)));
  assert.match(text, /in your own words: "You have about twelve dollars of WALNUT\." "/, 'the related statement first');
  assert.match(text, /otherwise carry on\.$/);
});

test('the Decisions judge sends Dexter\'s earlier statements, and asks about them, only when there are any', async () => {
  const bodies: Array<{ input: string; questions: Array<{ name: string; instructions?: string }> }> = [];
  const judge = createDecisionsJudge('sk-test', (async (_url: string, init: RequestInit) => {
    bodies.push(JSON.parse(String(init.body)));
    return new Response(JSON.stringify({ answers: [{ name: 'contradicts', type: 'predicate', probability: 0.01 },
      { name: 'supported', type: 'predicate', probability: 0.93 },
      { name: 'kind', type: 'choice', choice: 'none', probabilities: [{ value: 'none', probability: 0.97 }] },
      { name: 'contradicts_told', type: 'predicate', probability: 0.99 }] }), { status: 200 });
  }) as typeof fetch);
  assert.deepEqual(await judge({ tasks: '1. finished.', conversation: '', sentence: RELAYED, told: '- "MAPLE is about twenty-one forty,"' }),
    { contradicts: 0.01, kind: 'none', kindProbability: 0.97, supported: 0.93, contradictsTold: 0.99 });
  assert.match(bodies[0]!.input, /\n\nWHAT DEXTER ALREADY TOLD THE CALLER EARLIER ON THIS CALL \(its own words, before the latest sentence\):\n- "MAPLE is about twenty-one forty,"\n\nRECENT CONVERSATION:/);
  assert.deepEqual(bodies[0]!.questions.map(question => question.name), ['contradicts', 'supported', 'kind', 'contradicts_told']);
  assert.equal(bodies[0]!.questions[3]!.instructions, TOLD_INSTRUCTIONS);
  await judge({ tasks: '1. finished.', conversation: '', sentence: 'Okay.', told: '' });
  assert.doesNotMatch(bodies[1]!.input, /ALREADY TOLD/);
  assert.deepEqual(bodies[1]!.questions.map(question => question.name), ['contradicts', 'supported', 'kind'],
    'without earlier statements the request is the calibrated one');
});
