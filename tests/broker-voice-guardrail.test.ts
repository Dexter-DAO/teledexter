import test from 'node:test';
import assert from 'node:assert/strict';
import { concernsTasks, correctionText, createDecisionsJudge, createVoiceGuardrail, type GuardrailJudge,
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
  tasks: () => VoiceTask[] = () => [pending], truth?: () => VoiceTask[]) {
  const judged: Array<{ sentence: string; tasks: string; conversation: string }> = [];
  const corrections: Array<{ text: string; delegationId: string | null; sessionId: string }> = [];
  const triggers: GuardrailTrigger[] = [];
  const judge: GuardrailJudge = async input => { judged.push(input); return verdict(input.sentence, input.tasks); };
  const guardrail = createVoiceGuardrail({ callId: 'call_1', judge, tasks, ...(truth ? { truth } : {}),
    correct: async (text, delegationId, sessionId) => { corrections.push({ text, delegationId, sessionId }); return { eventId: 'c1', status: 'accepted' } as LiveDelivery; },
    record: trigger => triggers.push(trigger) });
  return { guardrail, judged, corrections, triggers };
}
const contradiction: JudgeVerdict = { contradicts: 0.97, kind: 'false_success', kindProbability: 0.9 };
const fine: JudgeVerdict = { contradicts: 0.02, kind: 'none', kindProbability: 0.95 };

test('only sentences that can concern a request reach the judge', () => {
  assert.equal(concernsTasks('Basil likes six hours of sun and well-drained soil.'), false);
  assert.equal(concernsTasks('Your Apple sale went through.'), true);
  assert.equal(concernsTasks("I'm buying it back now."), true);
  assert.equal(concernsTasks('On it.'), true);
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
  assert.match(correction.text, /^Correction: You said something finished or went through that has not\. The task list says: The latest request is in progress with no result yet; progress only: The sale of about \$4\.00 of Apple was being placed\. Say now, in one short sentence, that you misspoke/);
  assert.deepEqual(h.triggers.map(value => [value.kind, value.correction, value.sentence]), [['false_success', 'accepted', 'Your Apple sale went through.']]);
  h.guardrail.stop();
});

test('true sentences, unsure verdicts and judge failures never correct', async () => {
  const verdicts: Array<JudgeVerdict | undefined | Error> = [fine, { contradicts: 0.85, kind: 'false_success', kindProbability: 0.9 },
    { contradicts: 0.99, kind: 'stale_pending', kindProbability: 0.9 }, undefined, new Error('timeout')];
  const h = harness(() => { const next = verdicts.shift(); if (next instanceof Error) throw next; return next; });
  const t = transcript();
  let at = 0;
  for (const sentence of ['It is still being placed.', 'It went through.', 'Still waiting on it.', 'It sold.', 'It is done.'])
    h.guardrail.observe(t.say('assistant', sentence, at += 5000), 'live_1');
  await wait(30);
  assert.equal(h.judged.length, 5);
  assert.deepEqual(h.corrections, []); assert.deepEqual(h.triggers, []);
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
  assert.match(correctionText('false_success', [justDelegated]), /The task list says: The latest request is just passed on to Primary, not started yet\./,
    'a correction right after a delegation never says nothing was requested');
});

test('the Decisions judge asks one predicate and one choice and reads both answers', async () => {
  let body: any;
  const judge = createDecisionsJudge('sk-test', (async (_url: string, init: RequestInit) => {
    body = JSON.parse(String(init.body));
    return new Response(JSON.stringify({ answers: [{ name: 'contradicts', type: 'predicate', probability: 0.93 },
      { name: 'kind', type: 'choice', choice: 'phantom_step', probabilities: [{ value: 'phantom_step', probability: 0.81 }] }] }), { status: 200 });
  }) as typeof fetch);
  assert.deepEqual(await judge({ tasks: '1. pending.', conversation: '', sentence: 'Buying it back now.' }),
    { contradicts: 0.93, kind: 'phantom_step', kindProbability: 0.81 });
  assert.equal(body.model, 'gpt-6-luna');
  assert.deepEqual(body.questions.map((question: { type: string; name: string }) => [question.type, question.name]), [['predicate', 'contradicts'], ['choice', 'kind']]);
  const failing = createDecisionsJudge('sk-test', (async () => new Response('{}', { status: 500 })) as unknown as typeof fetch);
  assert.equal(await failing({ tasks: '', conversation: '', sentence: 'x' }), undefined);
});
