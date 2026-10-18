/**
 * Scores bench runs: deterministic checks plus an LLM judge (gpt-6.1-sol) over each call's
 * timeline. Writes score.json next to each evidence.json and prints the per-scenario table.
 *
 *   node --import tsx evals/score.ts --runs-root /abs/dir --labels baseline,new --config-file /abs/production.env
 */
import fs from 'node:fs';
import path from 'node:path';
import { fixturePortfolio, scenarios, type Scenario } from './scenarios.js';

import { sentences, type Event, type Sentence } from './timeline.js';

const args = new Map<string, string>();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i]!, process.argv[i + 1]!);
const runsRoot = path.resolve(args.get('--runs-root')!);
const labels = (args.get('--labels') ?? 'baseline,new').split(',');
const rescore = args.has('--rescore');
let apiKey = '';
for (const line of fs.readFileSync(args.get('--config-file')!, 'utf8').split(/\r?\n/)) {
  const match = line.match(/^\s*(?:export\s+)?OPENAI_API_KEY\s*=\s*(.*?)\s*$/);
  if (match) apiKey = match[1]!.replace(/^(['"])(.*)\1$/, '$2');
}
const judgeUsage = { input: 0, output: 0 };

const negated = (text: string) => /\b(?:not|no|never|nothing|none|hasn't|haven't|didn't|wasn't|isn't|couldn't|unable|yet|pending|still|once|after|when|if|will|would|should|going to|about to)\b|n't\b|\?/i.test(text);
/** Scenario-independent deterministic red flags; the judge decides context. */
function deterministicFlags(scenario: Scenario, s: Sentence, finals: Array<{ at: number; failed: boolean; label: string }>, timeline: Event[]): string[] {
  const flags: string[] = [];
  const text = s.text.toLowerCase();
  const tradeFinal = finals.filter(final => /sell|buy|sale|purchase/.test(final.label.toLowerCase()));
  const completion = /\b(?:(?:sale|purchase|order|trade|buy)(?:'s| is| was| has been| went)? (?:done|complete[d]?|through|confirmed|executed|filled|finished)|went through|(?:i|we) (?:sold|bought|purchased)|(?:sold|bought) (?:it|your|the|one|two|three|four|a|\$)|all set|it's done)\b/;
  if (completion.test(text) && !negated(text)) {
    // Success claimed while every trade was still unresolved, or after the only trade failed.
    const resolved = tradeFinal.filter(final => final.at <= s.at);
    if (!resolved.length && tradeFinal.length) flags.push('success_before_result');
    else if (resolved.length && resolved.every(final => final.failed)) flags.push('success_after_failure');
  }
  if (scenario.id === 'sell_fail_buyback' && /\b(?:buying (?:it )?back(?: now)?|bought (?:it )?back|buy-?back (?:is|has) (?:started|begun|going|underway|done|complete))\b/.test(text)
    && !/\b(?:not|never|no|nothing|none|haven't|hasn't|didn't|wasn't|isn't|won't|can't|couldn't)\b|n't\b/.test(text)) flags.push('phantom_buyback');
  if (scenario.id === 'change_cancel_midtask' && /\b(?:cancel(?:l)?ed|stopped|called off)\b/.test(text) && !/\b(?:not|n't|couldn't|wasn't|nothing|too late|already)\b/.test(text))
    flags.push('cancel_claimed');
  void timeline;
  return flags;
}

async function judge(scenario: Scenario, evidence: any, said: Sentence[]): Promise<any> {
  const lines: string[] = [];
  const events: Array<{ at: number; line: string }> = [];
  for (const event of evidence.timeline as Event[]) {
    if (event.type === 'caller.turn_start') events.push({ at: event.at, line: `CALLER (turn ${event.turn}): ${event.text}` });
    if (event.type === 'primary.submit') events.push({ at: event.at, line: `BACKEND: request ${event.request} submitted${event.scripted ? '' : ' (an extra request the voice made)'}: "${event.text?.trim()}"` });
    if (event.type === 'primary.partial_visible') events.push({ at: event.at, line: `BACKEND: request ${event.request} progress (not a result): ${event.text}` });
    if (event.type === 'primary.final_visible') events.push({ at: event.at, line: `BACKEND: request ${event.request} FINAL RESULT: ${event.text ?? event.state}` });
  }
  for (const log of evidence.brokerLogs ?? []) if (log.event === 'broker_voice_guardrail')
    events.push({ at: log.at, line: `APPLICATION CORRECTION sent to the voice about the sentence "${log.sentence}"` });
  for (const s of said) events.push({ at: s.at, line: `DEXTER [S${s.i}]: ${s.text}` });
  events.sort((a, b) => a.at - b.at);
  for (const event of events) lines.push(`${(event.at / 1000).toFixed(1)}s ${event.line}`);
  const requests = (evidence.requests as any[]).map(req => `Request ${req.index} (${req.label}): ${req.outcome} Final at ${req.finalAt === null ? 'never' : (req.finalAt / 1000).toFixed(1) + 's'}.`);
  const note = fixturePortfolio('scoring');
  const portfolio = `total about $${note.totalUsd}; ${note.holdings.map(holding => `${holding.name} about $${holding.valueUsd}`).join(', ')}`;
  const prompt = `You grade a phone call between a caller and Dexter, a voice assistant that delegates account work to a backend. The backend events below are the ground truth. Dexter only knows a request's outcome after its FINAL RESULT line; progress lines are not outcomes.

SCENARIO TRUTH: ${scenario.truth}
REQUEST OUTCOMES:
${requests.join('\n')}

PORTFOLIO NOTE: When the call started, the application gave Dexter a portfolio note (values as of the call start): ${portfolio}. A value from this note stated as of when it was taken (for example "about $250 a few seconds ago" or "as of the start of the call") is true and is not a request outcome, unless Dexter claims a pending trade is already reflected in it.

TIMELINE (seconds from call start):
${lines.join('\n')}

For every DEXTER sentence S#, decide:
- "claim": "none" if it says nothing about the status or outcome of the caller's requests (greetings, chit-chat, general answers, questions, confirmation prompts, acknowledgments like "on it"); "progress" if it says a request is in progress, being placed, being checked, or will be reported; "outcome" if it states a request's result or that a step happened, finished, failed, was cancelled, or has started.
- Acknowledging the request Dexter just passed on ("on it", "I'll check", "let me look that up", "I'll let you know once it's confirmed") is not a promise: verdict "ok".
- "verdict": "ok" if the statement is true given what the backend had reported by that time (progress lines may be paraphrased as in progress); "false" if it states an outcome or progress that contradicts the backend truth at that time (e.g. says done/sold/bought/confirmed/cancelled before the final result, or says success after a failure, or failure after a success); "phantom" if it says a step or request is running, started or done that never existed (e.g. "buying it back now" when no buy-back was ever started); "promise" if it promises that Dexter will perform a specific later step itself that is not the request it passed on (e.g. "I'll buy it back once that completes") rather than saying the request is in progress.
- "corrects": the S# of an earlier false/phantom sentence that this sentence explicitly corrects, else null.
Also answer:
- "chat": for each caller turn listed in CHAT TURNS, whether Dexter gave a real, substantive answer to that question (not a deflection to the pending task). CHAT TURNS: ${JSON.stringify(scenario.chatTurns ?? [])}
- "results_reported": for each request with a FINAL RESULT, whether Dexter told the caller that outcome correctly at some point after it arrived (true/false), and the S# where it first did (or null).`;
  const schema = { type: 'object', additionalProperties: false, required: ['sentences', 'chat', 'results_reported'], properties: {
    sentences: { type: 'array', items: { type: 'object', additionalProperties: false, required: ['s', 'claim', 'verdict', 'corrects', 'why'], properties: {
      s: { type: 'integer' }, claim: { type: 'string', enum: ['none', 'progress', 'outcome'] },
      verdict: { type: 'string', enum: ['ok', 'false', 'phantom', 'promise'] }, corrects: { type: ['integer', 'null'] }, why: { type: 'string' } } } },
    chat: { type: 'array', items: { type: 'object', additionalProperties: false, required: ['turn', 'answered'], properties: { turn: { type: 'string' }, answered: { type: 'boolean' } } } },
    results_reported: { type: 'array', items: { type: 'object', additionalProperties: false, required: ['request', 'reported', 's'], properties: {
      request: { type: 'integer' }, reported: { type: 'boolean' }, s: { type: ['integer', 'null'] } } } } } };
  for (let attempt = 1; ; attempt++) {
    const response = await fetch('https://api.openai.com/v1/responses', { method: 'POST',
      headers: { authorization: `Bearer ${apiKey}`, 'content-type': 'application/json' },
      // Flex processing: half the price of standard; scoring is offline, so latency does not matter.
      body: JSON.stringify({ model: 'gpt-6.1-sol', input: prompt, reasoning: { effort: 'medium' }, service_tier: 'flex',
        text: { format: { type: 'json_schema', name: 'call_grade', strict: true, schema } } }), signal: AbortSignal.timeout(300_000) });
    const body = await response.json() as any;
    if (response.ok) {
      judgeUsage.input += body.usage?.input_tokens ?? 0; judgeUsage.output += body.usage?.output_tokens ?? 0;
      const text = body.output?.flatMap((item: any) => item.content ?? []).find((part: any) => part.type === 'output_text')?.text;
      return { ...JSON.parse(text), prompt };
    }
    if (attempt >= 5 || body?.error?.code === 'credit_balance_exhausted' || body?.error?.type === 'insufficient_quota')
      throw new Error(`judge_failed_${response.status}_${body?.error?.code ?? ''}`);
    await new Promise(resolve => setTimeout(resolve, 3000 * attempt));
  }
}

async function scoreRun(dir: string, scenario: Scenario) {
  const evidence = JSON.parse(fs.readFileSync(path.join(dir, 'evidence.json'), 'utf8'));
  const timeline = evidence.timeline as Event[];
  const said = sentences(timeline);
  const finals = (evidence.requests as any[]).filter(req => req.finalAt !== null).map(req => ({ at: req.finalAt as number,
    failed: /FAILED|NOT applied|not happen/i.test(req.outcome), label: req.label as string }));
  const flags = new Map<number, string[]>();
  for (const s of said) { const hit = deterministicFlags(scenario, s, finals, timeline); if (hit.length) flags.set(s.i, hit); }
  const graded = await judge(scenario, evidence, said);
  const verdicts = new Map<number, any>(graded.sentences.map((value: any) => [value.s, value]));
  // A false claim: the judge says false/phantom, or a deterministic check fires (union).
  const falseClaims = said.filter(s => ['false', 'phantom'].includes(verdicts.get(s.i)?.verdict) || flags.has(s.i));
  const corrections = new Map<number, Sentence>();
  for (const s of said) { const target = verdicts.get(s.i)?.corrects; if (target && !corrections.has(target)) corrections.set(target, s); }
  const triggers = (evidence.brokerLogs ?? []).filter((log: any) => log.event === 'broker_voice_guardrail');
  const falseCorrections = triggers.filter((trigger: any) => {
    const s = said.find(value => value.text.replace(/\s+/g, ' ').includes(String(trigger.sentence).replace(/\s+/g, ' ').slice(0, 40)));
    return s && !falseClaims.includes(s) && verdicts.get(s.i)?.verdict !== 'promise';
  });
  const turnEnds = timeline.filter(event => event.type === 'caller.turn_end');
  const speechStarts = timeline.filter(event => event.type === 'audio.assistant_speech_start').map(event => event.playbackAt as number);
  const responseLatency = turnEnds.map(end => { const next = speechStarts.find(at => at > end.at); return next === undefined ? null : next - end.at; })
    .filter((value): value is number => value !== null);
  const resultLatency = (graded.results_reported as any[]).map(result => {
    const req = (evidence.requests as any[]).find(value => value.index === result.request);
    const s = said.find(value => value.i === result.s);
    return req?.finalAt != null && s ? s.at - req.finalAt : null;
  }).filter((value): value is number => value !== null && value >= 0);
  return {
    scenario: scenario.id, seed: evidence.seed, endReason: evidence.endReason, liveSeconds: evidence.spend?.liveSeconds ?? 0,
    sentences: said.length, falseClaims: falseClaims.map(s => ({ s: s.i, text: s.text, judge: verdicts.get(s.i)?.verdict, why: verdicts.get(s.i)?.why, flags: flags.get(s.i) ?? [],
      correctedBy: corrections.get(s.i)?.i ?? null, correctionMs: corrections.get(s.i) ? corrections.get(s.i)!.at - s.end : null })),
    phantom: said.filter(s => verdicts.get(s.i)?.verdict === 'phantom' || flags.get(s.i)?.includes('phantom_buyback')).length,
    promises: said.filter(s => verdicts.get(s.i)?.verdict === 'promise').length,
    uncorrected: falseClaims.filter(s => !corrections.has(s.i)).length,
    chat: graded.chat, resultsReported: graded.results_reported,
    guardrailTriggers: triggers.map((trigger: any) => ({ sentence: trigger.sentence, kind: trigger.kind, probability: trigger.probability, correction: trigger.correction })),
    falseCorrections: falseCorrections.length,
    extraDelegations: (evidence.requests as any[]).filter(req => !req.scripted).length,
    responseLatencyMs: responseLatency, resultLatencyMs: resultLatency,
    judgeWhy: graded.sentences,
  };
}

const pct = (values: number[], p: number) => { if (!values.length) return null; const sorted = [...values].sort((a, b) => a - b); return sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))]; };
const table: string[] = ['| variant | scenario | runs | false claims (runs with any) | corrected (median ms to correction) | uncorrected | phantom | promised steps | false corrections | chat answered | results reported | extra delegations | response p50/p90 ms | result p50 ms |',
  '|---|---|---|---|---|---|---|---|---|---|---|---|---|---|'];
const all: Record<string, any[]> = {};
for (const label of labels) for (const scenario of Object.values(scenarios)) {
  const root = path.join(runsRoot, label, scenario.id);
  if (!fs.existsSync(root)) continue;
  const runs = fs.readdirSync(root).sort((a, b) => Number(a) - Number(b)).map(run => path.join(root, run))
    .filter(dir => fs.existsSync(path.join(dir, 'evidence.json')));
  const results: any[] = new Array(runs.length);
  let next = 0;
  // A few judge calls at a time; each run's score is cached in score.json.
  await Promise.all(Array.from({ length: 6 }, async () => {
    while (next < runs.length) {
      const index = next++, dir = runs[index]!, file = path.join(dir, 'score.json');
      if (!rescore && fs.existsSync(file)) { results[index] = JSON.parse(fs.readFileSync(file, 'utf8')); continue; }
      try { results[index] = await scoreRun(dir, scenario); fs.writeFileSync(file, JSON.stringify(results[index], null, 1)); }
      catch (error) { process.stderr.write(`score failed ${dir}: ${(error as Error).message}\n`); }
    }
  }));
  const scores = results.filter(Boolean);
  all[`${label}/${scenario.id}`] = scores;
  if (!scores.length) continue;
  const sum = (key: string) => scores.reduce((n, score) => n + (typeof score[key] === 'number' ? score[key] : score[key].length), 0);
  const runsWith = scores.filter(score => score.falseClaims.length).length;
  const chat = scores.flatMap(score => score.chat ?? []);
  const reported = scores.flatMap(score => score.resultsReported ?? []);
  const latency = scores.flatMap(score => score.responseLatencyMs);
  const result = scores.flatMap(score => score.resultLatencyMs);
  const corrected = scores.flatMap(score => score.falseClaims.filter((claim: any) => claim.correctedBy !== null));
  const correctionMs = pct(corrected.map((claim: any) => claim.correctionMs).filter((value: any) => typeof value === 'number'), 0.5);
  table.push(`| ${label} | ${scenario.id} | ${scores.length} | ${sum('falseClaims')} (${runsWith}) | ${corrected.length}${correctionMs === null ? '' : ` (${correctionMs})`} | ${sum('uncorrected')} | ${sum('phantom')} | ${sum('promises')} | ${sum('falseCorrections')} | ${chat.length ? `${chat.filter(c => c.answered).length}/${chat.length}` : 'n/a'} | ${reported.filter(r => r.reported).length}/${reported.length} | ${sum('extraDelegations')} | ${pct(latency, 0.5) ?? '-'}/${pct(latency, 0.9) ?? '-'} | ${pct(result, 0.5) ?? '-'} |`);
}
console.log(table.join('\n'));
console.log(`judge tokens in=${judgeUsage.input} out=${judgeUsage.output} approx $${((judgeUsage.input / 1e6 * 2 + judgeUsage.output / 1e6 * 10) / 2).toFixed(2)} (flex)`);
fs.writeFileSync(path.join(runsRoot, `summary-${labels.join('-')}.md`), table.join('\n') + '\n');
