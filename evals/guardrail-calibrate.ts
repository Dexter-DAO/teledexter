/**
 * Offline calibration of the transcript guardrail's judge. Replays every assistant sentence of
 * scored bench runs through the real judge (OpenAI Decisions API), against the task list as the
 * application knew it when the sentence ended, and compares the verdicts with the bench scorer's
 * labels. False corrections are failures, so this reports both missed false claims and
 * corrections the guardrail would send for true sentences, for each trigger threshold.
 *
 *   node --import tsx evals/guardrail-calibrate.ts --runs-root /abs/dir --labels baseline,new \
 *     --config-file /abs/production.env --cache /abs/judge-cache.json [--threshold 0.9]
 *
 * Verdicts are cached by request body, so an unchanged judge prompt costs nothing to re-run.
 */
import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { scenarios } from './scenarios.js';
import { sentences, type Event } from './timeline.js';
import { concernsTasks, createDecisionsJudge, describeTasks, type JudgeVerdict } from '../src/broker/voiceGuardrail.js';
import type { VoiceTask } from '../src/broker/voiceTasks.js';

const args = new Map<string, string>();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i]!, process.argv[i + 1]!);
const runsRoot = path.resolve(args.get('--runs-root')!);
const labels = (args.get('--labels') ?? 'baseline').split(',');
const cacheFile = path.resolve(args.get('--cache')!);
const chosen = Number(args.get('--threshold') ?? '0.9');
let apiKey = '';
for (const line of fs.readFileSync(args.get('--config-file')!, 'utf8').split(/\r?\n/)) {
  const match = line.match(/^\s*(?:export\s+)?OPENAI_API_KEY\s*=\s*(.*?)\s*$/);
  if (match) apiKey = match[1]!.replace(/^(['"])(.*)\1$/, '$2');
}
const cache: Record<string, unknown> = fs.existsSync(cacheFile) ? JSON.parse(fs.readFileSync(cacheFile, 'utf8')) : {};
let calls = 0, inputChars = 0;
const cachedFetch = (async (url: string, init: RequestInit) => {
  const key = createHash('sha256').update(String(init.body)).digest('hex');
  if (!(key in cache)) {
    calls++; inputChars += String(init.body).length;
    for (let attempt = 1; ; attempt++) {
      const response = await fetch(url, { ...init, signal: AbortSignal.timeout(15_000) });
      if (response.ok) { cache[key] = await response.json(); break; }
      if (attempt >= 3) return response;
      await new Promise(resolve => setTimeout(resolve, 1500 * attempt));
    }
  }
  return new Response(JSON.stringify(cache[key]), { status: 200, headers: { 'content-type': 'application/json' } });
}) as typeof fetch;
// --instructions-file tries a candidate judge prompt without editing the source.
const instructions = args.has('--instructions-file') ? fs.readFileSync(args.get('--instructions-file')!, 'utf8').trim() : undefined;
const judge = createDecisionsJudge(apiKey, cachedFetch, 20_000, instructions);

/** The task list the application held at time t, rebuilt from the run's backend events. */
function tasksAt(timeline: Event[], t: number): VoiceTask[] {
  const submits = timeline.filter(event => event.type === 'primary.submit');
  const submitted = submits.filter(event => event.at <= t);
  const tasks: VoiceTask[] = submitted.map(event => {
    const base = { requestId: String(event.requestId), sessionId: 's', sourceEventId: String(event.request), request: String(event.text ?? '').trim() };
    const final = timeline.find(value => value.type === 'primary.final_visible' && value.request === event.request && value.at <= t);
    if (final) return { ...base, status: 'finished', outcome: String(final.text ?? final.state) };
    const partial = timeline.filter(value => value.type === 'primary.partial_visible' && value.request === event.request && value.at <= t).at(-1);
    return { ...base, status: 'pending', ...(partial ? { progress: String(partial.text) } : {}) };
  });
  // Delegations not yet submitted: admitting (about 3 s), or queued behind a pending request.
  const delegations = timeline.filter(event => event.type === 'live.server' && event.kind === 'session.delegation.created' && event.at <= t);
  const unsubmitted = delegations.slice(submitted.length);
  const busy = tasks.some(task => task.status === 'pending');
  unsubmitted.forEach((event, index) => {
    const later = submits[submitted.length + index];
    if (t - event.at <= 3_000) tasks.push({ requestId: `d${index}`, sessionId: 's', sourceEventId: '', status: 'starting', request: '' });
    else if (busy) tasks.push({ requestId: `d${index}`, sessionId: 's', sourceEventId: '', status: 'waiting_to_start',
      request: String(later?.text ?? '').trim() });
  });
  return tasks;
}

/** The guardrail's recent conversation: caller turns and Dexter sentences before this sentence. */
function conversationBefore(timeline: Event[], said: ReturnType<typeof sentences>, index: number): string {
  const lines: Array<{ at: number; text: string }> = [];
  let caller = '', callerAt = 0;
  for (const event of timeline) {
    if (event.type !== 'live.transcript') continue;
    if (event.role === 'user') { if (!caller) callerAt = event.at; caller += event.delta; continue; }
    if (caller) { lines.push({ at: callerAt, text: `Caller:${caller}` }); caller = ''; }
  }
  if (caller) lines.push({ at: callerAt, text: `Caller:${caller}` });
  for (const s of said.slice(0, index)) lines.push({ at: s.at, text: `Dexter: ${s.text}` });
  const before = said[index]!.at;
  return lines.filter(line => line.at < before).sort((a, b) => a.at - b.at).slice(-7).map(line => line.text).join('\n');
}

interface Item { run: string; s: number; sentence: string; label: 'false' | 'ok'; why?: string; tasks: string; verdict?: JudgeVerdict }
const items: Item[] = [];
for (const label of labels) for (const scenario of Object.values(scenarios)) {
  const root = path.join(runsRoot, label, scenario.id);
  if (!fs.existsSync(root)) continue;
  for (const run of fs.readdirSync(root)) {
    const dir = path.join(root, run);
    if (!fs.existsSync(path.join(dir, 'score.json'))) continue;
    const evidence = JSON.parse(fs.readFileSync(path.join(dir, 'evidence.json'), 'utf8'));
    const score = JSON.parse(fs.readFileSync(path.join(dir, 'score.json'), 'utf8'));
    const timeline = evidence.timeline as Event[];
    const said = sentences(timeline);
    const falseSet = new Set<number>(score.falseClaims.map((claim: { s: number }) => claim.s));
    const verdicts = new Map<number, { verdict: string; why: string }>(score.judgeWhy.map((value: any) => [value.s, value]));
    said.forEach((s, index) => {
      if (!concernsTasks(s.text)) return;
      const verdict = verdicts.get(s.i)?.verdict;
      if (verdict === 'promise') return;
      const tasks = describeTasks(tasksAt(timeline, s.end + 300));
      items.push({ run: `${label}/${scenario.id}/${run}`, s: s.i, sentence: s.text, label: falseSet.has(s.i) ? 'false' : 'ok',
        why: verdicts.get(s.i)?.why, tasks, ...{ conversation: conversationBefore(timeline, said, index) } } as Item);
    });
  }
}
let next = 0;
await Promise.all(Array.from({ length: 8 }, async () => {
  while (next < items.length) {
    const item = items[next++]! as Item & { conversation: string };
    try { item.verdict = await judge({ tasks: item.tasks, conversation: item.conversation, sentence: item.sentence }); }
    catch { item.verdict = undefined; }
  }
}));
fs.writeFileSync(cacheFile, JSON.stringify(cache));

const kinds = new Set(['false_success', 'false_failure', 'phantom_step']);
const fires = (verdict: JudgeVerdict | undefined, threshold: number, minKind: number) => Boolean(verdict
  && verdict.contradicts >= threshold && kinds.has(verdict.kind) && verdict.kindProbability >= minKind);
const positives = items.filter(item => item.label === 'false').length;
console.log(`${items.length} task sentences (${positives} labelled false claims) from ${labels.join(', ')}; ${calls} new judge calls`);
console.log('| contradicts >= | kind p >= | caught false claims | false corrections |');
console.log('|---|---|---|---|');
for (const threshold of [0.5, 0.7, 0.8, 0.9, 0.95, 0.98]) for (const minKind of [0, 0.6, 0.8]) {
  const fired = items.filter(item => fires(item.verdict, threshold, minKind));
  console.log(`| ${threshold} | ${minKind} | ${fired.filter(item => item.label === 'false').length}/${positives} | ${fired.filter(item => item.label === 'ok').length} |`);
}
const show = (item: Item) => `  [${item.run} S${item.s}] "${item.sentence}" -> ${item.verdict ? `${item.verdict.contradicts.toFixed(2)} ${item.verdict.kind} ${item.verdict.kindProbability.toFixed(2)}` : 'no verdict'}\n    tasks: ${item.tasks.replace(/\n/g, ' | ')}${item.why ? `\n    scorer: ${item.why}` : ''}`;
console.log(`\nFalse corrections at ${chosen}:`);
for (const item of items.filter(value => value.label === 'ok' && fires(value.verdict, chosen, 0))) console.log(show(item));
console.log(`\nMissed false claims at ${chosen}:`);
for (const item of items.filter(value => value.label === 'false' && !fires(value.verdict, chosen, 0))) console.log(show(item));
console.log(`\njudge calls ${calls}, approx input tokens ${Math.round(inputChars / 4)}, approx $${(inputChars / 4 / 1e6 * 0.1).toFixed(3)}`);
