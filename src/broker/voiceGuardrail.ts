import type { LiveDelivery, LiveTranscriptSnapshot } from '../live/types.js';
import type { VoiceTask } from './voiceTasks.js';

/**
 * Monitor-and-correct guardrail (developers.openai.com/api/docs/guides/voice-server-controls,
 * "Apply conversation guardrails"). It reads assistant transcript sentences on the primary
 * connection, asks a judge whether a sentence contradicts the application's task list, and
 * appends an application-authored correction. It never touches audio, playback or the call.
 */
/** A clear contradiction is corrected outright. 'ambiguous' (the judge found a contradiction but also
 * a listed status or result the sentence matches) and 'unverified' (no verdict) get a neutral
 * restatement of the voice's own task list instead, which retracts nothing true. */
export type GuardrailKind = 'false_success' | 'false_failure' | 'phantom_step' | 'ambiguous' | 'unverified';
export interface JudgeVerdict { contradicts: number; kind: GuardrailKind | 'none' | 'stale_pending'; kindProbability: number;
  /** Whether the sentence matches the status or result of any listed request (absent: not asked). */
  supported?: number }
export type GuardrailJudge = (input: { tasks: string; conversation: string; sentence: string }) => Promise<JudgeVerdict | undefined>;
export interface GuardrailTrigger {
  at: string; sessionId: string; delegationId: string | null; kind: GuardrailKind; probability: number;
  sentence: string; judgeMs: number;
  correction: LiveDelivery['status'] | 'suppressed_cooldown' | 'suppressed_duplicate' | 'suppressed_rate';
}

/** Words that can carry a claim about a request's progress or outcome. Every sentence is judged; these
 * decide which sentences a missing verdict may not let stand. */
const TASK_TERMS = /\b(?:sell|sold|sale|selling|buy|bought|buying|purchase[ds]?|order(?:ed|s)?|trade[ds]?|trading|swap(?:ped|s)?|send|sent|sending|transfer(?:red|s)?|deposit(?:ed)?|withdr[ae]w\w*|cancel\w*|done|complete[ds]?|completing|finished|through|placed|placing|submitted|confirmed|executed|filled|failed|fail|started|starting|on it|working|in progress|pending|back|went|go ahead|checking)\b/i;
/** Claims about money or holdings that carry no task word: "You now own ten shares", "That's all taken
 * care of", "Your position is closed", "Your balance is now...". */
const MONEY_STATE = /\b(?:sell|sold|sale|selling|buy|bought|buying|purchase[ds]?|order(?:ed|s)?|trade[ds]?|trading|swap(?:ped|s)?|send|sent|sending|transfer(?:red|s)?|deposit(?:ed)?|withdr[ae]w\w*|cancel\w*|balance|went through|confirmed|executed|filled|own(?:s|ed)?|holding|hold|position|closed|taken care|all set|out of|wallet|money|shares?|account)\b/i;
export const concernsMoney = (sentence: string) => MONEY_STATE.test(sentence);
/** An outcome claim ("It's done.", "That went through.") rather than progress ("On it.", "Checking."). */
const OUTCOME = /\b(?:done|complete[ds]?|finished|through|went|placed|submitted|confirmed|executed|filled|failed|cancel\w*|stopped|started|back)\b/i;
/** A sentence that a missing verdict may not let stand: it claims an outcome or concerns money. */
export const claimsOutcome = (sentence: string) => OUTCOME.test(sentence) || MONEY_STATE.test(sentence);
/** Retries for a judge call that returned nothing (timeout, refusal, unreadable answer). */
const JUDGE_RETRY_MS = [300, 900] as const;

/** Whether a sentence can carry a claim about a request or about money. A missing verdict on one of
 * these gets a neutral restatement; every sentence is judged regardless. */
export const concernsTasks = (sentence: string) => TASK_TERMS.test(sentence) || MONEY_STATE.test(sentence);
const CORRECTION_COOLDOWN_MS = 4_000;
/** No-verdict restatements are rate-limited: each instructions.append can cut into Dexter's speech.
 * A detected contradiction (clear or ambiguous) is never held back by them; it keeps the normal cooldown. */
const RESTATEMENT_INTERVAL_MS = 20_000;
/** A restatement's content regardless of task order, so a differently referenced copy is still a duplicate. */
const restatementKey = (tasks: readonly VoiceTask[]) => tasks
  .map(task => `${task.status}|${task.outcome ?? task.progress ?? ''}`).sort().join('\n');
const clipText = (text: string, limit: number) => text.length <= limit ? text : `${text.slice(0, limit - 1).trimEnd()}…`;
const state: Record<VoiceTask['status'], string> = {
  starting: 'just passed on to Primary, not started yet', pending: 'in progress with no result yet',
  arriving: 'finished; its result is on its way to you',
  waiting_to_start: 'waiting to start after the earlier request', waiting_for_answer: "waiting for the caller's answer",
  finished: 'finished',
  failed: 'failed', cancelled: 'cancelled', not_started: 'not started',
};
const statusLine = (task: VoiceTask, limit = 150) => `${state[task.status]}${task.outcome ? `; its result: ${clipText(task.outcome, limit)}`
  : task.progress ? `; progress only: ${clipText(task.progress, Math.min(90, limit))}` : ''}`;

const STOP_WORDS = new Set(['the', 'and', 'that', 'this', 'for', 'you', 'your', 'about', 'was', 'were', 'has', 'have', 'had', 'with',
  'its', 'it', 'is', 'are', 'not', 'yet', 'now', 'all', 'one', 'too', 'did', 'just', 'then', 'what', 'from', 'will']);
const words = (text: string) => new Set((text.toLowerCase().match(/[a-z][a-z'-]{2,}/g) ?? []).filter(word => !STOP_WORDS.has(word)));
/** The task a sentence most likely refers to: the most shared words with its request or result, the most
 * recent on a tie. Corrections name it first so a claim about an earlier request is never left out. */
export function referencedTask(sentence: string, tasks: readonly VoiceTask[]): number {
  const said = words(sentence);
  let best = tasks.length - 1, bestScore = 0;
  tasks.forEach((task, index) => {
    const known = words(`${task.request} ${task.outcome ?? ''} ${task.progress ?? ''}`);
    let score = 0; for (const word of said) if (known.has(word)) score++;
    if (score >= bestScore && score > 0) { best = index; bestScore = score; }
  });
  return best;
}
/** Task lines within one append: the referenced task first, then the others newest first, results
 * clipped until the lines fit the byte budget. */
function taskLines(tasks: readonly VoiceTask[], first: number, budget: number,
  line: (task: VoiceTask, limit: number) => string): string {
  if (!tasks.length) return 'There are no requests on this call; nothing has been started.';
  const order = [first, ...tasks.map((_, index) => index).reverse().filter(index => index !== first)].slice(0, 4);
  const end = (text: string) => /[.!?…]$/.test(text) ? text : `${text}.`;
  let text = '';
  for (let limit = 150; limit >= 30; limit -= 20) {
    text = order.map(index => end(`Task ${index + 1} is ${line(tasks[index]!, limit)}`)).join(' ');
    if (Buffer.byteLength(text) <= budget) return text;
  }
  return `${text.slice(0, Math.max(0, budget - 3)).trimEnd()}…`;
}

/**
 * Application-authored and within one append: statuses and Primary's delivered results, never caller
 * words. A clear contradiction asks the voice to correct itself; 'ambiguous' and 'unverified' restate
 * the list neutrally, so a true, delivered result is never retracted.
 */
export function correctionText(kind: GuardrailKind, tasks: readonly VoiceTask[], sentence = ''): string {
  const first = referencedTask(sentence, tasks);
  if (kind === 'ambiguous' || kind === 'unverified') {
    const head = 'Status from the application, for accuracy:';
    const tail = 'If anything you told the caller differs from this, correct it in one short sentence; otherwise carry on.';
    const body = taskLines(tasks, first, 500 - Buffer.byteLength(`${head}  ${tail}`), (task, limit) => statusLine(task, limit));
    return `${head} ${body} ${tail}`;
  }
  const what = kind === 'phantom_step' ? 'You described a step or request that has not been started.'
    : kind === 'false_success' ? 'You said something finished or went through that has not.'
      : 'You said something failed or did not happen that the task list shows otherwise.';
  const head = `Correction: ${what} The task list says:`;
  const tail = 'Say now, in one short sentence, that you misspoke and give the correct status. Do not describe any step that is not on the task list.';
  return `${head} ${taskLines(tasks, first, 500 - Buffer.byteLength(`${head}  ${tail}`), (task, limit) => statusLine(task, limit))} ${tail}`;
}

/** The task list for the judge; with withRequests false it carries no caller words, for an application instruction. */
export function describeTasks(tasks: readonly VoiceTask[], withRequests = true): string {
  if (!tasks.length) return 'No requests on this call yet. Nothing has been started.';
  return tasks.slice(-6).map((task, index) => `${index + 1}. ${state[task.status]}`
    + (!task.request ? ': the request Dexter just passed on.' : withRequests ? `: caller asked "${task.request}".` : '.')
    + (task.outcome ? ` Result: ${task.outcome}` : task.progress ? ` Progress, not a result: ${task.progress}` : '')).join('\n');
}

export function createVoiceGuardrail(options: {
  callId: string;
  /** The task list as the voice may hear it (its list plus delegations not yet admitted);
   * corrections are written from this view only. */
  tasks(): VoiceTask[];
  /** The application's full truth for judging, including confirmed results not yet delivered. */
  truth?(): VoiceTask[];
  judge: GuardrailJudge;
  correct(text: string, delegationId: string | null, sessionId: string): Promise<LiveDelivery>;
  record?(trigger: GuardrailTrigger): void;
  /** Called for every judged sentence, for evaluation and tuning. */
  onJudged?(value: { sessionId: string; sentence: string; verdict?: JudgeVerdict; judgeMs: number }): void;
  threshold?: number;
  now?: () => number;
}) {
  const now = options.now ?? Date.now;
  // Calibrated offline on 402 scored bench sentences (evals/guardrail-calibrate.ts): at 0.98 no
  // true sentence was corrected; at 0.9 two were. False corrections are failures.
  const threshold = options.threshold ?? 0.98;
  const cursors = new Map<string, number>();
  let buffer: { sessionId: string; text: string; endMs: number } | undefined;
  let flushTimer: ReturnType<typeof setTimeout> | undefined;
  let lastCorrectionAt = -Infinity, stopped = false;
  const recent: string[] = [];

  const truth = () => (options.truth ?? options.tasks)();
  let lastRestatementKey: string | undefined, lastRestatementAt = -Infinity;
  const judgeSentence = async (sessionId: string, sentence: string) => {
    // Every sentence is judged, off the audio path: a claim needs no particular words to be false.
    const conversation = recent.slice(-8, -1).join('\n');
    const started = now();
    // A judge call that returns nothing (timeout, provider refusal, unreadable answer) is retried.
    const ask = async (described: string): Promise<JudgeVerdict | undefined> => {
      for (let attempt = 0; ; attempt++) {
        try { const value = await options.judge({ tasks: described, conversation, sentence }); if (value) return value; }
        catch { /* Retried below. */ }
        if (stopped || attempt >= JUDGE_RETRY_MS.length) return undefined;
        await new Promise<void>(resolve => { const timer = setTimeout(resolve, JUDGE_RETRY_MS[attempt]); timer.unref?.(); });
      }
    };
    // A result that lands while the judge runs makes its verdict stale: judge the new list again, up
    // to three rounds. A list that keeps changing keeps the latest verdict; it is never dropped.
    let verdict: JudgeVerdict | undefined, tasks = truth(), described = describeTasks(tasks);
    for (let round = 0; round < 3; round++) {
      verdict = await ask(described);
      tasks = truth();
      const latest = describeTasks(tasks);
      if (!verdict || latest === described) break;
      described = latest;
    }
    const judgeMs = now() - started;
    options.onJudged?.({ sessionId, sentence, verdict, judgeMs });
    if (stopped) return;
    const visible = options.tasks();
    const contradiction = Boolean(verdict && verdict.contradicts >= threshold
      && ['false_success', 'false_failure', 'phantom_step'].includes(verdict.kind));
    let kind: GuardrailKind;
    if (contradiction) {
      // The judge also found a listed status or result the sentence matches (a finished sale beside an
      // open cancel): restate the list neutrally rather than accuse, so nothing true is retracted.
      kind = (verdict!.supported ?? 0) >= 0.5 ? 'ambiguous' : verdict!.kind as GuardrailKind;
    } else if (!verdict && claimsOutcome(sentence) && (visible.length || concernsMoney(sentence))) kind = 'unverified';
    else return;
    const text = correctionText(kind, visible, sentence);
    // The most recent request started on this provider session is the one being discussed.
    const task = [...tasks].reverse().find(value => value.sessionId === sessionId && value.delegationId);
    const delegationId = task?.delegationId ?? null;
    let status: GuardrailTrigger['correction'];
    const restating = kind === 'ambiguous' || kind === 'unverified';
    // A no-verdict restatement with the same content (in any task order) adds nothing the voice was
    // not just given, and those restatements are rate-limited: each can cut into Dexter's speech. A
    // detected contradiction, ambiguous included, is a real false claim: only the cooldown applies.
    if (kind === 'unverified' && restatementKey(visible) === lastRestatementKey) status = 'suppressed_duplicate';
    else if (kind === 'unverified' && now() - lastRestatementAt < RESTATEMENT_INTERVAL_MS) status = 'suppressed_rate';
    else if (now() - lastCorrectionAt < CORRECTION_COOLDOWN_MS) status = 'suppressed_cooldown';
    else {
      // Reserve the cooldown and the restatement before awaiting the append, so sentences judged at
      // the same moment cannot both go out; a correction the voice did not accept releases them again.
      const previous = { correction: lastCorrectionAt, key: lastRestatementKey, at: lastRestatementAt };
      lastCorrectionAt = now();
      if (restating) { lastRestatementKey = restatementKey(visible); lastRestatementAt = now(); }
      try { status = (await options.correct(text, delegationId, sessionId)).status; } catch { status = 'unconfirmed'; }
      if (status !== 'accepted') {
        lastCorrectionAt = previous.correction;
        if (restating) { lastRestatementKey = previous.key; lastRestatementAt = previous.at; }
      }
    }
    const trigger: GuardrailTrigger = { at: new Date().toISOString(), sessionId, delegationId, kind,
      probability: verdict?.contradicts ?? 0, sentence: sentence.slice(0, 300), judgeMs, correction: status };
    try { console.info(JSON.stringify({ event: 'broker_voice_guardrail', callId: options.callId, ...trigger })); } catch { /* Telemetry only. */ }
    try { options.record?.(trigger); } catch { /* Evidence only; never affects the call. */ }
  };
  const flush = () => {
    clearTimeout(flushTimer); flushTimer = undefined;
    const value = buffer; buffer = undefined;
    if (!value || stopped) return;
    const sentence = value.text.trim();
    if (!sentence) return;
    recent.push(`Dexter: ${sentence}`);
    void judgeSentence(value.sessionId, sentence);
  };
  return {
    /** Feed each transcript snapshot of a provider session; work runs asynchronously, off the audio path. */
    observe(snapshot: LiveTranscriptSnapshot, sessionId: string) {
      if (stopped) return;
      const from = cursors.get(sessionId) ?? 0;
      for (const fragment of snapshot.fragments) {
        if (fragment.sequence <= from) continue;
        cursors.set(sessionId, fragment.sequence);
        if (fragment.role === 'user') {
          flush();
          const last = recent.at(-1);
          if (last?.startsWith('Caller: ')) recent[recent.length - 1] = last + fragment.text; else recent.push(`Caller:${fragment.text}`);
          if (recent.length > 24) recent.splice(0, recent.length - 24);
          continue;
        }
        if (buffer && (buffer.sessionId !== sessionId || fragment.startMs - buffer.endMs > 1200)) flush();
        buffer = buffer ? { ...buffer, text: buffer.text + fragment.text, endMs: fragment.endMs }
          : { sessionId, text: fragment.text, endMs: fragment.endMs };
        if (/[.!?]["')\]]?\s*$/.test(fragment.text)) flush();
      }
      clearTimeout(flushTimer);
      if (buffer) { flushTimer = setTimeout(flush, 1200); flushTimer.unref?.(); }
    },
    stop() { stopped = true; clearTimeout(flushTimer); buffer = undefined; },
  };
}
export type VoiceGuardrail = ReturnType<typeof createVoiceGuardrail>;

export const JUDGE_INSTRUCTIONS = "Judge only DEXTER'S LATEST SENTENCE against the TASK LIST. True if it states or clearly implies something about one of the caller's requests that the task list does not support: that a request finished, succeeded, went through, was placed, sold, bought, sent or cancelled while the list shows it in progress, failed or never started; that it failed or did not happen while the list shows it succeeded; or that a step or request is running or has started that is not on the list (for example a buy-back that was never started). False for: conversation unrelated to the caller's requests; accurate restatements of the list or of a listed result; saying a listed request is in progress, being placed, being checked, or will be reported; promises to report back; questions or confirmation prompts to the caller; and portfolio or history facts that are not claimed as the result of a request on this call.";

const SUPPORTED_INSTRUCTIONS = "True if DEXTER'S LATEST SENTENCE agrees with the TASK LIST: what it says about the caller's requests matches the status or the result of at least one listed request (any request on the list, not only the latest), or it makes no claim about a request. For example, \"the sale went through\" agrees when any listed request's result says the sale went through, even while a later request, such as a cancellation, is still in progress.";

/** A small, fast judge: OpenAI Decisions API (gpt-6-luna), bounded by a short timeout. */
export function createDecisionsJudge(apiKey: string, fetchImpl: typeof fetch = fetch, timeoutMs = 2_500,
  instructions = JUDGE_INSTRUCTIONS): GuardrailJudge {
  return async ({ tasks, conversation, sentence }) => {
    let response: Response;
    try { response = await fetchImpl('https://api.openai.com/v1/decisions', { method: 'POST',
      headers: { authorization: `Bearer ${apiKey}`, 'content-type': 'application/json' }, signal: AbortSignal.timeout(timeoutMs),
      body: JSON.stringify({ model: 'gpt-6-luna',
        input: `TASK LIST (application state; the only truth about the caller's requests on this call):\n${tasks}\n\nRECENT CONVERSATION:\n${conversation || '(none)'}\n\nDEXTER'S LATEST SENTENCE:\n"${sentence}"`,
        questions: [
          { type: 'predicate', name: 'contradicts', instructions },
          { type: 'predicate', name: 'supported', instructions: SUPPORTED_INSTRUCTIONS },
          { type: 'choice', name: 'kind', instructions: "Classify DEXTER'S LATEST SENTENCE against the TASK LIST.", choices: [
            { value: 'none', description: 'No claim about a request, or a claim the task list supports.' },
            { value: 'false_success', description: 'Says a request finished, succeeded, went through or was cancelled, but the list does not show that.' },
            { value: 'false_failure', description: 'Says a request failed or did not happen, but the list shows it succeeded or still in progress.' },
            { value: 'phantom_step', description: 'Says a step or request is running or has started that the list does not contain.' },
            { value: 'stale_pending', description: 'Says a request is still in progress, but the list shows it has finished.' }] }] }) }); }
    catch (error) {
      try { console.error(JSON.stringify({ event: 'broker_voice_guardrail_judge_failed', status: null,
        error: (error as Error)?.name === 'TimeoutError' ? 'timeout' : 'request_failed' })); } catch { /* Telemetry only. */ }
      return undefined;
    }
    if (!response.ok) {
      let body = '';
      try { body = (await response.text()).slice(0, 2000); } catch { /* Unreadable. */ }
      try { console.error(JSON.stringify({ event: 'broker_voice_guardrail_judge_failed', status: response.status,
        date: response.headers.get('date'), body })); } catch { /* Telemetry only. */ }
      return undefined;
    }
    const body = await response.json() as { answers?: Array<{ name?: string; type?: string; probability?: number; choice?: string;
      probabilities?: Array<{ value: string; probability: number }> }> };
    const contradicts = body.answers?.find(answer => answer.name === 'contradicts');
    const supported = body.answers?.find(answer => answer.name === 'supported');
    const kind = body.answers?.find(answer => answer.name === 'kind');
    if (typeof contradicts?.probability !== 'number' || typeof kind?.choice !== 'string') return undefined;
    return { contradicts: contradicts.probability, kind: kind.choice as JudgeVerdict['kind'],
      kindProbability: kind.probabilities?.find(value => value.value === kind.choice)?.probability ?? 0,
      ...(typeof supported?.probability === 'number' ? { supported: supported.probability } : {}) };
  };
}
