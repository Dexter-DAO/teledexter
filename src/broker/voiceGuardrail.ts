import type { LiveDelivery, LiveTranscriptSnapshot } from '../live/types.js';
import type { VoiceTask } from './voiceTasks.js';

/**
 * Monitor-and-correct guardrail (developers.openai.com/api/docs/guides/voice-server-controls,
 * "Apply conversation guardrails"). It reads assistant transcript sentences on the primary
 * connection, asks a judge whether a sentence contradicts the application's task list, and
 * appends an application-authored correction. It never touches audio, playback or the call.
 */
export type GuardrailKind = 'false_success' | 'false_failure' | 'phantom_step';
export interface JudgeVerdict { contradicts: number; kind: GuardrailKind | 'none' | 'stale_pending'; kindProbability: number }
export type GuardrailJudge = (input: { tasks: string; conversation: string; sentence: string }) => Promise<JudgeVerdict | undefined>;
export interface GuardrailTrigger {
  at: string; sessionId: string; delegationId: string | null; kind: GuardrailKind; probability: number;
  sentence: string; judgeMs: number; correction: LiveDelivery['status'] | 'suppressed_cooldown';
}

/** Words that can carry a claim about a request's progress or outcome. Anything else is ordinary conversation. */
const TASK_TERMS = /\b(?:sell|sold|sale|selling|buy|bought|buying|purchase[ds]?|order(?:ed|s)?|trade[ds]?|trading|swap(?:ped|s)?|send|sent|sending|transfer(?:red|s)?|deposit(?:ed)?|withdr[ae]w\w*|cancel\w*|done|complete[ds]?|completing|finished|through|placed|placing|submitted|confirmed|executed|filled|failed|fail|started|starting|on it|working|in progress|pending|back|went|go ahead|checking)\b/i;
/** Whether a sentence can carry a claim about a request; only these reach the judge. */
export const concernsTasks = (sentence: string) => TASK_TERMS.test(sentence);
const CORRECTION_COOLDOWN_MS = 4_000;
const clipText = (text: string, limit: number) => text.length <= limit ? text : `${text.slice(0, limit - 1).trimEnd()}…`;
const state: Record<VoiceTask['status'], string> = {
  starting: 'just passed on to Primary, not started yet', pending: 'in progress with no result yet',
  arriving: 'finished, and its result is being given to you now (you do not have it yet)',
  waiting_to_start: 'waiting to start after the earlier request', waiting_for_answer: "waiting for the caller's answer",
  finished: 'finished',
  failed: 'failed', cancelled: 'cancelled', not_started: 'not started',
};
const statusLine = (task: VoiceTask) => `${state[task.status]}${task.outcome ? `; its result: ${clipText(task.outcome, 150)}`
  : task.progress ? `; progress only: ${clipText(task.progress, 90)}` : ''}`;

/** Application-authored and within one append: no caller words, only statuses and Primary's results. */
export function correctionText(kind: GuardrailKind, tasks: readonly VoiceTask[]): string {
  const what = kind === 'phantom_step' ? 'You described a step or request that has not been started.'
    : kind === 'false_success' ? 'You said something finished or went through that has not.'
      : 'You said something failed or did not happen that the task list shows otherwise.';
  const latest = tasks.slice(-2);
  const end = (text: string) => /[.!?…]$/.test(text) ? text : `${text}.`;
  let truth = latest.length ? latest.map((task, index) => end(`${latest.length === 2 && index === 0 ? 'The earlier request' : 'The latest request'} is ${statusLine(task)}`)).join(' ')
    : 'There are no requests on this call; nothing has been started.';
  const build = () => `Correction: ${what} The task list says: ${truth} Say now, in one short sentence, that you misspoke and give this status. Do not describe any step that is not on the task list.`;
  while (Buffer.byteLength(build()) > 500 && truth.length > 40) truth = `${truth.slice(0, Math.floor(truth.length * 0.85)).trimEnd()}…`;
  return build();
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
  const judgeSentence = async (sessionId: string, sentence: string) => {
    if (!concernsTasks(sentence)) return;
    const conversation = recent.slice(-8, -1).join('\n');
    const started = now();
    let verdict: JudgeVerdict | undefined, tasks = truth(), described = describeTasks(tasks);
    // A result that lands while the judge runs makes its verdict stale: judge again on the new truth.
    for (let attempt = 0; attempt < 2; attempt++) {
      try { verdict = await options.judge({ tasks: described, conversation, sentence }); }
      catch { verdict = undefined; }
      tasks = truth();
      const latest = describeTasks(tasks);
      if (latest === described) break;
      described = latest; verdict = undefined;
    }
    const judgeMs = now() - started;
    options.onJudged?.({ sessionId, sentence, verdict, judgeMs });
    if (stopped || !verdict || verdict.contradicts < threshold
      || !['false_success', 'false_failure', 'phantom_step'].includes(verdict.kind)) return;
    const kind = verdict.kind as GuardrailKind;
    // The most recent request started on this provider session is the one being discussed.
    const task = [...tasks].reverse().find(value => value.sessionId === sessionId && value.delegationId);
    const delegationId = task?.delegationId ?? null;
    const correction = now() - lastCorrectionAt < CORRECTION_COOLDOWN_MS ? 'suppressed_cooldown' as const : undefined;
    let status: GuardrailTrigger['correction'] = correction ?? 'unconfirmed';
    if (!correction) {
      lastCorrectionAt = now();
      const text = correctionText(kind, options.tasks());
      try { status = (await options.correct(text, delegationId, sessionId)).status; } catch { status = 'unconfirmed'; }
    }
    const trigger: GuardrailTrigger = { at: new Date().toISOString(), sessionId, delegationId, kind,
      probability: verdict.contradicts, sentence: sentence.slice(0, 300), judgeMs, correction: status };
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

/** A small, fast judge: OpenAI Decisions API (gpt-6-luna), bounded by a short timeout. */
export function createDecisionsJudge(apiKey: string, fetchImpl: typeof fetch = fetch, timeoutMs = 2_500,
  instructions = JUDGE_INSTRUCTIONS): GuardrailJudge {
  return async ({ tasks, conversation, sentence }) => {
    const response = await fetchImpl('https://api.openai.com/v1/decisions', { method: 'POST',
      headers: { authorization: `Bearer ${apiKey}`, 'content-type': 'application/json' }, signal: AbortSignal.timeout(timeoutMs),
      body: JSON.stringify({ model: 'gpt-6-luna',
        input: `TASK LIST (application state; the only truth about the caller's requests on this call):\n${tasks}\n\nRECENT CONVERSATION:\n${conversation || '(none)'}\n\nDEXTER'S LATEST SENTENCE:\n"${sentence}"`,
        questions: [
          { type: 'predicate', name: 'contradicts', instructions },
          { type: 'choice', name: 'kind', instructions: "Classify DEXTER'S LATEST SENTENCE against the TASK LIST.", choices: [
            { value: 'none', description: 'No claim about a request, or a claim the task list supports.' },
            { value: 'false_success', description: 'Says a request finished, succeeded, went through or was cancelled, but the list does not show that.' },
            { value: 'false_failure', description: 'Says a request failed or did not happen, but the list shows it succeeded or still in progress.' },
            { value: 'phantom_step', description: 'Says a step or request is running or has started that the list does not contain.' },
            { value: 'stale_pending', description: 'Says a request is still in progress, but the list shows it has finished.' }] }] }) });
    if (!response.ok) return undefined;
    const body = await response.json() as { answers?: Array<{ name?: string; type?: string; probability?: number; choice?: string;
      probabilities?: Array<{ value: string; probability: number }> }> };
    const contradicts = body.answers?.find(answer => answer.name === 'contradicts');
    const kind = body.answers?.find(answer => answer.name === 'kind');
    if (typeof contradicts?.probability !== 'number' || typeof kind?.choice !== 'string') return undefined;
    return { contradicts: contradicts.probability, kind: kind.choice as JudgeVerdict['kind'],
      kindProbability: kind.probabilities?.find(value => value.value === kind.choice)?.probability ?? 0 };
  };
}
