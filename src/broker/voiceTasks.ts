import type { BrokerCall } from './store.js';
import type { BrokerDelegationRecord } from './backend.js';
import type { BrokerLiveSession, LiveDelivery } from '../live/types.js';
import { LIVE_APPEND_MAX_BYTES } from '../live/session.js';
import { cancelledRequestSpeech, failedRequestSpeech, requestFailureSpeech } from './voicePresentation.js';

/** One caller request on this call, as the voice may describe it. */
export interface VoiceTask {
  requestId: string;
  /** The provider session and delegation that started it; updates about it carry this ID. */
  sessionId: string;
  delegationId?: string;
  /** The caller turn that started it; its words reach a voice only where that voice may see them. */
  sourceEventId: string;
  status: 'pending' | 'waiting_to_start' | 'waiting_for_answer' | 'finished' | 'failed' | 'cancelled' | 'not_started';
  request: string;
  progress?: string;
  outcome?: string;
}

const clip = (text: string, limit: number) => {
  const value = text.replace(/\s+/g, ' ').trim();
  if (value.length <= limit) return value;
  const half = Math.floor((limit - 3) / 2);
  return `${value.slice(0, half).trimEnd()} … ${value.slice(-half).trimStart()}`;
};
/** The opening sentences of a result, whole, within the limit. */
const lead = (text: string, limit: number) => {
  const sentences = text.replace(/\s+/g, ' ').trim().split(/(?<=[.!?])\s+/);
  let out = '';
  for (const sentence of sentences) {
    if ((out + ' ' + sentence).trim().length > limit) break;
    out = (out + ' ' + sentence).trim();
  }
  return out || clip(text, limit);
};

/**
 * The application's task list: a projection of this call's saved request records. It states a
 * finished request's result only once that result has been delivered to the voice.
 */
export function callTasks(call: BrokerCall): VoiceTask[] {
  const tasks: VoiceTask[] = [];
  for (const raw of Object.values(call.delegations)) {
    const entry = raw as BrokerDelegationRecord;
    if (!entry?.submission) continue;
    const base = { requestId: entry.requestId, sessionId: entry.sessionId, delegationId: entry.delegationIds?.[0],
      sourceEventId: entry.submission.source.sourceEventId, request: clip(entry.submission.text, 120) };
    const partial = entry.primaryResult?.partialResult?.text;
    const pending = (): VoiceTask => {
      const pause = requestFailureSpeech(entry);
      return { ...base, status: 'pending', ...(entry.resultReadUnavailable
        ? { progress: 'The latest status check failed, so the outcome is not known yet.' }
        : pause ? { progress: pause }
          : partial ? { progress: clip(partial, 180) }
            : entry.status === 'dispatch_unknown' ? { progress: 'Not yet confirmed that the request was received.' } : {}) };
    };
    switch (entry.status) {
      case 'completed':
        // Primary's words reach the voice only through the eligible delivery. Until the voice has
        // the result itself, its list keeps the request in progress: a list announcing a result the
        // voice did not have yet prompted it to talk about the wait (eval status_pending).
        tasks.push(entry.delivery === 'accepted'
          ? { ...base, status: 'finished', outcome: lead(entry.primaryResult?.assistant?.text ?? '', 260) } : pending()); break;
      case 'failed':
        tasks.push({ ...base, status: 'failed', outcome: lead(failedRequestSpeech(entry), 260) }); break;
      case 'cancelled':
        tasks.push({ ...base, status: 'cancelled', outcome: lead(cancelledRequestSpeech(entry), 260) }); break;
      case 'not_admitted': case 'admission_refused': case 'admission_expired':
        tasks.push({ ...base, status: 'not_started', outcome: 'This request was not started.' }); break;
      case 'waiting_for_reply': {
        const question = entry.currentQuestionId ? entry.questions?.[entry.currentQuestionId]?.question.question : undefined;
        tasks.push({ ...base, status: 'waiting_for_answer', ...(question ? { progress: `Primary asked: ${clip(question, 160)}` } : {}) });
        break;
      }
      default: tasks.push(pending());
    }
    for (const turn of entry.deferredTurns ?? [])
      if (turn.status === 'queued') tasks.push({ requestId: turn.queuedRequest?.requestId ?? turn.sourceEventId,
        sessionId: turn.sourceSessionId ?? entry.sessionId, delegationId: turn.delegationId, sourceEventId: turn.sourceEventId,
        status: 'waiting_to_start', request: clip(turn.text, 120) });
  }
  return tasks;
}

const phrase: Record<VoiceTask['status'], string> = {
  pending: 'In progress, no result yet', waiting_to_start: 'Not started yet; it starts after the earlier request finishes',
  waiting_for_answer: "Waiting for the caller's answer",
  finished: 'Finished', failed: 'Failed', cancelled: 'Cancelled', not_started: 'Not started',
};
export function taskLine(task: VoiceTask, index: number, version: number, showRequest = true): string {
  const detail = task.outcome ? ` Result: ${task.outcome}` : task.progress ? ` Progress, not a result: ${task.progress}` : '';
  const asked = showRequest ? ` Caller asked: "${task.request}".` : ' An earlier request.';
  let line = `Task list ${version}, task ${index + 1}: ${phrase[task.status]}.${asked}${detail}`;
  while (Buffer.byteLength(line) > LIVE_APPEND_MAX_BYTES) line = `${line.slice(0, Math.floor(line.length * 0.9)).trimEnd()} …`;
  return line;
}
/** Header first, then one bounded line per task; the newest list replaces every earlier one. */
export function taskListLines(tasks: readonly VoiceTask[], version: number, visible: (task: VoiceTask) => boolean = () => true): string[] {
  const shown = tasks.slice(-6);
  const header = tasks.length
    ? `Task list ${version} replaces all earlier task lists. The caller has ${tasks.length} request${tasks.length === 1 ? '' : 's'} on this call${tasks.length > shown.length ? `; the ${shown.length} most recent follow` : ''}. Nothing else has been started.`
    : `Task list ${version}: the caller has made no requests on this call yet. Nothing has been started.`;
  return [header, ...shown.map((task, index) => taskLine(task, index + tasks.length - shown.length, version, visible(task)))];
}

/** Sends the task list to the attached voice as silent context whenever it changes. */
export function createVoiceTasks(load: () => BrokerCall | undefined, onSent?: (lines: string[], deliveries: LiveDelivery[]) => void) {
  let voice: BrokerLiveSession | undefined, sent = '', version = 0, timer: ReturnType<typeof setTimeout> | undefined, stopped = false;
  let visible: (task: VoiceTask) => boolean = () => true;
  const flush = () => {
    timer = undefined;
    const call = load(), target = voice;
    if (!call || !target || stopped) return;
    const tasks = callTasks(call);
    // Before the first request there is nothing to say: no list means nothing has been started.
    if (!tasks.length && !version) return;
    const key = JSON.stringify(tasks.map(task => [task.status, task.request, task.progress ?? null, task.outcome ?? null, visible(task)]));
    if (key === sent) return;
    sent = key;
    const lines = taskListLines(tasks, ++version, visible);
    void Promise.all(lines.map(line => target.appendContext(line).catch((): LiveDelivery => ({ eventId: '', status: 'unconfirmed' }))))
      .then(deliveries => {
        // A lost list is sent again on the next change or attachment.
        if (deliveries.some(delivery => delivery.status !== 'accepted') && voice === target) sent = '';
        onSent?.(lines, deliveries);
      });
  };
  const refresh = () => { if (!stopped && voice && !timer) { timer = setTimeout(flush, 150); timer.unref?.(); } };
  return {
    /** Coalesce bursts of saves (dispatching, running, partial) into one list. */
    refresh,
    /** A voice sees a request's words only where it may: its own session, or source admitted into its history. */
    attach(next: BrokerLiveSession, canSee: (task: VoiceTask) => boolean = () => true) {
      if (stopped || voice === next) return;
      voice = next; visible = canSee; sent = ''; clearTimeout(timer); timer = undefined; flush();
    },
    detach(previous: BrokerLiveSession) { if (voice === previous) { voice = undefined; clearTimeout(timer); timer = undefined; } },
    stop() { stopped = true; voice = undefined; clearTimeout(timer); },
  };
}
export type VoiceTasks = ReturnType<typeof createVoiceTasks>;
