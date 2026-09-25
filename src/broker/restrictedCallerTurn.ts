import { brokerIdentity } from './backend.js';
import { frozenCallerTurnSchema, type FrozenCallerTurn } from './callerTurn.js';
import type { LiveBackendChannel, LiveDelegationRequest, LiveTranscriptSnapshot } from '../live/types.js';

export interface RestrictedCallerFrontier {
  sessionId: string;
  audioThroughMs: number | undefined;
  observedSpeechAtMs: number | undefined;
}
export interface RestrictedCallerTurnOptions {
  callId: string; sessionId: string; mediaStartedAtMs: number; deadlineAtMs: number;
  stabilityMs: number; maxWaitMs: number; now?: () => number;
  frontier(): RestrictedCallerFrontier;
  subscribeSpeech(listener: () => void): () => void;
  isCurrent(): boolean;
}
/** Same 1200ms application grouping as the existing backend. It is source
 * selection for an explicit delegation, never a provider turn-complete event. */
function callerGroups(snapshot: LiveTranscriptSnapshot) {
  const fragments = snapshot.fragments.filter(fragment => fragment.role === 'user')
    .sort((a, b) => a.startMs - b.startMs || a.sequence - b.sequence);
  const groups: typeof fragments[] = [];
  for (const fragment of fragments) {
    const previous = groups.at(-1);
    if (!previous || fragment.startMs - Math.max(...previous.map(part => part.endMs)) > 1200) groups.push([fragment]);
    else previous.push(fragment);
  }
  return groups.map(parts => parts.sort((a, b) => a.sequence - b.sequence));
}
/** Context-free source admission for restricted voice. No Primary binding, capture
 * HTTP or journal transcript is created. The effect owner persists only safe
 * operation/turn identity after its finite setup decision and current-frontier check. */
export function createRestrictedCallerTurnFreezer(options: RestrictedCallerTurnOptions) {
  const now = options.now ?? Date.now;
  if (![options.mediaStartedAtMs, options.deadlineAtMs, options.stabilityMs, options.maxWaitMs].every(Number.isSafeInteger)
    || options.deadlineAtMs <= options.mediaStartedAtMs || options.stabilityMs < 1
    || options.maxWaitMs < options.stabilityMs || options.maxWaitMs > 30_000) throw new Error('invalid_restricted_caller_clock');
  async function freeze(request: LiveDelegationRequest, channel: Pick<LiveBackendChannel, 'getContext' | 'subscribeContext' | 'signal'>,
    usedSourceIds: readonly string[]): Promise<FrozenCallerTurn | undefined> {
    if (request.callId !== options.callId || request.sessionId !== options.sessionId || !options.isCurrent()
      || channel.signal.aborted || now() >= options.deadlineAtMs) return;
    const originalFrontier = options.frontier();
    if (originalFrontier.sessionId !== request.sessionId || originalFrontier.audioThroughMs === undefined) return;
    const snapshot = await new Promise<LiveTranscriptSnapshot | undefined>(resolve => {
      let ended = false, timer: ReturnType<typeof setTimeout> | undefined;
      let revision = channel.getContext().callerRevision, changedAt = now();
      let unsubscribeContext = () => {}, unsubscribeSpeech = () => {};
      const finish = (value?: LiveTranscriptSnapshot) => {
        if (ended) return; ended = true; clearTimeout(timer); clearTimeout(limit);
        unsubscribeContext(); unsubscribeSpeech(); channel.signal.removeEventListener('abort', abort); resolve(value);
      };
      const schedule = () => {
        if (ended) return;
        if (!options.isCurrent() || channel.signal.aborted || now() >= options.deadlineAtMs) { finish(); return; }
        clearTimeout(timer);
        const value = channel.getContext();
        if (value.callerRevision !== revision) { revision = value.callerRevision; changedAt = now(); }
        const frontier = options.frontier();
        if (frontier.sessionId !== request.sessionId) { finish(); return; }
        timer = setTimeout(() => {
          const current = channel.getContext(), latest = options.frontier();
          if (!options.isCurrent() || channel.signal.aborted || now() >= options.deadlineAtMs) { finish(); return; }
          if (current.callerRevision !== revision || now() - Math.max(changedAt, latest.observedSpeechAtMs ?? 0) < options.stabilityMs) {
            schedule(); return;
          }
          if (current.fragments.some(fragment => fragment.role === 'user' && fragment.text.trim())) finish(current);
        }, Math.max(1, options.stabilityMs - (now() - Math.max(changedAt, frontier.observedSpeechAtMs ?? 0))));
      };
      const abort = () => finish();
      const limit = setTimeout(() => finish(), Math.max(1, Math.min(options.maxWaitMs, options.deadlineAtMs - now())));
      unsubscribeContext = channel.subscribeContext(schedule); unsubscribeSpeech = options.subscribeSpeech(schedule);
      channel.signal.addEventListener('abort', abort, { once: true }); schedule();
    });
    if (!snapshot || channel.signal.aborted || !options.isCurrent() || now() >= options.deadlineAtMs) return;
    const frontier = options.frontier();
    if (frontier.sessionId !== request.sessionId || frontier.audioThroughMs === undefined || frontier.observedSpeechAtMs === undefined
      || frontier.audioThroughMs !== originalFrontier.audioThroughMs || frontier.observedSpeechAtMs !== originalFrontier.observedSpeechAtMs) return;
    const groups = callerGroups(snapshot);
    if (groups.some((group, index) => index > 0 && group[0].sequence <= groups[index - 1][0].sequence)) return;
    const candidate = groups.filter(parts => {
      const first = parts[0], source = brokerIdentity('source', options.callId, request.sessionId, first.eventId);
      const present = request.transcript.fragments.some(fragment => fragment.role === 'user'
        && fragment.eventId === first.eventId && fragment.sequence === first.sequence);
      return (present || first.startMs <= request.offsetMs) && !usedSourceIds.includes(source);
    }).at(-1);
    if (!candidate || snapshot.fragments.some(fragment => fragment.role === 'user'
      && fragment.sequence > Math.max(...candidate.map(part => part.sequence)))) return;
    const first = candidate[0], source = brokerIdentity('source', options.callId, request.sessionId, first.eventId);
    const frozen = frozenCallerTurnSchema.safeParse({ sessionId: request.sessionId, firstEventId: first.eventId,
      callerRevision: snapshot.callerRevision, firstSequence: first.sequence, lastSequence: Math.max(...candidate.map(part => part.sequence)),
      startMs: first.startMs, endMs: Math.max(...candidate.map(part => part.endMs)), audioThroughMs: frontier.audioThroughMs,
      observedSpeechAtMs: frontier.observedSpeechAtMs,
      turn: { sourceEventId: source, revision: 1, itemId: `live-turn-${source}`, role: 'user',
        text: candidate.map(part => part.text).join(''), occurredAt: new Date(options.mediaStartedAtMs + first.startMs).toISOString() } });
    return frozen.success ? frozen.data : undefined;
  }
  return { freeze };
}
