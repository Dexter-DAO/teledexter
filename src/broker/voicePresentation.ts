import type { BrokerCall } from './store.js';
import type { BrokerDelegationRecord } from './backend.js';
import type { LiveBackendUpdate } from '../live/types.js';

export function carrierPlaybackState(connected: boolean, sent: number, played: number, cleared: number): 'idle' | 'pending' | 'unavailable' {
  if (!connected || ![sent, played, cleared].every(value => Number.isSafeInteger(value) && value >= 0)
    || played + cleared > sent) return 'unavailable';
  return sent === played + cleared ? 'idle' : 'pending';
}

/** This is a presentation frontier, never caller consent or task admission. */
export function callerPresentationFrontier(call: BrokerCall): string {
  return JSON.stringify((call.liveGenerations?.segments ?? []).flatMap(segment => {
    const last = segment.transcript.fragments.filter(fragment => fragment.role === 'user').at(-1);
    return last ? [[segment.generationId, last.sequence, segment.transcript.callerRevision]] : [];
  }));
}

function sourcePosition(call: BrokerCall, entry: BrokerDelegationRecord): [number, number] {
  const position = (session: string, sequence: number): [number, number] =>
    [call.liveGenerations?.segments.find(segment => segment.sessionId === session)?.ordinal ?? 0, sequence];
  const source = call.liveGenerations?.segments.find(segment => segment.sessionId === entry.sessionId);
  const frozenLast = source?.transcript.fragments.filter(fragment => fragment.role === 'user')[entry.frozenCallerRevision - 1];
  const originalLast = frozenLast ? Math.min(entry.lastSequence, frozenLast.sequence) : entry.lastSequence;
  return Object.values(entry.questions ?? {}).reduce<[number, number]>((latest, question) => {
    const reply = question.reply;
    if (reply?.status !== 'accepted') return latest;
    const next = position(reply.sourceSessionId ?? entry.sessionId, reply.lastSequence);
    return next[0] > latest[0] || (next[0] === latest[0] && next[1] > latest[1]) ? next : latest;
  }, position(entry.sessionId, originalLast));
}

export function hasLaterCallerSpeech(call: BrokerCall, entry: BrokerDelegationRecord): boolean {
  const [ordinal, sequence] = sourcePosition(call, entry);
  return (call.liveGenerations?.segments ?? []).some(segment =>
    segment.ordinal >= ordinal && segment.transcript.fragments.some(fragment => fragment.role === 'user'
      && (segment.ordinal > ordinal || fragment.sequence > sequence)));
}

export function hasNewerCallerWork(call: BrokerCall, entry: BrokerDelegationRecord): boolean {
  const [ordinal, sequence] = sourcePosition(call, entry);
  const later = (session: string, first: number) => {
    const order = call.liveGenerations?.segments.find(segment => segment.sessionId === session)?.ordinal ?? 0;
    return order > ordinal || (order === ordinal && first > sequence);
  };
  return Object.values(call.delegations).some(raw => {
    const other = raw as BrokerDelegationRecord;
    return (other.requestId !== entry.requestId && later(other.sessionId, other.firstSequence))
      || other.deferredTurns?.some(turn => turn.status === 'queued'
        && later(turn.sourceSessionId ?? other.sessionId, turn.firstSequence));
  });
}

/** Only local observation and queue facts; none of these states authorize an action. */
export function pendingRequestState(entry: BrokerDelegationRecord): string {
  return JSON.stringify([entry.status, entry.resultReadUnavailable === true, entry.submitError?.code ?? null,
    (entry.deferredTurns ?? []).filter(turn => turn.status === 'queued').map(turn => turn.sourceEventId),
    entry.currentQuestionId ?? null]);
}

export function pendingRequestSpeech(entry: BrokerDelegationRecord, earlier: boolean): string {
  const queued = entry.deferredTurns?.some(turn => turn.status === 'queued');
  if (entry.resultReadUnavailable) return queued
    ? 'I cannot get the latest status of the earlier request. Your later requests are queued and have not started.'
    : 'I cannot get the latest status of your request. Its outcome is still unconfirmed.';
  if (queued) return 'Your later requests are queued behind the earlier request. They have not started.';
  if (['prepared', 'dispatching', 'dispatch_unknown'].includes(entry.status)) {
    if (entry.status === 'dispatch_unknown' && entry.submitError?.code === 'http_error')
      return 'My connection to your account tools returned an error. I am checking whether your request was saved.';
    return earlier ? 'I am still checking whether your earlier request was saved.'
      : 'I have not confirmed whether your request was saved. I am checking the original request.';
  }
  return earlier ? 'I am still waiting for the result of your earlier request.'
    : 'Your request is still being handled. I am waiting for its result.';
}

export function isFixedPendingStatus(entry: BrokerDelegationRecord, update?: LiveBackendUpdate): boolean {
  return !!update && update.kind === 'progress'
    && [pendingRequestSpeech(entry, false), pendingRequestSpeech(entry, true)].includes(update.text)
    && ['prepared', 'dispatching', 'dispatch_unknown', 'queued', 'running'].includes(entry.status)
    && update.id.startsWith(`${entry.requestId}_`);
}

export function failedRequestSpeech(entry: BrokerDelegationRecord): string {
  return entry.primaryResult?.error?.code === 'primary_financial_result_unknown'
    ? "I couldn't confirm the trade request. We need to check that same request before continuing."
    : 'I could not get an answer to that request.';
}
