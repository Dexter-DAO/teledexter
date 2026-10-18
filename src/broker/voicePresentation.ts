import type { BrokerCall } from './store.js';
import type { BrokerDelegationRecord } from './backend.js';
import type { LiveBackendUpdate } from '../live/types.js';
import { LIVE_APPEND_MAX_BYTES } from '../live/session.js';
import type { EligiblePhoneMessage } from './eligibleContext.js';

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

/** Correlate the task using only source text admitted into this provider session. */
export function requestPresentationContext(entry: BrokerDelegationRecord, messages: readonly EligiblePhoneMessage[]): string[] {
  const source = messages.find(message => message.role === 'user'
    && message.canonicalSource?.sourceEventId === entry.submission.source.sourceEventId
    && message.canonicalSource.revision === entry.submission.source.revision);
  if (!source) return [JSON.stringify({ kind: 'task_result_context', requestId: entry.requestId,
    state: entry.status, sourceAvailable: false })];
  const chunks: string[] = [];
  const render = (text: string, part: number) => JSON.stringify({ kind: 'task_result_context',
    requestId: entry.requestId, state: entry.status, part, sourceAvailable: true, requestText: text });
  let current = '';
  for (const character of source.text) {
    if (Buffer.byteLength(render(current + character, chunks.length + 1)) > LIVE_APPEND_MAX_BYTES) {
      if (!current) throw new Error('task_context_exceeds_voice_limit');
      chunks.push(render(current, chunks.length + 1)); current = '';
    }
    current += character;
  }
  if (current) chunks.push(render(current, chunks.length + 1));
  return chunks;
}

export function requestFailureSpeech(entry: BrokerDelegationRecord): string | undefined {
  const failure = entry.primaryResult?.failure;
  if (!failure) return;
  if (failure.kind === 'provider_cooldown') {
    const resume = failure.retryAt && entry.status === 'running' ? ' The same request is scheduled to continue after the pause.' : '';
    return `The AI provider has temporarily paused requests.${resume}`;
  }
  if (failure.kind === 'provider_request_budget') return 'The AI response exceeded the processing limit for this request.';
  if (failure.kind === 'provider_limit') return 'The AI provider has reached its current service limit.';
  return 'The AI response failed before I could finish explaining this request.';
}

export function failedRequestSpeech(entry: BrokerDelegationRecord): string {
  const code=entry.primaryResult?.error?.code;
  const provider:Record<string,string>={
    provider_busy: 'The AI service is busy and could not answer this request.',
    provider_request_budget_exceeded: 'The AI service could not process this request within its service limit.',
    provider_credit_limit: 'The AI service has reached its service credit limit.',
    provider_credits_unavailable: 'The AI service could not cover the processing cost for this request.',
    provider_rate_limited: 'The AI service is receiving too many requests to answer right now.',
  };
  const failure = code === 'primary_financial_result_unknown'
    ? "I couldn't confirm the trade request. We need to check that same request before continuing."
    : requestFailureSpeech(entry) ?? (code && provider[code] || 'I could not get an answer to that request.');
  // Outcome first. A settled action (confirmed, failed, refused) is the outcome and leads; other
  // progress was quiet context, so the caller hears it once, after the failure.
  const partial = entry.primaryResult?.partialResult;
  if (!partial) return failure;
  return partial.kind === 'action_status' && ['confirmed', 'failed', 'refused'].includes(partial.actionState)
    ? `${partial.text} ${failure}` : `${failure} The last observed step before that: ${partial.text}`;
}

/** A partial is progress: quiet context for the voice, tied to its task, never a result. */
export function partialResultUpdate(entry: BrokerDelegationRecord): LiveBackendUpdate | undefined {
  const partial = entry.primaryResult?.partialResult;
  if (!partial || !['running', 'queued'].includes(entry.status)) return;
  const settled = partial.kind === 'action_status' && ['confirmed', 'failed', 'refused'].includes(partial.actionState);
  return { id: `${entry.requestId}_partial_${partial.toolInvocationId}`, kind: 'progress', pending: true, speak: false,
    text: `Progress on this task, not its result: ${partial.text} ${settled
      ? 'That step is settled; the final result for the task will follow.'
      : 'The task is still pending; report no outcome until its final result arrives.'}` };
}
