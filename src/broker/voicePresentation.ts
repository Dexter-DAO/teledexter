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

/** Only local observation and queue facts; none of these states authorize an action. */
export function pendingRequestState(entry: BrokerDelegationRecord): string {
  return JSON.stringify([entry.status, entry.resultReadUnavailable === true, entry.submitError?.code ?? null,
    (entry.deferredTurns ?? []).filter(turn => turn.status === 'queued').map(turn => turn.sourceEventId),
    entry.currentQuestionId ?? null, entry.primaryResult?.failure?.kind ?? null,
    entry.primaryResult?.failure?.retryAt ?? null]);
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

/** Spoken pending status. Queue state stays internal; the caller hears one plain line. */
export function pendingRequestSpeech(entry: BrokerDelegationRecord, earlier: boolean): string {
  if (entry.resultReadUnavailable) return "I can't confirm how that's going yet, so I don't know the outcome.";
  const failure = requestFailureSpeech(entry);
  if (failure) return failure;
  if (['prepared', 'dispatching', 'dispatch_unknown'].includes(entry.status)) {
    if (entry.status === 'dispatch_unknown' && entry.submitError?.code === 'http_error')
      return 'My connection to your account tools returned an error. I am checking whether your request was saved.';
    return earlier ? 'I am still checking whether your earlier request was saved.'
      : 'I have not confirmed whether your request was saved. I am checking the original request.';
  }
  return 'Still working on it.';
}

export function isFixedPendingStatus(entry: BrokerDelegationRecord, update?: LiveBackendUpdate): boolean {
  return !!update && update.kind === 'progress'
    && [pendingRequestSpeech(entry, false), pendingRequestSpeech(entry, true)].includes(update.text)
    && ['prepared', 'dispatching', 'dispatch_unknown', 'queued', 'running'].includes(entry.status)
    && update.id.startsWith(`${entry.requestId}_`);
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
  const partial = entry.primaryResult?.partialResult;
  return partial && !(entry.partialDelivery?.toolInvocationId === partial.toolInvocationId
    && entry.partialDelivery.text === partial.text && entry.partialDelivery.status === 'accepted')
    ? `${partial.text}\n${failure}` : failure;
}

export function partialResultUpdate(entry: BrokerDelegationRecord): LiveBackendUpdate | undefined {
  const partial = entry.primaryResult?.partialResult;
  if (!partial || !['running', 'queued'].includes(entry.status)) return;
  return { id: `${entry.requestId}_partial_${partial.toolInvocationId}`, kind: 'result', pending: true, speak: true, text: partial.text };
}

export function isPartialResultUpdate(entry: BrokerDelegationRecord, update?: LiveBackendUpdate): boolean {
  const expected = partialResultUpdate(entry);
  return !!expected && !!update && update.id === expected.id && update.kind === expected.kind
    && update.speak === true && update.text === expected.text;
}
