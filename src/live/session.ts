import { createHash } from 'node:crypto';
import OpenAI from 'openai';
import { LiveWS } from 'openai/resources/live/ws';
import type { ClientEvent, InitialItem } from 'openai/resources/live/live';
import { validatePreparedEligibleHistory } from '../broker/eligibleHistory.js';
import type {
  BrokerLiveConnection, BrokerLiveEvent, BrokerLiveSession, BrokerLiveSessionOptions,
  LiveBackendChannel, LiveBackendUpdate, LiveDelegationRequest, LiveDelivery,
  LiveSessionClosed, LiveTranscriptFragment, LiveTranscriptSnapshot,
} from './types.js';

export type * from './types.js';

// Protocol: developers.openai.com/api/docs/guides/live-delegation and live-conversations.
const STARTUP_AUDIO_BYTES = 136_000; // 17 seconds of mono PCMU; startup deadline is 15s.
const MAX_AUDIO_BYTES = 64_000;
const MAX_TRANSCRIPT_BYTES = 512_000;
const MAX_FRAGMENTS = 8_000;
const MAX_DELEGATIONS = 256;
const MAX_PENDING_APPENDS = 32;
// UTF-8 bytes provide a conservative token upper bound, without guessing tokenization.
export const LIVE_APPEND_MAX_BYTES = 500;

const record = (v: unknown): Record<string, unknown> => v && typeof v === 'object' && !Array.isArray(v)
  ? v as Record<string, unknown> : {};
const id = (v: unknown): v is string => typeof v === 'string' && /^[a-zA-Z0-9_.:-]{1,200}$/.test(v);
const offset = (v: unknown): v is number => typeof v === 'number' && Number.isFinite(v) && v >= 0;
const clone = (v: LiveTranscriptSnapshot): LiveTranscriptSnapshot => ({ ...v, fragments: v.fragments.map(f => ({ ...f })) });
const audioBytes = (v: unknown): number | undefined => {
  if (typeof v !== 'string' || !v.length || v.length > Math.ceil(MAX_AUDIO_BYTES / 3) * 4
      || v.length % 4 !== 0 || !/^[A-Za-z0-9+/]*={0,2}$/.test(v)) return undefined;
  const decoded = Buffer.from(v, 'base64');
  return decoded.length && decoded.toString('base64') === v ? decoded.length : undefined;
};

export function createOpenAILiveConnection(apiKey: string): BrokerLiveConnection {
  const ws = new LiveWS(new OpenAI({ apiKey, maxRetries: 0 }), {
    reconnect: null, maxQueueSize: 256 * 1024, maxPayload: 1024 * 1024,
    headers: { 'User-Agent': 'dexter-broker/Node 0.1' },
  });
  return {
    onOpen: fn => { ws.socket.on('open', fn); },
    onEvent: fn => { ws.on('event', fn); },
    onError: fn => { ws.on('error', fn); },
    onClose: fn => { ws.socket.on('close', fn); },
    isOpen: () => ws.socket.readyState === 1,
    send: event => ws.send(event),
    close: () => ws.close(),
    terminate: () => ws.socket.platformSocket.terminate(),
  };
}

type AppendKind = 'thinking' | 'commentary' | 'instructions';
type PendingAppend = {
  kind: AppendKind;
  timer: ReturnType<typeof setTimeout>;
  resolve: (delivery: LiveDelivery) => void;
};
type Delegation = {
  offsetMs: number;
  controller: AbortController;
  updates: Map<string, { signature: string; delivery: Promise<LiveDelivery> }>;
};

/** Audio stays live while an injected backend owns account-bound, durable work. */
export function createBrokerLiveSession(options: BrokerLiveSessionOptions): BrokerLiveSession {
  const voice = options.voice ?? 'marin';
  if (!options.apiKey?.trim() || !id(options.callId) || !id(voice)
      || typeof options.instructions !== 'string' || !options.instructions.trim()
      || Buffer.byteLength(options.instructions) > 16_384) throw new Error('invalid_live_configuration');
  if (options.eligibleHistory && options.history !== undefined) throw new Error('ambiguous_live_history');
  const eligible = options.eligibleHistory === undefined ? undefined : validatePreparedEligibleHistory(options.eligibleHistory);
  const history = eligible?.history ?? options.history ?? [];
  if (history.length > 128 || history.some(h => !['user', 'assistant', 'developer'].includes(h.role)
      || typeof h.text !== 'string')
      || (!eligible && history.reduce((n, h) => n + Buffer.byteLength(h.text), 0) > 8_000)) throw new Error('invalid_live_history');
  const deadline = (value: number | undefined, fallback: number) => {
    if (value === undefined) return fallback;
    if (!Number.isInteger(value) || value < 1 || value > 120_000) throw new Error('invalid_live_timeout');
    return value;
  };
  const startupTimeout = deadline(options.startupTimeoutMs, 15_000);
  const closeTimeout = deadline(options.closeTimeoutMs, 15_000);
  const ackTimeout = deadline(options.appendAckTimeoutMs, 15_000);
  const transport = (options.connectionFactory ?? createOpenAILiveConnection)(options.apiKey);
  let started = false, ready = false, closing = false, closed = false, playbackBlocked = false;
  let outputAudioDiscarded = false;
  let sessionId: string | undefined, usageSeconds: number | undefined;
  let commandNumber = 0, audioDeliveryNumber = 0, transcriptBytes = 0;
  let startupAudio: string[] = [], startupAudioBytes = 0;
  const transcript: LiveTranscriptSnapshot = { revision: 0, callerRevision: 0, fragments: [] };
  let persistedTranscript = clone(transcript);
  const transcriptEvents = new Map<string, LiveTranscriptFragment>();
  const contextListeners = new Set<(snapshot: LiveTranscriptSnapshot) => void>();
  let captureBarrier = Promise.resolve();
  let captureFailed = false;
  const delegations = new Map<string, Delegation>();
  const appends = new Map<string, PendingAppend>();
  const issuedAppends = new Set<string>();
  const seenErrorEvents = new WeakSet<object>();
  const timers = new Set<ReturnType<typeof setTimeout>>();
  let resolveClosed!: (result: LiveSessionClosed) => void;
  const closedPromise = new Promise<LiveSessionClosed>(resolve => { resolveClosed = resolve; });
  const commandId = () => `broker_${++commandNumber}`;
  const emit = (event: BrokerLiveEvent) => { try { options.onEvent?.(event); } catch { /* Telemetry cannot prevent cleanup. */ } };
  const later = (fn: () => void, ms: number) => {
    const timer = setTimeout(() => { timers.delete(timer); fn(); }, ms);
    timers.add(timer);
    timer.unref?.();
    return timer;
  };
  const clearTimer = (timer: ReturnType<typeof setTimeout>) => { clearTimeout(timer); timers.delete(timer); };
  const settleAppend = (eventId: string, status: LiveDelivery['status'], reason?: string) => {
    const pending = appends.get(eventId);
    if (!pending) return;
    clearTimer(pending.timer);
    appends.delete(eventId);
    pending.resolve({ eventId, status, ...(reason ? { reason } : {}),
      observedCallerRevision: transcript.callerRevision, observedTranscriptSequence: transcript.revision });
    emit({ type: 'live.append.outcome', eventId, status, ...(reason ? { reason } : {}) });
  };
  const clearPlayback = (reason: 'application_interrupt' | 'session_closed' | 'session_failed') => {
    playbackBlocked = true;
    try { options.onPlaybackClear?.(reason); } catch { emit({ type: 'live.playback_clear_failed' }); }
  };
  const detachBackend = () => {
    // This abort only detaches updates from this call. The backend retains its work record.
    for (const delegation of delegations.values()) delegation.controller.abort();
    contextListeners.clear();
  };
  const finish = (finalized: boolean, reason: string) => {
    if (closed) return;
    closed = true; closing = true; ready = false;
    startupAudio = []; startupAudioBytes = 0;
    detachBackend();
    for (const eventId of appends.keys()) settleAppend(eventId, 'unconfirmed', 'session_ended');
    for (const timer of timers) clearTimeout(timer);
    timers.clear();
    clearPlayback(finalized ? 'session_closed' : 'session_failed');
    try { if (finalized) transport.close(); else transport.terminate(); } catch { /* Already disconnected. */ }
    // Flush queued transcript persistence before the host writes its final call record.
    let reported = false;
    const report = (transcriptPersisted: boolean) => {
      if (reported) return;
      reported = true;
      clearTimeout(drainTimer);
      const result: LiveSessionClosed = { callId: options.callId, sessionId, finalized, reason, usageSeconds, transcriptPersisted, outputAudioDiscarded };
      if (!transcriptPersisted) emit({ type: 'live.transcript_drain_unconfirmed' });
      try { options.onClosed(result); } catch { emit({ type: 'live.closed_observer_failed' }); }
      resolveClosed(result);
    };
    const drainTimer = setTimeout(() => report(false), closeTimeout);
    drainTimer.unref?.();
    void captureBarrier.then(() => report(!captureFailed));
  };
  const fail = (code: string) => { emit({ type: 'live.error', code }); finish(false, code); };
  const send = (event: ClientEvent): boolean => {
    if (closed || !transport.isOpen()) return false;
    try { transport.send(event); return true; } catch { fail('transport_send_failed'); return false; }
  };
  const append = (kind: AppendKind, text: string, delegationId: string | null): Promise<LiveDelivery> => {
    const eventId = commandId();
    if (closing || closed) return Promise.resolve({ eventId, status: 'closed' });
    if (!ready) return Promise.resolve({ eventId, status: 'rejected', reason: 'session_not_ready' });
    if (typeof text !== 'string' || !text.trim() || Buffer.byteLength(text) > LIVE_APPEND_MAX_BYTES)
      return Promise.resolve({ eventId, status: 'rejected', reason: 'invalid_append_content' });
    if (delegationId !== null && !delegations.has(delegationId))
      return Promise.resolve({ eventId, status: 'rejected', reason: 'unknown_delegation' });
    if (appends.size >= MAX_PENDING_APPENDS)
      return Promise.resolve({ eventId, status: 'rejected', reason: 'append_backpressure' });
    if (issuedAppends.size >= 8_000)
      return Promise.resolve({ eventId, status: 'rejected', reason: 'append_limit' });
    return new Promise(resolve => {
      const timer = later(() => settleAppend(eventId, 'unconfirmed', 'append_ack_timeout'), ackTimeout);
      appends.set(eventId, { kind, timer, resolve });
      issuedAppends.add(eventId);
      if (!send({ type: `session.${kind}.append`, event_id: eventId, delegation_id: delegationId, content: text }))
        settleAppend(eventId, 'unconfirmed', 'transport_unavailable');
    });
  };
  const start = () => {
    if (started || closing || closed || !transport.isOpen()) return;
    started = true;
    const input: InitialItem[] = eligible?.initialItems ?? history.map(h => h.role === 'assistant'
      ? { type: 'message', role: 'assistant', content: [{ type: 'output_text', text: h.text }] }
      : { type: 'message', role: h.role, content: [{ type: 'input_text', text: h.text }] });
    send({ type: 'session.start', event_id: commandId(), session: {
      model: 'gpt-live-1', instructions: options.instructions, input, store: false,
      audio: { format: { type: 'audio/pcmu', rate: 8000 }, output: { voice } },
      delegation: { type: 'client' },
    } });
  };
  const startupTimer = later(() => fail('startup_timeout'), startupTimeout);

  const capture = (event: Record<string, unknown>) => {
    if (!id(event.event_id) || typeof event.delta !== 'string' || !offset(event.start_ms)
        || !offset(event.end_ms) || event.end_ms < event.start_ms) { fail('invalid_transcript'); return; }
    const role = event.type === 'session.input_transcript.delta' ? 'user' : 'assistant';
    const prior = transcriptEvents.get(event.event_id);
    if (prior) {
      if (prior.role !== role || prior.text !== event.delta || prior.startMs !== event.start_ms || prior.endMs !== event.end_ms)
        fail('transcript_event_identity_changed');
      return;
    }
    transcriptBytes += Buffer.byteLength(event.delta);
    if (transcriptBytes > MAX_TRANSCRIPT_BYTES || transcript.fragments.length >= MAX_FRAGMENTS) { fail('transcript_limit'); return; }
    const fragment: LiveTranscriptFragment = { eventId: event.event_id, role, text: event.delta,
      startMs: event.start_ms, endMs: event.end_ms, sequence: ++transcript.revision };
    if (role === 'user') transcript.callerRevision++;
    transcriptEvents.set(fragment.eventId, fragment);
    transcript.fragments.push(fragment);
    // Provider deltas are appended in delivery order. Retain timestamps for
    // captions and application grouping without rearranging the transcript.
    const snapshot = clone(transcript);
    captureBarrier = captureBarrier.then(async () => {
      await options.onTranscript(clone(snapshot));
      persistedTranscript = snapshot;
      if (!closing && !closed) for (const listener of contextListeners) listener(clone(snapshot));
    }).catch(() => { captureFailed = true; fail('transcript_capture_failed'); });
  };
  const receiveDelegation = (event: Record<string, unknown>) => {
    const metadata = record(event.delegation);
    if (!id(metadata.id) || metadata.type !== 'delegation' || metadata.target !== 'client'
        || !offset(event.offset_ms) || !sessionId) { fail('invalid_client_delegation'); return; }
    const delegationId = metadata.id;
    const existing = delegations.get(delegationId);
    if (existing) {
      if (existing.offsetMs !== event.offset_ms) fail('delegation_identity_changed');
      return;
    }
    if (delegations.size >= MAX_DELEGATIONS) { fail('delegation_limit'); return; }
    const state: Delegation = { offsetMs: event.offset_ms, controller: new AbortController(), updates: new Map() };
    delegations.set(delegationId, state);
    const requestId = createHash('sha256').update(JSON.stringify(['dexter-broker/live/v1', options.callId, sessionId, delegationId])).digest('hex');
    const publish = (update: LiveBackendUpdate): Promise<LiveDelivery> => {
      if (!id(update.id)) return Promise.resolve({ eventId: '', status: 'rejected', reason: 'invalid_update_id' });
      const signature = JSON.stringify([update.kind, update.text,
        update.expectedCallerRevision ?? null, update.speak ?? update.kind === 'result']);
      const previous = state.updates.get(update.id);
      if (previous) return previous.signature === signature ? previous.delivery
        : Promise.resolve({ eventId: '', status: 'rejected', reason: 'update_identity_changed' });
      if (closing || closed) return Promise.resolve({ eventId: '', status: 'closed' });
      if (update.kind !== 'progress' && update.kind !== 'result')
        return Promise.resolve({ eventId: '', status: 'rejected', reason: 'invalid_update_kind' });
      if (state.updates.size >= 256) return Promise.resolve({ eventId: '', status: 'rejected', reason: 'update_limit' });
      if ((update.kind === 'result' || update.expectedCallerRevision !== undefined)
          && (!Number.isSafeInteger(update.expectedCallerRevision) || update.expectedCallerRevision! < 0))
        return Promise.resolve({ eventId: '', status: 'rejected', reason: 'caller_revision_required' });
      const delivery = update.expectedCallerRevision !== undefined && update.expectedCallerRevision !== transcript.callerRevision
        ? Promise.resolve<LiveDelivery>({ eventId: '', status: 'stale', reason: 'caller_context_changed' })
        : append((update.speak ?? update.kind === 'result') ? 'commentary' : 'thinking', update.text, delegationId);
      state.updates.set(update.id, { signature, delivery });
      return delivery;
    };
    const channel: LiveBackendChannel = {
      getContext: () => clone(persistedTranscript),
      subscribeContext: listener => {
        if (state.controller.signal.aborted) return () => {};
        contextListeners.add(listener);
        const unsubscribe = () => { contextListeners.delete(listener); state.controller.signal.removeEventListener('abort', unsubscribe); };
        state.controller.signal.addEventListener('abort', unsubscribe, { once: true });
        return unsubscribe;
      },
      publish, signal: state.controller.signal,
    };
    const acceptedSessionId = sessionId;
    const barrier = captureBarrier;
    void barrier.then(async () => {
      if (closing || closed) return;
      const request: LiveDelegationRequest = { callId: options.callId, sessionId: acceptedSessionId,
        delegationId, requestId, offsetMs: state.offsetMs, transcript: clone(persistedTranscript) };
      emit({ type: 'live.delegation.started', delegationId, requestId, revision: request.transcript.revision });
      await options.backend.delegate(request, channel);
    }).catch(() => emit({ type: 'live.delegation.error', delegationId, requestId, code: 'backend_failed' }));
  };

  const receiveError = (event: Record<string, unknown>) => {
    if (closed || seenErrorEvents.has(event)) return;
    seenErrorEvents.add(event);
    const error = record(event.error);
    const code = id(error.code) ? error.code : 'provider_error';
    if (id(error.client_event_id) && appends.has(error.client_event_id)) {
      settleAppend(error.client_event_id, 'rejected', code);
    } else if (id(error.client_event_id) && issuedAppends.has(error.client_event_id)) {
      emit({ type: 'live.append.late_rejection', eventId: error.client_event_id, code });
    } else fail(code);
  };

  transport.onEvent(value => {
    if (closed) return;
    const event = record(value), type = event.type;
    if (type === 'session.closed') {
      const finalSession = record(event.session);
      if (!sessionId || finalSession.id !== sessionId) { fail('closed_session_identity_mismatch'); return; }
      const seconds = record(event.usage).seconds;
      if (offset(seconds)) usageSeconds = seconds;
      finish(true, id(event.reason) ? event.reason : 'session_closed');
      return;
    }
    if (type === 'error') {
      receiveError(event);
      return;
    }
    if (typeof type === 'string' && /^session\.(thinking|commentary|instructions)\.appended$/.test(type)) {
      const eventId = event.client_event_id;
      if (id(eventId) && appends.get(eventId)?.kind === type.split('.')[1]) settleAppend(eventId, 'accepted');
      return;
    }
    // Drain transcripts after a graceful close request; backend subscriptions are already detached.
    if (ready && (type === 'session.input_transcript.delta' || type === 'session.output_transcript.delta')) { capture(event); return; }
    if (ready && type === 'session.output_audio.delta' && (closing || playbackBlocked)) {
      outputAudioDiscarded = true;
      emit({ type: 'live.output_audio.discarded' });
      return;
    }
    if (closing) return;
    if (type === 'session.started') {
      const session = record(event.session);
      if (ready) { if (session.id !== sessionId) fail('started_session_identity_changed'); return; }
      if (!started || !id(session.id) || session.model !== 'gpt-live-1'
          || (session.delegation != null && record(session.delegation).type !== 'client')) { fail('invalid_started_session'); return; }
      sessionId = session.id; ready = true; clearTimer(startupTimer);
      const queued = startupAudio; startupAudio = []; startupAudioBytes = 0;
      for (const audio of queued) if (!send({ type: 'session.input_audio.append', audio })) return;
      try { options.onReady({ callId: options.callId, sessionId }); } catch { fail('ready_observer_failed'); }
    } else if (type === 'session.output_audio.delta') {
      if (!ready || audioBytes(event.delta) === undefined) { fail('invalid_output_audio'); return; }
      if (!playbackBlocked) {
        // Primary WebSocket audio has no provider event ID. This connection-local
        // delivery ID tracks playback order; it cannot identify provider evidence.
        const deliveryId = `broker_audio_${++audioDeliveryNumber}`;
        try { options.onAudio(event.delta as string, { deliveryId }); } catch { fail('audio_delivery_failed'); }
      }
    } else if (type === 'session.usage.updated') {
      const seconds = record(event.usage).seconds;
      if (offset(seconds)) { usageSeconds = seconds; emit({ type: 'live.usage', seconds }); }
    } else if (type === 'session.delegation.created') {
      if (!ready) fail('delegation_before_ready'); else receiveDelegation(event);
    } else if (type === 'response.event') fail('unexpected_responses_backend');
  });
  transport.onError(error => {
    // The SDK emits a protocol error twice: as an event, then wrapped in its
    // error callback. Correlated command rejection remains recoverable on both.
    const providerEvent = record(record(error).error);
    if (providerEvent.type === 'error') receiveError(providerEvent);
    else if (!closed) fail('provider_connection_error');
  });
  transport.onClose(() => { if (!closed) fail('connection_closed_without_final_usage'); });
  transport.onOpen(start);
  if (transport.isOpen()) queueMicrotask(start);

  return {
    appendAudio(base64) {
      if (closing || closed) return false;
      const bytes = audioBytes(base64);
      if (bytes === undefined) { fail('invalid_input_audio'); return false; }
      if (!ready) {
        if (startupAudioBytes + bytes > STARTUP_AUDIO_BYTES) { fail('startup_audio_overflow'); return false; }
        startupAudio.push(base64); startupAudioBytes += bytes; return true;
      }
      return send({ type: 'session.input_audio.append', audio: base64 });
    },
    appendContext: text => append('thinking', text, null),
    appendInstructions: text => append('instructions', text, null),
    appendCommentary: text => append('commentary', text, null),
    interruptPlayback: instruction => { clearPlayback('application_interrupt'); return append('instructions', instruction, null); },
    resumePlayback: () => { if (!closing && !closed) playbackBlocked = false; },
    getTranscript: () => clone(transcript),
    close(reason = 'application_close') {
      if (closing || closed) return closedPromise;
      closing = true; startupAudio = []; startupAudioBytes = 0; detachBackend();
      clearPlayback('session_closed');
      emit({ type: 'live.closing', reason: id(reason) ? reason : 'application_close' });
      if (!ready || !transport.isOpen()) finish(false, 'closed_before_ready');
      else {
        later(() => finish(false, 'finalization_timeout'), closeTimeout);
        if (!send({ type: 'session.close', event_id: commandId() })) finish(false, 'close_send_failed');
      }
      return closedPromise;
    },
  };
}
