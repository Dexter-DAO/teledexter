import { createHash, randomUUID } from 'node:crypto';
import { z } from 'zod';
import {
  eligibleContextBindingSchema, historyVersionSchema, parseEligiblePhoneCall, sameHistoryVersion,
  type EligibleContextBinding, type EligibleContextClient, type EligibleDeliveryResponse,
  type EligibleDeliveryTarget, type EligiblePhoneCall, type HistoryVersion, type HistoryAdmission,
} from './eligibleContext.js';
import { PrimaryApiError } from './primaryApi.js';
import { sameCallAssignmentRef } from './assignment.js';
import type { LiveDelivery, LiveSessionClosed, LiveTranscriptSnapshot } from '../live/types.js';
import type { BrokerPublicationReceipt } from './publications.js';
import type { RequestDeliveryAssociation, RequestDeliveryJournal } from './requestDelivery.js';

const timestamp = z.iso.datetime({ offset: true });
const safeInteger = z.number().int().min(0).max(Number.MAX_SAFE_INTEGER);
const uuid = z.string().uuid();
const transcriptSchema = z.object({
  revision: safeInteger, callerRevision: safeInteger,
  fragments: z.array(z.object({ eventId: z.string().min(1).max(200), role: z.enum(['user', 'assistant']),
    text: z.string(), startMs: z.number().finite().min(0), endMs: z.number().finite().min(0), sequence: safeInteger,
  }).strict()).max(8000),
}).strict().refine(value => value.revision === value.fragments.length
  && value.callerRevision === value.fragments.filter(fragment => fragment.role === 'user').length
  && new Set(value.fragments.map(fragment => fragment.eventId)).size === value.fragments.length
  && value.fragments.every((fragment, index) => fragment.sequence === index + 1 && fragment.endMs >= fragment.startMs)
  && value.fragments.reduce((bytes, fragment) => bytes + Buffer.byteLength(fragment.text), 0) <= 512_000);
const clocksSchema = z.object({
  callDeadlineAt: timestamp, consumedAt: timestamp.optional(), mediaStartBy: timestamp.optional(),
  mediaStartedAt: timestamp.optional(),
}).strict().refine(value => (value.consumedAt === undefined) === (value.mediaStartBy === undefined))
  .refine(value => value.consumedAt === undefined || (value.mediaStartedAt !== undefined
    && Date.parse(value.mediaStartedAt) >= Date.parse(value.consumedAt)
    && Date.parse(value.mediaStartedAt) < Date.parse(value.mediaStartBy!)));
export type LiveGenerationClocks = z.infer<typeof clocksSchema>;
export const startupFailureCodeSchema = z.enum([
  'credit_balance_exhausted', 'insufficient_quota', 'invalid_api_key', 'model_not_found',
  'rate_limit_exceeded', 'permission_denied', 'invalid_request_error', 'authentication_error', 'server_error',
  'provider_error', 'provider_connection_error', 'connection_closed_without_final_usage',
  'transport_send_failed', 'invalid_started_session', 'started_session_identity_changed',
  'closed_session_identity_mismatch', 'invalid_output_audio', 'delegation_before_ready',
  'unexpected_responses_backend', 'ready_observer_failed', 'startup_timeout', 'closed_before_ready',
  'generation_start_timeout', 'generation_start_cancelled', 'generation_session_reused',
  'invalid_live_configuration', 'invalid_live_history', 'ambiguous_live_history',
  'history_payload_too_large', 'prepared_history_changed', 'generation_start_failed',
]);
const segmentSchema = z.object({
  generationId: uuid, ordinal: safeInteger.min(1), createdAt: timestamp,
  state: z.enum(['starting', 'active', 'retiring', 'closed', 'abandoned']),
  sessionId: z.string().min(1).max(200).optional(), readyAt: timestamp.optional(),
  version: historyVersionSchema, pageDigest: z.string().regex(/^[0-9a-f]{64}$/),
  transcript: transcriptSchema,
  inputFirstSample: safeInteger.optional(), inputLastSampleExclusive: safeInteger.optional(),
  retiringAt: timestamp.optional(), closedAt: timestamp.optional(), providerFinalized: z.boolean().optional(), transcriptPersisted: z.boolean().optional(),
  outputDiscarded: z.boolean(),
}).strict();
export const liveGenerationJournalSchema = z.object({
  version: z.literal(1), callId: uuid, binding: eligibleContextBindingSchema, clocks: clocksSchema,
  phase: z.enum(['idle', 'replacing', 'active', 'unavailable', 'stopped']),
  segments: z.array(segmentSchema),
  deliveryAttachment: z.object({ generationId: uuid, sessionId: z.string().min(1).max(200) }).strict().optional(),
  inputNextSample: safeInteger,
  inputGap: z.enum(['restart_during_generation', 'buffer_overflow', 'transport_refused', 'invalid_audio']).optional(),
}).strict().superRefine((value, ctx) => {
  const ids = new Set<string>();
  const sessionIds = new Set<string>();
  for (const [index, segment] of value.segments.entries()) {
    if (ids.has(segment.generationId) || (segment.sessionId !== undefined && sessionIds.has(segment.sessionId)) || segment.ordinal !== index + 1
      || segment.version.primaryDexterId !== value.binding.primaryDexterId
      || segment.version.threadId !== value.binding.threadId
      || (value.binding.ownerPrincipalId !== undefined && segment.version.ownerPrincipalId !== value.binding.ownerPrincipalId)
      || (segment.state === 'active' && !segment.sessionId)) {
      ctx.addIssue({ code: 'custom', message: 'Invalid generation identity' });
    }
    ids.add(segment.generationId);
    if (segment.sessionId !== undefined) sessionIds.add(segment.sessionId);
  }
  if (value.phase === 'active') {
    const current = value.segments.find(segment => segment.generationId === value.deliveryAttachment?.generationId);
    if (!current || current.state !== 'active' || current.sessionId !== value.deliveryAttachment?.sessionId)
      ctx.addIssue({ code: 'custom', message: 'Missing active delivery attachment' });
  } else if (value.deliveryAttachment) ctx.addIssue({ code: 'custom', message: 'Inactive delivery attachment' });
});
export type LiveGenerationJournal = z.infer<typeof liveGenerationJournalSchema>;
export type LiveGenerationAttachment = NonNullable<LiveGenerationJournal['deliveryAttachment']>;
type ReadyDelivery = Extract<EligibleDeliveryResponse['delivery'], { state: 'ready' }>;

/** Offsets count observed PCMU samples on this call, not canonical turn boundaries. */
export interface GenerationInputAudio {
  payload: string;
  callInputStartSample: number;
  /** Original host observation time; replay must not manufacture later speech. */
  observedAt?: number;
}
export interface GenerationPublication extends LiveDelivery {
  publicationReceipts?: BrokerPublicationReceipt[];
}
export interface GenerationHandle {
  sessionId: string;
  appendAudio(frame: GenerationInputAudio): boolean;
  /** The adapter uses general commentary, keeping this original request identity. */
  publish(input: { requestId: string; version: HistoryVersion; delivery: ReadyDelivery;
    deliveryAssociation?: RequestDeliveryAssociation }): Promise<GenerationPublication>;
  close(reason: 'context_replaced' | 'call_stopped' | 'context_unavailable'): Promise<LiveSessionClosed>;
}
export interface GenerationCallbacks {
  onAudio(payload: string): void;
  onTranscript(snapshot: LiveTranscriptSnapshot): void;
  onClosed(result: LiveSessionClosed): void;
}
export interface ContextReadFailure {
  stage: 'fetch' | 'assert' | 'delivery'; attempt: number; at: string; durationMs: number;
  code: PrimaryApiError['code']; status?: number; remoteCode?: string; retryable: boolean;
}
export interface StatusContextRefresh {
  generationId: string; observedAt: string; version: HistoryVersion; contentDigest: string;
  kind?: 'admitted_sources';
}
export interface LiveGenerationOptions {
  callId: string;
  binding: EligibleContextBinding;
  /** Created after the host's first-media admission; inbound mediaStartedAt is its saved observation.
   * Rotation never admits first media or derives a new deadline. */
  clocks: LiveGenerationClocks;
  initial?: LiveGenerationJournal;
  context: EligibleContextClient;
  persist(journal: LiveGenerationJournal): void;
  /** A segment's newer transcript: evidence persisted off the media path. Absent: persist the journal. */
  observeTranscript?(generationId: string, transcript: LiveGenerationJournal['segments'][number]['transcript']): void;
  /** Adapter startup/close/publication promises retain their bounded Live timeouts. */
  open(input: { generationId: string; ordinal: number; context: EligiblePhoneCall;
    allowOpening: boolean; signal: AbortSignal; callbacks: GenerationCallbacks }): Promise<GenerationHandle>;
  /** One initial greeting, inside the post-open assertion operation. Must not call queued controller
   * methods; it may await inputDelivered() to let held caller input reach Live first. */
  onInitialActivation?(attachment: LiveGenerationAttachment): Promise<void>;
  /** Optional context starts after activation and opening dispatch, without blocking input. */
  onActivated?(attachment: LiveGenerationAttachment): void;
  /** Drain only source admissions already observed on this generation; the host deadline remains independent. */
  beforeRetire?(generationId: string): Promise<void>;
  onAudio(payload: string, attachment: LiveGenerationAttachment): void;
  clearPlayback(): void;
  onProblem(code: string): void;
  onContextReadFailure?(failure: ContextReadFailure): void;
  onStatusContextRefresh?(refresh: StatusContextRefresh): void;
  maxBufferedAudioBytes?: number;
  /** Backlog replay speed relative to real time (8 kHz PCMU: 8 bytes per ms). Live
   * rejected unpaced backlog bursts with input_audio_rate_limit_exceeded; the default is 1.0x. */
  inputPaceRate?: number;
  /** May tighten the shared fetch/start/assert budget, never extend it. */
  transitionTimeoutMs?: number;
  now?: () => number;
  newId?: () => string;
}
export class LiveGenerationError extends Error {
  constructor(readonly code: string) { super(code); this.name = 'LiveGenerationError'; }
}
/** Only known codes cross the adapter boundary; provider messages never enter the journal. */
export class LiveGenerationStartupError extends LiveGenerationError {
  constructor(reason: unknown) {
    const parsed = startupFailureCodeSchema.safeParse(reason);
    super(parsed.success ? parsed.data : 'generation_start_failed');
    this.name = 'LiveGenerationStartupError';
  }
}
export interface GenerationDeliveryOutcome {
  requestId: string;
  attachment: LiveGenerationAttachment;
  response: EligibleDeliveryResponse;
  publication?: GenerationPublication;
}

export const LIVE_GENERATION_STARTUP_MS = 15_000;
// Existing default context HTTP waits are 10s each. Retirement is independent.
export const LIVE_GENERATION_TRANSITION_MS = 10_000 + LIVE_GENERATION_STARTUP_MS + 10_000;
const INPUT_BYTES_PER_SECOND = 8_000;
const MAX_INPUT_FRAME_BYTES = 64_000;

/** Host-controlled context boundaries; this does not identify every autonomous Live utterance. */
export function createLiveGenerationController(options: LiveGenerationOptions) {
  const now = options.now ?? Date.now;
  const newId = options.newId ?? randomUUID;
  const identity = eligibleContextBindingSchema.parse(options.binding);
  const clocks = clocksSchema.parse(options.clocks);
  if (clocks.mediaStartedAt !== undefined && Date.parse(clocks.mediaStartedAt) > now())
    throw new LiveGenerationError('media_start_unobserved');
  uuid.parse(options.callId);
  const transitionTimeout = options.transitionTimeoutMs ?? LIVE_GENERATION_TRANSITION_MS;
  if (!Number.isInteger(transitionTimeout) || transitionTimeout < 1 || transitionTimeout > LIVE_GENERATION_TRANSITION_MS)
    throw new LiveGenerationError('invalid_transition_timeout');
  // One complete permitted frame can cross a timer boundary. This is capacity,
  // not permission to extend either the transition or original call deadline.
  const bufferLimit = Math.ceil(INPUT_BYTES_PER_SECOND * transitionTimeout / 1000) + MAX_INPUT_FRAME_BYTES;
  const maxBuffered = options.maxBufferedAudioBytes ?? bufferLimit;
  if (!Number.isInteger(maxBuffered) || maxBuffered < 1 || maxBuffered > bufferLimit) throw new LiveGenerationError('invalid_audio_buffer_limit');
  let state: LiveGenerationJournal = options.initial ? liveGenerationJournalSchema.parse(options.initial) : {
    version: 1, callId: options.callId, binding: identity, clocks, phase: 'idle', segments: [], inputNextSample: 0,
  };
  if (state.callId !== options.callId || state.binding.bindingId !== identity.bindingId
    || state.binding.callSid !== identity.callSid || state.binding.primaryDexterId !== identity.primaryDexterId
    || state.binding.threadId !== identity.threadId
    || (identity.ownerPrincipalId !== undefined && state.binding.ownerPrincipalId !== identity.ownerPrincipalId)
    || !sameCallAssignmentRef(state.binding.callAssignmentRef, identity.callAssignmentRef)
    || JSON.stringify(state.clocks) !== JSON.stringify(clocks)) throw new LiveGenerationError('generation_identity_changed');
  let stopped = state.phase === 'stopped';
  let inputBlocked = false;
  let handle: GenerationHandle | undefined;
  let handleId: string | undefined;
  let activeId: string | undefined;
  const retirements = new Map<string, Promise<void>>();
  const openings = new Set<Promise<void>>();
  // Original provider input stays immutable. This cache is never reconstructed
  // from a caller supplied version or from an old process's journal.
  const admittedContexts = new Map<string, EligiblePhoneCall>();
  const admissions = new Map<string, HistoryAdmission>();
  const observedVersions = new Map<string, HistoryVersion>();
  const observedVersion = (generationId:string) => observedVersions.get(generationId) ?? segment(generationId).version;
  const recordVersion = (generationId:string,version:HistoryVersion) => {
    const prior=observedVersion(generationId);
    if(sameHistoryVersion(prior,version)) return;
    options.onStatusContextRefresh?.({generationId,observedAt:stamp(),version,kind:'admitted_sources',
      contentDigest:createHash('sha256').update(JSON.stringify(admissions.get(generationId))).digest('hex')});
    observedVersions.set(generationId,version);
  };
  let transitionAbort: AbortController | undefined;
  const reads = new Set<AbortController>();
  const canOpenInitially = state.segments.length === 0;
  let activated = false;
  let queued: Array<GenerationInputAudio & { bytes: number }> = [];
  let queuedBytes = 0;
  const paceRate = options.inputPaceRate ?? 1;
  // Above 1.0x is accepted only for an explicit composition; at 1.0x a backlog is replayed
  // without loss but persists as input delay because live frames keep arriving at 1.0x.
  if (!Number.isFinite(paceRate) || paceRate <= 0 || paceRate > 2) throw new LiveGenerationError('invalid_input_pace_rate');
  // A replay window: audio sent since it opened never exceeds elapsed time * paceRate.
  let pacer: ReturnType<typeof setTimeout> | undefined;
  let pacing: { startedAt: number; sentMs: number } | undefined;
  let tail: Promise<unknown> = Promise.resolve();
  const stamp = () => new Date(now()).toISOString();
  const save = () => { options.persist(liveGenerationJournalSchema.parse(structuredClone(state))); };
  const problem = (code: string) => { try { options.onProblem(code); } catch { /* Host telemetry cannot reopen output. */ } };
  const segment = (id: string) => state.segments.find(value => value.generationId === id)!;
  const expire = () => {
    if (stopped) throw new LiveGenerationError('call_stopped');
    if (now() >= Date.parse(clocks.callDeadlineAt)) throw new LiveGenerationError('call_deadline');
    if (inputBlocked) throw new LiveGenerationError('input_unavailable');
  };
  const serial = <T>(operation: (deadline: number) => Promise<T>): Promise<T> => {
    const pending = tail.then(() => operation(Math.min(now() + transitionTimeout, Date.parse(clocks.callDeadlineAt))));
    tail = pending.catch(() => {});
    return pending;
  };
  const attachment = (): LiveGenerationAttachment => {
    expire();
    if (state.phase !== 'active' || !state.deliveryAttachment || !handle || activeId !== state.deliveryAttachment.generationId)
      throw new LiveGenerationError('generation_unavailable');
    return { ...state.deliveryAttachment };
  };
  // inputDelivered() waiters: settled as frames reach the active generation, or (false) when it stops taking input.
  const deliveryWaiters = new Set<{ sample: number; resolve(delivered: boolean): void }>();
  const settleDeliveryWaiters = (blocked: boolean) => {
    const sent = activeId === undefined ? undefined : segment(activeId).inputLastSampleExclusive;
    for (const waiter of deliveryWaiters) if (blocked || (sent !== undefined && sent >= waiter.sample)) {
      deliveryWaiters.delete(waiter); waiter.resolve(!blocked);
    }
  };
  const block = (phase: 'replacing' | 'unavailable' | 'stopped') => {
    // Time spent blocked never becomes replay allowance: the next flush opens a new window.
    clearTimeout(pacer); pacer = undefined; pacing = undefined;
    activeId = undefined;
    state.deliveryAttachment = undefined;
    state.phase = phase;
    settleDeliveryWaiters(true);
    save();
    try { options.clearPlayback(); } catch { problem('playback_clear_failed'); }
  };
  const unavailable = (code: string) => {
    if (state.phase !== 'stopped') block('unavailable');
    problem(code);
  };
  const rejectSourceCallback = (generationId: string, code: string) => {
    const current = segment(generationId);
    const affectsDelivery = activeId === generationId && state.phase === 'active';
    // Preserve the last valid source snapshot. A retired or unexposed source
    // cannot invalidate the successor's delivery attachment.
    current.state = 'abandoned'; current.closedAt = stamp(); current.providerFinalized = false;
    current.transcriptPersisted = false; current.outputDiscarded = true;
    if (affectsDelivery) unavailable(code); else save();
  };
  const callbacks = (generationId: string): GenerationCallbacks => ({
    onAudio(payload) {
      if (stopped || now() >= Date.parse(clocks.callDeadlineAt) || state.phase !== 'active' || activeId !== generationId) {
        const old = segment(generationId);
        if (!old.outputDiscarded) { old.outputDiscarded = true; save(); }
        return;
      }
      try { options.onAudio(payload, attachment()); }
      catch { unavailable('output_delivery_failed'); }
    },
    onTranscript(snapshot) {
      const current = segment(generationId);
      if (current.state === 'closed' || current.state === 'abandoned') return;
      const parsed = transcriptSchema.safeParse(snapshot);
      if (!parsed.success || parsed.data.revision < current.transcript.revision
        || current.transcript.fragments.some((fragment, index) => JSON.stringify(fragment) !== JSON.stringify(parsed.data.fragments[index]))) {
        rejectSourceCallback(generationId, 'generation_transcript_changed'); return;
      }
      current.transcript = parsed.data;
      // Every transcript fragment lands here; rewriting the journal for each one stalled call
      // audio (call of Oct 17). Fragments are observations, written asynchronously.
      if (options.observeTranscript) options.observeTranscript(generationId, parsed.data);
      else save();
    },
    onClosed(result) {
      const current = segment(generationId);
      if (current.state === 'closed' || current.state === 'abandoned') return;
      if (result.callId !== options.callId || (current.sessionId && result.sessionId !== current.sessionId)) {
        rejectSourceCallback(generationId, 'generation_close_identity_mismatch'); return;
      }
      const unexpected = activeId === generationId && state.phase === 'active';
      current.state = 'closed'; current.closedAt = stamp(); current.providerFinalized = result.finalized;
      current.transcriptPersisted = result.transcriptPersisted;
      current.outputDiscarded ||= result.outputAudioDiscarded;
      if (unexpected) unavailable('generation_closed'); else save();
    },
  });
  const retire = (reason: 'context_replaced' | 'call_stopped' | 'context_unavailable',
    generationId = handleId, retiring = handle): Promise<void> => {
    if (!generationId) return Promise.resolve();
    const pending = retirements.get(generationId);
    if (pending) return pending;
    const old = segment(generationId);
    if (handleId === generationId) { handle = undefined; handleId = undefined; }
    if (old.state !== 'closed' && old.state !== 'abandoned') {
      old.state = 'retiring'; old.retiringAt ??= stamp(); old.outputDiscarded = true; save();
    }
    const retirement = Promise.resolve().then(async () => {
      if (reason === 'context_replaced') {
        try { await options.beforeRetire?.(generationId); }
        catch { problem('source_admission_unconfirmed'); }
      }
      if (!retiring) return;
      try { callbacks(generationId).onClosed(await retiring.close(reason)); }
      catch {
        if (old.state !== 'closed' && old.state !== 'abandoned') {
          old.state = 'abandoned'; old.closedAt = stamp(); old.providerFinalized = false;
          old.transcriptPersisted = false; save();
        }
        // This result belongs to the retired source. Its journal remains
        // unconfirmed without failing a successfully attached successor.
      }
    });
    retirements.set(generationId, retirement);
    void retirement.then(() => { retirements.delete(generationId); }, () => { retirements.delete(generationId); });
    return retirement;
  };
  if (state.phase !== 'idle' && state.phase !== 'stopped') {
    // The old socket and any unsent in-memory frames cannot be reconstructed.
    for (const old of state.segments) if (old.state !== 'closed' && old.state !== 'abandoned') {
      old.state = 'abandoned'; old.closedAt = stamp(); old.providerFinalized = false; old.outputDiscarded = true;
    }
    state.phase = 'idle'; state.deliveryAttachment = undefined; state.inputGap = 'restart_during_generation';
    save(); problem('restart_input_unconfirmed');
  }
  const sendInput = (frame: GenerationInputAudio & { bytes: number }) => {
    const current = segment(attachment().generationId);
    // A refused send may have an uncertain transport outcome. Never replay it.
    let accepted = false;
    try { accepted = handle!.appendAudio({ payload: frame.payload, callInputStartSample: frame.callInputStartSample,
      ...(frame.observedAt === undefined ? {} : { observedAt: frame.observedAt }) }); }
    catch { /* A throw may follow a send; preserve uncertainty and never replay. */ }
    if (!accepted) {
      inputBlocked = true; state.inputGap = 'transport_refused'; unavailable('input_transport_refused'); return false;
    }
    const firstInput = current.inputFirstSample === undefined;
    current.inputFirstSample ??= frame.callInputStartSample;
    current.inputLastSampleExclusive = frame.callInputStartSample + frame.bytes;
    if (firstInput) save();
    if (deliveryWaiters.size) settleDeliveryWaiters(false);
    return true;
  };
  const stopPacing = () => { clearTimeout(pacer); pacer = undefined; pacing = undefined; };
  /** Sends the queued backlog in order at no more than paceRate x real time. Live
   * frames that arrive meanwhile join the tail (appendAudio), so order and the
   * contiguous sample accounting are unchanged; direct sends resume once empty. */
  const drainInput = (check: () => void, maxFrames = Infinity) => {
    clearTimeout(pacer); pacer = undefined;
    pacing ??= { startedAt: now(), sentMs: 0 };
    let frames = 0;
    while (queued.length && frames < maxFrames) {
      const frame = queued[0];
      const frameMs = frame.bytes / 8;
      // The first frame of a window is always allowed; afterwards stay within real time.
      if (pacing.sentMs > 0 && pacing.sentMs + frameMs > (now() - pacing.startedAt) * paceRate) break;
      check();
      // A refused send may have an uncertain transport outcome. It leaves the queue first, never replayed.
      queued.shift(); queuedBytes -= frame.bytes;
      if (!sendInput(frame)) { stopPacing(); throw new LiveGenerationError('input_transport_refused'); }
      pacing.sentMs += frameMs;
      frames++;
    }
    if (!queued.length) { stopPacing(); save(); return; }
    const wait = Math.max(1, Math.ceil((pacing.sentMs + queued[0].bytes / 8) / paceRate - (now() - pacing.startedAt)));
    pacer = setTimeout(() => {
      pacer = undefined;
      // A block (replacement, unavailable, stop) pauses the replay; the next flush resumes it.
      if (stopped || inputBlocked || state.phase !== 'active' || !handle || activeId === undefined) { pacing = undefined; return; }
      try { drainInput(expire); }
      catch { /* sendInput already reported a refusal; stop/deadline own the remaining queue. */ }
    }, wait);
    pacer.unref?.();
  };
  const flushInput = (check: () => void, maxFrames = Infinity) => {
    if (!queued.length) { stopPacing(); save(); return; }
    drainInput(check, maxFrames);
  };
  const transient = (error: unknown): error is PrimaryApiError => error instanceof PrimaryApiError
    && (error.code === 'request_timeout' || error.code === 'request_failed'
      || (error.code === 'http_error' && error.retryable === true && [429, 502, 503, 504].includes(error.status ?? 0)));
  /** These three endpoints only read eligibility. Retrying never submits work,
   * answers a question or repeats an uncertain provider audio send. */
  const readContext = async <T>(stage: 'fetch' | 'assert' | 'delivery', operation: () => Promise<T>,
    deadline: number, onRetry?: () => void): Promise<T> => {
    const abort = new AbortController(); reads.add(abort);
    const interrupted = new Promise<never>((_resolve, reject) => {
      abort.signal.addEventListener('abort', () => reject(abort.signal.reason), { once: true });
    });
    void interrupted.catch(() => {});
    const check = () => {
      expire();
      if (abort.signal.aborted) throw abort.signal.reason;
      if (now() >= deadline) throw new LiveGenerationError('generation_transition_timeout');
    };
    const timer = setTimeout(() => abort.abort(new LiveGenerationError(
      now() >= Date.parse(clocks.callDeadlineAt) ? 'call_deadline' : 'generation_transition_timeout')),
    Math.max(1, deadline - now()));
    timer.unref?.();
    try {
      for (let attempt = 1; ; attempt++) {
        check(); const startedAt = now();
        try {
          const value = await Promise.race([operation(), interrupted]); check(); return value;
        } catch (error) {
          check();
          if (error instanceof PrimaryApiError) {
            try { options.onContextReadFailure?.({ stage, attempt, at: stamp(),
              durationMs: Math.max(0, now() - startedAt), code: error.code,
              ...(error.status === undefined ? {} : { status: error.status }),
              ...(error.remoteCode === undefined ? {} : { remoteCode: error.remoteCode }), retryable: transient(error) }); }
            catch { /* Diagnostic storage must not turn a recoverable read into a hangup. */ }
          }
          if (!transient(error) || attempt === 3) throw error;
          onRetry?.();
          // A brief bounded backoff avoids hammering an unavailable service.
          // The same deadline covers retries, replacement startup and delivery.
          let retryTimer: ReturnType<typeof setTimeout> | undefined;
          try { await Promise.race([new Promise<void>(resolve => { retryTimer = setTimeout(resolve, attempt * 100); }), interrupted]); }
          finally { clearTimeout(retryTimer); }
        }
      }
    } finally { clearTimeout(timer); reads.delete(abort); }
  };
  const restore = (current: LiveGenerationAttachment) => {
    expire();
    if (segment(current.generationId).state !== 'active' || handleId !== current.generationId || handle?.sessionId !== current.sessionId)
      throw new LiveGenerationError('generation_closed_during_context_recovery');
    activeId = current.generationId; state.phase = 'active'; state.deliveryAttachment = current; save();
    flushInput(expire);
    return attachment();
  };
  const replace = async (deadline: number): Promise<LiveGenerationAttachment> => {
    expire(); block('replacing');
    const abort = new AbortController();
    transitionAbort = abort;
    const transitionEndsAt = Math.min(deadline, now() + transitionTimeout, Date.parse(clocks.callDeadlineAt));
    const checkTransition = () => {
      expire();
      if (abort.signal.aborted) throw abort.signal.reason;
      if (now() >= transitionEndsAt) throw new LiveGenerationError('generation_transition_timeout');
    };
    const interrupted = new Promise<never>((_resolve, reject) => {
      abort.signal.addEventListener('abort', () => reject(abort.signal.reason), { once: true });
    });
    void interrupted.catch(() => {});
    const timer = setTimeout(() => abort.abort(new LiveGenerationError(
      now() >= Date.parse(clocks.callDeadlineAt) ? 'call_deadline' : 'generation_transition_timeout')),
    Math.max(1, transitionEndsAt - now()));
    timer.unref?.();
    const wait = async <T>(operation: () => Promise<T>): Promise<T> => {
      checkTransition();
      const result = await Promise.race([operation(), interrupted]);
      checkTransition();
      return result;
    };
    let candidate: { generationId: string; handle?: GenerationHandle } | undefined;
    try {
      // A second commit can invalidate a candidate during startup. One fresh
      // candidate is allowed, within this same deadline and unsent-input queue.
      for (let attempt = 0; attempt < 2; attempt++) {
        const context = parseEligiblePhoneCall(await wait(() => readContext('fetch', () => options.context.fetch(state.binding), transitionEndsAt)), state.binding);
        if (context.state !== 'open') throw new LiveGenerationError('call_not_open');
        // Old output is already gated. Keep its transport and source capture
        // until a fresh eligible page exists, then start the successor.
        void retire('context_replaced');
        state.binding = { ...state.binding, ownerPrincipalId: context.context.history.version.ownerPrincipalId,
          ...(context.callAssignmentRef ? { callAssignmentRef: context.callAssignmentRef } : {}) };
        const current: LiveGenerationJournal['segments'][number] = {
          generationId: newId(), ordinal: state.segments.length + 1, createdAt: stamp(), state: 'starting',
          version: context.context.history.version, pageDigest: context.context.history.pageDigest,
          transcript: { revision: 0, callerRevision: 0, fragments: [] }, outputDiscarded: false,
        };
        state.segments.push(current); save();
        admittedContexts.set(current.generationId, structuredClone(context));
        if(context.context.history.admission) admissions.set(current.generationId,structuredClone(context.context.history.admission));
        candidate = { generationId: current.generationId };
        const ownedCandidate = candidate;
        checkTransition();
        const allowOpening = canOpenInitially && !activated;
        const opening = options.open({ generationId: current.generationId, ordinal: current.ordinal, context,
          allowOpening, signal: abort.signal, callbacks: callbacks(current.generationId) });
        // A late adapter result never gains an attachment after stop/timeout.
        const openingCleanup = opening.then(async opened => {
          ownedCandidate.handle = opened;
          if (abort.signal.aborted) await retire('context_unavailable', current.generationId, opened);
        }, () => {});
        openings.add(openingCleanup);
        void openingCleanup.then(() => { openings.delete(openingCleanup); }, () => { openings.delete(openingCleanup); });
        const opened = await wait(() => opening);
        handle = opened; handleId = current.generationId;
        if (current.state !== 'starting' || !opened.sessionId) throw new LiveGenerationError('generation_closed_during_start');
        if (state.segments.some(value => value !== current && value.sessionId === opened.sessionId))
          throw new LiveGenerationError('generation_session_reused');
        current.sessionId = opened.sessionId; current.readyAt = stamp(); save();
        try { const checked=await wait(() => readContext('assert', () => options.context.assert(state.binding, observedVersion(current.generationId),admissions.get(current.generationId)), transitionEndsAt));
          recordVersion(current.generationId,checked.version); }
        catch (error) {
          if (attempt === 0 && changed(error)) { void retire('context_unavailable'); continue; }
          throw error;
        }
        if (current.state !== 'starting') throw new LiveGenerationError('generation_closed_during_start');
        current.state = 'active'; activeId = current.generationId; state.phase = 'active'; activated = true;
        state.deliveryAttachment = { generationId: current.generationId, sessionId: current.sessionId };
        save();
        // Dispatch the opening after at most one frame, even when synchronous
        // persistence takes long enough to earn more pacing allowance. Held
        // speech still drains through inputDelivered() before an outbound offer.
        flushInput(checkTransition, allowOpening && options.onInitialActivation ? 1 : Infinity);
        // The assert, activation, input flush and initial append dispatch share one
        // operation. Later publications still perform their own current-context check.
        if (allowOpening && options.onInitialActivation)
          await wait(() => options.onInitialActivation!(attachment()));
        const activeAttachment = attachment();
        try { options.onActivated?.(activeAttachment); } catch { /* Optional context. */ }
        return activeAttachment;
      }
      throw new LiveGenerationError('context_unavailable');
    } catch (error) {
      abort.abort(error);
      if (!stopped) unavailable(error instanceof LiveGenerationError ? error.code : 'context_unavailable');
      if (candidate?.handle) void retire(stopped ? 'call_stopped' : 'context_unavailable', candidate.generationId, candidate.handle);
      else if (!candidate) void retire(stopped ? 'call_stopped' : 'context_unavailable');
      else if (candidate) {
        const incomplete = segment(candidate.generationId);
        if (incomplete.state === 'starting') {
          incomplete.state = 'abandoned'; incomplete.closedAt = stamp(); incomplete.providerFinalized = false;
          incomplete.transcriptPersisted = false; incomplete.outputDiscarded = true; save();
        }
      }
      throw error;
    } finally {
      clearTimeout(timer);
      if (transitionAbort === abort) transitionAbort = undefined;
    }
  };
  const changed = (error: unknown) => error instanceof PrimaryApiError && error.status === 409 && error.remoteCode === 'context_changed';
  const beforeReuse = async (deadline: number): Promise<LiveGenerationAttachment> => {
    expire();
    if (state.phase !== 'active') return replace(deadline);
    const attached = attachment();
    const current = segment(attached.generationId);
    let suspended = false;
    try {
      const checked=await readContext('assert', () => options.context.assert(state.binding, observedVersion(current.generationId),admissions.get(current.generationId)), deadline, () => {
        if (!suspended) { suspended = true; block('replacing'); }
      });
      recordVersion(current.generationId,checked.version);
      if (suspended) return restore(attached);
    }
    catch (error) {
      if (changed(error)) return replace(deadline);
      unavailable('context_assert_unavailable'); await retire('context_unavailable'); throw error;
    }
    return attachment();
  };
  return {
    snapshot: (): LiveGenerationJournal => structuredClone(state),
    /** True when the delivery generation admits new sources in place: a new result then reaches
     * the same Live session instead of replacing it, so it needs no pause in Dexter's speech. */
    admitsSources: () => state.phase === 'active' && state.deliveryAttachment !== undefined
      && admissions.has(state.deliveryAttachment.generationId),
    bufferedAudioBytes: () => queuedBytes,
    /** Settles true once caller input before this call sample has been sent to the active generation
     * (a paced backlog replay included), or false if that generation stops taking input first. Not
     * queued: an initial activation can await it. It sends nothing. */
    inputDelivered(sampleExclusive: number): Promise<boolean> {
      if (stopped || state.phase !== 'active' || activeId === undefined) return Promise.resolve(false);
      const sent = segment(activeId).inputLastSampleExclusive;
      if (sent !== undefined && sent >= sampleExclusive) return Promise.resolve(true);
      return new Promise(resolve => { deliveryWaiters.add({ sample: sampleExclusive, resolve }); });
    },
    start: () => serial(beforeReuse),
    beforeReuse: () => serial(beforeReuse),
    /** Keep a complete status publication inside the same queue as exact result
     * delivery. Calling beforeReuse and publishing later permits another result
     * to interleave between chunks of a question. The callback must not recurse
     * into this controller's queued methods. */
    withCurrentAttachment<T>(publish: (current: LiveGenerationAttachment) => Promise<T>,
      stillRelevant: () => boolean = () => true): Promise<T> {
      return serial(async deadline => {
        if (!stillRelevant()) throw new LiveGenerationError('presentation_deferred');
        const current = await beforeReuse(deadline);
        const result = await publish(current);
        if (attachment().generationId !== current.generationId)
          throw new LiveGenerationError('publication_generation_changed');
        return result;
      });
    },
    appendAudio(frame: GenerationInputAudio): 'sent' | 'buffered' | 'refused' {
      if (stopped || now() >= Date.parse(clocks.callDeadlineAt)) { problem('input_after_call_deadline'); return 'refused'; }
      if (inputBlocked) return 'refused';
      let bytes: Buffer;
      try {
        if (typeof frame.payload !== 'string' || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(frame.payload)
          || frame.payload.length > 85_336) throw new Error();
        bytes = Buffer.from(frame.payload, 'base64');
        if (!bytes.length || bytes.length > MAX_INPUT_FRAME_BYTES || bytes.toString('base64') !== frame.payload || !Number.isSafeInteger(frame.callInputStartSample)
          || frame.callInputStartSample !== state.inputNextSample || (frame.observedAt !== undefined
            && (!Number.isSafeInteger(frame.observedAt) || frame.observedAt < 0 || frame.observedAt > now()))) throw new Error();
      } catch { inputBlocked = true; state.inputGap = 'invalid_audio'; unavailable('invalid_input_audio'); return 'refused'; }
      const next = { ...frame, bytes: bytes.length };
      if (state.phase === 'active' && !queued.length) {
        state.inputNextSample += bytes.length;
        return sendInput(next) ? 'sent' : 'refused';
      }
      if (queuedBytes + bytes.length > maxBuffered) {
        inputBlocked = true; state.inputGap = 'buffer_overflow'; unavailable('input_buffer_overflow'); return 'refused';
      }
      state.inputNextSample += bytes.length; queued.push(next); queuedBytes += bytes.length;
      return 'buffered';
    },
    deliver(requestId: string, source: EligibleDeliveryTarget, laterCall?: RequestDeliveryJournal,
      stillRelevant: () => boolean = () => true): Promise<GenerationDeliveryOutcome> {
      return serial(async deadline => {
        if (!stillRelevant()) throw new LiveGenerationError('presentation_deferred');
        let currentAttachment = await beforeReuse(deadline);
        let current = segment(currentAttachment.generationId);
        let response: EligibleDeliveryResponse;
        const readDelivery = async () => {
          let suspended = false;
          const value = await readContext('delivery', () => options.context.delivery(state.binding, requestId, { version: observedVersion(current.generationId), source, ...(admissions.has(current.generationId)?{admission:admissions.get(current.generationId)}:{}) }, laterCall), deadline, () => {
            if (!suspended) { suspended = true; block('replacing'); }
          });
          if (suspended) {
            await readContext('assert', () => options.context.assert(state.binding, observedVersion(current.generationId),admissions.get(current.generationId)), deadline);
            restore(currentAttachment);
          }
          return value;
        };
        try { response = await readDelivery(); }
        catch (error) {
          if (!changed(error)) { unavailable('eligible_delivery_unavailable'); await retire('context_unavailable'); throw error; }
          currentAttachment = await replace(deadline); current = segment(currentAttachment.generationId);
          // One replacement for this refusal; a second refusal propagates, never loops.
          try { response = await readDelivery(); }
          catch (retryError) { unavailable('eligible_delivery_unavailable'); await retire('context_unavailable'); throw retryError; }
        }
        if(admissions.has(current.generationId)) {
          if(!response.admission) throw new LiveGenerationError('delivery_generation_changed');
          admissions.set(current.generationId,structuredClone(response.admission));
          recordVersion(current.generationId,response.version);
        }
        const stillCurrent = attachment();
        if (stillCurrent.generationId !== currentAttachment.generationId || !sameHistoryVersion(response.version, observedVersion(current.generationId)))
          throw new LiveGenerationError('delivery_generation_changed');
        const outcome: GenerationDeliveryOutcome = { requestId, attachment: currentAttachment, response };
        if (response.delivery.state === 'ready') {
          if (!stillRelevant()) return { ...outcome, publication: { eventId: '', status: 'stale', reason: 'caller_context_changed' } };
          outcome.publication = await handle!.publish({ requestId, version: response.version, delivery: response.delivery,
            ...('deliveryAssociation' in response ? { deliveryAssociation: response.deliveryAssociation } : {}) });
          if (state.deliveryAttachment?.generationId !== currentAttachment.generationId || state.phase !== 'active')
            outcome.publication = { eventId: outcome.publication.eventId, status: 'stale', reason: 'generation_changed' };
        }
        return outcome;
      });
    },
    stop(): Promise<void> {
      // Gate immediately, including while fetch/open/assert is awaiting HTTP.
      // The host terminates the provider call on its own absolute timer without
      // awaiting this queued Live cleanup; a stalled adapter cannot extend it.
      stopped = true; block('stopped'); stopPacing();
      transitionAbort?.abort(new LiveGenerationError('call_stopped'));
      for (const read of reads) read.abort(new LiveGenerationError('call_stopped'));
      if (queuedBytes) problem('undelivered_buffered_audio');
      return serial(async () => {
        await retire('call_stopped');
        await Promise.all([...openings]);
        await Promise.all([...retirements.values()]);
      });
    },
  };
}
