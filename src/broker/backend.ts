import { createHash } from 'node:crypto';
import type { BrokerLiveBackend, LiveBackendChannel, LiveDelegationRequest, LiveTranscriptSnapshot } from '../live/session.js';
import { LIVE_APPEND_MAX_BYTES } from '../live/session.js';
import type { PrimaryApiClient, PrimaryQuestion, PrimaryQuestionResult, PrimaryReplyInput, PrimaryResult, PrimarySubmission, PrimaryTurn } from './primaryApi.js';
import { PrimaryApiError } from './primaryApi.js';
import type { BrokerCall, BrokerStore } from './store.js';

export interface PrimaryBackendOptions {
  callId: string;
  callSid: string;
  bindingId: string;
  api: Pick<PrimaryApiClient, 'submit' | 'result' | 'captureTurn' | 'question' | 'reply'>;
  store: Pick<BrokerStore, 'get' | 'update'>;
  stabilityMs?: number;
  maxWaitMs?: number;
  pollIntervalMs?: number;
  maxPollMs?: number;
  now?: () => number;
  lastCallerSpeechAt?: () => number | undefined;
  mediaStartedAt?: string;
}

export interface PrimaryBackend extends BrokerLiveBackend {
  /** Host-observed acoustic speech time, in Date.now() milliseconds. */
  noteCallerSpeech(atMs?: number, audioOffsetMs?: number): void;
  /** Freeze the observed spans before the host reconstructs closeout after disconnect. */
  persistAcousticEvidence(): void;
  captureCompletedTurns(snapshot: LiveTranscriptSnapshot, proof: { assistantPlaybackComplete: boolean; sessionFinalized?: boolean }): Promise<{
    captured: number; pending: number; unsupported: string[];
  }>;
  /** Reconcile uncertain admission with GET, then replay only exact journalled captures. */
  flushPendingCaptures(): Promise<{ pending: number; unsupported: string[] }>;
}

export interface BrokerDelegationRecord {
  version: 1;
  requestId: string;
  callId: string;
  callSid: string;
  bindingId: string;
  sessionId: string;
  delegationIds: string[];
  status: 'prepared' | 'dispatching' | 'dispatch_unknown' | 'queued' | 'running' | 'waiting_for_reply' | 'completed' | 'failed' | 'cancelled';
  updatedAt: string;
  submission: PrimarySubmission;
  frozenCallerRevision: number;
  firstSequence: number;
  lastSequence: number;
  completionBasis: 'transcript_stability' | 'speech_and_transcript_stability';
  sourceRevision: number;
  capturedText: string;
  pendingCapture?: PrimaryTurn;
  captureState?: 'dispatching' | 'accepted' | 'unknown';
  laterCallerRevision?: number;
  correctionState?: 'recorded_not_applied';
  primaryResult?: PrimaryResult;
  result?: string;
  delivery?: string;
  questions?: Record<string, BrokerQuestionRecord>;
  currentQuestionId?: string;
  questionNotice?: 'answer_too_long' | 'reply_unknown' | 'reply_refused' | 'question_unavailable' | 'answered_elsewhere';
  deferredTurns?: Array<{ delegationId: string; sourceEventId: string; text: string; firstSequence: number;
    callerRevision: number; status: 'recorded_not_submitted' }>;
}

export interface BrokerQuestionRecord {
  question: PrimaryQuestion;
  identityDigest: string;
  presentation?: { callerRevision: number; sequence: number; lastCallerEndMs: number; acceptedAt: string };
  reply?: BrokerQuestionReplyRecord;
  /** Earlier proven non-admissions remain owned evidence when the caller answers again. */
  refusedReplies?: BrokerQuestionReplyRecord[];
}

export interface BrokerQuestionReplyRecord {
  input: PrimaryReplyInput;
  inputDigest: string;
  status: 'prepared' | 'dispatching' | 'unknown' | 'accepted' | 'refused';
  callerRevision: number;
  firstSequence: number;
  lastSequence: number;
  refusal?: { origin: 'local_validation' | 'api_non_admission'; code: string; status?: number };
}

const terminal = (state: string) => ['completed', 'failed', 'cancelled'].includes(state);
const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const digest = (value: unknown) => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const unresolvedReply = (reply: BrokerQuestionRecord['reply']) => reply && ['prepared', 'dispatching', 'unknown'].includes(reply.status);
const replyAttempts = (record: BrokerQuestionRecord) => [...(record.refusedReplies ?? []), ...(record.reply ? [record.reply] : [])];
// These codes precede new-answer admission in the exact Phone source contract.
// This classifier is used only for a first POST, never to resolve an older unknown POST.
const firstReplyNonAdmission = (error: unknown): BrokerQuestionReplyRecord['refusal'] => {
  if (!(error instanceof PrimaryApiError)) return undefined;
  if (error.code === 'invalid_request') return { origin: 'local_validation', code: error.code };
  if (error.code !== 'http_error' || !error.status || !error.remoteCode) return undefined;
  const codes: Record<number, readonly string[]> = {
    400: ['invalid_request'],
    401: ['invalid_service_credential'],
    403: ['caller_possession_required', 'binding_unauthorized', 'caller_unlinked', 'caller_binding_ambiguous'],
    404: ['request_not_found', 'question_not_found'],
    409: ['call_closed', 'source_conflict', 'idempotency_conflict', 'question_cancelled', 'already_answered'],
    410: ['question_expired'],
    415: ['unsupported_media_type'],
    503: ['phone_questions_unavailable', 'primary_unavailable'],
  };
  return codes[error.status]?.includes(error.remoteCode)
    ? { origin: 'api_non_admission', code: error.remoteCode, status: error.status } : undefined;
};
const questionIdentity = (question: PrimaryQuestion) => digest([
  question.id, question.revision, question.expiresAt, question.runId, question.threadId,
  question.agent.id, question.question, question.context, question.proposedAnswer,
  question.yesMeaning, question.noMeaning, question.createdAt,
]);

/** Only literal yes/no chooses the canonical meanings supplied by the question. */
export function literalPhoneAnswer(text: string): Pick<PrimaryReplyInput['reply'], 'answer' | 'text'> {
  const literal = text.trim().toLowerCase();
  const word = literal.match(/^(yes|no)[.!]?$/)?.[1];
  if (word === 'yes' || word === 'no') return { answer: word, text: null };
  if (!text.trim() || Buffer.byteLength(text) > 2000) throw new Error('answer_too_long');
  return { answer: 'other', text: text.trim() };
}

interface AcousticSpan { startMs: number; endMs: number; lastSpeechAt: number }
interface CanonicalTurnRecord extends PrimaryTurn {
  state: 'dispatching' | 'captured' | 'unknown';
  acousticSpan?: AcousticSpan;
  recordId?: string;
}
// A closeout backend can coexist briefly with the detached voice backend. Both
// must serialize the same call journal and capture submissions.
const storeAdmissions = new WeakMap<object, Map<string, Promise<void>>>();

/** Timestamp grouping is application presentation state, not a provider turn-complete event. */
function callerGroups(snapshot: LiveTranscriptSnapshot) {
  const fragments = snapshot.fragments.filter(fragment => fragment.role === 'user')
    .sort((a, b) => a.startMs - b.startMs || a.sequence - b.sequence);
  const groups: typeof fragments[] = [];
  for (const fragment of fragments) {
    const previous = groups.at(-1);
    if (!previous || fragment.startMs - Math.max(...previous.map(part => part.endMs)) > 1200) groups.push([fragment]);
    else previous.push(fragment);
  }
  return groups;
}

const validTurnText = (text: string) => text.trim().length > 0 && [...text].length <= 4000
  && !text.includes('\0') && !/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/u.test(text);

/** Deterministic UUIDs separate source identity from the provider delegation ID. */
export function brokerIdentity(kind: string, ...parts: string[]): string {
  const hash = createHash('sha256').update(JSON.stringify(['dexter-broker/primary/v1', kind, ...parts])).digest('hex');
  return `${hash.slice(0, 8)}-${hash.slice(8, 12)}-5${hash.slice(13, 16)}-a${hash.slice(17, 20)}-${hash.slice(20, 32)}`;
}

/** Split at whitespace, retaining every character and keeping numeric tokens intact. */
export function liveResultChunks(text: string): string[] {
  const chunks: string[] = [];
  let current = '';
  for (const token of text.match(/\S+\s*|\s+/gu) ?? []) {
    if (Buffer.byteLength(token) > LIVE_APPEND_MAX_BYTES) throw new Error('result_token_exceeds_voice_limit');
    if (Buffer.byteLength(current + token) > LIVE_APPEND_MAX_BYTES) { chunks.push(current); current = ''; }
    current += token;
  }
  if (current) chunks.push(current);
  return chunks;
}

function shortSpokenResult(text: string): string {
  if (Buffer.byteLength(text) <= LIVE_APPEND_MAX_BYTES) return text;
  const sentence = text.match(/^\s*[\s\S]*?[.!?](?=\s|$)/u)?.[0]?.trim();
  if (sentence && Buffer.byteLength(sentence) <= LIVE_APPEND_MAX_BYTES) return sentence;
  return 'I have the full answer available. Which detail would you like first?';
}

function delay(ms: number, signal: AbortSignal): Promise<boolean> {
  if (signal.aborted) return Promise.resolve(false);
  return new Promise(resolve => {
    const finish = (elapsed: boolean) => { clearTimeout(timer); signal.removeEventListener('abort', abort); resolve(elapsed); };
    const abort = () => finish(false);
    const timer = setTimeout(() => finish(true), ms);
    signal.addEventListener('abort', abort, { once: true });
  });
}

export function createPrimaryBackend(options: PrimaryBackendOptions): PrimaryBackend {
  const now = options.now ?? Date.now;
  const positive = (value: number | undefined, fallback: number, max: number) => {
    const result = value ?? fallback;
    if (!Number.isInteger(result) || result < 1 || result > max) throw new Error('invalid_backend_timing');
    return result;
  };
  const stabilityMs = positive(options.stabilityMs, 700, 10_000);
  const maxWaitMs = positive(options.maxWaitMs, 30_000, 60_000);
  const pollIntervalMs = positive(options.pollIntervalMs, 750, 10_000);
  const maxPollMs = positive(options.maxPollMs, 90_000, 300_000);
  if (maxWaitMs < stabilityMs || !options.callId || !options.callSid || !uuidPattern.test(options.bindingId))
    throw new Error('invalid_backend_configuration');
  let lastSpeechAt: number | undefined;
  const savedSpans = options.store.get(options.callId)?.acousticSpans;
  const acousticSpans: AcousticSpan[] = Array.isArray(savedSpans) ? savedSpans.map((span: AcousticSpan) => {
    if (!span || ![span.startMs, span.endMs, span.lastSpeechAt].every(Number.isFinite)
        || span.startMs < 0 || span.endMs < span.startMs || span.lastSpeechAt < 0) throw new Error('invalid_saved_acoustic_span');
    return { ...span };
  }) : [];
  const speechListeners = new Set<() => void>();
  let admissions = storeAdmissions.get(options.store);
  if (!admissions) { admissions = new Map(); storeAdmissions.set(options.store, admissions); }
  const locked = async <T>(fn: () => Promise<T>): Promise<T> => {
    const previous = admissions!.get(options.callId) ?? Promise.resolve();
    let release!: () => void;
    const tail = new Promise<void>(resolve => { release = resolve; });
    admissions!.set(options.callId, tail);
    await previous;
    try { return await fn(); } finally {
      release();
      if (admissions!.get(options.callId) === tail) admissions!.delete(options.callId);
    }
  };
  const boundCall = (sessionId: string): BrokerCall => {
    const call = options.store.get(options.callId);
    if (!call || call.id !== options.callId || call.callSid !== options.callSid
        || call.primary?.bindingId !== options.bindingId || call.liveSessionId !== sessionId)
      throw new Error('broker_call_identity_mismatch');
    return call;
  };
  const entries = (sessionId: string): BrokerDelegationRecord[] => {
    return Object.values(boundCall(sessionId).delegations).map(value => {
      const entry = value as BrokerDelegationRecord;
      if (entry.version !== 1 || !uuidPattern.test(entry.requestId) || entry.callId !== options.callId
          || entry.callSid !== options.callSid || entry.bindingId !== options.bindingId
          || !Array.isArray(entry.delegationIds) || !entry.submission?.source
          || !uuidPattern.test(entry.submission.source.sourceEventId)) throw new Error('invalid_saved_delegation');
      for (const [questionId, record] of Object.entries(entry.questions ?? {})) {
        if (record.question.id !== questionId || record.identityDigest !== questionIdentity(record.question))
          throw new Error('invalid_saved_question');
        if (record.refusedReplies?.some(reply => reply.status !== 'refused')) throw new Error('invalid_saved_reply_history');
        for (const reply of replyAttempts(record)) if (reply.inputDigest !== digest(reply.input) || reply.input.source.callSid !== options.callSid
            || reply.input.source.revision !== 1 || reply.input.reply.questionRevision !== 1
            || !uuidPattern.test(reply.input.reply.clientReplyId) || !uuidPattern.test(reply.input.source.sourceEventId))
          throw new Error('invalid_saved_reply');
      }
      return entry;
    }).filter(entry => entry.sessionId === sessionId);
  };
  const save = (entry: BrokerDelegationRecord): BrokerDelegationRecord => {
    const call = boundCall(entry.sessionId);
    const next = { ...entry, updatedAt: new Date(now()).toISOString() };
    const canonicalTurns = { ...call.canonicalTurns };
    // Refusal proves that reply admission did not capture this turn. Freeze its
    // original source/text atomically with the refusal, before STT can revise it.
    // Ordinary capture recovery acknowledges revision 1 before revision 2 is sent.
    for (const question of Object.values(next.questions ?? {})) for (const reply of replyAttempts(question)) {
      if (reply.status !== 'refused') continue;
      const input = reply.input;
      const previous = canonicalTurns[input.source.sourceEventId] as CanonicalTurnRecord | undefined;
      if (previous && (previous.itemId !== input.source.itemId || previous.occurredAt !== input.source.receivedAt
          || previous.role !== 'user' || (previous.revision === 1 && previous.text !== input.text)))
        throw new Error('reply_source_identity_changed');
      if (!previous) canonicalTurns[input.source.sourceEventId] = {
        sourceEventId: input.source.sourceEventId, revision: 1, itemId: input.source.itemId,
        role: 'user', text: input.text, occurredAt: input.source.receivedAt, state: 'dispatching',
      } satisfies CanonicalTurnRecord;
    }
    options.store.update(call.id, { delegations: { ...call.delegations, [entry.requestId]: next }, canonicalTurns });
    return next;
  };
  const load = (entry: BrokerDelegationRecord) => entries(entry.sessionId).find(value => value.requestId === entry.requestId)!;
  const knownReply = (sessionId: string, sourceEventId: string) => {
    for (const entry of entries(sessionId)) for (const [questionId, record] of Object.entries(entry.questions ?? {})) {
      const reply = replyAttempts(record).find(attempt => attempt.input.source.sourceEventId === sourceEventId);
      if (reply) return { entry, questionId, record, reply };
    }
  };
  const observeQuestion = (entry: BrokerDelegationRecord, question: PrimaryQuestion): BrokerDelegationRecord => {
    if (!uuidPattern.test(question.id) || question.revision !== 1 || !uuidPattern.test(question.runId)
        || !uuidPattern.test(question.threadId) || !uuidPattern.test(question.agent.id)
        || !Number.isFinite(Date.parse(question.expiresAt)) || !Number.isFinite(Date.parse(question.createdAt))
        || !['pending', 'answered', 'expired', 'cancelled', 'unavailable'].includes(question.state)
        || (question.state === 'answered') !== Boolean(question.reply)) throw new Error('invalid_primary_question');
    const known = Object.values(entry.questions ?? {})[0]?.question;
    if (known && (known.runId !== question.runId || known.threadId !== question.threadId || known.agent.id !== question.agent.id))
      throw new Error('primary_question_owner_mismatch');
    const previous = entry.questions?.[question.id];
    const identityDigest = questionIdentity(question);
    if (previous && (previous.identityDigest !== identityDigest
        || (previous.question.state !== 'pending' && question.state === 'pending')
        || (previous.question.reply && digest(previous.question.reply) !== digest(question.reply))))
      throw new Error('primary_question_identity_changed');
    return { ...entry, questions: { ...entry.questions,
      [question.id]: { ...previous, question: structuredClone(question), identityDigest } } };
  };
  const applyReplyObservation = (entry: BrokerDelegationRecord, questionId: string, result: PrimaryQuestionResult) => {
    if (result.bindingId !== options.bindingId || result.requestId !== entry.requestId || result.question.id !== questionId)
      throw new Error('primary_question_identity_mismatch');
    let current = observeQuestion(load(entry), result.question);
    const record = current.questions![questionId];
    const reply = record.reply;
    if (!reply) return save(current);
    if (result.question.state === 'answered') {
      const received = result.question.reply!;
      const expected = reply.input.reply;
      if (received.clientReplyId !== expected.clientReplyId) {
        if (reply.status === 'accepted') throw new Error('primary_reply_identity_changed');
        return save({ ...current, questionNotice: 'answered_elsewhere', questions: { ...current.questions,
          [questionId]: { ...record, reply: { ...reply, status: 'refused' } } } });
      }
      if (received.questionRevision !== expected.questionRevision || received.answer !== expected.answer || received.text !== expected.text)
        throw new Error('primary_reply_identity_changed');
      const input = reply.input;
      const previous = boundCall(entry.sessionId).canonicalTurns?.[input.source.sourceEventId] as CanonicalTurnRecord | undefined;
      if (previous && (previous.itemId !== input.source.itemId || previous.occurredAt !== input.source.receivedAt
          || (previous.revision === 1 && previous.text !== input.text))) throw new Error('reply_source_identity_changed');
      if (!previous) persistCanonical(entry.sessionId, { sourceEventId: input.source.sourceEventId,
        revision: 1, itemId: input.source.itemId, role: 'user', text: input.text,
        occurredAt: input.source.receivedAt, state: 'captured', recordId: input.source.sourceEventId });
      current = { ...current, status: terminal(current.status) ? current.status : 'running', questionNotice: undefined,
        questions: { ...current.questions, [questionId]: { ...record, reply: { ...reply, status: 'accepted' } } } };
    } else if (result.question.state !== 'pending') {
      current = { ...current, questionNotice: 'question_unavailable', questions: { ...current.questions,
        [questionId]: { ...record, reply: { ...reply, status: 'refused' } } } };
    } else if (unresolvedReply(reply)) current = { ...current, questionNotice: 'reply_unknown' };
    return save(current);
  };
  const recoverReplies = async (entry: BrokerDelegationRecord) => {
    let current = load(entry);
    for (const [questionId, record] of Object.entries(current.questions ?? {})) {
      if (!unresolvedReply(record.reply)) continue;
      try { current = applyReplyObservation(current, questionId, await options.api.question(options.bindingId, current.requestId, questionId)); }
      catch { current = save({ ...load(current), questionNotice: 'reply_unknown' }); }
    }
    return current;
  };
  const mediaOrigin = (sessionId: string): number => {
    const origin = options.mediaStartedAt ?? boundCall(sessionId).mediaStartedAt;
    if (!origin || !Number.isFinite(Date.parse(origin))) throw new Error('media_time_origin_missing');
    return Date.parse(origin);
  };
  const speechTime = () => options.lastCallerSpeechAt?.() ?? lastSpeechAt;
  const acousticGroups = (snapshot: LiveTranscriptSnapshot, observedAt = now()) => {
    const grouped = acousticSpans.map(span => ({ span, fragments: [] as LiveTranscriptSnapshot['fragments'], ambiguous: false }));
    for (const fragment of snapshot.fragments.filter(f => f.role === 'user')) {
      // Provider timestamps are approximate. Match one acoustic span only; ambiguous
      // fragments stay unqualified instead of silently crossing source boundaries.
      const matches = grouped.filter(({ span }) => fragment.endMs >= span.startMs - 300 && fragment.startMs <= span.endMs + 300);
      if (matches.length === 1) matches[0].fragments.push(fragment);
      else for (const match of matches) match.ambiguous = true;
    }
    return grouped.filter(({ span, fragments, ambiguous }, index) => fragments.length && !ambiguous
      && (index < acousticSpans.length - 1 || observedAt - span.lastSpeechAt >= stabilityMs))
      // Timing chooses the source span; delta text itself is appended exactly in
      // provider delivery order, as required by the Live transcript contract.
      .map(group => ({ ...group, fragments: group.fragments.sort((a, b) => a.sequence - b.sequence) }));
  };
  const stableContext = (channel: LiveBackendChannel): Promise<LiveTranscriptSnapshot | undefined> => {
    if (channel.signal.aborted) return Promise.resolve(undefined);
    return new Promise(resolve => {
      let ended = false, timer: ReturnType<typeof setTimeout> | undefined;
      let revision = channel.getContext().callerRevision;
      let changedAt = now();
      const finish = (snapshot?: LiveTranscriptSnapshot) => {
        if (ended) return;
        ended = true; clearTimeout(timer); clearTimeout(limit);
        unsubscribe(); speechListeners.delete(schedule); channel.signal.removeEventListener('abort', abort);
        resolve(snapshot);
      };
      const schedule = () => {
        if (ended) return;
        clearTimeout(timer);
        const snapshot = channel.getContext();
        if (snapshot.callerRevision !== revision) { revision = snapshot.callerRevision; changedAt = now(); }
        const last = Math.max(changedAt, speechTime() ?? 0);
        timer = setTimeout(() => {
          const current = channel.getContext();
          if (current.callerRevision !== revision || now() - Math.max(changedAt, speechTime() ?? 0) < stabilityMs) { schedule(); return; }
          if (current.fragments.some(fragment => fragment.role === 'user' && fragment.text.trim())) finish(current);
        }, Math.max(1, stabilityMs - (now() - last)));
      };
      const abort = () => finish();
      const unsubscribe = channel.subscribeContext(schedule);
      const limit = setTimeout(() => finish(), maxWaitMs);
      speechListeners.add(schedule); channel.signal.addEventListener('abort', abort, { once: true });
      schedule();
    });
  };
  const accept = async (request: LiveDelegationRequest, channel: LiveBackendChannel): Promise<BrokerDelegationRecord | undefined> => {
    if (request.callId !== options.callId) throw new Error('broker_call_identity_mismatch');
    const before = entries(request.sessionId);
    const replay = before.find(entry => entry.delegationIds.includes(request.delegationId));
    if (replay) return replay;
    const snapshot = await stableContext(channel);
    if (!snapshot || channel.signal.aborted) return undefined;
    return locked(async () => {
      if (channel.signal.aborted) return undefined;
      if (boundCall(request.sessionId).endedAt) throw new Error('broker_call_ended');
      const saved = entries(request.sessionId);
      const duplicate = saved.find(entry => entry.delegationIds.includes(request.delegationId));
      if (duplicate) return duplicate;
      const callStartedAt = mediaOrigin(request.sessionId);
      const latestSourceStart = Math.max(0, ...saved.map(entry => Date.parse(entry.submission.source.receivedAt) - callStartedAt));
      const candidate = acousticGroups(snapshot).filter(group => {
        const first = group.fragments.reduce((a, b) => a.sequence < b.sequence ? a : b);
        const source = brokerIdentity('source', options.callId, request.sessionId, first.eventId);
        return first.startMs >= latestSourceStart && !saved.some(entry => entry.submission.source.sourceEventId === source
          || entry.deferredTurns?.some(turn => turn.sourceEventId === source)
          || Object.values(entry.questions ?? {}).some(record => replyAttempts(record).some(reply => reply.input.source.sourceEventId === source)));
      }).at(-1);
      const fragments = candidate?.fragments;
      const active = saved.find(entry => !terminal(entry.status));
      if (active) {
        let current = save({ ...active, delegationIds: [...active.delegationIds, request.delegationId] });
        if (Object.values(current.questions ?? {}).some(record => unresolvedReply(record.reply))) {
          current = await recoverReplies(current);
          if (Object.values(current.questions ?? {}).some(record => unresolvedReply(record.reply))) return current;
        }
        const questionId = current.currentQuestionId;
        const question = questionId ? current.questions?.[questionId] : undefined;
        const presentation = question && question.presentation;
        const first = fragments?.reduce((a, b) => a.sequence < b.sequence ? a : b);
        if (current.status === 'waiting_for_reply' && question && questionId && question.question.state === 'pending'
            && (!question.reply || question.reply.status === 'refused') && presentation && first && first.sequence > presentation.sequence
            && snapshot.callerRevision > presentation.callerRevision && first.startMs > presentation.lastCallerEndMs) {
          const text = fragments!.map(fragment => fragment.text).join('');
          if (!validTurnText(text)) throw new Error('invalid_completed_caller_turn');
          let answer: ReturnType<typeof literalPhoneAnswer>;
          try { answer = literalPhoneAnswer(text); }
          catch {
            return save({ ...current, questionNotice: 'answer_too_long', deferredTurns: [...current.deferredTurns ?? [], {
              delegationId: request.delegationId, sourceEventId: brokerIdentity('source', options.callId, request.sessionId, first.eventId),
              text, firstSequence: first.sequence, callerRevision: snapshot.callerRevision, status: 'recorded_not_submitted',
            }] });
          }
          // Re-read this exact question before admitting a reply; another channel may
          // already have answered it or its authority may have changed.
          try {
            current = applyReplyObservation(current, questionId, await options.api.question(options.bindingId, current.requestId, questionId));
          } catch { return save({ ...load(current), questionNotice: 'question_unavailable' }); }
          const refreshed = current.questions![questionId];
          if (refreshed.question.state !== 'pending' || Date.parse(refreshed.question.expiresAt) <= now())
            return save({ ...current, questionNotice: 'question_unavailable' });
          if (channel.signal.aborted || boundCall(request.sessionId).endedAt) return current;
          const sourceEventId = brokerIdentity('source', options.callId, request.sessionId, first.eventId);
          const input: PrimaryReplyInput = {
            source: { callSid: options.callSid, itemId: `live-turn-${sourceEventId}`, sourceEventId, revision: 1,
              receivedAt: new Date(mediaOrigin(request.sessionId) + first.startMs).toISOString() },
            role: 'user', text, reply: { questionRevision: 1,
              clientReplyId: brokerIdentity('question-reply', options.callId, request.sessionId, current.requestId, questionId, sourceEventId), ...answer },
          };
          options.store.update(options.callId, { acousticSpans: structuredClone(acousticSpans) });
          const reply: NonNullable<BrokerQuestionRecord['reply']> = { input, inputDigest: digest(input), status: 'prepared',
            callerRevision: snapshot.callerRevision, firstSequence: first.sequence,
            lastSequence: Math.max(...fragments!.map(fragment => fragment.sequence)) };
          if (refreshed.reply && refreshed.reply.status !== 'refused') return current;
          current = save({ ...current, questionNotice: undefined, questions: { ...current.questions,
            [questionId]: { ...refreshed, refusedReplies: [...refreshed.refusedReplies ?? [], ...refreshed.reply ? [refreshed.reply] : []], reply } } });
          current = save({ ...current, questions: { ...current.questions,
            [questionId]: { ...current.questions![questionId], reply: { ...reply, status: 'dispatching' } } } });
          try {
            return applyReplyObservation(current, questionId, await options.api.reply(options.bindingId, current.requestId, questionId, structuredClone(input)));
          } catch (error) {
            const refusal = firstReplyNonAdmission(error);
            return save({ ...load(current), questionNotice: refusal ? 'reply_refused' : 'reply_unknown', questions: { ...load(current).questions,
              [questionId]: { ...load(current).questions![questionId], reply: { ...reply,
                status: refusal ? 'refused' : 'unknown', ...(refusal ? { refusal } : {}) } } } });
          }
        }
        const deferredTurns = [...active.deferredTurns ?? []];
        if (fragments?.length) {
          const first = fragments.reduce((a, b) => a.sequence < b.sequence ? a : b);
          const text = fragments.map(fragment => fragment.text).join('');
          if (validTurnText(text)) deferredTurns.push({ delegationId: request.delegationId,
            sourceEventId: brokerIdentity('source', options.callId, request.sessionId, first.eventId), text,
            firstSequence: first.sequence, callerRevision: snapshot.callerRevision, status: 'recorded_not_submitted' });
        }
        // Preserve a separate caller request without guessing whether it corrects the
        // admitted action. It requires another caller utterance, never an automatic run.
        return save({ ...current, deferredTurns });
      }
      if (!fragments?.length) {
        const latest = saved.sort((a, b) => b.lastSequence - a.lastSequence)[0];
        return latest ? save({ ...latest, delegationIds: [...latest.delegationIds, request.delegationId] }) : undefined;
      }
      const text = fragments.map(fragment => fragment.text).join('');
      if (!validTurnText(text)) throw new Error('invalid_completed_caller_turn');
      const first = fragments.reduce((a, b) => a.sequence < b.sequence ? a : b);
      options.store.update(options.callId, { acousticSpans: structuredClone(acousticSpans) });
      const sourceEventId = brokerIdentity('source', options.callId, request.sessionId, first.eventId);
      const duplicateSource = saved.find(entry => entry.submission.source.sourceEventId === sourceEventId);
      if (duplicateSource) return save({ ...duplicateSource, delegationIds: [...duplicateSource.delegationIds, request.delegationId] });
      let entry: BrokerDelegationRecord = {
        version: 1, requestId: brokerIdentity('request', options.callId, request.sessionId, request.delegationId),
        callId: options.callId, callSid: options.callSid, bindingId: options.bindingId, sessionId: request.sessionId,
        delegationIds: [request.delegationId], status: 'prepared', updatedAt: new Date(now()).toISOString(),
        submission: { source: { callSid: options.callSid, itemId: `live-turn-${sourceEventId}`,
          sourceEventId, revision: 1, receivedAt: new Date(mediaOrigin(request.sessionId) + first.startMs).toISOString(), role: 'user' }, text },
        frozenCallerRevision: snapshot.callerRevision, firstSequence: first.sequence,
        lastSequence: Math.max(...fragments.map(fragment => fragment.sequence)),
        completionBasis: 'speech_and_transcript_stability',
        sourceRevision: 1, capturedText: text,
      };
      entry = save(entry); // This source and exact request survive a crash before POST.
      entry = save({ ...entry, status: 'dispatching' });
      try {
        const result = await options.api.submit(options.bindingId, entry.requestId, structuredClone(entry.submission));
        entry = applyResult(entry, result);
      } catch {
        // The server may have admitted the run. Only GET by this request ID may follow.
        entry = save({ ...entry, status: 'dispatch_unknown' });
      }
      return entry;
    });
  };
  const applyResult = (entry: BrokerDelegationRecord, result: PrimaryResult): BrokerDelegationRecord => {
    if (result.bindingId !== options.bindingId || result.requestId !== entry.requestId)
      throw new Error('primary_result_identity_mismatch');
    if (!['queued', 'running', 'waiting_for_reply', 'completed', 'failed', 'cancelled'].includes(result.state)
        || (result.state === 'completed' && !result.assistant?.text?.trim())) throw new Error('invalid_primary_result');
    let current = load(entry);
    if (terminal(current.status) && !terminal(result.state)) return current;
    const outcome = (value: PrimaryResult) => JSON.stringify([value.state, value.assistant ?? null, value.error ?? null]);
    if (terminal(current.status) && current.primaryResult && outcome(current.primaryResult) !== outcome(result))
      throw new Error('primary_terminal_result_changed');
    if (result.question) current = observeQuestion(current, result.question);
    if (result.state === 'waiting_for_reply' && !result.question) throw new Error('invalid_primary_result');
    return save({ ...current, status: result.state, primaryResult: structuredClone(result),
      ...(result.question ? { currentQuestionId: result.question.id } : {}),
      ...(result.assistant ? { result: result.assistant.text } : {}) });
  };
  const captureRevision = async (entry: BrokerDelegationRecord, channel: LiveBackendChannel): Promise<BrokerDelegationRecord> => {
    const latest = channel.getContext();
    if (latest.callerRevision <= entry.frozenCallerRevision || channel.signal.aborted) return load(entry);
    const stable = await stableContext(channel);
    if (!stable || channel.signal.aborted) return load(entry);
    return locked(async () => {
      let current = load(entry);
      if (channel.signal.aborted || boundCall(entry.sessionId).endedAt) return current;
      if (current.captureState === 'unknown' || current.captureState === 'dispatching') return current;
      const fragments = acousticGroups(stable).find(group => group.fragments.some(fragment => fragment.sequence === current.firstSequence))?.fragments;
      if (!fragments) return current;
      const text = fragments.map(fragment => fragment.text).join('');
      if (text === current.capturedText) return current;
      if (!validTurnText(text)) return current;
      const turn: PrimaryTurn = { sourceEventId: current.submission.source.sourceEventId, revision: current.sourceRevision + 1,
        itemId: current.submission.source.itemId, role: 'user', text, occurredAt: current.submission.source.receivedAt };
      current = save({ ...current, pendingCapture: turn, captureState: 'dispatching',
        laterCallerRevision: stable.callerRevision, lastSequence: Math.max(current.lastSequence, ...fragments.map(f => f.sequence)),
        correctionState: 'recorded_not_applied' });
      try {
        const result = await options.api.captureTurn(options.bindingId, structuredClone(turn));
        if (result.bindingId !== options.bindingId || result.sourceEventId !== turn.sourceEventId || result.revision !== turn.revision || result.recordId !== turn.sourceEventId)
          throw new Error('capture_identity_mismatch');
        return save({ ...current, sourceRevision: turn.revision, capturedText: text, pendingCapture: undefined, captureState: 'accepted' });
      } catch { return save({ ...current, captureState: 'unknown' }); }
    });
  };
  const deliverQuestion = async (entry: BrokerDelegationRecord, channel: LiveBackendChannel) => locked(async () => {
    let current = load(entry);
    const questionId = current.currentQuestionId;
    const record = questionId ? current.questions?.[questionId] : undefined;
    const snapshot = channel.getContext();
    if (channel.signal.aborted) return;
    const prefix = `${entry.requestId}_${questionId ?? 'question'}_${snapshot.callerRevision}`;
    const tell = async (text: string, suffix: string) => {
      const delivery = await channel.publish({ id: `${prefix}_${suffix}`, kind: 'result', text,
        expectedCallerRevision: snapshot.callerRevision });
      save({ ...load(entry), delivery: delivery.status });
    };
    if (Object.values(current.questions ?? {}).some(value => unresolvedReply(value.reply))) {
      await tell('I am checking whether your answer was saved. I will use that saved answer when it is confirmed.', 'reply_unknown');
      return;
    }
    if (!record || !questionId || record.question.state !== 'pending'
        || Date.parse(record.question.expiresAt) <= now() || current.questionNotice === 'question_unavailable') {
      await tell(record?.question.state === 'answered'
        ? 'This question has already been answered. I am checking how the request continued.'
        : 'This question is no longer available to answer. I can check the request status.', 'unavailable');
      return;
    }
    const question = record.question;
    const text = [question.question, question.context, `Proposed answer: ${question.proposedAnswer}`,
      `If you say yes: ${question.yesMeaning}`, `If you say no: ${question.noMeaning}`].join('\n');
    let chunks: string[];
    try { chunks = liveResultChunks(text); }
    catch { await tell('I cannot read this question accurately over the phone. Please open it in Dexter.', 'format_unavailable'); return; }
    if (current.questionNotice === 'answer_too_long') {
      const delivery = await channel.publish({ id: `${prefix}_shorter_answer`, kind: 'progress', speak: true,
        text: 'Please give me a shorter answer to this question.', expectedCallerRevision: snapshot.callerRevision });
      if (delivery.status !== 'accepted') return;
    }
    if (current.questionNotice === 'reply_refused') {
      const delivery = await channel.publish({ id: `${prefix}_reply_refused`, kind: 'progress', speak: true,
        text: record.reply?.refusal?.code === 'invalid_request'
          ? 'I could not accept that answer. Please say it again.'
          : 'That answer was not accepted. The request is still waiting for a reply.',
        expectedCallerRevision: snapshot.callerRevision });
      if (delivery.status !== 'accepted') return;
    }
    // Commentary appends preserve the question and both meanings, including amounts.
    // Only completion of every append establishes the barrier for a later caller turn.
    for (const [index, chunk] of chunks.entries()) {
      const delivery = await channel.publish({ id: `${prefix}_prompt_${index}`, kind: 'result', text: chunk,
        speak: true, expectedCallerRevision: snapshot.callerRevision });
      if (delivery.status !== 'accepted') { save({ ...load(entry), delivery: delivery.status }); return; }
    }
    const after = channel.getContext();
    if (channel.signal.aborted || after.callerRevision !== snapshot.callerRevision || boundCall(entry.sessionId).endedAt) return;
    current = load(entry);
    save({ ...current, delivery: 'accepted', questionNotice: undefined, questions: { ...current.questions,
      [questionId]: { ...current.questions![questionId], presentation: {
        callerRevision: after.callerRevision, sequence: Math.max(0, ...after.fragments.map(fragment => fragment.sequence)),
        lastCallerEndMs: Math.max(0, ...after.fragments.filter(fragment => fragment.role === 'user').map(fragment => fragment.endMs)),
        acceptedAt: new Date(now()).toISOString(),
      } } } });
  });
  const deliver = async (entry: BrokerDelegationRecord, channel: LiveBackendChannel) => {
    if (channel.signal.aborted) return;
    if (entry.status === 'waiting_for_reply' || Object.values(entry.questions ?? {}).some(record => unresolvedReply(record.reply))) {
      await deliverQuestion(entry, channel); return;
    }
    const current = channel.getContext();
    const reviewedRevision = Math.max(entry.frozenCallerRevision, ...Object.values(entry.questions ?? {})
      .filter(record => record.reply?.status === 'accepted').map(record => record.reply!.callerRevision));
    const changed = current.callerRevision !== reviewedRevision;
    let text: string;
    if (changed) {
      const state = terminal(entry.status)
        ? `The original request ${entry.status === 'completed' ? 'completed' : entry.status === 'failed' ? 'failed' : 'was cancelled'}.`
        : 'I am still checking the original request.';
      text = entry.correctionState === 'recorded_not_applied'
        ? `${state} Your correction has not changed that request.` : state;
    }
    else if (entry.status === 'completed') text = entry.primaryResult!.assistant!.text;
    else if (entry.status === 'failed') text = 'The request failed. Its saved result is available for review.';
    else if (entry.status === 'cancelled') text = 'The request was cancelled.';
    else if (['dispatching', 'dispatch_unknown', 'prepared'].includes(entry.status))
      text = 'I could not confirm the outcome. Please check the request status before trying again.';
    else text = 'Your request is still pending. I can check its status again.';
    if (entry.deferredTurns?.length) text += ' I also recorded what you said afterward, but have not acted on it. Please state the next request when you are ready.';
    const prefix = `${entry.requestId}_${current.callerRevision}`;
    if (changed && entry.status === 'completed') {
      let resultChunks: string[];
      try { resultChunks = liveResultChunks(entry.primaryResult!.assistant!.text); }
      catch { resultChunks = []; }
      if (resultChunks.length) {
        const provenance = await channel.publish({ id: `${prefix}_original_result_scope`, kind: 'progress',
          text: 'The following saved result belongs to the original request. It does not answer or apply anything the caller said afterward.',
          speak: false, expectedCallerRevision: current.callerRevision });
        if (provenance.status !== 'accepted') { save({ ...load(entry), delivery: provenance.status }); return; }
        for (const [index, chunk] of resultChunks.entries()) {
          const delivery = await channel.publish({ id: `${prefix}_original_result_${index}`, kind: 'progress', text: chunk,
            speak: false, expectedCallerRevision: current.callerRevision });
          if (delivery.status !== 'accepted') { save({ ...load(entry), delivery: delivery.status }); return; }
        }
      }
    }
    let chunks: string[];
    try { chunks = liveResultChunks(text); }
    catch {
      const outcome = await channel.publish({ id: `${prefix}_format_unavailable`, kind: 'result',
        text: 'I received the result, but cannot read that format accurately over this call. The full result has been saved.',
        expectedCallerRevision: current.callerRevision });
      save({ ...load(entry), delivery: `format_unsupported:${outcome.status}` });
      return;
    }
    if (chunks.length > 1) {
      for (const [index, chunk] of chunks.entries()) {
        const delivery = await channel.publish({ id: `${prefix}_context_${index}`, kind: 'progress', text: chunk,
          speak: false, expectedCallerRevision: current.callerRevision });
        if (delivery.status !== 'accepted') { save({ ...load(entry), delivery: delivery.status }); return; }
      }
    }
    const outcome = await channel.publish({ id: `${prefix}_${entry.status}_result`, kind: 'result',
      text: shortSpokenResult(text), expectedCallerRevision: current.callerRevision });
    save({ ...load(entry), delivery: outcome.status });
  };
  const persistCanonical = (sessionId: string, turn: CanonicalTurnRecord) => {
    const call = boundCall(sessionId);
    options.store.update(call.id, { canonicalTurns: { ...call.canonicalTurns, [turn.sourceEventId]: turn } });
  };
  const canonicalPayload = (turn: PrimaryTurn): PrimaryTurn => {
    if (!uuidPattern.test(turn.sourceEventId) || !Number.isInteger(turn.revision) || turn.revision < 1
        || !/^[A-Za-z0-9_-]{1,128}$/.test(turn.itemId) || !['user', 'assistant'].includes(turn.role)
        || !validTurnText(turn.text) || !Number.isFinite(Date.parse(turn.occurredAt))) throw new Error('invalid_saved_capture');
    return { sourceEventId: turn.sourceEventId, revision: turn.revision, itemId: turn.itemId,
      role: turn.role, text: turn.text, occurredAt: turn.occurredAt };
  };
  const pendingCount = (sessionId: string) => {
    const delegated = entries(sessionId);
    return delegated.filter(entry => ['prepared', 'dispatching', 'dispatch_unknown'].includes(entry.status)).length
      + delegated.filter(entry => entry.pendingCapture).length
      + delegated.reduce((count, entry) => count + Object.values(entry.questions ?? {}).filter(record => unresolvedReply(record.reply)).length, 0)
      + Object.values(boundCall(sessionId).canonicalTurns ?? {}).filter(value => (value as CanonicalTurnRecord).state !== 'captured').length;
  };
  const currentSession = () => {
    const sessionId = options.store.get(options.callId)?.liveSessionId;
    if (!sessionId) throw new Error('missing_live_session');
    boundCall(sessionId);
    return sessionId;
  };
  return {
    persistAcousticEvidence() {
      const sessionId = currentSession();
      options.store.update(boundCall(sessionId).id, { acousticSpans: structuredClone(acousticSpans) });
    },
    noteCallerSpeech(atMs = now(), audioOffsetMs) {
      if (options.store.get(options.callId)?.endedAt) throw new Error('broker_call_ended');
      if (!Number.isFinite(atMs) || atMs < 0) throw new Error('invalid_speech_time');
      lastSpeechAt = Math.max(lastSpeechAt ?? 0, atMs);
      if (audioOffsetMs !== undefined) {
        if (!Number.isFinite(audioOffsetMs) || audioOffsetMs < 0) throw new Error('invalid_audio_offset');
        const last = acousticSpans.at(-1);
        if (last && audioOffsetMs < last.endMs) throw new Error('audio_offset_regressed');
        if (!last || audioOffsetMs - last.endMs > stabilityMs) {
          acousticSpans.push({ startMs: Math.max(0, audioOffsetMs - 20), endMs: audioOffsetMs, lastSpeechAt: atMs });
        } else { last.endMs = audioOffsetMs; last.lastSpeechAt = atMs; }
      }
      for (const listener of speechListeners) listener();
    },
    async flushPendingCaptures() {
      const sessionId = currentSession();
      return locked(async () => {
        const unsupported: string[] = [];
        for (let entry of entries(sessionId)) {
          if (['prepared', 'dispatching', 'dispatch_unknown'].includes(entry.status)) {
            try { entry = applyResult(entry, await options.api.result(options.bindingId, entry.requestId)); }
            catch { unsupported.push('source_admission_unknown'); continue; }
          }
          entry = await recoverReplies(entry);
          if (Object.values(entry.questions ?? {}).some(record => unresolvedReply(record.reply))) unsupported.push('reply_outcome_unknown');
          if (!entry.pendingCapture) continue;
          try {
            const turn = canonicalPayload(entry.pendingCapture);
            const receipt = await options.api.captureTurn(options.bindingId, turn);
            if (receipt.bindingId !== options.bindingId || receipt.sourceEventId !== turn.sourceEventId
                || receipt.revision !== turn.revision || receipt.recordId !== turn.sourceEventId) throw new Error('capture_identity_mismatch');
            save({ ...load(entry), sourceRevision: turn.revision, capturedText: turn.text,
              pendingCapture: undefined, captureState: 'accepted' });
          } catch { save({ ...load(entry), captureState: 'unknown' }); unsupported.push('capture_outcome_unknown'); }
        }
        for (const value of Object.values(boundCall(sessionId).canonicalTurns ?? {})) {
          const turn = value as CanonicalTurnRecord;
          if (turn.state === 'captured') continue;
          try {
            const payload = canonicalPayload(turn);
            const delegated = entries(sessionId).find(entry => entry.submission.source.sourceEventId === turn.sourceEventId);
            const reply = knownReply(sessionId, turn.sourceEventId);
            if (unresolvedReply(reply?.reply)) { unsupported.push('reply_outcome_unknown'); continue; }
            if (delegated?.pendingCapture || (delegated && ['prepared', 'dispatching', 'dispatch_unknown'].includes(delegated.status))) {
              unsupported.push('source_admission_unknown'); continue;
            }
            const receipt = await options.api.captureTurn(options.bindingId, payload);
            if (receipt.bindingId !== options.bindingId || receipt.sourceEventId !== turn.sourceEventId
                || receipt.revision !== turn.revision || receipt.recordId !== turn.sourceEventId) throw new Error('capture_identity_mismatch');
            persistCanonical(sessionId, { ...turn, state: 'captured', recordId: receipt.recordId });
            if (delegated) save({ ...load(delegated), sourceRevision: turn.revision, capturedText: turn.text, correctionState: 'recorded_not_applied' });
            if (reply?.reply.status === 'accepted') save({ ...load(reply.entry), correctionState: 'recorded_not_applied' });
          } catch { persistCanonical(sessionId, { ...turn, state: 'unknown' }); unsupported.push('capture_outcome_unknown'); }
        }
        return { pending: pendingCount(sessionId), unsupported: [...new Set(unsupported)] };
      });
    },
    async captureCompletedTurns(snapshot, proof) {
      const sessionId = currentSession();
      if (!proof.sessionFinalized) return { captured: 0, pending: pendingCount(sessionId), unsupported: ['transcript_finalization_unconfirmed'] };
      const unsupported: string[] = [];
      const call = boundCall(sessionId);
      // A quiet interval after hangup supplies no evidence about whether the caller
      // had finished. Freeze the observation clock at the host's disconnect time.
      const observedAt = call.endedAt ? Math.min(now(), Date.parse(call.endedAt)) : now();
      options.store.update(call.id, { acousticSpans: structuredClone(acousticSpans) });
      const groups: Array<{ role: 'user' | 'assistant'; fragments: LiveTranscriptSnapshot['fragments']; span?: AcousticSpan }> =
        acousticGroups(snapshot, observedAt).map(group => ({ role: 'user', fragments: group.fragments, span: group.span }));
      const included = new Set(groups.flatMap(group => group.fragments.map(f => f.eventId)));
      if (snapshot.fragments.some(f => f.role === 'user' && !included.has(f.eventId))) unsupported.push('unqualified_caller_turn');
      const assistant = snapshot.fragments.filter(f => f.role === 'assistant');
      if (assistant.length && !proof.assistantPlaybackComplete) unsupported.push('assistant_playback_unconfirmed');
      if (proof.assistantPlaybackComplete) {
        // Group captions only after the host has confirmed the whole uninterrupted
        // output stream played. Neither provider text nor finalization supplies that proof.
        const typed = callerGroups({ ...snapshot, fragments: assistant.map(f => ({ ...f, role: 'user' })) });
        for (const group of typed) groups.push({ role: 'assistant', fragments: group.sort((a, b) => a.sequence - b.sequence) });
      }
      groups.sort((a, b) => a.fragments[0].startMs - b.fragments[0].startMs || a.fragments[0].sequence - b.fragments[0].sequence);
      let captured = 0;
      for (const group of groups) {
        const result = await locked(async () => {
          const current = boundCall(sessionId);
          const first = group.fragments.reduce((a, b) => a.sequence < b.sequence ? a : b);
          const sourceEventId = brokerIdentity('source', options.callId, sessionId, first.eventId);
          const text = group.fragments.map(f => f.text).join('');
          if (!validTurnText(text)) return 'invalid_completed_turn';
          const delegated = entries(sessionId).find(entry => entry.submission.source.sourceEventId === sourceEventId);
          const reply = knownReply(sessionId, sourceEventId);
          if (unresolvedReply(reply?.reply)) return 'reply_outcome_unknown';
          if (delegated && ['prepared', 'dispatching', 'dispatch_unknown'].includes(delegated.status)) return 'source_admission_unknown';
          if (delegated?.pendingCapture) return 'capture_outcome_unknown';
          const previous = current.canonicalTurns?.[sourceEventId] as CanonicalTurnRecord | undefined;
          if (previous && previous.state !== 'captured') return 'capture_outcome_unknown';
          if ((previous?.text ?? delegated?.capturedText) === text) return 'already_captured';
          const turn: CanonicalTurnRecord = {
            sourceEventId, revision: (previous?.revision ?? delegated?.sourceRevision ?? 0) + 1,
            itemId: delegated?.submission.source.itemId ?? `live-turn-${sourceEventId}`,
            role: group.role, text, occurredAt: previous?.occurredAt ?? delegated?.submission.source.receivedAt
              ?? new Date(mediaOrigin(sessionId) + first.startMs).toISOString(),
            state: 'dispatching', ...(group.span ? { acousticSpan: { ...group.span } } : {}),
          };
          const persist = (value: CanonicalTurnRecord) => persistCanonical(sessionId, value);
          persist(turn);
          try {
            const receipt = await options.api.captureTurn(options.bindingId, {
              sourceEventId: turn.sourceEventId, revision: turn.revision, itemId: turn.itemId,
              role: turn.role, text: turn.text, occurredAt: turn.occurredAt,
            });
            if (receipt.bindingId !== options.bindingId || receipt.sourceEventId !== sourceEventId || receipt.revision !== turn.revision || receipt.recordId !== turn.sourceEventId)
              throw new Error('capture_identity_mismatch');
            persist({ ...turn, state: 'captured', recordId: receipt.recordId });
            if (delegated) save({ ...load(delegated), sourceRevision: turn.revision, capturedText: text, correctionState: 'recorded_not_applied' });
            if (reply?.reply.status === 'accepted') save({ ...load(reply.entry), correctionState: 'recorded_not_applied' });
            return 'captured';
          } catch { persist({ ...turn, state: 'unknown' }); return 'capture_outcome_unknown'; }
        });
        if (result === 'captured') captured++;
        else if (result !== 'already_captured') unsupported.push(result);
      }
      return { captured, pending: pendingCount(sessionId), unsupported: [...new Set(unsupported)] };
    },
    async delegate(request, channel) {
      let entry = await accept(request, channel);
      if (channel.signal.aborted) return;
      if (!entry) {
        await channel.publish({ id: `${request.requestId}_clarify`, kind: 'result',
          text: 'Please finish your request, then give me a moment to check it.',
          expectedCallerRevision: channel.getContext().callerRevision });
        return;
      }
      const deadline = now() + maxPollMs;
      // Preserve caller changes in the local journal immediately. Only stable snapshots
      // go to the source-revision API; revisions never create another run here.
      const unsubscribe = channel.subscribeContext(snapshot => {
        if (snapshot.callerRevision > entry!.frozenCallerRevision) {
          const saved = load(entry!);
          const original = acousticGroups(snapshot).find(group => group.fragments.some(fragment => fragment.sequence === saved.firstSequence));
          if (original && original.fragments.map(fragment => fragment.text).join('') !== saved.capturedText) {
            save({ ...saved, laterCallerRevision: snapshot.callerRevision, correctionState: 'recorded_not_applied',
              lastSequence: Math.max(saved.lastSequence, ...original.fragments.map(f => f.sequence)) });
          }
        }
      });
      try {
        while (!channel.signal.aborted) {
          entry = load(entry);
          if (Object.values(entry.questions ?? {}).some(record => unresolvedReply(record.reply)))
            entry = await locked(() => recoverReplies(entry!));
          if (!terminal(entry.status)) {
            try { entry = applyResult(entry, await options.api.result(options.bindingId, entry.requestId)); }
            catch { /* An absent or unreadable result does not justify another POST. */ }
          }
          entry = await captureRevision(entry, channel);
          if (channel.signal.aborted) return;
          if (terminal(entry.status) || entry.status === 'waiting_for_reply' || now() >= deadline) break;
          if (!(await delay(pollIntervalMs, channel.signal))) return;
        }
        await deliver(load(entry), channel);
      } finally { unsubscribe(); }
    },
  };
}
