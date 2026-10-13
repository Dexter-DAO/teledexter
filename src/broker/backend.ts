import { createHash } from 'node:crypto';
import type { BrokerLiveBackend, LiveBackendChannel, LiveDelegationRequest, LiveTranscriptSnapshot } from '../live/session.js';
import { LIVE_APPEND_MAX_BYTES } from '../live/session.js';
import type { PrimaryApiClient, PrimaryApiErrorCode, PrimaryClosedCall, PrimaryQuestion, PrimaryQuestionResult, PrimaryReconciliation, PrimaryReplyInput, PrimaryResult, PrimarySubmission, PrimaryTurn, PrimaryTurnReceipt } from './primaryApi.js';
import { PrimaryApiError } from './primaryApi.js';
import { terminalCallStatuses, type BrokerCall, type BrokerStore } from './store.js';
import { beginBrokerPublication, settleBrokerPublication, selectBrokerPublicationReference, brokerPublicationReceiptsAccepted, type BrokerPublicationReceipt } from './publications.js';
import { brokerCallAssignmentSchema, phoneCallAssignmentRefSchema } from './assignment.js';
import { preparePrimaryHarkOutcomePatch, primaryHarkReceiptBindingFromCall } from './harkPrimaryReceiptOutbox.js';
import type { LiveBackendUpdate, LiveDelivery } from '../live/types.js';
import { acknowledgedTurn, originalTurn, rawSubmissionDigest, turnBodyDigest, validateNonAdmissionProof, type AcknowledgedTurnReceipt } from './reconciliation.js';
import { assertFrozenCallerSpeechCurrent, type FrozenCallerTurn } from './callerTurn.js';
import { requireCallerRoute, requireCallerRouteDispatch, CallerRouteExpiredError, getCallerRouteBySource,
  callerRoutePresentationDigest, type CallerQuestionTarget, type CallerRouteRef, type CallerRouteRecord } from './callerRouting.js';
import { canonicalJson } from '../outbound/canonical.js';
import { orderedAssociatedResultReference } from './associatedResult.js';
import { callerIntentDecisionSchema, type CallerIntentDecision } from './callerIntentContract.js';
import { resultSmsAcknowledgement, resultSmsResearchRefusal } from './resultSmsAcknowledgement.js';
import { phoneResultSmsAdmissionReceiptSchema, type PhoneResultSmsAdmissionReceipt } from './resultSmsContract.js';
import { failedRequestSpeech, pendingRequestSpeech, pendingRequestState, partialResultUpdate } from './voicePresentation.js';

export interface CallerSourceAnchor {
  frozen: FrozenCallerTurn;
  capture?: PrimaryTurnReceipt;
  /** An unresolved saved answer still owns this source. */
  replyPending: boolean;
}
export type SelectedQuestionSourceAnchor = CallerSourceAnchor;
export type ResolvedCallerTaskReference = NonNullable<Extract<CallerIntentDecision, { kind: 'task' }>['reference']>;

export interface PrimaryBackendOptions {
  callId: string;
  callSid: string;
  bindingId: string;
  api: Pick<PrimaryApiClient, 'submit' | 'result' | 'captureTurn' | 'question' | 'reply'> & Partial<Pick<PrimaryApiClient, 'reconcile'|'admitResultSms'>>;
  store: Pick<BrokerStore, 'get' | 'update'>;
  stabilityMs?: number;
  maxWaitMs?: number;
  pollIntervalMs?: number;
  maxPollMs?: number;
  /** Live media owns cancellation; keep observing accepted work until the call ends. */
  watchUntilCallEnds?: boolean;
  progressIntervalMs?: number;
  now?: () => number;
  lastCallerSpeechAt?: () => number | undefined;
  mediaStartedAt?: string;
  /** Canonical transcript clock owned by this backend; delivery may use another Live session. */
  sourceSessionId?: string;
  /** Suppresses the legacy delegation port. Explicit routed ports remain usable.
   * Parent supplies validated source evidence and reply state together. */
  selectedQuestionSources?(): readonly SelectedQuestionSourceAnchor[];
  /** Routing evidence owns the source before either downstream admission exists. */
  callerRouteSources?(): readonly CallerSourceAnchor[];
  callerRouteCaptureAccepted?(sourceEventId: string, receipt: PrimaryTurnReceipt): void;
  /** Resolve the exact persisted offered reference; absence/refusal never drops it. */
  resolveCallerTaskReference?(route: CallerRouteRecord): ResolvedCallerTaskReference | undefined
    | Promise<ResolvedCallerTaskReference | undefined>;
  /** Separates durable source admission from the longer call-owned result observer. */
  admissionSettled?(request: LiveDelegationRequest): void;
  observation?: {
    signal: AbortSignal;
    publish(entry: BrokerDelegationRecord, update?: LiveBackendUpdate): Promise<{
      delivery: LiveDelivery; sessionId?: string; snapshot?: LiveTranscriptSnapshot;
      inputThroughMs?: number; publicationReceipts?: BrokerPublicationReceipt[];
    }>;
  };
}

export interface PrimaryBackend extends BrokerLiveBackend {
  deliverFrozenResult(request: LiveDelegationRequest, route: CallerRouteRef, channel: LiveBackendChannel): Promise<void>;
  delegateFrozenTask(request: LiveDelegationRequest, route: CallerRouteRef, channel: LiveBackendChannel): Promise<void>;
  answerFrozenQuestion(request: LiveDelegationRequest, route: CallerRouteRef, channel: LiveBackendChannel): Promise<void>;
  questionTargets(sessionId: string): CallerQuestionTarget[];
  /** Freeze existing completed caller speech without submitting, replying or capturing. */
  freezeCallerTurn(request: LiveDelegationRequest, channel: LiveBackendChannel,
    usedSourceIds: readonly string[]): Promise<FrozenCallerTurn | undefined>;
  /** Call only under the shared admission lock for a not-yet-admitted frozen source. */
  assertFrozenCallerSpeechCurrent(frozen: FrozenCallerTurn): void;
  /** Retain quiet-time completion evidence without uploading provisional speech. */
  observeTranscript(snapshot: LiveTranscriptSnapshot): void;
  /** Host-observed acoustic speech time, in Date.now() milliseconds. */
  noteCallerSpeech(atMs?: number, audioOffsetMs?: number): void;
  /** Freeze the observed spans before the host reconstructs closeout after disconnect. */
  persistAcousticEvidence(): void;
  captureCompletedTurns(snapshot: LiveTranscriptSnapshot, proof: { assistantPlaybackComplete: boolean; sessionFinalized?: boolean }): Promise<{
    captured: number; pending: number; unsupported: string[];
  }>;
  /** Reconcile uncertain admission with GET, then replay only exact journalled captures. */
  flushPendingCaptures(): Promise<{ pending: number; unsupported: string[] }>;
  /** Capture the original source before closing admission for a finalized call. */
  prepareCloseout(): Promise<void>;
  /** Unresolved admission may cross the close fence only with an exact capture acknowledgement. */
  closeoutPending(): number;
  reconcileClosedRequests(closed: PrimaryClosedCall): Promise<{ pending: number }>;
}

export interface BrokerDelegationRecord {
  version: 1;
  requestId: string;
  callId: string;
  callSid: string;
  bindingId: string;
  sessionId: string;
  delegationIds: string[];
  delegationAliases?: Array<{ sessionId: string; delegationId: string }>;
  status: 'prepared' | 'dispatching' | 'dispatch_unknown' | 'queued' | 'running' | 'waiting_for_reply' | 'completed' | 'failed' | 'cancelled' | 'not_admitted' | 'admission_refused' | 'admission_expired';
  updatedAt: string;
  timing?: { firstSavedAt: string; firstDispatchAt?: string; firstAnswerAvailableAt?: string; firstDeliveryAcceptedAt?: string };
  submission: PrimarySubmission;
  frozenCallerRevision: number;
  firstSequence: number;
  lastSequence: number;
  /** Local received-audio clock; independent of Live transcript timestamps. */
  audioThroughMs?: number;
  completionBasis: 'transcript_stability' | 'speech_and_transcript_stability';
  sourceRevision: number;
  capturedText: string;
  pendingCapture?: PrimaryTurn;
  captureState?: 'dispatching' | 'accepted' | 'unknown';
  laterCallerRevision?: number;
  correctionState?: 'recorded_not_applied';
  primaryResult?: PrimaryResult;
  /** Confirmed tool facts may arrive before the final answer. Unknown appends must not replay. */
  partialDelivery?: { toolInvocationId: string; text: string; status: LiveDelivery['status'] | 'dispatching' };
  /** Receipt-only recovery never supplies task content or resolves task dispatch uncertainty. */
  resultSmsAdmission?: PhoneResultSmsAdmissionReceipt;
  result?: string;
  delivery?: string;
  submitError?: PrimaryApiDiagnostic;
  admissionRefusal?: { origin: 'api_non_admission'; status: 409 | 503;
    code: 'delivery_association_mismatch' | 'associated_result_unavailable' | SmsAdmissionRefusalCode; submissionDigest: string };
  admissionExpiry?: { origin: 'caller_intent_expiry'; validUntil: string; inputDigest: string };
  lastResultReadError?: PrimaryApiDiagnostic;
  /** Presentation only; keep the last diagnostic even after a successful read. */
  resultReadUnavailable?: boolean;
  originalCapture?: { turn: PrimaryTurn; state: 'dispatching' | 'unknown' | 'accepted'; receipt?: AcknowledgedTurnReceipt };
  reconciliation?: PrimaryReconciliation;
  reconciliationError?: PrimaryApiDiagnostic;
  questions?: Record<string, BrokerQuestionRecord>;
  currentQuestionId?: string;
  questionNotice?: 'answer_too_long' | 'reply_unknown' | 'reply_refused' | 'question_unavailable' | 'answered_elsewhere';
  /** Local ordering evidence; never added to the caller's raw submission text. */
  predecessorRequestId?: string;
  deferredTurns?: BrokerDeferredTurn[];
}

export interface BrokerDeferredTurn {
  sourceSessionId?: string;
  delegationId: string;
  sourceEventId: string;
  text: string;
  firstSequence: number;
  callerRevision: number;
  status: 'recorded_not_submitted' | 'queued' | 'promoted';
  /** Absent on legacy records and speech that cannot be admitted as another request. */
  queuedRequest?: {
    requestId: string;
    predecessorRequestId: string;
    submission: PrimarySubmission;
    bodyDigest: string;
    lastSequence: number;
    completionBasis: 'speech_and_transcript_stability';
    audioThroughMs?: number;
  };
}

export interface PrimaryApiDiagnostic {
  code: PrimaryApiErrorCode;
  status?: number;
  remoteCode?: string;
  retryable?: boolean;
  observedAt: string;
}

export interface BrokerQuestionPresentation {
  sessionId?: string; callerRevision: number; sequence: number; lastCallerEndMs: number; acceptedAt: string;
  inputThroughMs?: number; publicationReceipts?: BrokerPublicationReceipt[];
}
export interface BrokerQuestionRecord {
  question: PrimaryQuestion;
  identityDigest: string;
  presentation?: BrokerQuestionPresentation;
  reply?: BrokerQuestionReplyRecord;
  /** Earlier proven non-admissions remain owned evidence when the caller answers again. */
  refusedReplies?: BrokerQuestionReplyRecord[];
}

export interface BrokerQuestionReplyRecord {
  /** Frozen publication evidence for a definitely-undispatched successor answer. */
  admissionPresentation?: BrokerQuestionPresentation;
  sourceSessionId?: string;
  input: PrimaryReplyInput;
  inputDigest: string;
  status: 'prepared' | 'dispatching' | 'unknown' | 'accepted' | 'refused';
  callerRevision: number;
  firstSequence: number;
  lastSequence: number;
  audioThroughMs?: number;
  refusal?: { origin: 'local_validation' | 'api_non_admission'; code: string; status?: number };
}

const smsAdmissionRefusalCodes = ['sms_unsupported_number','carrier_opted_out','sms_consent_required',
  'sms_delivery_pending','sms_call_unavailable','sms_authority_revoked','sms_number_changed'] as const;
type SmsAdmissionRefusalCode = typeof smsAdmissionRefusalCodes[number];
const isSmsAdmissionRefusal = (status: number, code: string): code is SmsAdmissionRefusalCode =>
  (smsAdmissionRefusalCodes as readonly string[]).includes(code) && status === (code==='sms_delivery_pending' ? 503 : 409);
const taskAdmissionRefusalText = (entry: BrokerDelegationRecord) => entry.submission.resultSms
  ? resultSmsResearchRefusal(entry.admissionRefusal?.code)
  : 'I could not start that request from the saved result. Please tell me what you want me to check or do.';
const terminal = (state: string) => ['completed', 'failed', 'cancelled', 'not_admitted', 'admission_refused', 'admission_expired'].includes(state);
/** Store startup and every write enforce this independently of routed admission. */
export function validateBrokerAdmissionRefusals(call: BrokerCall, previous?: BrokerCall): void {
  for (const [requestId, raw] of Object.entries(call.delegations)) {
    if (!raw || typeof raw !== 'object') continue;
    const entry = raw as BrokerDelegationRecord, old = previous?.delegations[requestId] as BrokerDelegationRecord | undefined;
    if (entry.resultSmsAdmission) {
      phoneResultSmsAdmissionReceiptSchema.parse(entry.resultSmsAdmission);
      if (!entry.submission.resultSms || entry.submission.resultSms.requestId!==entry.requestId
        || ['prepared','admission_refused','admission_expired'].includes(entry.status) || !entry.timing?.firstDispatchAt)
        throw new Error('invalid_saved_sms_admission_recovery');
      if (previous && !old?.resultSmsAdmission && (!old || !['dispatching','dispatch_unknown'].includes(old.status) || !['dispatching','dispatch_unknown'].includes(entry.status)))
        throw new Error('sms_admission_recovery_requires_attempted_dispatch');
      if (old?.resultSmsAdmission && (old.resultSmsAdmission.intentId!==entry.resultSmsAdmission.intentId
        || old.resultSmsAdmission.intentDigest!==entry.resultSmsAdmission.intentDigest))
        throw new Error('saved_sms_admission_recovery_changed');
    }
    if (old?.resultSmsAdmission && !entry.resultSmsAdmission) throw new Error('saved_sms_admission_recovery_removed');
    if (old?.admissionExpiry && (entry.status !== 'admission_expired'
      || canonicalJson(entry.admissionExpiry) !== canonicalJson(old.admissionExpiry)
      || canonicalJson(entry.submission) !== canonicalJson(old.submission))) throw new Error('admission_expiry_changed');
    if (entry.status === 'admission_expired' || entry.admissionExpiry !== undefined) {
      const expiry = entry.admissionExpiry;
      const route = getCallerRouteBySource(call, entry.submission.source.sourceEventId), receipt = route?.intent?.response;
      if (entry.status !== 'admission_expired' || !expiry || expiry.origin !== 'caller_intent_expiry'
        || Object.keys(expiry).sort().join(',') !== 'inputDigest,origin,validUntil'
        || receipt?.state !== 'resolved' || receipt.decision.kind !== 'task'
        || expiry.validUntil !== receipt.validUntil || expiry.inputDigest !== receipt.inputDigest
        || !Number.isFinite(Date.parse(expiry.validUntil)) || !Number.isFinite(Date.parse(entry.updatedAt))
        || Date.parse(entry.updatedAt) < Date.parse(expiry.validUntil)
        || entry.primaryResult !== undefined || entry.result !== undefined || entry.submitError !== undefined
        || entry.lastResultReadError !== undefined || entry.reconciliation !== undefined || entry.admissionRefusal !== undefined
        || Object.keys(entry.questions ?? {}).length) throw new Error('invalid_saved_admission_expiry');
      if (previous && !old?.admissionExpiry && (!old || !['prepared', 'dispatching'].includes(old.status)
        || old.submitError !== undefined || old.lastResultReadError !== undefined || old.primaryResult !== undefined
        || old.reconciliation !== undefined || canonicalJson(old.submission) !== canonicalJson(entry.submission)))
        throw new Error('admission_expiry_requires_first_dispatch');
      continue;
    }
    if (old?.admissionRefusal && (entry.status !== 'admission_refused'
      || canonicalJson(entry.admissionRefusal) !== canonicalJson(old.admissionRefusal)
      || canonicalJson(entry.submission) !== canonicalJson(old.submission))) throw new Error('admission_refusal_changed');
    if (entry.status !== 'admission_refused' && entry.admissionRefusal === undefined) continue;
    const refusal = entry.admissionRefusal;
    if (previous && !old?.admissionRefusal && (!old || old.status !== 'dispatching'
      || old.submitError !== undefined || old.lastResultReadError !== undefined || old.primaryResult !== undefined
      || old.reconciliation !== undefined || canonicalJson(old.submission) !== canonicalJson(entry.submission)))
      throw new Error('admission_refusal_requires_first_dispatch');
    const associatedRefusal=!!entry.submission.associatedResultReference && refusal?.status===409
      && ['delivery_association_mismatch','associated_result_unavailable'].includes(refusal.code);
    const smsRefusal=!!entry.submission.resultSms && entry.submission.resultSms.requestId===entry.requestId
      && !!refusal && isSmsAdmissionRefusal(refusal.status,refusal.code);
    if (entry.status !== 'admission_refused' || !refusal || (!associatedRefusal && !smsRefusal)
      || refusal.origin !== 'api_non_admission'
      || Object.keys(refusal).sort().join(',') !== 'code,origin,status,submissionDigest'
      || refusal.submissionDigest !== rawSubmissionDigest(entry.bindingId, entry.submission)
      || (associatedRefusal && (entry.submission.relatedPrimaryRequestId !== undefined || entry.submission.referenceOrigin !== undefined))
      || entry.primaryResult !== undefined || entry.result !== undefined || entry.submitError !== undefined
      || entry.lastResultReadError !== undefined || entry.reconciliation !== undefined || Object.keys(entry.questions ?? {}).length)
      throw new Error('invalid_saved_admission_refusal');
    if(associatedRefusal) orderedAssociatedResultReference(entry.submission.associatedResultReference!);
  }
  for (const [id, raw] of Object.entries(previous?.delegations ?? {}))
    if (((raw as BrokerDelegationRecord | null)?.admissionRefusal || (raw as BrokerDelegationRecord | null)?.admissionExpiry
      || (raw as BrokerDelegationRecord | null)?.resultSmsAdmission) && !call.delegations[id])
      throw new Error('admission_refusal_removed');
}
const admissionUnknown = (state: string) => ['prepared', 'dispatching', 'dispatch_unknown'].includes(state);
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
  sourceSessionId?: string;
  state: 'dispatching' | 'captured' | 'unknown';
  acousticSpan?: AcousticSpan;
  recordId?: string;
}
// A closeout backend can coexist briefly with the detached voice backend. Both
// must serialize the same call journal and capture submissions.
const storeAdmissions = new WeakMap<object, Map<string, Promise<void>>>();
/** Shared by source freezing, routed admission and call closeout. */
export async function withCallerAdmission<T>(store: object, callId: string, fn: () => Promise<T> | T): Promise<T> {
  let admissions = storeAdmissions.get(store);
  if (!admissions) { admissions = new Map(); storeAdmissions.set(store, admissions); }
  const previous = admissions.get(callId) ?? Promise.resolve();
  let release!: () => void;
  const tail = new Promise<void>(resolve => { release = resolve; });
  admissions.set(callId, tail);
  await previous;
  try { return await fn(); } finally {
    release();
    if (admissions.get(callId) === tail) admissions.delete(callId);
  }
}
// One live worker delivers results and drains a call's queue. Admission remains
// independently locked so caller turns can be saved while a result GET waits.
const storeWorkers = new WeakMap<object, Map<string, Promise<void>>>();
// Concurrent Live delegations can observe the same saved request. Share notice
// reservations without changing its admission or transcript journal.
const storeNotices = new WeakMap<object, Map<string, {
  submitAnnounced: boolean; progressCount: number; lastAt: number; inFlight: boolean;
  stateKey?: string; attempt?: number; deferred?: boolean; uncertainKey?: string; wake?: () => void; spokenText?: string;
}>>();

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
  const progressIntervalMs = positive(options.progressIntervalMs, 18_000, 60_000);
  if (maxWaitMs < stabilityMs || !options.callId || !options.callSid || !uuidPattern.test(options.bindingId))
    throw new Error('invalid_backend_configuration');
  let lastSpeechAt: number | undefined;
  const savedCall = options.store.get(options.callId);
  const savedSpans = options.sourceSessionId ? savedCall?.sourceAcousticSpans?.[options.sourceSessionId] : savedCall?.acousticSpans;
  const acousticSpans: AcousticSpan[] = Array.isArray(savedSpans) ? savedSpans.map((span: AcousticSpan) => {
    if (!span || ![span.startMs, span.endMs, span.lastSpeechAt].every(Number.isFinite)
        || span.startMs < 0 || span.endMs < span.startMs || span.lastSpeechAt < 0) throw new Error('invalid_saved_acoustic_span');
    return { ...span };
  }) : [];
  const speechListeners = new Set<() => void>();
  let notices = storeNotices.get(options.store);
  if (!notices) { notices = new Map(); storeNotices.set(options.store, notices); }
  let workers = storeWorkers.get(options.store);
  if (!workers) { workers = new Map(); storeWorkers.set(options.store, workers); }
  const locked = <T>(fn: () => Promise<T>): Promise<T> => withCallerAdmission(options.store, options.callId, fn);
  const captureTurn = async (turn: PrimaryTurn): Promise<PrimaryTurnReceipt> => {
    const receipt = await options.api.captureTurn(options.bindingId, structuredClone(turn));
    if (turn.revision === 1 && options.callerRouteSources?.().some(source => source.frozen.turn.sourceEventId === turn.sourceEventId)) {
      acknowledgedTurn(options.bindingId, turn, receipt);
      options.callerRouteCaptureAccepted?.(turn.sourceEventId, structuredClone(receipt));
    }
    return receipt;
  };
  const settledAdmissions = new WeakSet<LiveDelegationRequest>();
  const settleAdmission = (request: LiveDelegationRequest) => {
    if (settledAdmissions.has(request)) return;
    settledAdmissions.add(request);
    options.admissionSettled?.(request);
  };
  const boundCall = (sessionId: string): BrokerCall => {
    const call = options.store.get(options.callId);
    if (!call || call.id !== options.callId || call.callSid !== options.callSid
        || call.primary?.bindingId !== options.bindingId || (call.liveSessionId !== sessionId
          && !call.liveGenerations?.segments.some(segment => segment.sessionId === sessionId)))
      throw new Error('broker_call_identity_mismatch');
    return call;
  };
  const persistSpans = () => {
    const call = options.store.get(options.callId)!;
    options.store.update(call.id, options.sourceSessionId
      ? { sourceAcousticSpans: { ...call.sourceAcousticSpans, [options.sourceSessionId]: structuredClone(acousticSpans) } }
      : { acousticSpans: structuredClone(acousticSpans) });
  };
  const sessionOrder = (sessionId: string) => options.store.get(options.callId)?.liveGenerations?.segments
    .find(segment => segment.sessionId === sessionId)?.ordinal ?? 0;
  const entries = (sessionId: string, all = false): BrokerDelegationRecord[] => {
    validateBrokerAdmissionRefusals(boundCall(sessionId));
    return Object.values(boundCall(sessionId).delegations).map(value => {
      const entry = value as BrokerDelegationRecord;
      if (entry.version !== 1 || !uuidPattern.test(entry.requestId) || entry.callId !== options.callId
          || entry.callSid !== options.callSid || entry.bindingId !== options.bindingId
          || !Array.isArray(entry.delegationIds) || !entry.submission?.source
          || !uuidPattern.test(entry.submission.source.sourceEventId)
          || entry.submission.source.callSid !== options.callSid || entry.submission.source.revision !== 1
          || entry.submission.source.role !== 'user') throw new Error('invalid_saved_delegation');
      if (boundCall(sessionId).liveGenerations) boundCall(entry.sessionId);
      for (const alias of entry.delegationAliases ?? []) {
        if (alias.sessionId === entry.sessionId || !alias.delegationId) throw new Error('invalid_saved_delegation_alias');
        boundCall(alias.sessionId);
      }
      for (const [questionId, record] of Object.entries(entry.questions ?? {})) {
        if (record.presentation?.sessionId) boundCall(record.presentation.sessionId);
        if (record.question.id !== questionId || record.identityDigest !== questionIdentity(record.question))
          throw new Error('invalid_saved_question');
        if (record.refusedReplies?.some(reply => reply.status !== 'refused')) throw new Error('invalid_saved_reply_history');
        for (const reply of replyAttempts(record)) {
          if (reply.sourceSessionId) boundCall(reply.sourceSessionId);
          if (reply.inputDigest !== digest(reply.input) || reply.input.source.callSid !== options.callSid
            || reply.input.source.revision !== 1 || reply.input.reply.questionRevision !== 1
            || !uuidPattern.test(reply.input.reply.clientReplyId) || !uuidPattern.test(reply.input.source.sourceEventId))
          throw new Error('invalid_saved_reply');
        }
      }
      if (entry.originalCapture) {
        if (turnBodyDigest(entry.originalCapture.turn) !== turnBodyDigest(originalTurn(entry.submission)))
          throw new Error('original_capture_changed');
        if (entry.originalCapture.state === 'accepted')
          acknowledgedTurn(options.bindingId, originalTurn(entry.submission), entry.originalCapture.receipt!);
      }
      if (entry.reconciliation) {
        const reconciliation = entry.reconciliation;
        if (reconciliation.requestId !== entry.requestId || reconciliation.bindingId !== options.bindingId)
          throw new Error('invalid_saved_reconciliation');
        if (reconciliation.outcome === 'admitted') {
          if (!uuidPattern.test(reconciliation.canonicalRequestId) || reconciliation.result.requestId !== reconciliation.canonicalRequestId
              || reconciliation.result.bindingId !== options.bindingId) throw new Error('invalid_saved_reconciliation');
        } else {
          const closed = boundCall(sessionId).primaryClosed;
          if (!closed || !entry.originalCapture?.receipt || entry.status !== 'not_admitted') throw new Error('invalid_saved_reconciliation');
          validateNonAdmissionProof(options.bindingId, entry.submission, entry.originalCapture.receipt, closed, reconciliation.proof);
        }
      } else if (entry.status === 'not_admitted') throw new Error('missing_non_admission_proof');
      for (const turn of entry.deferredTurns ?? []) {
        if (turn.sourceSessionId) boundCall(turn.sourceSessionId);
        const queued = turn.queuedRequest;
        if (turn.status === 'recorded_not_submitted' && !queued) continue;
        if (!queued || !['queued', 'promoted'].includes(turn.status)
            || !uuidPattern.test(queued.requestId) || !uuidPattern.test(queued.predecessorRequestId)
            || queued.requestId !== brokerIdentity('request', options.callId, turn.sourceSessionId ?? entry.sessionId, turn.delegationId)
            || queued.bodyDigest !== digest(queued.submission) || queued.submission.text !== turn.text
            || queued.submission.source.sourceEventId !== turn.sourceEventId
            || queued.submission.source.callSid !== options.callSid || queued.submission.source.revision !== 1
            || queued.submission.source.role !== 'user' || queued.submission.source.itemId !== `live-turn-${turn.sourceEventId}`
            || !Number.isFinite(Date.parse(queued.submission.source.receivedAt))
            || !Number.isSafeInteger(turn.firstSequence) || turn.firstSequence < 1
            || !Number.isSafeInteger(queued.lastSequence) || queued.lastSequence < turn.firstSequence
            || queued.completionBasis !== 'speech_and_transcript_stability' || !validTurnText(turn.text))
          throw new Error('invalid_saved_deferred_turn');
        if (turn.status === 'promoted') {
          const child = boundCall(sessionId).delegations[queued.requestId] as BrokerDelegationRecord | undefined;
          if (!child || child.predecessorRequestId !== queued.predecessorRequestId
              || digest(child.submission) !== queued.bodyDigest) throw new Error('invalid_promoted_deferred_turn');
        }
      }
      return entry;
    }).filter(entry => all || entry.sessionId === sessionId);
  };
  const save = (entry: BrokerDelegationRecord,
    apiErrors: Pick<BrokerDelegationRecord, 'submitError' | 'lastResultReadError'> = {}): BrokerDelegationRecord => {
    const call = boundCall(entry.sessionId);
    const previous = call.delegations[entry.requestId] as BrokerDelegationRecord | undefined;
    const next = { ...entry, updatedAt: new Date(now()).toISOString(),
      submitError: previous?.submitError ?? apiErrors.submitError,
      lastResultReadError: apiErrors.lastResultReadError ?? previous?.lastResultReadError };
    // These are local observations. Delivery acceptance is distinct from audio playback.
    const timing = { ...(previous?.timing ?? entry.timing ?? { firstSavedAt: next.updatedAt }) };
    if (next.status === 'dispatching') timing.firstDispatchAt ??= next.updatedAt;
    if (next.primaryResult?.state === 'completed') timing.firstAnswerAvailableAt ??= next.updatedAt;
    if (next.status === 'completed' && next.delivery === 'accepted') timing.firstDeliveryAcceptedAt ??= next.updatedAt;
    next.timing = timing;
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
        ...(options.sourceSessionId ? { sourceSessionId: reply.sourceSessionId ?? entry.sessionId } : {}),
      } satisfies CanonicalTurnRecord;
    }
    const harkBinding = next.status === 'completed' && next.primaryResult?.state === 'completed'
      ? primaryHarkReceiptBindingFromCall(call) : undefined;
    const harkPatch = harkBinding && next.primaryResult
      ? preparePrimaryHarkOutcomePatch({ binding: harkBinding, outbox: call.harkReceiptOutbox,
        canonicalRequestId: next.reconciliation?.outcome === 'admitted' ? next.reconciliation.canonicalRequestId : next.requestId,
        result: next.primaryResult, observedAt: next.updatedAt }) : undefined;
    validateBrokerAdmissionRefusals({ ...call, delegations: { ...call.delegations, [entry.requestId]: next } }, call);
    options.store.update(call.id, { delegations: { ...call.delegations, [entry.requestId]: next }, canonicalTurns,
      ...(harkPatch ? { harkReceiptOutbox: harkPatch.harkReceiptOutbox } : {}) });
    return next;
  };
  const load = (entry: BrokerDelegationRecord) => entries(entry.sessionId).find(value => value.requestId === entry.requestId)!;
  const captureUnadmittedSubmission = async (entry: BrokerDelegationRecord): Promise<BrokerDelegationRecord> => {
    let current = load(entry);
    if (!['admission_refused', 'admission_expired'].includes(current.status) || current.originalCapture?.state === 'accepted') return current;
    const turn = originalTurn(current.submission);
    current = save({ ...current, originalCapture: { turn, state: 'dispatching' } });
    try {
      const receipt = acknowledgedTurn(options.bindingId, turn, await captureTurn(turn));
      return save({ ...load(current), originalCapture: { turn, state: 'accepted', receipt } });
    } catch { return save({ ...load(current), originalCapture: { turn, state: 'unknown' } }); }
  };
  /** Called only by the catch for an initial POST, including queue promotion. */
  const firstSubmitFailure = async (entry: BrokerDelegationRecord, error: unknown): Promise<BrokerDelegationRecord> => {
    const current = load(entry);
    if (current.status === 'dispatching' && !current.submitError && !current.lastResultReadError
      && error instanceof PrimaryApiError && error.code === 'http_error' && error.remoteCode
      && ((current.submission.associatedResultReference && error.status===409
        && (error.remoteCode==='delivery_association_mismatch' || error.remoteCode==='associated_result_unavailable'))
        || (current.submission.resultSms && current.submission.resultSms.requestId===current.requestId
          && isSmsAdmissionRefusal(error.status ?? 0,error.remoteCode)))) {
      const refused = save({ ...current, status: 'admission_refused', admissionRefusal: {
        origin: 'api_non_admission', status: error.status as 409|503,
        code: error.remoteCode as NonNullable<BrokerDelegationRecord['admissionRefusal']>['code'],
        submissionDigest: rawSubmissionDigest(options.bindingId, current.submission) } });
      return captureUnadmittedSubmission(refused);
    }
    return save({ ...current, status: 'dispatch_unknown' }, { submitError: diagnostic(error) });
  };
  const checkTaskDispatch = (entry: BrokerDelegationRecord): BrokerDelegationRecord => {
    const call = boundCall(entry.sessionId);
    if (!getCallerRouteBySource(call, entry.submission.source.sourceEventId)) return entry;
    try { requireCallerRouteDispatch(call, entry.submission.source.sourceEventId, now()); return entry; }
    catch (error) {
      if (!(error instanceof CallerRouteExpiredError)) throw error;
      const expired = save({ ...load(entry), status: 'admission_expired', admissionExpiry: {
        origin: 'caller_intent_expiry', validUntil: error.validUntil, inputDigest: error.inputDigest } });
      return expired;
    }
  };
  const dispatchPreparedTask = async (entry: BrokerDelegationRecord): Promise<BrokerDelegationRecord> => {
    let current = checkTaskDispatch(entry);
    if (current.status === 'admission_expired') return captureUnadmittedSubmission(current);
    current = save({ ...current, status: 'dispatching' });
    // The durable write consumes the same fixed decision budget.
    current = checkTaskDispatch(current);
    if (current.status === 'admission_expired') return captureUnadmittedSubmission(current);
    try { return applyResult(current, await options.api.submit(options.bindingId, current.requestId, structuredClone(current.submission))); }
    catch (error) { return firstSubmitFailure(current, error); }
  };
  const diagnostic = (error: unknown): PrimaryApiDiagnostic | undefined => error instanceof PrimaryApiError
    ? { code: error.code, ...(error.status === undefined ? {} : { status: error.status }),
      ...(error.remoteCode === undefined ? {} : { remoteCode: error.remoteCode }),
      ...(error.retryable === undefined ? {} : { retryable: error.retryable }), observedAt: new Date(now()).toISOString() }
    : undefined;
  const recordReadError = (entry: BrokerDelegationRecord, error: unknown) =>
    save({ ...load(entry), resultReadUnavailable: true }, { lastResultReadError: diagnostic(error) });
  const knownReply = (sessionId: string, sourceEventId: string) => {
    for (const entry of entries(sessionId, true)) for (const [questionId, record] of Object.entries(entry.questions ?? {})) {
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
        ...(options.sourceSessionId ? { sourceSessionId: reply.sourceSessionId ?? entry.sessionId } : {}),
        occurredAt: input.source.receivedAt, state: 'captured', recordId: input.source.sourceEventId });
      current = { ...current, status: terminal(current.status) ? current.status : 'running', questionNotice: undefined,
        questions: { ...current.questions, [questionId]: { ...record, reply: { ...reply, status: 'accepted' } } } };
    } else if (result.question.state !== 'pending') {
      current = { ...current, questionNotice: 'question_unavailable', questions: { ...current.questions,
        [questionId]: { ...record, reply: { ...reply, status: 'refused' } } } };
    } else if (unresolvedReply(reply)) current = { ...current, questionNotice: 'reply_unknown' };
    return save(current);
  };
  const presentationAccepted = (entry: BrokerDelegationRecord, presentation?: BrokerQuestionPresentation) =>
    !!presentation?.sessionId && brokerPublicationReceiptsAccepted(boundCall(entry.sessionId), entry.primaryResult?.requestId ?? entry.requestId,
      presentation.sessionId, presentation.publicationReceipts);
  const dispatchPreparedReply = async (entry: BrokerDelegationRecord, questionId: string, stopped?: () => boolean) => {
    let current = load(entry);
    const record = current.questions![questionId];
    const reply = record.reply;
    if (!reply || reply.status !== 'prepared' || !reply.admissionPresentation) return current;
    const call = boundCall(entry.sessionId);
    if (call.endedAt || terminalCallStatuses.has(call.status) || options.observation?.signal.aborted)
      return save({ ...current, questionNotice: 'reply_refused', questions: { ...current.questions,
        [questionId]: { ...record, reply: { ...reply, status: 'refused', refusal: {
          origin: 'local_validation', code: 'call_ended_before_reply_dispatch' } } } } });
    if (stopped?.()) return current;
    const unavailable = record.question.state !== 'pending' || Date.parse(record.question.expiresAt) <= now();
    const publicationValid = presentationAccepted(current, reply.admissionPresentation);
    if (unavailable || !publicationValid) return save({ ...current,
      questionNotice: unavailable ? 'question_unavailable' : 'reply_refused', questions: { ...current.questions,
        [questionId]: { ...record, reply: { ...reply, status: 'refused', refusal: {
          origin: 'local_validation', code: unavailable ? 'question_unavailable' : 'question_publication_rejected' } } } } });
    const refuseExpiredIntent = (): BrokerDelegationRecord | undefined => {
      const call = boundCall(entry.sessionId);
      if (!getCallerRouteBySource(call, reply.input.source.sourceEventId)) return;
      try { requireCallerRouteDispatch(call, reply.input.source.sourceEventId, now()); }
      catch (error) {
        if (!(error instanceof CallerRouteExpiredError)) throw error;
        const saved = load(current), question = saved.questions![questionId];
        return save({ ...saved, questionNotice: 'reply_refused', questions: { ...saved.questions,
          [questionId]: { ...question, reply: { ...question.reply!, status: 'refused', refusal: {
            origin: 'local_validation', code: 'caller_intent_expired' } } } } });
      }
    };
    const beforeSave = refuseExpiredIntent(); if (beforeSave) return beforeSave;
    current = save({ ...current, questionNotice: undefined, questions: { ...current.questions,
      [questionId]: { ...record, reply: { ...reply, status: 'dispatching' } } } });
    const beforePost = refuseExpiredIntent(); if (beforePost) return beforePost;
    try {
      return applyReplyObservation(current, questionId,
        await options.api.reply(options.bindingId, current.requestId, questionId, structuredClone(reply.input)));
    } catch (error) {
      const refusal = firstReplyNonAdmission(error);
      current = load(current);
      return save({ ...current, questionNotice: refusal ? 'reply_refused' : 'reply_unknown', questions: { ...current.questions,
        [questionId]: { ...current.questions![questionId], reply: { ...reply,
          status: refusal ? 'refused' : 'unknown', ...(refusal ? { refusal } : {}) } } } });
    }
  };
  const recoverReplies = async (entry: BrokerDelegationRecord, stopped?: () => boolean) => {
    let current = load(entry);
    for (const [questionId, record] of Object.entries(current.questions ?? {})) {
      if (!unresolvedReply(record.reply)) continue;
      const call = boundCall(entry.sessionId);
      if (record.reply?.status === 'prepared' && record.reply.admissionPresentation
        && (call.endedAt || terminalCallStatuses.has(call.status) || options.observation?.signal.aborted)) {
        current = await dispatchPreparedReply(current, questionId, stopped);
        continue;
      }
      if (stopped?.()) break;
      try {
        current = applyReplyObservation(current, questionId, await options.api.question(options.bindingId, current.requestId, questionId));
        // Only the durable prepared state proves this exact answer has never been POSTed.
        if (options.observation) current = await dispatchPreparedReply(current, questionId, stopped);
      }
      catch { current = save({ ...load(current), questionNotice: 'reply_unknown' }); }
    }
    return current;
  };
  const mediaOrigin = (sessionId: string): number => {
    const call = boundCall(sessionId);
    const generation = call.liveGenerations?.segments.find(segment => segment.sessionId === sessionId);
    const origin = options.mediaStartedAt ?? call.mediaStartedAt;
    if (!origin || !Number.isFinite(Date.parse(origin))) throw new Error('media_time_origin_missing');
    if (generation && generation.inputFirstSample === undefined) throw new Error('source_audio_origin_missing');
    return Date.parse(origin) + (generation?.inputFirstSample ?? 0) / 8;
  };
  const speechTime = () => options.lastCallerSpeechAt?.() ?? lastSpeechAt;
  const assertFrozenSpeech = (frozen: FrozenCallerTurn) => assertFrozenCallerSpeechCurrent(frozen, {
    sessionId: currentSession(), audioThroughMs: acousticSpans.at(-1)?.endMs, observedSpeechAtMs: speechTime(),
  });
  const selectedSources = (sessionId: string): readonly SelectedQuestionSourceAnchor[] => {
    const merged = new Map<string, CallerSourceAnchor>();
    for (const source of [...options.selectedQuestionSources?.() ?? [], ...options.callerRouteSources?.() ?? []]) {
      const id = source.frozen.turn.sourceEventId, prior = merged.get(id);
      if (prior && (canonicalJson(prior.frozen) !== canonicalJson(source.frozen)
        || (prior.capture && source.capture && canonicalJson({ ...prior.capture, replayed: false })
          !== canonicalJson({ ...source.capture, replayed: false }))))
        throw new Error('caller_route_source_changed');
      merged.set(id, prior ? { frozen: prior.frozen, capture: prior.capture ?? source.capture,
        replyPending: prior.replyPending || source.replyPending } : source);
    }
    const sources = [...merged.values()];
    const ids = new Set<string>();
    for (const source of sources) {
      const frozen = source.frozen, turn = frozen.turn;
      if (ids.has(turn.sourceEventId) || typeof source.replyPending !== 'boolean'
        || turn.sourceEventId !== brokerIdentity('source', options.callId, frozen.sessionId, frozen.firstEventId)
        || turn.itemId !== `live-turn-${turn.sourceEventId}` || turn.role !== 'user' || turn.revision !== 1
        || !validTurnText(turn.text) || turn.occurredAt !== new Date(mediaOrigin(frozen.sessionId) + frozen.startMs).toISOString())
        throw new Error('selected_question_source_changed');
      if (source.capture) acknowledgedTurn(options.bindingId, turn, source.capture);
      ids.add(turn.sourceEventId);
    }
    return sources.filter(source => source.frozen.sessionId === sessionId);
  };
  const completedCallerGroups = (snapshot: LiveTranscriptSnapshot, observedAt = now()) => {
    // Captions and delegations share Live's session clock. Input amplitude is
    // only local activity/silence evidence: clicks and pauses prevent a reliable
    // one-to-one correspondence between captions and local sound bursts.
    const latestAudio = acousticSpans.at(-1);
    if (!latestAudio) return [];
    const sessionId = currentSession();
    const grouped = callerGroups(snapshot).map(fragments => {
      fragments.sort((a, b) => a.sequence - b.sequence);
      return { fragments, sourceEventId: brokerIdentity('source', options.callId, sessionId, fragments[0].eventId) };
    });
    if (grouped.some((group, index) => index > 0 && group.fragments[0].sequence <= grouped[index - 1].fragments[0].sequence)) return [];
    const matchesAnchor = (sourceEventId: string, firstSequence?: number, lastSequence?: number) => {
      const group = grouped.find(value => value.sourceEventId === sourceEventId);
      return !!group && (firstSequence === undefined || group.fragments[0].sequence === firstSequence)
        && (lastSequence === undefined || group.fragments.some(fragment => fragment.sequence === lastSequence));
    };
    // A late caption may extend its source, but cannot split, merge or replace
    // an already accepted source. Saved requests/replies remain byte-exact.
    for (const entry of entries(sessionId, true)) {
      if (entry.sessionId === sessionId && !matchesAnchor(entry.submission.source.sourceEventId, entry.firstSequence, entry.lastSequence)) return [];
      for (const turn of entry.deferredTurns ?? [])
        if ((turn.sourceSessionId ?? entry.sessionId) === sessionId && !matchesAnchor(turn.sourceEventId, turn.firstSequence, turn.queuedRequest?.lastSequence)) return [];
      for (const question of Object.values(entry.questions ?? {})) for (const reply of replyAttempts(question))
        if ((reply.sourceSessionId ?? entry.sessionId) === sessionId && !matchesAnchor(reply.input.source.sourceEventId, reply.firstSequence, reply.lastSequence)) return [];
    }
    for (const value of Object.values(boundCall(sessionId).canonicalTurns ?? {})) {
      const turn = value as CanonicalTurnRecord;
      if (turn.role === 'user' && (turn.sourceSessionId === undefined || turn.sourceSessionId === sessionId) && !matchesAnchor(turn.sourceEventId)) return [];
    }
    for (const source of selectedSources(sessionId))
      if (!matchesAnchor(source.frozen.turn.sourceEventId, source.frozen.firstSequence, source.frozen.lastSequence)) return [];
    if (observedAt - latestAudio.lastSpeechAt >= stabilityMs) return grouped;
    const checkpoints = boundCall(sessionId).callerCompletions;
    return grouped.filter(group => {
      const checkpoint = checkpoints?.[group.sourceEventId];
      return checkpoint?.sessionId === sessionId && Date.parse(checkpoint.observedAt) <= observedAt
        && checkpoint.firstSequence === group.fragments[0].sequence
        && checkpoint.lastSequence === Math.max(...group.fragments.map(fragment => fragment.sequence))
        && checkpoint.textDigest === digest(group.fragments.map(fragment => fragment.text).join(''));
    });
  };
  let observedTranscript: LiveTranscriptSnapshot | undefined;
  let transcriptChangedAt = 0;
  let completionTimer: ReturnType<typeof setTimeout> | undefined;
  const scheduleCallerCompletion = () => {
    clearTimeout(completionTimer);
    if (!observedTranscript || options.store.get(options.callId)?.endedAt) return;
    const source = options.sourceSessionId && options.store.get(options.callId)?.liveGenerations?.segments
      .find(segment => segment.sessionId === options.sourceSessionId);
    if (source && source.state !== 'active') return;
    const quietSince = Math.max(transcriptChangedAt, speechTime() ?? 0);
    completionTimer = setTimeout(() => {
      try {
        const snapshot = observedTranscript;
        if (!snapshot || options.store.get(options.callId)?.endedAt) return;
        const source = options.sourceSessionId && options.store.get(options.callId)?.liveGenerations?.segments
          .find(segment => segment.sessionId === options.sourceSessionId);
        if (source && source.state !== 'active') return;
        if (now() - Math.max(transcriptChangedAt, speechTime() ?? 0) < stabilityMs) { scheduleCallerCompletion(); return; }
        const sessionId = currentSession();
        const call = boundCall(sessionId);
        const checkpoints = { ...call.callerCompletions };
        let changed = false;
        for (const group of completedCallerGroups(snapshot)) {
          const firstSequence = group.fragments[0].sequence;
          const lastSequence = Math.max(...group.fragments.map(fragment => fragment.sequence));
          const textDigest = digest(group.fragments.map(fragment => fragment.text).join(''));
          const previous = checkpoints[group.sourceEventId];
          if (previous?.sessionId === sessionId && previous.firstSequence === firstSequence
              && previous.lastSequence === lastSequence && previous.textDigest === textDigest) continue;
          checkpoints[group.sourceEventId] = { sessionId, firstSequence, lastSequence, textDigest, observedAt: new Date(now()).toISOString() };
          changed = true;
        }
        if (changed) options.store.update(call.id, { callerCompletions: checkpoints });
      } catch {
        // The optional checkpoint must not crash an active call. Closeout will
        // report unqualified text when no durable completion evidence exists.
        console.error('broker_caller_completion_failed', { callId: options.callId });
      }
    }, Math.max(1, stabilityMs - (now() - quietSince)));
    completionTimer.unref();
  };
  speechListeners.add(scheduleCallerCompletion);
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
  const candidateCallerGroup = (request: LiveDelegationRequest, snapshot: LiveTranscriptSnapshot,
    audioThroughMs: number | undefined, latestSourceSequence: number, all: readonly BrokerDelegationRecord[],
    usedSourceIds: readonly string[] = []) => completedCallerGroups(snapshot).filter(group => {
    const first = group.fragments.reduce((a, b) => a.sequence < b.sequence ? a : b);
    const source = brokerIdentity('source', options.callId, request.sessionId, first.eventId);
    const presentAtDelegation = request.transcript.fragments.some(fragment => fragment.role === 'user'
      && fragment.eventId === first.eventId && fragment.sequence === first.sequence);
    return audioThroughMs !== undefined
      && (presentAtDelegation || first.startMs <= request.offsetMs)
      && first.sequence >= latestSourceSequence && !usedSourceIds.includes(source)
      && !all.some(entry => entry.submission.source.sourceEventId === source
        || entry.deferredTurns?.some(turn => turn.sourceEventId === source)
        || Object.values(entry.questions ?? {}).some(record => replyAttempts(record).some(reply => reply.input.source.sourceEventId === source)));
  }).at(-1);
  const freezeCallerTurn = async (request: LiveDelegationRequest, channel: LiveBackendChannel,
    usedSourceIds: readonly string[]): Promise<FrozenCallerTurn | undefined> => {
    if (request.callId !== options.callId || request.sessionId !== currentSession()) throw new Error('broker_call_identity_mismatch');
    const audioThroughMs = acousticSpans.at(-1)?.endMs;
    const snapshot = await stableContext(channel);
    if (!snapshot || channel.signal.aborted) return;
    return locked(async () => {
      const call = boundCall(request.sessionId);
      if (channel.signal.aborted || call.endedAt || call.primaryClosed || terminalCallStatuses.has(call.status)) return;
      const anchors = selectedSources(request.sessionId);
      const candidate = candidateCallerGroup(request, snapshot, audioThroughMs, 0, entries(request.sessionId, true),
        [...usedSourceIds, ...anchors.map(source => source.frozen.turn.sourceEventId)]);
      if (!candidate) return;
      const fragments = candidate.fragments, first = fragments.reduce((a, b) => a.sequence < b.sequence ? a : b);
      const text = fragments.map(fragment => fragment.text).join('');
      const observedSpeechAtMs = speechTime();
      if (!validTurnText(text) || observedSpeechAtMs === undefined) return;
      return { sessionId: request.sessionId, firstEventId: first.eventId, callerRevision: snapshot.callerRevision,
        firstSequence: first.sequence, lastSequence: Math.max(...fragments.map(fragment => fragment.sequence)),
        startMs: first.startMs, endMs: Math.max(...fragments.map(fragment => fragment.endMs)),
        audioThroughMs: audioThroughMs!, observedSpeechAtMs,
        turn: { sourceEventId: candidate.sourceEventId, revision: 1, itemId: `live-turn-${candidate.sourceEventId}`,
          role: 'user', text, occurredAt: new Date(mediaOrigin(request.sessionId) + first.startMs).toISOString() } } satisfies FrozenCallerTurn;
    });
  };
  // Both immediate and deferred requests freeze their submission at the caller
  // frontier. Any conversation reference belongs here, never at later promotion.
  const assignmentReference = (sessionId: string) => {
    const call = boundCall(sessionId);
    const assignment = call.callAssignment === undefined ? undefined : brokerCallAssignmentSchema.parse(call.callAssignment);
    const assignmentRef = call.primary?.callAssignmentRef;
    if ((assignment === undefined) !== (assignmentRef === undefined)
        || (assignment && (assignment.value.assignmentId !== call.requestId || assignment.value.brokerCallId !== call.id
          || assignment.value.purpose !== call.purpose || assignment.value.revision !== 1
          || assignmentRef!.assignmentId !== assignment.value.assignmentId || assignmentRef!.revision !== 1)))
      throw new Error('call_assignment_binding_mismatch');
    return assignmentRef === undefined ? undefined : phoneCallAssignmentRefSchema.parse(assignmentRef);
  };
  const submissionForTurn = (sessionId: string, snapshot: LiveTranscriptSnapshot,
    fragments: LiveTranscriptSnapshot['fragments']): PrimarySubmission => {
    const first = fragments.reduce((a, b) => a.sequence < b.sequence ? a : b);
    const sourceEventId = brokerIdentity('source', options.callId, sessionId, first.eventId);
    const call = boundCall(sessionId), frozenAssignmentRef = assignmentReference(sessionId);
    return { source: { callSid: options.callSid, itemId: `live-turn-${sourceEventId}`, sourceEventId,
      revision: 1, receivedAt: new Date(mediaOrigin(sessionId) + first.startMs).toISOString(), role: 'user' },
    text: fragments.map(fragment => fragment.text).join(''),
    ...selectBrokerPublicationReference(call, { callId: options.callId, callSid: options.callSid,
      bindingId: options.bindingId, sessionId }, snapshot, first.sequence),
    ...(frozenAssignmentRef === undefined ? {} : { callAssignmentRef: frozenAssignmentRef }) };
  };
  const frozenSubmission = (frozen: FrozenCallerTurn): PrimarySubmission => {
    const ref = assignmentReference(frozen.sessionId), turn = frozen.turn;
    return { source: { callSid: options.callSid, itemId: turn.itemId, sourceEventId: turn.sourceEventId,
      revision: 1, receivedAt: turn.occurredAt, role: 'user' }, text: turn.text,
    ...(ref === undefined ? {} : { callAssignmentRef: ref }) };
  };
  const questionTargets = (sessionId: string): CallerQuestionTarget[] => {
    const call = boundCall(sessionId);
    if (call.endedAt || call.primaryClosed || terminalCallStatuses.has(call.status)
      || call.liveGenerations?.phase !== 'active' || call.liveGenerations.deliveryAttachment?.sessionId !== sessionId) return [];
    return entries(sessionId, true).flatMap(entry => {
      const questionId = entry.currentQuestionId, record = questionId ? entry.questions?.[questionId] : undefined;
      const presentation = record?.presentation;
      if (!questionId || entry.status !== 'waiting_for_reply' || !record || record.question.state !== 'pending'
        || Date.parse(record.question.expiresAt) <= now() || unresolvedReply(record.reply) || record.reply?.status === 'accepted'
        || presentation?.sessionId !== sessionId || presentation.inputThroughMs === undefined
        || !presentationAccepted(entry, presentation)) return [];
      return [{ kind: 'ordinary' as const, requestId: entry.requestId, bindingId: options.bindingId,
        runId: record.question.runId, questionId, questionRevision: 1 as const,
        presentationDigest: callerRoutePresentationDigest(presentation) }];
    });
  };
  const acceptFrozenTask = (request: LiveDelegationRequest, ref: CallerRouteRef,
    channel: LiveBackendChannel): Promise<BrokerDelegationRecord | undefined> => locked(async () => {
    if (request.sessionId !== currentSession()) throw new Error('broker_call_identity_mismatch');
    const savedRoute = requireCallerRoute(boundCall(request.sessionId), request, ref, 'task', 'recover');
    let all = entries(request.sessionId, true);
    const frozen = savedRoute.frozen;
    const prior = all.find(entry => entry.submission.source.sourceEventId === frozen.turn.sourceEventId);
    if (prior) return prior;
    const queued = all.find(entry => entry.deferredTurns?.some(turn => turn.sourceEventId === frozen.turn.sourceEventId));
    if (queued) return queued;
    if (channel.signal.aborted || options.observation?.signal.aborted) return;
    assertFrozenSpeech(frozen);
    const route = requireCallerRoute(boundCall(request.sessionId), request, ref, 'task');
    if (route.decision?.kind !== 'task') throw new Error('caller_route_kind_mismatch');
    const submission = frozenSubmission(frozen);
    if(route.decision.delivery) {
      if(route.intent?.response?.v!=='phone.caller-intent.v3')throw new Error('caller_route_intent_decision_changed');
      submission.resultSms={v:'phone.result-sms.v1',callerIntentOperationId:route.intent.operationId,
        requestId:brokerIdentity('request',options.callId,request.sessionId,request.delegationId)};
    }
    if (route.decision.contextReference !== undefined) {
      if (!options.resolveCallerTaskReference) throw new Error('caller_route_context_reference_unsupported');
      const reference = await options.resolveCallerTaskReference(structuredClone(route));
      if (!reference) throw new Error('caller_route_context_reference_unavailable');
      const checked = callerIntentDecisionSchema.parse({ kind: 'task', reference });
      const receipt = route.intent?.response;
      if (checked.kind !== 'task' || !checked.reference || receipt?.state !== 'resolved'
        || receipt.decision.kind !== 'task' || canonicalJson(receipt.decision.reference) !== canonicalJson(checked.reference))
        throw new Error('caller_route_context_reference_changed');
      if (checked.reference.kind === 'associated')
        submission.associatedResultReference = orderedAssociatedResultReference(checked.reference.associatedResultReference);
      else {
        submission.relatedPrimaryRequestId = checked.reference.relatedPrimaryRequestId;
        submission.referenceOrigin = checked.reference.referenceOrigin;
      }
      requireCallerRoute(boundCall(request.sessionId), request, ref, 'task');
      if (channel.signal.aborted || options.observation?.signal.aborted) return;
      assertFrozenSpeech(frozen);
      all = entries(request.sessionId, true);
    }
    const active = all.find(entry => !terminal(entry.status))
      ?? all.find(entry => entry.deferredTurns?.some(turn => turn.status === 'queued'));
    if (active && !submission.resultSms) {
      const queuedTurns = all.flatMap(entry => (entry.deferredTurns ?? []).map(turn => ({ turn, session: turn.sourceSessionId ?? entry.sessionId })))
        .filter(value => value.turn.status === 'queued').sort((a, b) => sessionOrder(a.session) - sessionOrder(b.session)
          || a.turn.firstSequence - b.turn.firstSequence).map(value => value.turn);
      const turn: BrokerDeferredTurn = { delegationId: request.delegationId, sourceEventId: frozen.turn.sourceEventId,
        text: frozen.turn.text, firstSequence: frozen.firstSequence, callerRevision: frozen.callerRevision, status: 'queued',
        ...(active.sessionId !== request.sessionId ? { sourceSessionId: request.sessionId } : {}),
        queuedRequest: { requestId: brokerIdentity('request', options.callId, request.sessionId, request.delegationId),
          predecessorRequestId: queuedTurns.at(-1)?.queuedRequest!.requestId ?? active.requestId,
          submission, bodyDigest: digest(submission), lastSequence: frozen.lastSequence,
          audioThroughMs: frozen.audioThroughMs, completionBasis: 'speech_and_transcript_stability' } };
      persistSpans();
      const entry = save({ ...active, deferredTurns: [...active.deferredTurns ?? [], turn],
        ...(active.sessionId === request.sessionId ? { delegationIds: [...active.delegationIds, request.delegationId] }
          : { delegationAliases: [...active.delegationAliases ?? [], { sessionId: request.sessionId, delegationId: request.delegationId }] }) });
      settleAdmission(request);
      return entry;
    }
    persistSpans();
    let entry = save({ version: 1, requestId: brokerIdentity('request', options.callId, request.sessionId, request.delegationId),
      callId: options.callId, callSid: options.callSid, bindingId: options.bindingId, sessionId: request.sessionId,
      delegationIds: [request.delegationId], status: 'prepared', updatedAt: new Date(now()).toISOString(), submission,
      frozenCallerRevision: frozen.callerRevision, firstSequence: frozen.firstSequence, lastSequence: frozen.lastSequence,
      audioThroughMs: frozen.audioThroughMs, completionBasis: 'speech_and_transcript_stability',
      sourceRevision: 1, capturedText: frozen.turn.text });
    settleAdmission(request);
    return dispatchPreparedTask(entry);
  });
  const acceptFrozenAnswer = (request: LiveDelegationRequest, ref: CallerRouteRef,
    channel: LiveBackendChannel): Promise<BrokerDelegationRecord | undefined> => locked(async () => {
    if (request.sessionId !== currentSession()) throw new Error('broker_call_identity_mismatch');
    const route = requireCallerRoute(boundCall(request.sessionId), request, ref, 'ordinary_answer', 'recover');
    if (route.decision?.kind !== 'answer' || route.decision.target.kind !== 'ordinary') throw new Error('caller_route_kind_mismatch');
    const target = route.decision.target, frozen = route.frozen;
    let entry = entries(request.sessionId, true).find(value => value.requestId === target.requestId);
    if (!entry || target.bindingId !== options.bindingId) throw new Error('caller_route_question_changed');
    const record = entry.questions?.[target.questionId];
    if (!record || record.question.runId !== target.runId || record.question.revision !== target.questionRevision)
      throw new Error('caller_route_question_changed');
    if (replyAttempts(record).some(reply => reply.input.source.sourceEventId === frozen.turn.sourceEventId)) return entry;
    if (channel.signal.aborted || options.observation?.signal.aborted) return;
    assertFrozenSpeech(frozen);
    requireCallerRoute(boundCall(request.sessionId), request, ref, 'ordinary_answer');
    const presentation = record.presentation;
    if (!questionTargets(request.sessionId).some(candidate => canonicalJson(candidate) === canonicalJson(target))
      || !presentation || presentation.sessionId !== frozen.sessionId || presentation.inputThroughMs === undefined
      || frozen.firstSequence <= presentation.sequence || frozen.callerRevision <= presentation.callerRevision
      || frozen.startMs < presentation.inputThroughMs || frozen.observedSpeechAtMs <= Date.parse(presentation.acceptedAt)
      || !(frozen.startMs > presentation.lastCallerEndMs || (presentation.sequence === 0 && frozen.startMs === 0)))
      throw new Error('caller_route_question_changed');
    const input: PrimaryReplyInput = { source: { callSid: options.callSid, sourceEventId: frozen.turn.sourceEventId,
      revision: 1, itemId: frozen.turn.itemId, receivedAt: frozen.turn.occurredAt }, role: 'user', text: frozen.turn.text,
      reply: { questionRevision: 1, clientReplyId: brokerIdentity('question-reply', options.callId, request.sessionId,
        entry.requestId, target.questionId, frozen.turn.sourceEventId), ...literalPhoneAnswer(frozen.turn.text) } };
    const reply: BrokerQuestionReplyRecord = { input, inputDigest: digest(input), status: 'prepared',
      admissionPresentation: structuredClone(presentation), callerRevision: frozen.callerRevision,
      firstSequence: frozen.firstSequence, lastSequence: frozen.lastSequence, audioThroughMs: frozen.audioThroughMs,
      ...(entry.sessionId !== request.sessionId ? { sourceSessionId: request.sessionId } : {}) };
    persistSpans();
    entry = save({ ...entry, questionNotice: undefined, questions: { ...entry.questions,
      [target.questionId]: { ...record, refusedReplies: [...record.refusedReplies ?? [], ...record.reply ? [record.reply] : []], reply } } });
    settleAdmission(request);
    try { entry = applyReplyObservation(entry, target.questionId, await options.api.question(options.bindingId, entry.requestId, target.questionId)); }
    catch { return save({ ...load(entry), questionNotice: 'reply_unknown' }); }
    return dispatchPreparedReply(entry, target.questionId, () => channel.signal.aborted || Boolean(options.observation?.signal.aborted));
  });
  const unadmittedReasons = new WeakMap<LiveDelegationRequest, string>();
  const accept = async (request: LiveDelegationRequest, channel: LiveBackendChannel): Promise<BrokerDelegationRecord | undefined> => {
    if (request.callId !== options.callId) throw new Error('broker_call_identity_mismatch');
    const audioThroughMs = acousticSpans.at(-1)?.endMs;
    const matchesDelegation = (entry: BrokerDelegationRecord) => (entry.sessionId === request.sessionId && entry.delegationIds.includes(request.delegationId))
      || entry.delegationAliases?.some(alias => alias.sessionId === request.sessionId && alias.delegationId === request.delegationId);
    const before = entries(request.sessionId, true);
    const replay = before.find(entry => entry.predecessorRequestId && matchesDelegation(entry)) ?? before.find(matchesDelegation);
    if (replay) return replay;
    const snapshot = await stableContext(channel);
    if (!snapshot || channel.signal.aborted) {
      unadmittedReasons.set(request, channel.signal.aborted ? 'source_session_closed' : 'caller_context_not_stable');
      return undefined;
    }
    return locked(async () => {
      if (channel.signal.aborted) { unadmittedReasons.set(request, 'source_session_closed'); return undefined; }
      if (boundCall(request.sessionId).endedAt) throw new Error('broker_call_ended');
      const saved = entries(request.sessionId);
      const all = entries(request.sessionId, true);
      const duplicate = all.find(entry => entry.predecessorRequestId && matchesDelegation(entry)) ?? all.find(matchesDelegation);
      if (duplicate) return duplicate;
      const latestSourceSequence = Math.max(0, ...saved.map(entry => entry.firstSequence));
      const candidate = candidateCallerGroup(request, snapshot, audioThroughMs, latestSourceSequence, all);
      const fragments = candidate?.fragments;
      const queuedTurns = all.flatMap(entry => (entry.deferredTurns ?? []).map(turn => ({ turn, session: turn.sourceSessionId ?? entry.sessionId })))
        .filter(value => value.turn.status === 'queued').sort((a, b) => sessionOrder(a.session) - sessionOrder(b.session)
          || a.turn.firstSequence - b.turn.firstSequence).map(value => value.turn);
      // A newly delegated turn cannot jump ahead of saved work during the small
      // interval between a terminal result and queue promotion.
      const active = all.find(entry => !terminal(entry.status))
        ?? all.find(entry => entry.deferredTurns?.some(turn => turn.status === 'queued'));
      if (active) {
        let current = save(active.sessionId === request.sessionId
          ? { ...active, delegationIds: [...active.delegationIds, request.delegationId] }
          : { ...active, delegationAliases: [...active.delegationAliases ?? [], { sessionId: request.sessionId, delegationId: request.delegationId }] });
        if (Object.values(current.questions ?? {}).some(record => unresolvedReply(record.reply))) {
          if (options.observation) {
            const first = fragments?.reduce((a, b) => a.sequence < b.sequence ? a : b);
            const text = fragments?.map(fragment => fragment.text).join('');
            if (first && text && validTurnText(text)) current = save({ ...current, deferredTurns: [...current.deferredTurns ?? [], {
              ...(current.sessionId !== request.sessionId ? { sourceSessionId: request.sessionId } : {}),
              delegationId: request.delegationId, sourceEventId: brokerIdentity('source', options.callId, request.sessionId, first.eventId),
              text, firstSequence: first.sequence, callerRevision: snapshot.callerRevision, status: 'recorded_not_submitted',
            }] });
            return current; // The existing observer recovers the older answer; this turn cannot replace it.
          }
          current = await recoverReplies(current);
          if (Object.values(current.questions ?? {}).some(record => unresolvedReply(record.reply))) return current;
        }
        const questionId = current.currentQuestionId;
        const question = questionId ? current.questions?.[questionId] : undefined;
        const presentation = question && question.presentation;
        const first = fragments?.reduce((a, b) => a.sequence < b.sequence ? a : b);
        if (current.status === 'waiting_for_reply' && question && questionId && question.question.state === 'pending'
            && (!question.reply || question.reply.status === 'refused') && presentation
            && (presentation.sessionId ?? current.sessionId) === request.sessionId && first && first.sequence > presentation.sequence
            && (!options.observation || (presentationAccepted(current, presentation) && presentation.inputThroughMs !== undefined
              && first.startMs >= presentation.inputThroughMs && (speechTime() ?? 0) > Date.parse(presentation.acceptedAt)))
            && snapshot.callerRevision > presentation.callerRevision
            && (first.startMs > presentation.lastCallerEndMs || (options.observation && presentation.sequence === 0 && first.startMs === 0))) {
          const text = fragments!.map(fragment => fragment.text).join('');
          if (!validTurnText(text)) throw new Error('invalid_completed_caller_turn');
          let answer: ReturnType<typeof literalPhoneAnswer>;
          try { answer = literalPhoneAnswer(text); }
          catch {
            return save({ ...current, questionNotice: 'answer_too_long', deferredTurns: [...current.deferredTurns ?? [], {
              ...(current.sessionId !== request.sessionId ? { sourceSessionId: request.sessionId } : {}),
              delegationId: request.delegationId, sourceEventId: brokerIdentity('source', options.callId, request.sessionId, first.eventId),
              text, firstSequence: first.sequence, callerRevision: snapshot.callerRevision, status: 'recorded_not_submitted',
            }] });
          }
          let refreshed = current.questions![questionId];
          if (!options.observation) {
            // Re-read this exact question before admitting a reply; another channel may
            // already have answered it or its authority may have changed.
            try {
              current = applyReplyObservation(current, questionId, await options.api.question(options.bindingId, current.requestId, questionId));
            } catch { return save({ ...load(current), questionNotice: 'question_unavailable' }); }
            refreshed = current.questions![questionId];
            if (refreshed.question.state !== 'pending' || Date.parse(refreshed.question.expiresAt) <= now())
              return save({ ...current, questionNotice: 'question_unavailable' });
            if (channel.signal.aborted || boundCall(request.sessionId).endedAt) return current;
          }
          const sourceEventId = brokerIdentity('source', options.callId, request.sessionId, first.eventId);
          const input: PrimaryReplyInput = {
            source: { callSid: options.callSid, itemId: `live-turn-${sourceEventId}`, sourceEventId, revision: 1,
              receivedAt: new Date(mediaOrigin(request.sessionId) + first.startMs).toISOString() },
            role: 'user', text, reply: { questionRevision: 1,
              clientReplyId: brokerIdentity('question-reply', options.callId, request.sessionId, current.requestId, questionId, sourceEventId), ...answer },
          };
          persistSpans();
          const reply: NonNullable<BrokerQuestionRecord['reply']> = { input, inputDigest: digest(input), status: 'prepared',
            ...(options.observation ? { admissionPresentation: structuredClone(presentation) } : {}),
            ...(current.sessionId !== request.sessionId ? { sourceSessionId: request.sessionId } : {}),
            callerRevision: snapshot.callerRevision, firstSequence: first.sequence,
            audioThroughMs,
            lastSequence: Math.max(...fragments!.map(fragment => fragment.sequence)) };
          if (refreshed.reply && refreshed.reply.status !== 'refused') return current;
          current = save({ ...current, questionNotice: undefined, questions: { ...current.questions,
            [questionId]: { ...refreshed, refusedReplies: [...refreshed.refusedReplies ?? [], ...refreshed.reply ? [refreshed.reply] : []], reply } } });
          settleAdmission(request);
          if (options.observation) {
            // The exact proposed source is durable before the read; the server still checks current question admission.
            try {
              current = applyReplyObservation(current, questionId, await options.api.question(options.bindingId, current.requestId, questionId));
            } catch { return save({ ...load(current), questionNotice: 'reply_unknown' }); }
            const observed = current.questions![questionId];
            if (observed.question.state !== 'pending' || Date.parse(observed.question.expiresAt) <= now()
              || observed.reply?.status === 'refused' || observed.reply?.status === 'accepted') return current;
            if (options.observation.signal.aborted || boundCall(request.sessionId).endedAt) return current;
            return dispatchPreparedReply(current, questionId);
          }
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
        const deferredTurns = [...current.deferredTurns ?? []];
        if (fragments?.length) {
          const first = fragments.reduce((a, b) => a.sequence < b.sequence ? a : b);
          const text = fragments.map(fragment => fragment.text).join('');
          if (validTurnText(text)) {
            const sourceEventId = brokerIdentity('source', options.callId, request.sessionId, first.eventId);
            const turn: BrokerDeferredTurn = { delegationId: request.delegationId, sourceEventId, text,
              ...(current.sessionId !== request.sessionId ? { sourceSessionId: request.sessionId } : {}),
              firstSequence: first.sequence, callerRevision: snapshot.callerRevision, status: 'recorded_not_submitted' };
            // Early, expired and overlong question answers must never turn into
            // independent requests when the original run eventually finishes.
            if (current.status !== 'waiting_for_reply') {
              const submission = submissionForTurn(request.sessionId, snapshot, fragments);
              turn.status = 'queued';
              turn.queuedRequest = { requestId: brokerIdentity('request', options.callId, request.sessionId, request.delegationId),
                predecessorRequestId: queuedTurns.at(-1)?.queuedRequest!.requestId ?? current.requestId,
                submission, bodyDigest: digest(submission), lastSequence: Math.max(...fragments.map(fragment => fragment.sequence)),
                audioThroughMs,
                completionBasis: 'speech_and_transcript_stability' };
              persistSpans();
            }
            deferredTurns.push(turn);
          }
        }
        return save({ ...current, deferredTurns });
      }
      if (!fragments?.length) {
        const latest = saved.sort((a, b) => b.lastSequence - a.lastSequence)[0];
        if (!latest) unadmittedReasons.set(request, 'no_unclaimed_completed_caller_turn');
        return latest ? save({ ...latest, delegationIds: [...latest.delegationIds, request.delegationId] }) : undefined;
      }
      const text = fragments.map(fragment => fragment.text).join('');
      if (!validTurnText(text)) throw new Error('invalid_completed_caller_turn');
      const first = fragments.reduce((a, b) => a.sequence < b.sequence ? a : b);
      persistSpans();
      const sourceEventId = brokerIdentity('source', options.callId, request.sessionId, first.eventId);
      const duplicateSource = saved.find(entry => entry.submission.source.sourceEventId === sourceEventId);
      if (duplicateSource) return save({ ...duplicateSource, delegationIds: [...duplicateSource.delegationIds, request.delegationId] });
      let entry: BrokerDelegationRecord = {
        version: 1, requestId: brokerIdentity('request', options.callId, request.sessionId, request.delegationId),
        callId: options.callId, callSid: options.callSid, bindingId: options.bindingId, sessionId: request.sessionId,
        delegationIds: [request.delegationId], status: 'prepared', updatedAt: new Date(now()).toISOString(),
        submission: submissionForTurn(request.sessionId, snapshot, fragments),
        frozenCallerRevision: snapshot.callerRevision, firstSequence: first.sequence,
        audioThroughMs,
        lastSequence: Math.max(...fragments.map(fragment => fragment.sequence)),
        completionBasis: 'speech_and_transcript_stability',
        sourceRevision: 1, capturedText: text,
      };
      entry = save(entry); // This source and exact request survive a crash before POST.
      settleAdmission(request);
      entry = save({ ...entry, status: 'dispatching' });
      try {
        const result = await options.api.submit(options.bindingId, entry.requestId, structuredClone(entry.submission));
        entry = applyResult(entry, result);
      } catch (error) {
        // The server may have admitted the run. Only GET by this request ID may follow.
        entry = await firstSubmitFailure(entry, error);
      }
      return entry;
    });
  };
  const applyResult = (entry: BrokerDelegationRecord, result: PrimaryResult,
    reconciliation?: Extract<PrimaryReconciliation, { outcome: 'admitted' }>): BrokerDelegationRecord => {
    if (result.bindingId !== options.bindingId || result.requestId !== (reconciliation?.canonicalRequestId ?? resultRequestId(entry)))
      throw new Error('primary_result_identity_mismatch');
    if (!['queued', 'running', 'waiting_for_reply', 'completed', 'failed', 'cancelled'].includes(result.state)
        || (result.state === 'completed' && !result.assistant?.text?.trim())) throw new Error('invalid_primary_result');
    let current = load(entry);
    const priorPartial = current.primaryResult?.partialResult;
    if (priorPartial && result.partialResult?.toolInvocationId === priorPartial.toolInvocationId
      && canonicalJson(result.partialResult) !== canonicalJson(priorPartial)) throw new Error('primary_partial_result_changed');
    if (terminal(current.status) && !terminal(result.state)) return current;
    const outcome = (value: PrimaryResult) => JSON.stringify([value.state, value.assistant ?? null, value.error ?? null]);
    if (terminal(current.status) && current.primaryResult && outcome(current.primaryResult) !== outcome(result))
      throw new Error('primary_terminal_result_changed');
    if (result.question) current = observeQuestion(current, result.question);
    if (result.state === 'waiting_for_reply' && !result.question) throw new Error('invalid_primary_result');
    return save({ ...current, status: result.state, primaryResult: structuredClone(result), resultReadUnavailable: false,
      ...(reconciliation ? { reconciliation: structuredClone(reconciliation) } : {}),
      ...(result.question ? { currentQuestionId: result.question.id } : {}),
      ...(result.assistant ? { result: result.assistant.text } : {}) });
  };
  const captureRevision = async (entry: BrokerDelegationRecord, channel: LiveBackendChannel): Promise<BrokerDelegationRecord> => {
    if (options.sourceSessionId && entry.sessionId !== options.sourceSessionId) return load(entry);
    const latest = channel.getContext();
    if (latest.callerRevision <= entry.frozenCallerRevision || channel.signal.aborted) return load(entry);
    const stable = await stableContext(channel);
    if (!stable || channel.signal.aborted) return load(entry);
    return locked(async () => {
      let current = load(entry);
      if (channel.signal.aborted || boundCall(entry.sessionId).endedAt) return current;
      if (current.captureState === 'unknown' || current.captureState === 'dispatching') return current;
      const fragments = completedCallerGroups(stable).find(group => group.fragments.some(fragment => fragment.sequence === current.firstSequence))?.fragments;
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
        const result = await captureTurn(turn);
        if (result.bindingId !== options.bindingId || result.sourceEventId !== turn.sourceEventId || result.revision !== turn.revision || result.recordId !== turn.sourceEventId)
          throw new Error('capture_identity_mismatch');
        return save({ ...current, sourceRevision: turn.revision, capturedText: text, pendingCapture: undefined, captureState: 'accepted' });
      } catch { return save({ ...current, captureState: 'unknown' }); }
    });
  };
  const publishRequest = (entry: BrokerDelegationRecord, channel: LiveBackendChannel, update: LiveBackendUpdate): Promise<LiveDelivery> => {
    if (options.observation) return options.observation.signal.aborted
      ? Promise.resolve({ eventId: '', status: 'closed' }) : options.observation.publish(entry, update).then(value => value.delivery);
    if (channel.signal.aborted) return Promise.resolve({ eventId: '', status: 'closed' });
    const intent = beginBrokerPublication(options.store, { callId: options.callId, callSid: options.callSid,
      bindingId: options.bindingId, sessionId: entry.sessionId }, entry.requestId, update, channel.getContext());
    if (!intent) return channel.publish(update);
    if (!intent.created) return Promise.resolve(intent.record.outcome
      ? { eventId: intent.record.outcome.eventId, status: intent.record.outcome.status }
      : { eventId: '', status: 'unconfirmed' });
    // Attach a synchronous receipt write directly to the existing append promise.
    // Live close settles pending append promises before the host closes the store.
    let pending: Promise<LiveDelivery>;
    try { pending = channel.publish(update); }
    catch { pending = Promise.resolve({ eventId: '', status: 'unconfirmed' }); }
    return pending.then(delivery => {
      settleBrokerPublication(options.store, intent.record, delivery, channel.getContext(), channel.signal.aborted);
      return delivery;
    }, () => {
      const delivery: LiveDelivery = { eventId: '', status: 'unconfirmed' };
      settleBrokerPublication(options.store, intent.record, delivery, channel.getContext(), channel.signal.aborted);
      return delivery;
    });
  };
  const deliverPartial = async (entry: BrokerDelegationRecord, channel: LiveBackendChannel) => {
    const current = load(entry), update = partialResultUpdate(current), partial = current.primaryResult?.partialResult;
    if (!update || !partial) return;
    const previous = current.partialDelivery;
    if (previous?.toolInvocationId === partial.toolInvocationId
      && !['stale', 'rejected'].includes(previous.status)) return;
    save({ ...current, partialDelivery: { toolInvocationId: partial.toolInvocationId, text: partial.text, status: 'dispatching' } });
    // The publication journal also records individual append receipts. Saving intent
    // first prevents a lost response or process exit from replaying the observation.
    let status: LiveDelivery['status'] = 'unconfirmed';
    try { status = (await publishRequest(current, channel, { ...update,
      expectedCallerRevision: channel.getContext().callerRevision })).status; }
    catch { /* A thrown append has an unknown acceptance outcome. */ }
    if (status !== 'accepted' && options.store.get(options.callId)?.publications?.records.some(record =>
      record.requestId === entry.requestId && record.updateId.startsWith(update.id)
      && (!record.outcome || record.outcome.status === 'accepted'))) status = 'unconfirmed';
    save({ ...load(entry), partialDelivery: { toolInvocationId: partial.toolInvocationId, text: partial.text, status } });
  };
  const deliverQuestion = async (entry: BrokerDelegationRecord, channel: LiveBackendChannel) => locked(async () => {
    let current = load(entry);
    const questionId = current.currentQuestionId;
    const record = questionId ? current.questions?.[questionId] : undefined;
    const snapshot = channel.getContext();
    if (channel.signal.aborted) return;
    const prefix = `${entry.requestId}_${questionId ?? 'question'}_${snapshot.callerRevision}`;
    const tell = async (text: string, suffix: string) => {
      const delivery = await publishRequest(entry, channel, { id: `${prefix}_${suffix}`, kind: 'result', speak: true, text,
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
      const delivery = await publishRequest(entry, channel, { id: `${prefix}_shorter_answer`, kind: 'progress', speak: true,
        text: 'Please give me a shorter answer to this question.', expectedCallerRevision: snapshot.callerRevision });
      if (delivery.status !== 'accepted') return;
    }
    if (current.questionNotice === 'reply_refused') {
      const delivery = await publishRequest(entry, channel, { id: `${prefix}_reply_refused`, kind: 'progress', speak: true,
        text: record.reply?.refusal?.code === 'invalid_request'
          ? 'I could not accept that answer. Please say it again.'
          : 'That answer was not accepted. The request is still waiting for a reply.',
        expectedCallerRevision: snapshot.callerRevision });
      if (delivery.status !== 'accepted') return;
    }
    // Commentary appends preserve the question and both meanings, including amounts.
    // Only completion of every append establishes the barrier for a later caller turn.
    for (const [index, chunk] of chunks.entries()) {
      const delivery = await publishRequest(entry, channel, { id: `${prefix}_prompt_${index}`, kind: 'result', text: chunk,
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
    if (options.observation) {
      if (options.observation.signal.aborted || boundCall(entry.sessionId).endedAt) return;
      let state = load(entry);
      const question = state.currentQuestionId ? state.questions?.[state.currentQuestionId] : undefined;
      const tell = async (text: string, suffix: string) => {
        const result = await options.observation!.publish(state, { id: `${entry.requestId}_${state.currentQuestionId ?? 'question'}_${suffix}`,
          kind: 'progress', speak: true, text });
        save({ ...load(entry), delivery: result.delivery.status });
        return result.delivery.status;
      };
      if (state.status === 'admission_refused') {
        await tell(taskAdmissionRefusalText(state), 'admission_refused');
        return;
      }
      if (state.status === 'admission_expired') {
        await tell('The request took too long to confirm. Please tell me what you want me to do now.', 'admission_expired');
        return;
      }
      if (Object.values(state.questions ?? {}).some(value => unresolvedReply(value.reply))) {
        await tell('I am checking whether your answer was saved. I will use that saved answer when it is confirmed.', 'reply_unknown');
        return;
      }
      if (state.status === 'waiting_for_reply') {
        if (!question || question.question.state !== 'pending' || Date.parse(question.question.expiresAt) <= now()
          || state.questionNotice === 'question_unavailable') {
          await tell(question?.question.state === 'answered'
            ? 'This question has already been answered. I am checking how the request continued.'
            : 'This question is no longer available to answer. I can check the request status.', 'unavailable');
          return;
        }
        if (state.questionNotice === 'answer_too_long'
          && await tell('Please give me a shorter answer to this question.', 'shorter_answer') !== 'accepted') return;
        if (state.questionNotice === 'reply_refused' && await tell(question.reply?.refusal?.code === 'invalid_request'
          ? 'I could not accept that answer. Please say it again.'
          : 'That answer was not accepted. The request is still waiting for a reply.', 'reply_refused') !== 'accepted') return;
      }
      const receipt = await options.observation.publish(load(entry));
      let current = load(entry);
      if (options.observation.signal.aborted || boundCall(entry.sessionId).endedAt) return;
      current = save({ ...current, delivery: receipt.delivery.reason === 'result_format_unsupported'
        ? `format_unsupported:${receipt.delivery.status}` : receipt.delivery.status });
      const questionId = current.currentQuestionId;
      const deliveredQuestion = questionId ? current.questions?.[questionId] : undefined;
      const snapshot = receipt.snapshot;
      if (receipt.delivery.status === 'accepted' && receipt.sessionId && snapshot && questionId && deliveredQuestion
        && Number.isFinite(receipt.inputThroughMs) && receipt.inputThroughMs! >= 0
        && current.status === 'waiting_for_reply' && deliveredQuestion.question.state === 'pending'
        && !Object.values(current.questions ?? {}).some(value => unresolvedReply(value.reply))
        && boundCall(entry.sessionId).liveSessionId === receipt.sessionId
        && brokerPublicationReceiptsAccepted(boundCall(entry.sessionId), entry.requestId, receipt.sessionId, receipt.publicationReceipts)
        && receipt.delivery.observedCallerRevision === snapshot.callerRevision
        && receipt.delivery.observedTranscriptSequence === Math.max(0, ...snapshot.fragments.map(fragment => fragment.sequence))) {
        save({ ...current, questionNotice: undefined, questions: { ...current.questions,
          [questionId]: { ...deliveredQuestion, presentation: { sessionId: receipt.sessionId,
            inputThroughMs: receipt.inputThroughMs, publicationReceipts: structuredClone(receipt.publicationReceipts!),
            callerRevision: snapshot.callerRevision, sequence: receipt.delivery.observedTranscriptSequence,
            lastCallerEndMs: Math.max(0, ...snapshot.fragments.filter(fragment => fragment.role === 'user').map(fragment => fragment.endMs)),
            acceptedAt: new Date(now()).toISOString() } } } });
      }
      return;
    }
    if (channel.signal.aborted || boundCall(entry.sessionId).endedAt) return;
    if (entry.status === 'waiting_for_reply' || Object.values(entry.questions ?? {}).some(record => unresolvedReply(record.reply))) {
      await deliverQuestion(entry, channel); return;
    }
    const lastSpeech = speechTime();
    if (lastSpeech !== undefined && now() - lastSpeech < stabilityMs && !await stableContext(channel)) return;
    if (channel.signal.aborted || boundCall(entry.sessionId).endedAt) return;
    const current = channel.getContext();
    const spokenAt = speechTime();
    const interrupted = (): 'closed' | 'stale' | undefined => {
      if (channel.signal.aborted || boundCall(entry.sessionId).endedAt) return 'closed';
      if (channel.getContext().callerRevision !== current.callerRevision || speechTime() !== spokenAt) return 'stale';
      return undefined;
    };
    const reviewedRevision = Math.max(entry.frozenCallerRevision, ...Object.values(entry.questions ?? {})
      .filter(record => record.reply?.status === 'accepted').map(record => record.reply!.callerRevision));
    const changed = current.callerRevision !== reviewedRevision;
    let text: string;
    if (entry.status === 'completed') text = entry.primaryResult!.assistant!.text;
    else if (entry.status === 'admission_refused') text = taskAdmissionRefusalText(entry);
    else if (entry.status === 'admission_expired') text = 'The request took too long to confirm. Please tell me what you want me to do now.';
    else if (entry.status === 'failed') text = failedRequestSpeech(entry);
    else if (entry.status === 'cancelled') text = 'The request was cancelled.';
    else if (['dispatching', 'dispatch_unknown', 'prepared'].includes(entry.status))
      text = 'I could not confirm the outcome. Please check the request status before trying again.';
    else text = 'Your request is still pending. I can check its status again.';
    if (changed) text = `For your earlier request: ${text}`;
    const prefix = `${entry.requestId}_${current.callerRevision}`;
    let chunks: string[];
    try { chunks = liveResultChunks(text); }
    catch {
      const outcome = await publishRequest(entry, channel, { id: `${prefix}_format_unavailable`, kind: 'result', speak: true,
        text: 'I received the result, but cannot read that format accurately over this call. The full result has been saved.',
        expectedCallerRevision: current.callerRevision });
      save({ ...load(entry), delivery: `format_unsupported:${interrupted() ?? outcome.status}` });
      return;
    }
    // A caller interjection changes the presentation context, not the saved result.
    // Send every chunk as commentary while preserving the original request scope.
    save({ ...load(entry), delivery: 'dispatching' });
    for (const [index, chunk] of chunks.entries()) {
      const stopped = interrupted();
      if (stopped) { save({ ...load(entry), delivery: stopped }); return; }
      const outcome = await publishRequest(entry, channel, { id: `${prefix}_${entry.status}_result_${index}`, kind: 'result',
        speak: true, text: chunk, expectedCallerRevision: current.callerRevision });
      const status = interrupted() ?? outcome.status;
      if (status !== 'accepted') { save({ ...load(entry), delivery: status }); return; }
    }
    save({ ...load(entry), delivery: 'accepted' });
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
      + delegated.filter(entry => ['admission_refused', 'admission_expired'].includes(entry.status) && entry.originalCapture?.state !== 'accepted').length
      + delegated.filter(entry => entry.pendingCapture).length
      + delegated.reduce((count, entry) => count + Object.values(entry.questions ?? {}).filter(record => unresolvedReply(record.reply)).length, 0)
      + Object.values(boundCall(sessionId).canonicalTurns ?? {}).filter(value => {
        const turn = value as CanonicalTurnRecord;
        return (turn.sourceSessionId === undefined || turn.sourceSessionId === sessionId) && turn.state !== 'captured';
      }).length
      + selectedSources(sessionId).filter(source => !source.capture || source.replyPending).length;
  };
  const resultRequestId = (entry: BrokerDelegationRecord) => entry.reconciliation?.outcome === 'admitted'
    ? entry.reconciliation.canonicalRequestId : entry.requestId;
  const eligibleForClosedRecovery = (sessionId: string) => {
    const call = boundCall(sessionId);
    const segment = call.liveGenerations?.segments.find(value => value.sessionId === sessionId);
    return !!call.endedAt && terminalCallStatuses.has(call.status)
      && (segment ? segment.providerFinalized === true && segment.transcriptPersisted === true : call.transcriptCapture?.sessionFinalized === true);
  };
  const originalAcknowledged = (entry: BrokerDelegationRecord): boolean => {
    if (entry.originalCapture?.state !== 'accepted' || !entry.originalCapture.receipt) return false;
    acknowledgedTurn(options.bindingId, originalTurn(entry.submission), entry.originalCapture.receipt);
    return true;
  };
  const sourceReady = (entry: BrokerDelegationRecord) => !admissionUnknown(entry.status)
    || (eligibleForClosedRecovery(entry.sessionId) && originalAcknowledged(entry));
  const currentSession = () => {
    const sessionId = options.sourceSessionId ?? options.store.get(options.callId)?.liveSessionId;
    if (!sessionId) throw new Error('missing_live_session');
    boundCall(sessionId);
    return sessionId;
  };
  const promoteDeferred = (sessionId: string, channel: LiveBackendChannel) => locked(async (): Promise<BrokerDelegationRecord | undefined> => {
    const call = boundCall(sessionId);
    if (channel.signal.aborted || call.endedAt || call.primaryClosed || call.primaryClosePending
        || terminalCallStatuses.has(call.status)) return undefined;
    const saved = entries(sessionId, Boolean(options.observation));
    const active = saved.find(entry => !terminal(entry.status));
    if (active) return active;
    const pending = saved.flatMap(owner => (owner.deferredTurns ?? []).filter(turn => turn.status === 'queued')
      .map(turn => ({ owner, turn }))).sort((a, b) => sessionOrder(a.turn.sourceSessionId ?? a.owner.sessionId)
        - sessionOrder(b.turn.sourceSessionId ?? b.owner.sessionId) || a.turn.firstSequence - b.turn.firstSequence);
    const next = pending[0];
    if (!next) return undefined;
    const frozen = next.turn.queuedRequest!;
    const predecessor = saved.find(entry => entry.requestId === frozen.predecessorRequestId);
    if (!predecessor || !terminal(predecessor.status)) return undefined;
    if (saved.some(entry => entry.requestId === frozen.requestId
        || entry.submission.source.sourceEventId === next.turn.sourceEventId)) throw new Error('deferred_source_already_admitted');
    const timestamp = new Date(now()).toISOString();
    let child: BrokerDelegationRecord = {
      version: 1, requestId: frozen.requestId, predecessorRequestId: frozen.predecessorRequestId,
      callId: options.callId, callSid: options.callSid, bindingId: options.bindingId, sessionId: next.turn.sourceSessionId ?? next.owner.sessionId,
      delegationIds: [next.turn.delegationId], status: 'prepared', updatedAt: timestamp,
      submission: structuredClone(frozen.submission), frozenCallerRevision: next.turn.callerRevision,
      firstSequence: next.turn.firstSequence, lastSequence: frozen.lastSequence,
      ...(frozen.audioThroughMs === undefined ? {} : { audioThroughMs: frozen.audioThroughMs }),
      completionBasis: frozen.completionBasis, sourceRevision: 1, capturedText: next.turn.text,
    };
    // Publish parent disposition and the exact child journal in one durable write.
    // A crash after this point recovers the child by GET, never a fresh POST.
    const owner = { ...next.owner, updatedAt: timestamp, deferredTurns: next.owner.deferredTurns!.map(turn =>
      turn.sourceEventId === next.turn.sourceEventId ? { ...turn, status: 'promoted' as const } : turn) };
    options.store.update(call.id, { delegations: { ...call.delegations, [owner.requestId]: owner, [child.requestId]: child } });
    return dispatchPreparedTask(child);
  });
  const observeDelegation = async (request: LiveDelegationRequest, channel: LiveBackendChannel,
    admission: () => Promise<BrokerDelegationRecord | undefined>): Promise<void> => {
      let entry: BrokerDelegationRecord | undefined;
      try { entry = await admission(); }
      finally { settleAdmission(request); }
      const observationSignal = options.observation?.signal ?? channel.signal;
      if (observationSignal.aborted) return;
      if (!entry) {
        console.info(JSON.stringify({ event: 'broker_delegation_unadmitted', callId: options.callId,
          sessionId: request.sessionId, delegationId: request.delegationId,
          reason: unadmittedReasons.get(request) ?? 'no_qualified_caller_source' }));
        await channel.publish({ id: `${request.requestId}_unadmitted`, kind: 'progress', speak: false,
          text: 'No new completed caller turn was admitted by this delegation. Continue listening and preserve the existing task. This internal observation does not mean the caller must finish or repeat their request.',
          expectedCallerRevision: channel.getContext().callerRevision });
        return;
      }
      // The active observer owns status speech; queue admission can wake it
      // without waiting for the earlier result GET or starting the queued task.
      notices!.get(`${options.callId}:${entry.requestId}`)?.wake?.();
      const workerKey = options.observation ? options.callId : `${options.callId}:${request.sessionId}`;
      while (workers!.has(workerKey)) {
        await workers!.get(workerKey);
        if (observationSignal.aborted || boundCall(request.sessionId).endedAt) return;
        const saved = entries(request.sessionId, Boolean(options.observation));
        entry = saved.find(value => !terminal(value.status)) ?? load(entry);
        if (terminal(entry.status)
            && !saved.some(value => value.deferredTurns?.some(turn => turn.status === 'queued'))) return;
      }
      const run = async () => {
        let lastDeferredResult: string | undefined;
        const deferredResult = (exceptRequestId?: string) => {
          if (!options.observation || !options.watchUntilCallEnds || observationSignal.aborted
              || boundCall(request.sessionId).endedAt) return;
          const pending = entries(request.sessionId, true).filter(value => value.requestId !== exceptRequestId
            && ['completed', 'failed', 'cancelled'].includes(value.status)
            && value.primaryResult?.state === value.status && value.delivery === 'stale');
          if (!pending.length) return;
          const last = pending.findIndex(value => value.requestId === lastDeferredResult);
          const next = pending[(last + 1) % pending.length];
          if (next) lastDeferredResult = next.requestId;
          return next;
        };
        while (!observationSignal.aborted) {
          entry = load(entry!);
          const alreadyDelivered = terminal(entry.status) && entry.delivery === 'accepted';
          if (!alreadyDelivered) {
            const deadline = now() + maxPollMs;
            const noticeKey = `${options.callId}:${entry.requestId}`;
            let notice = notices!.get(noticeKey);
            if (!notice) {
              notice = { submitAnnounced: false, progressCount: 0, lastAt: now(), inFlight: false };
              notices!.set(noticeKey, notice);
            }
            const publishStatus = (immediate: boolean) => {
              if (observationSignal.aborted || options.store.get(options.callId)?.endedAt || notice!.inFlight) return;
              const current = load(entry!);
              if (terminal(current.status) || current.status === 'waiting_for_reply'
                  || Object.values(current.questions ?? {}).some(record => unresolvedReply(record.reply))) return;
              const reviewedRevision = Math.max(current.frozenCallerRevision, ...Object.values(current.questions ?? {})
                .filter(record => record.reply?.status === 'accepted').map(record => record.reply!.callerRevision));
              const revision = channel.getContext().callerRevision;
              const lastSpeech = speechTime();
              if (lastSpeech !== undefined && now() - lastSpeech < stabilityMs) return;
              const stateKey = JSON.stringify([pendingRequestState(current), revision]);
              const changed = notice!.stateKey !== undefined && notice!.stateKey !== stateKey;
              const text = pendingRequestSpeech(current, revision !== reviewedRevision);
              // Caller speech and queued turns are not news for the caller; only an unreadable status is.
              const urgent = (changed || notice!.stateKey === undefined) && current.resultReadUnavailable === true;
              const announceSubmit = immediate && current.status === 'dispatch_unknown' && !notice!.submitAnnounced;
              const retryAfter = notice!.deferred ? Math.min(progressIntervalMs, 1000) : progressIntervalMs;
              if (notice!.uncertainKey === stateKey) return; // An unknown append is not authority to speak it again.
              if (!announceSubmit && !urgent && (immediate || now() - notice!.lastAt < retryAfter)) return;
              if (!options.watchUntilCallEnds && now() >= deadline) return;
              if (changed) notice!.deferred = false;
              // Speak each distinct status line once; repeated waiting lines only interrupt the caller.
              if (text !== notice!.spokenText) notice!.progressCount = 0;
              if (notice!.progressCount >= 1) return;
              notice!.stateKey = stateKey;
              notice!.lastAt = now();
              notice!.inFlight = true;
              const attempt = notice!.attempt = (notice!.attempt ?? 0) + 1;
              // Receipt acceptance consumes the budget. Deferred speech remains
              // eligible at a later quiet boundary, without blocking result reads.
              void Promise.resolve().then(() => publishRequest(current, channel, { id: `${current.requestId}_${revision}_${announceSubmit ? 'submit_unknown' : `pending_${attempt}`}`,
                kind: 'progress', speak: true, text, expectedCallerRevision: revision }))
                .then(delivery => {
                  if (delivery.status === 'accepted') {
                    if (announceSubmit) notice!.submitAnnounced = true;
                    else notice!.progressCount++;
                    notice!.spokenText = text;
                    notice!.deferred = false;
                  } else if (delivery.status === 'unconfirmed') notice!.uncertainKey = stateKey;
                  else notice!.deferred = delivery.status === 'stale' || delivery.status === 'rejected';
                }, () => { notice!.uncertainKey = stateKey; })
                .finally(() => { notice!.inFlight = false; });
            };
            notice.wake = () => publishStatus(false);
            publishStatus(true);
            const progressTimer = setInterval(() => publishStatus(false), progressIntervalMs);
            progressTimer.unref();
            // Preserve caller changes in the local journal immediately. Only stable snapshots
            // go to the source-revision API; revisions never create another run here.
            const unsubscribe = channel.subscribeContext(snapshot => {
              if (entry!.sessionId === request.sessionId && !channel.signal.aborted && snapshot.callerRevision > entry!.frozenCallerRevision) {
                const saved = load(entry!);
                const original = completedCallerGroups(snapshot).find(group => group.fragments.some(fragment => fragment.sequence === saved.firstSequence));
                if (original && original.fragments.map(fragment => fragment.text).join('') !== saved.capturedText) {
                  save({ ...saved, laterCallerRevision: snapshot.callerRevision, correctionState: 'recorded_not_applied',
                    lastSequence: Math.max(saved.lastSequence, ...original.fragments.map(f => f.sequence)) });
                }
              }
            });
            try {
              while (!observationSignal.aborted) {
                entry = load(entry);
                if (Object.values(entry.questions ?? {}).some(record => unresolvedReply(record.reply)))
                  entry = await locked(() => recoverReplies(entry!,
                    () => observationSignal.aborted || Boolean(boundCall(entry!.sessionId).endedAt)));
                if (observationSignal.aborted || boundCall(entry.sessionId).endedAt) return;
                if (!terminal(entry.status)) {
                  try { entry = applyResult(entry, await options.api.result(options.bindingId, resultRequestId(entry))); }
                  catch (error) { entry = recordReadError(entry, error); /* GET errors never authorize another POST. */ }
                }
                if (entry.sessionId === request.sessionId && !channel.signal.aborted) entry = await captureRevision(entry, channel);
                if (observationSignal.aborted) return;
                if (terminal(entry.status)) break;
                await deliverPartial(entry, channel);
                publishStatus(false);
                // A terminal answer can wait across later work without replaying
                // that work. Questions keep control of their own reply boundary.
                if (entry.status !== 'waiting_for_reply') {
                  const deferred = deferredResult(entry.requestId);
                  if (deferred) await deliver(deferred, channel);
                }
                if (entry.status === 'waiting_for_reply') {
                  if (!options.watchUntilCallEnds) break;
                  const questionRecord = entry.currentQuestionId ? entry.questions?.[entry.currentQuestionId] : undefined;
                  if (!questionRecord?.presentation
                      || (options.observation && !presentationAccepted(entry, questionRecord.presentation))
                      || (questionRecord.presentation.sessionId ?? entry.sessionId) !== boundCall(entry.sessionId).liveSessionId || entry.questionNotice
                      || Object.values(entry.questions ?? {}).some(record => unresolvedReply(record.reply)))
                    await deliver(load(entry), channel);
                }
                if (now() >= deadline && !options.watchUntilCallEnds) break;
                if (!(await delay(pollIntervalMs, observationSignal))) return;
              }
              await deliver(load(entry), channel);
            } finally { notice.wake = undefined; clearInterval(progressTimer); unsubscribe(); }
          }
          const next = await promoteDeferred(entry.sessionId, { ...channel, signal: observationSignal });
          if (next && next.requestId !== entry.requestId) { entry = next; continue; }
          const deferred = deferredResult();
          if (!deferred || !await delay(pollIntervalMs, observationSignal)) return;
          entry = deferred;
        }
      };
      const work = run();
      workers!.set(workerKey, work);
      try { await work; }
      finally { if (workers!.get(workerKey) === work) workers!.delete(workerKey); }
  };
  return {
    freezeCallerTurn,
    assertFrozenCallerSpeechCurrent: assertFrozenSpeech,
    observeTranscript(snapshot) {
      if (options.store.get(options.callId)?.endedAt) return;
      if (observedTranscript?.callerRevision === snapshot.callerRevision) return;
      observedTranscript = structuredClone(snapshot);
      transcriptChangedAt = now();
      scheduleCallerCompletion();
    },
    async prepareCloseout() {
      const sessionId = currentSession();
      if (!eligibleForClosedRecovery(sessionId) || !options.api.reconcile) return;
      await locked(async () => {
        for (let entry of entries(sessionId)) {
          if (['admission_refused', 'admission_expired'].includes(entry.status) && entry.originalCapture?.state !== 'accepted')
            entry = await captureUnadmittedSubmission(entry);
          if (!admissionUnknown(entry.status)) continue;
          try { applyResult(entry, await options.api.result(options.bindingId, resultRequestId(entry))); continue; }
          catch (error) { recordReadError(entry, error); }
          entry = load(entry);
          if (originalAcknowledged(entry)) continue;
          const turn = originalTurn(entry.submission);
          entry = save({ ...entry, originalCapture: { turn, state: 'dispatching' } });
          try {
            const receipt = acknowledgedTurn(options.bindingId, turn, await captureTurn(turn));
            save({ ...load(entry), originalCapture: { turn, state: 'accepted', receipt } });
          } catch { save({ ...load(entry), originalCapture: { turn, state: 'unknown' } }); }
        }
      });
    },
    closeoutPending() {
      const sessionId = currentSession();
      return pendingCount(sessionId) - entries(sessionId).filter(entry => admissionUnknown(entry.status)
        && !!options.api.reconcile && sourceReady(entry)).length;
    },
    async reconcileClosedRequests(closed) {
      const sessionId = currentSession();
      const savedClosed = boundCall(sessionId).primaryClosed;
      if (closed.bindingId !== options.bindingId || closed.callSid !== options.callSid || closed.state !== 'closed'
          || savedClosed?.bindingId !== closed.bindingId || savedClosed.callSid !== closed.callSid
          || savedClosed.state !== 'closed') throw new Error('closed_call_identity_mismatch');
      await locked(async () => {
        for (let entry of entries(sessionId)) {
          if (!admissionUnknown(entry.status)) continue;
          if (!eligibleForClosedRecovery(sessionId) || !originalAcknowledged(entry) || !options.api.reconcile) continue;
          try {
            const reconciliation = await options.api.reconcile(options.bindingId, entry.requestId, structuredClone(entry.submission));
            if (reconciliation.bindingId !== options.bindingId || reconciliation.requestId !== entry.requestId)
              throw new Error('reconciliation_identity_mismatch');
            if (reconciliation.outcome === 'admitted') {
              if (!uuidPattern.test(reconciliation.canonicalRequestId) || reconciliation.result.requestId !== reconciliation.canonicalRequestId)
                throw new Error('reconciliation_identity_mismatch');
              // Read identity and actual result commit together; the original submission stays untouched.
              applyResult(entry, reconciliation.result, reconciliation);
            } else {
              validateNonAdmissionProof(options.bindingId, entry.submission, entry.originalCapture!.receipt!, closed, reconciliation.proof);
              save({ ...load(entry), status: 'not_admitted', reconciliation: structuredClone(reconciliation) });
            }
          } catch (error) { save({ ...load(entry), reconciliationError: diagnostic(error) }); }
        }
      });
      return { pending: pendingCount(sessionId) };
    },
    persistAcousticEvidence() {
      const sessionId = currentSession();
      persistSpans();
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
          if (['admission_refused', 'admission_expired'].includes(entry.status) && entry.originalCapture?.state !== 'accepted')
            entry = await captureUnadmittedSubmission(entry);
          if (['prepared', 'dispatching', 'dispatch_unknown'].includes(entry.status)) {
            try { entry = applyResult(entry, await options.api.result(options.bindingId, resultRequestId(entry))); }
            catch (error) { recordReadError(entry, error); unsupported.push('source_admission_unknown'); if (!sourceReady(entry)) continue; }
          }
          entry = await recoverReplies(entry);
          if (Object.values(entry.questions ?? {}).some(record => unresolvedReply(record.reply))) unsupported.push('reply_outcome_unknown');
          if (!entry.pendingCapture) continue;
          try {
            const turn = canonicalPayload(entry.pendingCapture);
            const receipt = await captureTurn(turn);
            if (receipt.bindingId !== options.bindingId || receipt.sourceEventId !== turn.sourceEventId
                || receipt.revision !== turn.revision || receipt.recordId !== turn.sourceEventId) throw new Error('capture_identity_mismatch');
            save({ ...load(entry), sourceRevision: turn.revision, capturedText: turn.text,
              pendingCapture: undefined, captureState: 'accepted' });
          } catch { save({ ...load(entry), captureState: 'unknown' }); unsupported.push('capture_outcome_unknown'); }
        }
        for (const value of Object.values(boundCall(sessionId).canonicalTurns ?? {})) {
          const turn = value as CanonicalTurnRecord;
          if (turn.sourceSessionId !== undefined && turn.sourceSessionId !== sessionId) continue;
          if (turn.state === 'captured') continue;
          try {
            const payload = canonicalPayload(turn);
            const delegated = entries(sessionId).find(entry => entry.submission.source.sourceEventId === turn.sourceEventId);
            const reply = knownReply(sessionId, turn.sourceEventId);
            if (unresolvedReply(reply?.reply)) { unsupported.push('reply_outcome_unknown'); continue; }
            if (delegated?.pendingCapture || (delegated && !sourceReady(delegated))) {
              unsupported.push('source_admission_unknown'); continue;
            }
            const receipt = await captureTurn(payload);
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
      const sourceSegment = call.liveGenerations?.segments.find(segment => segment.sessionId === sessionId);
      const sourceClosedAt = sourceSegment?.retiringAt ?? sourceSegment?.closedAt;
      const observedAt = Math.min(now(), call.endedAt ? Date.parse(call.endedAt) : Infinity,
        sourceClosedAt ? Date.parse(sourceClosedAt) : Infinity);
      persistSpans();
      const groups: Array<{ role: 'user' | 'assistant'; fragments: LiveTranscriptSnapshot['fragments'] }> =
        completedCallerGroups(snapshot, observedAt).map(group => ({ role: 'user', fragments: group.fragments }));
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
          const selected = selectedSources(sessionId).find(source => source.frozen.turn.sourceEventId === sourceEventId);
          if (unresolvedReply(reply?.reply)) return 'reply_outcome_unknown';
          if (selected?.replyPending) return 'reply_outcome_unknown';
          if (selected && !selected.capture) return 'capture_outcome_unknown';
          if (delegated && !sourceReady(delegated)) return 'source_admission_unknown';
          if (delegated?.pendingCapture) return 'capture_outcome_unknown';
          const previous = current.canonicalTurns?.[sourceEventId] as CanonicalTurnRecord | undefined;
          if (previous && previous.state !== 'captured') return 'capture_outcome_unknown';
          if ((previous?.text ?? delegated?.capturedText ?? selected?.frozen.turn.text) === text) return 'already_captured';
          const turn: CanonicalTurnRecord = {
            sourceEventId, revision: (previous?.revision ?? delegated?.sourceRevision ?? selected?.frozen.turn.revision ?? 0) + 1,
            itemId: delegated?.submission.source.itemId ?? selected?.frozen.turn.itemId ?? `live-turn-${sourceEventId}`,
            role: group.role, text, occurredAt: previous?.occurredAt ?? delegated?.submission.source.receivedAt
              ?? selected?.frozen.turn.occurredAt
              ?? new Date(mediaOrigin(sessionId) + first.startMs).toISOString(),
            state: 'dispatching', ...(options.sourceSessionId ? { sourceSessionId: sessionId } : {}), ...(previous?.acousticSpan ? { acousticSpan: { ...previous.acousticSpan } } : {}),
          };
          const persist = (value: CanonicalTurnRecord) => persistCanonical(sessionId, value);
          persist(turn);
          try {
            const receipt = await captureTurn({
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
    delegate(request, channel) {
      if (options.selectedQuestionSources) return Promise.reject(new Error('selected_question_requires_answer_port'));
      if (options.callerRouteSources) return Promise.reject(new Error('explicit_caller_route_required'));
      return observeDelegation(request, channel, () => accept(request, channel));
    },
    async deliverFrozenResult(request,ref,channel) {
      let receipt:PhoneResultSmsAdmissionReceipt|undefined,error:string|undefined;
      try {
        receipt=await locked(async()=>{
          if(!options.api.admitResultSms)throw new Error('sms_delivery_pending');
          const route=requireCallerRoute(boundCall(request.sessionId),request,ref,'deliver_result');
          if(route.decision?.kind!=='deliver_result' || route.intent?.response?.v!=='phone.caller-intent.v3')
            throw new Error('caller_route_intent_decision_changed');
          assertFrozenSpeech(route.frozen);
          if(channel.signal.aborted || options.observation?.signal.aborted)return;
          if(!route.capture) {
            const captured=await options.api.captureTurn(options.bindingId,structuredClone(route.frozen.turn));
            acknowledgedTurn(options.bindingId,route.frozen.turn,captured);
            options.callerRouteCaptureAccepted?.(route.frozen.turn.sourceEventId,captured);
          }
          requireCallerRoute(boundCall(request.sessionId),request,ref,'deliver_result');
          assertFrozenSpeech(route.frozen);
          return options.api.admitResultSms(options.bindingId,{v:'phone.result-sms.v1',
            callerIntentOperationId:route.intent.operationId,requestId:route.decision.target.requestId});
        });
      } catch(cause) { error=cause instanceof PrimaryApiError ? cause.remoteCode : undefined; }
      finally { settleAdmission(request); }
      if(!channel.signal.aborted && !boundCall(request.sessionId).endedAt)
        await channel.publish({id:`${request.requestId}_sms_admission`,kind:'result',
          text:resultSmsAcknowledgement(receipt,error),expectedCallerRevision:channel.getContext().callerRevision});
    },
    async delegateFrozenTask(request, route, channel) {
      const record=requireCallerRoute(boundCall(request.sessionId),request,route,'task','recover');
      if(record.decision?.kind==='task' && record.decision.delivery) {
        let entry:BrokerDelegationRecord|undefined;
        try { entry=await acceptFrozenTask(request,route,channel); }
        finally { settleAdmission(request); }
        if (entry && ['dispatching','dispatch_unknown'].includes(entry.status) && entry.submission.resultSms && entry.timing?.firstDispatchAt
          && !entry.resultSmsAdmission && options.api.admitResultSms) {
          const exact=entry;
          try {
            const receipt=await options.api.admitResultSms(options.bindingId,structuredClone(entry.submission.resultSms));
            entry=await locked(async()=>{
              const current=load(exact);
              if(!['dispatching','dispatch_unknown'].includes(current.status) || !current.timing?.firstDispatchAt) return current;
              return save({...current,resultSmsAdmission:phoneResultSmsAdmissionReceiptSchema.parse(receipt)});
            });
          } catch { /* The original task and SMS receipt remain uncertain; never submit new identities. */ }
        }
        // API's durable queue owns this explicit deferred-result request. It
        // must be admitted before hangup; an existing voice observer continues
        // on its original request and is never replaced by this acknowledgement.
        if(!channel.signal.aborted && !boundCall(request.sessionId).endedAt)
          await channel.publish({id:`${request.requestId}_sms_admission`,kind:'result',
            text:entry?.admissionRefusal ? taskAdmissionRefusalText(entry) : resultSmsAcknowledgement(entry?.primaryResult?.resultSms ?? entry?.resultSmsAdmission,entry?.submitError?.remoteCode),
            expectedCallerRevision:channel.getContext().callerRevision});
        return;
      }
      return observeDelegation(request, channel, () => acceptFrozenTask(request, route, channel));
    },
    answerFrozenQuestion(request, route, channel) {
      return observeDelegation(request, channel, () => acceptFrozenAnswer(request, route, channel));
    },
    questionTargets,
  };
}
