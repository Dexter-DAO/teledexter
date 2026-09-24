import { createHash } from 'node:crypto';
import { z } from 'zod';
import { brokerIdentity, literalPhoneAnswer, liveResultChunks, type SelectedQuestionSourceAnchor } from './backend.js';
import { canonicalJson } from '../outbound/canonical.js';
import { eligibleAssociatedDeliveryResponseSchema, type EligibleDeliveryTarget } from './eligibleContext.js';
import { selectedQuestionPublicationReceiptsAccepted } from './publications.js';
import { acknowledgedTurn } from './reconciliation.js';
import { parseRequestDeliveryJournal, type RequestDeliveryClient, type RequestDeliveryJournal } from './requestDelivery.js';
import { createSelectedQuestionHost, type SelectedQuestionAnswer, type SelectedQuestionHostOptions,
  type SelectedQuestionJournal, type SelectedQuestionSeed } from './selectedQuestion.js';
import { PrimaryApiError, replyInputSchema, type PrimaryApiClient, type PrimaryTurn, type PrimaryTurnReceipt } from './primaryApi.js';
import type { GenerationDeliveryOutcome } from './liveGeneration.js';
import { terminalCallStatuses, type BrokerCall, type BrokerStore } from './store.js';
import type { LiveBackendChannel, LiveDelegationRequest, LiveTranscriptSnapshot } from '../live/types.js';
import { frozenCallerTurnSchema as frozenSchema, type CallerTurnFreezer, type FrozenCallerTurn } from './callerTurn.js';
import { requireCallerRoute, requireCallerRouteDispatch, CallerRouteExpiredError, getCallerRouteBySource, callerRoutePresentationDigest,
  type CallerQuestionTarget, type CallerRouteRef } from './callerRouting.js';

const uuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
const nonnegative = z.number().finite().nonnegative();
const sequence = z.number().int().safe().nonnegative();
const time = z.iso.datetime({ offset: true });
const same = (a: unknown, b: unknown) => canonicalJson(a) === canonicalJson(b);
const textDigest = (text: string) => createHash('sha256').update(text, 'utf8').digest('hex');
function fail(code: string): never { throw new Error(code); }

/** Compatibility aliases; both admission paths use the same raw-source schema. */
export type FrozenSelectedCallerTurn = FrozenCallerTurn;
export type SelectedCallerTurnFreezer = CallerTurnFreezer;
const publicationSchema = z.object({ publicationSequence: sequence.positive(), sessionId: z.string().min(1).max(200),
  eventId: z.string().regex(/^[a-zA-Z0-9_.:-]{1,200}$/) }).strict();
const presentationSchema = z.object({ id: uuid, associationId: uuid, generationId: uuid,
  sessionId: z.string().min(1).max(200), response: eligibleAssociatedDeliveryResponseSchema,
  callerRevision: sequence, transcriptSequence: sequence, lastCallerEndMs: nonnegative,
  inputThroughMs: nonnegative, acceptedAt: time, publicationReceipts: z.array(publicationSchema).min(1),
}).strict();
export type SelectedQuestionVoicePresentation = z.infer<typeof presentationSchema>;
const answerSchema = z.object({ questionId: uuid, input: replyInputSchema,
  sourceSessionId: z.string().min(1).max(200), delegationId: z.string().min(1).max(200),
  presentationId: z.string().min(1).max(200) }).strict();
const captureSchema = z.object({ bindingId: uuid, sourceEventId: uuid, revision: z.literal(1), recordId: uuid,
  revisionRecordId: uuid, bodyDigest: z.string().regex(/^[0-9a-f]{64}$/), replayed: z.boolean() }).strict();
const sourceEvidenceSchema = z.object({ answer: answerSchema, frozen: frozenSchema,
  delegationRequestId: z.string().min(1).max(200), capture: captureSchema.optional() }).strict();
export interface SelectedQuestionSourceEvidence {
  answer: SelectedQuestionAnswer;
  frozen: FrozenSelectedCallerTurn;
  delegationRequestId: string;
  capture?: PrimaryTurnReceipt;
}
export interface SelectedQuestionVoiceEvidence {
  v: 'broker.selected-question-voice.v1';
  presentations: SelectedQuestionVoicePresentation[];
  sources: SelectedQuestionSourceEvidence[];
}

function followsPresentation(frozen: FrozenSelectedCallerTurn, presentation: SelectedQuestionVoicePresentation): boolean {
  return frozen.sessionId === presentation.sessionId
    && frozen.firstSequence > presentation.transcriptSequence && frozen.callerRevision > presentation.callerRevision
    && frozen.startMs >= presentation.inputThroughMs && frozen.observedSpeechAtMs > Date.parse(presentation.acceptedAt)
    && (frozen.startMs > presentation.lastCallerEndMs || (presentation.transcriptSequence === 0 && frozen.startMs === 0));
}

/** Stored as an additive field on the same Broker call, never a separate store. */
export function parseSelectedQuestionVoiceEvidence(value: unknown): SelectedQuestionVoiceEvidence {
  const parsed = z.object({ v: z.literal('broker.selected-question-voice.v1'),
    presentations: z.array(presentationSchema), sources: z.array(sourceEvidenceSchema) }).strict().parse(value);
  const ids = new Set<string>(), sources = new Set<string>(), replies = new Set<string>(), delegations = new Set<string>();
  for (const presentation of parsed.presentations) {
    if (ids.has(presentation.id) || presentation.associationId !== presentation.response.deliveryAssociation.id
      || presentation.id !== brokerIdentity('selected-presentation', presentation.associationId,
        presentation.sessionId, canonicalJson(presentation.publicationReceipts))
      || new Set(presentation.publicationReceipts.map(r => r.publicationSequence)).size !== presentation.publicationReceipts.length
      || new Set(presentation.publicationReceipts.map(r => r.eventId)).size !== presentation.publicationReceipts.length
      || presentation.publicationReceipts.some(r => r.sessionId !== presentation.sessionId)
      || presentation.response.delivery.state !== 'ready' || !('question' in presentation.response.delivery))
      fail('selected_voice_evidence_invalid');
    ids.add(presentation.id);
  }
  for (const source of parsed.sources) {
    const answer = source.answer, frozen = source.frozen;
    const presentation = parsed.presentations.find(p => p.id === answer.presentationId);
    const delegation = JSON.stringify([answer.sourceSessionId, answer.delegationId]);
    if (!presentation || sources.has(frozen.turn.sourceEventId) || replies.has(answer.input.reply.clientReplyId)
      || delegations.has(delegation) || answer.sourceSessionId !== frozen.sessionId
      || answer.sourceSessionId !== presentation.sessionId
      || answer.questionId !== presentation.response.deliveryAssociation.question.id
      || !followsPresentation(frozen, presentation)
      || !same(frozen.turn, { sourceEventId: answer.input.source.sourceEventId, revision: answer.input.source.revision,
        itemId: answer.input.source.itemId, role: answer.input.role, text: answer.input.text, occurredAt: answer.input.source.receivedAt })
      || answer.input.source.callSid !== presentation.response.callSid) fail('selected_voice_evidence_invalid');
    if (source.capture) acknowledgedTurn(presentation.response.bindingId, frozen.turn, source.capture);
    sources.add(frozen.turn.sourceEventId); replies.add(answer.input.reply.clientReplyId); delegations.add(delegation);
  }
  return parsed as SelectedQuestionVoiceEvidence;
}

/** Used by the existing call journal validator and on adapter restart. */
export function assertSelectedQuestionVoiceScope(seed: SelectedQuestionSeed,
  value: SelectedQuestionVoiceEvidence): SelectedQuestionVoiceEvidence {
  const parsed = parseSelectedQuestionVoiceEvidence(value);
  for (const presentation of parsed.presentations) {
    const response = presentation.response;
    if (response.bindingId !== seed.delivery.bindingId || response.callSid !== seed.delivery.callSid
      || response.requestId !== seed.request.originalRequestId
      || response.deliveryAssociation.execution.requestId !== seed.request.originalRequestId
      || response.version.ownerPrincipalId !== seed.scope.ownerPrincipalId
      || response.version.primaryDexterId !== seed.scope.primaryDexterId || response.version.threadId !== seed.scope.threadId)
      fail('selected_voice_scope_changed');
    if (!same(response.deliveryAssociation.execution, parsed.presentations[0]!.response.deliveryAssociation.execution))
      fail('selected_voice_execution_changed');
  }
  for (const source of parsed.sources) {
    const { frozen, answer } = source;
    if (frozen.turn.sourceEventId !== brokerIdentity('source', seed.deliveryCall.callId, frozen.sessionId, frozen.firstEventId)
      || frozen.turn.itemId !== `live-turn-${frozen.turn.sourceEventId}`
      || !same(answer.input.reply, { questionRevision: 1,
        clientReplyId: brokerIdentity('question-reply', seed.deliveryCall.callId, frozen.sessionId,
          seed.request.originalRequestId, answer.questionId, frozen.turn.sourceEventId), ...literalPhoneAnswer(frozen.turn.text) }))
      fail('selected_voice_source_changed');
  }
  return parsed;
}

export function assertSelectedQuestionVoiceEvidenceUpdate(previous: SelectedQuestionVoiceEvidence,
  next: SelectedQuestionVoiceEvidence): SelectedQuestionVoiceEvidence {
  const before = parseSelectedQuestionVoiceEvidence(previous), after = parseSelectedQuestionVoiceEvidence(next);
  if (after.presentations.length < before.presentations.length || after.presentations.length > before.presentations.length + 1
    || after.sources.length < before.sources.length || after.sources.length > before.sources.length + 1)
    fail('selected_voice_evidence_changed');
  for (const [index, item] of before.presentations.entries()) if (!same(item, after.presentations[index])) fail('selected_voice_evidence_changed');
  for (const [index, item] of before.sources.entries()) {
    const changed = after.sources[index]!;
    if (!same(item.answer, changed.answer) || !same(item.frozen, changed.frozen)
      || item.delegationRequestId !== changed.delegationRequestId || (item.capture && !same(item.capture, changed.capture)))
      fail('selected_voice_evidence_changed');
  }
  return after;
}

function publicationsAccepted(call: BrokerCall, presentation: SelectedQuestionVoicePresentation, historical = false): boolean {
  const result = presentation.response, receipt = result.delivery;
  if (receipt.state !== 'ready' || !('question' in receipt)
    || !selectedQuestionPublicationReceiptsAccepted(call, result, presentation.sessionId, presentation.publicationReceipts, historical)) return false;
  let chunks: string[];
  try { chunks = liveResultChunks(receipt.text); } catch { return false; }
  return chunks.length === presentation.publicationReceipts.length && chunks.every((chunk, index) => {
    const saved = call.publications?.records.find(record => record.publicationSequence === presentation.publicationReceipts[index].publicationSequence);
    return saved?.textDigest === textDigest(chunk) && saved.callerRevision === presentation.callerRevision;
  });
}

/** The future store field uses this on every write/read. Rejection tombstones
 * invalidate actionability without deleting the formerly accepted publication. */
export function validateSelectedQuestionVoiceCall(call: BrokerCall, value: SelectedQuestionVoiceEvidence,
  previous?: SelectedQuestionVoiceEvidence): SelectedQuestionVoiceEvidence {
  const host = call.selectedQuestion;
  if (!host || call.id !== host.initial.deliveryCall.callId || call.callSid !== host.initial.delivery.callSid
    || call.primary?.bindingId !== host.initial.delivery.bindingId
    || call.primary?.primaryDexterId !== host.initial.scope.primaryDexterId || call.primary?.threadId !== host.initial.scope.threadId
    || call.continuation?.originalRequestId !== host.initial.request.originalRequestId
    || call.continuation?.questionId !== host.initial.request.questionId || call.continuation?.questionRevision !== 1
    || call.liveGenerations?.clocks.callDeadlineAt !== host.initial.callDeadlineAt) fail('selected_voice_call_changed');
  const parsed = assertSelectedQuestionVoiceScope(host.initial, previous === undefined ? value
    : assertSelectedQuestionVoiceEvidenceUpdate(previous, value));
  for (const presentation of parsed.presentations) {
    const segment = call.liveGenerations!.segments.find(s => s.generationId === presentation.generationId);
    if (!segment || segment.sessionId !== presentation.sessionId || !same(segment.version, presentation.response.version)
      || !host.questions.some(q => same(q.record?.base.association, presentation.response.deliveryAssociation))
      || !publicationsAccepted(call, presentation, true)) fail('selected_voice_presentation_changed');
  }
  for (const source of parsed.sources) {
    const segment = call.liveGenerations!.segments.find(s => s.sessionId === source.frozen.sessionId);
    if (!call.mediaStartedAt || segment?.inputFirstSample === undefined
      || source.frozen.turn.occurredAt !== new Date(Date.parse(call.mediaStartedAt)
        + segment.inputFirstSample / 8 + source.frozen.startMs).toISOString()) fail('selected_voice_audio_origin_changed');
  }
  for (const slot of host.questions) for (const admission of slot.admissions)
    if (!parsed.sources.some(source => same(source.answer, admission.answer))) fail('selected_voice_admission_evidence_missing');
  return parsed;
}

export interface SelectedQuestionDeliveryObservation {
  outcome: GenerationDeliveryOutcome;
  snapshot: LiveTranscriptSnapshot;
  inputThroughMs: number;
  acceptedAt: string;
}

/** Eligibility, all accepted chunks, the current generation and audio ordering
 * jointly establish a reply barrier. This is publication evidence, never heard audio. */
export function selectedQuestionPresentation(call: BrokerCall, journal: RequestDeliveryJournal,
  observed: SelectedQuestionDeliveryObservation): SelectedQuestionVoicePresentation | undefined {
  const source = parseRequestDeliveryJournal(journal), { outcome, snapshot } = observed;
  if (!source.association || outcome.publication?.status !== 'accepted') return;
  const response = eligibleAssociatedDeliveryResponseSchema.parse(outcome.response);
  const attachment = call.liveGenerations?.deliveryAttachment;
  const segment = call.liveGenerations?.segments.find(s => s.generationId === attachment?.generationId);
  if (call.endedAt || terminalCallStatuses.has(call.status) || call.primaryClosed || !attachment || segment?.state !== 'active'
    || call.callSid !== source.target.delivery.callSid || call.primary?.bindingId !== source.target.delivery.bindingId
    || call.primary?.primaryDexterId !== source.scope.primaryDexterId || call.primary?.threadId !== source.scope.threadId
    || call.liveGenerations?.phase !== 'active' || !same(attachment, outcome.attachment)
    || !same(response.deliveryAssociation, source.association) || response.bindingId !== source.target.delivery.bindingId
    || response.callSid !== source.target.delivery.callSid || response.requestId !== source.request.originalRequestId
    || outcome.requestId !== source.request.originalRequestId || !same(segment.version, response.version)
    || response.version.ownerPrincipalId !== source.scope.ownerPrincipalId
    || response.version.primaryDexterId !== source.scope.primaryDexterId || response.version.threadId !== source.scope.threadId
    || response.delivery.state !== 'ready' || !('question' in response.delivery)
    || response.delivery.question.id !== source.target.question.id
    || response.delivery.question.runId !== source.target.execution.runId
    || response.delivery.question.agent.id !== source.scope.primaryDexterId
    || outcome.publication.observedCallerRevision !== snapshot.callerRevision
    || outcome.publication.observedTranscriptSequence !== Math.max(0, ...snapshot.fragments.map(f => f.sequence))) return;
  const receipts = outcome.publication.publicationReceipts;
  if (!receipts?.length) return;
  const inputThroughMs = segment.inputFirstSample === undefined ? 0
    : (segment.inputLastSampleExclusive! - segment.inputFirstSample) / 8;
  if (observed.inputThroughMs !== inputThroughMs) return;
  const candidate = presentationSchema.parse({
    id: brokerIdentity('selected-presentation', source.association.id, attachment.sessionId, canonicalJson(receipts)),
    associationId: source.association.id, generationId: attachment.generationId, sessionId: attachment.sessionId,
    response, callerRevision: snapshot.callerRevision,
    transcriptSequence: outcome.publication.observedTranscriptSequence,
    lastCallerEndMs: Math.max(0, ...snapshot.fragments.filter(f => f.role === 'user').map(f => f.endMs)),
    inputThroughMs, acceptedAt: observed.acceptedAt, publicationReceipts: receipts,
  });
  return publicationsAccepted(call, candidate) ? candidate : undefined;
}

export interface SelectedQuestionVoiceOptions extends Omit<SelectedQuestionHostOptions,
  'capture' | 'deliver' | 'replyIsQualified' | 'admittedReplyIsValid' | 'persist'> {
  readCall(): BrokerCall;
  persist(value: SelectedQuestionJournal): void;
  loadVoiceEvidence(): SelectedQuestionVoiceEvidence | undefined;
  persistVoiceEvidence(value: SelectedQuestionVoiceEvidence): void;
  freezer: SelectedCallerTurnFreezer;
  capture(bindingId: string, turn: PrimaryTurn): Promise<PrimaryTurnReceipt>;
  deliver(journal: RequestDeliveryJournal, target: EligibleDeliveryTarget): Promise<SelectedQuestionDeliveryObservation>;
  /** Called only after a durable host admission or a definite refusal. */
  admissionSettled?(request: LiveDelegationRequest): void;
  /** Same store/call mutex used by the backend; frozen admission never refreezes. */
  withCallerAdmission?<T>(operation: () => Promise<T>): Promise<T>;
}

export interface SelectedQuestionCloseout {
  pendingAdmissions: number;
  pendingCaptures: number;
  pendingReplies: number;
  ready: boolean;
}

/** Read evidence and reply state from the same saved call for source correction. */
export function selectedQuestionSourceAnchors(call: BrokerCall): SelectedQuestionSourceAnchor[] {
  if (!call.selectedQuestion) {
    if (call.selectedQuestionVoice) fail('selected_voice_host_missing');
    return [];
  }
  const evidence = validateSelectedQuestionVoiceCall(call, call.selectedQuestionVoice
    ?? { v: 'broker.selected-question-voice.v1', presentations: [], sources: [] });
  return evidence.sources.map(source => {
    const slot = call.selectedQuestion!.questions.find(q => q.admissions.some(a => same(a.answer, source.answer)));
    const admission = slot?.admissions.find(a => same(a.answer, source.answer));
    const attempt = admission?.attemptSequence === undefined ? undefined : slot?.record?.attempts[admission.attemptSequence - 1];
    return { frozen: structuredClone(source.frozen), ...(source.capture ? { capture: structuredClone(source.capture) } : {}),
      replyPending: Boolean(admission && !admission.localRefusal && (!attempt || !['accepted', 'refused'].includes(attempt.state))) };
  });
}

export interface SelectedQuestionRecoveryOptions {
  callId: string;
  store: Pick<BrokerStore, 'get' | 'recordSelectedQuestion' | 'recordSelectedQuestionVoice'>;
  primary: Pick<PrimaryApiClient, 'captureTurn'>;
  requests: RequestDeliveryClient;
  now?: () => number;
}

/** Maintenance after restart has only exact capture and reply-read recovery ports. */
export function createSelectedQuestionRecovery(options: SelectedQuestionRecoveryOptions): {
  prepareCloseout(): Promise<SelectedQuestionCloseout>;
} {
  const call = options.store.get(options.callId);
  if (!call) fail('selected_voice_call_missing');
  if (!call.selectedQuestion) {
    if (call.selectedQuestionVoice) fail('selected_voice_host_missing');
    return { async prepareCloseout() { return { pendingAdmissions: 0, pendingCaptures: 0, pendingReplies: 0, ready: true }; } };
  }
  const unavailable = async (): Promise<never> => fail('selected_voice_recovery_only');
  const adapter = createSelectedQuestionVoiceAdapter({ seed: call.selectedQuestion.initial,
    readCall: () => options.store.get(options.callId) ?? fail('selected_voice_call_missing'),
    load: () => options.store.get(options.callId)?.selectedQuestion,
    persist: value => { options.store.recordSelectedQuestion(options.callId, value); },
    loadVoiceEvidence: () => options.store.get(options.callId)?.selectedQuestionVoice,
    persistVoiceEvidence: value => { options.store.recordSelectedQuestionVoice(options.callId, value); },
    active: () => false, now: options.now,
    association: { associate: unavailable }, freezer: { freeze: unavailable }, deliver: unavailable,
    requests: { associate: unavailable, result: unavailable, reply: unavailable,
      question: journal => options.requests.question(journal) },
    capture: (bindingId, turn) => options.primary.captureTurn(bindingId, turn),
  });
  adapter.stop();
  return { prepareCloseout: () => adapter.prepareCloseout() };
}

/** Has no ordinary Primary submission port. The host retains exact associations
 * and capture acknowledgements while the adapter owns caller/publication proof. */
export function createSelectedQuestionVoiceAdapter(options: SelectedQuestionVoiceOptions) {
  const now = options.now ?? Date.now;
  let evidence = assertSelectedQuestionVoiceScope(options.seed, options.loadVoiceEvidence()
    ?? { v: 'broker.selected-question-voice.v1', presentations: [], sources: [] });
  const pending = new Map<LiveDelegationRequest, string | undefined>();
  // Call stop releases the rotation drain, but closeout must still observe the
  // completion/refusal of a source freeze already in progress.
  const preparingSources = new Set<LiveDelegationRequest>();
  const settle = (request: LiveDelegationRequest) => {
    if (!pending.has(request)) return; pending.delete(request); options.admissionSettled?.(request);
  };
  const save = (next: SelectedQuestionVoiceEvidence) => {
    const checked = validateSelectedQuestionVoiceCall(options.readCall(),
      assertSelectedQuestionVoiceScope(options.seed, assertSelectedQuestionVoiceEvidenceUpdate(evidence, next)));
    options.persistVoiceEvidence(structuredClone(checked)); evidence = checked;
  };
  const current = (sessionId: string) => {
    const call = options.readCall(), generations = call.liveGenerations;
    return options.active() && now() < Date.parse(options.seed.callDeadlineAt) && !call.endedAt && !call.primaryClosed
      && !terminalCallStatuses.has(call.status)
      && call.id === options.seed.deliveryCall.callId && call.callSid === options.seed.delivery.callSid
      && call.primary?.bindingId === options.seed.delivery.bindingId && generations?.phase === 'active'
      && call.primary?.primaryDexterId === options.seed.scope.primaryDexterId && call.primary?.threadId === options.seed.scope.threadId
      && generations.binding.ownerPrincipalId === options.seed.scope.ownerPrincipalId
      && generations.deliveryAttachment?.sessionId === sessionId
      && generations.segments.some(s => s.sessionId === sessionId && s.state === 'active');
  };
  const sourceFor = (answer: SelectedQuestionAnswer) => evidence.sources.find(s => same(s.answer, answer));
  const valid = (journal: RequestDeliveryJournal, answer: SelectedQuestionAnswer) => {
    const source = sourceFor(answer), presentation = evidence.presentations.find(p => p.id === answer.presentationId);
    return !!source && !!presentation && same(presentation.response.deliveryAssociation, journal.association)
      && publicationsAccepted(options.readCall(), presentation);
  };
  const intentAllowsDispatch = (answer: SelectedQuestionAnswer): boolean => {
    const call = options.readCall(), sourceEventId = answer.input.source.sourceEventId;
    if (!getCallerRouteBySource(call, sourceEventId)) return true;
    try { requireCallerRouteDispatch(call, sourceEventId, now()); return true; }
    catch { return false; }
  };
  const capture = async (bindingId: string, turn: PrimaryTurn) => {
    const index = evidence.sources.findIndex(s => same(s.frozen.turn, turn));
    if (index < 0 || bindingId !== options.seed.delivery.bindingId) fail('selected_voice_source_unknown');
    const saved = evidence.sources[index];
    if (saved.capture) return structuredClone(saved.capture);
    const receipt = await options.capture(bindingId, structuredClone(turn));
    acknowledgedTurn(bindingId, turn, receipt);
    save({ ...evidence, sources: evidence.sources.map((s, i) => i === index ? { ...s, capture: structuredClone(receipt) } : s) });
    return receipt;
  };
  const host = createSelectedQuestionHost({ ...options, capture,
    requests: { ...options.requests, reply(journal) {
      const sourceEventId = journal.reply?.input.source.sourceEventId;
      const call = options.readCall();
      if (sourceEventId && getCallerRouteBySource(call, sourceEventId)) {
        try { requireCallerRouteDispatch(call, sourceEventId, now()); }
        catch (error) {
          // This adapter has not called HTTP. The existing reducer preserves a
          // definite local refusal even if the durable dispatch marker was saved.
          if (error instanceof CallerRouteExpiredError) throw new PrimaryApiError('invalid_request');
          throw error;
        }
      }
      return options.requests.reply(journal);
    } },
    persist(journal) {
      options.persist(journal);
      const admitted = new Set(journal.questions.flatMap(q => q.admissions.map(a => a.answer.input.source.sourceEventId)));
      for (const [request, sourceId] of pending) if (sourceId && admitted.has(sourceId)) settle(request);
    },
    replyIsQualified: (journal, answer) => current(answer.sourceSessionId) && valid(journal, answer) && intentAllowsDispatch(answer),
    admittedReplyIsValid: (journal, answer) => valid(journal, answer) && intentAllowsDispatch(answer),
    async deliver(journal, target) {
      const observation = await options.deliver(journal, target);
      if (target.kind !== 'question') return;
      if (Date.parse(observation.acceptedAt) > now()) fail('selected_voice_receipt_unobserved');
      const presentation = selectedQuestionPresentation(options.readCall(), journal, observation);
      if (!presentation) return;
      const previous = evidence.presentations.find(p => p.id === presentation.id);
      if (previous) {
        if (!same(previous.response, presentation.response) || !same(previous.publicationReceipts, presentation.publicationReceipts))
          fail('selected_voice_presentation_changed');
      } else save({ ...evidence, presentations: [...evidence.presentations, presentation] });
    },
  });
  // The host is persisted first on fresh startup; retained voice evidence must
  // match its exact association, accepted publication records and source clocks.
  validateSelectedQuestionVoiceCall(options.readCall(), evidence);
  const stop = () => {
    const state = host.stop(); for (const request of pending.keys()) settle(request); return state;
  };
  const questionTargets = (sessionId: string): CallerQuestionTarget[] => {
    if (!current(sessionId)) return [];
    const state = host.snapshot(), slot = state.questions.at(-1), association = slot?.record?.base.association;
    if (state.stoppedAt || !slot || !association || slot.admissions.some(admission => !admission.localRefusal
      && (admission.attemptSequence === undefined || slot.record!.attempts[admission.attemptSequence - 1]?.state !== 'refused'))) return [];
    const presentation = evidence.presentations.filter(p => p.associationId === association.id && p.sessionId === sessionId).at(-1);
    const delivery = presentation?.response.delivery;
    if (!presentation || !delivery || delivery.state !== 'ready' || !('question' in delivery)
      || delivery.question.state !== 'pending' || Date.parse(delivery.question.expiresAt) <= now()
      || !publicationsAccepted(options.readCall(), presentation)) return [];
    return [{ kind: 'selected', bindingId: association.execution.bindingId, requestId: association.execution.requestId,
      runId: association.execution.runId, questionId: association.question.id, questionRevision: association.question.revision,
      associationId: association.id, presentationDigest: callerRoutePresentationDigest(presentation) }];
  };
  const admit = async (request: LiveDelegationRequest, channel: LiveBackendChannel, routeRef?: CallerRouteRef): Promise<void> => {
      preparingSources.add(request);
      pending.set(request, undefined);
      try {
        const route = routeRef ? requireCallerRoute(options.readCall(), request, routeRef, 'selected_answer', 'recover') : undefined;
        if (request.callId !== options.seed.deliveryCall.callId || channel.signal.aborted
          || (!routeRef && !current(request.sessionId))) fail('selected_voice_source_inactive');
        const target = route?.decision?.kind === 'answer' && route.decision.target.kind === 'selected' ? route.decision.target : undefined;
        if (routeRef && !target) fail('caller_route_kind_mismatch');
        const replay = evidence.sources.find(s => s.answer.sourceSessionId === request.sessionId && s.answer.delegationId === request.delegationId);
        if (replay) {
          if (replay.delegationRequestId !== request.requestId) fail('selected_voice_delegation_changed');
          if (route && (!same(replay.frozen, route.frozen) || replay.answer.questionId !== target!.questionId
            || !evidence.presentations.some(p => p.id === replay.answer.presentationId
              && callerRoutePresentationDigest(p) === target!.presentationDigest))) fail('caller_route_question_changed');
          preparingSources.delete(request);
          pending.set(request, replay.frozen.turn.sourceEventId); await host.answer(replay.answer); return;
        }
        if (routeRef) requireCallerRoute(options.readCall(), request, routeRef, 'selected_answer');
        if (!current(request.sessionId)) fail('selected_voice_source_inactive');
        const slot = host.snapshot().questions.at(-1)!;
        if (!slot.record?.base.association) fail('selected_voice_question_unassociated');
        const journal = slot.record.base;
        // Selection is scoped to this exact question association and current generation.
        const presentations = evidence.presentations.filter(p => p.associationId === journal.association!.id
          && p.sessionId === request.sessionId);
        const presentation = target ? presentations.find(p => callerRoutePresentationDigest(p) === target.presentationDigest)
          : presentations.at(-1);
        if (target && !questionTargets(request.sessionId).some(candidate => same(candidate, target)))
          fail('caller_route_question_changed');
        if (!presentation || !publicationsAccepted(options.readCall(), presentation)) fail('selected_voice_question_not_presented');
        const frozenValue = route ? route.frozen
          : await options.freezer.freeze(request, channel, evidence.sources.map(s => s.frozen.turn.sourceEventId));
        if (!frozenValue) fail('selected_voice_turn_unqualified');
        const frozen = frozenSchema.parse(frozenValue);
        const call = options.readCall(), segment = call.liveGenerations!.segments.find(s => s.sessionId === request.sessionId)!;
        if (!call.mediaStartedAt || segment?.inputFirstSample === undefined) fail('selected_voice_audio_origin_missing');
        const sourceId = brokerIdentity('source', call.id, request.sessionId, frozen.firstEventId);
        const origin = Date.parse(call.mediaStartedAt) + segment.inputFirstSample / 8;
        if (frozen.sessionId !== request.sessionId || frozen.turn.sourceEventId !== sourceId
          || frozen.turn.itemId !== `live-turn-${sourceId}`
          || frozen.turn.occurredAt !== new Date(origin + frozen.startMs).toISOString()
          || !followsPresentation(frozen, presentation))
          fail('selected_voice_turn_precedes_question');
        const input = replyInputSchema.parse({ source: { callSid: options.seed.delivery.callSid, sourceEventId: sourceId,
          revision: 1, itemId: frozen.turn.itemId, receivedAt: frozen.turn.occurredAt }, role: 'user', text: frozen.turn.text,
          reply: { questionRevision: 1, clientReplyId: brokerIdentity('question-reply', call.id, request.sessionId,
            journal.request.originalRequestId, journal.target.question.id, sourceId), ...literalPhoneAnswer(frozen.turn.text) } });
        const answer: SelectedQuestionAnswer = { questionId: journal.target.question.id, input,
          sourceSessionId: request.sessionId, delegationId: request.delegationId, presentationId: presentation.id };
        if (routeRef) requireCallerRoute(options.readCall(), request, routeRef, 'selected_answer');
        // Stop may win after this save: the raw source remains a capture obligation.
        save({ ...evidence, sources: [...evidence.sources, { answer, frozen, delegationRequestId: request.requestId }] });
        preparingSources.delete(request);
        pending.set(request, sourceId);
        if (channel.signal.aborted) fail('selected_voice_source_inactive');
        await host.answer(answer);
      } finally { preparingSources.delete(request); settle(request); }
  };
  return {
    host,
    evidence: () => structuredClone(evidence),
    pendingSourceCount: () => pending.size,
    delegate(request: LiveDelegationRequest, channel: LiveBackendChannel): Promise<void> {
      return admit(request, channel);
    },
    answerFrozen(request: LiveDelegationRequest, route: CallerRouteRef, channel: LiveBackendChannel): Promise<void> {
      if (!options.withCallerAdmission) return Promise.reject(new Error('caller_route_admission_lock_required'));
      return options.withCallerAdmission(() => admit(request, channel, route));
    },
    questionTargets,
    stop,
    async prepareCloseout(): Promise<SelectedQuestionCloseout> {
      stop();
      for (const source of evidence.sources) if (!source.capture) {
        try { await capture(options.seed.delivery.bindingId, source.frozen.turn); } catch { /* Keep exact capture pending. */ }
      }
      try { await host.recoverReply(); } catch { /* Uncertain HTTP outcome remains read-only recovery. */ }
      const saved = host.snapshot();
      const pendingCaptures = evidence.sources.filter(s => !s.capture).length;
      const pendingReplies = saved.questions.reduce((count, slot) => count + slot.admissions.filter(admission => {
        if (admission.localRefusal) return false;
        const attempt = admission.attemptSequence === undefined ? undefined : slot.record?.attempts[admission.attemptSequence - 1];
        return !attempt || !['accepted', 'refused'].includes(attempt.state);
      }).length, 0);
      const pendingAdmissions = preparingSources.size;
      return { pendingAdmissions, pendingCaptures, pendingReplies,
        ready: pendingAdmissions === 0 && pendingCaptures === 0 && pendingReplies === 0 };
    },
  };
}
