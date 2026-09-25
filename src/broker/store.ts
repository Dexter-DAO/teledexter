import { validateRestrictedSetupCall, restrictedSetupJournalSchema, type RestrictedSetupJournal } from './restrictedSetupJournal.js';
import { reserveVerificationCallback, parseVerificationCallbackJournal, validateVerificationCallbackCall, verificationCallbackPurpose, type VerificationCallbackJournal } from './verificationCallbackJournal.js';
import { phoneVerificationCallbackPermitSchema, type PhoneVerificationCallbackPermit } from './verificationCallbackContract.js';
import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { z } from 'zod';
import { brokerCallAssignmentSchema, phoneCallAssignmentSchema, retainedPhoneCallAssignmentRefSchema, sameCallAssignmentRef, type BrokerCallAssignment, type PhoneCallAssignmentRef } from './assignment.js';
import { selectedQuestionPublicationReceiptsAccepted, validateSelectedQuestionPublications, type BrokerPublicationJournal } from './publications.js';
import { resolvedHarkAssignmentSchema } from './harkAssignment.js';
import { canonicalJson } from '../outbound/canonical.js';
import { observeHark, pendingHarkEvents, settleHarkJournalEvent, validateHarkJournal,
  type HarkDeliveryOutcome, type HarkEndedTiming, type HarkJournal, type HarkJournalEvent, type HarkProviderStatus } from './harkJournal.js';
import { bindDurableFileParent, durableFileParentIsCurrent, durablyReplaceJsonFile, type DurableParentBinding } from '../outbound/DurableFile.js';
import { assertInboundMediaStart, createInboundJournal, inboundRequestId, reduceInboundJournal, validateInboundJournal,
  type InboundJournal, type InboundJournalEvent, type InboundWaitPolicy } from './inboundJournal.js';
import type { InboundBeginIdentity } from './inboundContract.js';
import type { PrimaryCall } from './primaryApi.js';
import { primaryHarkReceiptBindingFromCall, reducePrimaryHarkReceiptOutbox, validatePrimaryHarkReceiptOutbox,
  type PrimaryHarkReceiptBinding, type PrimaryHarkReceiptCommand, type PrimaryHarkReceiptOutbox } from './harkPrimaryReceiptOutbox.js';
import { validateBrokerAdmissionRefusals, type BrokerDelegationRecord } from './backend.js';
import { liveGenerationJournalSchema, type LiveGenerationJournal } from './liveGeneration.js';
import { callContinuationPatch, assertCallContinuationReplay, requestDeliveryRequestForContinuation, validateCallContinuation, type CallContinuation } from './callContinuation.js';
import { assertSelectedQuestionJournalUpdate, parseSelectedQuestionJournal, type SelectedQuestionJournal } from './selectedQuestion.js';

import { parseSelectedQuestionVoiceEvidence, validateSelectedQuestionVoiceCall, type SelectedQuestionVoiceEvidence } from './selectedQuestionVoice.js';

import { prepareCallerRouting, decideCallerRouting, captureCallerRouting, validateCallerRouting,
  prepareCallerIntent, recordCallerIntentResponse,
  type CallerRoutingJournal, type CallerRoutePreparation, type CallerRouteRecord, type CallerRouteRef, type CallerRouteDecision } from './callerRouting.js';
import type { PrimaryTurnReceipt } from './primaryApi.js';
import type { CallerIntentInput, CallerIntentResponse } from './callerIntentContract.js';

import type { CallRequestJournalAvailability } from './callRequestReceipt.js';
import { createOutboundObservation, reduceOutboundObservation, validateSelectedOutboundObservation,
  type OutboundObservation, type OutboundObservationEvent, type OutboundProviderIdentity } from './outboundObservation.js';
import { assertOutboundRetryReplay, outboundRetryOfPatch, OutboundRetryError,
  type OutboundRetryOf, type OutboundRetrySuccessor } from './outboundSettlement.js';
import { prepareOutboundRetry, validateOutboundRetries } from './outboundRetry.js';

export type CallStatus = 'prepared' | 'dialing' | 'queued' | 'ringing' | 'in-progress' | 'completed' | 'busy' | 'no-answer' | 'canceled' | 'failed' | 'unknown';
export const terminalCallStatuses = new Set<CallStatus>(['completed', 'busy', 'no-answer', 'canceled', 'failed']);
export interface HarkDispatch {
  version: 1;
  notificationDigest: string;
  state: 'not_dispatched' | 'dispatch_started' | 'provider_accepted';
  reservedAt: string;
  dispatchStartedAt?: string;
  providerAcceptedAt?: string;
  preDispatchFailure?: { observedAt: string; code: string; message: string };
}
export interface BrokerCall {
  id: string; requestId: string; direction: 'inbound' | 'outbound'; purpose: string;
  number: string; callSid?: string; streamSid?: string; status: CallStatus;
  createdAt: string; updatedAt: string; endedAt?: string;
  callAssignment?: BrokerCallAssignment;
  continuation?: CallContinuation;
  selectedQuestion?: SelectedQuestionJournal;
  selectedQuestionVoice?: SelectedQuestionVoiceEvidence;
  callerRouting?: CallerRoutingJournal;
  outboundObservation?: OutboundObservation;
  retryOf?: OutboundRetryOf;
  retrySuccessor?: OutboundRetrySuccessor;
  primary?: { bindingId: string; name: string | null; context: string; callAssignmentRef?: PhoneCallAssignmentRef;
    primaryDexterId?: string; threadId?: string };
  liveSessionId?: string; transcript?: unknown; transcriptRevision?: number;
  liveGenerations?: LiveGenerationJournal;
  sourceAcousticSpans?: Record<string, unknown>;
  mediaStartedAt?: string;
  harkJournal?: HarkJournal;
  harkReceiptOutbox?: PrimaryHarkReceiptOutbox;
  harkDispatch?: HarkDispatch;
  inbound?: InboundJournal;
  verificationCallback?: VerificationCallbackJournal;
  restrictedSetup?: RestrictedSetupJournal;
  voiceOpening?: { status: string; eventId?: string; reason?: string };
  finalized?: boolean; failure?: { code: string; message: string };
  canonicalTurns?: Record<string, unknown>;
  publications?: BrokerPublicationJournal;
  acousticSpans?: unknown;
  callerCompletions?: Record<string, { sessionId: string; firstSequence: number; lastSequence: number;
    textDigest: string; observedAt: string }>;
  primaryClosePending?: boolean;
  primaryClosed?: { bindingId: string; callSid: string; state: 'closed' };
  callback?: { requestId: string; consentedAt: string; state: 'requested' | 'started' | 'unavailable' };
  transcriptCapture?: {
    state: 'pending' | 'complete';
    assistantPlaybackComplete: boolean;
    sessionFinalized: boolean;
    pending: number;
    unsupported: string[];
  };
  metrics?: { receivedAudioBytes: number; sentAudioBytes: number; playedAudioBytes: number; clearedAudioBytes: number };
  delegations: Record<string, { requestId: string; status: string; updatedAt: string; result?: string }>;
}

const dispatchDigest = z.string().regex(/^[0-9a-f]{64}$/);
const dispatchTime = z.string().datetime({ offset: true });
const dispatchBase = { version: z.literal(1), notificationDigest: dispatchDigest, reservedAt: dispatchTime };
const preDispatchFailure = z.object({ observedAt: dispatchTime,
  code: z.string().min(1).max(80), message: z.string().min(1).max(400) }).strict().optional();
const harkDispatchSchema = z.discriminatedUnion('state', [
  z.object({ ...dispatchBase, state: z.literal('not_dispatched') }).strict(),
  z.object({ ...dispatchBase, state: z.literal('dispatch_started'), dispatchStartedAt: dispatchTime, preDispatchFailure }).strict(),
  z.object({ ...dispatchBase, state: z.literal('provider_accepted'), dispatchStartedAt: dispatchTime, providerAcceptedAt: dispatchTime, preDispatchFailure }).strict(),
]);
function validateHarkDispatch(call: BrokerCall): void {
  if (call.harkDispatch === undefined) return;
  const dispatch = harkDispatchSchema.parse(call.harkDispatch);
  if (call.callAssignment?.origin !== 'authenticated_hark_delivery') throw new Error('Hark dispatch requires its original assignment');
  if (dispatch.reservedAt !== call.createdAt) throw new Error('Hark dispatch reservation time changed');
  if (dispatch.state === 'provider_accepted') {
    if (!call.callSid || !/^CA[0-9a-fA-F]{32}$/.test(call.callSid)) throw new Error('Accepted Hark dispatch requires its provider SID');
  } else {
    if (call.callSid !== undefined || call.harkJournal !== undefined || call.primary !== undefined || call.streamSid !== undefined)
      throw new Error('Unaccepted Hark dispatch has provider evidence');
    if (dispatch.state === 'not_dispatched' && !['prepared', 'failed', 'canceled'].includes(call.status))
      throw new Error('Undispatched Hark call has dispatched status');
    if (dispatch.state === 'dispatch_started' && !['dialing', 'failed', 'unknown', 'canceled'].includes(call.status))
      throw new Error('Unaccepted Hark dispatch has provider status');
  }
}

function validateAssignment(call: BrokerCall): void {
  const assignment = call.callAssignment === undefined ? undefined : brokerCallAssignmentSchema.parse(call.callAssignment);
  if (assignment && (call.direction !== 'outbound' || assignment.value.assignmentId !== call.requestId
      || assignment.value.brokerCallId !== call.id || assignment.value.purpose !== call.purpose)) throw new Error('Call assignment identity mismatch');
  const reference = call.primary?.callAssignmentRef;
  if (reference !== undefined) {
    retainedPhoneCallAssignmentRefSchema.parse(reference);
    if (!assignment || !call.callSid || reference.assignmentId !== assignment.value.assignmentId
        || reference.revision !== assignment.value.revision) throw new Error('Call assignment reference mismatch');
  }
  if (assignment && call.primary && !reference) throw new Error('Call assignment reference missing');
  validateCallContinuation(call);
}

const primaryResultId = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
function validatePrimaryIdentity(call: BrokerCall): void {
  const primary = call.primary;
  // Old journals lack both IDs. Reading them must not manufacture or persist IDs.
  if (primary?.primaryDexterId === undefined && primary?.threadId === undefined) return;
  if (!primaryResultId.safeParse(primary?.primaryDexterId).success || !primaryResultId.safeParse(primary?.threadId).success)
    throw new Error('Invalid Primary result identity');
}

function assertPrimaryIdentityUnchanged(previous: BrokerCall, next: BrokerCall): void {
  if (!previous.primary) return;
  if (next.primary?.bindingId !== previous.primary.bindingId)
    throw new Error('Primary binding cannot change');
  for (const field of ['primaryDexterId', 'threadId'] as const) {
    if (previous.primary[field] !== undefined && next.primary?.[field] !== previous.primary[field])
      throw new Error('Primary result identity cannot change');
  }
}

function validateGenerations(call: BrokerCall, previous?: BrokerCall): void {
  if (call.liveGenerations === undefined) {
    if (previous?.liveGenerations) throw new Error('Live generation journal cannot be removed');
    return;
  }
  const next = liveGenerationJournalSchema.parse(call.liveGenerations);
  if (next.callId !== call.id || next.binding.callSid !== call.callSid
    || next.binding.bindingId !== call.primary?.bindingId || next.binding.primaryDexterId !== call.primary.primaryDexterId
    || next.binding.threadId !== call.primary.threadId || next.clocks.mediaStartedAt !== call.mediaStartedAt)
    throw new Error('Live generation call identity changed');
  const old = previous?.liveGenerations;
  if (!old) return;
  const priorBinding = old.binding.ownerPrincipalId === undefined ? { ...old.binding, ownerPrincipalId: next.binding.ownerPrincipalId } : old.binding;
  if (canonicalJson(priorBinding) !== canonicalJson(next.binding) || canonicalJson(old.clocks) !== canonicalJson(next.clocks)
    || next.inputNextSample < old.inputNextSample || next.segments.length < old.segments.length
    || (old.phase === 'stopped' && next.phase !== 'stopped')) throw new Error('Live generation history changed');
  for (const [index, before] of old.segments.entries()) {
    const after = next.segments[index]!;
    for (const key of ['generationId', 'ordinal', 'createdAt', 'version', 'pageDigest'] as const)
      if (canonicalJson(before[key]) !== canonicalJson(after[key])) throw new Error('Live generation source changed');
    for (const key of ['sessionId', 'readyAt', 'inputFirstSample', 'retiringAt', 'closedAt', 'providerFinalized', 'transcriptPersisted'] as const)
      if (before[key] !== undefined && before[key] !== after[key]) throw new Error('Live generation observation changed');
    if ((before.state === 'closed' || before.state === 'abandoned') && after.state !== before.state)
      throw new Error('Closed Live generation cannot reopen');
    if (after.transcript.revision < before.transcript.revision
      || before.transcript.fragments.some((fragment, index) => canonicalJson(fragment) !== canonicalJson(after.transcript.fragments[index]))
      || (before.inputLastSampleExclusive !== undefined && (after.inputLastSampleExclusive ?? -1) < before.inputLastSampleExclusive))
      throw new Error('Live generation clock moved backwards');
  }
  for (const [requestId, value] of Object.entries(previous!.delegations)) {
    const before = value as BrokerDelegationRecord;
    const after = call.delegations[requestId] as BrokerDelegationRecord | undefined;
    if (!after) throw new Error('Accepted call work cannot be removed');
    for (const key of ['requestId', 'callId', 'callSid', 'bindingId', 'sessionId', 'submission', 'firstSequence', 'frozenCallerRevision'] as const)
      if (canonicalJson(before[key]) !== canonicalJson(after[key])) throw new Error('Accepted call source changed');
  }
}

function selectedQuestionCallStopped(call: BrokerCall): boolean {
  return Boolean(call.endedAt || call.finalized || call.primaryClosed || terminalCallStatuses.has(call.status)
    || call.liveGenerations?.phase === 'stopped'
    || (call.liveGenerations && Date.now() >= Date.parse(call.liveGenerations.clocks.callDeadlineAt)));
}

function validateSelectedQuestionCall(call: BrokerCall, previous?: BrokerCall): void {
  if (call.selectedQuestion === undefined) {
    if (previous?.selectedQuestion) throw new Error('Selected question journal cannot be removed');
    return;
  }
  const journal = parseSelectedQuestionJournal(call.selectedQuestion);
  const seed = journal.initial;
  const continuation = validateCallContinuation(call);
  const generations = call.liveGenerations;
  const source = generations?.segments.find(segment => segment.sessionId === seed.deliveryCall.sessionId);
  if (!continuation || canonicalJson(seed.request) !== canonicalJson(requestDeliveryRequestForContinuation(continuation))
    || seed.deliveryCall.callId !== call.id || seed.delivery.callSid !== call.callSid
    || seed.delivery.bindingId !== call.primary?.bindingId || seed.scope.primaryDexterId !== call.primary.primaryDexterId
    || seed.scope.threadId !== call.primary.threadId || !generations || !source
    || seed.scope.ownerPrincipalId !== generations.binding.ownerPrincipalId
    || !sameCallAssignmentRef(generations.binding.callAssignmentRef, call.primary.callAssignmentRef)
    || seed.callDeadlineAt !== generations.clocks.callDeadlineAt
    || source.version.ownerPrincipalId !== seed.scope.ownerPrincipalId
    || source.version.primaryDexterId !== seed.scope.primaryDexterId || source.version.threadId !== seed.scope.threadId)
    throw new Error('Selected question call identity mismatch');
  for (const slot of journal.questions) for (const admission of slot.admissions) {
    const admittedSource = generations.segments.find(segment => segment.sessionId === admission.answer.sourceSessionId);
    if (!admittedSource || admittedSource.version.ownerPrincipalId !== seed.scope.ownerPrincipalId
      || admittedSource.version.primaryDexterId !== seed.scope.primaryDexterId || admittedSource.version.threadId !== seed.scope.threadId)
      throw new Error('Selected question source identity mismatch');
  }
  // Startup validates retained source identity even after its generation has closed.
  if (!previous) return;
  const before = previous.selectedQuestion;
  if (!before) {
    const attachment = generations.deliveryAttachment;
    if (selectedQuestionCallStopped(call) || generations.phase !== 'active' || source.state !== 'active'
      || !attachment || attachment.sessionId !== source.sessionId
      || attachment.generationId !== source.generationId
      || journal.stoppedAt || journal.questions.length !== 1 || journal.questions[0]!.record
      || journal.questions[0]!.admissions.length !== 0)
      throw new Error('Selected question requires its active initial generation');
    return;
  }
  assertSelectedQuestionJournalUpdate(before, journal);
  const stopped = selectedQuestionCallStopped(call) || Boolean(before.stoppedAt || journal.stoppedAt);
  if (stopped && journal.questions.length !== before.questions.length)
    throw new Error('Stopped call cannot select another question');
  for (const [index, slot] of journal.questions.entries()) {
    const old = before.questions[index];
    const oldAdmissions = old?.admissions.length ?? 0;
    if (stopped && (slot.admissions.length !== oldAdmissions
      || (slot.record?.attempts.length ?? 0) !== (old?.record?.attempts.length ?? 0)
      || slot.record?.attempts.some((attempt, i) => attempt.dispatches !== old?.record?.attempts[i]?.dispatches)))
      throw new Error('Stopped call cannot admit or dispatch another answer');
    for (const admission of slot.admissions.slice(oldAdmissions)) {
      const attachment = generations.deliveryAttachment;
      const active = generations.segments.find(segment => segment.generationId === attachment?.generationId);
      if (generations.phase !== 'active' || active?.state !== 'active'
        || admission.answer.sourceSessionId !== attachment?.sessionId || active.sessionId !== admission.answer.sourceSessionId)
        throw new Error('Selected question answer requires its active source generation');
    }
  }
}

function validateSelectedVoice(call: BrokerCall, previous?: BrokerCall): void {
  validateSelectedQuestionPublications(call, previous);
  const value = call.selectedQuestionVoice, before = previous?.selectedQuestionVoice;
  if (value === undefined) {
    if (before || call.selectedQuestion?.questions.some(slot => slot.admissions.length))
      throw new Error('Selected question answer requires saved voice evidence');
    return;
  }
  const evidence = validateSelectedQuestionVoiceCall(call, value, before);
  for (const presentation of evidence.presentations) {
    if (!selectedQuestionPublicationReceiptsAccepted(call, presentation.response, presentation.sessionId,
      presentation.publicationReceipts, true)) throw new Error('Selected question publication evidence changed');
  }
  for (const slot of call.selectedQuestion!.questions) for (const admission of slot.admissions) {
    if (!admission.captured) continue;
    const source = evidence.sources.find(item => canonicalJson(item.answer) === canonicalJson(admission.answer));
    const captured = admission.captured.callerSource;
    if (!source?.capture || !captured || captured.bindingId !== source.capture.bindingId
      || captured.sourceEventId !== source.capture.sourceEventId || captured.revision !== source.capture.revision
      || captured.recordId !== source.capture.recordId || captured.revisionRecordId !== source.capture.revisionRecordId
      || captured.bodyDigest !== source.capture.bodyDigest) throw new Error('Selected question capture acknowledgement changed');
  }
  if (!previous) return;
  for (const presentation of evidence.presentations.slice(before?.presentations.length ?? 0)) {
    const attachment = call.liveGenerations?.deliveryAttachment;
    if (selectedQuestionCallStopped(call) || call.liveGenerations?.phase !== 'active'
      || attachment?.sessionId !== presentation.sessionId || attachment.generationId !== presentation.generationId
      || !selectedQuestionPublicationReceiptsAccepted(call, presentation.response, presentation.sessionId, presentation.publicationReceipts))
      throw new Error('Selected question presentation requires current accepted publications');
  }
  for (const [index, slot] of (call.selectedQuestion?.questions ?? []).entries()) {
    const old = previous.selectedQuestion?.questions[index];
    for (const admission of slot.admissions.slice(old?.admissions.length ?? 0)) {
      const source = before?.sources.find(item => canonicalJson(item.answer) === canonicalJson(admission.answer));
      const presentation = source && before!.presentations.find(item => item.id === source.answer.presentationId);
      if (!source || !presentation || !selectedQuestionPublicationReceiptsAccepted(call,
        presentation.response, presentation.sessionId, presentation.publicationReceipts))
        throw new Error('Selected question answer requires prior durable source and accepted publications');
    }
    for (const [attemptIndex, attempt] of (slot.record?.attempts ?? []).entries()) {
      if (attempt.dispatches <= (old?.record?.attempts[attemptIndex]?.dispatches ?? 0)) continue;
      const source = evidence.sources.find(item => canonicalJson(item.answer.input) === canonicalJson(attempt.journal.reply?.input));
      const presentation = source && evidence.presentations.find(item => item.id === source.answer.presentationId);
      if (!source?.capture || !presentation || !selectedQuestionPublicationReceiptsAccepted(call,
        presentation.response, presentation.sessionId, presentation.publicationReceipts))
        throw new Error('Selected question dispatch requires captured source and accepted publications');
    }
  }
}

function receiptDelegation(call: BrokerCall, requestId: string): BrokerDelegationRecord {
  const entries = Object.values(call.delegations) as BrokerDelegationRecord[];
  const matches = entries.filter(entry => entry.primaryResult?.requestId === requestId && entry.status === 'completed');
  if (matches.length !== 1) throw new Error('Hark receipt requires its saved completed request');
  const entry = matches[0]!;
  if (entry.primaryResult?.state !== 'completed' || entry.primaryResult.bindingId !== call.primary?.bindingId
    || entry.bindingId !== call.primary?.bindingId || entry.callId !== call.id || entry.callSid !== call.callSid
    || !entry.primaryResult.assistant?.text?.trim()
    || canonicalJson(entry.submission?.callAssignmentRef) !== canonicalJson(call.primary?.callAssignmentRef)
    || (entry.requestId !== requestId && !(entry.reconciliation?.outcome === 'admitted'
      && entry.reconciliation.requestId === entry.requestId && entry.reconciliation.canonicalRequestId === requestId
      && entry.reconciliation.bindingId === call.primary?.bindingId)))
    throw new Error('Hark receipt completed request identity changed');
  return entry;
}
function validateHarkReceiptCall(call: BrokerCall): void {
  if (call.harkReceiptOutbox === undefined) return;
  const binding = primaryHarkReceiptBindingFromCall(call);
  if (!binding) throw new Error('Hark receipt binding unavailable');
  const box = validatePrimaryHarkReceiptOutbox(call.harkReceiptOutbox, binding);
  for (const entry of Object.values(box.entries)) {
    if (entry.body.evidence.kind !== 'primary_outcome') throw new Error('Broker Hark receipt requires a Primary outcome');
    receiptDelegation(call, entry.body.evidence.requestId);
  }
}
function assertHarkReceiptUpdate(previous: BrokerCall, next: BrokerCall): void {
  if (previous.harkReceiptOutbox === undefined && next.harkReceiptOutbox === undefined) return;
  const binding = primaryHarkReceiptBindingFromCall(next);
  if (!binding || !next.harkReceiptOutbox) throw new Error('Hark receipt outbox cannot be removed');
  let expected = previous.harkReceiptOutbox;
  for (const entry of Object.values(next.harkReceiptOutbox.entries)) {
    if (!previous.harkReceiptOutbox?.entries[entry.id]) expected = reducePrimaryHarkReceiptOutbox(expected, binding,
      { kind: 'enqueue', body: entry.body, observedAt: entry.observedAt });
  }
  if (canonicalJson(expected) !== canonicalJson(next.harkReceiptOutbox))
    throw new Error('Hark receipt delivery requires an explicit transition');
  for (const entry of Object.values(previous.harkReceiptOutbox?.entries ?? {})) {
    if (entry.body.evidence.kind !== 'primary_outcome') throw new Error('Broker Hark receipt requires a Primary outcome');
    const before = receiptDelegation(previous, entry.body.evidence.requestId).primaryResult!;
    const after = receiptDelegation(next, entry.body.evidence.requestId).primaryResult!;
    if (canonicalJson({ state: before.state, assistant: before.assistant })
      !== canonicalJson({ state: after.state, assistant: after.assistant }))
      throw new Error('Hark receipt terminal result cannot change');
  }
}

interface State { version: 1; calls: Record<string, BrokerCall> }

function validateBrokerState(state: State): void {
  if (state.version !== 1 || !state.calls || typeof state.calls !== 'object' || Array.isArray(state.calls)) throw new Error('Invalid broker call store');
  for (const [id, call] of Object.entries(state.calls)) {
    if (call.id !== id || !call.requestId || !call.number || !call.createdAt || !call.delegations) throw new Error('Invalid broker call record');
    validateAssignment(call);
    validatePrimaryIdentity(call);
    validateGenerations(call);
    validateSelectedQuestionCall(call);
    validateSelectedVoice(call);
    validateCallerRouting(call);
    validateBrokerAdmissionRefusals(call);
    validateHarkJournal(call);
    validateHarkDispatch(call);
    validateInboundJournal(call);
    validateVerificationCallbackCall(call);
    validateRestrictedSetupCall(call);
    validateHarkReceiptCall(call);
    validateSelectedOutboundObservation(call);
  }
  validateOutboundRetries(state.calls);
}

/** One process owns call admission. Provider dispatch always follows a durable state write. */
export class BrokerStore {
  private state: State;
  private readonly file: string;
  private readonly lock: string;
  private readonly lockId = randomUUID();
  private readonly parent: DurableParentBinding;
  private closed = false;
  private persistenceUncertain = false;

  constructor(readonly directory: string) {
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    this.file = path.join(directory, 'calls.json');
    this.lock = path.join(directory, 'writer.lock');
    this.parent = bindDurableFileParent(this.file);
    try { this.acquire(); } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
      const previous = JSON.parse(fs.readFileSync(this.lock, 'utf8')) as { pid: number };
      if (!Number.isInteger(previous.pid) || previous.pid < 1) throw new Error('Invalid broker writer lock');
      try { process.kill(previous.pid, 0); throw new Error('Another broker process owns this state'); }
      catch (probe) { if ((probe as NodeJS.ErrnoException).code !== 'ESRCH') throw probe; }
      fs.unlinkSync(this.lock);
      this.acquire();
    }
    try {
      const existing = fs.existsSync(this.file);
      this.state = existing ? JSON.parse(fs.readFileSync(this.file, 'utf8')) : { version: 1, calls: {} };
      validateBrokerState(this.state);
      // An empty store must have a durable journal before it can prove absence.
      if (!existing) this.persist(this.state);
    } catch (error) { this.close(); throw error; }
  }

  private acquire() {
    const fd = fs.openSync(this.lock, 'wx', 0o600);
    try { fs.writeFileSync(fd, JSON.stringify({ pid: process.pid, id: this.lockId })); fs.fsyncSync(fd); }
    finally { fs.closeSync(fd); }
  }

  private persist(next: State) {
    if (this.closed) throw new Error('Broker store is closed');
    if (this.persistenceUncertain) throw new Error('Broker journal write outcome is uncertain');
    // All write paths share this guard, including selected replies and ordinary queue updates.
    for (const [id, call] of Object.entries(next.calls)) {
      validateCallerRouting(call, this.state?.calls[id]);
      validateBrokerAdmissionRefusals(call, this.state?.calls[id]);
      validateSelectedOutboundObservation(call, this.state?.calls[id]);
      validateInboundJournal(call, this.state?.calls[id]);
      validateVerificationCallbackCall(call, this.state?.calls[id]);
      validateRestrictedSetupCall(call, this.state?.calls[id]);
    }
    validateOutboundRetries(next.calls, this.state?.calls);
    try { durablyReplaceJsonFile(this.file, JSON.stringify(next), fs, this.parent); }
    catch (error) {
      // A post-rename failure can leave next on disk and the old state in memory.
      // Further writes must not overwrite that reservation from the stale cache.
      this.persistenceUncertain = true;
      throw error;
    }
    this.state = next;
  }

  /** Full existing journal, with the same record validation used at startup. */
  readCallRequestJournal(): CallRequestJournalAvailability {
    if (this.closed) return { state: 'unavailable' };
    if (this.persistenceUncertain) return { state: 'uncertain' };
    const owned = () => {
      if (!durableFileParentIsCurrent(this.parent)) return false;
      const lock = JSON.parse(fs.readFileSync(this.lock, 'utf8')) as { pid?: unknown; id?: unknown };
      return lock.pid === process.pid && lock.id === this.lockId;
    };
    try {
      if (!owned()) return { state: 'unavailable' };
      const file = fs.lstatSync(this.file);
      if (!file.isFile() || file.isSymbolicLink()) return { state: 'unavailable' };
      const disk: State = JSON.parse(fs.readFileSync(this.file, 'utf8'));
      validateBrokerState(disk);
      if (canonicalJson(disk) !== canonicalJson(this.state) || !owned()) return { state: 'unavailable' };
      return { state: 'available', journal: structuredClone(disk) };
    } catch { return { state: 'unavailable' }; }
  }

  get(id: string): BrokerCall | undefined { const call = this.state.calls[id]; return call && structuredClone(call); }
  bySid(sid: string): BrokerCall | undefined { return this.list().find(call => call.callSid === sid); }
  list(): BrokerCall[] { return structuredClone(Object.values(this.state.calls)).sort((a, b) => b.createdAt.localeCompare(a.createdAt)); }

  reserveInbound(input: { requestId: string; purpose: string; providerProof: InboundBeginIdentity['providerProof'] },
    policy: InboundWaitPolicy, now = new Date().toISOString(), enrollmentMode = false): { call: BrokerCall; existing: boolean } {
    if (typeof enrollmentMode !== 'boolean') throw new Error('Invalid inbound mode construction');
    const proof = structuredClone(input.providerProof);
    const callSid = proof.params.CallSid;
    if (input.requestId !== inboundRequestId(callSid) || typeof input.purpose !== 'string'
        || !input.purpose || input.purpose.length > 1500) throw new Error('Invalid inbound reservation');
    const existing = this.list().find(call => call.requestId === input.requestId || call.callSid === callSid);
    if (existing) {
      if (!existing.inbound || existing.requestId !== input.requestId || existing.callSid !== callSid
          || existing.purpose !== input.purpose || canonicalJson(existing.inbound.begin.providerProof) !== canonicalJson(proof))
        throw new Error('Inbound reservation conflicts with its saved record');
      validateInboundJournal(existing);
      // New runtime policy cannot renew a saved attempt's wait or media budget.
      return { call: existing, existing: true };
    }
    const id = randomUUID();
    const call: BrokerCall = { id, requestId: input.requestId, direction: 'inbound', purpose: input.purpose,
      number: proof.params.From, callSid, status: 'prepared', createdAt: now, updatedAt: now, delegations: {},
      inbound: createInboundJournal({ brokerCallId: id, providerProof: proof }, policy, now, enrollmentMode) };
    validateInboundJournal(call);
    validateSelectedVoice(call);
    this.persist({ version: 1, calls: { ...this.state.calls, [id]: call } });
    return { call: structuredClone(call), existing: false };
  }

  transitionInbound(id: string, event: InboundJournalEvent): BrokerCall {
    const previous = this.state.calls[id];
    if (!previous) throw new Error('Unknown broker call');
    const next = reduceInboundJournal(previous, event, new Date().toISOString());
    if (next === previous) return structuredClone(previous);
    return this.saveHarkCall(next);
  }

  reserve(input: Pick<BrokerCall, 'requestId' | 'direction' | 'purpose' | 'number'> & { callSid?: string; continuation?: CallContinuation }, origin?: 'authenticated_broker_control'): { call: BrokerCall; existing: boolean } {
    for (const field of ['outboundObservation', 'retryOf', 'retrySuccessor'])
      if (Object.hasOwn(input, field)) throw new OutboundRetryError('call_request_conflict');
    if (Object.prototype.hasOwnProperty.call(input, 'callerRouting')) throw new Error('Caller routing requires an admitted call');
    if (Object.prototype.hasOwnProperty.call(input, 'callAssignment')) throw new Error('Assignment must originate from control reservation');
    if (Object.prototype.hasOwnProperty.call(input, 'selectedQuestion') || Object.prototype.hasOwnProperty.call(input, 'selectedQuestionVoice')) throw new Error('Selected question requires an admitted call');
    const { continuation, ...ordinary } = input;
    const savedInput = { ...ordinary, ...callContinuationPatch(continuation) };
    const existing = this.list().find(call => call.requestId === input.requestId || (input.callSid && call.callSid === input.callSid));
    if (existing) {
      assertCallContinuationReplay(existing.continuation, continuation);
      if (existing.callAssignment?.origin === 'authenticated_hark_delivery') throw new Error('Call origin conflicts with its saved record');
      if (existing.number !== input.number || existing.direction !== input.direction || existing.purpose !== input.purpose || (input.callSid && existing.callSid !== input.callSid)) throw new Error('Call request identity conflicts with its saved record');
      return { call: existing, existing: true };
    }
    const now = new Date().toISOString();
    const call: BrokerCall = { id: randomUUID(), ...savedInput, status: 'prepared', createdAt: now, updatedAt: now, delegations: {} };
    if (origin !== undefined) {
      if (origin !== 'authenticated_broker_control') throw new Error('Invalid call assignment origin');
      call.callAssignment = { origin, value: phoneCallAssignmentSchema.parse({ assignmentId: call.requestId,
        brokerCallId: call.id, revision: 1, purpose: call.purpose }) };
    }
    validateAssignment(call);
    validateSelectedVoice(call);
    this.persist({ version: 1, calls: { ...this.state.calls, [call.id]: call } });
    return { call: structuredClone(call), existing: false };
  }

  /** New selected calls bind provider identity before any asynchronous readiness or dial work.
   * A retry child and its predecessor edge use the same durable replacement. */
  reserveSelectedOutbound(input: Pick<BrokerCall, 'requestId' | 'purpose' | 'number'> & {
    continuation: CallContinuation; retryOf?: OutboundRetryOf;
  }, providerIdentity: OutboundProviderIdentity): { call: BrokerCall; existing: boolean } {
    if (this.readCallRequestJournal().state !== 'available') throw new OutboundRetryError('call_request_unavailable');
    const retry = outboundRetryOfPatch(input.retryOf);
    const continuation = callContinuationPatch(input.continuation).continuation;
    if (!continuation) throw new OutboundRetryError('call_request_conflict');
    const existing = Object.values(this.state.calls).find(call => call.requestId === input.requestId);
    if (existing) {
      assertOutboundRetryReplay(existing.retryOf, retry.retryOf);
      try { assertCallContinuationReplay(existing.continuation, continuation); }
      catch { throw new OutboundRetryError('call_request_conflict'); }
      if (existing.direction !== 'outbound' || existing.purpose !== input.purpose || existing.number !== input.number
        || existing.callAssignment?.origin !== 'authenticated_broker_control')
        throw new OutboundRetryError('call_request_conflict');
      // Historical recovery retains its old absence/identity; no new instrumentation or admission.
      return { call: structuredClone(existing), existing: true };
    }
    const now = new Date().toISOString(), id = randomUUID();
    const call: BrokerCall = { id, requestId: input.requestId, direction: 'outbound', purpose: input.purpose,
      number: input.number, continuation, ...retry, status: 'prepared', createdAt: now, updatedAt: now, delegations: {},
      callAssignment: { origin: 'authenticated_broker_control', value: phoneCallAssignmentSchema.parse({
        assignmentId: input.requestId, brokerCallId: id, revision: 1, purpose: input.purpose }) } };
    validateAssignment(call);
    call.outboundObservation = createOutboundObservation(call, providerIdentity);
    let calls = { ...this.state.calls, [id]: call };
    if (call.retryOf) {
      const prepared = prepareOutboundRetry<BrokerCall>(this.state.calls, { ...call, retryOf: call.retryOf });
      calls = { ...this.state.calls, [prepared.predecessor.id]: prepared.predecessor, [id]: prepared.child };
    }
    this.persist({ version: 1, calls });
    return { call: structuredClone(this.state.calls[id]!), existing: false };
  }

  /** No provider operation is authorized until this transition's durable write succeeds. */
  observeSelectedOutbound(id: string, event: OutboundObservationEvent): BrokerCall {
    if (this.readCallRequestJournal().state !== 'available') throw new OutboundRetryError('call_request_unavailable');
    const previous = this.state.calls[id];
    if (!previous) throw new OutboundRetryError('call_request_unavailable');
    const observation = reduceOutboundObservation(previous, event);
    if (observation.accepted && Object.values(this.state.calls).some(call => call.id !== id
      && call.callSid === observation.accepted!.callSid)) throw new OutboundRetryError('call_request_unavailable');
    const call: BrokerCall = { ...previous, outboundObservation: observation, updatedAt: event.observedAt,
      ...(observation.accepted ? { callSid: observation.accepted.callSid } : {}),
      ...(event.type === 'dispatch_started' ? { status: 'dialing' as const } : {}) };
    this.persist({ version: 1, calls: { ...this.state.calls, [id]: call } });
    return structuredClone(call);
  }

  /** Only the API's resolved Hark snapshot supplies this reservation. */
  reserveHark(value: unknown, notificationDigest?: string): { call: BrokerCall; existing: boolean } {
    if (notificationDigest !== undefined) dispatchDigest.parse(notificationDigest);
    const resolved = resolvedHarkAssignmentSchema.parse(value);
    const assignment = resolved.callAssignment;
    const id = assignment.brokerCallId;
    const existing = this.list().find(call => call.id === id || call.requestId === id);
    if (existing) {
      if (existing.id !== id || existing.requestId !== id || existing.direction !== 'outbound'
          || existing.number !== resolved.to || existing.purpose !== assignment.purpose
          || existing.callAssignment?.origin !== 'authenticated_hark_delivery'
          || canonicalJson(existing.callAssignment.value) !== canonicalJson(assignment))
        throw new Error('Hark delivery conflicts with its saved record');
      if (existing.harkDispatch?.notificationDigest !== notificationDigest)
        throw new Error('Hark notification conflicts with its saved record');
      return { call: existing, existing: true };
    }
    const now = new Date().toISOString();
    const call: BrokerCall = { id, requestId: id, direction: 'outbound', purpose: assignment.purpose,
      number: resolved.to, status: 'prepared', createdAt: now, updatedAt: now, delegations: {},
      callAssignment: { origin: 'authenticated_hark_delivery', value: assignment } };
    if (notificationDigest !== undefined) call.harkDispatch = { version: 1, notificationDigest,
      state: 'not_dispatched', reservedAt: now };
    validateAssignment(call);
    validateHarkDispatch(call);
    validateSelectedVoice(call);
    this.persist({ version: 1, calls: { ...this.state.calls, [id]: call } });
    return { call: structuredClone(call), existing: false };
  }

  /** Caller authentication is separate; this only checks the saved notification identity. */
  byHarkNotification(deliveryId: string, notificationDigest: string): BrokerCall | undefined {
    dispatchDigest.parse(notificationDigest);
    const call = this.list().find(item => item.id === deliveryId || item.requestId === deliveryId);
    if (!call) return undefined;
    if (call.id !== deliveryId || call.requestId !== deliveryId || call.callAssignment?.origin !== 'authenticated_hark_delivery')
      throw new Error('Hark delivery conflicts with its saved record');
    if (!call.harkDispatch) throw new Error('Hark notification identity unavailable');
    if (call.harkDispatch.notificationDigest !== notificationDigest) throw new Error('Hark notification conflicts with its saved record');
    validateHarkDispatch(call);
    return call;
  }

  beginHarkDispatch(id: string, observedAt: string): BrokerCall {
    dispatchTime.parse(observedAt);
    const previous = this.state.calls[id];
    if (!previous) throw new Error('Unknown broker call');
    validateHarkDispatch(previous);
    if (!previous.harkDispatch) throw new Error('Hark dispatch history unavailable');
    if (previous.harkDispatch.state !== 'not_dispatched') throw new Error('Hark dispatch already started');
    const { failure, ...current } = previous;
    return this.saveHarkCall({ ...current, status: 'dialing', updatedAt: observedAt,
      harkDispatch: { ...previous.harkDispatch, state: 'dispatch_started', dispatchStartedAt: observedAt,
        ...(failure ? { preDispatchFailure: { observedAt: previous.updatedAt, code: failure.code, message: failure.message } } : {}) } });
  }

  acceptHarkDispatch(id: string, sid: string, observedAt: string): BrokerCall {
    dispatchTime.parse(observedAt);
    if (!/^CA[0-9a-fA-F]{32}$/.test(sid)) throw new Error('Invalid call SID');
    const previous = this.state.calls[id];
    if (!previous) throw new Error('Unknown broker call');
    validateHarkDispatch(previous);
    if (!previous.harkDispatch) throw new Error('Hark dispatch history unavailable');
    if (previous.harkDispatch.state === 'not_dispatched') throw new Error('Hark dispatch was not started');
    if (previous.harkDispatch.state === 'provider_accepted') {
      if (previous.callSid !== sid) throw new Error('Call SID cannot change');
      return structuredClone(previous);
    }
    if (Object.values(this.state.calls).some(call => call.id !== id && call.callSid === sid)) throw new Error('Call SID is already bound');
    return this.saveHarkCall({ ...previous, callSid: sid, updatedAt: observedAt,
      harkDispatch: { ...previous.harkDispatch, state: 'provider_accepted', providerAcceptedAt: observedAt } });
  }

  recordRestrictedSetup(id: string, value: RestrictedSetupJournal): BrokerCall {
    const previous = this.state.calls[id];
    if (!previous) throw new Error('Unknown restricted setup call');
    const call = { ...previous, restrictedSetup: restrictedSetupJournalSchema.parse(value), updatedAt: new Date().toISOString() };
    validateRestrictedSetupCall(call, previous);
    this.persist({ version: 1, calls: { ...this.state.calls, [id]: call } });
    return structuredClone(call);
  }

  /** API permit supplies the destination, request identity and financial reservation. */
  reserveVerificationCallback(permit: PhoneVerificationCallbackPermit, observedAt: string): { call: BrokerCall; existing: boolean } {
    const exactPermit = phoneVerificationCallbackPermitSchema.parse(permit);
    const id = exactPermit.request.requestId;
    const matches = Object.values(this.state.calls).filter(call => call.id === id || call.requestId === id);
    if (matches.length) {
      const prior = matches[0];
      if (matches.length !== 1 || !prior?.verificationCallback
        || canonicalJson(prior.verificationCallback.permit) !== canonicalJson(exactPermit))
        throw new Error('Verification callback reservation conflicts');
      validateVerificationCallbackCall(prior);
      return { call: structuredClone(prior), existing: true };
    }
    const candidate = reserveVerificationCallback(exactPermit, observedAt);
    const call: BrokerCall = { id, requestId: id, direction: 'outbound', number: candidate.permit.to,
      purpose: verificationCallbackPurpose, createdAt: observedAt, updatedAt: observedAt,
      status: 'prepared', delegations: {}, verificationCallback: candidate };
    this.persist({ version: 1, calls: { ...this.state.calls, [id]: call } });
    return { call: structuredClone(call), existing: false };
  }

  recordVerificationCallback(id: string, value: VerificationCallbackJournal): BrokerCall {
    const previous = this.state.calls[id];
    if (!previous?.verificationCallback) throw new Error('Unknown verification callback');
    const journal = parseVerificationCallbackJournal(value);
    const sid = journal.provider?.callSid ?? journal.answer?.proof.params.CallSid;
    if (sid && Object.values(this.state.calls).some(call => call.id !== id && call.callSid === sid))
      throw new Error('Call SID is already bound');
    const call: BrokerCall = { ...previous, verificationCallback: journal,
      ...(journal.provider ? { status: journal.provider.status } : {}),
      ...(journal.providerTerminal ? { endedAt: previous.endedAt ?? journal.providerTerminal.observedAt } : {}),
      ...(sid ? { callSid: sid } : {}), updatedAt: new Date().toISOString() };
    validateVerificationCallbackCall(call, previous);
    this.persist({ version: 1, calls: { ...this.state.calls, [id]: call } });
    return structuredClone(call);
  }

  /** The IDs come from the authenticated API binding response, never caller input. */
  recordPrimaryBinding(id: string, response: PrimaryCall): BrokerCall {
    const previous = this.state.calls[id];
    if (!previous) throw new Error('Unknown broker call');
    if (previous.inbound) throw new Error('Inbound context requires an explicit transition');
    const binding = structuredClone(response);
    if (!previous.callSid || binding.callSid !== previous.callSid || binding.state !== 'open' || previous.primaryClosed)
      throw new Error('Primary call identity mismatch');
    const context = binding.context;
    if (!context || !primaryResultId.safeParse(context.primaryDexterId).success || !primaryResultId.safeParse(context.threadId).success
        || typeof context.displayName !== 'string' || !Array.isArray(context.messages)
        || context.messages.some(message => !['user', 'assistant'].includes(message.role) || typeof message.text !== 'string'))
      throw new Error('Invalid Primary binding context');
    if (previous.primary?.callAssignmentRef && !sameCallAssignmentRef(previous.primary.callAssignmentRef, binding.callAssignmentRef))
      throw new Error('Call assignment reference cannot change');
    return this.saveHarkCall({ ...previous, updatedAt: new Date().toISOString(), primary: {
      bindingId: binding.bindingId, primaryDexterId: context.primaryDexterId, threadId: context.threadId,
      name: context.displayName, context: context.messages.map(message => `${message.role}: ${message.text}`).join('\n').slice(-8_000),
      ...(binding.callAssignmentRef === undefined ? {} : { callAssignmentRef: binding.callAssignmentRef }),
    } });
  }

  /** The host saves the seed before association, then each captured/reconciled transition atomically. */
  recordSelectedQuestion(id: string, value: SelectedQuestionJournal): SelectedQuestionJournal {
    const previous = this.state.calls[id];
    if (!previous) throw new Error('Unknown broker call');
    const selectedQuestion = parseSelectedQuestionJournal(structuredClone(value));
    const call = { ...previous, selectedQuestion, updatedAt: new Date().toISOString() };
    validateSelectedQuestionCall(call, previous);
    validateSelectedVoice(call, previous);
    if (canonicalJson(previous.selectedQuestion) === canonicalJson(selectedQuestion)) return structuredClone(selectedQuestion);
    this.saveHarkCall(call);
    return structuredClone(selectedQuestion);
  }

  /** Source/presentation evidence is persisted independently before answer admission. */
  recordSelectedQuestionVoice(id: string, value: SelectedQuestionVoiceEvidence): SelectedQuestionVoiceEvidence {
    const previous = this.state.calls[id];
    if (!previous) throw new Error('Unknown broker call');
    const selectedQuestionVoice = parseSelectedQuestionVoiceEvidence(structuredClone(value));
    const call = { ...previous, selectedQuestionVoice, updatedAt: new Date().toISOString() };
    validateSelectedVoice(call, previous);
    if (canonicalJson(previous.selectedQuestionVoice) === canonicalJson(selectedQuestionVoice)) return structuredClone(selectedQuestionVoice);
    this.saveHarkCall(call);
    return structuredClone(selectedQuestionVoice);
  }

  /** Exact frozen source and resolver inputs are durable before classification. */
  prepareCallerRoute(id: string, input: CallerRoutePreparation): CallerRouteRecord {
    const previous = this.state.calls[id];
    if (!previous) throw new Error('Unknown broker call');
    const { journal, record } = prepareCallerRouting(previous, structuredClone(input));
    if (canonicalJson(previous.callerRouting) !== canonicalJson(journal))
      this.saveHarkCall({ ...previous, callerRouting: journal, updatedAt: new Date().toISOString() });
    return structuredClone(record);
  }

  decideCallerRoute(id: string, ref: CallerRouteRef,
    response: { inputDigest: string; decision: CallerRouteDecision }): CallerRouteRecord {
    const previous = this.state.calls[id];
    if (!previous) throw new Error('Unknown broker call');
    const journal = decideCallerRouting(previous, ref, structuredClone(response));
    if (canonicalJson(previous.callerRouting) !== canonicalJson(journal))
      this.saveHarkCall({ ...previous, callerRouting: journal, updatedAt: new Date().toISOString() });
    return structuredClone(journal.records.find(record => record.frozen.turn.sourceEventId === ref.sourceEventId)!);
  }

  prepareCallerIntent(id: string, ref: CallerRouteRef,
    operation: { operationId: string; input: CallerIntentInput }): CallerRouteRecord {
    const previous = this.state.calls[id];
    if (!previous) throw new Error('Unknown broker call');
    const journal = prepareCallerIntent(previous, ref, structuredClone(operation));
    if (canonicalJson(previous.callerRouting) !== canonicalJson(journal))
      this.saveHarkCall({ ...previous, callerRouting: journal, updatedAt: new Date().toISOString() });
    const record = journal.records.find(record => record.frozen.turn.sourceEventId === ref.sourceEventId);
    if (!record) throw new Error('caller_route_not_found');
    return structuredClone(record);
  }

  recordCallerIntentResponse(id: string, ref: CallerRouteRef, response: CallerIntentResponse): CallerRouteRecord {
    const previous = this.state.calls[id];
    if (!previous) throw new Error('Unknown broker call');
    const journal = recordCallerIntentResponse(previous, ref, structuredClone(response));
    if (canonicalJson(previous.callerRouting) !== canonicalJson(journal))
      this.saveHarkCall({ ...previous, callerRouting: journal, updatedAt: new Date().toISOString() });
    const record = journal.records.find(record => record.frozen.turn.sourceEventId === ref.sourceEventId);
    if (!record) throw new Error('caller_route_not_found');
    return structuredClone(record);
  }

  captureCallerRoute(id: string, ref: CallerRouteRef, receipt: PrimaryTurnReceipt): CallerRouteRecord {
    const previous = this.state.calls[id];
    if (!previous) throw new Error('Unknown broker call');
    const journal = captureCallerRouting(previous, ref, structuredClone(receipt));
    if (canonicalJson(previous.callerRouting) !== canonicalJson(journal))
      this.saveHarkCall({ ...previous, callerRouting: journal, updatedAt: new Date().toISOString() });
    return structuredClone(journal.records.find(record => record.frozen.turn.sourceEventId === ref.sourceEventId)!);
  }

  update(id: string, patch: Partial<Omit<BrokerCall, 'id' | 'requestId' | 'number' | 'direction' | 'purpose' | 'createdAt' | 'callAssignment' | 'continuation' | 'selectedQuestion' | 'selectedQuestionVoice' | 'callerRouting' | 'harkJournal' | 'harkDispatch' | 'inbound' | 'outboundObservation' | 'retryOf' | 'retrySuccessor' | 'verificationCallback' | 'restrictedSetup'>>): BrokerCall {
    const previous = this.state.calls[id];
    if (!previous) throw new Error('Unknown broker call');
    // Runtime callers can bypass Omit. Check the same property snapshot that
    // will be persisted, so accessors cannot change a value after validation.
    const nextPatch = { ...patch };
    if (Object.hasOwn(nextPatch, 'restrictedSetup')) {
      if (canonicalJson((nextPatch as Partial<BrokerCall>).restrictedSetup) !== canonicalJson(previous.restrictedSetup))
        throw new Error('Restricted setup requires an explicit transition');
      delete (nextPatch as Partial<BrokerCall>).restrictedSetup;
    }
    if (Object.hasOwn(nextPatch, 'verificationCallback')) {
      if (canonicalJson((nextPatch as Partial<BrokerCall>).verificationCallback) !== canonicalJson(previous.verificationCallback))
        throw new Error('Verification callback requires an explicit transition');
      delete (nextPatch as Partial<BrokerCall>).verificationCallback;
    }
    for (const key of ['outboundObservation', 'retryOf', 'retrySuccessor'] as const) {
      if (!Object.hasOwn(nextPatch, key)) continue;
      if (canonicalJson((nextPatch as Partial<BrokerCall>)[key]) !== canonicalJson(previous[key]))
        throw new OutboundRetryError('call_request_conflict');
      delete (nextPatch as Partial<BrokerCall>)[key];
    }
    if (Object.prototype.hasOwnProperty.call(nextPatch, 'callerRouting')) {
      if (canonicalJson((nextPatch as Partial<BrokerCall>).callerRouting) !== canonicalJson(previous.callerRouting))
        throw new Error('Caller routing requires an explicit transition');
      delete (nextPatch as Partial<BrokerCall>).callerRouting;
    }
    if (Object.prototype.hasOwnProperty.call(nextPatch, 'selectedQuestionVoice')) {
      if (canonicalJson((nextPatch as Partial<BrokerCall>).selectedQuestionVoice) !== canonicalJson(previous.selectedQuestionVoice))
        throw new Error('Selected question voice evidence requires an explicit transition');
      delete (nextPatch as Partial<BrokerCall>).selectedQuestionVoice;
    }
    if (Object.prototype.hasOwnProperty.call(nextPatch, 'selectedQuestion')) {
      if (canonicalJson((nextPatch as Partial<BrokerCall>).selectedQuestion) !== canonicalJson(previous.selectedQuestion))
        throw new Error('Selected question journal requires an explicit transition');
      delete (nextPatch as Partial<BrokerCall>).selectedQuestion;
    }
    if (Object.prototype.hasOwnProperty.call(nextPatch, 'continuation')) {
      assertCallContinuationReplay(previous.continuation, (nextPatch as Partial<BrokerCall>).continuation);
      delete (nextPatch as Partial<BrokerCall>).continuation;
    }
    if (Object.prototype.hasOwnProperty.call(nextPatch, 'mediaStartedAt'))
      assertInboundMediaStart(previous, nextPatch.mediaStartedAt, new Date().toISOString());
    for (const key of ['id', 'requestId', 'number', 'direction', 'purpose', 'createdAt'] as const) {
      if (Object.prototype.hasOwnProperty.call(nextPatch, key)
          && (nextPatch as Partial<BrokerCall>)[key] !== previous[key]) throw new Error('Call identity cannot change');
    }
    if (Object.prototype.hasOwnProperty.call(nextPatch, 'callAssignment')) {
      const assignment = (nextPatch as Partial<BrokerCall>).callAssignment;
      const normalized = assignment === undefined ? undefined : brokerCallAssignmentSchema.parse(assignment);
      if (canonicalJson(normalized) !== canonicalJson(previous.callAssignment)) throw new Error('Call assignment cannot change');
      (nextPatch as Partial<BrokerCall>).callAssignment = previous.callAssignment;
    }
    if (Object.prototype.hasOwnProperty.call(nextPatch, 'harkJournal')) {
      if (canonicalJson((nextPatch as Partial<BrokerCall>).harkJournal) !== canonicalJson(previous.harkJournal))
        throw new Error('Hark journal requires an observed lifecycle transition');
      delete (nextPatch as Partial<BrokerCall>).harkJournal;
    }
    if (Object.prototype.hasOwnProperty.call(nextPatch, 'harkDispatch')) {
      if (canonicalJson((nextPatch as Partial<BrokerCall>).harkDispatch) !== canonicalJson(previous.harkDispatch))
        throw new Error('Hark dispatch requires an explicit transition');
      delete (nextPatch as Partial<BrokerCall>).harkDispatch;
    }
    if (Object.prototype.hasOwnProperty.call(nextPatch, 'inbound')) {
      if (canonicalJson((nextPatch as Partial<BrokerCall>).inbound) !== canonicalJson(previous.inbound))
        throw new Error('Inbound journal requires an explicit transition');
      delete (nextPatch as Partial<BrokerCall>).inbound;
    }
    if (Object.prototype.hasOwnProperty.call(nextPatch, 'primary')) {
      nextPatch.primary = nextPatch.primary && structuredClone(nextPatch.primary);
      if (previous.inbound && canonicalJson(nextPatch.primary) !== canonicalJson(previous.primary))
        throw new Error('Inbound context requires an explicit transition');
      if (previous.callAssignment && previous.primary && nextPatch.primary?.bindingId !== previous.primary.bindingId)
        throw new Error('Assigned Primary binding cannot change');
      if (previous.primary?.callAssignmentRef && !sameCallAssignmentRef(previous.primary.callAssignmentRef, nextPatch.primary?.callAssignmentRef))
        throw new Error('Call assignment reference cannot change');
      for (const field of ['primaryDexterId', 'threadId'] as const) {
        if (nextPatch.primary?.[field] !== previous.primary?.[field])
          throw new Error('Primary result identity requires an API binding response');
      }
    }
    if (Object.prototype.hasOwnProperty.call(nextPatch, 'primaryClosed')) {
      const closed = nextPatch.primaryClosed;
      if (previous.inbound) {
        if (canonicalJson(closed) !== canonicalJson(previous.primaryClosed))
          throw new Error('Inbound closure requires an explicit receipt transition');
        delete nextPatch.primaryClosed;
      } else if (!closed || closed.state !== 'closed' || closed.bindingId !== previous.primary?.bindingId || closed.callSid !== previous.callSid
          || (previous.primaryClosed && (closed.bindingId !== previous.primaryClosed.bindingId || closed.callSid !== previous.primaryClosed.callSid)))
        throw new Error('Closed Primary binding cannot change');
    }
    if (Object.prototype.hasOwnProperty.call(nextPatch, 'callSid')) {
      if (previous.callSid !== undefined && previous.callSid !== nextPatch.callSid) throw new Error('Call SID cannot change');
      if (nextPatch.callSid !== undefined && (typeof nextPatch.callSid !== 'string' || !/^CA[0-9a-fA-F]{32}$/.test(nextPatch.callSid)))
        throw new Error('Invalid call SID');
    }
    if (nextPatch.callSid && Object.values(this.state.calls).some(call => call.id !== id && call.callSid === nextPatch.callSid)) throw new Error('Call SID is already bound');
    const rank: Partial<Record<CallStatus, number>> = { prepared: 0, dialing: 1, queued: 2, ringing: 3, 'in-progress': 4 };
    if (nextPatch.status && (terminalCallStatuses.has(previous.status) || (previous.status !== 'unknown' && ((rank[nextPatch.status] ?? 9) < (rank[previous.status] ?? -1))))) delete nextPatch.status;
    if (nextPatch.transcriptRevision !== undefined && nextPatch.transcriptRevision < (previous.transcriptRevision ?? 0)) { delete nextPatch.transcript; delete nextPatch.transcriptRevision; }
    const call: BrokerCall = { ...previous, ...nextPatch, updatedAt: new Date().toISOString() };
    assertPrimaryIdentityUnchanged(previous, call);
    validatePrimaryIdentity(call);
    validateGenerations(call, previous);
    validateSelectedQuestionCall(call, previous);
    validateSelectedVoice(call, previous);
    validateAssignment(call);
    validateHarkJournal(call);
    validateHarkDispatch(call);
    validateInboundJournal(call);
    validateVerificationCallbackCall(call);
    validateRestrictedSetupCall(call);
    validateHarkReceiptCall(call);
    assertHarkReceiptUpdate(previous, call);
    this.persist({ version: 1, calls: { ...this.state.calls, [id]: call } });
    return structuredClone(call);
  }

  private saveHarkCall(call: BrokerCall): BrokerCall {
    const previous = this.state.calls[call.id];
    if (previous) assertCallContinuationReplay(previous.continuation, call.continuation);
    if (previous) assertPrimaryIdentityUnchanged(previous, call);
    validatePrimaryIdentity(call);
    validateGenerations(call, previous);
    validateSelectedQuestionCall(call, previous);
    validateSelectedVoice(call, previous);
    validateAssignment(call);
    validateHarkJournal(call);
    validateHarkDispatch(call);
    validateInboundJournal(call);
    validateVerificationCallbackCall(call);
    validateRestrictedSetupCall(call);
    validateHarkReceiptCall(call);
    this.persist({ version: 1, calls: { ...this.state.calls, [call.id]: call } });
    return structuredClone(call);
  }

  observeHarkProvider(id: string, status: HarkProviderStatus, observedAt: string, endedTiming?: HarkEndedTiming): BrokerCall {
    const previous = this.state.calls[id];
    if (!previous) throw new Error('Unknown broker call');
    const mapped = status === 'in-progress' ? 'answered' : status;
    const harkJournal = observeHark(previous, mapped, observedAt, endedTiming);
    const providerStatus = harkJournal.providerStatus;
    const nextStatus = providerStatus === 'answered' || providerStatus === 'streaming' ? 'in-progress' : providerStatus;
    return this.saveHarkCall({ ...previous, harkJournal, status: nextStatus,
      ...(harkJournal.ended ? { endedAt: harkJournal.ended.observedAt } : {}), updatedAt: new Date().toISOString() });
  }

  observeHarkMediaStarted(id: string, observedAt: string): BrokerCall {
    const previous = this.state.calls[id];
    if (!previous) throw new Error('Unknown broker call');
    const harkJournal = observeHark(previous, 'streaming', observedAt);
    // A late/replayed media callback cannot reopen a terminal provider call.
    const closed = Boolean(harkJournal.ended);
    return this.saveHarkCall({ ...previous, harkJournal,
      ...(closed ? {} : { status: 'in-progress' as const, mediaStartedAt: previous.mediaStartedAt ?? observedAt }),
      updatedAt: new Date().toISOString() });
  }

  pendingHarkEvents(id?: string): Array<{ callId: string; event: HarkJournalEvent }> {
    const calls = id === undefined ? Object.values(this.state.calls) : [this.state.calls[id]].filter(call => call !== undefined);
    return calls.flatMap(call => pendingHarkEvents(call).map(event => ({ callId: call.id, event })));
  }

  recordHarkAttempt(id: string, eventId: string, bodyDigest: string, attemptedAt: string): HarkJournalEvent {
    const call = this.get(id);
    const item = call?.harkJournal?.events[eventId];
    if (!call || !item || item.bodyDigest !== bodyDigest) throw new Error('Unknown Hark event identity');
    if (!Number.isFinite(Date.parse(attemptedAt)) || !/^\d{4}-\d{2}-\d{2}T/.test(attemptedAt)) throw new Error('Invalid attempt time');
    if (!pendingHarkEvents(call).some(event => event.id === eventId)) throw new Error('Hark event is not ready for delivery');
    item.attempts += 1;
    item.lastAttemptAt = attemptedAt;
    this.saveHarkCall(call);
    return structuredClone(item);
  }

  settleHarkEvent(id: string, eventId: string, bodyDigest: string, outcome: HarkDeliveryOutcome): HarkJournalEvent {
    const call = this.get(id);
    const item = call?.harkJournal?.events[eventId];
    if (!call?.harkJournal || !item || item.bodyDigest !== bodyDigest) throw new Error('Unknown Hark event identity');
    call.harkJournal.events[eventId] = settleHarkJournalEvent(item, outcome);
    this.saveHarkCall(call);
    return structuredClone(call.harkJournal.events[eventId]!);
  }

  listPrimaryHarkReceiptOutboxes(): Array<{ binding: PrimaryHarkReceiptBinding; outbox: PrimaryHarkReceiptOutbox }> {
    return Object.values(this.state.calls).flatMap(call => {
      if (!call.harkReceiptOutbox) return [];
      validateHarkReceiptCall(call);
      return [{ binding: primaryHarkReceiptBindingFromCall(call)!, outbox: structuredClone(call.harkReceiptOutbox) }];
    });
  }

  transitionPrimaryHarkReceiptOutbox(id: string, binding: PrimaryHarkReceiptBinding,
    command: PrimaryHarkReceiptCommand): PrimaryHarkReceiptOutbox {
    const call = this.get(id);
    if (!call?.harkReceiptOutbox || canonicalJson(primaryHarkReceiptBindingFromCall(call)) !== canonicalJson(binding))
      throw new Error('Hark receipt call identity mismatch');
    if (command.kind === 'enqueue') throw new Error('Hark receipt enqueue requires an atomic result update');
    call.harkReceiptOutbox = reducePrimaryHarkReceiptOutbox(call.harkReceiptOutbox, binding, command);
    this.saveHarkCall(call);
    return structuredClone(call.harkReceiptOutbox);
  }

  close() {
    if (this.closed) return;
    this.closed = true;
    try { const lock = JSON.parse(fs.readFileSync(this.lock, 'utf8')); if (lock.id === this.lockId) fs.unlinkSync(this.lock); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
  }
}
