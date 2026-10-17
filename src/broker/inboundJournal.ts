import { createHash } from 'node:crypto';
import { inboundModeSchema, enrollmentCommandSchema, enrollmentCompletionRecoverySchema, enrollmentTransportErrorSchema,
  sameEnrollmentCommand, type InboundMode, type EnrollmentCommand, type EnrollmentCompletionRecovery,
  type EnrollmentTransportError } from './inboundEnrollment.js';
import { z } from 'zod';
import { canonicalJson } from '../outbound/canonical.js';
import { inboundBeginIdentitySchema, inboundProviderProofSchema, inboundBeginRef, inboundApprovalRefSchema, recoverInboundResultSchema,
  consumedInboundClosureReceiptSchema, type InboundBeginIdentity, type InboundApprovalRef,
  type RecoverInboundResult, type ConsumedInboundClosureReceipt } from './inboundContract.js';
import type { PrimaryCall } from './primaryApi.js';
import type { BrokerCall } from './store.js';

export interface InboundWaitPolicy { version: 1; waitMs: number; mediaGraceMs: number }
export interface InboundJournal {
  version: 1;
  mode?: InboundMode;
  begin: InboundBeginIdentity;
  beginBodyDigest: string;
  policy: InboundWaitPolicy;
  waitUntil: string;
  /** Ordering evidence only; the API independently verifies carrier/account admission. */
  continuation?: { observedAt: string; beginBodyDigest: string; providerProof: InboundBeginIdentity['providerProof'] };
  admissionRefusal?: { observedAt: string; beginBodyDigest: string; status: 403; retryable: false;
    code: 'inbound_carrier_attestation_required' | 'inbound_enrollment_required' };
  /** Saved by earlier releases: one admission contact made before the continuation. New calls
   * begin admission only after the continuation; the field is validated for saved journals. */
  earlyAdmission?: { startedAt: string; beginBodyDigest: string };
  beginAttemptedAt?: string;
  consumeAttemptedAt?: string;
  remote?: RecoverInboundResult;
  issuedExpiresAt?: string;
  approvalRef?: InboundApprovalRef;
  mediaStartBy?: string;
  localStop?: { observedAt: string; reason: string };
  cancelRequestedAt?: string;
  closureRequestedAt?: string;
  closure?: ConsumedInboundClosureReceipt;
}
export type InboundJournalEvent =
  | { type: 'enrollment_dispatch'; command: EnrollmentCommand; observedAt: string }
  | { type: 'enrollment_observation'; result: EnrollmentCompletionRecovery; observedAt: string }
  | { type: 'enrollment_error'; error: EnrollmentTransportError; observedAt: string }
  | { type: 'enrollment_end_requested' | 'enrollment_end_acknowledged'; observedAt: string }
  | { type: 'begin_attempted' | 'consume_attempted' | 'cancel_requested' | 'close_requested'; observedAt: string }
  | { type: 'continuation'; providerProof: InboundBeginIdentity['providerProof']; observedAt: string }
  | { type: 'admission_refused'; code: 'inbound_carrier_attestation_required' | 'inbound_enrollment_required'; observedAt: string }
  | { type: 'stop'; reason: string; observedAt: string }
  | { type: 'outcome'; outcome: RecoverInboundResult; observedAt?: string }
  | { type: 'closure'; receipt: ConsumedInboundClosureReceipt; observedAt?: string }
  | { type: 'context'; context: PrimaryCall; observedAt?: string };

const time = z.string().datetime({ offset: true });
const primaryResultId = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
export const inboundWaitPolicySchema = z.object({ version: z.literal(1),
  waitMs: z.number().int().safe().positive(), mediaGraceMs: z.number().int().safe().positive() }).strict();
const journalSchema = z.object({
  version: z.literal(1), begin: inboundBeginIdentitySchema, beginBodyDigest: z.string().regex(/^[0-9a-f]{64}$/),
  mode: inboundModeSchema.optional(),
  policy: inboundWaitPolicySchema, waitUntil: time,
  continuation: z.object({ observedAt: time, beginBodyDigest: z.string().regex(/^[0-9a-f]{64}$/),
    providerProof: inboundProviderProofSchema }).strict().optional(),
  admissionRefusal: z.object({ observedAt: time, beginBodyDigest: z.string().regex(/^[0-9a-f]{64}$/),
    status: z.literal(403), retryable: z.literal(false),
    code: z.enum(['inbound_carrier_attestation_required', 'inbound_enrollment_required']) }).strict().optional(),
  earlyAdmission: z.object({ startedAt: time, beginBodyDigest: z.string().regex(/^[0-9a-f]{64}$/) }).strict().optional(),
  beginAttemptedAt: time.optional(), consumeAttemptedAt: time.optional(),
  remote: recoverInboundResultSchema.optional(), issuedExpiresAt: time.optional(), approvalRef: inboundApprovalRefSchema.optional(),
  mediaStartBy: time.optional(), localStop: z.object({ observedAt: time, reason: z.string().min(1).max(100) }).strict().optional(),
  cancelRequestedAt: time.optional(), closureRequestedAt: time.optional(), closure: consumedInboundClosureReceiptSchema.optional(),
}).strict();
const same = (left: unknown, right: unknown) => canonicalJson(left) === canonicalJson(right);
const timestampAfter = (timestamp: string, duration: number) => {
  time.parse(timestamp);
  const result = new Date(Date.parse(timestamp) + duration);
  if (!Number.isFinite(result.getTime())) throw new Error('Invalid inbound deadline');
  return time.parse(result.toISOString());
};
export const inboundRequestId = (callSid: string) => createHash('sha256').update(`incoming:${callSid}`).digest('hex');
export const inboundBeginBodyDigest = (begin: InboundBeginIdentity) => createHash('sha256').update(canonicalJson(begin)).digest('hex');

export function createInboundJournal(begin: InboundBeginIdentity, policy: InboundWaitPolicy, reservedAt: string,
  enrollmentMode = false): InboundJournal {
  inboundBeginIdentitySchema.parse(begin);
  inboundWaitPolicySchema.parse(policy);
  return { version: 1, begin: structuredClone(begin), beginBodyDigest: inboundBeginBodyDigest(begin),
    policy: structuredClone(policy), waitUntil: timestampAfter(reservedAt, policy.waitMs),
    ...(enrollmentMode ? { mode: { kind: 'undecided' as const } } : {}) };
}

function checkOutcomeIdentity(call: BrokerCall, outcome: RecoverInboundResult): void {
  const journal = call.inbound!;
  if (outcome.beginRef.brokerCallId !== call.id || outcome.beginRef.callSid !== call.callSid
      || !same(outcome.beginRef, inboundBeginRef(journal.begin)))
    throw new Error('Inbound outcome call identity mismatch');
  if (journal.remote && !same(journal.remote.beginRef, outcome.beginRef)) throw new Error('Inbound begin reference changed');
  if (journal.remote && journal.remote.state !== 'absent' && outcome.state !== 'absent'
      && journal.remote.attachmentId !== outcome.attachmentId) throw new Error('Inbound attachment changed');
  if ('approvalRef' in outcome && (outcome.approvalRef.attachmentId !== outcome.attachmentId
      || (journal.approvalRef && !same(journal.approvalRef, outcome.approvalRef)))) throw new Error('Inbound approval changed');
  if ('expiresAt' in outcome && journal.issuedExpiresAt && outcome.expiresAt !== journal.issuedExpiresAt)
    throw new Error('Inbound issued deadline changed');
  if (journal.remote?.state === 'bound' && outcome.state === 'bound'
      && (journal.remote.bindingId !== outcome.bindingId || journal.remote.consumedAt !== outcome.consumedAt || !same(journal.remote.billing, outcome.billing)))
    throw new Error('Inbound consumed binding changed');
}

function checkClosure(call: BrokerCall, receipt: ConsumedInboundClosureReceipt): void {
  const remote = call.inbound!.remote;
  if (remote?.state !== 'bound' || receipt.attachmentId !== remote.attachmentId
      || !same(receipt.beginRef, remote.beginRef) || !same(receipt.approvalRef, remote.approvalRef)
      || receipt.bindingId !== remote.bindingId || receipt.callSid !== call.callSid)
    throw new Error('Inbound closure identity mismatch');
  if (Date.parse(receipt.closedAt) < Date.parse(remote.consumedAt)) throw new Error('Inbound closure precedes consumption');
}

function checkContinuation(call: BrokerCall, proof: InboundBeginIdentity['providerProof'], at: string): void {
  const journal = call.inbound!;
  const original = journal.begin.providerProof;
  const url = new URL(original.url);
  url.pathname = `/broker/inbound/${call.id}/continue`;
  if (url.search || url.hash || proof.url !== url.href || proof.params.Direction !== 'inbound'
    || ['AccountSid', 'CallSid', 'From', 'To'].some(key => proof.params[key] !== original.params[key])
    || Date.parse(at) < Date.parse(call.createdAt) || Date.parse(at) >= Date.parse(journal.waitUntil))
    throw new Error('Inbound continuation identity mismatch');
}

/** Optional for old calls. This validates saved evidence without creating or renewing it. */
export function validateInboundJournal(call: BrokerCall, previous?: BrokerCall): void {
  if (previous?.inbound && call.inbound === undefined) throw new Error('Inbound journal cannot disappear');
  if (call.inbound === undefined) return;
  const journal = journalSchema.parse(call.inbound);
  const proof = journal.begin.providerProof;
  const params = proof.params;
  if (call.direction !== 'inbound' || journal.begin.brokerCallId !== call.id || call.callSid !== params.CallSid
      || call.requestId !== inboundRequestId(params.CallSid) || call.number !== params.From
      || params.Direction !== 'inbound' || !/^AC[0-9a-f]{32}$/i.test(params.AccountSid)
      || !/^\+[1-9]\d{7,14}$/.test(params.From) || !/^\+[1-9]\d{7,14}$/.test(params.To)
      || call.callAssignment || call.harkDispatch || call.harkJournal || call.callback)
    throw new Error('Inbound journal call identity mismatch');
  if (journal.beginBodyDigest !== inboundBeginBodyDigest(journal.begin)) throw new Error('Inbound original proof changed');
  if (journal.waitUntil !== timestampAfter(call.createdAt, journal.policy.waitMs)) throw new Error('Inbound wait deadline changed');
  const mode = journal.mode;
  if (mode?.kind === 'ordinary' && (!journal.continuation || mode.selectedAt !== journal.continuation.observedAt))
    throw new Error('Ordinary mode lacks its exact continuation');
  if (mode && mode.kind !== 'undecided'
    && (Date.parse(mode.selectedAt) < Date.parse(call.createdAt) || Date.parse(mode.selectedAt) >= Date.parse(journal.waitUntil)))
    throw new Error('Inbound mode selection outside original wait');
  if (mode && mode.kind !== 'ordinary') {
    if (journal.continuation || journal.earlyAdmission || journal.beginAttemptedAt || journal.consumeAttemptedAt || journal.remote
      || journal.approvalRef || journal.issuedExpiresAt || journal.admissionRefusal || journal.cancelRequestedAt
      || journal.closureRequestedAt || journal.closure || journal.mediaStartBy || call.primary || call.primaryClosed
      || call.mediaStartedAt || call.liveSessionId || call.liveGenerations || call.transcript
      || Object.keys(call.delegations).length) throw new Error('Enrollment mode has ordinary admission evidence');
  }
  if (mode?.kind === 'enrollment-only') {
    if (mode.command.brokerCallId !== call.id || mode.command.beginDigest !== inboundBeginRef(journal.begin).digest
      || mode.dispatchStartedAt !== mode.selectedAt) throw new Error('Enrollment command identity changed');
    if (mode.observation && (!sameEnrollmentCommand(mode.observation.result.command, mode.command)
      || Date.parse(mode.observation.observedAt) < Date.parse(mode.dispatchStartedAt)))
      throw new Error('Enrollment observation identity changed');
    if (mode.transportError && Date.parse(mode.transportError.observedAt) < Date.parse(mode.dispatchStartedAt))
      throw new Error('Enrollment error precedes dispatch');
    if (mode.endRequestedAt && (!journal.localStop || Date.parse(mode.endRequestedAt) < Date.parse(mode.selectedAt)))
      throw new Error('Enrollment end lacks local stop');
    if (mode.endAcknowledgedAt && (!mode.endRequestedAt || Date.parse(mode.endAcknowledgedAt) < Date.parse(mode.endRequestedAt)))
      throw new Error('Enrollment end acknowledgment lacks request');
  }
  if (previous?.inbound) {
    const old = previous.inbound.mode;
    if (old === undefined && mode !== undefined) throw new Error('Historical inbound mode cannot change');
    if (old && !mode) throw new Error('Inbound mode cannot disappear');
    if (old?.kind === 'ordinary' && !same(old, mode)) throw new Error('Ordinary inbound mode cannot change');
    if (old?.kind === 'enrollment-only') {
      if (mode?.kind !== 'enrollment-only' || old.selectedAt !== mode.selectedAt
        || old.dispatchStartedAt !== mode.dispatchStartedAt || !sameEnrollmentCommand(old.command, mode.command))
        throw new Error('Enrollment dispatch fence cannot change');
      if (old.observation?.result.state !== undefined && old.observation.result.state !== 'absent'
        && !same(old.observation.result, mode.observation?.result)) throw new Error('Enrollment committed result cannot change');
      for (const field of ['endRequestedAt', 'endAcknowledgedAt'] as const)
        if (old[field] !== undefined && mode[field] !== old[field]) throw new Error('Enrollment end evidence cannot change');
    }
  }
  if (previous?.inbound?.earlyAdmission && !same(previous.inbound.earlyAdmission, journal.earlyAdmission))
    throw new Error('Inbound early admission cannot change');
  if (journal.earlyAdmission) {
    if (journal.earlyAdmission.beginBodyDigest !== journal.beginBodyDigest
      || journal.earlyAdmission.startedAt !== journal.beginAttemptedAt
      || Date.parse(journal.earlyAdmission.startedAt) < Date.parse(call.createdAt)
      || Date.parse(journal.earlyAdmission.startedAt) >= Date.parse(journal.waitUntil))
      throw new Error('Inbound early admission identity mismatch');
    if (!journal.continuation && (journal.consumeAttemptedAt || journal.remote?.state === 'bound' || call.primary || call.mediaStartedAt))
      throw new Error('Inbound early admission requires continuation before consumption');
  }
  if (journal.continuation) {
    if (journal.continuation.beginBodyDigest !== journal.beginBodyDigest) throw new Error('Inbound continuation original proof changed');
    checkContinuation(call, journal.continuation.providerProof, journal.continuation.observedAt);
  }
  if (journal.admissionRefusal && (journal.admissionRefusal.beginBodyDigest !== journal.beginBodyDigest
    || (!journal.beginAttemptedAt && !journal.approvalRef)
    || !journal.localStop || !journal.cancelRequestedAt
    || Date.parse(journal.admissionRefusal.observedAt) < Date.parse(journal.beginAttemptedAt ?? call.createdAt)))
    throw new Error('Inbound admission refusal lacks stopped attempt');
  if (journal.remote) checkOutcomeIdentity(call, journal.remote);
  const remote = journal.remote;
  if (remote && 'expiresAt' in remote && remote.expiresAt !== null && remote.expiresAt !== journal.issuedExpiresAt)
    throw new Error('Inbound issued deadline missing');
  if (remote && 'approvalRef' in remote && !same(remote.approvalRef, journal.approvalRef)) throw new Error('Inbound approval missing');
  if (remote?.state === 'bound') {
    if (journal.mediaStartBy !== timestampAfter(remote.consumedAt, journal.policy.mediaGraceMs)) throw new Error('Inbound media deadline changed');
  } else if (journal.mediaStartBy !== undefined || journal.closure !== undefined || call.primary || call.primaryClosed)
    throw new Error('Inbound unbound call has binding evidence');
  if (journal.localStop && (!mode || mode.kind === 'ordinary') && !journal.cancelRequestedAt)
    throw new Error('Inbound stop lacks cancellation intent');
  if (journal.consumeAttemptedAt && (!journal.beginAttemptedAt || !journal.approvalRef)) throw new Error('Inbound consume lacks approval history');
  if (journal.closureRequestedAt && remote?.state !== 'bound') throw new Error('Inbound close lacks binding');
  if (call.primary && (remote?.state !== 'bound' || call.primary.bindingId !== remote.bindingId || call.primary.callAssignmentRef))
    throw new Error('Inbound private context binding mismatch');
  if (call.mediaStartedAt && (remote?.state !== 'bound' || !call.primary || !journal.mediaStartBy
      || !time.safeParse(call.mediaStartedAt).success || Date.parse(call.mediaStartedAt) < Date.parse(remote.consumedAt)
      || Date.parse(call.mediaStartedAt) >= Date.parse(journal.mediaStartBy))) throw new Error('Inbound media start lacks admission');
  if (journal.closure) {
    checkClosure(call, journal.closure);
    if (remote?.state !== 'bound' || remote.callState !== 'closed' || !call.primaryClosed
        || call.primaryClosed.bindingId !== journal.closure.bindingId || call.primaryClosed.callSid !== call.callSid
        || call.primaryClosed.state !== 'closed') throw new Error('Inbound closure receipt missing');
  }
  if (call.primaryClosed && (!journal.closure || !same(call.primaryClosed,
    { bindingId: journal.closure.bindingId, callSid: journal.closure.callSid, state: 'closed' })))
    throw new Error('Inbound closed binding lacks exact closure receipt');
}

export function assertInboundMediaStart(call: BrokerCall, startedAt: string | undefined, observedNow: string): void {
  if (!call.inbound) return;
  if (call.mediaStartedAt !== undefined) {
    if (call.mediaStartedAt !== startedAt) throw new Error('Inbound media start cannot change');
    return;
  }
  if (!startedAt || !time.safeParse(startedAt).success) throw new Error('Invalid inbound media start');
  const journal = call.inbound;
  if ((journal.mode && journal.mode.kind !== 'ordinary') || journal.localStop || call.primaryClosed || !call.primary || journal.remote?.state !== 'bound'
      || journal.remote.callState !== 'open' || !journal.mediaStartBy
      || ['completed', 'failed', 'busy', 'no-answer', 'canceled'].includes(call.status)
      || Date.parse(startedAt) < Date.parse(journal.remote.consumedAt)
      || Date.parse(startedAt) >= Date.parse(journal.mediaStartBy)
      || Date.parse(observedNow) >= Date.parse(journal.mediaStartBy)) throw new Error('Inbound media start unavailable');
}

/** The parent serializes remote operations; local stop can commit while one is pending. */
export function reduceInboundJournal(call: BrokerCall, event: InboundJournalEvent, now: string): BrokerCall {
  validateInboundJournal(call);
  if (!call.inbound) throw new Error('Inbound journal unavailable');
  const at = time.parse(event.observedAt ?? now);
  const next = structuredClone(call);
  const journal = next.inbound!;
  const stopped = () => !!journal.localStop || ['completed', 'failed', 'busy', 'no-answer', 'canceled'].includes(next.status);
  if (event.type === 'enrollment_dispatch') {
    const command = enrollmentCommandSchema.parse(event.command);
    if (journal.mode?.kind !== 'undecided' || stopped() || next.endedAt
      || Date.parse(at) >= Date.parse(journal.waitUntil)) throw new Error('Enrollment mode unavailable');
    journal.mode = { kind: 'enrollment-only', selectedAt: at, command, dispatchStartedAt: at };
  } else if (event.type === 'enrollment_observation' || event.type === 'enrollment_error'
    || event.type === 'enrollment_end_requested' || event.type === 'enrollment_end_acknowledged') {
    const mode = journal.mode;
    if (mode?.kind !== 'enrollment-only') throw new Error('Enrollment mode unavailable');
    if (event.type === 'enrollment_observation') {
      const result = enrollmentCompletionRecoverySchema.parse(event.result);
      if (!sameEnrollmentCommand(mode.command, result.command)) throw new Error('Enrollment observation identity changed');
      if (mode.observation?.result.state !== undefined && mode.observation.result.state !== 'absent') {
        if (!same(mode.observation.result, result)) throw new Error('Enrollment committed result cannot change');
      } else mode.observation = { observedAt: at, result };
    } else if (event.type === 'enrollment_error') {
      const error = enrollmentTransportErrorSchema.parse(event.error);
      if (error.observedAt !== at) throw new Error('Enrollment error clock changed');
      mode.transportError = error;
    } else if (event.type === 'enrollment_end_requested') {
      if (!journal.localStop) throw new Error('Enrollment end requires local stop');
      mode.endRequestedAt ??= at;
    } else {
      if (!mode.endRequestedAt) throw new Error('Enrollment end acknowledgment lacks request');
      mode.endAcknowledgedAt ??= at;
    }
  } else if (event.type === 'continuation') {
    if (journal.mode?.kind === 'enrollment-only') throw new Error('Enrollment leg cannot continue ordinary admission');
    const proof = inboundProviderProofSchema.parse(event.providerProof);
    if (Object.prototype.hasOwnProperty.call(proof.params, 'Digits')) throw new Error('Digit form cannot enter ordinary continuation');
    checkContinuation(call, proof, at);
    if (stopped() || next.endedAt) throw new Error('Inbound wait stopped');
    if (journal.mode?.kind === 'undecided') journal.mode = { kind: 'ordinary', selectedAt: at };
    journal.continuation ??= { observedAt: at, beginBodyDigest: journal.beginBodyDigest, providerProof: structuredClone(proof) };
  } else if (event.type === 'admission_refused') {
    const code = z.enum(['inbound_carrier_attestation_required', 'inbound_enrollment_required']).parse(event.code);
    if (!journal.beginAttemptedAt && journal.remote?.state !== 'approved' && journal.remote?.state !== 'bound')
      throw new Error('Inbound refusal lacks attempt or admission');
    if (journal.admissionRefusal && journal.admissionRefusal.code !== code) throw new Error('Inbound admission refusal changed');
    journal.admissionRefusal ??= { observedAt: at, beginBodyDigest: journal.beginBodyDigest, status: 403, retryable: false, code };
    journal.localStop ??= { observedAt: at, reason: code };
    journal.cancelRequestedAt ??= at;
  } else if (event.type === 'begin_attempted') {
    if (stopped() || next.endedAt || Date.parse(at) >= Date.parse(journal.waitUntil)) throw new Error('Inbound wait stopped');
    if (!journal.continuation) throw new Error('Inbound continuation required');
    journal.beginAttemptedAt ??= at;
  } else if (event.type === 'consume_attempted') {
    if (stopped() || journal.remote?.state !== 'approved' || Date.parse(at) >= Date.parse(journal.waitUntil)
        || Date.parse(at) >= Date.parse(journal.remote.expiresAt)) throw new Error('Inbound consumption unavailable');
    if (!journal.beginAttemptedAt) throw new Error('Inbound begin not attempted');
    if (journal.earlyAdmission && !journal.continuation) throw new Error('Inbound continuation required');
    journal.consumeAttemptedAt ??= at;
  } else if (event.type === 'stop') {
    z.string().min(1).max(100).parse(event.reason);
    journal.localStop ??= { reason: event.reason, observedAt: at };
    if (!journal.mode || journal.mode.kind === 'ordinary') journal.cancelRequestedAt ??= at;
  } else if (event.type === 'cancel_requested') {
    if (!journal.localStop) throw new Error('Inbound cancellation requires local stop');
    journal.cancelRequestedAt ??= at;
  } else if (event.type === 'close_requested') {
    if (journal.remote?.state !== 'bound') throw new Error('Inbound close lacks binding');
    journal.closureRequestedAt ??= at;
  } else if (event.type === 'outcome') {
    const outcome = recoverInboundResultSchema.parse(event.outcome);
    checkOutcomeIdentity(next, outcome);
    const previous = journal.remote;
    if (previous && previous.state !== 'absent' && outcome.state === 'absent') throw new Error('Inbound attachment cannot disappear');
    if (previous?.state === 'cancelled' || previous?.state === 'expired') {
      if (!same(previous, outcome)) throw new Error('Inbound terminal outcome changed');
    } else if (previous?.state === 'bound') {
      if (outcome.state === 'cancelled' || outcome.state === 'expired') throw new Error('Consumed inbound cannot become unbound');
      if (outcome.state === 'bound' && !(previous.callState === 'closed' && outcome.callState === 'open')) journal.remote = outcome;
    } else if (!(previous?.state === 'approved' && outcome.state === 'pending')) journal.remote = outcome;
    // A stale response must not overwrite evidence added by a later response.
    if (journal.remote === outcome) {
      if ('expiresAt' in outcome && outcome.expiresAt !== null) journal.issuedExpiresAt ??= outcome.expiresAt;
      if ('approvalRef' in outcome) journal.approvalRef ??= outcome.approvalRef;
      if (outcome.state === 'bound') {
        journal.mediaStartBy ??= timestampAfter(outcome.consumedAt, journal.policy.mediaGraceMs);
        if (outcome.billing) next.inboundBilling ??= { version: 1, ref: structuredClone(outcome.billing) };
      }
    }
  } else if (event.type === 'closure') {
    const receipt = consumedInboundClosureReceiptSchema.parse(event.receipt);
    checkClosure(next, receipt);
    if (journal.closure && !same(journal.closure, receipt)) throw new Error('Inbound closure receipt changed');
    journal.closure ??= receipt;
    journal.remote = { ...journal.remote!, state: 'bound', callState: 'closed' } as RecoverInboundResult;
    next.primaryClosed = { bindingId: receipt.bindingId, callSid: receipt.callSid, state: 'closed' };
  } else if (event.type === 'context') {
    const context = structuredClone(event.context);
    if (stopped() || next.primaryClosed || journal.remote?.state !== 'bound' || journal.remote.callState !== 'open'
        || context.state !== 'open' || context.bindingId !== journal.remote.bindingId || context.callSid !== next.callSid
        || context.callAssignmentRef || !same(context.billing, journal.remote.billing) || !journal.mediaStartBy || Date.parse(at) >= Date.parse(journal.mediaStartBy))
      throw new Error('Inbound private context unavailable');
    if (!context.context || !primaryResultId.safeParse(context.context.primaryDexterId).success || !primaryResultId.safeParse(context.context.threadId).success
        || typeof context.context.displayName !== 'string' || !Array.isArray(context.context.messages)
        || context.context.messages.some(message => !['user', 'assistant'].includes(message.role) || typeof message.text !== 'string'))
      throw new Error('Invalid inbound private context');
    next.primary = { bindingId: context.bindingId, name: context.context.displayName,
      primaryDexterId: context.context.primaryDexterId, threadId: context.context.threadId,
      context: context.context.messages.map(message => `${message.role}: ${message.text}`).join('\n').slice(-8_000) };
  } else throw new Error('Unknown inbound journal transition');
  validateInboundJournal(next, call);
  if (same(next, call)) return call;
  next.updatedAt = at;
  return next;
}
