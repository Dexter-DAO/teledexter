import { createHash } from 'node:crypto';
import { z } from 'zod';
import { canonicalJson } from '../outbound/canonical.js';
import { inboundBeginIdentitySchema, inboundBeginRef, inboundApprovalRefSchema, recoverInboundResultSchema,
  consumedInboundClosureReceiptSchema, type InboundBeginIdentity, type InboundApprovalRef,
  type RecoverInboundResult, type ConsumedInboundClosureReceipt } from './inboundContract.js';
import type { PrimaryCall } from './primaryApi.js';
import type { BrokerCall } from './store.js';

export interface InboundWaitPolicy { version: 1; waitMs: number; mediaGraceMs: number }
export interface InboundJournal {
  version: 1;
  begin: InboundBeginIdentity;
  beginBodyDigest: string;
  policy: InboundWaitPolicy;
  waitUntil: string;
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
  | { type: 'begin_attempted' | 'consume_attempted' | 'cancel_requested' | 'close_requested'; observedAt: string }
  | { type: 'stop'; reason: string; observedAt: string }
  | { type: 'outcome'; outcome: RecoverInboundResult; observedAt?: string }
  | { type: 'closure'; receipt: ConsumedInboundClosureReceipt; observedAt?: string }
  | { type: 'context'; context: PrimaryCall; observedAt?: string };

const time = z.string().datetime({ offset: true });
export const inboundWaitPolicySchema = z.object({ version: z.literal(1),
  waitMs: z.number().int().safe().positive(), mediaGraceMs: z.number().int().safe().positive() }).strict();
const journalSchema = z.object({
  version: z.literal(1), begin: inboundBeginIdentitySchema, beginBodyDigest: z.string().regex(/^[0-9a-f]{64}$/),
  policy: inboundWaitPolicySchema, waitUntil: time,
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

export function createInboundJournal(begin: InboundBeginIdentity, policy: InboundWaitPolicy, reservedAt: string): InboundJournal {
  inboundBeginIdentitySchema.parse(begin);
  inboundWaitPolicySchema.parse(policy);
  return { version: 1, begin: structuredClone(begin), beginBodyDigest: inboundBeginBodyDigest(begin),
    policy: structuredClone(policy), waitUntil: timestampAfter(reservedAt, policy.waitMs) };
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
      && (journal.remote.bindingId !== outcome.bindingId || journal.remote.consumedAt !== outcome.consumedAt))
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

/** Optional for old calls. This validates saved evidence without creating or renewing it. */
export function validateInboundJournal(call: BrokerCall): void {
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
  if (journal.remote) checkOutcomeIdentity(call, journal.remote);
  const remote = journal.remote;
  if (remote && 'expiresAt' in remote && remote.expiresAt !== null && remote.expiresAt !== journal.issuedExpiresAt)
    throw new Error('Inbound issued deadline missing');
  if (remote && 'approvalRef' in remote && !same(remote.approvalRef, journal.approvalRef)) throw new Error('Inbound approval missing');
  if (remote?.state === 'bound') {
    if (journal.mediaStartBy !== timestampAfter(remote.consumedAt, journal.policy.mediaGraceMs)) throw new Error('Inbound media deadline changed');
  } else if (journal.mediaStartBy !== undefined || journal.closure !== undefined || call.primary || call.primaryClosed)
    throw new Error('Inbound unbound call has binding evidence');
  if (journal.localStop && !journal.cancelRequestedAt) throw new Error('Inbound stop lacks cancellation intent');
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
  if (journal.localStop || call.primaryClosed || !call.primary || journal.remote?.state !== 'bound'
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
  if (event.type === 'begin_attempted') {
    if (stopped() || Date.parse(at) >= Date.parse(journal.waitUntil)) throw new Error('Inbound wait stopped');
    journal.beginAttemptedAt ??= at;
  } else if (event.type === 'consume_attempted') {
    if (stopped() || journal.remote?.state !== 'approved' || Date.parse(at) >= Date.parse(journal.waitUntil)
        || Date.parse(at) >= Date.parse(journal.remote.expiresAt)) throw new Error('Inbound consumption unavailable');
    if (!journal.beginAttemptedAt) throw new Error('Inbound begin not attempted');
    journal.consumeAttemptedAt ??= at;
  } else if (event.type === 'stop') {
    z.string().min(1).max(100).parse(event.reason);
    journal.localStop ??= { reason: event.reason, observedAt: at };
    journal.cancelRequestedAt ??= at;
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
      if (outcome.state === 'bound') journal.mediaStartBy ??= timestampAfter(outcome.consumedAt, journal.policy.mediaGraceMs);
    }
  } else if (event.type === 'closure') {
    const receipt = consumedInboundClosureReceiptSchema.parse(event.receipt);
    checkClosure(next, receipt);
    if (journal.closure && !same(journal.closure, receipt)) throw new Error('Inbound closure receipt changed');
    journal.closure ??= receipt;
    journal.remote = { ...journal.remote!, state: 'bound', callState: 'closed' } as RecoverInboundResult;
    next.primaryClosed = { bindingId: receipt.bindingId, callSid: receipt.callSid, state: 'closed' };
  } else if (event.type === 'context') {
    const context = event.context;
    if (stopped() || next.primaryClosed || journal.remote?.state !== 'bound' || journal.remote.callState !== 'open'
        || context.state !== 'open' || context.bindingId !== journal.remote.bindingId || context.callSid !== next.callSid
        || context.callAssignmentRef || !journal.mediaStartBy || Date.parse(at) >= Date.parse(journal.mediaStartBy))
      throw new Error('Inbound private context unavailable');
    if (!context.context || typeof context.context.displayName !== 'string' || !Array.isArray(context.context.messages)
        || context.context.messages.some(message => !['user', 'assistant'].includes(message.role) || typeof message.text !== 'string'))
      throw new Error('Invalid inbound private context');
    next.primary = { bindingId: context.bindingId, name: context.context.displayName,
      context: context.context.messages.map(message => `${message.role}: ${message.text}`).join('\n').slice(-8_000) };
  } else throw new Error('Unknown inbound journal transition');
  validateInboundJournal(next);
  if (same(next, call)) return call;
  next.updatedAt = at;
  return next;
}
