import { z } from 'zod';
import { canonicalJson, sha256Hex } from '../outbound/canonical.js';
import { HarkPhoneStatusCallbackResultSchema } from '../outbound/contract.js';
import { buildHarkStatusBody, type HarkStatusBody, type HarkStatusDeliveryResult } from './harkStatusDelivery.js';
import { HarkCallEndedReceiptSchema, type HarkOutboundStatusBody, type HarkCallEndedBody, type HarkBillingDeliveryResult } from './harkBillingDelivery.js';
import type { BrokerCall } from './store.js';

export type HarkProviderStatus = 'queued' | 'ringing' | 'in-progress' | 'completed' | 'busy' | 'no-answer' | 'canceled' | 'failed';
type ObservedStatus = Exclude<HarkProviderStatus, 'in-progress'> | 'answered' | 'streaming';
export interface HarkEndedTiming { startedAt: string; endedAt: string; durationSeconds: number }
export type HarkJournalEvent = {
  id: string; bodyDigest: string; observedAt: string; state: 'pending' | 'delivered' | 'rejected';
  attempts: number; lastAttemptAt?: string; lastOutcome?: { state: 'pending' | 'rejected'; reason: string; httpStatus?: number };
  receipt?: unknown;
} & (
  | { kind: 'hark_status'; body: HarkStatusBody }
  | { kind: 'outbound_status'; body: HarkOutboundStatusBody }
  | { kind: 'call_ended'; body: HarkCallEndedBody }
);
export interface HarkJournal {
  version: 1;
  providerStatus: ObservedStatus;
  ended?: { providerStatus: ObservedStatus; observedAt: string; timing?: HarkEndedTiming };
  events: Record<string, HarkJournalEvent>;
}
export type HarkDeliveryOutcome = HarkStatusDeliveryResult | HarkBillingDeliveryResult;
const observedStatuses = ['queued', 'ringing', 'answered', 'streaming', 'completed', 'busy', 'no-answer', 'canceled', 'failed'] as const;
const terminal = new Set<ObservedStatus>(['completed', 'busy', 'no-answer', 'canceled', 'failed']);
const rank: Record<ObservedStatus, number> = { queued: 0, ringing: 1, answered: 2, streaming: 3, completed: 4, busy: 4, 'no-answer': 4, canceled: 4, failed: 4 };
const timestamp = z.string().datetime({ offset: true });
const timingSchema = z.object({ startedAt: timestamp, endedAt: timestamp, durationSeconds: z.number().int().safe().nonnegative() }).strict()
  .refine(timing => Date.parse(timing.startedAt) <= Date.parse(timing.endedAt), 'Invalid call timing');
const journalSchema = z.object({
  version: z.literal(1), providerStatus: z.enum(observedStatuses),
  ended: z.object({ providerStatus: z.enum(observedStatuses), observedAt: timestamp, timing: timingSchema.optional() }).strict().optional(),
  events: z.record(z.string(), z.object({
    id: z.string(), kind: z.enum(['hark_status', 'outbound_status', 'call_ended']), body: z.unknown(),
    bodyDigest: z.string().regex(/^[a-f0-9]{64}$/), observedAt: timestamp,
    state: z.enum(['pending', 'delivered', 'rejected']), attempts: z.number().int().safe().nonnegative(),
    lastAttemptAt: timestamp.optional(), receipt: z.unknown().optional(),
    lastOutcome: z.object({ state: z.enum(['pending', 'rejected']), reason: z.string().min(1).max(100),
      httpStatus: z.number().int().min(100).max(599).optional() }).strict().optional(),
  }).strict()),
}).strict();

function binding(call: BrokerCall) {
  if (call.callAssignment?.origin !== 'authenticated_hark_delivery' || !call.callSid
      || !/^CA[0-9a-fA-F]{32}$/.test(call.callSid)) throw new Error('Hark provider binding unavailable');
  const snapshot = call.callAssignment.value.hark;
  return { intentId: call.id, callSid: call.callSid, hark: { schemaVersion: 1 as const,
    caseId: snapshot.caseId, watchId: snapshot.watch.id, eventId: snapshot.event.id,
    deliveryId: snapshot.deliveryId, originTaskId: snapshot.originTaskId } };
}

function statusEvents(call: BrokerCall, status: ObservedStatus, observedAt: string): HarkJournalEvent[] {
  const bound = binding(call);
  const hark = buildHarkStatusBody(bound, status, observedAt);
  const outbound: HarkOutboundStatusBody = { intentId: call.id, eventId: `hark:${call.id}`, callSid: bound.callSid, state: status };
  return [event('hark_status', hark.sourceEventId, hark, observedAt),
    event('outbound_status', `voice-status:${call.id}:${status}`, outbound, observedAt)];
}
function event(kind: HarkJournalEvent['kind'], id: string, body: HarkJournalEvent['body'], observedAt: string): HarkJournalEvent {
  return { kind, id, body: structuredClone(body), bodyDigest: sha256Hex(canonicalJson(body)),
    observedAt, state: 'pending', attempts: 0 } as HarkJournalEvent;
}
function endedEvent(call: BrokerCall, timing: HarkEndedTiming, observedAt: string): HarkJournalEvent {
  binding(call);
  if (call.callAssignment?.origin !== 'authenticated_hark_delivery') throw new Error('Hark assignment unavailable');
  const body: HarkCallEndedBody = { source: 'phone', direction: 'outbound-alert', alert_intent_id: call.id,
    alert_event_id: `hark:${call.id}`, external_sid: call.callSid!, started_at: timing.startedAt,
    ended_at: timing.endedAt, duration_seconds: timing.durationSeconds,
    caller_user_id: call.callAssignment.value.hark.ownerSupabaseUserId };
  return event('call_ended', `voice-ended:${call.id}`, body, observedAt);
}

/** Provider evidence and its outgoing records are committed together by BrokerStore. */
export function observeHark(call: BrokerCall, status: ObservedStatus, observedAt: string, timing?: HarkEndedTiming): HarkJournal {
  binding(call);
  timestamp.parse(observedAt);
  if (!observedStatuses.includes(status)) throw new Error('Invalid Hark provider status');
  if (timing !== undefined) { timingSchema.parse(timing); if (!terminal.has(status)) throw new Error('Call timing requires provider termination'); }
  const prior = call.harkJournal;
  if (prior) validateHarkJournal(call);
  if (prior && terminal.has(prior.providerStatus) && prior.providerStatus !== status) {
    if (!terminal.has(status)) return structuredClone(prior);
    throw new Error('Hark provider terminal status conflicts');
  }
  if (prior && rank[status] < rank[prior.providerStatus]) return structuredClone(prior);
  const next: HarkJournal = prior ? structuredClone(prior) : { version: 1, providerStatus: status, events: {} };
  next.providerStatus = status;
  for (const item of statusEvents(call, status, observedAt)) {
    // A repeated callback recovers its first timestamp and exact signed body.
    if (!next.events[item.id]) next.events[item.id] = item;
  }
  if (terminal.has(status)) {
    next.ended ??= { providerStatus: status, observedAt };
    if (timing !== undefined) {
      if (next.ended.timing && canonicalJson(next.ended.timing) !== canonicalJson(timing)) throw new Error('Hark ended timing cannot change');
      next.ended.timing ??= structuredClone(timing);
      const item = endedEvent(call, next.ended.timing, next.ended.observedAt);
      next.events[item.id] ??= item;
    }
  }
  return next;
}

function validateReceipt(item: HarkJournalEvent, receipt: unknown): void {
  if (item.kind === 'hark_status') {
    const parsed = HarkPhoneStatusCallbackResultSchema.parse(receipt);
    for (const field of ['caseId', 'eventId', 'deliveryId', 'intentId', 'callSid', 'originTaskId', 'sourceEventId'] as const)
      if (parsed[field] !== item.body[field]) throw new Error('Hark status receipt identity mismatch');
  } else if (item.kind === 'outbound_status') {
    if (canonicalJson(receipt) !== canonicalJson({ purpose: 'voice.outbound-status.v2', httpStatus: 204 }))
      throw new Error('Outbound status acknowledgment missing');
  } else {
    HarkCallEndedReceiptSchema.parse(receipt);
  }
}

/** Validate stored identities and bodies without normalizing or rewriting legacy bytes. */
export function validateHarkJournal(call: BrokerCall): void {
  if (call.harkJournal === undefined) return;
  binding(call);
  journalSchema.parse(call.harkJournal);
  const journal = call.harkJournal;
  const expected = new Map<string, HarkJournalEvent>();
  for (const item of Object.values(journal.events)) {
    if (item.kind !== 'call_ended') {
      const status = item.kind === 'hark_status' ? item.body.status : item.body.state;
      if (!observedStatuses.includes(status as ObservedStatus) || rank[status as ObservedStatus] > rank[journal.providerStatus]
          || (terminal.has(status as ObservedStatus) && status !== journal.providerStatus))
        throw new Error('Invalid saved Hark observation');
      for (const candidate of statusEvents(call, status as ObservedStatus, item.observedAt)) expected.set(candidate.id, candidate);
    }
  }
  if (terminal.has(journal.providerStatus) !== Boolean(journal.ended)
      || (journal.ended && (journal.ended.providerStatus !== journal.providerStatus || !terminal.has(journal.ended.providerStatus))))
    throw new Error('Invalid Hark terminal observation');
  if (journal.ended?.timing) {
    const candidate = endedEvent(call, journal.ended.timing, journal.ended.observedAt);
    expected.set(candidate.id, candidate);
  }
  for (const [id, item] of Object.entries(journal.events)) {
    const candidate = expected.get(id);
    if (!candidate || item.id !== id || candidate.kind !== item.kind || candidate.observedAt !== item.observedAt || candidate.bodyDigest !== item.bodyDigest
        || canonicalJson(candidate.body) !== canonicalJson(item.body)) throw new Error('Hark outbox identity or body changed');
    if ((item.attempts === 0) !== (item.lastAttemptAt === undefined)) throw new Error('Invalid Hark attempt evidence');
    if (item.state === 'delivered') {
      if (!item.attempts || item.lastOutcome !== undefined) throw new Error('Invalid Hark delivery evidence');
      validateReceipt(item, item.receipt);
    } else if (item.receipt !== undefined || (item.lastOutcome !== undefined && item.lastOutcome.state !== item.state)
        || (item.state === 'rejected' && (!item.attempts || item.lastOutcome?.state !== 'rejected'))) {
      throw new Error('Invalid Hark pending evidence');
    }
  }
  if (!statusEvents(call, journal.providerStatus, '2026-01-01T00:00:00.000Z').every(item => journal.events[item.id]))
    throw new Error('Hark current observation is missing its outbox');
  if (journal.ended?.timing && !journal.events[`voice-ended:${call.id}`]) throw new Error('Hark ended outbox missing');
  if ([...expected.keys()].some(id => !journal.events[id])) throw new Error('Hark paired observation missing');
  const ended = journal.events[`voice-ended:${call.id}`];
  if (ended && ended.attempts > 0 && Object.values(journal.events).some(item => item.kind === 'outbound_status' && item.state !== 'delivered'))
    throw new Error('Hark ended attempt preceded status acknowledgment');
}

/** First saved answer evidence, or media start if no answer callback was saved. */
export function harkObservedStart(call: BrokerCall): string | undefined {
  if (!call.harkJournal) return undefined;
  validateHarkJournal(call);
  return call.harkJournal.events[`voice-status:${call.id}:answered`]?.observedAt
    ?? call.harkJournal.events[`voice-status:${call.id}:streaming`]?.observedAt;
}

export function pendingHarkEvents(call: BrokerCall): HarkJournalEvent[] {
  if (!call.harkJournal) return [];
  validateHarkJournal(call);
  const entries = Object.values(call.harkJournal.events);
  const result: HarkJournalEvent[] = [];
  for (const kind of ['hark_status', 'outbound_status'] as const) {
    const firstUnsettled = entries.find(item => item.kind === kind && item.state !== 'delivered');
    if (firstUnsettled?.state === 'pending') result.push(firstUnsettled);
  }
  const ended = entries.find(item => item.kind === 'call_ended' && item.state === 'pending');
  if (ended && entries.some(item => item.kind === 'outbound_status')
      && entries.filter(item => item.kind === 'outbound_status').every(item => item.state === 'delivered')) result.push(ended);
  return structuredClone(result);
}

export function settleHarkJournalEvent(item: HarkJournalEvent, outcome: HarkDeliveryOutcome): HarkJournalEvent {
  if (!item.attempts) throw new Error('Hark send attempt was not recorded');
  if (item.state === 'delivered' && outcome.state !== 'delivered') throw new Error('Hark delivery receipt cannot change');
  if (item.state === 'rejected') {
    const repeat = outcome.state === 'rejected' && canonicalJson(item.lastOutcome) === canonicalJson({
      state: outcome.state, reason: outcome.reason, ...(outcome.httpStatus === undefined ? {} : { httpStatus: outcome.httpStatus }),
    });
    if (repeat) return structuredClone(item);
    throw new Error('Hark event was rejected');
  }
  const next = structuredClone(item);
  if (outcome.state === 'delivered') {
    if (item.kind === 'hark_status') {
      if ('purpose' in outcome) throw new Error('Wrong Hark delivery kind');
      next.receipt = outcome.receipt;
    } else {
      const purpose = item.kind === 'outbound_status' ? 'voice.outbound-status.v2' : 'voice.call-ended.v2';
      if (!('purpose' in outcome) || outcome.purpose !== purpose) throw new Error('Wrong billing delivery kind');
      if (item.kind === 'outbound_status') {
        if (outcome.httpStatus !== 204) throw new Error('Outbound status was not acknowledged');
        next.receipt = { purpose, httpStatus: 204 };
      } else {
        if (outcome.httpStatus !== 200 || !('receipt' in outcome)) throw new Error('Call ended receipt missing');
        next.receipt = outcome.receipt;
      }
    }
    validateReceipt(item, next.receipt);
    if (item.state === 'delivered' && canonicalJson(item.receipt) !== canonicalJson(next.receipt)) throw new Error('Hark delivery receipt cannot change');
    next.state = 'delivered';
    delete next.lastOutcome;
  } else {
    next.state = outcome.state;
    next.lastOutcome = { state: outcome.state, reason: outcome.reason,
      ...(outcome.httpStatus === undefined ? {} : { httpStatus: outcome.httpStatus }) };
  }
  return next;
}
