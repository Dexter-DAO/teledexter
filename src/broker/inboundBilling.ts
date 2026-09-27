import { z } from 'zod';
import { canonicalJson, sha256Hex } from '../outbound/canonical.js';
import type { BrokerCall } from './store.js';

const uuid = z.string().uuid();
const sid = z.string().regex(/^CA[0-9a-f]{32}$/i);
const time = z.string().datetime({ offset: true });
const digest = z.string().regex(/^[0-9a-f]{64}$/);
export const phoneInboundBillingRefSchema = z.object({
  version: z.literal(1), bindingId: uuid, callSid: sid, termsDigest: digest,
}).strict();
export type PhoneInboundBillingRef = z.infer<typeof phoneInboundBillingRefSchema>;
export const inboundProviderTerminalSchema = z.object({
  source: z.literal('authenticated_provider_fetch'), observedAt: time,
  accountSid: z.string().regex(/^AC[0-9a-f]{32}$/i), callSid: sid,
  from: z.string().regex(/^\+[1-9]\d{7,14}$/), to: z.string().regex(/^\+[1-9]\d{7,14}$/),
  direction: z.literal('inbound'), status: z.literal('completed'),
  startedAt: time, endedAt: time, durationSeconds: z.number().int().safe().nonnegative(),
}).strict().refine(value => Date.parse(value.startedAt) <= Date.parse(value.endedAt)
  && Date.parse(value.endedAt) <= Date.parse(value.observedAt)
  && Math.abs(value.durationSeconds - (Date.parse(value.endedAt) - Date.parse(value.startedAt)) / 1000) <= 1);
export const inboundCallEndedSchema = z.object({
  source: z.literal('phone'), direction: z.literal('inbound'), purpose: z.literal('primary_inbound_v1'),
  external_sid: sid, phone_binding_id: uuid, broker_call_id: uuid, billing_terms_digest: digest,
  started_at: time, ended_at: time, duration_seconds: z.number().int().safe().nonnegative(),
  provider_terminal: inboundProviderTerminalSchema,
}).strict().refine(body => body.external_sid === body.provider_terminal.callSid
  && body.started_at === body.provider_terminal.startedAt && body.ended_at === body.provider_terminal.endedAt
  && body.duration_seconds === body.provider_terminal.durationSeconds);
export type InboundCallEnded = z.infer<typeof inboundCallEndedSchema>;
export const inboundNonCompletedStatusSchema = z.enum(['failed', 'busy', 'no-answer', 'canceled']);
export const inboundProviderStatusSchema = z.object({
  source: z.literal('authenticated_provider_fetch'), observedAt: time,
  accountSid: z.string().regex(/^AC[0-9a-f]{32}$/i), callSid: sid,
  from: z.string().regex(/^\+[1-9]\d{7,14}$/), to: z.string().regex(/^\+[1-9]\d{7,14}$/),
  direction: z.literal('inbound'), status: inboundNonCompletedStatusSchema,
  startedAt: time.nullable(), endedAt: time.nullable(), durationSeconds: z.number().int().safe().nonnegative().nullable(),
}).strict().refine(value => (value.startedAt === null || Date.parse(value.startedAt) <= Date.parse(value.observedAt))
  && (value.endedAt === null || Date.parse(value.endedAt) <= Date.parse(value.observedAt))
  && (value.startedAt === null || value.endedAt === null || (Date.parse(value.startedAt) <= Date.parse(value.endedAt)
    && (value.durationSeconds === null || Math.abs(value.durationSeconds - (Date.parse(value.endedAt) - Date.parse(value.startedAt)) / 1000) <= 1))));
export const inboundStatusEndedSchema = z.object({
  source: z.literal('phone'), direction: z.literal('inbound'), purpose: z.literal('primary_inbound_status_v1'),
  external_sid: sid, phone_binding_id: uuid, broker_call_id: uuid, billing_terms_digest: digest,
  provider_terminal: inboundProviderStatusSchema,
}).strict().refine(body => body.external_sid === body.provider_terminal.callSid);
export type InboundStatusEnded = z.infer<typeof inboundStatusEndedSchema>;
export const inboundStatusReceiptSchema = z.object({ ok: z.literal(true), id: uuid,
  status: inboundNonCompletedStatusSchema }).strict();
export type InboundStatusDelivery =
  | { state: 'delivered'; receipt: z.infer<typeof inboundStatusReceiptSchema> }
  | { state: 'pending' | 'rejected'; reason: string; httpStatus?: number };
export const inboundBillingReceiptSchema = z.object({ ok: z.literal(true), id: uuid,
  status: z.enum(['completed', 'short', 'silent', 'error', 'tool_failure']) }).strict();
export type InboundBillingDelivery =
  | { state: 'delivered'; receipt: z.infer<typeof inboundBillingReceiptSchema> }
  | { state: 'pending' | 'rejected'; reason: string; httpStatus?: number };
const deliveryError = z.object({ state: z.enum(['pending', 'rejected']), reason: z.string().min(1).max(80),
  httpStatus: z.number().int().min(100).max(599).optional() }).strict();
const eventSchema = z.object({ body: inboundCallEndedSchema, bodyDigest: digest,
  state: z.enum(['pending', 'delivered', 'rejected']), attempts: z.number().int().safe().nonnegative(),
  lastAttemptAt: time.optional(), lastOutcome: deliveryError.optional(), receipt: inboundBillingReceiptSchema.optional(),
}).strict();
const statusEventSchema = eventSchema.extend({ body: inboundStatusEndedSchema, receipt: inboundStatusReceiptSchema.optional() });
const journalSchema = z.object({ version: z.literal(1), ref: phoneInboundBillingRefSchema,
  terminal: z.object({ status: z.enum(['completed', 'failed', 'busy', 'no-answer', 'canceled']), observedAt: time }).strict().optional(),
  lastTimingAttemptAt: time.optional(), timingError: z.enum(['provider_read_failed', 'provider_identity_mismatch',
    'provider_timing_unavailable', 'provider_terminal_conflict', 'provider_terminal_pending']).optional(), event: eventSchema.optional(), statusEvent: statusEventSchema.optional(),
}).strict();
export type InboundBillingJournal = z.infer<typeof journalSchema>;
export type InboundBillingCommand =
  | { kind: 'terminal'; status: NonNullable<InboundBillingJournal['terminal']>['status']; observedAt: string }
  | { kind: 'timing_attempt'; observedAt: string }
  | { kind: 'timing_failed'; reason: NonNullable<InboundBillingJournal['timingError']> }
  | { kind: 'provider_status'; observation: z.infer<typeof inboundProviderStatusSchema> }
  | { kind: 'status_attempt'; bodyDigest: string; observedAt: string }
  | { kind: 'status_delivery'; bodyDigest: string; result: InboundStatusDelivery }
  | { kind: 'provider'; observation: z.infer<typeof inboundProviderTerminalSchema> }
  | { kind: 'attempt'; bodyDigest: string; observedAt: string }
  | { kind: 'delivery'; bodyDigest: string; result: InboundBillingDelivery };
const same = (a: unknown, b: unknown) => canonicalJson(a) === canonicalJson(b);

export function validateInboundBilling(call: BrokerCall, previous?: BrokerCall): void {
  const old = previous?.inboundBilling;
  if (!call.inboundBilling) {
    if (old || (call.inbound?.remote?.state === 'bound' && call.inbound.remote.billing))
      throw new Error('Inbound billing journal missing');
    return;
  }
  const value = journalSchema.parse(call.inboundBilling);
  const bound = call.inbound?.remote;
  if (call.direction !== 'inbound' || !call.inbound || bound?.state !== 'bound'
    || !same(value.ref, bound.billing) || value.ref.bindingId !== bound.bindingId || value.ref.callSid !== call.callSid)
    throw new Error('Inbound billing binding mismatch');
  if (previous?.inbound?.remote?.state === 'bound' && !old)
    throw new Error('Historical inbound call cannot acquire billing');
  if (old && !same(old.ref, value.ref)) throw new Error('Inbound billing reference changed');
  if (old?.terminal && !same(old.terminal, value.terminal)) throw new Error('Inbound terminal evidence changed');
  const event = value.event;
  if (event) {
    const body = event.body, proof = call.inbound.begin.providerProof.params;
    if (!value.terminal || value.terminal.status !== 'completed' || body.broker_call_id !== call.id
      || body.external_sid !== call.callSid || body.phone_binding_id !== value.ref.bindingId
      || body.billing_terms_digest !== value.ref.termsDigest || body.provider_terminal.accountSid !== proof.AccountSid
      || body.provider_terminal.from !== proof.From || body.provider_terminal.to !== proof.To
      || event.bodyDigest !== sha256Hex(canonicalJson(body))) throw new Error('Inbound financial evidence mismatch');
    if ((event.attempts > 0) !== Boolean(event.lastAttemptAt)
      || (event.state === 'delivered') !== Boolean(event.receipt)
      || (event.state !== 'pending' && event.attempts === 0)
      || (event.lastOutcome && event.lastOutcome.state !== event.state)) throw new Error('Inbound receipt state invalid');
  }
  const statusEvent = value.statusEvent;
  if (statusEvent) {
    const body = statusEvent.body, proof = call.inbound.begin.providerProof.params;
    if (event || !value.terminal || value.terminal.status !== body.provider_terminal.status
      || body.broker_call_id !== call.id || body.external_sid !== call.callSid || body.phone_binding_id !== value.ref.bindingId
      || body.billing_terms_digest !== value.ref.termsDigest || body.provider_terminal.accountSid !== proof.AccountSid
      || body.provider_terminal.from !== proof.From || body.provider_terminal.to !== proof.To
      || statusEvent.bodyDigest !== sha256Hex(canonicalJson(body))) throw new Error('Inbound status evidence mismatch');
    if ((statusEvent.attempts > 0) !== Boolean(statusEvent.lastAttemptAt)
      || (statusEvent.state === 'delivered') !== Boolean(statusEvent.receipt)
      || (statusEvent.state !== 'pending' && statusEvent.attempts === 0)
      || (statusEvent.lastOutcome && statusEvent.lastOutcome.state !== statusEvent.state)
      || (statusEvent.receipt && (statusEvent.receipt.id !== value.ref.bindingId
        || statusEvent.receipt.status !== body.provider_terminal.status))) throw new Error('Inbound status receipt state invalid');
  }
  if (old?.statusEvent && (!statusEvent || !same(old.statusEvent.body, statusEvent.body)
    || old.statusEvent.bodyDigest !== statusEvent.bodyDigest || statusEvent.attempts < old.statusEvent.attempts
    || (old.statusEvent.state !== 'pending' && !same(old.statusEvent, statusEvent))))
    throw new Error('Inbound status receipt cannot change');
  if (old?.event && (!event || !same(old.event.body, event.body) || old.event.bodyDigest !== event.bodyDigest
    || event.attempts < old.event.attempts || (old.event.state !== 'pending' && !same(old.event, event))))
    throw new Error('Inbound billing receipt cannot change');
}

export function reduceInboundBilling(call: BrokerCall, command: InboundBillingCommand): InboundBillingJournal {
  validateInboundBilling(call);
  if (!call.inboundBilling) throw new Error('Inbound billing unavailable');
  const next = structuredClone(call.inboundBilling);
  if (command.kind === 'terminal') {
    time.parse(command.observedAt);
    if (next.terminal && next.terminal.status !== command.status) throw new Error('Inbound terminal evidence conflicts');
    next.terminal ??= { status: command.status, observedAt: command.observedAt };
  } else if (command.kind === 'timing_attempt') {
    if (next.event || next.statusEvent || (!next.terminal && !call.endedAt && !call.inbound?.localStop)) throw new Error('Inbound timing is not pending');
    next.lastTimingAttemptAt = time.parse(command.observedAt);
  } else if (command.kind === 'timing_failed') {
    if (next.event || next.statusEvent || (!next.terminal && !call.endedAt && !call.inbound?.localStop)) throw new Error('Inbound timing is not pending');
    next.timingError = command.reason;
  } else if (command.kind === 'provider_status') {
    const observation = inboundProviderStatusSchema.parse(command.observation);
    if (next.terminal?.status !== observation.status) throw new Error('Inbound terminal status evidence required');
    const body = inboundStatusEndedSchema.parse({ source: 'phone', direction: 'inbound', purpose: 'primary_inbound_status_v1',
      external_sid: call.callSid, phone_binding_id: next.ref.bindingId, broker_call_id: call.id,
      billing_terms_digest: next.ref.termsDigest, provider_terminal: observation });
    if (next.statusEvent && !same(next.statusEvent.body, body)) throw new Error('Inbound status cannot change');
    next.statusEvent ??= { body, bodyDigest: sha256Hex(canonicalJson(body)), state: 'pending', attempts: 0 };
    delete next.timingError;
  } else if (command.kind === 'status_attempt' || command.kind === 'status_delivery') {
    const event = next.statusEvent;
    if (!event || event.bodyDigest !== command.bodyDigest || event.state !== 'pending')
      throw new Error('Inbound status callback is not pending');
    if (command.kind === 'status_attempt') {
      event.attempts++; event.lastAttemptAt = time.parse(command.observedAt);
    } else {
      if (!event.attempts) throw new Error('Inbound status callback was not attempted');
      event.state = command.result.state;
      if (command.result.state === 'delivered') {
        event.receipt = inboundStatusReceiptSchema.parse(command.result.receipt); delete event.lastOutcome;
      } else event.lastOutcome = deliveryError.parse(command.result);
    }
  } else if (command.kind === 'provider') {
    const observation = inboundProviderTerminalSchema.parse(command.observation);
    if (next.terminal?.status !== 'completed') throw new Error('Inbound completed evidence required');
    const body = inboundCallEndedSchema.parse({ source: 'phone', direction: 'inbound', purpose: 'primary_inbound_v1',
      external_sid: call.callSid, phone_binding_id: next.ref.bindingId, broker_call_id: call.id,
      billing_terms_digest: next.ref.termsDigest, started_at: observation.startedAt, ended_at: observation.endedAt,
      duration_seconds: observation.durationSeconds, provider_terminal: observation });
    if (next.event && !same(next.event.body, body)) throw new Error('Inbound timing cannot change');
    next.event ??= { body, bodyDigest: sha256Hex(canonicalJson(body)), state: 'pending', attempts: 0 };
    delete next.timingError;
  } else {
    const event = next.event;
    if (!event || event.bodyDigest !== command.bodyDigest || event.state !== 'pending')
      throw new Error('Inbound callback is not pending');
    if (command.kind === 'attempt') {
      event.attempts++; event.lastAttemptAt = time.parse(command.observedAt);
    } else {
      if (!event.attempts) throw new Error('Inbound callback was not attempted');
      event.state = command.result.state;
      if (command.result.state === 'delivered') {
        event.receipt = inboundBillingReceiptSchema.parse(command.result.receipt); delete event.lastOutcome;
      } else event.lastOutcome = deliveryError.parse(command.result);
    }
  }
  validateInboundBilling({ ...call, inboundBilling: next }, call);
  return next;
}

/** Only an authenticated fetch with exact leg identity may create the financial body. */
export function inboundTerminalFromProvider(call: BrokerCall, raw: unknown, observedAt: string) {
  const row = raw as { sid?: unknown; accountSid?: unknown; from?: unknown; to?: unknown; direction?: unknown;
    status?: unknown; startTime?: unknown; endTime?: unknown; duration?: unknown } | null;
  const expected = call.inbound?.begin.providerProof.params;
  if (!row || !expected || row.sid !== call.callSid || row.accountSid !== expected.AccountSid
    || row.from !== expected.From || row.to !== expected.To || row.direction !== 'inbound')
    return { error: 'provider_identity_mismatch' as const };
  if (row.status !== call.inboundBilling?.terminal?.status) return { error: 'provider_terminal_conflict' as const };
  if (inboundNonCompletedStatusSchema.safeParse(row.status).success) {
    const date = (value: unknown) => value == null ? null
      : value instanceof Date && Number.isFinite(value.getTime()) ? value.toISOString() : undefined;
    const duration = row.duration == null ? null
      : typeof row.duration === 'string' && /^\d+$/.test(row.duration) ? Number(row.duration) : undefined;
    const parsed = inboundProviderStatusSchema.safeParse({ source: 'authenticated_provider_fetch', observedAt,
      accountSid: row.accountSid, callSid: row.sid, from: row.from, to: row.to, direction: row.direction, status: row.status,
      startedAt: date(row.startTime), endedAt: date(row.endTime), durationSeconds: duration });
    return parsed.success ? { statusObservation: parsed.data } : { error: 'provider_timing_unavailable' as const };
  }
  if (row.status !== 'completed' || !(row.startTime instanceof Date) || !(row.endTime instanceof Date)
    || typeof row.duration !== 'string' || !/^\d+$/.test(row.duration)
    || !Number.isFinite(row.startTime.getTime()) || !Number.isFinite(row.endTime.getTime()))
    return { error: 'provider_timing_unavailable' as const };
  const parsed = inboundProviderTerminalSchema.safeParse({ source: 'authenticated_provider_fetch', observedAt,
    accountSid: row.accountSid, callSid: row.sid, from: row.from, to: row.to, direction: row.direction, status: row.status,
    startedAt: row.startTime.toISOString(), endedAt: row.endTime.toISOString(), durationSeconds: Number(row.duration) });
  return parsed.success ? { observation: parsed.data } : { error: 'provider_timing_unavailable' as const };
}

export function inboundBillingSummary(call: BrokerCall) {
  if (!call.inboundBilling) return undefined;
  const value = call.inboundBilling;
  if (value.terminal && value.terminal.status !== 'completed') {
    return { state: value.statusEvent?.state === 'delivered' ? 'not_billable' : value.statusEvent?.state ?? 'status_pending',
      receiptId: value.statusEvent?.receipt?.id ?? null, providerStatus: value.terminal.status,
      durationSeconds: value.statusEvent?.body.provider_terminal.durationSeconds ?? null,
      problem: value.statusEvent?.lastOutcome?.reason ?? value.timingError ?? null };
  }
  return { state: value.event?.state ?? (value.terminal ? 'timing_pending' : 'awaiting_terminal'),
    receiptId: value.event?.receipt?.id ?? null, providerStatus: value.terminal?.status ?? null,
    durationSeconds: value.event?.body.duration_seconds ?? null,
    problem: value.event?.lastOutcome?.reason ?? value.timingError ?? null };
}
