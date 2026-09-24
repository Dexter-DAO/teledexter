import { z } from 'zod';
import { canonicalJson } from '../outbound/canonical.js';
import { validateCallContinuation, type CallContinuationContext } from './callContinuation.js';
import { OUTBOUND_SETTLEMENT_VERSION, createOutboundSettlement, type OutboundSettlement } from './outboundSettlement.js';

export const OUTBOUND_OBSERVATION_VERSION = 'broker.outbound-observation.v1' as const;
const uuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
const sid = z.string().regex(/^CA[0-9a-fA-F]{32}$/);
const at = z.string().datetime({ offset: true });
const digest = z.string().regex(/^[0-9a-f]{64}$/);
const phone = z.string().regex(/^\+[1-9][0-9]{1,14}$/);
const reason = z.enum(['call_already_open', 'preflight_refused', 'service_stopping']);
const terminalStatus = z.enum(['completed', 'busy', 'no-answer', 'canceled', 'failed']);
const terminalSource = z.enum(['signed_status_callback', 'authenticated_fetch', 'authenticated_update']);

export const outboundProviderIdentitySchema = z.object({
  accountSid: z.string().regex(/^AC[0-9a-fA-F]{32}$/), from: phone, to: phone,
}).strict();
export type OutboundProviderIdentity = z.infer<typeof outboundProviderIdentitySchema>;
export const outboundAcceptedIdentitySchema = outboundProviderIdentitySchema.extend({ callSid: sid }).strict();
export type OutboundAcceptedIdentity = z.infer<typeof outboundAcceptedIdentitySchema>;

const neverSettlement = z.object({ kind: z.literal('never_dispatched'), observedAt: at, reason }).strict();
const providerSettlement = z.object({ kind: z.literal('provider_terminal'), callSid: sid, observedAt: at,
  providerStatus: terminalStatus, source: terminalSource, evidenceDigest: digest }).strict();

export const outboundObservationSchema = z.object({
  v: z.literal(OUTBOUND_OBSERVATION_VERSION), callId: uuid, requestId: uuid,
  providerIdentity: outboundProviderIdentitySchema, reservedAt: at, dispatchStartedAt: at.optional(),
  accepted: z.object({ callSid: sid, observedAt: at }).strict().optional(),
  settlement: z.discriminatedUnion('kind', [neverSettlement, providerSettlement]).optional(),
}).strict().superRefine((value, context) => {
  if (value.accepted !== undefined && value.dispatchStartedAt === undefined)
    context.addIssue({ code: z.ZodIssueCode.custom, message: 'Provider acceptance requires dispatch fence' });
  const settlement = value.settlement;
  if (settlement?.kind === 'never_dispatched' && (value.dispatchStartedAt !== undefined || value.accepted !== undefined))
    context.addIssue({ code: z.ZodIssueCode.custom, message: 'Undispatched settlement has provider dispatch evidence' });
  if (settlement?.kind === 'provider_terminal'
    && (value.dispatchStartedAt === undefined || value.accepted?.callSid !== settlement.callSid))
    context.addIssue({ code: z.ZodIssueCode.custom, message: 'Terminal settlement requires matching acceptance and fence' });
});
export type OutboundObservation = z.infer<typeof outboundObservationSchema>;

const eventIdentity = { callId: uuid, requestId: uuid, observedAt: at };
export const outboundObservationEventSchema = z.discriminatedUnion('type', [
  z.object({ ...eventIdentity, type: z.literal('dispatch_started') }).strict(),
  z.object({ ...eventIdentity, type: z.literal('never_dispatched'), reason }).strict(),
  z.object({ ...eventIdentity, type: z.literal('provider_accepted'), identity: outboundAcceptedIdentitySchema }).strict(),
  z.object({ ...eventIdentity, type: z.literal('provider_terminal'), identity: outboundAcceptedIdentitySchema,
    providerStatus: terminalStatus, source: terminalSource, evidenceDigest: digest }).strict(),
]);
export type OutboundObservationEvent = z.infer<typeof outboundObservationEventSchema>;

export interface OutboundObservationCall extends CallContinuationContext {
  number: string;
  createdAt: string;
  callSid?: string;
  outboundObservation?: unknown;
}
export type OutboundObservationErrorCode = 'outbound_observation_invalid' | 'outbound_observation_missing'
  | 'outbound_observation_scope' | 'outbound_observation_identity' | 'outbound_observation_changed'
  | 'outbound_dispatch_already_started' | 'outbound_dispatch_required' | 'outbound_already_settled';
export class OutboundObservationError extends Error {
  constructor(readonly code: OutboundObservationErrorCode) { super(code); this.name = 'OutboundObservationError'; }
}
function fail(code: OutboundObservationErrorCode): never { throw new OutboundObservationError(code); }

function parseObservation(value: unknown): OutboundObservation {
  const parsed = outboundObservationSchema.safeParse(value);
  if (!parsed.success) fail('outbound_observation_invalid');
  return parsed.data;
}

function assertScope(call: OutboundObservationCall, value: OutboundObservation): void {
  try {
    if (validateCallContinuation(call) === undefined) fail('outbound_observation_scope');
  } catch { fail('outbound_observation_scope'); }
  if (value.callId !== call.id || value.requestId !== call.requestId || value.reservedAt !== call.createdAt
    || value.providerIdentity.to !== call.number || value.accepted?.callSid !== call.callSid)
    fail('outbound_observation_identity');
}

/** Historical absence is untouched. Call only after the existing full journal validation. */
export function validateSelectedOutboundObservation(call: OutboundObservationCall,
  previous?: OutboundObservationCall): OutboundObservation | undefined {
  if (call.outboundObservation === undefined) {
    if (previous?.outboundObservation !== undefined) fail('outbound_observation_changed');
    return undefined;
  }
  const next = parseObservation(call.outboundObservation);
  assertScope(call, next);
  if (previous === undefined) return next;
  // New instrumentation belongs to reservation, never read adoption of an old call.
  if (previous.outboundObservation === undefined) fail('outbound_observation_changed');
  const before = validateSelectedOutboundObservation(previous);
  if (before === undefined) fail('outbound_observation_missing');
  for (const key of ['v', 'callId', 'requestId', 'providerIdentity', 'reservedAt'] as const)
    if (canonicalJson(before[key]) !== canonicalJson(next[key])) fail('outbound_observation_changed');
  for (const key of ['dispatchStartedAt', 'accepted', 'settlement'] as const)
    if (before[key] !== undefined && canonicalJson(before[key]) !== canonicalJson(next[key]))
      fail('outbound_observation_changed');
  return next;
}

/** Only the new selected reservation may add this field, in its existing atomic journal write. */
export function createOutboundObservation(call: OutboundObservationCall,
  providerIdentity: OutboundProviderIdentity): OutboundObservation {
  if (call.outboundObservation !== undefined) fail('outbound_observation_changed');
  if (call.callSid !== undefined) fail('outbound_observation_identity');
  const value = parseObservation({ v: OUTBOUND_OBSERVATION_VERSION, callId: call.id, requestId: call.requestId,
    providerIdentity, reservedAt: call.createdAt });
  assertScope(call, value);
  return value;
}

function assertProviderIdentity(value: OutboundObservation, identity: OutboundAcceptedIdentity): void {
  if (identity.accountSid !== value.providerIdentity.accountSid || identity.from !== value.providerIdentity.from
    || identity.to !== value.providerIdentity.to || (value.accepted !== undefined && identity.callSid !== value.accepted.callSid))
    fail('outbound_observation_identity');
}

/**
 * Pure transition only. The producer verifies callbacks/API responses before invoking this.
 * Persist the returned fence before create; persist accepted.callSid with call.callSid atomically.
 * A thrown/uncertain persistence result must prevent provider contact and latch the store unavailable.
 */
export function reduceOutboundObservation(call: OutboundObservationCall,
  input: OutboundObservationEvent): OutboundObservation {
  const parsed = outboundObservationEventSchema.safeParse(input);
  if (!parsed.success) fail('outbound_observation_invalid');
  const event = parsed.data;
  const before = validateSelectedOutboundObservation(call);
  if (before === undefined) fail('outbound_observation_missing');
  if (event.callId !== before.callId || event.requestId !== before.requestId) fail('outbound_observation_identity');
  let next: OutboundObservation;
  if (event.type === 'dispatch_started') {
    if (before.settlement !== undefined) fail('outbound_already_settled');
    // An exact duplicate must not authorize a second provider create.
    if (before.dispatchStartedAt !== undefined) fail('outbound_dispatch_already_started');
    next = { ...before, dispatchStartedAt: event.observedAt };
  } else if (event.type === 'never_dispatched') {
    if (before.dispatchStartedAt !== undefined || before.accepted !== undefined || call.callSid !== undefined)
      fail('outbound_dispatch_already_started');
    if (before.settlement !== undefined) {
      if (before.settlement.kind !== 'never_dispatched' || before.settlement.reason !== event.reason)
        fail('outbound_already_settled');
      return before;
    }
    next = { ...before, settlement: { kind: 'never_dispatched', observedAt: event.observedAt, reason: event.reason } };
  } else {
    assertProviderIdentity(before, event.identity);
    if (before.settlement?.kind === 'never_dispatched') fail('outbound_already_settled');
    if (before.dispatchStartedAt === undefined) fail('outbound_dispatch_required');
    const accepted = before.accepted ?? { callSid: event.identity.callSid, observedAt: event.observedAt };
    // Later valid observations may neither replace the first receipt nor reopen the transport.
    if (before.settlement !== undefined || (event.type === 'provider_accepted' && before.accepted !== undefined)) return before;
    next = event.type === 'provider_accepted' ? { ...before, accepted } : { ...before, accepted, settlement: {
      kind: 'provider_terminal', callSid: event.identity.callSid, observedAt: event.observedAt,
      providerStatus: event.providerStatus, source: event.source, evidenceDigest: event.evidenceDigest,
    } };
  }
  const checked = validateSelectedOutboundObservation({ ...call, outboundObservation: next,
    ...(next.accepted === undefined ? {} : { callSid: next.accepted.callSid }) }, call);
  if (checked === undefined) fail('outbound_observation_missing');
  return checked;
}

/** Derive the public field; never keep a second independently writable settlement. */
export function outboundSettlementFromObservation(call: OutboundObservationCall): OutboundSettlement | undefined {
  const observation = validateSelectedOutboundObservation(call);
  if (observation === undefined) return undefined;
  const identity = { v: OUTBOUND_SETTLEMENT_VERSION, callId: observation.callId, requestId: observation.requestId };
  return observation.settlement === undefined ? { ...identity, state: 'unresolved' }
    : createOutboundSettlement({ ...identity, state: 'settled', ...observation.settlement });
}
