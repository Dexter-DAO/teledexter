import { createHash } from 'node:crypto';
import { z } from 'zod';

export const OUTBOUND_SETTLEMENT_VERSION = 'broker.outbound-settlement.v1' as const;
const uuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
const digest = z.string().regex(/^[0-9a-f]{64}$/);
const callSid = z.string().regex(/^CA[0-9a-fA-F]{32}$/);
const observedAt = z.string().datetime({ offset: true });
const identity = { v: z.literal(OUTBOUND_SETTLEMENT_VERSION), callId: uuid, requestId: uuid };
const unresolved = z.object({ ...identity, state: z.literal('unresolved') }).strict();
const neverDispatched = z.object({ ...identity, state: z.literal('settled'), kind: z.literal('never_dispatched'),
  observedAt, reason: z.enum(['call_already_open', 'preflight_refused', 'service_stopping']) }).strict();
const providerTerminal = z.object({ ...identity, state: z.literal('settled'), kind: z.literal('provider_terminal'),
  observedAt, callSid, providerStatus: z.enum(['completed', 'busy', 'no-answer', 'canceled', 'failed']),
  source: z.enum(['signed_status_callback', 'authenticated_fetch', 'authenticated_update']), evidenceDigest: digest }).strict();
const settledInputSchema = z.union([neverDispatched, providerTerminal]);
export type OutboundSettledInput = z.infer<typeof settledInputSchema>;

/** Only binds the projected fields. Authentication and durable observation are producer obligations. */
export function outboundSettlementDigest(input: OutboundSettledInput): string {
  const value = settledInputSchema.parse(input);
  const provider = value.kind === 'provider_terminal' ? value : undefined;
  return createHash('sha256').update(JSON.stringify(['broker.outbound-settlement/v1',
    value.callId, value.requestId, value.kind, value.observedAt, provider?.callSid ?? null,
    provider?.providerStatus ?? null, provider?.source ?? null, provider?.evidenceDigest ?? null,
    value.kind === 'never_dispatched' ? value.reason : null]), 'utf8').digest('hex');
}

export const outboundSettlementSchema = z.union([
  unresolved, neverDispatched.extend({ digest }).strict(), providerTerminal.extend({ digest }).strict(),
]).superRefine((value, context) => {
  if (value.state === 'unresolved') return;
  const { digest: savedDigest, ...input } = value;
  if (savedDigest !== outboundSettlementDigest(input)) context.addIssue({ code: z.ZodIssueCode.custom,
    path: ['digest'], message: 'Outbound settlement digest differs' });
});
export type OutboundSettlement = z.infer<typeof outboundSettlementSchema>;
export type SettledOutboundSettlement = Extract<OutboundSettlement, { state: 'settled' }>;

/** Called after trusted observation is durably retained; it does not authenticate that observation. */
export function createOutboundSettlement(input: OutboundSettledInput): SettledOutboundSettlement {
  const value = settledInputSchema.parse(input);
  return { ...value, digest: outboundSettlementDigest(value) };
}

export interface OutboundSettlementCallIdentity { callId: string; requestId: string; callSid?: string }

/** Undefined preserves historical absence. Never infer evidence from local status or endedAt. */
export function outboundSettlementForCall(value: unknown, call: OutboundSettlementCallIdentity): OutboundSettlement | undefined {
  if (value === undefined) return undefined;
  const receipt = outboundSettlementSchema.parse(value);
  if (receipt.callId !== call.callId || receipt.requestId !== call.requestId
    || (receipt.state === 'settled' && (receipt.kind === 'provider_terminal'
      ? receipt.callSid !== call.callSid : call.callSid !== undefined)))
    throw new Error('Outbound settlement call identity differs');
  return receipt;
}

export const outboundRetryOfSchema = z.object({ callId: uuid, requestId: uuid, settlementDigest: digest }).strict();
export const outboundRetrySuccessorSchema = outboundRetryOfSchema;
export type OutboundRetryOf = z.infer<typeof outboundRetryOfSchema>;
export type OutboundRetrySuccessor = z.infer<typeof outboundRetrySuccessorSchema>;

export function parseOptionalOutboundRetryOf(value: unknown): OutboundRetryOf | undefined {
  return value === undefined ? undefined : outboundRetryOfSchema.parse(value);
}

export function outboundRetryOfPatch(value: unknown): { retryOf?: OutboundRetryOf } {
  const retryOf = parseOptionalOutboundRetryOf(value);
  return retryOf === undefined ? {} : { retryOf };
}

export class OutboundRetryError extends Error {
  constructor(readonly code: 'call_request_conflict' | 'call_retry_unavailable' | 'call_request_unavailable') {
    super(code); this.name = 'OutboundRetryError';
  }
}

/** Existing request recovery compares the optional edge before any fresh admission. */
export function assertOutboundRetryReplay(saved: unknown, incoming: unknown): OutboundRetryOf | undefined {
  const before = parseOptionalOutboundRetryOf(saved), after = parseOptionalOutboundRetryOf(incoming);
  if (before === undefined ? after !== undefined : after === undefined || before.callId !== after.callId
    || before.requestId !== after.requestId || before.settlementDigest !== after.settlementDigest)
    throw new OutboundRetryError('call_request_conflict');
  return before;
}

/** Transport evidence permits only fresh admission, never an automatic call or authority reuse. */
export function assertOutboundRetrySettlement(call: OutboundSettlementCallIdentity, value: unknown,
  retryOf: OutboundRetryOf): SettledOutboundSettlement {
  const edge = outboundRetryOfSchema.parse(retryOf);
  if (edge.callId !== call.callId || edge.requestId !== call.requestId)
    throw new OutboundRetryError('call_request_conflict');
  let settlement: OutboundSettlement | undefined;
  try { settlement = outboundSettlementForCall(value, call); }
  catch { throw new OutboundRetryError('call_request_unavailable'); }
  if (settlement === undefined || settlement.state === 'unresolved') throw new OutboundRetryError('call_retry_unavailable');
  if (settlement.digest !== edge.settlementDigest) throw new OutboundRetryError('call_request_conflict');
  if (settlement.kind === 'provider_terminal' && settlement.providerStatus === 'completed')
    throw new OutboundRetryError('call_retry_unavailable');
  return settlement;
}
