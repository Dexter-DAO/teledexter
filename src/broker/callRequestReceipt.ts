import { z } from 'zod';
import { validateCallContinuation, type CallContinuationContext } from './callContinuation.js';
import { outboundSettlementFromObservation, type OutboundObservationCall } from './outboundObservation.js';
import { outboundRetryOfSchema, outboundRetryOfPatch, outboundSettlementSchema } from './outboundSettlement.js';

export const CALL_REQUEST_RECEIPT_NAMESPACE = 'dexter-broker-call-request/v1';
export const PHONE_OWNER_CALL_PURPOSE = 'Discuss the selected saved question.';
const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
// Match the API consumer schema. Producer lookup below preserves saved bytes.
const wireUuid = z.string().regex(uuidPattern).transform(value => value.toLowerCase());
export const callRequestContinuationSchema = z.object({ originalRequestId: wireUuid,
  questionId: wireUuid, questionRevision: z.literal(1) }).strict();
export const callRequestStateSchema = z.enum(['prepared', 'dialing', 'queued', 'ringing', 'in-progress',
  'completed', 'busy', 'no-answer', 'canceled', 'failed', 'unknown']);
export const callRequestCallSchema = z.object({ id: wireUuid, requestId: wireUuid,
  direction: z.literal('outbound'), purpose: z.literal(PHONE_OWNER_CALL_PURPOSE),
  continuation: callRequestContinuationSchema, status: callRequestStateSchema,
  retryOf: outboundRetryOfSchema.optional(), outboundSettlement: outboundSettlementSchema.optional() }).strict()
  .refine(value => !value.outboundSettlement || (value.outboundSettlement.callId === value.id
    && value.outboundSettlement.requestId === value.requestId), { message: 'Saved settlement identity differs' });
export const callRequestReceiptSchema = z.object({ schemaVersion: z.literal(1),
  namespace: z.literal(CALL_REQUEST_RECEIPT_NAMESPACE), requestId: wireUuid, call: callRequestCallSchema })
  .strict().refine(value => value.requestId === value.call.requestId,
    { message: 'Saved request identity differs' });
export type CallRequestReceipt = z.infer<typeof callRequestReceiptSchema>;

export type CallRequestReceiptErrorCode = 'invalid_call_request' | 'unauthorized'
  | 'call_request_not_found' | 'call_request_conflict' | 'call_request_unavailable';
const errorStatuses = { invalid_call_request: 400, unauthorized: 401, call_request_not_found: 404,
  call_request_conflict: 409, call_request_unavailable: 503 } as const;
export class CallRequestReceiptError extends Error {
  readonly status: 400 | 401 | 404 | 409 | 503;
  constructor(readonly code: CallRequestReceiptErrorCode) {
    super(code); this.name = 'CallRequestReceiptError'; this.status = errorStatuses[code];
  }
}

/** Existing BrokerStore must verify its full journal using its existing record
 * validators, current parent/writer ownership, open state, write-failure latch,
 * and disk/cache equality before returning available. Missing storage is
 * unavailable. Neither a recent-call list nor a cached miss satisfies this port.
 * The operation is synchronous and read-only; it must never adopt or repair data. */
export type CallRequestJournalAvailability = { state: 'available'; journal: unknown }
  | { state: 'unavailable' | 'uncertain' };
export interface CallRequestJournalPort {
  readCallRequestJournal(): CallRequestJournalAvailability;
}

function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
function unavailable(): never { throw new CallRequestReceiptError('call_request_unavailable'); }

/** Validate route syntax without rewriting an existing request identity. */
export function callRequestIdFromLookupPath(route: string): string {
  const prefix = '/internal/broker/call-requests/';
  const requestId = route.startsWith(prefix) ? route.slice(prefix.length) : '';
  if (!uuidPattern.test(requestId)) throw new CallRequestReceiptError('invalid_call_request');
  return requestId;
}

function fullJournal(port: CallRequestJournalPort): Record<string, Record<string, unknown>> {
  try {
    const read = port.readCallRequestJournal();
    if (read.state !== 'available') return unavailable();
    const journal: unknown = structuredClone(read.journal);
    if (!object(journal) || journal.version !== 1 || !object(journal.calls)
      || Object.keys(journal).some(key => key !== 'version' && key !== 'calls')) return unavailable();
    const seen = new Set<string>();
    for (const [id, call] of Object.entries(journal.calls)) {
      // Preserve the existing journal's identity syntax and casing. This index
      // imposes no new selected-call schema on unrelated legacy records.
      if (!object(call) || call.id !== id || !id || typeof call.requestId !== 'string'
        || !call.requestId || seen.has(call.requestId)) return unavailable();
      seen.add(call.requestId);
    }
    return journal.calls as Record<string, Record<string, unknown>>;
  } catch { return unavailable(); }
}

/** A compatible receipt is a saved reservation, including terminal and unknown
 * states. This function performs no current admission or provider operation. */
export function readCallRequestReceipt(port: CallRequestJournalPort, requestId: unknown): CallRequestReceipt {
  if (typeof requestId !== 'string' || !uuidPattern.test(requestId))
    throw new CallRequestReceiptError('invalid_call_request');
  const calls = fullJournal(port);
  const call = Object.values(calls).find(value => value.requestId === requestId);
  if (!call) throw new CallRequestReceiptError('call_request_not_found');
  if (call.continuation === undefined || call.direction !== 'outbound'
    || call.purpose !== PHONE_OWNER_CALL_PURPOSE || call.inbound !== undefined
    || call.harkJournal !== undefined || call.harkDispatch !== undefined || call.harkReceiptOutbox !== undefined
    || (object(call.callAssignment) && call.callAssignment.origin === 'authenticated_hark_delivery'))
    throw new CallRequestReceiptError('call_request_conflict');
  let transport: ReturnType<typeof outboundSettlementFromObservation>;
  try { transport = outboundSettlementFromObservation(call as unknown as OutboundObservationCall); }
  catch { return unavailable(); }
  let retry: ReturnType<typeof outboundRetryOfPatch>;
  try { retry = outboundRetryOfPatch(call.retryOf); } catch { return unavailable(); }
  const projected = { id: call.id, requestId: call.requestId, direction: call.direction,
    purpose: call.purpose, continuation: call.continuation, status: call.status, ...retry,
    ...(transport === undefined ? {} : { outboundSettlement: transport }) };
  // These malformed selected records cannot come from the promised validated
  // source. Preserve storage uncertainty rather than treating corruption as absence.
  if (!callRequestCallSchema.safeParse(projected).success) return unavailable();
  try { validateCallContinuation(call as unknown as CallContinuationContext); }
  catch { return unavailable(); }
  const receipt = { schemaVersion: 1, namespace: CALL_REQUEST_RECEIPT_NAMESPACE, requestId, call: projected };
  if (!callRequestReceiptSchema.safeParse(receipt).success) return unavailable();
  // Validation intentionally does not replace the saved IDs with normalized ones.
  return receipt as CallRequestReceipt;
}

/** A route-local mapper prevents storage exception text from entering the wire. */
export function callRequestReceiptFailure(error: unknown): {
  status: 400 | 401 | 404 | 409 | 503; body: { error: CallRequestReceiptErrorCode };
} {
  const code = error instanceof CallRequestReceiptError && Object.hasOwn(errorStatuses, error.code)
    ? error.code : 'call_request_unavailable';
  return { status: errorStatuses[code], body: { error: code } };
}
