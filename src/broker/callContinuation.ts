import { z } from 'zod';
import { brokerCallAssignmentSchema } from './assignment.js';
import { PHONE_REQUEST_DELIVERY_VERSION, requestDeliveryRequestSchema,
  type RequestDeliveryRequest } from './requestDelivery.js';

/** An explicit selection of existing work. It supplies no execution authority. */
export const callContinuationSchema = requestDeliveryRequestSchema.omit({ v: true }).strict();
export type CallContinuation = z.infer<typeof callContinuationSchema>;
export type CallContinuationErrorCode = 'invalid_call_continuation' | 'call_continuation_not_allowed'
  | 'call_continuation_identity_mismatch' | 'call_continuation_changed';
export class CallContinuationError extends Error {
  constructor(readonly code: CallContinuationErrorCode) {
    super(code);
    this.name = 'CallContinuationError';
  }
}

/** Undefined is legacy absence; null and unsupported fields are invalid. */
export function parseOptionalCallContinuation(value: unknown): CallContinuation | undefined {
  if (value === undefined) return undefined;
  const parsed = callContinuationSchema.safeParse(value);
  if (!parsed.success) throw new CallContinuationError('invalid_call_continuation');
  return parsed.data;
}

/** Omit the key entirely for calls without an explicit continuation. */
export function callContinuationPatch(value: unknown): { continuation?: CallContinuation } {
  const continuation = parseOptionalCallContinuation(value);
  return continuation === undefined ? {} : { continuation };
}

/** Call reservation compares this before any readiness check or provider dispatch. */
export function assertCallContinuationReplay(saved: unknown, incoming: unknown): CallContinuation | undefined {
  const original = parseOptionalCallContinuation(saved);
  const selected = parseOptionalCallContinuation(incoming);
  if (original === undefined ? selected !== undefined : selected === undefined
    || original.originalRequestId !== selected.originalRequestId || original.questionId !== selected.questionId
    || original.questionRevision !== selected.questionRevision) throw new CallContinuationError('call_continuation_changed');
  return original;
}

export interface CallContinuationContext {
  id: string;
  requestId: string;
  direction: string;
  purpose: string;
  continuation?: unknown;
  callAssignment?: unknown;
  inbound?: unknown;
  harkJournal?: unknown;
  harkDispatch?: unknown;
  harkReceiptOutbox?: unknown;
}

/** Authentication is the control route's responsibility; the journal must retain its assignment. */
export function validateCallContinuation(call: CallContinuationContext): CallContinuation | undefined {
  const continuation = parseOptionalCallContinuation(call.continuation);
  if (continuation === undefined) return undefined;
  if (call.direction !== 'outbound' || call.inbound !== undefined || call.harkJournal !== undefined
    || call.harkDispatch !== undefined || call.harkReceiptOutbox !== undefined)
    throw new CallContinuationError('call_continuation_not_allowed');
  const assignment = brokerCallAssignmentSchema.safeParse(call.callAssignment);
  if (!assignment.success || assignment.data.origin !== 'authenticated_broker_control')
    throw new CallContinuationError('call_continuation_not_allowed');
  const value = assignment.data.value;
  if (value.assignmentId !== call.requestId || value.brokerCallId !== call.id || value.purpose !== call.purpose)
    throw new CallContinuationError('call_continuation_identity_mismatch');
  return continuation;
}

/** Resolve owner, binding, canonical request and run only through the API association response. */
export function requestDeliveryRequestForContinuation(value: unknown): RequestDeliveryRequest {
  const continuation = parseOptionalCallContinuation(value);
  if (continuation === undefined) throw new CallContinuationError('invalid_call_continuation');
  return requestDeliveryRequestSchema.parse({ v: PHONE_REQUEST_DELIVERY_VERSION, ...continuation });
}
