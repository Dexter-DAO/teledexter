import { restrictedMediaSchema, validateRestrictedMedia } from './restrictedMedia.js';
import { z } from 'zod';
import { validateInboundJournal } from './inboundJournal.js';
import { phoneVerificationCallbackAccessDigest, phoneVerificationCallbackAccessRequestSchema,
  phoneVerificationCallbackRefSchema, type PhoneVerificationCallbackAccessRequest } from './verificationCallbackContract.js';
import type { BrokerCall } from './store.js';

const timestamp = z.string().datetime().refine(value => new Date(value).toISOString() === value);
const uuid = z.string().uuid().refine(value => value === value.toLowerCase());
const digest = z.string().regex(/^[0-9a-f]{64}$/);
export const restrictedSetupJournalSchema = z.object({
  v: z.literal(1), callId: uuid, callSid: z.string().regex(/^CA[0-9a-fA-F]{32}$/),
  beginBodyDigest: digest, attachmentId: uuid, disposition: z.enum(['cancelled', 'expired']),
  selectedAt: timestamp, deadlineAt: timestamp,
  access: z.object({ request: phoneVerificationCallbackRefSchema, dispatchStartedAt: timestamp,
    refusal: z.object({ observedAt: timestamp, status: z.union([z.literal(403), z.literal(409)]),
      code: z.enum(['callback_consent_required', 'phone_tab_required', 'callback_financial_admission_required']),
      retryable: z.literal(false) }).strict().refine(value => value.status === (value.code === 'callback_consent_required' ? 403 : 409)).optional(),
  }).strict().optional(),
  stoppedAt: timestamp.optional(), media: restrictedMediaSchema.optional(),
}).strict();
export type RestrictedSetupJournal = z.output<typeof restrictedSetupJournalSchema>;
function fail(): never { throw new Error('restricted_setup_disposition_refused'); }
const equal = (a: unknown, b: unknown) => JSON.stringify(a) === JSON.stringify(b);

/** This additive disposition cannot remove the ordinary attempt, stop or cancellation history. */
export function validateRestrictedSetupCall(call: BrokerCall, previous?: BrokerCall): void {
  if (previous?.restrictedSetup && !call.restrictedSetup) fail();
  if (!call.restrictedSetup) return;
  const r = restrictedSetupJournalSchema.parse(call.restrictedSetup);
  validateInboundJournal(call, previous);
  const j = call.inbound;
  if (!j || call.direction !== 'inbound' || call.id !== r.callId || call.callSid !== r.callSid
    || j.beginBodyDigest !== r.beginBodyDigest || !j.admissionRefusal || !j.localStop || !j.cancelRequestedAt
    || j.mode?.kind === 'enrollment-only' || j.mode?.kind === 'undecided'
    || !j.remote || (j.remote.state !== 'cancelled' && j.remote.state !== 'expired')
    || j.remote.state !== r.disposition || j.remote.attachmentId !== r.attachmentId
    || j.approvalRef || j.consumeAttemptedAt || j.closure || call.primary || call.primaryClosed
    || call.verificationCallback || call.callback || call.callAssignment || call.harkJournal || call.continuation
    || call.transcript || call.canonicalTurns || Object.keys(call.delegations).length
    || Date.parse(r.selectedAt) < Date.parse(j.admissionRefusal.observedAt)
    || Date.parse(r.selectedAt) < Date.parse(call.createdAt) || Date.parse(r.deadlineAt) <= Date.parse(r.selectedAt)) fail();
  if (r.access) {
    const input: PhoneVerificationCallbackAccessRequest = { v: 1, requestId: r.access.request.requestId,
      original: j.begin, explicitlyRequested: true };
    if (r.access.request.requestDigest !== phoneVerificationCallbackAccessDigest(input)
      || Date.parse(r.access.dispatchStartedAt) < Date.parse(r.selectedAt)
      || Date.parse(r.access.dispatchStartedAt) >= Date.parse(r.deadlineAt)
      || (r.access.refusal && Date.parse(r.access.refusal.observedAt) < Date.parse(r.access.dispatchStartedAt))) fail();
  }
  if (r.media) {
    validateRestrictedMedia(r.media, previous?.restrictedSetup?.media);
    if (r.media.deadlineAt !== r.deadlineAt || Date.parse(r.media.admittedAt) < Date.parse(r.selectedAt)
      || r.media.protectedRequestedAt || r.media.privateRequestedAt) fail();
  }
  if (previous?.restrictedSetup?.media && !r.media) fail();
  const p = previous?.restrictedSetup;
  if (p) {
    for (const key of ['v', 'callId', 'callSid', 'beginBodyDigest', 'attachmentId', 'disposition', 'selectedAt', 'deadlineAt'] as const)
      if (p[key] !== r[key]) fail();
    if ((p.access && (!r.access || !equal(p.access.request, r.access.request) || p.access.dispatchStartedAt !== r.access.dispatchStartedAt
      || (p.access.refusal && !equal(p.access.refusal, r.access.refusal)))) || (p.stoppedAt && p.stoppedAt !== r.stoppedAt)) fail();
  }
}
export function prepareRestrictedSetup(call: BrokerCall, selectedAt: string, originalDeadlineAt: string): RestrictedSetupJournal {
  // Existing disposition recovers as saved even after expiry. It never acquires a new clock.
  if (call.restrictedSetup) { validateRestrictedSetupCall(call); return structuredClone(call.restrictedSetup); }
  const j = call.inbound;
  if (!j?.remote || (j.remote.state !== 'cancelled' && j.remote.state !== 'expired') || !call.callSid) fail();
  const value = restrictedSetupJournalSchema.parse({ v: 1, callId: call.id, callSid: call.callSid,
    beginBodyDigest: j.beginBodyDigest, attachmentId: j.remote.attachmentId, disposition: j.remote.state,
    selectedAt, deadlineAt: originalDeadlineAt });
  validateRestrictedSetupCall({ ...call, restrictedSetup: value }, call);
  return value;
}
/** Host calls only after exact accepted explicit callback request; this fence precedes the service POST. */
export function prepareRestrictedAccess(call: BrokerCall, input: PhoneVerificationCallbackAccessRequest,
  observedAt: string): RestrictedSetupJournal {
  validateRestrictedSetupCall(call);
  const r = call.restrictedSetup, value = phoneVerificationCallbackAccessRequestSchema.parse(input);
  if (!r || !call.inbound || r.stoppedAt || call.endedAt || r.access
    || Date.parse(observedAt) >= Date.parse(r.deadlineAt) || !equal(value.original, call.inbound.begin)) fail();
  const next: RestrictedSetupJournal = { ...r, access: { request: { requestId: value.requestId,
    requestDigest: phoneVerificationCallbackAccessDigest(value) }, dispatchStartedAt: observedAt } };
  validateRestrictedSetupCall({ ...call, restrictedSetup: next }, call); return next;
}
export function stopRestrictedSetup(call: BrokerCall, observedAt: string): RestrictedSetupJournal {
  validateRestrictedSetupCall(call); const r = call.restrictedSetup; if (!r) fail();
  timestamp.parse(observedAt);
  const next = { ...r, stoppedAt: r.stoppedAt ?? observedAt };
  validateRestrictedSetupCall({ ...call, restrictedSetup: next }, call); return next;
}

export function recordRestrictedAccessRefusal(call: BrokerCall, refusal: NonNullable<NonNullable<RestrictedSetupJournal['access']>['refusal']>): RestrictedSetupJournal {
  validateRestrictedSetupCall(call); const saved = call.restrictedSetup;
  if (!saved?.access) fail();
  const next = restrictedSetupJournalSchema.parse({ ...saved, access: { ...saved.access, refusal } });
  validateRestrictedSetupCall({ ...call, restrictedSetup: next }, call); return next;
}
