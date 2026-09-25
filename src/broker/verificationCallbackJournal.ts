import { callbackReceiptSchema, callbackEndedTimingSchema, validateCallbackReceipts } from './verificationCallbackReceipts.js';
import { restrictedMediaSchema, validateRestrictedMedia } from './restrictedMedia.js';
import { z } from 'zod';
import {
  callbackCommandForPurpose, callbackProviderProofSchema, phoneVerificationCallbackAnswerSchema,
  phoneVerificationCallbackCommandRef, phoneVerificationCallbackCommandRefSchema,
  phoneVerificationCallbackCompletionRecoverySchema, phoneVerificationCallbackCompletionSchema,
  phoneVerificationCallbackDispatchResultSchema, phoneVerificationCallbackPermitSchema,
  phoneVerificationCallbackStateSchema, sameCallbackCommand, sameCallbackRef, assertCallbackStateIdentity,
  type CallbackProviderProof, type PhoneVerificationCallbackCompletion, type PhoneVerificationCallbackCompletionRecovery,
  type PhoneVerificationCallbackDispatchResult, type PhoneVerificationCallbackPermit, type PhoneVerificationCallbackState,
} from './verificationCallbackContract.js';

const timestamp = z.string().datetime().refine(value => new Date(value).toISOString() === value);
const uuid = z.string().uuid().refine(value => value === value.toLowerCase());
const callSid = z.string().regex(/^CA[0-9a-fA-F]{32}$/);
const terminal = z.enum(['completed', 'busy', 'failed', 'no-answer', 'canceled']);
const observation = z.object({ observedAt: timestamp, callSid,
  status: z.enum(['queued', 'ringing', 'in-progress', 'completed', 'busy', 'failed', 'no-answer', 'canceled']),
  source: z.enum(['create', 'signed_callback', 'fetch', 'hangup']),
}).strict();
const journalShape = z.object({
  v: z.literal(1), permit: phoneVerificationCallbackPermitSchema, reservedAt: timestamp,
  dispatch: z.object({ attemptId: uuid, startedAt: timestamp,
    grant: phoneVerificationCallbackDispatchResultSchema.optional(), providerStartedAt: timestamp.optional(),
  }).strict().optional(),
  provider: observation.optional(),
  endedTiming: callbackEndedTimingSchema.optional(), receipts: z.array(callbackReceiptSchema).max(9).optional(),
  providerTerminal: z.object({ observedAt: timestamp, callSid, status: terminal,
    source: z.enum(['create', 'signed_callback', 'fetch', 'hangup']), }).strict().optional(),
  answer: z.object({ startedAt: timestamp, proof: callbackProviderProofSchema }).strict().optional(),
  remote: phoneVerificationCallbackStateSchema.optional(),
  media: restrictedMediaSchema.optional(),
  completion: z.object({ startedAt: timestamp, command: phoneVerificationCallbackCommandRefSchema,
    observation: phoneVerificationCallbackCompletionRecoverySchema.optional(), }).strict().optional(),
  stopRequestedAt: timestamp.optional(), endRequestedAt: timestamp.optional(), endDispatchedAt: timestamp.optional(), endAcknowledgedAt: timestamp.optional(),
}).strict();
export type VerificationCallbackJournal = z.output<typeof journalShape>;
export class VerificationCallbackJournalError extends Error {
  constructor(readonly code: 'invalid' | 'identity_changed' | 'transition_refused') {
    super(`callback_journal_${code}`); this.name = 'VerificationCallbackJournalError';
  }
}
function fail(code: VerificationCallbackJournalError['code']): never { throw new VerificationCallbackJournalError(code); }
const same = (a: unknown, b: unknown) => JSON.stringify(a) === JSON.stringify(b);
function at(raw: string): number {
  if (!timestamp.safeParse(raw).success) fail('invalid');
  return Date.parse(raw);
}
function answerMatches(j: VerificationCallbackJournal, proof: CallbackProviderProof): boolean {
  const p = proof.params;
  return proof.url === j.permit.answerUrl && p.AccountSid === j.permit.providerAccountSid
    && p.From === j.permit.from && p.To === j.permit.to && /^CA[0-9a-fA-F]{32}$/.test(p.CallSid ?? '')
    && p.Direction === 'outbound-api' && p.CallStatus === 'in-progress'
    && !Object.hasOwn(p, 'Digits') && !Object.hasOwn(p, 'SpeechResult')
    && (!j.provider || j.provider.callSid === p.CallSid);
}
function completionMatches(j: VerificationCallbackJournal, value: PhoneVerificationCallbackCompletionRecovery): boolean {
  return !!j.completion && sameCallbackCommand(value.command, j.completion.command)
    && (value.state !== 'enrolled' || j.permit.purpose === 'enrollment')
    && (value.state !== 'admitted' || j.permit.purpose === 'account_access');
}
/** Validates private state only. No transcript, raw challenge or enrollment form is a journal field. */
export function parseVerificationCallbackJournal(raw: unknown): VerificationCallbackJournal {
  const parsed = journalShape.safeParse(raw);
  if (!parsed.success) fail('invalid');
  const j = parsed.data, d = j.dispatch;
  if (at(j.reservedAt) < at(j.permit.createdAt) || at(j.reservedAt) >= at(j.permit.expiresAt)) fail('invalid');
  if (d && (at(d.startedAt) < at(j.reservedAt) || at(d.startedAt) >= at(j.permit.expiresAt))) fail('invalid');
  if (d?.grant && (!same(d.grant.permit, j.permit) || d.grant.dispatchAttemptId !== d.attemptId)) fail('identity_changed');
  if (d?.providerStartedAt && (!d.grant || at(d.providerStartedAt) < at(d.startedAt)
    || at(d.providerStartedAt) >= at(j.permit.expiresAt))) fail('invalid');
  if (j.provider && (!d?.providerStartedAt || at(j.provider.observedAt) < at(d.providerStartedAt))) fail('invalid');
  if (j.providerTerminal && (!j.provider || j.providerTerminal.callSid !== j.provider.callSid
    || at(j.providerTerminal.observedAt) < at(d?.providerStartedAt ?? j.reservedAt))) fail('invalid');
  if (j.endedTiming && !j.providerTerminal) fail('invalid');
  if (j.receipts) {
    validateCallbackReceipts(j.permit, j.provider?.callSid, j.receipts);
    for (const receipt of j.receipts) if (receipt.kind === 'ended' && (!j.endedTiming || !j.providerTerminal
      || receipt.body.started_at !== j.endedTiming.startedAt || receipt.body.ended_at !== j.endedTiming.endedAt
      || receipt.body.duration_seconds !== j.endedTiming.durationSeconds)) fail('identity_changed');
  }
  if (j.answer && (!d?.providerStartedAt || !answerMatches(j, j.answer.proof)
    || at(j.answer.startedAt) < at(d.providerStartedAt) || at(j.answer.startedAt) >= at(j.permit.expiresAt))) fail('invalid');
  if (j.remote) {
    try { assertCallbackStateIdentity(j.remote, j.permit.request); } catch { fail('identity_changed'); }
    if (j.remote.state === 'reserved' && !same(j.remote.permit, j.permit)) fail('identity_changed');
    if ((j.remote.state === 'dispatching' || j.remote.state === 'answered')
      && j.remote.dispatchAttemptId !== d?.attemptId) fail('identity_changed');
    if (j.remote.state === 'answered' && (!j.answer || j.remote.callSid !== j.answer.proof.params.CallSid
      || j.remote.purpose !== j.permit.purpose || j.remote.expiresAt !== j.permit.expiresAt)) fail('identity_changed');
  }
  if (j.media) {
    validateRestrictedMedia(j.media);
    if (!j.answer || at(j.media.admittedAt) < at(j.answer.startedAt)
      || at(j.media.deadlineAt) !== Math.min(at(j.permit.expiresAt), at(j.answer.startedAt) + j.permit.maxDurationSeconds * 1000)
      || (j.media.privateRequestedAt && (j.permit.purpose !== 'account_access' || j.completion?.observation?.state !== 'admitted'))
      || (j.media.protectedRequestedAt && j.permit.purpose !== 'enrollment')) fail('invalid');
  }
  if (j.completion) {
    if (!j.answer || !callbackCommandForPurpose(j.completion.command, j.permit.purpose)
      || !sameCallbackRef(j.completion.command.request, j.permit.request)
      || j.completion.command.callSid !== j.answer.proof.params.CallSid
      || at(j.completion.startedAt) < at(j.answer.startedAt)
      || at(j.completion.startedAt) >= at(j.permit.expiresAt)) fail('identity_changed');
    if (j.completion.observation && !completionMatches(j, j.completion.observation)) fail('identity_changed');
  }
  if (j.remote?.state === 'completed' && (!j.completion || !completionMatches(j, j.remote.result))) fail('identity_changed');
  if (j.endDispatchedAt && (!j.endRequestedAt || at(j.endDispatchedAt) < at(j.endRequestedAt)
    || (!j.provider && !j.answer))) fail('invalid');
  if (j.endAcknowledgedAt && (!j.endDispatchedAt || at(j.endAcknowledgedAt) < at(j.endDispatchedAt))) fail('invalid');
  return structuredClone(j);
}
export function validateVerificationCallbackUpdate(previous: VerificationCallbackJournal, next: VerificationCallbackJournal): void {
  const p = parseVerificationCallbackJournal(previous), n = parseVerificationCallbackJournal(next);
  if (!same(p.permit, n.permit) || p.reservedAt !== n.reservedAt) fail('identity_changed');
  if (p.dispatch && (!n.dispatch || p.dispatch.attemptId !== n.dispatch.attemptId || p.dispatch.startedAt !== n.dispatch.startedAt
    || (p.dispatch.grant && !same(p.dispatch.grant, n.dispatch.grant))
    || (p.dispatch.providerStartedAt && p.dispatch.providerStartedAt !== n.dispatch.providerStartedAt))) fail('identity_changed');
  if (p.endedTiming && !same(p.endedTiming, n.endedTiming)) fail('identity_changed');
  if (p.receipts && !n.receipts) fail('identity_changed');
  if (n.receipts) {
    validateCallbackReceipts(n.permit, n.provider?.callSid, n.receipts, p.receipts);
    const observedState = n.providerTerminal?.status ?? n.provider?.status;
    for (const receipt of n.receipts.slice(p.receipts?.length ?? 0)) if (receipt.kind === 'status'
      && receipt.body.state !== (observedState === 'in-progress' ? 'answered' : observedState)) fail('identity_changed');
  }
  if (p.media && !n.media) fail('identity_changed');
  if (n.media) validateRestrictedMedia(n.media, p.media);
  if (p.provider && (!n.provider || p.provider.callSid !== n.provider.callSid
    || at(n.provider.observedAt) < at(p.provider.observedAt))) fail('identity_changed');
  for (const key of ['answer', 'providerTerminal', 'stopRequestedAt', 'endRequestedAt', 'endDispatchedAt', 'endAcknowledgedAt'] as const)
    if (p[key] !== undefined && !same(p[key], n[key])) fail('identity_changed');
  if (p.completion && (!n.completion || p.completion.startedAt !== n.completion.startedAt
    || !same(p.completion.command, n.completion.command)
    || (p.completion.observation?.state !== undefined && p.completion.observation.state !== 'absent'
      && !same(p.completion.observation, n.completion.observation)))) fail('identity_changed');
  if (p.remote && ['completed', 'cancelled', 'expired'].includes(p.remote.state) && !same(p.remote, n.remote)) fail('identity_changed');
}
function next(previous: VerificationCallbackJournal, patch: Partial<VerificationCallbackJournal>): VerificationCallbackJournal {
  const result = parseVerificationCallbackJournal({ ...previous, ...patch });
  validateVerificationCallbackUpdate(previous, result); return result;
}
function live(j: VerificationCallbackJournal, when: string): void {
  const now = at(when);
  if (now < at(j.reservedAt) || now >= at(j.permit.expiresAt) || j.stopRequestedAt || j.endRequestedAt || j.providerTerminal
    || (j.remote && ['completed', 'cancelled', 'expired'].includes(j.remote.state))) fail('transition_refused');
  if (j.answer && now >= at(j.answer.startedAt) + j.permit.maxDurationSeconds * 1000) fail('transition_refused');
}
export function reserveVerificationCallback(permit: PhoneVerificationCallbackPermit, when: string): VerificationCallbackJournal {
  return parseVerificationCallbackJournal({ v: 1, permit, reservedAt: when });
}
export function prepareVerificationCallbackDispatch(raw: VerificationCallbackJournal, attemptId: string, when: string): VerificationCallbackJournal {
  const j = parseVerificationCallbackJournal(raw); live(j, when);
  if (j.dispatch) fail('transition_refused');
  return next(j, { dispatch: { attemptId, startedAt: when } });
}
export function recordVerificationCallbackGrant(raw: VerificationCallbackJournal, grant: PhoneVerificationCallbackDispatchResult): VerificationCallbackJournal {
  const j = parseVerificationCallbackJournal(raw); if (!j.dispatch || j.dispatch.providerStartedAt) fail('transition_refused');
  return next(j, { dispatch: { ...j.dispatch, grant } });
}
/** Persist this fence before provider.create. Its existence permanently prohibits another create, including absent recovery. */
export function fenceVerificationCallbackProvider(raw: VerificationCallbackJournal, when: string): VerificationCallbackJournal {
  const j = parseVerificationCallbackJournal(raw); live(j, when);
  if (!j.dispatch?.grant || j.dispatch.providerStartedAt) fail('transition_refused');
  return next(j, { dispatch: { ...j.dispatch, providerStartedAt: when } });
}
export function recordVerificationCallbackProvider(raw: VerificationCallbackJournal, value: {
  accountSid: string; from: string; to: string; callSid: string;
  status: z.output<typeof observation>['status']; source: z.output<typeof observation>['source']; observedAt: string;
}): VerificationCallbackJournal {
  const j = parseVerificationCallbackJournal(raw);
  if (value.accountSid !== j.permit.providerAccountSid || value.from !== j.permit.from || value.to !== j.permit.to
    || (j.provider && j.provider.callSid !== value.callSid)
    || (j.answer && j.answer.proof.params.CallSid !== value.callSid)) fail('identity_changed');
  const provider = observation.parse({ callSid: value.callSid, status: value.status, source: value.source, observedAt: value.observedAt });
  // A late create acknowledgment cannot undo an earlier signed terminal callback.
  if (j.providerTerminal) return j;
  return next(j, { provider, ...(terminal.safeParse(value.status).success
    ? { providerTerminal: { ...provider, status: terminal.parse(value.status) } } : {}) });
}
export function prepareVerificationCallbackAnswer(raw: VerificationCallbackJournal, proof: CallbackProviderProof, when: string): VerificationCallbackJournal {
  const j = parseVerificationCallbackJournal(raw); live(j, when);
  if (!j.dispatch?.providerStartedAt || !answerMatches(j, proof)) fail('identity_changed');
  if (j.answer) { if (!same(j.answer.proof, proof)) fail('identity_changed'); return j; }
  phoneVerificationCallbackAnswerSchema.parse({ v: 1, request: j.permit.request,
    brokerCallId: j.permit.request.requestId, dispatchAttemptId: j.dispatch.attemptId, providerProof: proof });
  return next(j, { answer: { startedAt: when, proof } });
}
export function recordVerificationCallbackRemote(raw: VerificationCallbackJournal, state: PhoneVerificationCallbackState): VerificationCallbackJournal {
  const j = parseVerificationCallbackJournal(raw);
  return next(j, { remote: state, ...(state.state === 'completed' && j.completion
    ? { completion: { ...j.completion, observation: state.result } } : {}) });
}
/** Challenge is parsed ephemerally and excluded from command and private journal. */
export function prepareVerificationCallbackCompletion(raw: VerificationCallbackJournal, input: PhoneVerificationCallbackCompletion,
  when: string): VerificationCallbackJournal {
  const j = parseVerificationCallbackJournal(raw); live(j, when);
  const value = phoneVerificationCallbackCompletionSchema.parse(input);
  if (j.remote?.state !== 'answered' || value.purpose !== j.permit.purpose || j.completion
    || value.callSid !== j.remote.callSid || !sameCallbackRef(value.request, j.permit.request)) fail('transition_refused');
  return next(j, { completion: { startedAt: when, command: phoneVerificationCallbackCommandRef(value) } });
}
export function recordVerificationCallbackCompletion(raw: VerificationCallbackJournal, result: PhoneVerificationCallbackCompletionRecovery): VerificationCallbackJournal {
  const j = parseVerificationCallbackJournal(raw); if (!j.completion) fail('transition_refused');
  return next(j, { completion: { ...j.completion, observation: result } });
}
/** Local hangup intent/ACK are distinct from exact provider terminal evidence. */
export function stopVerificationCallback(raw: VerificationCallbackJournal, when: string): VerificationCallbackJournal {
  const j = parseVerificationCallbackJournal(raw); at(when);
  return next(j, { stopRequestedAt: j.stopRequestedAt ?? when });
}
export function endVerificationCallback(raw: VerificationCallbackJournal, when: string, acknowledged = false): VerificationCallbackJournal {
  const j = parseVerificationCallbackJournal(raw); at(when);
  if (acknowledged && !j.endDispatchedAt) fail('transition_refused');
  return next(j, acknowledged ? { endAcknowledgedAt: j.endAcknowledgedAt ?? when }
    : { endRequestedAt: j.endRequestedAt ?? when });
}

export function fenceVerificationCallbackEnd(raw: VerificationCallbackJournal, when: string): VerificationCallbackJournal {
  const j = parseVerificationCallbackJournal(raw);
  if (!j.endRequestedAt || j.endDispatchedAt || (!j.provider && !j.answer)) fail('transition_refused');
  return next(j, { endDispatchedAt: when });
}

export const verificationCallbackPurpose = 'Complete the account-approved phone verification or optional callback.';
export interface VerificationCallbackCall {
  id: string; requestId: string; direction: string; number: string; purpose: string; createdAt: string;
  callSid?: string; verificationCallback?: VerificationCallbackJournal;
  primary?: { callAssignmentRef?: unknown }; callAssignment?: unknown; continuation?: unknown; selectedQuestion?: unknown;
  inbound?: unknown; callback?: unknown; harkJournal?: unknown; harkDispatch?: unknown;
  transcript?: unknown; canonicalTurns?: unknown; callerRouting?: unknown; delegations: Record<string, unknown>;
}
export function validateVerificationCallbackCall(call: VerificationCallbackCall, previous?: VerificationCallbackCall): void {
  if (previous?.verificationCallback && !call.verificationCallback) fail('identity_changed');
  if (!call.verificationCallback) return;
  const j = parseVerificationCallbackJournal(call.verificationCallback);
  const admitted = j.permit.purpose === 'account_access' && j.completion?.observation?.state === 'admitted';
  if (call.id !== j.permit.request.requestId || call.requestId !== call.id || call.direction !== 'outbound'
    || call.number !== j.permit.to || call.purpose !== verificationCallbackPurpose || call.createdAt !== j.reservedAt
    || (call.primary && (!admitted || call.primary.callAssignmentRef)) || call.callAssignment || call.continuation || call.selectedQuestion || call.inbound || call.callback
    || call.harkJournal || call.harkDispatch || ((!admitted || !call.primary)
      && (call.transcript || call.canonicalTurns || call.callerRouting || Object.keys(call.delegations).length))) fail('identity_changed');
  const sid = j.provider?.callSid ?? j.answer?.proof.params.CallSid;
  if (call.callSid !== sid) fail('identity_changed');
  if (previous?.verificationCallback) validateVerificationCallbackUpdate(previous.verificationCallback, j);
}
