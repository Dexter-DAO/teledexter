import { createHmac } from 'node:crypto';
import { canonicalJson } from '../outbound/canonical.js';
import { frozenCallerTurnSchema, assertFrozenCallerSpeechCurrent, type FrozenCallerTurn } from './callerTurn.js';
import type { LiveDelegationRequest, LiveTranscriptSnapshot } from '../live/types.js';
import { parseVerificationCallbackJournal, type VerificationCallbackJournal } from './verificationCallbackJournal.js';
import { sameCallbackRef, type PhoneVerificationCallbackCompletion, type PhoneVerificationCallbackPurpose,
  type PhoneVerificationCallbackRef, type PhoneVerificationCallbackTurn } from './verificationCallbackContract.js';

/** Process-local capability authored after the actual setup tool's source admission.
 * Restart can recover a saved command but cannot recreate this accepted execution. */
export interface QualifiedVerificationCallbackTurn {
  readonly request: PhoneVerificationCallbackRef;
  readonly callSid: string;
  readonly sessionId: string;
  readonly purpose: PhoneVerificationCallbackPurpose;
  readonly turn: PhoneVerificationCallbackTurn;
}
const accepted = new WeakSet<QualifiedVerificationCallbackTurn>();
function fail(): never { throw new Error('callback_turn_unqualified'); }
export function qualifyVerificationCallbackTurn(input: {
  journal: VerificationCallbackJournal;
  request: LiveDelegationRequest;
  frozen: FrozenCallerTurn;
  toolCallId: string;
  current: { sessionId: string; audioThroughMs: number | undefined; observedSpeechAtMs: number | undefined;
    transcript: LiveTranscriptSnapshot };
  acceptedAtMs: number;
  receiptSecret: string;
  /** Trusted setup resolver selects completion; model/caller fields cannot set this port. */
  execution: 'complete_verification';
  isCurrent(): boolean;
}): QualifiedVerificationCallbackTurn {
  const j = parseVerificationCallbackJournal(input.journal), frozen = frozenCallerTurnSchema.parse(input.frozen);
  const call = j.remote;
  if (!/^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$/.test(input.toolCallId) || input.receiptSecret.length < 32 || input.execution !== 'complete_verification' || !input.isCurrent()
    || call?.state !== 'answered' || !j.answer || j.completion || j.stopRequestedAt || j.endRequestedAt || j.providerTerminal
    || !Number.isSafeInteger(input.acceptedAtMs) || input.acceptedAtMs < Date.parse(j.answer.startedAt)
    || input.acceptedAtMs >= Math.min(Date.parse(j.permit.expiresAt), Date.parse(j.answer.startedAt) + 180_000)
    || input.request.callId !== j.permit.request.requestId || input.request.sessionId !== frozen.sessionId
    || input.current.transcript.callerRevision !== frozen.callerRevision) fail();
  assertFrozenCallerSpeechCurrent(frozen, input.current);
  const fragments = input.current.transcript.fragments.filter(fragment => fragment.role === 'user'
    && fragment.sequence >= frozen.firstSequence && fragment.sequence <= frozen.lastSequence);
  if (!fragments.length || fragments[0].eventId !== frozen.firstEventId
    || fragments.map(fragment => fragment.text).join('') !== frozen.turn.text) fail();
  // Raw text may contain a volunteered challenge. Keyed local receipt prevents
  // offline enumeration; only its safe id/digest can enter journal or API input.
  const bytes = canonicalJson(['dexter.broker.callback-accepted-turn/v1', j.permit.request, call.callSid,
    j.permit.purpose, input.request.sessionId, input.request.delegationId, input.request.requestId, input.toolCallId, frozen]);
  const digest = createHmac('sha256', input.receiptSecret).update(bytes).digest('hex');
  const identity = createHmac('sha256', input.receiptSecret).update('dexter.broker.callback-turn-id/v1\0' + bytes).digest();
  identity[6] = (identity[6] & 15) | 128; identity[8] = (identity[8] & 63) | 128;
  const hex = identity.subarray(0, 16).toString('hex');
  const id = `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
  const value: QualifiedVerificationCallbackTurn = Object.freeze({
    request: Object.freeze({ ...j.permit.request }), callSid: call.callSid, sessionId: frozen.sessionId,
    purpose: j.permit.purpose, turn: Object.freeze({ id, digest }),
  });
  accepted.add(value); return value;
}
export function assertQualifiedVerificationCallbackTurn(value: QualifiedVerificationCallbackTurn,
  input: PhoneVerificationCallbackCompletion): void {
  if (!accepted.has(value) || !sameCallbackRef(value.request, input.request) || value.callSid !== input.callSid
    || value.purpose !== input.purpose || value.turn.id !== input.turn.id || value.turn.digest !== input.turn.digest) fail();
}
