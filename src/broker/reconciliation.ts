import { createHash } from 'node:crypto';
import type { PrimaryClosedCall, PrimarySubmission, PrimaryTurn, PrimaryTurnReceipt } from './primaryApi.js';

export interface PhoneNonAdmissionProof {
  schemaVersion: 1;
  kind: 'phone_closed_call_non_admission';
  callSid: string;
  source: { itemId: string; sourceEventId: string; revision: 1; receivedAt: string };
  rawSubmissionDigest: string;
  turn: { recordId: string; revisionRecordId: string; bodyDigest: string };
  closedAt: string;
}
export type AcknowledgedTurnReceipt = PrimaryTurnReceipt & { revisionRecordId: string; bodyDigest: string };
const sha = (value: unknown) => createHash('sha256').update(JSON.stringify(value), 'utf8').digest('hex');
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

export function originalTurn(input: PrimarySubmission): PrimaryTurn {
  return { sourceEventId: input.source.sourceEventId, revision: 1, itemId: input.source.itemId,
    role: 'user', text: input.text, occurredAt: input.source.receivedAt };
}
export function turnBodyDigest(turn: PrimaryTurn): string {
  return sha({ sourceEventId: turn.sourceEventId, revision: turn.revision, itemId: turn.itemId,
    role: turn.role, text: turn.text, occurredAt: turn.occurredAt });
}
export function rawSubmissionDigest(bindingId: string, input: PrimarySubmission): string {
  return sha({ bindingId, callSid: input.source.callSid, itemId: input.source.itemId,
    sourceEventId: input.source.sourceEventId, revision: input.source.revision,
    receivedAt: input.source.receivedAt, role: input.source.role, text: input.text });
}
export function acknowledgedTurn(bindingId: string, turn: PrimaryTurn, receipt: PrimaryTurnReceipt): AcknowledgedTurnReceipt {
  if (receipt.bindingId !== bindingId || receipt.sourceEventId !== turn.sourceEventId
      || receipt.revision !== turn.revision || receipt.recordId !== turn.sourceEventId
      || !receipt.revisionRecordId || !uuid.test(receipt.revisionRecordId)
      || receipt.bodyDigest !== turnBodyDigest(turn)) throw new Error('capture_acknowledgement_mismatch');
  return receipt as AcknowledgedTurnReceipt;
}
export function validateNonAdmissionProof(bindingId: string, input: PrimarySubmission,
  receipt: PrimaryTurnReceipt, closed: PrimaryClosedCall, proof: PhoneNonAdmissionProof): void {
  const ack = acknowledgedTurn(bindingId, originalTurn(input), receipt);
  if (closed.bindingId !== bindingId || closed.callSid !== input.source.callSid || closed.state !== 'closed'
      || proof.schemaVersion !== 1 || proof.kind !== 'phone_closed_call_non_admission'
      || proof.callSid !== input.source.callSid || proof.source.itemId !== input.source.itemId
      || proof.source.sourceEventId !== input.source.sourceEventId || proof.source.revision !== 1
      || proof.source.receivedAt !== input.source.receivedAt
      || proof.rawSubmissionDigest !== rawSubmissionDigest(bindingId, input)
      || proof.turn.recordId !== ack.recordId || proof.turn.revisionRecordId !== ack.revisionRecordId
      || proof.turn.bodyDigest !== ack.bodyDigest || !Number.isFinite(Date.parse(proof.closedAt))
      || new Date(proof.closedAt).toISOString() !== proof.closedAt) throw new Error('non_admission_proof_mismatch');
}
