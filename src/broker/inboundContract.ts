import { createHash } from 'node:crypto';
import { z } from 'zod';
import { phoneInboundBillingRefSchema } from './inboundBilling.js';

const uuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
const sha256 = z.string().regex(/^[0-9a-f]{64}$/);
const callSid = z.string().regex(/^CA[0-9a-fA-F]{32}$/);
const accountSid = /^AC[0-9a-fA-F]{32}$/;
const phoneNumber = /^\+[1-9][0-9]{7,14}$/;
const timestamp = z.iso.datetime({ offset: true });
const inputUuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i)
  .transform(value => value.toLowerCase());

export const inboundProviderProofSchema = z.object({
  url: z.string().url().max(2048),
  signature: z.string().max(128),
  params: z.record(z.string().max(128), z.string().max(2048)).refine(params =>
    Object.keys(params).length <= 80 && accountSid.test(params.AccountSid ?? '')
    && callSid.safeParse(params.CallSid).success && params.Direction === 'inbound'
    && phoneNumber.test(params.From ?? '') && phoneNumber.test(params.To ?? '')),
}).strict();

export const inboundBeginIdentitySchema = z.object({
  brokerCallId: inputUuid,
  providerProof: inboundProviderProofSchema,
}).strict();
export type InboundBeginIdentity = z.infer<typeof inboundBeginIdentitySchema>;

export const inboundBeginRefSchema = z.object({ brokerCallId: uuid, callSid, digest: sha256 }).strict();
export type InboundBeginRef = z.infer<typeof inboundBeginRefSchema>;

export const inboundApprovalRefSchema = z.object({ attachmentId: uuid, approvalId: uuid, digest: sha256 }).strict();
export type InboundApprovalRef = z.infer<typeof inboundApprovalRefSchema>;

const envelope = { attachmentId: uuid, beginRef: inboundBeginRefSchema };
export const inboundOutcomeSchema = z.discriminatedUnion('state', [
  z.object({ ...envelope, state: z.literal('pending'), expiresAt: timestamp }).strict(),
  z.object({ ...envelope, state: z.literal('approved'), expiresAt: timestamp,
    approvalRef: inboundApprovalRefSchema }).strict(),
  z.object({ ...envelope, state: z.literal('bound'), approvalRef: inboundApprovalRefSchema,
    bindingId: uuid, consumedAt: timestamp, callState: z.enum(['open', 'closed']), billing: phoneInboundBillingRefSchema.optional() }).strict(),
  z.object({ ...envelope, state: z.literal('expired'), expiresAt: timestamp }).strict(),
  z.object({ ...envelope, state: z.literal('cancelled'), cancelledAt: timestamp,
    expiresAt: timestamp.nullable() }).strict(),
]).refine(outcome => !('approvalRef' in outcome) || outcome.approvalRef.attachmentId === outcome.attachmentId);
export type InboundOutcome = z.infer<typeof inboundOutcomeSchema>;

export const recoverInboundResultSchema = z.union([
  inboundOutcomeSchema,
  z.object({ state: z.literal('absent'), beginRef: inboundBeginRefSchema }).strict(),
]);
export type RecoverInboundResult = z.infer<typeof recoverInboundResultSchema>;

export const consumeInboundApprovalSchema = z.object({
  providerProof: inboundProviderProofSchema,
  inboundApprovalRef: inboundApprovalRefSchema,
}).strict();
export type ConsumeInboundApproval = z.infer<typeof consumeInboundApprovalSchema>;

export const closeConsumedInboundAttachmentSchema = z.object({
  begin: inboundBeginIdentitySchema,
  approvalRef: inboundApprovalRefSchema,
  bindingId: uuid,
}).strict();
export type CloseConsumedInboundAttachment = z.infer<typeof closeConsumedInboundAttachmentSchema>;

export const consumedInboundClosureReceiptSchema = z.object({
  attachmentId: uuid,
  beginRef: inboundBeginRefSchema,
  approvalRef: inboundApprovalRefSchema,
  bindingId: uuid,
  callSid,
  state: z.literal('closed'),
  closedAt: timestamp,
}).strict().refine(receipt => receipt.approvalRef.attachmentId === receipt.attachmentId
  && receipt.beginRef.callSid === receipt.callSid);
export type ConsumedInboundClosureReceipt = z.infer<typeof consumedInboundClosureReceiptSchema>;

export function sameInboundBeginRef(left: InboundBeginRef, right: InboundBeginRef): boolean {
  return left.brokerCallId === right.brokerCallId && left.callSid === right.callSid && left.digest === right.digest;
}

export function sameInboundApprovalRef(left: InboundApprovalRef, right: InboundApprovalRef): boolean {
  return left.attachmentId === right.attachmentId && left.approvalId === right.approvalId && left.digest === right.digest;
}

/** Exact shared proof projection; this does not verify the provider signature. */
export function inboundProofDigest(proof: { url: string; params: Record<string, string> }): string {
  return createHash('sha256').update(JSON.stringify([
    proof.url, Object.keys(proof.params).sort().map(key => [key, proof.params[key]]),
  ]), 'utf8').digest('hex');
}

/** Shared phone.inbound-begin.v1 identity projection, never possession proof. */
export function inboundBeginRef(input: InboundBeginIdentity): InboundBeginRef {
  const begin = inboundBeginIdentitySchema.parse(input);
  const p = begin.providerProof.params;
  const digest = createHash('sha256').update(JSON.stringify([
    'phone.inbound-begin.v1', begin.brokerCallId, p.AccountSid, p.CallSid,
    'inbound', p.From, p.To, inboundProofDigest(begin.providerProof),
  ]), 'utf8').digest('hex');
  return { brokerCallId: begin.brokerCallId, callSid: p.CallSid!, digest };
}
