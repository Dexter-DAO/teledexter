import { createHash } from 'node:crypto';
import { z } from 'zod';
// Wire mirror of API phoneResultSmsContract.ts. Keep the cross-repository fixture test.
function encodePrimaryDexterEventCursor(threadId: string, sequence: bigint) {
  return Buffer.from(`dexter-primary-event/v1:${threadId}:${sequence}`, 'utf8').toString('base64url');
}

export const PHONE_RESULT_SMS_VERSION = 'phone.result-sms.v1';
const uuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
const digest = z.string().regex(/^[a-f0-9]{64}$/);
const callSid = z.string().regex(/^CA[0-9a-f]{32}$/i);
const phone = z.string().regex(/^\+[1-9][0-9]{7,14}$/);
const sequence = z.string().regex(/^[1-9][0-9]{0,18}$/)
  .refine(value => /^[1-9][0-9]{0,18}$/.test(value) && BigInt(value) <= 9_223_372_036_854_775_807n);
const hash = (value: string) => createHash('sha256').update(value, 'utf8').digest('hex');

/** The caller selects a saved task; recipient and result content are server-owned. */
export const phoneResultSmsAdmissionSchema = z.object({
  v: z.literal(PHONE_RESULT_SMS_VERSION),
  callerIntentOperationId: uuid,
  requestId: uuid,
}).strict();
export type PhoneResultSmsAdmission = z.infer<typeof phoneResultSmsAdmissionSchema>;

export const phoneResultSmsSourceSchema = z.object({
  callerIntentOperationId: uuid,
  sourceEventId: uuid,
  sourceItemId: z.string().min(1).max(200),
  sourceRevision: z.literal(1),
  sourceDigest: digest,
}).strict();

export const phoneResultSmsAnswerSchema = z.object({
  eventId: uuid,
  messageId: uuid,
  sequence,
  contentDigest: digest,
}).strict();
export type PhoneResultSmsAnswer = z.infer<typeof phoneResultSmsAnswerSchema>;

/** This schema describes immutable identity. It grants no current send authority. */
export const phoneResultSmsIdentitySchema = z.object({
  v: z.literal(PHONE_RESULT_SMS_VERSION),
  intentId: uuid,
  bindingId: uuid,
  callSid,
  requestId: uuid,
  runId: uuid,
  ownerPrincipalId: uuid,
  primaryDexterId: uuid,
  threadId: uuid,
  phoneNumber: phone,
  source: phoneResultSmsSourceSchema,
  admittedAt: z.string().datetime({ offset: true }),
  expiresAt: z.string().datetime({ offset: true }),
}).strict();
export type PhoneResultSmsIdentity = z.infer<typeof phoneResultSmsIdentitySchema>;

export function phoneResultSmsIntentDigest(value: PhoneResultSmsIdentity): string {
  return hash(JSON.stringify(['dexter-phone-result-sms-intent/v1', phoneResultSmsIdentitySchema.parse(value)]));
}

export const phoneResultSmsStateSchema = z.enum(['waiting_result', 'ready', 'dispatching', 'accepted',
  'delivered', 'refused', 'expired', 'uncertain']);
export type PhoneResultSmsState = z.infer<typeof phoneResultSmsStateSchema>;
export const phoneResultSmsAdmissionReceiptSchema = z.object({
  v: z.literal(PHONE_RESULT_SMS_VERSION), intentId: uuid, intentDigest: digest,
  state: phoneResultSmsStateSchema, replayed: z.boolean(),
}).strict();
export type PhoneResultSmsAdmissionReceipt = z.infer<typeof phoneResultSmsAdmissionReceiptSchema>;

/** Same wire URL as phoneSavedConversationHref; the account reader still authenticates its owner. */
export function phoneResultSmsAnswerHref(threadId: string, messageId: string, eventSequence: string): string {
  uuid.parse(threadId); uuid.parse(messageId); sequence.parse(eventSequence);
  const query = new URLSearchParams({ threadId, messageId });
  const value = BigInt(eventSequence);
  // readEvents is exclusive. The result's own cursor would skip the requested answer.
  if (value > 1n) query.set('eventCursor', encodePrimaryDexterEventCursor(threadId, value - 1n));
  return `https://dexter.cash/wallet/dexter?${query}`;
}

/** Matches SmsService's existing durable key, including its fixed identification/STOP wrapper. */
export function phoneResultSmsReceiptIdentity(number: string, sid: string, message: string) {
  phone.parse(number); callSid.parse(sid);
  if (!message.trim() || message.length > 1000 || /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(message))
    throw new TypeError('invalid_sms_message');
  const bodyHash = hash(`Dexter: ${message.trim()}\nReply STOP to opt out. HELP for help.`);
  return { bodyHash, messageId: hash(`${sid}:${number}:${bodyHash}`) };
}

export function phoneResultSmsPayload(identityValue: PhoneResultSmsIdentity, answerValue: PhoneResultSmsAnswer) {
  const identity = phoneResultSmsIdentitySchema.parse(identityValue);
  const answer = phoneResultSmsAnswerSchema.parse(answerValue);
  const href = phoneResultSmsAnswerHref(identity.threadId, answer.messageId, answer.sequence);
  const message = `Your requested answer is ready: ${href}`;
  return { answer, href, message, ...phoneResultSmsReceiptIdentity(identity.phoneNumber, identity.callSid, message) };
}
export type PhoneResultSmsPayload = ReturnType<typeof phoneResultSmsPayload>;

export const phoneResultSmsResolvedSchema = z.object({
  identity: phoneResultSmsIdentitySchema,
  intentDigest: digest,
  payload: z.object({ answer: phoneResultSmsAnswerSchema, href: z.string().max(1000),
    message: z.string().max(1000), bodyHash: digest, messageId: digest }).strict(),
}).strict().superRefine((value, context) => {
  // Zod can run object refinements after a child refinement has reported an issue.
  // Do not turn safeParse into a thrown exception by re-parsing a dirty child.
  if (!phoneResultSmsIdentitySchema.safeParse(value.identity).success
    || !phoneResultSmsAnswerSchema.safeParse(value.payload.answer).success) return;
  if (value.intentDigest !== phoneResultSmsIntentDigest(value.identity))
    context.addIssue({ code: z.ZodIssueCode.custom, message: 'Intent digest differs from its frozen identity.' });
  const expected = phoneResultSmsPayload(value.identity, value.payload.answer);
  for (const key of ['href', 'message', 'bodyHash', 'messageId'] as const)
    if (expected[key] !== value.payload[key])
      context.addIssue({ code: z.ZodIssueCode.custom, path: ['payload', key], message: 'Frozen result payload differs.' });
});
export type PhoneResultSmsResolved = z.infer<typeof phoneResultSmsResolvedSchema>;

export const phoneResultSmsReceiptSchema = z.object({
  ok: z.literal(true), messageId: digest,
  status: z.enum(['dispatching', 'uncertain', 'accepted', 'scheduled', 'queued', 'sending', 'sent',
    'delivered', 'read', 'undelivered', 'failed', 'canceled']),
  duplicate: z.boolean(),
}).strict();
export type PhoneResultSmsReceipt = z.infer<typeof phoneResultSmsReceiptSchema>;

export const phoneResultSmsDispatchRefSchema = z.object({
  v: z.literal(PHONE_RESULT_SMS_VERSION), intentId: uuid, intentDigest: digest,
  messageId: digest,
}).strict();
export type PhoneResultSmsDispatchRef = z.infer<typeof phoneResultSmsDispatchRefSchema>;
