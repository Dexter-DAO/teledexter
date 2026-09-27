import { createHash } from 'node:crypto';
import { z } from 'zod';
import { canonicalHarkValue } from './harkAssignment.js';

const uuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
const digest = z.string().regex(/^[0-9a-f]{64}$/);
const text = (max: number) => z.string().min(1).max(max).refine(s => !/[\u0000\uD800-\uDFFF]/u.test(s));
export const NOTIFICATION_PHONE_PURPOSE = 'Discuss the saved notification with its owner, using current information where needed.';
export const phoneNotificationSourceSchema = z.discriminatedUnion('kind', [
  z.object({ kind: z.literal('publication_v1'), publishId: uuid, topicId: uuid, subscriptionId: uuid }).strict(),
  z.object({ kind: z.literal('owner_destination_test_v1') }).strict(),
  z.object({ kind: z.literal('watcher_event_v1'), producer: z.enum(['market_pulse', 'founder_portfolio_pulse']), eventId: text(160) }).strict(),
  z.object({ kind: z.literal('portfolio_watch_event_v1'), intentId: uuid, eventKey: text(200), bodySha256: digest }).strict(),
]);
export const phoneNotificationSnapshotSchema = z.object({
  kind: z.literal('phone_notification_v1'), intentId: uuid,
  eventId: z.string().min(8).max(160).regex(/^[A-Za-z0-9._:-]+$/),
  ownerSupabaseUserId: uuid, destinationId: uuid, phoneNumberDigest: digest,
  source: phoneNotificationSourceSchema,
  title: text(200), summary: text(4000), evidence: z.record(z.string(), z.unknown()),
  observedAt: z.string().datetime(), deliveryNotBefore: z.string().datetime(), expiresAt: z.string().datetime(),
  billing: z.object({ mandateId: uuid, unitPriceMicro: z.number().int().safe().positive(),
    freeSeconds: z.number().int().safe().nonnegative() }).strict(),
  sourceDigest: digest,
}).strict().superRefine((snapshot, ctx) => {
  const { sourceDigest, ...source } = snapshot;
  if (phoneNotificationSourceDigest(source) !== sourceDigest || Buffer.byteLength(JSON.stringify(snapshot), 'utf8') > 32_768
    || Date.parse(snapshot.deliveryNotBefore) >= Date.parse(snapshot.expiresAt)
    || Date.parse(snapshot.observedAt) > Date.parse(snapshot.expiresAt)
    || (snapshot.source.kind === 'publication_v1' && snapshot.eventId !== `hark:${snapshot.source.publishId}:${snapshot.source.subscriptionId}`)
    || (snapshot.source.kind === 'owner_destination_test_v1' && !/^hark:self-test:[0-9a-f-]{36}$/.test(snapshot.eventId))
    || (snapshot.source.kind === 'watcher_event_v1' && (snapshot.source.eventId !== snapshot.eventId
      || !snapshot.eventId.startsWith(snapshot.source.producer === 'market_pulse' ? 'market-pulse:' : 'wallet-portfolio:')))
    || (snapshot.source.kind === 'portfolio_watch_event_v1'
      && (snapshot.source.intentId !== snapshot.intentId || snapshot.eventId !== `portfolio:${snapshot.intentId}`)))
    ctx.addIssue({ code: z.ZodIssueCode.custom, message: 'notification_snapshot_invalid' });
});
export type PhoneNotificationSnapshot = z.infer<typeof phoneNotificationSnapshotSchema>;
export function phoneNotificationSourceDigest(source: Omit<PhoneNotificationSnapshot, 'sourceDigest'>): string {
  return createHash('sha256').update(JSON.stringify({ namespace: 'dexter-phone-notification-source/v1', source: canonicalHarkValue(source) })).digest('hex');
}
export const phoneNotificationNumberDigest = (number: string) =>
  createHash('sha256').update(`dexter-phone-notification-number/v1:${number}`).digest('hex');
export const phoneNotificationRefSchema = z.object({ v: z.literal(1), intentId: uuid, sourceDigest: digest }).strict();
export type PhoneNotificationRef = z.infer<typeof phoneNotificationRefSchema>;

export const notificationPhoneCallAssignmentSchema = z.object({
  assignmentId: uuid, brokerCallId: uuid, revision: z.literal(1), purpose: z.literal(NOTIFICATION_PHONE_PURPOSE),
  notification: phoneNotificationSnapshotSchema,
}).strict().superRefine((assignment, ctx) => {
  if (assignment.assignmentId !== assignment.notification.intentId || assignment.brokerCallId !== assignment.notification.intentId)
    ctx.addIssue({ code: z.ZodIssueCode.custom, message: 'notification_assignment_identity_invalid' });
});
export type NotificationPhoneCallAssignment = z.infer<typeof notificationPhoneCallAssignmentSchema>;
export const resolvedNotificationAssignmentSchema = z.object({
  schemaVersion: z.literal(1), to: z.string().regex(/^\+[1-9]\d{7,14}$/), callAssignment: notificationPhoneCallAssignmentSchema,
}).strict().superRefine((resolved, ctx) => {
  if (phoneNotificationNumberDigest(resolved.to) !== resolved.callAssignment.notification.phoneNumberDigest)
    ctx.addIssue({ code: z.ZodIssueCode.custom, message: 'notification_destination_digest_invalid' });
});
export type ResolvedNotificationAssignment = z.infer<typeof resolvedNotificationAssignmentSchema>;
