import { createHash } from 'node:crypto';
import { z } from 'zod';

const uuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
const digest = z.string().regex(/^[0-9a-f]{64}$/);
const text = (max: number) => z.string().min(1).max(max).refine(s => !/[\u0000\uD800-\uDFFF]/u.test(s));
const timestamp = z.string().datetime();
export const HARK_PHONE_PURPOSE = 'Discuss the saved Hark watch event with its owner, using current information where needed.';
export const HARK_PHONE_SNAPSHOT_MAX_BYTES = 32_768;

export const phoneHarkSnapshotSchema = z.object({
  kind: z.literal('hark_watch_event_v1'),
  ownerSupabaseUserId: uuid,
  destinationId: uuid,
  deliveryId: uuid,
  caseId: uuid,
  originTaskId: text(300),
  expiresAt: timestamp,
  watch: z.object({
    id: uuid, draftDigest: digest, request: text(4000), name: text(160),
    source: z.record(z.string(), z.unknown()), condition: z.record(z.string(), z.unknown()), attention: z.record(z.string(), z.unknown()),
  }).strict(),
  event: z.object({
    id: uuid, bodyDigest: digest, observedAt: timestamp, acceptedAt: timestamp,
    kind: text(120), subject: z.record(z.string(), z.unknown()), summary: text(2000),
    facts: z.record(z.string(), z.unknown()), evidence: z.array(z.unknown()).max(32),
  }).strict(),
  billing: z.object({
    mandateId: uuid, unitPriceMicro: z.number().int().safe().positive(),
    freeSeconds: z.number().int().safe().nonnegative(),
  }).strict(),
  sourceDigest: digest,
}).strict().superRefine((snapshot, ctx) => {
  const { sourceDigest, ...source } = snapshot;
  if (harkSourceDigest(source) !== sourceDigest || Buffer.byteLength(JSON.stringify(snapshot), 'utf8') > HARK_PHONE_SNAPSHOT_MAX_BYTES)
    ctx.addIssue({ code: z.ZodIssueCode.custom, message: 'hark_snapshot_invalid' });
});
export type PhoneHarkSnapshot = z.infer<typeof phoneHarkSnapshotSchema>;

/** Canonicalizes JSON object keys only; array order, strings and numbers stay exact. */
export function canonicalHarkValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalHarkValue);
  if (value !== null && typeof value === 'object')
    return Object.fromEntries(Object.keys(value).sort().map(key => [key, canonicalHarkValue((value as Record<string, unknown>)[key])]));
  return value;
}
export function harkSourceDigest(source: Omit<PhoneHarkSnapshot, 'sourceDigest'>): string {
  return createHash('sha256').update(JSON.stringify({ namespace: 'dexter-phone-hark-source/v1', source: canonicalHarkValue(source) })).digest('hex');
}

export const harkPhoneCallAssignmentSchema = z.object({
  assignmentId: uuid,
  brokerCallId: uuid,
  revision: z.literal(1),
  purpose: z.literal(HARK_PHONE_PURPOSE),
  hark: phoneHarkSnapshotSchema,
}).strict().superRefine((assignment, ctx) => {
  if (assignment.assignmentId !== assignment.hark.deliveryId || assignment.brokerCallId !== assignment.hark.deliveryId)
    ctx.addIssue({ code: z.ZodIssueCode.custom, message: 'hark_assignment_identity_invalid' });
});
export type HarkPhoneCallAssignment = z.infer<typeof harkPhoneCallAssignmentSchema>;

export const resolvedHarkAssignmentSchema = z.object({
  schemaVersion: z.literal(1),
  to: z.string().regex(/^\+[1-9]\d{7,14}$/),
  callAssignment: harkPhoneCallAssignmentSchema,
}).strict();
export type ResolvedHarkAssignment = z.infer<typeof resolvedHarkAssignmentSchema>;
