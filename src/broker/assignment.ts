import { z } from 'zod';
import { harkPhoneCallAssignmentSchema } from './harkAssignment.js';
import { notificationPhoneCallAssignmentSchema } from './notificationAssignment.js';

const retainedUuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
const uuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
// Existing journal identities remain readable byte-for-byte. This is never the
// admission schema for a new assignment or an API registration.
const retainedPhoneCallAssignmentSchema = z.object({
  assignmentId: retainedUuid, brokerCallId: retainedUuid, revision: z.literal(1),
  // Match the authenticated control route: String.length counts UTF-16 units.
  purpose: z.string().min(1).max(1500).refine(value => value === value.trim()),
}).strict();
const operatorPhoneCallAssignmentSchema = retainedPhoneCallAssignmentSchema.extend({
  assignmentId: uuid, brokerCallId: uuid,
  purpose: z.string().min(1).max(1500).refine(value => value === value.trim() && !/[\u0000\uD800-\uDFFF]/u.test(value)),
});
export const newPhoneCallControlSchema = operatorPhoneCallAssignmentSchema.pick({ assignmentId: true, purpose: true });
export const phoneCallAssignmentSchema = z.union([operatorPhoneCallAssignmentSchema, harkPhoneCallAssignmentSchema, notificationPhoneCallAssignmentSchema]);
export const phoneCallAssignmentRefSchema = z.object({
  assignmentId: uuid, revision: z.literal(1), digest: z.string().regex(/^[0-9a-f]{64}$/),
}).strict();
export const retainedPhoneCallAssignmentRefSchema = phoneCallAssignmentRefSchema.extend({ assignmentId: retainedUuid });
export type PhoneCallAssignment = z.infer<typeof phoneCallAssignmentSchema>;
export type PhoneCallAssignmentRef = z.infer<typeof phoneCallAssignmentRefSchema>;
export const brokerCallAssignmentSchema = z.discriminatedUnion('origin', [
  z.object({ origin: z.literal('authenticated_broker_control'), value: retainedPhoneCallAssignmentSchema }).strict(),
  z.object({ origin: z.literal('authenticated_hark_delivery'), value: harkPhoneCallAssignmentSchema }).strict(),
  z.object({ origin: z.literal('authenticated_notification_delivery'), value: notificationPhoneCallAssignmentSchema }).strict(),
]);
export type BrokerCallAssignment = z.infer<typeof brokerCallAssignmentSchema>;
export function sameCallAssignmentRef(a: PhoneCallAssignmentRef | undefined, b: PhoneCallAssignmentRef | undefined): boolean {
  return a === undefined ? b === undefined : b !== undefined
    && a.assignmentId === b.assignmentId && a.revision === b.revision && a.digest === b.digest;
}
