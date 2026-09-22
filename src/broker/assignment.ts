import { z } from 'zod';

const retainedUuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
const uuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
// Existing journal identities remain readable byte-for-byte. This is never the
// admission schema for a new assignment or an API registration.
const retainedPhoneCallAssignmentSchema = z.object({
  assignmentId: retainedUuid, brokerCallId: retainedUuid, revision: z.literal(1),
  // Match the authenticated control route: String.length counts UTF-16 units.
  purpose: z.string().min(1).max(1500).refine(value => value === value.trim()),
}).strict();
export const phoneCallAssignmentSchema = retainedPhoneCallAssignmentSchema.extend({
  assignmentId: uuid, brokerCallId: uuid,
  purpose: z.string().min(1).max(1500).refine(value => value === value.trim() && !/[\u0000\uD800-\uDFFF]/u.test(value)),
});
export const newPhoneCallControlSchema = phoneCallAssignmentSchema.pick({ assignmentId: true, purpose: true });
export const phoneCallAssignmentRefSchema = z.object({
  assignmentId: uuid, revision: z.literal(1), digest: z.string().regex(/^[0-9a-f]{64}$/),
}).strict();
export const retainedPhoneCallAssignmentRefSchema = phoneCallAssignmentRefSchema.extend({ assignmentId: retainedUuid });
export type PhoneCallAssignment = z.infer<typeof phoneCallAssignmentSchema>;
export type PhoneCallAssignmentRef = z.infer<typeof phoneCallAssignmentRefSchema>;
export type BrokerCallAssignment = { origin: 'authenticated_broker_control'; value: PhoneCallAssignment };
export const brokerCallAssignmentSchema = z.object({
  origin: z.literal('authenticated_broker_control'), value: retainedPhoneCallAssignmentSchema,
}).strict();
export function sameCallAssignmentRef(a: PhoneCallAssignmentRef | undefined, b: PhoneCallAssignmentRef | undefined): boolean {
  return a === undefined ? b === undefined : b !== undefined
    && a.assignmentId === b.assignmentId && a.revision === b.revision && a.digest === b.digest;
}
