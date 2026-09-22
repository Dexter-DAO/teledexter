import { z } from 'zod';

const uuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
export const phoneCallAssignmentSchema = z.object({
  assignmentId: uuid, brokerCallId: uuid, revision: z.literal(1),
  // Match the authenticated control route: String.length counts UTF-16 units.
  purpose: z.string().min(1).max(1500).refine(value => value === value.trim()),
}).strict();
export const phoneCallAssignmentRefSchema = z.object({
  assignmentId: uuid, revision: z.literal(1), digest: z.string().regex(/^[0-9a-f]{64}$/),
}).strict();
export type PhoneCallAssignment = z.infer<typeof phoneCallAssignmentSchema>;
export type PhoneCallAssignmentRef = z.infer<typeof phoneCallAssignmentRefSchema>;
export type BrokerCallAssignment = { origin: 'authenticated_broker_control'; value: PhoneCallAssignment };
export const brokerCallAssignmentSchema = z.object({
  origin: z.literal('authenticated_broker_control'), value: phoneCallAssignmentSchema,
}).strict();
export function sameCallAssignmentRef(a: PhoneCallAssignmentRef | undefined, b: PhoneCallAssignmentRef | undefined): boolean {
  return a === undefined ? b === undefined : b !== undefined
    && a.assignmentId === b.assignmentId && a.revision === b.revision && a.digest === b.digest;
}
