import { z } from 'zod';

const uuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);

/** Frozen shared wire96392. This carries lineage; the API supplies currently
 * eligible content and checks the new call's ordinary execution authority. */
export const associatedResultReferenceSchema = z.object({
  v: z.literal('phone.associated-result-reference.v1'),
  deliveryAssociationId: uuid,
  originalRequestId: uuid,
  assistantMessageId: uuid,
}).strict();
export type AssociatedResultReference = z.infer<typeof associatedResultReferenceSchema>;

/** Explicit ordering is part of the raw submission digest contract. */
export function orderedAssociatedResultReference(value: AssociatedResultReference): AssociatedResultReference {
  const checked = associatedResultReferenceSchema.parse(value);
  return { v: checked.v, deliveryAssociationId: checked.deliveryAssociationId,
    originalRequestId: checked.originalRequestId, assistantMessageId: checked.assistantMessageId };
}
