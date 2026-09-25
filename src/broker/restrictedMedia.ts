import { z } from 'zod';
const timestamp = z.string().datetime().refine(value => new Date(value).toISOString() === value);
export const restrictedMediaSchema = z.object({ admittedAt: timestamp, deadlineAt: timestamp,
  startedAt: timestamp.optional(), streamSid: z.string().regex(/^MZ[0-9a-fA-F]{32}$/).optional(), sessionId: z.string().min(1).max(200).optional(),
  protectedRequestedAt: timestamp.optional(), protectedDispatchedAt: timestamp.optional(),
  privateRequestedAt: timestamp.optional(), privateDispatchedAt: timestamp.optional(), stoppedAt: timestamp.optional(),
}).strict();
export type RestrictedMedia = z.output<typeof restrictedMediaSchema>;
export function validateRestrictedMedia(next: RestrictedMedia, previous?: RestrictedMedia): void {
  const value = restrictedMediaSchema.parse(next);
  if (Date.parse(value.deadlineAt) <= Date.parse(value.admittedAt)) throw new Error('restricted_media_clock_invalid');
  for (const key of ['startedAt', 'protectedRequestedAt', 'protectedDispatchedAt', 'privateRequestedAt', 'privateDispatchedAt', 'stoppedAt'] as const) {
    if (value[key] && Date.parse(value[key]) < Date.parse(value.admittedAt)) throw new Error('restricted_media_clock_invalid');
  }
  if ((value.protectedDispatchedAt && !value.protectedRequestedAt) || (value.privateDispatchedAt && !value.privateRequestedAt)
    || (value.protectedRequestedAt && value.privateRequestedAt) || (value.sessionId && !value.streamSid) || (!!value.streamSid !== !!value.startedAt))
    throw new Error('restricted_media_transition_invalid');
  for (const key of ['startedAt', 'protectedRequestedAt', 'protectedDispatchedAt', 'privateRequestedAt', 'privateDispatchedAt'] as const) {
    if (value[key] && Date.parse(value[key]) >= Date.parse(value.deadlineAt)) throw new Error('restricted_media_clock_invalid');
  }
  for (const kind of ['protected', 'private'] as const) {
    const requested = value[`${kind}RequestedAt`], dispatched = value[`${kind}DispatchedAt`];
    if (requested && !value.sessionId) throw new Error('restricted_media_transition_invalid');
    if (dispatched && requested && Date.parse(dispatched) < Date.parse(requested)) throw new Error('restricted_media_clock_invalid');
  }
  if (previous) for (const key of Object.keys(previous) as (keyof RestrictedMedia)[]) {
    if (previous[key] !== value[key]) throw new Error('restricted_media_identity_changed');
  }
}
export function changeRestrictedMedia(current: RestrictedMedia, patch: Partial<RestrictedMedia>): RestrictedMedia {
  const next = restrictedMediaSchema.parse({ ...current, ...patch }); validateRestrictedMedia(next, current); return next;
}
