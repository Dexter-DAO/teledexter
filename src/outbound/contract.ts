import { z } from 'zod';
import { canonicalJson } from './canonical.js';
import { phoneNotificationRefSchema } from '../broker/notificationAssignment.js';

const MAX_FOLLOWUP_CONTEXT_BYTES = 24 * 1024;

export const HarkPhoneBindingSchema = z
  .object({
    schemaVersion: z.literal(1),
    caseId: z.string().uuid(),
    watchId: z.string().uuid(),
    eventId: z.string().uuid(),
    deliveryId: z.string().uuid(),
    originTaskId: z.string().min(1).max(300),
  })
  .strict();

export type HarkPhoneBinding = z.infer<typeof HarkPhoneBindingSchema>;

export const HarkDecisionTypeSchema = z.enum([
  'acknowledge',
  'dismiss',
  'snooze',
  'reply',
  'choice',
  'instruction',
]);

export const HarkDecisionSchema = z
  .object({
    type: HarkDecisionTypeSchema,
    text: z.string().trim().min(1).max(4_000).optional(),
    snoozeUntil: z.string().datetime({ offset: true }).optional(),
  })
  .strict()
  .superRefine((decision, ctx) => {
    if (
      (decision.type === 'reply' ||
        decision.type === 'choice' ||
        decision.type === 'instruction') &&
      !decision.text
    ) {
      ctx.addIssue({
        code: 'custom',
        path: ['text'],
        message: `${decision.type} requires the owner's natural-language text`,
      });
    }
    if (decision.type === 'snooze' && !decision.snoozeUntil) {
      ctx.addIssue({
        code: 'custom',
        path: ['snoozeUntil'],
        message: 'snooze requires an exact snoozeUntil timestamp',
      });
    }
  });

export type HarkDecision = z.infer<typeof HarkDecisionSchema>;

// Exact Hark API ownerDecisionSchema wire contract. The phone tool keeps its
// natural snooze timestamp / choice wording above; the bridge normalizes that
// input into this union once, before reserving the durable callback body.
export const HarkPhoneDecisionSchema = z.discriminatedUnion('type', [
  z.object({
    type: z.literal('acknowledge'),
    text: z.string().trim().min(1).max(4_000).optional(),
  }).strict(),
  z.object({
    type: z.literal('dismiss'),
    text: z.string().trim().min(1).max(4_000).optional(),
  }).strict(),
  z.object({
    type: z.literal('snooze'),
    seconds: z.number().int().min(60).max(30 * 24 * 60 * 60),
    text: z.string().trim().min(1).max(4_000).optional(),
  }).strict(),
  z.object({
    type: z.literal('reply'),
    text: z.string().trim().min(1).max(4_000),
  }).strict(),
  z.object({
    type: z.literal('choice'),
    value: z.string().trim().min(1).max(500),
    text: z.string().trim().min(1).max(4_000).optional(),
  }).strict(),
  z.object({
    type: z.literal('instruction'),
    text: z.string().trim().min(1).max(4_000),
  }).strict(),
]);

export type HarkPhoneDecision = z.infer<typeof HarkPhoneDecisionSchema>;

export const HarkPhoneDeliveryStatusSchema = z
  .object({
    v: z.literal('hark.phone-status.v1'),
    sourceEventId: z.string().min(16).max(200),
    caseId: z.string().uuid(),
    eventId: z.string().uuid(),
    deliveryId: z.string().uuid(),
    intentId: z.string().uuid(),
    callSid: z.string().regex(/^CA[0-9a-fA-F]{32}$/),
    originTaskId: z.string().min(1).max(300),
    status: z.enum([
      'queued',
      'ringing',
      'answered',
      'streaming',
      'completed',
      'failed',
      'busy',
      'no-answer',
      'canceled',
      'expired',
    ]),
    observedAt: z.string().datetime({ offset: true }),
  })
  .strict();

export type HarkPhoneDeliveryStatus = z.infer<typeof HarkPhoneDeliveryStatusSchema>;

export const HarkPhoneResponseSchema = z
  .object({
    v: z.literal('hark.response.v1'),
    sourceResponseId: z.string().min(16).max(200),
    caseId: z.string().uuid(),
    eventId: z.string().uuid(),
    deliveryId: z.string().uuid(),
    intentId: z.string().uuid(),
    callSid: z.string().regex(/^CA[0-9a-fA-F]{32}$/),
    originTaskId: z.string().min(1).max(300),
    observedAt: z.string().datetime({ offset: true }),
    decision: HarkPhoneDecisionSchema,
    supersedesResponseId: z.string().uuid().nullable(),
  })
  .strict();

export type HarkPhoneResponse = z.infer<typeof HarkPhoneResponseSchema>;

export const HarkPhoneContinuationSchema = z
  .object({
    v: z.literal('hark.continuation.v1'),
    continuationId: z.string().uuid(),
    idempotencyKey: z.string().uuid(),
    origin: z.object({
      protocol: z.literal('hermes'),
      runtimeId: z.string().trim().min(1).max(200),
      taskId: z.string().trim().min(1).max(300),
      conversationId: z.string().trim().min(1).max(300),
    }).strict(),
    case: z.object({
      id: z.string().uuid(),
      eventId: z.string().uuid(),
      summary: z.string().trim().min(1).max(2_000),
    }).strict(),
    response: z.object({
      id: z.string().uuid(),
      decision: HarkPhoneDecisionSchema,
    }).strict(),
  })
  .strict()
  .superRefine((continuation, context) => {
    if (continuation.continuationId !== continuation.idempotencyKey) {
      context.addIssue({
        code: 'custom',
        path: ['idempotencyKey'],
        message: 'continuation idempotencyKey must equal continuationId',
      });
    }
  });

export const HarkSanitizedContinuationSchema = z
  .object({
    id: z.string().uuid(),
    originTaskId: z.string().trim().min(1).max(300),
  })
  .strict();

export const HarkPhoneResponseCallbackResultSchema = z
  .object({
    v: z.literal('hark.phone-callback-result.v1'),
    ok: z.literal(true),
    replay: z.boolean(),
    responseId: z.string().uuid(),
    caseId: z.string().uuid(),
    eventId: z.string().uuid(),
    deliveryId: z.string().uuid(),
    intentId: z.string().uuid(),
    callSid: z.string().regex(/^CA[0-9a-fA-F]{32}$/),
    originTaskId: z.string().min(1).max(300),
    sourceResponseId: z.string().min(16).max(200),
    caseState: z.string().min(1).max(80),
    continuation: HarkPhoneContinuationSchema.optional(),
  })
  .strict();

export const HarkPhoneStatusCallbackResultSchema = z
  .object({
    v: z.literal('hark.phone-callback-result.v1'),
    ok: z.literal(true),
    replay: z.boolean(),
    caseId: z.string().uuid(),
    eventId: z.string().uuid(),
    deliveryId: z.string().uuid(),
    intentId: z.string().uuid(),
    callSid: z.string().regex(/^CA[0-9a-fA-F]{32}$/),
    originTaskId: z.string().min(1).max(300),
    sourceEventId: z.string().min(16).max(200),
    caseState: z.string().min(1).max(80),
  })
  .strict();

export const HarkSanitizedPhoneResponseCallbackResultSchema =
  HarkPhoneResponseCallbackResultSchema
    .omit({ continuation: true })
    .extend({ continuation: HarkSanitizedContinuationSchema.optional() })
    .strict();

export const HarkPhoneCallbackResultSchema = z.union([
  HarkSanitizedPhoneResponseCallbackResultSchema,
  HarkPhoneStatusCallbackResultSchema,
]);

export type SanitizedHarkContinuationResult = z.infer<
  typeof HarkPhoneCallbackResultSchema
>;

export const OutboundAlertRequestSchema = z
  .object({
    idempotencyKey: z
      .string()
      .min(8)
      .max(160)
      .regex(/^[A-Za-z0-9._:-]+$/),
    eventId: z.string().min(1).max(160),
    to: z.string().regex(/^\+[1-9]\d{7,14}$/),
    recipientName: z.string().min(1).max(80).optional(),
    subject: z.string().min(1).max(120),
    severity: z.enum(['info', 'warning', 'critical']),
    briefing: z.string().min(1).max(700),
    observedAt: z.string().datetime({ offset: true }),
    ttlSeconds: z.number().int().min(30).max(600).default(180),
    followupContext: z.record(z.string(), z.unknown()).default({}),
    hark: HarkPhoneBindingSchema.optional(),
    notificationRef: phoneNotificationRefSchema.optional(),
  })
  .strict()
  .superRefine((value, ctx) => {
    if (Buffer.byteLength(canonicalJson(value.followupContext), 'utf8') > MAX_FOLLOWUP_CONTEXT_BYTES) {
      ctx.addIssue({
        code: 'custom',
        path: ['followupContext'],
        message: `followupContext exceeds ${MAX_FOLLOWUP_CONTEXT_BYTES} bytes`,
      });
    }
    if (value.notificationRef) {
      if (value.hark || value.idempotencyKey !== value.eventId) ctx.addIssue({ code: 'custom',
        path: ['notificationRef'], message: 'Notification reference requires one source and its exact event identity' });
    }
    if (value.hark) {
      const expectedEventId = `hark:${value.hark.deliveryId}`;
      if (value.eventId !== expectedEventId) {
        ctx.addIssue({
          code: 'custom',
          path: ['eventId'],
          message: 'Hark alert eventId must exactly identify its delivery as hark:<deliveryId>',
        });
      }
      if (value.idempotencyKey !== value.eventId) {
        ctx.addIssue({
          code: 'custom',
          path: ['idempotencyKey'],
          message: 'Hark alert idempotencyKey must exactly match its delivery eventId',
        });
      }
    }
  });

export type OutboundAlertRequest = z.infer<typeof OutboundAlertRequestSchema>;

/**
 * Exact billing identity authored by dexter-api for a paid Phone delivery.
 * This deliberately is not a permissive publisher-context shape: possession
 * of the internal token is useful only when this whole binding survives
 * schema validation and dexter-api reflects the exact request as authorized.
 */
export const ServerAuthoredPhoneNotificationSchema = z
  .object({
    source: z.enum(['hark', 'notification']),
    publishId: z.string().uuid().nullable(),
    topicId: z.string().uuid().nullable(),
    topicAddress: z.string().max(100).nullable(),
    subscriptionId: z.string().uuid().nullable(),
    destinationId: z.string().uuid(),
    billingMandateId: z.string().uuid(),
    billingUnitPriceMicro: z.number().int().min(1),
    billingFreeSeconds: z.number().int().min(0),
    title: z.string().min(1).max(120),
    priority: z.string().min(1).max(40),
    tags: z.array(z.string()),
  })
  .strict();

export type ServerAuthoredPhoneNotification = z.infer<
  typeof ServerAuthoredPhoneNotificationSchema
>;

const ServerAuthoredFollowupContextSchema = z
  .object({
    notification: ServerAuthoredPhoneNotificationSchema,
    evidence: z.unknown().optional(),
  })
  .strict();

export function serverAuthoredPhoneNotification(
  alert: OutboundAlertRequest,
): ServerAuthoredPhoneNotification | null {
  const parsed = ServerAuthoredFollowupContextSchema.safeParse(
    alert.followupContext,
  );
  return parsed.success ? parsed.data.notification : null;
}

export interface FrozenAlert {
  intentId: string;
  eventId: string;
  recipientName: string | null;
  to: string;
  subject: string;
  severity: OutboundAlertRequest['severity'];
  briefing: string;
  observedAt: string;
  followupContext: Record<string, unknown>;
}
