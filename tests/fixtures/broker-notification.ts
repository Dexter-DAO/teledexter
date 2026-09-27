import { NOTIFICATION_PHONE_PURPOSE, phoneNotificationSourceDigest, phoneNotificationNumberDigest,
  type PhoneNotificationSnapshot, type ResolvedNotificationAssignment } from '../../src/broker/notificationAssignment.js';
export function makeResolvedNotificationAssignment(): ResolvedNotificationAssignment {
  const to = '+12025550123';
  const source: Omit<PhoneNotificationSnapshot, 'sourceDigest'> = {
    kind: 'phone_notification_v1', intentId: '33333333-3333-4333-8333-333333333333',
    eventId: 'portfolio:33333333-3333-4333-8333-333333333333',
    ownerSupabaseUserId: '11111111-1111-4111-8111-111111111111', destinationId: '22222222-2222-4222-8222-222222222222',
    phoneNumberDigest: phoneNotificationNumberDigest(to),
    source: { kind: 'portfolio_watch_event_v1', intentId: '33333333-3333-4333-8333-333333333333',
      eventKey: 'held-mint:generation-one:crossing-one', bodySha256: 'b'.repeat(64) },
    title: 'Saved holding alert', summary: 'The watched holding crossed the saved threshold.',
    evidence: { observation: 'Saved observation, not an order instruction.', price: '12.50', decimals: 6 },
    observedAt: '2026-09-27T00:00:00.000Z', deliveryNotBefore: '2026-09-27T00:00:00.000Z', expiresAt: '2099-09-27T00:00:00.000Z',
    billing: { mandateId: '77777777-7777-4777-8777-777777777777', unitPriceMicro: 100000, freeSeconds: 60 },
  };
  const notification = { ...source, sourceDigest: phoneNotificationSourceDigest(source) };
  return { schemaVersion: 1, to, callAssignment: { assignmentId: source.intentId, brokerCallId: source.intentId,
    revision: 1, purpose: NOTIFICATION_PHONE_PURPOSE, notification } };
}
export function notificationWire(value = makeResolvedNotificationAssignment()) {
  const source = value.callAssignment.notification;
  return { idempotencyKey: source.eventId, eventId: source.eventId, to: value.to,
    subject: 'Untrusted wire title', briefing: 'Untrusted wire summary', severity: 'info' as const,
    observedAt: source.observedAt, ttlSeconds: 180, followupContext: {},
    notificationRef: { v: 1 as const, intentId: source.intentId, sourceDigest: source.sourceDigest } };
}
