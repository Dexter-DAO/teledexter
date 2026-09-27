import { OutboundAlertRequestSchema } from '../outbound/contract.js';
import { resolvedNotificationAssignmentSchema, type ResolvedNotificationAssignment } from './notificationAssignment.js';

export class NotificationAssignmentError extends Error {
  constructor(readonly status: number, readonly code: string) { super(code); }
}

export type NotificationAssignmentResolver = (notification: unknown, internalToken: string) => Promise<ResolvedNotificationAssignment>;

/** The producer credential is reflected to the local API, never persisted. */
export function createNotificationAssignmentResolver(options: {
  apiBaseUrl: string; fetchImpl?: typeof fetch; timeoutMs?: number;
}): NotificationAssignmentResolver {
  const base = new URL(options.apiBaseUrl);
  if (base.protocol !== 'http:' || !['127.0.0.1', '[::1]', 'localhost'].includes(base.hostname)
      || base.username || base.password || base.search || base.hash || base.pathname !== '/')
    throw new Error('Notification resolver requires a local API origin');
  const timeoutMs = options.timeoutMs ?? 10_000;
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 10_000) throw new Error('Invalid notification resolver timeout');
  const request = options.fetchImpl ?? fetch;
  return async (input, internalToken) => {
    const parsed = OutboundAlertRequestSchema.safeParse(input);
    if (!parsed.success || !parsed.data.notificationRef) throw new NotificationAssignmentError(400, 'invalid_notification');
    if (!internalToken || Buffer.byteLength(internalToken, 'utf8') > 4096) throw new NotificationAssignmentError(401, 'notification_authorization_required');
    const notification = parsed.data;
    try {
      const response = await request(new URL('/internal/phone/resolve-notification-assignment', base), {
        method: 'POST', redirect: 'error', signal: AbortSignal.timeout(timeoutMs),
        headers: { 'content-type': 'application/json', 'x-internal-auth': internalToken },
        body: JSON.stringify({ notification }),
      });
      if (!response.ok) {
        await response.body?.cancel();
        throw new NotificationAssignmentError([400, 401, 403, 409].includes(response.status) ? response.status : 503,
          'notification_assignment_refused');
      }
      if (response.status !== 200 || Number(response.headers.get('content-length') ?? 0) > 65_536) {
        await response.body?.cancel();
        throw new NotificationAssignmentError(503, 'invalid_notification_assignment');
      }
      const chunks: Uint8Array[] = [];
      let size = 0;
      const reader = response.body?.getReader();
      try {
        if (reader) for (;;) {
          const next = await reader.read();
          if (next.done) break;
          size += next.value.byteLength;
          if (size > 65_536) { await reader.cancel(); throw new NotificationAssignmentError(503, 'invalid_notification_assignment'); }
          chunks.push(next.value);
        }
      } finally { reader?.releaseLock(); }
      const value = resolvedNotificationAssignmentSchema.parse(JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks))));
      const source = value.callAssignment.notification;
      if (value.to !== notification.to || source.intentId !== notification.notificationRef!.intentId
          || source.sourceDigest !== notification.notificationRef!.sourceDigest || source.eventId !== notification.eventId
          || notification.idempotencyKey !== source.eventId)
        throw new NotificationAssignmentError(503, 'notification_assignment_identity_mismatch');
      return value;
    } catch (error) {
      if (error instanceof NotificationAssignmentError) throw error;
      throw new NotificationAssignmentError(503, 'notification_assignment_unavailable');
    }
  };
}
