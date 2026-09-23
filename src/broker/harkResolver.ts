import { OutboundAlertRequestSchema } from '../outbound/contract.js';
import { resolvedHarkAssignmentSchema, type ResolvedHarkAssignment } from './harkAssignment.js';

export class HarkAssignmentError extends Error {
  constructor(readonly status: number, readonly code: string) { super(code); }
}

export type HarkAssignmentResolver = (notification: unknown, internalToken: string) => Promise<ResolvedHarkAssignment>;

/** The producer credential is reflected to the local API, never persisted. */
export function createHarkAssignmentResolver(options: {
  apiBaseUrl: string; fetchImpl?: typeof fetch; timeoutMs?: number;
}): HarkAssignmentResolver {
  const base = new URL(options.apiBaseUrl);
  if (base.protocol !== 'http:' || !['127.0.0.1', '[::1]', 'localhost'].includes(base.hostname)
      || base.username || base.password || base.search || base.hash || base.pathname !== '/')
    throw new Error('Hark resolver requires a local API origin');
  const request = options.fetchImpl ?? fetch;
  return async (input, internalToken) => {
    const parsed = OutboundAlertRequestSchema.safeParse(input);
    if (!parsed.success || !parsed.data.hark) throw new HarkAssignmentError(400, 'invalid_hark_notification');
    if (!internalToken || internalToken.length > 4096) throw new HarkAssignmentError(401, 'hark_authorization_required');
    const notification = parsed.data;
    try {
      const response = await request(new URL('/internal/phone/resolve-hark-assignment', base), {
        method: 'POST', redirect: 'error', signal: AbortSignal.timeout(options.timeoutMs ?? 10_000),
        headers: { 'content-type': 'application/json', 'x-internal-auth': internalToken },
        body: JSON.stringify({ notification }),
      });
      if (!response.ok) {
        await response.body?.cancel();
        throw new HarkAssignmentError([400, 401, 403, 409].includes(response.status) ? response.status : 503,
          'hark_assignment_refused');
      }
      if (response.status !== 200 || Number(response.headers.get('content-length') ?? 0) > 65_536) {
        await response.body?.cancel();
        throw new HarkAssignmentError(503, 'invalid_hark_assignment');
      }
      const chunks: Uint8Array[] = [];
      let size = 0;
      const reader = response.body?.getReader();
      try {
        if (reader) for (;;) {
          const next = await reader.read();
          if (next.done) break;
          size += next.value.byteLength;
          if (size > 65_536) { await reader.cancel(); throw new HarkAssignmentError(503, 'invalid_hark_assignment'); }
          chunks.push(next.value);
        }
      } finally { reader?.releaseLock(); }
      const value = resolvedHarkAssignmentSchema.parse(JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks))));
      const source = value.callAssignment.hark;
      if (value.to !== notification.to || source.deliveryId !== notification.hark!.deliveryId
          || source.caseId !== notification.hark!.caseId || source.watch.id !== notification.hark!.watchId
          || source.event.id !== notification.hark!.eventId || source.originTaskId !== notification.hark!.originTaskId)
        throw new HarkAssignmentError(503, 'hark_assignment_identity_mismatch');
      return value;
    } catch (error) {
      if (error instanceof HarkAssignmentError) throw error;
      throw new HarkAssignmentError(503, 'hark_assignment_unavailable');
    }
  };
}
