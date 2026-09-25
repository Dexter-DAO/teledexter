import { createPrimaryApiTransport, PrimaryApiError, type PrimaryApiOptions } from './primaryApi.js';
import {
  PRIMARY_HARK_CALL_RECEIPT_ASSOCIATION_PENDING, classifyPrimaryHarkReceiptFailure,
  primaryHarkCallReceiptResultSchema, validatePrimaryHarkReceiptOutbox, validatePrimaryHarkReceiptResult,
  type PrimaryHarkReceiptBinding, type PrimaryHarkReceiptEntry, type PrimaryHarkReceiptDelivery,
  type PrimaryHarkReceiptOutbox, type PrimaryHarkReceiptCommand,
} from './harkPrimaryReceiptOutbox.js';

export type PrimaryHarkReceiptSender = (binding: PrimaryHarkReceiptBinding,
  entry: PrimaryHarkReceiptEntry) => Promise<PrimaryHarkReceiptDelivery>;

/** HARK-RECEIPTS-HTTP-WIRE.v2: one attempt using the existing Phone service credential. */
export function createPrimaryHarkReceiptSender(options: PrimaryApiOptions): PrimaryHarkReceiptSender {
  // Keep the configured credential private; no token or HTTP body enters diagnostics.
  const connection = { ...options, maxBodyBytes: options.maxBodyBytes ?? 16 * 1024 };
  return async (binding, entry) => {
    let saved: PrimaryHarkReceiptOutbox;
    try { saved = validatePrimaryHarkReceiptOutbox({ version: 1, binding, entries: { [entry.id]: entry } }, binding); }
    catch { return { state: 'rejected', reason: 'invalid_receipt_body' }; }
    const identity = saved.binding;
    const item = saved.entries[entry.id]!;
    if (item.body.evidence.kind !== 'primary_outcome') return { state: 'rejected', reason: 'unsupported_evidence' };
    if (item.state !== 'pending' || item.attempts < 1) return { state: 'rejected', reason: 'invalid_receipt_body' };
    let result: unknown;
    try {
      const request = createPrimaryApiTransport(connection);
      result = await request('POST', `/calls/${identity.bindingId}/hark-receipts`, item.body,
        { 'X-Dexter-Binding-Id': identity.bindingId }, 200);
    } catch (error) {
      if (!(error instanceof PrimaryApiError)) return { state: 'pending', reason: 'request_failed' };
      if (error.code === 'invalid_configuration') return { state: 'pending', reason: 'configuration_unavailable' };
      if (error.code === 'invalid_request') return { state: 'rejected', reason: 'invalid_receipt_body' };
      if (error.status !== undefined && error.status >= 400) {
        // A typed409 is retryable only when the frozen HTTP envelope confirms it.
        const code = error.status === 409 && error.remoteCode === PRIMARY_HARK_CALL_RECEIPT_ASSOCIATION_PENDING
          && error.retryable === true ? error.remoteCode : undefined;
        return classifyPrimaryHarkReceiptFailure({ kind: 'http', status: error.status, code });
      }
      if (error.code === 'http_error') return { state: 'rejected', reason: 'http_refusal',
        ...(error.status === undefined ? {} : { httpStatus: error.status }) };
      return { state: 'pending', reason: ['invalid_response', 'response_too_large'].includes(error.code)
        ? 'invalid_response' : 'request_failed' };
    }
    const parsed = primaryHarkCallReceiptResultSchema.safeParse(result);
    if (!parsed.success) return { state: 'pending', reason: 'invalid_response', httpStatus: 200 };
    try { return { state: 'delivered', result: validatePrimaryHarkReceiptResult(identity, item.body, parsed.data) }; }
    catch { return { state: 'rejected', reason: 'identity_mismatch', httpStatus: 200 }; }
  };
}

export interface PrimaryHarkReceiptDispatchStore {
  /** Snapshots include the binding derived from the immutable saved call fields. */
  listPrimaryHarkReceiptOutboxes(): ReadonlyArray<{ binding: PrimaryHarkReceiptBinding; outbox: PrimaryHarkReceiptOutbox }>;
  /** Validate the current binding and persist the reducer output before returning. */
  transitionPrimaryHarkReceiptOutbox(callId: string, binding: PrimaryHarkReceiptBinding,
    command: PrimaryHarkReceiptCommand): PrimaryHarkReceiptOutbox;
}

/** Serialized, bounded flush; the caller owns scheduling and durable call storage. */
export function createPrimaryHarkReceiptDispatcher(options: {
  store: PrimaryHarkReceiptDispatchStore; send: PrimaryHarkReceiptSender; now?: () => string;
}) {
  let active: Promise<void> | undefined;
  function flush(): Promise<void> {
    if (active) return active;
    active = (async () => {
      const attempted = new Set<string>();
      for (let count = 0; count < 24; count++) {
        let next: { binding: PrimaryHarkReceiptBinding; entry: PrimaryHarkReceiptEntry } | undefined;
        for (const candidate of options.store.listPrimaryHarkReceiptOutboxes()) {
          const box = validatePrimaryHarkReceiptOutbox(candidate.outbox, candidate.binding);
          const entry = Object.values(box.entries).find(item => item.state === 'pending'
            && !attempted.has(`${box.binding.callId}:${item.id}`));
          if (entry) { next = { binding: box.binding, entry }; break; }
        }
        if (!next) return;
        const { binding, entry } = next;
        attempted.add(`${binding.callId}:${entry.id}`);
        const observedAt = (options.now ?? (() => new Date().toISOString()))();
        const attemptedBox = options.store.transitionPrimaryHarkReceiptOutbox(binding.callId, binding, {
          kind: 'attempt', id: entry.id, bodyDigest: entry.bodyDigest, expectedAttempts: entry.attempts, observedAt,
        });
        const saved = validatePrimaryHarkReceiptOutbox(attemptedBox, binding).entries[entry.id];
        if (!saved || saved.state !== 'pending' || saved.bodyDigest !== entry.bodyDigest
          || saved.attempts !== entry.attempts + 1 || saved.lastAttemptAt !== observedAt)
          throw new Error('primary_hark_receipt_attempt_not_saved');
        let outcome: PrimaryHarkReceiptDelivery;
        try { outcome = await options.send(binding, saved); }
        catch { outcome = { state: 'pending', reason: 'request_failed' }; }
        const settled = options.store.transitionPrimaryHarkReceiptOutbox(binding.callId, binding, {
          kind: 'settle', id: saved.id, bodyDigest: saved.bodyDigest, attempt: saved.attempts, outcome,
        });
        validatePrimaryHarkReceiptOutbox(settled, binding);
      }
    })().catch(() => { throw new Error('primary_hark_receipt_dispatch_state_unavailable'); })
      .finally(() => { active = undefined; });
    return active;
  }
  return { flush };
}
