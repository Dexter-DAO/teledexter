import { deliverHarkBilling } from './harkBillingDelivery.js';
import { inboundTerminalFromProvider, inboundBillingReceiptSchema, inboundStatusReceiptSchema, type InboundBillingDelivery, type InboundStatusDelivery } from './inboundBilling.js';
import { terminalCallStatuses, type BrokerStore, type CallStatus } from './store.js';

/** Read final provider evidence, then retry one immutable financial event until acknowledged. */
export function createInboundBillingLifecycle(options: {
  store: BrokerStore; apiBaseUrl?: string; signingSecret?: string;
  fetchCall(callSid: string): Promise<unknown>;
  send?: typeof deliverHarkBilling;
  now?: () => number;
}) {
  const active = new Map<string, Promise<void>>();
  const now = options.now ?? Date.now;
  function reconcile(callId: string): Promise<void> {
    const existing = active.get(callId);
    if (existing) return existing;
    const operation = run(callId).finally(() => { active.delete(callId); });
    active.set(callId, operation);
    return operation;
  }
  async function run(callId: string) {
    let call = options.store.get(callId);
    if (!call?.inboundBilling || !call.callSid) return;
    let journal = call.inboundBilling;
    if (!journal.event && !journal.statusEvent) {
      if (!journal.terminal && !call.endedAt && !call.inbound?.localStop) return;
      if (journal.lastTimingAttemptAt && now() - Date.parse(journal.lastTimingAttemptAt) < 30_000) return;
      // A signed terminal callback can be missed. The authenticated fetch can
      // establish the same terminal state; its full identity is still required.
      options.store.transitionInboundBilling(callId, { kind: 'timing_attempt', observedAt: new Date(now()).toISOString() });
      let raw: unknown;
      try { raw = await options.fetchCall(call.callSid); }
      catch {
        options.store.transitionInboundBilling(callId, { kind: 'timing_failed', reason: 'provider_read_failed' });
        return;
      }
      call = options.store.get(callId)!;
      const row = raw as { sid?: unknown; accountSid?: unknown; from?: unknown; to?: unknown; direction?: unknown; status?: unknown } | null;
      const proof = call.inbound!.begin.providerProof.params;
      const matches = row && row.sid === call.callSid && row.accountSid === proof.AccountSid
        && row.from === proof.From && row.to === proof.To && row.direction === 'inbound';
      if (!call.inboundBilling!.terminal && matches && terminalCallStatuses.has(row.status as CallStatus)) {
        call = options.store.observeInboundProvider(callId, row.status as CallStatus, new Date(now()).toISOString());
        call = options.store.transitionInboundBilling(callId, { kind: 'timing_attempt', observedAt: new Date(now()).toISOString() });
      }
      if (!call.inboundBilling!.terminal) {
        options.store.transitionInboundBilling(callId, { kind: 'timing_failed',
          reason: matches ? 'provider_terminal_pending' : 'provider_identity_mismatch' });
        return;
      }
      const result = inboundTerminalFromProvider(call, raw, new Date(now()).toISOString());
      if (result.error) {
        options.store.transitionInboundBilling(callId, { kind: 'timing_failed', reason: result.error });
        return;
      }
      call = options.store.transitionInboundBilling(callId, result.statusObservation
        ? { kind: 'provider_status', observation: result.statusObservation }
        : { kind: 'provider', observation: result.observation! });
      journal = call.inboundBilling!;
    }
    if (journal.statusEvent) {
      const event = journal.statusEvent;
      if (event.state !== 'pending') return;
      const retryMs = Math.min(300_000, 10_000 * 2 ** Math.min(event.attempts, 5));
      if (event.lastAttemptAt && now() - Date.parse(event.lastAttemptAt) < retryMs) return;
      options.store.transitionInboundBilling(callId, { kind: 'status_attempt', bodyDigest: event.bodyDigest, observedAt: new Date(now()).toISOString() });
      let outcome: InboundStatusDelivery;
      try {
        const result = await (options.send ?? deliverHarkBilling)({ kind: 'inbound-status-ended', body: event.body,
          apiBaseUrl: options.apiBaseUrl ?? '', signingSecret: options.signingSecret ?? '' });
        const receipt = result.state === 'delivered' && result.purpose === 'voice.call-ended.v2'
          ? inboundStatusReceiptSchema.safeParse(result.receipt) : undefined;
        outcome = result.state === 'delivered'
          ? receipt?.success && receipt.data.id === journal.ref.bindingId && receipt.data.status === event.body.provider_terminal.status
            ? { state: 'delivered', receipt: receipt.data } : { state: 'pending', reason: 'invalid_response' }
          : result;
      } catch { outcome = { state: 'pending', reason: 'request_failed' }; }
      options.store.transitionInboundBilling(callId, { kind: 'status_delivery', bodyDigest: event.bodyDigest, result: outcome });
      return;
    }
    const event = journal.event;
    if (!event || event.state !== 'pending') return;
    const retryMs = Math.min(300_000, 10_000 * 2 ** Math.min(event.attempts, 5));
    if (event.lastAttemptAt && now() - Date.parse(event.lastAttemptAt) < retryMs) return;
    options.store.transitionInboundBilling(callId, { kind: 'attempt', bodyDigest: event.bodyDigest, observedAt: new Date(now()).toISOString() });
    let outcome: InboundBillingDelivery;
    try {
      const result = await (options.send ?? deliverHarkBilling)({ kind: 'inbound-call-ended', body: event.body,
        apiBaseUrl: options.apiBaseUrl ?? '', signingSecret: options.signingSecret ?? '' });
      const receipt = result.state === 'delivered' && result.purpose === 'voice.call-ended.v2'
        ? inboundBillingReceiptSchema.safeParse(result.receipt) : undefined;
      outcome = result.state === 'delivered'
        ? receipt?.success ? { state: 'delivered', receipt: receipt.data }
          : { state: 'pending', reason: 'invalid_response' }
        : result;
    } catch { outcome = { state: 'pending', reason: 'request_failed' }; }
    options.store.transitionInboundBilling(callId, { kind: 'delivery', bodyDigest: event.bodyDigest, result: outcome });
  }
  return { reconcile };
}
