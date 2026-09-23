import type { BrokerStore } from './store.js';
import { deliverHarkStatus } from './harkStatusDelivery.js';
import { deliverHarkBilling } from './harkBillingDelivery.js';

/** One writer drains the existing call journal. Transport retries never reserve or dial. */
export function createHarkLifecycleDelivery(options: {
  store: BrokerStore; apiBaseUrl: string; signingSecret: string;
  statusSender?: typeof deliverHarkStatus; billingSender?: typeof deliverHarkBilling;
}) {
  let active: Promise<void> | undefined;
  function flush(): Promise<void> {
    if (active) return active;
    active = (async () => {
      const attempted = new Set<string>();
      // Re-evaluate after acknowledgments so a bound SID can unlock its end receipt.
      for (let count = 0; count < 24; count++) {
        const next = options.store.pendingHarkEvents().find(item => !attempted.has(item.event.id));
        if (!next) return;
        const { callId } = next;
        attempted.add(next.event.id);
        const event = options.store.recordHarkAttempt(callId, next.event.id, next.event.bodyDigest, new Date().toISOString());
        const connection = { apiBaseUrl: options.apiBaseUrl, signingSecret: options.signingSecret };
        const outcome = event.kind === 'hark_status'
          ? await (options.statusSender ?? deliverHarkStatus)({ ...connection, body: event.body })
          : event.kind === 'outbound_status'
            ? await (options.billingSender ?? deliverHarkBilling)({ ...connection, kind: 'outbound-status', body: event.body })
            : await (options.billingSender ?? deliverHarkBilling)({ ...connection, kind: 'call-ended', body: event.body });
        options.store.settleHarkEvent(callId, event.id, event.bodyDigest, outcome);
      }
    })().finally(() => { active = undefined; });
    return active;
  }
  return { flush };
}
