import { parseEligiblePhoneCall, type EligibleContextBinding, type EligibleContextClient,
  type EligiblePhoneCall } from './eligibleContext.js';
import { PrimaryApiError, type PrimaryCall } from './primaryApi.js';
import type { BrokerCall } from './store.js';

type StartupCall = Pick<BrokerCall, 'id' | 'callSid' | 'primary' | 'liveGenerations'>;

function bindingFor(call: StartupCall): EligibleContextBinding | undefined {
  if (!call.callSid || !call.primary?.primaryDexterId || !call.primary.threadId) return;
  return { callSid: call.callSid, bindingId: call.primary.bindingId,
    primaryDexterId: call.primary.primaryDexterId, threadId: call.primary.threadId,
    ...(call.primary.callAssignmentRef ? { callAssignmentRef: call.primary.callAssignmentRef } : {}) };
}

/** A validated admission response may seed one first generation. Never persist it
 * or reuse it after a restart/replacement; the generation still asserts its version. */
export function createInboundStartupContexts(client: EligibleContextClient) {
  const pages = new Map<string, EligiblePhoneCall>();
  return {
    remember(call: StartupCall, response: PrimaryCall): void {
      pages.delete(call.id);
      const binding = bindingFor(call);
      if (!binding || call.liveGenerations) return;
      try {
        const page = parseEligiblePhoneCall(response, binding);
        if (page.state === 'open') pages.set(call.id, page);
      } catch { /* An unavailable optimization must retain the normal fresh fetch. */ }
    },
    take(call: StartupCall): EligibleContextClient {
      let initial = pages.get(call.id);
      pages.delete(call.id);
      const expected = bindingFor(call);
      if (!initial || !expected || call.liveGenerations) return client;
      try { initial = parseEligiblePhoneCall(initial, expected); }
      catch { return client; }
      return {
        async fetch(binding) {
          const page = initial; initial = undefined;
          if (page) {
            let checked: EligiblePhoneCall | undefined;
            try { checked = parseEligiblePhoneCall(page, binding); }
            catch { /* A changed owner or binding requires a fresh authenticated read. */ }
            if (checked) {
              // Admission may precede media by several seconds. Recheck eligibility
              // before loading retained text into Live, as well as the controller's
              // existing post-open assertion before caller audio is enabled.
              try {
                await client.assert(binding,checked.context.history.version,checked.context.history.admission);
                return checked; // Keep the original page/version together; the controller records the newer validation.
              }
              catch (error) {
                if (!(error instanceof PrimaryApiError && error.status === 409 && error.remoteCode === 'context_changed')) throw error;
              }
            }
          }
          return client.fetch(binding);
        },
        assert: (binding, version, admission) => client.assert(binding, version, admission),
        delivery: (binding, requestId, input, laterCall) => client.delivery(binding, requestId, input, laterCall),
      };
    },
    discard(callId: string): void { pages.delete(callId); },
    clear(): void { pages.clear(); },
  };
}
