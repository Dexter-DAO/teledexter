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

/** A binding response this recent seeds Live without a second eligibility read. The greeting
 * stream attaches Live in the same turn its binding lands (milliseconds later), and that second
 * read alone held Live back 0.45-1.36 s on the Oct 13 production calls (journal: first generation
 * createdAt minus mediaStartedAt). An older page (admission well before media) is rechecked first. */
export const STARTUP_PAGE_FRESH_MS = 1_000;

/** A validated admission response may seed one first generation. Never persist it
 * or reuse it after a restart/replacement; the generation still asserts its version
 * after Live opens, before any caller audio or Live output passes. */
export function createInboundStartupContexts(client: EligibleContextClient, options: { now?: () => number; freshMs?: number } = {}) {
  const now = options.now ?? Date.now;
  const freshMs = options.freshMs ?? STARTUP_PAGE_FRESH_MS;
  const pages = new Map<string, { page: EligiblePhoneCall; receivedAt: number }>();
  return {
    remember(call: StartupCall, response: PrimaryCall): void {
      pages.delete(call.id);
      const binding = bindingFor(call);
      if (!binding || call.liveGenerations) return;
      try {
        const page = parseEligiblePhoneCall(response, binding);
        if (page.state === 'open') pages.set(call.id, { page, receivedAt: now() });
      } catch { /* An unavailable optimization must retain the normal fresh fetch. */ }
    },
    take(call: StartupCall): EligibleContextClient {
      const remembered = pages.get(call.id);
      pages.delete(call.id);
      const expected = bindingFor(call);
      if (!remembered || !expected || call.liveGenerations) return client;
      let initial: EligiblePhoneCall | undefined;
      try { initial = parseEligiblePhoneCall(remembered.page, expected); }
      catch { return client; }
      return {
        async fetch(binding) {
          const page = initial; initial = undefined;
          if (page) {
            let checked: EligiblePhoneCall | undefined;
            try { checked = parseEligiblePhoneCall(page, binding); }
            catch { /* A changed owner or binding requires a fresh authenticated read. */ }
            // The binding response is the API's own eligibility read; a second one moments later
            // adds only its round trip. The post-open assertion still gates caller audio and Live
            // output, and replaces a page that changed in between.
            if (checked && freshMs > 0 && now() - remembered.receivedAt <= freshMs) return checked;
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
