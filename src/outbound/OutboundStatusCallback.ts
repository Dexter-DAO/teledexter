import type {
  OutboundAlertRecord,
  OutboundAlertStore,
} from './OutboundAlertStore.js';
import type { PhoneApiCallbackOutbox } from './PhoneApiCallbackOutbox.js';

const TERMINAL_OUTBOUND_STATES = new Set([
  'completed',
  'failed',
  'busy',
  'no-answer',
  'canceled',
  'expired',
]);

export type DurableOutboundStatusResult =
  | { kind: 'invalid' }
  | { kind: 'forbidden' }
  | { kind: 'ignored' }
  | { kind: 'retry'; error: string }
  | {
      kind: 'updated';
      callbackId: string;
      record: OutboundAlertRecord;
    };

/**
 * Transaction boundary for a Twilio lifecycle webhook. The exact API callback
 * is durably reserved before the Twilio status mutation is acknowledged. If
 * the second ledger write fails, Twilio can retry and the first callback body
 * (including observedAt) is reused byte-for-byte.
 */
export function persistOutboundStatusCallback(input: {
  store: OutboundAlertStore;
  callbackOutbox: PhoneApiCallbackOutbox;
  intentId: string;
  callSid: string;
  callStatus: string;
  sequenceNumber: number;
  now?: () => number;
}): DurableOutboundStatusResult {
  const nowMs = input.now?.() ?? Date.now();
  const preview = input.store.previewTwilioStatus(
    input.intentId,
    input.callSid,
    input.callStatus,
    input.sequenceNumber,
    nowMs,
  );
  if (!preview) return { kind: 'forbidden' };
  if (preview.kind === 'invalid') return { kind: 'invalid' };
  if (preview.kind !== 'updated') return { kind: 'ignored' };

  const callbackId =
    `outbound-status:${input.intentId}:${input.callSid}:${input.sequenceNumber}`;
  const existingCallback = input.callbackOutbox.get(callbackId);
  const callbackBody = existingCallback?.body ?? {
    intentId: preview.record.intentId,
    eventId: preview.record.request.eventId,
    callSid: input.callSid,
    state: preview.record.state,
    observedAt: new Date(nowMs).toISOString(),
  };

  try {
    const reserved = input.callbackOutbox.reserve({
      id: callbackId,
      path: '/internal/voice/outbound-status',
      body: callbackBody,
    });
    if (reserved.state === 'rejected') {
      return { kind: 'retry', error: 'callback_identity_conflict' };
    }
    if (TERMINAL_OUTBOUND_STATES.has(preview.record.state)) {
      // If Twilio never opened media, the signed terminal status is the
      // authoritative close fence. Promote the pre-TwiML receipt before the
      // provider webhook is acknowledged; retries are byte-stable.
      input.callbackOutbox.finalizeAwaitingMedia({
        id: `call-ended:${input.callSid}`,
        endedAtMs: nowMs,
      });
    }
    const update = input.store.updateTwilioStatus(
      input.intentId,
      input.callSid,
      input.callStatus,
      input.sequenceNumber,
      nowMs,
    );
    if (!update || update.kind !== 'updated') {
      return { kind: 'retry', error: 'status_persistence_race' };
    }
    return {
      kind: 'updated',
      callbackId,
      record: update.record,
    };
  } catch (error) {
    return {
      kind: 'retry',
      error: error instanceof Error ? error.message : String(error),
    };
  }
}
