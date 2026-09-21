import type { OutboundAlertRequest } from './contract.js';
import {
  OutboundAlertStore,
  type OutboundAlertRecord,
} from './OutboundAlertStore.js';

export interface VoiceCallRequest {
  to: string;
  from: string;
  answerUrl: string;
  statusUrl: string;
}

export interface VoiceCallResult {
  callSid: string;
}

export interface VoiceDialer {
  placeCall(request: VoiceCallRequest): Promise<VoiceCallResult>;
}

export type DispatchResult =
  | { kind: 'accepted'; replay: false; record: OutboundAlertRecord }
  | { kind: 'accepted'; replay: true; record: OutboundAlertRecord }
  | { kind: 'conflict'; record: OutboundAlertRecord };

export interface OutboundAlertServiceOptions {
  store: OutboundAlertStore;
  dialer: VoiceDialer;
  publicBaseUrl: string;
  fromNumber: string;
  admitNewCall?: () => Promise<
    | { ok: true }
    | { ok: false; reason: string }
  >;
}

export class OutboundCallAdmissionError extends Error {
  readonly code = 'outbound_call_unavailable';

  constructor(readonly reason: string) {
    super(`outbound call unavailable: ${reason}`);
    this.name = 'OutboundCallAdmissionError';
  }
}

export class OutboundAlertService {
  constructor(private readonly options: OutboundAlertServiceOptions) {}

  async dispatch(request: OutboundAlertRequest, nowMs = Date.now()): Promise<DispatchResult> {
    const inspected = this.options.store.inspectReservation(request, nowMs);
    if (inspected.kind === 'conflict') {
      return { kind: 'conflict', record: inspected.record };
    }
    if (inspected.kind === 'existing') {
      return { kind: 'accepted', replay: true, record: inspected.record };
    }

    const admission = await this.options.admitNewCall?.();
    if (admission && !admission.ok) {
      throw new OutboundCallAdmissionError(admission.reason);
    }

    // Another request may have reserved the same idempotency key while this
    // request awaited the shared readiness check. Reserve rechecks identity;
    // only its one `new` result may reach Twilio.
    const reserved = this.options.store.reserve(request, nowMs);
    if (reserved.kind === 'conflict') {
      return { kind: 'conflict', record: reserved.record };
    }
    if (reserved.kind === 'existing') {
      return { kind: 'accepted', replay: true, record: reserved.record };
    }

    const { intentId } = reserved.record;
    this.options.store.markDialing(intentId);

    const answerUrl = new URL(
      `/outbound-alerts/${encodeURIComponent(intentId)}/answer`,
      ensureTrailingSlash(this.options.publicBaseUrl),
    );
    const statusUrl = new URL(
      `/outbound-alerts/${encodeURIComponent(intentId)}/status`,
      ensureTrailingSlash(this.options.publicBaseUrl),
    );

    let placed: VoiceCallResult;
    try {
      placed = await this.options.dialer.placeCall({
        to: request.to,
        from: this.options.fromNumber,
        answerUrl: answerUrl.toString(),
        statusUrl: statusUrl.toString(),
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      // A rejected client Promise does not prove Twilio rejected the create.
      // The provider may have accepted the call before the response was lost.
      // Freeze this as uncertain and wait for a signed Twilio lifecycle/answer
      // callback to adopt the one real SID; never redial automatically.
      this.options.store.markDispatchUncertain(intentId, message);
      throw error;
    }
    if (!/^CA[0-9a-fA-F]{32}$/u.test(placed.callSid)) {
      const error = new Error('provider returned an invalid call SID');
      this.options.store.markDispatchUncertain(intentId, error.message);
      throw error;
    }
    // A failure after the provider returns must never relabel the real call as
    // a dispatch failure. markQueued retains the SID in memory and latches
    // readiness until its ledger can be durably re-proven.
    const record = this.options.store.markQueued(intentId, placed.callSid);
    return { kind: 'accepted', replay: false, record };
  }
}

function ensureTrailingSlash(url: string): string {
  return url.endsWith('/') ? url : `${url}/`;
}
