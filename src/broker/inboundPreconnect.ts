import type { BrokerLiveConnection } from '../live/types.js';
import type { BrokerCall } from './store.js';
import { sameInboundApprovalRef, type InboundApprovalRef } from './inboundContract.js';

type Entry = { callSid: string; approval: InboundApprovalRef; connection?: BrokerLiveConnection;
  state: 'waiting' | 'failed' | 'adopted'; startedAt: number; opened: boolean; timer?: ReturnType<typeof setTimeout> };

/** Owns one transport per admitted inbound leg. It never sends a provider command. */
export interface InboundTransportTiming {
  event: 'broker_inbound_transport'; version: 1; callId: string;
  stage: 'connecting' | 'open' | 'adopted' | 'discarded'; elapsedMs: number; observedAt: string;
}
export function createInboundPreconnect(connect: () => BrokerLiveConnection,
  onTiming?: (event: InboundTransportTiming) => void) {
  const entries = new Map<string, Entry>();
  let closed = false;
  function emit(id: string, entry: Entry, stage: InboundTransportTiming['stage']) {
    try { onTiming?.({ event: 'broker_inbound_transport', version: 1, callId: id, stage,
      elapsedMs: Math.max(0, performance.now() - entry.startedAt), observedAt: new Date().toISOString() }); }
    catch { /* Telemetry cannot change transport ownership. */ }
  }
  function discard(id: string) {
    const entry = entries.get(id);
    if (!entry || entry.state === 'adopted' || entry.state === 'failed') return;
    entry.state = 'failed'; emit(id, entry, 'discarded');
    const connection = entry.connection; entry.connection = undefined;
    try { connection?.terminate(); } catch { /* A failed warm transport cannot retain the call. */ }
  }
  function forget(id: string) {
    discard(id); clearTimeout(entries.get(id)?.timer); entries.delete(id);
  }
  return {
    begin(call: BrokerCall) {
      const remote = call.inbound?.remote;
      if (closed || entries.has(call.id) || remote?.state !== 'approved' || !call.callSid
        || call.endedAt || call.inbound?.localStop || call.mediaStartedAt
        || (call.inbound?.mode && call.inbound.mode.kind !== 'ordinary')) return;
      const remaining = Math.min(Date.parse(remote.expiresAt), Date.parse(call.inbound!.waitUntil)) - Date.now();
      if (!Number.isFinite(remaining) || remaining <= 0) return;
      const entry: Entry = { callSid: call.callSid, approval: { ...remote.approvalRef }, state: 'waiting', startedAt: performance.now(), opened: false };
      entries.set(call.id, entry);
      // The existing admission deadline also bounds sockets whose caller never starts media.
      entry.timer = setTimeout(() => {
        if (entry.state === 'adopted') entries.delete(call.id);
        else discard(call.id); // A failed initial slot stays closed until the call is stopped.
      }, remaining); entry.timer.unref();
      emit(call.id, entry, 'connecting');
      try {
        entry.connection = connect();
        const opened = () => {
          if (entry.opened || entry.state === 'failed') return;
          entry.opened = true; emit(call.id, entry, 'open');
        };
        entry.connection.onOpen(opened);
        if (entry.connection.isOpen()) opened();
        entry.connection.onError(() => discard(call.id));
        entry.connection.onClose(() => discard(call.id));
      } catch { discard(call.id); }
    },
    factory(call: BrokerCall): (() => BrokerLiveConnection) | undefined {
      const entry = entries.get(call.id);
      if (!entry || entry.state === 'adopted') return;
      // Called by the session constructor only after it validates the prepared eligible history.
      return () => {
        const remote = call.inbound?.remote;
        if (closed || entry.state !== 'waiting' || !entry.connection || call.callSid !== entry.callSid
          || remote?.state !== 'bound' || remote.callState !== 'open' || !sameInboundApprovalRef(remote.approvalRef, entry.approval)
          || call.primary?.bindingId !== remote.bindingId || !call.mediaStartedAt || call.endedAt
          || call.inbound?.localStop || call.primaryClosed || call.primaryClosePending) {
          discard(call.id); throw new Error('inbound_preconnect_unavailable');
        }
        const connection = entry.connection; entry.connection = undefined;
        entry.state = 'adopted'; emit(call.id, entry, 'adopted'); return connection;
      };
    },
    discard, forget,
    close() { closed = true; for (const id of entries.keys()) forget(id); },
  };
}
