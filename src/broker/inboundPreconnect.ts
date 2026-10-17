import type { BrokerLiveConnection } from '../live/types.js';
import type { BrokerCall } from './store.js';
import { sameInboundApprovalRef, type InboundApprovalRef } from './inboundContract.js';

/** `approval` binds an inbound socket to its admission from approval on: adoption then requires that
 * exact approval, and a failed socket is not replaced. Until then a socket is a warm optimization,
 * like the outbound one. */
type Entry = { callSid: string; direction: 'inbound' | 'outbound'; approval?: InboundApprovalRef; connection?: BrokerLiveConnection;
  state: 'waiting' | 'failed' | 'adopted'; startedAt: number; opened: boolean; timer?: ReturnType<typeof setTimeout> };

/** Owns one transport per admitted inbound leg. It never sends a provider command. */
export interface InboundTransportTiming {
  event: 'broker_inbound_transport'; version: 1; callId: string;
  stage: 'connecting' | 'open' | 'adopted' | 'discarded'; elapsedMs: number; observedAt: string;
}
/** Upper bound for a warm socket opened before its account binding. Outbound: Primary binding (10s
 * client wait) plus media start (8s). Inbound: the same bound on the wait for approval, which then
 * re-arms the socket to the admission deadline. */
export const OUTBOUND_PRECONNECT_TTL_MS = 20_000;
export function createInboundPreconnect(connect: () => BrokerLiveConnection,
  onTiming?: (event: InboundTransportTiming) => void, warmTtlMs = OUTBOUND_PRECONNECT_TTL_MS) {
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
  /** The existing admission deadline (or the warm TTL) also bounds sockets whose caller never starts media. */
  function arm(id: string, entry: Entry, remaining: number) {
    clearTimeout(entry.timer);
    entry.timer = setTimeout(() => {
      if (entry.state === 'adopted') entries.delete(id);
      else discard(id); // A failed initial slot stays closed until the call is stopped.
    }, remaining); entry.timer.unref();
  }
  function open(id: string, entry: Entry, remaining: number) {
      entries.set(id, entry);
      arm(id, entry, remaining);
      emit(id, entry, 'connecting');
      try {
        entry.connection = connect();
        const opened = () => {
          if (entry.opened || entry.state === 'failed') return;
          entry.opened = true; emit(id, entry, 'open');
        };
        entry.connection.onOpen(opened);
        if (entry.connection.isOpen()) opened();
        entry.connection.onError(() => discard(id));
        entry.connection.onClose(() => discard(id));
      } catch { discard(id); }
  }
  /** An ordinary inbound leg whose wait has not stopped, ended or started media. */
  const inboundWaiting = (call: BrokerCall) => !closed && Boolean(call.inbound) && Boolean(call.callSid) && !call.endedAt
    && !call.inbound!.localStop && !call.mediaStartedAt && !(call.inbound!.mode && call.inbound!.mode.kind !== 'ordinary');
  return {
    /** Signed inbound continuation: its stream answers the leg and admission starts. Connect only the
     * Live socket meanwhile; no session, instructions, history or audio is sent until media start
     * adopts it, and adoption still requires the bound admission. */
    beginInbound(call: BrokerCall) {
      if (entries.has(call.id) || !inboundWaiting(call) || !call.inbound!.continuation || call.inbound!.closure || call.primaryClosed) return;
      const remaining = Math.min(Date.parse(call.inbound!.waitUntil) - Date.now(), warmTtlMs);
      if (!Number.isFinite(remaining) || remaining <= 0) return;
      open(call.id, { callSid: call.callSid!, direction: 'inbound', state: 'waiting', startedAt: performance.now(), opened: false }, remaining);
    },
    /** Approved inbound admission: from here the socket belongs to this approval. A socket opened at
     * the continuation is kept (no second connect); one that already failed or expired is replaced. */
    begin(call: BrokerCall) {
      const remote = call.inbound?.remote;
      if (!inboundWaiting(call) || remote?.state !== 'approved') return;
      const remaining = Math.min(Date.parse(remote.expiresAt), Date.parse(call.inbound!.waitUntil)) - Date.now();
      if (!Number.isFinite(remaining) || remaining <= 0) return;
      const existing = entries.get(call.id);
      if (existing) {
        if (existing.approval || existing.state === 'adopted' || existing.direction !== 'inbound' || existing.callSid !== call.callSid) return;
        if (existing.state === 'waiting' && existing.connection) {
          existing.approval = { ...remote.approvalRef }; arm(call.id, existing, remaining); return;
        }
        forget(call.id);
      }
      open(call.id, { callSid: call.callSid!, direction: 'inbound', approval: { ...remote.approvalRef },
        state: 'waiting', startedAt: performance.now(), opened: false }, remaining);
    },
    /** Signed outbound answer: connect only the Live socket while Primary binding runs.
     * No session, instructions, history or audio is sent until media start adopts it. */
    beginOutbound(call: BrokerCall) {
      if (closed || entries.has(call.id) || call.direction !== 'outbound' || call.inbound || call.verificationCallback
        || call.restrictedSetup || !call.callSid || call.endedAt || call.mediaStartedAt || call.primaryClosed || call.primaryClosePending) return;
      open(call.id, { callSid: call.callSid, direction: 'outbound', state: 'waiting', startedAt: performance.now(), opened: false }, warmTtlMs);
    },
    factory(call: BrokerCall): (() => BrokerLiveConnection) | undefined {
      const entry = entries.get(call.id);
      if (!entry || entry.state === 'adopted') return;
      // A warm socket (outbound, or inbound before approval) is only an optimization: if it closed,
      // failed or expired, the session opens its own connection exactly as before preconnect existed.
      if (!entry.approval && (entry.state !== 'waiting' || !entry.connection)) { forget(call.id); return; }
      // Called by the session constructor only after it validates the prepared eligible history.
      return () => {
        const remote = call.inbound?.remote;
        const admitted = entry.direction === 'inbound'
          ? remote?.state === 'bound' && remote.callState === 'open' && call.primary?.bindingId === remote.bindingId
            && (!entry.approval || sameInboundApprovalRef(remote.approvalRef, entry.approval))
          // Outbound: the signed answer callback and Primary's provider call check produced this binding.
          : call.direction === 'outbound' && !call.inbound && Boolean(call.primary?.bindingId);
        if (closed || entry.state !== 'waiting' || !entry.connection || call.callSid !== entry.callSid
          || !admitted || !call.mediaStartedAt || call.endedAt
          || call.inbound?.localStop || call.primaryClosed || call.primaryClosePending) {
          const outbound = entry.direction === 'outbound';
          discard(call.id);
          if (outbound && !closed && entry.state === 'failed' && call.direction === 'outbound') return connect();
          throw new Error('inbound_preconnect_unavailable');
        }
        const connection = entry.connection; entry.connection = undefined;
        entry.state = 'adopted'; emit(call.id, entry, 'adopted'); return connection;
      };
    },
    discard, forget,
    close() { closed = true; for (const id of entries.keys()) forget(id); },
  };
}
