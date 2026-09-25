import type { BrokerConfig } from './config.js';
import type { PrimaryApiClient, PrimaryCall } from './primaryApi.js';
import type { InboundOutcome } from './inboundContract.js';
import { BrokerStore, type BrokerCall, terminalCallStatuses } from './store.js';
import { verifyTwilio, type ProviderCallProof } from './phone.js';
import { createHash } from 'node:crypto';

export interface InboundProviderCall {
  sid: string; accountSid: string; from: string; to: string; direction: string; status: string;
}
export interface InboundFlowOptions {
  config: Pick<BrokerConfig, 'twilioAccountSid' | 'twilioAuthToken' | 'publicOrigin' | 'from'>;
  store: BrokerStore;
  primary: PrimaryApiClient;
  fetchCall(sid: string): Promise<InboundProviderCall>;
  closeBound(call: BrokerCall): Promise<void>;
  now?: () => number;
}
export const inboundPolicy = Object.freeze({ version: 1 as const, waitMs: 120_000, mediaGraceMs: 30_000 });

/** Coordinates one original inbound leg. Account approval is supplied by the API's separately authenticated owner path. */
export function createInboundFlow(options: InboundFlowOptions) {
  const { config, store, primary } = options;
  const now = options.now ?? Date.now;
  const pending = new Map<string, Promise<BrokerCall>>();
  const timestamp = () => new Date(now()).toISOString();
  function saved(id: string): BrokerCall {
    const call = store.get(id);
    if (!call?.inbound || !call.callSid || call.direction !== 'inbound') throw new Error('inbound_call_identity_mismatch');
    return call;
  }
  function serial(id: string, operation: () => Promise<BrokerCall>): Promise<BrokerCall> {
    const before = pending.get(id);
    const work = (before ? before.catch(() => undefined) : Promise.resolve()).then(operation);
    pending.set(id, work);
    void work.finally(() => { if (pending.get(id) === work) pending.delete(id); }).catch(() => {});
    return work;
  }
  function verify(proof: ProviderCallProof, route: string, call?: BrokerCall): void {
    if (proof.url !== config.publicOrigin + route || !verifyTwilio(config, proof.signature, route, proof.params)
      || proof.params.Direction !== 'inbound' || proof.params.To !== config.from
      || (call && (proof.params.CallSid !== call.callSid || proof.params.From !== call.number)))
      throw new Error('inbound_call_identity_mismatch');
  }
  function stopped(call: BrokerCall): boolean {
    return Boolean(call.inbound?.localStop || call.endedAt || terminalCallStatuses.has(call.status));
  }
  function bound(call: BrokerCall): Extract<InboundOutcome, { state: 'bound' }> | undefined {
    return call.inbound?.remote?.state === 'bound' ? call.inbound.remote : undefined;
  }
  function mediaAllowed(call: BrokerCall): boolean {
    const receipt = bound(call);
    return Boolean(receipt && receipt.callState === 'open' && !call.inbound?.closure && !call.primaryClosed
      && call.primary?.bindingId === receipt.bindingId && !stopped(call) && !call.mediaStartedAt
      && call.inbound?.mediaStartBy && now() < Date.parse(call.inbound.mediaStartBy));
  }
  function stopLocal(id: string, reason: string): BrokerCall {
    return store.transitionInbound(id, { type: 'stop', reason, observedAt: timestamp() });
  }
  function record(id: string, outcome: InboundOutcome): BrokerCall {
    return store.transitionInbound(id, { type: 'outcome', outcome, observedAt: timestamp() });
  }
  async function settle(id: string): Promise<BrokerCall> {
    let call = saved(id);
    if (call.inbound!.closure) {
      if (call.primaryClosePending) await options.closeBound(call);
      return saved(id);
    }
    const remote = call.inbound!.remote;
    if (remote?.state !== 'cancelled' && remote?.state !== 'expired' && remote?.state !== 'bound') {
      if (!primary.cancelInbound) throw new Error('inbound_api_unavailable');
      store.transitionInbound(id, { type: 'cancel_requested', observedAt: timestamp() });
      call = record(id, await primary.cancelInbound(call.inbound!.begin));
    }
    // A cancellation may reveal the binding that committed before the stop.
    // Closing it uses the usual transcript/request ordering, with v3's exact receipt at its closure step.
    if (bound(call)) await options.closeBound(call);
    return saved(id);
  }
  async function observe(call: BrokerCall): Promise<InboundProviderCall> {
    const result = await options.fetchCall(call.callSid!);
    if (result.sid !== call.callSid || result.accountSid !== config.twilioAccountSid
      || result.direction !== 'inbound' || result.from !== call.number || result.to !== config.from)
      throw new Error('inbound_provider_identity_mismatch');
    return result;
  }
  async function advance(id: string): Promise<BrokerCall> {
    let call = saved(id);
    if (stopped(call)) { stopLocal(id, call.inbound!.localStop?.reason ?? 'provider_terminal'); return settle(id); }
    if (call.mediaStartedAt) return call;
    const previous = bound(call);
    const deadline = previous ? call.inbound!.mediaStartBy! : call.inbound!.waitUntil;
    if (now() >= Date.parse(deadline)) {
      stopLocal(id, previous ? 'media_start_expired' : 'broker_cancelled'); return settle(id);
    }
    if (!primary.beginInbound || !primary.recoverInbound || !primary.inboundStatus || !primary.openInboundCall)
      throw new Error('inbound_api_unavailable');
    if (!call.inbound!.remote || call.inbound!.remote.state === 'absent') {
      if (call.inbound!.beginAttemptedAt) {
        const recovered = await primary.recoverInbound(call.inbound!.begin);
        if (recovered.state !== 'absent') call = record(id, recovered);
        // An absent read cannot undo a concurrent local stop or authorize a different begin.
        if (stopped(saved(id))) return settle(id);
      }
      if (!call.inbound!.remote || call.inbound!.remote.state === 'absent') {
        if (now() >= Date.parse(call.inbound!.waitUntil)) { stopLocal(id, 'broker_cancelled'); return settle(id); }
        store.transitionInbound(id, { type: 'begin_attempted', observedAt: timestamp() });
        call = record(id, await primary.beginInbound(call.inbound!.begin));
      }
    } else {
      call = record(id, await primary.inboundStatus((call.inbound!.remote as InboundOutcome).attachmentId));
    }
    if (stopped(saved(id))) return settle(id);
    if (saved(id).mediaStartedAt) return saved(id);
    if (call.inbound!.remote?.state === 'cancelled' || call.inbound!.remote?.state === 'expired') {
      stopLocal(id, `attachment_${call.inbound!.remote.state}`); return settle(id);
    }
    if (now() >= Date.parse(call.inbound!.waitUntil) && !bound(call)) { stopLocal(id, 'broker_cancelled'); return settle(id); }
    if (call.inbound!.remote?.state === 'pending') return call;
    const observed = await observe(call);
    call = saved(id);
    if (terminalCallStatuses.has(observed.status as BrokerCall['status'])) {
      store.update(id, { status: observed.status as BrokerCall['status'], endedAt: call.endedAt ?? timestamp() });
      stopLocal(id, 'provider_terminal'); return settle(id);
    }
    if (stopped(call)) return settle(id);
    if (call.mediaStartedAt) return call;
    if (observed.status !== 'in-progress') return call;
    let context: PrimaryCall;
    try {
      const receipt = bound(call);
      if (receipt) {
        if (receipt.callState === 'closed' || now() >= Date.parse(call.inbound!.mediaStartBy!)) {
          stopLocal(id, receipt.callState === 'closed' ? 'binding_closed' : 'media_start_expired'); return settle(id);
        }
        context = await primary.getContext(receipt.bindingId);
      } else if (call.inbound!.remote?.state === 'approved') {
        store.transitionInbound(id, { type: 'consume_attempted', observedAt: timestamp() });
        context = await primary.openInboundCall(call.inbound!.begin.providerProof, call.inbound!.remote.approvalRef);
        // /calls carries private context, but only this persisted outcome anchors media grace.
        const recovered = await primary.recoverInbound(call.inbound!.begin);
        if (recovered.state === 'absent') throw new Error('inbound_binding_outcome_unavailable');
        call = record(id, recovered);
        if (!bound(call)) throw new Error('inbound_binding_outcome_unavailable');
      } else return call;
    } catch (error) {
      // A lost consume/context reply can hide a committed binding. Retain the attempt and
      // recover its context-free outcome on the next iteration before any further action.
      throw error;
    }
    call = saved(id);
    if (stopped(call)) return settle(id);
    if (call.mediaStartedAt) return call;
    if (!bound(call) || context.bindingId !== bound(call)!.bindingId || context.callSid !== call.callSid)
      throw new Error('inbound_binding_identity_mismatch');
    if (context.state !== 'open' || bound(call)!.callState === 'closed'
      || now() >= Date.parse(call.inbound!.mediaStartBy!)) {
      stopLocal(id, context.state !== 'open' ? 'binding_closed' : 'media_start_expired'); return settle(id);
    }
    store.transitionInbound(id, { type: 'context', context, observedAt: timestamp() });
    return saved(id);
  }
  function accept(proof: ProviderCallProof): Promise<BrokerCall> {
    verify(proof, '/broker/incoming-call');
    const requestId = createHash('sha256').update(`incoming:${proof.params.CallSid}`).digest('hex');
    const { call } = store.reserveInbound({ requestId, purpose: 'The person called Dexter.', providerProof: proof }, inboundPolicy, timestamp());
    return serial(call.id, () => advance(call.id));
  }
  function resume(id: string, proof: ProviderCallProof): Promise<BrokerCall> {
    const call = saved(id);
    verify(proof, `/broker/inbound/${id}/continue`, call);
    return serial(id, () => advance(id));
  }
  function stop(id: string, reason: string): Promise<BrokerCall> {
    stopLocal(id, reason); // Visible immediately even when begin/consume is in flight.
    return serial(id, () => settle(id));
  }
  function tick(id: string): Promise<BrokerCall> { return serial(id, () => advance(id)); }
  async function drain(): Promise<void> { await Promise.allSettled([...pending.values()]); }
  return { accept, resume, stop, stopLocal, tick, mediaAllowed, drain };
}
