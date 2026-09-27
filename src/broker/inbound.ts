import type { BrokerConfig } from './config.js';
import { PrimaryApiError, type PrimaryApiClient, type PrimaryCall } from './primaryApi.js';
import type { InboundOutcome } from './inboundContract.js';
import { BrokerStore, type BrokerCall, terminalCallStatuses } from './store.js';
import { verifyTwilio, type ProviderCallProof } from './phone.js';
import { createHash } from 'node:crypto';
import { createInboundAttemptTiming, type InboundTimingSink } from './inboundTiming.js';
import { INBOUND_ENROLLMENT_RESPONSE_PATH, enrollmentCompletionInputSchema, enrollmentCommand,
  sameEnrollmentCommand, enrollmentTransportErrorSchema, isEnrollmentHttpFailure, type EnrollmentCommand,
  type EnrollmentCompletionRecovery } from './inboundEnrollment.js';

export interface InboundProviderCall {
  sid: string; accountSid: string; from: string; to: string; direction: string; status: string;
}
export interface InboundFlowOptions {
  config: Pick<BrokerConfig, 'twilioAccountSid' | 'twilioAuthToken' | 'publicOrigin' | 'from'>;
  store: BrokerStore;
  primary: PrimaryApiClient;
  fetchCall(sid: string): Promise<InboundProviderCall>;
  closeBound(call: BrokerCall): Promise<void>;
  /** In-memory handoff after the original leg and context have been admitted. */
  onContext?(call: BrokerCall, context: PrimaryCall): void;
  onTiming?: InboundTimingSink;
  now?: () => number;
  /** Internal composition only. No entry gesture, route, settings or runtime activation is selected here. */
  enrollment?: { endLeg(call: BrokerCall): Promise<void> };
}
export const inboundPolicy = Object.freeze({ version: 1 as const, waitMs: 120_000, mediaGraceMs: 30_000 });

/** Coordinates one original inbound leg; the API owns account admission. */
export function createInboundFlow(options: InboundFlowOptions) {
  const { config, store, primary } = options;
  if (options.enrollment && (!primary.completeInboundEnrollment || !primary.recoverInboundEnrollment
    || typeof options.enrollment.endLeg !== 'function'))
    throw new Error('inbound_enrollment_api_unavailable');
  const now = options.now ?? Date.now;
  const pending = new Map<string, Promise<BrokerCall>>();
  const ending = new Map<string, Promise<void>>();
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
    if (call.inbound?.mode && call.inbound.mode.kind !== 'ordinary') return false;
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
  function enrollmentError(id: string, error: unknown): void {
    const observedAt = timestamp();
    const value = error instanceof PrimaryApiError ? { observedAt, code: error.remoteCode ?? error.code,
      ...(error.status === undefined ? {} : { status: error.status }),
      ...(error.retryable === undefined ? {} : { retryable: error.retryable }) } : { observedAt, code: 'request_failed' };
    const parsed = enrollmentTransportErrorSchema.safeParse(value);
    store.transitionInbound(id, { type: 'enrollment_error', observedAt,
      error: parsed.success ? parsed.data : { observedAt, code: 'request_failed' } });
  }
  function enrollmentObservation(id: string, result: EnrollmentCompletionRecovery): BrokerCall {
    return store.transitionInbound(id, { type: 'enrollment_observation', result, observedAt: timestamp() });
  }
  function endEnrollmentLeg(id: string): Promise<void> {
    const existing = ending.get(id);
    if (existing) return existing;
    const work = (async () => {
      const call = saved(id);
      if (!stopped(call)) return;
      if (!options.enrollment) throw new Error('inbound_enrollment_transport_unavailable');
      if (call.inbound?.mode?.kind === 'enrollment-only') {
        if (call.inbound.mode.endAcknowledgedAt) return;
        store.transitionInbound(id, { type: 'enrollment_end_requested', observedAt: timestamp() });
        await options.enrollment.endLeg(saved(id));
        // This acknowledges a hangup command, not a provider-terminal status.
        store.transitionInbound(id, { type: 'enrollment_end_acknowledged', observedAt: timestamp() });
      } else await options.enrollment.endLeg(call);
    })();
    ending.set(id, work);
    void work.finally(() => { if (ending.get(id) === work) ending.delete(id); }).catch(() => {});
    return work;
  }
  async function advanceEnrollment(id: string): Promise<BrokerCall> {
    let call = saved(id);
    const mode = call.inbound?.mode;
    if (mode?.kind !== 'enrollment-only') throw new Error('inbound_enrollment_mode_conflict');
    if (!stopped(call) && now() >= Date.parse(call.inbound!.waitUntil)) stopLocal(id, 'broker_cancelled');
    // End a stopped leg before waiting for its completion readback.
    await endEnrollmentLeg(id);
    if (!mode.observation || mode.observation.result.state === 'absent') {
      if (!primary.recoverInboundEnrollment) throw new Error('inbound_enrollment_api_unavailable');
      try { call = enrollmentObservation(id, await primary.recoverInboundEnrollment(mode.command)); }
      catch (error) { enrollmentError(id, error); throw new Error('inbound_enrollment_recovery_unavailable'); }
    } else call = saved(id);
    if (call.inbound?.mode?.kind === 'enrollment-only' && call.inbound.mode.observation?.result.state === 'activated')
      stopLocal(id, 'enrollment_activated');
    await endEnrollmentLeg(id);
    return saved(id);
  }
  async function settle(id: string): Promise<BrokerCall> {
    let call = saved(id);
    if (call.inbound?.mode?.kind === 'enrollment-only') return advanceEnrollment(id);
    if (call.inbound?.mode?.kind === 'undecided') { await endEnrollmentLeg(id); return saved(id); }
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
  async function advanceAttempt(id: string): Promise<BrokerCall> {
    const timing = createInboundAttemptTiming(id, options.onTiming);
    let call = saved(id);
    if (call.inbound?.mode?.kind === 'enrollment-only') return advanceEnrollment(id);
    if (stopped(call)) { stopLocal(id, call.inbound!.localStop?.reason ?? 'provider_terminal'); return settle(id); }
    if (call.mediaStartedAt) return call;
    const previous = bound(call);
    const deadline = previous ? call.inbound!.mediaStartBy! : call.inbound!.waitUntil;
    if (now() >= Date.parse(deadline)) {
      stopLocal(id, previous ? 'media_start_expired' : 'broker_cancelled'); return settle(id);
    }
    if (call.inbound?.mode?.kind === 'undecided') return call;
    if (!primary.beginInbound || !primary.recoverInbound || !primary.inboundStatus || !primary.openInboundCall)
      throw new Error('inbound_api_unavailable');
    // Recover an uncertain old attempt before considering fresh admission. Historical
    // sealed outcomes need no new continuation marker or reconstructed attestation.
    if (!call.inbound!.remote || call.inbound!.remote.state === 'absent') {
      if (call.inbound!.beginAttemptedAt) {
        call = store.transitionInbound(id, { type: 'outcome', outcome: await timing.measure('recover_attachment', () => primary.recoverInbound!(call.inbound!.begin)), observedAt: timestamp() });
      }
    } else {
      const attachmentId = call.inbound!.remote.attachmentId;
      call = record(id, await timing.measure('poll_attachment', () => primary.inboundStatus!(attachmentId)));
    }
    call = saved(id);
    if (stopped(call)) return settle(id);
    const state = call.inbound!.remote?.state;
    if (state === undefined || state === 'absent' || state === 'pending') {
      if (now() >= Date.parse(call.inbound!.waitUntil)) { stopLocal(id, 'broker_cancelled'); return settle(id); }
      // Returning hold TwiML is insufficient. Only a separately authenticated
      // continuation on this exact leg permits the original-proof begin POST.
      if (!call.inbound!.continuation) return call;
      store.transitionInbound(id, { type: 'begin_attempted', observedAt: timestamp() });
      // A retryable 503 or lost response preserves this attempt. The next
      // iteration reads the same outcome before any original-proof resubmission.
      call = record(id, await timing.measure('begin_admission', () => primary.beginInbound!(call.inbound!.begin)));
    }
    if (stopped(saved(id))) return settle(id);
    if (saved(id).mediaStartedAt) return saved(id);
    if (call.inbound!.remote?.state === 'cancelled' || call.inbound!.remote?.state === 'expired') {
      stopLocal(id, `attachment_${call.inbound!.remote.state}`); return settle(id);
    }
    if (now() >= Date.parse(call.inbound!.waitUntil) && !bound(call)) { stopLocal(id, 'broker_cancelled'); return settle(id); }
    if (call.inbound!.remote?.state === 'pending') return call;
    const observed = await timing.measure('provider_identity', () => observe(call));
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
        context = await timing.measure('refresh_context', () => primary.getContext(receipt.bindingId));
      } else if (call.inbound!.remote?.state === 'approved') {
        store.transitionInbound(id, { type: 'consume_attempted', observedAt: timestamp() });
        const approvalRef = call.inbound!.remote.approvalRef;
        context = await timing.measure('open_context', () => primary.openInboundCall!(call.inbound!.begin.providerProof, approvalRef));
        // /calls carries private context, but only this persisted outcome anchors media grace.
        const recovered = await timing.measure('recover_binding', () => primary.recoverInbound!(call.inbound!.begin));
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
    options.onContext?.(saved(id), context);
    return saved(id);
  }
  async function advance(id: string): Promise<BrokerCall> {
    try { return await advanceAttempt(id); }
    catch (error) {
      if (error instanceof PrimaryApiError && error.status === 403 && error.retryable === false
        && (error.remoteCode === 'inbound_carrier_attestation_required' || error.remoteCode === 'inbound_enrollment_required')
        && (!saved(id).inbound!.mode || saved(id).inbound!.mode?.kind === 'ordinary')
        && !saved(id).inbound!.admissionRefusal) {
        // Current enrollment may fail at begin, consumption, or context access.
        // Local refusal never proves cancellation; a committed consume wins and closes.
        store.transitionInbound(id, { type: 'admission_refused', code: error.remoteCode, observedAt: timestamp() });
        return settle(id);
      }
      throw error;
    }
  }
  function accept(proof: ProviderCallProof): Promise<BrokerCall> {
    verify(proof, '/broker/incoming-call');
    const requestId = createHash('sha256').update(`incoming:${proof.params.CallSid}`).digest('hex');
    const { call, existing } = store.reserveInbound({ requestId, purpose: 'The person called Dexter.', providerProof: proof },
      inboundPolicy, timestamp(), options.enrollment !== undefined);
    // First return hold; maintenance also sees no marker and cannot begin.
    return existing ? serial(call.id, () => advance(call.id)) : Promise.resolve(call);
  }
  function resume(id: string, proof: ProviderCallProof): Promise<BrokerCall> {
    const call = saved(id);
    verify(proof, `/broker/inbound/${id}/continue`, call);
    if (Object.prototype.hasOwnProperty.call(proof.params, 'Digits')) throw new Error('inbound_digit_form_route_mismatch');
    return serial(id, () => {
      const current = saved(id);
      if (current.inbound?.mode?.kind === 'enrollment-only') return advanceEnrollment(id);
      // A terminal/expired callback remains useful for recovery and closure.
      if (!stopped(current) && now() < Date.parse(current.inbound!.waitUntil))
        store.transitionInbound(id, { type: 'continuation', providerProof: proof, observedAt: timestamp() });
      return advance(id);
    });
  }
  function completeEnrollment(id: string, digitProof: ProviderCallProof): Promise<BrokerCall> {
    const call = saved(id);
    verify(digitProof, INBOUND_ENROLLMENT_RESPONSE_PATH, call);
    const parsed = enrollmentCompletionInputSchema.safeParse({ begin: call.inbound!.begin, digitProof });
    if (!parsed.success) throw new Error('inbound_enrollment_voice_invalid');
    const input = parsed.data;
    const command: EnrollmentCommand = enrollmentCommand(input);
    return serial(id, async () => {
      const current = saved(id);
      const mode = current.inbound?.mode;
      // Late enrollment input cannot change or terminate an ordinary call.
      if (!mode || mode.kind === 'ordinary') throw new Error('inbound_enrollment_mode_conflict');
      if (mode.kind === 'enrollment-only') {
        if (!sameEnrollmentCommand(mode.command, command)) throw new Error('inbound_enrollment_command_conflict');
        return advanceEnrollment(id);
      }
      if (!options.enrollment || !primary.completeInboundEnrollment) throw new Error('inbound_enrollment_api_unavailable');
      // One durable write chooses enrollment-only and fences POST. Neither the raw
      // digit form nor its signature is passed to a journal transition.
      store.transitionInbound(id, { type: 'enrollment_dispatch', command, observedAt: timestamp() });
      try { enrollmentObservation(id, await primary.completeInboundEnrollment(input)); }
      catch (error) {
        enrollmentError(id, error);
        if (error instanceof PrimaryApiError && error.retryable === false
          && isEnrollmentHttpFailure(error.status, { code: error.remoteCode, retryable: error.retryable })) {
          stopLocal(id, 'enrollment_transport_refused');
          await endEnrollmentLeg(id);
        }
        throw new Error('inbound_enrollment_completion_unavailable');
      }
      if (saved(id).inbound?.mode?.kind === 'enrollment-only') {
        const latest = saved(id).inbound?.mode;
        if (latest?.kind === 'enrollment-only' && latest.observation?.result.state === 'activated')
          stopLocal(id, 'enrollment_activated');
      }
      await endEnrollmentLeg(id);
      return saved(id);
    });
  }
  function stop(id: string, reason: string): Promise<BrokerCall> {
    const call = stopLocal(id, reason); // Visible immediately even when remote work is in flight.
    // Enrollment hangup cannot wait behind its completion POST or recovery GET.
    const end = call.inbound?.mode && call.inbound.mode.kind !== 'ordinary' ? endEnrollmentLeg(id) : undefined;
    // Attach the rejection handler immediately while serial admission may still wait.
    const ended = end?.then(() => undefined, () => 'inbound_enrollment_end_unavailable' as const);
    return serial(id, async () => {
      if (await ended) throw new Error('inbound_enrollment_end_unavailable');
      return settle(id);
    });
  }
  function tick(id: string): Promise<BrokerCall> { return serial(id, () => advance(id)); }
  async function drain(): Promise<void> { await Promise.allSettled([...pending.values(), ...ending.values()]); }
  return { accept, resume, completeEnrollment, stop, stopLocal, tick, mediaAllowed, drain };
}
