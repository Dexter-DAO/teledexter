import { PrimaryApiError } from './primaryApi.js';
import { randomUUID } from 'node:crypto';
import { assertFrozenCallerSpeechCurrent, type FrozenCallerTurn } from './callerTurn.js';
import type { LiveBackendChannel, LiveDelegationRequest } from '../live/types.js';
import { restrictedSetupToolSchema } from '../live/restrictedSetupTools.js';
import type { RestrictedSetupHost, RestrictedSetupScope, RestrictedSetupPhase } from './restrictedSetupVoice.js';
import type { RestrictedCallerFrontier } from './restrictedCallerTurn.js';
import { BrokerStore } from './store.js';
import type { VerificationCallbackClient } from './verificationCallbackClient.js';
import type { createVerificationCallbackFlow } from './verificationCallbackFlow.js';
import { prepareRestrictedAccess, recordRestrictedAccessRefusal, stopRestrictedSetup, validateRestrictedSetupCall } from './restrictedSetupJournal.js';
import { parseVerificationCallbackJournal } from './verificationCallbackJournal.js';
import { qualifyVerificationCallbackTurn, type QualifiedVerificationCallbackTurn } from './verificationCallbackTurn.js';
import { phoneVerificationCallbackCompletionSchema, type PhoneVerificationCallbackRef } from './verificationCallbackContract.js';

export interface RestrictedSetupHostOptions {
  store: BrokerStore;
  api: VerificationCallbackClient;
  flow: ReturnType<typeof createVerificationCallbackFlow>;
  receiptSecret: string;
  now?: () => number;
  isCurrent(scope: RestrictedSetupScope): boolean;
  frontier(scope: RestrictedSetupScope): RestrictedCallerFrontier;
  freeze(scope: RestrictedSetupScope, request: LiveDelegationRequest,
    channel: Pick<LiveBackendChannel, 'getContext' | 'subscribeContext' | 'signal'>,
    usedSourceIds: readonly string[]): Promise<FrozenCallerTurn | undefined>;
  /** Pop only a previously authenticated same-leg protected capture. It is not a
   * model argument or transcript search. Returning a code consumes this local
   * ephemeral value even if the following HTTP result is lost. */
  takeChallenge(scope: RestrictedSetupScope, request: PhoneVerificationCallbackRef): string | undefined;
  /** Opens protected capture only after this explicit completion request. The
   * caller-facing transport retires Live input before collecting any code. */
  requestProtectedInput(scope: RestrictedSetupScope): Promise<void>;
  /** Exact protected transport attachment, after retiring the old Live generation. */
  isProtectedInputCurrent(scope: RestrictedSetupScope): boolean;
  /** Host stops this original restricted leg, independently of API cleanup. */
  stopOriginal(scope: RestrictedSetupScope): void;
}
function fail(): never { throw new Error('restricted_setup_unavailable'); }
/** Finite tool consumer. Original caller text lives only in the admission stack;
 * the existing journals retain safe operation and keyed turn receipts. */
export interface RestrictedSetupHostController extends RestrictedSetupHost {
  completeProtectedInput(scope: RestrictedSetupScope): Promise<RestrictedSetupPhase>;
  abandonProtectedInput(scope: RestrictedSetupScope): void;
}
export function createRestrictedSetupHost(options: RestrictedSetupHostOptions): RestrictedSetupHostController {
  const now = options.now ?? Date.now;
  if (options.receiptSecret.length < 32) fail();
  const used = new Set<string>();
  const protectedTurns = new Map<string, { scope: RestrictedSetupScope; turn: QualifiedVerificationCallbackTurn }>();
  let serial: Promise<unknown> = Promise.resolve();
  function call(scope: RestrictedSetupScope) {
    const value = options.store.get(scope.callId);
    if (!value || value.callSid !== scope.callSid || value.primary || value.endedAt || !options.isCurrent(scope)) fail();
    if (value.verificationCallback) {
      const j = parseVerificationCallbackJournal(value.verificationCallback);
      if (!j.answer || j.answer.proof.params.CallSid !== scope.callSid || j.stopRequestedAt || j.endRequestedAt || j.providerTerminal
        || now() >= Math.min(Date.parse(j.permit.expiresAt), Date.parse(j.answer.startedAt) + j.permit.maxDurationSeconds * 1000)) fail();
    } else {
      validateRestrictedSetupCall(value);
      if (!value.restrictedSetup || value.restrictedSetup.stoppedAt || now() >= Date.parse(value.restrictedSetup.deadlineAt)) fail();
    }
    return value;
  }
  const current = (scope: RestrictedSetupScope) => { try { call(scope); return true; } catch { return false; } };
  async function phase(scope: RestrictedSetupScope, signal: AbortSignal): Promise<RestrictedSetupPhase> {
    const saved = call(scope);
    if (signal.aborted) fail();
    if (saved.verificationCallback) {
      const j = saved.verificationCallback;
      if (j.completion) {
        await options.flow.recover(j.permit.request);
        const latest = options.store.get(scope.callId)?.verificationCallback?.completion?.observation;
        return latest?.state === 'enrolled' || latest?.state === 'admitted' ? 'complete'
          : latest?.state === 'rejected' ? 'unavailable' : 'checking';
      }
      return j.remote?.state === 'answered' ? 'verification_available' : 'checking';
    }
    if (saved.restrictedSetup?.access?.refusal) return 'account_authentication_required';
    const ref = saved.restrictedSetup?.access?.request;
    if (!ref) return 'account_authentication_required';
    const result = await options.api.recover(ref, signal);
    return result.state === 'cancelled' || result.state === 'expired' ? 'unavailable'
      : result.state === 'absent' ? 'checking' : 'callback_requested';
  }
  return {
    isCurrent: current,
    abandonProtectedInput(scope) {
      const saved = protectedTurns.get(scope.callId);
      if (saved && saved.scope.callSid === scope.callSid && saved.scope.sessionId === scope.sessionId) protectedTurns.delete(scope.callId);
    },
    async completeProtectedInput(scope) {
      const pending = protectedTurns.get(scope.callId);
      if (!pending || pending.scope.callSid !== scope.callSid || pending.scope.sessionId !== scope.sessionId
        || !options.isProtectedInputCurrent(scope)) fail();
      // Consume the accepted instruction once. Restart cannot fabricate it; a
      // lost completion result is recovered from the existing durable command.
      protectedTurns.delete(scope.callId);
      const saved = options.store.get(scope.callId), j = saved?.verificationCallback;
      if (!j || saved.primary || saved.endedAt || j.permit.purpose !== 'enrollment' || j.stopRequestedAt
        || j.endRequestedAt || j.providerTerminal || j.completion || !j.answer
        || j.answer.proof.params.CallSid !== scope.callSid
        || now() >= Math.min(Date.parse(j.permit.expiresAt), Date.parse(j.answer.startedAt) + j.permit.maxDurationSeconds * 1000)) fail();
      const challenge = options.takeChallenge(scope, j.permit.request);
      const completion = phoneVerificationCallbackCompletionSchema.parse({ v: 1, request: j.permit.request,
        brokerCallId: scope.callId, callSid: scope.callSid, turn: pending.turn.turn, accepted: true, purpose: 'enrollment', challenge });
      await options.flow.complete(completion, pending.turn);
      const result = options.store.get(scope.callId)?.verificationCallback?.completion?.observation;
      return result?.state === 'enrolled' ? 'complete' : result?.state === 'rejected' ? 'unavailable' : 'checking';
    },
    async observe(scope, signal) { return { ...scope, phase: await phase(scope, signal) }; },
    handleCaller(input, signal, caller) {
      const scope = { callId: input.callId, callSid: input.callSid, sessionId: input.sessionId };
      const acceptedFrontier = { ...options.frontier(scope) };
      const acceptedRevision = caller.getContext().callerRevision;
      const unchanged = () => { const latest = options.frontier(scope); return latest.sessionId === acceptedFrontier.sessionId
        && latest.audioThroughMs === acceptedFrontier.audioThroughMs && latest.observedSpeechAtMs === acceptedFrontier.observedSpeechAtMs
        && caller.getContext().callerRevision === acceptedRevision; };
      const run = serial.then(async () => {
        const tool = restrictedSetupToolSchema.parse(input.tool);
        const answer = (value: RestrictedSetupPhase) => ({ callId: input.callId, callSid: input.callSid, sessionId: input.sessionId, phase: value,
          requestId: input.request.requestId, publicationId: `setup_${input.toolCallId}` });
        call(scope); if (signal.aborted) fail();
        if (tool.name === 'phone_setup_status') return answer(await phase(scope, signal));
        if (!unchanged()) return answer('checking');
        const channel = { ...caller, signal };
        const frozen = await options.freeze(scope, input.request, channel, [...used]);
        if (!frozen || signal.aborted || !current(scope)) return answer('checking');
        assertFrozenCallerSpeechCurrent(frozen, options.frontier(scope));
        if (frozen.audioThroughMs !== acceptedFrontier.audioThroughMs || frozen.observedSpeechAtMs !== acceptedFrontier.observedSpeechAtMs)
          return answer('checking');
        if (caller.getContext().callerRevision !== frozen.callerRevision) return answer('checking');
        // One exact raw source can admit one setup effect in this generation.
        if (used.has(frozen.turn.sourceEventId)) return answer('checking');
        used.add(frozen.turn.sourceEventId);
        const speechCurrent = () => { try { assertFrozenCallerSpeechCurrent(frozen, options.frontier(scope));
          return caller.getContext().callerRevision === frozen.callerRevision; } catch { return false; } };
        const saved = call(scope);
        if (tool.name === 'decline_phone_setup') {
          if (saved.verificationCallback) await options.flow.stop(saved.verificationCallback.permit.request);
          else {
            options.store.recordRestrictedSetup(saved.id, stopRestrictedSetup(saved, new Date(now()).toISOString()));
            options.stopOriginal(scope);
          }
          return answer('declined');
        }
        if (tool.name === 'request_verified_callback') {
          if (!saved.restrictedSetup || !saved.inbound) return answer('unavailable');
          if (saved.restrictedSetup.access?.refusal) return answer('account_authentication_required');
          if (saved.restrictedSetup.access) {
            const observed = await options.api.recover(saved.restrictedSetup.access.request, signal);
            if (observed.state !== 'reserved' || signal.aborted || !current(scope) || !speechCurrent()) return answer('checking');
            // A fresh explicit request may resume the same saved operation. This
            // never repeats prepareAccess, and kickoff retains its provider fence.
            await options.flow.kickoff(observed.request); return answer('callback_requested');
          }
          const request = { v: 1 as const, requestId: randomUUID(), original: saved.inbound.begin, explicitlyRequested: true as const };
          options.store.recordRestrictedSetup(saved.id, prepareRestrictedAccess(saved, request, new Date(now()).toISOString()));
          // The permanent access fence precedes POST. Unknown/absent recovery
          // never repeats this POST or manufactures a second request.
          let remote;
          try { remote = await options.api.prepareAccess(request, signal); }
          catch (error) {
            if (error instanceof PrimaryApiError && error.retryable === false
              && ((error.status === 403 && error.remoteCode === 'callback_consent_required')
                || (error.status === 409 && (error.remoteCode === 'phone_tab_required' || error.remoteCode === 'callback_financial_admission_required')))) {
              options.store.recordRestrictedSetup(saved.id, recordRestrictedAccessRefusal(call(scope), { observedAt: new Date(now()).toISOString(),
                status: error.status, code: error.remoteCode, retryable: false }));
              return answer('account_authentication_required');
            }
            throw error;
          }
          if (signal.aborted || !current(scope) || !speechCurrent()) return answer('checking');
          if (remote.state !== 'reserved') return answer(remote.state === 'absent' ? 'checking' : 'unavailable');
          await options.flow.kickoff(remote.request);
          return answer('callback_requested');
        }
        if (!saved.verificationCallback) return answer('unavailable');
        const j = saved.verificationCallback;
        if (j.completion) { await options.flow.recover(j.permit.request); return answer('checking'); }
        const turn = qualifyVerificationCallbackTurn({ journal: j, request: input.request, frozen, toolCallId: input.toolCallId,
          current: { ...options.frontier(scope), transcript: caller.getContext() }, acceptedAtMs: now(),
          receiptSecret: options.receiptSecret, execution: 'complete_verification', isCurrent: () => current(scope) });
        // Protected challenge is never present in the function call or its output.
        const challenge = j.permit.purpose === 'enrollment' ? options.takeChallenge(scope, j.permit.request) : undefined;
        if (j.permit.purpose === 'enrollment' && challenge === undefined) {
          if (protectedTurns.has(scope.callId)) fail();
          protectedTurns.set(scope.callId, { scope: { ...scope }, turn });
          try { await options.requestProtectedInput(scope); }
          catch (error) { protectedTurns.delete(scope.callId); throw error; }
          return answer('verification_available');
        }
        const completion = phoneVerificationCallbackCompletionSchema.parse({ v: 1, request: j.permit.request,
          brokerCallId: saved.id, callSid: input.callSid, turn: turn.turn, accepted: true, purpose: j.permit.purpose,
          ...(challenge === undefined ? {} : { challenge }) });
        await options.flow.complete(completion, turn);
        const result = options.store.get(saved.id)?.verificationCallback?.completion?.observation;
        return answer(result?.state === 'enrolled' || result?.state === 'admitted' ? 'complete'
          : result?.state === 'rejected' ? 'unavailable' : 'checking');
      });
      serial = run.catch(() => {}); return run;
    },
  };
}
