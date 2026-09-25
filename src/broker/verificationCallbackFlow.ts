import type { PrimaryApiClient, PrimaryCall } from './primaryApi.js';
import { assertQualifiedVerificationCallbackTurn, type QualifiedVerificationCallbackTurn } from './verificationCallbackTurn.js';
import { randomUUID } from 'node:crypto';
import { BrokerStore } from './store.js';
import type { VerificationCallbackClient } from './verificationCallbackClient.js';
import { phoneVerificationCallbackRefSchema, phoneVerificationCallbackCommandRef, sameCallbackRef, sameCallbackCommand, type CallbackProviderProof,
  type PhoneVerificationCallbackCompletion, type PhoneVerificationCallbackRef, type PhoneVerificationCallbackPermit } from './verificationCallbackContract.js';
import { parseVerificationCallbackJournal, prepareVerificationCallbackDispatch, recordVerificationCallbackGrant,
  fenceVerificationCallbackProvider, recordVerificationCallbackProvider, prepareVerificationCallbackAnswer,
  recordVerificationCallbackRemote, prepareVerificationCallbackCompletion, recordVerificationCallbackCompletion,
  stopVerificationCallback, endVerificationCallback, fenceVerificationCallbackEnd, type VerificationCallbackJournal } from './verificationCallbackJournal.js';
import type { VerificationCallbackKickoffResult } from './verificationCallbackKickoff.js';

export interface VerificationCallbackProviderObservation {
  timing?: import('./verificationCallbackReceipts.js').CallbackEndedTiming;
  accountSid: string; from: string; to: string; callSid: string;
  status: 'queued' | 'ringing' | 'in-progress' | 'completed' | 'busy' | 'failed' | 'no-answer' | 'canceled';
}
export interface VerificationCallbackFlowOptions {
  store: BrokerStore;
  api: VerificationCallbackClient;
  provider: {
    /** Existing Twilio adapter must derive all fields from this exact permit. */
    create(input: { permit: PhoneVerificationCallbackPermit; dispatchAttemptId: string }): Promise<VerificationCallbackProviderObservation>;
    hangup(callSid: string): Promise<VerificationCallbackProviderObservation>;
  };
  /** Already signature-authenticated answer form, with exact original bytes. */
  verifyAnswer(proof: CallbackProviderProof): boolean;
  now?: () => number;
  providerTimeoutMs: number;
}
function fail(): never { throw new Error('callback_unavailable'); }
/** Unmounted coordinator. Actual Store writes own the fences; no second side store. */
export function createVerificationCallbackFlow(options: VerificationCallbackFlowOptions) {
  const now = options.now ?? Date.now;
  if (!Number.isSafeInteger(options.providerTimeoutMs) || options.providerTimeoutMs < 1 || options.providerTimeoutMs > 30_000) fail();
  const timestamp = () => new Date(now()).toISOString();
  const privateAdmissions = new Map<string, Promise<PrimaryCall>>();
  const pending = new Map<string, { digest: string; promise: Promise<VerificationCallbackKickoffResult> }>();
  function read(ref: PhoneVerificationCallbackRef): VerificationCallbackJournal {
    const call = options.store.get(ref.requestId);
    if (!call?.verificationCallback) fail();
    const j = parseVerificationCallbackJournal(call.verificationCallback);
    if (!sameCallbackRef(j.permit.request, ref)) fail(); return j;
  }
  function save(ref: PhoneVerificationCallbackRef, value: VerificationCallbackJournal): VerificationCallbackJournal {
    const call = options.store.recordVerificationCallback(ref.requestId, value);
    if (!call.verificationCallback) fail(); return call.verificationCallback;
  }
  function result(j: VerificationCallbackJournal): VerificationCallbackKickoffResult {
    const state = j.completion?.observation && j.completion.observation.state !== 'absent' ? 'completed'
      : j.remote?.state ?? (j.dispatch ? 'dispatching' : 'reserved');
    return { v: 1, request: j.permit.request, state };
  }
  async function bounded<T>(work: Promise<T>, deadline: number): Promise<T> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try { return await Promise.race([work, new Promise<never>((_yes, no) => {
      timer = setTimeout(() => no(new Error('callback_unavailable')), Math.max(1, Math.min(options.providerTimeoutMs, deadline - now())));
    })]); } finally { clearTimeout(timer); }
  }
  async function recover(raw: PhoneVerificationCallbackRef): Promise<VerificationCallbackKickoffResult> {
    const ref = phoneVerificationCallbackRefSchema.parse(raw);
    let j = read(ref);
    if (j.completion) {
      const observed = await options.api.recoverCompletion(j.completion.command, j.permit.purpose);
      j = save(ref, recordVerificationCallbackCompletion(read(ref), observed));
    } else {
      const observed = await options.api.recover(ref);
      j = save(ref, recordVerificationCallbackRemote(read(ref), observed));
    }
    // Recovery never calls create/dispatch/answer/complete. Terminal enrollment
    // may finish the same leg through its separate one-shot hangup fence.
    if (j.permit.purpose === 'enrollment' && j.completion?.observation && j.completion.observation.state !== 'absent')
      await endLeg(ref);
    return result(read(ref));
  }
  function observeProvider(ref: PhoneVerificationCallbackRef, value: VerificationCallbackProviderObservation,
    source: 'create' | 'signed_callback' | 'fetch' | 'hangup'): VerificationCallbackJournal {
    return save(ref, recordVerificationCallbackProvider(read(ref), { ...value, source, observedAt: timestamp() }));
  }
  async function runKickoff(ref: PhoneVerificationCallbackRef): Promise<VerificationCallbackKickoffResult> {
    const old = options.store.get(ref.requestId);
    if (old) {
      if (!old.verificationCallback || !sameCallbackRef(old.verificationCallback.permit.request, ref)) fail();
      // Existing attempt is recovery-only even when a previous HTTP response was lost.
      if (old.verificationCallback.dispatch || old.verificationCallback.stopRequestedAt) return recover(ref);
    }
    const remote = await options.api.recover(ref);
    if (!sameCallbackRef(remote.request, ref) || remote.state !== 'reserved') {
      if (old?.verificationCallback) return result(save(ref, recordVerificationCallbackRemote(read(ref), remote)));
      fail();
    }
    const reserved = options.store.reserveVerificationCallback(remote.permit, timestamp());
    if (reserved.call.verificationCallback?.dispatch) return recover(ref);
    let j = save(ref, recordVerificationCallbackRemote(read(ref), remote));
    j = save(ref, prepareVerificationCallbackDispatch(j, randomUUID(), timestamp()));
    if (!j.dispatch) fail();
    const grant = await options.api.dispatch({ v: 1, request: ref, dispatchAttemptId: j.dispatch.attemptId });
    j = save(ref, recordVerificationCallbackGrant(read(ref), grant));
    j = save(ref, recordVerificationCallbackRemote(j, { v: 1, request: ref, state: 'dispatching',
      dispatchAttemptId: grant.dispatchAttemptId, expiresAt: grant.permit.expiresAt }));
    j = save(ref, fenceVerificationCallbackProvider(j, timestamp()));
    // Late successful create responses still record exact provider identity after
    // timeout. Neither timeout nor an absent service GET clears the durable fence.
    const contact = Promise.resolve().then(() => {
      const current = read(ref);
      if (current.stopRequestedAt || current.endRequestedAt || current.providerTerminal || now() >= Date.parse(current.permit.expiresAt)) fail();
      return options.provider.create({ permit: current.permit, dispatchAttemptId: grant.dispatchAttemptId });
    })
      .then(value => {
        const observed = observeProvider(ref, value, 'create');
        if (observed.stopRequestedAt && !observed.providerTerminal) void endLeg(ref).catch(() => {});
        return observed;
      });
    return result(await bounded(contact, Date.parse(j.permit.expiresAt)));
  }
  function kickoff(raw: PhoneVerificationCallbackRef): Promise<VerificationCallbackKickoffResult> {
    const ref = phoneVerificationCallbackRefSchema.parse(raw), key = ref.requestId;
    const call = options.store.get(key);
    if (call?.verificationCallback && !sameCallbackRef(call.verificationCallback.permit.request, ref)) fail();
    const prior = pending.get(key);
    if (prior) { if (prior.digest !== ref.requestDigest) fail(); return prior.promise; }
    const work = runKickoff(ref); pending.set(key, { digest: ref.requestDigest, promise: work });
    void work.finally(() => { if (pending.get(key)?.promise === work) pending.delete(key); }).catch(() => {});
    return work;
  }
  async function answer(ref: PhoneVerificationCallbackRef, proof: CallbackProviderProof) {
    let j = read(ref);
    if (!options.verifyAnswer(proof)) fail();
    if (j.answer) { if (JSON.stringify(j.answer.proof) !== JSON.stringify(proof)) fail(); return recover(ref); }
    j = save(ref, prepareVerificationCallbackAnswer(j, proof, timestamp()));
    if (!j.dispatch || !j.answer) fail();
    const remote = await options.api.answer({ v: 1, request: ref, brokerCallId: ref.requestId,
      dispatchAttemptId: j.dispatch.attemptId, providerProof: j.answer.proof }, j.permit.purpose);
    return result(save(ref, recordVerificationCallbackRemote(read(ref), remote)));
  }
  async function complete(input: PhoneVerificationCallbackCompletion, turn: QualifiedVerificationCallbackTurn) {
    assertQualifiedVerificationCallbackTurn(turn, input);
    let j = read(input.request);
    if (j.completion) {
      if (input.purpose !== j.permit.purpose || !sameCallbackCommand(j.completion.command, phoneVerificationCallbackCommandRef(input))) fail();
      return recover(input.request);
    }
    j = save(input.request, prepareVerificationCallbackCompletion(j, input, timestamp()));
    const observed = await options.api.complete(input);
    j = save(input.request, recordVerificationCallbackCompletion(read(input.request), observed));
    // Enrollment ends its leg and never asks Primary for context. Account access
    // must retire the restricted session before the separate privateCall consumer.
    if (j.permit.purpose === 'enrollment') await endLeg(input.request);
    return result(read(input.request));
  }
  /** Existing /calls wire, using the exact saved signed answer proof. The API
   * resolves completed callback authority; no assignment or new schema field.
   * The returned private context never enters the restricted setup host. */
  function privateCall(ref: PhoneVerificationCallbackRef, primary: Pick<PrimaryApiClient, 'openCall' | 'openVerifiedCallbackCall'>,
    setupSessionRetired: () => boolean): Promise<PrimaryCall> {
    const j = read(ref);
    if (j.permit.purpose !== 'account_access' || j.completion?.observation?.state !== 'admitted'
      || !j.answer || j.stopRequestedAt || j.endRequestedAt || j.providerTerminal
      || j.remote?.state === 'cancelled' || j.remote?.state === 'expired' || !setupSessionRetired()
      || now() >= Math.min(Date.parse(j.permit.expiresAt), Date.parse(j.completion.observation.admission.expiresAt),
        Date.parse(j.answer.startedAt) + j.permit.maxDurationSeconds * 1000)) fail();
    const admissionExpiresAt = j.completion.observation.admission.expiresAt, answerStartedAt = j.answer.startedAt;
    const old = privateAdmissions.get(ref.requestId); if (old) return old;
    const originalProof = structuredClone(j.answer.proof);
    const work = Promise.resolve().then(async () => {
      const current = read(ref);
      if (!setupSessionRetired() || current.stopRequestedAt || current.endRequestedAt || current.providerTerminal) fail();
      const opened = await (primary.openVerifiedCallbackCall ? primary.openVerifiedCallbackCall(originalProof) : primary.openCall(originalProof));
      if (opened.callSid !== originalProof.params.CallSid || opened.callAssignmentRef) fail();
      options.store.recordPrimaryBinding(ref.requestId, opened);
      const after = read(ref);
      if (!setupSessionRetired() || after.stopRequestedAt || after.endRequestedAt || after.providerTerminal
        || now() >= Math.min(Date.parse(j.permit.expiresAt), Date.parse(admissionExpiresAt),
          Date.parse(answerStartedAt) + j.permit.maxDurationSeconds * 1000)) {
        // Preserve the binding committed during a local stop as a close obligation.
        // Its private context is never returned to the retired setup consumer.
        options.store.update(ref.requestId, { primaryClosePending: true }); fail();
      }
      return opened;
    });
    privateAdmissions.set(ref.requestId, work);
    void work.finally(() => { if (privateAdmissions.get(ref.requestId) === work) privateAdmissions.delete(ref.requestId); }).catch(() => {});
    return work;
  }
  async function endLeg(ref: PhoneVerificationCallbackRef): Promise<void> {
    let j = read(ref);
    if (j.endDispatchedAt) return;
    if (!j.endRequestedAt) j = save(ref, endVerificationCallback(j, timestamp()));
    const sid = j.provider?.callSid ?? j.answer?.proof.params.CallSid;
    if (!sid || j.providerTerminal) return;
    save(ref, fenceVerificationCallbackEnd(j, timestamp()));
    const ended = options.provider.hangup(sid).then(value => {
      observeProvider(ref, value, 'hangup');
      save(ref, endVerificationCallback(read(ref), timestamp(), true));
    });
    await bounded(ended, now() + options.providerTimeoutMs);
  }
  async function stop(ref: PhoneVerificationCallbackRef): Promise<void> {
    save(ref, stopVerificationCallback(read(ref), timestamp()));
    // Hangup starts independently of any held API/creation/completion promise.
    const ending = endLeg(ref);
    const cancelling = options.api.cancel(ref).then(remote => { save(ref, recordVerificationCallbackRemote(read(ref), remote)); });
    await Promise.all([ending, cancelling]);
  }
  return { kickoff, recover, answer, complete, stop, observeProvider, endLeg, privateCall };
}
