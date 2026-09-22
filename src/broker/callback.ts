import { createHash } from 'node:crypto';
import Twilio from 'twilio';
import type { BrokerConfig } from './config.js';
import { isPhoneNumber } from './config.js';
import { fallbackTwiml, verifyTwilio, type ProviderCallProof } from './phone.js';
import { BrokerStore, terminalCallStatuses, type BrokerCall, type CallStatus } from './store.js';

type CallbackConfig = Pick<BrokerConfig, 'twilioAccountSid' | 'twilioAuthToken' | 'publicOrigin' | 'from' | 'allowedTo'>;
export interface ObservedCallbackCall {
  sid: string; accountSid: string; from: string; to: string; direction: string; status: string;
}
export interface CallbackOutboundRequest {
  requestId: string; purpose: string; to: string; callbackFor: string;
}
export interface CallbackFlowOptions {
  config: CallbackConfig;
  store: BrokerStore;
  fetchCall(callSid: string): Promise<ObservedCallbackCall>;
  /** Must reserve durably before dispatch and recover the same request on retry. */
  requestOutbound(input: CallbackOutboundRequest): Promise<BrokerCall>;
  now?: () => number;
  providerTimeoutMs?: number;
  cooldownMs?: number;
  maxPerHour?: number;
  maxPendingAgeMs?: number;
}

export const callbackPurpose = 'You asked Dexter to call you back. Ask what you would like help with.';
const callbackReceived = 'Thanks. You can hang up now. If phone access is ready, Dexter will call the verified number saved to your account.';
const callbackDeclined = 'You can call Dexter again whenever you are ready. Goodbye.';
const sidPattern = /^CA[0-9a-f]{32}$/i;

/** Stable UUIDv8: one callback request for this exact provider account and inbound leg. */
export function callbackRequestId(accountSid: string, callSid: string): string {
  const bytes = createHash('sha256').update(`dexter-broker/callback/v1:${accountSid}:${callSid}`).digest().subarray(0, 16);
  bytes[6] = (bytes[6] & 15) | 128;
  bytes[8] = (bytes[8] & 63) | 128;
  const hex = bytes.toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

/** Public inbound legs collect callback consent; account work belongs to the outbound leg. */
export function createCallbackFlow(options: CallbackFlowOptions) {
  const { config, store } = options;
  const now = options.now ?? Date.now;
  const providerTimeoutMs = options.providerTimeoutMs ?? 4_000;
  const cooldownMs = options.cooldownMs ?? 60_000;
  const maxPerHour = options.maxPerHour ?? 3;
  const maxPendingAgeMs = options.maxPendingAgeMs ?? 5 * 60_000;
  for (const [name, value] of Object.entries({ providerTimeoutMs, maxPerHour, maxPendingAgeMs })) {
    if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`Invalid callback ${name}`);
  }
  if (!Number.isSafeInteger(cooldownMs) || cooldownMs < 0) throw new Error('Invalid callback cooldownMs');
  const admitting = new Set<string>();
  const dispatching = new Set<string>();

  function savedInbound(call: BrokerCall): BrokerCall {
    const saved = store.get(call.id);
    if (!saved || saved.direction !== 'inbound' || !saved.callSid || !sidPattern.test(saved.callSid)
      || !isPhoneNumber(saved.number) || saved.callSid !== call.callSid || saved.number !== call.number) {
      throw new Error('callback_call_identity_mismatch');
    }
    return saved;
  }

  function verify(call: BrokerCall, proof: ProviderCallProof, route: string): BrokerCall {
    const saved = savedInbound(call);
    if (proof.url !== config.publicOrigin + route
      || !verifyTwilio(config, proof.signature, route, proof.params)
      || proof.params.CallSid !== saved.callSid || proof.params.Direction !== 'inbound'
      || proof.params.From !== saved.number || proof.params.To !== config.from) {
      throw new Error('callback_call_identity_mismatch');
    }
    return saved;
  }

  async function observe(call: BrokerCall): Promise<ObservedCallbackCall> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const observed = await Promise.race([
        options.fetchCall(call.callSid!),
        new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error('callback_provider_timeout')), providerTimeoutMs); }),
      ]);
      if (observed.sid !== call.callSid || observed.accountSid !== config.twilioAccountSid
        || observed.direction !== 'inbound' || observed.from !== call.number || observed.to !== config.from) {
        throw new Error('callback_provider_identity_mismatch');
      }
      return observed;
    } finally { clearTimeout(timer); }
  }

  function offer(call: BrokerCall, proof: ProviderCallProof): string {
    const saved = verify(call, proof, '/broker/incoming-call');
    if (saved.callback || terminalCallStatuses.has(saved.status) || saved.endedAt) return fallbackTwiml(callbackDeclined);
    const response = new Twilio.twiml.VoiceResponse();
    response.gather({ input: ['dtmf'], numDigits: 1, timeout: 8, actionOnEmptyResult: false,
      action: `${config.publicOrigin}/broker/callback/${saved.id}`, method: 'POST' })
      .say({ voice: 'Polly.Matthew' }, 'To have Dexter call the verified number saved to your account, press 1.');
    response.say({ voice: 'Polly.Matthew' }, callbackDeclined);
    response.hangup();
    return response.toString();
  }

  async function consent(call: BrokerCall, proof: ProviderCallProof): Promise<string> {
    let saved = verify(call, proof, `/broker/callback/${call.id}`);
    if (proof.params.Digits !== '1') return fallbackTwiml(callbackDeclined);
    const received = fallbackTwiml(callbackReceived);
    if (saved.callback || admitting.has(saved.id) || saved.endedAt || terminalCallStatuses.has(saved.status)) return received;
    if (!config.allowedTo.includes(saved.number)) return received;
    admitting.add(saved.id);
    try {
      const observed = await observe(saved);
      if (observed.status !== 'ringing' && observed.status !== 'in-progress') return received;
      saved = savedInbound(saved);
      if (saved.callback || saved.endedAt || terminalCallStatuses.has(saved.status)) return received;
      const timestamp = now();
      const prior = store.list().filter(other => other.id !== saved.id && other.number === saved.number && other.callback)
        .map(other => Date.parse(other.callback!.consentedAt));
      if (prior.some(at => !Number.isFinite(at) || timestamp - at < cooldownMs)
        || prior.filter(at => timestamp - at < 60 * 60_000).length >= maxPerHour) return received;
      store.update(saved.id, { callback: {
        requestId: callbackRequestId(config.twilioAccountSid, saved.callSid!),
        consentedAt: new Date(timestamp).toISOString(), state: 'requested',
      } });
      return received;
    } catch { return received; }
    finally { admitting.delete(saved.id); }
  }

  function finish(inbound: BrokerCall, state: 'started' | 'unavailable') {
    const current = savedInbound(inbound);
    if (current.callback?.state === 'requested') store.update(current.id, { callback: { ...current.callback, state } });
  }

  function recover(inbound: BrokerCall): boolean {
    const outbound = store.list().find(call => call.requestId === inbound.callback!.requestId);
    if (!outbound) return false;
    if (outbound.direction !== 'outbound' || outbound.number !== inbound.number || outbound.purpose !== callbackPurpose) {
      finish(inbound, 'unavailable');
      return true;
    }
    finish(inbound, outbound.status === 'failed' || outbound.status === 'canceled' ? 'unavailable' : 'started');
    return true;
  }

  /** Run from tracked maintenance. No sleep, provider create, or account lookup on the inbound HTTP leg. */
  async function dispatchPending(): Promise<void> {
    for (const candidate of store.list()) {
      if (candidate.callback?.state !== 'requested' || dispatching.has(candidate.id)) continue;
      dispatching.add(candidate.id);
      try {
        const inbound = savedInbound(candidate);
        if (inbound.callback!.requestId !== callbackRequestId(config.twilioAccountSid, inbound.callSid!)
          || !config.allowedTo.includes(inbound.number)) { finish(inbound, 'unavailable'); continue; }
        // Recover even after expiry: an accepted/uncertain dispatch remains the same call.
        if (recover(inbound)) continue;
        const age = now() - Date.parse(inbound.callback!.consentedAt);
        if (!Number.isFinite(age) || age < 0 || age > maxPendingAgeMs) { finish(inbound, 'unavailable'); continue; }
        const observed = await observe(inbound);
        if (!terminalCallStatuses.has(observed.status as CallStatus)) continue;
        const dispatchAge = now() - Date.parse(inbound.callback!.consentedAt);
        if (dispatchAge < 0 || dispatchAge > maxPendingAgeMs) { finish(inbound, 'unavailable'); continue; }
        store.update(inbound.id, { status: observed.status as CallStatus, endedAt: inbound.endedAt ?? new Date(now()).toISOString() });
        // The shared outbound executor rechecks current saved destination/Primary readiness
        // before model preflight, durably reserves before dial, and refuses uncertain retries.
        try {
          await options.requestOutbound({ requestId: inbound.callback!.requestId, purpose: callbackPurpose,
            to: inbound.number, callbackFor: inbound.id });
          if (!recover(inbound)) finish(inbound, 'unavailable');
        } catch { if (!recover(inbound)) finish(inbound, 'unavailable'); }
      } catch {
        // A read failure leaves the exact request pending until expiry or the next observation.
      } finally { dispatching.delete(candidate.id); }
    }
  }

  return { offer, consent, dispatchPending };
}
