import { restrictedSetupToolSchema, type RestrictedSetupTool, type RestrictedSetupTools, type RestrictedSetupToolResult } from '../live/restrictedSetupTools.js';
import { createHash } from 'node:crypto';
import { createBrokerLiveSession } from '../live/session.js';
import type { BrokerLiveSession, BrokerLiveSessionOptions, LiveBackendChannel,
  LiveDelegationRequest, LiveSessionClosed } from '../live/types.js';

/** Internal host port only. These are not shared API request/response schemas. */
export type RestrictedSetupPhase = 'account_authentication_required' | 'verification_available'
  | 'standing_callback_available' | 'checking' | 'verification_requested' | 'callback_requested'
  | 'complete' | 'declined' | 'unavailable';
export interface RestrictedSetupScope { callId: string; callSid: string; sessionId: string }
export interface RestrictedSetupObservation extends RestrictedSetupScope { phase: RestrictedSetupPhase }
export interface RestrictedSetupResolution extends RestrictedSetupObservation {
  requestId: string;
  publicationId: string;
}
export type RestrictedSetupCallerContext = Pick<LiveBackendChannel, 'getContext' | 'subscribeContext'>;
export interface RestrictedSetupHost {
  /** True only while this exact transport remains in restricted setup mode. */
  isCurrent(scope: RestrictedSetupScope): boolean;
  /** Read/recover existing setup state. This method must not dispatch a call. */
  observe(scope: RestrictedSetupScope, signal: AbortSignal): Promise<RestrictedSetupObservation>;
  /** Owns explicit caller-turn qualification and durable operation identity before
   * any effect. Only linking, a permitted verification call, an evidenced standing
   * callback, or declining setup are permitted. Account authentication/consent and
   * API-selected destination are verified by the host, never inferred from speech,
   * From, a billing contact or model text. Incoming access is not verification-call
   * consent. Chargeable callbacks need the API's exact mandate/tariff/leg permit;
   * this original restricted leg must not use caller-number billing inference.
   * Replays/unknown effects recover the same
   * journal operation; this voice adapter supplies no dispatch authority. */
  handleCaller(input: RestrictedSetupScope & { request: LiveDelegationRequest; tool: RestrictedSetupTool; toolCallId: string },
    signal: AbortSignal, caller: RestrictedSetupCallerContext): Promise<RestrictedSetupResolution>;
}

export const restrictedSetupInstructions = `You are Dexter, helping the caller set up phone access. Speak naturally, listen to interruptions, and keep explanations brief.
You have setup assistance only. Help the person understand account approval, phone verification and an optional callback when the setup host offers it. Their signed-in Dexter account supplies account approval. The setup host supplies the verified call destination.
Use the named setup functions for an explicit setup request, a change of mind, or a setup-status question. Ask a brief clarifying question when their request is unclear. Ordinary greetings and acknowledgments stay conversational.
Use only the setup outcomes returned by that host. A requested call is still pending; describe it as requested until a later outcome confirms more. Explain an unavailable option plainly and keep helping with the available setup path.
Private account information and account actions become available only after separate account admission. During setup, explain the next step without claiming to read balances, holdings, history, permissions or saved conversations. A person's spoken account facts remain their statements.
Ask the caller to enter a verification code only through the separate secure capture step when the host opens it. Never ask them to say passwords, seed phrases, passkey secrets or verification codes aloud. A spoken yes does not itself prove account ownership or phone possession.
Use the returned setup state to answer directly. Keep internal identifiers, attestation labels and implementation details out of speech.`;

const spoken: Record<RestrictedSetupPhase, string> = {
  account_authentication_required: 'I can help you set up phone access. Start with approval from your signed-in Dexter account, then we can arrange the verification call.',
  verification_available: 'I am ready to verify your phone. I will guide you through the next step.',
  standing_callback_available: 'Would you like me to call your verified number so we can continue there?',
  checking: 'I am checking the saved setup request.',
  verification_requested: 'The verification call has been requested. Answer that call to continue setup.',
  callback_requested: 'The callback has been requested. We can continue on that call.',
  complete: 'Phone setup is complete.',
  declined: 'Okay. We can leave phone setup for another time.',
  unavailable: 'I could not complete that setup step. We can check the available setup options.',
};
const safeId = (value: unknown): value is string => typeof value === 'string'
  && /^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$/.test(value);
function exactObservation(value: RestrictedSetupObservation, scope: RestrictedSetupScope): boolean {
  return value !== null && typeof value === 'object' && value.callId === scope.callId
    && value.callSid === scope.callSid && value.sessionId === scope.sessionId
    && Object.hasOwn(spoken, value.phase);
}

export interface RestrictedSetupVoiceOptions {
  apiKey: string;
  /** Explicit Responses model selection, supplied by the host at composition. */
  setupModel: string;
  callId: string;
  callSid: string;
  voice?: string;
  /** The host's existing absolute deadline; this adapter never renews it. */
  deadlineAtMs: number;
  hostTimeoutMs: number;
  host: RestrictedSetupHost;
  voiceFactory?: (options: BrokerLiveSessionOptions) => BrokerLiveSession;
  now?: () => number;
  onAudio: BrokerLiveSessionOptions['onAudio'];
  onPlaybackClear?: BrokerLiveSessionOptions['onPlaybackClear'];
  onReady?(scope: RestrictedSetupScope): void;
  /** Host stops provider/media independently of a hung host request or Live close. */
  onStopRequested(reason: 'deadline' | 'host_closed' | 'identity_changed'): void;
  /** Sensitive capture closes only this Live generation; the host owns the leg.
   * Never send this restricted-leg observation to caller-number billing inference. */
  onClosed(result: LiveSessionClosed, intent: 'stop' | 'sensitive_input' | 'provider_close'): void;
}

export interface RestrictedSetupVoice {
  appendAudio(base64: string): boolean;
  refresh(): Promise<void>;
  /** Synchronously gates input/output, then retires this generation. The host may
   * collect a signed sensitive form on its separate transport. No resume method. */
  suspendForSensitiveInput(): Promise<LiveSessionClosed>;
  close(): Promise<LiveSessionClosed>;
}

/** Reuses Live audio/interruption with empty history and a finite setup-only port.
 * No Primary client, account history, assignment, model tool result or transcript
 * persistence hook can be supplied through this construction interface. */
export function createRestrictedSetupVoice(options: RestrictedSetupVoiceOptions): RestrictedSetupVoice {
  const now = options.now ?? Date.now;
  if (!safeId(options.setupModel) || !safeId(options.callId) || !/^CA[0-9a-f]{32}$/i.test(options.callSid)
    || !Number.isSafeInteger(options.deadlineAtMs) || options.deadlineAtMs <= now()
    || !Number.isSafeInteger(options.hostTimeoutMs) || options.hostTimeoutMs < 1 || options.hostTimeoutMs > 30_000)
    throw new Error('invalid_restricted_setup_configuration');
  const lifetime = new AbortController();
  let sessionId: string | undefined;
  let blocked = false;
  let intent: 'stop' | 'sensitive_input' | 'provider_close' = 'provider_close';
  let closing: Promise<LiveSessionClosed> | undefined;
  let observing: Promise<void> | undefined;
  let voice: BrokerLiveSession;
  const requests = new Map<string, { digest: string; promise: Promise<RestrictedSetupToolResult> }>();
  const scope = (): RestrictedSetupScope | undefined => sessionId
    ? { callId: options.callId, callSid: options.callSid, sessionId } : undefined;
  const current = (value: RestrictedSetupScope): boolean => !blocked && !lifetime.signal.aborted
    && now() < options.deadlineAtMs && value.sessionId === sessionId && options.host.isCurrent(value);
  const retire = (next: 'stop' | 'sensitive_input', reason: string) => {
    if (closing) return closing;
    blocked = true; intent = next; lifetime.abort(); clearTimeout(deadline);
    closing = voice.close(reason);
    return closing;
  };
  const stop = (reason: 'deadline' | 'host_closed' | 'identity_changed') => {
    if (blocked) return;
    // The provider stop signal precedes asynchronous Live/host cleanup.
    blocked = true;
    try { options.onStopRequested(reason); } finally {
      void retire('stop', `restricted_setup_${reason}`).catch(() => {});
    }
  };
  async function hostCall<T>(work: (signal: AbortSignal) => Promise<T>): Promise<T> {
    const controller = new AbortController();
    const abort = () => controller.abort();
    lifetime.signal.addEventListener('abort', abort, { once: true });
    if (lifetime.signal.aborted) abort();
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([Promise.resolve().then(() => {
        if (controller.signal.aborted) throw new Error('restricted_setup_stopped');
        return work(controller.signal);
      }), new Promise<never>((_resolve, reject) => {
        const rejectAbort = () => reject(new Error('restricted_setup_stopped'));
        controller.signal.addEventListener('abort', rejectAbort, { once: true });
        if (controller.signal.aborted) rejectAbort();
        timer = setTimeout(() => { controller.abort(); reject(new Error('restricted_setup_host_timeout')); },
          Math.max(1, Math.min(options.hostTimeoutMs, options.deadlineAtMs - now())));
      })]);
    } finally {
      clearTimeout(timer); lifetime.signal.removeEventListener('abort', abort);
    }
  }
  const refresh = (): Promise<void> => {
    if (observing) return observing;
    const s = scope();
    if (!s || !current(s)) return Promise.resolve();
    const run = (async () => {
      let phase: RestrictedSetupPhase;
      try {
        const observed = await hostCall(signal => options.host.observe(s, signal));
        if (!exactObservation(observed, s)) throw new Error('restricted_setup_identity_changed');
        phase = observed.phase;
      } catch { phase = 'unavailable'; }
      if (current(s)) await voice.appendCommentary?.(spoken[phase]);
    })();
    observing = run;
    void run.finally(() => { if (observing === run) observing = undefined; }).catch(() => {});
    return run;
  };
  const execute: RestrictedSetupTools['execute'] = async (input, channel) => {
    const s = scope(), request = input.request;
    if (!s || !current(s) || channel.signal.aborted || request.callId !== s.callId
      || request.sessionId !== s.sessionId || !safeId(request.requestId) || !safeId(request.delegationId)
      || !safeId(input.toolCallId)) return { phase: 'unavailable' };
    const tool = restrictedSetupToolSchema.parse(input.tool), frozen = structuredClone(request);
    const digest = createHash('sha256').update(JSON.stringify([frozen, tool])).digest('hex');
    const prior = requests.get(input.toolCallId);
    if (prior) {
      if (prior.digest !== digest) throw new Error('restricted_setup_request_changed');
      return prior.promise;
    }
    // Exact function identity is retained before any await. Metadata or an opaque
    // delegation cannot select this effect port. The host owns durable recovery.
    const promise = Promise.resolve().then(async (): Promise<RestrictedSetupToolResult> => {
      if (!current(s) || channel.signal.aborted) return { phase: 'unavailable' };
      try {
        const result = await hostCall(signal => options.host.handleCaller({ ...s, request: frozen,
          tool, toolCallId: input.toolCallId }, signal, {
          getContext: () => channel.getContext(), subscribeContext: listener => channel.subscribeContext(listener),
        }));
        if (!exactObservation(result, s) || result.requestId !== frozen.requestId || !safeId(result.publicationId))
          throw new Error('restricted_setup_identity_changed');
        return current(s) && !channel.signal.aborted ? { phase: result.phase } : { phase: 'unavailable' };
      } catch {
        // A lost response cannot establish success or start a second operation.
        return { phase: 'checking' };
      }
    });
    requests.set(input.toolCallId, { digest, promise }); return promise;
  };
  const deadline = setTimeout(() => stop('deadline'), options.deadlineAtMs - now());
  deadline.unref?.();
  try {
    voice = (options.voiceFactory ?? createBrokerLiveSession)({ apiKey: options.apiKey,
      callId: options.callId, voice: options.voice, instructions: restrictedSetupInstructions,
      history: [], restrictedSetupTools: { model: options.setupModel, execute },
      backend: { async delegate() { throw new Error('restricted_setup_named_tool_required'); } },
      onAudio(payload, metadata) { const s = scope(); if (s && current(s)) options.onAudio(payload, metadata); },
      // Sensitive input is collected outside this adapter. Setup conversation
      // fragments remain local to Live and are never canonical account memory.
      onTranscript() {},
      onPlaybackClear: options.onPlaybackClear,
      onReady(metadata) {
        if (blocked) return;
        if (metadata.callId !== options.callId || !safeId(metadata.sessionId)
          || (sessionId && sessionId !== metadata.sessionId)) { stop('identity_changed'); return; }
        const first = sessionId === undefined; sessionId = metadata.sessionId;
        const s = scope(); if (!s || !current(s)) { stop('host_closed'); return; }
        if (first) { options.onReady?.(s); queueMicrotask(() => { void refresh().catch(() => {}); }); }
      },
      onClosed(result) {
        blocked = true; lifetime.abort(); clearTimeout(deadline);
        options.onClosed(result, intent);
      },
    });
  } catch (error) { clearTimeout(deadline); lifetime.abort(); throw error; }
  return {
    appendAudio(payload) {
      const s = scope();
      if (blocked || now() >= options.deadlineAtMs) return false;
      if (s && !current(s)) { stop('host_closed'); return false; }
      return voice.appendAudio(payload);
    },
    refresh,
    suspendForSensitiveInput: () => retire('sensitive_input', 'restricted_setup_sensitive_input'),
    close: () => retire('stop', 'restricted_setup_closed'),
  };
}
