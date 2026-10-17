import type { BrokerSmsBridge } from './sms.js';
import { createResultSmsClient, type ResultSmsClient } from './resultSmsClient.js';
import { dispatchResultSms, resultSmsFailure } from './resultSmsDispatch.js';
import { createInboundBillingLifecycle } from './inboundBillingLifecycle.js';
import { inboundBillingSummary } from './inboundBilling.js';
import { createRestrictedSetupServer, type RestrictedSetupServerOptions } from './restrictedSetupServer.js';
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { createHash } from 'node:crypto';
import { WebSocketServer, WebSocket } from 'ws';
import Twilio from 'twilio';
import { createBrokerLiveSession, type BrokerLiveSession, type BrokerLiveSessionOptions } from '../live/session.js';
import type { LiveTranscriptSnapshot } from '../live/session.js';
import { createPrimaryApiClient, type PrimaryApiClient } from './primaryApi.js';
import { createPrimaryBackend, type BrokerDelegationRecord } from './backend.js';
import { eligiblePhoneCallSchema, type EligibleContextClient } from './eligibleContext.js';
import { createGenerationVoice, type GenerationVoiceOptions, type GenerationVoiceSession } from './generationVoice.js';
import { carrierPlaybackState } from './voicePresentation.js';
import { createInboundStartupContexts } from './startupContext.js';
import { createInboundPreconnect } from './inboundPreconnect.js';
import { createNotificationAssignmentResolver, NotificationAssignmentError, type NotificationAssignmentResolver } from './notificationResolver.js';
import { createBrokerDependencyHealth, type BrokerDependencyHealth } from './dependencyHealth.js';
import { createSelectedQuestionRecovery, selectedQuestionSourceAnchors } from './selectedQuestionVoice.js';
import { recoverCallerRouteCaptures } from './mixedWork.js';
import { createCallerIntentResolver } from './callerIntentResolver.js';
import type { CallerIntentClient } from './callerIntentClient.js';
import { callerRouteRef, callerRouteSources } from './callerRouting.js';
import { outboundAcceptedIdentitySchema, outboundSettlementFromObservation,
  type OutboundAcceptedIdentity } from './outboundObservation.js';
import { outboundRetryOfSchema, outboundRetryOfPatch, OutboundRetryError,
  type OutboundRetryOf } from './outboundSettlement.js';
import { newPhoneCallControlSchema } from './assignment.js';
import { assertCallContinuationReplay, callContinuationPatch, CallContinuationError, type CallContinuation } from './callContinuation.js';
import { callRequestIdFromLookupPath, callRequestReceiptFailure, readCallRequestReceipt } from './callRequestReceipt.js';
import { authorizedHarkIngress } from './harkAuth.js';
import { OutboundAlertRequestSchema } from '../outbound/contract.js';
import { canonicalJson, sha256Hex } from '../outbound/canonical.js';
import { createHarkAssignmentResolver, HarkAssignmentError, type HarkAssignmentResolver } from './harkResolver.js';
import { createHarkLifecycleDelivery } from './harkLifecycle.js';
import { createPrimaryHarkReceiptDispatcher, createPrimaryHarkReceiptSender,
  type PrimaryHarkReceiptSender } from './harkPrimaryReceiptDelivery.js';
import { type HarkEndedTiming, type HarkProviderStatus } from './harkJournal.js';
import { harkProviderTiming } from './harkTiming.js';
import type { deliverHarkStatus } from './harkStatusDelivery.js';
import type { deliverHarkBilling } from './harkBillingDelivery.js';
import { rejectBrokerPublication } from './publications.js';
import { createCallbackFlow } from './callback.js';
import { createInboundFlow } from './inbound.js';
import { logInboundTiming, observeInboundStreamResponse, observeInboundHoldResponse } from './inboundTiming.js';
import { type BrokerConfig, isPhoneNumber } from './config.js';
import { BrokerStore, type BrokerCall, type CallStatus, terminalCallStatuses } from './store.js';
import { brokerInstructions, brokerHistory, brokerOpening } from './instructions.js';
import { greetingClipEligible, playGreetingClip, type GreetingClipPlayback } from './greetingClip.js';
import { probeLive } from './preflight.js';
import { authorizedControl, callTwiml, fallbackTwiml, inputSpeechEnergy, mediaStart, PlaybackReceipts, hasAudiblePcmu, carriesSpeechPcmu, safeFailure, streamToken, verifyTwilio, type ProviderCallProof } from './phone.js';

/** A pre-speech backlog shorter than this is ordinary network jitter, not a delay worth clearing. */
const PRE_SPEECH_CLEAR_MIN_MS = 300;
/** Re-queued after a pre-speech clear: the last 200 ms of PCMU, which may hold the speech onset. */
const PRE_SPEECH_ONSET_BYTES = 1_600;
const PRE_SPEECH_TAIL_LIMIT = 400;
/** Live output held behind the greeting clip is bounded like the carrier backlog (6 s). */
const GREETING_HOLD_MAX_BYTES = 48_000;
/** Live output is released, and caller input opened, this long after the clip's own duration even without carrier marks. */
const GREETING_ACK_GRACE_MS = 1_500;
/** Caller audio kept between the greeting's end and Live attaching: the newest 15 s. Typical admission
 * keeps 0-2 s here; Live startup then buffers up to its own 15 s inside the generation controller's ~35 s. */
const HELD_INPUT_MAX_BYTES = 120_000;
/** Caller frame energy above this is speech (the threshold every caller-speech observation uses). */
const CALLER_SPEECH_ENERGY = 0.012;
/** Admission poll interval while a greeting stream waits for inbound approval: the pace of the
 * Pause-and-redirect hold loop it replaces, so the API sees the same status and begin rate. */
const INBOUND_ADMISSION_POLL_MS = 1_000;
/** Spoken before the provider hangup for stops the caller should hear explained. */
const SPOKEN_STOPS: Record<string, string> = {
  voice_unavailable: 'My voice connection stopped working. Please try me again shortly.',
  primary_unavailable: 'I could not reach your Dexter account just now. Please try again shortly.',
};

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const CALL_SID = /^CA[0-9a-f]{32}$/i;
const providerStates = new Set<CallStatus>(['queued', 'ringing', 'in-progress', 'completed', 'busy', 'no-answer', 'canceled', 'failed']);
function selectedTransportFields(call: BrokerCall) {
  const settlement = outboundSettlementFromObservation(call);
  return { ...outboundRetryOfPatch(call.retryOf),
    ...(settlement === undefined ? {} : { outboundSettlement: settlement }) };
}

function json(response: ServerResponse, status: number, value: unknown) {
  response.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' });
  response.end(JSON.stringify(value));
}
function xml(response: ServerResponse, value: string) {
  response.writeHead(200, { 'content-type': 'text/xml', 'cache-control': 'no-store' }); response.end(value);
}
async function body(request: IncomingMessage, limit = 64_000): Promise<string> {
  const chunks: Buffer[] = []; let size = 0;
  for await (const chunk of request) { size += chunk.length; if (size > limit) throw new Error('request_too_large'); chunks.push(Buffer.from(chunk)); }
  return Buffer.concat(chunks).toString('utf8');
}
const publicCall = (call: BrokerCall) => ({
  id: call.id, requestId: call.requestId, direction: call.direction, purpose: call.purpose,
  recipientLast4: call.number.slice(-4), callSid: call.callSid, status: call.status,
  createdAt: call.createdAt, updatedAt: call.updatedAt, endedAt: call.endedAt,
  primaryConnected: Boolean(call.primary), finalized: call.finalized, failure: call.failure,
  voiceOpening: call.voiceOpening, inboundBilling: inboundBillingSummary(call),
  primaryClosePending: call.primaryClosePending ?? false, transcriptCapture: call.transcriptCapture,
  callback: call.callback,
  ...callContinuationPatch(call.continuation),
  ...selectedTransportFields(call),
  metrics: call.metrics, journey: call.journey, delegationCount: Object.keys(call.delegations).length,
  ...(call.callAssignment?.origin === 'authenticated_notification_delivery' ? { notificationDelivery: {
    intentId: call.callAssignment.value.notification.intentId,
    sourceKind: call.callAssignment.value.notification.source.kind,
    pendingCallbacks: Object.values(call.harkJournal?.events ?? {}).filter(event => event.state === 'pending').length,
    rejectedCallbacks: Object.values(call.harkJournal?.events ?? {}).filter(event => event.state === 'rejected').length,
    awaitingCallTiming: call.harkJournal?.ended?.providerStatus === 'completed' && !call.harkJournal.ended.timing,
    callReceiptRecorded: Object.values(call.harkJournal?.events ?? {}).some(event => event.kind === 'call_ended' && event.state === 'delivered'),
  } } : {}),
  ...(call.callAssignment?.origin === 'authenticated_hark_delivery' ? { watchDelivery: {
    deliveryId: call.callAssignment.value.hark.deliveryId,
    pendingCallbacks: Object.values(call.harkJournal?.events ?? {}).filter(event => event.state === 'pending').length,
    rejectedCallbacks: Object.values(call.harkJournal?.events ?? {}).filter(event => event.state === 'rejected').length,
    awaitingCallTiming: Boolean(call.harkJournal?.ended && !call.harkJournal.ended.timing),
    callReceiptRecorded: Object.values(call.harkJournal?.events ?? {}).some(event => event.kind === 'call_ended' && event.state === 'delivered'),
  } } : {}),
});

export interface BrokerServerOptions {
  /** Ordinary inbound transport only; production supplies the same Live connector as the session adapter. */
  inboundPreconnect?: () => import('../live/types.js').BrokerLiveConnection;
  sms?: BrokerSmsBridge;
  resultSms?: ResultSmsClient;
  config: BrokerConfig;
  store?: BrokerStore;
  primary?: PrimaryApiClient;
  /** Coordinated successor composition. Omission retains the legacy transport contract. */
  eligibleContext?: EligibleContextClient;
  /** Explicit successor composition; ordinary and Hark calls omit this selection. */
  selectedQuestion?: GenerationVoiceOptions['selectedQuestion'];
  /** Same-call new work uses the shared Primary decision port and original call budget. */
  callerIntent?: { client: CallerIntentClient; timeoutMs: number };
  provider?: ReturnType<typeof Twilio>;
  voiceFactory?: (options: BrokerLiveSessionOptions) => BrokerLiveSession;
  probe?: typeof probeLive;
  dependencyHealth?: () => Promise<BrokerDependencyHealth>;
  resolveHarkAssignment?: HarkAssignmentResolver;
  resolveNotificationAssignment?: NotificationAssignmentResolver;
  harkStatusSender?: typeof deliverHarkStatus;
  harkBillingSender?: typeof deliverHarkBilling;
  inboundBillingSender?: typeof deliverHarkBilling;
  harkPrimaryReceiptSender?: PrimaryHarkReceiptSender;
  /** Explicit composition point for the chosen owner approval surface. Omitted in current production startup. */
  /** Main supplies this only for an explicitly configured verification model. */
  restrictedSetup?: Pick<RestrictedSetupServerOptions, 'model' | 'api' | 'receiptTransport'>;
  inboundAttachment?: {
    waitingResponse(input: { callId: string; continuationUrl: string; attachmentId?: string; expiresAt?: string }): string;
  };
  /** Pre-rendered "Hey, it's Dexter." (8 kHz PCMU) for outbound calls, streamed as soon as the
   * media stream starts (while Primary binds) ahead of Live's opening offer. Omission retains
   * the Live-spoken greeting. */
  greetingClip?: Buffer;
  /** Pre-rendered "Hey, it's Dexter. What can I do for you?" for inbound calls: the whole
   * opening; Live then waits for the caller. Omission retains the Live-spoken greeting. */
  inboundGreetingClip?: Buffer;
  /** Admission poll interval for a greeting stream waiting on inbound approval (default 1 s). */
  inboundAdmissionPollMs?: number;
  /** Age up to which a binding response seeds Live without a second eligibility read
   * (default STARTUP_PAGE_FRESH_MS); 0 rechecks every page first. */
  startupPageFreshMs?: number;
}

export function createBrokerServer(options: BrokerServerOptions) {
  const { config } = options;
  const store = options.store ?? new BrokerStore(config.stateDir);
  const primary = options.primary ?? createPrimaryApiClient({ apiBaseUrl: config.apiBaseUrl, phoneServiceSecret: config.phoneServiceSecret,
    ...(options.eligibleContext ? { contextProtocol: { version: 'memory-eligible-v1' as const,
      parse: (value: unknown) => eligiblePhoneCallSchema.parse(value) } } : {}) });
  const resultSms = options.resultSms ?? createResultSmsClient({ apiBaseUrl: config.apiBaseUrl,
    phoneServiceSecret: config.phoneServiceSecret });
  const provider = options.provider ?? Twilio(config.twilioAccountSid, config.twilioAuthToken, { autoRetry: false, timeout: 15_000 });
  const voiceFactory = options.voiceFactory ?? createBrokerLiveSession;
  const probe = options.probe ?? probeLive;
  const dependencyHealth = options.dependencyHealth ?? createBrokerDependencyHealth({
    apiBaseUrl: config.apiBaseUrl, harkApiBaseUrl: config.harkApiBaseUrl,
  });
  const preconnect = options.eligibleContext && options.inboundPreconnect ? createInboundPreconnect(options.inboundPreconnect,
    event => console.info(JSON.stringify(event))) : undefined;
  const startupContexts = options.eligibleContext ? createInboundStartupContexts(options.eligibleContext,
    options.startupPageFreshMs === undefined ? {} : { freshMs: options.startupPageFreshMs }) : undefined;
  const resolveHarkAssignment = options.resolveHarkAssignment ?? (config.harkApiBaseUrl
    ? createHarkAssignmentResolver({ apiBaseUrl: config.harkApiBaseUrl }) : undefined);
  const resolveNotificationAssignment = options.resolveNotificationAssignment ?? (config.harkApiBaseUrl
    ? createNotificationAssignmentResolver({ apiBaseUrl: config.harkApiBaseUrl }) : undefined);
  const managedNotification = (call: BrokerCall) => call.callAssignment?.origin === 'authenticated_hark_delivery'
    || call.callAssignment?.origin === 'authenticated_notification_delivery';
  const notificationWindow = (call: BrokerCall) => {
    if (call.callAssignment?.origin === 'authenticated_hark_delivery') return { expiresAt: call.callAssignment.value.hark.expiresAt };
    if (call.callAssignment?.origin === 'authenticated_notification_delivery') return call.callAssignment.value.notification;
    return undefined;
  };
  const notificationIsDue = (call: BrokerCall) => {
    const window = notificationWindow(call);
    return !window || (Date.parse(window.expiresAt) > Date.now()
      && (!('deliveryNotBefore' in window) || Date.parse(window.deliveryNotBefore) <= Date.now()));
  };
  const harkLifecycle = config.harkApiBaseUrl && config.harkCallbackSigningSecret
    ? createHarkLifecycleDelivery({ store, apiBaseUrl: config.harkApiBaseUrl, signingSecret: config.harkCallbackSigningSecret,
      statusSender: options.harkStatusSender, billingSender: options.harkBillingSender }) : undefined;
  const harkReceipts = createPrimaryHarkReceiptDispatcher({ store,
    send: options.harkPrimaryReceiptSender ?? createPrimaryHarkReceiptSender({
      apiBaseUrl: config.apiBaseUrl, phoneServiceSecret: config.phoneServiceSecret }) });
  const inboundBilling = createInboundBillingLifecycle({ store, fetchCall: sid => provider.calls(sid).fetch(),
    apiBaseUrl: config.verificationSetup?.apiBaseUrl ?? config.harkApiBaseUrl,
    signingSecret: config.verificationSetup?.signingSecret ?? config.harkCallbackSigningSecret, send: options.inboundBillingSender });
  const active = new Map<string, { stop(reason: string): Promise<void> }>();
  const dialing = new Set<string>();
  const harkDispatching = new Set<string>();
  const claimed = new Set<string>();
  const pendingRequests = new Set<Promise<void>>();
  const sessionCalls = new Set<string>();
  const closingBindings = new Map<string, Promise<void>>();
  // Outbound Primary bindings started by the answer route; the media stream attaches Live when one settles.
  const pendingBinds = new Map<string, Promise<BrokerCall>>();
  // Each background binding until it settles and, when nothing will attach it, its close finishes.
  // Shutdown waits for these: a binding that lands after the store closes is named nowhere and stays open.
  const bindSettlements = new Set<Promise<void>>();
  // Keep a stopped host until its already-observed source preparation settles.
  // A restart recovers only the durable captures and reply-read obligations.
  const selectedHosts = new Map<string, GenerationVoiceSession>();
  let accepting = true;
  const providerFinished = (call: BrokerCall) => call.verificationCallback ? Boolean(call.verificationCallback.providerTerminal) : call.outboundObservation
    ? outboundSettlementFromObservation(call)?.state === 'settled'
    : call.callSid && managedNotification(call)
      ? Boolean(call.harkJournal?.ended) : terminalCallStatuses.has(call.status);
  const needsHarkTiming = (call: BrokerCall) => call.harkJournal?.ended?.providerStatus === 'completed' && !call.harkJournal.ended.timing;
  const flushHark = async () => {
    try { await harkLifecycle?.flush(); }
    catch (error) { console.error('[dexter-broker] Hark callback journal unavailable', { code: safeFailure(error).code }); }
    try { await harkReceipts.flush(); }
    catch (error) { console.error('[dexter-broker] Hark receipt journal unavailable', { code: safeFailure(error).code }); }
  };
  function observeProviderState(id: string, status: HarkProviderStatus, timing?: HarkEndedTiming): BrokerCall {
    const call = store.get(id)!;
    const observedAt = new Date().toISOString();
    if (call.inboundBilling) return store.observeInboundProvider(id, status, observedAt);
    if (!managedNotification(call))
      return store.update(id, { status, ...(terminalCallStatuses.has(status) ? { endedAt: observedAt } : {}) });
    return store.observeHarkProvider(id, status, observedAt, timing);
  }
  function selectedProviderObservation(call: BrokerCall, identity: OutboundAcceptedIdentity,
    terminal?: { status: string; source: 'signed_status_callback' | 'authenticated_fetch' | 'authenticated_update'; evidenceDigest: string }): BrokerCall {
    if (!call.outboundObservation) return call;
    const checked = outboundAcceptedIdentitySchema.parse(identity);
    const base = { callId: call.id, requestId: call.requestId, observedAt: new Date().toISOString(), identity: checked };
    if (terminal && terminalCallStatuses.has(terminal.status as CallStatus)) {
      const providerStatus = terminal.status as 'completed' | 'busy' | 'no-answer' | 'canceled' | 'failed';
      return store.observeSelectedOutbound(call.id, { ...base, type: 'provider_terminal', providerStatus,
        source: terminal.source, evidenceDigest: terminal.evidenceDigest });
    }
    return store.observeSelectedOutbound(call.id, { ...base, type: 'provider_accepted' });
  }
  function observeProviderRecord(call: BrokerCall, observed: { status: string; sid?: string; accountSid?: string; from?: string; to?: string },
    source: 'authenticated_fetch' | 'authenticated_update'): BrokerCall {
    if (!providerStates.has(observed.status as CallStatus)) throw new Error('invalid_provider_call_state');
    const hark = managedNotification(call);
    if (hark && observed.sid !== undefined && observed.sid !== call.callSid) throw new Error('provider_call_identity_mismatch');
    const bound = observed.sid === call.callSid && observed.accountSid === config.twilioAccountSid
      && observed.from === config.from && observed.to === call.number;
    if (call.outboundObservation) {
      const identity = outboundAcceptedIdentitySchema.parse({ callSid: observed.sid, accountSid: observed.accountSid,
        from: observed.from, to: observed.to });
      const evidenceDigest = sha256Hex(JSON.stringify(['broker.provider-observation/v1', source,
        call.id, call.requestId, identity.accountSid, identity.callSid, identity.from, identity.to, observed.status]));
      selectedProviderObservation(call, identity, { status: observed.status, source, evidenceDigest });
    }
    // Callback arrival and restart time describe an observation, not call duration.
    const timing = hark && bound ? harkProviderTiming(observed) : undefined;
    return observeProviderState(call.id, observed.status as HarkProviderStatus, timing);
  }
  const callbacks = createCallbackFlow({ config, store,
    fetchCall: sid => provider.calls(sid).fetch(),
    requestOutbound: input => requestOutbound(input).then(result => result.call),
  });

  const inbound = createInboundFlow({ config, store, primary,
    onTiming: logInboundTiming,
    onApprovedTransport: call => preconnect?.begin(call),
    onTransportDiscard: (id, terminal) => terminal ? preconnect?.forget(id) : preconnect?.discard(id),
    onContext: (call, context) => startupContexts?.remember(call, context),
    fetchCall: sid => provider.calls(sid).fetch(), closeBound: async call => {
      // Media may have started while a provider/account read was in flight.
      // Its owner must finalize Live and capture before closing account admission.
      const media = active.get(call.id);
      if (media) await media.stop(terminalCallStatuses.has(call.status) ? 'provider_terminal' : call.inbound?.localStop?.reason ?? 'attachment_stopped');
      else await closePrimary(store.get(call.id)!);
    } });

  const restricted = options.restrictedSetup ? createRestrictedSetupServer({ ...options.restrictedSetup, config, store, primary,
    provider, voiceFactory, active, claimed, accepting: () => accepting, closePrimary }) : undefined;

  function closePrimary(call: BrokerCall): Promise<void> {
    startupContexts?.discard(call.id);
    preconnect?.forget(call.id);
    const inboundBound = call.inbound?.remote?.state === 'bound' ? call.inbound.remote : undefined;
    const bindingId = call.primary?.bindingId ?? inboundBound?.bindingId;
    if (!bindingId) return Promise.resolve();
    const existing = closingBindings.get(call.id);
    if (existing) return existing;
    store.update(call.id, { primaryClosePending: true });
    const pending = (async () => {
      // Closing admission first would make an unacknowledged, never-accepted turn
      // impossible to recover. Reconcile the exact journal before closing it.
      const current = store.get(call.id)!;
      const recoveries: ReturnType<typeof createPrimaryBackend>[] = [];
      if (current.continuation) {
        const host = selectedHosts.get(current.id);
        if (host) {
          if (!(await host.prepareSelectedCloseout()).ready) return;
        } else if (current.selectedQuestion || current.selectedQuestionVoice) {
          if (!options.selectedQuestion) throw new Error('selected_question_recovery_unavailable');
          const recovery = createSelectedQuestionRecovery({ callId: current.id, store, primary,
            requests: options.selectedQuestion.requests });
          if (!(await recovery.prepareCloseout()).ready) return;
        }
      }
      if (current.callerRouting && await recoverCallerRouteCaptures({ callId: current.id, store, primary })) return;
      if (current.callSid && current.liveSessionId) {
        const segments = current.liveGenerations?.segments.filter(segment => segment.sessionId);
        const sources = segments?.length ? segments.map(segment => ({ sessionId: segment.sessionId!,
          transcript: segment.transcript, proof: { sessionFinalized: segment.providerFinalized === true && segment.transcriptPersisted === true,
            // Rotation clears queued output. A socket close never proves a whole assistant turn was heard.
            assistantPlaybackComplete: false } }))
          : [{ sessionId: current.liveSessionId, transcript: current.transcript as LiveTranscriptSnapshot | undefined,
            proof: current.transcriptCapture }];
        let pendingCount = 0;
        const unsupported: string[] = [];
        for (const source of sources) {
          const recovery = createPrimaryBackend({ callId: current.id, callSid: current.callSid,
            bindingId, api: primary, store, ...(segments ? { sourceSessionId: source.sessionId } : {}),
            callerRouteSources: () => callerRouteSources(store.get(current.id)!),
            callerRouteCaptureAccepted(sourceId, receipt) {
              const record = store.get(current.id)?.callerRouting?.records.find(value => value.frozen.turn.sourceEventId === sourceId);
              if (record) store.captureCallerRoute(current.id, callerRouteRef(record), receipt);
            },
            ...(current.continuation ? { selectedQuestionSources: () => selectedQuestionSourceAnchors(store.get(current.id)!) } : {}) });
          recoveries.push(recovery);
          if (!current.primaryClosed) await recovery.prepareCloseout();
          const flushed = await recovery.flushPendingCaptures();
          let sourcePending = flushed.pending;
          unsupported.push(...flushed.unsupported);
          if (source.transcript && source.proof) {
            const captured = await recovery.captureCompletedTurns(source.transcript, source.proof);
            sourcePending = captured.pending;
            unsupported.push(...captured.unsupported);
          }
          pendingCount += sourcePending;
        }
        if (current.transcriptCapture || segments) store.update(call.id, { transcriptCapture: {
          state: pendingCount ? 'pending' : 'complete', pending: pendingCount, unsupported: [...new Set(unsupported)],
          assistantPlaybackComplete: segments ? false : current.transcriptCapture?.assistantPlaybackComplete ?? false,
          sessionFinalized: segments ? segments.every(segment => segment.providerFinalized === true)
            : current.transcriptCapture?.sessionFinalized ?? false,
        } });
        if (recoveries.some(recovery => recovery.closeoutPending())) return;
      }
      let closed = store.get(call.id)!.primaryClosed;
      if (!closed) {
        if (inboundBound) {
          if (!primary.closeInbound) throw new Error('inbound_api_unavailable');
          store.transitionInbound(call.id, { type: 'close_requested', observedAt: new Date().toISOString() });
          const receipt = await primary.closeInbound(inboundBound.attachmentId, {
            begin: call.inbound!.begin, approvalRef: inboundBound.approvalRef, bindingId,
          });
          closed = store.transitionInbound(call.id, { type: 'closure', receipt }).primaryClosed!;
        } else {
          closed = await primary.close(bindingId);
          if (closed.bindingId !== bindingId || closed.callSid !== call.callSid || closed.state !== 'closed')
            throw new Error('closed_call_identity_mismatch');
          store.update(call.id, { primaryClosed: closed });
        }
      }
      for (const recovery of recoveries) if ((await recovery.reconcileClosedRequests(closed)).pending) return;
      const captured = store.get(call.id)!.transcriptCapture;
      if (captured) store.update(call.id, { transcriptCapture: { ...captured, state: 'complete', pending: 0,
        unsupported: captured.unsupported.filter(reason => reason !== 'source_admission_unknown') } });
      store.update(call.id, { primaryClosePending: false });
      selectedHosts.delete(call.id);
    })().catch(error => { store.update(call.id, { failure: safeFailure(error) }); })
      .finally(() => closingBindings.delete(call.id));
    closingBindings.set(call.id, pending);
    return pending;
  }

  async function requestOutbound(input: { requestId: string; purpose: string; to: string; callbackFor?: string;
    origin?: 'authenticated_broker_control'; continuation?: CallContinuation; retryOf?: OutboundRetryOf }): Promise<{ replayed: boolean; call: BrokerCall }> {
    const to = input.to;
    if (!accepting || !UUID.test(input.requestId) || !config.allowedTo.includes(to)) throw new Error('call_not_available');
    if (input.callbackFor) {
      const inbound = store.get(input.callbackFor);
      if (!inbound || inbound.direction !== 'inbound' || inbound.number !== to || !terminalCallStatuses.has(inbound.status)
        || inbound.callback?.requestId !== input.requestId || inbound.callback.state !== 'requested') throw new Error('callback_identity_mismatch');
    }
    const reservation = input.continuation ? store.reserveSelectedOutbound({ requestId: input.requestId,
      purpose: input.purpose.trim(), number: to, continuation: input.continuation, ...outboundRetryOfPatch(input.retryOf) },
      { accountSid: config.twilioAccountSid, from: config.from, to })
      : store.reserve({ requestId: input.requestId, direction: 'outbound', purpose: input.purpose.trim(), number: to }, input.origin);
    return dialReserved(reservation);
  }

  async function dialReserved(reservation: { call: BrokerCall; existing: boolean }): Promise<{ replayed: boolean; call: BrokerCall }> {
    const call = store.get(reservation.call.id)!;
    const hark = managedNotification(call);
    if (hark) {
      if (call.harkDispatch?.state === 'provider_accepted' && call.callSid)
        return { replayed: true, call };
      if (!call.harkDispatch || call.harkDispatch.state !== 'not_dispatched')
        throw new HarkAssignmentError(409, 'hark_dispatch_uncertain');
      if (harkDispatching.has(call.id)) throw new HarkAssignmentError(409, 'hark_dispatch_pending');
    } else if (reservation.existing) return { replayed: true, call };

    const to = call.number;
    sessionCalls.add(call.id);
    if (dialing.has(to) || store.list().some(other => other.id !== call.id && other.number === to && !providerFinished(other))) {
      if (call.outboundObservation) store.observeSelectedOutbound(call.id, { type: 'never_dispatched',
        callId: call.id, requestId: call.requestId, observedAt: new Date().toISOString(), reason: 'call_already_open' });
      store.update(call.id, { status: 'canceled', failure: { code: 'call_already_open', message: 'Another call for this person needs to finish first' } });
      throw Object.assign(new Error('Another call is already open'), { code: 'call_already_open' });
    }
    dialing.add(to);
    if (hark) harkDispatching.add(call.id);
    try {
      try {
        if (!notificationIsDue(call))
          throw new HarkAssignmentError(403, 'hark_event_expired');
        const readiness = await primary.readiness(to);
        if (!readiness.eligible) throw Object.assign(new Error('The account is not ready for this call'), { code: readiness.reason });
        await probe({ apiKey: config.apiKey, voice: config.voice, model: config.model });
      }
      catch (error) {
        if (call.outboundObservation) store.observeSelectedOutbound(call.id, { type: 'never_dispatched',
          callId: call.id, requestId: call.requestId, observedAt: new Date().toISOString(), reason: 'preflight_refused' });
        store.update(call.id, { status: 'failed', failure: safeFailure(error) }); throw error;
      }
      if (!accepting) {
        if (call.outboundObservation) store.observeSelectedOutbound(call.id, { type: 'never_dispatched',
          callId: call.id, requestId: call.requestId, observedAt: new Date().toISOString(), reason: 'service_stopping' });
        store.update(call.id, { status: 'canceled' }); throw new Error('service_shutting_down');
      }
      if (!notificationIsDue(call)) {
        store.update(call.id, { status: 'canceled', failure: { code: 'hark_event_expired', message: 'The saved watch event has expired' } });
        throw new HarkAssignmentError(403, 'hark_event_expired');
      }
      if (hark) store.beginHarkDispatch(call.id, new Date().toISOString());
      else if (call.outboundObservation) store.observeSelectedOutbound(call.id, { type: 'dispatch_started',
        callId: call.id, requestId: call.requestId, observedAt: new Date().toISOString() });
      else store.update(call.id, { status: 'dialing' });
      let providerResponded = false;
      try {
        const created = await provider.calls.create({ from: config.from, to,
          url: `${config.publicOrigin}/broker/answer/${call.id}`, method: 'POST',
          statusCallback: `${config.publicOrigin}/broker/status/${call.id}`, statusCallbackMethod: 'POST',
          statusCallbackEvent: ['initiated', 'ringing', 'answered', 'completed'],
          timeout: 30, timeLimit: config.maxCallSeconds, record: false,
        });
        providerResponded = true;
        if (!CALL_SID.test(created.sid)) throw new Error('invalid_provider_call_id');
        if (hark) store.acceptHarkDispatch(call.id, created.sid, new Date().toISOString());
        else if (call.outboundObservation) selectedProviderObservation(store.get(call.id)!, outboundAcceptedIdentitySchema.parse({
          callSid: created.sid, accountSid: created.accountSid, from: created.from, to: created.to }));
        else store.update(call.id, { callSid: created.sid });
        const saved = observeProviderState(call.id, 'queued');
        return { replayed: false, call: saved };
      } catch (error) {
        // A signed callback can durably establish acceptance before create returns.
        // A transport error must not erase that receipt or regress terminal state.
        const saved = store.get(call.id)!;
        if (hark && saved.harkDispatch?.state === 'provider_accepted' && saved.callSid) {
          if (!providerResponded) return { replayed: true, call: saved };
          store.update(call.id, { failure: safeFailure(error) });
          throw error;
        }
        if (saved.outboundObservation?.accepted && saved.callSid) {
          if (!providerResponded) return { replayed: true, call: saved };
          store.update(call.id, { failure: safeFailure(error) });
          throw error;
        }
        const status = (error as { status?: number }).status;
        store.update(call.id, { status: !hark && status && status >= 400 && status < 500 ? 'failed' : 'unknown', failure: safeFailure(error) });
        throw error;
      }
    } finally { dialing.delete(to); harkDispatching.delete(call.id); }
  }

  async function bind(call: BrokerCall, proof: ProviderCallProof): Promise<BrokerCall> {
    const binding = await primary.openCall(proof, call.callAssignment?.value);
    if (binding.callSid !== call.callSid || binding.state !== 'open') throw new Error('primary_call_identity_mismatch');
    const bound = store.recordPrimaryBinding(call.id, binding);
    // The validated binding response seeds the first generation (the path inbound
    // admission already uses): media start then runs only the mandatory pre-load
    // eligibility recheck instead of a full context read. Never reused after restart.
    if (bound.direction === 'outbound' && !bound.inbound) startupContexts?.remember(bound, binding);
    return bound;
  }

  async function endWaitingCall(call: BrokerCall): Promise<void> {
    if (!call.callSid || active.has(call.id) || claimed.has(call.id) || providerFinished(call)) return;
    try {
      const ended = await provider.calls(call.callSid).update({ status: 'completed' });
      const current = store.get(call.id)!;
      if (ended.status === 'completed') observeProviderRecord(current, ended, 'authenticated_update');
      else store.update(call.id, { status: 'unknown' });
    } catch (error) { store.update(call.id, { failure: safeFailure(error) }); }
  }

  async function inboundResponse(call: BrokerCall, response: ServerResponse, originalHold = false): Promise<void> {
    call = store.get(call.id)!;
    if (restricted) { const setup = await restricted.inboundResponse(call); if (setup !== undefined) return xml(response, setup); }
    if (!options.inboundAttachment || !accepting) inbound.stopLocal(call.id, 'service_unavailable');
    if (call.inbound?.localStop || call.endedAt || terminalCallStatuses.has(call.status) || !options.inboundAttachment || !accepting) {
      // Hanging up this HTTP leg and closing its account attachment have separate receipts.
      xml(response, '<Response><Hangup/></Response>');
      await inbound.stop(call.id, call.inbound?.localStop?.reason ?? 'provider_terminal').catch(error => {
        store.update(call.id, { failure: safeFailure(error) });
      });
      return;
    }
    // The account stream already started: Twilio follows its trailing redirect only once that stream
    // is gone (a crash or restart after Live attached). Its media cannot resume, and a non-TwiML
    // reply plays Twilio's "application error", so end the leg. Its status callback and maintenance
    // close the attachment as for any ended call.
    if (call.mediaStartedAt || active.has(call.id) || claimed.has(call.id)) return xml(response, '<Response><Hangup/></Response>');
    if (inbound.mediaAllowed(call) || inbound.waitingMediaAllowed(call)) return inboundStream(call, response);
    const remote = call.inbound!.remote;
    try {
      const rendered = options.inboundAttachment!.waitingResponse({ callId: call.id,
        continuationUrl: `${config.publicOrigin}/broker/inbound/${call.id}/continue`,
        ...(remote && remote.state !== 'absent' ? { attachmentId: remote.attachmentId } : {}),
        ...(remote && 'expiresAt' in remote && remote.expiresAt ? { expiresAt: remote.expiresAt } : {}),
      });
      if (originalHold && !call.inbound!.mode && !call.inbound!.continuation)
        observeInboundHoldResponse(call.id, response, logInboundTiming);
      return xml(response, rendered);
    } catch (error) {
      store.update(call.id, { failure: safeFailure(error) });
      await inbound.stop(call.id, 'approval_surface_unavailable').catch(() => {});
      return xml(response, '<Response><Hangup/></Response>');
    }
  }

  /** Stream instructions answer the leg. Before admission binds, the stream carries only the
   * greeting clip; its trailing signed redirect returns the leg to the continuation if admission
   * ends without account media (restricted setup or hangup). */
  function inboundStream(call: BrokerCall, response: ServerResponse): void {
    observeInboundStreamResponse(call.id, response, logInboundTiming);
    xml(response, callTwiml(config.publicOrigin, call.id, streamToken(config.controlToken, call.id, call.callSid!),
      `${config.publicOrigin}/broker/inbound/${call.id}/continue`));
  }

  async function handle(request: IncomingMessage, response: ServerResponse) {
    const route = request.url || '/';
    if (options.sms && await options.sms.handle(request, response)) return;
    if (restricted && await restricted.handle(request, response)) return;
    if (request.method === 'GET' && route === '/broker/health') {
      const dependencies = await dependencyHealth();
      const ok = accepting && dependencies.ok;
      return json(response, ok ? 200 : 503, { ok, service: 'dexter-broker', model: config.model,
        revision: process.env.BROKER_REVISION || 'development', activeCalls: active.size, dependencies,
        notificationSourceKinds: ['publication_v1', 'owner_destination_test_v1', 'portfolio_watch_event_v1', 'watcher_event_v1'],
        notificationWatcherProducers: ['market_pulse', 'founder_portfolio_pulse', 'cate_market_cap'] });
    }
    if (route === '/internal/outbound-alerts' && request.method === 'POST') {
      if (!accepting || !config.harkInternalToken) return json(response, 503, { ok: false, error: 'hark_broker_not_configured' });
      if (!['127.0.0.1', '::1', '::ffff:127.0.0.1'].includes(request.socket.remoteAddress ?? ''))
        return json(response, 403, { ok: false, error: 'forbidden' });
      const token = request.headers['x-dexter-internal-auth'];
      if (!authorizedHarkIngress(request, config.harkInternalToken) || typeof token !== 'string')
        return json(response, 401, { ok: false, error: 'hark_authorization_required' });
      let notification: unknown;
      try { notification = JSON.parse(await body(request, 65_536)); }
      catch { return json(response, 400, { ok: false, error: 'invalid_hark_notification' }); }
      try {
        const parsed = OutboundAlertRequestSchema.safeParse(notification);
        if (!parsed.success || (!parsed.data.hark && !parsed.data.notificationRef)) throw new HarkAssignmentError(400, 'invalid_hark_notification');
        const normalized = parsed.data;
        const generic = Boolean(normalized.notificationRef);
        const notificationDigest = sha256Hex(canonicalJson({ namespace: generic
          ? 'dexter-broker-notification/v1' : 'dexter-broker-hark-notification/v1', notification: normalized }));
        const saved = generic ? store.byNotification(normalized.notificationRef!.intentId, notificationDigest)
          : store.byHarkNotification(normalized.hark!.deliveryId, notificationDigest);
        if (saved?.harkDispatch?.state === 'provider_accepted' && saved.callSid) {
          // Read-only recovery of the original receipt, not renewed dial authority.
          return json(response, 200, { ok: true, replay: true, intentId: saved.id, callSid: saved.callSid, state: saved.status });
        }
        if (saved && saved.harkDispatch?.state !== 'not_dispatched') throw new HarkAssignmentError(409, 'hark_dispatch_uncertain');
        if (saved && harkDispatching.has(saved.id)) throw new HarkAssignmentError(409, 'hark_dispatch_pending');
        // API resolution binds the current owner, saved source, destination and billing authority.
        const reservation = generic
          ? resolveNotificationAssignment ? store.reserveNotification(await resolveNotificationAssignment(normalized, token), notificationDigest)
            : (() => { throw new NotificationAssignmentError(503, 'notification_broker_not_configured'); })()
          : resolveHarkAssignment ? store.reserveHark(await resolveHarkAssignment(normalized, token), notificationDigest)
            : (() => { throw new HarkAssignmentError(503, 'hark_broker_not_configured'); })();
        const result = await dialReserved(reservation);
        return json(response, result.replayed ? 200 : 202, { ok: true, replay: result.replayed,
          intentId: result.call.id, callSid: result.call.callSid ?? null, state: result.call.status });
      } catch (error) {
        if (error instanceof HarkAssignmentError || error instanceof NotificationAssignmentError) return json(response, error.status, { ok: false, error: error.code });
        if (error instanceof Error && ['Hark delivery conflicts with its saved record', 'Hark notification conflicts with its saved record', 'Notification delivery conflicts with its saved record', 'Notification conflicts with its saved record'].includes(error.message))
          return json(response, 409, { ok: false, error: 'hark_delivery_conflict' });
        if (error instanceof Error && ['Hark notification identity unavailable', 'Notification identity unavailable'].includes(error.message))
          return json(response, 409, { ok: false, error: 'hark_dispatch_history_unavailable' });
        if ((error as { code?: string }).code === 'call_already_open') return json(response, 409, { ok: false, error: 'call_already_open' });
        throw error;
      }
    }
    if (route.startsWith('/internal/broker/')) {
      // Receipt recovery remains available while new-call admission is stopped.
      if (request.method === 'GET' && route.startsWith('/internal/broker/call-requests')) {
        const receiptJson = (status: number, value: unknown) => {
          response.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'private, no-store' });
          response.end(JSON.stringify(value));
        };
        if (!authorizedControl(request.headers.authorization, config.controlToken))
          return receiptJson(401, { error: 'unauthorized' });
        try {
          const requestId = callRequestIdFromLookupPath(route);
          return receiptJson(200, readCallRequestReceipt(store, requestId));
        } catch (error) {
          const failure = callRequestReceiptFailure(error);
          return receiptJson(failure.status, failure.body);
        }
      }
      if (!authorizedControl(request.headers.authorization, config.controlToken)) return json(response, 401, { error: 'unauthorized' });
      if (request.method === 'POST' && route === '/internal/broker/result-sms') {
        let input: unknown;
        try { input = JSON.parse(await body(request, 2_048)); }
        catch { return json(response, 400, { ok: false, error: 'invalid_request' }); }
        const result = await dispatchResultSms(input, { client: resultSms, sms: options.sms, accepting: () => accepting });
        return json(response, !result.ok && result.error === 'invalid_request' ? 400 : 200, result);
      }
      if (request.method === 'POST' && route === '/internal/broker/result-sms/eligibility') {
        let input: Record<string, unknown>;
        try {
          input = JSON.parse(await body(request, 2_048));
          const keys = ['bindingId', 'callSid', 'primaryDexterId', 'threadId'];
          if (!input || Array.isArray(input) || Object.keys(input).length !== keys.length
            || Object.keys(input).some(key => !keys.includes(key))
            || !['bindingId', 'primaryDexterId', 'threadId'].every(key => typeof input[key] === 'string' && UUID.test(input[key] as string))
            || typeof input.callSid !== 'string' || !CALL_SID.test(input.callSid)) throw new Error('invalid_request');
        } catch { return json(response, 400, { ok: false, error: 'invalid_request' }); }
        const admittedCall = () => {
          if (!accepting) return undefined;
          const calls = store.list().filter(call => call.callSid === input.callSid && call.primary?.bindingId === input.bindingId);
          if (calls.length !== 1) return undefined;
          const call = calls[0];
          if (call.status !== 'in-progress' || !call.mediaStartedAt || call.endedAt || call.finalized
            || call.primaryClosed || call.primaryClosePending || call.restrictedSetup
            || !call.primary || call.primary.primaryDexterId !== input.primaryDexterId || call.primary.threadId !== input.threadId)
            return undefined;
          return call;
        };
        if (!admittedCall()) return json(response, 409, { ok: false, error: 'sms_call_unavailable' });
        try {
          const current = await primary.getContext(input.bindingId as string);
          if (current.state !== 'open' || current.bindingId !== input.bindingId || current.callSid !== input.callSid
            || current.context.primaryDexterId !== input.primaryDexterId || current.context.threadId !== input.threadId)
            return json(response, 409, { ok: false, error: 'sms_call_unavailable' });
        } catch { return json(response, 409, { ok: false, error: 'sms_call_unavailable' }); }
        const call = admittedCall();
        if (!call) return json(response, 409, { ok: false, error: 'sms_call_unavailable' });
        if (!options.sms) return json(response, 503, { ok: false, error: 'sms_delivery_pending' });
        try { return json(response, 200, options.sms.requestedEligibility(call.number)); }
        catch (error) {
          const failure = resultSmsFailure(error);
          return json(response, ['sms_delivery_pending', 'sms_unavailable'].includes(failure.error) ? 503 : 409, failure);
        }
      }
      if (request.method === 'GET' && route === '/internal/broker/calls') return json(response, 200, { calls: store.list().slice(0, 50).map(publicCall) });
      if (request.method !== 'POST' || !accepting) return json(response, 405, { error: 'unavailable' });
      const input = JSON.parse(await body(request, 8_000) || '{}') as Record<string, unknown>;
      if (route === '/internal/broker/sms') {
        const keys = ['bindingId', 'requestId', 'callSid', 'primaryDexterId', 'threadId', 'sourceEventId', 'sourceItemId', 'sourceRevision', 'message'];
        if (!input || Array.isArray(input) || Object.keys(input).some(key => !keys.includes(key))
          || !['bindingId', 'requestId', 'primaryDexterId', 'threadId', 'sourceEventId'].every(key => typeof input[key] === 'string' && UUID.test(input[key] as string))
          || typeof input.callSid !== 'string' || !CALL_SID.test(input.callSid)
          || typeof input.sourceItemId !== 'string' || !input.sourceItemId || input.sourceItemId.length > 200
          || input.sourceRevision !== 1 || typeof input.message !== 'string' || !input.message.trim()
          || input.message.length > 1000 || /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(input.message))
          return json(response, 400, { ok: false, error: 'invalid_request' });
        const admittedCall = () => {
          const call = store.bySid(input.callSid as string);
          if (!call || call.status !== 'in-progress' || !call.mediaStartedAt || call.endedAt || call.finalized
            || call.primaryClosed || call.primaryClosePending || call.restrictedSetup
            || !call.primary || call.primary.bindingId !== input.bindingId || call.primary.primaryDexterId !== input.primaryDexterId
            || call.primary.threadId !== input.threadId) return undefined;
          const matching = (Object.values(call.delegations) as BrokerDelegationRecord[]).filter(entry =>
            entry.version === 1 && entry.callId === call.id && entry.callSid === call.callSid && entry.bindingId === input.bindingId
            && (entry.requestId === input.requestId || (entry.reconciliation?.outcome === 'admitted'
              && entry.reconciliation.canonicalRequestId === input.requestId && entry.reconciliation.bindingId === input.bindingId))
            && ['dispatching', 'dispatch_unknown', 'queued', 'running'].includes(entry.status)
            && entry.submission?.source.role === 'user' && entry.submission.source.callSid === call.callSid
            && entry.submission.source.sourceEventId === input.sourceEventId && entry.submission.source.itemId === input.sourceItemId
            && entry.submission.source.revision === input.sourceRevision);
          return matching.length === 1 ? call : undefined;
        };
        if (!admittedCall()) return json(response, 409, { ok: false, error: 'sms_call_unavailable' });
        // Reuse the binding read's current owner, caller verification and Primary checks.
        // Its context stays private; it supplies no instructions or text to this action.
        try {
          const current = await primary.getContext(input.bindingId as string);
          if (current.state !== 'open' || current.bindingId !== input.bindingId || current.callSid !== input.callSid
            || current.context.primaryDexterId !== input.primaryDexterId || current.context.threadId !== input.threadId)
            return json(response, 409, { ok: false, error: 'sms_call_unavailable' });
        } catch { return json(response, 409, { ok: false, error: 'sms_call_unavailable' }); }
        const call = admittedCall();
        if (!call || !accepting) return json(response, 409, { ok: false, error: 'sms_call_unavailable' });
        if (!options.sms) return json(response, 200, { ok: false, error: 'sms_delivery_pending', enrollmentUrl: 'https://dexter.cash/sms' });
        return json(response, 200, await options.sms.sendRequested(call.number, call.callSid!, input.message));
      }
      if (route === '/internal/broker/preflight') return json(response, 200, await probe({ apiKey: config.apiKey, model: config.model, voice: config.voice }));
      if (route === '/internal/broker/calls') {
        if (typeof input.requestId !== 'string' || !UUID.test(input.requestId)
          || typeof input.purpose !== 'string' || !input.purpose.trim() || input.purpose.length > 1500
          || (input.to !== undefined && !isPhoneNumber(input.to))
          || Object.keys(input).some(key => !['requestId', 'purpose', 'to', 'continuation', 'retryOf'].includes(key))
          || (input.retryOf !== undefined && (!input.continuation || !outboundRetryOfSchema.safeParse(input.retryOf).success)))
          return json(response, 400, { error: 'invalid_call_request' });
        const to = input.to as string | undefined ?? (config.allowedTo.length === 1 ? config.allowedTo[0] : undefined);
        if (!to || !config.allowedTo.includes(to)) return json(response, 403, { error: 'recipient_not_authorized' });
        const saved = store.list().find(call => call.requestId === input.requestId);
        // Historical exact retries recover their original record. New calls
        // must satisfy the shared API contract before reservation or dialing.
        if (!saved && !newPhoneCallControlSchema.safeParse({ assignmentId: input.requestId,
          purpose: input.purpose.trim() }).success) return json(response, 400, { error: 'invalid_call_request' });
        try {
          const selected = callContinuationPatch(input.continuation);
          if (saved) assertCallContinuationReplay(saved.continuation, selected.continuation);
          // Historical receipts require no new admission. New selected calls
          // need the complete eligible-history and selected-answer composition.
          if (selected.continuation && !saved && (!options.eligibleContext || !options.selectedQuestion))
            return json(response, 503, { error: 'call_continuation_unavailable' });
          const result = await requestOutbound({ requestId: input.requestId, purpose: input.purpose.trim(), to,
            origin: 'authenticated_broker_control', ...selected, ...outboundRetryOfPatch(input.retryOf) });
          return json(response, result.replayed ? 200 : 201, { replayed: result.replayed, call: publicCall(result.call) });
        } catch (error) {
          if (error instanceof OutboundRetryError) return json(response,
            error.code === 'call_request_unavailable' ? 503 : 409, { error: error.code });
          if (error instanceof CallContinuationError) {
            if (error.code !== 'invalid_call_continuation' && (input.retryOf !== undefined || saved?.retryOf !== undefined))
              return json(response, 409, { error: 'call_request_conflict' });
            return json(response, error.code === 'invalid_call_continuation' ? 400 : 409, { error: error.code });
          }
          if ((error as { code?: string }).code === 'call_already_open') return json(response, 409, { error: 'call_already_open' });
          if (input.continuation) return json(response, 503, { error: 'call_request_unavailable' });
          throw error;
        }
      }
      if (route === '/internal/broker/end' || route === '/internal/broker/reconcile') {
        if (typeof input.id !== 'string' || !UUID.test(input.id)) return json(response, 400, { error: 'invalid_call_id' });
        const call = store.get(input.id);
        if (!call?.callSid) return json(response, 409, { error: 'saved_provider_call_required' });
        if (call.verificationCallback || call.restrictedSetup) {
          if (!restricted) return json(response, 503, { error: 'restricted_phone_unavailable' });
          if (route.endsWith('/reconcile')) await restricted.reconcile(call);
          else await restricted.stop(call, 'operator_requested');
          return json(response, 200, { call: publicCall(store.get(call.id)!) });
        }
        if (route.endsWith('/reconcile')) {
          const observed = await provider.calls(call.callSid).fetch();
          if ((call.direction === 'outbound' && (observed.to !== call.number || observed.from !== config.from))
            || (call.direction === 'inbound' && (observed.from !== call.number || observed.to !== config.from))) throw new Error('provider_call_identity_mismatch');
          if (providerStates.has(observed.status as CallStatus)) observeProviderRecord(call, observed, 'authenticated_fetch');
          const reconciled = store.get(call.id)!;
          if ((terminalCallStatuses.has(reconciled.status) || reconciled.primaryClosePending) && active.has(call.id)) await active.get(call.id)!.stop('provider_terminal');
          else if (terminalCallStatuses.has(reconciled.status) || reconciled.primaryClosePending) {
            if (reconciled.inbound) await inbound.stop(reconciled.id, 'provider_terminal');
            else await closePrimary(reconciled);
          }
        } else if (call.inbound && !active.has(call.id)) {
          inbound.stopLocal(call.id, 'operator_requested');
          await endWaitingCall(call);
          await inbound.stop(call.id, 'operator_requested');
        } else if (active.has(call.id)) await active.get(call.id)!.stop('operator_requested');
        else if (!providerFinished(call)) {
          store.update(call.id, { endedAt: call.endedAt ?? new Date().toISOString() });
          const ended = await provider.calls(call.callSid).update({ status: 'completed' });
          if (ended.status === 'completed') observeProviderRecord(call, ended, 'authenticated_update');
          else store.update(call.id, { status: 'unknown', endedAt: new Date().toISOString() });
          await closePrimary(call);
        }
        else if (call.primaryClosePending) await closePrimary(call);
        return json(response, 200, { call: publicCall(store.get(call.id)!) });
      }
      return json(response, 404, { error: 'not_found' });
    }

    if (request.method !== 'POST') return json(response, 404, { error: 'not_found' });
    const incoming = route === '/broker/incoming-call';
    const match = route.match(/^\/broker\/(answer|status|callback)\/([0-9a-f-]{36})$/);
    const continuation = route.match(/^\/broker\/inbound\/([0-9a-f-]{36})\/continue$/);
    const genericStatus = route === '/broker/status';
    if (!incoming && !match && !genericStatus && !continuation) return json(response, 404, { error: 'not_found' });
    const raw = new URLSearchParams(await body(request));
    const params: Record<string, string> = {};
    for (const [key, value] of raw) { if (key in params) return json(response, 400, { error: 'duplicate_callback_field' }); params[key] = value; }
    const proof = verifyTwilio(config, request.headers['x-twilio-signature'] as string | undefined, route, params);
    if (!proof || !CALL_SID.test(params.CallSid || '')) return json(response, 403, { error: 'invalid_provider_callback' });
    if (!accepting) return xml(response, fallbackTwiml('Dexter is being updated. Please call again shortly.'));
    let call: BrokerCall;
    if (continuation) {
      const saved = store.get(continuation[1]);
      if (!saved?.inbound || saved.callSid !== params.CallSid || params.Direction !== 'inbound'
        || params.From !== saved.number || params.To !== config.from) return json(response, 403, { error: 'call_identity_mismatch' });
      if (!options.inboundAttachment) {
        await inbound.stop(saved.id, 'service_unavailable').catch(error => { store.update(saved.id, { failure: safeFailure(error) }); });
        return xml(response, '<Response><Hangup/></Response>');
      }
      try { inbound.recordContinuation(saved.id, proof); }
      catch (error) { store.update(saved.id, { failure: safeFailure(error) }); return inboundResponse(store.get(saved.id)!, response); }
      // The saved continuation lets the stream answer this leg now. The greeting clip plays
      // while admission runs, and the stream attaches Live once admission binds.
      const current = store.get(saved.id)!;
      if (!current.mediaStartedAt && !active.has(current.id) && !claimed.has(current.id) && inbound.waitingMediaAllowed(current)) {
        // Admission begins once these instructions are delivered: they answer the leg, and
        // the API's begin refuses a leg that is still ringing.
        response.once('close', () => {
          void inbound.tick(saved.id).catch(error => {
            try { store.update(saved.id, { failure: safeFailure(error) }); } catch { /* The stream's admission loop retries or ends the wait. */ }
          });
        });
        return inboundStream(current, response);
      }
      try { call = await inbound.resume(saved.id, proof); }
      catch (error) { store.update(saved.id, { failure: safeFailure(error) }); call = store.get(saved.id)!; }
      return inboundResponse(call, response);
    }
    if (incoming) {
      if (params.To !== config.from || !isPhoneNumber(params.From)) return json(response, 403, { error: 'call_identity_mismatch' });
      const retained = store.bySid(params.CallSid);
      if ((!retained && options.inboundAttachment) || retained?.inbound) {
        if (params.Direction !== 'inbound' || (retained && !retained.inbound)) return json(response, 409, { error: 'call_mode_conflict' });
        if (retained?.inbound && !options.inboundAttachment) {
          if (canonicalJson(retained.inbound.begin.providerProof) !== canonicalJson(proof)) return json(response, 409, { error: 'call_identity_mismatch' });
          await inbound.stop(retained.id, 'service_unavailable').catch(error => { store.update(retained.id, { failure: safeFailure(error) }); });
          return xml(response, '<Response><Hangup/></Response>');
        }
        try { call = await inbound.accept(proof); }
        catch (error) {
          const reserved = store.bySid(params.CallSid);
          if (!reserved?.inbound) throw error;
          // An identity mismatch cannot recover a different saved caller's wait.
          if (canonicalJson(reserved.inbound.begin.providerProof) !== canonicalJson(proof)) return json(response, 409, { error: 'call_identity_mismatch' });
          store.update(reserved.id, { failure: safeFailure(error) }); call = store.get(reserved.id)!;
        }
        sessionCalls.add(call.id);
        return inboundResponse(call, response, true);
      }
      const requestId = createHash('sha256').update(`incoming:${params.CallSid}`).digest('hex');
      call = store.reserve({ requestId, direction: 'inbound', purpose: 'The person called Dexter.', number: params.From, callSid: params.CallSid }).call;
      sessionCalls.add(call.id);
      return xml(response, callbacks.offer(call, proof));
    } else {
      const saved = genericStatus ? store.bySid(params.CallSid) : store.get(match![2]);
      if (!saved || (saved.direction === 'outbound' && (params.From !== config.from || params.To !== saved.number))
          || (saved.direction === 'inbound' && (params.To !== config.from || params.From !== saved.number))
          || (saved.direction === 'inbound' && match?.[1] === 'answer')
          || (saved.callSid && saved.callSid !== params.CallSid)) return json(response, 403, { error: 'call_identity_mismatch' });
      if (saved.outboundObservation) {
        const identity = outboundAcceptedIdentitySchema.parse({ accountSid: params.AccountSid, callSid: params.CallSid,
          from: params.From, to: params.To });
        const statusCallback = genericStatus || match?.[1] === 'status';
        call = selectedProviderObservation(saved, identity, statusCallback ? { status: params.CallStatus,
          source: 'signed_status_callback', evidenceDigest: sha256Hex(JSON.stringify([
            'broker.provider-status-callback/v1', proof.url, Object.keys(proof.params).sort().map(key => [key, proof.params[key]])])) } : undefined);
      } else if (!saved.callSid && saved.harkDispatch) {
        if (saved.harkDispatch.state !== 'dispatch_started') return json(response, 403, { error: 'call_not_dispatched' });
        call = store.acceptHarkDispatch(saved.id, params.CallSid, new Date().toISOString());
      } else call = saved.callSid ? saved : store.update(saved.id, { callSid: params.CallSid });
      if (match?.[1] === 'callback') {
        if (call.inbound) return json(response, 409, { error: 'call_mode_conflict' });
        return xml(response, await callbacks.consent(call, proof));
      }
      if (genericStatus || match![1] === 'status') {
        if (providerStates.has(params.CallStatus as CallStatus)) {
          const state = params.CallStatus as CallStatus;
          call = observeProviderState(call.id, state as HarkProviderStatus);
          if (terminalCallStatuses.has(call.status)) {
            if (active.has(call.id)) await active.get(call.id)!.stop('provider_terminal');
            else if (call.inbound) await inbound.stop(call.id, 'provider_terminal');
            else { pendingBinds.delete(call.id); await closePrimary(call); }
          }
        }
        return json(response, 200, { ok: true });
      }
    }
    if (terminalCallStatuses.has(call.status)) return xml(response, '<Response><Hangup/></Response>');
    // This is the signed answer route, distinct from a ringing status callback.
    if (managedNotification(call)) call = observeProviderState(call.id, 'in-progress');
    // Answer with the media stream at once. The greeting clip carries no account data and
    // plays while Primary verifies and binds this provider call; the stream attaches Live
    // (account context) only once that binding exists, or speaks the failure if it does not.
    if (!call.primary && !pendingBinds.has(call.id)) {
      const id = call.id;
      // Open only the Live socket (no session, no account text) while Primary binds;
      // Live adopts it at attach. Bounded by its own TTL.
      preconnect?.beginOutbound(call);
      const binding = bind(call, proof);
      pendingBinds.set(id, binding);
      const settled = binding.then(() => {
        // A live stream (connected now or later) attaches the binding. A leg that already ended,
        // a stream that is stopping (its stop sets endedAt first) or a stopping broker leaves
        // nothing to attach it, so it closes here, before shutdown closes the store.
        const current = store.get(id)!;
        if (accepting && !current.endedAt && !terminalCallStatuses.has(current.status)) return;
        pendingBinds.delete(id);
        return closePrimary(current);
      }, error => {
        preconnect?.forget(id); startupContexts?.discard(id);
        try { store.update(id, { failure: safeFailure(error) }); } catch { /* The stream speaks the failure and ends the call. */ }
      });
      bindSettlements.add(settled);
      const forget = () => { bindSettlements.delete(settled); };
      void settled.then(forget, forget);
    }
    xml(response, callTwiml(config.publicOrigin, call.id, streamToken(config.controlToken, call.id, call.callSid!)));
  }

  const server = createServer((request, response) => {
    const pending = handle(request, response).catch(error => {
      if (!response.headersSent) json(response, 503, { error: safeFailure(error) });
      else response.end();
    });
    pendingRequests.add(pending);
    void pending.finally(() => pendingRequests.delete(pending));
  });
  server.requestTimeout = 30_000;
  const sockets = new WebSocketServer({ noServer: true, maxPayload: 64 * 1024 });
  server.on('upgrade', (request, socket, head) => {
    const route = request.url || '';
    const setupMatch = route.match(/^\/broker\/setup-media\/([0-9a-f-]{36})$/);
    const match = route.match(/^\/broker\/media\/([0-9a-f-]{36})$/) ?? setupMatch;
    if (!accepting || !match || !verifyTwilio(config, request.headers['x-twilio-signature'] as string | undefined, route, {}, true)) {
      socket.write('HTTP/1.1 403 Forbidden\r\n\r\n'); socket.destroy(); return;
    }
    const call = store.get(match[1]);
    if (setupMatch) {
      if (!restricted || !call || !restricted.eligible(call) || claimed.has(call.id)) {
        socket.write('HTTP/1.1 409 Conflict\r\n\r\n'); socket.destroy(); return;
      }
      claimed.add(call.id); sockets.handleUpgrade(request, socket, head, ws => restricted.connect(ws, call)); return;
    }
    // A stream may connect before its account binding exists (outbound binding in flight,
    // inbound admission after the signed continuation); it then carries only the greeting.
    // An inbound stream whose wait already ended is accepted only to hand the leg back to its TwiML.
    if (!call?.callSid || call.endedAt || terminalCallStatuses.has(call.status) || claimed.has(call.id)
      || !(call.verificationCallback ? call.primary && restricted?.privateMediaAllowed(call)
        : call.inbound ? inbound.mediaAllowed(call) || (Boolean(call.inbound.continuation) && !call.mediaStartedAt)
          : call.primary || pendingBinds.has(call.id))) {
      socket.write('HTTP/1.1 409 Conflict\r\n\r\n'); socket.destroy(); return;
    }
    claimed.add(call.id);
    sockets.handleUpgrade(request, socket, head, ws => connectMedia(ws, call));
  });

  function connectMedia(ws: WebSocket, call: BrokerCall) {
    let streamSid: string | undefined;
    let live: BrokerLiveSession | undefined;
    let connectedSessionId: string | undefined;
    let backend: ReturnType<typeof createPrimaryBackend> | undefined;
    let stopping = false;
    let stopPromise: Promise<void> | undefined;
    // Inbound admission ended without account media: the stream was handed back to its TwiML.
    let released = false, releaseAfterGreeting = false;
    let inputBytes = call.liveGenerations?.inputNextSample ?? 0;
    let lastSpeechAt = 0;
    const receipts = new PlaybackReceipts();
    let audioSentRecorded = Boolean(call.journey?.firstAudioSentAt);
    let playbackRecorded = Boolean(call.journey?.firstPlaybackConfirmedAt);
    let audibleSentRecorded = false, audiblePlaybackRecorded = false;
    let speechSentRecorded = false, speechPlaybackRecorded = false;
    // Unplayed pre-speech chunks (mark name + payload), kept until the first speech chunk.
    let preSpeechTail: Array<{ name: string; payload: string; bytes: number; audible: boolean }> = [];
    // Greeting clip: while it plays, Live output is held in order (never sent), so
    // the carrier queue holds only the clip and Live can neither overlap nor clear it.
    let greeting: GreetingClipPlayback | undefined;
    let greetingHold: string[] | undefined;
    let greetingHeldBytes = 0;
    let greetingReleaseTimer: ReturnType<typeof setTimeout> | undefined;
    const greetingMarks = new Set<string>();
    let greetingPlaybackRecorded = false;
    // Caller input gate. Closed while the greeting clip plays, so words spoken over the greeting
    // never reach Live and cannot draw a reply to a greeting that is already over. From the
    // moment it opens, every caller frame is kept: held here until Live attaches, then
    // delivered first and in order.
    let inputOpenedAt: number | undefined;
    let heldInput: Array<{ payload: string; bytes: number }> = [];
    let heldInputBytes = 0, droppedInputBytes = 0, greetingInputBytes = 0;
    // Caller audio Live has received: Live's input clock, counted from mediaStartedAt.
    let fedBytes = 0;
    // Arrival of the latest caller speech frame after the greeting and before Dexter first spoke:
    // the end of what the caller said before the first reply (journey.callerSpeechEndedAt).
    let callerSpeechEndedAt: number | undefined;
    const recordAudibleTiming = (stage: 'firstAudibleAudioSentAt' | 'firstAudiblePlaybackAcknowledgedAt'
      | 'firstSpeechAudioSentAt' | 'firstSpeechPlaybackAcknowledgedAt'
      | 'greetingClipStartedAt' | 'greetingClipFirstPlaybackAcknowledgedAt' | 'greetingClipPlayedAt', detail: Record<string, number> = {}) => {
      try { console.info(JSON.stringify({ event: 'broker_audio_timing', version: 1, callId: call.id,
        stage, observedAt: new Date().toISOString(), ...detail })); } catch { /* Timing cannot interrupt audio or playback. */ }
    };
    const sendOutputChunk = (payload: string, bytes: Buffer, audible: boolean, speech: boolean): string => {
      ws.send(JSON.stringify({ event: 'media', streamSid, media: { payload } }));
      if (!audioSentRecorded) {
        const saved = store.get(call.id)!;
        store.update(call.id, { journey: { ...saved.journey, firstAudioSentAt: new Date().toISOString() } });
        audioSentRecorded = true;
      }
      if (bytes.length > 0 && audible && !audibleSentRecorded) {
        audibleSentRecorded = true; recordAudibleTiming('firstAudibleAudioSentAt');
      }
      const name = receipts.sent(bytes.length, audible, speech);
      ws.send(JSON.stringify({ event: 'mark', streamSid, mark: { name } }));
      return name;
    };
    /** Twilio plays queued outbound media in order and in real time. Live
     * streams its own near-silent frames before it speaks, so a backlog of them
     * would delay the greeting by exactly its length. Before the first speech
     * chunk, drop that unplayed backlog and re-queue only its last 200 ms, which
     * may hold the speech onset. Only ever pre-speech audio is dropped. */
    const clearPreSpeechBacklog = () => {
      const backlogMs = receipts.backlogMs;
      if (backlogMs < PRE_SPEECH_CLEAR_MIN_MS) return;
      const unplayed = preSpeechTail.filter(entry => receipts.isPending(entry.name));
      const dropped = receipts.dropPreSpeech();
      if (!dropped) return;
      ws.send(JSON.stringify({ event: 'clear', streamSid }));
      const keep: typeof unplayed = [];
      let keptBytes = 0;
      for (let index = unplayed.length - 1; index >= 0 && keptBytes + unplayed[index].bytes <= PRE_SPEECH_ONSET_BYTES; index--) {
        keep.unshift(unplayed[index]); keptBytes += unplayed[index].bytes;
      }
      for (const entry of keep) sendOutputChunk(entry.payload, Buffer.from(entry.payload, 'base64'), entry.audible, false);
      try { console.info(JSON.stringify({ event: 'broker_pre_speech_clear', version: 1, callId: call.id,
        observedAt: new Date().toISOString(), backlogMs, droppedMs: dropped / 8, requeuedMs: keptBytes / 8 })); } catch { /* Telemetry only. */ }
    };
    /** Live audio path after the greeting (unchanged pre-speech handling). */
    const forwardLiveAudio = (payload: string) => {
      if (stopping || ws.readyState !== WebSocket.OPEN || !streamSid) return;
      if (ws.bufferedAmount > 256_000 || receipts.pendingCount > 300 || receipts.backlogMs > 6_000) { void stop('audio_backpressure'); return; }
      const bytes = Buffer.from(payload, 'base64');
      const audible = hasAudiblePcmu(bytes);
      const speech = bytes.length > 0 && carriesSpeechPcmu(bytes);
      if (speech && !speechSentRecorded) {
        clearPreSpeechBacklog();
        preSpeechTail = [];
      }
      const name = sendOutputChunk(payload, bytes, audible, speech);
      if (speech && !speechSentRecorded) {
        speechSentRecorded = true;
        // Dexter's first words (never the greeting clip). After a caller who already spoke, the gap
        // since their last speech frame is the silence they heard before the first reply.
        const sentAt = Date.now();
        try {
          const saved = store.get(call.id)!;
          if (!saved.journey?.firstSpeechAudioSentAt) store.update(call.id, { journey: { ...saved.journey,
            firstSpeechAudioSentAt: new Date(sentAt).toISOString(),
            ...(callerSpeechEndedAt === undefined ? {} : { callerSpeechEndedAt: new Date(callerSpeechEndedAt).toISOString() }) } });
        } catch { /* Timing evidence cannot interrupt audio. */ }
        recordAudibleTiming('firstSpeechAudioSentAt', { queuedAheadMs: Math.max(0, receipts.backlogMs - bytes.length / 8),
          ...(callerSpeechEndedAt === undefined ? {} : { afterCallerSpeechMs: sentAt - callerSpeechEndedAt }) });
      } else if (!speechSentRecorded) {
        preSpeechTail.push({ name, payload, bytes: bytes.length, audible });
        if (preSpeechTail.length > PRE_SPEECH_TAIL_LIMIT) preSpeechTail.shift();
      }
    };
    /** The greeting has been heard (or its playback can no longer be confirmed): keep caller audio from now on. */
    const openInput = () => {
      if (inputOpenedAt !== undefined) return;
      inputOpenedAt = Date.now();
      if (releaseAfterGreeting) releaseToTwiml();
    };
    /** Ends the hold and sends held Live audio, in arrival order, behind the clip. */
    const releaseGreetingHold = (reason: 'played' | 'ack_timeout' | 'send_stopped') => {
      if (!greetingHold) return;
      clearTimeout(greetingReleaseTimer); greetingReleaseTimer = undefined;
      const held = greetingHold, heldMs = greetingHeldBytes / 8;
      greetingHold = undefined; greetingHeldBytes = 0;
      if (reason === 'played' && !stopping) {
        const saved = store.get(call.id)!;
        if (!saved.journey?.greetingClipPlayedAt) store.update(call.id, { journey: { ...saved.journey, greetingClipPlayedAt: new Date().toISOString() } });
      }
      recordAudibleTiming('greetingClipPlayedAt', { heldLiveMs: heldMs, ...(reason === 'played' ? {} : { incomplete: 1 }) });
      openInput();
      for (const payload of held) { if (stopping) break; forwardLiveAudio(payload); }
    };
    const startGreetingClip = (clip: Buffer) => {
      const saved = store.get(call.id)!;
      store.update(call.id, { journey: { ...saved.journey, greetingClipStartedAt: new Date().toISOString() } });
      recordAudibleTiming('greetingClipStartedAt', { clipMs: clip.length / 8 });
      greetingHold = [];
      greetingReleaseTimer = setTimeout(() => releaseGreetingHold('ack_timeout'), clip.length / 8 + GREETING_ACK_GRACE_MS);
      greeting = playGreetingClip(clip, {
        send(frame) {
          if (stopping || ws.readyState !== WebSocket.OPEN || !streamSid) return false;
          // Speech-flagged so a pre-speech clear can never drop it.
          greetingMarks.add(sendOutputChunk(frame.toString('base64'), frame, hasAudiblePcmu(frame), true));
          return true;
        },
        onSent() { if (!greetingMarks.size) releaseGreetingHold('played'); },
      });
      if (!greeting.sending && greetingMarks.size === 0) releaseGreetingHold('send_stopped');
    };
    /** Delivers one caller frame to Live and advances Live's input clock. */
    const feedLive = (payload: string): boolean => {
      const bytes = Buffer.from(payload, 'base64');
      fedBytes += bytes.length;
      // Twilio timestamps are media positions, not an authorization or a turn boundary.
      if (inputSpeechEnergy(bytes) > CALLER_SPEECH_ENERGY) { lastSpeechAt = Date.now(); backend?.noteCallerSpeech(lastSpeechAt, fedBytes / 8); }
      return live!.appendAudio(payload) !== false;
    };
    const metrics = () => ({ receivedAudioBytes: inputBytes, sentAudioBytes: receipts.sentBytes, playedAudioBytes: receipts.playedBytes, clearedAudioBytes: receipts.clearedBytes });
    const startTimer = setTimeout(() => { void stop('media_start_timeout'); }, 8_000);
    const callDeadlineAt = call.liveGenerations?.clocks.callDeadlineAt ?? call.verificationCallback?.media?.deadlineAt ?? new Date(Date.now() + config.maxCallSeconds * 1000).toISOString();
    const durationTimer = setTimeout(() => { void stop('duration_limit'); }, Math.max(0, Date.parse(callDeadlineAt) - Date.now()));

    /** Inbound admission ended without account media (refusal, expiry, cancellation): close this
     * stream so Twilio runs its trailing signed redirect, whose continuation answers with
     * restricted setup or a hangup. The caller hears the whole greeting first. */
    function releaseToTwiml(): void {
      if (stopping || released) return;
      if (inputOpenedAt === undefined) { releaseAfterGreeting = true; return; }
      released = true;
      clearTimeout(startTimer); clearTimeout(durationTimer);
      try { console.info(JSON.stringify({ event: 'broker_greeting_stream_released', version: 1, callId: call.id,
        observedAt: new Date().toISOString() })); } catch { /* Telemetry only. */ }
      claimed.delete(call.id); active.delete(call.id);
      if (ws.readyState === WebSocket.OPEN) ws.close();
    }

    function stop(reason: string): Promise<void> {
      if (stopPromise) return stopPromise;
      stopping = true;
      preconnect?.discard(call.id);
      clearTimeout(startTimer); clearTimeout(durationTimer);
      greeting?.cancel(); clearTimeout(greetingReleaseTimer); greetingHold = undefined;
      const previous = store.get(call.id)!;
      if (previous.inbound) inbound.stopLocal(call.id, reason);
      // An unbound call has nothing to close yet; a binding that lands later is closed by its owner.
      store.update(call.id, { endedAt: previous.endedAt ?? new Date().toISOString(), ...(previous.primary ? { primaryClosePending: true } : {}) });
      stopPromise = (async () => {
        const closing = live?.close(reason);
        try {
          const saved = store.get(call.id);
          if (saved?.verificationCallback) {
            if (!restricted) throw new Error('restricted_phone_unavailable');
            await restricted.endPrivateProvider(saved, reason);
          } else if (reason !== 'provider_terminal' && saved && !providerFinished(saved)) {
            if (SPOKEN_STOPS[reason]) {
              const spoken = await provider.calls(call.callSid!).update({ twiml: fallbackTwiml(SPOKEN_STOPS[reason]) });
              if (providerStates.has(spoken.status as CallStatus)) observeProviderRecord(saved, spoken, 'authenticated_update');
              else store.update(call.id, { status: 'unknown' });
            } else {
              const ended = await provider.calls(call.callSid!).update({ status: 'completed' });
              if (ended.status === 'completed') observeProviderRecord(saved, ended, 'authenticated_update');
              else store.update(call.id, { status: 'unknown' });
            }
          }
        } catch (error) { store.update(call.id, { status: 'unknown', failure: safeFailure(error) }); }
        // Closing <Connect><Stream> resumes its trailing verb. Install any
        // audible failure response on the provider before closing this socket.
        if (ws.readyState === WebSocket.OPEN) ws.close();
        const result = await closing;
        if (live && backend) {
          backend.persistAcousticEvidence();
          const transcript = live.getTranscript();
          store.update(call.id, { transcript, transcriptRevision: transcript.revision, transcriptCapture: {
            state: 'pending', pending: 0, unsupported: [],
            assistantPlaybackComplete: result?.outputAudioDiscarded === false && receipts.pendingCount === 0 && receipts.clearedBytes === 0,
            sessionFinalized: result?.finalized === true,
          } });
        }
        store.update(call.id, { metrics: metrics() });
        await closePrimary(store.get(call.id)!);
      })().finally(() => { claimed.delete(call.id); active.delete(call.id); });
      return stopPromise;
    }

    /** Starts the account voice once the binding exists: Live opens with this call's context and
     * receives the caller audio kept since the greeting ended, first and in order. */
    function attach(): void {
      if (stopping || released || live) return;
      if (!accepting) { void stop('service_shutdown'); return; }
      const bound = store.get(call.id)!;
      if (!bound.primary || (bound.inbound && !inbound.mediaAllowed(bound))) throw new Error('media_binding_unavailable');
      pendingBinds.delete(call.id);
      // Input sample 0 is the first caller frame Live receives: the greeting's end (now, if the
      // greeting is still playing), moved past any dropped held audio, and never before an
      // inbound binding was consumed.
      const now = Date.now();
      let origin = Math.min(now, inputOpenedAt === undefined ? now : inputOpenedAt + droppedInputBytes / 8);
      if (bound.inbound?.remote?.state === 'bound') origin = Math.max(origin, Date.parse(bound.inbound.remote.consumedAt));
      const mediaStartedAt = bound.mediaStartedAt ?? new Date(origin).toISOString();
      if (managedNotification(bound)) store.observeHarkMediaStarted(call.id, mediaStartedAt);
      else store.update(call.id, { mediaStartedAt });
      // Evidence for the caller audio Live receives: held since the greeting ended, dropped from
      // an overlong wait, and discarded because it was spoken over the greeting.
      try { console.info(JSON.stringify({ event: 'broker_account_media', version: 1, callId: call.id, observedAt: new Date(now).toISOString(),
        mediaStartedAt, heldInputMs: heldInputBytes / 8, droppedInputMs: droppedInputBytes / 8, greetingInputMs: greetingInputBytes / 8 })); }
      catch { /* Telemetry only. */ }
      if (!options.eligibleContext) backend = createPrimaryBackend({ callId: call.id, callSid: call.callSid!, bindingId: bound.primary.bindingId, api: primary, store, watchUntilCallEnds: true, lastCallerSpeechAt: () => lastSpeechAt });
      const voiceOptions: BrokerLiveSessionOptions = { apiKey: config.apiKey, callId: call.id, voice: config.voice,
        instructions: brokerInstructions({ direction: call.direction, purpose: call.purpose, registeredAssignment: !!call.callAssignment,
          ...(call.callAssignment?.origin === 'authenticated_hark_delivery' ? { watch: call.callAssignment.value.hark } : {}),
          ...(call.callAssignment?.origin === 'authenticated_notification_delivery' ? { notification: call.callAssignment.value.notification } : {}),
          name: bound.primary.name, context: bound.primary.context }),
        history: brokerHistory({ name: bound.primary.name, context: bound.primary.context }),
        backend: backend ?? { async delegate() { throw new Error('generation_backend_required'); } },
        onAudio(payload) {
          if (stopping || ws.readyState !== WebSocket.OPEN || !streamSid) return;
          if (greetingHold) {
            greetingHeldBytes += Buffer.byteLength(payload, 'base64');
            if (greetingHeldBytes > GREETING_HOLD_MAX_BYTES) { void stop('audio_backpressure'); return; }
            greetingHold.push(payload); return;
          }
          forwardLiveAudio(payload);
        },
        onTranscript(snapshot) {
          store.update(call.id, { transcript: snapshot, transcriptRevision: snapshot.revision });
          backend?.observeTranscript(snapshot);
        },
        onReady({ sessionId }) {
          connectedSessionId = sessionId;
          const saved = store.get(call.id)!;
          if (!saved.journey?.liveReadyAt) store.update(call.id, { journey: { ...saved.journey, liveReadyAt: new Date().toISOString() } });
          // The selected host publishes the exact eligible saved question.
          // Its operator purpose must never become an automatic new task.
          if (call.continuation) return;
          // The generation controller owns its opening inside validated activation.
          if (options.eligibleContext) return;
          store.update(call.id, { liveSessionId: sessionId, voiceOpening: { status: 'pending' } });
          // Live's documented speak-first command runs while phone audio
          // continues. Its receipt confirms context injection, not playback.
          queueMicrotask(() => {
            if (stopping || !live) return;
            void live.appendInstructions(brokerOpening(call.direction, !!call.callAssignment, call.callAssignment?.origin === 'authenticated_hark_delivery', call.callAssignment?.origin === 'authenticated_notification_delivery',
              Boolean(store.get(call.id)!.journey?.greetingClipStartedAt)))
              .then(result => { store.update(call.id, { voiceOpening: result }); })
              .catch(() => { if (!stopping) store.update(call.id, { voiceOpening: { status: 'unconfirmed', reason: 'opening_delivery_failed' } }); });
          });
        },
        onClosed(result) { store.update(call.id, { finalized: result.finalized, metrics: metrics() }); if (!stopping) void stop(result.finalized ? result.reason : 'voice_unavailable'); },
        onPlaybackClear() {
          // During the clip the carrier holds only the clip: Live's clear drops its own held audio.
          if (greetingHold) { greetingHold = []; greetingHeldBytes = 0; return; }
          receipts.clear();
          if (ws.readyState === WebSocket.OPEN && streamSid) ws.send(JSON.stringify({ event: 'clear', streamSid }));
        },
        onEvent(event) {
          if (event.type === 'live.append.late_rejection' && connectedSessionId && typeof event.eventId === 'string')
            rejectBrokerPublication(store, { callId: call.id, callSid: call.callSid!, bindingId: bound.primary!.bindingId,
              sessionId: connectedSessionId }, event.eventId, typeof event.code === 'string' ? event.code : 'provider_error');
          if (event.type === 'live.error') store.update(call.id, { failure: { code: String(event.code || 'live_error'), message: 'The voice connection could not continue' } });
        },
      };
      live = options.eligibleContext ? createGenerationVoice({ call: store.get(call.id)!, callDeadlineAt,
        apiKey: config.apiKey, voice: config.voice, store, primary,
        playbackState: () => carrierPlaybackState(!stopping && ws.readyState === WebSocket.OPEN,
          receipts.audibleSentBytes, receipts.audiblePlayedBytes, receipts.audibleClearedBytes),
        context: startupContexts!.take(store.get(call.id)!),
        voiceFactory(settings) {
          const connectionFactory = preconnect?.factory(store.get(call.id)!);
          return voiceFactory(connectionFactory ? { ...settings, connectionFactory } : settings);
        },
        ...(options.selectedQuestion ? { selectedQuestion: options.selectedQuestion } : {}),
        ...(call.continuation && options.callerIntent ? { callerIntent: createCallerIntentResolver({
          callId: call.id, store, client: options.callerIntent.client, timeoutMs: options.callerIntent.timeoutMs,
        }) } : {}),
        onAudio: payload => voiceOptions.onAudio(payload, { deliveryId: 'generation_host_audio' }),
        onReady: voiceOptions.onReady, onClosed: voiceOptions.onClosed,
        onPlaybackClear: () => voiceOptions.onPlaybackClear?.('application_interrupt'),
        onProblem(code) { queueMicrotask(() => { if (stopping) return;
          // Recording the failure must never take down the process and every other call with it.
          try { store.update(call.id, { failure: safeFailure(Object.assign(new Error(code), { code })) }); }
          catch (error) { console.error(JSON.stringify({ event: 'broker_failure_record_unavailable', callId: call.id, code,
            message: (error as Error).message })); }
          void stop('voice_unavailable');
        }); },
      }) : voiceFactory(voiceOptions);
      if (call.continuation && options.eligibleContext) selectedHosts.set(call.id, live as GenerationVoiceSession);
      const held = heldInput; heldInput = []; heldInputBytes = 0;
      for (const entry of held) if (stopping || !feedLive(entry.payload)) { if (!stopping) void stop('input_audio_refused'); return; }
    }
    const attachSafely = () => {
      try { attach(); }
      catch (error) {
        try { store.update(call.id, { failure: safeFailure(error) }); } catch { /* The stop below still ends the call. */ }
        void stop('media_error');
      }
    };

    /** Drives the queued inbound admission while the greeting stream holds the leg (the signed
     * redirect loop used to drive it). Live attaches when the binding and its context exist. */
    async function admitInbound(): Promise<void> {
      // The continuation's own admission counts as the first poll.
      let polled = true;
      for (;;) {
        if (stopping || released || live) return;
        const current = store.get(call.id)!;
        if (inbound.mediaAllowed(current)) { attachSafely(); return; }
        if (!inbound.waitingMediaAllowed(current)) { releaseToTwiml(); return; }
        if (inbound.busy(call.id)) { await inbound.idle(call.id); continue; }
        if (polled) {
          polled = false;
          await new Promise<void>(resolve => { setTimeout(resolve, options.inboundAdmissionPollMs ?? INBOUND_ADMISSION_POLL_MS).unref(); });
          continue;
        }
        polled = true;
        try { await inbound.tick(call.id); }
        catch (error) {
          // The same admission recovers on the next poll; the original wait deadline still ends it.
          try { store.update(call.id, { failure: safeFailure(error) }); } catch { /* Retry regardless. */ }
        }
      }
    }
    /** Attaches Live when the answer route's Primary binding settles; a failed binding is spoken and ends the call. */
    function awaitOutboundBinding(): void {
      const binding = store.get(call.id)!.primary ? undefined : pendingBinds.get(call.id);
      if (!binding) { attachSafely(); return; }
      // A binding that lands after this stream began stopping is closed by the answer route,
      // which sees the endedAt that stop() records first.
      binding.then(() => { if (!stopping) attachSafely(); },
        () => { pendingBinds.delete(call.id); void stop('primary_unavailable'); });
    }

    active.set(call.id, { stop });
    ws.on('error', () => { if (!released) void stop('media_socket_error'); });
    ws.on('close', () => { if (!released) void stop('caller_disconnected'); });
    ws.on('message', raw => {
      if (stopping || released) return;
      try {
        const frame = JSON.parse(raw.toString());
        if (frame.event === 'start') {
          if (streamSid) throw new Error('duplicate_media_start');
          const current = store.get(call.id)!;
          if (call.verificationCallback && (!restricted || !restricted.privateMediaAllowed(current))) throw new Error('callback_media_not_admitted');
          // An inbound wait that ended before this frame (refusal, expiry) plays no greeting; its
          // admission loop hands the leg straight back to the continuation.
          const greets = !call.inbound || inbound.mediaAllowed(current) || inbound.waitingMediaAllowed(current);
          streamSid = mediaStart(frame.start, { accountSid: config.twilioAccountSid, callSid: call.callSid!, token: streamToken(config.controlToken, call.id, call.callSid!) });
          clearTimeout(startTimer);
          store.update(call.id, { streamSid, ...(managedNotification(call) ? {} : { status: 'in-progress' as const }),
            journey: { ...current.journey, streamStartedAt: current.journey?.streamStartedAt ?? new Date().toISOString() } });
          // The clip carries no account data, so it starts now, before admission or binding
          // finishes; Live's opening is later told it was heard.
          const clip = call.direction === 'inbound' ? options.inboundGreetingClip : options.greetingClip;
          if (clip && greets && greetingClipEligible(store.get(call.id)!)) startGreetingClip(clip); else openInput();
          if (call.inbound) void admitInbound();
          else awaitOutboundBinding();
        } else if (frame.event === 'media') {
          if (!streamSid || frame.streamSid !== streamSid || frame.media?.track !== 'inbound'
              || typeof frame.media.payload !== 'string' || frame.media.payload.length > 32768) throw new Error('invalid_input_audio');
          const bytes = Buffer.from(frame.media.payload, 'base64');
          inputBytes += bytes.length;
          if (inputBytes > (config.maxCallSeconds + 10) * 8000) throw new Error('input_duration_exceeded');
          if (inputOpenedAt === undefined) {
            // Spoken over the greeting: never delivered. An attached Live hears the same length
            // of silence instead, so its input clock stays aligned with the call.
            greetingInputBytes += bytes.length;
            if (live && !feedLive(Buffer.alloc(bytes.length, 0xff).toString('base64'))) void stop('input_audio_refused');
            return;
          }
          if (!speechSentRecorded && inputSpeechEnergy(bytes) > CALLER_SPEECH_ENERGY) callerSpeechEndedAt = Date.now();
          if (live) { if (!feedLive(frame.media.payload)) void stop('input_audio_refused'); return; }
          heldInput.push({ payload: frame.media.payload, bytes: bytes.length }); heldInputBytes += bytes.length;
          while (heldInputBytes > HELD_INPUT_MAX_BYTES) {
            const dropped = heldInput.shift()!;
            heldInputBytes -= dropped.bytes; droppedInputBytes += dropped.bytes;
          }
        } else if (frame.event === 'mark' && frame.streamSid === streamSid && typeof frame.mark?.name === 'string') {
          const previouslyPlayed = receipts.playedBytes, previouslyAudiblePlayed = receipts.audiblePlayedBytes;
          const previouslySpeechPlayed = receipts.speechPlayedBytes;
          receipts.acknowledge(frame.mark.name);
          if (audibleSentRecorded && !audiblePlaybackRecorded && receipts.audiblePlayedBytes > previouslyAudiblePlayed) {
            audiblePlaybackRecorded = true; recordAudibleTiming('firstAudiblePlaybackAcknowledgedAt');
          }
          if (speechSentRecorded && !speechPlaybackRecorded && receipts.speechPlayedBytes > previouslySpeechPlayed) {
            speechPlaybackRecorded = true; recordAudibleTiming('firstSpeechPlaybackAcknowledgedAt');
          }
          if (receipts.playedBytes > previouslyPlayed && !playbackRecorded) {
            const saved = store.get(call.id)!;
            store.update(call.id, { journey: { ...saved.journey, firstPlaybackConfirmedAt: new Date().toISOString() } });
            playbackRecorded = true;
          }
          // After this mark's own accounting, so released Live audio never inherits the clip's playback timing.
          if (greetingMarks.delete(frame.mark.name)) {
            if (!greetingPlaybackRecorded) { greetingPlaybackRecorded = true; recordAudibleTiming('greetingClipFirstPlaybackAcknowledgedAt'); }
            if (!greetingMarks.size && greeting && !greeting.sending) releaseGreetingHold('played');
          }
        } else if (frame.event === 'stop') {
          if (streamSid && frame.streamSid !== streamSid) throw new Error('stop_stream_mismatch');
          void stop('caller_hangup');
        }
      } catch (error) { store.update(call.id, { failure: safeFailure(error) }); void stop('media_error'); }
    });
  }

  let maintenance: Promise<void> | undefined;
  function maintain(): Promise<void> {
    if (!accepting) return Promise.resolve();
    if (maintenance) return maintenance;
    maintenance = (async () => {
      await callbacks.dispatchPending();
      for (const call of store.list()) {
        if (call.verificationCallback || call.restrictedSetup) {
          try { if (restricted) await restricted.maintain(call); } catch (error) { store.update(call.id, { failure: safeFailure(error) }); }
          continue;
        }
        if (active.has(call.id) || claimed.has(call.id)) continue;
        if (call.inbound) {
          try {
            if (!options.inboundAttachment) inbound.stopLocal(call.id, 'service_unavailable');
            if (call.mediaStartedAt) inbound.stopLocal(call.id, 'media_session_lost');
            await inbound.tick(call.id);
          } catch (error) { store.update(call.id, { failure: safeFailure(error) }); }
          finally {
            const current = store.get(call.id)!;
            if (current.inbound?.localStop) await endWaitingCall(current);
          }
          await inboundBilling.reconcile(call.id);
          continue;
        }
        // A terminal callback can be durable just before a process stops,
        // without reaching the first closePrimary write. Recover that exact
        // selected call after restart, while active media retains its own stop.
        if (call.primaryClosePending || (call.continuation && call.primary && !call.primaryClosed
          && providerFinished(call) && !active.has(call.id) && !claimed.has(call.id))) await closePrimary(call);
        if (!call.callSid || (providerFinished(call) && !needsHarkTiming(call)) || active.has(call.id) || claimed.has(call.id)
            || Date.now() - Date.parse(call.createdAt) < 60_000) continue;
        try {
          const observed = await provider.calls(call.callSid).fetch();
          const matches = call.direction === 'inbound'
            ? observed.from === call.number && observed.to === config.from
            : observed.to === call.number && observed.from === config.from;
          if (!matches) throw new Error('provider_call_identity_mismatch');
          // Media may have connected while the provider lookup was pending.
          if (active.has(call.id) || claimed.has(call.id)) {
            if (terminalCallStatuses.has(observed.status as CallStatus) && active.has(call.id)) {
              observeProviderRecord(call, observed, 'authenticated_fetch');
              await active.get(call.id)!.stop('provider_terminal');
            }
            continue;
          }
          const current = store.get(call.id)!;
          store.update(call.id, { endedAt: current.endedAt ?? new Date().toISOString() });
          if (terminalCallStatuses.has(observed.status as CallStatus)) {
            observeProviderRecord(call, observed, 'authenticated_fetch');
          } else if (providerFinished(current)) {
            // A delayed provider read cannot reopen a terminal call or add time.
            continue;
          } else {
            const ended = await provider.calls(call.callSid).update({ status: 'completed' });
            if (ended.status === 'completed') observeProviderRecord(call, ended, 'authenticated_update');
            else store.update(call.id, { status: 'unknown', endedAt: new Date().toISOString() });
          }
          await closePrimary(store.get(call.id)!);
        } catch (error) { store.update(call.id, { failure: safeFailure(error) }); }
      }
      await flushHark();
    })().finally(() => { maintenance = undefined; });
    return maintenance;
  }
  const maintenanceTimer = setInterval(() => {
    void maintain().catch(error => { console.error('[dexter-broker] Maintenance could not finish', { code: safeFailure(error).code }); });
  }, 10_000);
  maintenanceTimer.unref();

  async function close() {
    accepting = false;
    options.sms?.stop();
    startupContexts?.clear();
    preconnect?.close();
    // Freeze every saved wait before awaiting any in-flight begin/consume operation.
    for (const call of store.list()) if (call.inbound && !call.inbound.closure) inbound.stopLocal(call.id, 'service_shutdown');
    clearInterval(maintenanceTimer);
    server.close();
    await Promise.allSettled([...pendingRequests]);
    await options.sms?.close();
    await maintenance;
    await Promise.allSettled([...active.values()].map(call => call.stop('service_shutdown')));
    // The answer route finished long ago, but Primary may still be opening its binding. A binding
    // that lands now (accepting is false) is recorded and closed by its settlement, before the store closes.
    await Promise.allSettled([...bindSettlements]);
    for (const call of store.list()) if (call.verificationCallback || call.restrictedSetup) {
      if (restricted) await restricted.stop(call, 'service_shutdown').catch(() => {});
    }
    for (const call of store.list()) if (call.inbound && !call.restrictedSetup) {
      await endWaitingCall(store.get(call.id)!);
      await inbound.stop(call.id, 'service_shutdown').catch(error => { store.update(call.id, { failure: safeFailure(error) }); });
    }
    await inbound.drain();
    for (const id of sessionCalls) {
      const call = store.get(id);
      if (!call?.callSid || call.verificationCallback || call.restrictedSetup || providerFinished(call)) continue;
      try {
        const ended = await provider.calls(call.callSid).update({ status: 'completed' });
        if (ended.status === 'completed') observeProviderRecord(call, ended, 'authenticated_update');
        else store.update(id, { status: 'unknown', endedAt: new Date().toISOString() });
        await closePrimary(call);
      } catch (error) { store.update(id, { status: 'unknown', failure: safeFailure(error) }); }
    }
    await Promise.allSettled([...closingBindings.values()]);
    await flushHark();
    sockets.close();
    store.close();
  }
  return { server, store, close, maintain };
}
