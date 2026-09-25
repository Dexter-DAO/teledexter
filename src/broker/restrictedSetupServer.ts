import { createHmac } from 'node:crypto';
import type { IncomingMessage, ServerResponse } from 'node:http';
import Twilio from 'twilio';
import { WebSocket } from 'ws';
import type { BrokerConfig } from './config.js';
import { BrokerStore, type BrokerCall } from './store.js';
import { PrimaryApiError, type PrimaryApiClient } from './primaryApi.js';
import type { BrokerLiveSessionOptions } from '../live/types.js';
import { createVerificationCallbackClient, type VerificationCallbackClient } from './verificationCallbackClient.js';
import { createVerificationCallbackFlow, type VerificationCallbackProviderObservation } from './verificationCallbackFlow.js';
import { createVerificationCallbackKickoff, VERIFICATION_CALLBACK_KICKOFF_PATH } from './verificationCallbackKickoff.js';
import { type PhoneVerificationCallbackPermit } from './verificationCallbackContract.js';
import { createRestrictedSetupVoice, type RestrictedSetupVoice, type RestrictedSetupScope } from './restrictedSetupVoice.js';
import { createRestrictedSetupHost, type RestrictedSetupHostController } from './restrictedSetupHost.js';
import { createRestrictedCallerTurnFreezer } from './restrictedCallerTurn.js';
import { createVerificationCallbackChallenge } from './verificationCallbackChallenge.js';
import { prepareRestrictedSetup, stopRestrictedSetup, validateRestrictedSetupCall } from './restrictedSetupJournal.js';
import { callbackStatusReceipt, callbackEndedReceipt, deliverCallbackReceipt, verificationCallbackProviderTiming, type CallbackVoiceReceipt } from './verificationCallbackReceipts.js';
import { changeRestrictedMedia, type RestrictedMedia } from './restrictedMedia.js';
import { authorizedControl, callTwiml, inputSpeechEnergy, mediaStart, PlaybackReceipts, streamToken, verifyTwilio } from './phone.js';

type Active = { stop(reason: string): Promise<void> };
export interface RestrictedSetupServerOptions {
  config: BrokerConfig; store: BrokerStore; primary: PrimaryApiClient;
  provider: ReturnType<typeof Twilio>; voiceFactory: (options: BrokerLiveSessionOptions) => ReturnType<typeof import('../live/session.js').createBrokerLiveSession>;
  model: string; api?: VerificationCallbackClient;
  active: Map<string, Active>; claimed: Set<string>; accepting(): boolean;
  closePrimary(call: BrokerCall): Promise<void>;
  receiptTransport?: { apiBaseUrl: string; signingSecret: string; fetchImpl?: typeof fetch };
}
const uuid = '[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}';
const terminal = new Set(['completed', 'busy', 'failed', 'no-answer', 'canceled']);
function fail(): never { throw new Error('restricted_phone_unavailable'); }
class RestrictedBodyTooLarge extends Error {}
const endTwiml = () => '<Response><Hangup/></Response>';
function sendJson(response: ServerResponse, status: number, value: unknown) {
  response.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' }); response.end(JSON.stringify(value));
}
function sendXml(response: ServerResponse, xml: string) {
  response.writeHead(200, { 'content-type': 'text/xml', 'cache-control': 'no-store' }); response.end(xml);
}
async function read(request: IncomingMessage, max: number): Promise<Buffer> {
  const chunks: Buffer[] = []; let size = 0;
  for await (const chunk of request) { const bytes = Buffer.from(chunk); size += bytes.length; if (size > max) throw new RestrictedBodyTooLarge(); chunks.push(bytes); }
  return Buffer.concat(chunks);
}
/** Mounted by the explicit server option; main supplies it only when the
 * coordinated verification model setting is present. */
export function createRestrictedSetupServer(options: RestrictedSetupServerOptions) {
  const { config, store, provider, active, claimed } = options;
  if (!/^[A-Za-z0-9_.:-]{1,200}$/.test(options.model)) fail();
  const receiptTransport = options.receiptTransport ?? { apiBaseUrl: config.harkApiBaseUrl ?? '', signingSecret: config.harkCallbackSigningSecret ?? '' };
  const receiptOrigin = new URL(receiptTransport.apiBaseUrl);
  if (receiptOrigin.protocol !== 'http:' || !['127.0.0.1', 'localhost', '[::1]'].includes(receiptOrigin.hostname)
    || receiptOrigin.pathname !== '/' || receiptOrigin.username || receiptOrigin.password || receiptOrigin.search || receiptOrigin.hash
    || receiptTransport.signingSecret.trim().length < 32) fail();
  const flushing = new Map<string, Promise<void>>();
  const privateWaitDeadlines = new Map<string, ReturnType<typeof setTimeout>>();
  function clearPrivateWaitDeadline(id: string) {
    const timer = privateWaitDeadlines.get(id);
    if (timer !== undefined) { clearTimeout(timer); privateWaitDeadlines.delete(id); }
  }
  const api = options.api ?? createVerificationCallbackClient({ apiBaseUrl: config.apiBaseUrl, phoneServiceSecret: config.phoneServiceSecret });
  const setupToken = (call: BrokerCall) => streamToken(config.controlToken + ':restricted', call.id, call.callSid!);
  const actors = new Map<string, { host: RestrictedSetupHostController; scope(): RestrictedSetupScope | undefined;
    challenge: ReturnType<typeof createVerificationCallbackChallenge>; protectedReady(): boolean; recover(): Promise<void>; stop(reason: string): Promise<void> }>();
  function get(id: string) { const call = store.get(id); if (!call?.callSid) fail(); return call; }
  function media(call: BrokerCall): RestrictedMedia | undefined { return call.verificationCallback?.media ?? call.restrictedSetup?.media; }
  function saveMedia(id: string, value: RestrictedMedia) {
    const call = get(id);
    if (call.verificationCallback) return store.recordVerificationCallback(id, { ...call.verificationCallback, media: value });
    if (call.restrictedSetup) return store.recordRestrictedSetup(id, { ...call.restrictedSetup, media: value });
    fail();
  }
  function patchMedia(id: string, patch: Partial<RestrictedMedia>) {
    const saved = media(get(id)); if (!saved) fail(); return saveMedia(id, changeRestrictedMedia(saved, patch));
  }
  function observation(value: { sid: string; accountSid: string; from: string; to: string; status: string }): VerificationCallbackProviderObservation {
    if (!['queued', 'ringing', 'in-progress', ...terminal].includes(value.status)) fail();
    const timing = verificationCallbackProviderTiming(value);
    return { accountSid: value.accountSid, from: value.from, to: value.to, callSid: value.sid,
      status: value.status as VerificationCallbackProviderObservation['status'], ...(timing ? { timing } : {}) };
  }
  function flushReceipts(id: string): Promise<void> {
    const old = flushing.get(id); if (old) return old;
    const work = (async () => {
      for (;;) {
        const journal = store.get(id)?.verificationCallback; if (!journal) fail();
        const index = journal.receipts?.findIndex(value => !value.acknowledgedAt) ?? -1;
        if (index < 0 || !journal.receipts) return;
        const saved = journal.receipts[index], accepted = await deliverCallbackReceipt({ ...receiptTransport, value: saved });
        if (!accepted) return;
        const current = store.get(id)?.verificationCallback; if (!current?.receipts || JSON.stringify(current.receipts[index]) !== JSON.stringify(saved)) fail();
        const receipts = [...current.receipts]; receipts[index] = accepted;
        store.recordVerificationCallback(id, { ...current, receipts });
      }
    })();
    flushing.set(id, work); void work.finally(() => { if (flushing.get(id) === work) flushing.delete(id); }).catch(() => {}); return work;
  }
  async function billed(permit: PhoneVerificationCallbackPermit, value: VerificationCallbackProviderObservation,
    _source: 'create' | 'signed_callback' | 'fetch' | 'hangup') {
    const journal = get(permit.request.requestId).verificationCallback; if (!journal || journal.provider?.callSid !== value.callSid) fail();
    const receipts: CallbackVoiceReceipt[] = [...(journal.receipts ?? [])];
    const status = callbackStatusReceipt(permit, value.callSid, journal.providerTerminal?.status ?? value.status);
    if (!receipts.some(item => item.kind === 'status' && status.kind === 'status' && item.body.state === status.body.state)) receipts.push(status);
    const timing = journal.endedTiming ?? (journal.providerTerminal ? value.timing : undefined);
    if (timing && !receipts.some(item => item.kind === 'ended')) receipts.push(callbackEndedReceipt(permit, value.callSid, timing));
    store.recordVerificationCallback(permit.request.requestId, { ...journal, ...(timing ? { endedTiming: timing } : {}), receipts });
    await flushReceipts(permit.request.requestId); return value;
  }
  const flow = createVerificationCallbackFlow({ store, api, providerTimeoutMs: 15000,
    verifyAnswer(proof) { const route = new URL(proof.url).pathname;
      return proof.url === config.publicOrigin + route && !!verifyTwilio(config, proof.signature, route, proof.params); },
    provider: {
      async create({ permit }) {
        if (!options.accepting() || permit.providerAccountSid !== config.twilioAccountSid || permit.from !== config.from
          || new URL(permit.answerUrl).origin !== config.publicOrigin || new URL(permit.responseUrl).origin !== config.publicOrigin) fail();
        const result = observation(await provider.calls.create({ from: permit.from, to: permit.to, url: permit.answerUrl,
          method: 'POST', statusCallback: `${config.publicOrigin}/broker/status/${permit.request.requestId}`,
          statusCallbackMethod: 'POST', statusCallbackEvent: ['initiated', 'ringing', 'answered', 'completed'],
          timeLimit: permit.maxDurationSeconds }));
        flow.observeProvider(permit.request, result, 'create');
        return billed(permit, result, 'create');
      },
      async hangup(sid) {
        const call = store.bySid(sid); if (!call?.verificationCallback) fail();
        const result = observation(await provider.calls(sid).update({ status: 'completed' }));
        flow.observeProvider(call.verificationCallback.permit.request, result, 'hangup');
        return billed(call.verificationCallback.permit, result, 'hangup');
      },
    } });
  const kickoff = createVerificationCallbackKickoff(config.controlToken, { kickoff: ref => flow.kickoff(ref) });
  function setupTwiml(call: BrokerCall) {
    const value = new Twilio.twiml.VoiceResponse();
    const stream = value.connect().stream({ url: `${config.publicOrigin.replace(/^https:/, 'wss:')}/broker/setup-media/${call.id}` });
    stream.parameter({ name: 'token', value: setupToken(call) }); value.hangup(); return value.toString();
  }
  function eligible(call: BrokerCall): boolean {
    if (!options.accepting() || !call.callSid || call.endedAt || call.primary) return false;
    const saved = media(call); if (!saved || saved.stoppedAt || saved.protectedRequestedAt || saved.privateRequestedAt
      || Date.now() >= Date.parse(saved.deadlineAt)) return false;
    if (call.verificationCallback) return call.verificationCallback.remote?.state === 'answered'
      && !call.verificationCallback.stopRequestedAt && !call.verificationCallback.providerTerminal;
    try { validateRestrictedSetupCall(call); return !!call.restrictedSetup && !call.restrictedSetup.stoppedAt; } catch { return false; }
  }
  function initialMedia(call: BrokerCall) {
    if (media(call)) return call;
    const callback = call.verificationCallback;
    const deadlineAt = callback?.answer ? new Date(Math.min(Date.parse(callback.permit.expiresAt),
      Date.parse(callback.answer.startedAt) + callback.permit.maxDurationSeconds * 1000)).toISOString() : call.restrictedSetup?.deadlineAt;
    if (!deadlineAt) fail();
    return saveMedia(call.id, { admittedAt: new Date().toISOString(), deadlineAt });
  }
  async function inboundResponse(raw: BrokerCall): Promise<string | undefined> {
    if (!raw.inbound || raw.primary || raw.inbound.approvalRef || raw.inbound.consumeAttemptedAt || !raw.inbound.admissionRefusal
      || !raw.inbound.localStop || raw.endedAt || !options.accepting()) return;
    if (raw.inbound.remote?.state !== 'cancelled' && raw.inbound.remote?.state !== 'expired') return;
    const originalDeadline = raw.inbound.waitUntil;
    let call = raw;
    if (!call.restrictedSetup) call = store.recordRestrictedSetup(call.id, prepareRestrictedSetup(call,
      new Date().toISOString(), originalDeadline));
    call = initialMedia(call);
    return eligible(call) ? setupTwiml(call) : endTwiml();
  }
  async function handle(request: IncomingMessage, response: ServerResponse): Promise<boolean> {
    const route = request.url ?? '/';
    if (route === VERIFICATION_CALLBACK_KICKOFF_PATH) {
      if (request.method !== 'POST') { sendJson(response, 405, { error: { code: 'callback_invalid', retryable: false } }); return true; }
      if (!options.accepting()) { sendJson(response, 503, { error: { code: 'callback_unavailable', retryable: true } }); return true; }
      // Authenticate before reading a control body, matching the frozen handler.
      if (!['127.0.0.1', '::1', '::ffff:127.0.0.1'].includes(request.socket.remoteAddress ?? '')
        || !authorizedControl(request.headers.authorization, config.controlToken)) {
        sendJson(response, 403, { error: { code: 'callback_owner_required', retryable: false } }); return true;
      }
      try { const result = await kickoff({ remoteAddress: request.socket.remoteAddress, authorization: request.headers.authorization,
        contentType: typeof request.headers['content-type'] === 'string' ? request.headers['content-type'] : undefined, body: await read(request, 4096) });
        sendJson(response, result.status, result.body);
      } catch { sendJson(response, 413, { error: { code: 'callback_invalid', retryable: false } }); }
      return true;
    }
    const answer = route.match(new RegExp(`^/broker/verification-callback/(${uuid})$`));
    const protectedForm = route.match(new RegExp(`^/broker/verification-callback-response/(${uuid})$`));
    const status = route.match(new RegExp(`^/broker/status/(${uuid})$`));
    const call = store.get((answer ?? protectedForm ?? status)?.[1] ?? '');
    if (!answer && !protectedForm && !(status && call?.verificationCallback)) return false;
    if (request.method !== 'POST' || !call?.verificationCallback) { sendJson(response, 404, { error: 'not_found' }); return true; }
    if (typeof request.headers['content-type'] !== 'string'
      || !/^application\/x-www-form-urlencoded(?:\s*;.*)?$/i.test(request.headers['content-type'])) {
      sendJson(response, 415, { error: 'invalid_callback_form' }); return true;
    }
    try {
      const bytes = await read(request, answer || protectedForm ? 262144 : 64000);
      const text = new TextDecoder('utf-8', { fatal: true }).decode(bytes), params: Record<string, string> = {};
      for (const [key, value] of new URLSearchParams(text)) { if (Object.hasOwn(params, key)) fail(); params[key] = value; }
      const proof = verifyTwilio(config, request.headers['x-twilio-signature'] as string | undefined, route, params);
      if (!proof) { sendJson(response, 403, { error: 'invalid_provider_callback' }); return true; }
      const permit = call.verificationCallback.permit;
      if (params.AccountSid !== permit.providerAccountSid || params.From !== permit.from || params.To !== permit.to
        || params.Direction !== 'outbound-api' || (call.callSid && params.CallSid !== call.callSid)) {
        sendJson(response, 403, { error: 'call_identity_mismatch' }); return true;
      }
      if (status) {
        const observed = observation({ sid: params.CallSid, accountSid: params.AccountSid, from: params.From, to: params.To, status: params.CallStatus });
        const savedObservation = flow.observeProvider(permit.request, observed, 'signed_callback');
        const ending = savedObservation.stopRequestedAt && !savedObservation.providerTerminal ? flow.endLeg(permit.request) : Promise.resolve();
        void ending.catch(() => {});
        // Gate media immediately on a terminal observation; receipt HTTP cannot delay it.
        const closing = terminal.has(observed.status) ? (async () => {
          store.update(call.id, { status: observed.status, endedAt: call.endedAt ?? new Date().toISOString() });
          await active.get(call.id)?.stop('provider_terminal'); await options.closePrimary(get(call.id));
        })() : Promise.resolve();
        void closing.catch(() => {});
        await billed(permit, observed, 'signed_callback'); await ending; await closing;
        sendJson(response, 200, { ok: true }); return true;
      }
      if (!options.accepting()) { sendXml(response, endTwiml()); return true; }
      if (answer) {
        if (media(call)?.streamSid || media(call)?.privateRequestedAt) {
          sendJson(response, 409, { error: 'media_already_started' }); return true;
        }
        const answered = observation({ sid: params.CallSid, accountSid: params.AccountSid, from: params.From, to: params.To, status: params.CallStatus });
        if (answered.status !== 'in-progress') fail();
        flow.observeProvider(permit.request, answered, 'signed_callback');
        const billing = billed(permit, answered, 'signed_callback');
        void billing.catch(() => {});
        await flow.answer(permit.request, proof); await billing;
        const current = initialMedia(get(call.id));
        sendXml(response, eligible(current) ? setupTwiml(current) : endTwiml()); return true;
      }
      const actor = actors.get(call.id), scope = actor?.scope();
      if (!actor || !scope || !actor.protectedReady()) fail();
      actor.challenge.capture(proof); await actor.host.completeProtectedInput(scope);
      sendXml(response, endTwiml()); return true;
    } catch (error) {
      if (error instanceof RestrictedBodyTooLarge) { sendJson(response, 413, { error: { code: 'callback_invalid', retryable: false } }); return true; }
      if (answer && error instanceof PrimaryApiError && error.retryable === false
        && ((error.status === 403 && error.remoteCode === 'callback_consent_required')
          || (error.status === 410 && error.remoteCode === 'callback_expired'))) {
        await flow.stop(call.verificationCallback.permit.request).catch(() => {}); sendXml(response, endTwiml()); return true;
      }
      // Form bodies, codes and signatures never reach generic safeFailure logging.
      sendJson(response, 503, { error: { code: 'callback_unavailable', retryable: true } }); return true;
    }
  }
  async function stopSaved(call: BrokerCall, reason: string) {
    clearPrivateWaitDeadline(call.id);
    const saved = store.get(call.id); if (!saved) fail();
    const savedMedia = media(saved);
    // A fully stopped original leg has no callback or Primary work to recover.
    // Maintenance and shutdown may revisit it after its host has drained.
    if (saved.restrictedSetup?.stoppedAt && saved.endedAt && terminal.has(saved.status)
      && (!savedMedia || savedMedia.stoppedAt) && !saved.primaryClosePending
      && !active.has(saved.id) && !claimed.has(saved.id)) {
      validateRestrictedSetupCall(saved);
      return;
    }
    if (savedMedia && !savedMedia.stoppedAt) patchMedia(call.id, { stoppedAt: new Date().toISOString() });
    if (saved.verificationCallback) {
      if (reason === 'provider_terminal') await flow.recover(saved.verificationCallback.permit.request).catch(() => {});
      else await flow.stop(saved.verificationCallback.permit.request);
    } else if (saved.restrictedSetup) {
      store.recordRestrictedSetup(call.id, stopRestrictedSetup(saved, new Date().toISOString()));
      if (reason !== 'provider_terminal') await provider.calls(saved.callSid!).update({ status: 'completed' });
    }
    store.update(call.id, { endedAt: saved.endedAt ?? new Date().toISOString() });
    await options.closePrimary(store.get(call.id)!);
  }
  function connect(ws: WebSocket, initial: BrokerCall) {
    let streamSid: string | undefined, sessionId: string | undefined, inputBytes = 0, speechThroughMs: number | undefined, speechAt: number | undefined;
    let stopping = false, detached = false, protectedReady = false, voice: RestrictedSetupVoice | undefined;
    let stopPromise: Promise<void> | undefined;
    const receipts = new PlaybackReceipts(), listeners = new Set<() => void>(), initialClock = media(initial);
    if (!initialClock) { ws.close(); claimed.delete(initial.id); return; }
    const clock = initialClock;
    const deadline = setTimeout(() => { void stop('duration_limit'); }, Math.max(1, Date.parse(clock.deadlineAt) - Date.now()));
    const starting = setTimeout(() => { void stop('media_start_timeout'); }, 8000);
    const scope = () => sessionId ? { callId: initial.id, callSid: initial.callSid!, sessionId } : undefined;
    const isCurrent = (value: RestrictedSetupScope) => !stopping && !detached && (sessionId === undefined || value.sessionId === sessionId)
      && value.callId === initial.id && value.callSid === initial.callSid && Date.now() < Date.parse(clock.deadlineAt);
    const frontier = () => ({ sessionId: sessionId ?? '', audioThroughMs: speechThroughMs, observedSpeechAtMs: speechAt });
    const challenge = createVerificationCallbackChallenge({ journal() {
      const j = get(initial.id).verificationCallback; if (!j) fail(); return j;
    }, verifySignature(proof) { return proof.url === config.publicOrigin + new URL(proof.url).pathname
      && !!verifyTwilio(config, proof.signature, new URL(proof.url).pathname, proof.params); }, protectedInputActive: () => protectedReady && !stopping });
    let freezer: ReturnType<typeof createRestrictedCallerTurnFreezer> | undefined;
    const host = createRestrictedSetupHost({ store, api, flow,
      receiptSecret: createHmac('sha256', config.phoneServiceSecret).update('dexter.broker.restricted-turn/v1').digest('hex'),
      isCurrent, frontier, freeze: (_scope, request, channel, used) => freezer?.freeze(request, channel, used) ?? Promise.resolve(undefined),
      takeChallenge(value, ref) { return challenge.take(ref, value.callSid); },
      isProtectedInputCurrent: value => !stopping && protectedReady && value.callId === initial.id && value.callSid === initial.callSid && value.sessionId === sessionId,
      async requestProtectedInput(value) {
        if (!isCurrent(value) || !voice) fail();
        const call = get(initial.id), j = call.verificationCallback; if (!j || j.permit.purpose !== 'enrollment') fail();
        patchMedia(call.id, { protectedRequestedAt: new Date().toISOString() }); detached = true;
        await voice.suspendForSensitiveInput();
        if (stopping || Date.now() >= Date.parse(clock.deadlineAt)) fail();
        protectedReady = true;
        const twiml = new Twilio.twiml.VoiceResponse();
        const gather = twiml.gather({ input: ['dtmf'], numDigits: 8, action: j.permit.responseUrl, method: 'POST',
          timeout: Math.max(1, Math.min(30, Math.ceil((Date.parse(clock.deadlineAt) - Date.now()) / 1000))) });
        gather.say('Enter the eight-digit code shown in your Dexter account.'); twiml.hangup();
        patchMedia(call.id, { protectedDispatchedAt: new Date().toISOString() });
        const result = observation(await provider.calls(call.callSid!).update({ twiml: twiml.toString() })); flow.observeProvider(j.permit.request, result, 'fetch'); await billed(j.permit, result, 'fetch');
        if (ws.readyState === WebSocket.OPEN) ws.close();
      }, stopOriginal() { void stop('caller_declined'); },
    });
    actors.set(initial.id, { host, scope, challenge, protectedReady: () => protectedReady, stop,
      async recover() {
        const j = get(initial.id).verificationCallback;
        if (!j?.completion || stopping) return;
        if (!j.completion.observation || j.completion.observation.state === 'absent') await flow.recover(j.permit.request);
        const current = get(initial.id).verificationCallback;
        if (current?.completion?.observation?.state === 'admitted') await enterPrivate();
        else if (current?.completion?.observation && current.completion.observation.state !== 'absent') await stop('verification_finished');
      } });
    async function enterPrivate() {
      const call = get(initial.id), j = call.verificationCallback;
      if (stopping || detached || !voice || !j || j.completion?.observation?.state !== 'admitted') return;
      patchMedia(call.id, { privateRequestedAt: new Date().toISOString() }); detached = true;
      await voice.close();
      await flow.privateCall(j.permit.request, options.primary, () => detached && !stopping);
      if (stopping || !options.accepting() || Date.now() >= Date.parse(clock.deadlineAt)) return stop('private_admission_stopped');
      patchMedia(call.id, { privateDispatchedAt: new Date().toISOString() });
      // Release this retired attachment before provider update can open the new socket.
      clearTimeout(starting); challenge.clear(); actors.delete(initial.id);
      privateWaitDeadlines.set(initial.id, deadline);
      if (active.get(initial.id) === activeHandle) { active.delete(initial.id); claimed.delete(initial.id); }
      const result = observation(await provider.calls(call.callSid!).update({ twiml: callTwiml(config.publicOrigin, call.id,
        streamToken(config.controlToken, call.id, call.callSid!)) }));
      flow.observeProvider(j.permit.request, result, 'fetch'); await billed(j.permit, result, 'fetch');
      if (ws.readyState === WebSocket.OPEN) ws.close();
    }
    function stop(reason: string): Promise<void> {
      if (stopPromise) return stopPromise;
      clearPrivateWaitDeadline(initial.id);
      const successor = active.get(initial.id);
      if (detached && successor && successor !== activeHandle) { clearTimeout(deadline); return successor.stop(reason); }
      stopping = true; clearTimeout(deadline); clearTimeout(starting);
      challenge.clear(); const value = scope(); if (value) host.abandonProtectedInput(value);
      // Provider termination starts before waiting on Live's transcript drain.
      const ending = stopSaved(get(initial.id), reason); const retiring = voice?.close();
      stopPromise = Promise.allSettled([ending, retiring]).then(() => { if (ws.readyState === WebSocket.OPEN) ws.close(); })
        .finally(() => { actors.delete(initial.id); if (active.get(initial.id) === activeHandle) { active.delete(initial.id); claimed.delete(initial.id); } }); return stopPromise;
    }
    const activeHandle = { stop };
    active.set(initial.id, activeHandle);
    ws.on('error', () => { if (!detached) void stop('media_socket_error'); });
    ws.on('close', () => { if (!detached) void stop('caller_disconnected'); });
    ws.on('message', raw => {
      if (stopping || detached) return;
      try {
        const frame = JSON.parse(raw.toString());
        if (frame.event === 'start') {
          if (streamSid || !eligible(get(initial.id))) fail();
          streamSid = mediaStart(frame.start, { accountSid: config.twilioAccountSid, callSid: initial.callSid!, token: setupToken(initial) });
          if (media(get(initial.id))?.streamSid) fail();
          patchMedia(initial.id, { streamSid, startedAt: new Date().toISOString() }); clearTimeout(starting);
          voice = createRestrictedSetupVoice({ apiKey: config.apiKey, callId: initial.id, callSid: initial.callSid!, setupModel: options.model,
            voice: config.voice, deadlineAtMs: Date.parse(clock.deadlineAt), hostTimeoutMs: 30000, voiceFactory: options.voiceFactory,
            host: { ...host, async handleCaller(...args) { const result = await host.handleCaller(...args);
              if (result.phase === 'complete' && get(initial.id).verificationCallback?.permit.purpose === 'account_access')
                queueMicrotask(() => { void enterPrivate().catch(() => stop('private_admission_failed')); });
              return result; } },
            onReady(value) { sessionId = value.sessionId; patchMedia(initial.id, { sessionId });
              freezer = createRestrictedCallerTurnFreezer({ callId: initial.id, sessionId, mediaStartedAtMs: Date.parse(media(get(initial.id))?.startedAt ?? clock.admittedAt),
                deadlineAtMs: Date.parse(clock.deadlineAt), stabilityMs: 300, maxWaitMs: 30000, frontier,
                isCurrent: () => { const current = scope(); return current !== undefined && isCurrent(current); }, subscribeSpeech(fn) { listeners.add(fn); return () => { listeners.delete(fn); }; } }); },
            onAudio(payload) {
              if (stopping || detached || ws.readyState !== WebSocket.OPEN || !streamSid) return;
              if (ws.bufferedAmount > 256000 || receipts.pendingCount > 300 || receipts.backlogMs > 6000) { void stop('audio_backpressure'); return; }
              ws.send(JSON.stringify({ event: 'media', streamSid, media: { payload } }));
              ws.send(JSON.stringify({ event: 'mark', streamSid, mark: { name: receipts.sent(Buffer.from(payload, 'base64').length) } }));
            }, onPlaybackClear() { receipts.clear(); if (ws.readyState === WebSocket.OPEN && streamSid) ws.send(JSON.stringify({ event: 'clear', streamSid })); },
            onStopRequested(reason) { void stop(reason); }, onClosed(_result, intent) { if (!detached && !stopping && intent !== 'sensitive_input') void stop('voice_closed'); },
          });
        } else if (frame.event === 'media') {
          if (!streamSid || frame.streamSid !== streamSid || frame.media?.track !== 'inbound' || typeof frame.media.payload !== 'string'
            || frame.media.payload.length > 32768) fail();
          const bytes = Buffer.from(frame.media.payload, 'base64');
          if (!bytes.length || bytes.toString('base64') !== frame.media.payload) fail();
          inputBytes += bytes.length; if (inputBytes > (Date.parse(clock.deadlineAt) - Date.parse(clock.admittedAt) + 1000) * 8) fail();
          if (inputSpeechEnergy(bytes) > 0.012) { speechThroughMs = inputBytes / 8; speechAt = Date.now(); for (const fn of listeners) fn(); }
          if (voice?.appendAudio(frame.media.payload) === false) void stop('input_audio_refused');
        } else if (frame.event === 'mark' && frame.streamSid === streamSid) receipts.acknowledge(frame.mark?.name);
        else if (frame.event === 'stop') { if (streamSid && frame.streamSid !== streamSid) fail(); void stop('caller_hangup'); }
      } catch { void stop('restricted_media_error'); }
    });
  }
  async function maintain(call: BrokerCall): Promise<boolean> {
    if (!call.verificationCallback && !call.restrictedSetup) return false;
    if (active.has(call.id) || claimed.has(call.id)) {
      if (call.verificationCallback) await flushReceipts(call.id);
      await actors.get(call.id)?.recover(); return true;
    }
    if (call.primaryClosePending) await options.closePrimary(call);
    if (call.verificationCallback) await flushReceipts(call.id);
    if (call.verificationCallback) {
      await flow.recover(call.verificationCallback.permit.request).catch(() => {});
      if (call.callSid && (!call.verificationCallback.providerTerminal || !call.verificationCallback.endedTiming)) {
        const value = observation(await provider.calls(call.callSid).fetch()); flow.observeProvider(call.verificationCallback.permit.request, value, 'fetch');
        await billed(call.verificationCallback.permit, value, 'fetch');
        if (terminal.has(value.status)) { store.update(call.id, { status: value.status, endedAt: call.endedAt ?? new Date().toISOString() }); await options.closePrimary(get(call.id)); return true; }
      }
    }
    const current = store.get(call.id)!;
    const clock = media(current);
    const lostMedia = clock?.privateDispatchedAt ? (current.mediaStartedAt !== undefined
      || Date.now() >= Math.min(Date.parse(clock.deadlineAt), Date.parse(clock.privateDispatchedAt) + 30000)) : !!clock?.streamSid;
    if ((current.callSid || current.verificationCallback) && (current.endedAt || !options.accepting() || (clock && (lostMedia || Date.now() >= Date.parse(clock.deadlineAt)))
      || (!clock && current.verificationCallback && Date.now() >= Date.parse(current.verificationCallback.permit.expiresAt))))
      await stopSaved(current, 'restricted_session_lost');
    return true;
  }
  function privateMediaAllowed(call: BrokerCall): boolean {
    const j = call.verificationCallback, clock = j?.media;
    return !!(j && call.primary && j.completion?.observation?.state === 'admitted' && clock?.privateDispatchedAt
      && !call.mediaStartedAt && !call.endedAt && !j.stopRequestedAt && !j.providerTerminal && !clock.stoppedAt
      && Date.now() < Math.min(Date.parse(clock.deadlineAt), Date.parse(clock.privateDispatchedAt) + 30000));
  }
  async function reconcile(call: BrokerCall) {
    if (!call.callSid) fail();
    const observed = await provider.calls(call.callSid).fetch();
    if (call.verificationCallback) {
      const value = observation(observed); flow.observeProvider(call.verificationCallback.permit.request, value, 'fetch');
      const closing = terminal.has(value.status) ? (async () => {
        store.update(call.id, { status: value.status, endedAt: call.endedAt ?? new Date().toISOString() });
        const host = active.get(call.id); if (host) await host.stop('provider_terminal'); else await options.closePrimary(get(call.id));
      })() : Promise.resolve();
      void closing.catch(() => {});
      await billed(call.verificationCallback.permit, value, 'fetch'); await closing;
    } else {
      if (observed.sid !== call.callSid || observed.accountSid !== config.twilioAccountSid || observed.from !== call.number
        || observed.to !== config.from || observed.direction !== 'inbound') fail();
      if (terminal.has(observed.status)) {
        store.update(call.id, { status: observed.status, endedAt: call.endedAt ?? new Date().toISOString() });
        const host = active.get(call.id); if (host) await host.stop('provider_terminal'); else await stopSaved(call, 'provider_terminal');
      }
    }
  }
  // The ordinary private host calls this before its own transcript/capture closeout.
  // This function must not close the Primary binding while that host is draining.
  async function endPrivateProvider(call: BrokerCall, reason: string) {
    clearPrivateWaitDeadline(call.id);
    const saved = get(call.id); if (!saved.verificationCallback) fail();
    const clock = media(saved); if (clock && !clock.stoppedAt) patchMedia(call.id, { stoppedAt: new Date().toISOString() });
    if (reason !== 'provider_terminal') await flow.stop(saved.verificationCallback.permit.request);
    else await flushReceipts(call.id);
  }
  return { handle, inboundResponse, eligible, connect, maintain, flow, privateMediaAllowed, reconcile, endPrivateProvider,
    async stop(call: BrokerCall, reason: string) { const activeCall = active.get(call.id); if (activeCall) await activeCall.stop(reason); else await stopSaved(call, reason); },
  };
}
