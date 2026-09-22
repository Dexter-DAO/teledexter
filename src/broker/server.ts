import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { createHash } from 'node:crypto';
import { WebSocketServer, WebSocket } from 'ws';
import Twilio from 'twilio';
import { createBrokerLiveSession, type BrokerLiveSession, type BrokerLiveSessionOptions } from '../live/session.js';
import type { LiveTranscriptSnapshot } from '../live/session.js';
import { createPrimaryApiClient, type PrimaryApiClient } from './primaryApi.js';
import { createPrimaryBackend } from './backend.js';
import { createCallbackFlow } from './callback.js';
import { type BrokerConfig, isPhoneNumber } from './config.js';
import { BrokerStore, type BrokerCall, type CallStatus, terminalCallStatuses } from './store.js';
import { brokerInstructions, brokerHistory } from './instructions.js';
import { probeLive } from './preflight.js';
import { authorizedControl, callTwiml, fallbackTwiml, inputSpeechEnergy, mediaStart, PlaybackReceipts, safeFailure, streamToken, verifyTwilio, type ProviderCallProof } from './phone.js';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const CALL_SID = /^CA[0-9a-f]{32}$/i;
const providerStates = new Set<CallStatus>(['queued', 'ringing', 'in-progress', 'completed', 'busy', 'no-answer', 'canceled', 'failed']);

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
  voiceOpening: call.voiceOpening,
  primaryClosePending: call.primaryClosePending ?? false, transcriptCapture: call.transcriptCapture,
  callback: call.callback,
  metrics: call.metrics, delegationCount: Object.keys(call.delegations).length,
});

export interface BrokerServerOptions {
  config: BrokerConfig;
  store?: BrokerStore;
  primary?: PrimaryApiClient;
  provider?: ReturnType<typeof Twilio>;
  voiceFactory?: (options: BrokerLiveSessionOptions) => BrokerLiveSession;
  probe?: typeof probeLive;
}

export function createBrokerServer(options: BrokerServerOptions) {
  const { config } = options;
  const store = options.store ?? new BrokerStore(config.stateDir);
  const primary = options.primary ?? createPrimaryApiClient({ apiBaseUrl: config.apiBaseUrl, phoneServiceSecret: config.phoneServiceSecret });
  const provider = options.provider ?? Twilio(config.twilioAccountSid, config.twilioAuthToken, { autoRetry: false, timeout: 15_000 });
  const voiceFactory = options.voiceFactory ?? createBrokerLiveSession;
  const probe = options.probe ?? probeLive;
  const active = new Map<string, { stop(reason: string): Promise<void> }>();
  const dialing = new Set<string>();
  const claimed = new Set<string>();
  const pendingRequests = new Set<Promise<void>>();
  const sessionCalls = new Set<string>();
  const closingBindings = new Map<string, Promise<void>>();
  let accepting = true;
  const callbacks = createCallbackFlow({ config, store,
    fetchCall: sid => provider.calls(sid).fetch(),
    requestOutbound: input => requestOutbound(input).then(result => result.call),
  });

  function closePrimary(call: BrokerCall): Promise<void> {
    if (!call.primary) return Promise.resolve();
    const existing = closingBindings.get(call.id);
    if (existing) return existing;
    store.update(call.id, { primaryClosePending: true });
    const pending = (async () => {
      // Closing admission first would make an unacknowledged, never-accepted turn
      // impossible to recover. Reconcile the exact journal before closing it.
      const current = store.get(call.id)!;
      if (current.callSid && current.liveSessionId) {
        const recovery = createPrimaryBackend({ callId: current.id, callSid: current.callSid,
          bindingId: current.primary!.bindingId, api: primary, store });
        const flushed = await recovery.flushPendingCaptures();
        let pendingCount = flushed.pending;
        let unsupported = flushed.unsupported;
        if (current.transcriptCapture?.state === 'pending' && current.transcript) {
          const captured = await recovery.captureCompletedTurns(current.transcript as LiveTranscriptSnapshot, current.transcriptCapture);
          pendingCount = captured.pending;
          unsupported = [...new Set([...unsupported, ...captured.unsupported])];
        }
        if (current.transcriptCapture) store.update(call.id, { transcriptCapture: {
          ...current.transcriptCapture, state: pendingCount ? 'pending' : 'complete', pending: pendingCount, unsupported,
        } });
        if (pendingCount) return;
      }
      await primary.close(call.primary!.bindingId);
      store.update(call.id, { primaryClosePending: false });
    })().catch(error => { store.update(call.id, { failure: safeFailure(error) }); })
      .finally(() => closingBindings.delete(call.id));
    closingBindings.set(call.id, pending);
    return pending;
  }

  async function requestOutbound(input: { requestId: string; purpose: string; to: string; callbackFor?: string }): Promise<{ replayed: boolean; call: BrokerCall }> {
    const to = input.to;
    if (!accepting || !UUID.test(input.requestId) || !config.allowedTo.includes(to)) throw new Error('call_not_available');
    if (input.callbackFor) {
      const inbound = store.get(input.callbackFor);
      if (!inbound || inbound.direction !== 'inbound' || inbound.number !== to || !terminalCallStatuses.has(inbound.status)
        || inbound.callback?.requestId !== input.requestId || inbound.callback.state !== 'requested') throw new Error('callback_identity_mismatch');
    }
    const reservation = store.reserve({ requestId: input.requestId, direction: 'outbound', purpose: input.purpose.trim(), number: to });
    if (reservation.existing) return { replayed: true, call: reservation.call };
    const call = reservation.call;
    sessionCalls.add(call.id);
    if (dialing.has(to) || store.list().some(other => other.id !== call.id && other.number === to && !terminalCallStatuses.has(other.status))) {
      store.update(call.id, { status: 'canceled', failure: { code: 'call_already_open', message: 'Another call for this person needs to finish first' } });
      throw Object.assign(new Error('Another call is already open'), { code: 'call_already_open' });
    }
    dialing.add(to);
    try {
      try {
        const readiness = await primary.readiness(to);
        if (!readiness.eligible) throw Object.assign(new Error('The account is not ready for this call'), { code: readiness.reason });
        await probe({ apiKey: config.apiKey, voice: config.voice, model: config.model });
      }
      catch (error) { store.update(call.id, { status: 'failed', failure: safeFailure(error) }); throw error; }
      if (!accepting) { store.update(call.id, { status: 'canceled' }); throw new Error('service_shutting_down'); }
      store.update(call.id, { status: 'dialing' });
      try {
        const created = await provider.calls.create({ from: config.from, to,
          url: `${config.publicOrigin}/broker/answer/${call.id}`, method: 'POST',
          statusCallback: `${config.publicOrigin}/broker/status/${call.id}`, statusCallbackMethod: 'POST',
          statusCallbackEvent: ['initiated', 'ringing', 'answered', 'completed'],
          timeout: 30, timeLimit: config.maxCallSeconds, record: false,
        });
        if (!CALL_SID.test(created.sid)) throw new Error('invalid_provider_call_id');
        const saved = store.update(call.id, { callSid: created.sid, status: 'queued' });
        return { replayed: false, call: saved };
      } catch (error) {
        const status = (error as { status?: number }).status;
        store.update(call.id, { status: status && status >= 400 && status < 500 ? 'failed' : 'unknown', failure: safeFailure(error) });
        throw error;
      }
    } finally { dialing.delete(to); }
  }

  async function bind(call: BrokerCall, proof: ProviderCallProof): Promise<BrokerCall> {
    const binding = await primary.openCall(proof);
    if (binding.callSid !== call.callSid || binding.state !== 'open') throw new Error('primary_call_identity_mismatch');
    const context = binding.context;
    const text = context.messages.map(message => `${message.role}: ${message.text}`).join('\n');
    return store.update(call.id, { primary: { bindingId: binding.bindingId, name: context.displayName, context: text.slice(-8_000) } });
  }

  async function handle(request: IncomingMessage, response: ServerResponse) {
    const route = request.url || '/';
    if (request.method === 'GET' && route === '/broker/health') return json(response, 200, { ok: accepting, service: 'dexter-broker', model: config.model, revision: process.env.BROKER_REVISION || 'development', activeCalls: active.size });
    if (route.startsWith('/internal/broker/')) {
      if (!authorizedControl(request.headers.authorization, config.controlToken)) return json(response, 401, { error: 'unauthorized' });
      if (request.method === 'GET' && route === '/internal/broker/calls') return json(response, 200, { calls: store.list().slice(0, 50).map(publicCall) });
      if (request.method !== 'POST' || !accepting) return json(response, 405, { error: 'unavailable' });
      const input = JSON.parse(await body(request, 8_000) || '{}') as Record<string, unknown>;
      if (route === '/internal/broker/preflight') return json(response, 200, await probe({ apiKey: config.apiKey, model: config.model, voice: config.voice }));
      if (route === '/internal/broker/calls') {
        if (typeof input.requestId !== 'string' || !UUID.test(input.requestId)
          || typeof input.purpose !== 'string' || !input.purpose.trim() || input.purpose.length > 1500
          || (input.to !== undefined && !isPhoneNumber(input.to))
          || Object.keys(input).some(key => !['requestId', 'purpose', 'to'].includes(key))) return json(response, 400, { error: 'invalid_call_request' });
        const to = input.to as string | undefined ?? (config.allowedTo.length === 1 ? config.allowedTo[0] : undefined);
        if (!to || !config.allowedTo.includes(to)) return json(response, 403, { error: 'recipient_not_authorized' });
        try {
          const result = await requestOutbound({ requestId: input.requestId, purpose: input.purpose.trim(), to });
          return json(response, result.replayed ? 200 : 201, { replayed: result.replayed, call: publicCall(result.call) });
        } catch (error) {
          if ((error as { code?: string }).code === 'call_already_open') return json(response, 409, { error: 'call_already_open' });
          throw error;
        }
      }
      if (route === '/internal/broker/end' || route === '/internal/broker/reconcile') {
        if (typeof input.id !== 'string' || !UUID.test(input.id)) return json(response, 400, { error: 'invalid_call_id' });
        const call = store.get(input.id);
        if (!call?.callSid) return json(response, 409, { error: 'saved_provider_call_required' });
        if (route.endsWith('/reconcile')) {
          const observed = await provider.calls(call.callSid).fetch();
          if ((call.direction === 'outbound' && (observed.to !== call.number || observed.from !== config.from))
            || (call.direction === 'inbound' && (observed.from !== call.number || observed.to !== config.from))) throw new Error('provider_call_identity_mismatch');
          if (providerStates.has(observed.status as CallStatus)) store.update(call.id, { status: observed.status as CallStatus });
          const reconciled = store.get(call.id)!;
          if ((terminalCallStatuses.has(reconciled.status) || reconciled.primaryClosePending) && active.has(call.id)) await active.get(call.id)!.stop('provider_terminal');
          else if (terminalCallStatuses.has(reconciled.status) || reconciled.primaryClosePending) await closePrimary(reconciled);
        } else if (active.has(call.id)) await active.get(call.id)!.stop('operator_requested');
        else if (!terminalCallStatuses.has(call.status)) {
          store.update(call.id, { endedAt: call.endedAt ?? new Date().toISOString() });
          const ended = await provider.calls(call.callSid).update({ status: 'completed' });
          store.update(call.id, { status: ended.status === 'completed' ? 'completed' : 'unknown', endedAt: new Date().toISOString() });
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
    const genericStatus = route === '/broker/status';
    if (!incoming && !match && !genericStatus) return json(response, 404, { error: 'not_found' });
    const raw = new URLSearchParams(await body(request));
    const params: Record<string, string> = {};
    for (const [key, value] of raw) { if (key in params) return json(response, 400, { error: 'duplicate_callback_field' }); params[key] = value; }
    const proof = verifyTwilio(config, request.headers['x-twilio-signature'] as string | undefined, route, params);
    if (!proof || !CALL_SID.test(params.CallSid || '')) return json(response, 403, { error: 'invalid_provider_callback' });
    if (!accepting) return xml(response, fallbackTwiml('Dexter is being updated. Please call again shortly.'));
    let call: BrokerCall;
    if (incoming) {
      if (params.To !== config.from || !isPhoneNumber(params.From)) return json(response, 403, { error: 'call_identity_mismatch' });
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
      call = saved.callSid ? saved : store.update(saved.id, { callSid: params.CallSid });
      if (match?.[1] === 'callback') return xml(response, await callbacks.consent(call, proof));
      if (genericStatus || match![1] === 'status') {
        if (providerStates.has(params.CallStatus as CallStatus)) {
          const state = params.CallStatus as CallStatus;
          call = store.update(call.id, { status: state, ...(terminalCallStatuses.has(state) ? { endedAt: new Date().toISOString() } : {}) });
          if (terminalCallStatuses.has(call.status)) {
            if (active.has(call.id)) await active.get(call.id)!.stop('provider_terminal');
            else await closePrimary(call);
          }
        }
        return json(response, 200, { ok: true });
      }
    }
    if (terminalCallStatuses.has(call.status)) return xml(response, '<Response><Hangup/></Response>');
    try { call = await bind(call, proof); }
    catch (error) {
      store.update(call.id, { status: 'failed', failure: safeFailure(error) });
      return xml(response, fallbackTwiml('I could not reach your Dexter account just now. Please try again shortly.'));
    }
    if (!accepting || terminalCallStatuses.has(store.get(call.id)!.status)) {
      await closePrimary(store.get(call.id)!);
      return xml(response, '<Response><Hangup/></Response>');
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
    const match = route.match(/^\/broker\/media\/([0-9a-f-]{36})$/);
    if (!accepting || !match || !verifyTwilio(config, request.headers['x-twilio-signature'] as string | undefined, route, {}, true)) {
      socket.write('HTTP/1.1 403 Forbidden\r\n\r\n'); socket.destroy(); return;
    }
    const call = store.get(match[1]);
    if (!call?.primary || !call.callSid || call.endedAt || terminalCallStatuses.has(call.status) || claimed.has(call.id)) {
      socket.write('HTTP/1.1 409 Conflict\r\n\r\n'); socket.destroy(); return;
    }
    claimed.add(call.id);
    sockets.handleUpgrade(request, socket, head, ws => connectMedia(ws, call));
  });

  function connectMedia(ws: WebSocket, call: BrokerCall) {
    let streamSid: string | undefined;
    let live: BrokerLiveSession | undefined;
    let backend: ReturnType<typeof createPrimaryBackend> | undefined;
    let stopping = false;
    let stopPromise: Promise<void> | undefined;
    let inputBytes = 0;
    let lastSpeechAt = 0;
    const receipts = new PlaybackReceipts();
    const metrics = () => ({ receivedAudioBytes: inputBytes, sentAudioBytes: receipts.sentBytes, playedAudioBytes: receipts.playedBytes, clearedAudioBytes: receipts.clearedBytes });
    const startTimer = setTimeout(() => { void stop('media_start_timeout'); }, 8_000);
    const durationTimer = setTimeout(() => { void stop('duration_limit'); }, config.maxCallSeconds * 1000);

    function stop(reason: string): Promise<void> {
      if (stopPromise) return stopPromise;
      stopping = true;
      clearTimeout(startTimer); clearTimeout(durationTimer);
      const previous = store.get(call.id)!;
      store.update(call.id, { endedAt: previous.endedAt ?? new Date().toISOString(), primaryClosePending: true });
      stopPromise = (async () => {
        const closing = live?.close(reason);
        try {
          const saved = store.get(call.id);
          if (reason !== 'provider_terminal' && saved && !terminalCallStatuses.has(saved.status)) {
            if (reason === 'voice_unavailable') {
              const spoken = await provider.calls(call.callSid!).update({ twiml: fallbackTwiml('My voice connection stopped working. Please try me again shortly.') });
              store.update(call.id, { status: providerStates.has(spoken.status as CallStatus) ? spoken.status as CallStatus : 'unknown' });
            } else {
              const ended = await provider.calls(call.callSid!).update({ status: 'completed' });
              store.update(call.id, { status: ended.status === 'completed' ? 'completed' : 'unknown' });
            }
          }
        } catch (error) { store.update(call.id, { status: 'unknown', failure: safeFailure(error) }); }
        // Closing <Connect><Stream> resumes its trailing <Hangup>. Install any
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
    active.set(call.id, { stop });
    ws.on('error', () => { void stop('media_socket_error'); });
    ws.on('close', () => { void stop('caller_disconnected'); });
    ws.on('message', raw => {
      if (stopping) return;
      try {
        const frame = JSON.parse(raw.toString());
        if (frame.event === 'start') {
          if (streamSid) throw new Error('duplicate_media_start');
          streamSid = mediaStart(frame.start, { accountSid: config.twilioAccountSid, callSid: call.callSid!, token: streamToken(config.controlToken, call.id, call.callSid!) });
          clearTimeout(startTimer);
          store.update(call.id, { streamSid, status: 'in-progress', mediaStartedAt: new Date().toISOString() });
          backend = createPrimaryBackend({ callId: call.id, callSid: call.callSid!, bindingId: call.primary!.bindingId, api: primary, store, lastCallerSpeechAt: () => lastSpeechAt });
          live = voiceFactory({ apiKey: config.apiKey, callId: call.id, voice: config.voice,
            instructions: brokerInstructions({ direction: call.direction, purpose: call.purpose, name: call.primary!.name, context: call.primary!.context }),
            history: brokerHistory({ name: call.primary!.name, context: call.primary!.context }),
            backend,
            onAudio(payload) {
              if (stopping || ws.readyState !== WebSocket.OPEN || !streamSid) return;
              if (ws.bufferedAmount > 256_000 || receipts.pendingCount > 300 || receipts.backlogMs > 6_000) { void stop('audio_backpressure'); return; }
              ws.send(JSON.stringify({ event: 'media', streamSid, media: { payload } }));
              const name = receipts.sent(Buffer.from(payload, 'base64').length);
              ws.send(JSON.stringify({ event: 'mark', streamSid, mark: { name } }));
            },
            onTranscript(snapshot) { store.update(call.id, { transcript: snapshot, transcriptRevision: snapshot.revision }); },
            onReady({ sessionId }) {
              store.update(call.id, { liveSessionId: sessionId, voiceOpening: { status: 'pending' } });
              // Live's documented speak-first command runs while phone audio
              // continues. Its receipt confirms context injection, not playback.
              queueMicrotask(() => {
                if (stopping || !live) return;
                void live.appendInstructions(call.direction === 'outbound'
                  ? 'Immediately say in English: "Hi, this is Dexter." Explain the call purpose from your initial instructions, then invite the person to speak and pause to listen.'
                  : 'Immediately say in English: "Hi, this is Dexter. What would you like to do?" Then pause to listen.')
                  .then(result => { store.update(call.id, { voiceOpening: result }); })
                  .catch(() => { if (!stopping) store.update(call.id, { voiceOpening: { status: 'unconfirmed', reason: 'opening_delivery_failed' } }); });
              });
            },
            onClosed(result) { store.update(call.id, { finalized: result.finalized, metrics: metrics() }); if (!stopping) void stop(result.finalized ? result.reason : 'voice_unavailable'); },
            onPlaybackClear() {
              receipts.clear();
              if (ws.readyState === WebSocket.OPEN && streamSid) ws.send(JSON.stringify({ event: 'clear', streamSid }));
            },
            onEvent(event) {
              if (event.type === 'live.error') store.update(call.id, { failure: { code: String(event.code || 'live_error'), message: 'The voice connection could not continue' } });
            },
          });
        } else if (frame.event === 'media') {
          if (!streamSid || frame.streamSid !== streamSid || frame.media?.track !== 'inbound'
              || typeof frame.media.payload !== 'string' || frame.media.payload.length > 32768) throw new Error('invalid_input_audio');
          const bytes = Buffer.from(frame.media.payload, 'base64');
          inputBytes += bytes.length;
          if (inputBytes > (config.maxCallSeconds + 10) * 8000) throw new Error('input_duration_exceeded');
          // Twilio timestamps are media positions, not an authorization or a turn boundary.
          if (inputSpeechEnergy(bytes) > 0.012) { lastSpeechAt = Date.now(); backend?.noteCallerSpeech(lastSpeechAt, inputBytes / 8); }
          live?.appendAudio(frame.media.payload);
        } else if (frame.event === 'mark' && frame.streamSid === streamSid && typeof frame.mark?.name === 'string') {
          receipts.acknowledge(frame.mark.name);
        } else if (frame.event === 'stop') {
          if (streamSid && frame.streamSid !== streamSid) throw new Error('stop_stream_mismatch');
          void stop('caller_hangup');
        }
      } catch (error) { store.update(call.id, { failure: safeFailure(error) }); void stop('media_error'); }
    });
  }

  let maintenance: Promise<void> | undefined;
  const maintenanceTimer = setInterval(() => {
    if (!accepting || maintenance) return;
    maintenance = (async () => {
      await callbacks.dispatchPending();
      for (const call of store.list()) {
        if (active.has(call.id) || claimed.has(call.id)) continue;
        if (call.primaryClosePending) await closePrimary(call);
        if (!call.callSid || terminalCallStatuses.has(call.status) || active.has(call.id) || claimed.has(call.id)
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
              store.update(call.id, { status: observed.status as CallStatus, endedAt: new Date().toISOString() });
              await active.get(call.id)!.stop('provider_terminal');
            }
            continue;
          }
          const current = store.get(call.id)!;
          store.update(call.id, { endedAt: current.endedAt ?? new Date().toISOString() });
          if (terminalCallStatuses.has(observed.status as CallStatus)) {
            store.update(call.id, { status: observed.status as CallStatus, endedAt: new Date().toISOString() });
          } else {
            const ended = await provider.calls(call.callSid).update({ status: 'completed' });
            store.update(call.id, { status: ended.status === 'completed' ? 'completed' : 'unknown', endedAt: new Date().toISOString() });
          }
          await closePrimary(store.get(call.id)!);
        } catch (error) { store.update(call.id, { failure: safeFailure(error) }); }
      }
    })().finally(() => { maintenance = undefined; });
  }, 10_000);
  maintenanceTimer.unref();

  async function close() {
    accepting = false;
    clearInterval(maintenanceTimer);
    server.close();
    await Promise.allSettled([...pendingRequests]);
    await maintenance;
    await Promise.allSettled([...active.values()].map(call => call.stop('service_shutdown')));
    for (const id of sessionCalls) {
      const call = store.get(id);
      if (!call?.callSid || terminalCallStatuses.has(call.status)) continue;
      try {
        const ended = await provider.calls(call.callSid).update({ status: 'completed' });
        store.update(id, { status: ended.status === 'completed' ? 'completed' : 'unknown', endedAt: new Date().toISOString() });
        await closePrimary(call);
      } catch (error) { store.update(id, { status: 'unknown', failure: safeFailure(error) }); }
    }
    await Promise.allSettled([...closingBindings.values()]);
    sockets.close();
    store.close();
  }
  return { server, store, close };
}
