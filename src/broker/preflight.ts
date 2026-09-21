import OpenAI from 'openai';
import { LiveWS } from 'openai/resources/live/ws';
import type { ClientEvent } from 'openai/resources/live/live';

export interface LiveProbeConnection {
  onOpen(listener: () => void): void;
  onEvent(listener: (event: unknown) => void): void;
  onError(listener: (error: unknown) => void): void;
  onClose(listener: () => void): void;
  send(event: ClientEvent): void;
  close(): void;
  terminate(): void;
}

export interface LiveProbeOptions {
  apiKey: string;
  voice: string;
  model?: string;
  timeoutMs?: number;
  connectionFactory?: (apiKey: string) => LiveProbeConnection;
}

export interface LiveProbeResult {
  model: string;
  audioBytes: number;
  sessionId: string;
  finalized: true;
}

const providerCodes = new Set([
  'credit_balance_exhausted', 'insufficient_quota', 'invalid_api_key',
  'model_not_found', 'rate_limit_exceeded', 'permission_denied',
  'invalid_request_error', 'authentication_error', 'server_error',
]);

export class LiveProbeError extends Error {
  constructor(readonly code: string) {
    // Provider messages, causes and socket errors may contain credentials.
    super(`Live voice preflight failed: ${code}`);
    this.name = 'LiveProbeError';
  }
}

function object(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === 'object'
    ? value as Record<string, unknown> : {};
}

function providerFailure(value: unknown): string {
  const code = object(value).code;
  return typeof code === 'string' && providerCodes.has(code)
    ? code : 'live_provider_error';
}

function connect(apiKey: string): LiveProbeConnection {
  const ws = new LiveWS(new OpenAI({ apiKey, maxRetries: 0 }), {
    reconnect: null,
    maxQueueSize: 64 * 1024,
    maxPayload: 1024 * 1024,
  });
  return {
    onOpen: (listener) => { ws.socket.on('open', listener); },
    onEvent: (listener) => { ws.on('event', listener); },
    onError: (listener) => { ws.on('error', listener); },
    onClose: (listener) => { ws.socket.on('close', listener); },
    send: (event) => { ws.send(event); },
    close: () => { ws.close(); },
    terminate: () => { ws.socket.platformSocket.terminate(); },
  };
}

/**
 * Prove generated G.711 audio and finalized Live usage without dialing anyone.
 * No backend tools or Responses model are configured in this client delegation.
 * https://developers.openai.com/api/docs/guides/live-delegation#send-the-right-kind-of-update
 * https://developers.openai.com/api/docs/guides/voice-websockets?api=live
 */
export async function probeLive(options: LiveProbeOptions): Promise<LiveProbeResult> {
  const model = options.model ?? 'gpt-live-1';
  const timeoutMs = options.timeoutMs ?? 15_000;
  const identifier = /^[a-zA-Z0-9_.:-]{1,180}$/;
  if (typeof options.apiKey !== 'string' || !options.apiKey.trim()
    || typeof options.voice !== 'string' || !identifier.test(options.voice)
    || typeof model !== 'string' || !identifier.test(model) || !Number.isInteger(timeoutMs)
    || timeoutMs < 1 || timeoutMs > 15_000) {
    throw new LiveProbeError('live_preflight_invalid_configuration');
  }

  return new Promise<LiveProbeResult>((resolve, reject) => {
    let connection: LiveProbeConnection | undefined;
    let settled = false;
    let startSent = false;
    let started = false;
    let closeSent = false;
    let audioBytes = 0;
    let sessionId = '';

    const finish = (code?: string) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      // Stop even a socket still connecting; a close handshake must not outlive
      // the probe deadline. After session.closed, final usage is already known.
      try { connection?.close(); } catch { /* Cleanup still terminates below. */ }
      try { connection?.terminate(); } catch { /* Never expose transport errors. */ }
      if (code) reject(new LiveProbeError(code));
      else resolve({ model, audioBytes, sessionId, finalized: true });
    };
    const timer = setTimeout(() => finish('live_preflight_timeout'), timeoutMs);
    const send = (event: ClientEvent) => {
      if (settled) return;
      try { connection!.send(event); }
      catch { finish('live_transport_error'); }
    };

    try {
      connection = (options.connectionFactory ?? connect)(options.apiKey);
      connection.onError((error) => finish(providerFailure(error)));
      connection.onClose(() => finish('live_closed_without_finalization'));
      // Register final-event handling before any command can close the session.
      connection.onEvent((value) => {
        if (settled) return;
        const event = object(value);
        if (event.type === 'error') {
          finish(providerFailure(event.error));
        } else if (event.type === 'session.started') {
          const session = object(event.session);
          if (!startSent || started || typeof session.id !== 'string'
            || !identifier.test(session.id)
            || (session.model !== undefined && session.model !== model)) {
            finish('live_session_mismatch');
            return;
          }
          started = true;
          sessionId = session.id;
          // Acknowledgment of this instruction is not audio proof. Wait for
          // output_audio.delta, then keep receiving through session.closed.
          send({
            type: 'session.instructions.append', event_id: 'probe_speak',
            delegation_id: null,
            content: 'Say "Voice connection ready" once now. Do not delegate or ask a question.',
          });
        } else if (event.type === 'session.output_audio.delta') {
          if (!started) { finish('live_audio_before_session'); return; }
          const delta = event.delta;
          if (typeof delta !== 'string' || delta.length === 0
            || delta.length > 1024 * 1024 || delta.length % 4 !== 0
            || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(delta)) {
            finish('live_invalid_audio');
            return;
          }
          const bytes = Buffer.from(delta, 'base64');
          if (bytes.length === 0 || bytes.toString('base64') !== delta) {
            finish('live_invalid_audio');
            return;
          }
          audioBytes += bytes.length;
          if (!closeSent) {
            closeSent = true;
            send({ type: 'session.close', event_id: 'probe_close' });
          }
        } else if (event.type === 'session.closed') {
          if (started && (object(event.session).id !== sessionId
            || (object(event.session).model !== undefined && object(event.session).model !== model))) {
            finish('live_session_mismatch');
            return;
          }
          finish(started && closeSent && audioBytes > 0
            ? undefined : 'live_no_audio');
        }
      });
      connection.onOpen(() => {
        if (settled || startSent) return;
        startSent = true;
        send({
          type: 'session.start', event_id: 'probe_start',
          session: {
            model,
            instructions: 'This is a brief voice connection check. Follow the application instruction to speak once. No backend work is needed.',
            audio: { format: { type: 'audio/pcmu', rate: 8000 }, output: { voice: options.voice } },
            delegation: { type: 'client' },
          },
        });
      });
    } catch {
      finish('live_transport_error');
    }
  });
}
