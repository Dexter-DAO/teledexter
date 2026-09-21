import WebSocket from 'ws';

export type RealtimeReadinessReason =
  | 'openai_quota_exhausted'
  | 'openai_auth_failed'
  | 'openai_rate_limited'
  | 'openai_model_unavailable'
  | 'openai_generation_failed'
  | 'openai_audio_missing'
  | 'openai_connection_failed'
  | 'openai_probe_timeout';

export type RealtimeProbeResult =
  | { ok: true; checkedAt: string }
  | { ok: false; checkedAt: string; reason: RealtimeReadinessReason };

export type RealtimeReadinessSnapshot =
  | {
      status: 'unknown' | 'checking';
      checkedAt: string | null;
      expiresAt: string | null;
      reason: null;
    }
  | {
      status: 'ready';
      checkedAt: string;
      expiresAt: string;
      reason: null;
    }
  | {
      status: 'unavailable';
      checkedAt: string;
      expiresAt: string;
      reason: RealtimeReadinessReason;
    };

interface ProbeSocket {
  on(event: 'open', listener: () => void): this;
  on(event: 'message', listener: (data: unknown) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
  on(event: 'close', listener: () => void): this;
  send(data: string): void;
  close(): void;
  terminate?(): void;
}

export type RealtimeSocketFactory = (
  url: string,
  options: { headers: Record<string, string> },
) => ProbeSocket;

export interface RealtimeProbeOptions {
  apiKey: string;
  model: string;
  voice: string;
  timeoutMs?: number;
  endpoint?: string;
  socketFactory?: RealtimeSocketFactory;
  now?: () => number;
}

function classifyProviderError(event: unknown): RealtimeReadinessReason {
  const payload = event as any;
  const code = String(payload?.error?.code || payload?.code || '').toLowerCase();
  const message = String(payload?.error?.message || payload?.message || '').toLowerCase();
  if (
    code === 'insufficient_quota'
    || message.includes('exceeded your current quota')
    || message.includes('billing')
  ) return 'openai_quota_exhausted';
  if (
    code === 'invalid_api_key'
    || code === 'invalid_api_key_error'
    || message.includes('incorrect api key')
    || message.includes('invalid api key')
  ) return 'openai_auth_failed';
  if (code === 'rate_limit_exceeded' || message.includes('rate limit')) {
    return 'openai_rate_limited';
  }
  if (
    code === 'model_not_found'
    || message.includes('model') && message.includes('not found')
  ) return 'openai_model_unavailable';
  return 'openai_generation_failed';
}

function messageText(data: unknown): string {
  if (typeof data === 'string') return data;
  if (Buffer.isBuffer(data)) return data.toString('utf8');
  if (data instanceof ArrayBuffer) return Buffer.from(data).toString('utf8');
  if (Array.isArray(data) && data.every(Buffer.isBuffer)) {
    return Buffer.concat(data).toString('utf8');
  }
  return String(data);
}

function responseCompletedWithGeneratedOutput(response: any): boolean {
  if (response?.status === 'completed') return true;
  return response?.status === 'incomplete'
    && response?.status_details?.reason === 'max_output_tokens';
}

/**
 * Proves the configured Realtime model can perform one tiny audio generation.
 * A successful WebSocket handshake is deliberately insufficient: quota and
 * billing failures can arrive only after inference begins.
 */
export function probeRealtimeAudioGeneration(
  options: RealtimeProbeOptions,
): Promise<RealtimeProbeResult> {
  const now = options.now ?? Date.now;
  const checkedAt = (): string => new Date(now()).toISOString();
  const endpoint = new URL(options.endpoint ?? 'wss://api.openai.com/v1/realtime');
  endpoint.searchParams.set('model', options.model);
  const socketFactory = options.socketFactory ?? ((url, socketOptions) => (
    new WebSocket(url, socketOptions) as unknown as ProbeSocket
  ));

  return new Promise((resolve) => {
    let settled = false;
    let responseRequested = false;
    let receivedAudio = false;
    let socket: ProbeSocket | null = null;

    const finish = (result: RealtimeProbeResult): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try { socket?.close(); } catch {}
      resolve(result);
    };

    const timer = setTimeout(() => {
      try { socket?.terminate?.(); } catch {}
      finish({ ok: false, checkedAt: checkedAt(), reason: 'openai_probe_timeout' });
    }, options.timeoutMs ?? 7_000);
    timer.unref?.();

    try {
      socket = socketFactory(endpoint.toString(), {
        headers: {
          Authorization: `Bearer ${options.apiKey}`,
        },
      });
    } catch {
      finish({ ok: false, checkedAt: checkedAt(), reason: 'openai_connection_failed' });
      return;
    }

    socket.on('open', () => {
      // Wait for session.created so model negotiation has completed before
      // requesting the generation that proves spend/quota availability.
    });
    socket.on('message', (raw) => {
      let event: any;
      try {
        event = JSON.parse(messageText(raw));
      } catch {
        return;
      }
      if (event?.type === 'session.created' && !responseRequested) {
        responseRequested = true;
        try {
          socket?.send(JSON.stringify({
            type: 'response.create',
            response: {
              conversation: 'none',
              input: [{
                type: 'message',
                role: 'user',
                content: [{ type: 'input_text', text: 'Say only: ready.' }],
              }],
              instructions: 'Say only the word ready.',
              // Leave enough room for reasoning-capable Realtime models to
              // emit an actual audio delta. Four total tokens can be consumed
              // before a spoken sample exists, producing a false negative.
              max_output_tokens: 32,
              output_modalities: ['audio'],
              audio: { output: { voice: options.voice } },
            },
          }));
        } catch {
          finish({ ok: false, checkedAt: checkedAt(), reason: 'openai_connection_failed' });
        }
        return;
      }
      if (
        event?.type === 'response.output_audio.delta'
        && typeof event?.delta === 'string'
        && event.delta.length > 0
      ) {
        receivedAudio = true;
        return;
      }
      if (event?.type === 'error') {
        finish({
          ok: false,
          checkedAt: checkedAt(),
          reason: classifyProviderError(event.error),
        });
        return;
      }
      if (event?.type === 'response.done') {
        if (!responseCompletedWithGeneratedOutput(event.response)) {
          finish({
            ok: false,
            checkedAt: checkedAt(),
            reason: classifyProviderError(event.response?.status_details),
          });
          return;
        }
        finish(receivedAudio
          ? { ok: true, checkedAt: checkedAt() }
          : { ok: false, checkedAt: checkedAt(), reason: 'openai_audio_missing' });
      }
    });
    socket.on('error', () => {
      finish({ ok: false, checkedAt: checkedAt(), reason: 'openai_connection_failed' });
    });
    socket.on('close', () => {
      if (!settled) {
        finish({ ok: false, checkedAt: checkedAt(), reason: 'openai_connection_failed' });
      }
    });
  });
}

export interface RealtimeReadinessGateOptions {
  probe: () => Promise<RealtimeProbeResult>;
  successTtlMs?: number;
  failureTtlMs?: number;
  now?: () => number;
}

export type RealtimeSessionConnectResult =
  | { ok: true }
  | {
      ok: false;
      reason: RealtimeReadinessReason;
      error: unknown;
    };

/**
 * The readiness probe and the caller's session are separate provider
 * operations. Preserve that boundary explicitly: a session may still fail
 * after a successful probe, and the media handler must then advance Twilio to
 * the frozen-alert <Say> that follows <Connect>.
 */
export async function connectRealtimeSession(
  connect: () => Promise<void>,
  classifyError: (error: unknown) => RealtimeReadinessReason | null,
): Promise<RealtimeSessionConnectResult> {
  try {
    await connect();
    return { ok: true };
  } catch (error) {
    return {
      ok: false,
      reason: classifyError(error) ?? 'openai_connection_failed',
      error,
    };
  }
}

export class RealtimeReadinessGate {
  private snapshot: RealtimeReadinessSnapshot = {
    status: 'unknown',
    checkedAt: null,
    expiresAt: null,
    reason: null,
  };
  private inFlight: Promise<RealtimeReadinessSnapshot> | null = null;
  private readonly now: () => number;
  private epoch = 0;

  constructor(private readonly options: RealtimeReadinessGateOptions) {
    this.now = options.now ?? Date.now;
  }

  current(): RealtimeReadinessSnapshot {
    return structuredClone(this.snapshot);
  }

  async check(force = false): Promise<RealtimeReadinessSnapshot> {
    const expiresAtMs = this.snapshot.expiresAt
      ? Date.parse(this.snapshot.expiresAt)
      : 0;
    if (!force && this.snapshot.status !== 'checking' && expiresAtMs > this.now()) {
      return this.current();
    }
    if (this.inFlight) return this.inFlight;

    const priorCheckedAt = this.snapshot.checkedAt;
    this.snapshot = {
      status: 'checking',
      checkedAt: priorCheckedAt,
      expiresAt: null,
      reason: null,
    };
    const checkEpoch = this.epoch;
    this.inFlight = this.options.probe()
      .then((result): RealtimeReadinessSnapshot => {
        // A fatal error from an actual call is stronger evidence than an older
        // probe. Never let that older promise erase the tripped circuit.
        if (checkEpoch !== this.epoch) return this.current();
        const ttlMs = result.ok
          ? (this.options.successTtlMs ?? 60_000)
          : (this.options.failureTtlMs ?? 15_000);
        const expiresAt = new Date(this.now() + ttlMs).toISOString();
        this.snapshot = result.ok
          ? {
              status: 'ready',
              checkedAt: result.checkedAt,
              expiresAt,
              reason: null,
            }
          : {
              status: 'unavailable',
              checkedAt: result.checkedAt,
              expiresAt,
              reason: result.reason,
            };
        return this.current();
      })
      .catch((): RealtimeReadinessSnapshot => {
        if (checkEpoch !== this.epoch) return this.current();
        const checkedAt = new Date(this.now()).toISOString();
        this.snapshot = {
          status: 'unavailable',
          checkedAt,
          expiresAt: new Date(
            this.now() + (this.options.failureTtlMs ?? 15_000),
          ).toISOString(),
          reason: 'openai_connection_failed',
        };
        return this.current();
      })
      .finally(() => {
        this.inFlight = null;
      });
    return this.inFlight;
  }

  trip(reason: RealtimeReadinessReason): void {
    const nowMs = this.now();
    this.epoch += 1;
    this.snapshot = {
      status: 'unavailable',
      checkedAt: new Date(nowMs).toISOString(),
      expiresAt: new Date(
        nowMs + (this.options.failureTtlMs ?? 15_000),
      ).toISOString(),
      reason,
    };
  }
}
