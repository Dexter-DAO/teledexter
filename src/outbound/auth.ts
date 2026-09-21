import { createHmac, timingSafeEqual } from 'node:crypto';
import { canonicalJson, sha256Hex } from './canonical.js';

export interface SignedAlertHeaders {
  timestamp?: string;
  nonce?: string;
  signature?: string;
}

export interface VerifySignedAlertOptions {
  secret: string;
  body: unknown;
  headers: SignedAlertHeaders;
  nowMs?: number;
  maxSkewSeconds?: number;
  replayCache?: NonceReplayCache;
}

export type SignedAlertVerification =
  | { ok: true }
  | {
      ok: false;
      reason:
        | 'missing_headers'
        | 'invalid_timestamp'
        | 'expired'
        | 'invalid_nonce'
        | 'bad_signature'
        | 'replayed_nonce';
    };

/**
 * Process-local replay cache. The signed request is additionally protected by
 * the durable idempotency key, so a replay after restart cannot place a second
 * call.
 */
export class NonceReplayCache {
  private readonly seen = new Map<string, number>();

  has(nonce: string, nowMs: number): boolean {
    this.prune(nowMs);
    const expiresAt = this.seen.get(nonce);
    return expiresAt !== undefined && expiresAt > nowMs;
  }

  add(nonce: string, expiresAtMs: number): void {
    this.seen.set(nonce, expiresAtMs);
  }

  private prune(nowMs: number): void {
    for (const [nonce, expiresAt] of this.seen) {
      if (expiresAt <= nowMs) this.seen.delete(nonce);
    }
  }
}

export function signedAlertPayload(timestamp: string, nonce: string, body: unknown): string {
  const bodyHash = sha256Hex(canonicalJson(body));
  return `v1\n${timestamp}\n${nonce}\n${bodyHash}`;
}

export function createSignedAlertSignature(
  secret: string,
  timestamp: string,
  nonce: string,
  body: unknown,
): string {
  const digest = createHmac('sha256', secret)
    .update(signedAlertPayload(timestamp, nonce, body), 'utf8')
    .digest('hex');
  return `v1=${digest}`;
}

export function verifySignedAlertRequest(options: VerifySignedAlertOptions): SignedAlertVerification {
  const {
    secret,
    body,
    headers,
    nowMs = Date.now(),
    maxSkewSeconds = 60,
    replayCache,
  } = options;

  const { timestamp, nonce, signature } = headers;
  if (!timestamp || !nonce || !signature) return { ok: false, reason: 'missing_headers' };
  if (!/^\d{10,13}$/.test(timestamp)) return { ok: false, reason: 'invalid_timestamp' };
  if (!/^[A-Za-z0-9_-]{16,160}$/.test(nonce)) return { ok: false, reason: 'invalid_nonce' };

  const numericTimestamp = Number(timestamp);
  const requestMs = timestamp.length === 10 ? numericTimestamp * 1000 : numericTimestamp;
  if (!Number.isFinite(requestMs)) return { ok: false, reason: 'invalid_timestamp' };
  if (Math.abs(nowMs - requestMs) > maxSkewSeconds * 1000) {
    return { ok: false, reason: 'expired' };
  }

  const expected = createSignedAlertSignature(secret, timestamp, nonce, body);
  const providedBytes = Buffer.from(signature, 'utf8');
  const expectedBytes = Buffer.from(expected, 'utf8');
  if (
    providedBytes.length !== expectedBytes.length ||
    !timingSafeEqual(providedBytes, expectedBytes)
  ) {
    return { ok: false, reason: 'bad_signature' };
  }

  if (replayCache?.has(nonce, nowMs)) return { ok: false, reason: 'replayed_nonce' };
  replayCache?.add(nonce, nowMs + maxSkewSeconds * 1000);
  return { ok: true };
}
