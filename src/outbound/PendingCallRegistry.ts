import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';

export interface PendingCallContext<T> {
  callSid: string;
  context: T;
  expiresAtMs: number;
}

interface StoredPendingCall<T> extends PendingCallContext<T> {
  tokenHash: Buffer;
}

function tokenDigest(token: string): Buffer {
  return createHash('sha256').update(token, 'utf8').digest();
}

export function pendingCallTokenHash(token: string): string {
  return tokenDigest(token).toString('hex');
}

/**
 * One-shot bridge between a TwiML webhook and its subsequent media WebSocket.
 * It replaces the old process-wide "next caller" slot, which could attach the
 * wrong identity whenever two calls overlapped.
 */
export class PendingCallRegistry<T> {
  private readonly calls = new Map<string, StoredPendingCall<T>>();

  issue(callSid: string, context: T, expiresAtMs: number, nowMs = Date.now()): string {
    this.prune(nowMs);
    const token = randomBytes(32).toString('base64url');
    this.calls.set(callSid, {
      callSid,
      context,
      expiresAtMs,
      tokenHash: tokenDigest(token),
    });
    return token;
  }

  issueWithToken(
    callSid: string,
    token: string,
    context: T,
    expiresAtMs: number,
    nowMs = Date.now(),
  ): T {
    this.prune(nowMs);
    const existing = this.calls.get(callSid);
    if (existing) {
      if (!timingSafeEqual(existing.tokenHash, tokenDigest(token))) {
        throw new Error(`pending call ${callSid} already has a different stream token`);
      }
      return existing.context;
    }
    this.calls.set(callSid, {
      callSid,
      context,
      expiresAtMs,
      tokenHash: tokenDigest(token),
    });
    return context;
  }

  consume(callSid: string, token: string, nowMs = Date.now()): T | null {
    this.prune(nowMs);
    const pending = this.calls.get(callSid);
    if (!pending || pending.expiresAtMs <= nowMs) return null;

    const supplied = tokenDigest(token);
    if (
      supplied.length !== pending.tokenHash.length ||
      !timingSafeEqual(supplied, pending.tokenHash)
    ) {
      return null;
    }

    this.calls.delete(callSid);
    return pending.context;
  }

  size(nowMs = Date.now()): number {
    this.prune(nowMs);
    return this.calls.size;
  }

  /** Revoke the one-shot bridge after a durable no-media finalization fence. */
  revoke(callSid: string): boolean {
    return this.calls.delete(callSid);
  }

  private prune(nowMs: number): void {
    for (const [callSid, pending] of this.calls) {
      if (pending.expiresAtMs <= nowMs) this.calls.delete(callSid);
    }
  }
}
