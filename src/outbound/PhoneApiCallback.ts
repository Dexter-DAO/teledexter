import { createHmac, randomBytes } from 'node:crypto';
import { canonicalJson, sha256Hex } from './canonical.js';
import type { PhoneApiCallbackPath } from './PhoneApiCallbackOutbox.js';

export interface PhoneApiCallbackOptions {
  apiBaseUrl: string;
  path: PhoneApiCallbackPath;
  signingSecret: string;
  body: Record<string, unknown>;
  fetchImpl?: typeof fetch;
  now?: () => number;
  nonce?: () => string;
  timeoutMs?: number;
}

export function phoneApiCallbackSignedPayload(
  callbackPath: PhoneApiCallbackPath,
  timestamp: string,
  nonce: string,
  body: unknown,
): string {
  return `v2\n${callbackPath}\n${timestamp}\n${nonce}\n${sha256Hex(canonicalJson(body))}`;
}

export function createPhoneApiCallbackSignature(
  secret: string,
  callbackPath: PhoneApiCallbackPath,
  timestamp: string,
  nonce: string,
  body: unknown,
): string {
  const digest = createHmac('sha256', secret)
    .update(phoneApiCallbackSignedPayload(callbackPath, timestamp, nonce, body), 'utf8')
    .digest('hex');
  return `v2=${digest}`;
}

/**
 * Phone-to-API lifecycle callbacks are financial ingress once call duration can
 * create a mandate charge. Sign the exact canonical body with the callback-only
 * secret; callers must never fall back to an unsigned localhost request.
 */
export async function postSignedPhoneApiCallback(
  options: PhoneApiCallbackOptions,
): Promise<Response> {
  if (options.signingSecret.length < 32) {
    throw new Error('phone_api_callback_signing_not_configured');
  }
  const now = options.now ?? Date.now;
  const nonce = (options.nonce ?? (() => randomBytes(18).toString('base64url')))();
  const timestamp = String(Math.floor(now() / 1_000));
  const signature = createPhoneApiCallbackSignature(
    options.signingSecret,
    options.path,
    timestamp,
    nonce,
    options.body,
  );
  const base = options.apiBaseUrl.replace(/\/+$/u, '');
  const path = options.path.startsWith('/') ? options.path : `/${options.path}`;

  return (options.fetchImpl ?? fetch)(`${base}${path}`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-dexter-alert-timestamp': timestamp,
      'x-dexter-alert-nonce': nonce,
      'x-dexter-callback-signature': signature,
    },
    body: JSON.stringify(options.body),
    signal: AbortSignal.timeout(options.timeoutMs ?? 5_000),
  });
}
