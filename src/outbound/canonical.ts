import { createHash } from 'node:crypto';

function canonicalizeValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(canonicalizeValue);
  }

  if (value && typeof value === 'object') {
    const object = value as Record<string, unknown>;
    return Object.fromEntries(
      Object.keys(object)
        .sort()
        .map((key) => [key, canonicalizeValue(object[key])]),
    );
  }

  return value;
}

/**
 * Stable JSON representation used by both request authentication and
 * idempotency. Callers do not have to reproduce the sender's whitespace or
 * property insertion order.
 */
export function canonicalJson(value: unknown): string {
  return JSON.stringify(canonicalizeValue(value));
}

export function sha256Hex(value: string): string {
  return createHash('sha256').update(value, 'utf8').digest('hex');
}
