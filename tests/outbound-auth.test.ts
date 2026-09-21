import assert from 'node:assert/strict';
import test from 'node:test';
import {
  createSignedAlertSignature,
  NonceReplayCache,
  verifySignedAlertRequest,
} from '../src/outbound/auth.js';

const SECRET = 'test-secret-that-is-at-least-thirty-two-bytes';

test('signed alerts authenticate independent of object key order', () => {
  const timestamp = '1785110400';
  const nonce = 'abcdefghijklmnop';
  const signedBody = { z: 1, nested: { b: 2, a: 1 } };
  const receivedBody = { nested: { a: 1, b: 2 }, z: 1 };
  const signature = createSignedAlertSignature(SECRET, timestamp, nonce, signedBody);

  assert.deepEqual(
    verifySignedAlertRequest({
      secret: SECRET,
      body: receivedBody,
      headers: { timestamp, nonce, signature },
      nowMs: 1_785_110_400_000,
    }),
    { ok: true },
  );
});

test('signed alerts reject tampering, stale timestamps, and nonce replay', () => {
  const nowMs = 1_785_110_400_000;
  const timestamp = String(nowMs / 1000);
  const nonce = 'abcdefghijklmnop';
  const body = { eventId: 'event-1' };
  const signature = createSignedAlertSignature(SECRET, timestamp, nonce, body);
  const replayCache = new NonceReplayCache();

  assert.deepEqual(
    verifySignedAlertRequest({
      secret: SECRET,
      body,
      headers: { timestamp, nonce, signature },
      nowMs,
      replayCache,
    }),
    { ok: true },
  );
  assert.deepEqual(
    verifySignedAlertRequest({
      secret: SECRET,
      body,
      headers: { timestamp, nonce, signature },
      nowMs,
      replayCache,
    }),
    { ok: false, reason: 'replayed_nonce' },
  );
  assert.deepEqual(
    verifySignedAlertRequest({
      secret: SECRET,
      body: { eventId: 'tampered' },
      headers: { timestamp, nonce: 'qrstuvwxyzABCDEF', signature },
      nowMs,
    }),
    { ok: false, reason: 'bad_signature' },
  );
  assert.deepEqual(
    verifySignedAlertRequest({
      secret: SECRET,
      body,
      headers: {
        timestamp: String((nowMs - 61_000) / 1000),
        nonce: 'qrstuvwxyzABCDEF',
        signature,
      },
      nowMs,
    }),
    { ok: false, reason: 'expired' },
  );
});
