import assert from 'node:assert/strict';
import test from 'node:test';
import {
  createPhoneApiCallbackSignature,
  postSignedPhoneApiCallback,
} from '../src/outbound/PhoneApiCallback.js';

const SECRET = 'phone-api-callback-secret-longer-than-thirty-two-bytes';
const NOW_MS = Date.parse('2026-08-17T12:00:00.000Z');
const NONCE = 'callback-nonce-1234567890';

test('Phone signs the exact call-ended body with timestamp and nonce', async () => {
  const body = {
    source: 'phone',
    direction: 'inbound',
    external_sid: `CA${'1'.repeat(32)}`,
    caller_user_id: '00000000-0000-4000-8000-000000000001',
    duration_seconds: 181,
  };
  let capturedUrl = '';
  let capturedInit: RequestInit | undefined;
  const fetchImpl = async (url: string | URL | Request, init?: RequestInit) => {
    capturedUrl = String(url);
    capturedInit = init;
    return new Response(JSON.stringify({ ok: true }), { status: 200 });
  };

  await postSignedPhoneApiCallback({
    apiBaseUrl: 'http://localhost:3030/',
    path: '/internal/voice/call-ended',
    signingSecret: SECRET,
    body,
    fetchImpl,
    now: () => NOW_MS,
    nonce: () => NONCE,
  });

  assert.equal(capturedUrl, 'http://localhost:3030/internal/voice/call-ended');
  assert.deepEqual(JSON.parse(String(capturedInit?.body)), body);
  const headers = capturedInit?.headers as Record<string, string>;
  assert.equal(headers['x-dexter-alert-timestamp'], String(NOW_MS / 1_000));
  assert.equal(headers['x-dexter-alert-nonce'], NONCE);
  assert.equal(headers['x-dexter-alert-signature'], undefined);
  assert.equal(
    headers['x-dexter-callback-signature'],
    createPhoneApiCallbackSignature(
      SECRET,
      '/internal/voice/call-ended',
      String(NOW_MS / 1_000),
      NONCE,
      body,
    ),
  );
  assert.notEqual(
    headers['x-dexter-callback-signature'],
    createPhoneApiCallbackSignature(
      SECRET,
      '/internal/voice/outbound-status',
      String(NOW_MS / 1_000),
      NONCE,
      body,
    ),
  );
  assert.notEqual(
    headers['x-dexter-callback-signature'],
    createPhoneApiCallbackSignature(
      SECRET,
      '/internal/voice/call-ended',
      String(NOW_MS / 1_000),
      NONCE,
      { ...body, duration_seconds: 999 },
    ),
  );
});

test('Phone signs outbound lifecycle and refuses an unsigned fallback', async () => {
  const body = {
    intentId: '11111111-1111-4111-8111-111111111111',
    eventId: 'hark:publish-1:subscription-1',
    callSid: `CA${'2'.repeat(32)}`,
    state: 'completed',
    observedAt: '2026-08-17T12:00:00.000Z',
  };
  let callCount = 0;
  const fetchImpl = async () => {
    callCount += 1;
    return new Response(null, { status: 204 });
  };

  await postSignedPhoneApiCallback({
    apiBaseUrl: 'http://localhost:3030',
    path: '/internal/voice/outbound-status',
    signingSecret: SECRET,
    body,
    fetchImpl,
    now: () => NOW_MS,
    nonce: () => NONCE,
  });
  assert.equal(callCount, 1);

  await assert.rejects(
    postSignedPhoneApiCallback({
      apiBaseUrl: 'http://localhost:3030',
      path: '/internal/voice/outbound-status',
      signingSecret: '',
      body,
      fetchImpl,
    }),
    /phone_api_callback_signing_not_configured/,
  );
  assert.equal(callCount, 1);
});
