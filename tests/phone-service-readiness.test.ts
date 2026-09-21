import assert from 'node:assert/strict';
import test from 'node:test';
import { phoneServiceReadiness } from '../src/outbound/PhoneServiceReadiness.js';

const checkedAt = '2026-08-31T00:00:00.000Z';
const expiresAt = '2026-08-31T00:01:00.000Z';

test('Realtime degradation keeps notification health up while conversation is down', () => {
  const readiness = phoneServiceReadiness({
    financial: { ok: true, reasons: [] },
    api: { status: 'ready', checkedAt, expiresAt, reason: null },
    realtime: {
      status: 'unavailable',
      checkedAt,
      expiresAt,
      reason: 'openai_quota_exhausted',
    },
  });

  assert.equal(readiness.healthOk, true);
  assert.equal(readiness.httpStatus, 200);
  assert.equal(readiness.notificationReady, true);
  assert.equal(readiness.conversationReady, false);
  assert.deepEqual(readiness.notificationReasons, []);
  assert.deepEqual(readiness.conversationReasons, ['openai_quota_exhausted']);
});

test('Dexter API or financial degradation blocks notification health', () => {
  const apiDown = phoneServiceReadiness({
    financial: { ok: true, reasons: [] },
    api: {
      status: 'unavailable',
      checkedAt,
      expiresAt,
      reason: 'dexter_api_unreachable',
    },
    realtime: { status: 'ready', checkedAt, expiresAt, reason: null },
  });
  assert.equal(apiDown.healthOk, false);
  assert.equal(apiDown.httpStatus, 503);
  assert.deepEqual(apiDown.notificationReasons, ['dexter_api_unreachable']);

  const ledgerDown = phoneServiceReadiness({
    financial: { ok: false, reasons: ['callback_outbox_unavailable'] },
    api: { status: 'ready', checkedAt, expiresAt, reason: null },
    realtime: { status: 'ready', checkedAt, expiresAt, reason: null },
  });
  assert.equal(ledgerDown.healthOk, false);
  assert.equal(ledgerDown.httpStatus, 503);
  assert.deepEqual(ledgerDown.notificationReasons, ['callback_outbox_unavailable']);
});
