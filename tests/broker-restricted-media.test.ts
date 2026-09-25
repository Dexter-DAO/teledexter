import assert from 'node:assert/strict';
import test from 'node:test';
import { changeRestrictedMedia, validateRestrictedMedia } from '../src/broker/restrictedMedia.js';
const base = { admittedAt: '2026-09-25T06:00:00.000Z', deadlineAt: '2026-09-25T06:03:00.000Z' };
const active = { ...base, startedAt: '2026-09-25T06:00:01.000Z', streamSid: 'MZ' + '1'.repeat(32), sessionId: 'setup_session' };
test('restricted media has one immutable absolute budget and separate source start', () => {
  validateRestrictedMedia(active, base);
  assert.throws(() => changeRestrictedMedia(active, { deadlineAt: '2026-09-25T06:04:00.000Z' }));
  assert.throws(() => changeRestrictedMedia(active, { startedAt: '2026-09-25T06:00:02.000Z' }));
  assert.throws(() => changeRestrictedMedia(active, { sessionId: 'new_session' }));
});
test('protected and private transport requests are mutually exclusive and follow actual session', () => {
  assert.throws(() => changeRestrictedMedia(base, { protectedRequestedAt: '2026-09-25T06:00:05.000Z' }));
  const protectedState = changeRestrictedMedia(active, { protectedRequestedAt: '2026-09-25T06:00:05.000Z' });
  assert.throws(() => changeRestrictedMedia(protectedState, { privateRequestedAt: '2026-09-25T06:00:06.000Z' }));
  assert.throws(() => changeRestrictedMedia(protectedState, { protectedDispatchedAt: '2026-09-25T06:00:04.000Z' }));
});
test('transport dispatch cannot start after fixed deadline while a later stop remains recordable', () => {
  const requested = changeRestrictedMedia(active, { privateRequestedAt: '2026-09-25T06:02:59.000Z' });
  assert.throws(() => changeRestrictedMedia(requested, { privateDispatchedAt: base.deadlineAt }));
  assert.equal(changeRestrictedMedia(requested, { stoppedAt: '2026-09-25T06:03:02.000Z' }).deadlineAt, base.deadlineAt);
});
test('saved media state validates after JSON restart and cannot remove consumed dispatch fence', () => {
  const dispatched = changeRestrictedMedia(active, { protectedRequestedAt: '2026-09-25T06:00:05.000Z', protectedDispatchedAt: '2026-09-25T06:00:06.000Z' });
  const restored = JSON.parse(JSON.stringify(dispatched)); validateRestrictedMedia(restored, dispatched);
  delete restored.protectedDispatchedAt; assert.throws(() => validateRestrictedMedia(restored, dispatched));
});
test('stream identity requires its actual start observation; provider approval alone supplies neither', () => {
  validateRestrictedMedia(base);
  assert.throws(() => validateRestrictedMedia({ ...base, streamSid: active.streamSid }));
  assert.throws(() => validateRestrictedMedia({ ...base, sessionId: active.sessionId }));
});
