import assert from 'node:assert/strict';
import test from 'node:test';
import { PendingCallRegistry } from '../src/outbound/PendingCallRegistry.js';

test('per-CallSid registry cannot cross-wire overlapping calls', () => {
  const registry = new PendingCallRegistry<{ subject: string }>();
  const nowMs = 1_785_110_400_000;
  const tokenA = registry.issue('CA-a', { subject: 'CATE' }, nowMs + 60_000, nowMs);
  const tokenB = registry.issue('CA-b', { subject: 'Jimothy' }, nowMs + 60_000, nowMs);

  assert.equal(registry.consume('CA-a', tokenB, nowMs), null);
  assert.equal(registry.consume('CA-b', tokenA, nowMs), null);
  assert.deepEqual(registry.consume('CA-b', tokenB, nowMs), { subject: 'Jimothy' });
  assert.deepEqual(registry.consume('CA-a', tokenA, nowMs), { subject: 'CATE' });
});

test('media context is one-shot and expires', () => {
  const registry = new PendingCallRegistry<string>();
  const nowMs = 1_785_110_400_000;
  const token = registry.issue('CA-one', 'context', nowMs + 1_000, nowMs);

  assert.equal(registry.consume('CA-one', token, nowMs), 'context');
  assert.equal(registry.consume('CA-one', token, nowMs), null);

  const expired = registry.issue('CA-expired', 'old', nowMs + 1, nowMs);
  assert.equal(registry.consume('CA-expired', expired, nowMs + 2), null);
});
