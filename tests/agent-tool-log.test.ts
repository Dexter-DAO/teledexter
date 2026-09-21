import assert from 'node:assert/strict';
import test from 'node:test';
import { onAgentToolEnd, onAgentToolStart } from '../src/session/handlers/onAgentTool.js';
import type { CallLogEntry } from '../src/types/CallEntry.js';

function callEntry(): CallLogEntry {
  return {
    streamSid: 'MZ123',
    callerNumber: '+15615550100',
    stirVerstat: 'outbound-alert',
    startedAt: '2026-08-03T12:00:00.000Z',
    transcriptSegments: [],
    toolCalls: [],
  };
}

test('local Realtime tools are logged once from start through completion', () => {
  const entry = callEntry();
  const details = {
    toolCall: {
      callId: 'call-1',
      name: 'get_alert_policy',
      arguments: '{"section":"trigger"}',
      status: 'in_progress',
    },
  };

  onAgentToolStart(entry, { name: 'get_alert_policy' }, details);
  onAgentToolStart(entry, { name: 'get_alert_policy' }, details);

  assert.equal(entry.toolCalls.length, 1);
  assert.deepEqual(entry.toolCalls[0], {
    name: 'get_alert_policy',
    arguments: '{"section":"trigger"}',
    timestamp: entry.toolCalls[0].timestamp,
    itemId: 'call-1',
    status: 'in_progress',
    result: null,
    error: null,
  });

  onAgentToolEnd(entry, { name: 'get_alert_policy' }, '{"thresholdPct":20}', details);

  assert.equal(entry.toolCalls.length, 1);
  assert.equal(entry.toolCalls[0].status, 'completed');
  assert.equal(entry.toolCalls[0].result, '{"thresholdPct":20}');
  assert.equal(entry.toolCalls[0].error, null);
});

test('local tool logging bounds oversized arguments and results', () => {
  const entry = callEntry();
  const details = {
    toolCall: {
      callId: 'call-large',
      name: 'get_alert_snapshot',
      arguments: 'x'.repeat(5_000),
    },
  };

  onAgentToolStart(entry, { name: 'get_alert_snapshot' }, details);
  onAgentToolEnd(entry, { name: 'get_alert_snapshot' }, 'y'.repeat(20_000), details);

  assert.match(entry.toolCalls[0].arguments, /\[truncated\]$/);
  assert.ok((entry.toolCalls[0].arguments.length ?? 0) < 4_100);
  assert.match(entry.toolCalls[0].result ?? '', /\[truncated\]$/);
  assert.ok((entry.toolCalls[0].result?.length ?? 0) < 16_100);
});
