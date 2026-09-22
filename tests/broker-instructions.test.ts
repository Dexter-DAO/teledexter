import assert from 'node:assert/strict';
import test from 'node:test';
import { brokerInstructions, brokerHistory } from '../src/broker/instructions.js';

test('call direction and purpose guide the opening while account text stays in history', () => {
  const call = { direction: 'outbound' as const, purpose: 'Your requested test call.', name: 'Dexter', context: 'Account fact: blue lantern.' };
  const instructions = brokerInstructions(call);
  assert.match(instructions, /You called the person/);
  assert.ok(instructions.includes(call.purpose));
  assert.ok(!instructions.includes(call.context));
  for (const heading of ['Backchannel policy:', 'Interruption policy:', 'Delegation policy:', 'Backend tools:', 'Delegate to the backend when:', 'Do not delegate to the backend when:']) assert.ok(instructions.includes(heading));
  assert.ok(brokerHistory(call)[0].text.includes(call.context));
  assert.match(brokerInstructions({ ...call, direction: 'inbound' }), /The person called you/);
});

test('history budget includes JSON escaping and preserves complete Unicode characters', () => {
  const history = brokerHistory({ name: 'Dexter', context: '\\"😀'.repeat(6000) + 'Latest account fact.' });
  assert.ok(Buffer.byteLength(history[0].text) <= 8000);
  const data = JSON.parse(history[0].text.split('\n')[1]);
  assert.ok(data.context.endsWith('Latest account fact.'));
  assert.ok(!data.context.includes('\uFFFD'));
  assert.deepEqual(brokerHistory({}), []);
});
