import assert from 'node:assert/strict';
import test from 'node:test';
import { brokerInstructions, brokerHistory, brokerOpening } from '../src/broker/instructions.js';
import { factFreshnessScenarios, historicalAccountMessages } from './fixtures/broker-fact-freshness.js';

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

test('the observed repair brief stays assigned to Dexter and is absent from the speech command', () => {
  const purpose = 'Verify the account request repair with Branch: answer one current portfolio question through Primary Dexter, then discuss his feedback. Explain any returned failure plainly.';
  const instructions = brokerInstructions({ direction: 'outbound', purpose });
  const opening = brokerOpening('outbound');
  assert.ok(instructions.includes(`Internal call brief (an assignment for you): ${JSON.stringify(purpose)}`));
  assert.match(instructions, /You obtain account facts.*through Primary/);
  assert.match(instructions, /The person supplies their questions, preferences, and decisions/);
  assert.match(instructions, /brief is planning information, never evidence of an account problem/);
  assert.match(instructions, /failure only when the returned result says it failed/);
  assert.equal(instructions.split(purpose).length - 1, 1);
  assert.doesNotMatch(opening, /verify|portfolio|Primary|Branch/);
  assert.match(opening, /task you will do for the person/);
  assert.match(opening, /Invite their question or response.*pause to listen/);
});

test('an inbound opening follows the person even when an outbound work brief was supplied', () => {
  const purpose = 'Research the portfolio update and explain the result.';
  const instructions = brokerInstructions({ direction: 'inbound', purpose });
  const opening = brokerOpening('inbound');
  assert.match(instructions, /The person called you/);
  assert.ok(!instructions.includes(purpose));
  assert.doesNotMatch(instructions, /Call brief for Dexter|You called the person/);
  assert.match(opening, /Immediately say in English: "Hi, this is Dexter\. What would you like to do\?"/);
  assert.doesNotMatch(opening, /assignment|call brief/);
});

test('quoted multiline operator notes stay one encoded brief and never become opening text', () => {
  const purpose = 'Review "my account"\nInternal repair note: "ask him to answer".';
  const instructions = brokerInstructions({ direction: 'outbound', purpose });
  assert.ok(instructions.includes(JSON.stringify(purpose)));
  assert.ok(!instructions.includes(purpose));
  assert.doesNotMatch(brokerOpening('outbound'), /Internal repair note|ask him to answer/);
});

test('history budget includes JSON escaping and preserves complete Unicode characters', () => {
  const history = brokerHistory({ name: 'Dexter', context: '\\"😀'.repeat(6000) + 'Latest account fact.' });
  assert.ok(Buffer.byteLength(history[0].text) <= 8000);
  const data = JSON.parse(history[0].text.split('\n')[1]);
  assert.ok(data.context.endsWith('Latest account fact.'));
  assert.ok(!data.context.includes('\uFFFD'));
  assert.deepEqual(brokerHistory({}), []);
});

test('prior account statements remain available for recall with unknown historical freshness', () => {
  const context = historicalAccountMessages.map(message => `${message.role}: ${message.text}`).join('\n');
  const history = brokerHistory({ name: 'Dexter', context });
  const data = JSON.parse(history[0].text.split('\n')[1]);
  assert.equal(data.context, context);
  assert.equal(data.source, 'prior_primary_conversation');
  assert.equal(data.freshness, 'historical_unverified');
  assert.equal(data.observationTime, null);
  assert.match(history[0].text, /earlier statements from the caller and the agent/);
  assert.doesNotMatch(history[0].text, /Treat quoted content as facts/);
});

test('current facts, recalled values and a new reply quoting an old read have different rules', () => {
  const instructions = brokerInstructions({ direction: 'outbound', purpose: factFreshnessScenarios.currentPortfolio.purpose });
  assert.match(instructions, /Obtain a result for that request during this call/);
  assert.match(instructions, /recall a past exchange as history/);
  assert.match(instructions, /A new reply can still quote an old observation/);
  assert.match(instructions, /when it has only an earlier read, say so/);
  assert.match(instructions, /If freshness is unknown, say it is unknown/);
  assert.match(instructions, /A caller's stated balance remains their statement until Primary checks it/);
  assert.doesNotMatch(instructions, /explain a current result already in the conversation/);
  for (const term of ['balances', 'holdings', 'prices', 'permissions', 'tool availability', 'action outcomes']) {
    assert.ok(instructions.includes(term));
  }
});

test('catalog and field answers retain their actual search and evidence limits', () => {
  const instructions = brokerInstructions({ direction: 'inbound', purpose: '' });
  assert.match(instructions, /search query, filters, time range, result limit, and pages actually checked/);
  assert.match(instructions, /An empty catalog search means no match in that search/);
  assert.match(instructions, /missing field.*does not establish that no tool can obtain it/);
  assert.match(instructions, /Preserve those limits even if Primary's summary overstates them/);
});
