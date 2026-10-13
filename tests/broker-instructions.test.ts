import assert from 'node:assert/strict';
import test from 'node:test';
import { DEXTER_CAPABILITIES, brokerInstructions, brokerHistory, brokerOpening, brokerWatchBrief } from '../src/broker/instructions.js';
import { makeHarkSnapshot } from './fixtures/broker-hark.js';
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
  const purpose = 'Verify the account request repair with Avery Example: answer one current portfolio question through Primary Dexter, then discuss his feedback. Explain any returned failure plainly.';
  const instructions = brokerInstructions({ direction: 'outbound', purpose });
  const opening = brokerOpening('outbound');
  assert.ok(instructions.includes(`Internal call brief (an assignment for you): ${JSON.stringify(purpose)}`));
  assert.match(instructions, /You obtain account facts.*through Primary/);
  assert.match(instructions, /The person supplies their questions, preferences, and decisions/);
  assert.match(instructions, /brief is planning information, never evidence of an account problem/);
  assert.match(instructions, /failure only when the returned result says it failed/);
  assert.equal(instructions.split(purpose).length - 1, 1);
  assert.doesNotMatch(opening, /verify|portfolio|Primary|Avery/);
  assert.match(opening, /task you will do for the person/);
  assert.match(opening, /"Hey, it's Dexter\." and offer the useful task .* as one short question/);
  assert.match(opening, /pause to listen/);
});

test('an inbound opening follows the person even when an outbound work brief was supplied', () => {
  const purpose = 'Research the portfolio update and explain the result.';
  const instructions = brokerInstructions({ direction: 'inbound', purpose });
  const opening = brokerOpening('inbound');
  assert.match(instructions, /The person called you/);
  assert.ok(!instructions.includes(purpose));
  assert.doesNotMatch(instructions, /Call brief for Dexter|You called the person/);
  assert.match(opening, /Immediately say in English: "Hey, it's Dexter\. What can I do for you\?"/);
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

test('registered call opening starts its assigned work from the real first response without another task interview', () => {
  const call = { direction: 'outbound' as const, purpose: 'Read the current portfolio.', registeredAssignment: true };
  const instructions = brokerInstructions(call);
  const opening = brokerOpening('outbound', true);
  assert.match(instructions, /task is registered with Primary separately from the caller's speech/);
  assert.match(instructions, /first response, including an ordinary greeting or acknowledgment, delegate it/);
  assert.match(instructions, /Preserve what they actually say/);
  assert.match(instructions, /greetings and acknowledgments do not restart the task/);
  assert.match(opening, /"Hey, it's Dexter\." and offer the useful task you called to do as one short question/);
  assert.match(opening, /"Hey, it's Dexter\. Want me to pull up your portfolio\?"/);
  assert.doesNotMatch(opening, /Invite their question|What would you like|What can I do for you/);
  assert.match(instructions, /finishes a new task or correction while earlier work is still pending/);
  assert.match(instructions, /Ordinary acknowledgments such as thank-you stay conversational/);
  assert.doesNotMatch(brokerInstructions({ ...call, registeredAssignment: false }), /task is registered with Primary/);
  assert.equal(brokerOpening('inbound', true), brokerOpening('inbound'));
});

test('watch briefing exposes its exact reported event and time without copying the evidence snapshot', () => {
  const watch = makeHarkSnapshot();
  const brief = JSON.parse(brokerWatchBrief(watch));
  assert.deepEqual(brief, { source: 'saved_watch_event', watchName: watch.watch.name,
    observedAt: watch.event.observedAt, summary: watch.event.summary });
  const instructions = brokerInstructions({ direction: 'outbound', purpose: 'Internal watch assignment', registeredAssignment: true, watch });
  assert.ok(instructions.includes(brokerWatchBrief(watch)));
  for (const privateField of [watch.ownerSupabaseUserId, watch.billing.mandateId, watch.originTaskId, watch.sourceDigest])
    assert.ok(!instructions.includes(privateField));
  assert.ok(!brokerInstructions({ direction: 'inbound', purpose: '', watch }).includes(watch.event.summary));
});

test('an oversized escaped watch summary is omitted whole while its name and observation remain', () => {
  const watch = makeHarkSnapshot();
  watch.event.summary = '\u0001'.repeat(2000);
  const encoded = brokerWatchBrief(watch);
  assert.ok(Buffer.byteLength(encoded) <= 8192);
  assert.deepEqual(JSON.parse(encoded), { source: 'saved_watch_event', watchName: watch.watch.name,
    observedAt: watch.event.observedAt, summaryOmitted: true, explanationFromPrimaryRequired: true });
});


test('trade language distinguishes preparing, confirmation and execution while pending work allows a topic change', () => {
  const instructions = brokerInstructions({ direction: 'inbound', purpose: '' });
  assert.match(instructions, /request to buy or sell starts by preparing an order for review/);
  assert.match(instructions, /only when Primary reports that exact execution stage/);
  assert.match(instructions, /Read the prepared quote and obtain the required confirmation before execution/);
  assert.match(instructions, /Acknowledge a change of topic and answer the new question/);
  assert.match(instructions, /leave internal debugging details in the saved record/);
});

test('capability and conversational questions are answered by voice without delegation or recaps', () => {
  for (const direction of ['inbound', 'outbound'] as const) {
    const instructions = brokerInstructions({ direction, purpose: 'Read the current portfolio.', registeredAssignment: direction === 'outbound' });
    assert.ok(instructions.includes(`What you can do: ${DEXTER_CAPABILITIES}`));
    assert.match(instructions, /what they can do with their account or holdings.*answer directly from that list in one or two sentences/);
    assert.match(instructions, /Answer those questions yourself; they need no account data, so do not delegate them/);
    assert.match(instructions, /Do not delegate to the backend when:[\s\S]*The caller asks what you can do, what they can do/);
    assert.match(instructions, /Repeat numbers only when the caller asks for them/);
    assert.match(instructions, /Refer only to requests the caller actually made during this call/);
    assert.match(instructions, /A repeat of the request already pending is not a new task/);
  }
  for (const capability of ['portfolio', 'wallet history', 'tokenized stocks', 'buys and sells under the permissions you already set', 'watches', 'text you'])
    assert.ok(DEXTER_CAPABILITIES.includes(capability), capability);
});

test('pending work gets one short acknowledgment and no spoken queue mechanics', () => {
  const instructions = brokerInstructions({ direction: 'inbound', purpose: '' });
  assert.match(instructions, /never say "queued", "follow-ups", or "later requests"/);
  assert.match(instructions, /at most one short acknowledgment such as "On it\."/);
  assert.match(instructions, /stay quiet until the result arrives unless the caller speaks; do not say you are still waiting/);
  assert.match(instructions, /A queued receipt means the later request has not started/);
});
