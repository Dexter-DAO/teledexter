import assert from 'node:assert/strict';
import test from 'node:test';
import { DEXTER_CAPABILITIES, brokerInstructions, brokerHistory, brokerOpening, brokerWatchBrief } from '../src/broker/instructions.js';
import { makeHarkSnapshot } from './fixtures/broker-hark.js';
import { LIVE_APPEND_MAX_BYTES } from '../src/live/session.js';
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
  assert.match(opening, /Immediately say: "Hey, it's Dexter\. What can I do for you\?"/);
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
  assert.match(instructions, /A correction changes or cancels work already requested\. Delegate it even while that work is in progress\./);
  assert.match(instructions, /acknowledges, or thanks you, as in "okay", "cool" or "I'm good", or makes waiting statements/);
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


test('trade language distinguishes preparing, confirmation and execution while pending work allows any topic', () => {
  const instructions = brokerInstructions({ direction: 'inbound', purpose: '' });
  assert.match(instructions, /A request to buy or sell starts with Primary preparing the order and usually asking the caller to confirm/);
  assert.match(instructions, /only when Primary's result reports that exact stage/);
  assert.match(instructions, /Read the prepared quote and get that confirmation before execution/);
  assert.match(instructions, /Answer unrelated questions and chat fully; a request in progress never limits what you can talk about/);
  assert.match(instructions, /leave internal debugging details and mechanics such as queues out of speech/);
});

test('the template headings, the delegation rules and the task list are the only truth about requests', () => {
  for (const direction of ['inbound', 'outbound'] as const) {
    const text = brokerInstructions({ direction, purpose: 'Read the current portfolio.' });
    for (const heading of ['Backchannel policy:', 'Interruption policy:', 'Delegation policy:', 'Backend tools:',
      'Delegate to the backend when:', 'Do not delegate to the backend when:', 'Task list:'])
      assert.ok(text.includes(heading), heading);
    assert.match(text, /Delegate before giving an answer that depends on backend work\.\nDo not guess the result while waiting\./);
    assert.match(text, /Delegate the caller's whole request in one delegation, including a multi-step request such as "sell it, then buy it back"\. Primary performs and sequences every step\. Never promise, schedule or narrate a step yourself/);
    assert.match(text, /It is the only truth about those requests/);
    assert.match(text, /A step or request that is not on the task list has not started\./);
    assert.match(text, /In progress: you have no outcome yet\. Never say it is done, placed, confirmed, sold, bought, sent or cancelled/);
    assert.match(text, /Say an action succeeded only when its result says it succeeded\./);
    assert.match(text, /If the application sends you a correction, say right away that you misspoke/);
    assert.doesNotMatch(text, /Truth rule|neutral hold|holding line/, 'no gate-era hold scripts');
  }
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
    assert.match(instructions, /A repeat is not a new task; say you're on it/);
  }
  for (const capability of ['portfolio', 'wallet history', 'tokenized stocks', 'buy and sell for you under the permissions you\'ve set',
    'paid data services', 'price or market-cap watches'])
    assert.ok(DEXTER_CAPABILITIES.includes(capability), capability);
});

test('call 039b39cd: the capability answer is two short sentences from the list only, with no history or limitations', () => {
  // SMS delivery is disabled in production, and the list states only what Primary's tools do.
  assert.doesNotMatch(DEXTER_CAPABILITIES, /\btext\b|\bSMS\b|\bcan't\b|\bcannot\b|\bonly\b|limit order|USDC/i);
  assert.equal(DEXTER_CAPABILITIES.split(/[.!?](?:\s|$)/).filter(Boolean).length, 1, 'the list itself is one spoken sentence');
  for (const direction of ['inbound', 'outbound'] as const) {
    const instructions = brokerInstructions({ direction, purpose: 'Read the current portfolio.', registeredAssignment: direction === 'outbound' });
    assert.match(instructions, /That list is the only source for a capability answer: say at most two short sentences/);
    assert.match(instructions, /never add, rename, or qualify capabilities using startup history, earlier conversation, or returned results/);
    assert.match(instructions, /never count or enumerate categories/);
    assert.match(instructions, /Do not mention limitations, restrictions, or things you cannot do unless the caller asks what you can't do/);
    assert.equal(instructions.match(/What you can do: /g)?.length, 1, 'one capability list reaches the voice');
  }
});

test('pending work gets one short acknowledgment, no repeated waiting lines and no queue mechanics', () => {
  const instructions = brokerInstructions({ direction: 'inbound', purpose: '' });
  assert.match(instructions, /After one short acknowledgment such as "On it\.", don't keep repeating that you're working on it or thank them for holding/);
  assert.match(instructions, /Never invent a completion estimate or timeout/);
  assert.match(instructions, /Waiting to start: it begins after the earlier request finishes/);
  assert.match(instructions, /mechanics such as queues out of speech/);
  assert.match(instructions, /When a result arrives, tell the caller at the next natural pause, outcome first/);
});

test('every voice opening fits one Live instructions append, so the speak-first greeting is never rejected locally', () => {
  // 819f2af shipped a 513-byte registered outbound opening; session.ts rejected it as
  // invalid_append_content (call 6b2ef8d1) and the greeting waited for the callee to speak.
  for (const direction of ['outbound', 'inbound'] as const)
    for (const registered of [false, true]) for (const watch of [false, true]) for (const notification of [false, true])
      for (const greeted of [false, true]) for (const heard of [false, true]) {
        const bytes = Buffer.byteLength(brokerOpening(direction, registered, watch, notification, greeted, heard));
        assert.ok(bytes > 0 && bytes <= LIVE_APPEND_MAX_BYTES, `${direction}/${registered}/${watch}/${notification}/${greeted}/${heard}: ${bytes} bytes`);
      }
});

test('an outbound opening after held caller words acknowledges them and offers in one reply, with no second greeting', () => {
  for (const [registered, watch, notification] of [[false, false, false], [true, false, false], [true, true, false], [true, false, true]] as const) {
    const heard = brokerOpening('outbound', registered, watch, notification, true, true);
    assert.notEqual(heard, brokerOpening('outbound', registered, watch, notification, true));
    assert.match(heard, /^The caller heard you say "Hey, it's Dexter\." and may have answered or said hello\. If so, acknowledge it briefly and, in that same reply, /);
    assert.match(heard, /Never greet again or say your name\./);
    assert.doesNotMatch(heard, /Immediately say|"Hey, it's Dexter\. Want/);
    // A registered task starts from the person's answer to the offer, never from the hello before it.
    if (registered) assert.match(heard, /Their hello is not their answer: pause, then delegate their answer to begin the registered task or honor a correction\./);
    else assert.match(heard, /Then pause to listen\.$/);
  }
  assert.match(brokerOpening('outbound', true, false, false, true, true), /"Want me to pull up your portfolio\?"/);
  assert.match(brokerOpening('outbound', true, true, false, true, true), /explain why the saved watch triggered this call/);
  assert.match(brokerOpening('outbound', true, false, true, true, true), /explain the saved notification/);
  // Inbound openings wait for the caller and keep their own text; without the clip Live's own greeting answers the hello.
  assert.equal(brokerOpening('inbound', false, false, false, true, true), brokerOpening('inbound', false, false, false, true));
  assert.equal(brokerOpening('outbound', false, false, false, false, true), brokerOpening('outbound'));
});

test('openings name no language and the instructions follow the caller into theirs', () => {
  for (const direction of ['inbound', 'outbound'] as const)
    for (const greeted of [false, true])
      assert.doesNotMatch(brokerOpening(direction, direction === 'outbound', false, false, greeted), /English/);
  assert.match(brokerInstructions({ direction: 'inbound', purpose: '' }),
    /When the caller speaks another language, reply in that language for the rest of the call/);
});

test('spoken amounts are dollars first, taken only from results delivered during this call, and never raw units', () => {
  for (const direction of ['inbound', 'outbound'] as const) {
    const text = brokerInstructions({ direction, purpose: 'Read the current portfolio.' });
    assert.match(text, /Spoken amounts:\nSay holding and trade amounts in dollars, rounded to the cent \("about three dollars of Apple"\), or "less than a cent"\./);
    assert.match(text, /Give a token or share count only when the caller asks, as a short decimal with two significant digits/);
    assert.match(text, /Never say "base tokens", "base units", "raw" or "lamports", or read out a long string of digits\./);
    assert.match(text, /Speak only figures that appear in a result delivered during this call\. Never compute totals, differences or percentages, and never present a figure from startup history as current\./);
    assert.doesNotMatch(text, /meaning and units of returned amounts/);
    assert.match(text, /Keep the meaning of returned percentages and times\./);
  }
});

test('after the opening a lone hello gets "I\'m here", and Dexter never greets or asks again', () => {
  for (const direction of ['inbound', 'outbound'] as const) {
    const text = brokerInstructions({ direction, purpose: 'Read the current portfolio.' });
    assert.match(text, /After the opening, answer a lone "hello\?" or "are you there\?" with a brief "I'm here\." Never greet the caller again or ask again what they want\./);
  }
});

test('main instructions stay within the Live configuration limit for inbound and the longest outbound brief', () => {
  // src/live/session.ts refuses instructions over 16,384 bytes (invalid_live_configuration).
  assert.ok(Buffer.byteLength(brokerInstructions({ direction: 'inbound', purpose: 'The person called Dexter.' })) < 13_000);
  assert.ok(Buffer.byteLength(brokerInstructions({ direction: 'outbound', purpose: 'x'.repeat(1500), registeredAssignment: true })) < 14_000);
});

