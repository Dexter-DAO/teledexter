import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import { conversationContext, voiceContextSchema, type VoiceContext } from '../src/broker/conversationContext.js';
import { isToldFact, spokenSentences, toldFacts, TOLD_BLOCK_BUDGET, TOLD_BLOCK_PREFIX } from '../src/broker/toldFacts.js';
import type { LiveTranscriptFragment } from '../src/live/types.js';

// Call 1325b55e, scrubbed: see the fixture's _comment.
const call = JSON.parse(readFileSync(new URL('./fixtures/voice-told-facts.json', import.meta.url), 'utf8')) as {
  holdingLabels: string[]; fragments: LiveTranscriptFragment[]; requests: Array<{ firstSequence: number; lastSequence: number }> };
const READ_OUT = 'MAPLE is about twenty-one forty, Birch nine oh-five, Cedarleaf four eighty, SpaceX two ninety, Apple one sixty, Tesla one oh-five, USDC forty cents,';

/** The voice context the backend builds for a request of the call: the speech before it, and the facts. */
function requestContext(index: number, withTold = true) {
  const request = call.requests[index]!;
  const before = call.fragments.filter(fragment => fragment.sequence < request.firstSequence);
  const first = call.fragments.find(fragment => fragment.sequence === request.firstSequence)!;
  const words = call.fragments.filter(fragment => fragment.role === 'user' && fragment.sequence >= request.firstSequence
    && fragment.sequence <= request.lastSequence).map(fragment => fragment.text).join('');
  const context = conversationContext(before, request.firstSequence, first.startMs,
    withTold ? { facts: toldFacts(before, call.holdingLabels), anchor: words } : undefined)!;
  return { words, context };
}
const size = (context: VoiceContext) => context.recentSpeech.reduce((total, turn) => total + turn.text.length, 0);

test('call 1325b55e: the facts Dexter told the caller are kept for the whole call, in order, once each', () => {
  assert.deepEqual(toldFacts(call.fragments, call.holdingLabels), [
    'Your portfolio is about forty-one dollars and twenty cents.',
    READ_OUT,
    'Just to confirm: sell one dollar of your SpaceX stock into cash at market price.',
    'We have about forty cents in USDC.',
    "That read didn't list every holding.",
    "Dexter wouldn't accept your SpaceX holding as an exact catalog selection for stock trading, so no order was created.",
    'You still hold that SpaceX stock, worth about two dollars and ninety cents.',
    'No single holding is worth twenty dollars.',
    'That was your whole portfolio.',
    'Birch Labs is the largest,',
    'Right, MAPLE.',
    "I haven't mentioned anything called Maple, but Cedar Leaf is five hundred tokens worth about four dollars and eighty cents.",
  ]);
  for (const sentence of ['On it.', "I'm checking how to run that sale.", 'Should I run it?', 'Checking that one.', "I'm still waiting on the sale.", 'Okay.'])
    assert.equal(isToldFact(sentence, call.holdingLabels), false, sentence);
  assert.equal(isToldFact('Right, MAPLE.', call.holdingLabels), true, 'a holding named from a portfolio note');
  assert.equal(isToldFact('Right, MAPLE.'), false);
  assert.equal(isToldFact('Cedar Leaf is the one.', call.holdingLabels), true, 'spaced or not, the same holding');
});

test('a sentence the caller talked over is one fact; a pause in Dexter\'s own speech ends one', () => {
  assert.deepEqual(spokenSentences(call.fragments.filter(fragment => fragment.sequence >= 93 && fragment.sequence <= 99)),
    ["That read didn't list every holding."], '"Ah, I" and "see" fell between its words');
  assert.deepEqual(spokenSentences(call.fragments.filter(fragment => fragment.sequence >= 36 && fragment.sequence <= 41)),
    ['USDC forty cents,', 'Okay.', 'Just to confirm']);
});

test('call 1325b55e: "the token you said I had $20 worth of" reaches Primary with the read-out it refers to', () => {
  const { words, context } = requestContext(1);
  assert.match(words, /the token that you said I had about \$20 worth of/);
  const without = requestContext(1, false).context;
  assert.ok(!JSON.stringify(without).includes('MAPLE'), 'the rolling window alone had lost the read-out, as on the call');
  const [block, ...window] = context.recentSpeech;
  assert.equal(block!.role, 'dexter');
  assert.equal(block!.text, `${TOLD_BLOCK_PREFIX}"Your portfolio is about forty-one dollars and twenty cents." "${READ_OUT}" `
    + '"Just to confirm: sell one dollar of your SpaceX stock into cash at market price." "That read didn\'t list every holding."');
  assert.deepEqual(window, without.recentSpeech.slice(1), 'the same recent speech, less its oldest turn');
  assert.ok(!block!.text.includes('You still hold that SpaceX stock'), 'a fact still in the recent speech is not repeated');
  voiceContextSchema.parse(context);
});

test('call 1325b55e: "you said Maple" reaches Primary with the read-out that named Maple', () => {
  const { words, context } = requestContext(2);
  assert.match(words, /you said Maple/);
  assert.ok(!JSON.stringify(requestContext(2, false).context).includes('MAPLE is about'), 'gone from the rolling window');
  assert.ok(context.recentSpeech[0]!.text.startsWith(TOLD_BLOCK_PREFIX));
  assert.ok(context.recentSpeech[0]!.text.includes(`"${READ_OUT}"`));
  assert.equal(context.recentSpeech.length, 6);
  voiceContextSchema.parse(context);
});

test('call 1325b55e: a request whose recent speech already holds every fact is sent exactly as before', () => {
  assert.deepEqual(requestContext(0).context, requestContext(0, false).context);
  assert.equal(requestContext(0).context.recentSpeech.length, 1);
});

test('the facts block keeps to its share of the six-item, 6000-character contract and keeps the facts a request is about', () => {
  const fragments: LiveTranscriptFragment[] = [];
  const say = (role: 'user' | 'assistant', text: string, startMs: number) =>
    fragments.push({ eventId: `e${fragments.length + 1}`, sequence: fragments.length + 1, role, text, startMs, endMs: startMs + 500 });
  say('assistant', 'You have about twelve dollars of WALNUT.', 1_000);
  for (let index = 0; index < 60; index++)
    say('assistant', ` You have about ${index + 2} dollars of token number ${index}, and its price moved a little today.`, 5_000 + index * 2_000);
  for (let index = 0; index < 8; index++) say(index % 2 ? 'assistant' : 'user', ` ${'word '.repeat(700)}`, 230_000 + index * 2_000);
  say('user', ' Sell some of the WALNUT you mentioned.', 260_000);
  const request = fragments.at(-1)!;
  const facts = toldFacts(fragments.slice(0, -1), ['WALNUT']);
  assert.equal(facts.length, 61);
  const context = conversationContext(fragments, request.sequence, request.startMs, { facts, anchor: request.text })!;
  voiceContextSchema.parse(context);
  const [block, ...window] = context.recentSpeech;
  assert.ok(block!.text.length <= TOLD_BLOCK_BUDGET, String(block!.text.length));
  assert.equal(size({ recentSpeech: window }), 6000 - TOLD_BLOCK_BUDGET, 'long recent turns fill the rest, newest first');
  assert.ok(size(context) <= 6000);
  assert.ok(block!.text.includes('"You have about twelve dollars of WALNUT."'), 'the oldest fact stays when the request is about it');
  assert.ok(block!.text.includes('token number 59'), 'the newest facts fill the rest');
  assert.ok(!block!.text.includes('token number 0,'), 'the oldest unrelated facts give way');
});
