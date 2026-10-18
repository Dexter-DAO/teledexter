import assert from 'node:assert/strict';
import test from 'node:test';
import { faithfulRelay } from '../src/live/speechFacts.js';
const ok = (spoken: string, delivered: string) => faithfulRelay(spoken, delivered) && faithfulRelay(delivered, spoken);
const cases: Array<[string, string, boolean, string]> = [
  ['The SpaceX sale did not go through.', 'Unfortunately, the Spacex sale did not go through.', true, 'case + filler'],
  ['The SpaceX sale did not go through.', "The SpaceX sale didn't go through.", true, 'contraction'],
  ['The SpaceX sale did not go through.', 'The SpaceX sale went through.', false, 'polarity went through'],
  ['The BONK sale went through.', "The BONK sale didn't go through.", false, 'polarity reverse'],
  ["The SpaceX sale didn't go through, and nothing was sold.", "The sale's done.", false, 'sale done vs failure'],
  ["The SpaceX sale didn't go through, and nothing was sold.", "The SpaceX sale is done.", false, 'is done'],
  ['Nothing was sold.', 'Nothing was sold. Buying it back now.', false, 'appended claim'],
  ['Your BONK buy is being placed.', 'Your BONK buy was placed.', false, 'pending -> done'],
  ['Your BONK buy is being placed.', 'Your BONK buy is placed.', false, 'pending -> placed (being dropped)'],
  ['I will send 5 SOL.', 'I sent 5 SOL.', false, 'future -> past'],
  ["I haven't started the buy-back.", 'I have started the buy-back.', false, 'drop negation'],
  ['Sold 10 BONK. Bought 5 WIF.', 'Sold 5 BONK. Bought 10 WIF.', false, 'cross-sentence number swap'],
  ['Sold 10 BONK for $5.', 'Sold 5 BONK for $10.', false, '$ swap'],
  ['Sold 2000 BONK for 0.5 SOL.', 'Sold 0.5 BONK for 2000 SOL.', false, 'same-clause number swap'],
  ['You have 3 SOL and 40 USDC.', 'You have 40 SOL and 3 USDC.', false, 'balance swap (verbless)'],
  ['Swapped 5 BONK for WIF.', 'Swapped 5 WIF for BONK.', false, 'swap direction reversed'],
  ['Sold your BONK, but the WIF sale failed.', 'Sold your WIF, but the BONK sale failed.', false, 'asset/outcome swap'],
  ['Nothing was sold.', 'Nothing was not sold.', false, 'double negative'],
  ['The send to Alice failed.', 'The send to Alice did not fail.', false, 'negated failure'],
  ['Your order is pending.', 'Your order is not pending.', false, 'not pending'],
  ['Bought 1.5 SOL.', 'Bought 15 SOL.', false, 'decimal'],
  ['Bought 1,000 BONK.', 'Bought 1000 BONK.', true, 'thousands sep'],
  ['The buy failed. Nothing was sent.', 'The buy failed, nothing was sent.', true, 'punct merge'],
  ['Your SpaceX stock and cash are unchanged.', 'Your SpaceX stock and cash have not changed.', true, 'paraphrase unchanged'],
  ['The sale failed and nothing was bought.', 'The sale was bought and nothing failed.', false, 'verb reshuffle'],
];
for (const [d, s, want, label] of cases) test(`facts: ${label}`, () => assert.equal(ok(s, d), want, `${JSON.stringify(s)} vs ${JSON.stringify(d)}`));


test('facts: all-caps ticker that is a grammar word must still be supplied', () => {
  assert.equal(faithfulRelay('Buy CAN.', 'Buy.'), false);
  assert.equal(faithfulRelay('Buy CAN.', 'Buy CAN.'), true);
});
test('facts: negation attaches to its own outcome, even when both polarities occur', () => {
  assert.equal(ok('Sold BONK. Did not buy WIF.', 'Did not sell BONK. Bought WIF.'), false);
});


test('facts: an unavailable result cannot become available', () => {
  assert.equal(ok('Available.', 'Not available.'), false);
});
