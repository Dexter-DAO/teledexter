import assert from 'node:assert/strict';
import test from 'node:test';

import {
  PHONE_OPENDXTER_INSTRUCTIONS,
  PHONE_OPENDEXTER_TOOL_NAMES,
} from '../src/session/phoneOpenDexterInstructions.js';
import { createSessionState } from '../src/session/SessionState.js';
import {
  classifyFatalSessionError,
  onError,
} from '../src/session/handlers/onError.js';

test('Phone teaches the hosted opaque-intent purchase sequence', () => {
  assert.deepEqual(PHONE_OPENDEXTER_TOOL_NAMES, [
    'x402_search',
    'x402_check',
    'x402_fetch',
    'x402_status',
    'x402_access',
    'x402_wallet',
    'dexter_portfolio',
    'dexter_prepare_asset_action',
    'dexter_execute_asset_action',
    'dexter_asset_action_status',
    'dexter_reconcile_asset_action',
    'dexter_wallet_history',
  ]);
  assert.match(PHONE_OPENDXTER_INSTRUCTIONS, /x402_check/);
  assert.match(PHONE_OPENDXTER_INSTRUCTIONS, /only the opaque intentId/);
  assert.match(PHONE_OPENDXTER_INSTRUCTIONS, /maxAmountAtomic/);
  assert.match(PHONE_OPENDXTER_INSTRUCTIONS, /same intentId/);
  assert.match(PHONE_OPENDXTER_INSTRUCTIONS, /never automatically call x402_fetch again/);
  assert.match(PHONE_OPENDXTER_INSTRUCTIONS, /removed x402_pay alias is not a tool/);
  assert.doesNotMatch(PHONE_OPENDXTER_INSTRUCTIONS, /call the tool with the URL/i);
});

test('Phone alias recovery never recommends the retired paid-call shape', () => {
  const messages: string[] = [];
  onError(
    createSessionState('phone-opendexter-contract'),
    (message) => messages.push(message),
    { error: { message: 'Tool mcp_dexter not found' } },
  );

  assert.equal(messages.length, 1);
  assert.match(messages[0]!, new RegExp(PHONE_OPENDEXTER_TOOL_NAMES.join(', ')));
  assert.match(messages[0]!, /intentId and maxAmountAtomic/);
  assert.match(messages[0]!, /Do NOT call mcp_dexter or x402_pay/);
});

test('an unavailable configured model hands the live call to Twilio fallback', () => {
  assert.equal(
    classifyFatalSessionError({
      error: { code: 'model_not_found', message: 'The model was not found.' },
    }),
    'openai_model_unavailable',
  );
});
