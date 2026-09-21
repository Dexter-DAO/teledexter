import assert from 'node:assert/strict';
import test from 'node:test';
import Twilio from 'twilio';
import {
  canonicalTwilioWebhookUrl,
  validateTwilioWebhook,
} from '../src/outbound/twilioWebhook.js';

test('Twilio webhook validation binds secret-free callback URL and form body', () => {
  const token = 'twilio-test-auth-token';
  const rawUrl = '/outbound-alerts/intent-1/answer';
  const publicBase = 'https://phone.dexter.cash';
  const url = canonicalTwilioWebhookUrl(publicBase, rawUrl);
  const body = { CallSid: 'CA123', CallStatus: 'in-progress' };
  const signature = Twilio.getExpectedTwilioSignature(token, url, body);
  assert.equal(new URL(url).search, '');
  assert.equal(rawUrl.includes('token'), false);

  assert.equal(
    validateTwilioWebhook(token, publicBase, {
      headers: { 'x-twilio-signature': signature },
      rawUrl,
      body,
    }),
    true,
  );
  assert.equal(
    validateTwilioWebhook(token, publicBase, {
      headers: { 'x-twilio-signature': signature },
      rawUrl,
      body: { ...body, CallStatus: 'completed' },
    }),
    false,
  );
});
