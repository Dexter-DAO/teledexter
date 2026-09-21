import assert from 'node:assert/strict';
import test from 'node:test';
import { buildAlertTwilioFallbackSpeech } from '../src/outbound/alertConversation.js';
import type { FrozenAlert } from '../src/outbound/contract.js';
import { PendingCallRegistry } from '../src/outbound/PendingCallRegistry.js';
import {
  buildPathBoundMediaStreamUrl,
  buildTwilioStreamTwiml,
  isValidMediaBootstrapParams,
} from '../src/outbound/TwilioMediaBootstrap.js';

const INBOUND_SID = `CA${'1'.repeat(32)}`;
const OUTBOUND_SID = `CA${'2'.repeat(32)}`;
const OUTBOUND_TOKEN = 'b'.repeat(43);

test('inbound TwiML uses a path-bound one-shot bootstrap with no query string', () => {
  const registry = new PendingCallRegistry<{ direction: 'inbound' }>();
  const nowMs = 1_785_110_400_000;
  const token = registry.issue(
    INBOUND_SID,
    { direction: 'inbound' },
    nowMs + 60_000,
    nowMs,
  );
  const streamUrl = buildPathBoundMediaStreamUrl(
    'wss://phone.dexter.cash',
    INBOUND_SID,
    token,
  );
  const twiml = buildTwilioStreamTwiml({
    streamUrl,
    fallbackMessage: 'Realtime failed & the call is safe.',
  });

  assert.equal(streamUrl, `wss://phone.dexter.cash/media-stream/${INBOUND_SID}/${token}`);
  assert.equal(streamUrl.includes('?'), false);
  assert.match(twiml, new RegExp(`<Stream url="${streamUrl}" />`));
  assert.equal(twiml.includes('<Parameter'), false);
  assert.match(twiml, /Realtime failed &amp; the call is safe\./);

  const url = new URL(streamUrl);
  const [, route, callSid, streamToken] = url.pathname.split('/');
  assert.equal(route, 'media-stream');
  assert.deepEqual(
    registry.consume(callSid, streamToken, nowMs),
    { direction: 'inbound' },
  );
});

test('outbound TwiML binds the frozen alert context to its own CallSid', () => {
  const registry = new PendingCallRegistry<{
    direction: 'outbound-alert';
    eventId: string;
  }>();
  const nowMs = 1_785_110_400_000;
  registry.issueWithToken(
    OUTBOUND_SID,
    OUTBOUND_TOKEN,
    { direction: 'outbound-alert', eventId: 'cate-below-5.36m' },
    nowMs + 60_000,
    nowMs,
  );
  const streamUrl = buildPathBoundMediaStreamUrl(
    'wss://phone.dexter.cash/',
    OUTBOUND_SID,
    OUTBOUND_TOKEN,
  );
  const frozenAlert: FrozenAlert = {
    intentId: 'intent-frozen',
    eventId: 'event-frozen',
    recipientName: 'Branch',
    to: '+15615550100',
    subject: 'FISH market cap up 20%',
    severity: 'warning',
    briefing: 'FISH crossed the configured line.',
    observedAt: '2026-08-31T00:00:00.000Z',
    followupContext: {
      asset: 'FISH',
      direction: 'up',
      changePct: 20,
      marketCapUsd: 120_000,
    },
  };
  const fallbackSpeech = buildAlertTwilioFallbackSpeech(frozenAlert);
  const twiml = buildTwilioStreamTwiml({
    streamUrl,
    fallbackMessage: fallbackSpeech,
  });

  assert.equal(streamUrl.includes('?'), false);
  assert.match(twiml, /<Connect>[\s\S]*<Stream url="wss:\/\/phone\.dexter\.cash\/media-stream\//);
  assert.match(twiml, /<\/Connect>[\s\S]*<Say voice="Polly\.Matthew">/);
  assert.match(twiml, /Dexter alert for Branch\./u);
  assert.match(twiml, /FISH just ripped about 20 percent/u);
  assert.match(twiml, /Live conversation is unavailable right now\./u);
  assert.deepEqual(
    registry.consume(OUTBOUND_SID, OUTBOUND_TOKEN, nowMs),
    { direction: 'outbound-alert', eventId: 'cate-below-5.36m' },
  );
  assert.equal(registry.consume(OUTBOUND_SID, OUTBOUND_TOKEN, nowMs), null);
});

test('media bootstrap rejects malformed CallSids and tokens', () => {
  assert.equal(isValidMediaBootstrapParams(INBOUND_SID, 'a'.repeat(43)), true);
  assert.equal(isValidMediaBootstrapParams('CA123', 'a'.repeat(43)), false);
  assert.equal(isValidMediaBootstrapParams(INBOUND_SID, '../escape'), false);
  assert.throws(
    () => buildPathBoundMediaStreamUrl('wss://phone.dexter.cash', 'CA123', 'a'.repeat(43)),
    /invalid media bootstrap parameters/,
  );
});
