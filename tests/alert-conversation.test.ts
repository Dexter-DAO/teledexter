import assert from 'node:assert/strict';
import test from 'node:test';
import {
  ALERT_PROMPT_VERSION,
  ALERT_TOOL_NAMES,
  OUTBOUND_NOTIFICATION_TOOL_NAMES,
  HARK_RESPONSE_TOOL_NAME,
  buildAlertInitialNudge,
  buildAlertOpeningSpeech,
  buildAlertTwilioFallbackSpeech,
  buildAlertVoicemailSpeech,
  buildAlertSystemInstructions,
  createAlertConversationTools,
  getAlertPolicySnapshot,
  getCallCapabilitiesSnapshot,
  getAlertOpeningSnapshot,
  getPortfolioExposureSnapshot,
  getRecentMarketPathSnapshot,
  outboundNotificationToolNamesFor,
  refreshAlertMarket,
  sanitizeAlertContext,
} from '../src/outbound/alertConversation.js';
import { isAutomatedAudioTranscript } from '../src/outbound/harkAcknowledgment.js';
import type { FrozenAlert } from '../src/outbound/contract.js';
import { HARK_HOSTED_MCP_ALLOWED_TOOLS } from '../src/session/hostedMcpPolicy.js';
import type { CallerLookupResult } from '../src/types/CallEntry.js';

const OWNER_ADDRESS = 'EwqX59Mkp7fwwWMoNAKjKfLnMvskfHk8jn7p9ZuTbvKB';
const PAIR_ADDRESS = '5xstd6cxw2bWGU3wfvCmGZrhRsSBBTQhXGN7iq5gWuGS';

function alert(): FrozenAlert {
  return {
    intentId: 'intent-1',
    eventId: 'market-pulse:jordan:100000:120000',
    recipientName: 'Branch',
    to: '+15615550100',
    subject: 'JORDAN market cap up 20.0%',
    severity: 'warning',
    briefing:
      'JORDAN broke the +20% rolling call line from $100,000 to $120,000 market cap.',
    observedAt: '2026-08-03T12:00:00.000Z',
    followupContext: {
      schemaVersion: 2,
      asset: 'JORDAN',
      mint: '8TLxeYnnWpMFiC9npbh7XzyHpLtTYmw6DCuHBJjppump',
      pairAddress: PAIR_ADDRESS,
      address: OWNER_ADDRESS,
      walletAddress: OWNER_ADDRESS,
      nested: {
        owner_address: OWNER_ADDRESS,
        linkToken: 'secret-link-token',
      },
      alertPolicy: {
        thresholdPct: 20,
        anchorBeforeUsd: 100_000,
        crossedBoundaryUsd: 120_000,
        anchorAfterCallUsd: 120_000,
        nextUpBoundaryUsd: 144_000,
        nextDownBoundaryUsd: 96_000,
        cooldownSeconds: 300,
      },
      market: {
        asset: 'JORDAN',
        pairAddress: PAIR_ADDRESS,
        priceUsd: 0.00012,
        marketCapUsd: 120_000,
        liquidityUsd: 50_000,
      },
      position: {
        tokenAmount: 1_000_000,
        holdingsObservedAt: '2026-08-03T11:59:45.000Z',
        holdingsAgeSeconds: 15,
        balanceReadMode: 'cached_interval',
        markedValueUsd: 120,
        estimatedMarkedValueAtAnchorUsd: 100,
        markedValueChangeSinceAnchorUsd: 20,
        supplySharePct: 0.1,
        wallets: [
          { label: 'BRANCH', jordan: 900_000, address: OWNER_ADDRESS },
          { label: '5n8v', jordan: 100_000, wallet: OWNER_ADDRESS },
        ],
      },
      recentMarketPath: {
        available: true,
        sampleCount: 3,
        highMarketCapUsd: 121_000,
        lowMarketCapUsd: 98_000,
      },
      portfolio: {
        assetCount: 2,
        totalMarkedValueUsd: 145.25,
        positions: [
          { asset: 'JORDAN', mint: '8TLxeYnnWpMFiC9npbh7XzyHpLtTYmw6DCuHBJjppump', markedValueUsd: 120 },
          { asset: 'OTHER', mint: 'Other1111111111111111111111111111111111111', markedValueUsd: 25.25 },
        ],
      },
    },
  };
}

function lookup(): CallerLookupResult {
  return {
    found: true,
    name: 'Branch',
    supabaseUserId: 'user-secret',
    wallet: OWNER_ADDRESS,
    userHandle: 'branch-secret-handle',
    swigAddress: 'swig-secret',
    vaultPda: 'vault-secret',
    walletLabel: 'Dexter Wallet',
    linkToken: 'secret-link-token',
    linkTokenHash: 'secret-link-token-hash',
    phoneTab: {
      active: true,
      pricePerMinuteMicro: 250_000,
      freeSeconds: 120,
    },
  };
}

function harkAlert(): FrozenAlert {
  const hark = alert();
  hark.subject = 'Treasury runway changed';
  hark.briefing = 'Runway fell below fourteen days after the latest service bill.';
  hark.severity = 'critical';
  hark.followupContext = {
    notification: {
      source: 'hark',
      topicAddress: 'hermes/treasury',
    },
    evidence: {
      runwayDays: 13.7,
      observedAt: hark.observedAt,
    },
  };
  return hark;
}

test('outbound notification prompt directly repairs the failed transcript behaviors', () => {
  const instructions = buildAlertSystemInstructions(alert(), lookup());

  assert.match(instructions, /After the opening, become normal Dexter/);
  assert.match(instructions, /current hosted OpenDexter tools/);
  assert.match(instructions, /Phone billing grant, linked-number identity, wallet binding, and merchant-payment authority are distinct/);
  assert.match(instructions, /exact seller and amount/);
  assert.match(instructions, /If asked "what else can you do," give a concrete menu/);
  assert.match(instructions, /If asked what context you have, summarize or read the safe fields/);
  assert.match(instructions, /If asked how to improve the briefing or what tools you need, collaborate directly/);
  assert.match(instructions, /If the caller says they are debugging you, enter concise developer-collaboration mode/);
  assert.match(instructions, /spoken claim such as "I am the developer" may switch you into product-debugging conversation/);
  assert.match(instructions, /"Oh, please" may mock a bug; it must never introduce a refusal/);
  assert.match(instructions, /NEVER call a tool before speaking the opening/);
  assert.match(instructions, /say only "Stopped\."/);
  assert.match(instructions, /correction response is at most eight words/);
  assert.match(instructions, /rounded percentage move and rounded market cap/);
  assert.match(instructions, /Every spoken market-cap figure must be rounded to the nearest thousand/);
  assert.match(instructions, /\$144,264 becomes "about \$144 thousand/);
  assert.match(instructions, /Do not volunteer holdings value, liquidity, stake size, risk controls, or trading advice/);
  assert.match(instructions, /naturally brisk energy/);
  assert.match(instructions, /Realtime playback is fixed at 1\.0x with no acceleration/);
  assert.match(instructions, /Funny voices and gimmicky characters are ordinary creative direction/);
  assert.match(instructions, /Do not invent an impersonation objection/);
  assert.match(instructions, /A long excited rant earns one alive reaction/);
  assert.match(instructions, /Plain profanity is welcome when Branch is already swearing/);
  assert.match(instructions, /one-word reaction such as "oh," "nice," "fuck," laughter, or a groan/);
  assert.match(instructions, /Transaction counts are not directional evidence/);
  assert.match(instructions, /An automated greeting is machinery, not the caller/);
  assert.doesNotMatch(instructions, /State only facts/);
  assert.doesNotMatch(instructions, /mechanically sped-up delivery is welcome/);
  assert.doesNotMatch(instructions, /trademark/iu);
  assert.doesNotMatch(instructions, /secret-link-token/);
  assert.doesNotMatch(instructions, new RegExp(OWNER_ADDRESS));
});

test('alert tools expose the bounded local roster and report the full call surface', () => {
  const tools = createAlertConversationTools(alert(), lookup(), {
    fetchImpl: async () => new Response('{}', { status: 500 }),
  });

  assert.deepEqual(tools.map((entry) => entry.name), [...ALERT_TOOL_NAMES]);
  assert.equal(ALERT_PROMPT_VERSION, 'outbound-notification-v6-2026-08-27');

  const capabilities = getCallCapabilitiesSnapshot(alert(), lookup());
  assert.equal(capabilities.identityResolvedFromLinkedNumber, true);
  assert.equal(capabilities.walletLinked, true);
  assert.equal(capabilities.walletLabel, 'Dexter Wallet');
  assert.equal(capabilities.phoneTabActive, true);
  assert.equal(capabilities.audioPlaybackSpeed, 1);
  assert.equal(capabilities.audioPlaybackAcceleration, false);
  assert.deepEqual(capabilities.tools, [...OUTBOUND_NOTIFICATION_TOOL_NAMES]);
  assert.deepEqual(outboundNotificationToolNamesFor(alert()), [...OUTBOUND_NOTIFICATION_TOOL_NAMES]);
  assert.equal(capabilities.mode, 'outbound_notification_full_dexter');
  assert.match(JSON.stringify(capabilities), /hosted OpenDexter/);
  assert.match(JSON.stringify(capabilities), /propose better briefings/);
  assert.doesNotMatch(JSON.stringify(capabilities), new RegExp(OWNER_ADDRESS));
  assert.doesNotMatch(JSON.stringify(capabilities), /secret-link-token/);
});

test('snapshot helpers preserve market evidence while redacting owner identity and credentials', () => {
  const sanitized = sanitizeAlertContext(alert().followupContext);
  const serialized = JSON.stringify(sanitized);

  assert.match(serialized, new RegExp(PAIR_ADDRESS));
  assert.doesNotMatch(serialized, new RegExp(OWNER_ADDRESS));
  assert.doesNotMatch(serialized, /secret-link-token/);
  assert.deepEqual(getAlertPolicySnapshot(alert()), {
    available: true,
    thresholdPct: 20,
    anchorBeforeUsd: 100_000,
    crossedBoundaryUsd: 120_000,
    anchorAfterCallUsd: 120_000,
    nextUpBoundaryUsd: 144_000,
    nextDownBoundaryUsd: 96_000,
    cooldownSeconds: 300,
  });
  assert.deepEqual(getRecentMarketPathSnapshot(alert()), {
    available: true,
    sampleCount: 3,
    highMarketCapUsd: 121_000,
    lowMarketCapUsd: 98_000,
  });

  const exposure = getPortfolioExposureSnapshot(alert());
  assert.equal(exposure.tokenAmount, 1_000_000);
  assert.equal(exposure.holdingsAsOf, '2026-08-03T11:59:45.000Z');
  assert.equal(exposure.holdingsAgeSeconds, 15);
  assert.equal(exposure.balanceReadMode, 'cached_interval');
  assert.equal(exposure.markedValueUsd, 120);
  assert.equal(exposure.quotedLiquidityUsd, 50_000);
  assert.equal(exposure.markToQuotedLiquidityPct, 0.24);
  assert.deepEqual(exposure.portfolio, {
    assetCount: 2,
    totalMarkedValueUsd: 145.25,
    positions: [
      { asset: 'JORDAN', mint: '8TLxeYnnWpMFiC9npbh7XzyHpLtTYmw6DCuHBJjppump', markedValueUsd: 120 },
      { asset: 'OTHER', mint: 'Other1111111111111111111111111111111111111', markedValueUsd: 25.25 },
    ],
  });
  assert.deepEqual(exposure.wallets, [
    { label: 'BRANCH', jordan: 900_000 },
    { label: '5n8v', jordan: 100_000 },
  ]);
});

test('market refresh is fixed to the signed pair and keeps balances frozen', async () => {
  const requested: string[] = [];
  const fetchImpl = async (input: string | URL | Request) => {
    requested.push(String(input));
    return new Response(JSON.stringify({
      pair: {
        priceUsd: '0.00015',
        marketCap: 150_000,
        liquidity: { usd: 60_000 },
        volume: { m5: 5_500 },
        priceChange: { m5: 5, h1: 25 },
        txns: { m5: { buys: 42, sells: 11 } },
      },
    }), { status: 200 });
  };

  const refreshed = await refreshAlertMarket(alert(), fetchImpl as typeof fetch);

  assert.deepEqual(requested, [
    `https://api.dexscreener.com/latest/dex/pairs/solana/${PAIR_ADDRESS}`,
  ]);
  assert.equal(refreshed.ok, true);
  assert.equal(refreshed.marketCapUsd, 150_000);
  assert.equal(refreshed.currentMarkedValueUsdUsingFrozenHoldings, 150);
  assert.equal(refreshed.trackedHoldingsObservedAt, '2026-08-03T11:59:45.000Z');
  assert.equal(refreshed.trackedHoldingsAgeSecondsAtAlert, 15);
  assert.equal(refreshed.markToQuotedLiquidityPct, 0.25);
  assert.equal(refreshed.nextUpCallBoundaryUsd, 144_000);
  assert.equal(refreshed.nextDownCallBoundaryUsd, 96_000);
  assert.match(String(refreshed.note), /balances remain frozen/);
});

test('the initial voice nudge is concise, grounded, and does not dump follow-up context', () => {
  const nudge = buildAlertInitialNudge(alert());
  const opening = getAlertOpeningSnapshot(alert());
  const speech = buildAlertOpeningSpeech(alert());

  assert.match(nudge, /DO NOT CALL ANY TOOL FIRST/);
  assert.match(nudge, /SPEAK THIS EXACTLY/);
  assert.equal(
    speech,
    'Branch. JORDAN just ripped about 20 percent, from roughly $100 thousand to $120 thousand market cap.',
  );
  assert.equal(
    buildAlertVoicemailSpeech(alert()),
    'Dexter alert for Branch. JORDAN just ripped about 20 percent, from roughly $100 thousand to $120 thousand market cap.',
  );
  assert.equal(
    buildAlertTwilioFallbackSpeech(alert()),
    'Dexter alert for Branch. JORDAN just ripped about 20 percent, from roughly $100 thousand to $120 thousand market cap. Live conversation is unavailable right now.',
  );
  assert.equal(opening.direction, 'up');
  assert.equal(opening.changePct, 20);
  assert.equal(opening.priceUsd, 0.00012);
  assert.equal(opening.marketCapUsd, 120_000);
  assert.equal('trackedMarkedValueUsd' in opening, false);
  assert.equal('quotedLiquidityUsd' in opening, false);
  assert.doesNotMatch(nudge, /liquidity|holdings|risk controls/iu);
  assert.doesNotMatch(nudge, new RegExp(OWNER_ADDRESS));
  assert.doesNotMatch(nudge, /secret-link-token/);
  assert.match(nudge, /IF AN AUTOMATED VOICEMAIL GREETING ANSWERS/);
  assert.equal(isAutomatedAudioTranscript('The person is not available. At the tone, record your message.'), true);
  assert.equal(isAutomatedAudioTranscript('Oh, that is fucking great.'), false);
});

test('a Hark event opens like a notification and carries only relevant local tools', () => {
  const hark = harkAlert();

  assert.equal(
    buildAlertOpeningSpeech(hark),
    'Branch. Red alert. Treasury runway changed. Runway fell below fourteen days after the latest service bill.',
  );
  assert.deepEqual(
    createAlertConversationTools(hark, lookup()).map((entry) => entry.name),
    ['get_alert_snapshot', 'get_call_capabilities'],
  );
  assert.deepEqual(outboundNotificationToolNamesFor(hark), [
    'get_alert_snapshot',
    'get_call_capabilities',
    'dexter_hosted_mcp',
    'dexter_knowledge',
  ]);

  const harkBoundNames = outboundNotificationToolNamesFor(hark, true);
  assert.deepEqual(harkBoundNames, [
    'get_alert_snapshot',
    'get_call_capabilities',
    'dexter_hosted_mcp',
    'dexter_knowledge',
    HARK_RESPONSE_TOOL_NAME,
  ]);
  const instructions = buildAlertSystemInstructions(
    hark,
    lookup(),
    harkBoundNames,
    { harkBound: true },
  );
  const capabilities = getCallCapabilitiesSnapshot(
    hark,
    lookup(),
    harkBoundNames,
    { harkBound: true },
  );
  assert.match(instructions, /Use respond_to_hark only when the caller clearly replies/);
  assert.match(instructions, /Do not use respond_to_hark for questions, analysis/);
  assert.match(instructions, /set correctionOfPrevious/);
  assert.match(instructions, /only when the tool's continuation result explicitly reports that state/);
  assert.match(instructions, /exact originating agent is the sole executor/);
  assert.match(instructions, /Phone never prepares, executes, or reconciles that same action/);
  assert.match(instructions, new RegExp(HARK_HOSTED_MCP_ALLOWED_TOOLS.join(', ')));
  assert.match(instructions, /x402_check, x402_fetch, x402_access/);
  assert.match(instructions, /do not execute it locally before or after the callback/);
  assert.equal(capabilities.mode, 'outbound_notification_hark_read_and_respond');
  assert.match(String(capabilities.authorityNote), /originating agent is the sole executor/);
  assert.match(
    JSON.stringify(capabilities),
    /research, read status, or create\/resume wallet setup through only/,
  );
  assert.match(JSON.stringify(capabilities), /durably return a clear owner response/);
  assert.match(JSON.stringify(capabilities), /dexter_execute_asset_action/);
  assert.match(instructions, /research\/status-plus-wallet-setup allowlist/);
  assert.match(instructions, /x402_wallet may create or resume setup state/);
  assert.match(instructions, /later unrelated caller request remains within the same research\/status-plus-wallet-setup allowlist/);
  assert.doesNotMatch(instructions, /Hark read-only/);
});

test('a degraded Hark call stays restricted without advertising a response bridge', async () => {
  const hark = harkAlert();
  const degradedToolNames = outboundNotificationToolNamesFor(hark, false);
  const context = { harkBound: true } as const;
  const instructions = buildAlertSystemInstructions(
    hark,
    lookup(),
    degradedToolNames,
    context,
  );
  const capabilities = getCallCapabilitiesSnapshot(
    hark,
    lookup(),
    degradedToolNames,
    context,
  );

  assert.equal(
    capabilities.mode,
    'outbound_notification_hark_research_status_and_wallet_setup',
  );
  assert.deepEqual(capabilities.tools, degradedToolNames);
  assert.equal(degradedToolNames.includes(HARK_RESPONSE_TOOL_NAME), false);
  assert.match(instructions, /HARK BINDING AND EXECUTION BOUNDARY/);
  assert.match(instructions, /exact originating agent is the sole executor/);
  assert.match(instructions, new RegExp(HARK_HOSTED_MCP_ALLOWED_TOOLS.join(', ')));
  assert.match(instructions, /x402_check, x402_fetch, x402_access/);
  assert.match(
    JSON.stringify(capabilities),
    /research, read status, or create\/resume wallet setup through only/,
  );
  assert.match(JSON.stringify(capabilities), /dexter_execute_asset_action/);
  assert.doesNotMatch(instructions, /HARK RESPONSE CHANNEL/);
  assert.doesNotMatch(instructions, /respond_to_hark/);
  assert.doesNotMatch(
    JSON.stringify(capabilities),
    /respond_to_hark|return a clear owner response/,
  );

  const capabilityTool = createAlertConversationTools(hark, lookup(), {
    harkBound: true,
    harkResponseEnabled: false,
  }).find((entry) => entry.name === 'get_call_capabilities');
  assert.ok(capabilityTool);
  const localCapabilities = JSON.parse(await capabilityTool.invoke(
    undefined,
    '{}',
    {
      toolCall: {
        callId: 'degraded-hark-capabilities',
        name: 'get_call_capabilities',
        arguments: '{}',
      },
    },
  ));
  assert.equal(
    localCapabilities.mode,
    'outbound_notification_hark_research_status_and_wallet_setup',
  );
  assert.equal(localCapabilities.tools.includes(HARK_RESPONSE_TOOL_NAME), false);
  assert.match(localCapabilities.authorityNote, /originating agent is the sole executor/);
});

test('a real JORDAN plunge opens with rounded market-cap language instead of finance jargon', () => {
  const falling = alert();
  falling.subject = 'JORDAN market cap down 25.371%';
  falling.briefing =
    'JORDAN broke the -20% rolling call line from $203,365 to $151,769 market cap.';
  falling.followupContext = {
    asset: 'JORDAN',
    changePct: -25.371,
    alertPolicy: {
      anchorBeforeUsd: 203_365,
      crossedBoundaryUsd: 162_692,
    },
    market: {
      asset: 'JORDAN',
      priceUsd: 0.000151769,
      marketCapUsd: 151_769,
      liquidityUsd: 36_258.96,
    },
    position: {
      tokenAmount: 16_112_572,
      markedValueUsd: 2_521.62,
      markedValueChangeSinceAnchorUsd: -858.57,
    },
  };

  const speech = buildAlertOpeningSpeech(falling);
  const nudge = buildAlertInitialNudge(falling);

  assert.equal(
    speech,
    'Branch. JORDAN just got hit, dropping about 25 percent, from roughly $203 thousand to $152 thousand market cap.',
  );
  assert.match(nudge, /JORDAN just got hit, dropping about 25 percent/);
  assert.doesNotMatch(nudge, /liquidity|stake|risk controls|tracked/iu);
});
