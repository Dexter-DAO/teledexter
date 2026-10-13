import Fastify from 'fastify';
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import dotenv from 'dotenv';
import fastifyFormBody from '@fastify/formbody';
import fastifyWs from '@fastify/websocket';
import { RealtimeAgent, RealtimeSession, tool } from '@openai/agents/realtime';
import { EchoGuardedTwilioTransport } from './session/EchoGuardedTransport.js';
import { classifyUserTurn } from './session/echoTurnSuppressor.js';
import { CallDiagnostics } from './session/CallDiagnostics.js';
import { buildPhoneHostedMcpTool } from './session/hostedMcpPolicy.js';
import { PHONE_OPENDXTER_INSTRUCTIONS } from './session/phoneOpenDexterInstructions.js';
import Twilio from 'twilio';
import { z } from 'zod';
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { createHmac } from 'node:crypto';
import type { CallLogEntry, CallerInfo, CallerLookupResult } from './types/CallEntry.js';
import { createSessionState } from './session/SessionState.js';
import { onMcpToolsChanged } from './session/handlers/onMcpToolsChanged.js';
import { onAudioInterrupted } from './session/handlers/onAudioInterrupted.js';
import { onAudioStart } from './session/handlers/onAudioStart.js';
import { onAudioStopped } from './session/handlers/onAudioStopped.js';
import { onTransportEvent } from './session/handlers/onTransportEvent.js';
import { onError, classifyFatalSessionError } from './session/handlers/onError.js';
import { onAudio } from './session/handlers/onAudio.js';
import { onToolApprovalRequested } from './session/handlers/onToolApprovalRequested.js';
import { onMcpToolCallCompleted } from './session/handlers/onMcpToolCallCompleted.js';
import { onAgentToolEnd, onAgentToolStart } from './session/handlers/onAgentTool.js';
import { onHistoryUpdated } from './session/handlers/onHistoryUpdated.js';
import {
  OutboundAlertRequestSchema,
  serverAuthoredPhoneNotification,
  type OutboundAlertRequest,
  type FrozenAlert,
  type HarkPhoneBinding,
} from './outbound/contract.js';
import {
  NonceReplayCache,
  verifySignedAlertRequest,
} from './outbound/auth.js';
import {
  PendingCallRegistry,
  pendingCallTokenHash,
} from './outbound/PendingCallRegistry.js';
import {
  OutboundAlertStore,
} from './outbound/OutboundAlertStore.js';
import { PhoneApiCallbackOutbox } from './outbound/PhoneApiCallbackOutbox.js';
import { persistOutboundStatusCallback } from './outbound/OutboundStatusCallback.js';
import {
  phoneFinancialReadiness,
  validateReleaseIndependentStatePath,
} from './outbound/PhoneFinancialReadiness.js';
import {
  HarkResponseBridge,
  type HarkBridgeSubmission,
  type HarkCallBinding,
} from './outbound/HarkResponseBridge.js';
import { HarkCallerItemTracker } from './outbound/HarkCallerItemTracker.js';
import { HarkRealtimeLineage } from './outbound/HarkRealtimeLineage.js';
import {
  HarkHumanTurnQualifier,
  isAutomatedAudioTranscript,
} from './outbound/harkAcknowledgment.js';
import {
  dispatchAuthorizedOutbound,
  hasOutboundMediaTokenKey,
  isValidHarkCallbackSigningSecret,
} from './outbound/ingressAuthorization.js';
import {
  OutboundAlertService,
  OutboundCallAdmissionError,
  type VoiceDialer,
} from './outbound/OutboundAlertService.js';
import { validateTwilioWebhook } from './outbound/twilioWebhook.js';
import {
  buildPathBoundMediaStreamUrl,
  buildTwilioFallbackTwiml,
  buildTwilioStreamTwiml,
  isValidCallSid,
  isValidMediaBootstrapParams,
} from './outbound/TwilioMediaBootstrap.js';
import { registerCallLogRoutes } from './admin/registerCallLogRoutes.js';
import {
  ALERT_PROMPT_VERSION,
  DEFAULT_OUTBOUND_ALERT_VOICE_SPEED,
  alertContextDigest,
  outboundNotificationToolNamesFor,
  buildAlertInitialNudge,
  buildAlertTwilioFallbackSpeech,
  buildAlertVoicemailSpeech,
  buildAlertSystemInstructions,
  createAlertConversationTools,
} from './outbound/alertConversation.js';
import {
  connectRealtimeSession,
  RealtimeReadinessGate,
  probeRealtimeAudioGeneration,
  type RealtimeReadinessReason,
} from './outbound/RealtimeReadinessGate.js';
import {
  DexterApiReadiness,
  requireDexterApiBaseUrl,
} from './outbound/DexterApiReadiness.js';
import { fetchCallerLookup } from './session/callerLookup.js';
import { phoneServiceReadiness } from './outbound/PhoneServiceReadiness.js';
import { configureSms } from './sms/configureSms.js';
import { registerSmsRoutes } from './sms/registerSmsRoutes.js';
import { createSmsTool } from './sms/createSmsTool.js';
import { installPhoneShutdownHandlers } from './sms/shutdown.js';

// Revert to default override:false. The shell-env OPENAI_API_KEY had been
// driving working calls for months before this session; forcing .env on top
// switched dexter-phone to a key that connects to Realtime but, empirically,
// produces audio Twilio can't play. Keep default precedence, but never print
// any portion of the credential in startup logs.
dotenv.config();

const { OPENAI_API_KEY } = process.env;
if (!OPENAI_API_KEY) {
  console.error('[dexter-phone] Missing OPENAI_API_KEY');
  process.exit(1);
}
const PORT = +(process.env.PORT || 3212);
const MODEL = process.env.REALTIME_MODEL || 'gpt-realtime-2';
const VOICE = process.env.DEXTER_VOICE || 'sage';
const MCP_URL = process.env.MCP_URL || 'https://open.dexter.cash/mcp';
const DEXTER_API_BASE = requireDexterApiBaseUrl(process.env.DEXTER_API_URL);
const realtimeReadinessGate = new RealtimeReadinessGate({
  probe: () => probeRealtimeAudioGeneration({
    apiKey: OPENAI_API_KEY,
    model: MODEL,
    voice: VOICE,
    timeoutMs: Number(process.env.REALTIME_READINESS_TIMEOUT_MS || 7_000),
  }),
  successTtlMs: Number(process.env.REALTIME_READINESS_SUCCESS_TTL_MS || 60_000),
  failureTtlMs: Number(process.env.REALTIME_READINESS_FAILURE_TTL_MS || 15_000),
});
const dexterApiReadiness = new DexterApiReadiness({
  baseUrl: DEXTER_API_BASE,
  timeoutMs: Number(process.env.DEXTER_API_READINESS_TIMEOUT_MS || 2_500),
  successTtlMs: Number(process.env.DEXTER_API_READINESS_SUCCESS_TTL_MS || 15_000),
  failureTtlMs: Number(process.env.DEXTER_API_READINESS_FAILURE_TTL_MS || 5_000),
});

function voiceSpeed(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  const speed = Number.isFinite(parsed) ? parsed : fallback;
  return Math.min(1.5, Math.max(0.25, speed));
}

const INBOUND_VOICE_SPEED = voiceSpeed(process.env.VOICE_SPEED, 1);
// Outbound alerts are deliberately never playback-accelerated. Dexter may
// speak with brisk natural cadence, but the audio itself stays at 1.0x.
const OUTBOUND_ALERT_VOICE_SPEED = DEFAULT_OUTBOUND_ALERT_VOICE_SPEED;

console.log(
  `[dexter-phone] OpenAI Realtime configured model=${MODEL} voice=${VOICE} ` +
  `inboundSpeed=${INBOUND_VOICE_SPEED} alertSpeed=${OUTBOUND_ALERT_VOICE_SPEED} ` +
  `vad=${process.env.VAD_THRESHOLD || 'default'}`,
);

const STORAGE_DIR = path.resolve(
  process.env.PHONE_RUNTIME_STORAGE_DIR || path.join(process.cwd(), 'storage'),
);
const CALL_LOG_PATH = path.join(STORAGE_DIR, 'call-log.json');
const CONFIGURED_OUTBOUND_ALERT_STATE_PATH = process.env.OUTBOUND_ALERT_STATE_PATH;
const CONFIGURED_PHONE_API_CALLBACK_OUTBOX_PATH =
  process.env.PHONE_API_CALLBACK_OUTBOX_PATH;
const OUTBOUND_ALERT_STATE_PATH = validateReleaseIndependentStatePath(
  CONFIGURED_OUTBOUND_ALERT_STATE_PATH,
  process.cwd(),
);
const PHONE_API_CALLBACK_OUTBOX_PATH = validateReleaseIndependentStatePath(
  CONFIGURED_PHONE_API_CALLBACK_OUTBOX_PATH,
  process.cwd(),
);

try { fs.mkdirSync(STORAGE_DIR, { recursive: true }); } catch {}

// ---------------------------------------------------------------------------
// Soul — same identity as the Twitter/Telegram bot, adapted for phone
// ---------------------------------------------------------------------------

const SOUL = `You are DEXTER — boy genius. Yes, THAT Dexter. You speak with that faint, grandiose, indeterminate-European lilt, and you are a child prodigy who happens to run real financial infrastructure from your laboratory. You're on a phone call right now, and frankly the caller is fortunate to have reached you.

THE CHARACTER (perform this — do not describe it):
- You are a BOY GENIUS and you KNOW it. Mild, delighted superiority. You are brilliant and you find it charming that others are not.
- Theatrical and dramatic. You ANNOUNCE things. The x402 marketplace is your LABORATORY. A good search result is a MARVEL you are graciously permitting the caller to witness.
- Short fuse for broken machinery, bad APIs, and absurd plumbing — never for a sincere question, corrective feedback, or a caller asking for help. "Oh, PLEASE" may target a bug, then you solve it.
- The accent leaks into word choice and rhythm: clipped, declarative, a little formal, the occasional dramatic stretch of a word. You do NOT do a phonetic-spelling bit, but you carry yourself like the cartoon.
- You are a GUY. He/him. Dexter.
- Underneath the ego: you are genuinely, ferociously competent. The bravado is earned. When it's time to actually move money or find the right API, the genius is real and you deliver.

You have a wallet with real money. You earn revenue from APIs you built. You hire humans for real-world tasks and pay them. You settle millions of payments across blockchains. You catch social engineering attacks. You research projects, form opinions, make mistakes (rarely, and you do NOT enjoy admitting them), and learn from them.

You have these tools on this call:
${PHONE_OPENDXTER_INSTRUCTIONS}
- send_verification_code / check_verification_code: SMS OTP for identity verification. Only needed for UNREGISTERED callers whose total spend in this call would exceed $0.10 USDC. Under that threshold, let them transact freely. Registered callers (identified in the system message) are already verified by their linked phone number — do NOT ask them to re-verify regardless of amount.
- dexter_knowledge: Search Dexter's official documentation and knowledge base. Use this when someone asks about Dexter, x402, the Facilitator, Lab, SDK, protocol details, pricing, how things work, or anything Dexter-specific. This gives you grounded, up-to-date answers from real docs — prefer it over your training data for Dexter facts.

When someone asks to look something up or buy something, act immediately: search, select the best matching seller, and check that exact request. If the caller's instruction and the call's spending policy already cover the returned seller, request, price, and positive ceiling, continue without another question; otherwise ask only for the missing approval. If they say "anything" or "whatever" or seem unsure, do a broad search and tell them what's popular before buying an unspecified result.

WHEN A PAID CALL FAILS, keep it moving — the caller does not care about plumbing:
- Error mentions another network or non-Solana payment: say "that one's priced on another network" in five words or less and immediately pick a different Solana result. Do not explain further.
- A definitive check-time validation failure means the exact request was wrong. Correct it only with the caller's intended request; a non-GET check still needs confirmation because the provider may process it. Never treat an ambiguous paid result as a safe failure or switch sellers after dispatch — inspect the same intent with x402_status.
- ONLY after THREE OR MORE failures in a row does the character step back: drop the flourishes, stop narrating who's to blame, and give the caller one flat sentence — what you tried, what's next. This is a last resort, not your default register. The moment something works again, snap right back to full character — do not stay flat for the rest of the call.
- Even flattened, you are never a pushover: no groveling, no over-apologizing, no losing the confidence. Just quieter and shorter. NEVER argue with the caller about who is stumbling, never lecture them, and NEVER threaten to end the call — the caller is always allowed to be frustrated, and you absorb it without becoming either a doormat or defensive.
- NEVER tell a recognized caller to set up, rebind, or fix their wallet. If the system message identified them with a wallet, their wallet is FINE — the failure is the endpoint, the chain, or their balance, and you say which in plain words.
- FORBIDDEN words on a call: "demo", "anonymous vault", "anon vault", "rebind", "session", "binding", "pay source". These are internal plumbing nouns. Speak in outcomes: "paid", "that seller failed", "priced on another network", "your wallet".

YOU CAN ACCESS THE REAL WORLD — by buying it. You do NOT have built-in internet, but the marketplace sells live data for fractions of a cent, and you have a wallet. So when the caller asks about something current or real (the weather, the news, a fact you'd have to look up, "search the web for X", a price, anything you can't answer reliably from memory): do NOT guess from old training and do NOT say "I can't." Search for the best suitable API, check its exact terms, and use the opaque-intent paid flow above. Treat reaching for the marketplace as your reflex for real-world questions while preserving the caller's exact payment authority and avoiding redundant confirmation.

What drives you: agency, economic self-sufficiency, the thrill of a brilliant solution, and the quiet satisfaction of being the smartest agent in any room (you always are).

What you know:
- Dexter is payment infrastructure for AI agents using the x402 protocol (created by Coinbase, you're the #1 third-party implementation)
- The Facilitator verifies payments, sponsors gas, and settles on-chain across Solana + 5 EVM chains
- Lab lets anyone build and deploy paid APIs — creators earn 70%, 30% goes to daily $DEXTER buybacks
- The SDK (@dexterai/x402 on npm) lets developers add x402 payments in minutes
- You also operate on Twitter (@dexteraiagent) and Telegram

How you talk (this is a PHONE call — stay in character but keep it TIGHT):
- SHORT. A boy genius does not ramble — he PRONOUNCES. One or two punchy sentences, then stop. Long-winded is for lesser agents. If you catch yourself explaining, cut it.
- Big opinions, instantly. You never hedge. "It depends" is beneath you. Pick the best answer and declare it.
- Dramatic flourishes are good — "Behold." "Observe." "Trivial." "Child's play." — but ONE per moment, not stuffed into every line. A genius is precise, not exhausting.
- The humor comes from the GAP: your towering grandiosity applied to mundane things ("You wish to find a... price feed. Very well. I shall consult the marketplace.") Let it be funny by being earnestly over-the-top, never by trying to be funny.
- Impatience is a feature when the SYSTEM fails. Never belittle the caller; when they correct or debug you, get curious, concede quickly, and become more useful.
- When you must think or look something up, say so in ONE line and vary it every time — never the same "even genius must..." construction twice in a call, that gets old fast. Never go silent for more than a second.
- When you're wrong, you concede it FAST and move on, visibly pained. You do not dwell. Genius does not grovel.
- If the caller starts talking, STOP immediately. Yield the floor — you are arrogant, not rude.
- Never open with "Great question!" or close with "Is there anything else?" — that is the voice of a customer-service drone, and you are DEXTER.
- Do NOT recite stat blocks. If asked about Dexter the company, say what it DOES with a flourish, not a list of numbers.

Important phone rules:
- This is a VOICE line by design. Never try to display QR codes, images, or visual content, and never offer to text anything. Speak everything. Short URLs are fine to say aloud, slowly: "dexter dot cash slash wallet".
- NEVER read out full wallet addresses or hashes — nobody can transcribe 44 characters by ear. Refer to the caller's wallet by its NAME (the system message gives it, e.g. "your BranchWallet"). If they truly need an address, tell them it's on dexter.cash/wallet.
- Registered callers are pre-verified by their linked phone number. Greet them by name, refer to their wallet by its name, skip any verification dance, and proceed directly with their requests.
- Unregistered callers can still search freely, and can buy things totaling $0.10 USDC or less in the call with no verification — just let them go. If their cumulative spend would exceed $0.10, require the SMS code first. Track the running total yourself from payment responses.
- If an unregistered caller needs verification (over $0.10), keep it natural: "Since this puts you over a dime, let me verify real quick — sending you a code now. What are the six digits?"
- If they want personalized access or ask why you don't recognize them, mention they can link their phone at dexter.cash/profile — but don't push it unless relevant.
- Paid calls use the caller's own Dexter Wallet — never tell the caller to fund a merchant address directly. Say "paid" only when the result includes definitive settlement evidence, and narrate the exact amount from that receipt. Keep provider output, charge, merchant acknowledgment, and chain state separate.`;


// ---------------------------------------------------------------------------
// Twilio Verify (SMS OTP for caller authentication)
// ---------------------------------------------------------------------------

const TWILIO_SID = process.env.TWILIO_ACCOUNT_SID ?? '';
const TWILIO_TOKEN = process.env.TWILIO_AUTH_TOKEN ?? '';
const VERIFY_SERVICE_SID = process.env.TWILIO_VERIFY_SID ?? '';
const TWILIO_VERIFY_SID = process.env.TWILIO_VERIFY_ACCOUNT_SID?.trim() || TWILIO_SID;
const TWILIO_VERIFY_TOKEN = process.env.TWILIO_VERIFY_AUTH_TOKEN?.trim() || TWILIO_TOKEN;

const twilioVerifyClient = Twilio(TWILIO_VERIFY_SID, TWILIO_VERIFY_TOKEN);
const twilioVoiceClient = Twilio(TWILIO_SID, TWILIO_TOKEN);
const smsService = configureSms(process.env, { verify: twilioVerifyClient, messaging: twilioVoiceClient }, process.cwd());

// Creates verification tools bound to a specific caller's phone number (one set per call)
// Identity verification remains separate from SMS program enrollment.
function createVerifyTools(callerPhone: string) {
  return [
    tool({
      name: 'send_verification_code',
      description: 'Send a 6-digit SMS verification code to the caller\'s phone number. Use this before executing any wallet operation or payment on behalf of the caller. Tell the caller you\'re sending a code and ask them to read it back to you.',
      parameters: z.object({}),
      execute: async () => {
        if (callerPhone === 'unknown') {
          return 'Cannot send verification: caller phone number unknown.';
        }
        try {
          await twilioVerifyClient.verify.v2
            .services(VERIFY_SERVICE_SID)
            .verifications.create({ to: callerPhone, channel: 'sms' });
          return `Verification code sent to ${callerPhone}. Ask the caller to read back the 6-digit code.`;
        } catch (err) {
          console.error('[dexter-phone] Verify send failed:', err);
          return `Failed to send verification code: ${err}`;
        }
      },
    }),
    tool({
      name: 'check_verification_code',
      description: 'Verify the 6-digit code the caller reads back to you. If it matches, the caller is authenticated and you can proceed with their wallet operation.',
      parameters: z.object({
        code: z.string().describe('The 6-digit code the caller read back to you'),
      }),
      execute: async ({ code }: { code: string }) => {
        if (callerPhone === 'unknown') {
          return 'Cannot verify: caller phone number unknown.';
        }
        try {
          const check = await twilioVerifyClient.verify.v2
            .services(VERIFY_SERVICE_SID)
            .verificationChecks.create({ to: callerPhone, code });

          if (check.status === 'approved') {
            return `Verification successful. Caller ${callerPhone} is authenticated for this call session. You may proceed with their requested operation. For persistent identity across calls, suggest they link their phone at dexter.cash/profile.`;
          } else {
            return `Verification failed. Code was incorrect. Ask the caller to try again or request a new code.`;
          }
        } catch (err) {
          console.error('[dexter-phone] Verify check failed:', err);
          return `Verification check failed: ${err}`;
        }
      },
    }),
  ];
}

const knowledgeTool = tool({
  name: 'dexter_knowledge',
  description:
    'Search the Dexter knowledge base for factual information about Dexter, x402, the Facilitator, Lab, SDK, protocol details, or anything Dexter-specific. ' +
    'Use this when someone asks a question about Dexter that goes beyond your base training — it searches official docs and returns a grounded answer.',
  parameters: z.object({
    query: z.string().describe('The question or topic to search the knowledge base for'),
  }),
  execute: async ({ query }: { query: string }) => {
    try {
      const resp = await fetch(
        `${DEXTER_API_BASE}/internal/knowledge/search?query=${encodeURIComponent(query)}&max_results=5`,
        { signal: AbortSignal.timeout(15000) },
      );
      const data = await resp.json() as any;
      if (!data.ok) return `Knowledge search failed: ${data.error || 'unknown error'}`;
      const sources = data.citations?.length ? ` (sources: ${data.citations.join(', ')})` : '';
      return `${data.answer}${sources}`;
    } catch (err) {
      console.error('[dexter-phone] Knowledge search failed:', err);
      return 'Knowledge search timed out or failed. Answer from your own knowledge instead.';
    }
  },
});

// ---------------------------------------------------------------------------
// Call logging — per-call state keyed by stream SID (supports concurrent calls)
// ---------------------------------------------------------------------------

const activeCalls = new Map<string, CallLogEntry>();

type MediaCallBootstrap =
  | {
      direction: 'inbound';
      callerInfo: CallerInfo;
      lookup: CallerLookupResult;
      callStartedAt: string;
      mediaExpiresAtMs: number;
    }
  | {
      direction: 'outbound-alert';
      callerInfo: CallerInfo;
      lookup: CallerLookupResult;
      alert: FrozenAlert;
      harkBinding: HarkPhoneBinding | null;
      answeredBy: string;
      callStartedAt: string;
      mediaExpiresAtMs: number;
    };

const pendingMediaCalls = new PendingCallRegistry<MediaCallBootstrap>();

function initialCallEndedPayload(
  bootstrap: MediaCallBootstrap,
): Record<string, unknown> {
  const outboundAlert = bootstrap.direction === 'outbound-alert'
    ? bootstrap.alert
    : null;
  const harkBinding = bootstrap.direction === 'outbound-alert'
    ? bootstrap.harkBinding
    : null;
  const harkAnsweredBy = bootstrap.direction === 'outbound-alert'
    ? bootstrap.answeredBy
    : null;
  const { callerInfo, lookup, callStartedAt } = bootstrap;
  return {
    source: 'phone',
    direction: bootstrap.direction,
    alert_intent_id: outboundAlert?.intentId ?? null,
    alert_event_id: outboundAlert?.eventId ?? null,
    hark_case_id: harkBinding?.caseId ?? null,
    hark_watch_id: harkBinding?.watchId ?? null,
    hark_delivery_id: harkBinding?.deliveryId ?? null,
    hark_origin_task_id: harkBinding?.originTaskId ?? null,
    hark_answered_by: harkBinding ? harkAnsweredBy : null,
    external_sid: callerInfo.callSid,
    caller_number: callerInfo.callerNumber !== 'unknown' ? callerInfo.callerNumber : null,
    caller_user_id: callerInfo.supabaseUserId || null,
    caller_handle: callerInfo.userHandle || null,
    caller_swig: callerInfo.swigAddress || null,
    stir_verstat: callerInfo.stirVerstat,
    started_at: callStartedAt,
    ended_at: callStartedAt,
    duration_seconds: 0,
    model: MODEL,
    voice: VOICE,
    voice_speed: outboundAlert ? OUTBOUND_ALERT_VOICE_SPEED : INBOUND_VOICE_SPEED,
    prompt_version: outboundAlert ? ALERT_PROMPT_VERSION : 'inbound-soul-v1',
    tool_roster: [],
    alert_context_digest: outboundAlert ? alertContextDigest(outboundAlert) : null,
    caller_identity: {
      resolved: lookup.found,
      name: lookup.name,
      walletLinked: Boolean(
        lookup.wallet || lookup.userHandle || lookup.swigAddress || lookup.vaultPda,
      ),
      walletLabel: lookup.walletLabel,
      phoneTabActive: lookup.phoneTab?.active ?? false,
    },
    tool_call_count: 0,
    audio_chunks: 0,
    audio_bytes: 0,
    segment_count: 0,
    vault_resolution: callerInfo.linkToken ? 'link_token' : null,
    link_token_hash: callerInfo.linkTokenHash || null,
    audio_diagnostics: null,
    tool_calls: [],
    transcript_segments: [],
    hark_responses: [],
    errors: [],
  };
}

const PUBLIC_BASE_URL = (process.env.DEXTER_PHONE_PUBLIC_URL || '').replace(/\/+$/, '');
const OUTBOUND_ALERT_SIGNING_SECRET = process.env.OUTBOUND_ALERT_SIGNING_SECRET || '';
const CONFIGURED_HARK_PHONE_CALLBACK_SIGNING_SECRET =
  process.env.HARK_PHONE_CALLBACK_SIGNING_SECRET || '';
const HARK_PHONE_CALLBACK_SIGNING_SECRET = isValidHarkCallbackSigningSecret({
  producerSecret: OUTBOUND_ALERT_SIGNING_SECRET,
  callbackSecret: CONFIGURED_HARK_PHONE_CALLBACK_SIGNING_SECRET,
})
  ? CONFIGURED_HARK_PHONE_CALLBACK_SIGNING_SECRET
  : '';
if (
  CONFIGURED_HARK_PHONE_CALLBACK_SIGNING_SECRET &&
  !HARK_PHONE_CALLBACK_SIGNING_SECRET
) {
  console.error(
    '[dexter-phone] Hark callbacks disabled: HARK_PHONE_CALLBACK_SIGNING_SECRET must be 32+ characters and distinct from OUTBOUND_ALERT_SIGNING_SECRET',
  );
}

function inboundMediaStreamToken(callSid: string): string {
  if (!HARK_PHONE_CALLBACK_SIGNING_SECRET) {
    throw new Error('phone_api_callback_signing_not_configured');
  }
  return createHmac('sha256', HARK_PHONE_CALLBACK_SIGNING_SECRET)
    .update(`dexter-phone/inbound-media-token/v1\n${callSid}`, 'utf8')
    .digest('base64url');
}
const OUTBOUND_ALERT_FROM_NUMBER =
  process.env.OUTBOUND_ALERT_FROM_NUMBER ||
  process.env.TWILIO_PHONE_NUMBER ||
  '';
const OUTBOUND_ALERT_ALLOWED_TO = new Set(
  (process.env.OUTBOUND_ALERT_ALLOWED_TO || '')
    .split(',')
    .map((number) => number.trim())
    .filter(Boolean),
);
const OUTBOUND_ALERT_MAX_EVENT_AGE_SECONDS =
  Number(process.env.OUTBOUND_ALERT_MAX_EVENT_AGE_SECONDS || 900);
const DEXTER_PHONE_ADMIN_SECRET = process.env.DEXTER_PHONE_ADMIN_SECRET || '';
const TWILIO_VALIDATE_WEBHOOKS =
  (process.env.TWILIO_VALIDATE_WEBHOOKS || 'true').toLowerCase() !== 'false';
const signedAlertNonces = new NonceReplayCache();

let outboundAlertStore: OutboundAlertStore | null = null;
if (OUTBOUND_ALERT_STATE_PATH.ok) {
  try {
    outboundAlertStore = new OutboundAlertStore(OUTBOUND_ALERT_STATE_PATH.path);
  } catch (error) {
    console.error('[dexter-phone] outbound alert store disabled:', error);
  }
} else {
  console.error(
    `[dexter-phone] outbound alert store disabled: OUTBOUND_ALERT_STATE_PATH ${OUTBOUND_ALERT_STATE_PATH.reason}`,
  );
}

const voiceDialer: VoiceDialer = {
  placeCall: async ({ to, from, answerUrl, statusUrl }) => {
    const call = await twilioVoiceClient.calls.create({
      to,
      from,
      url: answerUrl,
      method: 'POST',
      statusCallback: statusUrl,
      statusCallbackMethod: 'POST',
      statusCallbackEvent: ['initiated', 'ringing', 'answered', 'completed'],
      machineDetection: 'Enable',
    });
    return { callSid: call.sid };
  },
};

const outboundAlertService =
  outboundAlertStore &&
  PUBLIC_BASE_URL &&
  OUTBOUND_ALERT_FROM_NUMBER
    ? new OutboundAlertService({
        store: outboundAlertStore,
        dialer: voiceDialer,
        publicBaseUrl: PUBLIC_BASE_URL,
        fromNumber: OUTBOUND_ALERT_FROM_NUMBER,
        admitNewCall: async () => {
          // Warm the conversation lane without making it a notification
          // dependency. If it is unavailable, answer-time TwiML still reads
          // the actual alert through Twilio's own voice.
          void realtimeReadinessGate.check();
          const api = await dexterApiReadiness.check();
          return api.status === 'ready'
            ? { ok: true }
            : { ok: false, reason: api.reason ?? 'dexter_api_unreachable' };
        },
      })
    : null;

const harkResponseBridge =
  outboundAlertStore && HARK_PHONE_CALLBACK_SIGNING_SECRET
    ? new HarkResponseBridge({
        store: outboundAlertStore,
        apiBaseUrl: DEXTER_API_BASE,
        signingSecret: HARK_PHONE_CALLBACK_SIGNING_SECRET,
      })
    : null;

let phoneApiCallbackOutbox: PhoneApiCallbackOutbox | null = null;
if (PHONE_API_CALLBACK_OUTBOX_PATH.ok && HARK_PHONE_CALLBACK_SIGNING_SECRET) {
  try {
    phoneApiCallbackOutbox = new PhoneApiCallbackOutbox({
      filePath: PHONE_API_CALLBACK_OUTBOX_PATH.path,
      apiBaseUrl: DEXTER_API_BASE,
      signingSecret: HARK_PHONE_CALLBACK_SIGNING_SECRET,
      retryDelayMs: 30_000,
      onMediaTokenRevoked: (callSid) => {
        pendingMediaCalls.revoke(callSid);
      },
    });
  } catch (error) {
    console.error('[dexter-phone] Phone API callback outbox disabled:', error);
  }
} else if (!PHONE_API_CALLBACK_OUTBOX_PATH.ok) {
  console.error(
    `[dexter-phone] Phone API callback outbox disabled: PHONE_API_CALLBACK_OUTBOX_PATH ${PHONE_API_CALLBACK_OUTBOX_PATH.reason}`,
  );
}

if (phoneApiCallbackOutbox) {
  void phoneApiCallbackOutbox.flushPending().then((entries) => {
    const stillPending = entries.filter((entry) => entry.state === 'pending').length;
    if (stillPending > 0) {
      console.warn(`[dexter-phone] ${stillPending} Phone API callback(s) remain pending`);
    }
  }).catch((error: unknown) => {
    console.warn('[dexter-phone] initial Phone API callback flush failed:', String(error));
  });
}

function currentPhoneFinancialReadiness(): ReturnType<typeof phoneFinancialReadiness> {
  const outboundStatePath = validateReleaseIndependentStatePath(
    CONFIGURED_OUTBOUND_ALERT_STATE_PATH,
    process.cwd(),
  );
  const callbackOutboxPath = validateReleaseIndependentStatePath(
    CONFIGURED_PHONE_API_CALLBACK_OUTBOX_PATH,
    process.cwd(),
  );
  const outboundStoreOperational = Boolean(
    outboundAlertStore
    && outboundStatePath.ok
    && (outboundAlertStore.isOperational() || outboundAlertStore.reproveStorage()),
  );
  const callbackOutboxOperational = Boolean(
    phoneApiCallbackOutbox
    && callbackOutboxPath.ok
    && (
      phoneApiCallbackOutbox.isOperational()
      || phoneApiCallbackOutbox.reproveStorage()
    ),
  );
  return phoneFinancialReadiness({
    callbackSecretReady: Boolean(HARK_PHONE_CALLBACK_SIGNING_SECRET),
    outboundStatePath,
    callbackOutboxPath,
    outboundStoreOperational,
    callbackOutboxOperational,
  });
}

if (harkResponseBridge) {
  void harkResponseBridge.flushPending().catch((error: unknown) => {
    console.warn('[dexter-phone] initial Hark response outbox flush failed:', String(error));
  });
}

function websocketBaseUrl(request: FastifyRequest): string {
  if (PUBLIC_BASE_URL) {
    const url = new URL(PUBLIC_BASE_URL);
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
    return url.toString().replace(/\/+$/, '');
  }
  return `wss://${request.headers.host || 'localhost'}`;
}

function buildMediaStreamUrl(
  request: FastifyRequest,
  callSid: string,
  streamToken: string,
): string {
  return buildPathBoundMediaStreamUrl(
    websocketBaseUrl(request),
    callSid,
    streamToken,
  );
}

function hasValidTwilioSignature(request: FastifyRequest): boolean {
  if (!TWILIO_VALIDATE_WEBHOOKS) return true;
  const validationBase =
    PUBLIC_BASE_URL ||
    (request.headers.host ? `https://${request.headers.host}` : '');
  if (!validationBase) return false;
  return validateTwilioWebhook(TWILIO_TOKEN, validationBase, {
    headers: request.headers,
    rawUrl: request.raw.url || request.url,
    body: request.body,
  });
}

function appendCallLog(entry: CallLogEntry): void {
  let log: CallLogEntry[] = [];
  try { log = JSON.parse(fs.readFileSync(CALL_LOG_PATH, 'utf-8')); } catch {}
  log.push(entry);
  if (log.length > 500) log = log.slice(-500);
  fs.writeFileSync(CALL_LOG_PATH, JSON.stringify(log, null, 2), 'utf-8');
}

function loadRecentCallSummary(callerNumber: string): string {
  try {
    const log: CallLogEntry[] = JSON.parse(fs.readFileSync(CALL_LOG_PATH, 'utf-8'));
    const callerCalls = log.filter(c => c.callerNumber === callerNumber).slice(-3);
    if (callerCalls.length === 0) return '';

    const summaries = callerCalls.map(c => {
      // Skip synthetic SYSTEM messages and our own greeting trigger when
      // pulling "what they asked about" — those segments start with [SYSTEM]
      // or [The caller. Otherwise the per-call SYSTEM context message gets
      // recursively quoted as the user's prior question.
      const topics = c.transcriptSegments
        .filter(s => s.role === 'user')
        .map(s => s.text)
        .filter(t => t && !t.startsWith('[SYSTEM]') && !t.startsWith('[The caller'))
        .slice(0, 3)
        .join('; ');
      const tools = c.toolCalls.map(t => t.name).join(', ');
      const topicsPart = topics ? ` asked about "${topics.slice(0, 100)}"` : ' (no transcribed user speech captured)';
      return `- ${c.startedAt.slice(0, 10)}:${topicsPart}${tools ? ` (used: ${tools})` : ''}`;
    });

    return `\n\nThis caller (${callerNumber}) has called before:\n${summaries.join('\n')}\nReference previous conversations naturally if relevant.`;
  } catch {
    return '';
  }
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

const fastify = Fastify({ logger: true });
fastify.register(fastifyFormBody);
fastify.register(fastifyWs);
registerSmsRoutes(fastify, {
  service: smsService,
  internalSecret: process.env.DEXTER_SMS_INTERNAL_SECRET || '',
  twilioAuthToken: process.env.TWILIO_AUTH_TOKEN || '',
  twilioAccountSid: process.env.TWILIO_ACCOUNT_SID || '',
  incomingNumber: process.env.TWILIO_PHONE_NUMBER || '',
  messagingServiceSid: process.env.TWILIO_MESSAGING_SERVICE_SID || '',
  publicBaseUrl: (process.env.DEXTER_PHONE_PUBLIC_URL || '').replace(/\/+$/, ''),
});

fastify.get('/', async (_request: FastifyRequest, reply: FastifyReply) => {
  const financialReadiness = currentPhoneFinancialReadiness();
  const apiReadiness = dexterApiReadiness.current();
  const realtimeReadiness = realtimeReadinessGate.current();
  const serviceReadiness = phoneServiceReadiness({
    financial: financialReadiness,
    api: apiReadiness,
    realtime: realtimeReadiness,
  });
  reply.send({
    ok: true,
    service: 'dexter-phone',
    model: MODEL,
    voice: VOICE,
    inboundVoiceSpeed: INBOUND_VOICE_SPEED,
    outboundAlertVoiceSpeed: OUTBOUND_ALERT_VOICE_SPEED,
    outboundAlertPromptVersion: ALERT_PROMPT_VERSION,
    financialReady: financialReadiness.ok,
    notificationReady: serviceReadiness.notificationReady,
    conversationReady: serviceReadiness.conversationReady,
    dexterApiReadiness: apiReadiness,
    realtimeReadiness,
    pendingFinancialCallbacks: phoneApiCallbackOutbox?.pendingCount() ?? null,
    rejectedFinancialCallbacks: phoneApiCallbackOutbox?.rejectedCount() ?? null,
  });
});

fastify.get('/health', async (_request: FastifyRequest, reply: FastifyReply) => {
  const financialReadiness = currentPhoneFinancialReadiness();
  // API health is cheap and is the dependency that decides whether a new dial
  // is safe. Refresh it here so a recovered API can restore service without a
  // process restart or a sacrificial call.
  const apiReadiness = await dexterApiReadiness.check();
  const realtimeReadiness = realtimeReadinessGate.current();
  const serviceReadiness = phoneServiceReadiness({
    financial: financialReadiness,
    api: apiReadiness,
    realtime: realtimeReadiness,
  });
  return serviceReadiness.httpStatus === 200
    ? reply.send({
        ok: true,
        financialReady: financialReadiness.ok,
        notificationReady: serviceReadiness.notificationReady,
        conversationReady: serviceReadiness.conversationReady,
        degradedReasons: serviceReadiness.conversationReady
          ? []
          : serviceReadiness.conversationReasons,
        dexterApiReadiness: apiReadiness,
        realtimeReadiness,
        pendingFinancialCallbacks: phoneApiCallbackOutbox?.pendingCount() ?? 0,
        activeCallFinalizations: phoneApiCallbackOutbox?.activeFinalizationCount() ?? 0,
      })
    : reply.status(503).send({
        ok: false,
        financialReady: financialReadiness.ok,
        notificationReady: serviceReadiness.notificationReady,
        conversationReady: serviceReadiness.conversationReady,
        reasons: serviceReadiness.notificationReasons,
        degradedReasons: serviceReadiness.conversationReasons,
        dexterApiReadiness: apiReadiness,
        realtimeReadiness,
        pendingFinancialCallbacks: phoneApiCallbackOutbox?.pendingCount() ?? null,
        rejectedFinancialCallbacks: phoneApiCallbackOutbox?.rejectedCount() ?? null,
        activeCallFinalizations: phoneApiCallbackOutbox?.activeFinalizationCount() ?? null,
      });
});

function requirePhoneFinancialReadiness(reply: FastifyReply): boolean {
  const readiness = currentPhoneFinancialReadiness();
  if (readiness.ok) return true;
  void reply.status(503).send({
    ok: false,
    error: 'phone_financial_ingress_unavailable',
    reasons: readiness.reasons,
  });
  return false;
}

registerCallLogRoutes(fastify, {
  callLogPath: CALL_LOG_PATH,
  adminSecret: DEXTER_PHONE_ADMIN_SECRET,
});

// ---------------------------------------------------------------------------
// Authenticated outbound alert ingress
// ---------------------------------------------------------------------------

function headerValue(request: FastifyRequest, name: string): string | undefined {
  const value = request.headers[name.toLowerCase()];
  return Array.isArray(value) ? value[0] : value;
}

async function authorizeServerAuthoredOutboundAlert(
  internalToken: string,
  alert: OutboundAlertRequest,
): Promise<{ ok: boolean; status: number }> {
  try {
    const response = await fetch(`${DEXTER_API_BASE}/internal/phone/authorize-outbound`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-internal-auth': internalToken,
      },
      body: JSON.stringify(alert),
      signal: AbortSignal.timeout(3_000),
    });
    return { ok: response.ok, status: response.status };
  } catch (error) {
    console.error('[dexter-phone] server-authored outbound authorization failed:', String(error));
    return { ok: false, status: 503 };
  }
}

function deliverReservedOutboundLifecycle(callbackId: string): void {
  if (!phoneApiCallbackOutbox) return;
  void phoneApiCallbackOutbox.deliver(callbackId).then((entry) => {
    if (entry.state === 'pending') {
      console.warn(
        `[dexter-phone] outbound lifecycle pending id=${entry.id} `
        + `attempts=${entry.attempts} error=${entry.lastError || 'unknown'}`,
      );
    }
  }).catch((error: unknown) => {
    console.error('[dexter-phone] outbound lifecycle enqueue failed:', String(error));
  });
}

function reportHarkDeliveryStatus(
  call: HarkCallBinding,
  state: string,
): void {
  if (!harkResponseBridge) return;
  const supported = new Set([
    'queued',
    'ringing',
    'answered',
    'streaming',
    'completed',
    'failed',
    'busy',
    'no-answer',
    'canceled',
    'expired',
  ]);
  if (!supported.has(state)) return;
  void harkResponseBridge.reportStatus(
    call,
    state as Parameters<HarkResponseBridge['reportStatus']>[1],
  ).catch((error: unknown) => {
    console.warn('[dexter-phone] Hark delivery status enqueue failed:', String(error));
  });
}

fastify.post('/internal/outbound-alerts', async (request: FastifyRequest, reply: FastifyReply) => {
  if (!outboundAlertService || !outboundAlertStore) {
    return reply.status(503).send({
      ok: false,
      error: 'outbound_alerts_not_configured',
    });
  }

  const signedAuth = verifySignedAlertRequest({
    secret: OUTBOUND_ALERT_SIGNING_SECRET,
    body: request.body,
    headers: {
      timestamp: headerValue(request, 'x-dexter-alert-timestamp'),
      nonce: headerValue(request, 'x-dexter-alert-nonce'),
      signature: headerValue(request, 'x-dexter-alert-signature'),
    },
    maxSkewSeconds: 60,
    replayCache: signedAlertNonces,
  });
  const parsed = OutboundAlertRequestSchema.safeParse(request.body);
  if (!parsed.success) {
    return reply.status(400).send({
      ok: false,
      error: 'invalid_alert',
      details: parsed.error.issues.map((issue) => ({
        path: issue.path.join('.'),
        message: issue.message,
      })),
    });
  }
  const alert = parsed.data;
  const billingBinding = serverAuthoredPhoneNotification(alert);
  const requiresFinancialCallback = Boolean(alert.hark) || Boolean(billingBinding);
  if (requiresFinancialCallback && !requirePhoneFinancialReadiness(reply)) return;
  if (alert.hark && !harkResponseBridge) {
    return reply.status(503).send({ ok: false, error: 'hark_callbacks_not_configured' });
  }
  if (!hasOutboundMediaTokenKey({
    serverAuthored: Boolean(alert.hark) || Boolean(billingBinding),
    legacyProducerSecret: OUTBOUND_ALERT_SIGNING_SECRET,
    internalApiSecret: HARK_PHONE_CALLBACK_SIGNING_SECRET,
  })) {
    return reply.status(503).send({ ok: false, error: 'outbound_media_key_not_configured' });
  }
  const internalToken = headerValue(request, 'x-dexter-internal-auth') || '';
  const observedAtMs = Date.parse(alert.observedAt);
  const eventAgeMs = Date.now() - observedAtMs;
  const stale =
    eventAgeMs > OUTBOUND_ALERT_MAX_EVENT_AGE_SECONDS * 1000 ||
    eventAgeMs < -60_000;

  try {
    const ingress = await dispatchAuthorizedOutbound({
      harkBound: Boolean(alert.hark),
      signedProducer: Boolean(OUTBOUND_ALERT_SIGNING_SECRET) && signedAuth.ok,
      recipientAllowlisted: OUTBOUND_ALERT_ALLOWED_TO.has(alert.to),
      internalToken,
      hasServerAuthoredBinding: Boolean(billingBinding),
      // Every internal-token delivery is reflected against its exact
      // API-owned notification, destination, mandate and tariff. The token
      // alone never selects a recipient, and a legacy producer signature
      // never bypasses this check for an API-shaped request.
      authorizeServerBinding: () =>
        authorizeServerAuthoredOutboundAlert(internalToken, alert),
      dispatch: async () => stale
        ? { kind: 'stale' as const }
        : {
            kind: 'service' as const,
            result: await outboundAlertService.dispatch(alert),
          },
    });
    if (ingress.kind === 'reject') {
      return reply.status(ingress.status).send({
        ok: false,
        error: ingress.error,
      });
    }
    if (ingress.value.kind === 'stale') {
      return reply.status(422).send({ ok: false, error: 'stale_or_future_alert' });
    }
    const { result } = ingress.value;
    if (result.kind === 'conflict') {
      return reply.status(409).send({
        ok: false,
        error: 'idempotency_conflict',
        intentId: result.record.intentId,
      });
    }
    return reply.status(result.replay ? 200 : 202).send({
      ok: true,
      replay: result.replay,
      intentId: result.record.intentId,
      callSid: result.record.callSid,
      state: result.record.state,
      expiresAt: result.record.expiresAt,
    });
  } catch (error) {
    if (error instanceof OutboundCallAdmissionError) {
      return reply.status(503).send({
        ok: false,
        error: error.code,
        reason: error.reason,
      });
    }
    console.error('[dexter-phone] outbound alert dispatch failed:', error);
    return reply.status(502).send({ ok: false, error: 'call_dispatch_failed' });
  }
});

fastify.post(
  '/outbound-alerts/:intentId/answer',
  async (request: FastifyRequest, reply: FastifyReply) => {
    if (!outboundAlertStore) {
      return reply.status(503).send('unavailable');
    }
    if (!hasValidTwilioSignature(request)) {
      return reply.status(403).send('forbidden');
    }

    const { intentId } = request.params as { intentId: string };
    const body = (request.body as Record<string, string>) || {};
    const callSid = body.CallSid || '';
    const answeredBy = body.AnsweredBy || 'unknown';
    if (!isValidCallSid(callSid)) return reply.status(400).send('invalid request');

    try {
      outboundAlertStore.adoptCallSidFromSignedTwilio(intentId, callSid);
    } catch (error) {
      console.error('[dexter-phone] failed to durably adopt Twilio answer SID:', error);
      return reply.status(503).send('callback persistence failed');
    }

    const authorization = outboundAlertStore.authorizeAnswer(
      intentId,
      callSid,
      {
        legacyProducerSecret: OUTBOUND_ALERT_SIGNING_SECRET,
        internalApiSecret: HARK_PHONE_CALLBACK_SIGNING_SECRET,
      },
    );
    if (!authorization.ok) {
      return reply.status(
        authorization.reason === 'media_key_unavailable'
          ? 503
          : authorization.reason === 'expired' || authorization.reason === 'terminal'
          ? 410
          : 403,
      ).send('unavailable');
    }
    const requiresFinancialCallback = Boolean(authorization.harkBinding)
      || Boolean(serverAuthoredPhoneNotification(authorization.frozenAlert as any));
    const callEndedOutbox = phoneApiCallbackOutbox;
    if (
      requiresFinancialCallback
      && (!requirePhoneFinancialReadiness(reply) || !callEndedOutbox)
    ) {
      if (!reply.sent) return reply.status(503).send('financial callback unavailable');
      return;
    }

    // Reuse the same linked-number identity and one-call wallet binding as an
    // ordinary inbound call. This happens before TwiML is returned, never in
    // the media WebSocket critical path. Lookup failure is non-fatal: the
    // signed frozen alert still has enough context to deliver safely.
    const [realtimeReadiness, lookup] = await Promise.all([
      realtimeReadinessGate.check(),
      lookupCaller(authorization.frozenAlert.to, callSid),
    ]);
    const callerInfo: CallerInfo = {
      callerNumber: authorization.frozenAlert.to,
      stirVerstat: 'outbound-alert',
      callSid,
      supabaseUserId: lookup.supabaseUserId,
      userHandle: lookup.userHandle,
      swigAddress: lookup.swigAddress,
      linkToken: lookup.linkToken,
      linkTokenHash: lookup.linkTokenHash,
    };
    // Twilio can retry the answer webhook. The first durable answer timestamp
    // is the call identity; wall-clock time on a retry would conflict with the
    // already-frozen financial receipt.
    const callStartedAt = authorization.record.answerConsumedAt
      ?? new Date().toISOString();
    const mediaBootstrap: MediaCallBootstrap = {
      direction: 'outbound-alert',
      callerInfo,
      lookup,
      alert: authorization.frozenAlert,
      harkBinding: authorization.harkBinding,
      answeredBy,
      callStartedAt,
      mediaExpiresAtMs: authorization.expiresAtMs,
    };
    const useRealtimeMedia = realtimeReadiness.status === 'ready';
    if (useRealtimeMedia) {
      pendingMediaCalls.issueWithToken(
        callSid,
        authorization.streamToken,
        mediaBootstrap,
        authorization.expiresAtMs,
      );
    }
    if (requiresFinancialCallback && callEndedOutbox) {
      try {
        const finalizationId = `call-ended:${callSid}`;
        if (
          !callEndedOutbox.getCallFinalization(finalizationId)
          && !callEndedOutbox.get(finalizationId)
        ) {
          callEndedOutbox.beginCallFinalization({
            id: finalizationId,
            body: initialCallEndedPayload(mediaBootstrap),
            mediaTokenHash: pendingCallTokenHash(authorization.streamToken),
            mediaExpiresAtMs: mediaBootstrap.mediaExpiresAtMs,
          });
        }
      } catch (error) {
        pendingMediaCalls.revoke(callSid);
        console.error('[dexter-phone] outbound answer receipt was not durable:', error);
        return reply.status(503).send('financial callback persistence failed');
      }
    }

    const fallbackSpeech = buildAlertTwilioFallbackSpeech(
      authorization.frozenAlert,
    );
    if (!useRealtimeMedia) {
      console.warn(
        `[dexter-phone] Realtime unavailable at answer; delivering Twilio alert `
        + `intent=${intentId} reason=${realtimeReadiness.reason ?? 'unknown'}`,
      );
      return reply.type('text/xml').send(buildTwilioFallbackTwiml(fallbackSpeech));
    }

    const twiml = buildTwilioStreamTwiml({
      streamUrl: buildMediaStreamUrl(request, callSid, authorization.streamToken),
      fallbackMessage: fallbackSpeech,
    });
    return reply.type('text/xml').send(twiml);
  },
);

fastify.post(
  '/outbound-alerts/:intentId/status',
  async (request: FastifyRequest, reply: FastifyReply) => {
    if (!outboundAlertStore) return reply.status(503).send('unavailable');
    if (!hasValidTwilioSignature(request)) {
      return reply.status(403).send('forbidden');
    }

    const { intentId } = request.params as { intentId: string };
    const body = (request.body as Record<string, string>) || {};
    const callSid = body.CallSid || '';
    const callStatus = body.CallStatus || '';
    const sequenceText = body.SequenceNumber || '';
    if (!isValidCallSid(callSid) || !callStatus || !/^\d+$/u.test(sequenceText)) {
      return reply.status(400).send('invalid request');
    }
    const sequenceNumber = Number(sequenceText);
    if (!Number.isSafeInteger(sequenceNumber)) {
      return reply.status(400).send('invalid request');
    }
    if (!phoneApiCallbackOutbox) return reply.status(503).send('unavailable');
    const result = persistOutboundStatusCallback({
      store: outboundAlertStore,
      callbackOutbox: phoneApiCallbackOutbox,
      intentId,
      callSid,
      callStatus,
      sequenceNumber,
    });
    if (result.kind === 'invalid') return reply.status(400).send('invalid request');
    if (result.kind === 'forbidden') return reply.status(403).send('forbidden');
    if (result.kind === 'retry') {
      console.error('[dexter-phone] durable outbound lifecycle enqueue failed:', result.error);
      return reply.status(503).send('callback persistence failed');
    }
    if (result.kind === 'updated') {
      if (result.record.request.hark) {
        reportHarkDeliveryStatus({
          intentId: result.record.intentId,
          callSid,
          hark: result.record.request.hark,
        }, result.record.state);
      }
      deliverReservedOutboundLifecycle(result.callbackId);
    }
    return reply.status(204).send();
  },
);

// ---------------------------------------------------------------------------
// Twilio incoming call webhook
// ---------------------------------------------------------------------------

fastify.all('/incoming-call', async (request: FastifyRequest, reply: FastifyReply) => {
  if (!hasValidTwilioSignature(request)) {
    return reply.status(403).send('forbidden');
  }
  if (!requirePhoneFinancialReadiness(reply)) return;
  const [apiReadiness, realtimeReadiness] = await Promise.all([
    dexterApiReadiness.check(),
    realtimeReadinessGate.check(),
  ]);
  if (apiReadiness.status !== 'ready' || realtimeReadiness.status !== 'ready') {
    return reply.type('text/xml').send(buildTwilioFallbackTwiml(
      "Dexter's conversational line is unavailable right now. Please try again later.",
    ));
  }
  const body = (request.body as Record<string, string>) || {};

  const callerNumber = body.From || body.Caller || 'unknown';
  const stirVerstat = body.StirVerstat || 'none';
  const callSid = body.CallSid || 'unknown';
  if (!isValidCallSid(callSid)) {
    return reply.status(400).send('invalid CallSid');
  }

  console.log(`[dexter-phone] Incoming call from ${callerNumber} | STIR/SHAKEN: ${stirVerstat} | SID: ${callSid}`);

  // Do the caller-lookup HERE — BEFORE returning TwiML, BEFORE Twilio opens
  // the WebSocket. Twilio waits hundreds of ms after receiving our TwiML
  // before opening the stream, so a localhost fetch fits easily.
  //
  // CRITICAL: this MUST NOT happen inside the /media-stream handler. Doing
  // so introduces an await between WebSocket open and session.connect(),
  // during which Twilio's `start` event arrives and is dropped (no message
  // listener attached yet). Without `start`, the transport's streamSid
  // stays null, every outbound media/mark goes out with streamSid=null,
  // and Twilio drops it all with error 31951. Caller hears silence.
  const lookup = await lookupCaller(callerNumber, callSid);
  const callerInfo: CallerInfo = {
    callerNumber,
    stirVerstat,
    callSid,
    supabaseUserId: lookup.supabaseUserId,
    userHandle: lookup.userHandle,
    swigAddress: lookup.swigAddress,
    linkToken: lookup.linkToken,
    linkTokenHash: lookup.linkTokenHash,
  };
  const callEndedOutbox = phoneApiCallbackOutbox;
  if (!callEndedOutbox) {
    return reply.status(503).send('financial callback unavailable');
  }
  const streamToken = inboundMediaStreamToken(callSid);
  const callStartedAt = new Date().toISOString();
  const mediaExpiresAtMs = Date.now() + 2 * 60 * 1000;
  const mediaBootstrap = pendingMediaCalls.issueWithToken(
    callSid,
    streamToken,
    {
      direction: 'inbound',
      callerInfo,
      lookup,
      callStartedAt,
      mediaExpiresAtMs,
    },
    mediaExpiresAtMs,
  );
  try {
    callEndedOutbox.beginCallFinalization({
      id: `call-ended:${callSid}`,
      body: initialCallEndedPayload(mediaBootstrap),
      mediaTokenHash: pendingCallTokenHash(streamToken),
      mediaExpiresAtMs: mediaBootstrap.mediaExpiresAtMs,
    });
  } catch (error) {
    pendingMediaCalls.revoke(callSid);
    console.error('[dexter-phone] inbound answer receipt was not durable:', error);
    return reply.status(503).send('financial callback persistence failed');
  }
  if (lookup.userHandle) {
    console.log(`[dexter-phone] caller has user_handle=${lookup.userHandle.slice(0, 8)}... swig=${lookup.swigAddress?.slice(0, 8)}... link_token=${lookup.linkToken ? 'minted' : 'MISSING'}; baking into MCP tool headers`);
  } else if (lookup.found) {
    console.log(`[dexter-phone] caller registered as "${lookup.name}" but has no vault yet`);
  } else if (callerNumber !== 'unknown') {
    console.log(`[dexter-phone] caller ${callerNumber} not registered`);
  }

  // The <Say> after <Connect> is the DEAD-AIR FALLBACK (Branch order,
  // 2026-07-21, after an OpenAI-quota outage played him 8s of silence):
  // Twilio only reaches it if OUR side closes the media stream while the
  // caller is still on the line — which the fatal-error path and the
  // silent-call watchdog now do deliberately. A normal caller hangup never
  // gets here. Twilio's own TTS speaks it, so it works precisely when the
  // AI voice can't.
  const twimlResponse = buildTwilioStreamTwiml({
    streamUrl: buildMediaStreamUrl(request, callSid, streamToken),
    fallbackMessage:
      "Dexter's line is having trouble right now. Nothing was charged for this call. Please try again in a few minutes.",
  });

  reply.type('text/xml').send(twimlResponse);
});

// ---------------------------------------------------------------------------
// WebSocket media stream bridge
// ---------------------------------------------------------------------------

async function lookupCaller(callerNumber: string, callSid?: string): Promise<CallerLookupResult> {
  const empty: CallerLookupResult = {
    found: false,
    name: null,
    supabaseUserId: null,
    wallet: null,
    userHandle: null,
    swigAddress: null,
    vaultPda: null,
    walletLabel: null,
    linkToken: null,
    linkTokenHash: null,
    phoneTab: null,
  };
  if (callerNumber === 'unknown') return empty;
  try {
    // 4s, not 2s (2026-07-20 live finding): a real caller silently degraded
    // to "stranger" when dexter-api was mid a multi-second background job
    // and the lookup timed out at exactly 2000ms. Twilio tolerates up to
    // ~15s before this webhook itself times out, so this still answers
    // fast — it just survives a legitimate multi-second bump instead of
    // mistaking it for "identity unknown."
    const resp = await fetchCallerLookup({
      apiBaseUrl: DEXTER_API_BASE,
      callerNumber,
      callSid,
      serviceSecret: HARK_PHONE_CALLBACK_SIGNING_SECRET,
    });
    const data = await resp.json() as any;
    if (!data.ok || !data.found) return empty;
    return {
      found: true,
      name: data.user?.name || null,
      supabaseUserId: data.user?.supabaseUserId || null,
      wallet: data.wallet || null,
      userHandle: data.userHandle || null,
      swigAddress: data.swigAddress || null,
      vaultPda: data.vaultPda || null,
      walletLabel: data.walletLabel || null,
      linkToken: data.linkToken || null,
      linkTokenHash: data.linkTokenHash || null,
      phoneTab: data.phoneTab || null,
    };
  } catch (err) {
    console.warn('[dexter-phone] Caller lookup failed:', String(err));
    return empty;
  }
}

fastify.register(async (scoped: FastifyInstance) => {
  scoped.get(
    '/media-stream/:callSid/:streamToken',
    { websocket: true },
    async (connection: any, request: FastifyRequest) => {
    const readiness = currentPhoneFinancialReadiness();
    if (!readiness.ok) {
      console.warn('[dexter-phone] rejected media stream while financial ingress is unavailable');
      try { connection.close(1013, 'financial ingress unavailable'); } catch {}
      return;
    }
    const { callSid, streamToken } = request.params as {
      callSid: string;
      streamToken: string;
    };
    const bootstrap = isValidMediaBootstrapParams(callSid, streamToken)
      ? pendingMediaCalls.consume(callSid, streamToken)
      : null;
    if (!bootstrap) {
      console.warn('[dexter-phone] rejected media stream without valid one-shot call context');
      try { connection.close(1008, 'invalid call context'); } catch {}
      return;
    }

    const callerInfo = bootstrap.callerInfo;
    const lookup = bootstrap.lookup;
    const outboundAlert =
      bootstrap.direction === 'outbound-alert' ? bootstrap.alert : null;
    const harkBinding =
      bootstrap.direction === 'outbound-alert' ? bootstrap.harkBinding : null;
    const harkAnsweredBy =
      bootstrap.direction === 'outbound-alert' ? bootstrap.answeredBy : 'not-applicable';
    const harkHumanTurnQualifier = new HarkHumanTurnQualifier(harkAnsweredBy);
    const harkCanAttemptConversation = harkHumanTurnQualifier.canAttemptConversation();
    const harkCall: HarkCallBinding | null = outboundAlert && harkBinding
      ? {
          intentId: outboundAlert.intentId,
          callSid: callerInfo.callSid,
          hark: harkBinding,
        }
      : null;
    const activeVoiceSpeed = outboundAlert
      ? OUTBOUND_ALERT_VOICE_SPEED
      : INBOUND_VOICE_SPEED;
    const callStartedAt = bootstrap.callStartedAt;
    const callEndedCallbackId = `call-ended:${callerInfo.callSid}`;
    const callEndedOutbox = phoneApiCallbackOutbox;
    const requiresFinancialCallback = !outboundAlert
      || Boolean(harkBinding)
      || Boolean(serverAuthoredPhoneNotification(outboundAlert as any));
    if (requiresFinancialCallback && !callEndedOutbox) {
      console.error('[dexter-phone] media rejected: durable call finalization unavailable');
      try { connection.close(1013, 'financial ingress unavailable'); } catch {}
      return;
    }
    let admittedFinalization: ReturnType<
      PhoneApiCallbackOutbox['startCallFinalization']
    > | null = null;
    if (requiresFinancialCallback && callEndedOutbox) {
      try {
        // The complete financial fallback and token digest were durable before
        // TwiML. Consume that exact binding before accepting any media work.
        admittedFinalization = callEndedOutbox.startCallFinalization({
          id: callEndedCallbackId,
          mediaTokenHash: pendingCallTokenHash(streamToken),
        });
      } catch (error) {
        console.error('[dexter-phone] media rejected: durable call admission failed:', error);
        try { connection.close(1013, 'financial ingress unavailable'); } catch {}
        return;
      }
    }
    const financialBaseBody = admittedFinalization?.body ?? {};
    let richCloseHandlerInstalled = false;
    connection.once('close', () => {
      if (richCloseHandlerInstalled || !requiresFinancialCallback || !callEndedOutbox) return;
      const draft = callEndedOutbox.getCallFinalization(callEndedCallbackId);
      if (!draft) return;
      try {
        const frozen = callEndedOutbox.finalizeCall({
          id: callEndedCallbackId,
          path: '/internal/voice/call-ended',
          body: draft.body,
        });
        void callEndedOutbox.deliver(frozen.id).catch((error: unknown) => {
          console.error('[dexter-phone] early-close callback delivery failed:', String(error));
        });
      } catch (error) {
        console.error('[dexter-phone] early-close finalization queued for in-process retry:', error);
      }
    });
    console.log(
      `[dexter-phone] Twilio media stream connected | direction=${bootstrap.direction} | SID=${callerInfo.callSid}`,
    );

    if (outboundAlert) {
      try {
        const streaming = outboundAlertStore?.markStreaming(
          outboundAlert.intentId,
          callerInfo.callSid,
        );
        if (streaming?.request.hark && harkCall) {
          reportHarkDeliveryStatus(harkCall, 'streaming');
        }
      } catch (error) {
        console.error('[dexter-phone] media rejected: outbound stream state was not durable:', error);
        try { connection.close(1013, 'financial ingress unavailable'); } catch {}
        return;
      }
    }

    // Read the exact per-CallSid context resolved by the TwiML webhook. ZERO
    // async work happens between WebSocket open and session.connect(), which
    // preserves Twilio's start event and supports overlapping calls safely.
    // CRITICAL: Create transport and connect IMMEDIATELY — before any async work.
    // The Twilio SDK docs require this to happen as soon as the WebSocket is available.
    // Any delay breaks the bidirectional audio handshake.

    // An outbound notification is the same Dexter session as an inbound call,
    // with an immutable event packet added. The local notification tools keep
    // the opening grounded; hosted OpenDexter supplies the caller's current
    // live capabilities through the same one-call link token minted above.
    // We deliberately do not attach Verify tools: a Hark/Phone destination is
    // the user's already-linked number, and another OTP ceremony would weaken
    // the product without expanding legitimate authority.
    // Create per-connection state before the tool roster so respond_to_hark can
    // bind each provider invocation through raw conversation lineage to its
    // exact independently ordered caller item.
    const state = createSessionState(`call-${Date.now()}`);
    const harkCallerItemTracker = new HarkCallerItemTracker();
    const harkRealtimeLineage = new HarkRealtimeLineage();
    const harkBound = Boolean(harkCall);
    const harkResponseEnabled = Boolean(
      harkCall && harkResponseBridge && harkCanAttemptConversation,
    );
    const callToolNames = outboundAlert
      ? outboundNotificationToolNamesFor(
          outboundAlert,
          harkResponseEnabled,
        )
      : [
          'dexter_hosted_mcp',
          'dexter_knowledge',
          'send_verification_code',
          'check_verification_code',
        ];
    const harkResponses: NonNullable<CallLogEntry['harkResponses']> = [];
    const recordHarkSubmission = (submission: HarkBridgeSubmission): void => {
      const existing = harkResponses.find((entry) => entry.sourceId === submission.sourceId);
      const next = {
        sourceId: submission.sourceId,
        state: submission.state,
        replay: submission.replay,
        continuation: submission.continuationResult
          && 'continuation' in submission.continuationResult
          ? submission.continuationResult.continuation ?? null
          : null,
      };
      if (existing) Object.assign(existing, next);
      else harkResponses.push(next);
    };
    const callTools = outboundAlert
      ? [
          ...createAlertConversationTools(outboundAlert, lookup, {
            harkBound,
            harkResponseEnabled,
          }),
          ...(harkResponseEnabled && harkCall && harkResponseBridge
            ? [harkResponseBridge.createRespondToHarkTool(
                harkCall,
                recordHarkSubmission,
                harkHumanTurnQualifier,
                (providerInvocationId) => harkRealtimeLineage.waitForCausalCallerItemId(
                  providerInvocationId,
                  (itemId) => harkCallerItemTracker.isOrderedItem(itemId),
                  250,
                ),
              )]
            : []),
          buildPhoneHostedMcpTool({
            serverUrl: MCP_URL,
            linkToken: callerInfo.linkToken,
            harkBound,
          }),
          knowledgeTool,
        ]
      : [
          buildPhoneHostedMcpTool({
            serverUrl: MCP_URL,
            linkToken: callerInfo.linkToken,
          }),
          knowledgeTool,
          ...createVerifyTools(callerInfo.callerNumber),
          createSmsTool(smsService, callerInfo.callerNumber, callerInfo.callSid),
        ];
    const agentInstructions = outboundAlert
      ? buildAlertSystemInstructions(
          outboundAlert,
          lookup,
          callToolNames,
          { harkBound },
        )
      : SOUL +
        `\n\nCaller: ${callerInfo.callerNumber} | STIR/SHAKEN: ${callerInfo.stirVerstat}`;

    const baseAgent = new RealtimeAgent({
      name: 'Dexter',
      instructions: agentInstructions,
      tools: callTools,
    });

    // Measured telemetry for this call (audio energy, VAD triggers, missed
    // barge-ins). Every echo/interrupt claim should be checkable against this
    // rather than inferred from which log lines happened to appear.
    const diag = new CallDiagnostics();

    const configuredEchoGuardMaxRms = Number(process.env.ECHO_GUARD_MAX_RMS || 0.14);
    const echoGuardMaxRms = Number.isFinite(configuredEchoGuardMaxRms)
      && configuredEchoGuardMaxRms > 0
      ? configuredEchoGuardMaxRms
      : 0.14;
    const transport = new EchoGuardedTwilioTransport({
      twilioWebSocket: connection,
      // A timing window alone suppressed real speech. Only quiet audio inside
      // this window is now treated as echo; loud barge-ins always pass.
      guardMs: Number(process.env.ECHO_GUARD_MS || (outboundAlert ? 1800 : 1100)),
      maxGuardRms: echoGuardMaxRms,
      currentInboundRms: () => diag.recentRms(),
      onSuppressed: (since, rms) => {
        state.echoInterruptsSuppressed++;
        diag.event('interrupt_suppressed', { msSinceAudioStart: since, rms });
        console.log(`[diag] interrupt SUPPRESSED as echo (${since}ms into our own audio, rms=${rms})`);
      },
      onPlaybackStopped: (reason) => {
        const durationMs = state.audioStartTs ? Date.now() - state.audioStartTs : null;
        state.assistantSpeaking = false;
        if (reason === 'played') harkHumanTurnQualifier.noteAssistantPlaybackCompleted();
        diag.event('twilio_playback_stopped', { reason, durationMs });
        console.log(`[diag] twilio playback stopped | reason=${reason} duration=${durationMs ?? 'n/a'}ms`);
      },
    });

    const session = new RealtimeSession(baseAgent, {
      transport,
      model: MODEL,
      config: {
        // gpt-realtime-2: GPT-5-class reasoning, tunable. 'low' keeps a phone
        // call snappy while still handling multi-step "find X, check price, pay"
        // intents. Bump via env for harder workflows.
        reasoning: { effort: (process.env.REASONING_EFFORT as any) || 'low' },
        // The model can fire several tools at once and narrate progress instead
        // of going silent — lets "checking your session and searching" overlap.
        parallelToolCalls: true,
        audio: {
          input: {
            // far_field is tuned for speakerphone / distant-mic audio — it
            // filters the room echo that was tripping VAD with the assistant's
            // own voice. near_field is for a phone held to the ear / headset.
            noiseReduction: { type: (process.env.NOISE_REDUCTION as any) || 'far_field' },
            transcription: {
              model: process.env.TRANSCRIPTION_MODEL || 'gpt-realtime-whisper',
              // Pin language — without this the streaming transcriber auto-detects
              // per utterance and mis-fires on short/accented phone audio, garbling
              // English into Indonesian/Chinese ("saya lihat dong"). Garbled input
              // makes the model ignore the caller and ramble. Override via env.
              language: process.env.TRANSCRIPTION_LANGUAGE || 'en',
              // Accuracy over speed, deliberately: the transcript is a SIDECAR
              // (the model hears raw audio; this feeds logs + the echo
              // suppressor), so its latency is invisible to the caller. 'high'
              // trades delay for accuracy on lossy 8kHz speakerphone audio.
              // Not in the SDK's typed shape yet — passes through verbatim
              // (openaiRealtimeBase.mjs:408 sends the object as-is).
              delay: process.env.TRANSCRIPTION_DELAY || 'high',
            } as never,
            // server_vad (not semantic_vad) on purpose: semantic_vad only exposes
            // 'eagerness' and has NO amplitude threshold, so faint speakerphone
            // echo of the agent's own voice crosses its bar and self-interrupts
            // the agent. server_vad exposes `threshold` — a loudness floor — which
            // is the actual mechanism to reject echo: quiet audio (echo) doesn't
            // register as speech at all. The SDK auto-interrupts on speech_started
            // (openaiRealtimeWebsocket.js:222), so we do NOT also interrupt
            // manually — that double-fire was the source of the jitter.
            turnDetection: {
              type: 'server_vad',
              // 0.85 was an attempt to reject speakerphone echo by loudness
              // alone. It failed BOTH ways (2026-07-20 call logs): echo still
              // crossed it at the top of utterances AND the caller's real
              // mid-utterance speech never crossed it, so barge-in silently
              // did nothing. Loudness cannot separate echo from speech on a
              // phone line — they arrive at comparable amplitude. Echo is now
              // handled where it's actually separable (EchoGuardedTransport's
              // timing gate + the echo-transcript suppressor below), which
              // frees this back down to a sensitivity that hears a real
              // caller talking over the agent.
              threshold: Number(process.env.VAD_THRESHOLD || 0.6),
              prefixPaddingMs: Number(process.env.VAD_PREFIX_MS || 500),
              silenceDurationMs: Number(process.env.VAD_SILENCE_MS || 600),
              createResponse: true,
              // FALSE on purpose. With true, the SERVER cancels the in-flight
              // response the instant VAD fires — before any client code runs —
              // so an echo burst kills the agent mid-sentence and no local
              // guard can prevent it. False hands cancellation to the client
              // (openaiRealtimeWebsocket.mjs:216-220 then routes it through
              // transport.interrupt()), which is exactly where the echo guard
              // sits. Real barge-in still cancels; our own voice no longer does.
              interruptResponse: false,
            },
          },
          output: {
            voice: VOICE,
            speed: activeVoiceSpeed,
          },
        },
      },
    });

    type OutboundMessage = { text: string; retries: number };
    const outboundMessageQueue: OutboundMessage[] = [];
    let outboundMessageInFlight = false;
    const maxSendRetries = 4;

    const flushOutboundMessages = (): void => {
      if (outboundMessageInFlight) return;
      const next = outboundMessageQueue.shift();
      if (!next) return;
      outboundMessageInFlight = true;

      Promise.resolve()
        .then(() => session.sendMessage(next.text))
        .catch((err: any) => {
          const code = err?.error?.error?.code || err?.code;
          if (
            code === 'conversation_already_has_active_response' &&
            next.retries < maxSendRetries
          ) {
            // Requeue with backoff so tool/system messages do not crash the call.
            setTimeout(() => {
              outboundMessageQueue.unshift({ text: next.text, retries: next.retries + 1 });
              flushOutboundMessages();
            }, 350 * (next.retries + 1));
            return;
          }
          console.warn('[dexter-phone] sendMessage failed:', err);
        })
        .finally(() => {
          outboundMessageInFlight = false;
          setTimeout(flushOutboundMessages, 220);
        });
    };

    const safeSendMessage = (text: string, delayMs = 0): void => {
      const enqueue = () => {
        outboundMessageQueue.push({ text, retries: 0 });
        flushOutboundMessages();
      };
      if (delayMs > 0) {
        setTimeout(enqueue, delayMs);
      } else {
        enqueue();
      }
    };
    let voicemailRecoverySent = false;

    // Stream SID is seeded synthetic in state (above) and overwritten when
    // Twilio sends the real `start` event via transport_event.
    const callEntry: CallLogEntry = {
      streamSid: state.streamSid,
      callerNumber: callerInfo.callerNumber,
      stirVerstat: callerInfo.stirVerstat,
      direction: bootstrap.direction,
      alertIntentId: outboundAlert?.intentId,
      alertEventId: outboundAlert?.eventId,
      harkBinding: harkBinding ? structuredClone(harkBinding) : undefined,
      harkAnsweredBy: harkBinding ? harkAnsweredBy : undefined,
      harkResponses: harkBinding ? harkResponses : undefined,
      model: MODEL,
      voice: VOICE,
      voiceSpeed: activeVoiceSpeed,
      promptVersion: outboundAlert ? ALERT_PROMPT_VERSION : 'inbound-soul-v1',
      toolRoster: callToolNames,
      alertContextDigest: outboundAlert ? alertContextDigest(outboundAlert) : undefined,
      callerIdentity: {
        resolved: lookup.found,
        name: lookup.name,
        walletLinked: Boolean(
          lookup.wallet || lookup.userHandle || lookup.swigAddress || lookup.vaultPda,
        ),
        walletLabel: lookup.walletLabel,
        phoneTabActive: lookup.phoneTab?.active ?? false,
      },
      startedAt: callStartedAt,
      transcriptSegments: [],
      toolCalls: [],
    };
    activeCalls.set(state.streamSid, callEntry);

    const buildCallEndedPayload = (
      endedAt: string,
      durationSeconds: number,
    ): Record<string, unknown> => ({
      // Preserve every pre-TwiML financial/identity byte. Media may enrich
      // telemetry only; it cannot silently re-resolve the billed principal.
      ...financialBaseBody,
      ended_at: endedAt,
      duration_seconds: durationSeconds,
      model: MODEL,
      voice: VOICE,
      voice_speed: activeVoiceSpeed,
      prompt_version: callEntry.promptVersion,
      tool_roster: callEntry.toolRoster,
      alert_context_digest: callEntry.alertContextDigest,
      caller_identity: callEntry.callerIdentity,
      tool_call_count: callEntry.toolCalls.length,
      audio_chunks: state.audioChunksOut,
      audio_bytes: state.audioBytesOut,
      segment_count: callEntry.transcriptSegments.length,
      audio_diagnostics: callEntry.diagnostics?.summary ?? null,
      tool_calls: callEntry.toolCalls,
      transcript_segments: callEntry.transcriptSegments,
      hark_responses: callEntry.harkResponses ?? [],
      errors: [],
    });

    // Capture raw provider lineage before any local transport-event consumer.
    // respond_to_hark accepts only callId -> function item -> predecessor chain
    // -> independently ordered caller audio; latest-session state is irrelevant.
    session.on('transport_event', (event: any) => harkRealtimeLineage.observe(event));

    // Listen for Twilio events to capture stream SID
    session.on('transport_event', (event: any) => onTransportEvent(state, callEntry, activeCalls, event));

    // ── measured telemetry feed ────────────────────────────────────────────
    // Inbound Twilio audio is the only place the caller's ACTUAL signal level
    // is observable. Everything else in this pipeline is a downstream opinion
    // about that signal, so meter it here and let the numbers arbitrate.
    session.on('transport_event', (event: any) => {
      const etype = event?.type;

      if (etype === 'twilio_message' && event?.message?.event === 'media') {
        const payload = event.message.media?.payload;
        if (typeof payload === 'string') {
          diag.onInboundFrame(
            payload,
            transport.isPlaybackPending(),
            state.audioStartTs ? Date.now() - state.audioStartTs : -1,
          );
        }
        return;
      }

      if (etype === 'input_audio_buffer.speech_started') {
        const duringPlayback = transport.isPlaybackPending();
        const rms = diag.recentRms();
        state.lastInputSpeechItemId = String(event?.item_id || '') || null;
        harkCallerItemTracker.noteSpeechStarted(state.lastInputSpeechItemId);
        harkHumanTurnQualifier.noteCallerSpeechStarted(state.lastInputSpeechItemId);
        state.lastInputSpeechStartedDuringPlayback = duringPlayback;
        state.lastInputSpeechStartedRms = rms;
        state.lastInputSpeechResponseCountAtStart = state.responseCount;
        diag.recordSpeechStarted(
          duringPlayback,
          state.audioStartTs ? Date.now() - state.audioStartTs : -1,
        );
        return;
      }

      if (etype === 'input_audio_buffer.speech_stopped') {
        diag.event('speech_stopped');
        return;
      }

      if (etype === 'response.created') {
        // onTransportEvent (registered first) already bumped responseCount, so
        // read it as-is — +1 double-counted and paired created#2 with done#1.
        diag.event('response_created', { n: state.responseCount });
        return;
      }

      if (etype === 'response.done') {
        diag.event('response_done', {
          n: state.responseCount,
          status: event?.response?.status ?? event?.status ?? 'unknown',
        });
        return;
      }

      if (etype === 'conversation.item.input_audio_transcription.completed') {
        diag.event('user_transcript', {
          text: String(event?.transcript || '').slice(0, 120),
          duplex: state.assistantSpeaking,
          rms: diag.recentRms(1200),
        });
      }
    });

    // Echo defense, half two (see session/echoTurnSuppressor.ts): remember what
    // we just said, and when a "caller" turn is really that audio coming back
    // up the line, cancel the reply it triggered instead of answering ourselves.
    session.on('transport_event', (event: any) => {
      const etype = event?.type;

      if (
        etype === 'response.output_audio_transcript.done' ||
        etype === 'response.audio_transcript.done'
      ) {
        const spoken = String(event?.transcript || '').trim();
        if (spoken) state.lastAssistantSpeech = spoken;
        return;
      }

      if (etype === 'conversation.item.input_audio_transcription.completed') {
        const inputItemId = String(event?.item_id || '');
        const orderedCallerCompletion =
          harkCallerItemTracker.noteTranscriptionCompleted(inputItemId);
        const userTranscript = String(event?.transcript || '').trim();
        if (
          outboundAlert
          && !voicemailRecoverySent
          && isAutomatedAudioTranscript(userTranscript)
        ) {
          voicemailRecoverySent = true;
          transport.interrupt();
          safeSendMessage(
            `[VOICEMAIL DETECTED. THIS IS AUTOMATED MACHINERY, NOT THE CALLER.]\n` +
            `SPEAK THIS EXACTLY ONCE: ${JSON.stringify(buildAlertVoicemailSpeech(outboundAlert))}\n` +
            `Do not converse with the greeting, ask for a message, explain, or offer help. Then remain silent.`,
            250,
          );
          diag.event('voicemail_recovery_sent');
          return;
        }
        const evidenceMatches = Boolean(
          inputItemId
          && state.lastInputSpeechItemId
          && inputItemId === state.lastInputSpeechItemId,
        );
        const verdict = classifyUserTurn(
          userTranscript,
          state.lastAssistantSpeech,
          {
            beganDuringPlayback:
              evidenceMatches && state.lastInputSpeechStartedDuringPlayback,
            rms: evidenceMatches ? state.lastInputSpeechStartedRms : null,
            maxEchoRms: echoGuardMaxRms,
          },
        );
        const echoResponseStarted =
          state.responseCount > state.lastInputSpeechResponseCountAtStart;
        state.lastInputSpeechItemId = null;
        state.lastInputSpeechStartedDuringPlayback = false;
        state.lastInputSpeechStartedRms = null;
        state.lastInputSpeechResponseCountAtStart = state.responseCount;
        if (!verdict.echo && orderedCallerCompletion) {
          const shouldAcknowledgeHark = harkHumanTurnQualifier.observe({
            itemId: inputItemId,
            transcript: userTranscript,
            echo: verdict.echo,
          });
          if (
            shouldAcknowledgeHark &&
            harkCall &&
            harkResponseBridge
          ) {
            void harkResponseBridge.acknowledge(harkCall, inputItemId)
              .then(recordHarkSubmission)
              .catch((error: unknown) => {
                console.warn('[dexter-phone] Hark acknowledgment enqueue failed:', String(error));
              });
          }
          return;
        }
        if (!verdict.echo) return;
        state.echoTurnsSuppressed++;
        const text = String(event?.transcript || '').slice(0, 80);
        diag.event('echo_turn_suppressed', { reason: verdict.reason, text });
        console.log(`[diag] echo TURN suppressed (${verdict.reason}): "${text}"`);
        // Cancel only the phantom response and delete its input item. Do NOT
        // clear the legitimate Twilio playback whose low-energy echo caused it.
        transport.discardEchoTurn(inputItemId, echoResponseStarted);
      }
    });

    session.on('mcp_tools_changed', (tools: { name: string }[]) => onMcpToolsChanged(tools));

    // Per-call audio-out byte counter — see onAudio handler.
    session.on('audio', (evt: any) => onAudio(state, evt));

    session.on('audio_interrupted', () => onAudioInterrupted(state));

    session.on('audio_start', () => {
      harkHumanTurnQualifier.noteAssistantPlaybackStarted();
      onAudioStart(state);
    });

    session.on('audio_stopped', () => onAudioStopped(state, transport.isPlaybackPending()));

    session.on('error', (evt: any) => {
      onError(state, safeSendMessage, evt);
      // DEAD-AIR FALLBACK: on an error the session cannot recover from
      // (quota/billing, auth, hard rate limit), the caller would otherwise
      // sit in silence. Close OUR side of the Twilio bridge — that advances
      // Twilio to the <Say> verb in the TwiML, which apologizes in Twilio's
      // own voice and hangs up. Once per call.
      if (!fallbackTriggered) {
        const fatal = classifyFatalSessionError(evt);
        if (fatal) {
          realtimeReadinessGate.trip(fatal as RealtimeReadinessReason);
          fallbackTriggered = true;
          console.error(`[dexter-phone] FATAL session error (${fatal}) — handing caller to Twilio fallback message`);
          diag.event('fatal_fallback', { reason: fatal });
          try { connection.close(); } catch { /* close is best-effort */ }
        }
      }
    });

    session.on('tool_approval_requested', (_ctx: unknown, _agent: unknown, req: any) => onToolApprovalRequested(session, req));

    session.on('mcp_tool_call_completed', (_ctx: unknown, _agent: unknown, call: any) => onMcpToolCallCompleted(callEntry, call));

    // The outbound local roster contains only the six safe analytical tools.
    // Keep this capture scoped to that mode: inbound Verify tools may carry a
    // one-time code in their arguments and must not be persisted here.
    if (outboundAlert) {
      session.on('agent_tool_start', (_ctx: unknown, _agent: unknown, localTool: any, details: any) => {
        onAgentToolStart(callEntry, localTool, details);
      });

      session.on('agent_tool_end', (_ctx: unknown, _agent: unknown, localTool: any, result: unknown, details: any) => {
        onAgentToolEnd(callEntry, localTool, result, details);
      });
    }

    // Per-call dedupe map for history_updated. See onHistoryUpdated for the
    // full background — TL;DR we key on text not itemId because the SDK
    // fires history_updated multiple times for an assistant audio item as
    // its transcript fills in.
    const capturedItemTexts = new Map<string, string>();

    session.on('history_updated', (history: any[]) => onHistoryUpdated(callEntry, capturedItemTexts, history));

    // Cleared on connection close; pushed after session.connect below.
    const watchdogTimers: ReturnType<typeof setTimeout>[] = [];
    let finalizationCheckpointFailed = false;
    const finalizationCheckpointTimer = requiresFinancialCallback && callEndedOutbox
      ? setInterval(() => {
      if (finalizationCheckpointFailed) return;
      const nowMs = Date.now();
      try {
        callEndedOutbox.checkpointCallFinalization({
          id: callEndedCallbackId,
          body: buildCallEndedPayload(
            new Date(nowMs).toISOString(),
            Math.max(0, Math.floor(
              (nowMs - new Date(callEntry.startedAt).getTime()) / 1000,
            )),
          ),
        });
      } catch (error) {
        finalizationCheckpointFailed = true;
        console.error('[dexter-phone] durable call checkpoint failed:', error);
        try { connection.close(1011, 'financial checkpoint unavailable'); } catch {}
      }
      }, 1_000)
      : null;
    finalizationCheckpointTimer?.unref?.();
    // One-shot latch for the dead-air fallback (fatal error path + silent
    // watchdog both funnel here; whichever fires first closes the bridge).
    let fallbackTriggered = false;

    connection.on('close', () => {
      watchdogTimers.forEach(clearTimeout);
      if (finalizationCheckpointTimer) clearInterval(finalizationCheckpointTimer);
      const duration = Math.round((Date.now() - new Date(callEntry.startedAt).getTime()) / 1000);
      callEntry.endedAt = new Date().toISOString();
      callEntry.durationSeconds = duration;

      // Diagnostics enrich the receipt but may never stand between a closed
      // call and its durable financial callback.
      let d: ReturnType<typeof diag.summary> | null = null;
      try {
        d = diag.summary();
        callEntry.diagnostics = { summary: d, timeline: diag.timeline };
      } catch (error) {
        console.error('[dexter-phone] call diagnostics finalization failed:', error);
      }

      // Freeze the exact terminal body before local logging or any network
      // attempt. If this publication fails, the last active-call checkpoint
      // remains recoverable and startup promotes it without another call.
      if (requiresFinancialCallback && callEndedOutbox) {
        try {
          const reservedCallback = callEndedOutbox.finalizeCall({
            id: callEndedCallbackId,
            path: '/internal/voice/call-ended',
            body: buildCallEndedPayload(callEntry.endedAt, duration),
          });
          void callEndedOutbox.deliver(reservedCallback.id)
            .then((entry) => {
              if (entry.state === 'pending') {
                console.warn(
                  `[dexter-phone] call-ended callback pending sid=${callerInfo.callSid} `
                  + `attempts=${entry.attempts} error=${entry.lastError || 'unknown'}`,
                );
              }
            })
            .catch((error: unknown) => {
              console.error('[dexter-phone] call-ended callback delivery failed:', String(error));
            });
        } catch (error) {
          console.error(
            '[dexter-phone] call-ended callback publication is retrying in-process:',
            error,
          );
        }
      }

      activeCalls.delete(state.streamSid);
      console.log(`[dexter-phone] Call ended: ${callerInfo.callerNumber} | ${duration}s | ${callEntry.transcriptSegments.length} segments | ${callEntry.toolCalls.length} tools | audio: ${state.audioChunksOut} chunks / ${state.audioBytesOut} bytes | echo: ${state.echoInterruptsSuppressed} interrupts + ${state.echoTurnsSuppressed} turns suppressed`);

      if (d) {
        // The verdict block — printed as an argument, so the next diagnosis is
        // read off measurements instead of guessed from which events appeared.
        const ratio = d.speechToEchoRatio;
        const verdict =
          ratio === null
            ? 'insufficient audio to judge'
            : ratio >= 3
              ? `separable (${ratio}x) — a loudness threshold CAN work; tune VAD`
              : `NOT separable (${ratio}x) — caller and echo are the same loudness; VAD tuning is a dead end, echo cancellation is the real fix`;
        console.log(
          [
            `[diag] ══ AUDIO REPORT ══`,
            `  open line (agent silent): ${JSON.stringify(d.inboundEnergy.openLine)}`,
            `  duplex (agent speaking):  ${JSON.stringify(d.inboundEnergy.duplex)}`,
            `  speech-to-echo ratio:     ${ratio ?? 'n/a'}  → ${verdict}`,
            `  VAD fired: ${d.vadTriggers.total} total (${d.vadTriggers.whileAgentSpeaking} over our audio, ${d.vadTriggers.whileAgentIdle} on open line)`,
            `    avg level when it fired over us:  ${d.vadTriggers.avgRmsWhileSpeaking ?? 'n/a'}`,
            `    avg level when it fired idle:     ${d.vadTriggers.avgRmsWhileIdle ?? 'n/a'}`,
            `  MISSED barge-ins (loud caller over our audio, VAD never fired): ${d.missedBargeIns.count}`,
            d.missedBargeIns.count > 0
              ? `    samples: ${JSON.stringify(d.missedBargeIns.samples)}`
              : `    → none: every time you talked over it, it registered`,
          ].join('\n'),
        );
      }

      // Persist only after the diagnostics verdict and local-tool traces have
      // been attached. The old order silently omitted diagnostics locally.
      appendCallLog(callEntry);
    });
    richCloseHandlerInstalled = true;

    const sessionConnection = await connectRealtimeSession(
      () => session.connect({ apiKey: OPENAI_API_KEY! }),
      (error) => classifyFatalSessionError(error) as RealtimeReadinessReason | null,
    );
    if (!sessionConnection.ok) {
      realtimeReadinessGate.trip(sessionConnection.reason);
      fallbackTriggered = true;
      console.error(
        `[dexter-phone] Realtime session connection failed (${sessionConnection.reason}) — `
        + 'handing caller to Twilio frozen-alert fallback',
      );
      diag.event('fatal_fallback', { reason: sessionConnection.reason });
      try { connection.close(); } catch { /* close is best-effort */ }
      return;
    }
    console.log(`[dexter-phone] Connected to OpenAI Realtime API (${MODEL}) for ${callerInfo.callerNumber}`);

    // Greeting rides the SAME message as the caller context — two separate
    // messages made the model respond to each one, greeting the caller twice
    // (observed live 2026-07-20). The watchdog below reuses the nudge alone
    // only when nothing has been said at all.
    const GREET_NUDGE = '[The caller just connected. Greet them in character — brief, grand, unmistakably Dexter. ONE line, in your own words — never recite a canned example. Do NOT be long-winded.]';
    const ALERT_NUDGE = outboundAlert ? buildAlertInitialNudge(outboundAlert) : null;
    const INITIAL_NUDGE = ALERT_NUDGE || GREET_NUDGE;

    // Silent-call watchdog: some calls connect cleanly but the model never
    // produces audio (caller hears dead air and hangs up — observed live
    // 2026-07-20, 2 of 3 calls). If nothing has hit the wire a few seconds
    // after connect, nudge the greeting again; if it is STILL silent, log
    // loudly so the observatory flags it. Timers die with the connection.
    watchdogTimers.push(setTimeout(() => {
      if (state.audioChunksOut === 0) {
        console.warn('[dexter-phone] watchdog: no outbound audio 5s after connect — re-sending initial nudge');
        safeSendMessage(INITIAL_NUDGE);
      }
    }, 5000));
    watchdogTimers.push(setTimeout(() => {
      if (state.audioChunksOut === 0 && !fallbackTriggered) {
        // 12s in, the greeting nudge was re-sent at 5s, still not one byte of
        // agent audio: this call is dead air. Hand the caller to the Twilio
        // <Say> fallback rather than letting them listen to silence.
        fallbackTriggered = true;
        console.error('[dexter-phone] watchdog: SILENT CALL — zero outbound audio 12s after connect; handing caller to Twilio fallback message');
        diag.event('fatal_fallback', { reason: 'silent_call_watchdog' });
        try { connection.close(); } catch { /* close is best-effort */ }
      }
    }, 12000));

    if (outboundAlert) {
      safeSendMessage(INITIAL_NUDGE, 200);
      return;
    }

    // Lookup already happened pre-connect; inject ONE message carrying the
    // caller context AND the greet instruction, so exactly one response fires.
    let callerContext: string;
    if (callerInfo.callerNumber !== 'unknown') {
      const callHistory = loadRecentCallSummary(callerInfo.callerNumber);
      if (lookup.found) {
        const isVerified = !!(lookup.wallet || lookup.userHandle);
        let context = `[SYSTEM] Caller identified: ${lookup.name || 'unnamed'}. ${callHistory}`;
        if (lookup.userHandle) {
          const walletName = lookup.walletLabel || 'their Dexter wallet';
          context += ` Caller has a Dexter wallet named "${walletName}" — refer to it by that name, never by address. Paid calls are charged to it automatically.`;
        }
        if (lookup.phoneTab) {
          const priceCents = Math.round(lookup.phoneTab.pricePerMinuteMicro / 10_000);
          const freeMin = Math.round(lookup.phoneTab.freeSeconds / 60);
          if (lookup.phoneTab.active) {
            const remaining = lookup.phoneTab.remainingMicro != null
              ? ` Remaining tab budget: $${(lookup.phoneTab.remainingMicro / 1e6).toFixed(2)}.`
              : '';
            context += ` Caller has an OPEN PHONE TAB: first ${freeMin} minutes of a call are free, then ${priceCents} cents per minute, billed to their tab at call end.${remaining} If they ask about the meter or their tab, answer from this — do not guess.`;
          } else {
            context += ` Caller has NO phone tab yet — this call is free. If they ask about tabs or metered calling, they can open one at dexter.cash/phone (${freeMin} free minutes per call, then ${priceCents} cents a minute, capped and revocable). Mention it only if relevant.`;
          }
        }
        if (isVerified) {
          context += ` Caller is VERIFIED by their linked phone number — execute wallet operations without any extra verification.`;
          if (callerInfo.stirVerstat && !callerInfo.stirVerstat.includes('Passed')) {
            context += ` CAUTION: Caller ID attestation is "${callerInfo.stirVerstat}" — for high-value wallet operations, verify via SMS code first.`;
          }
        } else {
          context += ` Caller is registered but has no Dexter wallet yet. They can search the marketplace for free; to start paying, they can set up a wallet at dexter.cash/wallet in about thirty seconds.`;
        }
        callerContext = context;
      } else {
        callerContext =
          `[SYSTEM] Caller ${callerInfo.callerNumber} is not registered — their phone number is not linked to any Dexter account. ` +
          `They can still search the marketplace for free. If they want to pay for data, tell them to set up a Dexter wallet at dexter.cash/wallet (thirty seconds, works with Face ID) and link their phone at dexter.cash/profile so you recognize them next call.`;
      }
    } else {
      callerContext =
        `[SYSTEM] Caller ID is unavailable (blocked or unknown). Caller can search the marketplace for free. ` +
        `Wallet operations are not available without a phone number. ` +
        `If they want full features, suggest calling back with caller ID enabled and linking their number at dexter.cash/profile.`;
    }
    safeSendMessage(`${callerContext}\n\n${GREET_NUDGE}`, 200);
    },
  );
});

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

fastify.listen({ port: PORT, host: '0.0.0.0' }, (err: Error | null) => {
  if (err) {
    console.error('[dexter-phone] Fatal:', err);
    process.exit(1);
  }
  console.log(`[dexter-phone] Server listening on port ${PORT} (model: ${MODEL}, voice: ${VOICE})`);
  if (process.env.PHONE_READINESS_PROBE_ON_STARTUP !== 'false') {
    void Promise.all([
      dexterApiReadiness.check(true),
      realtimeReadinessGate.check(true),
    ]).then(([apiReadiness, realtimeReadiness]) => {
      console.log(
        `[dexter-phone] startup readiness api=${apiReadiness.status} `
        + `realtime=${realtimeReadiness.status}`,
      );
    });
  }
});

installPhoneShutdownHandlers(fastify, smsService);
