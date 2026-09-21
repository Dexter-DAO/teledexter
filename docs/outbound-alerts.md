# Outbound monitoring alerts

[Back to README](../README.md) · [Configuration](configuration.md) · [Operations](operations.md)

`dexter-phone` can place a proactive voice call from a monitoring event. The
watcher decides that a threshold was crossed; the phone service authenticates
the frozen event and its destination, then places the call. Healthy calls use
OpenAI Realtime. If Realtime is unavailable, an authorized alert can still be
spoken through Twilio's notification fallback.

The frozen event tools are read-only. A non-Hark call retains the ordinary
Dexter tool access after the opening. Dexter can explain the trigger, calculate
from the evidence, refresh
public market data, discuss portfolio and liquidity context when asked, use the
current hosted OpenDexter roster, and collaborate on improving the briefing. A
hosted action succeeds only through its existing Wallet authority and tool
contract. Phone Tab billing, linked identity, and a one-call Wallet binding do
not create additional financial authority.

Outbound calls use the same linked-number lookup as inbound Dexter Phone calls.
That restores the caller's existing Dexter identity, wallet label, Phone Tab
state, and one-call link-token lifecycle. The link token remains server-side,
is never exposed to the model, and is revoked through the existing call-ended
path. Linked identity and Phone billing do not grant trade, transfer, merchant
payment, or alert-setting authority, and no new OTP or passkey ceremony is
introduced.

The local evidence roster for prompt version
`outbound-notification-v6-2026-08-27` is selected from:

- `get_alert_snapshot` — inspect the immutable signed event by section.
- `refresh_market` — refresh the exact attached Solana pair from DexScreener.
- `get_recent_market_path` — inspect the watcher's bounded sampled path.
- `get_alert_policy` — explain the anchor, threshold, cooldown, and next lines.
- `get_portfolio_exposure` — analyze holdings, mark, supply share, and liquidity.
- `get_call_capabilities` — report identity state, mode, tools, and restrictions.

Interactive outbound calls receive `dexter_knowledge`. Non-Hark calls discover
the full hosted OpenDexter MCP roster. Hark calls restrict hosted tools to
`x402_search`, `x402_status`, `x402_wallet`, `dexter_portfolio`,
`dexter_asset_action_status`, and `dexter_wallet_history`, as defined in
[hostedMcpPolicy.ts](../src/session/hostedMcpPolicy.ts). Wallet setup may change
setup state, but these calls cannot prepare, execute, or reconcile the case's
financial instruction. The originating agent remains its sole executor.

When the Hark response bridge is available and conversation is permitted, Phone
also attaches `respond_to_hark`. That tool returns a clear answer, dismissal,
snooze, choice, or instruction concerning the exact originating case. Ordinary
questions, analysis, speaking-style corrections, and unrelated conversation are
not Hark responses. If the response bridge is unavailable, the hosted-tool
restrictions still apply and the response tool is absent.

Owner wallet addresses, credentials, link tokens, user handles, vault identifiers,
and Swig identifiers are stripped before follow-up context reaches the model.
Token mints and market pair addresses remain because they identify the public
asset and public market being analyzed.

The JORDAN producer refreshes public market data every five seconds but caches
wallet balances between slower RPC reads. If Solana RPC rate-limits a refresh,
market threshold detection continues against the last successful balance read;
the packet includes the exact holdings timestamp, age, and cache mode so Dexter
can disclose staleness rather than silently dropping the price observation.

## Request

`POST /internal/outbound-alerts`

```json
{
  "idempotencyKey": "hark:44444444-4444-4444-8444-444444444444",
  "eventId": "hark:44444444-4444-4444-8444-444444444444",
  "to": "+15615550100",
  "recipientName": "Branch",
  "subject": "CATE",
  "severity": "critical",
  "briefing": "CATE crossed below 5.36 million on expanding sell flow.",
  "observedAt": "2026-07-27T07:00:00.000Z",
  "ttlSeconds": 180,
  "followupContext": {
    "marketCapUsd": 5350000,
    "direction": "down",
    "levelUsd": 5360000,
    "evidenceWindow": "15m"
  },
  "hark": {
    "schemaVersion": 1,
    "caseId": "11111111-1111-4111-8111-111111111111",
    "watchId": "22222222-2222-4222-8222-222222222222",
    "eventId": "33333333-3333-4333-8333-333333333333",
    "deliveryId": "44444444-4444-4444-8444-444444444444",
    "originTaskId": "hermes-task-42"
  }
}
```

`hark` is optional for legacy monitoring calls. When present, the top-level
`eventId` and `idempotencyKey` must both be `hark:<deliveryId>`. That outer key
authorizes and deduplicates one delivery and therefore one call intent. The
nested `hark.eventId` remains the actual Hark event UUID; Phone uses that nested
UUID in status and response callbacks. All five opaque fields are included in the
canonical alert hash, persisted through restart, and carried separately from
model-visible follow-up context.

Constraints:

- `to` must be E.164. Signed first-party watcher calls must use the configured
  allowlist; Hark deliveries are accepted only after Dexter API validates the
  exact delivery, owner destination, and Phone Tab state.
- `briefing` is at most 700 characters; follow-up context is at most 24 KiB.
- `ttlSeconds` is 30–600 seconds.
- Old events are rejected according to `OUTBOUND_ALERT_MAX_EVENT_AGE_SECONDS`.
- An idempotency key can describe exactly one canonical payload. Reusing it
  returns the original intent; changing the body returns `409`.

## Authentication

Legacy non-Hark watcher intents use the producer HMAC below and the configured
recipient allowlist. A request containing `hark` always uses Dexter API as its
authority: it must carry `X-Dexter-Internal-Auth`, and Phone calls
`POST /internal/phone/authorize-outbound` to validate the exact case, event,
delivery, destination, and Phone Tab state before dialing. A valid legacy producer
HMAC never bypasses that API authorization.

Send three headers:

```text
X-Dexter-Alert-Timestamp: 1785110400
X-Dexter-Alert-Nonce: <at least 16 URL-safe random characters>
X-Dexter-Alert-Signature: v1=<hex HMAC-SHA256>
```

The HMAC input is:

```text
v1
<timestamp>
<nonce>
<sha256(canonical-json-body)>
```

Canonical JSON recursively sorts object keys while preserving array order.
Requests outside the 60-second signing window or with a repeated nonce are
rejected. The durable idempotency record is written before Twilio is called, so
a retry or process restart cannot dial twice. A crash in the narrow dispatch
window fails safe as an existing `dialing` intent rather than risking a
duplicate call.

The reference signing helper is `createSignedAlertSignature` in
[src/outbound/auth.ts](../src/outbound/auth.ts). Producers in other repositories
must use the same canonical JSON and signature contract.

## Call lifecycle

1. The signed intent is persisted with an expiration and assigned Twilio
   `CallSid`.
2. Twilio receives answer and status callback URLs containing only the
   non-secret intent ID. They have no query string or callback credential, so
   Fastify and Nginx request logs cannot capture one.
3. Both callback routes validate `X-Twilio-Signature`, the exact intent ID, and
   the exact Twilio `CallSid`. Status callbacks also require Twilio's integer
   `SequenceNumber`. Phone persists the highest accepted sequence, ignores stale
   or duplicate callbacks, never exits a terminal state, and keeps local
   `streaming` separate from Twilio's ordered `answered` lifecycle event.
4. The answer route derives a short-lived, one-shot media token from the correct
   domain-separated root, intent ID, and `CallSid`: legacy intents use
   `OUTBOUND_ALERT_SIGNING_SECRET`; Hark intents use
   `HARK_PHONE_CALLBACK_SIGNING_SECRET`. The latter does not depend on the legacy
   producer secret. The route then returns a path-bound WebSocket URL:
   `/media-stream/:callSid/:streamToken`. Twilio Media Streams do not support
   query strings.
5. The WebSocket route consumes the per-call context before constructing the
   Realtime session. Concurrent calls cannot read a process-wide "next caller"
   slot or attach another call's briefing, and Twilio's original `start` frame
   reaches the transport without interception.
6. The configured Realtime model receives exact deterministic opening copy: a good-news,
   bad-news, heads-up, or controlled-replay cue plus the rounded percentage move
   and rounded market cap. It must speak that copy before using a tool, add
   nothing, and then become silent. Holdings, liquidity, risk controls, and
   trading advice are never volunteered; they are available only for an explicit
   follow-up. Every spoken market-cap figure remains rounded to the nearest
   thousand unless the caller explicitly requests exact precision. Frozen facts
   remain labeled as frozen, and `refresh_market` can
   fetch current public pair data while tracked balances remain frozen at the
   alert observation time.
7. If Realtime fails or sends no audio after the stream opens, closing it
   advances Twilio to the post-`<Connect>` Polly message. If the readiness probe
   already found Realtime unavailable, the answer route uses Twilio speech
   directly without opening an interactive media session.

For a Hark-bound call, Twilio AMD `machine` results can never acknowledge or
respond. AMD `human` alone is insufficient: the first meaningful completed,
non-echo caller utterance qualifies the call. An `unknown` result must produce two
distinct meaningful, non-echo, non-automated completed caller utterances with a
completed Dexter Twilio playback boundary between them. Two VAD/ASR fragments from
one voicemail or one caller turn do not qualify. Playback counters are frozen for
each caller item at server-observed speech start, so a delayed ASR completion cannot
make a pre-playback utterance look as though it began after playback. A clear owner
decision invokes `respond_to_hark`. Realtime may emit that tool call just before the delayed
completed-transcription or raw function-item event arrives. Phone briefly awaits
the provider-authored conversation item for the server-envelope tool-call ID, then
walks its raw `previous_item_id` chain back to the exact caller input item. The
chain must end at independently observed, ordered caller audio; latest session
state and model arguments are never causal identity. That exact item must then
satisfy the AMD, playback, meaningful-turn, and echo rules. Another call, provider
invocation, or caller item cannot wake it, and either lineage or qualification
timeout fails closed. Caller items receive a monotonic order only at
server-observed speech start, so a delayed completion for an older overlapping
item cannot retarget a later tool. A completion with no known start cannot qualify
the human, create an automatic acknowledgment, or become an eligible causal item,
and is never promoted into a tool-binding candidate.
Phone then persists the exact callback in the same protected ledger before posting
it to:

- `POST /internal/hark/v1/deliveries/:deliveryId/status`
- `POST /internal/hark/v1/cases/:caseId/phone-responses`

Both callbacks reuse the canonical HMAC header names described above but sign with
the distinct `HARK_PHONE_CALLBACK_SIGNING_SECRET`, never the watcher producer
secret. The canonical signed status body carries `caseId`, `eventId`, `deliveryId`,
`intentId`, `callSid`, and `originTaskId`; its `deliveryId` must match the route.
The signed response body carries the same binding and its `caseId` must match the
route. Status uses a stable `sourceEventId`; owner responses use a stable
`sourceResponseId` bound to the provider's stable Realtime tool-call ID after Phone
has captured a completed caller item. Natural phone input is normalized once into
the API decision union before reservation: an absolute `snoozeUntil` becomes a
clamped 60-second-to-30-day integer `seconds`, and a choice carries an explicit
`value`. A retry of that provider invocation retains the same ID and, after any
correction dependency is resolved, the same byte-equivalent JSON body even if later
call state has advanced. The same words in a later provider invocation produce a
new response. A correction durably captures its predecessor once with a null,
unsent supersedes placeholder. After that exact predecessor is accepted, Phone
validates its callback `responseId` UUID, persists that API UUID into
`supersedesResponseId`, recomputes the body hash, and only then makes the
correction's first HTTP attempt. Pending dependencies and their corrections survive
restart without dropping the caller's answer.

A response callback is delivered only when a 2xx response strictly matches this
versioned receipt shape. `responseId` must be the API response UUID and `caseState`
is required. When the API creates a continuation it returns the raw, immutable
`hark.continuation.v1` work item; absence is represented by omitting the field:

```json
{
  "v": "hark.phone-callback-result.v1",
  "ok": true,
  "replay": false,
  "responseId": "77777777-7777-4777-8777-777777777777",
  "caseId": "11111111-1111-4111-8111-111111111111",
  "eventId": "33333333-3333-4333-8333-333333333333",
  "deliveryId": "44444444-4444-4444-8444-444444444444",
  "intentId": "55555555-5555-4555-8555-555555555555",
  "callSid": "CA66666666666666666666666666666666",
  "originTaskId": "hermes-task-42",
  "sourceResponseId": "phone-response:exact-source-id",
  "caseState": "resuming",
  "continuation": {
    "v": "hark.continuation.v1",
    "continuationId": "88888888-8888-4888-8888-888888888888",
    "idempotencyKey": "88888888-8888-4888-8888-888888888888",
    "origin": {
      "protocol": "hermes",
      "runtimeId": "runtime-1",
      "taskId": "hermes-task-42",
      "conversationId": "conversation-1"
    },
    "case": {
      "id": "11111111-1111-4111-8111-111111111111",
      "eventId": "33333333-3333-4333-8333-333333333333",
      "summary": "JORDAN crossed +20%."
    },
    "response": {
      "id": "77777777-7777-4777-8777-777777777777",
      "decision": {
        "type": "instruction",
        "text": "Buy only if it remains above the threshold."
      }
    }
  }
}
```

Status callbacks use the same version but require the strict status receipt:

```json
{
  "v": "hark.phone-callback-result.v1",
  "ok": true,
  "replay": false,
  "caseId": "11111111-1111-4111-8111-111111111111",
  "eventId": "33333333-3333-4333-8333-333333333333",
  "deliveryId": "44444444-4444-4444-8444-444444444444",
  "intentId": "55555555-5555-4555-8555-555555555555",
  "callSid": "CA66666666666666666666666666666666",
  "originTaskId": "hermes-task-42",
  "sourceEventId": "phone-status:exact-source-id",
  "caseState": "waiting_owner"
}
```

Malformed JSON, the wrong receipt kind, missing required IDs, an unversioned body,
or `ok: false` on 2xx remains retryable. Before marking its outbox entry delivered,
Phone requires every echoed case, event, delivery, intent, call, origin-task, and
source ID to equal that exact attempted body. It also requires continuation
idempotency identity, nested origin task, case/event IDs, API response UUID, and
decision to match the accepted response. Phone reduces the validated raw work item
to its continuation ID and origin task for narration; it does not invent a current
queued/completed status on replay. A mismatched receipt or continuation identity is
rejected and is never exposed as success.

Every call log and Observatory payload records the model, voice, voice speed,
prompt version, exact tool roster, safe caller-identity resolution,
alert-context digest, local tool calls, transcript, and audio diagnostics. This
makes regressions such as
"what else can you do?" producing a refusal attributable to an exact prompt and
tool contract rather than guesswork.

Outbound alerts use fixed Realtime audio output speed `1.0`. There is no alert
playback-speed override: brisk delivery is generated as natural vocal cadence,
never by mechanically accelerating the audio. The prompt records this distinction
so Dexter cannot claim the backend slowed down when only its speaking style changed.

Original gimmick characters are supported as short vocal bits when requested.
A generic request for a silly voice is treated as creative direction, not
silently reinterpreted as a request to copy a particular person or character.

Interruption state follows Twilio playback, not OpenAI generation completion.
The returned `done:<itemId>` mark ends the speaking state. If the caller begins
speaking after generation has ended but while Twilio still has queued audio,
the transport sends Twilio `clear` immediately and truncates the unheard model
audio from conversation history. The opening echo guard suppresses only
low-energy audio; a loud caller always wins, even when repeating Dexter's words.

The local ledger uses the required absolute `OUTBOUND_ALERT_STATE_PATH`, outside
the checkout and temporary directories, with mode `0600`. It retains terminal
idempotency records for seven days. Pending Hark callbacks
are never pruned before delivery or explicit rejection. Startup and bounded
in-process retries drain the same durable outbox in repeated 100-entry batches,
then schedule another bounded pass if work remains. On load, earlier ledger formats
are migrated to version 4 and obsolete answer/status token hashes are stripped.

## Deployment checklist

Use the [configuration guide](configuration.md) for environment defaults and
the [operations guide](operations.md) for readiness and call monitoring.

1. Keep a 32+ character random `OUTBOUND_ALERT_SIGNING_SECRET` in each legacy
   non-Hark watcher and `dexter-phone`.
2. Generate a different 32+ character `HARK_PHONE_CALLBACK_SIGNING_SECRET` and
   install it only in `dexter-phone` and the Dexter API Hark callback verifier.
   Phone also derives Hark media tokens from this root; Hark does not need the
   legacy producer secret.
3. Set `DEXTER_PHONE_PUBLIC_URL` to the exact public HTTPS origin Twilio calls.
4. Set `OUTBOUND_ALERT_STATE_PATH` and `PHONE_API_CALLBACK_OUTBOX_PATH` to
   explicit absolute files outside the checked-out release (for example beneath
   `/var/lib/dexter-phone/`). Both ledgers must remain on durable storage across
   deploys and restarts. Create the canonical parent as the service user with no
   group/world write bit. Any existing ledger must be a service-owned, regular,
   non-symlink, single-link mode-`0600` file; temporary-directory paths are
   rejected.
5. Set `OUTBOUND_ALERT_ALLOWED_TO` to the intended legacy recipient number(s), never a
   wildcard.
6. Set `OUTBOUND_ALERT_FROM_NUMBER` to a Twilio voice-capable sender.
7. Keep `TWILIO_VALIDATE_WEBHOOKS=true`.
8. Set a separate 32+ character `DEXTER_PHONE_ADMIN_SECRET`; Nginx loopback is
   not an authorization boundary for `/calls`.
9. Set `DEXTER_API_URL` to a stable internal dexter-api endpoint. Do not pin it
   to a blue/green application port that changes during deployment.
10. Run `npm test`, `npm run typecheck`, and `npm run build`.
11. Confirm `/health` reports `financialReady: true` and
    `notificationReady: true`. `conversationReady: false` means authorized alerts
    can still be spoken by Twilio, but interactive AI conversation is degraded.
    A 503 is a hard call-admission block.
12. Reload the service with its updated environment only after explicit
   deployment approval.
13. First run a mocked dispatch, then one explicitly approved controlled call.
    Verify the transcript for a healthy Realtime call and the spoken frozen alert
    for the Realtime-unavailable fallback.

The monitoring worker should use episode-aware keys: crossing below a level and
later crossing back above it are distinct events, while repeated samples inside
the same episode reuse the same key.
