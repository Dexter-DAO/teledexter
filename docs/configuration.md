# Configuration

[Back to README](../README.md) · [Operations](operations.md) · [Outbound alerts](outbound-alerts.md)

These settings describe the current source. A deployed release can have different
overrides; `GET /` reports its configured model and voice. Existing process
environment values take precedence over `.env` through `dotenv.config()`.
Keep credentials private and out of commits.

## Initial setup

1. Supply your own OpenAI and Twilio credentials. Point `DEXTER_API_URL` at a
   reachable Dexter API that provides its `/health` and `/internal/phone/`
   routes. Its health response must contain `ok: true` and
   `service: "dexter-api"`.
2. Create a durable directory outside this checkout, owned by the user that runs
   Phone. For example, on Linux:

   ```bash
   install -d -m 700 "$HOME/.local/state/dexter-phone"
   ```

3. Configure these values in your private environment. Replace the placeholders
   and use the absolute path to the directory you created; do not use a literal
   `~` or `$HOME` inside path values in `.env`.

   ```dotenv
   OPENAI_API_KEY=<your-openai-key>
   DEXTER_API_URL=https://<your-internal-dexter-api-host>
   DEXTER_PHONE_PUBLIC_URL=https://<your-phone-host>
   TWILIO_ACCOUNT_SID=<your-twilio-account-sid>
   TWILIO_AUTH_TOKEN=<your-twilio-auth-token>
   TWILIO_PHONE_NUMBER=<your-voice-capable-E.164-number>
   HARK_PHONE_CALLBACK_SIGNING_SECRET=<shared-with-api-verifier-at-least-32-characters>
   OUTBOUND_ALERT_STATE_PATH=/absolute/durable/path/outbound-alert-state.json
   PHONE_API_CALLBACK_OUTBOX_PATH=/absolute/durable/path/api-callback-outbox.json
   ```

   The callback secret is required for inbound calls too. Install the matching
   secret in the Dexter API callback verifier. If you use legacy watcher alerts,
   their producer secret must be a different value.
4. Select a Realtime model your OpenAI project can access. Set `REALTIME_MODEL`
   and `DEXTER_VOICE` to override the source defaults below. Add a Twilio Verify
   Service SID if you need verification codes.
5. Start Phone with `npm run dev`, then inspect `/health`. Startup normally
   probes Dexter API and generates a short Realtime audio response. This is a
   real provider request, not a simulated health check.

Missing `OPENAI_API_KEY` or an absent/invalid `DEXTER_API_URL` stops startup.
Invalid financial storage or callback configuration prevents call admission
and makes `/health` return 503.

## Core settings

| Variable | Source default | Purpose |
|---|---|---|
| `OPENAI_API_KEY` | Required | Realtime sessions and audio probes |
| `DEXTER_API_URL` | Required | Caller lookup, knowledge, authority, and callbacks |
| `PORT` | `3212` | HTTP and WebSocket listener |
| `MCP_URL` | `https://open.dexter.cash/mcp` | Hosted tools |
| `DEXTER_PHONE_PUBLIC_URL` | None | Exact public HTTPS origin for Twilio callbacks and signature validation |

## Voice and audio

| Variable | Source default | Purpose |
|---|---|---|
| `REALTIME_MODEL` | `gpt-realtime-2` | Realtime session and probe model |
| `DEXTER_VOICE` | `sage` | Output voice |
| `TRANSCRIPTION_MODEL` | `gpt-realtime-whisper` | Input transcription |
| `TRANSCRIPTION_LANGUAGE` | `en` | Transcription language |
| `TRANSCRIPTION_DELAY` | `high` | Transcription delay setting |
| `REASONING_EFFORT` | `low` | Realtime reasoning effort |
| `NOISE_REDUCTION` | `far_field` | Input noise reduction; `near_field` is another configuration option |
| `VOICE_SPEED` | `1.0` | Inbound speed, clamped to `0.25`–`1.5` |
| `VAD_THRESHOLD` | `0.6` | Server voice-activity threshold |
| `VAD_PREFIX_MS` | `500` | Audio retained before detected speech |
| `VAD_SILENCE_MS` | `600` | Silence used to end a speech turn |
| `ECHO_GUARD_MS` | `1100` inbound / `1800` outbound | Initial playback window for suppressing low-energy echo |
| `ECHO_GUARD_MAX_RMS` | `0.14` | Maximum input energy eligible for echo suppression |

Turn detection uses `server_vad` with `createResponse: true` and
`interruptResponse: false`. Client-side transport handling controls cancellation
so it can reject echo before interrupting playback. Real caller speech can still
interrupt Dexter. Outbound alert playback stays at `1.0x` regardless of
`VOICE_SPEED`.

## Twilio

| Variable | Purpose |
|---|---|
| `TWILIO_ACCOUNT_SID` / `TWILIO_AUTH_TOKEN` | Your account credentials for calls and webhook validation |
| `TWILIO_PHONE_NUMBER` | Voice-capable E.164 sender when no outbound-specific number is set |
| `TWILIO_VERIFY_SID` | Verify Service SID for SMS verification codes |
| `TWILIO_VERIFY_ACCOUNT_SID` / `TWILIO_VERIFY_AUTH_TOKEN` | Optional separate Verify account; otherwise uses the primary account |
| `TWILIO_VALIDATE_WEBHOOKS` | Defaults to `true`; verifies `X-Twilio-Signature` |

Use explicit credentials for your account. General `send_sms` delivery has been
removed; a Messaging Service SID is not a requirement for this service. Inbound
calls attach Verify tools. Already linked outbound calls do not attach those
tools. Verification is separate from Wallet permissions.

## Durable state and administration

| Variable | Requirement / default |
|---|---|
| `HARK_PHONE_CALLBACK_SIGNING_SECRET` | Required for financial readiness; 32+ characters and distinct from any legacy producer secret |
| `OUTBOUND_ALERT_STATE_PATH` | Required absolute path for outbound intent and Hark callback state |
| `PHONE_API_CALLBACK_OUTBOX_PATH` | Required absolute path for financial call-lifecycle callbacks |
| `PHONE_RUNTIME_STORAGE_DIR` | Call-log directory; defaults to `storage/` under the working directory |
| `DEXTER_PHONE_ADMIN_SECRET` | 32+ characters to enable access to call-log HTTP routes |
| `DEXTER_PHONE_LOG` | Optional PM2 output-log path for `npm run calls` |

Both ledger paths must be outside the checkout/release and temporary directories.
Their parents must already exist, resolve without symlinks, belong to the service
user, and have no group/world write permissions. Existing ledger files must be
regular, singly linked, service-owned files with mode `0600`. Phone creates a
missing ledger; do not create an empty JSON file manually.

`PHONE_RUNTIME_STORAGE_DIR` controls diagnostic call logs, not either financial
ledger. Keep the ledgers across releases and restarts.

## Outbound alerts

| Variable | Requirement / default |
|---|---|
| `OUTBOUND_ALERT_SIGNING_SECRET` | 32+ characters for legacy non-Hark watcher HMACs |
| `OUTBOUND_ALERT_ALLOWED_TO` | Comma-separated E.164 allowlist for legacy alerts; empty admits no legacy recipients |
| `OUTBOUND_ALERT_FROM_NUMBER` | Twilio voice-capable E.164 sender; otherwise uses `TWILIO_PHONE_NUMBER` |
| `OUTBOUND_ALERT_MAX_EVENT_AGE_SECONDS` | `900`; maximum age of submitted evidence |

Hark deliveries are authorized by Dexter API for the exact delivery and
destination. They do not depend on the legacy producer secret or its recipient
allowlist. Both flows still require valid Twilio, callback, and durable-state
configuration. See the [outbound contract](outbound-alerts.md) for request signing,
the Hark internal-auth header, and deployment checks.

## Readiness controls

| Variable | Default |
|---|---|
| `DEXTER_API_READINESS_TIMEOUT_MS` | `2500` |
| `DEXTER_API_READINESS_SUCCESS_TTL_MS` | `15000` |
| `DEXTER_API_READINESS_FAILURE_TTL_MS` | `5000` |
| `REALTIME_READINESS_TIMEOUT_MS` | `7000` |
| `REALTIME_READINESS_SUCCESS_TTL_MS` | `60000` |
| `REALTIME_READINESS_FAILURE_TTL_MS` | `15000` |
| `PHONE_READINESS_PROBE_ON_STARTUP` | `true` |

Setting `PHONE_READINESS_PROBE_ON_STARTUP=false` skips eager startup probes;
call admission still checks dependencies. `/health` refreshes API readiness
through its cache and reports the most recent Realtime probe state. See
[readiness interpretation](operations.md#readiness) before using HTTP 200 as an
interactive-call acceptance check.
