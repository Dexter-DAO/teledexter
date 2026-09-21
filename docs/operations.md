# Operations

[Back to README](../README.md) · [Configuration](configuration.md) · [Outbound alerts](outbound-alerts.md)

## Twilio routing

1. Set `DEXTER_PHONE_PUBLIC_URL` to the exact public HTTPS origin Twilio will use.
2. Point the number's Voice webhook to `https://<your-domain>/incoming-call`
   using POST.
3. Forward HTTP requests and WebSocket upgrades on `/media-stream/` to Phone's
   listener, port `3212` unless overridden.
4. Keep `TWILIO_VALIDATE_WEBHOOKS=true`. Signature verification uses the public
   URL, so proxy routing and the configured origin must agree.

For proactive calls, also configure the sender, authorization, and durable
state described in the [outbound contract](outbound-alerts.md).

## Readiness

```bash
curl -sS http://localhost:3212/
curl -sS http://localhost:3212/health
```

`GET /` returns service metadata, model, voice, and readiness snapshots. It is
not an acceptance test for a phone call.

| Field | What it checks |
|---|---|
| `financialReady` | Callback signing configuration and operational durable ledgers |
| `notificationReady` | Financial readiness plus a healthy Dexter API connection |
| `conversationReady` | Notification readiness plus successful Realtime audio generation |

`/health` returns 200 when notifications are ready, even if interactive Realtime
conversation is unavailable. In that state, an authorized outbound alert can
still call and have Twilio read its frozen briefing. Inbound calls receive an
unavailability message. API or financial-readiness failure produces HTTP 503.

Inspect `reasons`, `degradedReasons`, `dexterApiReadiness`, and
`realtimeReadiness` for the cause. Pending financial callbacks can represent
normal retry state. Storage failures or terminal callback identity conflicts
block financial readiness.

A health probe does not verify Twilio routing, caller-to-wallet binding, hosted
tool compatibility, or a completed financial action. Verify those with the
relevant controlled call and its receipts when releasing changes.

## HTTP and WebSocket routes

| Route | Method | Purpose |
|---|---|---|
| `/` | GET | Service metadata and readiness snapshots |
| `/health` | GET | Readiness and dependency status |
| `/incoming-call` | ALL | Twilio-signed inbound webhook; returns TwiML |
| `/media-stream/:callSid/:streamToken` | WS | Audio bridge with a one-shot bootstrap token |
| `/internal/outbound-alerts` | POST | Authenticated, idempotent alert; Hark uses API authorization |
| `/outbound-alerts/:intentId/answer` | POST | Twilio-signed answer webhook |
| `/outbound-alerts/:intentId/status` | POST | Twilio-signed status webhook |
| `/calls` | GET | Recent call summaries; requires `X-Dexter-Phone-Admin-Secret` |
| `/calls/:index` | GET | One call record; requires the same admin header |

## Call monitoring

The CLI reads PM2's output log and reconstructs calls from connection markers:

```bash
npm run calls            # latest call
npm run calls -- brief   # selected identity, transcription, and tool events
npm run calls -- watch   # follow the log
```

Set `DEXTER_PHONE_LOG` if the log is somewhere other than
`~/.pm2/logs/dexter-phone-out.log`. HTTP call-log routes use the persisted
`call-log.json` under `PHONE_RUNTIME_STORAGE_DIR` (default `storage/`).
Call records include transcripts and tool activity; keep them private.

## Process management

From the intended release directory, with the service environment configured:

```bash
pm2 start npm --name dexter-phone -- run start
pm2 restart dexter-phone --update-env
pm2 logs dexter-phone --lines 200
```

`npm start` executes `src/server.ts` through `tsx`. `npm run build` is a
typecheck and emits no release artifact. Keep the two durable ledger paths
outside the release directory when replacing code.

Shell/PM2 environment values take precedence over `.env`; check the effective
model and voice at `/` after a restart. Configure `DEXTER_API_URL` to a stable
reachable API address, rather than assuming a particular local application port.

## Tools and authority

Ordinary inbound and non-Hark outbound calls receive the hosted MCP roster
without a local allowlist. The connected server determines which tools exist;
the call's Wallet authority determines which actions can execute.

The local inbound tools are `dexter_knowledge`, `send_verification_code`,
`check_verification_code`, and `send_sms`. The SMS tool accepts message text
only and sends to the current caller after they request a text. Even a one-off
follow-up requires that caller's verified, current-version web enrollment at
[dexter.cash/sms](https://dexter.cash/sms), active delivery configuration, and
an unsuppressed subscription. Phone linking and in-call identity verification
leave SMS consent unchanged. Outbound alert calls attach the relevant
frozen-evidence tools and knowledge tool; `send_sms` is limited to inbound
calls. Hark calls may also attach `respond_to_hark`.

The Hark hosted allowlist is defined in
[hostedMcpPolicy.ts](../src/session/hostedMcpPolicy.ts): `x402_search`,
`x402_status`, `x402_wallet`, `dexter_portfolio`, `dexter_asset_action_status`,
and `dexter_wallet_history`. These names must match the connected MCP server.
Hark calls cannot prepare, execute, or reconcile the originating instruction.

Paid API instructions use check → opaque intent → one approved fetch. An
uncertain result is inspected using the same intent, without automatically
repeating the paid request. Asset instructions similarly prepare an exact action
and use its durable execution/status contract. The source instructions live in
[phoneOpenDexterInstructions.ts](../src/session/phoneOpenDexterInstructions.ts);
check their tool names against the server when upgrading the MCP integration.

## Troubleshooting

**The process exits on startup.** Check `OPENAI_API_KEY` and `DEXTER_API_URL`.
The API address must be an explicit HTTP(S) URL.

**Health returns 503.** Read the reported reasons. Confirm the API base reaches
Dexter API's own health endpoint. Check that both ledger parents exist and
meet ownership/permission requirements, and that the callback secret is valid.

**Health is 200 but the call is not interactive.** Inspect `conversationReady`
and the Realtime probe result. Verify model access, credentials, and provider
availability. The notification fallback does not imply a working AI session.

**Twilio gets 403.** Check the account auth token and exact public origin used
for signature verification. Confirm the proxy preserves the requested route.

**A caller is not recognized.** Check the number's account link and API lookup
result. A running Phone process does not prove identity lookup succeeded.

**Dexter interrupts itself or misses barge-in.** Inspect audio diagnostics and
current settings. The source uses `server_vad` and guarded client-side
cancellation. Review echo guard, VAD, and noise reduction settings together;
making detection less sensitive can also suppress real caller speech.

**A hosted tool is missing or fails.** Check `MCP_URL`, the roster loaded for
that call, its server-side wallet binding, and its call mode. Hark has a restricted
roster. A prompt naming a tool does not make it available.

**No SMS link arrives.** General SMS delivery defaults off. Check the caller's
web enrollment and suppression state, then the intended campaign, Messaging
Service, Advanced Opt Out configuration, and delivery flags described in the
[SMS guide](sms.md). An accepted or queued message still needs delivery evidence.

`DEXTER_SMS_ENROLLMENT_ENABLED=true` permits the website enrollment flow to
send Verify codes even while `DEXTER_SMS_SEND_ENABLED=false` keeps general
texts off. These enrollment and delivery flags leave the existing in-call
`send_verification_code` and `check_verification_code` tools unchanged. Verify
codes establish phone ownership; enrollment requires separate affirmative SMS
consent.
