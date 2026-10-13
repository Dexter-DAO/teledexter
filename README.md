<p align="center">
  <img src="https://raw.githubusercontent.com/Dexter-DAO/dexter-x402-sdk/main/assets/dexter-wordmark.svg" alt="Dexter" width="360">
</p>

<h1 align="center">Teledexter</h1>

<p align="center">
  <strong>A phone line to your own AI financial agent on Solana.</strong><br>
  Call <strong>+1 (561) 940-2402</strong> from your verified number.<br>
  <a href="https://www.loom.com/share/db201aec86bf4108a203e63542ab5e1d">Live demo</a> · <a href="https://www.loom.com/share/4b55495806da4c1c8e9089618d2ddcb0">Pitch</a>
</p>

<p align="center">
  <a href="https://nodejs.org"><img src="https://img.shields.io/badge/node-%3E%3D22-339933" alt="Node 22 or newer"></a>
  <a href="#live-on-mainnet"><img src="https://img.shields.io/badge/Solana-mainnet-6f5cff" alt="Solana mainnet"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-111111" alt="MIT license"></a>
</p>

Teledexter is a phone line to your own Dexter, the AI agent attached to your self-custodial Dexter account. You call Dexter from your verified number, or Dexter calls you for an arranged call. On the call it reads your portfolio, researches assets, and buys or sells tokenized stocks under the standing permission you set in advance with your passkey. Calls triggered by saved watches are implemented but not yet proven in production. This repository is the voice broker that runs those calls. Teledexter succeeds an earlier product, Dexter Phone; the service and its process are still named `dexter-broker`.

## Live on mainnet

Each of these trades was requested by voice on a phone call to Dexter and settled on Solana mainnet. Primary Dexter carried out each one under the account's standing permissions; the broker relayed the conversation and holds no wallet keys.

| Time (UTC) | Trade | Transaction |
| --- | --- | --- |
| 2026-09-30 16:21 | Bought 0.01467362 AAPLx for 5 USDC | [`2ePLu4tj…R1AE4b`](https://solscan.io/tx/2ePLu4tj4BS3ckMsJrCe62jmEhUnU3hziSymuEyzChQoQnJLLcWBx1JHq7U5XRvpMA3KDKMDhvmBGAsQ55R1AE4b) |
| 2026-10-13 04:11 | Sold 0.00875977 AAPLx for 2.977647 USDC | [`5XtuXQwS…efvtDP`](https://solscan.io/tx/5XtuXQwSLvtTKvdy6VzHSz9yPNPgaCK3Yxm1qifjaBikcCnPP7zuJyHLGhxQgKf2Lqcmp2FqrMb3w2onWxefvtDP) |
| 2026-10-13 04:13 | Bought 0.00289208 AAPLx for 1 USDC | [`2TcC3Tn7…6J2bmr`](https://solscan.io/tx/2TcC3Tn7KBTLdZyfBzTzuQwJ6NdeXmzRkHpXufJHPoLkqugz2P1Bp5pyeNqboJtKmpz83Q3HS4gY5o4afp6J2bmr) |
| 2026-10-13 05:49 | Sold 0.00292246 AAPLx for 0.993421 USDC | [`5QzyX2Ui…H16v4T`](https://solscan.io/tx/5QzyX2UixL1jSNrAbVw71fXEKUtoLugyatPgPho3y9ybo7siL7fzMGDdrUbf68qsBg1zuwfg6NRG8NSWhmH16v4T) |
| 2026-10-13 05:56 | Bought 0.00289197 AAPLx for 1 USDC | [`3ooYTJWw…UXFWst`](https://solscan.io/tx/3ooYTJWwYR5uSKLyKZV9mV1EjKV8DggiWoHfh6VevUvKRJQwciiuftvUFhNwTMtSwZCvA6HfBog6bXyzdPUXFWst) |
| 2026-10-13 06:26 | Bought 0.00289167 AAPLx for 1 USDC on the [demo call](https://www.loom.com/share/db201aec86bf4108a203e63542ab5e1d) | [`3669NH69…VWmcz5`](https://solscan.io/tx/3669NH69yj9MzQRSW9hCYnsV9uHA62QwQzmDBTgq3i9ynMhV9dJKve1j2y52jDpom7SFxwFpbjkFHc8S1qVWmcz5) |

The limits each trade ran under are enforced on-chain by the Dexter Vault program, [`Hg3wRayd…pncj2fhc`](https://solscan.io/account/Hg3wRaydFtJhYrdvYrKECacpJYDsC9Px7yKmpncj2fhc), which is not part of this repository.

## How it works

```
  Caller (phone)
      ^
      |  PSTN
      v
  Twilio Programmable Voice + Media Streams (8 kHz PCMU over WebSocket)
      ^
      |  signed webhooks, media WebSocket
      v
  Teledexter broker (this repo) <---- WebSocket ----> OpenAI GPT-Live (gpt-live-1)
      |                                               conversation and delegation
      |  HTTP(S), service-authenticated requests
      v
  Dexter API (private) --> Primary Dexter agent --> OpenDexter tools --> Solana
```

GPT-Live holds the spoken conversation. When the caller asks for something that needs their account, GPT-Live emits a delegation. The broker turns the caller's completed words into a request, saves it, and submits it to Primary Dexter through the Dexter API. It then watches the request, relays any question Primary asks, and hands the result back to GPT-Live to speak at a quiet moment in the conversation.

## Repository map

| Path | Contents |
| --- | --- |
| `src/broker/` | HTTP server, Twilio webhooks and media, call journal, Primary request delivery and recovery, inbound attachment, saved-watch and notification calls, result SMS |
| `src/live/` | GPT-Live WebSocket session and the restricted phone-setup tools |
| `src/outbound/` | Request signing and verification, canonical JSON, durable file writes, Twilio webhook validation, callback outbox |
| `src/sms/` | SMS enrollment, consent ledger and sending (optional) |
| `scripts/broker.mts` | Operator CLI: `status`, `preflight`, `call`, `end`, `reconcile` |
| `scripts/qualify-live-phone.mts` | Runs the broker against real GPT-Live with simulated Twilio and Primary |
| `scripts/calls.mjs` | Read-only call journey timing report |
| `deploy/` | PM2 configuration and an nginx location snippet |
| `docs/` | Operator and protocol reference |

Not in this repository: the Dexter API, Primary Dexter, the OpenDexter tools, account authority, the wallet vault, and all Solana transaction building, signing and execution. The broker holds no wallet keys and never builds, signs or submits a transaction. It forwards what the caller said and relays what Primary returns; Primary acts under the permissions the account owner granted in Dexter.

## Design points

- **Durable call journal with a single writer.** Calls, requests, transcript captures and delivery receipts are written to `calls.json` in `BROKER_STATE_DIR`. The store takes an exclusive `writer.lock` at startup, so two broker processes cannot write the same journal. Work is recovered from the journal after a restart.
- **Idempotent request IDs.** An operator call is created with a caller-chosen UUID; repeating it returns the saved call instead of dialing again. Requests to Primary use IDs derived from the call, voice session and delegation, saved before submission. After a lost reply the broker reconciles by that ID and never repeats the original submit.
- **Interjections while work is pending.** If the caller asks for something else while a request is running, the new request is saved and queued behind it. A completed result survives later interruptions and is spoken at the next quiet boundary.
- **Results survive hangup.** Hanging up ends the voice session but does not cancel work Primary already accepted. Closeout recovers uncertain submissions and keeps the binding open until transcript captures and question replies are resolved. Texting a requested result after the call (`POST /internal/broker/result-sms`) is implemented but off by default; sending requires an approved US A2P 10DLC messaging campaign (see [docs/sms.md](docs/sms.md)).
- **Signed webhooks and caller possession.** Every Twilio webhook and media WebSocket is checked against the Twilio signature for its exact URL. Caller ID alone never opens an account: incoming calls are admitted by the Dexter API through the same-call attachment flow, or, in callback mode, the broker confirms the call with a fresh Twilio read and calls the saved number back.
- **Audio preflight before dialing.** Before dialing an account call (operator-arranged, callback, saved-watch or notification), the broker checks account readiness with the API and opens a short GPT-Live session that must produce real output audio. If either fails, no call is placed. Phone-setup verification callbacks are dialed only under a permit issued by the Dexter API.

## Quick start

Requires Node.js 22 (the test script relies on Node 22's `--test` glob expansion).

```sh
npm ci
cp .env.example .env     # fill in the required values
npm run dev              # tsx watch src/broker/main.ts
```

Compiled:

```sh
npm run build            # emits dist/
npm start                # node dist/broker/main.js
```

The broker listens on `BROKER_HOST:BROKER_PORT` (default `127.0.0.1:3213`) and serves Twilio under `/broker/`. Expose it through an HTTPS reverse proxy at `BROKER_PUBLIC_ORIGIN` (see `deploy/nginx-broker.conf`), point the Twilio number's voice webhook at `/broker/incoming-call` and its status callback at `/broker/status`. A working call also needs a reachable Dexter API that accepts the broker's `PHONE_SERVICE_SECRET`.

Operator CLI (talks to the running broker on loopback):

```sh
npm run broker -- status
npm run broker -- preflight     # one short GPT-Live session; consumes provider credits
npm run broker -- call --request-id <uuid> --purpose "<why Dexter is calling>" --receipt <new-file>
```

`call` dials a real telephone. See [docs/BROKER_OPERATIONS.md](docs/BROKER_OPERATIONS.md) for deployment, incoming-call modes, recovery and the health endpoint.

## Configuration

`src/broker/main.ts` loads the file named by `BROKER_ENV_FILE` (default `.env`); values already in the process environment take precedence.

Required:

| Variable | Meaning |
| --- | --- |
| `BROKER_PUBLIC_ORIGIN` | HTTPS origin Twilio reaches the broker at, with no path |
| `BROKER_STATE_DIR` | Absolute directory outside the source tree for the call journal |
| `BROKER_CONTROL_TOKEN` | At least 32 characters; bearer token for the operator routes |
| `OPENAI_API_KEY` | OpenAI key with GPT-Live access |
| `TWILIO_ACCOUNT_SID` | Twilio account that owns the phone number |
| `TWILIO_AUTH_TOKEN` | Twilio auth token; also validates webhook signatures |
| `TWILIO_PHONE_NUMBER` | E.164 number the broker calls from and answers on |
| `DEXTER_API_URL` | Dexter API origin; plain HTTP is accepted only for loopback |
| `PHONE_SERVICE_SECRET` | At least 32 characters; shared with the Dexter API to authenticate broker requests |

Optional:

| Variable | Default | Meaning |
| --- | --- | --- |
| `BROKER_ENV_FILE` | `.env` | Environment file loaded at startup and by the CLI |
| `BROKER_HOST` | `127.0.0.1` | Listen address |
| `BROKER_PORT` | `3213` | Listen port (1024-65535) |
| `BROKER_LIVE_MODEL` | `gpt-live-1` | GPT-Live model |
| `BROKER_LIVE_VOICE` | `marin` | GPT-Live voice |
| `BROKER_MAX_CALL_SECONDS` | `1800` | Call length limit (30-14400) |
| `BROKER_ALLOWED_TO` | empty | Comma-separated E.164 numbers that operator and callback calls may dial; must not include `TWILIO_PHONE_NUMBER`. With one entry, the CLI uses it by default |
| `BROKER_REVISION` | `development` | Release identifier reported by `GET /broker/health` |
| `BROKER_VERIFICATION_SETUP_MODEL` | empty | GPT-Live model for the restricted phone-setup voice. Setting it enables same-call incoming attachment; empty selects the press-1 callback offer. Requires a loopback HTTP API origin and `HARK_PHONE_CALLBACK_SIGNING_SECRET` |
| `BROKER_HARK_API_URL` | empty | Loopback HTTP origin of the Dexter API; enables calls from saved watches and notifications. Requires `HARK_PHONE_CALLBACK_SIGNING_SECRET` and `DEXTER_INTERNAL_TOKEN` |
| `HARK_PHONE_CALLBACK_SIGNING_SECRET` | empty | At least 32 characters; signs callbacks to the Dexter API for watch calls and phone setup |
| `DEXTER_INTERNAL_TOKEN` | empty | Authenticates the notification producer on `POST /internal/outbound-alerts` (at most 4096 bytes) |
| `OUTBOUND_ALERT_SIGNING_SECRET` | empty | Read only to ensure `HARK_PHONE_CALLBACK_SIGNING_SECRET` is a different key |

SMS (implemented, off by default; the bridge loads only when a `DEXTER_SMS_*` variable is set, and sending requires an approved US A2P 10DLC campaign; see [docs/sms.md](docs/sms.md)):

| Variable | Default | Meaning |
| --- | --- | --- |
| `DEXTER_SMS_STATE_PATH` | none | Absolute path to the SMS ledger, outside the release directory and temporary directories |
| `DEXTER_SMS_INTERNAL_SECRET` | none | At least 32 characters; authenticates the enrollment proxy |
| `DEXTER_PHONE_PUBLIC_URL` | none | HTTPS origin for SMS webhooks and links |
| `TWILIO_VERIFY_SID` | none | Twilio Verify service SID (`VA...`) used for enrollment codes |
| `TWILIO_VERIFY_ACCOUNT_SID` | `TWILIO_ACCOUNT_SID` | Account for Twilio Verify |
| `TWILIO_VERIFY_AUTH_TOKEN` | `TWILIO_AUTH_TOKEN` | Auth token for Twilio Verify |
| `TWILIO_MESSAGING_SERVICE_SID` | empty | Messaging service used to send |
| `DEXTER_SMS_ENROLLMENT_ENABLED` | `false` | Accept web enrollment |
| `DEXTER_SMS_SEND_ENABLED` | `false` | Allow sending |
| `DEXTER_SMS_CAMPAIGN_APPROVED` | `false` | Operator assertion that the messaging campaign is approved |
| `DEXTER_SMS_ADVANCED_OPT_OUT_CONFIGURED` | `false` | Operator assertion that Twilio Advanced Opt Out is configured |
| `DEXTER_SMS_CONSENT_RETENTION_DAYS` | `1461` | Consent record retention |
| `DEXTER_SMS_MESSAGE_RETENTION_DAYS` | `90` | Message record retention |
| `TWILIO_VALIDATE_WEBHOOKS` | `true` | Setting `false` disables SMS sending |

## Tests

```sh
npm run typecheck
npm test
```

Tests use Node's built-in runner through `tsx`, with local HTTP servers and simulated Twilio, GPT-Live and Dexter API peers. They place no calls and need no credentials. The SMS tests create a temporary ledger under `~/.local/state`, because the ledger validator refuses temporary directories.

`scripts/qualify-live-phone.mts` runs the broker against real GPT-Live with simulated telephony and account results; it needs an OpenAI key and consumes credits. See [docs/LIVE_PROTOCOL.md](docs/LIVE_PROTOCOL.md).

## Documentation

- [docs/BROKER_OPERATIONS.md](docs/BROKER_OPERATIONS.md): deployment, incoming calls, controlled calls, recovery, health
- [docs/inbound-call-attachments.md](docs/inbound-call-attachments.md): keeping an incoming caller on the line during account admission
- [docs/HARK.md](docs/HARK.md) and [docs/NOTIFICATIONS.md](docs/NOTIFICATIONS.md): calls from saved watches and notifications
- [docs/PHONE_RESULT_CONTINUITY.md](docs/PHONE_RESULT_CONTINUITY.md): partial results, provider pauses and voice replacement
- [docs/INBOUND_BILLING.md](docs/INBOUND_BILLING.md): incoming call outcomes and billing evidence
- [docs/LIVE_PROTOCOL.md](docs/LIVE_PROTOCOL.md): GPT-Live event contract
- [docs/sms.md](docs/sms.md): SMS enrollment, consent and result texts (off by default)

## License

[MIT](LICENSE)

---

<p align="center">
  <a href="https://dexter.cash">Dexter</a> ·
  <a href="https://docs.dexter.cash/docs">Docs</a> ·
  <a href="https://github.com/Dexter-DAO/opendexter-ide">OpenDexter</a>
</p>
