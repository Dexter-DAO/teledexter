# Broker operations

The broker connects Twilio telephone audio to GPT-Live and delegates account work to Primary Dexter. It runs as one process with a durable call journal. Primary owns account authority and accepted requests.

## Release state

As of September 22, 2026, release `10c8115` is running on port `3213` with callback and question continuation. Its upgrade passed acceptance at 01:00 UTC, including the actual running code paths, preserved journal, public health route, and stability through a maintenance interval. All 274 Broker tests, source TypeScript checks, and the exact package build pass. Shared API release `d781d4f` passed runtime acceptance at 02:06 UTC with Phone ingress, same-call question replies, transcript capture, and local memory support. Provider-backed memory recall remains disabled. The most recent GPT-Live preflight was blocked by exhausted credits; there is no completed call proving the combined path, and the existing number has not been rerouted to the broker.

Angel applied the nginx location for `/broker/` after Wallet acceptance. The existing service on port `3212` and the Twilio number's current inbound configuration remain in place. Record the exact source, installed package, running process, provider configuration, and completed call when each is verified.

The deployed Primary ingress permits verified outbound calls. It rejects inbound account binding with `caller_possession_required`, because caller ID alone does not establish possession. The inbound handler therefore offers a callback without opening Primary, reading account data, or starting GPT-Live. The outbound leg establishes possession of the current saved destination.

The client accepts `queued`, `running`, `waiting_for_reply`, `completed`, `failed`, and `cancelled`. A waiting result carries its saved question. Completion requires the identified terminal assistant output; an earlier question is not a completed result. Hangup ends voice delivery and closes admission after source recovery; it does not cancel work already accepted by Primary. Later transcript corrections are recorded as source revisions and do not retroactively change an admitted action.

## Questions during a call

When Primary asks a question, the broker passes its wording, context, proposed answer, and the meaning of each choice to Live. It waits for those updates to be accepted and a subsequent completed caller turn before answering. Provider acceptance of those instructions does not prove the caller heard them; audible delivery remains part of the call trial.

Literal yes and no answers, including ordinary trailing punctuation, select the corresponding saved choice. Qualified answers retain the caller's words as `other`. The broker saves the answer and its completed source turn before sending it to the exact original request and question. The API joins the answer, transcript source, and continuation of the same run in one transaction.

If the response is lost, recovery reads that exact question and checks the saved reply identity and content. It can still recover the old answer after Primary has asked a second question. A changed reply, expired question, or answer already supplied elsewhere cannot become a new request. Closeout keeps unresolved reply recovery pending with the original binding.

This release supports answers within the original verified call. Attaching a later verified callback to an earlier question is proposed separately. Until that integration is qualified, a different call or thread cannot reuse the original request's answer authority.

## Configuration

`src/broker/main.ts` and the operator CLI load the file named by `BROKER_ENV_FILE`, falling back to `.env`. Existing process environment values take precedence. Keep production credentials outside releases in `/var/lib/dexter-broker/config/production.env`, readable only by the service owner. Load the file through the application; shell-sourcing it is unnecessary.

| Variable | Required value or default |
| --- | --- |
| `BROKER_HOST` | Deployment pins `127.0.0.1`. |
| `BROKER_PORT` | Deployment pins `3213`. |
| `BROKER_PUBLIC_ORIGIN` | HTTPS origin only, normally `https://phone.dexter.cash`; no `/broker` suffix. |
| `BROKER_STATE_DIR` | Absolute directory outside the checkout or release. Deployment uses `/var/lib/dexter-broker/state`. |
| `BROKER_CONTROL_TOKEN` | At least 32 characters; authenticates local operator routes. |
| `OPENAI_API_KEY` | Provider key with GPT-Live access and available credits. |
| `BROKER_LIVE_MODEL` | `gpt-live-1`. Keep this value for the implemented voice transport. |
| `BROKER_LIVE_VOICE` | Defaults to `marin`. |
| `BROKER_MAX_CALL_SECONDS` | Defaults to `1800`; accepted values are `30` through `14400`. |
| `BROKER_ALLOWED_TO` | Comma-separated E.164 outbound destinations. One entry becomes the default destination. Empty permits no outbound calls. |
| `TWILIO_ACCOUNT_SID` | Account owning the caller number. |
| `TWILIO_AUTH_TOKEN` | Validates callbacks and authenticates the Twilio client. |
| `TWILIO_PHONE_NUMBER` | E.164 caller number; must differ from allowed destinations. |
| `DEXTER_API_URL` | API base URL, normally `https://api.dexter.cash`. HTTPS is required except for loopback. |
| `PHONE_SERVICE_SECRET` | At least 32 characters; must match the API's phone callback signing secret. |
| `BROKER_REVISION` | Release identifier exposed by the health endpoint. The PM2 configuration defaults to the release directory name. |

The Primary client uses `/internal/primary-dexter/channels/phone`. Its bearer token is the hexadecimal HMAC-SHA256 of `dexter-broker/primary-ingress/v1` under `PHONE_SERVICE_SECRET`. The API uses `HARK_PHONE_CALLBACK_SIGNING_SECRET` for the matching value. Never put either the secret or the derived bearer in commands, receipts, or documentation.

The private journal contains phone numbers, transcripts, and account context. Its directory is created with mode `0700`; the writer lock uses `0600`. Preserve the directory and its ownership across releases. The operator call list returns only the recipient's last four digits, but purpose and failure text can still contain personal information.

## Isolated deployment

Prepare an immutable directory under `/var/lib/dexter-broker/releases/<release-id>` containing the reviewed source, lockfile, compiled `dist`, and installed dependencies. Build and validate that exact release before starting it:

```sh
npm ci
npm run typecheck
npm test
npm run build
```

The release includes `tsx` because `npm run broker` uses the operator script. `npm start` itself runs compiled JavaScript. Record the source revision and build checks with the release receipt.

The PM2 configuration requires `BROKER_RELEASE_DIR` to name the actual immutable directory. It rejects symbolic-link release paths. Set it in the operator environment, then use the configuration from that release:

```sh
pm2 start "$BROKER_RELEASE_DIR/deploy/ecosystem.config.cjs" --only dexter-broker
```

Use this command only when applying the reviewed deployment. The configuration starts one forked process and gives shutdown 65 seconds; the application has a 60-second shutdown deadline. The journal permits one writer, so upgrades require the previous broker process to stop before the replacement starts. Avoid a rolling overlap against the same state directory. A rollback selects the previous immutable release and retains the same journal; verify journal compatibility first.

On this host, PM2 6.0.5 retains an existing process's normalized script and working-directory paths during `startOrRestart`. For an upgrade to another release directory, verify and stop the exact broker process, confirm its exit and writer release, then remove only its stopped PM2 definition before starting the new configuration. Check the new process's actual paths and code hashes along with health. Record the newly allocated PM2 ID for shared release coordination.

The initial `fb3553f` release can read the journal's JSON shape but cannot recover the successor's pending callbacks or question replies. After the successor handles that work, use a release that understands those records for recovery. Preserve the current journal; restoring an earlier snapshot could erase evidence of an accepted answer or an outbound call.

`deploy/nginx-broker.conf` is the accepted location snippet for the existing HTTPS `server` block for `phone.dexter.cash`. Its `proxy_pass` preserves the `/broker/` prefix and supports WebSocket upgrades. It exposes no `/internal/broker/` routes. Future changes go through Angel's shared nginx release: preserve existing locations, check the full configuration with `nginx -t`, then reload.

`GET /broker/health` reports process readiness, model, revision, and active call count. A successful health response establishes HTTP reachability. It does not establish provider credits, Primary eligibility, working audio, or a completed call.

Outbound call creation sets its own answer and status callbacks under `/broker/answer/:id` and `/broker/status/:id`; it can use the existing sender without rerouting the number. Inbound activation requires the callback source to be released and the number to be configured explicitly: voice POST to `/broker/incoming-call` and status POST to `/broker/status`. The Gather action posts to `/broker/callback/:id`. The existing number configuration remains unchanged by this preparation.

## Inbound callback

The inbound response offers one DTMF choice: press 1 to request a callback. The offer and acknowledgment use the same wording whether the caller's account is ready or unavailable. The inbound leg supplies no account context to the caller or a voice model.

Consent requires the signed Twilio POST for the exact inbound call, caller number, owned destination, and provider account. A fresh Twilio read must confirm those identities and an active call before consent is saved. This read has a four-second deadline. A matching caller ID alone never opens Primary.

The consent record stores a stable request UUID derived from the provider account and inbound CallSid, its timestamp, and state in the existing call journal. The HTTP response says the person can hang up and ends the inbound leg. Maintenance checks pending callbacks every 10 seconds. It fetches the exact inbound call again and requires provider-confirmed termination before invoking the existing outbound executor.

For the initial trial, the destination must be in `BROKER_ALLOWED_TO`. The shared outbound executor then asks the API to confirm that exact number remains a verified saved destination with ready Primary authority. Only after that check does it run the mandatory GPT-Live audio preflight and dial. A failed readiness check prevents preflight and dialing.

Consent expires after five minutes. The callback helper admits at most three requests per number per hour, with at least 60 seconds between admissions; these limits survive process restart. They apply to inbound callback requests. Exact webhook retries retain the original consent and request ID. An existing outbound request is recovered even when its outcome is uncertain, so a timeout or restart cannot produce a second dial.

The operator call list includes `callback.state`: `requested` means consent is waiting for dispatch, `started` means an outbound request is already saved, and `unavailable` means this callback will receive no further dispatch attempt. `started` does not prove that the phone rang or a person answered; inspect the associated outbound call status and provider evidence. If readiness or credits were unavailable, the caller can make a fresh request after the underlying issue is resolved and the callback limits allow it.

## Controlled call

Run the CLI from the release with `BROKER_ENV_FILE` pointing to its private configuration. The CLI connects to loopback and supplies the control token internally.

```sh
npm run broker -- status
```

After provider credits and the Primary ingress release are available, an authorized operator can run a provider preflight:

```sh
npm run broker -- preflight
```

Preflight opens one GPT-Live session for at most 15 seconds. Success requires actual output audio and the matching final `session.closed` event. It makes no telephone call. It has no retry or reconnect loop.

For an authorized test call, save one UUID as `BROKER_REQUEST_ID` before dispatch. Keep it with the purpose and receipt. Use the same ID if the local HTTP response is lost:

```sh
npm run broker -- call --request-id "$BROKER_REQUEST_ID" --purpose 'An authorized test of our Dexter phone conversation.' --receipt "$BROKER_RECEIPT_PATH"
```

The receipt path must be a new private file. With one allowed destination, the CLI needs no number in the command. Multiple destinations require `--to` with an allowed E.164 number. The service first saves the request, checks account readiness, and runs another mandatory audio preflight before dispatching to Twilio. Repeating the same request returns the saved call instead of dialing again. A different request to a person with an unresolved call is blocked.

Confirm the saved provider SID, audible conversation, and an identified Primary result. Check both provider termination and local closeout afterward. A financial-action claim additionally needs the corresponding Primary result and transaction or receipt evidence. A spoken acknowledgment alone is insufficient.

## Close and recovery

These commands use the broker call UUID returned by `status`, not the Twilio SID:

```sh
npm run broker -- end --id "$BROKER_CALL_ID"
npm run broker -- reconcile --id "$BROKER_CALL_ID"
```

`end` stops the active call or requests provider completion for its saved SID. `reconcile` fetches that exact SID, checks the direction and both numbers, and updates local status. Neither command creates a call. Reconciliation requires a saved provider SID.

Before closing Primary admission, the service marks `primaryClosePending: true` in `calls.json`. It restores saved delegation and source journals, reads uncertain request admission using the original request ID, and replays pending source captures with their original IDs, revision, text, and timestamp. An uncertain submission is recovered through result GETs; it is never dispatched again by closeout. Pending capture keeps the Primary binding open.

Once pending captures are acknowledged, the service closes the same binding. A failed close leaves `primaryClosePending` set. Maintenance retries pending recovery every 10 seconds while the process accepts work, including records loaded after restart. Explicit `reconcile` also attempts closeout for terminal calls or a pending binding. Inspect `primaryClosePending` and `transcriptCapture` in `status`; an HTTP success from reconciliation can still carry pending work.

`transcriptCapture.state: complete` means no eligible journaled captures remain pending. Its `unsupported` field can still show omitted turns. Caller turns require acoustic completion evidence. Assistant capture requires confirmed playback and session finalization. Interrupted, discarded, or unconfirmed speech stays unqualified; it cannot become proof that the person heard a result.

A call with status `unknown` and no provider SID needs operator investigation of the original dispatch. Preserve the request ID and journal, establish the provider outcome, and resolve that record before attempting another call. Generating a new request ID to bypass uncertainty risks a duplicate call. Do not delete the writer lock while its owning process is alive.

Maintenance also reconciles inactive, unresolved calls older than 60 seconds by saved SID and ends a provider call that is still open. During shutdown the service drains HTTP requests, stops its active calls, and awaits closeout before releasing the journal lock. A forced exit leaves recovery evidence on disk for the next process.
