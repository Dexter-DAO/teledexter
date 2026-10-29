# Dexter SMS

SMS enrollment verifies ownership of a phone number, then records a separate affirmative choice to receive Dexter messages. Call-requested messages use the call's recipient. Conversational texts use a separate thread for each linked phone number and are dispatched after the signed incoming webhook is acknowledged.

Production sending is disabled by default. Enrollment and general sending are controlled by separate flags. Existing Twilio Verify tools used during calls retain their separate purpose.

## Enrollment contract

The public `/sms` page calls its own server proxy. The proxy supplies `X-Dexter-Sms-Internal-Secret` and a 64-character lowercase hex `X-Dexter-Sms-Client-Id`, calculated as HMAC-SHA256 of a trusted client IP with the same dedicated secret. Accept the IP only from the site's trusted ingress. The secret and verification proof stay on the server; the browser holds a secure HttpOnly SameSite cookie through the proxy.

Every enrollment response uses `Cache-Control: no-store`. POST bodies are JSON and reject extra properties. All routes require the internal secret. Client identity is required for POST routes.

| Route | Request | Successful result |
| --- | --- | --- |
| `GET /internal/sms/config` | No body | `ok`, `enrollmentAvailable`, `deliveryActive`, `disclosureVersion` |
| `POST /internal/sms/verify/start` | `phone` in E.164 NANP format | `ok`, `challengeId`, ISO `expiresAt`, `resendAfterSeconds` |
| `POST /internal/sms/verify/check` | `challengeId`, six-digit `code` | `ok`, `proofToken`, ISO `expiresAt`, `status` |
| `POST /internal/sms/enroll` | `proofToken`, `consent: true`, current `disclosureVersion` | `ok`, `status` |
| `POST /internal/sms/status` | `proofToken` | `ok`, `status` |
| `POST /internal/sms/withdraw` | `proofToken` | `ok`, `status` |

`status` contains `enrolled`, `carrierOptedOut`, `disclosureVersion`, `updatedAt`, `deliveryActive`, and `maskedPhone`. OTP completion grants a 15-minute proof bound to the proxy's client identity. It leaves SMS enrollment unchanged. Challenges expire after ten minutes. Verification codes and raw proof tokens are excluded from the ledger.

The current disclosure version is `dexter-sms-2026-09-16`. Enrollment stores the server-authored disclosure, its version, phone number, ownership-verification timestamp, enrollment timestamp, proxy client hash, and source URL. Consent is required as the literal boolean `true`. An activated program sends one confirmation after a new web enrollment. A provider failure preserves the user's consent and the attempted-message record; it does not report delivered status.

Errors use `{ ok: false, error, retryAfterSeconds? }`. Malformed requests and wrong codes return 400, missing or expired proof returns 401, a carrier opt-out returns 409, rate limits return 429, unavailable setup returns 503, and provider verification failures return 502. Unauthorized internal calls return 404. Rate-limited responses include `Retry-After`.

OTP starts allow one request per phone per minute, three per phone per hour, ten per client per hour, and 100 across the service per hour. Each challenge permits five provider checks, with one in flight at a time. There is also a 30-check hourly client limit. Durable reservations occur before provider calls, including requests that later fail.

## Configuration

| Variable | Purpose |
| --- | --- |
| `DEXTER_SMS_INTERNAL_SECRET` | Dedicated random secret of at least 32 characters, shared only with the website proxy |
| `DEXTER_SMS_STATE_PATH` | Absolute path to the SMS ledger outside every release directory; its existing parent must be owned by the service user with restrictive permissions |
| `DEXTER_SMS_ENROLLMENT_ENABLED` | Explicit `true` allows enrollment OTP requests; default disabled |
| `DEXTER_SMS_SEND_ENABLED` | Explicit `true` permits general sends when every other delivery condition is met; default disabled |
| `DEXTER_SMS_CAMPAIGN_APPROVED` | Operator assertion after the intended campaign and number are approved and associated; default false |
| `DEXTER_SMS_ADVANCED_OPT_OUT_CONFIGURED` | Operator assertion after the required Messaging Service controls below are configured; default false |
| `DEXTER_SMS_CONSENT_RETENTION_DAYS` | Evidence retention after withdrawal; default 1461 days |
| `DEXTER_SMS_MESSAGE_RETENTION_DAYS` | Outbound metadata retention; default 90 days. Inbox entries have a fixed one-day limit |
| `TWILIO_MESSAGING_SERVICE_SID` | Approved Messaging Service used for every general send |
| `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN` | Account credentials for Messaging and its signed callbacks |
| `TWILIO_VERIFY_SID` | Verify service SID, beginning with `VA` |
| `TWILIO_VERIFY_ACCOUNT_SID`, `TWILIO_VERIFY_AUTH_TOKEN` | Optional separate Verify account credentials; otherwise the main account is used |
| `TWILIO_PHONE_NUMBER` | Expected destination number for incoming texts |
| `DEXTER_PHONE_PUBLIC_URL` | Canonical HTTPS origin for signature validation and delivery callback URLs |

`TWILIO_VALIDATE_WEBHOOKS=false` also disables general sending. SMS callbacks always require signatures. SMS setup requires explicit environment credentials and never uses source-code credential fallbacks.

Retention defaults are implementation choices that must agree with the published policy. Active consent evidence is retained while enrolled. After the configured withdrawal period, the detailed evidence expires while the minimal opt-out record remains to prevent further sends. Outbound records retain a message hash, destination, call or enrollment reference, Twilio message SID, delivery state, and provider error code. Outgoing message bodies are excluded. Incoming text bodies are cleared when a turn finishes or is suppressed. Inbox entries expire after one day; pending turns stop after ten minutes. Webhook deduplication and outbound reservations retain their 90-day history independently. Expiry runs during ledger mutations and startup.

A dedicated SMS worker owns the routes, dispatcher, provider and ledger. Cloning, JSON serialization and disk sync run on that worker, leaving the voice event loop available even when SMS disk I/O stalls. A worker failure disables SMS while voice stays available. The ledger uses atomic replacement with file and directory fsync and mode `0600`. One process owns an exclusive adjacent `.lock` file. A candidate must have its own state path; it cannot share the production file. SIGINT and SIGTERM stop SMS mutations, release the lock, and close the HTTP server. Graceful server close also releases the lock.

SIGKILL or a host crash can leave a lock behind. Startup probes the recorded positive integer PID and removes the lock only when the operating system reports `ESRCH` (no such process), then acquires a new exclusive lock. A live PID, malformed record, or permission error keeps SMS disabled. Ledger bytes are preserved. SMS initialization failure is logged as `service: sms, state: disabled` and voice continues. Other broker startup failures exit with status 1. Incomplete carrier dispatches recover as uncertain and remain excluded from automatic retries.

## Messaging Service setup

Set the approved service's incoming webhook to `<DEXTER_PHONE_PUBLIC_URL>/sms/incoming` with POST. Attach the intended number to that service. The sender supplies a per-message status callback under `/sms/status/:id`.

Enable Advanced Opt Out with the program's approved STOP, START, and HELP responses. Include STOP, STOPALL, UNSUBSCRIBE, CANCEL, END, QUIT, REVOKE, and OPTOUT; START, UNSTOP, and YES; and HELP and INFO. Twilio owns all keyword replies. The endpoint returns empty TwiML, including when `OptOutType` is absent, to avoid duplicate responses.

STOP immediately records suppression. Web enrollment cannot clear a carrier opt-out. START restores a prior subscription when its disclosure version is still current and verified enrollment evidence exists. For a new recipient or changed disclosure, START clears the carrier flag while website enrollment remains required. Use a carrier-unblock response that directs users to `/sms` to enroll or check status; a blanket subscription confirmation would describe those cases incorrectly.

Every callback validates its signature against the configured HTTPS origin and exact form fields, then binds the account and recipient. Replayed control messages are deduplicated. Delivery callbacks bind message ID, Twilio SID, and destination, and avoid moving a delivered message back to an earlier transport state. Immediate Twilio error 21610 records suppression even when a later callback never arrives.

The call tool accepts message text only. Its destination and call SID come from the call context, and the server requires active current-version consent. It adds Dexter identification and STOP/HELP instructions. A repeated call/message pair returns the recorded attempt. A transport failure stays uncertain and is never automatically retried. Limits are five messages per call, 60 per recipient per hour, and 2,000 per service per day; enrollment confirmations count toward recipient and service limits.

Ordinary incoming SMS is stored as `pending_dispatch` for recipients with current consent. The broker drains the inbox after HTTP responses finish and every two seconds, preserving message order for each phone. Pending Primary turns use the same MessageSid on every poll. A completed answer sends through `sendThreadReply(phone, messageSid, text)` with a send reservation keyed by phone and MessageSid. A restart or a changed answer cannot create a second carrier attempt. Outcomes become `replied`, `suppressed`, or `uncertain`; `replied` records a saved outbound attempt, while delivery callbacks track transport outcome separately. A crash-recovered carrier reservation remains `uncertain`. Permanent per-message API refusals are logged and suppressed so later texts can proceed; API outages retain the same pending message with durable exponential backoff, capped at six failed attempts or ten minutes. Queued/running turns also expire after ten minutes, allowing later messages from that phone to proceed. HTTP 403 and other permanent request refusals finish silently. A final `setup_required` result receives one fixed setup link, saved with the existing one-time signup marker across messages and restarts.

A never-enrolled sender receives one response with `https://dexter.cash/sms`, triggered only by an ordinary inbound text. This is the sole subscription-consent exception: the reply invites signup and records no subscription. A durable `signupReplyAttempted` flag and carrier reservation are saved together before I/O; subsequent texts remain silent across restarts and message retention. STOP and web withdrawal suppress this response. For a subscribed phone without an owner-verified SMS account binding, the same one-time signup response is sent after the API lookup. Missing principal, paused Primary authority or an archived SMS thread gets one setup response pointing to `https://dexter.cash`, using the same durable marker. HELP/START/STOP remain owned by Twilio Advanced Opt Out and produce empty application TwiML.

Primary resolves the account through `sms_account_enrollments`, created by an authenticated owner after Twilio Verify approves that exact phone. The saved account and Vault tuple must still match. Profile phone fields and incoming voice challenges grant no SMS account access. SMS uses its own per-number thread and immutable `sms_read_only` execution policy. Only the current portfolio reader and a passkey-link response are available to the model. Financial actions, delegated work and account memory writes are unavailable from SMS. Money requests direct the owner to `https://dexter.cash`, where the existing passkey session provides approval. Consent is checked before a turn and immediately before its answer is sent. Answers use the model's normal wording. One outbound filter removes phone-number-shaped sequences and URLs, domains and IP addresses outside `https://dexter.cash`, including Unicode dot variants. The remaining answer is sent with the existing Dexter prefix and STOP/HELP footer. Answers are limited to 1,000 characters before the standard Dexter prefix and STOP/HELP footer.

The new limits permit a conversation of up to one answer per minute while keeping a daily service ceiling. Rate limits reserve slots before provider I/O; carrier-uncertain sends are never retried. If the signup response reaches a rate cap, it can wait without consuming its one-time marker.

The expanded inbox states require this reader. Before rolling back to the previous broker release (`2c69df0`), remove every `DEXTER_SMS_*` variable from the old process environment, including inherited PM2 values. False send/enrollment flags still load the incompatible ledger and can prevent voice startup. With every SMS variable absent, the old bridge skips SMS and voice can listen. Preserve the complete ledger and its one-time signup markers; re-enable SMS only with a compatible reader. Restoring an older ledger snapshot can cause duplicate sends. Verify signed voice reception after rollback.

## Account phone verification

SMS consent and account access are separate. The API exposes owner-authenticated `POST /api/user/phone/sms/enrollment/start` with `{phone}`, `/check` with `{challengeId,code}`. It calls the existing control-bearer broker endpoints `/internal/broker/sms/enrollment/start` and `/check`. Only the API supplies the authenticated owner ID. The worker derives an owner-specific client ID from the existing broker control token, then reuses the SMS service's Twilio Verify provider and durable abuse limits. A different owner cannot check or recover that challenge.

Approval returns only the verified phone and timestamp. The API saves a separate owner/account/Vault binding. This flow leaves SMS consent unchanged and sends no welcome message. An approved, unexpired challenge can recover the same result after a lost response without another Verify check. Neither OTP digits nor raw proof tokens are saved. The public `/sms` signup retains its independent explicit consent requirement. The account signup page must call these authenticated endpoints before website users can create the new account binding.

## Requested results after a call

The deferred delivery transport accepts a request that the API saved during an admitted call. When sending is enabled, it can send the requested result after hangup. The API freezes the instruction, task, owner, verified number and first-send deadline; the completed task supplies one exact saved answer. The text contains an authenticated link to that answer. The model supplies neither the destination nor the link.

`POST /internal/broker/result-sms` requires the existing Broker control bearer and a strict `{v,intentId,intentDigest,messageId}` reference. Broker resolves its immutable identity through the Dexter API, then reads the SMS ledger. A saved receipt remains recoverable after expiry, STOP, account revocation or provider disablement. If no receipt exists, the API must admit a new send against current authority, verified number and result eligibility. Broker checks the deadline again after that request and enforces current consent and provider readiness before sending.

The ledger reserves the existing deterministic message ID before carrier I/O. Concurrent retries return that reservation. An interrupted dispatch recovers as uncertain; it cannot produce a second carrier attempt. A receipt created while another admission request is in flight remains recoverable even if that admission subsequently fails. Shutdown waits for admitted carrier requests and durable outcomes before releasing the ledger.

`POST /internal/broker/result-sms/eligibility` lets the API check availability before accepting a new caller instruction. It takes exactly `{bindingId,callSid,primaryDexterId,threadId}` with the same control bearer. Broker requires a current private call and uses its saved number. The check sends nothing and reserves no message or rate slot. Only NANP `+1` numbers are supported.

Recovery of saved deferred records requires a compatible API and broker reader. Existing immediate `/internal/broker/sms` requests retain their active-call requirements. Carrier approval and an enrolled recipient are still required for delivery.

## Tests

Run `npm run typecheck` and `node --import tsx --test tests/sms.test.ts`. The tests use a fake provider and cover consent, OTP abuse limits, restart persistence, signatures, recipient binding, opt-out handling, delivery state, disabled sending, and ledger failures. `scripts/sms-preview.mts` runs the actual routes on loopback port 34118 with a temporary ledger and fixed test code `123456`; it never loads credentials or contacts Twilio.

Run `node --import tsx --test tests/broker-result-sms.test.ts tests/broker-sms.test.ts tests/sms.test.ts` for the deferred HTTP transport and retained ledger. These tests cover hangup, concurrent requests, delayed admission, process exit during dispatch, restart, expiry, STOP, authority revocation, exact identity validation and shutdown.

SMS is implemented and off by default: the broker loads it only when a `DEXTER_SMS_*` variable is set, and `DEXTER_SMS_SEND_ENABLED` and `DEXTER_SMS_CAMPAIGN_APPROVED` default to false. Sending to US numbers requires an approved A2P 10DLC campaign and number association, published matching disclosures, and both flags set.
