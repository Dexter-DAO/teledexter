# Dexter SMS

SMS enrollment verifies ownership of a phone number, then records a separate affirmative choice to receive Dexter messages. The initial sender attaches to an incoming phone call and uses that call's recipient. Incoming messages are stored for a future agent dispatcher. That dispatcher is still required before conversational SMS can be offered.

Production sending is disabled by default. Enrollment can be enabled for review and registration while general message delivery remains pending. Existing Twilio Verify tools used during calls retain their separate purpose.

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
| `DEXTER_SMS_MESSAGE_RETENTION_DAYS` | Outbound metadata and inbound message retention; default 90 days |
| `TWILIO_MESSAGING_SERVICE_SID` | Approved Messaging Service used for every general send |
| `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN` | Account credentials for Messaging and its signed callbacks |
| `TWILIO_VERIFY_SID` | Verify service SID, beginning with `VA` |
| `TWILIO_VERIFY_ACCOUNT_SID`, `TWILIO_VERIFY_AUTH_TOKEN` | Optional separate Verify account credentials; otherwise the main account is used |
| `TWILIO_PHONE_NUMBER` | Expected destination number for incoming texts |
| `DEXTER_PHONE_PUBLIC_URL` | Canonical HTTPS origin for signature validation and delivery callback URLs |

`TWILIO_VALIDATE_WEBHOOKS=false` also disables general sending. SMS callbacks always require signatures. SMS setup requires explicit environment credentials and never uses source-code credential fallbacks.

Retention defaults are implementation choices that must agree with the published policy. Active consent evidence is retained while enrolled. After the configured withdrawal period, the detailed evidence expires while the minimal opt-out record remains to prevent further sends. Outbound records retain a message hash, destination, call or enrollment reference, Twilio message SID, delivery state, and provider error code. Outgoing message bodies are excluded. Incoming text bodies are retained with their dispatch state. Expiry runs during ledger mutations and startup.

The ledger uses atomic replacement with file and directory fsync and mode `0600`. One process owns an exclusive adjacent `.lock` file. A candidate must have its own state path; it cannot share the production file. SIGINT and SIGTERM stop SMS mutations, release the lock, and close the HTTP server. Graceful server close also releases the lock.

SIGKILL or a host crash can leave a lock behind. Recovery requires the operator to inspect the recorded PID, verify that no process still owns or writes the ledger, and preserve the ledger bytes. Only then remove that specific stale lock and restart the SMS service. Confirm `/internal/sms/config` returns the intended readiness after restart. Never remove a lock held by a running process. Incomplete message dispatches recover as uncertain and remain excluded from automatic retries.

## Messaging Service setup

Set the approved service's incoming webhook to `https://phone.dexter.cash/sms/incoming` with POST. Attach the intended number to that service. The sender supplies a per-message status callback under `/sms/status/:id`.

Enable Advanced Opt Out with the program's approved STOP, START, and HELP responses. Include STOP, STOPALL, UNSUBSCRIBE, CANCEL, END, QUIT, REVOKE, and OPTOUT; START, UNSTOP, and YES; and HELP and INFO. Twilio owns all keyword replies. The endpoint returns empty TwiML, including when `OptOutType` is absent, to avoid duplicate responses.

STOP immediately records suppression. Web enrollment cannot clear a carrier opt-out. START restores a prior subscription when its disclosure version is still current and verified enrollment evidence exists. For a new recipient or changed disclosure, START clears the carrier flag while website enrollment remains required. Use a carrier-unblock response that directs users to `/sms` to enroll or check status; a blanket subscription confirmation would describe those cases incorrectly.

Every callback validates its signature against the configured HTTPS origin and exact form fields, then binds the account and recipient. Replayed control messages are deduplicated. Delivery callbacks bind message ID, Twilio SID, and destination, and avoid moving a delivered message back to an earlier transport state. Immediate Twilio error 21610 records suppression even when a later callback never arrives.

The call tool accepts message text only. Its destination and call SID come from the call context, and the server requires active current-version consent. It adds Dexter identification and STOP/HELP instructions. A repeated call/message pair returns the recorded attempt. A transport failure stays uncertain and is never automatically retried. Limits are five messages per call, ten per recipient per hour, and 100 per service per day; enrollment confirmations count toward recipient and service limits.

Ordinary incoming SMS is stored as `pending_dispatch` for enrolled recipients and `suppressed` otherwise. The pending state means the message awaits an agent integration. It does not mean a reply was produced or queued.

## Verification before activation

Run `npm run typecheck` and `node --import tsx --test tests/sms.test.ts`. The tests use a fake provider and cover consent, OTP abuse limits, restart persistence, signatures, recipient binding, opt-out handling, delivery state, disabled sending, and ledger failures. `scripts/sms-preview.mts` runs the actual routes on loopback port 34118 with a temporary ledger and fixed test code `123456`; it never loads credentials or contacts Twilio.

Production activation still requires the approved campaign and number association, published matching disclosures, the proxy's trusted-IP configuration, and a controlled recipient test covering delivery, STOP, START, and HELP. Repository typechecking emits no release artifact. The deployment owner must prepare an immutable Phone release and preserve its external runtime state before any cutover.
