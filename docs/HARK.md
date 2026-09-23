# Calls from saved watches

A saved Hark watch can ask Dexter to call when its condition is met. The call opens with the watch name, the reported event and its observation time. Primary receives the saved assignment so the caller can discuss it, request current information and continue with work allowed by their existing permissions.

This adapter is source preparation. Activation requires the matching API release, Broker configuration and coordinated routing. The existing Phone service and telephone number routing remain selected until that release is qualified.

## Assignment and delivery

The existing notification producer sends its Hark-v1 body to Broker's local `POST /internal/outbound-alerts`, using `x-dexter-internal-auth`. Broker verifies the current configured `DEXTER_INTERNAL_TOKEN` locally with exact, constant-time comparison. The header must occur once and contain at most 4096 UTF-8 bytes; its value is never trimmed, logged or saved in the call journal. Dial admission forwards that credential to the local API resolver as `x-internal-auth` with `{notification}`. The resolver route is `POST /internal/phone/resolve-hark-assignment`. The existing authorization route is `/internal/phone/authorize-outbound`.

The resolver checks the saved watch, event, delivery, owner, destination and billing agreement. It reserves the API's delivery intent and voice lifecycle before returning the assignment. That reservation carries no charge.

Broker saves the returned assignment and a digest of the complete normalized notification before dialing. For a new Hark delivery, the delivery UUID also identifies the Broker call, request and assignment. A changed notification, changed assignment on a new attempt, or collision with an operator call is refused. The journal records whether dispatch has started and whether a provider call ID has been received.

A failure before dispatch can retry after fresh API admission and account readiness checks. Beginning that dispatch preserves the previous failure in history and clears the current error. Once dispatch starts, an attempt without a saved provider call ID returns a non-success response and remains uncertain. It cannot redial. Older records without dispatch evidence keep that uncertainty. Concurrent retries also cannot cancel the original attempt or start another call.

New provider acceptance returns HTTP202 with `ok`, `intentId`, `callSid`, `state` and `replay:false`. Exact saved receipt recovery returns HTTP200 with `replay:true`. Both require a saved provider call ID. A signed callback can establish that receipt before the provider's create response arrives.

Receipt recovery verifies the current service credential and original notification digest, then returns the saved call without calling the resolver or provider again. This also works after the event expires or delivery becomes terminal. Current admission still governs any new dispatch and account access when the call is answered. Uncertain dispatch or missing historical identity returns HTTP409; failure before dispatch remains a non-success response. The API sender cannot mark an undialed failure as accepted.

At the signed answer callback, Broker gives Primary the same saved assignment. The API rechecks current admission and compares the saved source. Changes cause refusal rather than replacing the assignment during the call. The watch's originating task identifies its source; Primary continues to use its current owner-approved authority.

## Speech and evidence

The complete snapshot stays in the call journal and the API assignment. Its maximum size is 32,768 UTF-8 bytes. Hark call registration allows a 49,152-byte request to accommodate that snapshot and provider proof; ordinary requests retain their 24 KiB limit.

Live receives a brief containing the watch name, event summary and observation time. Those fields are attributed observations. Current prices and account facts come through Primary when needed. The opening explains why Dexter called and leaves room for the caller's response. Portfolio answers start with a useful summary, with detail available on request.

## Status and billing

Provider observations create durable outgoing records in the same journal update. Hark delivery status and paid-call accounting have separate acknowledgments.

| Callback | Acknowledgment |
| --- | --- |
| Hark status | Exact source, delivery and call identities in the returned receipt |
| `/internal/voice/outbound-status` | HTTP204; binds the provider call to its reserved billing lifecycle |
| `/internal/voice/call-ended` | HTTP200 with the saved voice-call receipt |

Call-ended waits for the saved outbound-status records to be acknowledged. Its owner, call identity, timestamps and duration remain unchanged on retry; each attempt receives a fresh signature nonce. A recorded voice-call receipt alone does not establish settlement or successful conversation delivery.

For completed calls, billing timing comes from the bound provider call's recorded start, end and duration. Callback arrival time and time spent offline never add billable seconds. Missing timing remains pending, and maintenance can recover it from the same completed provider call. Reservation failures and provider results without qualified timing create no invented duration or charge.

Local account or voice failures remain separate from provider termination. A call that still has an active provider connection blocks another call to that number and can still be ended explicitly.

## Configuration and checks

Set `BROKER_HARK_API_URL` to the selected local API HTTP origin and reuse `HARK_PHONE_CALLBACK_SIGNING_SECRET`. The callback key must differ from the alert ingress signing key. Hark also requires the existing API/producer `DEXTER_INTERNAL_TOKEN` in Broker's private environment. The release owner must coordinate its rotation across the producer, API and Broker. Ordinary Broker control continues to use its separate Bearer credential. The API release owner selects these values and coordinates the notification route.

Focused tests cover the actual retained notification producer over local HTTP, saved assignment registration after a signed answer, duplicate and uncertain dispatch, journal recovery, signed delivery receipts, billing order, delayed timing recovery and legacy operator behavior. Shared API tests qualify database admission separately. Full source checks and a useful customer conversation remain release requirements.
