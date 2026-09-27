# Saved notification calls

Broker accepts three generic sources through the existing authenticated local `/internal/outbound-alerts` route: a saved publication subscription, an owner-requested destination test, and a saved portfolio-watch event. The request carries `notificationRef: { v: 1, intentId, sourceDigest }`. Its event ID and idempotency key retain the original producer identity.

The API resolves that reference through `/internal/phone/resolve-notification-assignment`, using the reflected producer credential. It checks the current source, owner, destination, Phone mandate and tariff. Broker stores the returned assignment under `authenticated_notification_delivery`; the snapshot binds its owner, destination digest, exact source, title, summary, evidence, delivery window and billing terms. The wire headline and briefing cannot replace that snapshot. Signed answer registration checks the assignment with the API again before account context or voice admission.

Broker writes the dispatch transition before contacting the call provider. An accepted call replays its original receipt after restart, including when its source has since been revoked. An uncertain provider response retains the original dispatch identity and requires reconciliation. A new or safely undispatched call must pass current API admission again. Broker also checks the saved delivery window before and after voice preflight.

Generic calls use the existing durable outbound-status and call-ended outbox. Status callbacks retain the original event ID; call-ended retains the owner and actual provider timing. Failed, busy, unanswered or canceled calls create a terminal status without invented timing or a financial usage event. Hark case callbacks and Hark Primary receipts remain specific to saved Hark watches. Existing Hark assignment and callback bytes remain unchanged.

Focused tests use local HTTP servers and provider doubles to cover source resolution, frozen identity, registration, duplicate dispatch, lost acknowledgements, restart, due-time checks, terminal status and callback ordering. These fixtures establish the tested behavior; provider-backed call quality requires a customer call.
