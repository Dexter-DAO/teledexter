# Incoming call outcomes

An incoming account-bound call can carry an optional billing reference from the shared API. Broker freezes that reference at admission and retains it through context reads, restart and closeout. Historical calls without a reference never acquire one later.

After termination, Broker fetches the exact saved provider call. Completed billing evidence requires matching account, call SID, participants and direction, plus consistent provider start, end and duration. Missing or inconsistent evidence stays pending for another read. Callback arrival and local close timestamps never substitute for provider duration.

The journal saves the immutable signed callback body before delivery. A lost acknowledgement or restart replays that body. Completed calls use `primary_inbound_v1`; failed, busy, unanswered and canceled calls use `primary_inbound_status_v1`. The latter reports preserve nullable provider timing and create no usage event. Both acknowledgement forms must match the saved call identity.

First Live readiness, first audio sent and first confirmed playback are also retained on the call. These observations separate voice startup from audio generation and playback. Delegation records retain submission timing for diagnosis.

## Compatibility

The broker accepts API responses with or without the optional billing reference. Once billing journals exist, retain a compatible reader during rollback.

## Tests

The fixture in `tests/fixtures/inbound-billing-journey.ts` uses actual Broker routes, persistence and signed callbacks, with simulated provider/voice connections. It supports missing timing, terminal failures and acknowledgement loss. Financial settlement is replaced by a local test ledger in that fixture.
