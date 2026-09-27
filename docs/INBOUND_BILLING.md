# Incoming call outcomes

An incoming account-bound call can carry an optional billing reference from the shared API. Broker freezes that reference at admission and retains it through context reads, restart and closeout. Historical calls without a reference never acquire one later.

After termination, Broker fetches the exact saved provider call. Completed billing evidence requires matching account, call SID, participants and direction, plus consistent provider start, end and duration. Missing or inconsistent evidence stays pending for another read. Callback arrival and local close timestamps never substitute for provider duration.

The journal saves the immutable signed callback body before delivery. A lost acknowledgement or restart replays that body. Completed calls use `primary_inbound_v1`; failed, busy, unanswered and canceled calls use `primary_inbound_status_v1`. The latter reports preserve nullable provider timing and create no usage event. Both acknowledgement forms must match the saved call identity.

First Live readiness, first audio sent and first confirmed playback are also retained on the call. These observations separate voice startup from audio generation and playback. Delegation records retain submission timing for diagnosis.

## Integration

Apply the API's reviewed incoming-billing schema before activating the API writer. Upgrade Broker before the new API: the previous strict response parser rejects the optional billing reference. This Broker accepts responses from the old API and preserves their historical meaning.

The existing journal remains at the configured state directory. Drain calls before replacement. Once new billing journals exist, retain a compatible reader during rollback or forward repair.

The normal inbound HTTP/media suite and API PostgreSQL fixture exercise the connected path. The fixture in `tests/fixtures/inbound-billing-journey.ts` uses actual Broker routes, persistence and signed callbacks, with simulated provider/voice connections. It supports missing timing, terminal failures and acknowledgement loss. Root integration tests cover actual API admission, receipt SQL, history and concurrent outcome conflicts. Financial settlement is replaced by a local test ledger in that fixture.

The complete Broker suite passes 1,533 tests with 11 optional tests skipped. These tests establish source behavior under the supplied inputs. A controlled telephone call still supplies the provider, listening and payment evidence.
