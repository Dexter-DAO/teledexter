# Keep an incoming call connected during account approval

The incoming-call attachment keeps a caller on the original telephone leg while the Dexter API decides whether the call may attach to an account. Once that approval is consumed, the broker obtains the person's Primary Dexter context and uses the normal voice conversation path. Callers without a linked account are routed to the restricted phone-setup voice instead.

`src/broker/main.ts` selects this adapter (`createInboundHoldAdapter`) when `BROKER_VERIFICATION_SETUP_MODEL` is set. Without it, incoming calls receive the press-1 callback offer described in [BROKER_OPERATIONS.md](BROKER_OPERATIONS.md#incoming-calls).

## Integration

`createBrokerServer` accepts an optional `inboundAttachment.waitingResponse` function for the first leg. The public adapter (`src/broker/inboundHold.ts`) returns a bare redirect of the same call to the supplied continuation URL. A redirect does not pick up, so the caller hears ringing until the continuation answers (Twilio posted it 376-449 ms after the first webhook on calls 28a563f6 and d1b57e33). The adapter receives the call ID, attachment ID when known, and the API's issued expiry when known. It cannot supply an owner, grant or approval to Broker.

The shared API owns approval and verifies the signed original incoming call. Broker stores that original proof before sending begin. Subsequent Twilio callbacks must match the saved caller, called number, provider account and CallSid. Continuation callbacks never replace the original proof.

The continuation route is `POST /broker/inbound/:callId/continue`. It requires the exact Twilio signature. The existing incoming route selects the adapter only for new calls. Existing callback and Hark records keep their original identities.

The continuation saves its signed proof as ordering evidence and answers at once with `<Connect><Stream>` followed by a signed redirect back to the continuation. Admission (begin, recovery, provider check, consumption and context) starts once that response is delivered. The broker's own provider check (a Twilio read of the exact leg) runs alongside the consume, which makes the same active-call check in the API; the context is recorded, and Live can attach, only when both pass. A leg that ended meanwhile closes any binding the consume made. The stream response also opens the Live WebSocket (only the socket: no session, history or audio), so Live's connection is ready when admission binds; adoption still requires the bound admission and, once approved, that exact approval. The API's begin requires an answered leg, so no admission contact happens before the continuation. The stream plays the pre-rendered greeting and holds caller audio spoken after it. Live attaches with the account context only after admission is bound and its context loaded. While the stream waits, the broker polls the same admission after 100, 200 and 400 ms, then every second, so a begin refused because Twilio had not yet picked up the leg is retried within about 0.1 s. If admission ends without a binding (refusal, cancellation or expiry), the broker closes the stream after the greeting. The trailing redirect then returns the call to this route, which answers with restricted setup or a hangup. A redirect that arrives after account media started (the stream dropped in a broker crash or restart) is answered with a hangup. See [BROKER_OPERATIONS.md](BROKER_OPERATIONS.md#greeting-and-media-start).

The client implements the shared v1/v2/v3 contract:

- Begin, recover and cancel use the saved begin identity.
- Status returns the same attachment and its current state.
- The inbound arm of `/calls` consumes the approved attachment and returns guarded account context.
- Closure uses the exact attachment, original begin, approval reference and consumed binding. Its receipt records the API's committed `closedAt` even if account access has since changed.

The API's begin digest comes from its frozen ordered tuple and literal fixture. The journal also retains a separate digest of its original envelope, including the signature. These serve different purposes: shared identity comparison and preservation of the original locally saved request.

## Waiting and recovery

Each reservation freezes policy version 1: a 120-second local waiting budget and a 30-second media-start grace. The waiting budget starts when Broker saves the incoming call. It does not choose the approval's expiry. The media grace starts at the API's persisted `consumedAt`. A retry, late response or process restart cannot extend either deadline.

A lost begin reply triggers recovery of the same begin identity. An absent lookup is only an observation; cancellation must still establish its durable fence. A lost consume reply can recover the binding without loading private context. If the caller has stopped waiting, Broker closes that recovered binding instead of starting voice.

Local stop intent is saved immediately, including while an API request is pending. The eventual API result can establish either cancellation or an already consumed binding. Those outcomes remain distinct. Provider hangup is attempted independently of failed API cleanup, so an unavailable account service cannot prolong a stopped call indefinitely.

A binding receipt does not prove media started. The greeting stream may connect before the binding exists; it carries only the constant clip and no account data. Before Live attaches, Broker checks the fixed grace, local stop, provider terminal state and prior media start. `mediaStartedAt` (the first caller sample Live receives) is never earlier than the binding's `consumedAt` and never after its grace. A session that already started cannot be recreated after a crash through attachment recovery.

Normal shutdown records stop intent before waiting for pending operations. It includes calls restored from the journal, even when this process received none of their webhooks. A planned process replacement therefore ends waiting calls; it does not promise seamless telephone continuity across that replacement.

## Closeout

Voice finalization, completed transcript capture, accepted request recovery and binding closure retain their existing order. The inbound closure receipt can be saved without inventing a private context record. Completed closure does not erase outstanding request reconciliation or transcript work; maintenance continues any pending closeout.

The provider leg, media playback, transcript capture, account binding and financial results have separate evidence. This adapter creates no grant and submits no task merely because approval or a binding exists.

## Tests

Focused tests cover the original signed incoming leg through the continuation, the greeting stream, approval, media startup and closure. They exercise lost begin and consume replies, cancellation races, authority loss, fixed deadlines, restart, shutdown, stale callbacks and compatibility with existing calls. HTTP and WebSocket tests use local provider doubles; they establish broker behavior under those inputs.
