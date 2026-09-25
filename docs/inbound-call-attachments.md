# Keep an incoming call connected during account approval

This source adds an incoming-call attachment to Broker. A caller stays on the original telephone leg while the shared API waits for account approval. Once that approval is consumed, Broker obtains the person's Primary Dexter context and uses the existing voice conversation path.

The account approval surface is still awaiting a product choice. Production startup does not select the new adapter, and this change does not move the telephone number. Existing callback calls retain their saved behavior.

## Integration

`createBrokerServer` accepts an optional `inboundAttachment.waitingResponse` function. The approved public adapter supplies the instructions for the caller and redirects the same call to the supplied continuation URL. It receives the call ID, attachment ID when known, and the API's issued expiry when known. It cannot supply an owner, grant or approval to Broker.

The shared API owns approval and verifies the signed original incoming call. Broker stores that original proof before sending begin. Subsequent Twilio callbacks must match the saved caller, called number, provider account and CallSid. Continuation callbacks never replace the original proof.

The new continuation route is `POST /broker/inbound/:callId/continue`. It requires the exact Twilio signature. The existing incoming route selects the adapter only for new calls. Existing callback and Hark records keep their original identities.

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

A binding receipt does not prove media started. Broker checks the fixed grace, local stop, provider terminal state and prior media start before returning stream instructions, accepting the socket and processing its start frame. A session that already started cannot be recreated after a crash through attachment recovery.

Normal shutdown records stop intent before waiting for pending operations. It includes calls restored from the journal, even when this process received none of their webhooks. A planned process replacement therefore ends waiting calls; it does not promise seamless telephone continuity across that replacement.

## Closeout

Voice finalization, completed transcript capture, accepted request recovery and binding closure retain their existing order. The inbound closure receipt can be saved without inventing a private context record. Completed closure does not erase outstanding request reconciliation or transcript work; maintenance continues any pending closeout.

The provider leg, media playback, transcript capture, account binding and financial results have separate evidence. This adapter creates no grant and submits no task merely because approval or a binding exists.

## Qualification

Focused tests cover the original signed incoming leg through waiting, approval, media startup and closure. They exercise lost begin and consume replies, cancellation races, authority loss, fixed deadlines, restart, shutdown, stale callbacks and compatibility with existing calls. HTTP and WebSocket tests use local provider doubles; they establish Broker behavior under those inputs.

Public approval UX, combined shared-API qualification, deployment and an actual incoming telephone conversation remain separate acceptance steps.
