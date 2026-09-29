# Call purpose and follow-up requests

This source change connects an arranged outbound call to the task Dexter was asked to perform. It also retains a later caller request while earlier work is running. Deployment requires the matching Phone task-context API. The current customer preview has not yet qualified these changes.

An authenticated operator call saves its purpose before dialing. Once the answered call is verified, Broker registers that saved assignment with Primary and retains the returned reference. The introduction explains the task. The caller's first response can be an ordinary greeting, a correction, or a refusal; Primary receives the actual words alongside the separately attributed assignment. The shared API records which accepted request first started the assignment, so another greeting does not automatically restart it.

New operator calls require a lowercase request UUID and a purpose of at most 1,500 UTF-16 code units. The route trims surrounding whitespace and rejects NUL and unpaired surrogate characters before saving or dialing. Emoji are supported. Exact retries of older saved calls return their original records without rewriting their IDs or purpose.

Later tasks use new Live delegations. Broker saves each completed caller turn and its request identity before submitting it. While a request is pending, subsequent delegated tasks enter the call's queue. Broker retains the current result before promoting the next queued task. Speech for an older result yields to a newer caller request. A lost submission response is recovered through the original request's result lookup. Hanging up leaves unsubmitted queued work saved.

While media remains connected, the result observer continues beyond the initial polling window. It stops when media closes. Questions retain their original presentation and answer association while that observer runs.

Caller text and the delegation position use Live's session timeline. Local audio activity establishes that audio arrived and became quiet; sound bursts do not identify transcript turns. Broker selects the phrase whose first fragment was present at the handoff or began before its position. It preserves the whole stable phrase, including words that finish afterward. A distinct later phrase needs its own delegation. Saved source identities and request contents survive late captions and restarts unchanged.

The journal records how much local audio had arrived when each delegation began. This observation is separate from the transcript-derived occurrence time; it does not claim exact acoustic onset or assign audio exclusively to one request. Queued requests retain that observation from their own delegation, even when another request takes time to finish.

During a quiet interval, Broker records which caller text had stabilized. These local checkpoints let final transcript capture retain earlier completed speech if the call ends with a click or an unfinished new sentence. A checkpoint uploads no memory and applies only to its exact recorded text. Finalized late captions may still qualify against audio that was already quiet before disconnect. Waiting after hangup cannot finish an interrupted sentence.

Two optional references accompany the unchanged caller text:

- `callAssignmentRef` identifies the task registered for this call.
- `relatedPrimaryRequestId` with `referenceOrigin: broker_status_publication` identifies a previous request whose spoken-output command was accepted before this caller turn. It establishes command acceptance, not hearing or consent.

Both references are frozen when the caller turn enters the queue. Later playback or connection events do not rewrite an already saved submission. Old calls without an assignment retain their original format. Callbacks remain unassigned; they do not inherit an operator's task.

The voice layer uses prior messages for historical recall. Current account answers need a current result and its observation context. The shared Primary agent and tools remain responsible for obtaining the requested information and interpreting search coverage.

## Pending work and result timing

Fixed waiting notices refresh and validate the call's eligible context. When the owner, binding, context epoch and complete eligible content remain identical, an advancing work-event cursor can retain the same voice session. The original generation version stays unchanged. Each accepted context refresh has separate append-only audit evidence. A changed epoch, message, display name, local context or omission boundary takes the existing replacement path; revoked or unavailable authority keeps output blocked.

The call-wide caller transcript and observed speech frontier are captured before result-context I/O. Another caller turn during that I/O prevents publication from the returned result, including when the replacement voice has an empty transcript. Pending notices also yield to later caller speech. Result presentation waits for an idle carrier and 800 milliseconds without observed speech or output. Disconnected or inconsistent playback receipts refuse presentation. The wait lasts at most five seconds; after that, the result remains saved and undelivered. No new financial request is created by a delivery refusal.

A genuine eligible-context change still replaces the voice connection. Casual conversation that has not entered eligible history is absent from the replacement prompt. This change reduces progress-driven replacements and prevents stale interruptions; it does not establish complete conversational continuity. A deferred result has no automatic later announcement after its observer finishes. A later explicit status request can retrieve saved work through the existing request path.

A failed result with `primary_financial_result_unknown` stays failed. The voice explains that the same trade request needs checking before continuing. Trade instructions distinguish order preparation, the required confirmation and an evidenced execution outcome.

## Conversation qualification

`scripts/qualify-live-phone.mts --scenario followup` runs the Broker server and GPT-Live with simulated telephony and account results. It requires separate caller audio files through `--input-audio` and `--followup-audio` and accepts a task brief through `--call-purpose-file`. It consumes OpenAI credits and does not dial a customer.

The first result is held until a second spoken task produces an actual Live delegation and a saved queue entry. Evidence includes both raw caller submissions, their order, returned answers, transcript timing and generated audio. The report keeps its semantic verdict unset for review. A successful rehearsal still leaves customer-call and shared account-data qualification outstanding.
