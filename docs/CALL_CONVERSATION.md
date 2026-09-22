# Call purpose and follow-up requests

This source change connects an arranged outbound call to the task Dexter was asked to perform. It also retains a later caller request while earlier work is running. Deployment requires the matching Phone task-context API. The current customer preview has not yet qualified these changes.

An authenticated operator call saves its purpose before dialing. Once the answered call is verified, Broker registers that saved assignment with Primary and retains the returned reference. The introduction explains the task. The caller's first response can be an ordinary greeting, a correction, or a refusal; Primary receives the actual words alongside the separately attributed assignment. The shared API records which accepted request first started the assignment, so another greeting does not automatically restart it.

Later tasks use new Live delegations. Broker saves each completed caller turn and its request identity before submitting it. While a request is pending, subsequent delegated tasks enter the call's queue. The current result is delivered before the next task is submitted. A lost submission response is recovered through the original request's result lookup. Hanging up leaves unsubmitted queued work saved.

While media remains connected, the result observer continues beyond the initial polling window. It stops when media closes. Questions retain their original presentation and answer association while that observer runs.

Two optional references accompany the unchanged caller text:

- `callAssignmentRef` identifies the task registered for this call.
- `relatedPrimaryRequestId` with `referenceOrigin: broker_status_publication` identifies a previous request whose spoken-output command was accepted before this caller turn. It establishes command acceptance, not hearing or consent.

Both references are frozen when the caller turn enters the queue. Later playback or connection events do not rewrite an already saved submission. Old calls without an assignment retain their original format. Callbacks remain unassigned; they do not inherit an operator's task.

The voice layer uses prior messages for historical recall. Current account answers need a current result and its observation context. The shared Primary agent and tools remain responsible for obtaining the requested information and interpreting search coverage.

## Conversation qualification

`scripts/qualify-live-phone.mts --scenario followup` runs the Broker server and GPT-Live with simulated telephony and account results. It requires separate caller audio files through `--input-audio` and `--followup-audio` and accepts a task brief through `--call-purpose-file`. It consumes OpenAI credits and does not dial a customer.

The first result is held until a second spoken task produces an actual Live delegation and a saved queue entry. Evidence includes both raw caller submissions, their order, returned answers, transcript timing and generated audio. The report keeps its semantic verdict unset for review. A successful rehearsal still leaves customer-call and shared account-data qualification outstanding.
