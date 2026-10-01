# Call purpose and follow-up requests

This source change connects an arranged outbound call to the task Dexter was asked to perform. It also retains a later caller request while earlier work is running. Deployment requires the matching Phone task-context API. The current customer preview has not yet qualified these changes.

An authenticated operator call saves its purpose before dialing. Once the answered call is verified, Broker registers that saved assignment with Primary and retains the returned reference. The introduction explains the task. The caller's first response can be an ordinary greeting, a correction, or a refusal; Primary receives the actual words alongside the separately attributed assignment. The shared API records which accepted request first started the assignment, so another greeting does not automatically restart it.

New operator calls require a lowercase request UUID and a purpose of at most 1,500 UTF-16 code units. The route trims surrounding whitespace and rejects NUL and unpaired surrogate characters before saving or dialing. Emoji are supported. Exact retries of older saved calls return their original records without rewriting their IDs or purpose.

Later tasks use new Live delegations. Broker saves each completed caller turn and its request identity before submitting it. While a request is pending, subsequent delegated tasks enter the call's queue. Broker retains the current result before promoting the next queued task. A saved terminal result waits for a quiet pause while later queued work continues. Questions retain their own answer controls. A lost submission response is recovered through the original request's result lookup. Hanging up leaves unsubmitted queued work saved.

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

The call-wide caller transcript and observed speech frontier are captured before result-context I/O. Another caller turn during that I/O prevents publication from the returned result, including when the replacement voice has an empty transcript. Pending notices yield while the caller or carrier is active, then report the current saved request state at a quiet boundary. Later conversation does not permanently suppress them. Result presentation waits for acknowledged or cleared audible output and 800 milliseconds without observed caller or assistant speech. Exact digital-silence PCMU frames retain total byte and mark accounting without extending the speech boundary. Every nonzero output sample, including quiet speech, retains the audible playback barrier. Disconnected or inconsistent playback receipts refuse presentation. Each wait lasts at most five seconds. A source-confirmed completed, failed or cancelled result that is deferred remains saved for another presentation attempt while the call is active. Queued work gets a turn between attempts, and running-work polls can revisit the earlier result. Pending questions keep priority. Presentation reads never submit the original action again.

A genuine eligible-context change still replaces the voice connection. Casual conversation that has not entered eligible history is absent from the replacement prompt. This change reduces progress-driven replacements and prevents stale interruptions; it does not establish complete conversational continuity. Source-confirmed terminal results survive repeated caller interruptions. Completed assistant text is rechecked against current eligible history. A forgotten or unavailable source remains withheld. Failed and cancelled outcomes use fixed local wording from the matching saved Primary result after current-context validation. After hangup, a later explicit status request can retrieve saved work through the existing request path.

A failed result with `primary_financial_result_unknown` stays failed. The voice explains that the same trade request needs checking before continuing. Trade instructions distinguish order preparation, the required confirmation and an evidenced execution outcome.

## Conversation qualification

`scripts/qualify-live-phone.mts --scenario followup` runs the Broker server and GPT-Live with simulated telephony and account results. It requires separate caller audio files through `--input-audio` and `--followup-audio` and accepts a task brief through `--call-purpose-file`. It consumes OpenAI credits and does not dial a customer.

The first result is held until a second spoken task produces an actual Live delegation and a saved queue entry. Evidence includes both raw caller submissions, their order, returned answers, transcript timing and generated audio. The report keeps its semantic verdict unset for review. A successful rehearsal still leaves customer-call and shared account-data qualification outstanding.

## Completed result after caller interruptions

On September 30, 2026, a caller requested a $5 Apple purchase. The purchase finalized, but the completed answer arrived 111.7 seconds after dispatch and was marked stale because later progress questions were queued. The caller hung up 6.3 seconds later without a purchase confirmation in the generated voice transcript. The original source and financial result remain preserved.

The repair retains source-confirmed completed, failed and cancelled answers through interruptions and presents them at a quiet boundary. An answer to an earlier request receives a brief introduction. Completed assistant text retains its exact eligible wording and source binding. Failure and cancellation messages retain their fixed wording and saved result identity. Accepted answers are not published again for duplicate delegations. The voice instructions also require evidence for progress claims instead of inventing a check, timeout status or estimate.

Offline regressions cover the incident, topic changes, cancellation, repeated interruptions, queued-work progress, playback, forgotten sources, duplicate delivery and hangup. These tests qualify source behavior. Deployment and confirmation that a caller actually hears the result require separate evidence.

## Pending sale and queued requests

A later September 30 call requested a $4 Apple sale. Preparation remained pending and result reads returned HTTP 503. The caller then requested a $1 buy and asked to check again. Both later turns stayed queued, while the generated speech said it was switching orders and checking again. No sale or buy Execute occurred in the retained run.

Broker now wakes the current observer when a later request is saved in the queue. Fixed status wording states that queued work has not started. A failed result read reports that the latest status is unavailable; it makes no claim about a trade's outcome. A subsequent valid result clears that presentation state while retaining the last bounded error diagnostic.

Accepted notices consume the repetition budget. A stale or rejected notice can be retried at a later quiet boundary. An unknown append receipt prevents blind duplicate speech. The caller and playback frontiers, current saved state and eligible context are checked before publication. These presentation changes neither cancel the earlier action nor submit queued work. Hangup retains the existing block on queue promotion.

The offline incident sequence includes the sale, progress question, queued buy, status error and hangup. Separate cases cover repeated interruptions, recovery from a read error, uncertain speech receipts and delivery of terminal outcomes. These fixtures prove the application behavior under those inputs; they do not prove what a new customer call will sound like.

A composed regression also exposed starvation under continuous digital-silence output: the old boundary treated every audio frame as speech. The corrected boundary distinguishes the two PCMU zero encodings from nonzero samples. Tests keep silence streaming while a saved result waits for an audible mark and caller speech to finish. Total transport accounting and backpressure remain unchanged. The call's audio bytes were not recorded, so this fixture establishes the defect without claiming its exact contribution to that call.

Opening measurements retain the original first-frame journal timestamps. The `broker_audio_timing` log separately records the first sent frame containing a nonzero PCMU sample and the first carrier acknowledgment of such an uncleared frame. Digital silence, empty frames, cleared audio and duplicate marks do not count. These observations do not identify words or prove that the caller heard the greeting. The log leaves the journal schema unchanged so the previous release can still read it.

### Opening the voice transport during account lookup

An approved inbound call can open its initial voice-provider connection while the account context loads. The Broker first checks the saved approval and the provider's exact, active call identity. It sends the voice session and conversation history only after authenticated media arrives and the existing eligibility check passes. The post-start eligibility check still controls audio activation.

The initial connection belongs to that call. A failed connection is closed without an automatic replacement. Consume failure, revocation, admission expiry, media failure, hangup and shutdown close an unused connection. Terminal cleanup removes its saved in-memory slot. After adoption, the voice session owns cleanup, and an intentional later generation replacement uses the normal connection path.

`broker_inbound_transport` logs connection start, socket open, adoption and discard with call ID and elapsed time. These logs can measure handshake overlap against the existing account-read spans. Local composition tests establish the authorization and cleanup behavior; customer opening latency still requires a subsequent call measurement.

### Keep authorized conversation additions in the current voice session

The October 1 account call exposed five unnecessary context replacements. A new
account answer was first loaded as replacement history and then published again.
The Broker now keeps the current Live session when the API confirms that its
admitted sources remain eligible. Memory corrections, forgotten or removed
content, changed authority and closed calls still stop reuse.

Results use the original delegation channel while that session remains active.
Progress updates use quiet thinking context. An answer to earlier work has one
contextualized publication. A genuine replacement omits an unaccepted result
from startup history until its authoritative publication; previously accepted
answers remain eligible history. A replacement never addresses an old session's
delegation ID.

The admitted-source request is bounded to 128 message references and 40 memory
references. Reaching the bound requires a fresh eligible generation. The API
rechecks the existing context epoch and exact source witnesses under the current
owner, call and memory locks. Ordinary additions require the same assertion
round trips as a legacy assertion; combined admission and display-name reading
removes one further round trip from each assertion.

Deploy the compatible Broker before the API starts returning admission metadata.
The new Broker reads old call journals and older API responses. Once a journal
contains admission refresh evidence, retain this compatible Broker when rolling
the API back. Automated and composed tests establish source behavior; an actual
call remains necessary to verify what the caller hears.
