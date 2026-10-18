# Broker operations

The broker connects Twilio telephone audio to GPT-Live and delegates account work to Primary Dexter through the Dexter API. It runs as one process with a durable call journal. Primary owns account authority and accepted requests.

## Request states

The client accepts `queued`, `running`, `waiting_for_reply`, `completed`, `failed`, and `cancelled`. A waiting result carries its saved question. Completion requires the identified terminal assistant output; an earlier question is not a completed result. Hangup ends voice delivery and closes admission after source recovery; it does not cancel work already accepted by Primary. Later transcript corrections are recorded as source revisions and do not retroactively change an admitted action.

## Questions during a call

When Primary asks a question, the broker passes its wording, context, proposed answer, and the meaning of each choice to Live. It waits for those updates to be accepted and a subsequent completed caller turn before answering. Provider acceptance of those instructions does not prove the caller heard them.

Literal yes and no answers, including ordinary trailing punctuation, select the corresponding saved choice. Qualified answers retain the caller's words as `other`. The broker saves the answer and its completed source turn before sending it to the exact original request and question. The API joins the answer, transcript source, and continuation of the same run in one transaction.

If the response is lost, recovery reads that exact question and checks the saved reply identity and content. It can still recover the old answer after Primary has asked a second question. A changed reply, expired question, or answer already supplied elsewhere cannot become a new request. Closeout keeps unresolved reply recovery pending with the original binding. Answers are accepted only within the original verified call; a different call or thread cannot reuse the original request's answer authority.

## Configuration

`src/broker/main.ts` and the operator CLI load the file named by `BROKER_ENV_FILE`, falling back to `.env`. Existing process environment values take precedence. Keep production credentials outside release directories, readable only by the service owner. The full variable list is in the [README](../README.md#configuration).

The Primary client uses `/internal/primary-dexter/channels/phone`. Its bearer token is the hexadecimal HMAC-SHA256 of `dexter-broker/primary-ingress/v1` under `PHONE_SERVICE_SECRET`. Never put either the secret or the derived bearer in commands, receipts, or documentation.

The private journal contains phone numbers, transcripts, and account context. Its directory is created with mode `0700`; the writer lock uses `0600`. Preserve the directory and its ownership across releases. The operator call list returns only the recipient's last four digits, but purpose and failure text can still contain personal information.

## Journal files

The state directory holds three kinds of journal file. Together they are the journal; reads, request deduplication and call request receipts cover all of them.

- **`calls.json`**: every call still in progress or still owed work. Each durable update (call identity, billing references, requests, publications, terminal results) rewrites it with fsync before the broker acts on it.
- **`calls.d/<call id>.json`**: a settled call. Every 10 minutes, maintenance moves calls that no later work will change out of `calls.json`. A call qualifies when it ended at least 30 minutes ago and has a final provider status, a closed Primary binding and a captured transcript. It must also have no pending Hark, receipt or billing delivery and no retry link. Each durable write then stays the size of the calls in progress, not the whole history. Settling the 18 October production journal took `calls.json` from 1.8 MB to 143 KB, and one durable write from about 33 ms to about 8 ms on this host. A settled call that changes goes back into `calls.json`, and that copy wins over its file. Each file is fsynced before `calls.json` drops its call. Log: `broker_calls_settled`.
- **`live.d/<call id>.json`**: live transcripts that are not in `calls.json` yet. Transcript fragments arrive several times a second, so they never cause a journal write. Readers see them at once in memory. They reach this file asynchronously, coalesced, with at most one write in flight per call. A slow or blocked disk delays only this file, never call audio (`tests/broker-audio-blocked-disk.test.ts`). The next durable update carries them into `calls.json`, and so does a clean stop. Startup folds any left by a crash and removes the file. A crash loses at most the transcript since the last completed write. Logs: `broker_observation_write_failed`, `broker_observation_discarded`.

Measured on a copy of the 18 October production journal (1.8 MB, 50 calls), on this host under `nice`/`ionice`: before, a transcript fragment held the event loop for 41 ms at the median and 59 ms at p90, with a 108 ms worst case. After, it holds the loop for 0.7 ms (p90 1.3 ms). The event loop's p99 delay went from 43 ms to 2 ms.

Releases before this one read only `calls.json`. Run `scripts/rollback-journal-compat.mjs` before rolling back (see below); it folds `calls.d` and `live.d` back into `calls.json`.

## Deployment

Prepare an immutable release directory containing the reviewed source, lockfile, compiled `dist`, and installed dependencies. Build and validate that exact release before starting it:

```sh
npm ci
npm run typecheck
npm test
npm run build
```

The release includes `tsx` because `npm run broker` uses the operator script. `npm start` itself runs compiled JavaScript.

`deploy/ecosystem.config.cjs` is a PM2 configuration. It requires `BROKER_RELEASE_DIR` to name a real (non-symlinked) directory under `/var/lib/dexter-broker/releases`, and sets the state directory and env file paths for that layout; adjust them for your host.

```sh
pm2 start "$BROKER_RELEASE_DIR/deploy/ecosystem.config.cjs" --only dexter-broker
pm2 save
```

`pm2 save` records the running release so a reboot resurrects the same one.

The configuration starts one forked process and gives shutdown 65 seconds; the application has a 60-second shutdown deadline. The journal permits one writer, so an upgrade must stop the previous broker process before the replacement starts. Avoid a rolling overlap against the same state directory. A rollback selects the previous release and retains the same journal; verify journal compatibility first. Preserve the current journal; restoring an earlier snapshot could erase evidence of an accepted answer or an outbound call.

### Rolling back

Releases before this one read only `calls.json`, so a rollback first folds the settled calls (`calls.d`) and live transcripts (`live.d`) back into it. **Use this release's copy of `scripts/rollback-journal-compat.mjs`, the release being rolled back from.** The previous release's copy exits 0 but folds nothing, and that release would then see only the calls still in `calls.json`. The script folds both directories and backs up the original `calls.json`. It then renames the directories to `*.rollback-<time>.bak`, which earlier releases ignore.

`--target` names the release being rolled back to. `6e23b1be9103` and later read every journey key, so their journeys are left as saved. Without `--target`, the journey conversion for `prod-2026-10-13` (below) also runs.

```sh
REL=/var/lib/dexter-broker/releases/teledexter-<this release>
node "$REL/scripts/rollback-journal-compat.mjs" /var/lib/dexter-broker/state --dry-run --target 6e23b1be9103
```

The dry run must report `settledCallsFolded` equal to `settledFiles`, the number of files in `calls.d`. Then:

```sh
pm2 stop dexter-broker
node "$REL/scripts/rollback-journal-compat.mjs" /var/lib/dexter-broker/state --target 6e23b1be9103
pm2 delete dexter-broker
BROKER_RELEASE_DIR=/var/lib/dexter-broker/releases/teledexter-6e23b1be9103 pm2 start /var/lib/dexter-broker/releases/teledexter-6e23b1be9103/deploy/ecosystem.config.cjs --only dexter-broker
pm2 save
```

Run `pm2 save` after every start and every rollback. A reboot resurrects whatever `~/.pm2/dump.pm2` names. An old dump would start the previous release on a journal that was never folded.

Rolling forward again needs no step, because this release reads a folded `calls.json` as is. The call records this release adds (`endReason`, `voiceCloses`, `voiceGuardrail`) are plain fields. Production's `6e23b1b` store loads a folded journal written by this release and keeps those fields across its own writes.

### Rolling back to prod-2026-10-13

The release before the greeting stream (`prod-2026-10-13`, release directory `teledexter-50e476cb88d7`) refuses to start on a journal this release has written, even after one call:

- its journey schema rejects the keys `streamStartedAt`, `firstSpeechAudioSentAt` and `callerSpeechEndedAt`;
- it rejects any journey on a call without `mediaStartedAt` ("Call journey lacks media evidence"), which is every call that ended during the greeting.

Under PM2 that refusal is a restart loop and every call fails. Check and convert the journal with this release's script, after this broker stops and before the previous release starts:

```sh
node "$CURRENT_RELEASE_DIR/scripts/rollback-journal-compat.mjs" /var/lib/dexter-broker/state --dry-run
pm2 stop dexter-broker
node "$CURRENT_RELEASE_DIR/scripts/rollback-journal-compat.mjs" /var/lib/dexter-broker/state
```

Then start the previous release as in the deployment step above, with `BROKER_RELEASE_DIR` naming its directory.

- **What changes.** The script removes those three keys and drops the journey of calls without `mediaStartedAt`, and a journey left empty. Every other field of every call stays as saved. The journey is timing evidence only; no call, request or binding state depends on it. A journal that needs no change (one the previous release wrote) is left untouched.
- **Safety.** It takes the broker's writer lock, so it refuses (`rollback_broker_running`) while a broker owns the directory. `--dry-run` only reads and reports, so it can run while the broker is still up.
- **Durability.** The original journal is kept beside it as `calls.json.rollback-<time>.bak`. Both files are written the way the store writes: a new `0600` temporary file, fsync, rename, then fsync of the directory.
- **Proof.** Journals written by this release (an outbound call that ended during the greeting, an attached outbound call, an attached inbound call, and an inbound caller who hung up during the greeting) were refused by that release's `dist/broker/store.js` before the script and loaded after it. This release still reads the converted journal, so rolling forward again needs no step.

`deploy/nginx-broker.conf` is a location snippet for the HTTPS `server` block that serves `BROKER_PUBLIC_ORIGIN`. Its `proxy_pass` preserves the `/broker/` prefix and supports WebSocket upgrades. It exposes no `/internal/` routes.

## Telephone routes

Outbound call creation sets its own answer and status callbacks under `/broker/answer/:id` and `/broker/status/:id`. For incoming calls, configure the Twilio number with voice POST to `/broker/incoming-call` and status POST to `/broker/status`. Media streams connect to `/broker/media/:id` (account calls) and `/broker/setup-media/:id` (phone setup calls).

## Greeting and media start

Account calls answer with the media stream before account work finishes, so the greeting is heard at pickup:

- **Outbound.** The signed answer callback returns `<Connect><Stream>` immediately and starts the Primary binding (`openCall`, including the API's provider-call check) in the background.
- **Inbound** (same-call attachment). The first webhook returns a bare `<Redirect>`, which does not pick up. The signed continuation returns `<Connect><Stream>` and then a signed `<Redirect>` back to the continuation. The `<Connect>` answers the call. Admission begins once that response is delivered, because the API's begin refuses a leg that is still ringing. Once approved, the API's consume and the broker's own Twilio call-status check run in parallel (the consume makes the same active-call check), and Live attaches only when both pass. A leg that ended, or that Twilio describes differently from the signed leg, ends the wait and closes any binding the consume made. The same stream response also opens the Live WebSocket, as the outbound answer does: the socket only, with no session and no account data until Live attaches, and adoption requires the bound admission (and, from approval, that exact approval). Its handshake (0.24-0.68 s on the Oct 13 calls) is then done before admission finishes. Like the outbound socket it is an optimization: an admission step that the next poll retries leaves it open, and if it fails or idles out (20 s before approval) Live connects normally. If it failed before approval, approval opens a fresh socket under the earlier inbound rule: it survives retried steps once its binding is committed, but if it fails, Live cannot start and the call ends.

The pre-rendered clip starts as soon as Twilio's media `start` frame arrives: "Hey, it's Dexter." for outbound calls, and "Hey, it's Dexter. What can I do for you?" for inbound calls. It carries no account data. GPT-Live, with the account context, attaches to the stream only after the binding exists (outbound) or admission is bound with its context (inbound). Until then the stream carries only the clip. Selected-question continuations, verification callbacks and restricted setup keep their own Live-spoken openings and play no clip.

Caller audio on that stream:

- Audio spoken while the clip plays is discarded, so Live never answers a greeting that is already over. The clip counts as playing until Twilio acknowledges its last mark, or its length plus 1.5 s without marks. If Live is already attached, it receives the same length of silence instead.
- Audio after the clip is kept. Until Live attaches, the broker holds the newest 15 s, then delivers it first and in order; the generation controller replays that backlog at 1.5x real time.
- An outbound opening makes Live speak first, so it waits for what the person said. If the backlog Live receives when it goes live holds caller speech (a "Hello?" after "Hey, it's Dexter."), the opening instruction is sent once the replay has delivered the last of that speech, and its text says the person may have answered: Live acknowledges briefly and makes the offer in the same reply, with no second greeting and no separate "I'm here". Before this, the opening went out after the first 20 ms of backlog and the held words reached Live mid-offer (call10's "Yeah, I'm here"). The wait is the replay time of the backlog up to those words (1.5x real time), about 0.7-1.7 s in simulation with the Oct 13 timings; held silence never delays the opening. Inbound openings (wait for the caller) still go first.

`mediaStartedAt` is the time of the first caller sample Live receives: the greeting's end (or attach time, if the clip is still playing), and never earlier than an inbound binding's `consumedAt`. Input sample positions count from it. `journey.streamStartedAt` records the provider stream start; `journey.greetingClipStartedAt` and `greetingClipPlayedAt` record the clip.

Live starts only once the binding exists, so its startup (1.75-2.6 s after the history read on the Oct 13 calls) now follows admission instead of hiding under the clip. A caller who asks something right after the greeting can wait that long, plus the 1.5x replay of what they said, for the first reply. Two changes shorten that wait and make it visible:

- The binding response seeds Live directly. The admission or binding response carries the eligible history page, and Live attaches milliseconds later, so a page up to 1 s old (`STARTUP_PAGE_FRESH_MS`) opens Live without a second eligibility read; that read alone took 0.45-1.36 s on the Oct 13 calls. An older page is rechecked before Live opens. Either way the generation asserts the page's version after Live opens and before any caller audio, opening or Live output passes; a page that changed is replaced, and lost authority ends the call before Live says anything.
- `journey.firstSpeechAudioSentAt` records Dexter's first spoken words (never the clip), and `journey.callerSpeechEndedAt` the caller's last speech frame after the greeting and before them. Their difference is the silence the caller heard before the first reply; the `firstSpeechAudioSentAt` log stage carries it as `afterCallerSpeechMs`.

While an inbound stream waits, the broker polls the same admission after 100, 200 and 400 ms, then every second (the pace of the hold redirects it replaced). The quick first polls are for a race with Twilio's pickup: admission starts as the stream response leaves, and the API's begin refuses a leg that is still ringing (`call_not_active`). A single miss now costs about 0.1-0.3 s instead of about 1.4 s. A refusal, cancellation or the 120-second wait limit ends the wait. The broker then closes the stream after the clip. Twilio runs the trailing redirect, and the continuation answers with restricted setup or a hangup. The same redirect also runs when the stream drops after Live attached (a broker crash or restart mid-call); that call cannot resume, so the continuation answers `<Hangup/>` and the caller hears a clean end instead of Twilio's "application error". A failed outbound binding is spoken ("I could not reach your Dexter account just now. Please try again shortly.") and ends the call. A call that ends while Primary is still binding it, or whose stream is stopping, has that binding closed by the answer route when it lands. Shutdown waits for every in-flight outbound binding and closes it before the journal closes, so the binding is saved and closed rather than left open in the API.

Log events: `broker_audio_timing` stages `greetingClipStartedAt`, `greetingClipFirstPlaybackAcknowledgedAt` and `greetingClipPlayedAt`; `broker_account_media` (`mediaStartedAt`, `heldInputMs`, `droppedInputMs`, `greetingInputMs`) when Live attaches; `broker_greeting_stream_released` when an inbound stream is handed back to its TwiML; and `broker_opening_after_caller_speech` (`heldSpeechInputMs`, `waitedMs`) when an outbound opening waited for held caller speech.

## Incoming calls

Every Twilio webhook must carry a valid signature for the exact URL. Incoming calls follow one of two paths, selected at startup.

**Same-call attachment** (when `BROKER_VERIFICATION_SETUP_MODEL` is set). The broker saves the signed incoming call and redirects it to `POST /broker/inbound/:callId/continue`. The signed continuation answers with the media stream, and the greeting plays while the Dexter API decides whether the caller may attach to an account ([Greeting and media start](#greeting-and-media-start)). A caller whose number is linked to an account is bound to that account's Primary Dexter and talks to GPT-Live on the same call. A caller without a linked account hears the greeting, then gets a restricted phone-setup voice whose only tools are setup status, a verified-callback request, and completing or declining setup (`src/live/restrictedSetupTools.ts`). See [inbound-call-attachments.md](inbound-call-attachments.md).

**Callback offer** (when `BROKER_VERIFICATION_SETUP_MODEL` is empty). The inbound response offers one DTMF choice: press 1 to request a callback. The offer and acknowledgment use the same wording whether the caller's account is ready or unavailable. The inbound leg supplies no account context to the caller or a voice model. The Gather action posts to `/broker/callback/:id`.

Consent requires the signed Twilio POST for the exact inbound call, caller number, owned destination, and provider account. A fresh Twilio read must confirm those identities and an active call before consent is saved. This read has a four-second deadline. A matching caller ID alone never opens Primary.

The consent record stores a stable request UUID derived from the provider account and inbound CallSid, its timestamp, and state in the call journal. The response tells the person they can hang up and ends the inbound leg. Maintenance checks pending callbacks every 10 seconds. It fetches the exact inbound call again and requires provider-confirmed termination before invoking the outbound executor.

The destination must be in `BROKER_ALLOWED_TO`. The outbound executor asks the API to confirm that exact number is linked to an account with ready Primary authority. Only after that check does it run the mandatory GPT-Live audio preflight and dial. A failed readiness check prevents preflight and dialing. Account readiness alone does not prove phone possession; the signed answer callback and current provider call check establish it before account access.

Consent expires after five minutes. The callback helper admits at most three requests per number per hour, with at least 60 seconds between admissions; these limits survive process restart. Exact webhook retries retain the original consent and request ID. An existing outbound request is recovered even when its outcome is uncertain, so a timeout or restart cannot produce a second dial.

The operator call list includes `callback.state`: `requested` means consent is waiting for dispatch, `started` means an outbound request is already saved, and `unavailable` means this callback will receive no further dispatch attempt. `started` does not prove that the phone rang or a person answered; inspect the associated outbound call status.

## Controlled call

Run the CLI with `BROKER_ENV_FILE` pointing to the private configuration. The CLI connects to the broker on loopback and supplies the control token internally.

```sh
npm run broker -- status
npm run broker -- preflight
```

Preflight opens one GPT-Live session for at most 15 seconds. Success requires actual output audio and the matching final `session.closed` event. It makes no telephone call and has no retry or reconnect loop. It consumes provider credits.

For a test call, save one UUID as `BROKER_REQUEST_ID` before dispatch. Keep it with the purpose and receipt. Use the same ID if the local HTTP response is lost:

```sh
npm run broker -- call --request-id "$BROKER_REQUEST_ID" --purpose "Read the caller's current portfolio and discuss their questions." --receipt "$BROKER_RECEIPT_PATH"
```

The receipt path must be a new private file. With one allowed destination, the CLI needs no number in the command. Multiple destinations require `--to` with an allowed E.164 number. The service first saves the request, checks account readiness, and runs another mandatory audio preflight before dispatching to Twilio. Repeating the same request returns the saved call instead of dialing again. A different request to a person with an unresolved call is blocked.

A financial-action claim needs the corresponding Primary result and transaction or receipt evidence. A spoken acknowledgment alone is insufficient.

## Close and recovery

Ended calls print one readable transcript to the broker log after voice cleanup and the Primary closeout attempt. To print the same transcript from an existing journal without contacting the broker or taking its writer lock:

```sh
npm run broker -- transcript --id "$BROKER_CALL_ID"
```

It reads `$BROKER_STATE_DIR/calls.json` from the broker env file, with the settled calls and live transcripts beside it (`calls.d`, `live.d`); pass `--journal /path/to/calls.json` to read a copy instead.

The header uses the call's creation and end times. Consecutive fragments from the same speaker become one turn, including across Live generations. Each Primary line includes the submitted text, saved status, and elapsed time from first save to last status update (including delivery). Missing timing is shown as `unknown`. Logs are not replayed for calls already ended when the broker starts; the command can still read them. No transcript storage is added.

These commands use the broker call UUID returned by `status`, not the Twilio SID:

```sh
npm run broker -- end --id "$BROKER_CALL_ID"
npm run broker -- reconcile --id "$BROKER_CALL_ID"
```

`end` stops the active call or requests provider completion for its saved SID. `reconcile` fetches that exact SID, checks the direction and both numbers, and updates local status. Neither command creates a call. Reconciliation requires a saved provider SID.

Before closing Primary admission, the service marks `primaryClosePending: true` in `calls.json`. It restores saved delegation and source journals, reads uncertain request admission using the original request ID, and replays pending source captures with their original IDs, revision, text, and timestamp. Pending captures or unresolved question replies keep the Primary binding open.

For an ended call with a terminal provider status and finalized transcript, an unknown submission can also receive a definitive admission check. The service first captures the exact original raw transcript as revision 1 and saves the API acknowledgement, including its revision row ID and body digest. Qualified later revisions are saved afterward.

Once captures and replies are resolved, the service closes the same binding and saves `primaryClosed`. Closure prevents new admission on that call. It then sends the original submission body to the API's request reconciliation endpoint. An existing admission returns its saved run; if recovered under a canonical request ID, that ID is retained separately for result reads. The original request ID and body remain unchanged.

A `not_admitted` outcome is saved only after its call, source, raw submission digest and stored transcript receipt match the API proof. This establishes that the closed call has no matching admitted request. The journal retains the original submission and error alongside this outcome, without creating a failed run or assistant answer. Closeout never repeats the original submit.

A failed capture, close or reconciliation leaves `primaryClosePending` set. Maintenance resumes from saved acknowledgements every 10 seconds while the process accepts work, including records loaded after restart. Explicit `reconcile` also attempts closeout for terminal calls or a pending binding. Inspect `primaryClosePending` and `transcriptCapture` in `status`; an HTTP success from reconciliation can still carry pending work.

`transcriptCapture.state: complete` means no eligible journaled captures remain pending. Its `unsupported` field can still show omitted turns. Caller turns require acoustic completion evidence. Assistant capture requires confirmed playback and session finalization. Interrupted, discarded, or unconfirmed speech stays unqualified; it cannot become proof that the person heard a result.

A call with status `unknown` and no provider SID needs operator investigation of the original dispatch. Preserve the request ID and journal, establish the provider outcome, and resolve that record before attempting another call. Generating a new request ID to bypass uncertainty risks a duplicate call. Do not delete the writer lock while its owning process is alive.

Maintenance also reconciles inactive, unresolved calls older than 60 seconds by saved SID and ends a provider call that is still open. During shutdown the service drains HTTP requests, stops its active calls, waits for any outbound Primary binding still being opened in the background (and closes it), and awaits closeout before releasing the journal lock. A forced exit leaves recovery evidence on disk for the next process.

## Health

`GET /broker/health` reports process readiness, model, revision (`BROKER_REVISION`), and active call count. It checks the configured Primary and Hark API origins before answering and returns HTTP 503 when either origin is unavailable, reports an invalid health identity, or serves a different release. Each probe has a two-second limit and reads at most 16 KiB; concurrent checks share one request and results are cached for at most five seconds. A successful response establishes HTTP reachability and API release identity. It does not establish provider credits, account authority, working audio, or a completed call.

## Call journey report

`npm run calls -- journey` builds a read-only timing report from the call journal:

```sh
npm run calls -- journey --journal /absolute/path/to/state/calls.json --log /absolute/path/to/broker.log
```

Add `--call-id <broker-call-uuid>` to select an earlier call; a settled call is read from `calls.d` beside the journal. The JSON report joins admission and consumption times, stream start, the greeting clip, Live attaching (`mediaStartedAt`), provider readiness, first audio and carrier playback acknowledgment. `createdToStream` approximates time to pickup plus stream setup, `greetingClipPlayedToMedia` is how long the caller waited after the greeting before Live attached, and `callerSpeechEndToFirstSpeech` is the silence between the caller's last words after the greeting and Dexter's first words (null when the caller said nothing first). It also shows each saved request's dispatch, answer availability and publication acceptance. Publication acceptance means the answer entered the voice session; the journal has no per-answer playback or individual tool spans. Missing timestamps remain null.

The report reads at most 32 MiB of journal data and the final 8 MiB of the supplied log. Its coverage fields report truncation and omitted rows. It emits timing, status and broker request IDs, excluding phone numbers, transcripts, provider proofs and raw errors. It opens neither the writer lock nor a network connection.
