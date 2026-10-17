# Live protocol reference

Broker uses the OpenAI Node SDK's `LiveWS` and `ClientEvent`/`ServerEvent` types. GPT-Live carries the conversation; Primary Dexter is the account-bound backend. The implementation is in `src/live/session.ts` and `src/broker/server.ts`.

| Path | Contract and implementation rule |
| --- | --- |
| Audio | `session.output_audio.delta` carries base64 `delta`. The primary WebSocket supplies no `event_id` or audio-done event. Preserve identical chunks and assign local playback delivery IDs. |
| Transcripts | Input/output transcript deltas carry `event_id`, `delta`, `start_ms`, and `end_ms`. Persist snapshots in delivery order. Fragments can arrive after speech; wait for acoustic and transcript settling before submitting the complete request. |
| Delegation | `session.delegation.created` supplies `delegation.id`, `target`, and `offset_ms`; build task text from saved transcripts. Broker requests client mode and accepts omitted/null delegation in the returned session. |
| Appends | Send `event_id`; correlate `*.appended` through `client_event_id` and rejections through `error.client_event_id`. The SDK can deliver one protocol error through both callbacks: deduplicate it and preserve recoverable command rejection. |
| Close | Send `session.close`, await matching `session.closed` and usage, then drain transcript persistence. Track audio playback separately from provider finalization. |

See the official [WebSocket event contract](https://developers.openai.com/api/docs/guides/voice-websockets#send-and-receive-events) and [client delegation guide](https://developers.openai.com/api/docs/guides/live-delegation#keep-the-conversation-context-in-your-application).

Keep input flowing throughout startup and silence: 160 bytes of 8 kHz PCMU every 20 ms. Put conversation policy in `instructions` and account reference data in initial history. After `session.started`, send the explicit opening through `session.instructions.append` with `delegation_id: null`. Its receipt confirms acceptance; audio and playback establish the greeting. Follow the official [opening sequence](https://developers.openai.com/api/docs/guides/live-conversations#greet-before-the-caller-speaks).

Account calls usually open with a pre-rendered greeting clip that the broker plays on the Twilio stream before Live exists. The opening then tells Live the caller already heard it (`brokerOpening(..., greeted)`): inbound Live waits for the caller, and outbound Live continues with the offer. Caller audio spoken over the clip never reaches Live; an attached Live receives silence of the same length. Audio after the clip is held until Live attaches and is sent first, in order. Live output that arrives while the clip plays is held behind it. An outbound opening makes Live speak first, so caller speech held while Live started is delivered before it, and the opening (`brokerOpening(..., greeted, heardCaller)`) tells Live the person may have answered: it acknowledges briefly and offers in the same reply. Held silence does not delay the opening. See [BROKER_OPERATIONS.md](BROKER_OPERATIONS.md#greeting-and-media-start).

SDK-typed fixtures in `tests/live-session.test.ts` participate in normal `npm run typecheck`. Run them with `node --import tsx --test tests/live-session.test.ts`.

`scripts/qualify-live-phone.mts` accepts `--module-root`, `--module-variant src|dist`, a new `--evidence-dir` outside the repository, and `--config-file`. Optional `--input-audio` takes raw 8 kHz PCMU; use the neutral request "Please ask Primary for my test phrase."

Qualification uses actual Live preflight/audio/transcripts with simulated Twilio callbacks, playback receipts, and Primary. The dummy result is "Your test phrase is blue lantern." Check greeting before caller speech, complete submitted request, spoken result, and finalization. This exercises the SDK and broker media path without a telephone carrier or a real account.
