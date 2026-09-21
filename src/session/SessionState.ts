// ---------------------------------------------------------------------------
// Per-WebSocket-connection mutable state shared across the session.on(...)
// handlers extracted out of src/server.ts in Step 2 of the session-event
// refactor.
//
// Background: the original handlers were inline closures reading/writing a
// pile of `let` variables in the parent scope (audioBytesOut, streamSid,
// assistantSpeaking, ...). To pull the bodies into their own files without
// changing behavior, those primitives are bundled here so handlers can
// mutate `state.audioBytesOut++` instead of relying on closure capture.
//
// See .planning/specs/2026-05-30-session-event-audit-and-refactor.md.
// Do not add fields here until a later step actually needs them.
// ---------------------------------------------------------------------------

export interface SessionState {
  // Current Twilio stream SID. Starts as a synthetic `call-<ts>` placeholder
  // and is overwritten when the real Twilio `start` event arrives via
  // transport_event. The activeCalls map in server.ts is rekeyed in lockstep.
  streamSid: string;

  // True while Twilio still has assistant audio queued for the caller. OpenAI
  // generation completion alone must not flip this false; the returned
  // `done:<itemId>` Twilio mark is the real playback boundary.
  assistantSpeaking: boolean;

  // ms timestamp of last audio_start. 0 when never started.
  audioStartTs: number;

  // ms timestamp of last manual interrupt() call. 0 when never called.
  // (Currently informational only — manual interrupts were removed.)
  lastInterruptCallTs: number;

  // OpenAI Realtime response counter, incremented on each
  // transport_event `response.created`.
  responseCount: number;

  // One-shot guard: only send the MCP alias-correction system message once
  // per call, no matter how many times the model retries the bad tool name.
  mcpAliasCorrectionSent: boolean;

  // Passive audio-out observation (NOT a wrap of connection.send). Logged
  // on call end and shipped to the Observatory.
  audioBytesOut: number;
  audioChunksOut: number;

  // ── echo defense (2026-07-20) ──────────────────────────────────────────
  // Rolling text of what the agent most recently SAID, so a "user" turn that
  // is really our own voice looping back through the caller's speakerphone
  // can be recognized and dropped instead of answered. Live proof: the agent
  // said "Welcome, caller—" and the transcriber reported the CALLER saying
  // "Welcome", which the model then replied to.
  lastAssistantSpeech: string;

  // Acoustic evidence for the current caller turn. The item ID ties the VAD
  // start to its eventual transcript so word overlap alone can never suppress
  // a loud, genuine utterance such as "Faster".
  lastInputSpeechItemId: string | null;
  lastInputSpeechStartedDuringPlayback: boolean;
  lastInputSpeechStartedRms: number | null;
  lastInputSpeechResponseCountAtStart: number;

  // Diagnostics, surfaced on call end: how often our own voice tried to
  // interrupt us, and how many bogus echo turns were dropped.
  echoInterruptsSuppressed: number;
  echoTurnsSuppressed: number;
}

export function createSessionState(initialStreamSid: string): SessionState {
  return {
    streamSid: initialStreamSid,
    assistantSpeaking: false,
    audioStartTs: 0,
    lastInterruptCallTs: 0,
    responseCount: 0,
    mcpAliasCorrectionSent: false,
    audioBytesOut: 0,
    audioChunksOut: 0,
    lastAssistantSpeech: '',
    lastInputSpeechItemId: null,
    lastInputSpeechStartedDuringPlayback: false,
    lastInputSpeechStartedRms: null,
    lastInputSpeechResponseCountAtStart: 0,
    echoInterruptsSuppressed: 0,
    echoTurnsSuppressed: 0,
  };
}
