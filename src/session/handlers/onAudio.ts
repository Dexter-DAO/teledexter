// ---------------------------------------------------------------------------
// session.on('audio') handler.
//
// Extracted verbatim from src/server.ts in Step 2 of the session-event
// refactor. Per-call audio-out byte counter — OBSERVATION ONLY. This is
// a passive listener on the SDK's `audio` event, NOT a wrap of
// connection.send. (Wrapping connection.send broke the Twilio start-event
// handshake in a prior session; never go that route again.) Logged on
// call end so we can tell whether OpenAI actually generated audio for
// this call.
// ---------------------------------------------------------------------------

import type { SessionState } from '../SessionState.js';

export function onAudio(state: SessionState, evt: any): void {
  const len = evt?.data?.byteLength || evt?.data?.length || 0;
  state.audioBytesOut += len;
  state.audioChunksOut++;
}
