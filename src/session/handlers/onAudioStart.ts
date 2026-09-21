// ---------------------------------------------------------------------------
// session.on('audio_start') handler.
//
// Extracted verbatim from src/server.ts in Step 2 of the session-event
// refactor. Stamps audioStartTs, flips assistantSpeaking true, and logs
// the diag line. No callEntry mutation, no field captures — Step 3 will
// enrich.
// ---------------------------------------------------------------------------

import type { SessionState } from '../SessionState.js';

export function onAudioStart(state: SessionState): void {
  state.audioStartTs = Date.now();
  state.assistantSpeaking = true;
  console.log(`[diag] audio_start (response #${state.responseCount})`);
}
