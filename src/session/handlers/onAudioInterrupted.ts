// ---------------------------------------------------------------------------
// session.on('audio_interrupted') handler.
//
// Extracted verbatim from src/server.ts in Step 2 of the session-event
// refactor. Reads lastInterruptCallTs/assistantSpeaking/audioStartTs and
// flips assistantSpeaking false. Log diagnostics only — no callEntry
// mutation, no new event subscriptions, no field captures. Step 3 will
// enrich.
// ---------------------------------------------------------------------------

import type { SessionState } from '../SessionState.js';

export function onAudioInterrupted(state: SessionState): void {
  const delta = state.lastInterruptCallTs ? `${Date.now() - state.lastInterruptCallTs}ms after interrupt()` : 'no prior interrupt()';
  const wasPlaying = state.assistantSpeaking ? `${Date.now() - state.audioStartTs}ms of audio` : 'was not speaking';
  state.assistantSpeaking = false;
  console.log(`[diag] audio_interrupted | ${delta} | ${wasPlaying}`);
}
