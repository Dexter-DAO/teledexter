// ---------------------------------------------------------------------------
// session.on('audio_stopped') handler.
//
// Extracted verbatim from src/server.ts in Step 2 of the session-event
// refactor. Flips assistantSpeaking false and logs how long the agent
// was speaking. NOTE: barge-in is flipped on the FIRST response.done with
// status=completed, not here — audio_stopped also fires on
// interrupted/cancelled audio. See the inline comment preserved below.
// ---------------------------------------------------------------------------

import type { SessionState } from '../SessionState.js';

export function onAudioStopped(state: SessionState, playbackPending = false): void {
  const dur = state.assistantSpeaking ? `${Date.now() - state.audioStartTs}ms` : 'was not speaking';
  if (!playbackPending) state.assistantSpeaking = false;
  console.log(
    playbackPending
      ? `[diag] audio_generation_stopped | Twilio playback still pending after ${dur}`
      : `[diag] audio_stopped | played ${dur}`,
  );
  // OpenAI audio generation may stop while Twilio still has seconds queued.
  // Leave the state true until the transport receives Twilio's done mark or
  // explicitly clears the playback buffer.
}
