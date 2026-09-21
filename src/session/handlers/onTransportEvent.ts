// ---------------------------------------------------------------------------
// session.on('transport_event') handler.
//
// Extracted verbatim from src/server.ts in Step 2 of the session-event
// refactor. Handles:
//   - Twilio `start` event: rekeys activeCalls when the real streamSid
//     arrives, replacing the synthetic placeholder seeded in state.
//   - speech_started / speech_stopped / user_transcription / response.created
//     / response.done: diag logging only.
//
// No new event subscriptions, no field captures beyond what the original
// inline handler did. Step 3 will enrich.
// ---------------------------------------------------------------------------

import type { CallLogEntry } from '../../types/CallEntry.js';
import type { SessionState } from '../SessionState.js';

export function onTransportEvent(
  state: SessionState,
  callEntry: CallLogEntry,
  activeCalls: Map<string, CallLogEntry>,
  event: any,
): void {
  const etype = event?.type;

  if (etype === 'twilio_message' && event?.message?.event === 'start') {
    const sid = event.message.start?.streamSid;
    console.log(`[diag] twilio start | streamSid=${sid}`);
    if (sid && sid !== state.streamSid) {
      activeCalls.delete(state.streamSid);
      state.streamSid = sid;
      callEntry.streamSid = sid;
      activeCalls.set(sid, callEntry);
    }
  }

  if (etype === 'input_audio_buffer.speech_started') {
    const sinceAudioStart = state.assistantSpeaking ? Date.now() - state.audioStartTs : Infinity;
    // Diagnostic only — we do NOT call session.interrupt() here. The SDK
    // already auto-interrupts on speech_started when VAD is enabled
    // (openaiRealtimeWebsocket.js:222). Calling it manually too was a second,
    // conflicting interrupt — the source of the start/stop jitter. Echo is now
    // rejected at the detection layer via the server_vad `threshold` above, so
    // the SDK's own VAD shouldn't fire on the agent's quiet self-echo at all.
    console.log(`[diag] speech_started | assistantSpeaking=${state.assistantSpeaking} sinceAudioStart=${sinceAudioStart === Infinity ? 'n/a' : sinceAudioStart + 'ms'} (SDK auto-interrupts; no manual call)`);
  }

  if (etype === 'input_audio_buffer.speech_stopped') {
    console.log(`[diag] speech_stopped`);
  }

  if (etype === 'conversation.item.input_audio_transcription.completed') {
    const transcript = (event as any).transcript || '';
    console.log(`[diag] user_transcription: "${transcript.slice(0, 200)}" | assistantSpeaking=${state.assistantSpeaking}`);
  }

  if (etype === 'response.created') {
    state.responseCount++;
    console.log(`[diag] response.created #${state.responseCount} | assistantSpeaking=${state.assistantSpeaking}`);
  }

  if (etype === 'response.done') {
    const status = (event as any).response?.status || (event as any).status || 'unknown';
    console.log(`[diag] response.done #${state.responseCount} status=${status} | assistantSpeaking=${state.assistantSpeaking}`);
    // Note: we used to flip barge-in here via updateSessionConfig, but that
    // path triggered Twilio 31951 "Invalid message" errors (the partial
    // config got shallow-spread by TwilioRealtimeTransport and dropped
    // audio.output.voice). Barge-in is pinned from session start and handled
    // by EchoGuardedTwilioTransport against actual Twilio playback state.
  }
}
