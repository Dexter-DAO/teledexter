// ---------------------------------------------------------------------------
// Echo-turn suppressor — the second half of the speakerphone echo defense
// (see EchoGuardedTransport.ts for the first half and the full diagnosis).
//
// Even with interrupts gated, echo that crosses VAD still gets COMMITTED as a
// user turn and transcribed. With `createResponse: true` the server then
// answers it. Live 2026-07-20: agent said "Welcome, caller—", the transcriber
// reported the caller saying "Welcome", and the agent replied to itself.
// Empty transcriptions (echo/noise carrying no recognizable words) do the
// same thing — a reply generated from nothing.
//
// SAFETY MODEL: classification requires three independent signals — actual
// Twilio playback, low inbound energy, and either empty ASR or a short match to
// Dexter's most recent speech. The call site cancels only the phantom response;
// it does not clear the legitimate playback that produced the echo.
// ---------------------------------------------------------------------------

/** Lowercase, strip punctuation, collapse whitespace — compare words only. */
function normalize(text: string): string {
  return text
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

export type EchoVerdict =
  | { echo: false }
  | { echo: true; reason: 'low_energy_empty' | 'low_energy_matches_agent_speech' };

export interface EchoTurnEvidence {
  /** The VAD start occurred while Twilio still had assistant audio queued. */
  beganDuringPlayback: boolean;
  /** Peak inbound RMS around the VAD start. */
  rms: number | null;
  /** Only energy at or below this ceiling can be speakerphone echo. */
  maxEchoRms: number;
}

/**
 * Label a completed user transcription as echo or genuine.
 * Text resemblance is never sufficient by itself. Suppression requires the
 * turn to have begun during real Twilio playback and to be acoustically quiet.
 * That preserves loud corrections even when they repeat a word Dexter used.
 */
export function classifyUserTurn(
  transcript: string,
  agentSpeech: string,
  evidence: EchoTurnEvidence,
): EchoVerdict {
  if (
    !evidence.beganDuringPlayback
    || evidence.rms === null
    || !Number.isFinite(evidence.rms)
    || !Number.isFinite(evidence.maxEchoRms)
    || evidence.maxEchoRms <= 0
    || evidence.rms <= 0
    || evidence.rms > evidence.maxEchoRms
  ) {
    return { echo: false };
  }

  const user = normalize(transcript);

  if (!user) return { echo: true, reason: 'low_energy_empty' };

  const agent = normalize(agentSpeech);
  if (!agent) return { echo: false };

  // Long turns are the caller genuinely talking — never suppress those, even
  // if they overlap the agent's wording.
  const words = user.split(' ');
  if (words.length > 6) return { echo: false };

  // A short fragment that appears verbatim in what the agent just said is our
  // own voice looping back. Guard the tiny cases ("no", "ok") so a real
  // one-word answer that happens to occur in the sentence still gets through.
  if (user.length >= 4 && agent.includes(user)) {
    return { echo: true, reason: 'low_energy_matches_agent_speech' };
  }

  return { echo: false };
}
