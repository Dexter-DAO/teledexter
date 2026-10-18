import { readFileSync } from 'node:fs';
import type { BrokerCall } from './store.js';

/** Outbound clip words. The Live opening continues after them with the offer. */
export const GREETING_CLIP_TEXT = "Hey, it's Dexter.";
/** 8 kHz mono PCMU (G.711 mu-law), headerless: OpenAI TTS gpt-4o-mini-tts, voice "marin". */
export const GREETING_CLIP_ASSET = new URL('../../assets/greeting-hey-its-dexter.pcmu', import.meta.url);
/** Inbound clip words: the whole inbound opening. Live then waits for the caller. */
export const INBOUND_GREETING_CLIP_TEXT = "Hey, it's Dexter. What can I do for you?";
export const INBOUND_GREETING_CLIP_ASSET = new URL('../../assets/greeting-hey-its-dexter-what-can-i-do.pcmu', import.meta.url);

const FRAME_BYTES = 160; // 20 ms of 8 kHz PCMU, Twilio's own media frame size.
const LEAD_BYTES = 800; // 100 ms sent ahead so carrier jitter never starves playback.
const MIN_CLIP_BYTES = 2_400, MAX_CLIP_BYTES = 32_000; // 0.3 s to 4 s.

/** Reads the shipped clip once at startup. Returns undefined (no clip, spoken
 * opening) when the asset is missing or implausible, never a partial clip. */
export function loadGreetingClip(path: URL | string = GREETING_CLIP_ASSET): Buffer | undefined {
  try {
    const clip = readFileSync(path);
    return clip.length >= MIN_CLIP_BYTES && clip.length <= MAX_CLIP_BYTES
      ? Buffer.concat([Buffer.alloc(2_400, 0xff), clip]) : undefined;
  } catch { return undefined; }
}

/** The clip replaces only the first words of brokerOpening. It is played only on
 * the first media connection of a call whose Live opening will be brokerOpening:
 * never on selected-question continuations, verification callbacks or restricted
 * setup (those own their openings), and never after an opening or audio already
 * happened (restart, re-adoption). It carries no account data. */
export function greetingClipEligible(call: BrokerCall): boolean {
  return !call.continuation && !call.verificationCallback && !call.restrictedSetup
    && !call.voiceOpening && !call.journey?.firstAudioSentAt && !call.journey?.greetingClipStartedAt
    && !(call.liveGenerations?.segments.length);
}

export interface GreetingClipPlayback {
  /** True until every clip frame is sent. */
  readonly sending: boolean;
  cancel(): void;
}

/** Streams the clip in 20 ms frames at real-time pace (100 ms lead), scheduled
 * against the start time so timer drift never accumulates. */
export function playGreetingClip(clip: Buffer, options: {
  send(frame: Buffer): boolean;
  onSent(): void;
  now?: () => number;
  setTimer?: (fn: () => void, ms: number) => ReturnType<typeof setTimeout>;
  clearTimer?: (timer: ReturnType<typeof setTimeout>) => void;
}): GreetingClipPlayback {
  const now = options.now ?? (() => Date.now());
  const setTimer = options.setTimer ?? ((fn, ms) => setTimeout(fn, ms));
  const clearTimer = options.clearTimer ?? (timer => clearTimeout(timer));
  const startedAt = now();
  let offset = 0, sending = true;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const finish = () => { if (!sending) return; sending = false; if (timer) clearTimer(timer); timer = undefined; };
  const pump = () => {
    timer = undefined;
    if (!sending) return;
    // Bytes the carrier may hold now: elapsed real time plus the lead.
    const allowed = Math.min(clip.length, LEAD_BYTES + Math.floor((now() - startedAt) * 8));
    while (sending && offset < allowed) {
      const frame = clip.subarray(offset, Math.min(clip.length, offset + FRAME_BYTES));
      offset += frame.length;
      if (!options.send(frame)) { finish(); return; }
    }
    if (!sending) return;
    if (offset >= clip.length) { finish(); options.onSent(); return; }
    // The next whole frame is due once real time covers it.
    const due = startedAt + (offset + FRAME_BYTES - LEAD_BYTES) / 8;
    timer = setTimer(pump, Math.max(1, Math.ceil(due - now())));
  };
  pump();
  return { get sending() { return sending; }, cancel: finish };
}
