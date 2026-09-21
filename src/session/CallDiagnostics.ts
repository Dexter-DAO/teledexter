// ---------------------------------------------------------------------------
// CallDiagnostics — measured call telemetry, not vibes.
//
// Written 2026-07-20 because every echo/barge-in diagnosis so far has been an
// INFERENCE from which events did or didn't appear in a log. That produced a
// confident wrong answer ("the caller's speech was too quiet to cross the VAD
// threshold") that could not be checked, because nothing in the pipeline ever
// measured how loud anything actually was.
//
// This measures. Twilio hands us g711 μ-law frames (8kHz, 20ms, ~160 bytes)
// on the inbound leg; we decode them to linear PCM and track signal energy,
// bucketed by whether OUR audio was playing at the time:
//
//   openLine  — energy while the agent is silent. Contains the caller's real
//               speech and the line's noise floor.
//   duplex    — energy while the agent is audible. Contains the caller's real
//               speech PLUS our own voice echoing back off their speakerphone.
//
// The decisive comparison, available for the first time:
//   • caller speech energy vs echo energy — if they overlap, NO loudness
//     threshold can separate them and VAD tuning is a dead end (the echo
//     canceller becomes the only real fix).
//   • energy at each speech_started — was VAD triggered by a loud thing
//     (real speech) or a quiet thing (echo)?
//   • energy during stretches where the caller talked over the agent and
//     NOTHING fired — proves or kills the "too quiet" theory outright.
//
// NOTE on units: OpenAI's `threshold` is a probability from their VAD model,
// not raw amplitude, so these numbers are not directly comparable to it. What
// they ARE good for is the ratio and separability question above, which is
// what actually decides the architecture.
// ---------------------------------------------------------------------------

const MAX_TIMELINE = 600;
const RECENT_WINDOW_MS = 400;

/** ITU-T G.711 μ-law → 16-bit linear PCM. */
function ulawToLinear(uVal: number): number {
  const u = ~uVal & 0xff;
  let t = ((u & 0x0f) << 3) + 0x84;
  t <<= (u & 0x70) >> 4;
  return (u & 0x80) ? 0x84 - t : t - 0x84;
}

interface Frame {
  t: number;
  rms: number;
  duplex: boolean;
}

export interface TimelineEntry {
  t: number;
  type: string;
  detail?: Record<string, unknown>;
}

interface Bucket {
  samples: number[];
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const i = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
  return sorted[i];
}

function stats(values: number[]) {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return {
    frames: values.length,
    p50: Number(percentile(sorted, 50).toFixed(4)),
    p90: Number(percentile(sorted, 90).toFixed(4)),
    p99: Number(percentile(sorted, 99).toFixed(4)),
    max: Number(sorted[sorted.length - 1].toFixed(4)),
  };
}

export class CallDiagnostics {
  readonly startedAt = Date.now();
  private frames: Frame[] = [];
  private openLine: Bucket = { samples: [] };
  private duplex: Bucket = { samples: [] };
  timeline: TimelineEntry[] = [];
  /** Energy measured at each speech_started, split by duplex state. */
  triggerEnergy: Array<{ t: number; rms: number; duplex: boolean; msSinceAudioStart: number }> = [];
  /** Loud inbound stretches while the agent spoke and VAD never fired. */
  missedBargeIns: Array<{ t: number; rms: number; msSinceAudioStart: number }> = [];

  private lastSpeechStartedAt = 0;
  private loudDuplexRunStart = 0;
  private loudDuplexRunPeak = 0;

  // Cached noise floor. Recomputed at most every RECOMPUTE_EVERY open-line
  // frames — NOT on every duplex frame. The previous version sorted the
  // full, unbounded openLine.samples array ~25×/sec on the audio-forwarding
  // event loop (review 2026-07-20: rising CPU / audio jitter on long calls).
  private cachedNoiseFloor = 0;
  private openFramesSinceFloor = 0;
  private static readonly RECOMPUTE_EVERY = 250; // ~5s of open-line audio

  /** Rolling reference: the quietest sustained energy seen (noise floor). */
  private get noiseFloor(): number {
    return this.cachedNoiseFloor;
  }

  private now(): number {
    return Date.now() - this.startedAt;
  }

  event(type: string, detail?: Record<string, unknown>): void {
    if (this.timeline.length < MAX_TIMELINE) {
      this.timeline.push({ t: this.now(), type, ...(detail ? { detail } : {}) });
    }
    if (type === 'speech_started') this.lastSpeechStartedAt = Date.now();
  }

  /**
   * Feed one inbound Twilio media frame.
   * @param payloadB64 base64 μ-law audio
   * @param duplex     true if agent audio was playing when this arrived
   * @param msSinceAudioStart ms into the agent's current utterance (-1 if idle)
   */
  onInboundFrame(payloadB64: string, duplex: boolean, msSinceAudioStart: number): void {
    let buf: Buffer;
    try {
      buf = Buffer.from(payloadB64, 'base64');
    } catch {
      return;
    }
    if (buf.length === 0) return;

    let sumSquares = 0;
    for (let i = 0; i < buf.length; i++) {
      const s = ulawToLinear(buf[i]) / 32768;
      sumSquares += s * s;
    }
    const rms = Math.sqrt(sumSquares / buf.length);
    const t = this.now();

    this.frames.push({ t, rms, duplex });
    if (this.frames.length > 4000) this.frames.shift();
    const bucket = duplex ? this.duplex : this.openLine;
    bucket.samples.push(rms);
    // Bound the buckets (review 2026-07-20: unbounded, ~45k floats/bucket on a
    // 30-min call). Amortized O(1): splice only when well over the cap, keeping
    // the most recent window — plenty for a stable percentile distribution.
    if (bucket.samples.length > 30_000) {
      bucket.samples.splice(0, bucket.samples.length - 20_000);
    }

    // Refresh the cached noise floor occasionally from open-line audio only.
    // The sort cost is now amortized to once per ~5s, off the per-frame path.
    if (!duplex && ++this.openFramesSinceFloor >= CallDiagnostics.RECOMPUTE_EVERY) {
      this.openFramesSinceFloor = 0;
      const s = stats(this.openLine.samples);
      if (s) this.cachedNoiseFloor = s.p50;
    }

    // Missed barge-in detector: a sustained loud stretch while WE are talking
    // that never produced a speech_started. That is the exact fingerprint of
    // "I talked over him and nothing happened."
    if (duplex) {
      const floor = this.noiseFloor;
      const loud = rms > Math.max(0.02, floor * 3);
      if (loud) {
        if (!this.loudDuplexRunStart) {
          this.loudDuplexRunStart = t;
          this.loudDuplexRunPeak = rms;
        } else {
          this.loudDuplexRunPeak = Math.max(this.loudDuplexRunPeak, rms);
        }
      } else if (this.loudDuplexRunStart) {
        const runMs = t - this.loudDuplexRunStart;
        const firedRecently = Date.now() - this.lastSpeechStartedAt < 1500;
        // >300ms of loud audio is a person talking, not a click or a blip.
        if (runMs >= 300 && !firedRecently && this.missedBargeIns.length < 50) {
          this.missedBargeIns.push({
            t: this.loudDuplexRunStart,
            rms: Number(this.loudDuplexRunPeak.toFixed(4)),
            msSinceAudioStart,
          });
        }
        this.loudDuplexRunStart = 0;
        this.loudDuplexRunPeak = 0;
      }
    } else if (this.loudDuplexRunStart) {
      this.loudDuplexRunStart = 0;
      this.loudDuplexRunPeak = 0;
    }
  }

  /** Energy over the window immediately before now — what tripped VAD. */
  recentRms(windowMs = RECENT_WINDOW_MS): number {
    const cutoff = this.now() - windowMs;
    const recent = this.frames.filter((f) => f.t >= cutoff);
    if (recent.length === 0) return 0;
    const peak = Math.max(...recent.map((f) => f.rms));
    return Number(peak.toFixed(4));
  }

  recordSpeechStarted(duplex: boolean, msSinceAudioStart: number): void {
    const rms = this.recentRms();
    this.triggerEnergy.push({ t: this.now(), rms, duplex, msSinceAudioStart });
    this.event('speech_started', { rms, duplex, msSinceAudioStart });
  }

  /**
   * The verdict block. Reads as an argument, not a dump: does the data
   * support separating echo from speech by loudness at all?
   */
  summary() {
    const open = stats(this.openLine.samples);
    const dup = stats(this.duplex.samples);
    const triggersWhileSpeaking = this.triggerEnergy.filter((e) => e.duplex);
    const triggersWhileIdle = this.triggerEnergy.filter((e) => !e.duplex);

    const avg = (xs: number[]) =>
      xs.length ? Number((xs.reduce((a, b) => a + b, 0) / xs.length).toFixed(4)) : null;

    // Echo level proxy: duplex energy when the caller almost certainly wasn't
    // talking — the typical (median) duplex frame.
    const echoLevel = dup?.p50 ?? null;
    // Real speech proxy: loud open-line frames.
    const speechLevel = open?.p99 ?? null;
    const separable =
      echoLevel !== null && speechLevel !== null && echoLevel > 0
        ? Number((speechLevel / echoLevel).toFixed(2))
        : null;

    return {
      inboundEnergy: { openLine: open, duplex: dup },
      // >~3x means a loudness gate can plausibly separate caller from echo.
      // Near 1x means it cannot, and VAD tuning is the wrong lever entirely.
      speechToEchoRatio: separable,
      vadTriggers: {
        total: this.triggerEnergy.length,
        whileAgentSpeaking: triggersWhileSpeaking.length,
        whileAgentIdle: triggersWhileIdle.length,
        avgRmsWhileSpeaking: avg(triggersWhileSpeaking.map((e) => e.rms)),
        avgRmsWhileIdle: avg(triggersWhileIdle.map((e) => e.rms)),
      },
      // The direct test of the "his speech was too quiet" claim.
      missedBargeIns: {
        count: this.missedBargeIns.length,
        samples: this.missedBargeIns.slice(0, 10),
      },
      timelineEntries: this.timeline.length,
    };
  }
}
