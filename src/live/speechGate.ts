import { faithfulRelay, normalizeSpeech, speechSentences } from './speechFacts.js';
import { pcmToPcmu } from './pcm.js';

const FRAME_BYTES = 960, QUIET_MS = 300, TRANSCRIPT_WAIT_MS = 1500;
const normalize = normalizeSpeech;
const sentences = speechSentences;
const neutral = (text: string) => /^(on it|still working on it|i'm on it|i am on it)$/.test(normalize(text));
type Fragment = { id: string; text: string; start: number; end: number };
type Evidence = { text: string; parts: string[]; cursor: number; terminal: boolean; inFlight: number; failures: number; failureReported: boolean;
  resolve(heard: boolean): void };
type Segment = { id: number; start: number; end: number; speechEnd: number; receivedAt: number;
  frames: Buffer[]; texts: Fragment[]; epoch: number; timer?: ReturnType<typeof setTimeout> };

/** Checks reflected, timestamped audio before any of it reaches the media bridge. */
export function createSpeechGate(options: {
  release(audio: Buffer, played: (heard: boolean) => void, segment: { id: number; receivedAt: number; requestId?: string }): void;
  steer(text: string): void;
  clear(requestId?: string): void;
  event(event: Record<string, unknown>): void;
}) {
  const requests = new Set<string>(), evidence = new Map<string, Evidence>();
  let segment: Segment | undefined, waiting: Segment[] = [], fragments: Fragment[] = [];
  let serial = 0, epoch = 0, closed = false, lastEnd = -1;
  let lastActivity = Date.now(), lastHolding = '', holdingRequested = '';
  let holdTimer: ReturnType<typeof setTimeout> | undefined;
  const active = () => requests.size > 0;
  const event = (type: string, data: Record<string, unknown> = {}) => options.event({ type, ...data });
  const scheduleHold = () => {
    clearTimeout(holdTimer);
    if (!active() || closed) return;
    holdTimer = setTimeout(() => {
      if (Date.now() - lastActivity >= 5000) {
        if ([...evidence.values()].some(item => item.inFlight > 0)) {
          lastActivity = Date.now(); scheduleHold(); return;
        }
        if ([...evidence.values()].some(item => item.terminal && item.failures < 2)) {
          holdingRequested = '';
          options.steer('Read the latest supplied Primary result exactly. Its outcome is available; do not replace it with a holding line.');
        } else {
          holdingRequested = normalize(lastHolding).toLowerCase() === 'on it' ? 'Still working on it.' : 'On it.';
          options.steer(`Say exactly "${holdingRequested}" Nothing else. Request progress must use the latest supplied commentary exactly.`);
        }
        lastActivity = Date.now();
      }
      scheduleHold();
    }, Math.max(1, 5000 - (Date.now() - lastActivity)));
    holdTimer.unref?.();
  };
  const discard = (value: Segment, reason: string) => {
    clearTimeout(value.timer);
    if (['unsupported_speech', 'transcript_timeout'].includes(reason)) {
      for (const [requestId, item] of evidence) if (item.terminal && item.cursor < item.parts.length) {
        item.failures++;
        if (item.failures >= 2) {
          if (!item.failureReported) {
            item.failureReported = true;
            event('broker_speech_relay_failed', { requestId, discardedAttempts: item.failures, pendingRequests: requests.size });
          }
          if (Date.now() - lastActivity >= 5000) {
            holdingRequested = normalize(lastHolding) === 'on it' ? 'Still working on it.' : 'On it.';
            options.steer(`Say exactly "${holdingRequested}" Then relay the supplied result, preserving its facts and outcome: ${item.text}`);
          } else options.steer(`Relay the supplied result, preserving its facts and outcome: ${item.text}`);
        }
      }
    }
    event('live.speech_gate.discarded', { segmentId: value.id, reason, startMs: value.start, endMs: value.end });
    scheduleHold();
  };
  const clearSegments = (reason: string) => {
    epoch++;
    if (segment) discard(segment, reason);
    for (const value of waiting) discard(value, reason);
    segment = undefined; waiting = []; fragments = [];
  };
  const check = (value: Segment): boolean => {
    if (value.epoch !== epoch || closed) return true;
    const texts = value.texts.sort((a, b) => a.start - b.start);
    if (texts[0]?.start > value.start + 100) return false;
    let through = value.start;
    for (const text of texts) {
      if (text.start < value.start - 100 || text.end > value.speechEnd + 100) return false;
      if (text.start > through + 1000) return false;
      through = Math.max(through, text.end);
    }
    if (!texts.length || through < value.speechEnd - 80) return false;
    const text = texts.map(f => f.text).join(''), normalized = normalize(text);
    let matched: { id: string; item: Evidence; count: number } | undefined;
    for (const [id, item] of evidence) {
      for (let count = 1; count <= item.parts.length - item.cursor; count++) {
        if (faithfulRelay(text, item.parts.slice(item.cursor, item.cursor + count).join(' ')) &&
            faithfulRelay(item.parts.slice(item.cursor, item.cursor + count).join(' '), text)) {
          matched = { id, item, count }; break;
        }
      }
      if (matched) break;
    }
    const isHolding = neutral(text);
    const repeated = isHolding && normalize(lastHolding).toLowerCase() === normalized.toLowerCase();
    if (!matched && (!isHolding || repeated || isHolding && !holdingRequested)) { discard(value, repeated ? 'repeated_hold' : 'unsupported_speech'); return true; }
    clearTimeout(value.timer);
    const approvedEpoch = epoch;
    // Reserve the sentence range so a following segment cannot replay it while
    // the carrier is acknowledging the previous segment.
    if (matched) { matched.item.cursor += matched.count; matched.item.inFlight++; matched.item.failures = 0; }
    if (isHolding) lastHolding = text.trim();
    holdingRequested = ''; lastActivity = Date.now(); scheduleHold();
    event('live.speech_gate.released', { segmentId: value.id, text, startMs: value.start, endMs: value.end,
      latencyMs: Date.now() - value.receivedAt, transcriptWaitMs: Math.max(0, Date.now() - value.receivedAt - (value.end - value.start)),
      source: matched ? 'primary' : 'holding' });
    let settled = false;
    options.release(pcmToPcmu(Buffer.concat(value.frames)), heard => {
      if (settled) return; settled = true;
      if (!matched || ![...evidence.values()].includes(matched.item) || approvedEpoch !== epoch) return;
      matched.item.inFlight--;
      if (!heard) { matched.item.resolve(false); matched.item.cursor = 0; return; }
      if (matched.item.cursor === matched.item.parts.length && matched.item.inFlight === 0) {
        matched.item.resolve(true);
        if (matched.item.terminal) {
          const currentId = [...evidence].find(([, item]) => item === matched.item)?.[0];
          if (currentId) { requests.delete(currentId); evidence.delete(currentId); }
          if (!active()) {
            clearSegments('terminal_spoken');
            for (const remaining of evidence.values()) remaining.resolve(false); evidence.clear();
            options.clear();
            event('live.speech_gate.opened');
          }
        }
      }
      scheduleHold();
    }, { id: value.id, receivedAt: value.receivedAt, requestId: matched?.id });
    return true;
  };
  const drain = () => {
    while (waiting.length && check(waiting[0])) waiting.shift();
  };
  const finishSegment = () => {
    if (!segment) return;
    const value = segment; segment = undefined; clearTimeout(value.timer);
    value.texts = fragments.filter(f => f.end > value.start && f.start < value.speechEnd);
    waiting.push(value);
    value.timer = setTimeout(() => {
      if (!waiting.includes(value)) return;
      discard(value, 'transcript_timeout'); waiting = waiting.filter(s => s !== value); drain();
    }, TRANSCRIPT_WAIT_MS);
    value.timer.unref?.();
    drain();
  };
  const splitSentences = () => {
    if (!segment) return;
    const boundary = fragments.find(f => f.start >= segment!.start && f.end > segment!.start && f.end < segment!.end
      && /[.!?]["']?\s*$/.test(f.text) && fragments.some(next => next.start >= f.end && next.start <= f.end + 80));
    if (!boundary) return;
    const bytes = Math.round((boundary.end - segment.start) * 48);
    const audio = Buffer.concat(segment.frames);
    if (bytes <= 0 || bytes >= audio.length || bytes % 2) return;
    const rest = { ...segment, id: ++serial, start: boundary.end, receivedAt: segment.receivedAt + boundary.end - segment.start,
      frames: [audio.subarray(bytes)], texts: [] };
    segment.frames = [audio.subarray(0, bytes)]; segment.end = boundary.end; segment.speechEnd = boundary.end;
    finishSegment(); segment = rest;
    rest.timer = setTimeout(finishSegment, 1200); rest.timer.unref?.();
    splitSentences();
  };
  const statusRequested = () => {
    if (!active()) return;
    holdingRequested = normalize(lastHolding) === 'on it' ? 'Still working on it.' : 'On it.';
    const latest = [...evidence.values()].at(-1);
    if (latest && !latest.terminal && !latest.inFlight && latest.cursor === latest.parts.length) latest.cursor = 0;
    const remaining = latest?.parts.slice(latest.cursor).join(' ');
    options.steer(remaining ? `Relay this delivered update: ${remaining}` : `Say exactly "${holdingRequested}"`);
    scheduleHold();
  };
  return {
    active,
    has: (id: string) => requests.has(id),
    pending(id: string, aliases: readonly string[] = []) {
      if (closed) return;
      const wasActive = active();
      requests.add(id); // Install the canonical id before removing aliases.
      for (const alias of aliases) if (alias !== id) {
        requests.delete(alias);
        const prior = evidence.get(alias);
        if (prior && !evidence.has(id)) evidence.set(id, prior);
        evidence.delete(alias);
      }
      if (!wasActive) { clearSegments('request_started'); options.clear(); lastActivity = Date.now(); holdingRequested = 'On it.'; }
      scheduleHold();
    },
    release(id: string) {
      if (!requests.delete(id)) return;
      evidence.get(id)?.resolve(false); evidence.delete(id);
      options.clear(id);
      if (!active()) { clearSegments('delegation_unadmitted'); options.clear(); event('live.speech_gate.opened'); }
      scheduleHold();
    },
    deliver(id: string, text: string, terminal: boolean) {
      const prior = evidence.get(id);
      prior?.resolve(false); holdingRequested = '';
      // Invalidate only this request's approved playback. Other requests retain
      // their cursor and acknowledgment; raw buffered audio is checked against
      // the latest evidence when its transcript arrives.
      evidence.delete(id);
      options.clear(id);
      let resolve!: (heard: boolean) => void;
      const heard = new Promise<boolean>(done => { resolve = done; });
      evidence.set(id, { text, parts: sentences(text), cursor: 0, terminal, inFlight: 0,
        failures: prior?.text === text ? prior.failures : 0, failureReported: prior?.text === text ? prior.failureReported : false, resolve });
      lastActivity = Date.now(); scheduleHold();
      return heard;
    },
    transcript(id: string, text: string, start: number, end: number) {
      if (!active() || fragments.some(f => f.id === id)) return;
      const fragment = { id, text, start, end }; fragments.push(fragment);
      fragments = fragments.filter(f => f.end >= lastEnd - 35_000).slice(-2000);
      for (const value of waiting) if (end > value.start && start < value.speechEnd && !value.texts.some(f => f.id === id)) value.texts.push(fragment);
      // A late transcript covering already-quiet audio must validate that whole
      // segment before sentence splitting can release a terminal prefix.
      if (segment && segment.end > segment.speechEnd && fragments.some(f => f.end >= segment!.speechEnd - 80
        && f.end <= segment!.speechEnd + 80 && /[.!?]["']?\s*$/.test(f.text))) finishSegment();
      else splitSentences();
      drain();
    },
    audio(pcm: Buffer, start: number, end: number) {
      if (!active() || closed) return;
      if (!Number.isFinite(start) || !Number.isFinite(end) || start < lastEnd || end <= start
        || Math.abs(pcm.length / 48 - (end - start)) > 2 || pcm.length % 2) {
        clearSegments('invalid_audio_alignment'); return;
      }
      if (lastEnd >= 0 && start > lastEnd + 40 && segment) { discard(segment, 'audio_gap'); segment = undefined; }
      lastEnd = end;
      for (let offset = 0; offset < pcm.length; offset += FRAME_BYTES) {
        const frame = pcm.subarray(offset, Math.min(offset + FRAME_BYTES, pcm.length));
        const at = start + offset / 48, to = at + frame.length / 48;
        let energy = 0;
        for (let n = 0; n < frame.length; n += 2) energy += frame.readInt16LE(n) ** 2;
        const voiced = Math.sqrt(energy / (frame.length / 2)) >= 100;
        if (!segment && voiced) segment = { id: ++serial, start: at, end: to, speechEnd: to, receivedAt: Date.now(), frames: [], texts: [], epoch };
        if (!segment) continue;
        clearTimeout(segment.timer);
        segment.timer = setTimeout(finishSegment, 1200); segment.timer.unref?.();
        segment.frames.push(Buffer.from(frame)); segment.end = to;
        if (voiced) segment.speechEnd = to;
        if (segment.end - segment.start > 30_000 || waiting.length >= 8) { discard(segment, 'segment_limit'); segment = undefined; }
        else if (!voiced && fragments.some(f => f.end >= segment!.speechEnd - 80 && f.end <= segment!.speechEnd + 80 && /[.!?]["']?\s*$/.test(f.text))) finishSegment();
        else if (to - segment.speechEnd >= QUIET_MS && (to - segment.speechEnd >= 1200
          || fragments.some(f => f.end >= segment!.speechEnd - 80 && f.end <= segment!.speechEnd + 80 && /[.!?]["']?\s*$/.test(f.text)))) finishSegment();
        else splitSentences();
      }
    },
    interrupt() { clearSegments('interrupted'); for (const item of evidence.values()) { item.cursor = 0; item.inFlight = 0; item.resolve(false); } },
    statusRequested: statusRequested,
    callerSpoke(text = '') {
      if (!text || /how.?s it going|status|any update|what.?s happening|still (?:there|working)|are you (?:there|done)/i.test(text)) statusRequested();
      else scheduleHold();
    },
    close() { closed = true; clearTimeout(holdTimer); clearSegments('closed'); for (const item of evidence.values()) item.resolve(false); requests.clear(); evidence.clear(); },
  };
}
