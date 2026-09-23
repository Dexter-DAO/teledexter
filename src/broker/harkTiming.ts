import type { HarkEndedTiming } from './harkJournal.js';

/** Qualify the installed Twilio SDK's completed-call timing; receipt time is unrelated. */
export function harkProviderTiming(observed: unknown): HarkEndedTiming | undefined {
  if (observed === null || typeof observed !== 'object') return undefined;
  try {
    const record = observed as { status?: unknown; startTime?: unknown; endTime?: unknown; duration?: unknown };
    const { status, startTime, endTime, duration } = record;
    if (status !== 'completed' || !(startTime instanceof Date) || !(endTime instanceof Date)
        || typeof duration !== 'string' || !/^\d+$/.test(duration)) return undefined;
    const startMs = Date.prototype.getTime.call(startTime);
    const endMs = Date.prototype.getTime.call(endTime);
    const durationSeconds = Number(duration);
    if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs < startMs
        || !Number.isSafeInteger(durationSeconds) || durationSeconds < 0
        || durationSeconds > Math.ceil((endMs - startMs) / 1000)) return undefined;
    // Provider start can include dialing/ringing, so its duration can be shorter
    // than the timestamp interval. Preserve the supplied seconds without rounding.
    const startedAt = new Date(startMs).toISOString();
    const endedAt = new Date(endMs).toISOString();
    if (!/^\d{4}-/.test(startedAt) || !/^\d{4}-/.test(endedAt)) return undefined;
    return { startedAt, endedAt, durationSeconds };
  } catch { return undefined; }
}
