import { z } from 'zod';
import type { PrimaryTurn } from './primaryApi.js';
import type { LiveBackendChannel, LiveDelegationRequest } from '../live/types.js';

/** Produced by the backend's completedCallerGroups/stableContext admission.
 * Audio offsets provide ordering; firstEventId owns the original source. */
export interface FrozenCallerTurn {
  sessionId: string;
  firstEventId: string;
  callerRevision: number;
  firstSequence: number;
  lastSequence: number;
  startMs: number;
  endMs: number;
  audioThroughMs: number;
  observedSpeechAtMs: number;
  turn: PrimaryTurn & { revision: 1; role: 'user' };
}

export interface CallerTurnFreezer {
  /** Freeze without submit/reply/capture HTTP under the existing call lock. */
  freeze(request: LiveDelegationRequest, channel: LiveBackendChannel,
    usedSourceIds: readonly string[]): Promise<FrozenCallerTurn | undefined>;
}

/** Read from the source backend immediately before fresh admission, under its
 * existing call lock. Non-silent input advances these values before captions.
 * Silence and later delivery generations cannot substitute a source frontier. */
export function assertFrozenCallerSpeechCurrent(frozen: FrozenCallerTurn, current: {
  sessionId: string; audioThroughMs: number | undefined; observedSpeechAtMs: number | undefined;
}): void {
  if (current.sessionId !== frozen.sessionId || current.audioThroughMs !== frozen.audioThroughMs
    || current.observedSpeechAtMs !== frozen.observedSpeechAtMs)
    throw new Error('caller_route_acoustic_changed');
}

const uuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
const nonnegative = z.number().finite().nonnegative();
const sequence = z.number().int().safe().nonnegative();
const time = z.iso.datetime({ offset: true });

/** Same durable raw-source shape used by selected-question voice evidence. */
export const frozenCallerTurnSchema = z.object({ sessionId: z.string().min(1).max(200), firstEventId: z.string().min(1),
  callerRevision: sequence, firstSequence: sequence.positive(), lastSequence: sequence.positive(),
  startMs: nonnegative, endMs: nonnegative, audioThroughMs: nonnegative, observedSpeechAtMs: nonnegative,
  turn: z.object({ sourceEventId: uuid, revision: z.literal(1), itemId: z.string().min(1),
    role: z.literal('user'), text: z.string().refine(value => value.trim().length > 0 && [...value].length <= 4000
      && !value.includes('\0') && !/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/u.test(value)),
    occurredAt: time }).strict(),
}).strict().refine(value => value.lastSequence >= value.firstSequence && value.endMs >= value.startMs);
