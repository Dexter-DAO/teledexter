import { randomUUID } from 'node:crypto';
import type { ServerResponse } from 'node:http';
import { PrimaryApiError } from './primaryApi.js';

export type InboundTimingStage = 'recover_attachment' | 'poll_attachment' | 'begin_admission' | 'early_begin_admission' | 'hold_response'
  | 'provider_identity' | 'open_context' | 'recover_binding' | 'refresh_context' | 'stream_response';
export interface InboundTimingEvent {
  event: 'broker_inbound_timing'; version: 1; callId: string; attemptId: string; spanId: string;
  stage: InboundTimingStage; phase: 'started' | 'completed' | 'failed' | 'aborted'; observedAt: string;
  elapsedMs?: number; primaryApiStatus?: number;
}
export type InboundTimingSink = (event: InboundTimingEvent) => void;

/** Diagnostics only: no provider proof, context, recipient or billing evidence. */
export function createInboundAttemptTiming(callId: string, sink?: InboundTimingSink,
  clock = { wall: Date.now, monotonic: () => performance.now() }) {
  const attemptId = randomUUID();
  function start(stage: InboundTimingStage) {
    const spanId = randomUUID(), began = clock.monotonic();
    let finished = false;
    function emit(phase: InboundTimingEvent['phase'], error?: unknown) {
      try {
        const primaryApiStatus = error instanceof PrimaryApiError && Number.isInteger(error.status)
          && error.status! >= 100 && error.status! <= 599 ? error.status : undefined;
        sink?.({ event: 'broker_inbound_timing', version: 1, callId, attemptId, spanId, stage, phase,
          observedAt: new Date(clock.wall()).toISOString(),
          ...(phase === 'started' ? {} : { elapsedMs: Math.round(Math.max(0, clock.monotonic() - began) * 1000) / 1000 }),
          ...(primaryApiStatus === undefined ? {} : { primaryApiStatus }) });
      } catch { /* Diagnostic output must not alter admission or recovery. */ }
    }
    emit('started');
    return (phase: Exclude<InboundTimingEvent['phase'], 'started'>, error?: unknown) => {
      if (finished) return;
      finished = true; emit(phase, error);
    };
  }
  async function measure<T>(stage: InboundTimingStage, operation: () => Promise<T>): Promise<T> {
    if (!sink) return operation();
    const finish = start(stage);
    try { const result = await operation(); finish('completed'); return result; }
    catch (error) { finish('failed', error); throw error; }
  }
  return { start, measure };
}

/** A server-flushed stream response is distinct from provider media/playback. */
export function observeInboundStreamResponse(callId: string, response: ServerResponse, sink: InboundTimingSink) {
  const finish = createInboundAttemptTiming(callId, sink).start('stream_response');
  response.once('finish', () => finish('completed'));
  response.once('close', () => finish(response.writableFinished ? 'completed' : 'aborted'));
}

export const logInboundTiming: InboundTimingSink = event => console.info(JSON.stringify(event));

/** The early contact follows a successfully flushed hold, never an aborted response. */
export function observeInboundHoldResponse(callId: string, response: ServerResponse, sink: InboundTimingSink,
  onFinished: () => void) {
  const finish = createInboundAttemptTiming(callId, sink).start('hold_response');
  let aborted = false;
  response.once('finish', () => { if (!aborted) { finish('completed'); onFinished(); } });
  response.once('close', () => { aborted = !response.writableFinished; finish(aborted ? 'aborted' : 'completed'); });
}
