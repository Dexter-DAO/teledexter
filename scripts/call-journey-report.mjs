import fs from 'node:fs';
import path from 'node:path';

// A read-only projection of existing evidence. Do not import BrokerStore: opening
// the live journal through its writer would acquire or alter the writer lock.
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const STAGES = new Set(['recover_attachment', 'poll_attachment', 'begin_admission', 'provider_identity',
  'open_context', 'recover_binding', 'refresh_context', 'stream_response']);
const CALL_STATES = new Set(['prepared', 'dialing', 'queued', 'ringing', 'in-progress', 'completed', 'busy', 'no-answer', 'canceled', 'failed', 'unknown']);
const REQUEST_STATES = new Set(['prepared', 'dispatching', 'dispatch_unknown', 'queued', 'running', 'waiting_for_reply', 'completed', 'failed', 'cancelled', 'not_admitted', 'admission_refused', 'admission_expired']);
const DELIVERIES = new Set(['accepted', 'unconfirmed', 'dispatching', 'interrupted', 'unavailable', 'cancelled', 'failed', 'refused']);
const ERRORS = new Set(['invalid_configuration', 'invalid_request', 'request_timeout', 'request_failed', 'http_error', 'response_too_large', 'invalid_response', 'identity_mismatch']);
const MAX_JOURNAL_BYTES = 32 * 1024 * 1024;
const MAX_LOG_BYTES = 8 * 1024 * 1024;
const MAX_LINE_BYTES = 16 * 1024;
const MAX_SPANS = 2048;
const MAX_REQUESTS = 512;
const MAX_GENERATIONS = 128;
const record = value => value !== null && typeof value === 'object' && !Array.isArray(value) ? value : {};
const time = value => typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(value)
  && Number.isFinite(Date.parse(value)) ? new Date(value).toISOString() : null;
const id = value => typeof value === 'string' && UUID.test(value) ? value : null;
const choice = (value, allowed) => allowed.has(value) ? value : value === undefined ? null : 'other';
const elapsed = (start, end) => start && end && Date.parse(end) >= Date.parse(start) ? Date.parse(end) - Date.parse(start) : null;
const httpStatus = value => Number.isInteger(value) && value >= 100 && value <= 599 ? value : null;

function diagnostic(value) {
  if (!value || typeof value !== 'object') return null;
  return { code: choice(value.code, ERRORS), status: httpStatus(value.status),
    retryable: typeof value.retryable === 'boolean' ? value.retryable : null, observedAt: time(value.observedAt) };
}

function spansFromLog(text, callId) {
  const spans = new Map(); let dropped = 0, oversizedLines = 0;
  for (const line of text.split('\n')) {
    if (Buffer.byteLength(line) > MAX_LINE_BYTES) { oversizedLines++; continue; }
    // PM2 may prefix JSON lines with its process/date decoration. No raw line is
    // included in the output, even for malformed JSON or rejected event fields.
    const begin = line.indexOf('{'); if (begin < 0) continue;
    let event; try { event = JSON.parse(line.slice(begin)); } catch { continue; }
    if (event?.event !== 'broker_inbound_timing' || event.version !== 1 || event.callId !== callId
      || !id(event.attemptId) || !id(event.spanId) || !STAGES.has(event.stage) || !time(event.observedAt)
      || !['started', 'completed', 'failed', 'aborted'].includes(event.phase)) continue;
    const key = `${event.attemptId}:${event.spanId}`;
    if (!spans.has(key)) {
      if (spans.size === MAX_SPANS) { dropped++; continue; }
      spans.set(key, { attemptId: event.attemptId, spanId: event.spanId, stage: event.stage,
        startedAt: null, finishedAt: null, phase: 'started', elapsedMs: null, primaryApiStatus: null });
    }
    const span = spans.get(key);
    if (span.stage !== event.stage) { dropped++; continue; }
    if (event.phase === 'started') span.startedAt ??= time(event.observedAt);
    else if (!span.finishedAt) Object.assign(span, { finishedAt: time(event.observedAt), phase: event.phase,
      elapsedMs: Number.isFinite(event.elapsedMs) && event.elapsedMs >= 0 ? event.elapsedMs : null,
      primaryApiStatus: httpStatus(event.primaryApiStatus) });
  }
  return { spans: [...spans.values()].sort((a, b) => (a.startedAt ?? a.finishedAt).localeCompare(b.startedAt ?? b.finishedAt)),
    dropped, oversizedLines };
}

export function createCallJourneyReport(journal, logText = '', options = {}) {
  if (journal?.version !== 1 || !journal.calls || typeof journal.calls !== 'object' || Array.isArray(journal.calls))
    throw new Error('journey_journal_invalid');
  if (options.callId !== undefined && !id(options.callId)) throw new Error('journey_call_id_invalid');
  const calls = Object.entries(journal.calls).filter(([key, call]) => id(key) && call?.id === key && time(call.createdAt));
  const selected = options.callId ? calls.find(([key]) => key === options.callId)
    : calls.sort((a, b) => Date.parse(b[1].createdAt) - Date.parse(a[1].createdAt))[0];
  if (!selected) throw new Error('journey_call_not_found');
  const [callId, call] = selected;
  const inbound = record(call.inbound), journey = record(call.journey);
  const timestamps = {
    callCreatedAt: time(call.createdAt), continuationObservedAt: time(inbound.continuation?.observedAt),
    admissionAttemptedAt: time(inbound.beginAttemptedAt), consumptionAttemptedAt: time(inbound.consumeAttemptedAt),
    consumedAt: inbound.remote?.state === 'bound' ? time(inbound.remote.consumedAt) : null,
    // streamStartedAt: the provider media stream started (greeting only until Live attaches).
    // mediaStartedAt: Live attached; the time of the first caller sample it receives.
    streamStartedAt: time(journey.streamStartedAt), mediaStartedAt: time(call.mediaStartedAt), liveReadyAt: time(journey.liveReadyAt),
    firstAudioSentAt: time(journey.firstAudioSentAt), firstPlaybackConfirmedAt: time(journey.firstPlaybackConfirmedAt),
    // Pre-rendered greeting clip: started at stream start, played = every clip mark acknowledged.
    greetingClipStartedAt: time(journey.greetingClipStartedAt), greetingClipPlayedAt: time(journey.greetingClipPlayedAt),
    endedAt: time(call.endedAt),
  };
  const sourceGenerations = Array.isArray(call.liveGenerations?.segments) ? call.liveGenerations.segments : [];
  const generations = sourceGenerations.slice(0, MAX_GENERATIONS).map(segment => ({
    ordinal: Number.isSafeInteger(segment.ordinal) && segment.ordinal > 0 ? segment.ordinal : null,
    createdAt: time(segment.createdAt), providerReadyAt: time(segment.readyAt),
    providerOpenMs: elapsed(time(segment.createdAt), time(segment.readyAt)),
  }));
  const sourceRequests = Object.entries(record(call.delegations));
  const requests = sourceRequests.slice(0, MAX_REQUESTS).map(([key, entry]) => {
    const timing = record(entry.timing), savedAt = time(timing.firstSavedAt), dispatchAt = time(timing.firstDispatchAt);
    const answerAt = time(timing.firstAnswerAvailableAt), deliveryAt = time(timing.firstDeliveryAcceptedAt);
    return { requestId: id(entry.requestId) ?? id(key), status: choice(entry.status, REQUEST_STATES),
      resultState: choice(entry.primaryResult?.state, REQUEST_STATES), delivery: choice(entry.delivery, DELIVERIES),
      savedAt, dispatchAt, answerAvailableAt: answerAt, publicationAcceptedAt: deliveryAt,
      dispatchWaitMs: elapsed(savedAt, dispatchAt), dispatchToAnswerMs: elapsed(dispatchAt, answerAt),
      answerToPublicationMs: elapsed(answerAt, deliveryAt),
      submitError: diagnostic(entry.submitError), lastResultReadError: diagnostic(entry.lastResultReadError) };
  });
  const parsed = spansFromLog(logText, callId);
  return {
    version: 1, callId, direction: choice(call.direction, new Set(['inbound', 'outbound'])), status: choice(call.status, CALL_STATES),
    timestamps,
    durationsMs: { createdToMedia: elapsed(timestamps.callCreatedAt, timestamps.mediaStartedAt),
      consumption: elapsed(timestamps.consumptionAttemptedAt, timestamps.consumedAt),
      consumedToMedia: elapsed(timestamps.consumedAt, timestamps.mediaStartedAt),
      mediaToFirstProviderOpen: elapsed(timestamps.mediaStartedAt, generations[0]?.createdAt),
      createdToFirstProviderReady: elapsed(timestamps.callCreatedAt, generations[0]?.providerReadyAt),
      mediaToLiveReady: elapsed(timestamps.mediaStartedAt, timestamps.liveReadyAt),
      createdToFirstAudio: elapsed(timestamps.callCreatedAt, timestamps.firstAudioSentAt),
      createdToFirstPlayback: elapsed(timestamps.callCreatedAt, timestamps.firstPlaybackConfirmedAt),
      createdToStream: elapsed(timestamps.callCreatedAt, timestamps.streamStartedAt),
      streamToGreetingClip: elapsed(timestamps.streamStartedAt, timestamps.greetingClipStartedAt),
      greetingClipPlayedToMedia: elapsed(timestamps.greetingClipPlayedAt, timestamps.mediaStartedAt) },
    generations, stages: parsed.spans, requests,
    coverage: { logProvided: options.logProvided ?? Boolean(logText), logTailTruncated: options.logTailTruncated === true,
      droppedSpans: parsed.dropped, oversizedLogLines: parsed.oversizedLines,
      omittedRequests: Math.max(0, sourceRequests.length - requests.length),
      omittedGenerations: Math.max(0, sourceGenerations.length - generations.length),
      firstAudioRecorded: timestamps.firstAudioSentAt !== null, firstPlaybackRecorded: timestamps.firstPlaybackConfirmedAt !== null,
      requestsWithoutTiming: requests.filter(request => !request.savedAt).length },
    interpretation: {
      stages: 'Spans may overlap; use their individual elapsed times rather than adding them.',
      playback: 'First playback is a call-wide carrier mark acknowledgment. It does not identify playback of a particular answer.',
      results: 'Publication acceptance records delivery into the voice session. Per-tool spans and per-answer playback are absent from this journal.',
      absent: 'Null means the selected evidence has no usable timestamp; it does not mean zero elapsed time.',
    },
  };
}

function readBounded(file, maxBytes, tail = false) {
  if (!path.isAbsolute(file)) throw new Error('journey_path_must_be_absolute');
  const fd = fs.openSync(file, 'r');
  try {
    const stat = fs.fstatSync(fd); if (!stat.isFile()) throw new Error('journey_input_not_file');
    if (!tail && stat.size > maxBytes) throw new Error('journey_journal_too_large');
    const bytes = Math.min(stat.size, maxBytes), buffer = Buffer.alloc(bytes), start = stat.size - bytes;
    let read = 0;
    while (read < bytes) { const count = fs.readSync(fd, buffer, read, bytes - read, start + read); if (!count) break; read += count; }
    let text = buffer.subarray(0, read).toString('utf8');
    if (tail && start) text = text.slice(text.indexOf('\n') + 1);
    return { text, truncated: start > 0 };
  } finally { fs.closeSync(fd); }
}

export function runCallJourneyReport(args) {
  const parsed = new Map();
  for (let index = 0; index < args.length; index += 2) {
    const flag = args[index], value = args[index + 1];
    if (!['--journal', '--log', '--call-id'].includes(flag) || !value || parsed.has(flag)) throw new Error('journey_arguments_invalid');
    parsed.set(flag, value);
  }
  if (!parsed.has('--journal')) throw new Error('journey_journal_required');
  const source = readBounded(parsed.get('--journal'), MAX_JOURNAL_BYTES);
  let journal; try { journal = JSON.parse(source.text); } catch { throw new Error('journey_journal_invalid'); }
  const log = parsed.has('--log') ? readBounded(parsed.get('--log'), MAX_LOG_BYTES, true) : { text: '', truncated: false };
  return createCallJourneyReport(journal, log.text, { callId: parsed.get('--call-id'),
    logProvided: parsed.has('--log'), logTailTruncated: log.truncated });
}
