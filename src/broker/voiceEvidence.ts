import type { BrokerStore } from './store.js';
import type { LiveSessionClosed } from '../live/types.js';
import { startupFailureCodeSchema } from './liveGeneration.js';

/** Codes that may enter the call record. An unrecognized provider string is recorded as a generic code
 * (and logged as such), so provider-chosen text never reaches the journal. */
const KNOWN_CODES = new Set<string>([...startupFailureCodeSchema.options,
  // session.closed reasons (live-conversations guide) and application close reasons
  'close_requested', 'expired', 'content', 'remote_hangup', 'connection_lost', 'application_close', 'call_stopped',
  'context_replaced', 'context_unavailable', 'finalization_timeout', 'close_send_failed', 'session_closed',
  // Live session failures (src/live/session.ts)
  'invalid_input_audio', 'startup_audio_overflow', 'invalid_transcript', 'transcript_event_identity_changed',
  'transcript_limit', 'transcript_capture_failed', 'invalid_client_delegation', 'delegation_identity_changed',
  'delegation_limit', 'audio_delivery_failed', 'invalid_setup_response_identity', 'uncorrelated_setup_response',
  'unknown_setup_delegation', 'invalid_setup_tool', 'setup_tool_identity_changed', 'setup_tool_limit', 'setup_tool_failed',
  // Other documented provider errors
  'input_audio_rate_limit_exceeded', 'immutable_field_update', 'responses_validation_failed',
]);
const knownCode = (value: string) => KNOWN_CODES.has(value) ? value : 'unrecognized_provider_code';

export interface VoiceCloseRecord {
  at: string;
  generationId?: string;
  sessionId?: string;
  /** The real reason the provider session ended, e.g. caller hangup, close_requested, or the provider's error code. */
  reason: string;
  finalized: boolean;
  providerError?: { code: string };
}

type Store = Pick<BrokerStore, 'get' | 'update'>;

/** Every voice session's close reason and provider error, kept on the call record and logged. */
export function recordVoiceClose(store: Store, callId: string, result: LiveSessionClosed, generationId?: string): void {
  const record: VoiceCloseRecord = { at: new Date().toISOString(), ...(generationId ? { generationId } : {}),
    ...(result.sessionId ? { sessionId: result.sessionId } : {}), reason: knownCode(result.reason), finalized: result.finalized,
    ...(result.providerError ? { providerError: { code: knownCode(result.providerError.code) } } : {}) };
  try { console.info(JSON.stringify({ event: 'broker_voice_closed', callId, ...record })); } catch { /* Telemetry only. */ }
  // Evidence must never affect the call. Bounded so a reconnecting call cannot grow its record.
  try { const call = store.get(callId); if (call) store.update(callId, { voiceCloses: [...call.voiceCloses ?? [], record].slice(-20) }); }
  catch { /* The log line above still carries it. */ }
}
