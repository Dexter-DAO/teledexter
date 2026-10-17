import type { BrokerCall } from './store.js';
import type { BrokerDelegationRecord } from './backend.js';
import type { LiveTranscriptSnapshot } from '../live/types.js';

const oneLine = (text: string) => text.replace(/\s+/g, ' ').trim();
function duration(start?: string, end?: string): string {
  const ms = start && end ? Date.parse(end) - Date.parse(start) : NaN;
  return Number.isFinite(ms) && ms >= 0 ? `${(ms / 1000).toFixed(3)}s` : 'unknown';
}

/** Read existing evidence only; fragment text already contains its own word spacing. */
export function formatCallTranscript(call: BrokerCall): string {
  const lines = [`CALL ${call.id} | ${call.direction} | start=${call.createdAt} | end=${call.endedAt ?? 'unknown'} | duration=${duration(call.createdAt, call.endedAt)}`];
  const fragments = call.liveGenerations?.segments.flatMap(segment => segment.transcript.fragments)
    ?? (call.transcript as LiveTranscriptSnapshot | undefined)?.fragments ?? [];
  let role: 'user' | 'assistant' | undefined;
  let text = '';
  const flush = () => { if (role && oneLine(text)) lines.push(`${role === 'user' ? 'CALLER' : 'DEXTER'}: ${oneLine(text)}`); };
  for (const fragment of fragments) {
    if (fragment.role !== role) { flush(); role = fragment.role; text = ''; }
    text += fragment.text;
  }
  flush();
  for (const saved of Object.values(call.delegations)) {
    const delegation = saved as Partial<BrokerDelegationRecord>;
    // Elapsed journal lifetime includes dispatch, Primary work and result delivery.
    lines.push(`PRIMARY: ${oneLine(delegation.submission?.text ?? '(text unavailable)')} | status=${saved.status} | duration=${duration(delegation.timing?.firstSavedAt, saved.updatedAt)}`);
  }
  return lines.join('\n');
}
