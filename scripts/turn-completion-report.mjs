// Read-only journal analysis. Usage: node scripts/turn-completion-report.mjs calls.json AS_OF_ISO
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';

const [file, asOf] = process.argv.slice(2);
const end = Date.parse(asOf), start = end - 14 * 86400_000;
if (!file || !Number.isFinite(end)) throw new Error('Provide calls.json and an explicit ISO window end');
const bytes = readFileSync(file);
const calls = Object.values(JSON.parse(bytes).calls).filter(call => Date.parse(call.createdAt) >= start && Date.parse(call.createdAt) <= end);
const rows = [], missing = [];
let callerGroups = 0;
for (const call of calls) {
  for (const segment of call.liveGenerations?.segments ?? []) {
    const fragments = segment.transcript?.fragments.filter(fragment => fragment.role === 'user') ?? [];
    const groups = [];
    for (const fragment of fragments.sort((a, b) => a.startMs - b.startMs || a.sequence - b.sequence)) {
      const previous = groups.at(-1);
      if (!previous || fragment.startMs - Math.max(...previous.map(part => part.endMs)) > 1200) groups.push([fragment]);
      else previous.push(fragment);
    }
    callerGroups += groups.length;
    const completions = Object.entries(call.callerCompletions ?? {}).filter(([, cp]) => cp.sessionId === segment.sessionId);
    for (const group of groups) {
      const firstSequence = Math.min(...group.map(fragment => fragment.sequence));
      if (!completions.some(([, cp]) => cp.firstSequence === firstSequence))
        missing.push({ callId: call.id, sessionId: segment.sessionId, firstSequence, reason: 'no_completion_checkpoint' });
    }
    for (const [sourceId, completion] of completions) {
      const observed = Date.parse(completion.observedAt);
      const group = fragments.filter(fragment => fragment.sequence >= completion.firstSequence && fragment.sequence <= completion.lastSequence);
      // This is the most recent acoustic evidence at the checkpoint, not a
      // guaranteed one-to-one identification of speech from caption timestamps.
      const span = (call.sourceAcousticSpans?.[segment.sessionId] ?? [])
        .filter(span => span.lastSpeechAt <= observed).sort((a, b) => b.lastSpeechAt - a.lastSpeechAt)[0];
      if (!span || !call.mediaStartedAt || segment.inputFirstSample === undefined || !group.length) {
        missing.push({ callId: call.id, sourceId, reason: 'missing_acoustic_or_clock_evidence' }); continue;
      }
      const speechEnd = Date.parse(call.mediaStartedAt) + segment.inputFirstSample / 8 + span.endMs;
      rows.push({ callId: call.id, sourceId, sessionId: segment.sessionId, direction: call.direction,
        firstSequence: completion.firstSequence, lastSequence: completion.lastSequence,
        captionStartMs: Math.min(...group.map(fragment => fragment.startMs)), captionEndMs: Math.max(...group.map(fragment => fragment.endMs)),
        acousticStartMs: span.startMs, acousticEndMs: span.endMs, speechEndAt: new Date(speechEnd).toISOString(),
        lastSpeechReceivedAt: new Date(span.lastSpeechAt).toISOString(), completionObservedAt: completion.observedAt,
        gapMs: observed - speechEnd, receivedGapMs: observed - span.lastSpeechAt });
    }
  }
}
const endpoint = row => `${row.callId}/${row.sessionId}/${row.acousticEndMs}`;
const count = new Map();
for (const row of rows) count.set(endpoint(row), (count.get(endpoint(row)) ?? 0) + 1);
for (const row of rows) row.sharedAcousticEndpoint = count.get(endpoint(row)) > 1;
const distribution = values => {
  values.sort((a, b) => a - b);
  const percentile = p => values[Math.ceil(p * values.length) - 1] ?? null;
  return { n: values.length, p50: percentile(.5), p90: percentile(.9), max: percentile(1) };
};
console.log(JSON.stringify({ sourceSha256: createHash('sha256').update(bytes).digest('hex'),
  start: new Date(start).toISOString(), end: new Date(end).toISOString(), calls: calls.length, callerGroups,
  completionCheckpoints: calls.reduce((n, call) => n + Object.keys(call.callerCompletions ?? {}).length, 0),
  distinctAcousticEndpoints: count.size, sharedEndpointRows: rows.filter(row => row.sharedAcousticEndpoint).length,
  gapMs: distribution(rows.map(row => row.gapMs)), receivedGapMs: distribution(rows.map(row => row.receivedGapMs)),
  unsharedEndpointGapMs: distribution(rows.filter(row => !row.sharedAcousticEndpoint).map(row => row.gapMs)),
  missing, rows }, null, 2));
