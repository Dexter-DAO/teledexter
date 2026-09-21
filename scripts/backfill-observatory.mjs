#!/usr/bin/env node
/**
 * One-shot backfill: read storage/call-log.json and POST each entry to
 * /internal/voice/call-ended so the observatory has data immediately.
 *
 * Safe to re-run — the ingest endpoint upserts on (source, external_sid).
 * Each call's external_sid is its streamSid, which is unique per call.
 *
 * NOTE: This fires one notification per historical call. There are ~37
 * entries in the log, so expect ~37 emails arriving in a burst. This
 * is intentional — per Branch's "one email per call, no filtering"
 * policy. If you want a quieter backfill, run during a known window
 * or filter by recent entries only.
 */

import fs from 'node:fs';
import path from 'node:path';

const CALL_LOG_PATH = path.join(process.cwd(), 'storage', 'call-log.json');
const API_BASE = process.env.DEXTER_API_URL || 'http://localhost:3030';

const log = JSON.parse(fs.readFileSync(CALL_LOG_PATH, 'utf-8'));
console.log(`[backfill] ${log.length} calls in storage/call-log.json`);
console.log(`[backfill] sending to ${API_BASE} — expect ${log.length} notifications`);

let inserted = 0;
let updated = 0;
let failed = 0;
const startedAtIso = new Date().toISOString();

for (const c of log) {
  const payload = {
    source: 'phone',
    external_sid: c.streamSid || `BACKFILL_${c.startedAt}`,
    caller_number: c.callerNumber !== 'unknown' ? c.callerNumber : null,
    stir_verstat: c.stirVerstat,
    started_at: c.startedAt,
    ended_at: c.endedAt,
    duration_seconds: c.durationSeconds,
    tool_call_count: (c.toolCalls || []).length,
    segment_count: (c.transcriptSegments || []).length,
    tool_calls: c.toolCalls || [],
    transcript_segments: c.transcriptSegments || [],
    errors: [],
  };
  try {
    const res = await fetch(`${API_BASE}/internal/voice/call-ended`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (res.ok) {
      const body = await res.json();
      inserted++;
      const shortId = body.id ? body.id.slice(0, 8) : '?';
      console.log(`[backfill] ${c.startedAt} (${c.callerNumber || 'unknown'}) → ${body.status} (${shortId})`);
    } else {
      failed++;
      const text = await res.text().catch(() => '');
      console.warn(`[backfill] ${c.startedAt} → HTTP ${res.status} ${text.slice(0, 100)}`);
    }
  } catch (err) {
    failed++;
    console.warn(`[backfill] ${c.startedAt} → ${err?.message || err}`);
  }
}

console.log(`[backfill] done: ${inserted} ok, ${failed} failed (started ${startedAtIso})`);
process.exit(failed > 0 ? 1 : 0);
