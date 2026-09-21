#!/usr/bin/env node
// First-class call monitoring for dexter-phone.
//
//   npm run calls           -> full trace of the MOST RECENT call
//   npm run calls -- watch  -> live tail (follow the call happening now)
//   npm run calls -- brief  -> just the highlights of the last call
//
// A "call" is everything between a "Twilio media stream connected" marker and
// the next one (or end of log). No memorized line numbers — the boundary is in
// the log itself. Reads PM2's out log directly.

import fs from 'node:fs';
import { spawn } from 'node:child_process';
import os from 'node:os';
import path from 'node:path';

const OUT_LOG =
  process.env.DEXTER_PHONE_LOG ||
  path.join(os.homedir(), '.pm2', 'logs', 'dexter-phone-out.log');

const CALL_START_MARKER = 'Twilio media stream connected';
const mode = process.argv[2] || 'last';

function readLines() {
  try {
    return fs.readFileSync(OUT_LOG, 'utf-8').split('\n');
  } catch {
    console.error(`[calls] cannot read log at ${OUT_LOG}`);
    process.exit(1);
  }
}

// Return the slice of lines belonging to the last call (from the last start
// marker to end of file). If no marker, return the whole tail.
function lastCallLines() {
  const lines = readLines();
  let start = -1;
  for (let i = lines.length - 1; i >= 0; i--) {
    if (lines[i].includes(CALL_START_MARKER)) {
      start = i;
      break;
    }
  }
  return start === -1 ? lines.slice(-80) : lines.slice(start);
}

// Condense a call to its decision points: connect, identity, interrupts,
// user transcriptions, tool calls, cancellations, and end.
function brief(lines) {
  const keep = (l) =>
    /Twilio media stream connected|Caller identified|MCP tools loaded|calling session\.interrupt|audio_interrupted|user_transcription|MCP tool completed|status=cancelled|Funding SMS|SMS sent|Call ended|Realtime session error/.test(
      l,
    );
  return lines.filter(keep);
}

if (mode === 'watch') {
  console.log(`[calls] live tail of ${OUT_LOG} — Ctrl-C to stop\n`);
  const tail = spawn('tail', ['-n', '0', '-F', OUT_LOG], { stdio: 'inherit' });
  process.on('SIGINT', () => {
    tail.kill();
    process.exit(0);
  });
} else if (mode === 'brief') {
  const lines = brief(lastCallLines());
  console.log(`[calls] last call — ${lines.length} key events\n`);
  console.log(lines.join('\n'));
} else {
  // 'last' (default): full trace of the most recent call.
  const lines = lastCallLines();
  console.log(`[calls] most recent call — ${lines.length} log lines\n`);
  console.log(lines.join('\n'));
}
