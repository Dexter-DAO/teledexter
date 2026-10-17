import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { formatCallTranscript } from '../src/broker/callTranscript.js';
import type { BrokerCall } from '../src/broker/store.js';
import type { LiveTranscriptFragment } from '../src/live/types.js';

const start = '2026-10-17T12:00:00.000Z';
const end = '2026-10-17T12:00:30.500Z';
const fragment = (role: LiveTranscriptFragment['role'], text: string, sequence: number): LiveTranscriptFragment =>
  ({ role, text, sequence, eventId: `event-${sequence}`, startMs: sequence * 100, endMs: sequence * 100 + 100 });
const fragments = [fragment('user', ' Buy', 1), fragment('user', ' $1 of Apple.', 2),
  fragment('assistant', ' I\'ll', 3), fragment('assistant', ' check.\nPlease wait.', 4)];
const delegation = { requestId: 'request-1', status: 'completed', updatedAt: end,
  submission: { text: ' Buy $1 of Apple.\nPlease.' }, timing: { firstSavedAt: '2026-10-17T12:00:05.000Z' } };
const call: BrokerCall = {
  id: '143d44b6-9c83-43b9-a9db-06fefec57dc7', requestId: 'call-request', direction: 'inbound',
  purpose: 'Fixture', number: '+12025550100', status: 'completed', createdAt: start, updatedAt: end, endedAt: end,
  delegations: { 'request-1': delegation },
  liveGenerations: {
    version: 1, callId: '143d44b6-9c83-43b9-a9db-06fefec57dc7', phase: 'stopped', inputNextSample: 0,
    binding: { bindingId: 'binding', callSid: 'CA' + '1'.repeat(32), primaryDexterId: 'primary', threadId: 'thread' }, clocks: { callDeadlineAt: end },
    segments: [fragments, [fragment('assistant', ' Ready.', 1), fragment('user', ' Thanks!', 2)]].map((items, index) => ({
      generationId: `generation-${index}`, ordinal: index + 1, createdAt: start, state: 'closed', outputDiscarded: false,
      version: { ownerPrincipalId: 'owner', primaryDexterId: 'primary', threadId: 'thread', contextEpoch: '1', threadEventSequence: '1', digest: 'a'.repeat(64) },
      pageDigest: 'a'.repeat(64), transcript: { revision: items.length, callerRevision: items.filter(item => item.role === 'user').length, fragments: items },
    })),
  },
};
const expected = `CALL ${call.id} | inbound | start=${start} | end=${end} | duration=30.500s
CALLER: Buy $1 of Apple.
DEXTER: I'll check. Please wait. Ready.
CALLER: Thanks!
PRIMARY: Buy $1 of Apple. Please. | status=completed | duration=25.500s`;

test('formats exact turns across generations and delegation status and elapsed time', () => {
  assert.equal(formatCallTranscript(call), expected);
});

test('reads legacy transcripts and reports missing evidence without inventing timing', () => {
  const legacy = { ...call, liveGenerations: undefined, endedAt: undefined,
    transcript: { revision: fragments.length, callerRevision: 2, fragments },
    delegations: { old: { requestId: 'old', status: 'failed', updatedAt: end } } };
  assert.equal(formatCallTranscript(legacy), `CALL ${call.id} | inbound | start=${start} | end=unknown | duration=unknown
CALLER: Buy $1 of Apple.
DEXTER: I'll check. Please wait.
PRIMARY: (text unavailable) | status=failed | duration=unknown`);
  assert.equal(formatCallTranscript({ ...legacy, transcript: undefined, delegations: {} }),
    `CALL ${call.id} | inbound | start=${start} | end=unknown | duration=unknown`);
});

test('operator transcript reads the journal without credentials or journal writes', t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'call-transcript-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const journal = path.join(directory, 'calls.json');
  const original = JSON.stringify({ version: 1, calls: { [call.id]: call } });
  fs.writeFileSync(journal, original, { mode: 0o400 });
  const run = (id: string) => spawnSync(process.execPath,
    ['--import', 'tsx', 'scripts/broker.mts', 'transcript', '--journal', journal, '--id', id],
    { cwd: process.cwd(), encoding: 'utf8', env: { ...process.env, BROKER_CONTROL_TOKEN: '', BROKER_ENV_FILE: path.join(directory, 'no-env') } });
  const result = run(call.id);
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout, expected + '\n');
  const missing = run('missing');
  assert.equal(missing.status, 1);
  assert.match(missing.stderr, /Call not found: missing/);
  assert.equal(fs.readFileSync(journal, 'utf8'), original);
  assert.deepEqual(fs.readdirSync(directory), ['calls.json']);
});
