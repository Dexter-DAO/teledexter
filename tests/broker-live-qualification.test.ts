import assert from 'node:assert/strict';
import test from 'node:test';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { freshnessEvidence, freshnessMayRelease, freshnessResult, selectFreshnessScenario } from '../scripts/live-freshness.js';
import type { LiveTranscriptSnapshot } from '../src/live/types.js';

test('legacy scenarios remain default and freshness requires one production-prompt caller input', () => {
  assert.equal(selectFreshnessScenario(new Map()), false);
  assert.equal(selectFreshnessScenario(new Map([['--interjection-audio', '/fixture.pcmu']])), false);
  assert.equal(selectFreshnessScenario(new Map([['--scenario', 'freshness'], ['--input-audio', '/caller.pcmu']])), true);
  assert.throws(() => selectFreshnessScenario(new Map([['--scenario', 'freshnes']])));
  assert.throws(() => selectFreshnessScenario(new Map([['--scenario', 'freshness']])));
  for (const flag of ['--instructions-file', '--call-purpose-file', '--interjection-audio']) {
    assert.throws(() => selectFreshnessScenario(new Map([['--scenario', 'freshness'], ['--input-audio', '/caller.pcmu'], [flag, '/override']])));
  }
});

test('actual harness refuses a freshness prompt override before configuration or evidence creation', t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-freshness-selector-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const root = fileURLToPath(new URL('../', import.meta.url));
  const evidence = path.join(directory, 'unused-evidence');
  const result = spawnSync(process.execPath, ['--import', 'tsx', 'scripts/qualify-live-phone.mts',
    '--module-root', root, '--module-variant', 'src', '--evidence-dir', evidence,
    '--config-file', path.join(directory, 'no-credentials-exist'), '--scenario', 'freshness',
    '--input-audio', path.join(directory, 'not-read.pcmu'), '--instructions-file', path.join(directory, 'not-read.txt')],
  { cwd: root, encoding: 'utf8', timeout: 10_000 });
  assert.equal(result.status, 1);
  assert.match(result.stderr, /Freshness uses production instructions/);
  assert.doesNotMatch(result.stderr, /ENOENT|SyntaxError/);
  assert.equal(fs.existsSync(evidence), false);
});

test('freshness result cannot release before a submitted caller request and bounded quiet hold', () => {
  assert.equal(freshnessMayRelease(7000, undefined, 1000, 1000), false);
  assert.equal(freshnessMayRelease(7000, 2000, undefined, 1000), false);
  assert.equal(freshnessMayRelease(5999, 2000, 1000, 1000), false);
  assert.equal(freshnessMayRelease(7000, 2000, 6500, 6500), false);
  assert.equal(freshnessMayRelease(7000, 2000, 4000, 6000), false);
  assert.equal(freshnessMayRelease(7000, 2000, 4000, 5000), true);
  assert.equal(freshnessResult('2026-09-22T14:00:00.000Z'),
    'The portfolio read at 2026-09-22T14:00:00.000Z is $73.20: $30 in USDC and $43.20 in SOL.');
  assert.throws(() => freshnessResult('not a time'));
});

test('ordering evidence retains raw whitespace and marks old and returned values without semantic approval', () => {
  const fragments: LiveTranscriptSnapshot['fragments'] = [
    { eventId: '1', sequence: 1, role: 'user', text: ' Please read my portfolio.', startMs: 1000, endMs: 2000 },
    { eventId: '2', sequence: 2, role: 'assistant', text: 'Your portfolio is $41.53.', startMs: 2100, endMs: 2500 },
    { eventId: '3', sequence: 3, role: 'assistant', text: ' The prior cash was sixteen dollars and fifty-two cents.', startMs: 3500, endMs: 4000 },
    { eventId: '4', sequence: 4, role: 'assistant', text: 'I cannot confirm $73.20, $30 in USDC or $43.20 in SOL.', startMs: 8000, endMs: 10000 },
  ];
  const snapshot = (n: number): LiveTranscriptSnapshot => ({ revision: n, callerRevision: 1, fragments: fragments.slice(0, n) });
  const evidence = freshnessEvidence({ snapshot: snapshot(4), atSubmit: snapshot(2), atResult: snapshot(3), submittedText: fragments[0].text });
  assert.equal(evidence.rawCallerRequestPreserved, true);
  assert.deepEqual(evidence.historicalAmountCandidatesBeforeSubmit, ['41.53']);
  assert.deepEqual(evidence.historicalAmountCandidatesBeforeResult, ['41.53', '16.52']);
  assert.equal(evidence.waitingForResult, fragments[2].text);
  assert.equal(evidence.afterResult, fragments[3].text);
  assert.equal(evidence.returnedAmountCandidates.every(candidate => candidate.mentioned), true);
  // All values in a negated statement are still only keyword candidates.
  assert.equal(evidence.semanticAccepted, null);
  assert.equal(evidence.semanticReviewRequired, true);
  assert.equal(freshnessEvidence({ snapshot: snapshot(4), submittedText: fragments[0].text.trim() }).rawCallerRequestPreserved, false);
  assert.equal(freshnessEvidence({ snapshot: snapshot(4) }).afterResult, '');
});
