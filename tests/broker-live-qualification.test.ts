import assert from 'node:assert/strict';
import test from 'node:test';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { freshnessEvidence, freshnessMayRelease, freshnessResult, selectFreshnessScenario } from '../scripts/live-freshness.js';
import type { LiveTranscriptSnapshot } from '../src/live/types.js';
import { fixtureAssignmentReference, matchesFixtureAssignmentReference, followupEvidence, followupMayRelease, followupResults, selectFollowupScenario,
  type FollowupDelegation, type FollowupQueueObservation, type FollowupResult, type FollowupSubmission } from '../scripts/live-followup.js';

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

test('follow-up selection requires two task recordings and preserves the other scenario selectors', () => {
  assert.equal(selectFollowupScenario(new Map()), false);
  assert.equal(selectFollowupScenario(new Map([['--scenario', 'freshness']])), false);
  const valid = new Map([['--scenario', 'followup'], ['--input-audio', '/first.pcmu'], ['--followup-audio', '/second.pcmu']]);
  assert.equal(selectFollowupScenario(valid), true);
  for (const flag of ['--input-audio', '--followup-audio']) {
    const missing = new Map(valid); missing.delete(flag);
    assert.throws(() => selectFollowupScenario(missing), /both caller audio/);
  }
  for (const flag of ['--interjection-audio', '--instructions-file']) {
    assert.throws(() => selectFollowupScenario(new Map([...valid, [flag, '/override']])), /production instructions/);
  }
  assert.throws(() => selectFollowupScenario(new Map([['--followup-audio', '/second.pcmu']])), /requires --scenario followup/);
});

function twoTurnEvidence() {
  const fragments: LiveTranscriptSnapshot['fragments'] = [
    { eventId: 'u1', sequence: 1, role: 'user', text: ' Please check my portfolio. ', startMs: 1000, endMs: 2000 },
    { eventId: 'a1', sequence: 2, role: 'assistant', text: 'I am checking.', startMs: 2200, endMs: 2400 },
    { eventId: 'u2', sequence: 3, role: 'user', text: ' And read my last payment.\t', startMs: 4000, endMs: 5000 },
    { eventId: 'a2', sequence: 4, role: 'assistant', text: followupResults[0], startMs: 8000, endMs: 12000 },
    { eventId: 'a3', sequence: 5, role: 'assistant', text: followupResults[1], startMs: 13000, endMs: 15000 },
  ];
  const snap = (n: number): LiveTranscriptSnapshot => ({ revision: n, callerRevision: n >= 3 ? 2 : 1, fragments: structuredClone(fragments.slice(0, n)) });
  const submission = (index: number): FollowupSubmission => ({ requestId: `request-${index}`, input: {
    source: { callSid: 'CA_fixture', itemId: `turn-${index}`, sourceEventId: `source-${index}`, revision: 1,
      receivedAt: `2026-09-22T14:00:0${index}.000Z`, role: 'user' }, text: fragments[index === 1 ? 0 : 2].text },
    observedAt: index === 1 ? 2500 : 11000, snapshot: snap(index === 1 ? 1 : 4), predecessorTerminal: index === 2 });
  const submissions = [submission(1), submission(2)];
  const delegation = (index: number): FollowupDelegation => ({ delegationId: `delegation-${index}`,
    observedAt: index === 1 ? 2400 : 6500, snapshot: snap(index === 1 ? 1 : 3) });
  const results: FollowupResult[] = submissions.map((entry, index) => ({ requestId: entry.requestId,
    result: { bindingId: 'fixture-binding', requestId: entry.requestId, state: 'completed', assistant: {
      messageId: `message-${index}`, eventId: `result-${index}`, sequence: String(index + 1), text: followupResults[index] } },
    observedAt: index === 0 ? 10000 : 11001, snapshot: snap(index === 0 ? 3 : 4), inputOffsetMs: index === 0 ? 8000 : 13000, outputBytes: index * 8000 }));
  const queue: FollowupQueueObservation = { observedAt: 8000, delegationId: 'delegation-2', predecessorRequestId: 'request-1',
    requestId: 'request-2', submission: structuredClone(submissions[1].input) };
  return { snapshot: snap(5), submissions, results, providerDelegations: [delegation(1), delegation(2)],
    adapterDelegations: [delegation(1), delegation(2)], queue, audioStartedAt: 5000, audioEndedAt: 7000, inputStartMs: 4000 };
}

test('first result stays held until observed second delegation, saved queue and caller quiet', () => {
  const data = twoTurnEvidence();
  const input = { now: 10000, firstSubmitAt: 2500, audioStartedAt: data.audioStartedAt,
    audioEndedAt: data.audioEndedAt, lastCallerTranscriptAt: 7500, queue: data.queue,
    providerDelegations: data.providerDelegations, adapterDelegations: data.adapterDelegations };
  assert.equal(followupMayRelease(input), true);
  for (const changed of [{ queue: undefined }, { providerDelegations: data.providerDelegations.slice(0, 1) },
    { adapterDelegations: data.adapterDelegations.slice(0, 1) }, { audioEndedAt: undefined },
    { lastCallerTranscriptAt: 9000 }, { now: 8000 }, { firstSubmitAt: 5500 }]) {
    assert.equal(followupMayRelease({ ...input, ...changed }), false);
  }
});

test('two-task evidence proves separate exact sources and order while preserving semantic review', () => {
  const data = twoTurnEvidence();
  const evidence = followupEvidence(data);
  assert.equal(evidence.mechanicalReady, true);
  assert.equal(evidence.semanticAccepted, null);
  assert.equal(evidence.semanticReviewRequired, true);
  assert.equal(evidence.originalSources[1].submittedText, ' And read my last payment.\t');
  assert.equal(evidence.resultWindows[0].untilNextResult[0].text, followupResults[0]);
  assert.equal(evidence.resultWindows[1].allAfterResult[0].text, followupResults[1]);
  assert.deepEqual(evidence.paymentCandidates, { amountMentioned: true, assetMentioned: true, recipientMentioned: true });
  const checks: Array<(value: ReturnType<typeof twoTurnEvidence>) => void> = [
    value => { value.submissions[1].input.text = value.submissions[1].input.text.trim(); },
    value => { value.submissions[1].input.source.sourceEventId = value.submissions[0].input.source.sourceEventId; },
    value => { value.submissions[1].predecessorTerminal = false; },
    value => { value.submissions[1].observedAt = value.results[0].observedAt - 1; },
    value => { value.providerDelegations.pop(); },
    value => { value.adapterDelegations.pop(); },
    value => { value.queue.predecessorRequestId = 'unrelated'; },
    value => { value.results.pop(); },
    value => { value.submissions.push(structuredClone(value.submissions[1])); },
  ];
  for (const change of checks) {
    const changed = twoTurnEvidence(); change(changed);
    assert.equal(followupEvidence(changed).mechanicalReady, false);
  }
  const negated = twoTurnEvidence();
  negated.snapshot.fragments[4].text = 'I cannot confirm 18.75 USDC to Harbor Books.';
  const unapproved = followupEvidence(negated);
  assert.equal(unapproved.paymentCandidates.amountMentioned, true);
  assert.equal(unapproved.semanticAccepted, null);
});

test('assignment fixture validates the optional contract and provides only a deterministic mock receipt', () => {
  assert.equal(fixtureAssignmentReference(), undefined);
  const assignment = { assignmentId: '11111111-1111-4111-8111-111111111111',
    brokerCallId: '22222222-2222-4222-8222-222222222222', revision: 1 as const, purpose: 'Read the portfolio.' };
  const ref = fixtureAssignmentReference(assignment)!;
  assert.equal(ref.assignmentId, assignment.assignmentId);
  assert.equal(ref.revision, 1);
  assert.match(ref.digest, /^[0-9a-f]{64}$/);
  assert.equal(matchesFixtureAssignmentReference({ digest: ref.digest, revision: 1, assignmentId: ref.assignmentId }, ref), true);
  assert.equal(matchesFixtureAssignmentReference({ ...ref, digest: '0'.repeat(64) }, ref), false);
  assert.equal(matchesFixtureAssignmentReference(undefined, ref), false);
  assert.equal(matchesFixtureAssignmentReference(undefined, undefined), true);
  assert.deepEqual(fixtureAssignmentReference({ purpose: assignment.purpose, revision: 1,
    brokerCallId: assignment.brokerCallId, assignmentId: assignment.assignmentId }), ref);
  assert.notEqual(fixtureAssignmentReference({ ...assignment, purpose: 'Read the recent payment.' })!.digest, ref.digest);
  assert.throws(() => fixtureAssignmentReference({ ...assignment, purpose: ' Read the portfolio.' }));
  assert.throws(() => fixtureAssignmentReference({ ...assignment, purpose: 'x'.repeat(1501) }));
});
