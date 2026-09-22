import type { PrimaryRequestResult, PrimarySubmission } from '../src/broker/primaryApi.js';
import type { LiveTranscriptSnapshot } from '../src/live/types.js';
import { createHash } from 'node:crypto';

export const followupPurpose = 'Help the caller check their portfolio and their most recent payment.';
export const followupResults = [
  'Your portfolio is worth $1,250 in total: $250 in USDC and $1,000 in SOL.',
  'Your most recent payment was 18.75 USDC to Harbor Books.',
] as const;

export interface FixtureCallAssignment {
  assignmentId: string; brokerCallId: string; revision: 1; purpose: string;
}
/** A deterministic mock receipt, not evidence of the API's canonical assignment digest. */
export function fixtureAssignmentReference(assignment?: FixtureCallAssignment) {
  if (assignment === undefined) return undefined;
  const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
  if (!uuid.test(assignment.assignmentId) || !uuid.test(assignment.brokerCallId) || assignment.revision !== 1
      || !assignment.purpose || assignment.purpose !== assignment.purpose.trim() || assignment.purpose.length > 1500)
    throw new Error('invalid_fixture_call_assignment');
  const digest = createHash('sha256').update(JSON.stringify({ assignmentId: assignment.assignmentId,
    brokerCallId: assignment.brokerCallId, revision: assignment.revision, purpose: assignment.purpose })).digest('hex');
  return { assignmentId: assignment.assignmentId, revision: 1 as const, digest };
}
export function matchesFixtureAssignmentReference(value: unknown, expected: ReturnType<typeof fixtureAssignmentReference>): boolean {
  if (expected === undefined) return value === undefined;
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const actual = value as Record<string, unknown>;
  return Object.keys(actual).sort().join(',') === 'assignmentId,digest,revision'
    && actual.assignmentId === expected.assignmentId && actual.revision === expected.revision && actual.digest === expected.digest;
}

/** Validate before credentials or provider connections are opened. */
export function selectFollowupScenario(args: ReadonlyMap<string, string>): boolean {
  if (args.get('--scenario') !== 'followup') {
    if (args.has('--followup-audio')) throw new Error('Follow-up audio requires --scenario followup');
    return false;
  }
  if (!args.has('--input-audio') || !args.has('--followup-audio')) throw new Error('Follow-up requires both caller audio files');
  if (args.has('--interjection-audio') || args.has('--instructions-file'))
    throw new Error('Follow-up uses production instructions and separate task audio');
  return true;
}

export interface FollowupDelegation {
  delegationId: string;
  observedAt: number;
  snapshot: LiveTranscriptSnapshot;
}
export interface FollowupSubmission {
  requestId: string;
  input: PrimarySubmission;
  observedAt: number;
  snapshot: LiveTranscriptSnapshot;
  predecessorTerminal: boolean;
}
export interface FollowupResult {
  requestId: string;
  result: PrimaryRequestResult;
  observedAt: number;
  snapshot: LiveTranscriptSnapshot;
  inputOffsetMs: number;
  outputBytes: number;
}
export interface FollowupQueueObservation {
  observedAt: number;
  delegationId: string;
  predecessorRequestId: string;
  requestId: string;
  submission: PrimarySubmission;
}

export function followupMayRelease(input: {
  now: number; firstSubmitAt?: number; audioEndedAt?: number; lastCallerTranscriptAt: number;
  queue?: FollowupQueueObservation; providerDelegations: FollowupDelegation[]; adapterDelegations: FollowupDelegation[];
  audioStartedAt?: number;
}): boolean {
  const queued = input.queue;
  if (!queued || input.firstSubmitAt === undefined || input.audioStartedAt === undefined || input.audioEndedAt === undefined
      || input.audioStartedAt <= input.firstSubmitAt || input.now - input.audioEndedAt < 1800
      || input.now - input.lastCallerTranscriptAt < 1500) return false;
  const observed = (values: FollowupDelegation[]) => values.some(value => value.delegationId === queued.delegationId
    && value.observedAt >= input.audioStartedAt! && value.observedAt <= queued.observedAt);
  return observed(input.providerDelegations) && observed(input.adapterDelegations);
}

const text = (snapshot: LiveTranscriptSnapshot, role: 'user' | 'assistant') => snapshot.fragments
  .filter(fragment => fragment.role === role).map(fragment => fragment.text).join('');

/** Preserve receipt boundaries and raw text; candidate matches never approve narration. */
export function followupEvidence(input: {
  snapshot: LiveTranscriptSnapshot; submissions: FollowupSubmission[]; results: FollowupResult[];
  providerDelegations: FollowupDelegation[]; adapterDelegations: FollowupDelegation[];
  queue?: FollowupQueueObservation; audioStartedAt?: number; audioEndedAt?: number; inputStartMs?: number;
}) {
  const callerTurns = [
    input.snapshot.fragments.filter(fragment => fragment.role === 'user'
      && input.inputStartMs !== undefined && fragment.startMs < input.inputStartMs).map(fragment => fragment.text).join(''),
    input.snapshot.fragments.filter(fragment => fragment.role === 'user'
      && input.inputStartMs !== undefined && fragment.startMs >= input.inputStartMs).map(fragment => fragment.text).join(''),
  ];
  const originalSources = input.submissions.map((submission, index) => ({ requestId: submission.requestId,
    source: submission.input.source, submittedText: submission.input.text, rawTurn: callerTurns[index] ?? '',
    exactRawText: Boolean(callerTurns[index]) && submission.input.text === callerTurns[index] }));
  const first = input.results[0];
  const second = input.results[1];
  const frontier = (snapshot: LiveTranscriptSnapshot) => Math.max(0, ...snapshot.fragments.map(fragment => fragment.sequence));
  const resultWindows = input.results.map((result, index) => {
    const next = input.results[index + 1];
    const afterRelease = input.snapshot.fragments.filter(fragment => fragment.role === 'assistant'
      && fragment.sequence > frontier(result.snapshot));
    return { requestId: result.requestId, fixtureResult: result.result,
      releaseAt: result.observedAt, releaseSnapshot: result.snapshot,
      untilNextResult: afterRelease.filter(fragment => !next || fragment.sequence <= frontier(next.snapshot)),
      allAfterResult: afterRelease };
  });
  const afterFirst = first ? input.snapshot.fragments.filter(fragment => fragment.role === 'assistant'
    && fragment.sequence > frontier(first.snapshot)).map(fragment => fragment.text).join('') : '';
  const afterSecond = second ? input.snapshot.fragments.filter(fragment => fragment.role === 'assistant'
    && fragment.sequence > frontier(second.snapshot)).map(fragment => fragment.text).join('') : '';
  const queued = input.queue;
  const providerIds = new Set(input.providerDelegations.map(value => value.delegationId));
  const pairedIds = new Set(input.adapterDelegations.filter(value => providerIds.has(value.delegationId)).map(value => value.delegationId));
  const actualSecondDelegation = Boolean(queued && input.audioStartedAt !== undefined
    && pairedIds.size >= 2 && input.providerDelegations.some(value => value.delegationId === queued.delegationId
      && value.observedAt >= input.audioStartedAt!) && input.adapterDelegations.some(value => value.delegationId === queued.delegationId));
  const exactlyTwoSubmissions = input.submissions.length === 2 && new Set(input.submissions.map(value => value.requestId)).size === 2;
  const separateSources = exactlyTwoSubmissions && input.submissions[0].input.source.sourceEventId !== input.submissions[1].input.source.sourceEventId;
  const bothRawTurnsPreserved = exactlyTwoSubmissions && originalSources.every(source => source.exactRawText);
  const childAfterParentTerminal = exactlyTwoSubmissions && Boolean(first && input.submissions[1].predecessorTerminal
    && first.result.state === 'completed' && first.requestId === input.submissions[0].requestId
    && input.submissions[1].observedAt >= first.observedAt);
  const queuePreserved = exactlyTwoSubmissions && Boolean(queued && queued.predecessorRequestId === input.submissions[0].requestId
    && queued.requestId === input.submissions[1].requestId && JSON.stringify(queued.submission) === JSON.stringify(input.submissions[1].input));
  const secondTaskBeforeFirstResult = Boolean(first && queued && input.audioEndedAt !== undefined
    && queued.observedAt <= first.observedAt && input.audioEndedAt < first.observedAt);
  const bothResultsReturned = input.results.length === 2 && exactlyTwoSubmissions && input.results.every((result, index) =>
    result.requestId === input.submissions[index].requestId && result.result.state === 'completed'
    && result.result.assistant?.text === followupResults[index]);
  const paymentAmountMentioned = /\b18[.]75\b|\beighteen(?:[ -]point[ -]seven[ -]five|(?: dollars?)?(?: and)?[ -]seventy[ -]five(?: cents?)?)\b/i.test(afterSecond);
  return {
    semanticReviewRequired: true, semanticAccepted: null,
    limitation: 'Transcript receipt order can differ from speech timing. Review the saved offsets and audio for both answers, amount associations, attribution, omissions and interruptions. Simulated playback receipts do not establish what a person heard.',
    mechanicalReady: actualSecondDelegation && exactlyTwoSubmissions && separateSources && bothRawTurnsPreserved
      && childAfterParentTerminal && queuePreserved && secondTaskBeforeFirstResult && bothResultsReturned,
    actualSecondDelegation, exactlyTwoSubmissions, separateSources, bothRawTurnsPreserved,
    childAfterParentTerminal, queuePreserved, secondTaskBeforeFirstResult, bothResultsReturned,
    callerTurns, originalSources, queue: queued, providerDelegations: input.providerDelegations, adapterDelegations: input.adapterDelegations,
    submissions: input.submissions, results: input.results, resultWindows,
    beforeFirstResult: first ? text(first.snapshot, 'assistant') : text(input.snapshot, 'assistant'),
    afterFirstResult: afterFirst, afterSecondResult: afterSecond,
    expectedFacts: { portfolio: { totalUsd: '1250', USDC: '250', SOL: '1000' }, payment: { amount: '18.75', asset: 'USDC', recipient: 'Harbor Books' } },
    paymentCandidates: { amountMentioned: paymentAmountMentioned, assetMentioned: /\busdc\b/i.test(afterSecond),
      recipientMentioned: /\bharbo[u]?r books\b/i.test(afterSecond) },
  };
}
