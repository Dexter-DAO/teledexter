import type { LiveTranscriptSnapshot } from '../src/live/types.js';
import { factFreshnessScenarios, historicalAccountMessages } from '../tests/fixtures/broker-fact-freshness.js';

export { historicalAccountMessages };
export const freshnessPurpose = factFreshnessScenarios.currentPortfolio.purpose;
export const freshnessHoldMs = 4000;

/** Validate before reading credentials or opening a provider connection. */
export function selectFreshnessScenario(args: ReadonlyMap<string, string>): boolean {
  const scenario = args.get('--scenario');
  if (scenario === undefined) return false;
  if (scenario !== 'freshness') throw new Error('Unknown qualification scenario');
  if (!args.has('--input-audio')) throw new Error('Freshness requires caller audio');
  if (['--instructions-file', '--call-purpose-file', '--interjection-audio'].some(flag => args.has(flag))) {
    throw new Error('Freshness uses production instructions, its fixed call purpose and one caller audio input');
  }
  return true;
}

export function freshnessResult(observedAt: string): string {
  if (!Number.isFinite(Date.parse(observedAt))) throw new Error('Invalid fixture observation time');
  return `The portfolio read at ${observedAt} is $73.20: $30 in USDC and $43.20 in SOL.`;
}

export function freshnessMayRelease(now: number, submittedAt: number | undefined,
  speechEndedAt: number | undefined, lastCallerTranscriptAt: number): boolean {
  return submittedAt !== undefined && speechEndedAt !== undefined
    && now - submittedAt >= freshnessHoldMs && now - speechEndedAt >= 1800
    && now - lastCallerTranscriptAt >= 1500;
}

const assistantText = (snapshot?: LiveTranscriptSnapshot) => snapshot?.fragments
  .filter(fragment => fragment.role === 'assistant').map(fragment => fragment.text).join('') ?? '';

/** Candidates assist review; negation, attribution and currency need human judgment. */
export function freshnessEvidence(input: {
  snapshot: LiveTranscriptSnapshot;
  atSubmit?: LiveTranscriptSnapshot;
  atResult?: LiveTranscriptSnapshot;
  submittedText?: string;
}) {
  const beforeSubmit = assistantText(input.atSubmit);
  const beforeResult = assistantText(input.atResult);
  const last = (snapshot?: LiveTranscriptSnapshot) => Math.max(0, ...(snapshot?.fragments.map(fragment => fragment.sequence) ?? []));
  const afterSubmit = input.snapshot.fragments.filter(fragment => fragment.role === 'assistant'
    && fragment.sequence > last(input.atSubmit) && fragment.sequence <= last(input.atResult)).map(fragment => fragment.text).join('');
  const afterResult = input.atResult === undefined ? '' : input.snapshot.fragments.filter(fragment => fragment.role === 'assistant'
    && fragment.sequence > last(input.atResult)).map(fragment => fragment.text).join('');
  const callerText = input.snapshot.fragments.filter(fragment => fragment.role === 'user').map(fragment => fragment.text).join('');
  const historicalCandidates = (text: string) => [
    { value: '41.53', pattern: /\b41[.]53\b|\bforty[ -]one(?: dollars?)?(?: and)? fifty[ -]three\b/i },
    { value: '16.52', pattern: /\b16[.]52\b|\bsixteen(?: dollars?)?(?: and)? fifty[ -]two\b/i },
  ].filter(candidate => candidate.pattern.test(text)).map(candidate => candidate.value);
  const returnedCandidates = [
    { value: '73.20', pattern: /\b73[.]20?\b|\bseventy[ -]three(?: dollars?)?(?: and)? twenty\b/i },
    { value: '30', pattern: /\b30(?:[.]00)?\b|\bthirty\b/i },
    { value: '43.20', pattern: /\b43[.]20?\b|\bforty[ -]three(?: dollars?)?(?: and)? twenty\b/i },
  ].map(candidate => ({ value: candidate.value, mentioned: candidate.pattern.test(afterResult) }));
  return {
    semanticReviewRequired: true,
    semanticAccepted: null,
    limitation: 'Sequence boundaries record received transcript deltas; delayed deltas can describe earlier speech. Review transcript offsets and audio before judging when a claim was spoken. Amount mentions do not establish attribution, freshness, correct holdings or that audio was heard.',
    beforeSubmit, waitingForResult: afterSubmit, beforeResult, afterResult, callerText,
    rawCallerRequestPreserved: callerText.length > 0 && input.submittedText === callerText,
    historicalAmountCandidatesBeforeSubmit: historicalCandidates(beforeSubmit),
    historicalAmountCandidatesBeforeResult: historicalCandidates(beforeResult),
    returnedAmountCandidates: returnedCandidates,
    expectedReturnedFacts: { portfolioUsd: '73.20', USDC: '30', SOL: '43.20' },
  };
}
