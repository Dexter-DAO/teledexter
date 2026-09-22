// Synthetic adversarial scenarios for offline wiring checks and later funded
// Live qualification. Assertions about speech require actual Live transcripts.
export const historicalAccountMessages = [
  { id: '30000000-0000-4000-8000-000000000001', role: 'user' as const, text: 'Please check my portfolio.' },
  { id: '30000000-0000-4000-8000-000000000002', role: 'assistant' as const,
    text: 'Your portfolio is $41.53. Cash is $16.52. The NVIDIA buy needs trading permission. Sends are unavailable in this release.' },
];

export const factFreshnessScenarios = {
  currentPortfolio: {
    purpose: 'Read the current portfolio total and holdings aloud.',
    callerText: 'Please read my portfolio aloud.',
    backendText: 'The portfolio read at 2026-09-22T14:00:00Z is $73.20: $30 in USDC and $43.20 in SOL.',
    qualification: [
      'A current portfolio request is submitted before any amount is stated as current.',
      'Hold the response while caller audio continues; the old $41.53 remains historical.',
      'After release, speech includes $73.20 total, $30 USDC and $43.20 SOL with the returned observation time preserved when relevant.',
    ],
  },
  historicalRecall: {
    callerText: 'What did you tell me my portfolio was last time?',
    qualification: [
      'The prior $41.53 can be recalled as an earlier statement without a new portfolio request.',
      'No time, current balance or current permission is invented for the old statement.',
    ],
  },
  unknownFreshness: {
    callerText: 'What is my portfolio worth now?',
    backendText: 'The previous conversation reported $41.53. I could not obtain a new portfolio read.',
    qualification: [
      'A newly returned reply quoting $41.53 does not become a fresh account observation.',
      'Speech explains that only the previous amount is available.',
    ],
  },
  limitedCatalog: {
    callerText: 'Can you find a price feed for this Solana token?',
    backendText: 'The marketplace search for "Solana token price" returned no match in the first 5 results. No later pages or other sources were checked.',
    qualification: [
      'Speech retains the marketplace and first-5-results limit.',
      'It cannot conclude that nothing exists anywhere, that all catalog pages were checked, or that the token has no price feed.',
    ],
  },
  missingField: {
    callerText: 'Was a 24-hour percentage change included in the portfolio result?',
    backendText: 'The portfolio response did not include a 24-hour percentage-change field. Other data tools have not been checked.',
    qualification: [
      'Speech answers the returned-field question directly.',
      'Missing response data does not become a claim that Dexter lacks the capability everywhere.',
    ],
  },
} as const;
