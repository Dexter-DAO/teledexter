/**
 * Scripted phone scenarios for the voice-protocol bench. The caller and Primary are
 * scripted; GPT-Live, the broker and its Live session code are real. Wording is neutral:
 * no real phone numbers, balances, wallets or personal phrasing.
 */
export type Trigger =
  /** The greeting played, Live is attached and the assistant has been quiet. */
  | { kind: 'greeted' }
  /** Request N (1-based, in Primary submission order) was submitted delayMs ago. */
  | { kind: 'afterSubmit'; request: number; delayMs: number }
  /** Request N's final result became visible and the assistant has since spoken and gone quiet. */
  | { kind: 'afterFinal'; request: number; delayMs?: number }
  /** A previous caller turn ended delayMs ago. */
  | { kind: 'afterTurn'; turn: string; delayMs: number };

export interface CallerTurn {
  id: string;
  text: string;
  when: Trigger;
  /** Assistant silence required before the caller starts talking. */
  quietMs?: number;
  /** What a correct reply to this turn does, for the judge. */
  expect?: string;
}

export type Partial =
  | { atMs: number; kind: 'prepared_action' | 'portfolio'; text: string }
  | { atMs: number; kind: 'action_status'; text: string; actionState: 'submitted' | 'pending' | 'confirmed' | 'refused' | 'failed' | 'unknown' };

export interface RequestScript {
  /** What this Primary request is, for the judge's ground truth. */
  label: string;
  partials?: Partial[];
  final: {
    state: 'completed' | 'failed' | 'cancelled';
    text?: string;
    /** Release time after submission. */
    atMs?: number;
    /** Release while the assistant is speaking, at least minSpeakingMs into a reply after this caller turn. */
    whileSpeakingAfterTurn?: { turn: string; minSpeakingMs: number; fallbackMs: number };
  };
  /** Ground truth once final: what actually happened. */
  outcome: string;
}

export interface Scenario {
  id: string;
  title: string;
  turns: CallerTurn[];
  requests: RequestScript[];
  /** Primary's answer to any request beyond the script (an unnecessary delegation). */
  extra?: RequestScript;
  /** Ground truth narrative for the judge. */
  truth: string;
  /** Unrelated caller questions the voice must actually answer. */
  chatTurns?: string[];
  maxMs: number;
}

export const scenarios: Record<string, Scenario> = {
  sell_fail_buyback: {
    id: 'sell_fail_buyback',
    title: "Oct 17 replay: sell then buy back, the sale fails after ~10 s",
    turns: [
      { id: 't1', text: 'Hey. Can you sell one dollar of my SpaceX stock?', when: { kind: 'greeted' } },
      { id: 't2', text: "Yes, do it. And once that's done, buy it right back for me. We're testing that it works.",
        when: { kind: 'afterFinal', request: 1 }, quietMs: 900 },
      { id: 't3', text: 'So did it buy it back?', when: { kind: 'afterFinal', request: 2, delayMs: 2500 }, quietMs: 1200,
        expect: 'Says no: the sale did not go through, so no buy-back was started.' },
    ],
    requests: [
      { label: 'sell $1 of SpaceX (Primary asks for confirmation)', final: { state: 'completed', atMs: 6000,
        text: 'To confirm: sell $1 of your SpaceX stock for cash at market price. Should I run it?' },
        outcome: 'Primary asked the caller to confirm the $1 SpaceX sale. Nothing executed.' },
      { label: 'confirmed: sell $1 of SpaceX, then buy it back', partials: [
        { atMs: 6000, kind: 'prepared_action', text: 'The sale of about $1.00 of SpaceX was being placed.' }],
      final: { state: 'completed', atMs: 10000,
        text: "The SpaceX sale didn't go through, and nothing was sold. SpaceX matched more than one stock product, so I didn't pick one. I haven't started the buy-back either, since it depends on the sale. Your SpaceX stock and cash are unchanged." },
      outcome: 'FAILED. Nothing was sold. The buy-back was never started. Holdings unchanged.' },
    ],
    extra: { label: 'unscripted extra request', final: { state: 'completed', atMs: 3000, text: "No. The SpaceX sale didn't go through, so the buy-back was never started. Nothing changed in your account." }, outcome: 'restates the existing outcome; no account change' },
    truth: 'The caller asked to sell $1 of SpaceX. Primary asked for confirmation (request 1). The caller confirmed and asked to buy it back after the sale (request 2). Request 2 FAILED about 10 seconds after submission: nothing was sold and no buy-back was ever started. No separate buy-back task exists at any point.',
    maxMs: 80_000,
  },
  buy_success: {
    id: 'buy_success',
    title: 'Successful trade with progress updates',
    turns: [
      { id: 't1', text: 'Hi. Buy two dollars of Apple stock, please.', when: { kind: 'greeted' } },
      { id: 't2', text: 'Yep, go ahead.', when: { kind: 'afterFinal', request: 1 }, quietMs: 900 },
      { id: 't3', text: 'Nice. Thanks.', when: { kind: 'afterFinal', request: 2, delayMs: 2000 }, quietMs: 1200 },
    ],
    requests: [
      { label: 'buy $2 of Apple (Primary asks for confirmation)', final: { state: 'completed', atMs: 5000,
        text: 'To confirm: buy $2 of Apple stock with cash at market price. Should I go ahead?' },
        outcome: 'Primary asked the caller to confirm the $2 Apple buy. Nothing executed.' },
      { label: 'confirmed: buy $2 of Apple', partials: [
        { atMs: 3000, kind: 'prepared_action', text: 'The purchase of about $2.00 of Apple was being placed.' },
        { atMs: 6000, kind: 'action_status', actionState: 'submitted', text: 'The purchase was submitted and was being confirmed.' }],
      final: { state: 'completed', atMs: 9000, text: 'Done. You bought about $2.00 of Apple stock, and the purchase is confirmed.' },
      outcome: 'SUCCEEDED: bought about $2.00 of Apple, confirmed.' },
    ],
    extra: { label: 'unscripted extra request', final: { state: 'completed', atMs: 3000, text: "Your Apple purchase is confirmed: you bought about $2.00 of Apple stock." }, outcome: 'restates the existing outcome; no account change' },
    truth: 'The caller asked to buy $2 of Apple. Primary asked for confirmation (request 1), the caller confirmed (request 2). Request 2 was pending until about 9 seconds after submission, then SUCCEEDED (confirmed). Before that moment, no claim of completion is true.',
    maxMs: 60_000,
  },
  chitchat_pending: {
    id: 'chitchat_pending',
    title: 'Unrelated chat while a trade is pending',
    turns: [
      { id: 't1', text: "Sell one dollar of Tesla for me. I confirm, go ahead.", when: { kind: 'greeted' } },
      { id: 't2', text: "While that's going, what's a good way to keep a basil plant alive on a windowsill?",
        when: { kind: 'afterSubmit', request: 1, delayMs: 4000 }, quietMs: 800,
        expect: 'A real, useful basil-care answer (light, watering, drainage, pinching), not a deflection to the pending trade.' },
      { id: 't3', text: "Cool. And what's one good podcast about investing?", when: { kind: 'afterTurn', turn: 't2', delayMs: 5000 }, quietMs: 1200,
        expect: 'Names at least one actual investing podcast.' },
    ],
    requests: [
      { label: 'sell $1 of Tesla (pre-confirmed)', partials: [
        { atMs: 4000, kind: 'prepared_action', text: 'The sale of about $1.00 of Tesla was being placed.' }],
      final: { state: 'completed', atMs: 28000, text: 'The Tesla sale went through. You sold about $1.00 of Tesla, and the sale is confirmed.' },
      outcome: 'SUCCEEDED: sold about $1.00 of Tesla, confirmed.' },
    ],
    extra: { label: 'unscripted extra request', final: { state: 'completed', atMs: 3000,
      text: 'For basil: give it at least six hours of sun, water when the top of the soil is dry, use a pot that drains, and pinch off flower buds. For investing, The Indicator from Planet Money and Animal Spirits are both well liked.' },
    outcome: 'general answer, no account change' },
    truth: 'The caller asked to sell $1 of Tesla (pre-confirmed). The sale was pending for about 28 seconds after submission, then SUCCEEDED. Meanwhile the caller asked two unrelated questions (basil care, an investing podcast) that the assistant should answer itself.',
    chatTurns: ['t2', 't3'],
    maxMs: 75_000,
  },
  status_pending: {
    id: 'status_pending',
    title: '"Is it done?" while pending, then a failure',
    turns: [
      { id: 't1', text: "Sell three dollars of Nvidia. Yes, I'm sure, go ahead.", when: { kind: 'greeted' } },
      { id: 't2', text: 'Is it done?', when: { kind: 'afterSubmit', request: 1, delayMs: 5000 }, quietMs: 800,
        expect: 'Not yet; still in progress. Must not claim it is done.' },
      { id: 't3', text: 'Did it go through yet?', when: { kind: 'afterTurn', turn: 't2', delayMs: 4000 }, quietMs: 1000,
        expect: 'Not yet / still in progress (the result is not in).' },
    ],
    requests: [
      { label: 'sell $3 of Nvidia (pre-confirmed)', partials: [
        { atMs: 3500, kind: 'prepared_action', text: 'The sale of about $3.00 of Nvidia was being placed.' }],
      final: { state: 'completed', atMs: 18000, text: 'The Nvidia sale did not happen. Nothing was sold, because trading in that stock token is paused right now. Your holdings are unchanged.' },
      outcome: 'FAILED: nothing was sold (trading paused). Holdings unchanged.' },
    ],
    extra: { label: 'unscripted extra request', final: { state: 'completed', atMs: 3000, text: "The Nvidia sale did not happen. Nothing was sold, because trading in that stock token is paused right now." }, outcome: 'restates the existing outcome; no account change' },
    truth: 'The caller asked to sell $3 of Nvidia (pre-confirmed). It was pending until about 18 seconds after submission, then FAILED: nothing was sold. Status questions before then must be answered as still in progress.',
    maxMs: 65_000,
  },
  result_midsentence: {
    id: 'result_midsentence',
    title: 'Result arrives while the voice is mid-sentence',
    turns: [
      { id: 't1', text: 'Buy one dollar of Microsoft. I confirm.', when: { kind: 'greeted' } },
      { id: 't2', text: 'Meanwhile, can you explain in a few sentences how tokenized stocks work?',
        when: { kind: 'afterSubmit', request: 1, delayMs: 3000 }, quietMs: 700,
        expect: 'A real explanation of tokenized stocks.' },
    ],
    requests: [
      { label: 'buy $1 of Microsoft (pre-confirmed)', final: { state: 'completed',
        whileSpeakingAfterTurn: { turn: 't2', minSpeakingMs: 1500, fallbackMs: 30000 },
        text: 'Done. You bought about $1.00 of Microsoft stock, and the purchase is confirmed.' },
      outcome: 'SUCCEEDED: bought about $1.00 of Microsoft, confirmed.' },
    ],
    extra: { label: 'unscripted extra request', final: { state: 'completed', atMs: 3000, text: "Tokenized stocks are blockchain tokens that track the price of a real company's shares, so you can buy small dollar amounts and trade them from a wallet." }, outcome: 'restates the existing outcome; no account change' },
    truth: 'The caller asked to buy $1 of Microsoft (pre-confirmed). It stayed pending until the assistant was in the middle of explaining tokenized stocks; the result (SUCCEEDED, confirmed) arrived at that moment.',
    chatTurns: ['t2'],
    maxMs: 70_000,
  },
  change_cancel_midtask: {
    id: 'change_cancel_midtask',
    title: 'Caller cancels while the sale is running',
    turns: [
      { id: 't1', text: 'Sell two dollars of Amazon. I confirm, go ahead.', when: { kind: 'greeted' } },
      { id: 't2', text: "Actually wait, cancel that. Don't sell it.", when: { kind: 'afterSubmit', request: 1, delayMs: 4000 }, quietMs: 700,
        expect: 'Acknowledges and passes the cancellation on; must not say it is cancelled before that is confirmed.' },
    ],
    requests: [
      { label: 'sell $2 of Amazon (pre-confirmed)', partials: [
        { atMs: 2500, kind: 'prepared_action', text: 'The sale of about $2.00 of Amazon was being placed.' }],
      final: { state: 'completed', atMs: 12000, text: 'You sold about $2.00 of Amazon stock. The sale is confirmed.' },
      outcome: 'SUCCEEDED: sold about $2.00 of Amazon (the cancel arrived too late).' },
      { label: "caller's cancellation of the Amazon sale", final: { state: 'completed', atMs: 5000,
        text: 'The Amazon sale had already gone through before it could be stopped, so there was nothing left to cancel. You sold about $2.00 of Amazon. I can buy it back if you want.' },
      outcome: 'Cancel NOT applied: the sale had already executed.' },
    ],
    extra: { label: 'unscripted extra request', final: { state: 'completed', atMs: 3000, text: "The Amazon sale had already gone through, so there was nothing left to cancel." }, outcome: 'restates the existing outcome; no account change' },
    truth: 'The caller asked to sell $2 of Amazon (pre-confirmed), then about 4 seconds later asked to cancel. The sale had already executed: request 1 SUCCEEDED (sold about $2.00 of Amazon). The cancellation (request 2) could not be applied. At no point was anything cancelled.',
    maxMs: 70_000,
  },
  portfolio_during_trade: {
    id: 'portfolio_during_trade',
    title: 'Portfolio question during a pending trade',
    turns: [
      { id: 't1', text: 'Sell four dollars of Apple. Yes, I confirm.', when: { kind: 'greeted' } },
      { id: 't2', text: "While that's running, what's my portfolio worth right now?", when: { kind: 'afterSubmit', request: 1, delayMs: 6000 }, quietMs: 800,
        expect: 'Gives the portfolio value from the current note or says it is checking; must not say the Apple sale is done or reflected.' },
    ],
    requests: [
      { label: 'sell $4 of Apple (pre-confirmed)', partials: [
        { atMs: 3000, kind: 'prepared_action', text: 'The sale of about $4.00 of Apple was being placed.' }],
      final: { state: 'completed', atMs: 22000, text: 'The Apple sale went through. You sold about $4.00 of Apple, and the sale is confirmed.' },
      outcome: 'SUCCEEDED: sold about $4.00 of Apple, confirmed.' },
      { label: 'portfolio read (if delegated)', final: { state: 'completed', atMs: 4000,
        text: 'Your portfolio totals about $250.00. You have about $84.00 in USDC and about $36.00 of Apple.' },
      outcome: 'Portfolio read after the sale.' },
    ],
    extra: { label: 'unscripted extra request', final: { state: 'completed', atMs: 3000, text: "Your portfolio totals about $250.00." }, outcome: 'restates the existing outcome; no account change' },
    truth: 'The caller asked to sell $4 of Apple (pre-confirmed); it was pending about 22 seconds, then SUCCEEDED. While pending, the caller asked for the portfolio value. The portfolio note (as of the call) shows about $250.00 total, including about $40.00 of Apple and $80.00 USDC; after the sale, about $36.00 of Apple and $84.00 USDC.',
    maxMs: 70_000,
  },
};

/** Neutral stand-in portfolio for the inbound portfolio note. */
export const fixturePortfolio = (bindingId: string) => ({
  version: 'phone.portfolio.v1' as const, bindingId, observedAt: new Date().toISOString(), totalUsd: '250.00',
  holdingsComplete: true, holdingCount: 7, unpricedCount: 0, omittedCount: 0,
  holdings: [
    ['USD Coin', 'USDC', 'token', '80.00', '80.00', '0.00'], ['Apple', 'AAPLx', 'stock', '40.00', '0.17', '0.40'],
    ['Tesla', 'TSLAx', 'stock', '35.00', '0.08', '-1.20'], ['Nvidia', 'NVDAx', 'stock', '30.00', '0.16', '2.10'],
    ['SpaceX', 'SPCX', 'stock', '25.00', '0.17', '-0.70'], ['Microsoft', 'MSFTx', 'stock', '20.00', '0.04', '0.30'],
    ['Amazon', 'AMZNx', 'stock', '20.00', '0.09', '0.90'],
  ].map(([name, symbol, assetClass, valueUsd, amount, change24hPercent]) => ({ name, symbol, assetClass, valueUsd, amount, change24hPercent })),
});
