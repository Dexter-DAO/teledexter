import assert from 'node:assert/strict';
import test from 'node:test';
import { createPortfolioContext } from '../src/broker/portfolioContext.js';
import { brokerInstructions } from '../src/broker/instructions.js';
import type { PhonePortfolio } from '../src/broker/portfolio.js';
import { createPrimaryApiClient, type PrimaryResult } from '../src/broker/primaryApi.js';
import { createBrokerLiveSession, type BrokerLiveBackend, type LiveDelegationRequest } from '../src/live/session.js';
import { PreconnectTransport } from './helpers/preconnectTransport.js';

const bindingId = '11111111-1111-4111-8111-111111111111';
test.beforeEach(t => t.mock.timers.enable({ apis: ['Date', 'setTimeout', 'setInterval'], now: Date.parse('2026-10-17T12:52:00.000Z') }));
const facts = (totalUsd = '28.86'): PhonePortfolio => ({ version: 'phone.portfolio.v1', bindingId,
  observedAt: new Date().toISOString(), totalUsd, holdingsComplete: true, holdingCount: 1, unpricedCount: 0, omittedCount: 0,
  holdings: [{ name: 'USD Coin', symbol: 'USDC', assetClass: 'token', amount: totalUsd, valueUsd: totalUsd, change24hPercent: null }],
});
const flush = () => new Promise<void>(resolve => setImmediate(resolve));
function voice(t: test.TestContext, backend: BrokerLiveBackend = { async delegate() {} }) {
  const transport = new PreconnectTransport();
  const session = createBrokerLiveSession({ apiKey: 'synthetic', callId: 'call_test', instructions: brokerInstructions({ direction: 'inbound', purpose: '' }),
    backend, connectionFactory: () => transport,
    onAudio() {}, onTranscript() {}, onReady() {}, onClosed() {}, closeTimeoutMs: 20 });
  t.after(() => session.close()); transport.open();
  const notes = () => transport.sent.filter(event => event.type === 'session.thinking.append').map(event => event.content);
  return { session, transport, notes };
}
const result = (state: 'running' | 'completed' = 'running'): PrimaryResult => ({ bindingId, requestId: 'trade_1', state,
  ...(state === 'completed' ? { assistant: { messageId: 'message', eventId: 'event', sequence: '1', text: 'Trade complete.' } }
    : { partialResult: { kind: 'action_status', actionState: 'confirmed', intentId: 'intent_1', toolInvocationId: 'tool_1',
      observedAt: '2026-10-17T12:53:00.000Z', text: 'The trade was confirmed.' } }),
} as PrimaryResult);

test('bound fetch starts immediately but greeting and caller input do not await it; note is silent and dated', async t => {
  let resolve!: (value: PhonePortfolio) => void, reads = 0;
  const logs: Record<string, unknown>[] = [];
  t.mock.method(console, 'info', (text: string) => logs.push(JSON.parse(text)));
  const context = createPortfolioContext('call_test', bindingId, { portfolio: async binding => {
    assert.equal(binding, bindingId); reads++; return new Promise(done => { resolve = done; });
  } }); t.after(() => context.stop());
  assert.equal(reads, 1);
  const v = voice(t); await flush(); context.attach(v.session);
  assert.equal((await v.session.appendInstructions('Greet the caller.')).status, 'accepted');
  assert.equal(v.session.appendAudio(Buffer.alloc(160, 255).toString('base64')), true);
  assert.deepEqual(v.notes(), []);
  resolve(facts()); await flush();
  assert.match(v.notes().join(' '), /Caller's portfolio as of 2026-10-17T12:52:00.000Z: Your portfolio totals about \$28.86/);
  assert.match(v.notes().at(-1)!, /only current portfolio note/);
  assert.ok(v.notes().every(text => Buffer.byteLength(text) <= 500));
  assert.equal(v.transport.sent.some(event => event.type === 'session.commentary.append'), false);
  assert.equal(logs.length, 1); assert.equal(logs[0].callId, 'call_test'); assert.equal(logs[0].outcome, 'success');
  assert.ok(Number(logs[0].latencyMs) >= 0);
});

test('confirmed trade and final completion each refresh once; successor gets only replacement', async t => {
  let reads = 0;
  const context = createPortfolioContext('call_test', bindingId, { portfolio: async () => facts(++reads === 1 ? '28.86' : '30.00') });
  t.after(() => context.stop()); const v = voice(t); await flush(); context.attach(v.session); await flush();
  context.observe(result()); context.observe(result()); context.observe(result('completed')); await flush();
  assert.equal(reads, 3);
  const text = v.notes().join(' ');
  assert.match(text, /Portfolio note withdrawn\./);
  assert.match(text, /replaces all earlier portfolio notes/);
  assert.match(text, /about \$30.00/);
  const next = voice(t); await flush(); context.detach(v.session); context.attach(next.session); await flush();
  assert.doesNotMatch(next.notes().join(' '), /28.86/);
  assert.match(next.notes().join(' '), /about \$30.00/);
  assert.equal(reads, 3, 'voice replacement reuses only the current call observation');
});

test('completion includes a second trade that finished between polls, then deduplicates repeated completion', async t => {
  let reads = 0, truth = facts('20');
  const context = createPortfolioContext('call_test', bindingId, { portfolio: async () => { reads++; return truth; } });
  t.after(() => context.stop()); const v = voice(t); await flush(); context.attach(v.session); await flush();
  const holding = (name: string, symbol: string, valueUsd: string) => ({
    ...facts().holdings[0]!, name, symbol, amount: valueUsd, valueUsd,
  });
  truth = { ...truth, holdingCount: 2, holdings: [holding('USD Coin', 'USDC', '15'), holding('Apple', 'AAPL', '5')] };
  context.observe(result()); await flush();
  assert.equal(reads, 2);
  truth = { ...truth, holdingCount: 3, holdings: [holding('USD Coin', 'USDC', '10'),
    holding('Apple', 'AAPL', '5'), holding('Tesla', 'TSLA', '5')] };
  const completed: PrimaryResult = { bindingId, requestId: 'trade_1', state: 'completed',
    assistant: { messageId: 'message', eventId: 'event', sequence: '1', text: 'Both purchases went through.' } };
  context.observe(completed); await flush();
  const notes = v.notes(), current = notes.slice(notes.findLastIndex(text => text.includes('replaces all earlier'))).join(' ');
  assert.match(current, /of TSLA\./);
  assert.match(current, /about \$10\.00/);
  assert.doesNotMatch(current, /about \$15\.00/);
  assert.match(notes.at(-1)!, /only current portfolio note/);
  context.observe(completed); await flush(); assert.equal(reads, 3);
});

test('untyped completion refreshes conservatively, while ordinary running work and prepared trades do not', async t => {
  let reads = 0;
  const context = createPortfolioContext('call_test', bindingId, { portfolio: async () => { reads++; return facts(); } });
  t.after(() => context.stop()); await flush();
  context.observe({ bindingId, requestId: 'transfer', state: 'running' });
  context.observe({ bindingId, requestId: 'transfer', state: 'running', partialResult: {
    kind: 'prepared_action', toolInvocationId: 'prepare', observedAt: facts().observedAt, text: 'Prepared only.' } });
  assert.equal(reads, 1);
  const completed = { ...result('completed'), requestId: 'transfer', partialResult: undefined };
  context.observe(completed); context.observe(completed); await flush(); assert.equal(reads, 2);
});

for (const failure of ['error', 'timeout'] as const) test(`${failure} sends no context and logs one outcome`, async t => {
  const v = voice(t); await flush();
  const logs: Record<string, unknown>[] = [];
  t.mock.method(console, 'info', (text: string) => logs.push(JSON.parse(text)));
  const api = createPrimaryApiClient({ apiBaseUrl: 'https://api.example.test',
    phoneServiceSecret: 'synthetic-phone-service-secret-32-characters', fetch: async () => {
      if (failure === 'error') throw new Error('unavailable');
      return new Promise<Response>(() => {});
    } });
  const context = createPortfolioContext('call_test', bindingId, api);
  t.after(() => context.stop()); context.attach(v.session);
  if (failure === 'timeout') t.mock.timers.tick(2_000);
  await flush(); assert.deepEqual(v.notes(), []);
  assert.equal(logs.length, 1); assert.equal(logs[0].outcome, failure);
});

test('failed refresh withdraws old facts, and an obsolete read cannot republish them', async t => {
  let resolve!: (value: PhonePortfolio) => void, reads = 0;
  const context = createPortfolioContext('call_test', bindingId, { portfolio: async () => {
    if (++reads === 1) return new Promise(done => { resolve = done; });
    throw new Error('unavailable');
  } }); t.after(() => context.stop()); const v = voice(t); await flush(); context.attach(v.session);
  context.observe(result()); await flush(); resolve(facts()); await flush();
  assert.equal(reads, 2);
  assert.ok(v.notes().every(text => text.includes('withdrawn')));
  context.stop(); context.observe({ ...result(), requestId: 'late' }); assert.equal(reads, 2);
});

test('instructions allow the current note and retain delegation for actions and judgment', () => {
  const text = brokerInstructions({ direction: 'inbound', purpose: '' });
  assert.match(text, /latest complete portfolio note replaces every earlier portfolio note/);
  assert.match(text, /holdings, cash and "how much X do I have" questions directly/);
  assert.match(text, /Delegate trades, transfers, research, prices, missing facts/);
  assert.match(text, /anything needing judgment/);
  assert.match(text, /Keep note updates silent until the caller asks/);
  assert.match(text, /at most 60 seconds old/);
  assert.match(text, /Say its as-of age naturally/);
  assert.match(text, /expired, older or missing note requires delegation as usual/);
});

test('outbound instructions contain neither the portfolio paragraph nor the delegation exception', () => {
  const text = brokerInstructions({ direction: 'outbound', purpose: '' });
  assert.doesNotMatch(text, /portfolio note/);
  assert.match(text, /ongoing work\. Obtain a result for that request during this call/);
});

test('background refresh replaces changed content, sends only timestamps for unchanged content, and stops at hangup', async t => {
  let reads = 0;
  const logs: Record<string, unknown>[] = [];
  t.mock.method(console, 'info', (text: string) => logs.push(JSON.parse(text)));
  const context = createPortfolioContext('call_test', bindingId, { portfolio: async () => facts(++reads < 3 ? '10' : '20') });
  t.after(() => context.stop()); const v = voice(t); await flush(); context.attach(v.session); await flush();
  const replacements = () => v.notes().filter(text => text.includes('replaces all earlier')).length;
  assert.equal(reads, 1); assert.equal(replacements(), 1);
  t.mock.timers.tick(29_999); await flush(); assert.equal(reads, 1);
  t.mock.timers.tick(1); await flush();
  assert.equal(reads, 2); assert.equal(replacements(), 1);
  assert.match(v.notes().at(-1)!, /is unchanged.*2026-10-17T12:52:30.000Z/);
  assert.equal(logs.at(-1)!.publication, 'unchanged');
  t.mock.timers.tick(30_000); await flush();
  assert.equal(reads, 3); assert.equal(replacements(), 2);
  assert.match(v.notes().join(' '), /about \$20.00/);
  assert.equal(logs.at(-1)!.publication, 'published');
  assert.ok(logs.every(log => Number(log.latencyMs) >= 0));
  const before = v.notes().length;
  context.stop(); t.mock.timers.tick(120_000); await flush();
  assert.equal(reads, 3); assert.equal(v.notes().length, before);
});

test('post-action reads run serially after a superseded background read; periodic ticks cannot supersede them', async t => {
  let reads = 0, active = 0, maximum = 0;
  const pending: Array<{ resolve(value: PhonePortfolio): void; signal?: AbortSignal }> = [];
  const context = createPortfolioContext('call_test', bindingId, { portfolio: async (_binding, signal) => {
    reads++; maximum = Math.max(maximum, ++active);
    try {
      if (reads === 1) return facts('10');
      return await new Promise<PhonePortfolio>(resolve => pending.push({ resolve, signal }));
    } finally { active--; }
  } }); t.after(() => context.stop());
  const v = voice(t); await flush(); context.attach(v.session); await flush();
  t.mock.timers.tick(30_000); await flush(); assert.equal(reads, 2);
  context.observe(result());
  assert.equal(pending[0].signal?.aborted, true); assert.equal(reads, 2, 'aborted read must settle first');
  pending[0].resolve(facts('20')); await flush(); assert.equal(reads, 3);
  t.mock.timers.tick(30_000); await flush();
  assert.equal(reads, 3); assert.equal(pending[1].signal?.aborted, false, 'background never cancels the post-action read');
  pending[1].resolve(facts('30')); await flush();
  assert.equal(maximum, 1);
  assert.doesNotMatch(v.notes().join(' '), /about \$20\.00/);
  assert.match(v.notes().join(' '), /about \$30\.00/);
  assert.match(v.notes().at(-1)!, /only current portfolio note/);
});

test('failed background reads retain the observation only until its 60-second expiry', async t => {
  let reads = 0;
  const context = createPortfolioContext('call_test', bindingId, { portfolio: async () => {
    if (++reads > 1) throw new Error('unavailable'); return facts();
  } }); t.after(() => context.stop());
  const v = voice(t); await flush(); context.attach(v.session); await flush();
  t.mock.timers.tick(30_000); await flush();
  assert.match(v.notes().at(-1)!, /only current portfolio note/);
  t.mock.timers.tick(29_999); await flush();
  assert.match(v.notes().at(-1)!, /only current portfolio note/);
  t.mock.timers.tick(1); await flush();
  assert.match(v.notes().at(-1)!, /Portfolio note withdrawn/);
  assert.equal(v.transport.sent.some(event => event.type === 'session.commentary.append'), false);
});

test('background polling waits for live attachment and for a pending action to settle', async t => {
  let reads = 0;
  const context = createPortfolioContext('call_test', bindingId, { portfolio: async () => { reads++; return facts(); } });
  t.after(() => context.stop()); await flush();
  t.mock.timers.tick(30_000); await flush(); assert.equal(reads, 1);
  const v = voice(t); await flush(); context.attach(v.session); await flush();
  const confirmed = result();
  context.observe({ ...confirmed, partialResult: { ...confirmed.partialResult!, actionState: 'pending' } } as PrimaryResult);
  t.mock.timers.tick(30_000); await flush(); assert.equal(reads, 1);
  context.observe(confirmed); await flush(); assert.equal(reads, 2);
  context.detach(v.session); t.mock.timers.tick(30_000); await flush(); assert.equal(reads, 2);
});

for (const state of ['missing', 'stale', 'fresh'] as const)
test(`${state} portfolio context leaves client delegation unchanged for refresh and non-portfolio requests`, async t => {
  let reads = 0;
  const delegated: LiveDelegationRequest[] = [];
  const context = createPortfolioContext('call_test', bindingId, { portfolio: async () => {
    if (++reads > 1 || state === 'missing') throw new Error('unavailable'); return facts();
  } }); t.after(() => context.stop());
  const v = voice(t, { async delegate(request) { delegated.push(request); } });
  await flush(); context.attach(v.session); await flush();
  if (state === 'stale') { t.mock.timers.tick(60_000); await flush(); assert.match(v.notes().at(-1)!, /withdrawn/); }
  if (state === 'missing') assert.deepEqual(v.notes(), []);
  const before = reads;
  for (const [index, text] of ['What is my balance now?', 'Buy one dollar of Apple.', 'Research Apple.', 'What is the price of SOL?'].entries()) {
    v.transport.receive({ type: 'session.input_transcript.delta', event_id: `caller_${index}`, delta: text, start_ms: index * 3000, end_ms: index * 3000 + 1000 });
    v.transport.receive({ type: 'session.delegation.created', offset_ms: index * 3000 + 1200,
      delegation: { id: `delegation_${index}`, type: 'delegation', target: 'client' } });
    await flush();
    assert.equal(delegated.length, index + 1);
    assert.equal(delegated[index].transcript.fragments.at(-1)!.text, text);
  }
  assert.equal(reads, before, 'no request starts a portfolio read or routing step');
  assert.deepEqual(v.transport.sent.find(event => event.type === 'session.start')!.session.delegation, { type: 'client' });
  assert.equal(v.transport.sent.some(event => event.type === 'response.create' || event.type === 'response.item.create'), false);
});

test('hangup aborts an in-flight background read and prevents late publication', async t => {
  let reads = 0, resolve!: (value: PhonePortfolio) => void, signal: AbortSignal | undefined;
  const context = createPortfolioContext('call_test', bindingId, { portfolio: async (_binding, nextSignal) => {
    if (++reads === 1) return facts();
    signal = nextSignal; return new Promise(done => { resolve = done; });
  } }); t.after(() => context.stop());
  const v = voice(t); await flush(); context.attach(v.session); await flush();
  t.mock.timers.tick(30_000); await flush(); assert.equal(reads, 2);
  const before = v.notes().length;
  context.stop(); assert.equal(signal?.aborted, true); resolve(facts('99')); await flush();
  t.mock.timers.tick(90_000); await flush();
  assert.equal(reads, 2); assert.equal(v.notes().length, before);
});

test('review: a delayed note carries its broker-computed age and no expiry timestamp', async t => {
  const old = facts(); let resolve!: (value: PhonePortfolio) => void;
  const context = createPortfolioContext('call_test', bindingId, { portfolio: () => new Promise(done => { resolve = done; }) });
  t.after(() => context.stop()); const v = voice(t); await flush(); context.attach(v.session);
  t.mock.timers.tick(40_000); resolve(old); await flush();
  assert.match(v.notes().at(-1)!, /as of 40 seconds ago/);
  assert.doesNotMatch(v.notes().join(' '), /valid until|few seconds ago/);
});

test('review: each overlapping trade pauses reads until it settles and withdraws an intervening note', async t => {
  let reads = 0;
  const context = createPortfolioContext('call_test', bindingId, { portfolio: async () => { reads++; return facts(String(reads)); } });
  t.after(() => context.stop()); const v = voice(t); await flush(); context.attach(v.session); await flush();
  const action = (intentId: string, actionState: 'pending' | 'confirmed') => ({ ...result(),
    partialResult: { ...result().partialResult!, intentId, actionState } } as PrimaryResult);
  context.observe(action('one', 'pending')); context.observe(action('two', 'pending'));
  context.observe(action('one', 'confirmed')); await flush();
  t.mock.timers.tick(30_000); await flush();
  assert.equal(reads, 1, 'confirming one trade cannot unpause the other trade');
  context.observe(action('two', 'confirmed')); await flush(); assert.equal(reads, 2);
  context.observe(action('three', 'pending'));
  // A late confirmation for trade two must not unpause trade three.
  context.observe(action('two', 'confirmed')); t.mock.timers.tick(30_000); await flush(); assert.equal(reads, 2);
  const before = v.notes().length;
  context.observe(action('three', 'confirmed')); await flush();
  assert.match(v.notes()[before]!, /withdrawn/);
  assert.equal(reads, 3);
  assert.match(v.notes().at(-1)!, /only current portfolio note/);
});

test('review: an expired note is read immediately on reattach', async t => {
  let reads = 0;
  const context = createPortfolioContext('call_test', bindingId, { portfolio: async () => { reads++; return facts(String(reads)); } });
  t.after(() => context.stop()); const v = voice(t); await flush(); context.attach(v.session); await flush();
  context.detach(v.session); t.mock.timers.tick(61_000); await flush();
  const next = voice(t); await flush(); context.attach(next.session); await flush();
  assert.equal(reads, 2, 'reattach must not wait for the next poll');
  assert.match(next.notes().join(' '), /about \$2\.00/);
  assert.doesNotMatch(next.notes().join(' '), /about \$1\.00/);
});

test('review: detach clears the periodic timer and hangup clears the expiry timer', async t => {
  const intervals = t.mock.method(globalThis, 'clearInterval');
  const timeouts = t.mock.method(globalThis, 'clearTimeout');
  const context = createPortfolioContext('call_test', bindingId, { portfolio: async () => facts() });
  t.after(() => context.stop()); const v = voice(t); await flush(); context.attach(v.session); await flush();
  intervals.mock.resetCalls(); context.detach(v.session);
  assert.ok(intervals.mock.calls.some(call => call.arguments[0] !== undefined), 'detach must release its timer resource');
  timeouts.mock.resetCalls(); context.stop();
  assert.ok(timeouts.mock.calls.some(call => call.arguments[0] !== undefined), 'hangup must release its expiry timer resource');
});

test('call truth: a dispatched send observation withdraws the portfolio before completion', async t => {
  let reads = 0;
  const context = createPortfolioContext('call_test', bindingId, { portfolio: async () => { reads++; return facts(); } });
  t.after(() => context.stop()); const v = voice(t); await flush(); context.attach(v.session); await flush();
  const requestId = '33333333-3333-4333-8333-333333333333';
  const api = createPrimaryApiClient({ apiBaseUrl: 'https://api.example.test', phoneServiceSecret: 'synthetic-phone-service-secret-32-characters',
    fetch: async () => Response.json({ bindingId, requestId, state: 'running', partialResult: {
      kind: 'action_status', actionState: 'pending', intentId: '44444444-4444-4444-8444-444444444444',
      toolInvocationId: '55555555-5555-5555-8555-555555555555', observedAt: facts().observedAt,
      text: 'The account action is being attempted. Its outcome is not yet known.' } }) });
  context.observe(await api.result(bindingId, requestId)); await flush();
  assert.match(v.notes().at(-1)!, /Portfolio note withdrawn/);
  t.mock.timers.tick(30_000); await flush(); assert.equal(reads, 1, 'send stays paused while the attempt is pending');
  context.observe({ bindingId, requestId, state: 'completed', assistant: { messageId: 'm', eventId: 'e', sequence: '1', text: 'The transfer was confirmed.' } });
  await flush(); assert.equal(reads, 2);
  assert.match(v.notes().at(-1)!, /only current portfolio note/);
});

test('every holding name a note carried stays known for the call, even after the note is withdrawn', async t => {
  let reads = 0;
  const context = createPortfolioContext('call_test', bindingId, { portfolio: async () => ({ ...facts(), holdings: reads++ === 0
    ? [{ name: 'Maple Token', symbol: 'MAPLE', assetClass: 'token', amount: '5', valueUsd: '21.40', change24hPercent: null },
      { name: 'Unnamed', symbol: 'not a label!', assetClass: 'token', amount: '1', valueUsd: '1', change24hPercent: null }]
    : [{ name: 'USD Coin', symbol: 'USDC', assetClass: 'token', amount: '7', valueUsd: '7', change24hPercent: null }] }) });
  t.after(() => context.stop()); const v = voice(t); await flush(); context.attach(v.session); await flush();
  assert.deepEqual(context.labels(), ['MAPLE'], 'only the names a note states, as it states them');
  context.observe(result('completed')); await flush();
  assert.match(v.notes().join(' '), /Portfolio note withdrawn\./);
  assert.deepEqual(context.labels(), ['MAPLE', 'USDC'], 'the voice may already have read MAPLE out');
});
