import assert from 'node:assert/strict';
import test from 'node:test';
import { createPortfolioContext } from '../src/broker/portfolioContext.js';
import { brokerInstructions } from '../src/broker/instructions.js';
import type { PhonePortfolio } from '../src/broker/portfolio.js';
import { createPrimaryApiClient, type PrimaryResult } from '../src/broker/primaryApi.js';
import { createBrokerLiveSession } from '../src/live/session.js';
import { PreconnectTransport } from './helpers/preconnectTransport.js';

const bindingId = '11111111-1111-4111-8111-111111111111';
const facts = (totalUsd = '28.86'): PhonePortfolio => ({ version: 'phone.portfolio.v1', bindingId,
  observedAt: '2026-10-17T12:52:00.000Z', totalUsd, holdingsComplete: true, holdingCount: 1, unpricedCount: 0, omittedCount: 0,
  holdings: [{ name: 'USD Coin', symbol: 'USDC', assetClass: 'token', amount: totalUsd, valueUsd: totalUsd, change24hPercent: null }],
});
const flush = () => new Promise<void>(resolve => setImmediate(resolve));
function voice(t: test.TestContext) {
  const transport = new PreconnectTransport();
  const session = createBrokerLiveSession({ apiKey: 'synthetic', callId: 'call_test', instructions: brokerInstructions({ direction: 'inbound', purpose: '' }),
    backend: { async delegate() {} }, connectionFactory: () => transport,
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
  t.mock.timers.enable({ apis: ['setTimeout'] });
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
});

test('outbound instructions contain neither the portfolio paragraph nor the delegation exception', () => {
  const text = brokerInstructions({ direction: 'outbound', purpose: '' });
  assert.doesNotMatch(text, /portfolio note/);
  assert.match(text, /ongoing work\. Obtain a result for that request during this call/);
});
