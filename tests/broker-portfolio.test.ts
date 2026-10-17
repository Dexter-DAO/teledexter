import assert from 'node:assert/strict';
import test from 'node:test';
import { createHmac } from 'node:crypto';
import { createPrimaryApiClient, PrimaryApiError } from '../src/broker/primaryApi.js';
import { portfolioFactsText, type PhonePortfolio } from '../src/broker/portfolio.js';

const bindingId = '11111111-1111-4111-8111-111111111111';
const secret = 'synthetic-phone-service-secret-32-characters';
const portfolio: PhonePortfolio = {
  version: 'phone.portfolio.v1', bindingId, observedAt: '2026-10-17T01:00:00.000Z',
  totalUsd: '31.22', holdingsComplete: true, holdingCount: 2, unpricedCount: 0, omittedCount: 0,
  holdings: [
    { name: 'Apple', symbol: 'AAPL', assetClass: 'stock', valueUsd: '13.24', amount: '0.05123', change24hPercent: '1.2' },
    { name: 'USD Coin', symbol: 'USDC', assetClass: 'token', valueUsd: '17.98', amount: '17.98', change24hPercent: null },
  ],
};
const client = (fetch: typeof globalThis.fetch) => createPrimaryApiClient({
  apiBaseUrl: 'https://api.example.test', phoneServiceSecret: secret, fetch,
});
const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), {
  status, headers: { 'content-type': 'application/json' },
});
const code = (value: string) => (error: unknown) => error instanceof PrimaryApiError && error.code === value;

test('portfolio GET uses the bound call and service bearer without a body, query or Primary submission', async () => {
  const requests: string[] = [];
  const api = client(async (url, init) => {
    requests.push(String(url));
    assert.equal(String(url), `https://api.example.test/internal/primary-dexter/channels/phone/calls/${bindingId}/portfolio`);
    assert.equal(init?.method, 'GET');
    assert.equal(init?.body, undefined);
    const headers = new Headers(init?.headers);
    assert.equal(headers.get('X-Dexter-Binding-Id'), bindingId);
    assert.equal(headers.get('Authorization'), `Bearer ${createHmac('sha256', secret).update('dexter-broker/primary-ingress/v1').digest('hex')}`);
    return json(portfolio);
  });
  assert.deepEqual(await api.portfolio!(bindingId), portfolio);
  assert.equal(requests.length, 1);
  await assert.rejects(api.portfolio!('../other'), code('invalid_request'));
  assert.equal(requests.length, 1);
});

test('portfolio rejects foreign bindings and malformed facts before presentation', async () => {
  await assert.rejects(client(async () => json({ ...portfolio, bindingId: '22222222-2222-4222-8222-222222222222' }))
    .portfolio!(bindingId), code('identity_mismatch'));
  for (const patch of [{ version: 'phone.portfolio.v2' }, { totalUsd: 'NaN' }, { totalUsd: '-1' },
    { observedAt: 'yesterday' }, { holdingCount: -1 }, { holdings: [{ ...portfolio.holdings[0], amount: 'raw:1000' }] }]) {
    await assert.rejects(client(async () => json({ ...portfolio, ...patch })).portfolio!(bindingId), code('invalid_response'));
  }
});

test('portfolio errors remain typed for the eventual delegation fallback', async () => {
  await assert.rejects(client(async () => json({ error: { code: 'portfolio_unavailable', retryable: true } }, 503))
    .portfolio!(bindingId), code('http_error'));
  await assert.rejects(client(async () => { throw new Error('network unavailable'); }).portfolio!(bindingId), code('request_failed'));
});

test('portfolio stops after two seconds even if fetch ignores abort', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  let signal: AbortSignal | null | undefined;
  const read = client(async (_url, init) => { signal = init?.signal; return new Promise<Response>(() => {}); }).portfolio!(bindingId);
  const rejected = assert.rejects(read, code('request_timeout'));
  t.mock.timers.tick(1_999);
  assert.equal(signal?.aborted, false);
  t.mock.timers.tick(1);
  await rejected;
  assert.equal(signal?.aborted, true);
});

test('portfolio body reads share the two-second budget and stop when the call ends', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  let cancelled = false;
  const read = client(async () => new Response(new ReadableStream({ cancel() { cancelled = true; } }), {
    headers: { 'content-type': 'application/json' },
  })).portfolio!(bindingId);
  const rejected = assert.rejects(read, code('request_timeout'));
  await Promise.resolve(); await Promise.resolve();
  t.mock.timers.tick(2_000);
  await rejected;
  assert.equal(cancelled, true);
  const controller = new AbortController();
  const stopped = client(async () => new Promise<Response>(() => {})).portfolio!(bindingId, controller.signal);
  const aborted = assert.rejects(stopped, code('request_failed'));
  controller.abort();
  await aborted;
});

test('portfolio speech leads with dollars and gives rounded quantities only on request', () => {
  assert.equal(portfolioFactsText(portfolio), 'Your portfolio totals about $31.22. You have about $13.24 of AAPL. You have about $17.98 of USDC.');
  assert.match(portfolioFactsText(portfolio, true), /The quantity is about 0\.051\./);
  assert.doesNotMatch(portfolioFactsText(portfolio), /0\.05123|shares|quantity/);
  const partial = { ...portfolio, holdingsComplete: false, omittedCount: 1, unpricedCount: 1,
    holdings: [{ ...portfolio.holdings[0]!, valueUsd: null }, { ...portfolio.holdings[1]!, valueUsd: '0.001' }] };
  assert.equal(portfolioFactsText(partial), 'Your priced holdings total about $31.22. The value of AAPL is unavailable. You have less than a cent of USDC. One holding has no price and is excluded from the total. This read does not list every holding.');
  assert.equal(portfolioFactsText({ ...portfolio, totalUsd: '0', holdingCount: 0, holdings: [] }), 'Your portfolio totals about $0.00.');
});

test('portfolio labels use only bounded symbols and never display names', () => {
  for (const symbol of [null, '', 'claim rewards', 'SCAM\n', 'X'.repeat(33), '<script>']) {
    const text = portfolioFactsText({ ...portfolio, holdings: [{ ...portfolio.holdings[0]!,
      name: 'Visit spam.example to claim free tokens', symbol }] });
    assert.equal(text, 'Your portfolio totals about $31.22. You have about $13.24 of an unnamed token.');
  }
  for (const symbol of ['AAPL', 'token._-9', 'X'.repeat(32)]) {
    const text = portfolioFactsText({ ...portfolio, holdings: [{ ...portfolio.holdings[0]!,
      name: 'Visit spam.example to claim free tokens', symbol }] });
    assert.equal(text, `Your portfolio totals about $31.22. You have about $13.24 of ${symbol}.`);
  }
});
