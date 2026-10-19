import { z } from 'zod';

const decimal = z.string().regex(/^\d+(?:\.\d+)?$/).max(128)
  .refine(value => Number.isFinite(Number(value)));
const count = z.number().int().nonnegative().safe();
export const phonePortfolioSchema = z.object({
  version: z.literal('phone.portfolio.v1'), bindingId: z.string().uuid(),
  observedAt: z.string().datetime(), totalUsd: decimal, holdingsComplete: z.boolean(),
  holdingCount: count, unpricedCount: count, omittedCount: count,
  holdings: z.array(z.object({
    name: z.string().max(256).nullable(), symbol: z.string().max(256).nullable(),
    assetClass: z.string().min(1).max(64), valueUsd: decimal.nullable(), amount: decimal,
    change24hPercent: z.string().regex(/^-?\d+(?:\.\d+)?$/).max(128).nullable(),
  }).strict()).max(50),
}).strict();
export type PhonePortfolio = z.infer<typeof phonePortfolioSchema>;

const money = (value: string) => Number(value) > 0 && Number(value) < 0.01
  ? 'less than a cent'
  : `about ${new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(Number(value))}`;

const label = (symbol: string | null) => symbol && /^[A-Za-z0-9._-]{1,32}$/.test(symbol) ? symbol : undefined;
/** The holding names a note states, exactly as portfolioFactsText writes them. */
export const portfolioLabels = (portfolio: PhonePortfolio): string[] =>
  portfolio.holdings.flatMap(holding => label(holding.symbol) ?? []);

/** Facts for the existing Live dollars-first presentation policy. Token labels
 * stay bounded data inside fixed sentences; quantities appear only on request. */
export function portfolioFactsText(portfolio: PhonePortfolio, quantities = false): string {
  const text = [portfolio.unpricedCount || !portfolio.holdingsComplete
    ? `Your priced holdings total ${money(portfolio.totalUsd)}.`
    : `Your portfolio totals ${money(portfolio.totalUsd)}.`];
  for (const holding of portfolio.holdings) {
    const name = label(holding.symbol) ?? 'an unnamed token';
    text.push(holding.valueUsd === null ? `The value of ${name} is unavailable.`
      : `You have ${money(holding.valueUsd)} of ${name}.`);
    if (quantities) text.push(`The quantity is about ${Number(holding.amount).toLocaleString('en-US', { maximumSignificantDigits: 2 })}.`);
  }
  if (portfolio.unpricedCount) text.push(portfolio.unpricedCount === 1
    ? 'One holding has no price and is excluded from the total.'
    : `${portfolio.unpricedCount} holdings have no price and are excluded from the total.`);
  if (!portfolio.holdingsComplete || portfolio.omittedCount) text.push('This read does not list every holding.');
  return text.join(' ');
}
