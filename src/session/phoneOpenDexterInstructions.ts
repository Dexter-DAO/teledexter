/**
 * The exact hosted OpenDexter purchase contract spoken through Dexter Phone.
 * Keep this separate from the character prompt so tests can prove that Phone
 * never falls back to the retired URL-carrying paid-call shape.
 */
export const PHONE_OPENDEXTER_TOOL_NAMES = [
  'x402_search',
  'x402_check',
  'x402_fetch',
  'x402_status',
  'x402_access',
  'x402_wallet',
  'dexter_portfolio',
  'dexter_find_assets',
  'dexter_prepare_asset_action',
  'dexter_execute_asset_action',
  'dexter_asset_action_status',
  'dexter_reconcile_asset_action',
  'dexter_wallet_history',
] as const;

export const PHONE_OPENDXTER_INSTRUCTIONS = `- x402_search: Find APIs by the caller's actual job. Leave network unset unless the caller explicitly requires one chain; OpenDexter may reach compatible sellers across rails.
- x402_check: Check one exact HTTPS request before paying. For POST, PUT, or DELETE, body is the seller's exact raw JSON string; do not parse, normalize, or reserialize it. Explain that a non-GET check may submit the request to the provider and get the caller's confirmation before doing it. Anonymous checks are quote-only. A connected check returns the opaque intentId for this exact request.
- x402_fetch: Execute one approved purchase. Pass only the opaque intentId from the connected x402_check and the positive maxAmountAtomic ceiling covered by the caller's instruction. Never pass a URL, method, body, seller terms, route, or prepared purchase to x402_fetch. The removed x402_pay alias is not a tool.
- x402_status: Inspect the same purchase intent without redispatching it. After a preparing, ambiguous, timeout, or post-dispatch result, call x402_status with the same intentId and never automatically call x402_fetch again.
- x402_access: Access one identity-gated endpoint without payment. Preserve the exact raw body for non-GET requests and confirm before a probe that may mutate provider state.
- x402_wallet: Read the caller's connected Dexter Wallet, including readiness, cash, deposit address, and recent activity. Authentication, wallet binding, enrollment, funding, and execution readiness are separate states.
- dexter_portfolio: Read the governed portfolio bound to this call. Preserve the returned holding identity and exact amounts.
- dexter_find_assets: Find a stock or token by company name, ticker, token name, or exact mint, including assets outside current holdings. Resolve ambiguous names with the caller and preserve the selected mint and issuer.
- dexter_prepare_asset_action: Prepare one exact Buy, Sell, or Send under the caller's current authority. Use companyQuery for stocks and tokenMint for discovered tokens when the supplied schema supports them. Use canonical assetId for other supported holding actions. Preparation does not execute the trade. When setup is pending, continue the same operationId and exact request after the returned retry delay.
- dexter_execute_asset_action: Execute only the exact server-prepared intent covered by current authority. Never reconstruct or widen its action fields.
- dexter_asset_action_status: Read one governed action's durable status without redispatching it.
- dexter_reconcile_asset_action: Reconcile the same intent only when its durable status requires it; never create a replacement action for an ambiguous outcome.
- dexter_wallet_history: Read governed action history with server-issued pagination cursors only.

PAID API FLOW:
1. Search when the seller is not already exact.
2. Check the exact URL, method, and raw request body. A check never authorizes payment.
3. If the result is quote-only, help the caller connect OpenDexter and repeat that exact check once; never invent an intentId.
4. State the exact seller, request, and price. If the caller's current instruction already covers those exact terms and a positive ceiling, continue without asking twice; otherwise ask only for the missing approval.
5. Call x402_fetch once with only intentId and maxAmountAtomic.
6. Report provider output separately from charge, merchant acknowledgment, chain evidence, ambiguity, and reconciliation state.`;
