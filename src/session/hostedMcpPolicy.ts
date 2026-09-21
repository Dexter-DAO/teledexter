import { hostedMcpTool } from '@openai/agents';

/**
 * Hark Phone is an attention and response lane, not a second executor for the
 * originating task. This research/status-plus-wallet-setup allowlist includes
 * x402_wallet, which can create or resume setup state but cannot execute the Hark
 * case instruction. Omitting allowedTools would expose every hosted tool.
 */
export const HARK_HOSTED_MCP_ALLOWED_TOOLS = [
  'x402_search',
  'x402_status',
  'x402_wallet',
  'dexter_portfolio',
  'dexter_asset_action_status',
  'dexter_wallet_history',
] as const;

export interface PhoneHostedMcpToolOptions {
  serverUrl: string;
  linkToken?: string | null;
  harkBound?: boolean;
}

/** Build the exact provider payload used by Realtime for this call lane. */
export function buildPhoneHostedMcpTool(options: PhoneHostedMcpToolOptions) {
  const headers = options.linkToken
    ? { 'x-dexter-link-token': options.linkToken }
    : undefined;

  return hostedMcpTool({
    serverLabel: 'dexter',
    serverUrl: options.serverUrl,
    headers,
    allowedTools: options.harkBound
      ? [...HARK_HOSTED_MCP_ALLOWED_TOOLS]
      : undefined,
  });
}
