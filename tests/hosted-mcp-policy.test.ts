import assert from 'node:assert/strict';
import test from 'node:test';

import {
  HARK_HOSTED_MCP_ALLOWED_TOOLS,
  buildPhoneHostedMcpTool,
} from '../src/session/hostedMcpPolicy.js';

const FORBIDDEN_HARK_TOOLS = [
  'x402_check',
  'x402_fetch',
  'x402_access',
  'dexter_prepare_asset_action',
  'dexter_execute_asset_action',
  'dexter_reconcile_asset_action',
] as const;

test('Hark-bound hosted MCP serializes the exact research/status-plus-wallet-setup allowed_tools provider payload', () => {
  const tool = buildPhoneHostedMcpTool({
    serverUrl: 'https://open.dexter.cash/mcp',
    linkToken: 'one-call-link-token',
    harkBound: true,
  });
  const providerPayload = JSON.parse(JSON.stringify(tool.providerData)) as Record<string, unknown>;

  assert.deepEqual(providerPayload.allowed_tools, {
    tool_names: [...HARK_HOSTED_MCP_ALLOWED_TOOLS],
  });
  assert.deepEqual(providerPayload.headers, {
    'x-dexter-link-token': 'one-call-link-token',
  });
  assert.equal(providerPayload.server_url, 'https://open.dexter.cash/mcp');

  const allowed = (providerPayload.allowed_tools as { tool_names: string[] }).tool_names;
  for (const forbidden of FORBIDDEN_HARK_TOOLS) {
    assert.equal(allowed.includes(forbidden), false, `${forbidden} must remain unavailable`);
  }
});

test('non-Hark hosted MCP omits allowed_tools and retains the full live roster', () => {
  const tool = buildPhoneHostedMcpTool({
    serverUrl: 'https://open.dexter.cash/mcp',
    linkToken: 'one-call-link-token',
  });
  const providerPayload = JSON.parse(JSON.stringify(tool.providerData)) as Record<string, unknown>;

  assert.equal(Object.hasOwn(providerPayload, 'allowed_tools'), false);
  assert.deepEqual(providerPayload.headers, {
    'x-dexter-link-token': 'one-call-link-token',
  });
});
