// ---------------------------------------------------------------------------
// session.on('error') handler.
//
// Extracted verbatim from src/server.ts in Step 2 of the session-event
// refactor. Beyond logging, this handler ALSO:
//   - One-shots the MCP alias correction system message via safeSendMessage
//     when the model keeps trying the bogus `mcp_dexter` tool name. The
//     mcpAliasCorrectionSent flag on SessionState prevents the message from
//     firing more than once per call.
//   - Suppresses conversation_already_has_active_response warnings down to
//     a single console.warn (and returns).
//
// Step 3 will enrich the error capture (push to callEntry.errors etc.);
// this step is pure extraction.
// ---------------------------------------------------------------------------

import type { SessionState } from '../SessionState.js';
import { PHONE_OPENDEXTER_TOOL_NAMES } from '../phoneOpenDexterInstructions.js';

/**
 * Errors after which the session will never produce audio again — the caller
 * is guaranteed dead air unless we hand them to the Twilio <Say> fallback.
 * Detection is by content, not code, because the Realtime API nests these
 * inconsistently: quota/billing exhaustion, auth failure, hard rate limits.
 */
export function classifyFatalSessionError(evt: any): string | null {
  const code = String(
    evt?.error?.error?.code || evt?.error?.code || evt?.code || '',
  ).toLowerCase();
  const message = String(
    evt?.error?.error?.message || evt?.error?.message || evt?.message || evt || '',
  ).toLowerCase();
  if (code === 'insufficient_quota' || message.includes('exceeded your current quota') || message.includes('billing')) {
    return 'openai_quota_exhausted';
  }
  if (code === 'invalid_api_key' || message.includes('incorrect api key') || message.includes('invalid api key')) {
    return 'openai_auth_failed';
  }
  if (code === 'rate_limit_exceeded' || message.includes('rate limit')) {
    return 'openai_rate_limited';
  }
  if (
    code === 'model_not_found'
    || (message.includes('model') && message.includes('not found'))
  ) {
    return 'openai_model_unavailable';
  }
  return null;
}

export function onError(
  state: SessionState,
  safeSendMessage: (text: string, delayMs?: number) => void,
  evt: any,
): void {
  const code = evt?.error?.error?.code || evt?.error?.code || evt?.code;
  const message =
    evt?.error?.error?.message ||
    evt?.error?.message ||
    evt?.message ||
    String(evt || '');
  if (!state.mcpAliasCorrectionSent && String(message).includes('Tool mcp_dexter not found')) {
    state.mcpAliasCorrectionSent = true;
    safeSendMessage(
      `[SYSTEM] Tool alias failed. Retry using direct tools only: ${PHONE_OPENDEXTER_TOOL_NAMES.join(', ')}, send_sms, send_verification_code, check_verification_code, dexter_knowledge. x402_fetch accepts only intentId and maxAmountAtomic from the preceding connected x402_check. Do NOT call mcp_dexter or x402_pay.`,
      200,
    );
  }
  if (code === 'conversation_already_has_active_response') {
    console.warn(
      '[dexter-phone] Realtime overlap warning:',
      message,
    );
    return;
  }
  console.error('[dexter-phone] Realtime session error:', evt);
}
