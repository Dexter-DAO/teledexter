// ---------------------------------------------------------------------------
// session.on('mcp_tool_call_completed') handler.
//
// Extracted verbatim from src/server.ts in Step 2 of the session-event
// refactor. Pushes the completed (or incomplete=failed) MCP tool call
// into callEntry.toolCalls, with output split between `result` and
// `error` based on the status flag. Step 3 may add more captures here;
// this step is pure extraction.
// ---------------------------------------------------------------------------

import type { CallLogEntry } from '../../types/CallEntry.js';

interface RawMcpCall {
  name?: string;
  arguments?: string;
  output?: string | null;
  status?: 'in_progress' | 'completed' | 'incomplete' | string;
  itemId?: string;
  call_id?: string;
}

export function onMcpToolCallCompleted(callEntry: CallLogEntry, call: RawMcpCall): void {
  const status = call?.status || 'completed';
  const output = call?.output ?? null;
  const failed = status === 'incomplete';
  console.log(`[dexter-phone] MCP tool ${status}:`, call?.name, failed ? '(FAILED)' : '');
  callEntry.toolCalls.push({
    name: call?.name || 'unknown',
    arguments: call?.arguments || '',
    timestamp: new Date().toISOString(),
    itemId: call?.itemId,
    status,
    // SDK signals failure via status:'incomplete'; output may carry the
    // server's error string in that case. Surface both for the Observatory
    // detail page — Result for happy path, Error for incomplete.
    result: failed ? undefined : output,
    error: failed ? (output ?? 'tool returned status=incomplete') : undefined,
  });
}
