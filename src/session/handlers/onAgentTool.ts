import type { CallLogEntry } from '../../types/CallEntry.js';

type RawFunctionCall = {
  callId?: string;
  id?: string;
  name?: string;
  arguments?: string;
  status?: string;
};

function bounded(value: unknown, max = 16_000): string {
  const text = typeof value === 'string' ? value : JSON.stringify(value ?? null);
  return text.length <= max ? text : `${text.slice(0, max)}…[truncated]`;
}

function toolCallId(call: RawFunctionCall): string | undefined {
  return call.callId || call.id;
}

export function onAgentToolStart(
  callEntry: CallLogEntry,
  tool: { name?: string },
  details: { toolCall?: RawFunctionCall },
): void {
  const raw = details.toolCall ?? {};
  const itemId = toolCallId(raw);
  if (itemId && callEntry.toolCalls.some((entry) => entry.itemId === itemId)) return;

  callEntry.toolCalls.push({
    name: tool.name || raw.name || 'unknown_local_tool',
    arguments: bounded(raw.arguments ?? '', 4_000),
    timestamp: new Date().toISOString(),
    itemId,
    status: 'in_progress',
    result: null,
    error: null,
  });
}

export function onAgentToolEnd(
  callEntry: CallLogEntry,
  tool: { name?: string },
  result: unknown,
  details: { toolCall?: RawFunctionCall },
): void {
  const raw = details.toolCall ?? {};
  const itemId = toolCallId(raw);
  const existing = itemId
    ? callEntry.toolCalls.find((entry) => entry.itemId === itemId)
    : undefined;
  const entry = existing ?? {
    name: tool.name || raw.name || 'unknown_local_tool',
    arguments: bounded(raw.arguments ?? '', 4_000),
    timestamp: new Date().toISOString(),
    itemId,
    status: 'in_progress',
    result: null,
    error: null,
  };
  if (!existing) callEntry.toolCalls.push(entry);

  entry.status = 'completed';
  entry.result = bounded(result);
  entry.error = null;
}
