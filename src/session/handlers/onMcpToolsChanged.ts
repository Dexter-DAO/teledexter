// ---------------------------------------------------------------------------
// session.on('mcp_tools_changed') handler.
//
// Extracted verbatim from src/server.ts in Step 2 of the session-event
// refactor. Log-only — no state mutation. Do not enrich here; future
// captures belong in Step 3 (see the planning doc).
// ---------------------------------------------------------------------------

export function onMcpToolsChanged(tools: { name: string }[]): void {
  const names = tools.map((t) => t.name).join(', ');
  console.log(`[dexter-phone] MCP tools loaded: ${names || 'none'}`);
}
