// ---------------------------------------------------------------------------
// session.on('tool_approval_requested') handler.
//
// Extracted verbatim from src/server.ts in Step 2 of the session-event
// refactor. Behavior: auto-approve every tool the model wants to call,
// fire-and-forget. If session.approve() rejects, log it but do nothing
// else. Step 3 may capture pending approvals — not here.
// ---------------------------------------------------------------------------

// session.approve() is typed loosely on the SDK side (returns Promise<void>
// but the parameter is the approvalItem from the SDK event). We mirror the
// original inline handler's loose typing with `any` for the session arg.
export function onToolApprovalRequested(
  session: { approve(item: any): Promise<unknown> },
  req: any,
): void {
  console.log(`[dexter-phone] Auto-approving tool: ${req.approvalItem.rawItem.name}`);
  session.approve(req.approvalItem).catch((e: unknown) =>
    console.error('[dexter-phone] Tool approval failed:', e),
  );
}
