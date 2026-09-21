// ---------------------------------------------------------------------------
// session.on('history_updated') handler.
//
// Extracted verbatim from src/server.ts in Step 2 of the session-event
// refactor.
//
// Per-call state required: a Map<itemId, latest captured text>
// (capturedItemTexts). The map is owned by server.ts (one instance per
// WebSocket connection) and passed in so the handler can dedupe by text,
// not just by itemId. history_updated fires multiple times for an
// assistant audio item as its transcript fills in incrementally — keying
// on itemId alone permanently locked out the assistant's final
// transcript in an earlier version. See the inline comment preserved
// below for the full background.
// ---------------------------------------------------------------------------

import type { CallLogEntry } from '../../types/CallEntry.js';

export function onHistoryUpdated(
  callEntry: CallLogEntry,
  capturedItemTexts: Map<string, string>,
  history: any[],
): void {
  if (!history) return;
  for (const item of history) {
    if (item.type !== 'message' || !item.content || !item.itemId) continue;

    // Concatenate every text/transcript part of this item. Audio items
    // have `transcript`; text items have `text`. Either one is fine.
    let combined = '';
    for (const part of item.content) {
      const t = part.transcript || part.text;
      if (typeof t === 'string' && t.trim()) combined += (combined ? ' ' : '') + t;
    }
    if (!combined) continue;

    // If we've already captured this exact text for this item, skip.
    // If the text has grown since last time (more transcript arrived),
    // append the delta. Simplest correct behavior: store the LATEST full
    // text, overwriting any partial we had. Index the previous segment
    // by itemId so we can replace it.
    const prevText = capturedItemTexts.get(item.itemId);
    if (prevText === combined) continue;

    capturedItemTexts.set(item.itemId, combined);

    // Replace prior segment for this itemId, or push a new one.
    const idx = callEntry.transcriptSegments.findIndex(
      (seg: any) => seg.itemId === item.itemId,
    );
    const segment = {
      role: item.role || 'unknown',
      text: combined,
      timestamp: new Date().toISOString(),
      itemId: item.itemId,
    };
    if (idx >= 0) callEntry.transcriptSegments[idx] = segment as any;
    else callEntry.transcriptSegments.push(segment as any);
  }
}
