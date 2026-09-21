/**
 * Maintains server-observed caller-item ordering evidence for Hark.
 *
 * Realtime transcription completions can arrive out of order. Only a new,
 * server-observed speech-start event can make an item eligible for causal
 * lineage or human-turn qualification; a completion is evidence for its own
 * item but never an ordering event. A completion without a prior start is
 * quarantined so a delayed start cannot promote that orphan later.
 */
export class HarkCallerItemTracker {
  private nextStartOrder = 0;
  private readonly startOrderByItem = new Map<string, number>();
  private readonly completedWithoutStart = new Set<string>();
  private latestStartedItem: { itemId: string; order: number } | null = null;
  private bindingSuppressed = false;

  noteSpeechStarted(itemIdInput: unknown): boolean {
    const itemId = normalizeItemId(itemIdInput);
    if (!itemId) {
      this.bindingSuppressed = true;
      return false;
    }
    if (this.startOrderByItem.has(itemId)) return false;
    if (this.completedWithoutStart.has(itemId)) {
      this.bindingSuppressed = true;
      return false;
    }

    const order = ++this.nextStartOrder;
    this.startOrderByItem.set(itemId, order);
    if (!this.latestStartedItem || order > this.latestStartedItem.order) {
      this.latestStartedItem = { itemId, order };
    }
    this.bindingSuppressed = false;
    return true;
  }

  noteTranscriptionCompleted(itemIdInput: unknown): boolean {
    const itemId = normalizeItemId(itemIdInput);
    if (!itemId) {
      this.bindingSuppressed = true;
      return false;
    }
    if (!this.startOrderByItem.has(itemId)) {
      this.completedWithoutStart.add(itemId);
      this.bindingSuppressed = true;
      return false;
    }

    // Deliberately do not move latestStartedItem. The completion may belong
    // to an older overlapping turn; qualification is handled against itemId.
    return true;
  }

  currentItemId(): string | null {
    return this.bindingSuppressed ? null : this.latestStartedItem?.itemId ?? null;
  }

  isOrderedItem(itemIdInput: unknown): boolean {
    const itemId = normalizeItemId(itemIdInput);
    return Boolean(itemId) && this.startOrderByItem.has(itemId);
  }
}

function normalizeItemId(value: unknown): string {
  return typeof value === 'string' ? value.trim() : '';
}
