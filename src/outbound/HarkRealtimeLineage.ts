type LineageKind = 'caller' | 'function' | 'other';

type LineageNode = {
  kind: LineageKind;
  previousItemId: string | null | undefined;
};

type LineageWaiter = {
  isEligibleCallerItem: (itemId: string) => boolean;
  resolve: (itemId: string | null) => void;
  timer: ReturnType<typeof setTimeout>;
};

/**
 * Reconstructs provider-authored Realtime conversation lineage for one call.
 * Tool arguments are never used for identity. A function call resolves only
 * when its raw conversation item has a complete predecessor chain to a caller
 * input item that the caller-item tracker independently accepts.
 */
export class HarkRealtimeLineage {
  private readonly nodes = new Map<string, LineageNode>();
  private readonly functionItemByCallId = new Map<string, string>();
  private readonly waiters = new Map<string, Set<LineageWaiter>>();

  observe(event: unknown): void {
    if (!isRecord(event) || typeof event.type !== 'string') return;

    if (event.type === 'input_audio_buffer.committed') {
      const itemId = text(event.item_id);
      if (!itemId) return;
      this.mergeNode(itemId, {
        kind: 'caller',
        previousItemId: predecessor(event),
      });
      this.resolveWaiters();
      return;
    }

    if (
      event.type === 'conversation.item.added'
      || event.type === 'conversation.item.done'
      || event.type === 'conversation.item.created'
    ) {
      const item = isRecord(event.item) ? event.item : null;
      const itemId = item ? text(item.id) : '';
      if (!item || !itemId) return;
      const itemType = text(item.type);
      const role = text(item.role);
      const kind: LineageKind = itemType === 'function_call'
        ? 'function'
        : itemType === 'message' && role === 'user'
          ? 'caller'
          : 'other';
      this.mergeNode(itemId, {
        kind,
        previousItemId: predecessor(event),
      });
      if (kind === 'function') {
        const callId = text(item.call_id);
        if (callId) this.functionItemByCallId.set(callId, itemId);
      }
      this.resolveWaiters();
      return;
    }

    if (
      event.type === 'response.output_item.added'
      || event.type === 'response.output_item.done'
    ) {
      const item = isRecord(event.item) ? event.item : null;
      if (!item || text(item.type) !== 'function_call') return;
      const itemId = text(item.id);
      const callId = text(item.call_id);
      if (itemId) {
        const previousItemId = predecessor(event);
        if (previousItemId !== undefined) {
          this.mergeNode(itemId, { kind: 'function', previousItemId });
        }
        if (callId) this.functionItemByCallId.set(callId, itemId);
      }
      this.resolveWaiters();
      return;
    }

    if (event.type === 'response.function_call_arguments.done') {
      const itemId = text(event.item_id);
      const callId = text(event.call_id);
      if (itemId && callId) this.functionItemByCallId.set(callId, itemId);
      this.resolveWaiters();
    }
  }

  async waitForCausalCallerItemId(
    providerInvocationIdInput: unknown,
    isEligibleCallerItem: (itemId: string) => boolean,
    timeoutMsInput = 250,
  ): Promise<string | null> {
    const providerInvocationId = text(providerInvocationIdInput);
    if (!providerInvocationId) return null;
    const immediate = this.resolveCausalCallerItemId(
      providerInvocationId,
      isEligibleCallerItem,
    );
    if (immediate) return immediate;

    const requestedTimeout = Number.isFinite(timeoutMsInput)
      ? Math.floor(timeoutMsInput)
      : 250;
    const timeoutMs = Math.max(1, Math.min(1_000, requestedTimeout));
    return new Promise((resolve) => {
      let waiter!: LineageWaiter;
      waiter = {
        isEligibleCallerItem,
        resolve,
        timer: setTimeout(() => {
          const current = this.waiters.get(providerInvocationId);
          current?.delete(waiter);
          if (current?.size === 0) this.waiters.delete(providerInvocationId);
          resolve(null);
        }, timeoutMs),
      };
      const current = this.waiters.get(providerInvocationId) ?? new Set();
      current.add(waiter);
      this.waiters.set(providerInvocationId, current);
    });
  }

  resolveCausalCallerItemId(
    providerInvocationIdInput: unknown,
    isEligibleCallerItem: (itemId: string) => boolean,
  ): string | null {
    const providerInvocationId = text(providerInvocationIdInput);
    if (!providerInvocationId) return null;
    const functionItemId = this.functionItemByCallId.get(providerInvocationId);
    if (!functionItemId) return null;
    const functionNode = this.nodes.get(functionItemId);
    if (!functionNode || functionNode.kind !== 'function') return null;

    let itemId = functionNode.previousItemId;
    const visited = new Set<string>([functionItemId]);
    while (itemId) {
      if (visited.has(itemId) || visited.size > 256) return null;
      visited.add(itemId);
      const node = this.nodes.get(itemId);
      if (!node) return null;
      if (node.kind === 'caller') {
        return isEligibleCallerItem(itemId) ? itemId : null;
      }
      itemId = node.previousItemId;
    }
    return null;
  }

  private mergeNode(itemId: string, incoming: LineageNode): void {
    const existing = this.nodes.get(itemId);
    this.nodes.set(itemId, {
      kind: strongerKind(existing?.kind, incoming.kind),
      previousItemId: incoming.previousItemId !== undefined
        ? incoming.previousItemId
        : existing?.previousItemId,
    });
  }

  private resolveWaiters(): void {
    for (const [providerInvocationId, waiters] of this.waiters) {
      for (const waiter of waiters) {
        const itemId = this.resolveCausalCallerItemId(
          providerInvocationId,
          waiter.isEligibleCallerItem,
        );
        if (!itemId) continue;
        clearTimeout(waiter.timer);
        waiters.delete(waiter);
        waiter.resolve(itemId);
      }
      if (waiters.size === 0) this.waiters.delete(providerInvocationId);
    }
  }
}

function strongerKind(current: LineageKind | undefined, incoming: LineageKind): LineageKind {
  if (current === 'caller' || incoming === 'caller') return 'caller';
  if (current === 'function' || incoming === 'function') return 'function';
  return 'other';
}

function predecessor(event: Record<string, unknown>): string | null | undefined {
  if (!Object.prototype.hasOwnProperty.call(event, 'previous_item_id')) return undefined;
  if (event.previous_item_id === null) return null;
  return text(event.previous_item_id) || undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function text(value: unknown): string {
  return typeof value === 'string' ? value.trim() : '';
}
