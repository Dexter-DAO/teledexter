const AUTOMATED_AUDIO = /\b(?:leave (?:a )?message|after the (?:tone|beep)|mailbox|voicemail|not available|record your message|please hold|your call (?:has|is)|automated (?:system|message)|has been forwarded|fax)\b/iu;

export function isAutomatedAudioTranscript(value: unknown): boolean {
  return typeof value === 'string' && AUTOMATED_AUDIO.test(value);
}

function answerKind(answeredBy: string): 'human' | 'unknown' | 'machine' {
  const normalized = answeredBy.trim().toLowerCase();
  if (normalized === 'human') return 'human';
  if (normalized === 'unknown' || !normalized) return 'unknown';
  return 'machine';
}

export class HarkHumanTurnQualifier {
  private readonly kind: 'human' | 'unknown' | 'machine';
  private readonly meaningfulUnknownTurns = new Set<string>();
  private qualified = false;
  private assistantPlaybackStarts = 0;
  private lastCompletedAssistantPlayback = 0;
  private firstUnknownTurn: { itemId: string; playbackStarts: number } | null = null;
  private readonly callerPlaybackAtStart = new Map<string, {
    playbackStarts: number;
    completedPlaybacks: number;
  }>();
  private latestMeaningfulItemId: string | null = null;
  private readonly qualifiedHumanItemIds = new Set<string>();
  private readonly providerByUserItem = new Map<string, string>();
  private readonly userItemByProvider = new Map<string, string>();
  private readonly qualificationWaiters = new Map<string, {
    userItemId: string;
    promise: Promise<string | null>;
    resolve: (userItemId: string | null) => void;
    timer: ReturnType<typeof setTimeout>;
  }>();

  constructor(answeredBy: string) {
    this.kind = answerKind(answeredBy);
  }

  canAttemptConversation(): boolean {
    return this.kind !== 'machine';
  }

  isQualified(): boolean {
    return this.qualified;
  }

  currentHumanItemId(): string | null {
    return this.latestMeaningfulItemId;
  }

  noteAssistantPlaybackStarted(): void {
    if (this.kind !== 'machine') this.assistantPlaybackStarts += 1;
  }

  noteAssistantPlaybackCompleted(): void {
    if (this.kind !== 'machine') {
      this.lastCompletedAssistantPlayback = this.assistantPlaybackStarts;
    }
  }

  noteCallerSpeechStarted(itemIdInput: unknown): boolean {
    const itemId = normalizeItemId(itemIdInput);
    if (!itemId || this.kind === 'machine' || this.callerPlaybackAtStart.has(itemId)) {
      return false;
    }
    this.callerPlaybackAtStart.set(itemId, {
      playbackStarts: this.assistantPlaybackStarts,
      completedPlaybacks: this.lastCompletedAssistantPlayback,
    });
    return true;
  }

  async waitForQualifiedInvocation(input: {
    providerInvocationId: string;
    userItemId: string;
    timeoutMs: number;
  }): Promise<string | null> {
    const providerInvocationId = input.providerInvocationId.trim();
    const proposedUserItemId = input.userItemId.trim();
    const requestedTimeoutMs = Number.isFinite(input.timeoutMs)
      ? Math.floor(input.timeoutMs)
      : 2_000;
    const timeoutMs = Math.max(1, Math.min(5_000, requestedTimeoutMs));
    if (!providerInvocationId || this.kind === 'machine') return null;

    const existing = this.qualificationWaiters.get(providerInvocationId);
    if (existing) return existing.promise;

    // Both sides of this binding are immutable for the life of one call. A
    // delayed transcript can therefore wake only the provider invocation that
    // was executing for that exact server-observed caller item. A retry of the
    // same invocation remains attached to its original item even after later
    // caller turns advance the session state.
    const boundUserItemId = this.userItemByProvider.get(providerInvocationId);
    if (!boundUserItemId && !proposedUserItemId) return null;
    const userItemId = boundUserItemId ?? proposedUserItemId;
    if (boundUserItemId && proposedUserItemId && boundUserItemId !== proposedUserItemId) {
      if (this.qualifiedHumanItemIds.has(boundUserItemId)) return boundUserItemId;
      return null;
    }

    const claimedBy = this.providerByUserItem.get(userItemId);
    if (claimedBy && claimedBy !== providerInvocationId) return null;
    this.providerByUserItem.set(userItemId, providerInvocationId);
    this.userItemByProvider.set(providerInvocationId, userItemId);

    if (this.qualifiedHumanItemIds.has(userItemId)) return userItemId;

    let resolveWaiter!: (userItemId: string | null) => void;
    const promise = new Promise<string | null>((resolve) => {
      resolveWaiter = resolve;
    });
    const timer = setTimeout(() => {
      const waiter = this.qualificationWaiters.get(providerInvocationId);
      if (!waiter || waiter.userItemId !== userItemId) return;
      this.qualificationWaiters.delete(providerInvocationId);
      waiter.resolve(null);
    }, timeoutMs);
    this.qualificationWaiters.set(providerInvocationId, {
      userItemId,
      promise,
      resolve: resolveWaiter,
      timer,
    });
    return promise;
  }

  private resolveQualifiedInvocation(userItemId: string): void {
    if (!this.qualifiedHumanItemIds.has(userItemId)) return;
    const providerInvocationId = this.providerByUserItem.get(userItemId);
    if (!providerInvocationId) return;
    const waiter = this.qualificationWaiters.get(providerInvocationId);
    if (!waiter || waiter.userItemId !== userItemId) return;
    clearTimeout(waiter.timer);
    this.qualificationWaiters.delete(providerInvocationId);
    waiter.resolve(userItemId);
  }

  observe(input: {
    itemId: string;
    transcript: unknown;
    echo: boolean;
  }): boolean {
    if (this.kind === 'machine' || input.echo) return false;
    if (typeof input.transcript !== 'string') return false;
    const transcript = input.transcript.trim();
    if (!transcript || isAutomatedAudioTranscript(transcript)) return false;
    if (!input.itemId) return false;

    if (this.kind === 'human') {
      this.latestMeaningfulItemId = input.itemId;
      const newlyQualified = !this.qualified;
      this.qualified = true;
      this.qualifiedHumanItemIds.add(input.itemId);
      this.resolveQualifiedInvocation(input.itemId);
      return newlyQualified;
    }

    const playbackAtStart = this.callerPlaybackAtStart.get(input.itemId);
    if (!playbackAtStart) return false;

    // Twilio occasionally returns `unknown` for a real person. Do not trust one
    // greeting or two VAD fragments from the same voicemail/caller turn. The
    // second distinct completed utterance counts only after Dexter audio has
    // actually finished playing on Twilio between the two caller speech-start
    // events. Completion-time state is deliberately irrelevant because ASR can
    // arrive late. OpenAI generation completion alone is not that boundary.
    if (this.meaningfulUnknownTurns.has(input.itemId)) return false;
    this.meaningfulUnknownTurns.add(input.itemId);
    this.latestMeaningfulItemId = input.itemId;
    if (!this.firstUnknownTurn) {
      this.firstUnknownTurn = {
        itemId: input.itemId,
        playbackStarts: playbackAtStart.playbackStarts,
      };
      return false;
    }
    if (this.qualified) {
      this.qualifiedHumanItemIds.add(input.itemId);
      this.resolveQualifiedInvocation(input.itemId);
      return false;
    }
    if (input.itemId === this.firstUnknownTurn.itemId) return false;
    if (this.meaningfulUnknownTurns.size < 2) return false;
    if (playbackAtStart.completedPlaybacks <= this.firstUnknownTurn.playbackStarts) return false;
    this.qualified = true;
    this.qualifiedHumanItemIds.add(input.itemId);
    this.resolveQualifiedInvocation(input.itemId);
    return true;
  }
}

function normalizeItemId(value: unknown): string {
  return typeof value === 'string' ? value.trim() : '';
}
