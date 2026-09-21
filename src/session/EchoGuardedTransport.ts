// ---------------------------------------------------------------------------
// EchoGuardedTwilioTransport — real Twilio playback state + acoustic echo gate.
//
// OpenAI's WebSocket `response.output_audio.done` means generation is done. It
// does NOT mean Twilio has finished playing the media already queued on the
// phone leg. The upstream SDK resets its current item at generation-done, so a
// later caller interruption can otherwise become a no-op while Twilio keeps
// talking for several seconds.
//
// Twilio's returned `done:<itemId>` mark is the actual playback-complete
// boundary. This transport tracks that mark, clears Twilio even after the SDK
// has forgotten the item, and truncates the unheard portion of the item so the
// conversation history matches what the caller actually heard.
//
// Start-of-turn echo suppression is deliberately acoustic, not textual: only
// low-energy VAD starts during the short opening guard can be rejected. A loud
// human barge-in always clears playback, even during the first syllable.
// ---------------------------------------------------------------------------

import { TwilioRealtimeTransportLayer } from '@openai/agents-extensions';
import type { TransportLayerAudio } from '@openai/agents/realtime';

interface TwilioSocketLike {
  addEventListener: (type: string, listener: (event: any) => void) => void;
  send: (data: string) => void;
}

export type PlaybackStopReason = 'played' | 'interrupted' | 'closed';

export interface EchoGuardOptions {
  twilioWebSocket: unknown;
  /** Acoustic guard window measured from the first queued audio frame. */
  guardMs: number;
  /** Maximum inbound RMS that may be treated as speakerphone echo. */
  maxGuardRms: number;
  /** Peak inbound RMS immediately preceding the current VAD start. */
  currentInboundRms: () => number;
  /** Diagnostic hook for a low-energy start rejected as echo. */
  onSuppressed: (sincePlaybackStartMs: number, rms: number) => void;
  /** Fires only when Twilio has played or cleared every queued item. */
  onPlaybackStopped?: (reason: PlaybackStopReason) => void;
}

function finiteNonNegative(value: number): number {
  return Number.isFinite(value) && value >= 0 ? value : 0;
}

export class EchoGuardedTwilioTransport extends TwilioRealtimeTransportLayer {
  readonly #twilioWebSocket: TwilioSocketLike;
  readonly #guardMs: number;
  readonly #maxGuardRms: number;
  readonly #currentInboundRms: () => number;
  readonly #onSuppressed: (ms: number, rms: number) => void;
  readonly #onPlaybackStopped?: (reason: PlaybackStopReason) => void;

  #streamSid: string | null = null;
  #playbackStartedAt = 0;
  #activeItemId: string | null = null;
  #pendingItemIds = new Set<string>();
  #generatedMsByItem = new Map<string, number>();
  #playedMsByItem = new Map<string, number>();
  #discardedItemIds = new Set<string>();

  constructor(opts: EchoGuardOptions) {
    super({ twilioWebSocket: opts.twilioWebSocket as never });
    this.#twilioWebSocket = opts.twilioWebSocket as TwilioSocketLike;
    this.#guardMs = finiteNonNegative(opts.guardMs);
    this.#maxGuardRms = finiteNonNegative(opts.maxGuardRms);
    this.#currentInboundRms = opts.currentInboundRms;
    this.#onSuppressed = opts.onSuppressed;
    this.#onPlaybackStopped = opts.onPlaybackStopped;

    // Register immediately. Twilio may send `start` while the OpenAI socket is
    // still connecting, and we need the stream SID for the emergency clear.
    this.#twilioWebSocket.addEventListener('message', (message: any) => {
      let data: any;
      try {
        data = JSON.parse(message?.data?.toString?.() ?? String(message?.data ?? ''));
      } catch {
        return;
      }

      if (data?.event === 'start') {
        this.#streamSid = data.start?.streamSid || null;
        return;
      }

      if (data?.event !== 'mark' || typeof data.mark?.name !== 'string') return;
      this.#onReturnedMark(data.mark.name);
    });

    this.#twilioWebSocket.addEventListener('close', () => {
      this.#resetPlayback('closed');
    });
  }

  isPlaybackPending(): boolean {
    return this.#pendingItemIds.size > 0;
  }

  msSincePlaybackStart(): number {
    return this.#playbackStartedAt > 0 ? Date.now() - this.#playbackStartedAt : -1;
  }

  get activePlaybackItemId(): string | null {
    return this.#activeItemId;
  }

  /** Cancel a response triggered by verified low-energy echo without clearing
   * the legitimate assistant audio that Twilio is still playing. */
  discardEchoTurn(inputItemId: string, cancelEchoResponse: boolean): void {
    try {
      if (cancelEchoResponse) this._cancelResponse();
      if (inputItemId) {
        this.sendEvent({
          type: 'conversation.item.delete',
          item_id: inputItemId,
        });
      }
    } catch {
      // The response may already be gone; echo classification remains logged.
    }
  }

  #onReturnedMark(name: string): void {
    if (name.startsWith('done:')) {
      const itemId = name.slice('done:'.length);
      const completedPendingItem = this.#pendingItemIds.delete(itemId);
      this.#generatedMsByItem.delete(itemId);
      this.#playedMsByItem.delete(itemId);
      this.#discardedItemIds.delete(itemId);

      if (this.#activeItemId === itemId) {
        this.#activeItemId = [...this.#pendingItemIds].at(-1) ?? null;
      }
      if (this.#pendingItemIds.size === 0) {
        this.#resetPlayback('played', completedPendingItem);
      }
      return;
    }

    const separator = name.lastIndexOf(':');
    if (separator <= 0) return;
    const itemId = name.slice(0, separator);
    const playedMs = Number(name.slice(separator + 1));
    if (Number.isFinite(playedMs)) {
      this.#playedMsByItem.set(itemId, Math.max(0, playedMs));
    }
  }

  #resetPlayback(reason: PlaybackStopReason, forceNotify = false): void {
    const wasPending = forceNotify || this.#pendingItemIds.size > 0;
    this.#pendingItemIds.clear();
    this.#generatedMsByItem.clear();
    this.#playedMsByItem.clear();
    this.#activeItemId = null;
    this.#playbackStartedAt = 0;
    if (wasPending) this.#onPlaybackStopped?.(reason);
  }

  protected override _onAudio(audioEvent: TransportLayerAudio): void {
    const itemId = this.currentItemId || audioEvent.responseId;

    // Cancellation and Twilio clear can race with already-in-flight OpenAI
    // deltas. Never put the tail of an interrupted item back on the phone.
    if (this.#discardedItemIds.has(itemId)) return;

    const wasPending = this.isPlaybackPending();
    this.#activeItemId = itemId;
    this.#pendingItemIds.add(itemId);
    this.#generatedMsByItem.set(
      itemId,
      (this.#generatedMsByItem.get(itemId) ?? 0) + audioEvent.data.byteLength / 8,
    );
    if (!wasPending) this.#playbackStartedAt = Date.now();

    super._onAudio(audioEvent);
  }

  override _interrupt(elapsedTime: number, cancelOngoingResponse = true): void {
    const itemId = this.currentItemId || this.#activeItemId;
    if (itemId) this.#discardedItemIds.add(itemId);

    // Normal SDK path: Twilio clear + OpenAI cancellation/truncation use the
    // SDK's still-live item and its precise content index.
    super._interrupt(elapsedTime, cancelOngoingResponse);
    this.#resetPlayback('interrupted');
  }

  #interruptQueuedPlayback(cancelOngoingResponse: boolean): void {
    const itemId = this.#activeItemId;
    if (!itemId) return;

    this.#discardedItemIds.add(itemId);
    const generatedMs = this.#generatedMsByItem.get(itemId) ?? 0;
    const playedMs = this.#playedMsByItem.get(itemId) ?? 0;
    const audioEndMs = Math.max(
      0,
      Math.floor(Math.min(generatedMs, playedMs + 50)),
    );

    // The SDK has already reset its item because generation ended. Twilio is
    // still playing it, so clear the actual phone queue ourselves.
    this.#twilioWebSocket.send(JSON.stringify({
      event: 'clear',
      streamSid: this.#streamSid,
    }));
    this.#resetPlayback('interrupted');

    try {
      if (cancelOngoingResponse) this._cancelResponse();
      this.emit('audio_interrupted');
      this.sendEvent({
        type: 'conversation.item.truncate',
        item_id: itemId,
        content_index: 0,
        audio_end_ms: audioEndMs,
      });
    } catch {
      // Clearing Twilio is the latency-critical safety action. If the OpenAI
      // socket has already closed, there is no remote conversation to repair.
    }
  }

  /**
   * Called automatically for every Realtime `speech_started` event.
   * Low-energy speakerphone echo is rejected briefly; a real, loud caller is
   * always allowed through and immediately clears queued Twilio playback.
   */
  override interrupt(cancelOngoingResponse = true): void {
    if (!this.isPlaybackPending()) {
      super.interrupt(cancelOngoingResponse);
      return;
    }

    const since = this.msSincePlaybackStart();
    const rms = finiteNonNegative(this.#currentInboundRms());
    if (
      since >= 0
      && since < this.#guardMs
      && rms > 0
      && rms <= this.#maxGuardRms
    ) {
      this.#onSuppressed(since, rms);
      return;
    }

    if (this.currentItemId && typeof this._firstAudioTimestamp === 'number') {
      super.interrupt(cancelOngoingResponse);
      return;
    }

    // Critical tail case: OpenAI generation is done (and the SDK forgot the
    // item), but Twilio's returned done mark says the phone is still talking.
    this.#interruptQueuedPlayback(cancelOngoingResponse);
  }
}
