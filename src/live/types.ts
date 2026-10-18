import type { ClientEvent } from 'openai/resources/live/live';

/** One provider connection. Tests inject this boundary without opening a socket. */
export interface BrokerLiveConnection {
  onOpen(listener: () => void): void;
  onEvent(listener: (event: unknown) => void): void;
  onError(listener: (error: unknown) => void): void;
  onClose(listener: () => void): void;
  isOpen(): boolean;
  send(event: ClientEvent): void;
  close(): void;
  terminate(): void;
}

export interface LiveTranscriptFragment {
  eventId: string;
  role: 'user' | 'assistant';
  text: string;
  startMs: number;
  endMs: number;
  sequence: number;
}

/** Fragments preserve provider text exactly. No fragment implies a finished turn. */
export interface LiveTranscriptSnapshot {
  revision: number;
  callerRevision: number;
  fragments: LiveTranscriptFragment[];
}

export interface LiveDelegationRequest {
  callId: string;
  sessionId: string;
  delegationId: string;
  /** Stable across duplicate delivery of this exact call/session/delegation. */
  requestId: string;
  offsetMs: number;
  transcript: LiveTranscriptSnapshot;
}

export interface LiveDelivery {
  eventId: string;
  status: 'accepted' | 'rejected' | 'unconfirmed' | 'stale' | 'closed';
  reason?: string;
  /** Raw input frontier observed synchronously when this append settled. */
  observedCallerRevision?: number;
  observedTranscriptSequence?: number;
}

export interface LiveBackendUpdate {
  /** Backend-owned update identity, retained with the durable work record. */
  id: string;
  kind: 'progress' | 'result';
  /** Still an observation of pending work (a partial or a question), not the task's final result. */
  pending?: boolean;
  /** Only the last chunk of a final result finishes the task; later progress is outdated. */
  terminal?: boolean;
  text: string;
  /** Caller revision the backend reviewed. Required for a result. */
  expectedCallerRevision?: number;
  /** Progress is quiet by default; results are spoken by default. */
  speak?: boolean;
}

export interface LiveBackendChannel {
  getContext(): LiveTranscriptSnapshot;
  subscribeContext(listener: (snapshot: LiveTranscriptSnapshot) => void): () => void;
  publish(update: LiveBackendUpdate): Promise<LiveDelivery>;
  /** Cancels this voice subscription only. It does not cancel durable work. */
  signal: AbortSignal;
}

export interface BrokerLiveBackend {
  /** Persist requestId and account binding before performing or resuming work. */
  delegate(request: LiveDelegationRequest, channel: LiveBackendChannel): Promise<void>;
}

export interface LiveSessionClosed {
  callId: string;
  sessionId?: string;
  finalized: boolean;
  /** Separate from provider finalization; false if capture failed or its drain timed out. */
  transcriptPersisted: boolean;
  /** Any output audio suppressed by interruption or teardown makes heard-text proof incomplete. */
  outputAudioDiscarded: boolean;
  reason: string;
  /** The provider error that ended the session, when one did. Codes only; provider text never leaves the session. */
  providerError?: { code: string };
  usageSeconds?: number;
}

export interface BrokerLiveEvent {
  type: string;
  [key: string]: unknown;
}

export interface BrokerLiveSessionOptions {
  apiKey: string;
  callId: string;
  instructions: string;
  voice?: string;
  history?: Array<{ role: 'user' | 'assistant' | 'developer'; text: string }>;
  /** Complete, source-validated successor history. Provider startup accepts or refuses its token budget. */
  eligibleHistory?: import('../broker/eligibleHistory.js').PreparedEligibleHistory;
  backend: BrokerLiveBackend;
  /** Restricted setup only; absent preserves the original client delegation protocol. */
  restrictedSetupTools?: import('./restrictedSetupTools.js').RestrictedSetupTools;
  /** Ordered audio deliveries use connection-local IDs, never provider event IDs. */
  onAudio(base64: string, metadata: { deliveryId: string }): void;
  onTranscript(snapshot: LiveTranscriptSnapshot): void | Promise<void>;
  onReady(metadata: { callId: string; sessionId: string }): void;
  onClosed(result: LiveSessionClosed): void;
  onEvent?(event: BrokerLiveEvent): void;
  /** Only explicit application interruption or session teardown clears playback. */
  onPlaybackClear?(reason: 'application_interrupt' | 'session_closed' | 'session_failed'): void;
  connectionFactory?: (apiKey: string) => BrokerLiveConnection;
  startupTimeoutMs?: number;
  closeTimeoutMs?: number;
  appendAckTimeoutMs?: number;
}

export interface BrokerLiveSession {
  appendAudio(base64: string): boolean;
  appendContext(text: string): Promise<LiveDelivery>;
  publishDelegation?(delegationId:string,update:LiveBackendUpdate):Promise<LiveDelivery>;
  /** Application-authored instructions; a known delegation ties them to that task. */
  appendInstructions(text: string, delegationId?: string | null): Promise<LiveDelivery>;
  /** Unaddressed spoken result publication; never resumes an earlier delegation. */
  appendCommentary?(text: string): Promise<LiveDelivery>;
  /** Drops queued/new output until the application explicitly resumes playback. */
  interruptPlayback(instruction: string): Promise<LiveDelivery>;
  /** Local playback-only interruption; sends no model instruction or request. */
  pausePlayback?(): void;
  resumePlayback(): void;
  getTranscript(): LiveTranscriptSnapshot;
  close(reason?: string): Promise<LiveSessionClosed>;
}
