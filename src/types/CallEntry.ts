// ---------------------------------------------------------------------------
// Per-call data shapes for dexter-phone.
//
// Step 1 of the session-event refactor: types moved verbatim out of
// src/server.ts. No fields added, removed, or renamed. Future steps
// (see .planning/specs/2026-05-30-session-event-audit-and-refactor.md)
// will enrich CallLogEntry with error / guardrail / handoff / audio
// metrics — do not pre-empt them here.
// ---------------------------------------------------------------------------

export interface CallLogEntry {
  streamSid: string;
  callerNumber: string;
  stirVerstat: string;
  direction?: 'inbound' | 'outbound-alert';
  alertIntentId?: string;
  alertEventId?: string;
  harkBinding?: {
    caseId: string;
    watchId: string;
    eventId: string;
    deliveryId: string;
    originTaskId: string;
  };
  harkAnsweredBy?: string;
  harkResponses?: Array<{
    sourceId: string;
    state: 'pending' | 'delivered' | 'rejected';
    replay: boolean;
    continuation: {
      id: string;
      originTaskId: string;
    } | null;
  }>;
  model?: string;
  voice?: string;
  voiceSpeed?: number;
  promptVersion?: string;
  toolRoster?: string[];
  alertContextDigest?: string;
  callerIdentity?: {
    resolved: boolean;
    name: string | null;
    walletLinked: boolean;
    walletLabel: string | null;
    phoneTabActive: boolean;
  };
  startedAt: string;
  endedAt?: string;
  durationSeconds?: number;
  /** Measured audio/VAD telemetry — see session/CallDiagnostics.ts. */
  diagnostics?: {
    summary: unknown;
    timeline: Array<{ t: number; type: string; detail?: Record<string, unknown> }>;
  };
  transcriptSegments: Array<{ role: string; text: string; timestamp: string; itemId?: string }>;
  toolCalls: Array<{
    name: string;
    arguments: string;
    timestamp: string;
    itemId?: string;
    status?: 'completed' | 'incomplete' | string;
    result?: string | null;
    error?: string | null;
  }>;
}

// Phone-tab billing state from dexter-api (metered minutes, receiver-pays)
export interface PhoneTabStatus {
  active: boolean;
  pricePerMinuteMicro: number;
  freeSeconds: number;
  remainingMicro?: number | null;
  expiresAt?: string | null;
}

// Pending caller info from webhook (before stream SID is known)
export interface CallerInfo {
  callerNumber: string;
  stirVerstat: string;
  callSid: string;
  supabaseUserId?: string | null;
  userHandle?: string | null;
  swigAddress?: string | null;
  // Single-call wallet-binding credential minted by dexter-api at lookup.
  // Rides x-dexter-link-token on the hosted MCP tool; revoked at call end
  // via the hash echoed in the observatory payload.
  linkToken?: string | null;
  linkTokenHash?: string | null;
}

export interface CallerLookupResult {
  found: boolean;
  name: string | null;
  supabaseUserId: string | null;
  wallet: string | null;
  userHandle: string | null;
  swigAddress: string | null;
  vaultPda: string | null;
  walletLabel: string | null;
  linkToken: string | null;
  linkTokenHash: string | null;
  phoneTab: PhoneTabStatus | null;
}
