import fs from 'node:fs';
import path from 'node:path';
import { canonicalJson, sha256Hex } from './canonical.js';
import {
  bindDurableFileParent,
  durableFileParentIsCurrent,
  durablyReplaceJsonFile,
  type DurableParentBinding,
  type DurableFileSystem,
} from './DurableFile.js';
import { postSignedPhoneApiCallback } from './PhoneApiCallback.js';

export type PhoneApiCallbackPath =
  | '/internal/voice/call-ended'
  | '/internal/voice/outbound-status';

export interface PhoneApiCallbackEntry {
  id: string;
  path: PhoneApiCallbackPath;
  body: Record<string, unknown>;
  bodyHash: string;
  state: 'pending' | 'delivered' | 'rejected';
  attempts: number;
  createdAt: string;
  lastAttemptAt: string | null;
  completedAt: string | null;
  lastHttpStatus: number | null;
  lastError: string | null;
}

interface PersistedPhoneApiCallbackOutbox {
  version: 1 | 2 | 3;
  entries: PhoneApiCallbackEntry[];
  callFinalizations?: PhoneCallFinalizationDraft[];
}

export type PhoneCallFinalizationPhase =
  | 'awaiting_media'
  | 'active'
  | 'closed';

export interface PhoneCallFinalizationDraft {
  id: string;
  body: Record<string, unknown>;
  bodyHash: string;
  identityHash: string;
  phase: PhoneCallFinalizationPhase;
  mediaTokenHash: string | null;
  mediaExpiresAt: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface PhoneApiCallbackOutboxOptions {
  filePath: string;
  apiBaseUrl: string;
  signingSecret: string;
  fetchImpl?: typeof fetch;
  now?: () => number;
  nonce?: () => string;
  retryDelayMs?: number | null;
  retentionMs?: number;
  durableFileSystem?: DurableFileSystem;
  onMediaTokenRevoked?: (callSid: string) => void;
}

function cloneEntry(entry: PhoneApiCallbackEntry): PhoneApiCallbackEntry {
  return structuredClone(entry);
}

function cloneFinalization(
  entry: PhoneCallFinalizationDraft,
): PhoneCallFinalizationDraft {
  return structuredClone(entry);
}

const FINALIZATION_IDENTITY_FIELDS = [
  'source',
  'direction',
  'alert_intent_id',
  'alert_event_id',
  'hark_case_id',
  'hark_watch_id',
  'hark_delivery_id',
  'hark_origin_task_id',
  'hark_answered_by',
  'external_sid',
  'caller_number',
  'caller_user_id',
  'caller_handle',
  'caller_swig',
  'stir_verstat',
  'started_at',
  'vault_resolution',
  'link_token_hash',
] as const;

function callFinalizationIdentityHash(body: Record<string, unknown>): string {
  const identity = Object.fromEntries(
    FINALIZATION_IDENTITY_FIELDS.map((key) => [key, body[key] ?? null]),
  );
  return sha256Hex(canonicalJson(identity));
}

function validateCallFinalizationIdentity(
  id: string,
  body: Record<string, unknown>,
): void {
  const startedAtMs = typeof body.started_at === 'string'
    ? Date.parse(body.started_at)
    : Number.NaN;
  const endedAtMs = typeof body.ended_at === 'string'
    ? Date.parse(body.ended_at)
    : Number.NaN;
  if (
    typeof body.external_sid !== 'string'
    || !/^CA[0-9a-fA-F]{32}$/u.test(body.external_sid)
    || id !== `call-ended:${body.external_sid}`
    || body.source !== 'phone'
    || (body.direction !== 'inbound' && body.direction !== 'outbound-alert')
    || typeof body.started_at !== 'string'
    || typeof body.ended_at !== 'string'
    || !Number.isFinite(startedAtMs)
    || !Number.isFinite(endedAtMs)
    || endedAtMs < startedAtMs
    || !Number.isSafeInteger(body.duration_seconds)
    || (body.duration_seconds as number) < 0
  ) {
    throw new Error('phone_call_finalization_invalid');
  }
}

/**
 * Durable Phone -> API financial callback transport. The callback body is
 * frozen before its first HTTP attempt. Network retries change only the
 * timestamp, nonce and HMAC headers; the external-SID keyed body can never be
 * replaced by later duration, caller, intent or receipt data.
 */
export class PhoneApiCallbackOutbox {
  private readonly entries = new Map<string, PhoneApiCallbackEntry>();
  private readonly callFinalizations = new Map<string, PhoneCallFinalizationDraft>();
  private readonly inFlight = new Map<string, Promise<PhoneApiCallbackEntry>>();
  private retryTimer: NodeJS.Timeout | null = null;
  private readonly now: () => number;
  private readonly retryDelayMs: number | null;
  private readonly retentionMs: number;
  private storageHealthy = true;
  private readonly durableParent: DurableParentBinding;

  constructor(private readonly options: PhoneApiCallbackOutboxOptions) {
    if (!path.isAbsolute(options.filePath)) {
      throw new Error('phone_api_callback_outbox_path_not_absolute');
    }
    if (options.signingSecret.length < 32) {
      throw new Error('phone_api_callback_signing_not_configured');
    }
    this.now = options.now ?? Date.now;
    this.retryDelayMs = options.retryDelayMs === undefined ? 5_000 : options.retryDelayMs;
    this.retentionMs = options.retentionMs ?? 7 * 24 * 60 * 60 * 1_000;
    this.durableParent = bindDurableFileParent(options.filePath);
    this.load();
    // Prove the configured path is writable and durably publish even the
    // initial empty/current ledger before readiness can admit a call.
    this.persist();
    if (this.hasRetryableWork()) this.scheduleRetry();
  }

  async enqueue(input: {
    id: string;
    path: PhoneApiCallbackPath;
    body: Record<string, unknown>;
  }): Promise<PhoneApiCallbackEntry> {
    const reserved = input.path === '/internal/voice/call-ended'
      && this.callFinalizations.has(input.id)
      ? this.finalizeCall({
          id: input.id,
          path: '/internal/voice/call-ended',
          body: input.body,
        })
      : this.reserve(input);
    return reserved.state === 'pending'
      ? this.deliver(reserved.id)
      : reserved;
  }

  /**
   * Persist a complete, conservative call-ended fallback before returning
   * TwiML. Only the token digest is durable; the bearer token remains in the
   * one-shot media registry.
   */
  beginCallFinalization(input: {
    id: string;
    body: Record<string, unknown>;
    mediaTokenHash: string;
    mediaExpiresAtMs: number;
  }): PhoneCallFinalizationDraft {
    validateCallFinalizationIdentity(input.id, input.body);
    const startedAtMs = Date.parse(input.body.started_at as string);
    if (
      !/^[0-9a-f]{64}$/u.test(input.mediaTokenHash)
      || !Number.isFinite(input.mediaExpiresAtMs)
      || input.mediaExpiresAtMs < startedAtMs
    ) throw new Error('phone_call_finalization_media_identity_invalid');
    const existingCallback = this.entries.get(input.id);
    if (existingCallback) {
      if (
        existingCallback.path !== '/internal/voice/call-ended'
        || callFinalizationIdentityHash(existingCallback.body)
          !== callFinalizationIdentityHash(input.body)
      ) throw new Error('phone_call_finalization_identity_conflict');
      throw new Error('phone_call_finalization_already_frozen');
    }
    const identityHash = callFinalizationIdentityHash(input.body);
    const existing = this.callFinalizations.get(input.id);
    if (existing) {
      if (
        existing.identityHash !== identityHash
        || existing.mediaTokenHash !== input.mediaTokenHash
        || Date.parse(existing.mediaExpiresAt ?? '') !== input.mediaExpiresAtMs
      ) {
        throw new Error('phone_call_finalization_identity_conflict');
      }
      return cloneFinalization(existing);
    }
    const observedAt = new Date(this.now()).toISOString();
    const draft: PhoneCallFinalizationDraft = {
      id: input.id,
      body: structuredClone(input.body),
      bodyHash: sha256Hex(canonicalJson(input.body)),
      identityHash,
      phase: 'awaiting_media',
      mediaTokenHash: input.mediaTokenHash,
      mediaExpiresAt: new Date(input.mediaExpiresAtMs).toISOString(),
      createdAt: observedAt,
      updatedAt: observedAt,
    };
    this.callFinalizations.set(draft.id, draft);
    try {
      this.persist();
    } catch (error) {
      this.callFinalizations.delete(draft.id);
      throw error;
    }
    this.scheduleRetry();
    return cloneFinalization(draft);
  }

  /** Consume the durable token binding before any media/session work. */
  startCallFinalization(input: {
    id: string;
    mediaTokenHash: string;
  }): PhoneCallFinalizationDraft {
    const current = this.callFinalizations.get(input.id);
    if (!current) {
      if (this.entries.has(input.id)) {
        throw new Error('phone_call_finalization_already_frozen');
      }
      throw new Error('phone_call_finalization_not_found');
    }
    if (
      current.phase !== 'awaiting_media'
      || current.mediaTokenHash !== input.mediaTokenHash
      || Date.parse(current.mediaExpiresAt ?? '') <= this.now()
    ) throw new Error('phone_call_finalization_media_identity_invalid');
    const before = cloneFinalization(current);
    current.phase = 'active';
    current.updatedAt = new Date(this.now()).toISOString();
    try {
      this.persist();
    } catch (error) {
      this.callFinalizations.set(input.id, before);
      throw error;
    }
    return cloneFinalization(current);
  }

  /** Update only non-authority call telemetry while the call is active. */
  checkpointCallFinalization(input: {
    id: string;
    body: Record<string, unknown>;
  }): PhoneCallFinalizationDraft {
    validateCallFinalizationIdentity(input.id, input.body);
    const current = this.callFinalizations.get(input.id);
    if (!current) throw new Error('phone_call_finalization_not_found');
    if (current.phase !== 'active') {
      throw new Error('phone_call_finalization_not_active');
    }
    if (callFinalizationIdentityHash(input.body) !== current.identityHash) {
      throw new Error('phone_call_finalization_identity_conflict');
    }
    if (
      (input.body.duration_seconds as number)
        < (current.body.duration_seconds as number)
      || Date.parse(input.body.ended_at as string)
        < Date.parse(current.body.ended_at as string)
    ) {
      throw new Error('phone_call_finalization_regressed');
    }
    const before = cloneFinalization(current);
    current.body = structuredClone(input.body);
    current.bodyHash = sha256Hex(canonicalJson(input.body));
    current.updatedAt = new Date(this.now()).toISOString();
    try {
      this.persist();
    } catch (error) {
      this.callFinalizations.set(input.id, before);
      throw error;
    }
    return cloneFinalization(current);
  }

  /**
   * Atomically turn a call draft into the one frozen callback. A failed
   * publication remains closed in memory, latches readiness unhealthy and is
   * retried in-process until the pending callback itself is durable.
   */
  finalizeCall(input: {
    id: string;
    path: '/internal/voice/call-ended';
    body: Record<string, unknown>;
  }): PhoneApiCallbackEntry {
    validateCallFinalizationIdentity(input.id, input.body);
    const existing = this.entries.get(input.id);
    const bodyHash = sha256Hex(canonicalJson(input.body));
    if (existing) {
      if (existing.path !== input.path || existing.bodyHash !== bodyHash) {
        throw new Error('phone_api_callback_body_conflict');
      }
      return cloneEntry(existing);
    }
    const draft = this.callFinalizations.get(input.id);
    if (!draft) throw new Error('phone_call_finalization_not_found');
    if (callFinalizationIdentityHash(input.body) !== draft.identityHash) {
      throw new Error('phone_call_finalization_identity_conflict');
    }
    if (
      (input.body.duration_seconds as number) < (draft.body.duration_seconds as number)
      || Date.parse(input.body.ended_at as string)
        < Date.parse(draft.body.ended_at as string)
    ) {
      throw new Error('phone_call_finalization_regressed');
    }
    const closedDraft: PhoneCallFinalizationDraft = {
      ...draft,
      body: structuredClone(input.body),
      bodyHash,
      phase: 'closed',
      updatedAt: new Date(this.now()).toISOString(),
    };
    this.callFinalizations.set(input.id, closedDraft);
    this.revokeMediaToken(input.id);
    const entry = this.newPendingEntry(input, bodyHash);
    this.callFinalizations.delete(input.id);
    this.entries.set(entry.id, entry);
    try {
      this.persist();
    } catch (error) {
      this.entries.delete(entry.id);
      this.callFinalizations.set(closedDraft.id, closedDraft);
      this.scheduleRetry();
      throw error;
    }
    this.scheduleRetry();
    return cloneEntry(entry);
  }

  /** Finalize only a call whose TwiML never reached an authenticated stream. */
  finalizeAwaitingMedia(input: {
    id: string;
    endedAtMs?: number;
  }): PhoneApiCallbackEntry | null {
    const existing = this.entries.get(input.id);
    if (existing) {
      this.revokeMediaToken(input.id);
      return cloneEntry(existing);
    }
    const draft = this.callFinalizations.get(input.id);
    if (!draft || draft.phase !== 'awaiting_media') return null;
    const startedAtMs = Date.parse(draft.body.started_at as string);
    const endedAtMs = Math.max(startedAtMs, input.endedAtMs ?? this.now());
    return this.finalizeCall({
      id: input.id,
      path: '/internal/voice/call-ended',
      body: {
        ...draft.body,
        ended_at: new Date(endedAtMs).toISOString(),
        // No authenticated media stream means there is no billable voice
        // duration, regardless of how long Twilio kept the answer leg open.
        duration_seconds: 0,
      },
    });
  }

  /** Retry every stranded closed transition without requiring a restart. */
  flushFinalizations(nowMs = this.now()): PhoneApiCallbackEntry[] {
    const promoted: PhoneApiCallbackEntry[] = [];
    for (const draft of [...this.callFinalizations.values()]) {
      try {
        if (
          draft.phase === 'awaiting_media'
          && Date.parse(draft.mediaExpiresAt ?? '') <= nowMs
        ) {
          const entry = this.finalizeAwaitingMedia({ id: draft.id, endedAtMs: nowMs });
          if (entry) promoted.push(entry);
          continue;
        }
        if (draft.phase === 'closed') {
          const entry = this.finalizeCall({
            id: draft.id,
            path: '/internal/voice/call-ended',
            body: draft.body,
          });
          promoted.push(entry);
        }
      } catch {
        // storageHealthy remains false; the bounded timer below tries again.
      }
    }
    if (this.hasRetryableWork()) this.scheduleRetry();
    return promoted;
  }

  reserve(input: {
    id: string;
    path: PhoneApiCallbackPath;
    body: Record<string, unknown>;
  }): PhoneApiCallbackEntry {
    if (!input.id || input.id.length > 300) {
      throw new Error('phone_api_callback_id_invalid');
    }
    this.prune();
    const bodyHash = sha256Hex(canonicalJson(input.body));
    const existing = this.entries.get(input.id);
    if (existing) {
      if (existing.path !== input.path || existing.bodyHash !== bodyHash) {
        throw new Error('phone_api_callback_body_conflict');
      }
      return cloneEntry(existing);
    }

    const entry = this.newPendingEntry(input, bodyHash);
    this.entries.set(entry.id, entry);
    // The exact financial receipt body reaches disk before network I/O.
    try {
      this.persist();
    } catch (error) {
      this.entries.delete(entry.id);
      throw error;
    }
    return cloneEntry(entry);
  }

  private newPendingEntry(
    input: {
      id: string;
      path: PhoneApiCallbackPath;
      body: Record<string, unknown>;
    },
    bodyHash = sha256Hex(canonicalJson(input.body)),
  ): PhoneApiCallbackEntry {
    return {
      id: input.id,
      path: input.path,
      body: structuredClone(input.body),
      bodyHash,
      state: 'pending',
      attempts: 0,
      createdAt: new Date(this.now()).toISOString(),
      lastAttemptAt: null,
      completedAt: null,
      lastHttpStatus: null,
      lastError: null,
    };
  }

  async flushPending(): Promise<PhoneApiCallbackEntry[]> {
    const delivered: PhoneApiCallbackEntry[] = [];
    for (const entry of this.entries.values()) {
      if (entry.state !== 'pending') continue;
      delivered.push(await this.deliver(entry.id));
    }
    return delivered;
  }

  get(id: string): PhoneApiCallbackEntry | null {
    const entry = this.entries.get(id);
    return entry ? cloneEntry(entry) : null;
  }

  getCallFinalization(id: string): PhoneCallFinalizationDraft | null {
    const entry = this.callFinalizations.get(id);
    return entry ? cloneFinalization(entry) : null;
  }

  activeFinalizationCount(): number {
    return this.callFinalizations.size;
  }

  isOperational(): boolean {
    return this.storageHealthy
      && durableFileParentIsCurrent(this.durableParent)
      && !this.hasClosedFinalization()
      && [...this.entries.values()].every((entry) => entry.state !== 'rejected');
  }

  reproveStorage(): boolean {
    if (this.rejectedCount() > 0) return false;
    try {
      this.persist();
      if (this.hasClosedFinalization()) {
        this.scheduleRetry();
        return false;
      }
      return this.isOperational();
    } catch {
      return false;
    }
  }

  pendingCount(): number {
    return [...this.entries.values()].filter((entry) => entry.state === 'pending').length;
  }

  rejectedCount(): number {
    return [...this.entries.values()].filter((entry) => entry.state === 'rejected').length;
  }

  deliver(id: string): Promise<PhoneApiCallbackEntry> {
    const active = this.inFlight.get(id);
    if (active) return active;
    const promise = this.attemptDelivery(id).finally(() => {
      this.inFlight.delete(id);
    });
    this.inFlight.set(id, promise);
    return promise;
  }

  private async attemptDelivery(id: string): Promise<PhoneApiCallbackEntry> {
    const entry = this.entries.get(id);
    if (!entry) throw new Error('phone_api_callback_not_found');
    if (entry.state !== 'pending') return cloneEntry(entry);

    const beforeAttempt = cloneEntry(entry);
    entry.attempts += 1;
    entry.lastAttemptAt = new Date(this.now()).toISOString();
    entry.lastError = null;
    try {
      this.persist();
    } catch (error) {
      this.entries.set(id, beforeAttempt);
      throw error;
    }
    try {
      const response = await postSignedPhoneApiCallback({
        apiBaseUrl: this.options.apiBaseUrl,
        path: entry.path,
        signingSecret: this.options.signingSecret,
        body: entry.body,
        fetchImpl: this.options.fetchImpl,
        now: this.now,
        nonce: this.options.nonce,
      });
      entry.lastHttpStatus = response.status;
      const outcome = await callbackResponseOutcome(entry.path, response);
      if (outcome.kind === 'delivered') {
        entry.state = 'delivered';
        entry.completedAt = new Date(this.now()).toISOString();
        entry.lastError = null;
      } else {
        entry.state = outcome.kind === 'rejected' ? 'rejected' : 'pending';
        entry.completedAt = outcome.kind === 'rejected'
          ? new Date(this.now()).toISOString()
          : null;
        entry.lastError = outcome.error;
      }
    } catch (error) {
      entry.lastHttpStatus = null;
      entry.lastError = (error instanceof Error ? error.message : String(error)).slice(0, 500);
    }
    try {
      this.persist();
    } catch (error) {
      this.entries.set(id, beforeAttempt);
      throw error;
    }
    if (entry.state === 'pending') this.scheduleRetry();
    return cloneEntry(entry);
  }

  private scheduleRetry(): void {
    if (this.retryDelayMs === null || this.retryTimer) return;
    let delayMs = this.retryDelayMs;
    const nowMs = this.now();
    for (const draft of this.callFinalizations.values()) {
      if (draft.phase !== 'awaiting_media' || !draft.mediaExpiresAt) continue;
      delayMs = Math.min(
        delayMs,
        Math.max(0, Date.parse(draft.mediaExpiresAt) - nowMs),
      );
    }
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      void this.retryOutstanding().finally(() => {
        if (this.hasRetryableWork()) this.scheduleRetry();
      });
    }, delayMs);
    this.retryTimer.unref?.();
  }

  private async retryOutstanding(): Promise<void> {
    this.flushFinalizations();
    for (const entry of [...this.entries.values()]) {
      if (entry.state !== 'pending') continue;
      try {
        await this.deliver(entry.id);
      } catch {
        // Durable-storage faults stay latched and retry on the next bounded
        // maintenance tick; transport faults already remain pending.
      }
    }
  }

  private hasClosedFinalization(): boolean {
    return [...this.callFinalizations.values()]
      .some((draft) => draft.phase === 'closed');
  }

  private hasRetryableWork(): boolean {
    return this.hasClosedFinalization()
      || [...this.callFinalizations.values()]
        .some((draft) => draft.phase === 'awaiting_media')
      || [...this.entries.values()].some((entry) => entry.state === 'pending');
  }

  private revokeMediaToken(id: string): void {
    const callSid = id.startsWith('call-ended:')
      ? id.slice('call-ended:'.length)
      : '';
    if (!callSid) return;
    try {
      this.options.onMediaTokenRevoked?.(callSid);
    } catch {
      // The durable callback fence is authoritative. Media admission checks
      // this ledger even if the in-memory registry hook itself fails.
    }
  }

  private load(): void {
    if (!fs.existsSync(this.options.filePath)) return;
    const parsed = JSON.parse(
      fs.readFileSync(this.options.filePath, 'utf8'),
    ) as PersistedPhoneApiCallbackOutbox;
    if (
      (parsed.version !== 1 && parsed.version !== 2 && parsed.version !== 3)
      || !Array.isArray(parsed.entries)
      || (
        (parsed.version === 2 || parsed.version === 3)
        && !Array.isArray(parsed.callFinalizations)
      )
    ) {
      throw new Error('unsupported phone API callback outbox format');
    }
    for (const entry of parsed.entries) {
      if (
        !entry.id
        || (entry.path !== '/internal/voice/call-ended'
          && entry.path !== '/internal/voice/outbound-status')
        || sha256Hex(canonicalJson(entry.body)) !== entry.bodyHash
        || (entry.state !== 'pending'
          && entry.state !== 'delivered'
          && entry.state !== 'rejected')
      ) {
        throw new Error('invalid phone API callback outbox entry');
      }
      this.entries.set(entry.id, structuredClone(entry));
    }
    if (parsed.version === 2 || parsed.version === 3) {
      for (const draft of parsed.callFinalizations ?? []) {
        const normalized: PhoneCallFinalizationDraft = parsed.version === 2
          ? {
              ...draft,
              phase: 'active',
              mediaTokenHash: null,
              mediaExpiresAt: null,
            }
          : draft;
        validateCallFinalizationIdentity(normalized.id, normalized.body);
        if (
          sha256Hex(canonicalJson(normalized.body)) !== normalized.bodyHash
          || callFinalizationIdentityHash(normalized.body) !== normalized.identityHash
          || (
            normalized.phase !== 'awaiting_media'
            && normalized.phase !== 'active'
            && normalized.phase !== 'closed'
          )
          || (
            parsed.version === 3
            && (
              !normalized.mediaTokenHash
              || !/^[0-9a-f]{64}$/u.test(normalized.mediaTokenHash)
              || !normalized.mediaExpiresAt
              || !Number.isFinite(Date.parse(normalized.mediaExpiresAt))
            )
          )
          || typeof normalized.createdAt !== 'string'
          || typeof normalized.updatedAt !== 'string'
          || this.entries.has(normalized.id)
          || this.callFinalizations.has(normalized.id)
        ) {
          throw new Error('invalid phone call finalization entry');
        }
        this.callFinalizations.set(normalized.id, structuredClone(normalized));
      }
    }

    // A process restart proves every still-active media connection ended. Turn
    // its last durable checkpoint into the one frozen callback before startup
    // can admit another call or perform network delivery.
    for (const draft of this.callFinalizations.values()) {
      this.revokeMediaToken(draft.id);
      const recoveredBody = draft.phase === 'awaiting_media'
        ? {
            ...draft.body,
            ended_at: new Date(Math.max(
              Date.parse(draft.body.started_at as string),
              this.now(),
            )).toISOString(),
            duration_seconds: 0,
          }
        : draft.body;
      const recovered = this.newPendingEntry({
        id: draft.id,
        path: '/internal/voice/call-ended',
        body: recoveredBody,
      }, sha256Hex(canonicalJson(recoveredBody)));
      recovered.createdAt = draft.createdAt;
      this.entries.set(recovered.id, recovered);
    }
    this.callFinalizations.clear();
    this.prune();
  }

  private prune(): void {
    const nowMs = this.now();
    const removed: PhoneApiCallbackEntry[] = [];
    let changed = false;
    for (const [id, entry] of this.entries) {
      if (entry.state !== 'delivered' || !entry.completedAt) continue;
      if (Date.parse(entry.completedAt) + this.retentionMs <= nowMs) {
        removed.push(cloneEntry(entry));
        this.entries.delete(id);
        changed = true;
      }
    }
    if (changed) {
      try {
        this.persist();
      } catch (error) {
        for (const entry of removed) this.entries.set(entry.id, entry);
        throw error;
      }
    }
  }

  private persist(): void {
    const body: PersistedPhoneApiCallbackOutbox = {
      version: 3,
      entries: [...this.entries.values()],
      callFinalizations: [...this.callFinalizations.values()],
    };
    try {
      durablyReplaceJsonFile(
        this.options.filePath,
        JSON.stringify(body, null, 2),
        this.options.durableFileSystem,
        this.durableParent,
      );
      this.storageHealthy = true;
    } catch (error) {
      this.storageHealthy = false;
      throw error;
    }
  }
}

const CALLBACK_ID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu;

type CallbackResponseOutcome =
  | { kind: 'delivered' }
  | { kind: 'retry'; error: string }
  | { kind: 'rejected'; error: string };

async function callbackResponseOutcome(
  callbackPath: PhoneApiCallbackPath,
  response: Response,
): Promise<CallbackResponseOutcome> {
  if (response.status === 409) {
    return { kind: 'rejected', error: 'callback_identity_conflict' };
  }
  if (callbackPath === '/internal/voice/outbound-status') {
    if (response.status !== 204) {
      return { kind: 'retry', error: `unexpected_status_response:${response.status}` };
    }
    const body = await response.text().catch(() => '__unreadable__');
    return body === ''
      ? { kind: 'delivered' }
      : { kind: 'retry', error: 'unexpected_status_response_body' };
  }

  if (response.status !== 200) {
    return { kind: 'retry', error: `unexpected_call_ended_response:${response.status}` };
  }
  const body = await response.json().catch(() => null) as Record<string, unknown> | null;
  if (
    !body
    || body.ok !== true
    || typeof body.id !== 'string'
    || !CALLBACK_ID_RE.test(body.id)
    || typeof body.status !== 'string'
    || body.status.length === 0
  ) {
    return { kind: 'retry', error: 'unexpected_call_ended_response_body' };
  }
  return { kind: 'delivered' };
}
