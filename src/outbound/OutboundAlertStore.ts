import {
  createHmac,
  randomUUID,
} from 'node:crypto';
import fs from 'node:fs';
import { canonicalJson, sha256Hex } from './canonical.js';
import {
  bindDurableFileParent,
  durableFileParentIsCurrent,
  durablyReplaceJsonFile,
  type DurableParentBinding,
  type DurableFileSystem,
} from './DurableFile.js';
import {
  HarkPhoneResponseSchema,
  ServerAuthoredPhoneNotificationSchema,
  serverAuthoredPhoneNotification,
} from './contract.js';
import type {
  FrozenAlert,
  HarkPhoneBinding,
  HarkPhoneDeliveryStatus,
  HarkPhoneResponse,
  OutboundAlertRequest,
  SanitizedHarkContinuationResult,
  ServerAuthoredPhoneNotification,
} from './contract.js';

export type OutboundAlertState =
  | 'reserved'
  | 'dialing'
  | 'dispatch-uncertain'
  | 'queued'
  | 'ringing'
  | 'answered'
  | 'streaming'
  | 'completed'
  | 'failed'
  | 'busy'
  | 'no-answer'
  | 'canceled'
  | 'expired';

export interface OutboundAlertRecord {
  intentId: string;
  idempotencyKey: string;
  requestHash: string;
  request: OutboundAlertRequest;
  billingBinding: ServerAuthoredPhoneNotification | null;
  state: OutboundAlertState;
  createdAt: string;
  expiresAt: string;
  callSid: string | null;
  answerConsumedAt: string | null;
  streamConsumedAt: string | null;
  completedAt: string | null;
  lastTwilioStatus: string | null;
  lastTwilioSequenceNumber: number | null;
  lastError: string | null;
}

export type HarkOutboxState = 'pending' | 'delivered' | 'rejected';

export type HarkOutboxEntry = {
  id: string;
  intentId: string;
  kind: 'status' | 'response';
  binding: HarkPhoneBinding;
  body: HarkPhoneDeliveryStatus | HarkPhoneResponse;
  bodyHash: string;
  dependsOnId: string | null;
  state: HarkOutboxState;
  attempts: number;
  createdAt: string;
  lastAttemptAt: string | null;
  completedAt: string | null;
  lastError: string | null;
  continuationResult: SanitizedHarkContinuationResult | null;
};

interface PersistedStore {
  version: 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8;
  records: OutboundAlertRecord[];
  harkOutbox?: HarkOutboxEntry[];
}

export type ReserveResult =
  | { kind: 'new'; record: OutboundAlertRecord }
  | { kind: 'existing'; record: OutboundAlertRecord }
  | { kind: 'conflict'; record: OutboundAlertRecord };

export type ExistingReservationResult =
  | { kind: 'existing'; record: OutboundAlertRecord }
  | { kind: 'conflict'; record: OutboundAlertRecord }
  | { kind: 'none' };

export type AnswerAuthorization =
  | {
      ok: true;
      record: OutboundAlertRecord;
      frozenAlert: FrozenAlert;
      harkBinding: HarkPhoneBinding | null;
      streamToken: string;
      expiresAtMs: number;
    }
  | {
      ok: false;
      reason:
        | 'not_found'
        | 'expired'
        | 'wrong_call'
        | 'terminal'
        | 'media_key_unavailable';
    };

export interface OutboundMediaTokenSecrets {
  legacyProducerSecret: string;
  internalApiSecret: string;
}

const TERMINAL_STATES = new Set<OutboundAlertState>([
  'completed',
  'failed',
  'busy',
  'no-answer',
  'canceled',
  'expired',
]);

function cloneRecord(record: OutboundAlertRecord): OutboundAlertRecord {
  return structuredClone(record);
}

function cloneOutboxEntry(entry: HarkOutboxEntry): HarkOutboxEntry {
  return structuredClone(entry);
}

/**
 * `observedAt` is server lifecycle metadata, not recipient, content, Hark, or
 * economic authority. A producer rebuilding the same event after a lost HTTP
 * response may observe it again at a later instant. Keep the first accepted
 * timestamp frozen in the durable request, but exclude only that timestamp
 * from replay identity so the retry reconstructs the same intent.
 */
function requestIdentityHash(request: OutboundAlertRequest): string {
  const { observedAt: _observedAt, ...authorityAndContent } = request;
  return sha256Hex(canonicalJson(authorityAndContent));
}

export type ReserveHarkOutboxResult =
  | { kind: 'new'; entry: HarkOutboxEntry }
  | { kind: 'existing'; entry: HarkOutboxEntry }
  | { kind: 'conflict'; entry: HarkOutboxEntry };

export type TwilioStatusUpdateResult = {
  kind: 'updated' | 'stale' | 'terminal' | 'invalid';
  record: OutboundAlertRecord;
};

/**
 * Durable single-process intent ledger. Reservation is persisted before the
 * Twilio API is called: if the process dies in the dispatch window, a retry
 * returns the existing "dialing" record instead of placing a duplicate call.
 */
export class OutboundAlertStore {
  private readonly records = new Map<string, OutboundAlertRecord>();
  private readonly byIdempotencyKey = new Map<string, string>();
  private readonly harkOutbox = new Map<string, HarkOutboxEntry>();
  private storageHealthy = true;
  private readonly durableParent: DurableParentBinding;

  constructor(
    private readonly filePath: string,
    private readonly retentionMs = 7 * 24 * 60 * 60 * 1000,
    private readonly options: { durableFileSystem?: DurableFileSystem } = {},
  ) {
    this.durableParent = bindDurableFileParent(this.filePath);
    this.load();
    // A constructed store has proven that it can durably publish its current
    // ledger. This is part of call admission, not an eventual health check.
    this.persist();
  }

  reserve(request: OutboundAlertRequest, nowMs = Date.now()): ReserveResult {
    this.prune(nowMs);
    const requestHash = requestIdentityHash(request);
    const existingIntentId = this.byIdempotencyKey.get(request.idempotencyKey);
    if (existingIntentId) {
      const existing = this.records.get(existingIntentId);
      if (existing) {
        return existing.requestHash === requestHash
          ? { kind: 'existing', record: cloneRecord(existing) }
          : { kind: 'conflict', record: cloneRecord(existing) };
      }
    }

    const intentId = randomUUID();
    const createdAt = new Date(nowMs).toISOString();
    const expiresAt = new Date(nowMs + request.ttlSeconds * 1000).toISOString();
    const record: OutboundAlertRecord = {
      intentId,
      idempotencyKey: request.idempotencyKey,
      requestHash,
      request: structuredClone(request),
      billingBinding: serverAuthoredPhoneNotification(request),
      state: 'reserved',
      createdAt,
      expiresAt,
      callSid: null,
      answerConsumedAt: null,
      streamConsumedAt: null,
      completedAt: null,
      lastTwilioStatus: null,
      lastTwilioSequenceNumber: null,
      lastError: null,
    };

    this.records.set(intentId, record);
    this.byIdempotencyKey.set(request.idempotencyKey, intentId);
    try {
      this.persist();
    } catch (error) {
      this.records.delete(intentId);
      this.byIdempotencyKey.delete(request.idempotencyKey);
      throw error;
    }
    return { kind: 'new', record: cloneRecord(record) };
  }

  /**
   * Checks replay identity without creating an intent. This lets provider and
   * API readiness run only for a genuinely new call while matching retries
   * retain their existing no-redial behavior.
   */
  inspectReservation(
    request: OutboundAlertRequest,
    nowMs = Date.now(),
  ): ExistingReservationResult {
    this.prune(nowMs);
    const existingIntentId = this.byIdempotencyKey.get(request.idempotencyKey);
    if (!existingIntentId) return { kind: 'none' };
    const existing = this.records.get(existingIntentId);
    if (!existing) return { kind: 'none' };
    return existing.requestHash === requestIdentityHash(request)
      ? { kind: 'existing', record: cloneRecord(existing) }
      : { kind: 'conflict', record: cloneRecord(existing) };
  }

  isOperational(): boolean {
    return this.storageHealthy && durableFileParentIsCurrent(this.durableParent);
  }

  reproveStorage(): boolean {
    try {
      this.persist();
      return true;
    } catch {
      return false;
    }
  }

  markDialing(intentId: string): OutboundAlertRecord {
    return this.update(intentId, (record) => {
      record.state = 'dialing';
    });
  }

  markQueued(intentId: string, callSid: string): OutboundAlertRecord {
    const record = this.records.get(intentId);
    if (!record) throw new Error(`outbound alert intent not found: ${intentId}`);
    if (record.callSid && record.callSid !== callSid) {
      throw new Error('outbound alert provider SID conflict');
    }
    record.callSid = callSid;
    record.state = 'queued';
    record.lastTwilioStatus = 'queued';
    record.lastError = null;
    // The provider call already exists at this boundary. On a storage fault,
    // retain its exact SID in memory while readiness is latched false so a
    // later storage re-probe can durably recover it. Rolling back to a
    // callSid-less "dialing" record would orphan a real provider call.
    this.persist();
    return cloneRecord(record);
  }

  markDispatchUncertain(intentId: string, error: string): OutboundAlertRecord {
    return this.update(intentId, (record) => {
      record.state = 'dispatch-uncertain';
      record.lastError = error.slice(0, 500);
    });
  }

  /**
   * Adopt the first provider SID only after the caller has authenticated a
   * Twilio webhook. This repairs the accepted-call/lost-response window while
   * preserving the one-provider-call fence. A second SID can never replace it.
   */
  adoptCallSidFromSignedTwilio(
    intentId: string,
    callSid: string,
  ): OutboundAlertRecord | null {
    const current = this.records.get(intentId);
    if (!current) return null;
    if (current.callSid !== null) {
      return current.callSid === callSid ? cloneRecord(current) : null;
    }
    if (current.state !== 'dialing' && current.state !== 'dispatch-uncertain') {
      return null;
    }
    return this.update(intentId, (record) => {
      record.callSid = callSid;
      record.state = 'queued';
      record.lastTwilioStatus = 'initiated';
      record.lastError = null;
    });
  }

  authorizeAnswer(
    intentId: string,
    callSid: string,
    secrets: OutboundMediaTokenSecrets,
    nowMs = Date.now(),
  ): AnswerAuthorization {
    const record = this.records.get(intentId);
    if (!record) return { ok: false, reason: 'not_found' };
    if (new Date(record.expiresAt).getTime() <= nowMs) {
      if (record.state !== 'expired') {
        const before = cloneRecord(record);
        record.state = 'expired';
        record.completedAt = new Date(nowMs).toISOString();
        try {
          this.persist();
        } catch (error) {
          this.records.set(record.intentId, before);
          throw error;
        }
      }
      return { ok: false, reason: 'expired' };
    }
    if (record.callSid !== callSid) return { ok: false, reason: 'wrong_call' };
    if (TERMINAL_STATES.has(record.state)) return { ok: false, reason: 'terminal' };
    if (record.streamConsumedAt) return { ok: false, reason: 'terminal' };

    const mediaDomain = record.request.hark
      ? 'hark'
      : record.billingBinding
        ? 'api'
        : 'legacy';
    const mediaRootSecret = record.request.hark || record.billingBinding
      ? secrets.internalApiSecret
      : secrets.legacyProducerSecret;
    if (mediaRootSecret.length < 32) {
      return { ok: false, reason: 'media_key_unavailable' };
    }

    if (!record.answerConsumedAt) {
      const before = cloneRecord(record);
      record.answerConsumedAt = new Date(nowMs).toISOString();
      record.state = 'answered';
      record.lastTwilioStatus = 'in-progress';
      try {
        this.persist();
      } catch (error) {
        this.records.set(record.intentId, before);
        throw error;
      }
    }

    const streamToken = createHmac('sha256', mediaRootSecret)
      .update(
        `dexter-phone/media-token/v1\n${mediaDomain}\n${intentId}\n${callSid}`,
        'utf8',
      )
      .digest('base64url');

    return {
      ok: true,
      record: cloneRecord(record),
      frozenAlert: {
        intentId,
        eventId: record.request.eventId,
        recipientName: record.request.recipientName ?? null,
        to: record.request.to,
        subject: record.request.subject,
        severity: record.request.severity,
        briefing: record.request.briefing,
        observedAt: record.request.observedAt,
        followupContext: structuredClone(record.request.followupContext),
      },
      harkBinding: record.request.hark
        ? structuredClone(record.request.hark)
        : null,
      streamToken,
      expiresAtMs: new Date(record.expiresAt).getTime(),
    };
  }

  markStreaming(intentId: string, callSid: string, nowMs = Date.now()): OutboundAlertRecord | null {
    const record = this.records.get(intentId);
    if (!record || record.callSid !== callSid || TERMINAL_STATES.has(record.state)) return null;
    if (record.streamConsumedAt) return null;
    const before = cloneRecord(record);
    record.streamConsumedAt = new Date(nowMs).toISOString();
    record.state = 'streaming';
    try {
      this.persist();
    } catch (error) {
      this.records.set(intentId, before);
      throw error;
    }
    return cloneRecord(record);
  }

  updateTwilioStatus(
    intentId: string,
    callSid: string,
    twilioStatus: string,
    sequenceNumber: number,
    nowMs = Date.now(),
  ): TwilioStatusUpdateResult | null {
    const preview = this.previewTwilioStatus(
      intentId,
      callSid,
      twilioStatus,
      sequenceNumber,
      nowMs,
    );
    if (!preview || preview.kind !== 'updated') return preview;
    const record = this.records.get(intentId);
    if (!record) return null;
    const before = cloneRecord(record);
    this.records.set(intentId, cloneRecord(preview.record));
    try {
      this.persist();
    } catch (error) {
      this.records.set(intentId, before);
      throw error;
    }
    return { kind: 'updated', record: cloneRecord(preview.record) };
  }

  previewTwilioStatus(
    intentId: string,
    callSid: string,
    twilioStatus: string,
    sequenceNumber: number,
    nowMs = Date.now(),
  ): TwilioStatusUpdateResult | null {
    const current = this.records.get(intentId);
    if (!current) return null;
    const record = cloneRecord(current);
    if (record.callSid !== callSid) {
      if (
        record.callSid !== null
        || (record.state !== 'dialing' && record.state !== 'dispatch-uncertain')
      ) return null;
      record.callSid = callSid;
      record.state = 'queued';
      record.lastTwilioStatus = 'initiated';
      record.lastError = null;
    }
    const mapped = mapTwilioStatus(twilioStatus);
    if (!mapped || !Number.isSafeInteger(sequenceNumber) || sequenceNumber < 0) {
      return { kind: 'invalid', record };
    }
    if (
      record.lastTwilioSequenceNumber !== null
      && sequenceNumber <= record.lastTwilioSequenceNumber
    ) {
      return { kind: 'stale', record };
    }
    if (TERMINAL_STATES.has(record.state)) {
      return { kind: 'terminal', record };
    }

    record.lastTwilioSequenceNumber = sequenceNumber;
    record.lastTwilioStatus = twilioStatus.slice(0, 80);
    if (TERMINAL_STATES.has(mapped)) {
      record.state = mapped;
      record.completedAt ||= new Date(nowMs).toISOString();
    } else if (
      record.state !== 'streaming'
      && twilioProgress(mapped) >= twilioProgress(record.state)
    ) {
      record.state = mapped;
    }
    return { kind: 'updated', record };
  }

  get(intentId: string): OutboundAlertRecord | null {
    const record = this.records.get(intentId);
    return record ? cloneRecord(record) : null;
  }

  reserveHarkOutbox(
    input: {
      id: string;
      intentId: string;
      kind: HarkOutboxEntry['kind'];
      binding: HarkPhoneBinding;
      body: HarkOutboxEntry['body'];
      dependsOnId?: string | null;
    },
    nowMs = Date.now(),
  ): ReserveHarkOutboxResult {
    this.prune(nowMs);
    const bodyHash = sha256Hex(canonicalJson(input.body));
    const existing = this.harkOutbox.get(input.id);
    if (existing) {
      return existing.bodyHash === bodyHash &&
        existing.dependsOnId === (input.dependsOnId ?? null)
        ? { kind: 'existing', entry: cloneOutboxEntry(existing) }
        : { kind: 'conflict', entry: cloneOutboxEntry(existing) };
    }

    const entry: HarkOutboxEntry = {
      id: input.id,
      intentId: input.intentId,
      kind: input.kind,
      binding: structuredClone(input.binding),
      body: structuredClone(input.body),
      bodyHash,
      dependsOnId: input.dependsOnId ?? null,
      state: 'pending',
      attempts: 0,
      createdAt: new Date(nowMs).toISOString(),
      lastAttemptAt: null,
      completedAt: null,
      lastError: null,
      continuationResult: null,
    };
    this.harkOutbox.set(entry.id, entry);
    try {
      this.persist();
    } catch (error) {
      this.harkOutbox.delete(entry.id);
      throw error;
    }
    return { kind: 'new', entry: cloneOutboxEntry(entry) };
  }

  getHarkOutbox(id: string): HarkOutboxEntry | null {
    const entry = this.harkOutbox.get(id);
    return entry ? cloneOutboxEntry(entry) : null;
  }

  listPendingHarkOutbox(
    limit = 100,
    excludeIds: ReadonlySet<string> = new Set(),
  ): HarkOutboxEntry[] {
    return [...this.harkOutbox.values()]
      .filter((entry) => entry.state === 'pending' && !excludeIds.has(entry.id))
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt))
      .slice(0, Math.max(0, limit))
      .map(cloneOutboxEntry);
  }

  latestExplicitHarkResponse(intentId: string): HarkOutboxEntry | null {
    let latest: HarkOutboxEntry | null = null;
    for (const entry of this.harkOutbox.values()) {
      if (entry.intentId !== intentId || entry.kind !== 'response') continue;
      const body = entry.body as HarkPhoneResponse;
      if (body.decision.type === 'acknowledge') continue;
      // Map insertion order survives persistence. Replacing on equal timestamps
      // makes rapid same-millisecond corrections supersede the actual latest
      // response instead of falling back to the first one in that millisecond.
      if (!latest || entry.createdAt >= latest.createdAt) latest = entry;
    }
    return latest ? cloneOutboxEntry(latest) : null;
  }

  bindHarkCorrectionSupersedesResponse(
    id: string,
    predecessorId: string,
    apiResponseId: string,
  ): HarkOutboxEntry {
    return this.updateHarkOutbox(id, (entry) => {
      if (
        entry.kind !== 'response'
        || entry.state !== 'pending'
        || entry.dependsOnId !== predecessorId
      ) {
        throw new Error('Hark correction dependency binding did not match its durable outbox entry');
      }
      const body = entry.body as HarkPhoneResponse;
      if (body.supersedesResponseId === apiResponseId) return;
      if (body.supersedesResponseId !== null || entry.attempts !== 0) {
        throw new Error('Hark correction supersedes UUID cannot change after its first HTTP attempt');
      }
      const hydrated = HarkPhoneResponseSchema.parse({
        ...body,
        supersedesResponseId: apiResponseId,
      });
      entry.body = hydrated;
      entry.bodyHash = sha256Hex(canonicalJson(hydrated));
    });
  }

  markHarkOutboxAttempt(id: string, nowMs = Date.now()): HarkOutboxEntry {
    return this.updateHarkOutbox(id, (entry) => {
      entry.attempts += 1;
      entry.lastAttemptAt = new Date(nowMs).toISOString();
      entry.lastError = null;
    });
  }

  markHarkOutboxRetry(id: string, error: string): HarkOutboxEntry {
    return this.updateHarkOutbox(id, (entry) => {
      entry.state = 'pending';
      entry.lastError = error.slice(0, 500);
    });
  }

  markHarkOutboxDelivered(
    id: string,
    continuationResult: SanitizedHarkContinuationResult | null,
    nowMs = Date.now(),
  ): HarkOutboxEntry {
    return this.updateHarkOutbox(id, (entry) => {
      entry.state = 'delivered';
      entry.completedAt = new Date(nowMs).toISOString();
      entry.lastError = null;
      entry.continuationResult = continuationResult
        ? structuredClone(continuationResult)
        : null;
    });
  }

  markHarkOutboxRejected(
    id: string,
    error: string,
    nowMs = Date.now(),
  ): HarkOutboxEntry {
    return this.updateHarkOutbox(id, (entry) => {
      entry.state = 'rejected';
      entry.completedAt = new Date(nowMs).toISOString();
      entry.lastError = error.slice(0, 500);
      entry.continuationResult = null;
    });
  }

  private update(
    intentId: string,
    mutate: (record: OutboundAlertRecord) => void,
  ): OutboundAlertRecord {
    const record = this.records.get(intentId);
    if (!record) throw new Error(`outbound alert intent not found: ${intentId}`);
    const before = cloneRecord(record);
    mutate(record);
    try {
      this.persist();
    } catch (error) {
      this.records.set(intentId, before);
      throw error;
    }
    return cloneRecord(record);
  }

  private updateHarkOutbox(
    id: string,
    mutate: (entry: HarkOutboxEntry) => void,
  ): HarkOutboxEntry {
    const entry = this.harkOutbox.get(id);
    if (!entry) throw new Error(`Hark outbox entry not found: ${id}`);
    const before = cloneOutboxEntry(entry);
    mutate(entry);
    try {
      this.persist();
    } catch (error) {
      this.harkOutbox.set(id, before);
      throw error;
    }
    return cloneOutboxEntry(entry);
  }

  private load(): void {
    if (!fs.existsSync(this.filePath)) return;
    const parsed = JSON.parse(fs.readFileSync(this.filePath, 'utf8')) as PersistedStore;
    if (
      (parsed.version !== 1 &&
        parsed.version !== 2 &&
        parsed.version !== 3 &&
        parsed.version !== 4 &&
        parsed.version !== 5 &&
        parsed.version !== 6 &&
        parsed.version !== 7 &&
        parsed.version !== 8) ||
      !Array.isArray(parsed.records)
    ) {
      throw new Error('unsupported outbound alert state format');
    }
    for (const rawRecord of parsed.records) {
      // Version 1 persisted hashes for callback URL secrets. Callback
      // authorization now comes exclusively from Twilio's request signature
      // plus exact intent/CallSid binding, so strip those obsolete fields when
      // loading rather than carrying them forward in memory or on disk.
      const {
        answerTokenHash: _obsoleteAnswerTokenHash,
        statusTokenHash: _obsoleteStatusTokenHash,
        ...record
      } = rawRecord as OutboundAlertRecord & {
        answerTokenHash?: string;
        statusTokenHash?: string;
      };
      const derivedBillingBinding = serverAuthoredPhoneNotification(record.request);
      const derivedRequestHash = requestIdentityHash(record.request);
      if ((parsed.version === 7 || parsed.version === 8)
        && record.requestHash !== derivedRequestHash) {
        throw new Error('outbound alert request identity mismatch');
      }
      let billingBinding: ServerAuthoredPhoneNotification | null = null;
      if (parsed.version === 6 || parsed.version === 7 || parsed.version === 8) {
        if (record.billingBinding !== null) {
          const stored = ServerAuthoredPhoneNotificationSchema.safeParse(
            record.billingBinding,
          );
          if (!stored.success) {
            throw new Error('invalid outbound alert billing binding');
          }
          if (
            !derivedBillingBinding
            || canonicalJson(stored.data) !== canonicalJson(derivedBillingBinding)
          ) {
            throw new Error('outbound alert billing binding mismatch');
          }
          billingBinding = stored.data;
        }
      }
      this.records.set(record.intentId, {
        ...record,
        requestHash: derivedRequestHash,
        // Billing provenance exists only when v6+ ingress authenticated and
        // persisted it. An older legacy record is never upgraded into API
        // authority merely because its untrusted context matches a new shape.
        billingBinding,
        lastTwilioSequenceNumber: record.lastTwilioSequenceNumber ?? null,
      });
      this.byIdempotencyKey.set(record.idempotencyKey, record.intentId);
    }
    if (
      (
        parsed.version === 3
        || parsed.version === 4
        || parsed.version === 5
        || parsed.version === 6
        || parsed.version === 7
        || parsed.version === 8
      )
      && Array.isArray(parsed.harkOutbox)
    ) {
      for (const rawEntry of parsed.harkOutbox) {
        const entry = {
          ...rawEntry,
          dependsOnId: rawEntry.dependsOnId ?? null,
        };
        if (!entry?.id || !entry?.intentId || !entry?.bodyHash) continue;
        this.harkOutbox.set(entry.id, entry);
      }
    }
    this.prune(Date.now());
    if (parsed.version !== 8) this.persist();
  }

  private prune(nowMs: number): void {
    const recordsBefore = [...this.records.entries()].map(
      ([id, record]) => [id, cloneRecord(record)] as const,
    );
    const idempotencyBefore = [...this.byIdempotencyKey.entries()];
    const harkOutboxBefore = [...this.harkOutbox.entries()].map(
      ([id, entry]) => [id, cloneOutboxEntry(entry)] as const,
    );
    let changed = false;
    for (const [intentId, record] of this.records) {
      const expiresAtMs = new Date(record.expiresAt).getTime();
      if (expiresAtMs <= nowMs && !TERMINAL_STATES.has(record.state)) {
        record.state = 'expired';
        record.completedAt = new Date(nowMs).toISOString();
        changed = true;
      }
      const terminalAt = record.completedAt ? new Date(record.completedAt).getTime() : null;
      if (terminalAt !== null && terminalAt + this.retentionMs <= nowMs) {
        this.records.delete(intentId);
        this.byIdempotencyKey.delete(record.idempotencyKey);
        changed = true;
      }
    }
    for (const [id, entry] of this.harkOutbox) {
      if (entry.state === 'pending' || !entry.completedAt) continue;
      const completedAtMs = new Date(entry.completedAt).getTime();
      if (Number.isFinite(completedAtMs) && completedAtMs + this.retentionMs <= nowMs) {
        this.harkOutbox.delete(id);
        changed = true;
      }
    }
    if (changed) {
      try {
        this.persist();
      } catch (error) {
        this.records.clear();
        for (const [id, record] of recordsBefore) this.records.set(id, record);
        this.byIdempotencyKey.clear();
        for (const [key, id] of idempotencyBefore) this.byIdempotencyKey.set(key, id);
        this.harkOutbox.clear();
        for (const [id, entry] of harkOutboxBefore) this.harkOutbox.set(id, entry);
        throw error;
      }
    }
  }

  private persist(): void {
    const body: PersistedStore = {
      version: 8,
      records: [...this.records.values()],
      harkOutbox: [...this.harkOutbox.values()],
    };
    try {
      durablyReplaceJsonFile(
        this.filePath,
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

export function mapTwilioStatus(status: string): OutboundAlertState | null {
  switch (status) {
    case 'initiated':
    case 'queued':
      return 'queued';
    case 'ringing':
      return 'ringing';
    case 'answered':
    case 'in-progress':
      return 'answered';
    case 'completed':
      return 'completed';
    case 'busy':
      return 'busy';
    case 'no-answer':
      return 'no-answer';
    case 'canceled':
      return 'canceled';
    case 'failed':
      return 'failed';
    default:
      return null;
  }
}

function twilioProgress(state: OutboundAlertState): number {
  switch (state) {
    case 'reserved':
    case 'dialing':
    case 'dispatch-uncertain':
      return -1;
    case 'queued':
      return 0;
    case 'ringing':
      return 1;
    case 'answered':
      return 2;
    case 'streaming':
      return 3;
    default:
      return 4;
  }
}
