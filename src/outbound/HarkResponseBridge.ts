import { randomBytes } from 'node:crypto';
import { tool } from '@openai/agents/realtime';
import { z } from 'zod';
import { createSignedAlertSignature } from './auth.js';
import { canonicalJson, sha256Hex } from './canonical.js';
import type { HarkHumanTurnQualifier } from './harkAcknowledgment.js';
import {
  HarkDecisionSchema,
  HarkPhoneDecisionSchema,
  HarkPhoneDeliveryStatusSchema,
  HarkPhoneResponseSchema,
  HarkPhoneResponseCallbackResultSchema,
  HarkSanitizedPhoneResponseCallbackResultSchema,
  HarkPhoneStatusCallbackResultSchema,
  type HarkDecision,
  type HarkPhoneBinding,
  type HarkPhoneDecision,
  type HarkPhoneDeliveryStatus,
  type HarkPhoneResponse,
  type SanitizedHarkContinuationResult,
} from './contract.js';
import {
  OutboundAlertStore,
  type HarkOutboxEntry,
} from './OutboundAlertStore.js';

type FetchLike = typeof fetch;

export interface HarkCallBinding {
  intentId: string;
  callSid: string;
  hark: HarkPhoneBinding;
}

export interface HarkBridgeSubmission {
  sourceId: string;
  kind: HarkOutboxEntry['kind'];
  state: HarkOutboxEntry['state'];
  replay: boolean;
  error: string | null;
  continuationResult: SanitizedHarkContinuationResult | null;
}

export interface HarkResponseInvocation {
  providerInvocationId: string;
  userItemId: string;
}

export interface HarkResponseBridgeOptions {
  store: OutboundAlertStore;
  apiBaseUrl: string;
  signingSecret: string;
  fetchImpl?: FetchLike;
  now?: () => number;
  requestTimeoutMs?: number;
  retryInProcess?: boolean;
}

const TOOL_DECISION_TYPES = ['dismiss', 'snooze', 'reply', 'choice', 'instruction'] as const;

class HarkBindingMismatchError extends Error {}
class HarkCallbackResultError extends Error {}

const RespondToHarkToolSchema = z
  .object({
    decisionType: z.enum(TOOL_DECISION_TYPES),
    text: z.string().min(1).max(4_000).optional(),
    snoozeUntil: z.string().datetime({ offset: true }).optional(),
    correctionOfPrevious: z.boolean().optional().default(false),
  })
  .strict();

type RespondToHarkToolInput = z.infer<typeof RespondToHarkToolSchema>;

const HarkResponseInvocationSchema = z
  .object({
    providerInvocationId: z.string().min(1).max(240),
    userItemId: z.string().min(1).max(240),
  })
  .strict();

function stableSourceId(prefix: string, value: unknown): string {
  return `${prefix}:${sha256Hex(canonicalJson(value))}`;
}

function outboxId(kind: HarkOutboxEntry['kind'], sourceId: string): string {
  return `${kind}:${sourceId}`;
}

function publicSubmissionResult(submission: HarkBridgeSubmission): Record<string, unknown> {
  const continuation = callbackContinuation(submission.continuationResult);
  return {
    recorded: true,
    delivery: submission.state,
    replay: submission.replay,
    caseState: submission.continuationResult?.caseState ?? null,
    continuation,
    message: submission.state === 'delivered'
      ? continuation
        ? 'Hark recorded the response and linked the exact originating task continuation.'
        : 'Hark recorded the response.'
      : submission.state === 'pending'
        ? 'The response is durably recorded on this phone service and will retry delivery to Hark.'
        : 'The response is durably recorded, but Hark rejected the callback. Do not claim the task resumed.',
  };
}

function callbackContinuation(result: SanitizedHarkContinuationResult | null) {
  return result && 'continuation' in result ? result.continuation ?? null : null;
}

function sanitizeContinuationResult(
  value: unknown,
  expectedBody: HarkPhoneDeliveryStatus | HarkPhoneResponse,
  kind: HarkOutboxEntry['kind'],
): SanitizedHarkContinuationResult {
  if (kind === 'response') {
    const parsed = HarkPhoneResponseCallbackResultSchema.safeParse(value);
    if (!parsed.success) throw callbackResultError(parsed.error.issues[0]?.message);
    const body = expectedBody as HarkPhoneResponse;
    assertReceiptBinding(parsed.data, body, 'sourceResponseId');
    const { continuation: rawContinuation, ...receipt } = parsed.data;
    if (!rawContinuation) return receipt;
    if (rawContinuation.origin.taskId !== body.originTaskId) {
      throw new HarkBindingMismatchError(
        'Hark continuation origin.taskId did not match the authenticated Phone binding',
      );
    }
    if (
      rawContinuation.case.id !== body.caseId
      || rawContinuation.case.eventId !== body.eventId
      || rawContinuation.response.id !== parsed.data.responseId
      || canonicalJson(rawContinuation.response.decision) !== canonicalJson(body.decision)
    ) {
      throw new HarkBindingMismatchError(
        'Hark continuation case or response did not match the exact Phone callback',
      );
    }
    return {
      ...receipt,
      continuation: {
        id: rawContinuation.continuationId,
        originTaskId: rawContinuation.origin.taskId,
      },
    };
  }

  const parsed = HarkPhoneStatusCallbackResultSchema.safeParse(value);
  if (!parsed.success) throw callbackResultError(parsed.error.issues[0]?.message);
  assertReceiptBinding(
    parsed.data,
    expectedBody as HarkPhoneDeliveryStatus,
    'sourceEventId',
  );
  return parsed.data;
}

function normalizeDecisionForWire(
  decisionInput: HarkDecision,
  observedAtMs: number,
): HarkPhoneDecision {
  const decision = HarkDecisionSchema.parse(decisionInput);
  switch (decision.type) {
    case 'acknowledge':
    case 'dismiss':
      return HarkPhoneDecisionSchema.parse({
        type: decision.type,
        ...(decision.text ? { text: decision.text } : {}),
      });
    case 'snooze': {
      const requestedSeconds = Math.ceil(
        (Date.parse(decision.snoozeUntil as string) - observedAtMs) / 1_000,
      );
      const seconds = Math.max(60, Math.min(30 * 24 * 60 * 60, requestedSeconds));
      return HarkPhoneDecisionSchema.parse({
        type: 'snooze',
        seconds,
        ...(decision.text ? { text: decision.text } : {}),
      });
    }
    case 'choice': {
      const text = decision.text as string;
      return HarkPhoneDecisionSchema.parse({
        type: 'choice',
        value: text.slice(0, 500),
        ...(text.length > 500 ? { text } : {}),
      });
    }
    case 'reply':
    case 'instruction':
      return HarkPhoneDecisionSchema.parse({
        type: decision.type,
        text: decision.text,
      });
  }
}

function predecessorApiResponseId(dependency: HarkOutboxEntry): string {
  if (dependency.kind !== 'response') {
    throw new HarkBindingMismatchError(
      'Hark correction dependency was not a Phone response',
    );
  }
  const parsed = HarkSanitizedPhoneResponseCallbackResultSchema.safeParse(
    dependency.continuationResult,
  );
  if (!parsed.success) {
    throw new HarkBindingMismatchError(
      'Hark correction predecessor receipt did not contain a valid API response UUID',
    );
  }
  assertReceiptBinding(
    parsed.data,
    dependency.body as HarkPhoneResponse,
    'sourceResponseId',
  );
  return parsed.data.responseId;
}

function callbackResultError(message?: string): HarkCallbackResultError {
  return new HarkCallbackResultError(
    `invalid hark.phone-callback-result.v1: ${message ?? 'malformed body'}`,
  );
}

function assertReceiptBinding(
  actual: Record<string, unknown>,
  expected: HarkPhoneDeliveryStatus | HarkPhoneResponse,
  sourceField: 'sourceEventId' | 'sourceResponseId',
): void {
  const fields = [
    'caseId',
    'eventId',
    'deliveryId',
    'intentId',
    'callSid',
    'originTaskId',
    sourceField,
  ] as const;
  for (const field of fields) {
    if (actual[field] !== expected[field as keyof typeof expected]) {
      throw new HarkBindingMismatchError(
        `Hark callback receipt ${field} did not match the exact Phone outbox entry`,
      );
    }
  }
}

export class HarkResponseBridge {
  private readonly store: OutboundAlertStore;
  private readonly apiBaseUrl: string;
  private readonly signingSecret: string;
  private readonly fetchImpl: FetchLike;
  private readonly now: () => number;
  private readonly requestTimeoutMs: number;
  private readonly retryInProcess: boolean;
  private readonly inFlight = new Map<string, Promise<HarkOutboxEntry>>();
  private retryTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(options: HarkResponseBridgeOptions) {
    this.store = options.store;
    this.apiBaseUrl = options.apiBaseUrl.replace(/\/+$/u, '');
    this.signingSecret = options.signingSecret;
    this.fetchImpl = options.fetchImpl ?? fetch;
    this.now = options.now ?? Date.now;
    this.requestTimeoutMs = options.requestTimeoutMs ?? 3_000;
    this.retryInProcess = options.retryInProcess ?? true;
  }

  async reportStatus(
    call: HarkCallBinding,
    status: HarkPhoneDeliveryStatus['status'],
  ): Promise<HarkBridgeSubmission> {
    const sourceEventId = stableSourceId('phone-status', {
      caseId: call.hark.caseId,
      deliveryId: call.hark.deliveryId,
      intentId: call.intentId,
      callSid: call.callSid,
      originTaskId: call.hark.originTaskId,
      status,
    });
    const id = outboxId('status', sourceEventId);
    const existing = this.store.getHarkOutbox(id);
    if (existing) return this.deliverOrReplay(existing, true);

    const body = HarkPhoneDeliveryStatusSchema.parse({
      v: 'hark.phone-status.v1',
      sourceEventId,
      caseId: call.hark.caseId,
      eventId: call.hark.eventId,
      deliveryId: call.hark.deliveryId,
      intentId: call.intentId,
      callSid: call.callSid,
      originTaskId: call.hark.originTaskId,
      status,
      observedAt: new Date(this.now()).toISOString(),
    });
    return this.reserveAndDeliver({
      id,
      intentId: call.intentId,
      kind: 'status',
      binding: call.hark,
      body,
    });
  }

  async acknowledge(
    call: HarkCallBinding,
    userItemId: string,
  ): Promise<HarkBridgeSubmission> {
    return this.submitDecision(call, { type: 'acknowledge' }, false, {
      providerInvocationId: `automatic-ack:${userItemId}`,
      userItemId,
    });
  }

  async submitResponse(
    call: HarkCallBinding,
    decisionInput: HarkDecision,
    invocationInput: HarkResponseInvocation,
    correctionOfPrevious = false,
  ): Promise<HarkBridgeSubmission> {
    if (decisionInput.type === 'acknowledge') {
      return this.acknowledge(call, invocationInput.userItemId);
    }
    return this.submitDecision(call, decisionInput, correctionOfPrevious, invocationInput);
  }

  createRespondToHarkTool(
    call: HarkCallBinding,
    onRecorded?: (submission: HarkBridgeSubmission) => void,
    humanTurnQualifier?: HarkHumanTurnQualifier,
    resolveCausalCallerItemId: (
      providerInvocationId: string,
    ) => string | null | Promise<string | null> = () => null,
    qualificationWaitMs = 2_000,
  ) {
    return tool({
      name: 'respond_to_hark',
      description:
        'Return the owner\'s clear answer to the exact Hark case that initiated this call. Use only for an explicit reply, choice, instruction, dismissal, or snooze. Copy the owner\'s natural-language answer closely; never infer a decision from ambiguity or ordinary conversation.',
      parameters: RespondToHarkToolSchema,
      execute: async (input: RespondToHarkToolInput, _context, details) => {
        // callId comes from the Realtime provider envelope, never model input.
        // The caller item comes only from raw provider predecessor lineage that
        // ends at independently ordered server-observed audio. Together they
        // form the call-local one-to-one wake binding.
        const providerInvocationId = details?.toolCall?.callId?.trim() || '';
        if (!providerInvocationId || !humanTurnQualifier) {
          return JSON.stringify({
            recorded: false,
            delivery: 'blocked',
            continuation: null,
            message:
              'Hark could not bind this response to a provider tool invocation. Ask the caller to repeat the decision; do not claim it was recorded.',
          });
        }
        const userItemId = (await resolveCausalCallerItemId(providerInvocationId))?.trim() || '';
        const qualifiedUserItemId = await humanTurnQualifier.waitForQualifiedInvocation({
          providerInvocationId,
          userItemId,
          timeoutMs: qualificationWaitMs,
        });
        if (!qualifiedUserItemId) {
          return JSON.stringify({
            recorded: false,
            delivery: 'blocked',
            continuation: null,
            message: userItemId
              ? 'Hark did not receive a qualifying completed caller turn for this exact tool invocation in time. Ask the caller to repeat the decision; do not claim it was recorded.'
              : 'Hark could not bind this response to a server-observed caller turn. Ask the caller to repeat the decision; do not claim it was recorded.',
          });
        }
        const decision = HarkDecisionSchema.parse({
          type: input.decisionType,
          text: input.text?.trim() || undefined,
          snoozeUntil: input.snoozeUntil,
        });
        const submission = await this.submitResponse(
          call,
          decision,
          { providerInvocationId, userItemId: qualifiedUserItemId },
          input.correctionOfPrevious,
        );
        onRecorded?.(submission);
        return JSON.stringify(publicSubmissionResult(submission));
      },
    });
  }

  async flushPending(batchSize = 100, maxBatches = 20): Promise<void> {
    const attemptedIds = new Set<string>();
    let highestAttempts = 1;
    for (let batch = 0; batch < maxBatches; batch += 1) {
      const pending = this.store.listPendingHarkOutbox(batchSize, attemptedIds);
      if (pending.length === 0) break;
      for (const entry of pending) {
        attemptedIds.add(entry.id);
        const result = await this.deliver(entry);
        highestAttempts = Math.max(highestAttempts, result.attempts);
      }
      if (pending.length < batchSize) break;
    }
    if (this.store.listPendingHarkOutbox(1).length > 0) {
      this.scheduleRetry(highestAttempts);
    }
  }

  private async submitDecision(
    call: HarkCallBinding,
    decisionInput: HarkDecision,
    correctionOfPrevious: boolean,
    invocationInput: HarkResponseInvocation,
  ): Promise<HarkBridgeSubmission> {
    const naturalDecision = HarkDecisionSchema.parse(decisionInput);
    const invocation = HarkResponseInvocationSchema.parse(invocationInput);
    const sourceResponseId = stableSourceId('phone-response', {
      caseId: call.hark.caseId,
      deliveryId: call.hark.deliveryId,
      intentId: call.intentId,
      callSid: call.callSid,
      originTaskId: call.hark.originTaskId,
      providerInvocationId: invocation.providerInvocationId,
    });
    const id = outboxId('response', sourceResponseId);
    const existing = this.store.getHarkOutbox(id);
    if (existing) {
      const existingBody = existing.body as HarkPhoneResponse;
      const existingObservedAtMs = Date.parse(existingBody.observedAt);
      const expectedDecision = normalizeDecisionForWire(
        naturalDecision,
        existingObservedAtMs,
      );
      if (
        canonicalJson(existingBody.decision) !== canonicalJson(expectedDecision) ||
        Boolean(existing.dependsOnId) !== correctionOfPrevious
      ) {
        throw new Error(`Hark provider invocation conflict for ${id}`);
      }
      return this.deliverOrReplay(existing, true);
    }

    const predecessor = correctionOfPrevious
      ? this.store.latestExplicitHarkResponse(call.intentId)
      : null;
    if (correctionOfPrevious && !predecessor) {
      throw new Error('Hark correction requested before any prior explicit Phone response');
    }
    const observedAtMs = this.now();
    const decision = normalizeDecisionForWire(naturalDecision, observedAtMs);

    const body = HarkPhoneResponseSchema.parse({
      v: 'hark.response.v1',
      sourceResponseId,
      caseId: call.hark.caseId,
      eventId: call.hark.eventId,
      deliveryId: call.hark.deliveryId,
      intentId: call.intentId,
      callSid: call.callSid,
      originTaskId: call.hark.originTaskId,
      observedAt: new Date(observedAtMs).toISOString(),
      decision,
      // A correction's API response UUID is unknown until its exact durable
      // predecessor is accepted. The dependency carries that intent while
      // this null placeholder remains unsent; performDelivery hydrates and
      // persists the UUID/body hash before the correction's first attempt.
      supersedesResponseId: null,
    });
    return this.reserveAndDeliver({
      id,
      intentId: call.intentId,
      kind: 'response',
      binding: call.hark,
      body,
      dependsOnId: predecessor?.id ?? null,
    }, observedAtMs);
  }

  private async reserveAndDeliver(input: {
    id: string;
    intentId: string;
    kind: HarkOutboxEntry['kind'];
    binding: HarkPhoneBinding;
    body: HarkOutboxEntry['body'];
    dependsOnId?: string | null;
  }, reservedAtMs = this.now()): Promise<HarkBridgeSubmission> {
    const reserved = this.store.reserveHarkOutbox(input, reservedAtMs);
    if (reserved.kind === 'conflict') {
      throw new Error(`Hark source id conflict for ${input.id}`);
    }
    return this.deliverOrReplay(reserved.entry, reserved.kind === 'existing');
  }

  private async deliverOrReplay(
    entry: HarkOutboxEntry,
    replay: boolean,
  ): Promise<HarkBridgeSubmission> {
    const current = entry.state === 'pending' ? await this.deliver(entry) : entry;
    return {
      sourceId: entry.kind === 'status'
        ? (entry.body as HarkPhoneDeliveryStatus).sourceEventId
        : (entry.body as HarkPhoneResponse).sourceResponseId,
      kind: entry.kind,
      state: current.state,
      replay,
      error: current.lastError,
      continuationResult: current.continuationResult,
    };
  }

  private async deliver(entry: HarkOutboxEntry): Promise<HarkOutboxEntry> {
    const active = this.inFlight.get(entry.id);
    if (active) return active;
    const delivery = this.performDelivery(entry).finally(() => {
      this.inFlight.delete(entry.id);
    });
    this.inFlight.set(entry.id, delivery);
    return delivery;
  }

  private async performDelivery(entry: HarkOutboxEntry): Promise<HarkOutboxEntry> {
    let deliverable = entry;
    if (entry.dependsOnId) {
      let dependency = this.store.getHarkOutbox(entry.dependsOnId);
      if (!dependency) {
        return this.store.markHarkOutboxRejected(
          entry.id,
          'Hark correction predecessor is missing from the durable outbox',
          this.now(),
        );
      }
      if (dependency.state === 'pending') {
        dependency = await this.deliver(dependency);
      }
      if (dependency.state === 'rejected') {
        return this.store.markHarkOutboxRejected(
          entry.id,
          'Hark correction predecessor was rejected and cannot be superseded',
          this.now(),
        );
      }
      if (dependency.state !== 'delivered') {
        const pending = this.store.markHarkOutboxRetry(
          entry.id,
          'Waiting for the superseded Hark response to be accepted',
        );
        this.scheduleRetry(Math.max(1, dependency.attempts));
        return pending;
      }
      try {
        deliverable = this.store.bindHarkCorrectionSupersedesResponse(
          entry.id,
          dependency.id,
          predecessorApiResponseId(dependency),
        );
      } catch (error) {
        return this.store.markHarkOutboxRejected(
          entry.id,
          error instanceof Error ? error.message : String(error),
          this.now(),
        );
      }
    }

    const attempted = this.store.markHarkOutboxAttempt(deliverable.id, this.now());
    const timestamp = String(Math.floor(this.now() / 1_000));
    const nonce = randomBytes(18).toString('base64url');
    const signature = createSignedAlertSignature(
      this.signingSecret,
      timestamp,
      nonce,
      attempted.body,
    );
    const url = attempted.kind === 'status'
      ? `${this.apiBaseUrl}/internal/hark/v1/deliveries/${encodeURIComponent(attempted.binding.deliveryId)}/status`
      : `${this.apiBaseUrl}/internal/hark/v1/cases/${encodeURIComponent(attempted.binding.caseId)}/phone-responses`;

    try {
      const response = await this.fetchImpl(url, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-dexter-alert-timestamp': timestamp,
          'x-dexter-alert-nonce': nonce,
          'x-dexter-alert-signature': signature,
        },
        body: JSON.stringify(attempted.body),
        signal: AbortSignal.timeout(this.requestTimeoutMs),
      });
      const responseBody = await response.json().catch(() => null);
      if (response.ok) {
        const result = sanitizeContinuationResult(
          responseBody,
          attempted.body,
          attempted.kind,
        );
        return this.store.markHarkOutboxDelivered(entry.id, result, this.now());
      }

      const error = `Hark callback returned HTTP ${response.status}`;
      // Callback authentication is service-to-service configuration. During a
      // secret rollout, 401/403 can become valid without changing this durable
      // body, so retain the row and let the bounded retry/backlog drain recover
      // it. Exact binding/schema failures remain terminal through their 4xx or
      // strict successful-response validation paths.
      if (
        response.status === 401
        || response.status === 403
        || response.status === 408
        || response.status === 429
        || response.status >= 500
      ) {
        const pending = this.store.markHarkOutboxRetry(entry.id, error);
        this.scheduleRetry(pending.attempts);
        return pending;
      }
      return this.store.markHarkOutboxRejected(entry.id, error, this.now());
    } catch (error) {
      if (error instanceof HarkBindingMismatchError) {
        return this.store.markHarkOutboxRejected(entry.id, error.message, this.now());
      }
      if (error instanceof HarkCallbackResultError) {
        const pending = this.store.markHarkOutboxRetry(entry.id, error.message);
        this.scheduleRetry(pending.attempts);
        return pending;
      }
      const pending = this.store.markHarkOutboxRetry(
        entry.id,
        error instanceof Error ? error.message : String(error),
      );
      this.scheduleRetry(pending.attempts);
      return pending;
    }
  }

  private scheduleRetry(attempts: number): void {
    if (!this.retryInProcess) return;
    if (this.retryTimer) return;
    const delayMs = Math.min(60_000, 1_000 * (2 ** Math.min(attempts, 6)));
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      void this.flushPending().catch((error: unknown) => {
        console.warn('[dexter-phone] Hark response retry failed:', String(error));
      });
    }, delayMs);
    this.retryTimer.unref?.();
  }
}
