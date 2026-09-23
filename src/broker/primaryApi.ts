import { createHmac } from 'node:crypto';
import { z } from 'zod';
import { phoneCallAssignmentSchema, phoneCallAssignmentRefSchema, sameCallAssignmentRef, type PhoneCallAssignment, type PhoneCallAssignmentRef } from './assignment.js';
export type { PhoneCallAssignment, PhoneCallAssignmentRef } from './assignment.js';
import { rawSubmissionDigest, turnBodyDigest, type PhoneNonAdmissionProof } from './reconciliation.js';
import { canonicalJson } from '../outbound/canonical.js';
import {
  inboundBeginIdentitySchema, inboundOutcomeSchema, recoverInboundResultSchema,
  consumeInboundApprovalSchema, closeConsumedInboundAttachmentSchema,
  consumedInboundClosureReceiptSchema, sameInboundBeginRef, sameInboundApprovalRef, inboundBeginRef,
  type InboundBeginIdentity, type InboundBeginRef, type InboundApprovalRef, type InboundOutcome,
  type RecoverInboundResult, type CloseConsumedInboundAttachment, type ConsumedInboundClosureReceipt,
} from './inboundContract.js';
export type { InboundBeginIdentity, InboundBeginRef, InboundApprovalRef, InboundOutcome,
  RecoverInboundResult, CloseConsumedInboundAttachment, ConsumedInboundClosureReceipt } from './inboundContract.js';

export interface PrimaryProviderProof {
  url: string;
  signature: string;
  params: Record<string, string>;
}

export interface PrimaryContextMessage {
  id: string;
  role: 'user' | 'assistant';
  text: string;
}

export interface PrimaryContext {
  primaryDexterId: string;
  threadId: string;
  displayName: string;
  messages: PrimaryContextMessage[];
}

export interface PrimaryCall {
  bindingId: string;
  callSid: string;
  state: 'open' | 'closed';
  context: PrimaryContext;
  callAssignmentRef?: PhoneCallAssignmentRef;
}

export interface PrimarySubmission {
  source: {
    callSid: string;
    itemId: string;
    sourceEventId: string;
    revision: 1;
    receivedAt: string;
    role: 'user';
  };
  text: string;
  relatedPrimaryRequestId?: string;
  referenceOrigin?: 'broker_status_publication';
  callAssignmentRef?: PhoneCallAssignmentRef;
}

export interface PrimaryTurn {
  sourceEventId: string;
  revision: number;
  itemId: string;
  role: 'user' | 'assistant';
  text: string;
  occurredAt: string;
}

export interface PrimaryApiOptions {
  apiBaseUrl: string;
  phoneServiceSecret: string;
  fetch?: typeof globalThis.fetch;
  timeoutMs?: number;
  maxBodyBytes?: number;
}

export type PrimaryReadinessReason = 'ready' | 'caller_unlinked'
  | 'caller_binding_ambiguous' | 'primary_unavailable'
  | 'authority_unavailable' | 'runtime_unavailable';

export interface PrimaryReadiness {
  eligible: boolean;
  reason: PrimaryReadinessReason;
}

export interface PrimaryAssistant {
  messageId: string;
  eventId: string;
  sequence: string;
  text: string;
}

export interface PrimaryQuestion {
  id: string; revision: 1; expiresAt: string;
  runState: 'queued' | 'running' | 'waiting_for_reply' | 'completed' | 'failed' | 'cancelled';
  runId: string; threadId: string; agent: { id: string; name: string };
  question: string; context: string; proposedAnswer: string; yesMeaning: string; noMeaning: string;
  state: 'pending' | 'answered' | 'expired' | 'cancelled' | 'unavailable'; createdAt: string;
  reply: (PrimaryReply & { interpretedAnswer: string; receivedAt: string }) | null;
}
export interface PrimaryReply {
  questionRevision: 1; clientReplyId: string; answer: 'yes' | 'no' | 'other'; text: string | null;
}
export interface PrimaryReplyInput {
  source: Omit<PrimarySubmission['source'], 'role'>;
  role: 'user'; text: string; reply: PrimaryReply;
}
export interface PrimaryQuestionResult {
  requestId: string; bindingId: string; state: PrimaryQuestion['runState']; question: PrimaryQuestion; replayed?: boolean;
}

export type PrimaryRequestResult = {
  requestId: string;
  bindingId: string;
  replayed?: boolean;
  question?: PrimaryQuestion;
} & (
  | { state: 'queued' | 'running'; assistant?: never; error?: never }
  | { state: 'completed'; assistant: PrimaryAssistant; error?: never }
  | { state: 'failed' | 'cancelled'; assistant?: never; error: { code: string; retryable: false } }
  | { state: 'waiting_for_reply'; question: PrimaryQuestion; assistant?: never; error?: never }
);

export type PrimaryResult = PrimaryRequestResult;

export interface PrimaryTurnReceipt {
  bindingId: string;
  sourceEventId: string;
  revision: number;
  recordId: string;
  replayed: boolean;
  revisionRecordId?: string;
  bodyDigest?: string;
}

export type PrimaryReconciliation =
  | { outcome: 'admitted'; requestId: string; bindingId: string; canonicalRequestId: string; result: PrimaryResult }
  | { outcome: 'not_admitted'; requestId: string; bindingId: string; proof: PhoneNonAdmissionProof };

export interface PrimaryClosedCall {
  bindingId: string;
  callSid: string;
  state: 'closed';
}

export interface PrimaryApiClient {
  readiness(callerNumber: string): Promise<PrimaryReadiness>;
  openCall(providerProof: PrimaryProviderProof, callAssignment?: PhoneCallAssignment): Promise<PrimaryCall>;
  getContext(bindingId: string): Promise<PrimaryCall>;
  submit(bindingId: string, requestId: string, input: PrimarySubmission): Promise<PrimaryRequestResult>;
  result(bindingId: string, requestId: string): Promise<PrimaryRequestResult>;
  reconcile(bindingId: string, requestId: string, input: PrimarySubmission): Promise<PrimaryReconciliation>;
  question(bindingId: string, requestId: string, questionId: string): Promise<PrimaryQuestionResult>;
  reply(bindingId: string, requestId: string, questionId: string, input: PrimaryReplyInput): Promise<PrimaryQuestionResult>;
  captureTurn(bindingId: string, input: PrimaryTurn): Promise<PrimaryTurnReceipt>;
  close(bindingId: string): Promise<PrimaryClosedCall>;
  beginInbound?(begin: InboundBeginIdentity): Promise<InboundOutcome>;
  recoverInbound?(begin: InboundBeginIdentity): Promise<RecoverInboundResult>;
  cancelInbound?(begin: InboundBeginIdentity): Promise<InboundOutcome>;
  inboundStatus?(attachmentId: string): Promise<InboundOutcome>;
  closeInbound?(attachmentId: string, input: CloseConsumedInboundAttachment): Promise<ConsumedInboundClosureReceipt>;
  openInboundCall?(providerProof: PrimaryProviderProof, approvalRef: InboundApprovalRef): Promise<PrimaryCall>;
}

export type PrimaryApiErrorCode =
  | 'invalid_configuration' | 'invalid_request' | 'request_timeout'
  | 'request_failed' | 'http_error' | 'response_too_large'
  | 'invalid_response' | 'identity_mismatch';

/** Never carries raw provider errors, request bodies, bearer tokens or URLs. */
export class PrimaryApiError extends Error {
  readonly remoteCode?: string;
  readonly retryable?: boolean;
  constructor(readonly code: PrimaryApiErrorCode, readonly status?: number,
    remote?: { code: string; retryable: boolean }) {
    super(`Primary API request failed: ${code}${status === undefined ? '' : ` (HTTP ${status})`}`);
    this.name = 'PrimaryApiError';
    this.remoteCode = remote?.code;
    this.retryable = remote?.retryable;
  }
}

const PREFIX = '/internal/primary-dexter/channels/phone';
const MAX_TIMEOUT_MS = 30_000;
const MAX_BODY_BYTES = 64 * 1024;
const MAX_REQUEST_BYTES = 24 * 1024;
const MAX_HARK_CALL_REQUEST_BYTES = 48 * 1024;

function createTransport(options: PrimaryApiOptions) {
  let base: URL;
  try { base = new URL(options.apiBaseUrl); }
  catch { throw new PrimaryApiError('invalid_configuration'); }
  const timeoutMs = options.timeoutMs ?? 10_000;
  const maxBodyBytes = options.maxBodyBytes ?? MAX_BODY_BYTES;
  if (!['https:', 'http:'].includes(base.protocol) || base.username || base.password
    || base.search || base.hash
    || (base.protocol === 'http:' && !['localhost', '127.0.0.1', '[::1]'].includes(base.hostname))
    || typeof options.phoneServiceSecret !== 'string' || options.phoneServiceSecret.length < 32
    || !Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > MAX_TIMEOUT_MS
    || !Number.isInteger(maxBodyBytes) || maxBodyBytes < 1 || maxBodyBytes > MAX_BODY_BYTES) {
    throw new PrimaryApiError('invalid_configuration');
  }
  const baseUrl = base.href.replace(/\/+$/, '') + PREFIX;
  const bearer = createHmac('sha256', options.phoneServiceSecret)
    .update('dexter-broker/primary-ingress/v1').digest('hex');
  const fetcher = options.fetch ?? globalThis.fetch;

  return async (method: 'GET' | 'POST', path: string,
    body?: unknown, headers?: Record<string, string>, expectedStatus = 200, harkCallAssignment = false): Promise<unknown> => {
    let serialized: string | undefined;
    if (body !== undefined) {
      try { serialized = JSON.stringify(body); }
      catch { throw new PrimaryApiError('invalid_request'); }
      const maxRequestBytes = method === 'POST' && path === '/calls' && harkCallAssignment
        ? MAX_HARK_CALL_REQUEST_BYTES : MAX_REQUEST_BYTES;
      if (serialized === undefined || Buffer.byteLength(serialized) > maxRequestBytes) {
        throw new PrimaryApiError('invalid_request');
      }
    }
    const controller = new AbortController();
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    let timedOut = false;
    let timer: ReturnType<typeof setTimeout>;
    const deadline = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => {
        timedOut = true;
        controller.abort();
        void reader?.cancel().catch(() => {});
        reject(new PrimaryApiError('request_timeout'));
      }, timeoutMs);
    });
    const request = async () => {
      // One attempt. In particular, an uncertain POST must be recovered by its
      // existing identity, never repeated by a generic transport retry.
      const response = await fetcher(baseUrl + path, {
        method, redirect: 'error', signal: controller.signal,
        headers: {
          Accept: 'application/json', Authorization: `Bearer ${bearer}`,
          ...(serialized === undefined ? {} : { 'Content-Type': 'application/json' }),
          ...headers,
        },
        ...(serialized === undefined ? {} : { body: serialized }),
      });
      if (response.status >= 300 && response.status < 400) {
        void response.body?.cancel().catch(() => {});
        throw new PrimaryApiError('http_error', response.status);
      }
      const length = response.headers.get('content-length');
      if (length && /^\d+$/.test(length) && Number(length) > maxBodyBytes) {
        void response.body?.cancel().catch(() => {});
        throw new PrimaryApiError('response_too_large', response.status);
      }
      const chunks: Uint8Array[] = [];
      let size = 0;
      if (response.body) {
        reader = response.body.getReader();
        try {
          for (;;) {
            const chunk = await reader.read();
            if (chunk.done) break;
            size += chunk.value.byteLength;
            if (size > maxBodyBytes) {
              void reader.cancel().catch(() => {});
              throw new PrimaryApiError('response_too_large', response.status);
            }
            chunks.push(chunk.value);
          }
        } finally { reader.releaseLock(); reader = undefined; }
      }
      let decoded: unknown;
      try {
        const text = new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks));
        decoded = size ? JSON.parse(text) : undefined;
      } catch {
        throw new PrimaryApiError(response.ok ? 'invalid_response' : 'http_error', response.status);
      }
      if (!response.ok) {
        const remote = errorEnvelope.safeParse(decoded);
        const safeRemote = remote.success
          && !remote.data.error.code.includes(bearer)
          && !remote.data.error.code.includes(options.phoneServiceSecret)
          ? remote.data.error : undefined;
        throw new PrimaryApiError('http_error', response.status, safeRemote);
      }
      if (response.status !== expectedStatus) throw new PrimaryApiError('invalid_response', response.status);
      return decoded;
    };
    try { return await Promise.race([request(), deadline]); }
    catch (error) {
      if (timedOut) throw new PrimaryApiError('request_timeout');
      if (error instanceof PrimaryApiError) throw error;
      throw new PrimaryApiError('request_failed');
    } finally { clearTimeout(timer!); }
  };
}

const uuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
const callSid = z.string().regex(/^CA[0-9a-fA-F]{32}$/);
const itemId = z.string().regex(/^[a-zA-Z0-9_-]{1,128}$/);
const textValue = z.string().min(1).max(MAX_BODY_BYTES);
const sourceText = z.string().refine((value) => {
  if (!value.trim() || value.includes('\0')) return false;
  const scalars = Array.from(value);
  return scalars.length <= 4000 && scalars.every((character) => {
    const point = character.codePointAt(0)!;
    return point < 0xd800 || point > 0xdfff;
  });
});
const timestamp = z.iso.datetime({ offset: true });
const revision = z.number().int().min(1).max(2147483647);
const errorEnvelope = z.object({ error: z.object({
  code: z.string().regex(/^[a-z][a-z0-9_]{1,79}$/), retryable: z.boolean(),
}) });
const contextSchema = z.object({
  primaryDexterId: uuid, threadId: uuid, displayName: z.string().min(1).max(200),
  messages: z.array(z.object({ id: uuid, role: z.enum(['user', 'assistant']), text: textValue })).max(20),
}).refine((value) => value.messages.reduce((size, message) => size + Buffer.byteLength(message.text), 0) <= MAX_REQUEST_BYTES);
const callSchema = z.object({
  bindingId: uuid, callSid, state: z.enum(['open', 'closed']), context: contextSchema, callAssignmentRef: phoneCallAssignmentRefSchema.optional(),
});
const inboundCallSchema = z.object({
  bindingId: uuid, callSid, state: z.enum(['open', 'closed']),
  context: z.object({
    primaryDexterId: uuid, threadId: uuid, displayName: z.string().min(1).max(200),
    messages: z.array(z.object({ id: uuid, role: z.enum(['user', 'assistant']), text: textValue }).strict()).max(20),
  }).strict().refine(value => value.messages.reduce((size, message) => size + Buffer.byteLength(message.text), 0) <= MAX_REQUEST_BYTES),
}).strict();
const questionText = (limit: number) => z.string().min(1).refine(value => value.trim() === value
  && Buffer.byteLength(value) <= limit && !/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/u.test(value)
  && Array.from(value).every(c => c.codePointAt(0)! < 0xd800 || c.codePointAt(0)! > 0xdfff));
const replySchema = z.object({ questionRevision: z.literal(1), clientReplyId: uuid,
  answer: z.enum(['yes', 'no', 'other']), text: questionText(2000).nullable(),
}).strict().refine(value => (value.answer === 'other') === (value.text !== null));
const questionSchema = z.object({
  id: uuid, revision: z.literal(1), expiresAt: timestamp,
  runState: z.enum(['queued', 'running', 'waiting_for_reply', 'completed', 'failed', 'cancelled']),
  runId: uuid, threadId: uuid, agent: z.object({ id: uuid, name: questionText(512) }),
  question: questionText(400), context: questionText(1000), proposedAnswer: questionText(400),
  yesMeaning: questionText(400), noMeaning: questionText(400),
  state: z.enum(['pending', 'answered', 'expired', 'cancelled', 'unavailable']), createdAt: timestamp,
  reply: z.object({ questionRevision: z.literal(1), clientReplyId: uuid, answer: z.enum(['yes', 'no', 'other']),
    text: questionText(2000).nullable(), interpretedAnswer: questionText(2000), receivedAt: timestamp,
  }).refine(value => (value.answer === 'other') === (value.text !== null)).nullable(),
}).refine(value => (value.state === 'answered') === (value.reply !== null));
const requestBase = z.object({ requestId: uuid, bindingId: uuid, replayed: z.boolean().optional(), question: questionSchema.optional() });
const requestSchema = z.discriminatedUnion('state', [
  requestBase.extend({ state: z.enum(['queued', 'running']), assistant: z.never().optional(), error: z.never().optional() }),
  requestBase.extend({ state: z.literal('completed'), error: z.never().optional(), assistant: z.object({
    messageId: uuid, eventId: uuid, sequence: z.string().regex(/^[1-9][0-9]{0,39}$/), text: textValue,
  }) }),
  requestBase.extend({ state: z.enum(['failed', 'cancelled']), assistant: z.never().optional(), error: errorEnvelope.shape.error.extend({ retryable: z.literal(false) }) }),
  requestBase.extend({ state: z.literal('waiting_for_reply'), question: questionSchema,
    assistant: z.never().optional(), error: z.never().optional() }),
]);
const questionResultSchema = z.object({ requestId: uuid, bindingId: uuid,
  state: z.enum(['queued', 'running', 'waiting_for_reply', 'completed', 'failed', 'cancelled']),
  question: questionSchema, replayed: z.boolean().optional(),
});
const replyInputSchema = z.object({
  source: z.object({ callSid, itemId, sourceEventId: uuid, revision: z.literal(1), receivedAt: timestamp }).strict(),
  role: z.literal('user'), text: sourceText, reply: replySchema,
}).strict();
const submissionSchema = z.object({
  source: z.object({ callSid, itemId, sourceEventId: uuid, revision: z.literal(1), receivedAt: timestamp, role: z.literal('user') }).strict(),
  text: sourceText,
  relatedPrimaryRequestId: uuid.optional(), referenceOrigin: z.literal('broker_status_publication').optional(),
  callAssignmentRef: phoneCallAssignmentRefSchema.optional(),
}).strict().refine(value => (value.relatedPrimaryRequestId === undefined) === (value.referenceOrigin === undefined));
const turnSchema = z.object({
  sourceEventId: uuid, revision, itemId, role: z.enum(['user', 'assistant']), text: sourceText, occurredAt: timestamp,
}).strict();
const sha256 = z.string().regex(/^[0-9a-f]{64}$/);
const turnReceiptSchema = z.object({ bindingId: uuid, sourceEventId: uuid, revision, recordId: uuid, replayed: z.boolean(),
  revisionRecordId: uuid.optional(), bodyDigest: sha256.optional(),
}).refine(value => (value.revisionRecordId === undefined) === (value.bodyDigest === undefined));
const reconciliationSchema = z.discriminatedUnion('outcome', [
  z.object({ outcome: z.literal('admitted'), requestId: uuid, bindingId: uuid, canonicalRequestId: uuid, result: requestSchema }).strict(),
  z.object({ outcome: z.literal('not_admitted'), requestId: uuid, bindingId: uuid, proof: z.object({
    schemaVersion: z.literal(1), kind: z.literal('phone_closed_call_non_admission'), callSid,
    source: z.object({ itemId, sourceEventId: uuid, revision: z.literal(1), receivedAt: timestamp }).strict(),
    rawSubmissionDigest: sha256,
    turn: z.object({ recordId: uuid, revisionRecordId: uuid, bodyDigest: sha256 }).strict(), closedAt: timestamp,
  }).strict() }).strict(),
]);
const closedSchema = z.object({ bindingId: uuid, callSid, state: z.literal('closed') });
const readinessSchema = z.object({
  eligible: z.boolean(),
  reason: z.enum(['ready', 'caller_unlinked', 'caller_binding_ambiguous', 'primary_unavailable', 'authority_unavailable', 'runtime_unavailable']),
}).refine((value) => value.eligible === (value.reason === 'ready'));

function parse<T>(schema: z.ZodType<T>, value: unknown, code: 'invalid_request' | 'invalid_response'): T {
  const result = schema.safeParse(value);
  if (!result.success) throw new PrimaryApiError(code);
  return result.data;
}

function same(actual: string | number, expected: string | number) {
  if (actual !== expected) throw new PrimaryApiError('identity_mismatch');
}

export function createPrimaryApiClient(options: PrimaryApiOptions): PrimaryApiClient {
  const request = createTransport(options);
  // Known bindings add a second consistency check across open/context/submit/
  // close. Recovery in a fresh process still validates exact wire identities.
  const calls = new Map<string, {
    callSid: string; primaryDexterId: string; threadId: string; state: 'open' | 'closed'; callAssignmentRef?: PhoneCallAssignmentRef;
  }>();
  type InboundMemory = {
    beginRef?: InboundBeginRef; approvalRef?: InboundApprovalRef; callSid: string;
    bindingId?: string; state?: InboundOutcome['state']; expiresAt?: string | null;
    consumedAt?: string; cancelledAt?: string; callState?: 'open' | 'closed'; closedAt?: string;
  };
  const inboundCalls = new Map<string, InboundMemory>();
  const inboundBegins = new Map<string, { body: string; callSid: string; beginRef?: InboundBeginRef; attachmentId?: string }>();
  const inboundBySid = new Map<string, string>();
  const bindingHeaders = (bindingId: string) => {
    parse(uuid, bindingId, 'invalid_request');
    return { 'X-Dexter-Binding-Id': bindingId };
  };
  const remember = (call: PrimaryCall) => {
    const existing = calls.get(call.bindingId);
    if (existing) {
      same(call.callSid, existing.callSid);
      same(call.context.primaryDexterId, existing.primaryDexterId);
      same(call.context.threadId, existing.threadId);
      if (call.callAssignmentRef !== undefined && !sameCallAssignmentRef(call.callAssignmentRef, existing.callAssignmentRef))
        throw new PrimaryApiError('identity_mismatch');
      if (existing.state === 'closed' && call.state !== 'closed') throw new PrimaryApiError('identity_mismatch');
    }
    // Context does not need to repeat the registration receipt. Omission may
    // not erase the validated local receipt for this exact binding.
    const reference = call.callAssignmentRef ?? existing?.callAssignmentRef;
    calls.set(call.bindingId, {
      callSid: call.callSid, primaryDexterId: call.context.primaryDexterId,
      threadId: call.context.threadId, state: call.state,
      ...(reference === undefined ? {} : { callAssignmentRef: { ...reference } }),
    });
    return reference === undefined ? call : { ...call, callAssignmentRef: { ...reference } };
  };
  const requireSameApproval = (left: InboundApprovalRef, right: InboundApprovalRef) => {
    if (!sameInboundApprovalRef(left, right)) throw new PrimaryApiError('identity_mismatch');
  };
  const checkInboundTransport = (proof: PrimaryProviderProof) => {
    const url = new URL(proof.url);
    if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash || !proof.signature)
      throw new PrimaryApiError('invalid_request');
  };
  const checkedBegin = (input: InboundBeginIdentity) => {
    const begin = parse(inboundBeginIdentitySchema, input, 'invalid_request');
    checkInboundTransport(begin.providerProof);
    const body = canonicalJson(begin);
    const known = inboundBegins.get(begin.brokerCallId);
    if (known && known.body !== body) throw new PrimaryApiError('identity_mismatch');
    if (!known) inboundBegins.set(begin.brokerCallId, { body, callSid: begin.providerProof.params.CallSid!, beginRef: inboundBeginRef(begin) });
    return begin;
  };
  const checkBeginRef = (ref: InboundBeginRef, begin?: InboundBeginIdentity) => {
    if (begin) {
      same(ref.brokerCallId, begin.brokerCallId);
      same(ref.callSid, begin.providerProof.params.CallSid!);
      same(ref.digest, inboundBeginRef(begin).digest);
    }
    const known = inboundBegins.get(ref.brokerCallId);
    if (known) {
      same(ref.callSid, known.callSid);
      if (known.beginRef && !sameInboundBeginRef(ref, known.beginRef)) throw new PrimaryApiError('identity_mismatch');
    }
  };
  const rememberInbound = (outcome: InboundOutcome, begin?: InboundBeginIdentity) => {
    checkBeginRef(outcome.beginRef, begin);
    const previous = inboundCalls.get(outcome.attachmentId);
    if (previous) {
      same(outcome.beginRef.callSid, previous.callSid);
      if (previous.beginRef && !sameInboundBeginRef(outcome.beginRef, previous.beginRef)) throw new PrimaryApiError('identity_mismatch');
      if (previous.approvalRef && 'approvalRef' in outcome) requireSameApproval(outcome.approvalRef, previous.approvalRef);
      if (previous.bindingId && (outcome.state !== 'bound' || outcome.bindingId !== previous.bindingId))
        throw new PrimaryApiError('identity_mismatch');
      if (previous.state === 'approved' && outcome.state === 'pending') throw new PrimaryApiError('identity_mismatch');
      if ((previous.state === 'expired' || previous.state === 'cancelled') && previous.state !== outcome.state)
        throw new PrimaryApiError('identity_mismatch');
      if ('expiresAt' in outcome && previous.expiresAt !== undefined && outcome.expiresAt !== previous.expiresAt)
        throw new PrimaryApiError('identity_mismatch');
      if (outcome.state === 'bound') {
        if (previous.consumedAt !== undefined) same(outcome.consumedAt, previous.consumedAt);
        if (previous.callState === 'closed' && outcome.callState !== 'closed') throw new PrimaryApiError('identity_mismatch');
      }
      if (outcome.state === 'cancelled' && previous.cancelledAt !== undefined) same(outcome.cancelledAt, previous.cancelledAt);
    }
    const previousId = inboundBySid.get(outcome.beginRef.callSid);
    if (previousId !== undefined) same(outcome.attachmentId, previousId);
    const beginMemory = inboundBegins.get(outcome.beginRef.brokerCallId);
    if (beginMemory?.attachmentId !== undefined) same(outcome.attachmentId, beginMemory.attachmentId);
    if (outcome.state === 'bound') {
      const contextual = calls.get(outcome.bindingId);
      if (contextual) {
        same(outcome.beginRef.callSid, contextual.callSid);
        if (contextual.callAssignmentRef || (contextual.state === 'closed' && outcome.callState !== 'closed'))
          throw new PrimaryApiError('identity_mismatch');
      }
    }
    const next: InboundMemory = { ...previous, callSid: outcome.beginRef.callSid, beginRef: outcome.beginRef, state: outcome.state };
    if ('approvalRef' in outcome) next.approvalRef = outcome.approvalRef;
    if ('expiresAt' in outcome) next.expiresAt = outcome.expiresAt;
    if (outcome.state === 'bound') Object.assign(next, { bindingId: outcome.bindingId, consumedAt: outcome.consumedAt, callState: outcome.callState });
    if (outcome.state === 'cancelled') next.cancelledAt = outcome.cancelledAt;
    inboundCalls.set(outcome.attachmentId, structuredClone(next));
    inboundBySid.set(outcome.beginRef.callSid, outcome.attachmentId);
    if (beginMemory) inboundBegins.set(outcome.beginRef.brokerCallId, { ...beginMemory, beginRef: structuredClone(outcome.beginRef), attachmentId: outcome.attachmentId });
    return outcome;
  };
  const decodeResult = (value: unknown, bindingId: string, requestId: string, submit: boolean) => {
    const result = parse(requestSchema, value, 'invalid_response');
    same(result.bindingId, bindingId); same(result.requestId, requestId);
    if (submit && typeof result.replayed !== 'boolean') throw new PrimaryApiError('invalid_response');
    if (result.state === 'waiting_for_reply' && (!['pending', 'expired'].includes(result.question.state)
      || result.question.runState !== 'waiting_for_reply')) throw new PrimaryApiError('invalid_response');
    if (result.question) {
      const known = calls.get(bindingId);
      if (known) { same(result.question.threadId, known.threadId); same(result.question.agent.id, known.primaryDexterId); }
      same(result.question.runState, result.state);
    }
    return result;
  };
  const decodeQuestion = (value: unknown, bindingId: string, requestId: string, questionId: string) => {
    const result = parse(questionResultSchema, value, 'invalid_response');
    same(result.bindingId, bindingId); same(result.requestId, requestId); same(result.question.id, questionId);
    same(result.question.runState, result.state);
    const known = calls.get(bindingId);
    if (known) { same(result.question.threadId, known.threadId); same(result.question.agent.id, known.primaryDexterId); }
    return result;
  };
  return {
    async beginInbound(input) {
      const begin = checkedBegin(input);
      return rememberInbound(parse(inboundOutcomeSchema,
        await request('POST', '/inbound-attachments', begin), 'invalid_response'), begin);
    },
    async recoverInbound(input) {
      const begin = checkedBegin(input);
      const outcome = parse(recoverInboundResultSchema,
        await request('POST', '/inbound-attachments/recover', { begin }), 'invalid_response');
      if (outcome.state !== 'absent') return rememberInbound(outcome, begin);
      checkBeginRef(outcome.beginRef, begin);
      if (inboundBegins.get(begin.brokerCallId)?.attachmentId !== undefined
        || inboundBySid.has(outcome.beginRef.callSid)) throw new PrimaryApiError('identity_mismatch');
      return outcome;
    },
    async cancelInbound(input) {
      const begin = checkedBegin(input);
      const outcome = parse(inboundOutcomeSchema,
        await request('POST', '/inbound-attachments/cancel', { begin }), 'invalid_response');
      if (outcome.state === 'pending' || outcome.state === 'approved') throw new PrimaryApiError('invalid_response');
      return rememberInbound(outcome, begin);
    },
    async inboundStatus(attachmentId) {
      parse(uuid, attachmentId, 'invalid_request');
      const outcome = parse(inboundOutcomeSchema,
        await request('GET', `/inbound-attachments/${attachmentId}`), 'invalid_response');
      same(outcome.attachmentId, attachmentId);
      return rememberInbound(outcome);
    },
    async openInboundCall(providerProof, approvalRef) {
      const input = parse(consumeInboundApprovalSchema, { providerProof, inboundApprovalRef: approvalRef }, 'invalid_request');
      checkInboundTransport(input.providerProof);
      const sid = input.providerProof.params.CallSid!;
      const validateKnown = () => {
        const known = inboundCalls.get(input.inboundApprovalRef.attachmentId);
        if (known) {
          same(known.callSid, sid);
          if (known.approvalRef) requireSameApproval(input.inboundApprovalRef, known.approvalRef);
          if (known.state === 'expired' || known.state === 'cancelled') throw new PrimaryApiError('identity_mismatch');
          if (known.beginRef) {
            const begin = checkedBegin({ brokerCallId: known.beginRef.brokerCallId, providerProof: input.providerProof });
            checkBeginRef(known.beginRef, begin);
          }
        }
        const previousId = inboundBySid.get(sid);
        if (previousId !== undefined) same(previousId, input.inboundApprovalRef.attachmentId);
        return known;
      };
      validateKnown();
      const call = parse(inboundCallSchema, await request('POST', '/calls', input), 'invalid_response');
      const known = validateKnown();
      same(call.callSid, sid);
      if (known?.bindingId !== undefined) same(call.bindingId, known.bindingId);
      if (known?.callState === 'closed' && call.state !== 'closed') throw new PrimaryApiError('identity_mismatch');
      if (calls.get(call.bindingId)?.callAssignmentRef) throw new PrimaryApiError('identity_mismatch');
      const result = remember(call);
      inboundCalls.set(input.inboundApprovalRef.attachmentId, structuredClone({ ...known,
        callSid: sid, approvalRef: input.inboundApprovalRef, bindingId: call.bindingId, callState: call.state }));
      inboundBySid.set(sid, input.inboundApprovalRef.attachmentId);
      // The private context response has no consumedAt. Recover its separate
      // receipt before deriving any media-start deadline.
      return result;
    },
    async closeInbound(attachmentId, value) {
      parse(uuid, attachmentId, 'invalid_request');
      const input = parse(closeConsumedInboundAttachmentSchema, value, 'invalid_request');
      same(attachmentId, input.approvalRef.attachmentId);
      const begin = checkedBegin(input.begin);
      const known = inboundCalls.get(attachmentId);
      const sid = begin.providerProof.params.CallSid!;
      if (known) {
        same(known.callSid, sid);
        if (known.beginRef) checkBeginRef(known.beginRef, begin);
        if (known.approvalRef) requireSameApproval(input.approvalRef, known.approvalRef);
        if (known.bindingId !== undefined) same(input.bindingId, known.bindingId);
        if (known.state === 'expired' || known.state === 'cancelled') throw new PrimaryApiError('identity_mismatch');
      }
      const previousId = inboundBySid.get(sid);
      if (previousId !== undefined) same(attachmentId, previousId);
      const beginMemory = inboundBegins.get(begin.brokerCallId)!;
      if (beginMemory.attachmentId !== undefined) same(attachmentId, beginMemory.attachmentId);
      const contextual = calls.get(input.bindingId);
      if (contextual) {
        same(contextual.callSid, sid);
        if (contextual.callAssignmentRef) throw new PrimaryApiError('identity_mismatch');
      }
      const receipt = parse(consumedInboundClosureReceiptSchema,
        await request('POST', `/inbound-attachments/${attachmentId}/close`, { ...input, begin }), 'invalid_response');
      same(receipt.attachmentId, attachmentId); same(receipt.bindingId, input.bindingId); same(receipt.callSid, sid);
      checkBeginRef(receipt.beginRef, begin); requireSameApproval(receipt.approvalRef, input.approvalRef);
      // Another exact operation may have completed while HTTP was pending.
      const latest = inboundCalls.get(attachmentId);
      if (latest) {
        same(latest.callSid, sid);
        if (latest.beginRef && !sameInboundBeginRef(latest.beginRef, receipt.beginRef)) throw new PrimaryApiError('identity_mismatch');
        if (latest.approvalRef) requireSameApproval(latest.approvalRef, receipt.approvalRef);
        if (latest.bindingId !== undefined) same(latest.bindingId, receipt.bindingId);
        if (latest.closedAt !== undefined) same(receipt.closedAt, latest.closedAt);
        if (latest.state === 'expired' || latest.state === 'cancelled') throw new PrimaryApiError('identity_mismatch');
      }
      const latestContext = calls.get(input.bindingId);
      if (latestContext) {
        same(latestContext.callSid, sid);
        if (latestContext.callAssignmentRef) throw new PrimaryApiError('identity_mismatch');
      }
      const latestId = inboundBySid.get(sid);
      if (latestId !== undefined) same(attachmentId, latestId);
      const latestBegin = inboundBegins.get(begin.brokerCallId)!;
      if (latestBegin.attachmentId !== undefined) same(attachmentId, latestBegin.attachmentId);
      if (latestContext) calls.set(input.bindingId, { ...latestContext, state: 'closed' });
      inboundCalls.set(attachmentId, structuredClone({ ...latest, beginRef: receipt.beginRef,
        approvalRef: receipt.approvalRef, callSid: sid, bindingId: receipt.bindingId,
        state: 'bound', callState: 'closed', closedAt: receipt.closedAt }));
      inboundBySid.set(sid, attachmentId);
      inboundBegins.set(begin.brokerCallId, { ...latestBegin, attachmentId });
      return receipt;
    },
    async readiness(callerNumber) {
      parse(z.string().regex(/^\+[1-9]\d{7,14}$/), callerNumber, 'invalid_request');
      return parse(readinessSchema, await request('POST', '/readiness', { callerNumber }), 'invalid_response');
    },
    async openCall(providerProof, callAssignment) {
      const assignment = callAssignment === undefined ? undefined : parse(phoneCallAssignmentSchema, callAssignment, 'invalid_request');
      const proof = parse(z.object({
        url: z.string().url().max(2048), signature: z.string().min(1).max(512),
        params: z.record(z.string().min(1).max(128), z.string().max(2048))
          .refine((value) => Object.keys(value).length <= 80),
      }).strict(), providerProof, 'invalid_request');
      const proofUrl = new URL(proof.url);
      if (proofUrl.protocol !== 'https:' || proofUrl.username || proofUrl.password || proofUrl.hash) {
        throw new PrimaryApiError('invalid_request');
      }
      const expectedCallSid = parse(callSid, proof.params.CallSid, 'invalid_request');
      for (const key of ['AccountSid', 'Direction', 'From', 'To']) {
        if (!proof.params[key]) throw new PrimaryApiError('invalid_request');
      }
      const call = parse(callSchema, await request('POST', '/calls', { providerProof: proof,
        ...(assignment === undefined ? {} : { callAssignment: assignment }) }, undefined, 200,
      assignment !== undefined && 'hark' in assignment), 'invalid_response');
      if ((assignment === undefined) !== (call.callAssignmentRef === undefined)) throw new PrimaryApiError('identity_mismatch');
      if (assignment && call.callAssignmentRef) {
        same(call.callAssignmentRef.assignmentId, assignment.assignmentId); same(call.callAssignmentRef.revision, assignment.revision);
      }
      same(call.callSid, expectedCallSid);
      return remember(call);
    },
    async getContext(bindingId) {
      const call = parse(callSchema, await request('GET', `/calls/${bindingId}/context`, undefined, bindingHeaders(bindingId)), 'invalid_response');
      same(call.bindingId, bindingId);
      return remember(call);
    },
    async submit(bindingId, requestId, input) {
      const headers = bindingHeaders(bindingId);
      parse(uuid, requestId, 'invalid_request');
      const parsed = parse(submissionSchema, input, 'invalid_request');
      if (parsed.relatedPrimaryRequestId === requestId) throw new PrimaryApiError('invalid_request');
      const reference = parsed.relatedPrimaryRequestId === undefined ? {} : {
        relatedPrimaryRequestId: parsed.relatedPrimaryRequestId, referenceOrigin: parsed.referenceOrigin };
      const existing = calls.get(bindingId);
      if (existing) {
        same(parsed.source.callSid, existing.callSid);
        if (!sameCallAssignmentRef(parsed.callAssignmentRef, existing.callAssignmentRef)) throw new PrimaryApiError('identity_mismatch');
      }
      const { role, ...source } = parsed.source;
      return decodeResult(await request('POST', '/requests', { bindingId, source, role, text: parsed.text, ...reference,
        ...(parsed.callAssignmentRef === undefined ? {} : { callAssignmentRef: parsed.callAssignmentRef }) },
        { ...headers, 'Idempotency-Key': requestId }, 202), bindingId, requestId, true);
    },
    async result(bindingId, requestId) {
      const headers = bindingHeaders(bindingId);
      parse(uuid, requestId, 'invalid_request');
      return decodeResult(await request('GET', `/requests/${requestId}`, undefined, headers), bindingId, requestId, false);
    },
    async reconcile(bindingId, requestId, input) {
      const headers = bindingHeaders(bindingId);
      parse(uuid, requestId, 'invalid_request');
      const parsed = parse(submissionSchema, input, 'invalid_request');
      if (parsed.relatedPrimaryRequestId === requestId) throw new PrimaryApiError('invalid_request');
      const reference = parsed.relatedPrimaryRequestId === undefined ? {} : {
        relatedPrimaryRequestId: parsed.relatedPrimaryRequestId, referenceOrigin: parsed.referenceOrigin };
      const known = calls.get(bindingId);
      if (known) {
        same(parsed.source.callSid, known.callSid);
        if (!sameCallAssignmentRef(parsed.callAssignmentRef, known.callAssignmentRef)) throw new PrimaryApiError('identity_mismatch');
      }
      const { role, ...source } = parsed.source;
      const result = parse(reconciliationSchema, await request('POST', `/requests/${requestId}/reconcile`,
        { bindingId, role, text: parsed.text, source, ...reference,
          ...(parsed.callAssignmentRef === undefined ? {} : { callAssignmentRef: parsed.callAssignmentRef }) }, headers), 'invalid_response');
      same(result.bindingId, bindingId); same(result.requestId, requestId);
      if (result.outcome === 'admitted') {
        return { ...result, result: decodeResult(result.result, bindingId, result.canonicalRequestId, false) };
      }
      same(result.proof.callSid, parsed.source.callSid);
      for (const key of ['itemId', 'sourceEventId', 'revision', 'receivedAt'] as const) same(result.proof.source[key], parsed.source[key]);
      same(result.proof.rawSubmissionDigest, rawSubmissionDigest(bindingId, parsed));
      same(result.proof.turn.recordId, parsed.source.sourceEventId);
      same(result.proof.turn.bodyDigest, turnBodyDigest({ sourceEventId: source.sourceEventId, revision: 1,
        itemId: source.itemId, role, text: parsed.text, occurredAt: source.receivedAt }));
      if (new Date(result.proof.closedAt).toISOString() !== result.proof.closedAt) throw new PrimaryApiError('invalid_response');
      return result;
    },
    async question(bindingId, requestId, questionId) {
      const headers = bindingHeaders(bindingId);
      parse(uuid, requestId, 'invalid_request'); parse(uuid, questionId, 'invalid_request');
      return decodeQuestion(await request('GET', `/requests/${requestId}/questions/${questionId}`, undefined, headers), bindingId, requestId, questionId);
    },
    async reply(bindingId, requestId, questionId, input) {
      const headers = bindingHeaders(bindingId);
      parse(uuid, requestId, 'invalid_request'); parse(uuid, questionId, 'invalid_request');
      const parsed = parse(replyInputSchema, input, 'invalid_request');
      const known = calls.get(bindingId);
      if (known) same(parsed.source.callSid, known.callSid);
      const result = decodeQuestion(await request('POST', `/requests/${requestId}/questions/${questionId}/reply`, parsed, headers), bindingId, requestId, questionId);
      const reply = result.question.reply;
      if (typeof result.replayed !== 'boolean' || !reply) throw new PrimaryApiError('invalid_response');
      same(reply.clientReplyId, parsed.reply.clientReplyId); same(reply.questionRevision, parsed.reply.questionRevision);
      same(reply.answer, parsed.reply.answer);
      if (reply.text !== parsed.reply.text) throw new PrimaryApiError('identity_mismatch');
      return result;
    },
    async captureTurn(bindingId, input) {
      const headers = bindingHeaders(bindingId);
      const turn = parse(turnSchema, input, 'invalid_request');
      const receipt = parse(turnReceiptSchema, await request('POST', `/calls/${bindingId}/turns`, turn, headers), 'invalid_response');
      same(receipt.bindingId, bindingId); same(receipt.sourceEventId, turn.sourceEventId); same(receipt.revision, turn.revision);
      same(receipt.recordId, turn.sourceEventId);
      if (receipt.bodyDigest !== undefined) same(receipt.bodyDigest, turnBodyDigest(turn));
      return receipt;
    },
    async close(bindingId) {
      const closed = parse(closedSchema, await request('POST', `/calls/${bindingId}/close`, {}, bindingHeaders(bindingId)), 'invalid_response');
      same(closed.bindingId, bindingId);
      const existing = calls.get(bindingId);
      if (existing) {
        same(closed.callSid, existing.callSid);
        calls.set(bindingId, { ...existing, state: 'closed' });
      }
      return closed;
    },
  };
}
