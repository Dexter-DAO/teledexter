import { createHmac } from 'node:crypto';
import { z } from 'zod';
import { rawSubmissionDigest, turnBodyDigest, type PhoneNonAdmissionProof } from './reconciliation.js';

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
  openCall(providerProof: PrimaryProviderProof): Promise<PrimaryCall>;
  getContext(bindingId: string): Promise<PrimaryCall>;
  submit(bindingId: string, requestId: string, input: PrimarySubmission): Promise<PrimaryRequestResult>;
  result(bindingId: string, requestId: string): Promise<PrimaryRequestResult>;
  reconcile(bindingId: string, requestId: string, input: PrimarySubmission): Promise<PrimaryReconciliation>;
  question(bindingId: string, requestId: string, questionId: string): Promise<PrimaryQuestionResult>;
  reply(bindingId: string, requestId: string, questionId: string, input: PrimaryReplyInput): Promise<PrimaryQuestionResult>;
  captureTurn(bindingId: string, input: PrimaryTurn): Promise<PrimaryTurnReceipt>;
  close(bindingId: string): Promise<PrimaryClosedCall>;
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
    body?: unknown, headers?: Record<string, string>, expectedStatus = 200): Promise<unknown> => {
    let serialized: string | undefined;
    if (body !== undefined) {
      try { serialized = JSON.stringify(body); }
      catch { throw new PrimaryApiError('invalid_request'); }
      if (serialized === undefined || Buffer.byteLength(serialized) > MAX_REQUEST_BYTES) {
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
  bindingId: uuid, callSid, state: z.enum(['open', 'closed']), context: contextSchema,
});
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
}).strict();
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
    callSid: string; primaryDexterId: string; threadId: string; state: 'open' | 'closed';
  }>();
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
      if (existing.state === 'closed' && call.state !== 'closed') throw new PrimaryApiError('identity_mismatch');
    }
    calls.set(call.bindingId, {
      callSid: call.callSid, primaryDexterId: call.context.primaryDexterId,
      threadId: call.context.threadId, state: call.state,
    });
    return call;
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
    async readiness(callerNumber) {
      parse(z.string().regex(/^\+[1-9]\d{7,14}$/), callerNumber, 'invalid_request');
      return parse(readinessSchema, await request('POST', '/readiness', { callerNumber }), 'invalid_response');
    },
    async openCall(providerProof) {
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
      const call = parse(callSchema, await request('POST', '/calls', { providerProof: proof }), 'invalid_response');
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
      const existing = calls.get(bindingId);
      if (existing) same(parsed.source.callSid, existing.callSid);
      const { role, ...source } = parsed.source;
      return decodeResult(await request('POST', '/requests', { bindingId, source, role, text: parsed.text },
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
      const known = calls.get(bindingId);
      if (known) same(parsed.source.callSid, known.callSid);
      const { role, ...source } = parsed.source;
      const result = parse(reconciliationSchema, await request('POST', `/requests/${requestId}/reconcile`,
        { bindingId, role, text: parsed.text, source }, headers), 'invalid_response');
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
