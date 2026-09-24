import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { z } from 'zod';
import { brokerCallAssignmentSchema, phoneCallAssignmentSchema, retainedPhoneCallAssignmentRefSchema, sameCallAssignmentRef, type BrokerCallAssignment, type PhoneCallAssignmentRef } from './assignment.js';
import type { BrokerPublicationJournal } from './publications.js';
import { resolvedHarkAssignmentSchema } from './harkAssignment.js';
import { canonicalJson } from '../outbound/canonical.js';
import { observeHark, pendingHarkEvents, settleHarkJournalEvent, validateHarkJournal,
  type HarkDeliveryOutcome, type HarkEndedTiming, type HarkJournal, type HarkJournalEvent, type HarkProviderStatus } from './harkJournal.js';
import { bindDurableFileParent, durablyReplaceJsonFile, type DurableParentBinding } from '../outbound/DurableFile.js';
import { assertInboundMediaStart, createInboundJournal, inboundRequestId, reduceInboundJournal, validateInboundJournal,
  type InboundJournal, type InboundJournalEvent, type InboundWaitPolicy } from './inboundJournal.js';
import type { InboundBeginIdentity } from './inboundContract.js';
import type { PrimaryCall } from './primaryApi.js';

export type CallStatus = 'prepared' | 'dialing' | 'queued' | 'ringing' | 'in-progress' | 'completed' | 'busy' | 'no-answer' | 'canceled' | 'failed' | 'unknown';
export const terminalCallStatuses = new Set<CallStatus>(['completed', 'busy', 'no-answer', 'canceled', 'failed']);
export interface HarkDispatch {
  version: 1;
  notificationDigest: string;
  state: 'not_dispatched' | 'dispatch_started' | 'provider_accepted';
  reservedAt: string;
  dispatchStartedAt?: string;
  providerAcceptedAt?: string;
  preDispatchFailure?: { observedAt: string; code: string; message: string };
}
export interface BrokerCall {
  id: string; requestId: string; direction: 'inbound' | 'outbound'; purpose: string;
  number: string; callSid?: string; streamSid?: string; status: CallStatus;
  createdAt: string; updatedAt: string; endedAt?: string;
  callAssignment?: BrokerCallAssignment;
  primary?: { bindingId: string; name: string | null; context: string; callAssignmentRef?: PhoneCallAssignmentRef;
    primaryDexterId?: string; threadId?: string };
  liveSessionId?: string; transcript?: unknown; transcriptRevision?: number;
  mediaStartedAt?: string;
  harkJournal?: HarkJournal;
  harkDispatch?: HarkDispatch;
  inbound?: InboundJournal;
  voiceOpening?: { status: string; eventId?: string; reason?: string };
  finalized?: boolean; failure?: { code: string; message: string };
  canonicalTurns?: Record<string, unknown>;
  publications?: BrokerPublicationJournal;
  acousticSpans?: unknown;
  callerCompletions?: Record<string, { sessionId: string; firstSequence: number; lastSequence: number;
    textDigest: string; observedAt: string }>;
  primaryClosePending?: boolean;
  primaryClosed?: { bindingId: string; callSid: string; state: 'closed' };
  callback?: { requestId: string; consentedAt: string; state: 'requested' | 'started' | 'unavailable' };
  transcriptCapture?: {
    state: 'pending' | 'complete';
    assistantPlaybackComplete: boolean;
    sessionFinalized: boolean;
    pending: number;
    unsupported: string[];
  };
  metrics?: { receivedAudioBytes: number; sentAudioBytes: number; playedAudioBytes: number; clearedAudioBytes: number };
  delegations: Record<string, { requestId: string; status: string; updatedAt: string; result?: string }>;
}

const dispatchDigest = z.string().regex(/^[0-9a-f]{64}$/);
const dispatchTime = z.string().datetime({ offset: true });
const dispatchBase = { version: z.literal(1), notificationDigest: dispatchDigest, reservedAt: dispatchTime };
const preDispatchFailure = z.object({ observedAt: dispatchTime,
  code: z.string().min(1).max(80), message: z.string().min(1).max(400) }).strict().optional();
const harkDispatchSchema = z.discriminatedUnion('state', [
  z.object({ ...dispatchBase, state: z.literal('not_dispatched') }).strict(),
  z.object({ ...dispatchBase, state: z.literal('dispatch_started'), dispatchStartedAt: dispatchTime, preDispatchFailure }).strict(),
  z.object({ ...dispatchBase, state: z.literal('provider_accepted'), dispatchStartedAt: dispatchTime, providerAcceptedAt: dispatchTime, preDispatchFailure }).strict(),
]);
function validateHarkDispatch(call: BrokerCall): void {
  if (call.harkDispatch === undefined) return;
  const dispatch = harkDispatchSchema.parse(call.harkDispatch);
  if (call.callAssignment?.origin !== 'authenticated_hark_delivery') throw new Error('Hark dispatch requires its original assignment');
  if (dispatch.reservedAt !== call.createdAt) throw new Error('Hark dispatch reservation time changed');
  if (dispatch.state === 'provider_accepted') {
    if (!call.callSid || !/^CA[0-9a-fA-F]{32}$/.test(call.callSid)) throw new Error('Accepted Hark dispatch requires its provider SID');
  } else {
    if (call.callSid !== undefined || call.harkJournal !== undefined || call.primary !== undefined || call.streamSid !== undefined)
      throw new Error('Unaccepted Hark dispatch has provider evidence');
    if (dispatch.state === 'not_dispatched' && !['prepared', 'failed', 'canceled'].includes(call.status))
      throw new Error('Undispatched Hark call has dispatched status');
    if (dispatch.state === 'dispatch_started' && !['dialing', 'failed', 'unknown', 'canceled'].includes(call.status))
      throw new Error('Unaccepted Hark dispatch has provider status');
  }
}

function validateAssignment(call: BrokerCall): void {
  const assignment = call.callAssignment === undefined ? undefined : brokerCallAssignmentSchema.parse(call.callAssignment);
  if (assignment && (call.direction !== 'outbound' || assignment.value.assignmentId !== call.requestId
      || assignment.value.brokerCallId !== call.id || assignment.value.purpose !== call.purpose)) throw new Error('Call assignment identity mismatch');
  const reference = call.primary?.callAssignmentRef;
  if (reference !== undefined) {
    retainedPhoneCallAssignmentRefSchema.parse(reference);
    if (!assignment || !call.callSid || reference.assignmentId !== assignment.value.assignmentId
        || reference.revision !== assignment.value.revision) throw new Error('Call assignment reference mismatch');
  }
  if (assignment && call.primary && !reference) throw new Error('Call assignment reference missing');
}

const primaryResultId = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
function validatePrimaryIdentity(call: BrokerCall): void {
  const primary = call.primary;
  // Old journals lack both IDs. Reading them must not manufacture or persist IDs.
  if (primary?.primaryDexterId === undefined && primary?.threadId === undefined) return;
  if (!primaryResultId.safeParse(primary?.primaryDexterId).success || !primaryResultId.safeParse(primary?.threadId).success)
    throw new Error('Invalid Primary result identity');
}

function assertPrimaryIdentityUnchanged(previous: BrokerCall, next: BrokerCall): void {
  if (!previous.primary) return;
  if (next.primary?.bindingId !== previous.primary.bindingId)
    throw new Error('Primary binding cannot change');
  for (const field of ['primaryDexterId', 'threadId'] as const) {
    if (previous.primary[field] !== undefined && next.primary?.[field] !== previous.primary[field])
      throw new Error('Primary result identity cannot change');
  }
}

interface State { version: 1; calls: Record<string, BrokerCall> }

/** One process owns call admission. Provider dispatch always follows a durable state write. */
export class BrokerStore {
  private state: State;
  private readonly file: string;
  private readonly lock: string;
  private readonly lockId = randomUUID();
  private readonly parent: DurableParentBinding;
  private closed = false;

  constructor(readonly directory: string) {
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    this.file = path.join(directory, 'calls.json');
    this.lock = path.join(directory, 'writer.lock');
    this.parent = bindDurableFileParent(this.file);
    try { this.acquire(); } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
      const previous = JSON.parse(fs.readFileSync(this.lock, 'utf8')) as { pid: number };
      if (!Number.isInteger(previous.pid) || previous.pid < 1) throw new Error('Invalid broker writer lock');
      try { process.kill(previous.pid, 0); throw new Error('Another broker process owns this state'); }
      catch (probe) { if ((probe as NodeJS.ErrnoException).code !== 'ESRCH') throw probe; }
      fs.unlinkSync(this.lock);
      this.acquire();
    }
    try {
      this.state = fs.existsSync(this.file) ? JSON.parse(fs.readFileSync(this.file, 'utf8')) : { version: 1, calls: {} };
      if (this.state.version !== 1 || !this.state.calls || typeof this.state.calls !== 'object' || Array.isArray(this.state.calls)) throw new Error('Invalid broker call store');
      for (const [id, call] of Object.entries(this.state.calls)) {
        if (call.id !== id || !call.requestId || !call.number || !call.createdAt || !call.delegations) throw new Error('Invalid broker call record');
        validateAssignment(call);
        validatePrimaryIdentity(call);
        validateHarkJournal(call);
        validateHarkDispatch(call);
        validateInboundJournal(call);
      }
    } catch (error) { this.close(); throw error; }
  }

  private acquire() {
    const fd = fs.openSync(this.lock, 'wx', 0o600);
    try { fs.writeFileSync(fd, JSON.stringify({ pid: process.pid, id: this.lockId })); fs.fsyncSync(fd); }
    finally { fs.closeSync(fd); }
  }

  private persist(next: State) {
    if (this.closed) throw new Error('Broker store is closed');
    durablyReplaceJsonFile(this.file, JSON.stringify(next), fs, this.parent);
    this.state = next;
  }

  get(id: string): BrokerCall | undefined { const call = this.state.calls[id]; return call && structuredClone(call); }
  bySid(sid: string): BrokerCall | undefined { return this.list().find(call => call.callSid === sid); }
  list(): BrokerCall[] { return structuredClone(Object.values(this.state.calls)).sort((a, b) => b.createdAt.localeCompare(a.createdAt)); }

  reserveInbound(input: { requestId: string; purpose: string; providerProof: InboundBeginIdentity['providerProof'] },
    policy: InboundWaitPolicy, now = new Date().toISOString()): { call: BrokerCall; existing: boolean } {
    const proof = structuredClone(input.providerProof);
    const callSid = proof.params.CallSid;
    if (input.requestId !== inboundRequestId(callSid) || typeof input.purpose !== 'string'
        || !input.purpose || input.purpose.length > 1500) throw new Error('Invalid inbound reservation');
    const existing = this.list().find(call => call.requestId === input.requestId || call.callSid === callSid);
    if (existing) {
      if (!existing.inbound || existing.requestId !== input.requestId || existing.callSid !== callSid
          || existing.purpose !== input.purpose || canonicalJson(existing.inbound.begin.providerProof) !== canonicalJson(proof))
        throw new Error('Inbound reservation conflicts with its saved record');
      validateInboundJournal(existing);
      // New runtime policy cannot renew a saved attempt's wait or media budget.
      return { call: existing, existing: true };
    }
    const id = randomUUID();
    const call: BrokerCall = { id, requestId: input.requestId, direction: 'inbound', purpose: input.purpose,
      number: proof.params.From, callSid, status: 'prepared', createdAt: now, updatedAt: now, delegations: {},
      inbound: createInboundJournal({ brokerCallId: id, providerProof: proof }, policy, now) };
    validateInboundJournal(call);
    this.persist({ version: 1, calls: { ...this.state.calls, [id]: call } });
    return { call: structuredClone(call), existing: false };
  }

  transitionInbound(id: string, event: InboundJournalEvent): BrokerCall {
    const previous = this.state.calls[id];
    if (!previous) throw new Error('Unknown broker call');
    const next = reduceInboundJournal(previous, event, new Date().toISOString());
    if (next === previous) return structuredClone(previous);
    return this.saveHarkCall(next);
  }

  reserve(input: Pick<BrokerCall, 'requestId' | 'direction' | 'purpose' | 'number'> & { callSid?: string }, origin?: 'authenticated_broker_control'): { call: BrokerCall; existing: boolean } {
    if (Object.prototype.hasOwnProperty.call(input, 'callAssignment')) throw new Error('Assignment must originate from control reservation');
    const existing = this.list().find(call => call.requestId === input.requestId || (input.callSid && call.callSid === input.callSid));
    if (existing) {
      if (existing.callAssignment?.origin === 'authenticated_hark_delivery') throw new Error('Call origin conflicts with its saved record');
      if (existing.number !== input.number || existing.direction !== input.direction || existing.purpose !== input.purpose || (input.callSid && existing.callSid !== input.callSid)) throw new Error('Call request identity conflicts with its saved record');
      return { call: existing, existing: true };
    }
    const now = new Date().toISOString();
    const call: BrokerCall = { id: randomUUID(), ...input, status: 'prepared', createdAt: now, updatedAt: now, delegations: {} };
    if (origin !== undefined) {
      if (origin !== 'authenticated_broker_control') throw new Error('Invalid call assignment origin');
      call.callAssignment = { origin, value: phoneCallAssignmentSchema.parse({ assignmentId: call.requestId,
        brokerCallId: call.id, revision: 1, purpose: call.purpose }) };
    }
    validateAssignment(call);
    this.persist({ version: 1, calls: { ...this.state.calls, [call.id]: call } });
    return { call: structuredClone(call), existing: false };
  }

  /** Only the API's resolved Hark snapshot supplies this reservation. */
  reserveHark(value: unknown, notificationDigest?: string): { call: BrokerCall; existing: boolean } {
    if (notificationDigest !== undefined) dispatchDigest.parse(notificationDigest);
    const resolved = resolvedHarkAssignmentSchema.parse(value);
    const assignment = resolved.callAssignment;
    const id = assignment.brokerCallId;
    const existing = this.list().find(call => call.id === id || call.requestId === id);
    if (existing) {
      if (existing.id !== id || existing.requestId !== id || existing.direction !== 'outbound'
          || existing.number !== resolved.to || existing.purpose !== assignment.purpose
          || existing.callAssignment?.origin !== 'authenticated_hark_delivery'
          || canonicalJson(existing.callAssignment.value) !== canonicalJson(assignment))
        throw new Error('Hark delivery conflicts with its saved record');
      if (existing.harkDispatch?.notificationDigest !== notificationDigest)
        throw new Error('Hark notification conflicts with its saved record');
      return { call: existing, existing: true };
    }
    const now = new Date().toISOString();
    const call: BrokerCall = { id, requestId: id, direction: 'outbound', purpose: assignment.purpose,
      number: resolved.to, status: 'prepared', createdAt: now, updatedAt: now, delegations: {},
      callAssignment: { origin: 'authenticated_hark_delivery', value: assignment } };
    if (notificationDigest !== undefined) call.harkDispatch = { version: 1, notificationDigest,
      state: 'not_dispatched', reservedAt: now };
    validateAssignment(call);
    validateHarkDispatch(call);
    this.persist({ version: 1, calls: { ...this.state.calls, [id]: call } });
    return { call: structuredClone(call), existing: false };
  }

  /** Caller authentication is separate; this only checks the saved notification identity. */
  byHarkNotification(deliveryId: string, notificationDigest: string): BrokerCall | undefined {
    dispatchDigest.parse(notificationDigest);
    const call = this.list().find(item => item.id === deliveryId || item.requestId === deliveryId);
    if (!call) return undefined;
    if (call.id !== deliveryId || call.requestId !== deliveryId || call.callAssignment?.origin !== 'authenticated_hark_delivery')
      throw new Error('Hark delivery conflicts with its saved record');
    if (!call.harkDispatch) throw new Error('Hark notification identity unavailable');
    if (call.harkDispatch.notificationDigest !== notificationDigest) throw new Error('Hark notification conflicts with its saved record');
    validateHarkDispatch(call);
    return call;
  }

  beginHarkDispatch(id: string, observedAt: string): BrokerCall {
    dispatchTime.parse(observedAt);
    const previous = this.state.calls[id];
    if (!previous) throw new Error('Unknown broker call');
    validateHarkDispatch(previous);
    if (!previous.harkDispatch) throw new Error('Hark dispatch history unavailable');
    if (previous.harkDispatch.state !== 'not_dispatched') throw new Error('Hark dispatch already started');
    const { failure, ...current } = previous;
    return this.saveHarkCall({ ...current, status: 'dialing', updatedAt: observedAt,
      harkDispatch: { ...previous.harkDispatch, state: 'dispatch_started', dispatchStartedAt: observedAt,
        ...(failure ? { preDispatchFailure: { observedAt: previous.updatedAt, code: failure.code, message: failure.message } } : {}) } });
  }

  acceptHarkDispatch(id: string, sid: string, observedAt: string): BrokerCall {
    dispatchTime.parse(observedAt);
    if (!/^CA[0-9a-fA-F]{32}$/.test(sid)) throw new Error('Invalid call SID');
    const previous = this.state.calls[id];
    if (!previous) throw new Error('Unknown broker call');
    validateHarkDispatch(previous);
    if (!previous.harkDispatch) throw new Error('Hark dispatch history unavailable');
    if (previous.harkDispatch.state === 'not_dispatched') throw new Error('Hark dispatch was not started');
    if (previous.harkDispatch.state === 'provider_accepted') {
      if (previous.callSid !== sid) throw new Error('Call SID cannot change');
      return structuredClone(previous);
    }
    if (Object.values(this.state.calls).some(call => call.id !== id && call.callSid === sid)) throw new Error('Call SID is already bound');
    return this.saveHarkCall({ ...previous, callSid: sid, updatedAt: observedAt,
      harkDispatch: { ...previous.harkDispatch, state: 'provider_accepted', providerAcceptedAt: observedAt } });
  }

  /** The IDs come from the authenticated API binding response, never caller input. */
  recordPrimaryBinding(id: string, response: PrimaryCall): BrokerCall {
    const previous = this.state.calls[id];
    if (!previous) throw new Error('Unknown broker call');
    if (previous.inbound) throw new Error('Inbound context requires an explicit transition');
    const binding = structuredClone(response);
    if (!previous.callSid || binding.callSid !== previous.callSid || binding.state !== 'open' || previous.primaryClosed)
      throw new Error('Primary call identity mismatch');
    const context = binding.context;
    if (!context || !primaryResultId.safeParse(context.primaryDexterId).success || !primaryResultId.safeParse(context.threadId).success
        || typeof context.displayName !== 'string' || !Array.isArray(context.messages)
        || context.messages.some(message => !['user', 'assistant'].includes(message.role) || typeof message.text !== 'string'))
      throw new Error('Invalid Primary binding context');
    if (previous.primary?.callAssignmentRef && !sameCallAssignmentRef(previous.primary.callAssignmentRef, binding.callAssignmentRef))
      throw new Error('Call assignment reference cannot change');
    return this.saveHarkCall({ ...previous, updatedAt: new Date().toISOString(), primary: {
      bindingId: binding.bindingId, primaryDexterId: context.primaryDexterId, threadId: context.threadId,
      name: context.displayName, context: context.messages.map(message => `${message.role}: ${message.text}`).join('\n').slice(-8_000),
      ...(binding.callAssignmentRef === undefined ? {} : { callAssignmentRef: binding.callAssignmentRef }),
    } });
  }

  update(id: string, patch: Partial<Omit<BrokerCall, 'id' | 'requestId' | 'number' | 'direction' | 'purpose' | 'createdAt' | 'callAssignment' | 'harkJournal' | 'harkDispatch' | 'inbound'>>): BrokerCall {
    const previous = this.state.calls[id];
    if (!previous) throw new Error('Unknown broker call');
    // Runtime callers can bypass Omit. Check the same property snapshot that
    // will be persisted, so accessors cannot change a value after validation.
    const nextPatch = { ...patch };
    if (Object.prototype.hasOwnProperty.call(nextPatch, 'mediaStartedAt'))
      assertInboundMediaStart(previous, nextPatch.mediaStartedAt, new Date().toISOString());
    for (const key of ['id', 'requestId', 'number', 'direction', 'purpose', 'createdAt'] as const) {
      if (Object.prototype.hasOwnProperty.call(nextPatch, key)
          && (nextPatch as Partial<BrokerCall>)[key] !== previous[key]) throw new Error('Call identity cannot change');
    }
    if (Object.prototype.hasOwnProperty.call(nextPatch, 'callAssignment')) {
      const assignment = (nextPatch as Partial<BrokerCall>).callAssignment;
      const normalized = assignment === undefined ? undefined : brokerCallAssignmentSchema.parse(assignment);
      if (canonicalJson(normalized) !== canonicalJson(previous.callAssignment)) throw new Error('Call assignment cannot change');
      (nextPatch as Partial<BrokerCall>).callAssignment = previous.callAssignment;
    }
    if (Object.prototype.hasOwnProperty.call(nextPatch, 'harkJournal')) {
      if (canonicalJson((nextPatch as Partial<BrokerCall>).harkJournal) !== canonicalJson(previous.harkJournal))
        throw new Error('Hark journal requires an observed lifecycle transition');
      delete (nextPatch as Partial<BrokerCall>).harkJournal;
    }
    if (Object.prototype.hasOwnProperty.call(nextPatch, 'harkDispatch')) {
      if (canonicalJson((nextPatch as Partial<BrokerCall>).harkDispatch) !== canonicalJson(previous.harkDispatch))
        throw new Error('Hark dispatch requires an explicit transition');
      delete (nextPatch as Partial<BrokerCall>).harkDispatch;
    }
    if (Object.prototype.hasOwnProperty.call(nextPatch, 'inbound')) {
      if (canonicalJson((nextPatch as Partial<BrokerCall>).inbound) !== canonicalJson(previous.inbound))
        throw new Error('Inbound journal requires an explicit transition');
      delete (nextPatch as Partial<BrokerCall>).inbound;
    }
    if (Object.prototype.hasOwnProperty.call(nextPatch, 'primary')) {
      nextPatch.primary = nextPatch.primary && structuredClone(nextPatch.primary);
      if (previous.inbound && canonicalJson(nextPatch.primary) !== canonicalJson(previous.primary))
        throw new Error('Inbound context requires an explicit transition');
      if (previous.callAssignment && previous.primary && nextPatch.primary?.bindingId !== previous.primary.bindingId)
        throw new Error('Assigned Primary binding cannot change');
      if (previous.primary?.callAssignmentRef && !sameCallAssignmentRef(previous.primary.callAssignmentRef, nextPatch.primary?.callAssignmentRef))
        throw new Error('Call assignment reference cannot change');
      for (const field of ['primaryDexterId', 'threadId'] as const) {
        if (nextPatch.primary?.[field] !== previous.primary?.[field])
          throw new Error('Primary result identity requires an API binding response');
      }
    }
    if (Object.prototype.hasOwnProperty.call(nextPatch, 'primaryClosed')) {
      const closed = nextPatch.primaryClosed;
      if (previous.inbound) {
        if (canonicalJson(closed) !== canonicalJson(previous.primaryClosed))
          throw new Error('Inbound closure requires an explicit receipt transition');
        delete nextPatch.primaryClosed;
      } else if (!closed || closed.state !== 'closed' || closed.bindingId !== previous.primary?.bindingId || closed.callSid !== previous.callSid
          || (previous.primaryClosed && (closed.bindingId !== previous.primaryClosed.bindingId || closed.callSid !== previous.primaryClosed.callSid)))
        throw new Error('Closed Primary binding cannot change');
    }
    if (Object.prototype.hasOwnProperty.call(nextPatch, 'callSid')) {
      if (previous.callSid !== undefined && previous.callSid !== nextPatch.callSid) throw new Error('Call SID cannot change');
      if (nextPatch.callSid !== undefined && (typeof nextPatch.callSid !== 'string' || !/^CA[0-9a-fA-F]{32}$/.test(nextPatch.callSid)))
        throw new Error('Invalid call SID');
    }
    if (nextPatch.callSid && Object.values(this.state.calls).some(call => call.id !== id && call.callSid === nextPatch.callSid)) throw new Error('Call SID is already bound');
    const rank: Partial<Record<CallStatus, number>> = { prepared: 0, dialing: 1, queued: 2, ringing: 3, 'in-progress': 4 };
    if (nextPatch.status && (terminalCallStatuses.has(previous.status) || (previous.status !== 'unknown' && ((rank[nextPatch.status] ?? 9) < (rank[previous.status] ?? -1))))) delete nextPatch.status;
    if (nextPatch.transcriptRevision !== undefined && nextPatch.transcriptRevision < (previous.transcriptRevision ?? 0)) { delete nextPatch.transcript; delete nextPatch.transcriptRevision; }
    const call: BrokerCall = { ...previous, ...nextPatch, updatedAt: new Date().toISOString() };
    assertPrimaryIdentityUnchanged(previous, call);
    validatePrimaryIdentity(call);
    validateAssignment(call);
    validateHarkJournal(call);
    validateHarkDispatch(call);
    validateInboundJournal(call);
    this.persist({ version: 1, calls: { ...this.state.calls, [id]: call } });
    return structuredClone(call);
  }

  private saveHarkCall(call: BrokerCall): BrokerCall {
    const previous = this.state.calls[call.id];
    if (previous) assertPrimaryIdentityUnchanged(previous, call);
    validatePrimaryIdentity(call);
    validateAssignment(call);
    validateHarkJournal(call);
    validateHarkDispatch(call);
    validateInboundJournal(call);
    this.persist({ version: 1, calls: { ...this.state.calls, [call.id]: call } });
    return structuredClone(call);
  }

  observeHarkProvider(id: string, status: HarkProviderStatus, observedAt: string, endedTiming?: HarkEndedTiming): BrokerCall {
    const previous = this.state.calls[id];
    if (!previous) throw new Error('Unknown broker call');
    const mapped = status === 'in-progress' ? 'answered' : status;
    const harkJournal = observeHark(previous, mapped, observedAt, endedTiming);
    const providerStatus = harkJournal.providerStatus;
    const nextStatus = providerStatus === 'answered' || providerStatus === 'streaming' ? 'in-progress' : providerStatus;
    return this.saveHarkCall({ ...previous, harkJournal, status: nextStatus,
      ...(harkJournal.ended ? { endedAt: harkJournal.ended.observedAt } : {}), updatedAt: new Date().toISOString() });
  }

  observeHarkMediaStarted(id: string, observedAt: string): BrokerCall {
    const previous = this.state.calls[id];
    if (!previous) throw new Error('Unknown broker call');
    const harkJournal = observeHark(previous, 'streaming', observedAt);
    // A late/replayed media callback cannot reopen a terminal provider call.
    const closed = Boolean(harkJournal.ended);
    return this.saveHarkCall({ ...previous, harkJournal,
      ...(closed ? {} : { status: 'in-progress' as const, mediaStartedAt: previous.mediaStartedAt ?? observedAt }),
      updatedAt: new Date().toISOString() });
  }

  pendingHarkEvents(id?: string): Array<{ callId: string; event: HarkJournalEvent }> {
    const calls = id === undefined ? Object.values(this.state.calls) : [this.state.calls[id]].filter(call => call !== undefined);
    return calls.flatMap(call => pendingHarkEvents(call).map(event => ({ callId: call.id, event })));
  }

  recordHarkAttempt(id: string, eventId: string, bodyDigest: string, attemptedAt: string): HarkJournalEvent {
    const call = this.get(id);
    const item = call?.harkJournal?.events[eventId];
    if (!call || !item || item.bodyDigest !== bodyDigest) throw new Error('Unknown Hark event identity');
    if (!Number.isFinite(Date.parse(attemptedAt)) || !/^\d{4}-\d{2}-\d{2}T/.test(attemptedAt)) throw new Error('Invalid attempt time');
    if (!pendingHarkEvents(call).some(event => event.id === eventId)) throw new Error('Hark event is not ready for delivery');
    item.attempts += 1;
    item.lastAttemptAt = attemptedAt;
    this.saveHarkCall(call);
    return structuredClone(item);
  }

  settleHarkEvent(id: string, eventId: string, bodyDigest: string, outcome: HarkDeliveryOutcome): HarkJournalEvent {
    const call = this.get(id);
    const item = call?.harkJournal?.events[eventId];
    if (!call?.harkJournal || !item || item.bodyDigest !== bodyDigest) throw new Error('Unknown Hark event identity');
    call.harkJournal.events[eventId] = settleHarkJournalEvent(item, outcome);
    this.saveHarkCall(call);
    return structuredClone(call.harkJournal.events[eventId]!);
  }

  close() {
    if (this.closed) return;
    this.closed = true;
    try { const lock = JSON.parse(fs.readFileSync(this.lock, 'utf8')); if (lock.id === this.lockId) fs.unlinkSync(this.lock); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
  }
}
