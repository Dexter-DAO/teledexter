import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { brokerCallAssignmentSchema, phoneCallAssignmentRefSchema, sameCallAssignmentRef, type BrokerCallAssignment, type PhoneCallAssignmentRef } from './assignment.js';
import type { BrokerPublicationJournal } from './publications.js';
import { bindDurableFileParent, durablyReplaceJsonFile, type DurableParentBinding } from '../outbound/DurableFile.js';

export type CallStatus = 'prepared' | 'dialing' | 'queued' | 'ringing' | 'in-progress' | 'completed' | 'busy' | 'no-answer' | 'canceled' | 'failed' | 'unknown';
export const terminalCallStatuses = new Set<CallStatus>(['completed', 'busy', 'no-answer', 'canceled', 'failed']);
export interface BrokerCall {
  id: string; requestId: string; direction: 'inbound' | 'outbound'; purpose: string;
  number: string; callSid?: string; streamSid?: string; status: CallStatus;
  createdAt: string; updatedAt: string; endedAt?: string;
  callAssignment?: BrokerCallAssignment;
  primary?: { bindingId: string; name: string | null; context: string; callAssignmentRef?: PhoneCallAssignmentRef };
  liveSessionId?: string; transcript?: unknown; transcriptRevision?: number;
  mediaStartedAt?: string;
  voiceOpening?: { status: string; eventId?: string; reason?: string };
  finalized?: boolean; failure?: { code: string; message: string };
  canonicalTurns?: Record<string, unknown>;
  publications?: BrokerPublicationJournal;
  acousticSpans?: unknown;
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

function validateAssignment(call: BrokerCall): void {
  const assignment = call.callAssignment === undefined ? undefined : brokerCallAssignmentSchema.parse(call.callAssignment);
  if (assignment && (call.direction !== 'outbound' || assignment.value.assignmentId !== call.requestId
      || assignment.value.brokerCallId !== call.id || assignment.value.purpose !== call.purpose)) throw new Error('Call assignment identity mismatch');
  const reference = call.primary?.callAssignmentRef;
  if (reference !== undefined) {
    phoneCallAssignmentRefSchema.parse(reference);
    if (!assignment || !call.callSid || reference.assignmentId !== assignment.value.assignmentId
        || reference.revision !== assignment.value.revision) throw new Error('Call assignment reference mismatch');
  }
  if (assignment && call.primary && !reference) throw new Error('Call assignment reference missing');
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

  reserve(input: Pick<BrokerCall, 'requestId' | 'direction' | 'purpose' | 'number'> & { callSid?: string }, origin?: 'authenticated_broker_control'): { call: BrokerCall; existing: boolean } {
    if (Object.prototype.hasOwnProperty.call(input, 'callAssignment')) throw new Error('Assignment must originate from control reservation');
    const existing = this.list().find(call => call.requestId === input.requestId || (input.callSid && call.callSid === input.callSid));
    if (existing) {
      if (existing.number !== input.number || existing.direction !== input.direction || existing.purpose !== input.purpose || (input.callSid && existing.callSid !== input.callSid)) throw new Error('Call request identity conflicts with its saved record');
      return { call: existing, existing: true };
    }
    const now = new Date().toISOString();
    const call: BrokerCall = { id: randomUUID(), ...input, status: 'prepared', createdAt: now, updatedAt: now, delegations: {} };
    if (origin !== undefined) {
      if (origin !== 'authenticated_broker_control') throw new Error('Invalid call assignment origin');
      call.callAssignment = { origin, value: { assignmentId: call.requestId, brokerCallId: call.id, revision: 1, purpose: call.purpose } };
    }
    validateAssignment(call);
    this.persist({ version: 1, calls: { ...this.state.calls, [call.id]: call } });
    return { call: structuredClone(call), existing: false };
  }

  update(id: string, patch: Partial<Omit<BrokerCall, 'id' | 'requestId' | 'number' | 'direction' | 'purpose' | 'createdAt' | 'callAssignment'>>): BrokerCall {
    const previous = this.state.calls[id];
    if (!previous) throw new Error('Unknown broker call');
    // Runtime callers can bypass Omit. Check the same property snapshot that
    // will be persisted, so accessors cannot change a value after validation.
    const nextPatch = { ...patch };
    for (const key of ['id', 'requestId', 'number', 'direction', 'purpose', 'createdAt'] as const) {
      if (Object.prototype.hasOwnProperty.call(nextPatch, key)
          && (nextPatch as Partial<BrokerCall>)[key] !== previous[key]) throw new Error('Call identity cannot change');
    }
    if (Object.prototype.hasOwnProperty.call(nextPatch, 'callAssignment')) {
      const assignment = (nextPatch as Partial<BrokerCall>).callAssignment;
      const normalized = assignment === undefined ? undefined : brokerCallAssignmentSchema.parse(assignment);
      if (JSON.stringify(normalized) !== JSON.stringify(previous.callAssignment)) throw new Error('Call assignment cannot change');
      (nextPatch as Partial<BrokerCall>).callAssignment = normalized;
    }
    if (Object.prototype.hasOwnProperty.call(nextPatch, 'primary')) {
      if (previous.callAssignment && previous.primary && nextPatch.primary?.bindingId !== previous.primary.bindingId)
        throw new Error('Assigned Primary binding cannot change');
      if (previous.primary?.callAssignmentRef && !sameCallAssignmentRef(previous.primary.callAssignmentRef, nextPatch.primary?.callAssignmentRef))
        throw new Error('Call assignment reference cannot change');
      nextPatch.primary = nextPatch.primary && structuredClone(nextPatch.primary);
    }
    if (Object.prototype.hasOwnProperty.call(nextPatch, 'primaryClosed')) {
      const closed = nextPatch.primaryClosed;
      if (!closed || closed.state !== 'closed' || closed.bindingId !== previous.primary?.bindingId || closed.callSid !== previous.callSid
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
    validateAssignment(call);
    this.persist({ version: 1, calls: { ...this.state.calls, [id]: call } });
    return structuredClone(call);
  }

  close() {
    if (this.closed) return;
    this.closed = true;
    try { const lock = JSON.parse(fs.readFileSync(this.lock, 'utf8')); if (lock.id === this.lockId) fs.unlinkSync(this.lock); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
  }
}
