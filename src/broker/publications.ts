import { createHash } from 'node:crypto';
import type { LiveBackendUpdate, LiveDelivery, LiveTranscriptSnapshot } from '../live/types.js';
import { terminalCallStatuses, type BrokerCall, type BrokerStore } from './store.js';

export interface BrokerPublicationReference {
  relatedPrimaryRequestId: string;
  referenceOrigin: 'broker_status_publication';
}
export interface BrokerPublicationIdentity {
  callId: string; callSid: string; bindingId: string; sessionId: string;
}
export interface BrokerPublicationRecord extends BrokerPublicationIdentity {
  publicationSequence: number;
  updateId: string;
  requestId: string;
  requestState: string;
  kind: 'progress' | 'result';
  speak: true;
  textDigest: string;
  callerRevision: number;
  transcriptSequence: number;
  outcome?: { eventId: string; status: LiveDelivery['status']; callerRevision: number; transcriptSequence: number };
}
export interface BrokerPublicationJournal {
  records: BrokerPublicationRecord[];
  rejections: Array<{ sessionId: string; eventId: string; code: string }>;
}
type Store = Pick<BrokerStore, 'get' | 'update'>;
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const commandId = (value: string) => /^[a-zA-Z0-9_.:-]{1,200}$/.test(value);
const admitted = new Set(['queued', 'running', 'waiting_for_reply', 'completed', 'failed', 'cancelled']);
const sequence = (snapshot: LiveTranscriptSnapshot) => Math.max(0, ...snapshot.fragments.map(fragment => fragment.sequence));
const active = (call: BrokerCall) => !call.endedAt && !call.primaryClosed && !terminalCallStatuses.has(call.status);
const sameCall = (call: BrokerCall, identity: BrokerPublicationIdentity) => call.id === identity.callId
  && call.callSid === identity.callSid && call.primary?.bindingId === identity.bindingId && call.liveSessionId === identity.sessionId;
const journal = (call: BrokerCall): BrokerPublicationJournal => call.publications ?? { records: [], rejections: [] };

function requestMatches(call: BrokerCall, identity: BrokerPublicationIdentity, requestId: string): boolean {
  const entry = call.delegations[requestId] as undefined | {
    requestId: string; callId: string; callSid: string; bindingId: string; sessionId: string; status: string;
    primaryResult?: { requestId: string; bindingId: string; state: string };
  };
  return Boolean(entry && uuid.test(requestId) && entry.requestId === requestId && sameCall(call, entry)
    && entry.sessionId === identity.sessionId && admitted.has(entry.status)
    && entry.primaryResult?.requestId === requestId && entry.primaryResult.bindingId === identity.bindingId
    && entry.primaryResult.state === entry.status);
}

/** The intent is durable before the caller of this function sends commentary. */
export function beginBrokerPublication(store: Store, identity: BrokerPublicationIdentity, requestId: string,
  update: LiveBackendUpdate, snapshot: LiveTranscriptSnapshot): { record: BrokerPublicationRecord; created: boolean } | undefined {
  const call = store.get(identity.callId);
  if (!call || !active(call) || !sameCall(call, identity) || !requestMatches(call, identity, requestId)
      || update.speak !== true || !['progress', 'result'].includes(update.kind) || !commandId(update.id)
      || update.expectedCallerRevision !== snapshot.callerRevision) return undefined;
  const saved = journal(call);
  const intent = { ...identity, updateId: update.id, requestId, requestState: call.delegations[requestId].status,
    kind: update.kind, speak: true as const, textDigest: createHash('sha256').update(update.text, 'utf8').digest('hex'),
    callerRevision: snapshot.callerRevision, transcriptSequence: sequence(snapshot) };
  const previous = saved.records.find(record => record.sessionId === identity.sessionId && record.updateId === update.id);
  if (previous) {
    // Transcript progress does not alter the original publication frontier on retry.
    const { publicationSequence: _ordinal, outcome: _outcome, transcriptSequence: _frontier, ...original } = previous;
    const { transcriptSequence: _current, ...retry } = intent;
    if (JSON.stringify(original) !== JSON.stringify(retry)) throw new Error('publication_identity_changed');
    return { record: previous, created: false };
  }
  const ordinal = saved.records.length + 1;
  if (saved.records.some((record, index) => record.publicationSequence !== index + 1)) throw new Error('invalid_publication_order');
  const record: BrokerPublicationRecord = { ...intent, publicationSequence: ordinal };
  store.update(call.id, { publications: { ...saved, records: [...saved.records, record] } });
  return { record, created: true };
}

/** Receipt means the exact commentary command was accepted, never that it was heard. */
export function settleBrokerPublication(store: Store, intent: BrokerPublicationRecord, delivery: LiveDelivery,
  snapshot: LiveTranscriptSnapshot, closed: boolean): void {
  const call = store.get(intent.callId);
  if (!call) return;
  const saved = journal(call);
  const record = saved.records.find(value => value.publicationSequence === intent.publicationSequence);
  if (!record || JSON.stringify(record) !== JSON.stringify(intent)) throw new Error('publication_intent_changed');
  const duplicateCommand = saved.records.some(value => value.publicationSequence !== record.publicationSequence
    && value.sessionId === intent.sessionId && value.outcome?.eventId === delivery.eventId && delivery.eventId !== '');
  const hasFrontier = Number.isSafeInteger(delivery.observedCallerRevision) && delivery.observedCallerRevision! >= 0
    && Number.isSafeInteger(delivery.observedTranscriptSequence) && delivery.observedTranscriptSequence! >= intent.transcriptSequence;
  const status = closed || !active(call) || !sameCall(call, intent) ? 'closed'
    : snapshot.callerRevision !== intent.callerRevision
      || (hasFrontier && delivery.observedCallerRevision !== intent.callerRevision) ? 'stale'
    : delivery.status === 'accepted' && (!hasFrontier || !commandId(delivery.eventId) || duplicateCommand) ? 'unconfirmed' : delivery.status;
  const outcome = { eventId: delivery.eventId, status, callerRevision: delivery.observedCallerRevision ?? snapshot.callerRevision,
    transcriptSequence: delivery.observedTranscriptSequence ?? sequence(snapshot) };
  store.update(call.id, { publications: { ...saved, records: saved.records.map(value => value === record ? { ...record, outcome } : value) } });
}

/** Retain late rejection separately so a frozen caller submission remains immutable. */
export function rejectBrokerPublication(store: Store, identity: BrokerPublicationIdentity, eventId: string, code: string): void {
  const call = store.get(identity.callId);
  if (!call || !sameCall(call, identity) || !commandId(eventId)) return;
  const saved = journal(call);
  // The provider can reject immediately after acknowledging, before the receipt
  // continuation runs. A tombstone also covers that still-pending publication.
  if (!saved.records.some(record => record.sessionId === identity.sessionId && (!record.outcome || record.outcome.eventId === eventId))) return;
  if (saved.rejections.some(value => value.sessionId === identity.sessionId && value.eventId === eventId)) return;
  store.update(call.id, { publications: { ...saved, rejections: [...saved.rejections,
    { sessionId: identity.sessionId, eventId, code: commandId(code) ? code : 'provider_error' }] } });
}

/** Select only an unambiguous publication before this caller source began. */
export function selectBrokerPublicationReference(call: BrokerCall, identity: BrokerPublicationIdentity,
  snapshot: LiveTranscriptSnapshot, firstSequence: number): BrokerPublicationReference | undefined {
  if (!active(call) || !sameCall(call, identity) || !snapshot.fragments.some(f => f.role === 'user' && f.sequence === firstSequence)) return;
  const sequences = snapshot.fragments.map(fragment => fragment.sequence);
  if (new Set(sequences).size !== sequences.length || sequences.some(value => !Number.isSafeInteger(value) || value < 1)
      || snapshot.fragments.filter(f => f.role === 'user').length !== snapshot.callerRevision) return;
  const priorRevision = snapshot.fragments.filter(f => f.role === 'user' && f.sequence < firstSequence).length;
  const saved = journal(call);
  if (saved.records.some((record, index) => record.publicationSequence !== index + 1)) return;
  const candidates = saved.records.filter(record => record.sessionId === identity.sessionId && record.callerRevision === priorRevision);
  // Unknown acceptance cannot establish which request's context was available.
  if (candidates.some(record => !record.outcome || record.outcome.status === 'unconfirmed')) return;
  const accepted = candidates.filter(record => record.outcome?.status === 'accepted'
    && !saved.rejections.some(rejection => rejection.sessionId === record.sessionId && rejection.eventId === record.outcome!.eventId));
  if (!accepted.length) return;
  for (const record of accepted) {
    const receipt = record.outcome!;
    if (!sameCall(call, record) || !requestMatches(call, identity, record.requestId) || record.speak !== true
        || !['result', 'progress'].includes(record.kind) || !commandId(receipt.eventId)
        || receipt.callerRevision !== priorRevision || record.transcriptSequence >= firstSequence
        || receipt.transcriptSequence < record.transcriptSequence || receipt.transcriptSequence >= firstSequence
        || saved.records.some(other => other !== record && other.sessionId === record.sessionId && other.outcome?.eventId === receipt.eventId)) return;
  }
  const requests = new Set(accepted.map(record => record.requestId));
  if (requests.size !== 1) return;
  return { relatedPrimaryRequestId: accepted[0].requestId, referenceOrigin: 'broker_status_publication' };
}
