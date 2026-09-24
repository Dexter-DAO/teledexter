import { createHash } from 'node:crypto';
import { z } from 'zod';
import { canonicalJson } from '../outbound/canonical.js';
import { liveResultChunks } from './backend.js';
import { eligibleAssociatedDeliveryResponseSchema } from './eligibleContext.js';
import type { LiveBackendUpdate, LiveDelivery, LiveTranscriptSnapshot } from '../live/types.js';
import { terminalCallStatuses, type BrokerCall, type BrokerStore } from './store.js';

export interface BrokerPublicationReference {
  relatedPrimaryRequestId: string;
  referenceOrigin: 'broker_status_publication';
}
export interface BrokerPublicationIdentity {
  callId: string; callSid: string; bindingId: string; sessionId: string;
  /** Only present when the accepted request belongs to an earlier generation. */
  sourceSessionId?: string;
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
  /** Exact associated API response and its complete, ordered speech rendering. */
  selectedDelivery?: SelectedQuestionPublication;
  outcome?: { eventId: string; status: LiveDelivery['status']; callerRevision: number; transcriptSequence: number };
}
export interface BrokerPublicationReceipt {
  publicationSequence: number;
  sessionId: string;
  eventId: string;
}

export interface SelectedQuestionPublication {
  response: z.infer<typeof eligibleAssociatedDeliveryResponseSchema>;
  chunkIndex: number;
  chunkCount: number;
}

/** Recheck every accepted chunk, including rejection received before receipt persistence. */
export function brokerPublicationReceiptsAccepted(call: BrokerCall, requestId: string, sessionId: string,
  receipts: readonly BrokerPublicationReceipt[] | undefined): boolean {
  if (!receipts?.length || new Set(receipts.map(value => value.publicationSequence)).size !== receipts.length
      || new Set(receipts.map(value => value.eventId)).size !== receipts.length) return false;
  const saved = call.publications;
  return receipts.every(receipt => {
    const record = saved?.records.find(value => value.publicationSequence === receipt.publicationSequence);
    return receipt.sessionId === sessionId && commandId(receipt.eventId) && record?.requestId === requestId
      && record.callId === call.id && record.callSid === call.callSid && record.bindingId === call.primary?.bindingId
      && record.sessionId === sessionId && record.kind === 'result' && record.speak === true
      && record.outcome?.eventId === receipt.eventId && record.outcome.status === 'accepted'
      && !saved!.rejections.some(value => value.sessionId === sessionId && value.eventId === receipt.eventId);
  });
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
  const entry = call.delegations[requestId] as undefined | (BrokerCall['delegations'][string] & Partial<BrokerPublicationIdentity> & {
    primaryResult?: { requestId: string; bindingId: string; state: string };
  });
  return Boolean(entry && uuid.test(requestId) && entry.requestId === requestId
    && entry.callId === call.id && entry.callSid === call.callSid && entry.bindingId === call.primary?.bindingId
    && (identity.sourceSessionId === undefined
      ? entry.sessionId === identity.sessionId
      : identity.sourceSessionId !== identity.sessionId && entry.sessionId === identity.sourceSessionId
        && Boolean(call.liveGenerations?.segments.some(segment => segment.sessionId === identity.sourceSessionId))
        && Boolean(call.liveGenerations?.segments.some(segment => segment.sessionId === identity.sessionId)))
    && admitted.has(entry.status)
    && entry.primaryResult?.requestId === requestId && entry.primaryResult.bindingId === identity.bindingId
    && entry.primaryResult.state === entry.status);
}

const selectedPublicationSchema = z.object({
  callId: z.string().regex(uuid), callSid: z.string().regex(/^CA[0-9a-fA-F]{32}$/),
  bindingId: z.string().regex(uuid), sessionId: z.string().min(1).max(200),
  publicationSequence: z.number().int().safe().positive(), updateId: z.string().refine(commandId),
  requestId: z.string().regex(uuid), requestState: z.enum(['waiting_for_reply', 'completed']),
  kind: z.literal('result'), speak: z.literal(true), textDigest: z.string().regex(/^[0-9a-f]{64}$/),
  callerRevision: z.number().int().safe().nonnegative(), transcriptSequence: z.number().int().safe().nonnegative(),
  selectedDelivery: z.object({ response: eligibleAssociatedDeliveryResponseSchema,
    chunkIndex: z.number().int().safe().nonnegative(), chunkCount: z.number().int().safe().positive() }).strict(),
  outcome: z.object({ eventId: z.string(), status: z.enum(['accepted', 'rejected', 'unconfirmed', 'stale', 'closed']),
    callerRevision: z.number().int().safe().nonnegative(), transcriptSequence: z.number().int().safe().nonnegative() }).strict().optional(),
}).strict();
const equal = (a: unknown, b: unknown) => canonicalJson(a) === canonicalJson(b);

/** The association carries original execution identity. The publication itself
 * belongs to the current delivery generation; it creates no local delegation. */
function selectedPublicationMatches(call: BrokerCall, input: BrokerPublicationRecord, current: boolean): boolean {
  const parsed = selectedPublicationSchema.safeParse(input);
  if (!parsed.success) return false;
  const record = parsed.data, saved = record.selectedDelivery, response = saved.response, delivery = response.delivery;
  const host = call.selectedQuestion, generations = call.liveGenerations;
  const slot = current ? host?.questions.at(-1) : host?.questions.find(item => equal(item.record?.base.association, response.deliveryAssociation));
  const segment = generations?.segments.find(item => item.sessionId === record.sessionId);
  if (!host || !slot?.record || !segment || !call.continuation || delivery.state !== 'ready'
    || record.callId !== call.id || record.callSid !== call.callSid || record.bindingId !== call.primary?.bindingId
    || response.bindingId !== record.bindingId || response.callSid !== record.callSid
    || record.requestId !== host.initial.request.originalRequestId || response.requestId !== record.requestId
    || response.deliveryAssociation.execution.requestId !== record.requestId
    || !equal(response.deliveryAssociation, slot.record.base.association)
    || !equal(response.version, segment.version)
    || response.version.ownerPrincipalId !== host.initial.scope.ownerPrincipalId
    || response.version.primaryDexterId !== host.initial.scope.primaryDexterId
    || response.version.threadId !== host.initial.scope.threadId
    || delivery.source.provenance.runId !== slot.record.base.target.execution.runId
    || (delivery.source.kind === 'question'
      ? !('question' in delivery) || delivery.question.id !== slot.seed.request.questionId
        || delivery.question.revision !== slot.seed.request.questionRevision
        || delivery.question.agent.id !== host.initial.scope.primaryDexterId
        || record.requestState !== 'waiting_for_reply'
      : 'question' in delivery || record.requestState !== 'completed')) return false;
  if (current && (!active(call) || call.finalized || host.stoppedAt || !sameCall(call, record)
    || generations?.phase !== 'active' || segment.state !== 'active'
    || generations.deliveryAttachment?.generationId !== segment.generationId
    || generations.deliveryAttachment.sessionId !== segment.sessionId
    || Date.now() >= Date.parse(host.initial.callDeadlineAt))) return false;
  let chunks: string[];
  try { chunks = liveResultChunks(delivery.text); } catch { return false; }
  return saved.chunkCount === chunks.length && saved.chunkIndex < chunks.length
    && record.textDigest === createHash('sha256').update(chunks[saved.chunkIndex]!, 'utf8').digest('hex');
}

/** Historical rejection stays append-only. Existing ordinary journals do not
 * gain new fields or selected-call restrictions when this branch is absent. */
export function validateSelectedQuestionPublications(call: BrokerCall, previous?: BrokerCall): void {
  const saved = journal(call), before = previous && journal(previous);
  const selected = saved.records.filter(record => record.selectedDelivery !== undefined);
  const oldSelected = before?.records.filter(record => record.selectedDelivery !== undefined) ?? [];
  if (!selected.length && !oldSelected.length) return;
  if (saved.records.some((record, index) => record.publicationSequence !== index + 1)
    || selected.some(record => !selectedPublicationMatches(call, record, false))) throw new Error('selected_publication_identity_changed');
  for (const record of selected) {
    const receipt = record.outcome;
    if (receipt?.status === 'accepted' && (!commandId(receipt.eventId) || receipt.callerRevision !== record.callerRevision
      || receipt.transcriptSequence < record.transcriptSequence
      || saved.records.some(other => other !== record && other.sessionId === record.sessionId
        && other.outcome?.eventId === receipt.eventId))) throw new Error('selected_publication_receipt_changed');
  }
  if (!before) return;
  if (saved.records.length < before.records.length || saved.rejections.length < before.rejections.length
    || before.rejections.some((value, index) => !equal(value, saved.rejections[index])))
    throw new Error('selected_publication_history_changed');
  for (const record of oldSelected) {
    const after = saved.records[record.publicationSequence - 1];
    if (!after) throw new Error('selected_publication_history_changed');
    const { outcome: oldOutcome, ...oldIntent } = record, { outcome: nextOutcome, ...nextIntent } = after;
    if (!equal(oldIntent, nextIntent) || (oldOutcome !== undefined && !equal(oldOutcome, nextOutcome)))
      throw new Error('selected_publication_history_changed');
  }
  for (const record of selected.filter(item => !oldSelected.some(old => old.publicationSequence === item.publicationSequence))) {
    if (record.publicationSequence <= before.records.length || record.outcome !== undefined
      || !selectedPublicationMatches(call, record, true)) throw new Error('selected_publication_requires_current_intent');
  }
}

/** Persist the exact associated result before each commentary append. Only the
 * strict API delivery can supply text; neither purpose nor a fake run can do so. */
export function beginSelectedQuestionPublication(store: Store, identity: BrokerPublicationIdentity,
  responseValue: z.infer<typeof eligibleAssociatedDeliveryResponseSchema>, update: LiveBackendUpdate,
  snapshot: LiveTranscriptSnapshot, chunkIndex: number): { record: BrokerPublicationRecord; created: boolean } | undefined {
  const call = store.get(identity.callId);
  if (!call || !active(call) || !sameCall(call, identity) || identity.sourceSessionId !== undefined
    || update.kind !== 'result' || update.speak !== true || !commandId(update.id)
    || update.expectedCallerRevision !== snapshot.callerRevision) return;
  const response = eligibleAssociatedDeliveryResponseSchema.parse(responseValue);
  if (response.delivery.state !== 'ready') return;
  const chunks = liveResultChunks(response.delivery.text);
  if (!Number.isSafeInteger(chunkIndex) || chunkIndex < 0 || chunkIndex >= chunks.length || chunks[chunkIndex] !== update.text)
    throw new Error('selected_publication_render_changed');
  const saved = journal(call);
  const intent: BrokerPublicationRecord = { ...identity, publicationSequence: saved.records.length + 1,
    updateId: update.id, requestId: response.requestId,
    requestState: response.delivery.source.kind === 'question' ? 'waiting_for_reply' : 'completed',
    kind: 'result', speak: true, textDigest: createHash('sha256').update(update.text, 'utf8').digest('hex'),
    callerRevision: snapshot.callerRevision, transcriptSequence: sequence(snapshot),
    selectedDelivery: { response, chunkIndex, chunkCount: chunks.length } };
  if (!selectedPublicationMatches(call, intent, true)) return;
  const prior = saved.records.find(record => record.sessionId === identity.sessionId && record.updateId === update.id);
  if (prior) {
    const { publicationSequence: _priorOrder, outcome: _priorOutcome, transcriptSequence: _priorFrontier, ...original } = prior;
    const { publicationSequence: _nextOrder, transcriptSequence: _nextFrontier, ...retry } = intent;
    if (!equal(original, retry)) throw new Error('publication_identity_changed');
    return { record: prior, created: false };
  }
  store.update(call.id, { publications: { ...saved, records: [...saved.records, intent] } });
  return { record: intent, created: true };
}

/** The complete ordered receipt set must retain the very same API response. */
export function selectedQuestionPublicationReceiptsAccepted(call: BrokerCall,
  response: z.infer<typeof eligibleAssociatedDeliveryResponseSchema>, sessionId: string,
  receipts: readonly BrokerPublicationReceipt[], historical = false): boolean {
  const candidate = historical ? { ...call, publications: { records: call.publications?.records ?? [], rejections: [] } } : call;
  if (!brokerPublicationReceiptsAccepted(candidate, response.requestId, sessionId, receipts)) return false;
  if (response.delivery.state !== 'ready') return false;
  let chunks: string[];
  try { chunks = liveResultChunks(response.delivery.text); } catch { return false; }
  return receipts.length === chunks.length && receipts.every((receipt, index) => {
    const record = candidate.publications!.records.find(item => item.publicationSequence === receipt.publicationSequence)!;
    return selectedPublicationMatches(candidate, record, false) && record.selectedDelivery!.chunkIndex === index
      && record.selectedDelivery!.chunkCount === chunks.length && equal(record.selectedDelivery!.response, response);
  });
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
  const knownSession = call && (sameCall(call, identity) || (call.id === identity.callId && call.callSid === identity.callSid
    && call.primary?.bindingId === identity.bindingId && call.liveGenerations?.segments.some(segment => segment.sessionId === identity.sessionId)));
  if (!call || !knownSession || !commandId(eventId)) return;
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
    if (!sameCall(call, record) || !requestMatches(call, record, record.requestId) || record.speak !== true
        || !['result', 'progress'].includes(record.kind) || !commandId(receipt.eventId)
        || receipt.callerRevision !== priorRevision || record.transcriptSequence >= firstSequence
        || receipt.transcriptSequence < record.transcriptSequence || receipt.transcriptSequence >= firstSequence
        || saved.records.some(other => other !== record && other.sessionId === record.sessionId && other.outcome?.eventId === receipt.eventId)) return;
  }
  const requests = new Set(accepted.map(record => record.requestId));
  if (requests.size !== 1) return;
  return { relatedPrimaryRequestId: accepted[0].requestId, referenceOrigin: 'broker_status_publication' };
}
