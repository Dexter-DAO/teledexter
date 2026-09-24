import { createPrimaryBackend, liveResultChunks, type BrokerDelegationRecord, type PrimaryBackend } from './backend.js';
import { createLiveGenerationController, LIVE_GENERATION_STARTUP_MS, type GenerationHandle, type GenerationPublication, type LiveGenerationAttachment } from './liveGeneration.js';
import { prepareEligibleHistory } from './eligibleHistory.js';
import { eligibleAssociatedDeliveryResponseSchema, type EligibleContextClient, type EligibleContextBinding } from './eligibleContext.js';
import type { PrimaryApiClient } from './primaryApi.js';
import { terminalCallStatuses, type BrokerCall, type BrokerStore } from './store.js';
import { beginBrokerPublication, beginSelectedQuestionPublication, selectedQuestionPublicationReceiptsAccepted,
  settleBrokerPublication, rejectBrokerPublication, brokerPublicationReceiptsAccepted, type BrokerPublicationReceipt } from './publications.js';
import { requestDeliveryRequestForContinuation, validateCallContinuation } from './callContinuation.js';
import type { SelectedQuestionAssociationClient, SelectedQuestionSeed } from './selectedQuestion.js';
import type { RequestDeliveryClient } from './requestDelivery.js';
import { createSelectedQuestionVoiceAdapter, createSelectedQuestionRecovery, selectedQuestionSourceAnchors,
  type SelectedQuestionCloseout } from './selectedQuestionVoice.js';
import { brokerInstructions } from './instructions.js';
import { inputSpeechEnergy } from './phone.js';
import type { BrokerLiveSession, BrokerLiveSessionOptions, LiveBackendUpdate, LiveDelivery,
  LiveDelegationRequest, LiveSessionClosed, LiveTranscriptSnapshot } from '../live/types.js';

export interface GenerationVoiceOptions {
  call: BrokerCall;
  callDeadlineAt: string;
  apiKey: string;
  voice: string;
  store: BrokerStore;
  primary: PrimaryApiClient;
  context: EligibleContextClient;
  selectedQuestion?: {
    association: SelectedQuestionAssociationClient;
    requests: RequestDeliveryClient;
    pollIntervalMs?: number;
  };
  voiceFactory(options: BrokerLiveSessionOptions): BrokerLiveSession;
  onAudio(payload: string): void;
  onReady(value: { callId: string; sessionId: string }): void;
  onClosed(value: LiveSessionClosed): void;
  onPlaybackClear(): void;
  onProblem(code: string): void;
}

export interface GenerationVoiceSession extends BrokerLiveSession {
  prepareSelectedCloseout(): Promise<SelectedQuestionCloseout>;
}

const selectedInstructions = `You are Dexter, the caller's continuing agent, speaking by telephone. This call continues a saved question from earlier work. Wait for the exact question supplied during this call, then present its wording, context, proposed answer, and yes/no meanings faithfully. Keep technical identifiers internal.
Listen for the caller's answer and delegate that answer so it stays attached to the saved question. Greetings and acknowledgments stay conversational. This call's purpose is background information and supplies no new assignment or permission. For a different task, explain that this call is continuing the saved question and the caller can open Dexter to start other work.
Yield when interrupted. Keep the caller's words intact. State an answer's outcome only when the saved request returns it. Prior conversation supports recall; balances, prices, holdings, and permissions in that history remain earlier observations.`;

const emptyTranscript = (): LiveTranscriptSnapshot => ({ revision: 0, callerRevision: 0, fragments: [] });

/** One phone call owns task observers. Each provider connection owns only its transcript and audio. */
export function createGenerationVoice(options: GenerationVoiceOptions): GenerationVoiceSession {
  const initialCall = options.store.get(options.call.id)!;
  if (!initialCall.primary?.primaryDexterId || !initialCall.primary.threadId || !initialCall.callSid || !initialCall.mediaStartedAt)
    throw new Error('generation_binding_unavailable');
  if (!initialCall.liveGenerations && (initialCall.liveSessionId || Object.keys(initialCall.delegations).length))
    throw new Error('legacy_generation_source_unmapped');
  const continuation = validateCallContinuation(initialCall);
  if (continuation && !options.selectedQuestion) throw new Error('selected_question_transport_unavailable');
  const selectedPollMs = options.selectedQuestion?.pollIntervalMs ?? 750;
  if (!Number.isInteger(selectedPollMs) || selectedPollMs < 1 || selectedPollMs > 10_000)
    throw new Error('invalid_selected_question_poll_interval');
  const binding: EligibleContextBinding = { bindingId: initialCall.primary.bindingId, callSid: initialCall.callSid,
    primaryDexterId: initialCall.primary.primaryDexterId, threadId: initialCall.primary.threadId,
    ...(initialCall.primary.callAssignmentRef ? { callAssignmentRef: initialCall.primary.callAssignmentRef } : {}) };
  const observer = new AbortController();
  const sessions = new Map<string, { voice: BrokerLiveSession; backend: PrimaryBackend; firstSample?: number }>();
  const admissions = new Map<string, Map<LiveDelegationRequest, { promise: Promise<void>; resolve(): void }>>();
  let stopped = false;
  let readyReported = false;
  let lastSpeechAt = 0;
  let inputNextSample = initialCall.liveGenerations?.inputNextSample ?? 0;
  let closing: Promise<LiveSessionClosed> | undefined;
  let finalResult: LiveSessionClosed | undefined;
  let selected: ReturnType<typeof createSelectedQuestionVoiceAdapter> | undefined;
  let selectedTimer: ReturnType<typeof setTimeout> | undefined;
  let selectedClosing = false;
  let selectedTerminalDelivered = false;
  const selectedNotices = new Set<string>();
  const call = () => options.store.get(initialCall.id)!;
  const current = () => {
    const attachment = controller.snapshot().deliveryAttachment;
    return attachment ? { attachment, session: sessions.get(attachment.sessionId) } : undefined;
  };
  const closedDelivery = (): LiveDelivery => ({ eventId: '', status: 'closed' });

  const selectedActive = () => !stopped && !selectedClosing && !observer.signal.aborted && !call().endedAt
    && !call().primaryClosed && !terminalCallStatuses.has(call().status)
    && Date.now() < Date.parse(initialCall.liveGenerations?.clocks.callDeadlineAt ?? options.callDeadlineAt);
  const selectedNotice = async (state: 'failed' | 'cancelled' | 'unconfirmed') => {
    if (!selectedActive() || selectedNotices.has(state)) return;
    selectedNotices.add(state);
    const attachment = await controller.beforeReuse();
    if (!selectedActive()) return;
    const session = sessions.get(attachment.sessionId);
    if (!session?.voice.appendCommentary) return;
    const result = await session.voice.appendCommentary(state === 'failed' ? 'I could not get an answer to that request.'
      : state === 'cancelled' ? 'The request was cancelled.' : 'I could not check the saved request yet.');
    if (state !== 'unconfirmed' && result.status === 'accepted') selectedTerminalDelivered = true;
  };

  const publishSelected = async (result: Parameters<GenerationHandle['publish']>[0], attachment: LiveGenerationAttachment): Promise<GenerationPublication> => {
    const response = eligibleAssociatedDeliveryResponseSchema.parse({ bindingId: binding.bindingId, callSid: binding.callSid,
      requestId: result.requestId, version: result.version, delivery: result.delivery, deliveryAssociation: result.deliveryAssociation });
    const session = sessions.get(attachment.sessionId);
    if (!session?.voice.appendCommentary) throw new Error('commentary_transport_unavailable');
    const start = session.voice.getTranscript(), spokenAt = lastSpeechAt;
    const interrupted = (): LiveDelivery['status'] | undefined => !selectedActive() ? 'closed'
      : controller.snapshot().deliveryAttachment?.generationId !== attachment.generationId
        || session.voice.getTranscript().callerRevision !== start.callerRevision || lastSpeechAt !== spokenAt
        || ('question' in result.delivery && Boolean(selected?.pendingSourceCount())) ? 'stale' : undefined;
    let chunks: string[];
    try { chunks = liveResultChunks(result.delivery.text); }
    catch {
      const key = `format_${response.deliveryAssociation.id}_${result.delivery.source.messageId}`;
      if (!interrupted() && !selectedNotices.has(key)) {
        selectedNotices.add(key);
        try { await session.voice.appendCommentary('question' in result.delivery
          ? 'I cannot read this question accurately over the phone. Please open it in Dexter.'
          : 'I received the result, but cannot read that format accurately over this call. The full result has been saved.'); }
        catch { /* The question remains unpresented. */ }
      }
      return { eventId: '', status: 'unconfirmed', reason: 'result_format_unsupported' };
    }
    const prefix = `selected_${response.deliveryAssociation.id}_${result.delivery.source.messageId}_caller_${start.callerRevision}`;
    const previous = call().publications?.records.filter(record => record.sessionId === attachment.sessionId
      && record.updateId.startsWith(`${prefix}_attempt_`)) ?? [];
    let attempt = Number(previous.at(-1)?.updateId.match(/_attempt_(\d+)_\d+$/)?.[1] ?? 1);
    if (previous.some(record => record.updateId.startsWith(`${prefix}_attempt_${attempt}_`) && record.outcome
      && (record.outcome.status === 'rejected' || call().publications?.rejections.some(rejection =>
        rejection.sessionId === record.sessionId && rejection.eventId === record.outcome!.eventId)))) attempt++;
    let receipt: LiveDelivery = { eventId: '', status: 'unconfirmed' };
    const receipts: BrokerPublicationReceipt[] = [];
    for (const [chunkIndex, text] of chunks.entries()) {
      const halted = interrupted(); if (halted) return { ...receipt, status: halted };
      const intent = beginSelectedQuestionPublication(options.store, { callId: initialCall.id, callSid: binding.callSid,
        bindingId: binding.bindingId, sessionId: attachment.sessionId }, response,
      { id: `${prefix}_attempt_${attempt}_${chunkIndex}`, kind: 'result', text, speak: true,
        expectedCallerRevision: start.callerRevision }, session.voice.getTranscript(), chunkIndex);
      if (!intent) return { eventId: '', status: 'rejected', reason: 'selected_publication_unqualified' };
      if (!intent.created) receipt = intent.record.outcome
        ? { eventId: intent.record.outcome.eventId, status: intent.record.outcome.status,
          observedCallerRevision: intent.record.outcome.callerRevision, observedTranscriptSequence: intent.record.outcome.transcriptSequence }
        : { eventId: '', status: 'unconfirmed' };
      else {
        try { receipt = await session.voice.appendCommentary(text); }
        catch { receipt = { eventId: '', status: 'unconfirmed' }; }
        settleBrokerPublication(options.store, intent.record, receipt, session.voice.getTranscript(), !selectedActive());
      }
      const status = interrupted() ?? receipt.status;
      if (status !== 'accepted') return { ...receipt, status };
      receipts.push({ publicationSequence: intent.record.publicationSequence, sessionId: attachment.sessionId, eventId: receipt.eventId });
    }
    if (!selectedQuestionPublicationReceiptsAccepted(call(), response, attachment.sessionId, receipts))
      return { ...receipt, status: 'rejected', reason: 'publication_rejected' };
    return { ...receipt, publicationReceipts: receipts };
  };

  const publishText = async (entry: BrokerDelegationRecord, attachment: LiveGenerationAttachment,
    text: string, kind: 'progress' | 'result', updateId: string): Promise<GenerationPublication> => {
    const session = sessions.get(attachment.sessionId);
    if (!session?.voice.appendCommentary) throw new Error('commentary_transport_unavailable');
    const start = session.voice.getTranscript();
    const spokenAt = lastSpeechAt;
    const interrupted = (): LiveDelivery['status'] | undefined => stopped || observer.signal.aborted || call().endedAt ? 'closed'
      : controller.snapshot().deliveryAttachment?.generationId !== attachment.generationId
        || session.voice.getTranscript().callerRevision !== start.callerRevision || lastSpeechAt !== spokenAt ? 'stale' : undefined;
    let receipt: LiveDelivery = { eventId: '', status: 'unconfirmed' };
    const publicationReceipts: BrokerPublicationReceipt[] = [];
    const prefix = `${updateId}_caller_${start.callerRevision}`;
    const previous = call().publications?.records.filter(value => value.sessionId === attachment.sessionId
      && value.updateId.startsWith(`${prefix}_attempt_`)) ?? [];
    const lastAttempt = previous.at(-1)?.updateId.match(/_attempt_(\d+)_\d+$/)?.[1];
    let attempt = lastAttempt === undefined ? 1 : Number(lastAttempt);
    if (previous.some(value => value.updateId.startsWith(`${prefix}_attempt_${attempt}_`)
      && value.outcome && (value.outcome.status === 'rejected'
        || call().publications?.rejections.some(rejection => rejection.sessionId === value.sessionId
          && rejection.eventId === value.outcome!.eventId)))) attempt++;

    let chunks: string[];
    try { chunks = liveResultChunks(text); }
    catch {
      const notice = await publishText(entry, attachment, entry.status === 'waiting_for_reply'
        ? 'I cannot read this question accurately over the phone. Please open it in Dexter.'
        : 'I received the result, but cannot read that format accurately over this call. The full result has been saved.',
      'progress', `${updateId}_format_notice`);
      return { ...notice, status: notice.status === 'accepted' ? 'unconfirmed' : notice.status, reason: 'result_format_unsupported' };
    }
    for (const [index, chunk] of chunks.entries()) {
      const halted = interrupted();
      if (halted) return { eventId: receipt.eventId, status: halted };
      const snapshot = session.voice.getTranscript();
      const update: LiveBackendUpdate = { id: `${prefix}_attempt_${attempt}_${index}`, kind, text: chunk, speak: true,
        expectedCallerRevision: start.callerRevision };
      const identity = { callId: initialCall.id, callSid: binding.callSid, bindingId: binding.bindingId,
        sessionId: attachment.sessionId, ...(entry.sessionId !== attachment.sessionId ? { sourceSessionId: entry.sessionId } : {}) };
      const intent = beginBrokerPublication(options.store, identity, entry.requestId, update, snapshot);
      // Unknown submissions lack a confirmed run. Their fixed local status carries no related-request authority.
      if (intent && !intent.created) receipt = intent.record.outcome
        ? { eventId: intent.record.outcome.eventId, status: intent.record.outcome.status,
          observedCallerRevision: intent.record.outcome.callerRevision, observedTranscriptSequence: intent.record.outcome.transcriptSequence }
        : { eventId: '', status: 'unconfirmed' };
      else {
        try { receipt = await session.voice.appendCommentary(chunk); }
        catch { receipt = { eventId: '', status: 'unconfirmed' }; }
        const after = session.voice.getTranscript();
        if (intent) settleBrokerPublication(options.store, intent.record, receipt, after, stopped);
      }
      const status = interrupted() ?? receipt.status;
      if (status !== 'accepted') return { ...receipt, status };
      if (intent) publicationReceipts.push({ publicationSequence: intent.record.publicationSequence,
        sessionId: attachment.sessionId, eventId: receipt.eventId });
    }
    if (kind === 'result' && publicationReceipts.length && (!brokerPublicationReceiptsAccepted(call(), entry.requestId, attachment.sessionId, publicationReceipts)
      || publicationReceipts.length !== chunks.length)) return { ...receipt, status: 'rejected', reason: 'publication_rejected' };
    return { ...receipt, publicationReceipts };
  };

  const publish = async (entry: BrokerDelegationRecord, update?: LiveBackendUpdate) => {
    if (stopped || observer.signal.aborted || call().endedAt) return { delivery: closedDelivery() };
    const canonicalRequestId = entry.reconciliation?.outcome === 'admitted' ? entry.reconciliation.canonicalRequestId : entry.requestId;
    let attachment: LiveGenerationAttachment;
    let delivery: GenerationPublication;
    if (!update && (entry.status === 'completed' || entry.status === 'waiting_for_reply')) {
      const source = entry.status === 'completed' && entry.primaryResult?.assistant
        ? { kind: 'assistant' as const, messageId: entry.primaryResult.assistant.messageId }
        : entry.currentQuestionId ? { kind: 'question' as const, questionId: entry.currentQuestionId, questionRevision: 1 as const } : undefined;
      if (!source) return { delivery: { eventId: '', status: 'unconfirmed' as const, reason: 'eligible_source_unavailable' } };
      const outcome = await controller.deliver(canonicalRequestId, source);
      attachment = outcome.attachment;
      delivery = outcome.publication ?? { eventId: '', status: 'unconfirmed', reason: outcome.response.delivery.state === 'unavailable'
        ? outcome.response.delivery.reason : 'publication_unconfirmed' };
    } else {
      attachment = await controller.beforeReuse();
      const text = update?.text ?? (entry.status === 'failed' ? 'I could not get an answer to that request.'
        : entry.status === 'cancelled' ? 'The request was cancelled.' : 'I am checking the saved request status.');
      delivery = await publishText(entry, attachment, text, update?.kind ?? 'result', update?.id ?? `${entry.requestId}_${entry.status}`);
    }
    const segment = controller.snapshot().segments.find(value => value.generationId === attachment.generationId)!;
    return { delivery, publicationReceipts: delivery.publicationReceipts, sessionId: attachment.sessionId, snapshot: sessions.get(attachment.sessionId)?.voice.getTranscript(),
      inputThroughMs: segment.inputFirstSample === undefined ? 0
        : (segment.inputLastSampleExclusive! - segment.inputFirstSample) / 8 };
  };

  const controller = createLiveGenerationController({ callId: initialCall.id, binding,
    clocks: initialCall.liveGenerations?.clocks ?? { callDeadlineAt: options.callDeadlineAt, mediaStartedAt: initialCall.mediaStartedAt,
      ...(initialCall.inbound?.remote?.state === 'bound' ? { consumedAt: initialCall.inbound.remote.consumedAt,
        mediaStartBy: initialCall.inbound.mediaStartBy! } : {}) },
    initial: initialCall.liveGenerations, context: options.context,
    persist(journal) {
      options.store.update(initialCall.id, { liveGenerations: journal,
        ...(journal.deliveryAttachment ? { liveSessionId: journal.deliveryAttachment.sessionId } : {}) });
    },
    onAudio: options.onAudio,
    clearPlayback: options.onPlaybackClear,
    onProblem(code) { options.onProblem(code); },
    async beforeRetire(generationId) {
      const pending = admissions.get(generationId);
      if (!pending?.size || observer.signal.aborted) return;
      let release!: () => void;
      const aborted = new Promise<void>(resolve => { release = resolve; observer.signal.addEventListener('abort', resolveAbort, { once: true }); });
      function resolveAbort() { release(); }
      try { await Promise.race([Promise.all([...pending.values()].map(value => value.promise)), aborted]); }
      finally { observer.signal.removeEventListener('abort', resolveAbort); }
    },
    open(input) {
      const prepared = prepareEligibleHistory(input.context, binding);
      return new Promise<GenerationHandle>((resolve, reject) => {
        let voice: BrokerLiveSession;
        let backend: PrimaryBackend | undefined;
        let sessionId: string | undefined;
        let ready = false;
        let startCancelled = false;
        let sourceSpeechAt = 0;
        const pending = new Map<LiveDelegationRequest, { promise: Promise<void>; resolve(): void }>();
        admissions.set(input.generationId, pending);
        const rejectAfterClose = (reason: string) => {
          const error = new Error(reason);
          if (!voice) { reject(error); return; }
          void voice.close(reason).then(() => reject(error), () => reject(error));
        };
        const cancelStart = () => {
          if (ready || startCancelled) return;
          startCancelled = true;
          clearTimeout(startTimer);
          input.signal.removeEventListener('abort', cancelStart);
          rejectAfterClose('generation_start_cancelled');
        };
        const startTimer = setTimeout(() => {
          startCancelled = true;
          input.signal.removeEventListener('abort', cancelStart);
          rejectAfterClose('generation_start_timeout');
        }, LIVE_GENERATION_STARTUP_MS);
        startTimer.unref();
        input.signal.addEventListener('abort', cancelStart, { once: true });
        if (input.signal.aborted) { cancelStart(); return; }
        try {
          voice = options.voiceFactory({ apiKey: options.apiKey, callId: initialCall.id, voice: options.voice,
            eligibleHistory: prepared,
            instructions: continuation ? selectedInstructions : brokerInstructions({ direction: initialCall.direction, purpose: initialCall.purpose,
              registeredAssignment: input.allowOpening && !initialCall.voiceOpening && Boolean(initialCall.callAssignment),
              ...(initialCall.callAssignment?.origin === 'authenticated_hark_delivery' ? { watch: initialCall.callAssignment.value.hark } : {}),
              name: input.context.context.displayName }),
            backend: { async delegate(request, channel) {
              if (!ready || !backend || request.sessionId !== sessionId
                || controller.snapshot().deliveryAttachment?.sessionId !== sessionId) throw new Error('generation_source_not_ready');
              // The backend freezes this generation's caller source before observing durable work.
              let resolveAdmission!: () => void;
              const promise = new Promise<void>(resolve => { resolveAdmission = resolve; });
              pending.set(request, { promise, resolve: resolveAdmission });
              try {
                if (continuation) {
                  if (!selected || !selectedActive()) throw new Error('selected_question_not_ready');
                  await selected.delegate(request, channel);
                } else await backend.delegate(request, channel);
              }
              finally { pending.get(request)?.resolve(); pending.delete(request); }
            } },
            onAudio(payload) { input.callbacks.onAudio(payload); },
            onTranscript(snapshot) { input.callbacks.onTranscript(snapshot); backend?.observeTranscript(snapshot); },
            onReady(value) {
              if (ready) return;
              if (startCancelled || input.signal.aborted || stopped) {
                cancelStart(); void voice.close('generation_start_cancelled'); return;
              }
              if (sessions.has(value.sessionId) || controller.snapshot().segments.some(segment => segment.sessionId === value.sessionId)) {
                startCancelled = true; clearTimeout(startTimer); input.signal.removeEventListener('abort', cancelStart);
                void voice.close('generation_session_reused');
                reject(new Error('generation_session_reused')); return;
              }
              sessionId = value.sessionId;
              backend = createPrimaryBackend({ callId: initialCall.id, callSid: binding.callSid, bindingId: binding.bindingId,
                api: options.primary, store: options.store, sourceSessionId: sessionId, watchUntilCallEnds: true,
                admissionSettled(request) { pending.get(request)?.resolve(); pending.delete(request); },
                lastCallerSpeechAt: () => sourceSpeechAt,
                ...(continuation ? { selectedQuestionSources: () => selectedQuestionSourceAnchors(call()) }
                  : { observation: { signal: observer.signal, publish } }) });
              const record = { voice, backend, firstSample: undefined as number | undefined };
              sessions.set(sessionId, record);
              ready = true; clearTimeout(startTimer); input.signal.removeEventListener('abort', cancelStart);
              resolve({ sessionId,
                appendAudio(frame) {
                  record.firstSample ??= frame.callInputStartSample;
                  const bytes = Buffer.from(frame.payload, 'base64');
                  const accepted = voice.appendAudio(frame.payload);
                  if (accepted && inputSpeechEnergy(bytes) > 0.012) {
                    sourceSpeechAt = Math.max(sourceSpeechAt, frame.observedAt ?? Date.now());
                    backend!.noteCallerSpeech(sourceSpeechAt, (frame.callInputStartSample + bytes.length - record.firstSample) / 8);
                  }
                  return accepted;
                },
                async publish(result) {
                  const attachment = controller.snapshot().deliveryAttachment;
                  if (!attachment || attachment.sessionId !== sessionId) return closedDelivery();
                  if (continuation) return publishSelected(result, attachment);
                  const entries = Object.values(call().delegations) as BrokerDelegationRecord[];
                  const entry = entries.find(value => value.requestId === result.requestId || (value.reconciliation?.outcome === 'admitted'
                    && value.reconciliation.canonicalRequestId === result.requestId));
                  if (!entry) throw new Error('generation_result_request_unknown');
                  if ('question' in result.delivery && (entry.currentQuestionId !== result.delivery.question.id
                    || entry.questions?.[result.delivery.question.id]?.question.runId !== result.delivery.question.runId))
                    throw new Error('generation_question_identity_changed');
                  return publishText(entry, attachment, result.delivery.text, 'result', `${entry.requestId}_${result.delivery.source.messageId}`);
                },
                close(reason) { backend!.persistAcousticEvidence(); return voice.close(reason); },
              });
            },
            onClosed(result) {
              clearTimeout(startTimer); input.signal.removeEventListener('abort', cancelStart); finalResult = result;
              input.callbacks.onClosed(result);
              if (!ready) { startCancelled = true; reject(new Error('generation_closed_before_ready')); }
            },
            onPlaybackClear() {
              if (controller.snapshot().deliveryAttachment?.generationId === input.generationId) options.onPlaybackClear();
            },
            onEvent(event) {
              if (event.type === 'live.append.late_rejection' && sessionId && typeof event.eventId === 'string')
                rejectBrokerPublication(options.store, { callId: initialCall.id, callSid: binding.callSid,
                  bindingId: binding.bindingId, sessionId }, event.eventId, typeof event.code === 'string' ? event.code : 'provider_error');
              if (event.type === 'live.error' && controller.snapshot().deliveryAttachment?.generationId === input.generationId)
                options.onProblem(String(event.code ?? 'live_error'));
            },
          });
        } catch (error) {
          startCancelled = true; clearTimeout(startTimer); input.signal.removeEventListener('abort', cancelStart); reject(error);
        }
      });
    },
  });

  const beginSelected = (attachment: LiveGenerationAttachment) => {
    if (!continuation || !options.selectedQuestion) return;
    const saved = call();
    const generations = controller.snapshot();
    const seed: SelectedQuestionSeed = saved.selectedQuestion?.initial ?? {
      v: 'broker.selected-question-seed.v1', deliveryCall: { callId: saved.id, sessionId: attachment.sessionId },
      delivery: { bindingId: binding.bindingId, callSid: binding.callSid },
      scope: { ownerPrincipalId: generations.binding.ownerPrincipalId!, primaryDexterId: binding.primaryDexterId, threadId: binding.threadId },
      request: requestDeliveryRequestForContinuation(continuation), callDeadlineAt: generations.clocks.callDeadlineAt,
    };
    selected = createSelectedQuestionVoiceAdapter({ seed,
      readCall: call, load: () => call().selectedQuestion,
      persist: value => { options.store.recordSelectedQuestion(saved.id, value); },
      loadVoiceEvidence: () => call().selectedQuestionVoice,
      persistVoiceEvidence: value => { options.store.recordSelectedQuestionVoice(saved.id, value); },
      active: selectedActive, association: options.selectedQuestion.association, requests: options.selectedQuestion.requests,
      capture: (bindingId, turn) => options.primary.captureTurn(bindingId, turn),
      freezer: { async freeze(request, channel, usedSourceIds) {
        const source = sessions.get(request.sessionId);
        if (!source) throw new Error('selected_question_source_missing');
        return source.backend.freezeCallerTurn(request, channel, usedSourceIds);
      } },
      admissionSettled(request) {
        const generation = controller.snapshot().segments.find(segment => segment.sessionId === request.sessionId);
        const pending = generation ? admissions.get(generation.generationId) : undefined;
        pending?.get(request)?.resolve(); pending?.delete(request);
      },
      async deliver(journal, target) {
        const outcome = await controller.deliver(journal.request.originalRequestId, target, journal);
        const session = sessions.get(outcome.attachment.sessionId);
        // Streaming input advances the in-memory frontier between journal saves.
        // The presentation and its durable segment must observe this same boundary.
        const generations = controller.snapshot();
        options.store.update(saved.id, { liveGenerations: generations,
          ...(generations.deliveryAttachment ? { liveSessionId: generations.deliveryAttachment.sessionId } : {}) });
        const segment = generations.segments.find(value => value.generationId === outcome.attachment.generationId)!;
        if (!session) throw new Error('selected_question_delivery_session_missing');
        if (target.kind === 'assistant' && outcome.publication?.status === 'accepted') selectedTerminalDelivered = true;
        return { outcome, snapshot: session.voice.getTranscript(), acceptedAt: new Date().toISOString(),
          inputThroughMs: segment.inputFirstSample === undefined ? 0
            : (segment.inputLastSampleExclusive! - segment.inputFirstSample) / 8 };
      },
    });
    const observe = async (first: boolean) => {
      if (!selectedActive() || !selected) return;
      try {
        if (first) await selected.host.start();
        else if (selected.pendingSourceCount() === 0) {
          const result = await selected.host.poll();
          if (result.state === 'failed' || result.state === 'cancelled') await selectedNotice(result.state);
        }
      } catch {
        if (selectedActive()) {
          try { await selectedNotice('unconfirmed'); } catch { /* Retry observation within the original call deadline. */ }
        }
      } finally {
        if (selectedActive() && !selectedTerminalDelivered) {
          selectedTimer = setTimeout(() => { selectedTimer = undefined; void observe(false); }, selectedPollMs);
          selectedTimer.unref();
        }
      }
    };
    void observe(true);
  };

  const stopSelected = () => {
    selectedClosing = true; clearTimeout(selectedTimer); selectedTimer = undefined;
    selected?.stop();
  };

  // Starting is asynchronous so the server installs its call owner before any callbacks run.
  queueMicrotask(() => { void controller.start().then(attachment => {
    if (!stopped) beginSelected(attachment);
    if (!stopped && !readyReported) { readyReported = true; options.onReady({ callId: initialCall.id, sessionId: attachment.sessionId }); }
  }).catch(error => { options.onProblem(error instanceof Error ? error.message : 'generation_start_failed'); }); });
  return {
    async prepareSelectedCloseout() {
      stopSelected();
      if (selected) return selected.prepareCloseout();
      if (continuation && options.selectedQuestion) return createSelectedQuestionRecovery({ callId: initialCall.id,
        store: options.store, primary: options.primary, requests: options.selectedQuestion.requests }).prepareCloseout();
      return { pendingAdmissions: 0, pendingCaptures: 0, pendingReplies: 0, ready: true };
    },
    appendAudio(payload) {
      const bytes = Buffer.from(payload, 'base64');
      const observedAt = Date.now();
      if (inputSpeechEnergy(bytes) > 0.012) lastSpeechAt = observedAt;
      const outcome = controller.appendAudio({ payload, callInputStartSample: inputNextSample, observedAt });
      inputNextSample = controller.snapshot().inputNextSample;
      return outcome !== 'refused';
    },
    appendContext: async () => ({ eventId: '', status: 'rejected', reason: 'eligible_context_required' }),
    async appendInstructions(text) {
      const attachment = await controller.beforeReuse();
      return sessions.get(attachment.sessionId)!.voice.appendInstructions(text);
    },
    async interruptPlayback(instruction) {
      const attachment = await controller.beforeReuse();
      return sessions.get(attachment.sessionId)!.voice.interruptPlayback(instruction);
    },
    resumePlayback() { current()?.session?.voice.resumePlayback(); },
    getTranscript() { return current()?.session?.voice.getTranscript() ?? emptyTranscript(); },
    close(reason = 'call_stopped') {
      if (closing) return closing;
      stopped = true; observer.abort();
      // stop gates output synchronously. The server performs provider hangup before awaiting this promise.
      const cleanup = controller.stop();
      stopSelected();
      closing = cleanup.then(() => {
        const segments = controller.snapshot().segments;
        const result: LiveSessionClosed = { ...finalResult, callId: initialCall.id, reason,
          finalized: segments.length > 0 && segments.every(segment => segment.providerFinalized === true),
          transcriptPersisted: segments.length > 0 && segments.every(segment => segment.transcriptPersisted === true),
          outputAudioDiscarded: segments.some(segment => segment.outputDiscarded) };
        options.onClosed({ ...result, reason });
        return { ...result, reason };
      });
      return closing;
    },
  };
}
