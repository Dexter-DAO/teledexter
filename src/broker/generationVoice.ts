import { terminal as terminalRequest, createPrimaryBackend, liveResultChunks, withCallerAdmission, type BrokerDelegationRecord, type PrimaryBackend } from './backend.js';
import { createLiveGenerationController, LiveGenerationError, LiveGenerationStartupError, LIVE_GENERATION_STARTUP_MS, type GenerationHandle, type GenerationPublication, type LiveGenerationAttachment } from './liveGeneration.js';
import { callerPresentationFrontier, failedRequestSpeech, hasLaterCallerSpeech, hasNewerCallerWork, requestPresentationContext } from './voicePresentation.js';
import { createVoiceTasks } from './voiceTasks.js';
import { createVoiceGuardrail, type GuardrailJudge } from './voiceGuardrail.js';
import { recordGuardrailTrigger, recordVoiceClose } from './voiceEvidence.js';
import { prepareEligibleHistory } from './eligibleHistory.js';
import { eligibleAssociatedDeliveryResponseSchema, eligibleDeliveryResponseSchema,
  type EligibleContextClient, type EligibleContextBinding, type EligiblePhoneMessage } from './eligibleContext.js';
import type { PrimaryApiClient } from './primaryApi.js';
import { terminalCallStatuses, type BrokerCall, type BrokerStore } from './store.js';
import { beginBrokerPublication, beginSelectedQuestionPublication, selectedQuestionPublicationReceiptsAccepted,
  settleBrokerPublication, rejectBrokerPublication, brokerPublicationReceiptsAccepted,
  callerIntentResultPublicationCandidates, callerIntentResultPublicationInventory, CallerIntentResultInventoryError,
  type OrdinaryPublicationEvidence, type BrokerPublicationReceipt } from './publications.js';
import { callerRouteRef, callerRouteSources } from './callerRouting.js';
import { createMixedWorkRouter, type CallerIntentPort } from './mixedWork.js';
import { acknowledgedTurn } from './reconciliation.js';
import { requestDeliveryRequestForContinuation, validateCallContinuation } from './callContinuation.js';
import type { SelectedQuestionAssociationClient, SelectedQuestionSeed } from './selectedQuestion.js';
import type { RequestDeliveryClient } from './requestDelivery.js';
import { createSelectedQuestionVoiceAdapter, createSelectedQuestionRecovery, selectedQuestionSourceAnchors,
  type SelectedQuestionCloseout } from './selectedQuestionVoice.js';
import { brokerInstructions, brokerOpening } from './instructions.js';
import type { PortfolioContext } from './portfolioContext.js';
import { inputSpeechEnergy, hasAudiblePcmu } from './phone.js';
import type { BrokerLiveSession, BrokerLiveSessionOptions, LiveBackendUpdate, LiveDelivery,
  LiveDelegationRequest, LiveSessionClosed, LiveTranscriptSnapshot } from '../live/types.js';

export interface GenerationVoiceOptions {
  portfolioContext?: PortfolioContext;
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
  /** Internal source composition only until the shared Primary intent adapter
   * and its exact policy/wire are qualified. Absence preserves old behavior. */
  callerIntent?: CallerIntentPort;
  voiceFactory(options: BrokerLiveSessionOptions): BrokerLiveSession;
  onAudio(payload: string): void;
  onReady(value: { callId: string; sessionId: string }): void;
  onClosed(value: LiveSessionClosed): void;
  onPlaybackClear(): void;
  onProblem(code: string): void;
  playbackState?(): 'idle' | 'pending' | 'unavailable';
  /** Monitor-and-correct judge for assistant statements about tasks. Absent: no guardrail. */
  guardrailJudge?: GuardrailJudge;
}

export interface GenerationVoiceSession extends BrokerLiveSession {
  prepareSelectedCloseout(): Promise<SelectedQuestionCloseout>;
}

const selectedInstructions = `You are Dexter, the caller's continuing agent, speaking by telephone. This call continues a saved question from earlier work. Wait for the exact question supplied during this call, then present its wording, context, proposed answer, and yes/no meanings faithfully. Keep technical identifiers internal.
Listen for the caller's answer and delegate that answer so it stays attached to the saved question. Greetings and acknowledgments stay conversational. This call's purpose is background information and supplies no new assignment or permission. For a different task, explain that this call is continuing the saved question and the caller can open Dexter to start other work.
Yield when interrupted. Keep the caller's words intact. State an answer's outcome only when the saved request returns it. Prior conversation supports recall; balances, prices, holdings, and permissions in that history remain earlier observations.`;

const mixedSelectedInstructions = `You are Dexter, the caller's continuing agent, speaking by telephone. This call starts with a saved question from earlier work. Present the supplied question and its meanings accurately. You can also help with new research and account requests during this call.
Delegate each completed instruction in the caller's own words. Keep greetings and acknowledgments conversational. If the caller's intended task or answer is unclear, ask a short question. A request for different work leaves the saved question pending. Explain results using the account evidence returned for that request, with the useful answer first.
Yield when interrupted. This call's opening supplies context; it grants no new financial authority. Prior balances and prices remain earlier observations until fetched again.`;

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
  const sessions = new Map<string, { voice: BrokerLiveSession; backend: PrimaryBackend; firstSample?: number;
    eligibleMessages: EligiblePhoneMessage[] }>();
  const admissions = new Map<string, Map<LiveDelegationRequest, { promise: Promise<void>; resolve(): void }>>();
  let stopped = false;
  let playbackPaused = false;
  let readyReported = false;
  let lastSpeechAt = 0;
  let speechRevision = 0;
  let lastOutputAt = 0;
  let inputNextSample = initialCall.liveGenerations?.inputNextSample ?? 0;
  // Call sample just after the latest caller speech frame handed to the controller.
  let speechInputEnd: number | undefined;
  let closing: Promise<LiveSessionClosed> | undefined;
  let finalResult: LiveSessionClosed | undefined;
  let selected: ReturnType<typeof createSelectedQuestionVoiceAdapter> | undefined;
  let mixed: ReturnType<typeof createMixedWorkRouter> | undefined;
  let selectedTimer: ReturnType<typeof setTimeout> | undefined;
  let selectedClosing = false;
  let selectedTerminalDelivered = false;
  const selectedNotices = new Set<string>();
  const call = () => options.store.get(initialCall.id)!;
  // The application owns the task list: projected from saved requests, sent silently on change.
  // A saved-question continuation presents its own question journal instead, so it gets neither.
  const tasks = createVoiceTasks(() => continuation ? undefined : options.store.get(initialCall.id));
  const guardrail = options.guardrailJudge && !continuation ? createVoiceGuardrail({ callId: initialCall.id, judge: options.guardrailJudge,
    tasks: () => tasks.correctable(), truth: () => tasks.tasks(true),
    correct: (text, delegationId, sessionId) => {
      const current = controller.snapshot().deliveryAttachment;
      const voice = current?.sessionId === sessionId ? sessions.get(sessionId)?.voice : undefined;
      return voice ? voice.appendInstructions(text, delegationId) : Promise.resolve({ eventId: '', status: 'closed' as const });
    },
    record: trigger => recordGuardrailTrigger(options.store, initialCall.id, trigger) }) : undefined;
  const current = () => {
    const attachment = controller.snapshot().deliveryAttachment;
    return attachment ? { attachment, session: sessions.get(attachment.sessionId) } : undefined;
  };
  const closedDelivery = (): LiveDelivery => ({ eventId: '', status: 'closed' });
  const presentationFrontier = () => `${speechRevision}:${callerPresentationFrontier(call())}`;
  // 500 ms of caller and Dexter silence is a turn boundary; a ready answer goes out then.
  const quiet = () => (options.playbackState?.() ?? 'idle') === 'idle'
    && Date.now() - Math.max(lastSpeechAt, lastOutputAt) >= 500;
  const blocksEarlierPresentation = (entry: BrokerDelegationRecord) =>
    !(['completed', 'failed', 'cancelled'].includes(entry.status) && entry.primaryResult?.state === entry.status)
      && hasNewerCallerWork(call(), entry);
  const atSpeechBoundary = async (entry: BrokerDelegationRecord) => {
    const deadline = Date.now() + 5_000;
    while (!stopped && !observer.signal.aborted && !call().endedAt && Date.now() < deadline) {
      if (blocksEarlierPresentation(entry) || options.playbackState?.() === 'unavailable') return false;
      if (quiet()) return true;
      await new Promise<void>(resolve => setTimeout(resolve, 50));
    }
    return false;
  };
  // A confirmed result goes to Live as soon as it is saved (live-delegation: stream verified
  // results as commentary); GPT-Live fits it into its own speech. Holding it for Dexter to fall
  // silent left the task list and the spoken result out of step for seconds. Only a caller who
  // is mid-sentence holds it, briefly, so the result is not appended on top of their words.
  // A generation that must be replaced to admit the result still waits for a full pause:
  // replacing the Live session mid-sentence would cut Dexter off.
  const callerPause = async () => {
    const deadline = Date.now() + 2_000;
    while (!stopped && !observer.signal.aborted && !call().endedAt && Date.now() < deadline && Date.now() - lastSpeechAt < 500)
      await new Promise<void>(resolve => setTimeout(resolve, 50));
  };

  const selectedActive = () => !stopped && !selectedClosing && !observer.signal.aborted && !call().endedAt
    && !call().primaryClosed && !terminalCallStatuses.has(call().status)
    && Date.now() < Date.parse(initialCall.liveGenerations?.clocks.callDeadlineAt ?? options.callDeadlineAt);
  const selectedNotice = async (state: 'failed' | 'cancelled' | 'unconfirmed') => {
    if (!selectedActive() || selectedNotices.has(state)) return;
    selectedNotices.add(state);
    await controller.withCurrentAttachment(async attachment => {
      if (!selectedActive()) return;
      const session = sessions.get(attachment.sessionId);
      if (!session?.voice.appendCommentary) return;
      const result = await session.voice.appendCommentary(state === 'failed' ? 'I could not get an answer to that request.'
        : state === 'cancelled' ? 'The request was cancelled.' : 'I could not check the saved request yet.');
      if (state !== 'unconfirmed' && result.status === 'accepted') selectedTerminalDelivered = true;
    });
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
        || ('question' in result.delivery && Boolean(selected?.pendingSourceCount() || mixed?.pendingSourceCount())) ? 'stale' : undefined;
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
    text: string, kind: 'progress' | 'result', updateId: string,
    evidence?: OrdinaryPublicationEvidence['response'], historical=false, speak=true): Promise<GenerationPublication> => {
    const session = sessions.get(attachment.sessionId);
    if (!session?.voice.appendCommentary) throw new Error('commentary_transport_unavailable');
    const start = session.voice.getTranscript();
    const spokenAt = lastSpeechAt;
    // Caller speech defers only spoken non-final text (a question or notice). Quiet progress and a
    // confirmed final result go out regardless: Live decides when to say the result.
    const awaitsTurn = speak && !terminalRequest(entry.status);
    const interrupted = (): LiveDelivery['status'] | undefined => stopped || observer.signal.aborted || call().endedAt ? 'closed'
      : controller.snapshot().deliveryAttachment?.generationId !== attachment.generationId
        || awaitsTurn && (session.voice.getTranscript().callerRevision !== start.callerRevision || lastSpeechAt !== spokenAt) ? 'stale' : undefined;
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
    // A successor has no provider delegation ID for this task. Its newly admitted
    // history controls which source text can accompany the durable request identity.
    if (entry.sessionId !== attachment.sessionId && speak) {
      for (const context of requestPresentationContext(entry, session.eligibleMessages)) {
        const halted = interrupted(); if (halted) return { ...receipt, status: halted };
        try { receipt = await session.voice.appendContext(context); }
        catch { return { eventId: '', status: 'unconfirmed', reason: 'task_context_unconfirmed' }; }
        const status = interrupted() ?? receipt.status;
        if (status !== 'accepted') return { ...receipt, status, reason: 'task_context_unconfirmed' };
      }
    }
    for (const [index, chunk] of chunks.entries()) {
      const halted = interrupted();
      if (halted) return { eventId: receipt.eventId, status: halted };
      const snapshot = session.voice.getTranscript();
      const update: LiveBackendUpdate = { id: `${prefix}_attempt_${attempt}_${index}`, kind, text: chunk, speak,
        pending: !terminalRequest(entry.status), terminal: index === chunks.length - 1,
        expectedCallerRevision: start.callerRevision };
      const identity = { callId: initialCall.id, callSid: binding.callSid, bindingId: binding.bindingId,
        sessionId: attachment.sessionId, ...(entry.sessionId !== attachment.sessionId ? { sourceSessionId: entry.sessionId } : {}) };
      const intent = beginBrokerPublication(options.store, identity, entry.requestId, update, snapshot,
        evidence ? { response: evidence, chunkIndex: index,...(historical?{historical:true as const}:{}) } : undefined);
      // Unknown submissions lack a confirmed run. Their fixed local status carries no related-request authority.
      if (intent && !intent.created) receipt = intent.record.outcome
        ? { eventId: intent.record.outcome.eventId, status: intent.record.outcome.status,
          observedCallerRevision: intent.record.outcome.callerRevision, observedTranscriptSequence: intent.record.outcome.transcriptSequence }
        : { eventId: '', status: 'unconfirmed' };
      else {
        try { receipt = entry.sessionId===attachment.sessionId && session.voice.publishDelegation
          ? await session.voice.publishDelegation(entry.delegationIds[0]!,update)
          : speak ? await session.voice.appendCommentary(chunk) : await session.voice.appendContext(chunk); }
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
    // Quiet progress is context, not speech: it never waits for a pause or a turn.
    const progressOnly = update?.speak === false;
    const ordinaryResult = !update && ['completed', 'failed', 'cancelled', 'waiting_for_reply'].includes(entry.status);
    // A terminal outcome for this exact request is what the caller is waiting for. Once
    // a pause admits it, caller speech during the context read ("Hello?", "What are you
    // on?") is a prompt to hear it, not a reason to withhold it (call 039b39cd). Request
    // identity, the "For your earlier request" label and per-chunk barge-in still apply.
    const availableResult = !update && ['completed', 'failed', 'cancelled'].includes(entry.status)
      && entry.primaryResult?.state === entry.status;
    const deferred = () => ({ delivery: { eventId: '', status: 'stale' as const, reason: 'caller_presentation_deferred' } });
    if (availableResult && controller.admitsSources()) await callerPause();
    else if (ordinaryResult && !await atSpeechBoundary(entry)) return deferred();
    // Freeze before context I/O. A successor's empty transcript cannot make an
    // old answer current after the caller has started another turn.
    const frontier = presentationFrontier();
    const stillRelevant = () => !stopped && !observer.signal.aborted && !call().endedAt
      && (availableResult || progressOnly || (frontier === presentationFrontier()
        && (!ordinaryResult || (!blocksEarlierPresentation(entry) && quiet()))));
    const canonicalRequestId = entry.reconciliation?.outcome === 'admitted' ? entry.reconciliation.canonicalRequestId : entry.requestId;
    let attachment: LiveGenerationAttachment;
    let delivery: GenerationPublication;
    if (!update && (entry.status === 'completed' || entry.status === 'waiting_for_reply')) {
      const source = entry.status === 'completed' && entry.primaryResult?.assistant
        ? { kind: 'assistant' as const, messageId: entry.primaryResult.assistant.messageId }
        : entry.currentQuestionId ? { kind: 'question' as const, questionId: entry.currentQuestionId, questionRevision: 1 as const } : undefined;
      if (!source) return { delivery: { eventId: '', status: 'unconfirmed' as const, reason: 'eligible_source_unavailable' } };
      let outcome;
      try { outcome = await controller.deliver(canonicalRequestId, source, undefined, stillRelevant); }
      catch (error) {
        if (error instanceof LiveGenerationError && error.code === 'presentation_deferred') return deferred();
        throw error;
      }
      attachment = outcome.attachment;
      delivery = outcome.publication ?? { eventId: '', status: 'unconfirmed', reason: outcome.response.delivery.state === 'unavailable'
        ? outcome.response.delivery.reason : 'publication_unconfirmed' };
    } else {
      const outcomeText = update?.text ?? (entry.status === 'failed' ? failedRequestSpeech(entry)
        : entry.status === 'cancelled' ? 'The request was cancelled.' : 'I am checking the saved request status.');
      // Label at publication time: caller speech during the context read still marks it earlier.
      const text = () => !update && ['failed', 'cancelled'].includes(entry.status) && entry.primaryResult?.state === entry.status
        && hasLaterCallerSpeech(call(), entry) ? `For your earlier request: ${outcomeText}` : outcomeText;
      const present = async (currentAttachment: LiveGenerationAttachment) => ({ attachment: currentAttachment,
        delivery: stillRelevant()
          ? await publishText(entry, currentAttachment, text(), update?.kind ?? 'result', update?.id ?? `${entry.requestId}_${entry.status}`, undefined, false, update?.speak ?? true)
          : { eventId: '', status: 'stale' as const, reason: 'caller_context_changed' } });
      let published: Awaited<ReturnType<typeof present>>;
      try { published = await controller.withCurrentAttachment(present, stillRelevant); }
      catch (error) {
        if (error instanceof LiveGenerationError && error.code === 'presentation_deferred') return deferred();
        throw error;
      }
      attachment = published.attachment; delivery = published.delivery;
    }
    const segment = controller.snapshot().segments.find(value => value.generationId === attachment.generationId)!;
    return { delivery, publicationReceipts: delivery.publicationReceipts, sessionId: attachment.sessionId, snapshot: sessions.get(attachment.sessionId)?.voice.getTranscript(),
      inputThroughMs: segment.inputFirstSample === undefined ? 0
        : (segment.inputLastSampleExclusive! - segment.inputFirstSample) / 8 };
  };

  // Drain a startup or delivery backlog at 1.5x real time so the caller is heard
  // without a lasting lag; unpaced bursts tripped input_audio_rate_limit_exceeded.
  const controller = createLiveGenerationController({ callId: initialCall.id, binding, inputPaceRate: 1.5,
    onActivated(attachment) {
      const record = sessions.get(attachment.sessionId), voice = record?.voice;
      if (voice) {
        options.portfolioContext?.attach(voice);
        // A replacement session sees a request's words only when its eligible history admitted that source.
        tasks.attach(voice, task => task.sessionId === attachment.sessionId || record!.eligibleMessages.some(message =>
          message.role === 'user' && message.canonicalSource?.sourceEventId === task.sourceEventId));
      }
    },
    clocks: initialCall.liveGenerations?.clocks ?? { callDeadlineAt: options.callDeadlineAt, mediaStartedAt: initialCall.mediaStartedAt,
      ...(initialCall.inbound?.remote?.state === 'bound' ? { consumedAt: initialCall.inbound.remote.consumedAt,
        mediaStartBy: initialCall.inbound.mediaStartBy! } : {}) },
    initial: initialCall.liveGenerations, context: options.context,
    async onInitialActivation(attachment) {
      if (continuation || stopped || observer.signal.aborted || call().endedAt || call().voiceOpening) return;
      const voice = sessions.get(attachment.sessionId)?.voice;
      if (!voice) throw new LiveGenerationError('generation_source_not_ready');
      if (!readyReported) { readyReported = true; options.onReady({ callId: initialCall.id, sessionId: attachment.sessionId }); }
      if (stopped || observer.signal.aborted || call().endedAt) return;
      // An outbound opening makes Live speak first. Everything the person said while Live started
      // (a "Hello?" after "Hey, it's Dexter.") is still queued for its paced replay; it reaches Live
      // before the opening, so Live answers it inside the offer rather than after the offer began.
      // Silence alone holds nothing back.
      const heldSpeechEnd = initialCall.direction === 'outbound' ? speechInputEnd : undefined;
      if (heldSpeechEnd !== undefined) {
        const waitStartedAt = Date.now();
        if (!(await controller.inputDelivered(heldSpeechEnd))) return;
        if (stopped || observer.signal.aborted || call().endedAt) return;
        try { console.info(JSON.stringify({ event: 'broker_opening_after_caller_speech', version: 1, callId: initialCall.id,
          observedAt: new Date().toISOString(), heldSpeechInputMs: heldSpeechEnd / 8, waitedMs: Date.now() - waitStartedAt })); }
        catch { /* Telemetry only. */ }
      }
      // A saved intent prevents a restart or an unknown append receipt from
      // issuing another opening. Acceptance describes injection, not playback.
      options.store.update(initialCall.id, { voiceOpening: { status: 'pending' } });
      let receipt: LiveDelivery;
      try { receipt = await voice.appendInstructions(brokerOpening(initialCall.direction, Boolean(initialCall.callAssignment),
        initialCall.callAssignment?.origin === 'authenticated_hark_delivery',
        initialCall.callAssignment?.origin === 'authenticated_notification_delivery',
        Boolean(call().journey?.greetingClipStartedAt), heldSpeechEnd !== undefined)); }
      catch { receipt = { eventId: '', status: 'unconfirmed', reason: 'opening_delivery_failed' }; }
      if (!stopped && !observer.signal.aborted && !call().endedAt
        && controller.snapshot().deliveryAttachment?.generationId === attachment.generationId)
        options.store.update(initialCall.id, { voiceOpening: receipt });
    },
    persist(journal) {
      options.store.update(initialCall.id, { liveGenerations: journal,
        ...(journal.deliveryAttachment ? { liveSessionId: journal.deliveryAttachment.sessionId } : {}) });
    },
    observeTranscript(generationId, transcript) { options.store.observeGenerationTranscript(initialCall.id, generationId, transcript); },
    onAudio(payload) { if (!playbackPaused) {
      if (hasAudiblePcmu(Buffer.from(payload, 'base64'))) lastOutputAt = Date.now();
      options.onAudio(payload);
    } },
    clearPlayback: options.onPlaybackClear,
    onProblem(code) { options.onProblem(code); },
    onContextReadFailure(failure) {
      options.store.update(initialCall.id, { contextReadFailures: [...(call().contextReadFailures ?? []), failure].slice(-16) });
    },
    onStatusContextRefresh(refresh) {
      const prior = call().statusContextRefreshes ?? [];
      if (prior.length >= 1000) throw new LiveGenerationError('status_context_evidence_limit');
      options.store.update(initialCall.id, { statusContextRefreshes: [...prior, refresh] });
    },
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
      let prepared: ReturnType<typeof prepareEligibleHistory>;
      try {
        const pendingMessages=Object.values(call().delegations).flatMap(raw=>{
          const entry=raw as BrokerDelegationRecord;
          return entry.delivery!=='accepted' && entry.primaryResult?.state==='completed' && entry.primaryResult.assistant
            ? [entry.primaryResult.assistant.messageId] : [];
        });
        prepared = prepareEligibleHistory(input.context, binding,pendingMessages);
      }
      catch (error) { throw new LiveGenerationStartupError(error instanceof Error ? error.message : undefined); }
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
          const error = new LiveGenerationStartupError(reason);
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
            instructions: continuation ? (options.callerIntent ? mixedSelectedInstructions : selectedInstructions) : brokerInstructions({ direction: initialCall.direction, purpose: initialCall.purpose,
              registeredAssignment: input.allowOpening && !initialCall.voiceOpening && Boolean(initialCall.callAssignment),
              ...(initialCall.callAssignment?.origin === 'authenticated_hark_delivery' ? { watch: initialCall.callAssignment.value.hark } : {}),
              ...(initialCall.callAssignment?.origin === 'authenticated_notification_delivery' ? { notification: initialCall.callAssignment.value.notification } : {}),
              name: input.context.context.displayName }),
            backend: { async delegate(request, channel) {
              if (!ready || !backend || request.sessionId !== sessionId
                || controller.snapshot().deliveryAttachment?.sessionId !== sessionId) throw new Error('generation_source_not_ready');
              // The backend freezes this generation's caller source before observing durable work.
              let resolveAdmission!: () => void;
              const promise = new Promise<void>(resolve => { resolveAdmission = resolve; });
              pending.set(request, { promise, resolve: resolveAdmission });
              if (!continuation) tasks.starting(request);
              try {
                if (continuation) {
                  if (!selected || !selectedActive()) throw new Error('selected_question_not_ready');
                  if (mixed) await mixed.delegate(request, channel);
                  else await selected.delegate(request, channel);
                } else await backend.delegate(request, channel);
              }
              finally { pending.get(request)?.resolve(); pending.delete(request); tasks.settled(request); }
            } },
            onAudio(payload) { input.callbacks.onAudio(payload); },
            onTranscript(snapshot) {
              input.callbacks.onTranscript(snapshot); backend?.observeTranscript(snapshot);
              if (sessionId) guardrail?.observe(snapshot, sessionId);
            },
            onReady(value) {
              if (ready) return;
              if (startCancelled || input.signal.aborted || stopped) {
                cancelStart(); void voice.close('generation_start_cancelled'); return;
              }
              if (sessions.has(value.sessionId) || controller.snapshot().segments.some(segment => segment.sessionId === value.sessionId)) {
                startCancelled = true; clearTimeout(startTimer); input.signal.removeEventListener('abort', cancelStart);
                void voice.close('generation_session_reused');
                reject(new LiveGenerationStartupError('generation_session_reused')); return;
              }
              sessionId = value.sessionId;
              backend = createPrimaryBackend({ callId: initialCall.id, callSid: binding.callSid, bindingId: binding.bindingId,
                onResult: result => options.portfolioContext?.observe(result),
                onRequest: () => tasks.refresh(),
                api: options.primary, store: options.store, sourceSessionId: sessionId, watchUntilCallEnds: true,
                admissionSettled(request) { pending.get(request)?.resolve(); pending.delete(request); mixed?.admissionSettled(request); tasks.settled(request); },
                lastCallerSpeechAt: () => sourceSpeechAt,
                ...(continuation ? { selectedQuestionSources: () => selectedQuestionSourceAnchors(call()) } : {}),
                ...(continuation && options.callerIntent ? { callerRouteSources: () => callerRouteSources(call()) } : {}),
                callerRouteCaptureAccepted(sourceId, receipt) {
                  const route = call().callerRouting?.records.find(value => value.frozen.turn.sourceEventId === sourceId);
                  if (route) options.store.captureCallerRoute(initialCall.id, callerRouteRef(route), receipt);
                },
                resolveCallerTaskReference(route) {
                  if (route.decision?.kind !== 'task' || !route.decision.contextReference) return;
                  const ref = route.decision.contextReference;
                  return callerIntentResultPublicationCandidates(call(), route.frozen.sessionId)
                    .find(candidate => candidate.id === ref.id && candidate.digest === ref.digest)?.reference;
                },
                observation: { signal: observer.signal, publish } });
              const record = { voice, backend, firstSample: undefined as number | undefined,
                eligibleMessages: prepared.source.context.messages };
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
                  const entries = Object.values(call().delegations) as BrokerDelegationRecord[];
                  const entry = entries.find(value => value.requestId === result.requestId || (value.reconciliation?.outcome === 'admitted'
                    && value.reconciliation.canonicalRequestId === result.requestId));
                  if (result.deliveryAssociation !== undefined) {
                    if (!continuation || entry
                      || result.requestId !== continuation.originalRequestId
                      || result.deliveryAssociation.execution.requestId !== result.requestId)
                      throw new Error('generation_result_ownership_conflict');
                    return publishSelected(result, attachment);
                  }
                  if (!entry) throw new Error('generation_result_request_unknown');
                  if ('question' in result.delivery && (entry.currentQuestionId !== result.delivery.question.id
                    || entry.questions?.[result.delivery.question.id]?.question.runId !== result.delivery.question.runId))
                    throw new Error('generation_question_identity_changed');
                  const evidence = result.delivery.source.kind === 'assistant'
                    ? eligibleDeliveryResponseSchema.parse({ bindingId: binding.bindingId, callSid: binding.callSid,
                      requestId: result.requestId, version: result.version, delivery: result.delivery }) : undefined;
                  const historical=!!evidence && hasLaterCallerSpeech(call(),entry);
                  return publishText(entry, attachment, historical ? `For your earlier request: ${result.delivery.text}` : result.delivery.text, 'result',
                    `${entry.requestId}_${result.delivery.source.messageId}`, evidence,historical);
                },
                close(reason) { backend!.persistAcousticEvidence(); return voice.close(reason); },
              });
            },
            onClosed(result) {
              options.portfolioContext?.detach(voice); tasks.detach(voice);
              recordVoiceClose(options.store, initialCall.id, result, input.generationId);
              clearTimeout(startTimer); input.signal.removeEventListener('abort', cancelStart); finalResult = result;
              input.callbacks.onClosed(result);
              if (!ready) { startCancelled = true; reject(new LiveGenerationStartupError(result.reason)); }
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
          startCancelled = true; clearTimeout(startTimer); input.signal.removeEventListener('abort', cancelStart);
          reject(new LiveGenerationStartupError(error instanceof Error ? error.message : undefined));
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
      async capture(bindingId, turn) {
        const route = call().callerRouting?.records.find(value => value.frozen.turn.sourceEventId === turn.sourceEventId);
        if (route?.capture) return acknowledgedTurn(bindingId, turn, route.capture);
        const receipt = await options.primary.captureTurn(bindingId, turn);
        if (route) options.store.captureCallerRoute(initialCall.id, callerRouteRef(route), receipt);
        return receipt;
      },
      withCallerAdmission: operation => withCallerAdmission(options.store, initialCall.id, operation),
      assertFrozenCallerSpeechCurrent(frozen) {
        const source = sessions.get(frozen.sessionId);
        if (!source) throw new Error('selected_question_source_missing');
        source.backend.assertFrozenCallerSpeechCurrent(frozen);
      },
      freezer: { async freeze(request, channel, usedSourceIds) {
        const source = sessions.get(request.sessionId);
        if (!source) throw new Error('selected_question_source_missing');
        return source.backend.freezeCallerTurn(request, channel, usedSourceIds);
      } },
      admissionSettled(request) {
        const generation = controller.snapshot().segments.find(segment => segment.sessionId === request.sessionId);
        const pending = generation ? admissions.get(generation.generationId) : undefined;
        pending?.get(request)?.resolve(); pending?.delete(request);
        mixed?.admissionSettled(request);
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
        else if (selected.pendingSourceCount() === 0 && !mixed?.pendingSourceCount()) {
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
    mixed?.stop();
    selectedClosing = true; clearTimeout(selectedTimer); selectedTimer = undefined;
    selected?.stop();
  };

  if (continuation && options.callerIntent) mixed = createMixedWorkRouter({ callId: initialCall.id,
    store: options.store, intent: options.callerIntent, capture: options.primary,
    active: sessionId => selectedActive() && controller.snapshot().deliveryAttachment?.sessionId === sessionId,
    freezer: { freeze(request, channel, used) {
      const backend = sessions.get(request.sessionId)?.backend;
      if (!backend) throw new Error('caller_route_source_missing');
      return backend.freezeCallerTurn(request, channel, used);
    } },
    resolutionInput(request) {
      const segment = controller.snapshot().segments.find(value => value.sessionId === request.sessionId);
      const backend = sessions.get(request.sessionId)?.backend;
      if (!segment || !backend) throw new Error('caller_route_source_missing');
      const context = { version: segment.version,
        publicationSequence: call().publications?.records.at(-1)?.publicationSequence ?? 0,
        questions: [...backend.questionTargets(request.sessionId), ...(selected?.questionTargets(request.sessionId) ?? [])] };
      try {
        const inventory = callerIntentResultPublicationInventory(call(), request.sessionId);
        return { ...context, resultCoverage: inventory.resultCoverage,
          references: inventory.candidates.map(({ id, digest }) => ({ id, digest })) };
      } catch (error) {
        if (!(error instanceof CallerIntentResultInventoryError)) throw error;
        return { ...context, references: [], classificationRefusal: error.code };
      }
    },
    async admit(request, ref, decision, channel) {
      const backend = sessions.get(request.sessionId)?.backend;
      if (!backend) throw new Error('caller_route_source_missing');
      if (decision.kind === 'task') await backend.delegateFrozenTask(request, ref, channel);
      else if(decision.kind==='deliver_result')await backend.deliverFrozenResult(request,ref,channel);
      else if (decision.kind === 'answer' && decision.target.kind === 'ordinary') await backend.answerFrozenQuestion(request, ref, channel);
      else if (decision.kind === 'answer' && selected) await selected.answerFrozen(request, ref, channel);
      else throw new Error('caller_route_admission_unavailable');
    },
    async clarify(request, reason) {
      await controller.withCurrentAttachment(async attached => {
        if (!selectedActive() || attached.sessionId !== request.sessionId) return;
        const text = reason === 'mixed_intent' ? 'Please give me the answer and the new task separately.'
          : reason === 'ambiguous_target' ? 'Which earlier request do you mean?'
          : reason === 'unavailable' ? 'I could not work out that request. Please say what you want me to do.'
          : 'Are you answering the saved question, or asking me to do something else?';
        await sessions.get(attached.sessionId)?.voice.appendCommentary?.(text);
      });
    },
    admissionSettled(request) {
      const generation = controller.snapshot().segments.find(value => value.sessionId === request.sessionId);
      const pending = generation && admissions.get(generation.generationId);
      pending?.get(request)?.resolve(); pending?.delete(request);
    },
  });

  // Starting is asynchronous so the server installs its call owner before any callbacks run.
  queueMicrotask(() => { void controller.start().then(attachment => {
    if (!stopped) beginSelected(attachment);
    if (!stopped && !readyReported) { readyReported = true; options.onReady({ callId: initialCall.id, sessionId: attachment.sessionId }); }
  }).catch(error => { options.onProblem(error instanceof Error ? error.message : 'generation_start_failed'); }); });
  return {
    async prepareSelectedCloseout() {
      stopSelected();
      const routed = await mixed?.prepareCloseout();
      if (selected) {
        const result = await selected.prepareCloseout();
        if (!routed) return result;
        const pendingAdmissions = result.pendingAdmissions + routed.pendingAdmissions;
        const pendingCaptures = result.pendingCaptures + routed.pendingCaptures;
        return { ...result, pendingAdmissions, pendingCaptures,
          ready: result.ready && pendingAdmissions === 0 && pendingCaptures === 0 };
      }
      if (continuation && options.selectedQuestion) return createSelectedQuestionRecovery({ callId: initialCall.id,
        store: options.store, primary: options.primary, requests: options.selectedQuestion.requests }).prepareCloseout();
      return { pendingAdmissions: 0, pendingCaptures: 0, pendingReplies: 0, ready: true };
    },
    appendAudio(payload) {
      const bytes = Buffer.from(payload, 'base64');
      const observedAt = Date.now();
      const speech = inputSpeechEnergy(bytes) > 0.012;
      if (speech) { lastSpeechAt = observedAt; speechRevision++; }
      const start = inputNextSample;
      const outcome = controller.appendAudio({ payload, callInputStartSample: start, observedAt });
      inputNextSample = controller.snapshot().inputNextSample;
      if (speech && outcome !== 'refused') speechInputEnd = start + bytes.length;
      return outcome !== 'refused';
    },
    appendContext: async () => ({ eventId: '', status: 'rejected', reason: 'eligible_context_required' }),
    async appendInstructions(text) {
      return controller.withCurrentAttachment(attachment => sessions.get(attachment.sessionId)!.voice.appendInstructions(text));
    },
    async interruptPlayback(instruction) {
      // Barge-in is a local playback action. Silence the current stream before
      // any context I/O, then qualify the instruction against the current voice.
      playbackPaused = true;
      const voice = current()?.session?.voice;
      if (voice?.pausePlayback) voice.pausePlayback();
      else options.onPlaybackClear();
      const attachment = await controller.beforeReuse();
      // A resume may have arrived while the assertion was in flight. Appending
      // the instruction must not issue another late playback pause.
      return sessions.get(attachment.sessionId)!.voice.appendInstructions(instruction);
    },
    resumePlayback() { playbackPaused = false; current()?.session?.voice.resumePlayback(); },
    getTranscript() { return current()?.session?.voice.getTranscript() ?? emptyTranscript(); },
    close(reason = 'call_stopped') {
      if (closing) return closing;
      stopped = true; observer.abort(); tasks.stop(); guardrail?.stop();
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
