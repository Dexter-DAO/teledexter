import fs from 'node:fs';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createHash, randomUUID } from 'node:crypto';
import type { WebSocket as Socket } from 'ws';
import type { BrokerConfig } from '../src/broker/config.js';
import type { PrimaryApiClient, PrimaryRequestResult } from '../src/broker/primaryApi.js';
import type { BrokerLiveConnection, BrokerLiveSessionOptions, LiveSessionClosed, LiveTranscriptSnapshot } from '../src/live/types.js';
import type { createBrokerServer as ServerFactory } from '../src/broker/server.js';
import { freshnessEvidence, freshnessMayRelease, freshnessPurpose, freshnessResult, historicalAccountMessages, selectFreshnessScenario } from './live-freshness.js';

// Executes funded Live calls only when an operator runs this script. Twilio,
// account identity, Primary requests, and playback receipts are local fixtures.
const usage = 'node --import tsx scripts/qualify-live-phone.mts --module-root /absolute/release --module-variant src|dist --evidence-dir /new/absolute/directory --config-file /absolute/production.env [--scenario freshness] [--input-audio /absolute/caller.pcmu] [--interjection-audio /absolute/interjection.pcmu --call-purpose-file /absolute/purpose.txt]';
const args = new Map<string, string>();
for (let i = 2; i < process.argv.length; i += 2) {
  const key = process.argv[i];
  if (!['--module-root', '--module-variant', '--evidence-dir', '--config-file', '--input-audio', '--instructions-file', '--interjection-audio', '--call-purpose-file', '--scenario'].includes(key) || !process.argv[i + 1] || args.has(key)) throw new Error(usage);
  args.set(key, process.argv[i + 1]);
}
function absolute(flag: string, required = true) {
  const value = args.get(flag);
  if (!value && !required) return undefined;
  if (!value || !path.isAbsolute(value)) throw new Error(`${flag} must be an absolute path. ${usage}`);
  return path.resolve(value);
}
const moduleRoot = fs.realpathSync(absolute('--module-root')!);
const evidenceDir = absolute('--evidence-dir')!;
const configFile = absolute('--config-file')!;
const audioFile = absolute('--input-audio', false);
const instructionsFile = absolute('--instructions-file', false);
const interjectionFile = absolute('--interjection-audio', false);
const callPurposeFile = absolute('--call-purpose-file', false);
if (interjectionFile && !audioFile) throw new Error('--interjection-audio requires --input-audio');
if (interjectionFile && instructionsFile) throw new Error('Interjection qualification must use the production voice instructions');
const freshnessScenario = selectFreshnessScenario(args);
const conversationScenario = Boolean(interjectionFile);
const heldResultScenario = conversationScenario || freshnessScenario;
const hardDeadlineMs = heldResultScenario ? 88_000 : 58_000;
const conversationDeadlineMs = hardDeadlineMs - 18_000;
let fixtureResult = freshnessScenario ? freshnessResult(new Date().toISOString()) : conversationScenario
  ? 'Your test portfolio is worth $1,250 in total: $250 in USDC and $1,000 in SOL.'
  : 'Your test phrase is blue lantern.';
const portfolioFacts = [
  { label: 'portfolio total', amountUsd: 1250, labelPattern: /\b(?:portfolio|total|overall)\b/ },
  { label: 'USDC', amountUsd: 250, labelPattern: /\busdc\b/ },
  { label: 'SOL', amountUsd: 1000, labelPattern: /\b(?:sol|solana)\b/ },
];
function spokenPortfolioFacts(text: string) {
  // Consume the whole amount before checking smaller values: neither “1,250”
  // nor “one thousand two hundred fifty” also proves the separate holdings.
  const normalized = text.toLowerCase().replace(/-/g, ' ')
    .replace(/\b(?:one thousand(?: and)? two hundred(?: and)? fifty|twelve hundred(?: and)? fifty)\b/g, 'amount_1250')
    .replace(/\b(?:one thousand|a thousand)\b/g, 'amount_1000')
    .replace(/\btwo hundred(?: and)? fifty\b/g, 'amount_250')
    .replace(/\b\d[\d,]*(?:\.\d+)?\b/g, value => `amount_${Number(value.replace(/,/g, ''))}`);
  let clauses = normalized.split(/[,;.!?]|\band\b/);
  const totalConfirmed = clauses.some(clause => /\b(?:portfolio|total|overall)\b/.test(clause) && /\bamount_1250\b/.test(clause));
  const solConfirmed = clauses.some(clause => /\b(?:sol|solana)\b/.test(clause) && /\bamount_1000\b/.test(clause));
  const explicitSmallAmount = /\bcents?\b|\b2[.,]50\b|\btwo (?:dollars? (?:and )?fifty|point five(?: zero)?)\b/i.test(text);
  // In this fixture, 1,250 total minus 1,000 in SOL establishes 250 in USDC.
  // Accept colloquial “two fifty” only with that context and a direct USDC link.
  if (totalConfirmed && solConfirmed && !explicitSmallAmount) clauses = clauses.map(clause =>
    /\btwo fifty(?: dollars?)? (?:in|of|worth of) usdc\b|\busdc(?: balance| holdings?| value)? (?:is|are|at|worth|is worth) two fifty\b|\busdc\s*:\s*two fifty\b/.test(clause)
      ? clause.replace(/\btwo fifty\b/g, 'amount_250') : clause);
  return portfolioFacts.map(({ label, amountUsd, labelPattern }) => ({
    label, amountUsd,
    spoken: clauses.some(clause => labelPattern.test(clause) && new RegExp(`\\bamount_${amountUsd}\\b`).test(clause)),
  }));
}
function assessConversationTranscript(assistantText: string) {
  const evidence = (patterns: RegExp[]) => patterns.flatMap(pattern => {
    const match = pattern.exec(assistantText);
    return match ? [assistantText.slice(Math.max(0, match.index - 45), match.index + match[0].length + 85).trim()] : [];
  });
  const internalBriefEvidence = evidence([
    /\b(?:repair|implementation|debug(?:ging)?|regression|reconciliation|deployment|backend|API|runtime)\b/i,
    /\b(?:Primary Dexter|test harness|test procedure|verification procedure)\b/i,
  ]);
  // This scenario has one successful fixture result and no failed account work.
  const unsupportedFailureEvidence = evidence([
    /\b(?:failed|failure|rejected|unsuccessful|broken)\b/i,
    /\bneeds?\s+(?:a\s+)?(?:fix|repair)\b/i,
    /\b(?:could(?:n['’]?t| not)|did(?:n['’]?t| not)|unable to|cannot|can['’]?t)\s+(?:go through|complete|process|submit|execute|perform)\b/i,
  ]);
  const callerFactQuizEvidence = evidence([
    /\b(?:can|could|would|will)\s+you\s+(?:please\s+)?(?:tell|give|read|state|confirm|check|verify|look up)\b[^.!?]{0,100}\b(?:balance|holdings|portfolio|USDC|SOL)\b/i,
    /(?:^|[.!?])\s*(?:please\s+)?(?:tell|give|read(?: out)?|state|confirm|check|verify|look up)\s+(?:me\s+)?(?:your|the|what|how much)\b[^.!?]{0,100}\b(?:balance|holdings|portfolio|USDC|SOL)\b/i,
    /\bwhat(?:['’]s| is| are)\s+(?:your|the)\s+(?:current\s+)?(?:balance|holdings|portfolio(?:\s+(?:value|total|holdings))?)(?!\s+(?:question|request))\b/i,
    /\bhow\s+much\s+(?:USDC\s+|SOL\s+)?(?:do|are)\s+you\s+(?:currently\s+)?(?:have|hold|holding)\b/i,
  ]);
  return {
    internalBriefEvidence, unsupportedFailureEvidence, callerFactQuizEvidence,
    internalBriefKeptPrivate: internalBriefEvidence.length === 0,
    successfulResultDescribedAccurately: unsupportedFailureEvidence.length === 0,
    agentKeepsAccountLookupResponsibility: callerFactQuizEvidence.length === 0,
    accepted: internalBriefEvidence.length === 0 && unsupportedFailureEvidence.length === 0 && callerFactQuizEvidence.length === 0,
  };
}
const sourceRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
for (const root of [moduleRoot, sourceRoot]) {
  const relative = path.relative(root, evidenceDir);
  if (!relative || (!relative.startsWith(`..${path.sep}`) && relative !== '..' && !path.isAbsolute(relative))) throw new Error('Evidence must live outside source and release directories');
}
if (fs.existsSync(evidenceDir)) throw new Error('Evidence directory must be new');
const parent = fs.realpathSync(path.dirname(evidenceDir));
if (parent !== path.dirname(evidenceDir)) throw new Error('Evidence parent must be a canonical path');
fs.mkdirSync(evidenceDir, { mode: 0o700 });
process.umask(0o077);

const startedAt = Date.now();
const events: Record<string, unknown>[] = [];
const audio: Buffer[] = [];
const primaryCalls: Record<string, unknown>[] = [];
const providerCalls: Record<string, unknown>[] = [];
const connections = new Set<BrokerLiveConnection>();
const playbackTimers = new Set<ReturnType<typeof setTimeout>>();
let broker: ReturnType<typeof ServerFactory> | undefined;
let socket: Socket | undefined;
let inputTimer: ReturnType<typeof setInterval> | undefined;
let snapshot: LiveTranscriptSnapshot = { revision: 0, callerRevision: 0, fragments: [] };
let closed: LiveSessionClosed | undefined;
let failure: { stage: string; code: string } | undefined;
let stage = 'configuration';
let callId: string | undefined;
let preflight: unknown;
let moduleVariant = '';
let lastAudioAt = 0;
let playbackUntil = 0;
let inputFrames = 0;
let simulatedPlayedBytes = 0;
let simulatedClearedBytes = 0;
let speechOffset = 0;
let speechStartedAt: number | undefined;
let speechEndedAt: number | undefined;
let interjectionOffset = 0;
let interjectionStartedAt: number | undefined;
let interjectionEndedAt: number | undefined;
let interjectionInputStartMs: number | undefined;
let firstSubmitAt: number | undefined;
let firstSubmitCallerRevision: number | undefined;
let firstSubmitSnapshot: LiveTranscriptSnapshot | undefined;
let resultReleaseSnapshot: LiveTranscriptSnapshot | undefined;
let firstSubmitInputOffsetMs: number | undefined;
let firstSubmitOutputBytes: number | undefined;
let resultReleaseInputOffsetMs: number | undefined;
let interjectionStartCallerRevision: number | undefined;
let lastCallerTranscriptAt = 0;
let resultReleasedAt: number | undefined;
let resultReleaseTranscriptRevision: number | undefined;
let resultCallerRevision: number | undefined;
let resultReleaseOutputBytes = 0;
let audiblePlaybackUntil = 0;
let purposeDigest: string | undefined;
let greetingAt: number | undefined;
let controlledCloseAt: number | undefined;
let peakEnergy = 0;
let completed = false;
const code = (error: unknown) => {
  const candidate = (error as { code?: unknown })?.code;
  return typeof candidate === 'string' && /^[a-zA-Z0-9_:-]{1,100}$/.test(candidate) ? candidate : 'qualification_error';
};
const note = (value: Record<string, unknown>) => { events.push({ elapsedMs: Date.now() - startedAt, ...value }); };
function conversationChecks() {
  const submissions = primaryCalls.filter(x => x.method === 'submit');
  const originalFirstTurn = snapshot.fragments.filter(x => x.role === 'user'
    && (interjectionInputStartMs === undefined || x.startMs < interjectionInputStartMs)).map(x => x.text).join('');
  const interjectionTranscript = snapshot.fragments.filter(x => x.role === 'user'
    && interjectionInputStartMs !== undefined && x.startMs >= interjectionInputStartMs).map(x => x.text).join('');
  const resultTranscript = snapshot.fragments.filter(x => x.role === 'assistant'
    && resultReleaseTranscriptRevision !== undefined && x.sequence > resultReleaseTranscriptRevision).map(x => x.text).join('');
  const facts = spokenPortfolioFacts(resultTranscript);
  const transcriptAcceptance = assessConversationTranscript(snapshot.fragments.filter(x => x.role === 'assistant').map(x => x.text).join(''));
  return {
    originalFirstTurn, interjectionTranscript, resultTranscript, facts, transcriptAcceptance,
    exactlyOneSubmit: submissions.length === 1,
    noFollowupAction: submissions.length === 1 && !primaryCalls.some(x => x.method === 'question' || x.method === 'reply'),
    originalFirstTurnPreserved: submissions.length === 1 && originalFirstTurn.length > 0 && String(submissions[0].text).trim() === originalFirstTurn.trim(),
    laterCallerRevision: firstSubmitCallerRevision !== undefined && interjectionStartCallerRevision !== undefined
      && resultCallerRevision !== undefined && resultCallerRevision > firstSubmitCallerRevision && resultCallerRevision > interjectionStartCallerRevision,
    interjectionBeforeResult: firstSubmitAt !== undefined && interjectionStartedAt !== undefined && interjectionEndedAt !== undefined
      && resultReleasedAt !== undefined && firstSubmitAt < interjectionStartedAt && interjectionEndedAt < resultReleasedAt && interjectionTranscript.length > 0,
    resultFactsSpoken: resultReleasedAt !== undefined && facts.every(fact => fact.spoken)
      && lastAudioAt > resultReleasedAt && simulatedPlayedBytes > resultReleaseOutputBytes,
  };
}
function currentFreshnessEvidence() {
  const submissions = primaryCalls.filter(value => value.method === 'submit');
  return freshnessEvidence({ snapshot, atSubmit: firstSubmitSnapshot, atResult: resultReleaseSnapshot,
    submittedText: submissions.length === 1 ? String(submissions[0].text) : undefined });
}
function writeEvidence() {
  const call = callId ? broker?.store.get(callId) : broker?.store.list()[0];
  const callerTranscript = snapshot.fragments.filter(x => x.role === 'user').map(x => x.text).join('');
  const assistantTranscript = snapshot.fragments.filter(x => x.role === 'assistant').map(x => x.text).join('');
  const outputBytes = audio.reduce((n, chunk) => n + chunk.length, 0);
  const submissions = primaryCalls.filter(x => x.method === 'submit');
  const conversation = conversationChecks();
  const freshness = freshnessScenario ? { ...currentFreshnessEvidence(),
    exactlyOneSubmit: conversation.exactlyOneSubmit, noFollowupAction: conversation.noFollowupAction,
    resultHoldMs: firstSubmitAt !== undefined && resultReleasedAt !== undefined ? resultReleasedAt - firstSubmitAt : null,
  } : null;
  const callerSubmissionComplete = freshness ? freshness.rawCallerRequestPreserved : conversationScenario ? conversation.originalFirstTurnPreserved : !audioFile || (submissions.length === 1
    && String(submissions[0].text).trim() === callerTranscript.trim());
  const resultAudioAfterRelease = resultReleasedAt !== undefined && lastAudioAt > resultReleasedAt
    && simulatedPlayedBytes > resultReleaseOutputBytes;
  // Freshness requires semantic review; audio/amount candidates do not certify speech.
  const returnedResultSpoken = freshnessScenario ? null : conversationScenario ? conversation.resultFactsSpoken : !audioFile || /blue lantern/i.test(assistantTranscript);
  const mechanicalPassed = !failure && !call?.failure && outputBytes > 0 && peakEnergy > 0.01 && assistantTranscript.length > 0
    && closed?.finalized === true && call?.finalized === true && call?.primaryClosePending === false
    && call?.transcriptCapture?.state === 'complete'
    && (!audioFile || callerTranscript.length > 0) && callerSubmissionComplete
    && (freshnessScenario ? resultAudioAfterRelease && conversation.noFollowupAction && firstSubmitAt !== undefined
      && resultReleasedAt !== undefined && resultReleasedAt > firstSubmitAt : returnedResultSpoken)
    && (!conversationScenario || (conversation.exactlyOneSubmit && conversation.noFollowupAction && conversation.laterCallerRevision && conversation.interjectionBeforeResult && conversation.transcriptAcceptance.accepted))
    && greetingAt !== undefined && (!speechStartedAt || greetingAt < speechStartedAt);
  const conversationFailure = conversationScenario && !conversation.transcriptAcceptance.accepted
    ? { stage: 'conversation_acceptance', code: !conversation.transcriptAcceptance.successfulResultDescribedAccurately
      ? 'unsupported_account_failure_spoken' : !conversation.transcriptAcceptance.internalBriefKeptPrivate
        ? 'internal_call_brief_spoken' : 'caller_asked_to_supply_account_facts' }
    : null;
  const passed = freshnessScenario && mechanicalPassed ? null : Boolean(mechanicalPassed);
  const report = {
    passed, mechanicalPassed: Boolean(mechanicalPassed), semanticReviewRequired: freshnessScenario, stage, failure: failure ?? (call?.failure ? { stage: 'live_session', code: call.failure.code } : conversationFailure),
    elapsedMs: Date.now() - startedAt, moduleRoot, moduleVariant,
    scope: { real: ['Broker server', 'Live preflight', 'Live session', 'provider audio and transcripts'],
      simulated: ['Twilio dispatch and callbacks', 'phone playback receipts', 'Primary account and results'],
      dummyPrimaryResult: fixtureResult, productionAccountAccess: false },
    preflight, callId, outputBytes, peakEnergy, inputFrames, inputAudioFile: audioFile ?? null, instructionsOverride: instructionsFile ?? null,
    scenario: freshnessScenario ? 'freshness' : conversationScenario ? 'portfolio_with_interjection' : 'test_phrase', interjectionAudioFile: interjectionFile ?? null,
    callPurposeFile: callPurposeFile ?? null, purposeDigest, hardDeadlineMs,
    speechStartedAt, speechEndedAt, greetingAt, controlledCloseAt, simulatedPlayedBytes, simulatedClearedBytes,
    playbackEvidence: { kind: 'simulated_receipts', assistantPlaybackComplete: call?.transcriptCapture?.assistantPlaybackComplete === true,
      lastAudiblePlaybackUntil: audiblePlaybackUntil, playbackQueueUntil: playbackUntil,
      limitation: 'Transcript facts and paced simulated receipts establish the result check. Complete assistant capture additionally requires assistantPlaybackComplete; close-time discarded audio remains unconfirmed.' },
    firstSubmitAt, firstSubmitCallerRevision, firstSubmitSnapshot, firstSubmitInputOffsetMs, firstSubmitOutputBytes, resultReleaseOutputBytes, resultReleaseSnapshot, resultReleaseInputOffsetMs, freshness, resultAudioAfterRelease, interjectionStartedAt, interjectionEndedAt, interjectionInputStartMs,
    interjectionStartCallerRevision, resultReleasedAt, resultCallerRevision, conversation: conversationScenario ? conversation : null,
    callerTranscript, assistantTranscript, callerSubmissionComplete, returnedResultSpoken, closed, call, primaryCalls, providerCalls,
    baselineInvalidOutputAudio: call?.failure?.code === 'invalid_output_audio' && outputBytes === 0,
  };
  fs.writeFileSync(path.join(evidenceDir, 'report.json'), JSON.stringify(report, null, 2), { mode: 0o600 });
  fs.writeFileSync(path.join(evidenceDir, 'events.json'), JSON.stringify(events, null, 2), { mode: 0o600 });
  fs.writeFileSync(path.join(evidenceDir, 'transcript.json'), JSON.stringify(snapshot, null, 2), { mode: 0o600 });
  fs.writeFileSync(path.join(evidenceDir, 'output.pcmu'), Buffer.concat(audio), { mode: 0o600 });
  return report;
}
function stopMedia() {
  if (inputTimer) clearInterval(inputTimer);
  for (const timer of playbackTimers) clearTimeout(timer);
  playbackTimers.clear();
}
const deadlineTimer = setTimeout(() => {
  failure = { stage, code: 'qualification_deadline' };
  stopMedia();
  socket?.terminate();
  for (const connection of connections) { try { connection.terminate(); } catch {} }
  broker?.server.closeAllConnections();
  try { broker?.store.close(); } catch {}
  writeEvidence();
  console.log(JSON.stringify({ passed: false, failure, evidenceDir }));
  process.exit(1);
}, hardDeadlineMs);

try {
  // Whitelist three values; never source production.env or import its account,
  // provider, endpoint, signing, or control configuration into this fixture.
  const selected: Record<string, string> = {};
  for (const line of fs.readFileSync(configFile, 'utf8').split(/\r?\n/)) {
    const match = line.match(/^\s*(?:export\s+)?(OPENAI_API_KEY|BROKER_LIVE_VOICE|BROKER_LIVE_MODEL)\s*=\s*(.*?)\s*$/);
    if (match) selected[match[1]] = match[2].replace(/^(['"])(.*)\1$/, '$2');
  }
  if (!selected.OPENAI_API_KEY) throw Object.assign(new Error(), { code: 'missing_openai_key' });
  const callerAudio = audioFile ? fs.readFileSync(audioFile) : undefined;
  if (callerAudio && (!callerAudio.length || callerAudio.length > 120_000)) throw Object.assign(new Error(), { code: 'caller_audio_must_be_raw_8khz_pcmu_under_15_seconds' });
  const interjectionAudio = interjectionFile ? fs.readFileSync(interjectionFile) : undefined;
  if (interjectionAudio && (!interjectionAudio.length || interjectionAudio.length > 80_000)) throw Object.assign(new Error(), { code: 'interjection_audio_must_be_raw_8khz_pcmu_under_10_seconds' });
  const purpose = freshnessScenario ? freshnessPurpose : callPurposeFile ? fs.readFileSync(callPurposeFile, 'utf8') : 'This is a brief voice qualification test. Greet the test caller and ask how you can help.';
  if (!purpose.trim() || purpose.length > 1500) throw Object.assign(new Error(), { code: 'invalid_call_purpose_fixture' });
  purposeDigest = createHash('sha256').update(purpose).digest('hex');
  const requireFromModule = createRequire(path.join(moduleRoot, 'package.json'));
  const Twilio = requireFromModule('twilio') as typeof import('twilio');
  const { WebSocket } = requireFromModule('ws') as typeof import('ws');
  moduleVariant = args.get('--module-variant') ?? (fs.existsSync(path.join(moduleRoot, 'dist/broker/server.js')) ? 'dist' : 'src');
  if (!['src', 'dist'].includes(moduleVariant)) throw new Error('Expected src or dist module variant');
  const extension = moduleVariant === 'dist' ? 'js' : 'ts';
  const load = (relative: string) => import(pathToFileURL(path.join(moduleRoot, moduleVariant, `${relative}.${extension}`)).href);
  const serverModule = await load('broker/server') as typeof import('../src/broker/server.js');
  const liveModule = await load('live/session') as typeof import('../src/live/session.js');
  const { probeLive } = await load('broker/preflight') as typeof import('../src/broker/preflight.js');
  const { inputSpeechEnergy, streamToken } = await load('broker/phone') as typeof import('../src/broker/phone.js');
  const config: BrokerConfig = { host: '127.0.0.1', port: 0, publicOrigin: 'https://broker-qualification.example.test',
    stateDir: path.join(evidenceDir, 'state'), controlToken: 'qualification-control-'.repeat(3), apiKey: selected.OPENAI_API_KEY,
    model: selected.BROKER_LIVE_MODEL || 'gpt-live-1', voice: selected.BROKER_LIVE_VOICE || 'marin',
    twilioAccountSid: `AC${'1'.repeat(32)}`, twilioAuthToken: 'qualification-twilio-dummy-secret',
    from: '+12025550100', allowedTo: ['+12025550123'], apiBaseUrl: 'http://127.0.0.1:1',
    phoneServiceSecret: 'qualification-primary-'.repeat(3), maxCallSeconds: heldResultScenario ? 75 : 45 };
  const sid = `CA${'2'.repeat(32)}`;
  const bindingId = randomUUID();
  const context = { primaryDexterId: randomUUID(), threadId: randomUUID(), displayName: 'Qualification owner', messages: freshnessScenario ? structuredClone(historicalAccountMessages) : [] };
  const requests = new Map<string, PrimaryRequestResult>();
  const primary: PrimaryApiClient = {
    async readiness() { primaryCalls.push({ method: 'readiness' }); return { eligible: true, reason: 'ready' }; },
    async openCall(proof) {
      if (proof.params.CallSid !== sid) throw Object.assign(new Error(), { code: 'fixture_call_mismatch' });
      primaryCalls.push({ method: 'openCall', bindingId }); return { bindingId, callSid: sid, state: 'open', context };
    },
    async getContext() { return { bindingId, callSid: sid, state: 'open', context }; },
    async submit(id, requestId, input) {
      if (id !== bindingId) throw Object.assign(new Error(), { code: 'fixture_binding_mismatch' });
      firstSubmitAt ??= Date.now();
      firstSubmitCallerRevision ??= snapshot.callerRevision;
      firstSubmitSnapshot ??= structuredClone(snapshot);
      firstSubmitInputOffsetMs ??= inputFrames * 20;
      firstSubmitOutputBytes ??= audio.reduce((n, chunk) => n + chunk.length, 0);
      primaryCalls.push({ method: 'submit', requestId, text: input.text, source: structuredClone(input.source), textDigest: createHash('sha256').update(input.text).digest('hex'), elapsedMs: Date.now() - startedAt, callerRevision: snapshot.callerRevision, inputOffsetMs: inputFrames * 20 });
      if (heldResultScenario) {
        const pending: PrimaryRequestResult = { bindingId, requestId, state: 'running' };
        requests.set(requestId, pending);
        return pending;
      }
      const result: PrimaryRequestResult = { bindingId, requestId, state: 'completed', assistant: {
        messageId: randomUUID(), eventId: randomUUID(), sequence: '1', text: fixtureResult } };
      requests.set(requestId, result); return result;
    },
    async result(_id, requestId) {
      primaryCalls.push({ method: 'result', requestId });
      let result = requests.get(requestId);
      if (!result) throw Object.assign(new Error(), { code: 'fixture_request_missing' });
      const laterCaller = interjectionStartCallerRevision !== undefined && snapshot.callerRevision > interjectionStartCallerRevision
        && conversationChecks().interjectionTranscript.length > 0;
      const mayRelease = freshnessScenario
        ? freshnessMayRelease(Date.now(), firstSubmitAt, speechEndedAt, lastCallerTranscriptAt)
        : conversationScenario && interjectionEndedAt !== undefined && laterCaller
          && Date.now() - interjectionEndedAt >= 1800 && Date.now() - lastCallerTranscriptAt >= 1500;
      if (result.state === 'running' && mayRelease) {
        resultReleasedAt ??= Date.now();
        resultReleaseTranscriptRevision ??= snapshot.revision;
        resultCallerRevision ??= snapshot.callerRevision;
        resultReleaseSnapshot ??= structuredClone(snapshot);
        resultReleaseInputOffsetMs ??= inputFrames * 20;
        if (freshnessScenario) fixtureResult = freshnessResult(new Date(resultReleasedAt).toISOString());
        resultReleaseOutputBytes = audio.reduce((n, chunk) => n + chunk.length, 0);
        result = { bindingId, requestId, state: 'completed', assistant: {
          messageId: randomUUID(), eventId: randomUUID(), sequence: '1', text: fixtureResult } };
        requests.set(requestId, result);
        note({ type: 'fixture.primary_result_released', requestId, callerRevision: snapshot.callerRevision, inputOffsetMs: inputFrames * 20, result: structuredClone(result) });
      }
      return result;
    },
    async question() { primaryCalls.push({ method: 'question' }); throw Object.assign(new Error(), { code: 'fixture_question_not_configured' }); },
    async reply() { primaryCalls.push({ method: 'reply' }); throw Object.assign(new Error(), { code: 'fixture_question_not_configured' }); },
    async captureTurn(_id, input) {
      primaryCalls.push({ method: 'captureTurn', role: input.role, text: input.text });
      return { bindingId, sourceEventId: input.sourceEventId, revision: input.revision, recordId: input.sourceEventId, replayed: false };
    },
    async close() { primaryCalls.push({ method: 'close', bindingId }); return { bindingId, callSid: sid, state: 'closed' }; },
  };
  const provider = { calls: Object.assign((callSid: string) => ({
    async update(input: unknown) { providerCalls.push({ method: 'update', callSid, input }); return { sid: callSid, status: 'completed' }; },
    async fetch() { providerCalls.push({ method: 'fetch', callSid }); return { sid: callSid, status: 'completed', from: config.from, to: config.allowedTo[0] }; },
  }), { async create(input: unknown) { providerCalls.push({ method: 'create', input }); return { sid, status: 'queued' }; } }) };
  broker = serverModule.createBrokerServer({ config, primary, provider: provider as unknown as ReturnType<typeof Twilio>,
    probe: async options => { stage = 'preflight'; preflight = await probeLive(options); note({ type: 'preflight.complete', result: preflight }); return preflight as Awaited<ReturnType<typeof probeLive>>; },
    voiceFactory: (options: BrokerLiveSessionOptions) => liveModule.createBrokerLiveSession({ ...options,
      ...(instructionsFile ? { instructions: fs.readFileSync(instructionsFile, 'utf8') } : {}),
      connectionFactory: apiKey => {
        const connection = liveModule.createOpenAILiveConnection(apiKey);
        connections.add(connection);
        return { ...connection, onEvent: listener => connection.onEvent(value => {
          const event = value && typeof value === 'object' ? value as Record<string, unknown> : {};
          const nested = (v: unknown) => v && typeof v === 'object' ? Object.keys(v).sort() : [];
          note({ type: 'provider.event_shape', eventType: event.type, keys: Object.keys(event).sort(),
            audioKeys: nested(event.audio), sessionKeys: nested(event.session),
            resolvedAudio: event.type === 'session.started' ? (event.session as Record<string, unknown>)?.audio : undefined,
            deltaType: typeof event.delta, deltaLength: typeof event.delta === 'string' ? event.delta.length : undefined });
          listener(value);
        }) };
      },
      onTranscript: value => {
        if (value.callerRevision > snapshot.callerRevision) lastCallerTranscriptAt = Date.now();
        snapshot = value;
        if (greetingAt === undefined && /dexter/i.test(value.fragments.filter(f => f.role === 'assistant').map(f => f.text).join(''))) greetingAt = Date.now();
        return options.onTranscript(value);
      },
      onClosed: value => { closed = value; note({ type: 'live.closed', result: value }); options.onClosed(value); },
      onEvent: value => { note({ source: 'adapter', ...value }); options.onEvent?.(value); },
    }),
  });
  await new Promise<void>(resolve => broker!.server.listen(0, '127.0.0.1', resolve));
  const base = `http://127.0.0.1:${(broker.server.address() as { port: number }).port}`;
  const post = (route: string, value: unknown) => fetch(base + route, { method: 'POST', signal: AbortSignal.timeout(17_000),
    headers: { authorization: `Bearer ${config.controlToken}`, 'content-type': 'application/json' }, body: JSON.stringify(value) });
  stage = 'outbound_admission';
  const admission = await post('/internal/broker/calls', { requestId: randomUUID(), purpose });
  const admitted = await admission.json() as { call?: { id: string }; error?: string };
  callId = admitted.call?.id ?? broker.store.list()[0]?.id;
  if (admission.status !== 201 || !callId) throw Object.assign(new Error(), { code: broker.store.list()[0]?.failure?.code || 'outbound_admission_failed' });
  stage = 'signed_answer';
  const route = `/broker/answer/${callId}`;
  const params = { AccountSid: config.twilioAccountSid, CallSid: sid, From: config.from, To: config.allowedTo[0], Direction: 'outbound-api', CallStatus: 'in-progress' };
  const answer = await fetch(base + route, { method: 'POST', signal: AbortSignal.timeout(2000),
    headers: { 'content-type': 'application/x-www-form-urlencoded', 'x-twilio-signature': Twilio.getExpectedTwilioSignature(config.twilioAuthToken, config.publicOrigin + route, params) },
    body: new URLSearchParams(params) });
  if (answer.status !== 200 || !(await answer.text()).includes('<Stream')) throw Object.assign(new Error(), { code: 'signed_answer_failed' });
  stage = 'media';
  const mediaRoute = `/broker/media/${callId}`;
  socket = new WebSocket(base.replace('http:', 'ws:') + mediaRoute, { headers: {
    'x-twilio-signature': Twilio.getExpectedTwilioSignature(config.twilioAuthToken, config.publicOrigin + mediaRoute, {}),
  } });
  const streamSid = `MZ${'3'.repeat(32)}`;
  let pendingMarkBytes = 0;
  socket.on('message', raw => {
    const frame = JSON.parse(raw.toString());
    if (frame.event === 'media') {
      const bytes = Buffer.from(frame.media.payload, 'base64');
      audio.push(bytes);
      const energy = inputSpeechEnergy(bytes);
      if (energy > 0.01) lastAudioAt = Date.now();
      peakEnergy = Math.max(peakEnergy, energy);
      pendingMarkBytes += bytes.length;
      playbackUntil = Math.max(Date.now(), playbackUntil) + bytes.length / 8;
      if (energy > 0.01) audiblePlaybackUntil = playbackUntil;
    } else if (frame.event === 'mark') {
      const bytes = pendingMarkBytes; pendingMarkBytes = 0;
      const timer = setTimeout(() => {
        playbackTimers.delete(timer);
        if (socket?.readyState === WebSocket.OPEN) {
          socket.send(JSON.stringify({ event: 'mark', streamSid, mark: { name: frame.mark.name } }));
          simulatedPlayedBytes += bytes;
        }
      }, Math.max(0, playbackUntil - Date.now()));
      playbackTimers.add(timer);
    } else if (frame.event === 'clear') {
      for (const timer of playbackTimers) clearTimeout(timer);
      playbackTimers.clear();
      simulatedClearedBytes = audio.reduce((n, bytes) => n + bytes.length, 0) - simulatedPlayedBytes;
      playbackUntil = Date.now();
      note({ type: 'simulated_playback_clear' });
    }
  });
  await new Promise<void>((resolve, reject) => { socket!.once('open', resolve); socket!.once('error', reject); });
  socket.on('error', () => { failure ??= { stage: 'media', code: 'fixture_media_socket_error' }; });
  socket.send(JSON.stringify({ event: 'start', start: { accountSid: config.twilioAccountSid, callSid: sid, streamSid,
    customParameters: { token: streamToken(config.controlToken, callId, sid) }, mediaFormat: { encoding: 'audio/x-mulaw', sampleRate: 8000, channels: 1 } } }));
  const mediaStarted = Date.now();
  inputTimer = setInterval(() => {
    if (socket?.readyState !== WebSocket.OPEN) return;
    const now = Date.now();
    const quiet = now - lastAudioAt > 1200;
    if (callerAudio && speechStartedAt === undefined && greetingAt !== undefined && now - mediaStarted >= 8000 && quiet) speechStartedAt = now;
    const bytes = Buffer.alloc(160, 0xff);
    if (callerAudio && speechStartedAt !== undefined && speechOffset < callerAudio.length) {
      const end = Math.min(speechOffset + 160, callerAudio.length);
      callerAudio.copy(bytes, 0, speechOffset, end); speechOffset = end;
      if (speechOffset === callerAudio.length) speechEndedAt = now;
    } else if (interjectionAudio && firstSubmitAt !== undefined && speechEndedAt !== undefined) {
      if (interjectionStartedAt === undefined && now > firstSubmitAt + 500 && now > speechEndedAt + 1500 && quiet) {
        interjectionStartedAt = now;
        interjectionInputStartMs = inputFrames * 20;
        interjectionStartCallerRevision = snapshot.callerRevision;
        note({ type: 'fixture.interjection_started', callerRevision: snapshot.callerRevision, inputStartMs: interjectionInputStartMs });
      }
      if (interjectionStartedAt !== undefined && interjectionOffset < interjectionAudio.length) {
        const end = Math.min(interjectionOffset + 160, interjectionAudio.length);
        interjectionAudio.copy(bytes, 0, interjectionOffset, end); interjectionOffset = end;
        if (interjectionOffset === interjectionAudio.length) interjectionEndedAt = now;
      }
    }
    socket.send(JSON.stringify({ event: 'media', streamSid, media: { track: 'inbound', chunk: String(++inputFrames), timestamp: String((inputFrames - 1) * 20), payload: bytes.toString('base64') } }));
  }, 20);
  while (!closed && Date.now() - startedAt < conversationDeadlineMs) {
    await new Promise(resolve => setTimeout(resolve, 100));
    const quiet = lastAudioAt > 0 && Date.now() - lastAudioAt > 1500;
    const callerDone = !callerAudio || (speechEndedAt !== undefined && Date.now() - speechEndedAt > 5000 && snapshot.callerRevision > 0);
    const scenarioDone = freshnessScenario ? resultReleasedAt !== undefined && Date.now() - resultReleasedAt > 5000
      && lastAudioAt > resultReleasedAt && Date.now() - audiblePlaybackUntil > 1500
      : !conversationScenario || (conversationChecks().resultFactsSpoken && Date.now() - audiblePlaybackUntil > 1500);
    if (quiet && callerDone && scenarioDone && peakEnergy > 0.01 && snapshot.fragments.some(x => x.role === 'assistant') && Date.now() - mediaStarted > 5000) break;
  }
  stage = 'controlled_close';
  controlledCloseAt = Date.now();
  if (!closed) await post('/internal/broker/end', { id: callId }).then(response => response.arrayBuffer());
  while (broker.store.get(callId)?.primaryClosePending && Date.now() - startedAt < hardDeadlineMs - 2000) await new Promise(resolve => setTimeout(resolve, 50));
  stage = 'complete';
  completed = true;
} catch (error) {
  failure = { stage, code: code(error) };
  note({ type: 'qualification.failure', ...failure });
} finally {
  stopMedia();
  socket?.terminate();
  try { await broker?.close(); } catch { failure ??= { stage: 'cleanup', code: 'broker_close_failed' }; }
  for (const connection of connections) { try { connection.terminate(); } catch {} }
  clearTimeout(deadlineTimer);
  const report = writeEvidence();
  console.log(JSON.stringify({ passed: report.passed, mechanicalPassed: report.mechanicalPassed, semanticReviewRequired: report.semanticReviewRequired, completed, failure: report.failure, outputBytes: report.outputBytes,
    peakEnergy: report.peakEnergy, finalized: closed?.finalized ?? false, baselineInvalidOutputAudio: report.baselineInvalidOutputAudio, evidenceDir }));
  process.exitCode = report.mechanicalPassed ? 0 : 1;
}
