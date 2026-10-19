/**
 * One scripted phone call against the REAL gpt-live-1 through the broker's real server,
 * generation and Live session code (loaded from --module-root, so a previous release can
 * run as a baseline). Twilio media, the Dexter API (Primary) and its eligible context are
 * local fakes with scripted results and timings. Writes evidence.json into --out.
 *
 *   node --import tsx evals/run-one.ts --module-root /abs/release --scenario sell_fail_buyback \
 *     --out /abs/new/dir --seed 1 --config-file /abs/production.env --audio-cache /abs/cache
 *
 * Only OPENAI_API_KEY is read from the config file. No phone call is placed.
 */
import fs from 'node:fs';
import path from 'node:path';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { randomUUID, randomBytes } from 'node:crypto';
import { scenarios, fixturePortfolio, type RequestScript, type CallerTurn } from './scenarios.js';
import { callerAudio, callerVoice, pcmuEnergy } from './audio.js';

const args = new Map<string, string>();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i]!, process.argv[i + 1]!);
const need = (flag: string) => { const value = args.get(flag); if (!value) throw new Error(`missing ${flag}`); return value; };
const moduleRoot = fs.realpathSync(need('--module-root'));
const scenario = scenarios[need('--scenario')];
if (!scenario) throw new Error('unknown scenario');
const out = path.resolve(need('--out'));
const seed = Number(args.get('--seed') ?? '0');
const audioCache = path.resolve(need('--audio-cache'));
fs.mkdirSync(out, { recursive: true, mode: 0o700 });
let apiKey = '';
for (const line of fs.readFileSync(need('--config-file'), 'utf8').split(/\r?\n/)) {
  const match = line.match(/^\s*(?:export\s+)?OPENAI_API_KEY\s*=\s*(.*?)\s*$/);
  if (match) apiKey = match[1]!.replace(/^(['"])(.*)\1$/, '$2');
}
if (!apiKey) throw new Error('missing OPENAI_API_KEY');

const t0 = Date.now();
const now = () => Date.now() - t0;
const timeline: Array<Record<string, unknown>> = [];
const note = (type: string, data: Record<string, unknown> = {}) => { timeline.push({ at: now(), type, ...data }); };
const brokerLogs: Array<Record<string, unknown>> = [];
for (const level of ['info', 'log', 'error', 'warn'] as const) {
  console[level] = (...values: unknown[]) => {
    const text = values.map(value => typeof value === 'string' ? value : JSON.stringify(value)).join(' ');
    try { brokerLogs.push({ at: now(), level, ...JSON.parse(text) }); } catch { brokerLogs.push({ at: now(), level, text: text.slice(0, 4000) }); }
  };
}
process.on('unhandledRejection', error => { note('harness.unhandled_rejection', { message: String((error as Error)?.message ?? error).slice(0, 400) }); });

const load = (relative: string) => import(pathToFileURL(path.join(moduleRoot, 'src', `${relative}.ts`)).href);
const requireFromRoot = createRequire(path.join(moduleRoot, 'package.json'));
const Twilio = requireFromRoot('twilio');
const { WebSocket } = requireFromRoot('ws');

const spend: { ttsChars: number; liveSeconds: number } = { ttsChars: 0, liveSeconds: 0 };
const voiceName = callerVoice(seed);
const turnAudio = new Map<string, Buffer>();
for (const turn of scenario.turns) turnAudio.set(turn.id, await callerAudio(apiKey, turn.text, voiceName, audioCache, spend));

const serverModule = await load('broker/server');
const storeModule = await load('broker/store');
const liveModule = await load('live/session');
const { streamToken } = await load('broker/phone');
const greetingModule = await load('broker/greetingClip');

// --- Identities and fake Dexter API -------------------------------------------------------
const hex = (n: number) => randomBytes(n).toString('hex');
const callSid = `CA${hex(16)}`, streamSid = `MZ${hex(16)}`;
const bindingId = randomUUID(), primaryDexterId = randomUUID(), threadId = randomUUID(), ownerPrincipalId = randomUUID(), runId = randomUUID();
const at = new Date().toISOString();
const version = { ownerPrincipalId, primaryDexterId, threadId, contextEpoch: '1', threadEventSequence: '1000', digest: 'a'.repeat(64) };
const admission = { invalidationDigest: 'b'.repeat(64), messages: [], memories: [] };
const eligibleCall = () => ({ bindingId, callSid, state: 'open' as const, context: { primaryDexterId, threadId, displayName: 'Sam',
  messages: [], history: { version, admission, pageDigest: 'c'.repeat(64), localContext: '', nextBeforeSequence: null, hasMore: false, omittedForBudget: false } } });

interface Req { index: number; requestId: string; script: RequestScript; submittedAt: number; text: string;
  messageId: string; eventId: string; partialIds: string[]; shownPartials: Set<number>; finalAt?: number }
const requests: Req[] = [];
const byId = new Map<string, Req>();
const turnEnds = new Map<string, number>();
let audibleUntil = -1, speechStartedAt = -1, lastAudibleChunkAt = -1;
const speakingSince = () => now() < audibleUntil ? speechStartedAt : -1;

function finalDue(req: Req): boolean {
  const final = req.script.final, elapsed = now() - req.submittedAt;
  if (final.atMs !== undefined) return elapsed >= final.atMs;
  const rule = final.whileSpeakingAfterTurn!;
  if (elapsed >= rule.fallbackMs) return true;
  const ended = turnEnds.get(rule.turn), since = speakingSince();
  return ended !== undefined && since > ended && now() - since >= rule.minSpeakingMs;
}
function state(req: Req) {
  if (req.finalAt === undefined && finalDue(req)) {
    req.finalAt = now();
    note('primary.final_visible', { request: req.index + 1, requestId: req.requestId, state: req.script.final.state, text: req.script.final.text });
  }
  if (req.finalAt !== undefined) {
    const final = req.script.final;
    if (final.state === 'completed') return { bindingId, requestId: req.requestId, state: 'completed',
      assistant: { messageId: req.messageId, eventId: req.eventId, sequence: String(10 + req.index * 2), text: final.text! } };
    return { bindingId, requestId: req.requestId, state: final.state, error: { code: final.state === 'cancelled' ? 'cancelled' : 'primary_failed', retryable: false } };
  }
  const elapsed = now() - req.submittedAt;
  const partials = req.script.partials ?? [];
  let latest = -1;
  partials.forEach((partial, index) => { if (partial.atMs <= elapsed) latest = index; });
  if (latest < 0) return { bindingId, requestId: req.requestId, state: 'running' };
  if (!req.shownPartials.has(latest)) {
    req.shownPartials.add(latest);
    note('primary.partial_visible', { request: req.index + 1, requestId: req.requestId, text: partials[latest]!.text });
  }
  const partial = partials[latest]!;
  return { bindingId, requestId: req.requestId, state: 'running', partialResult: { kind: partial.kind, text: partial.text,
    toolInvocationId: req.partialIds[latest], observedAt: new Date(t0 + req.submittedAt + partial.atMs).toISOString(),
    ...(partial.kind === 'action_status' ? { intentId: req.requestId, actionState: partial.actionState } : {}) } };
}
const primary = {
  async readiness() { return { eligible: true, reason: 'ready' }; },
  async openCall() { return eligibleCall(); },
  async getContext() { return eligibleCall(); },
  async submit(_binding: string, requestId: string, input: { text: string; voiceContext?: unknown }) {
    let req = byId.get(requestId);
    if (!req) {
      const index = requests.length;
      const script = scenario.requests[index] ?? scenario.extra!;
      req = { index, requestId, script, submittedAt: now(), text: input.text, messageId: randomUUID(), eventId: randomUUID(),
        partialIds: (script.partials ?? []).map(() => randomUUID()), shownPartials: new Set() };
      requests.push(req); byId.set(requestId, req);
      note('primary.submit', { request: index + 1, requestId, text: input.text, scripted: index < scenario.requests.length,
        voiceContext: input.voiceContext ?? null });
    }
    return { bindingId, requestId, state: 'queued' };
  },
  async result(_binding: string, requestId: string) {
    const req = byId.get(requestId);
    if (!req) throw Object.assign(new Error('unknown request'), { code: 'fixture_request_missing' });
    return state(req);
  },
  async question() { throw new Error('fixture_question_not_configured'); },
  async reply() { throw new Error('fixture_question_not_configured'); },
  async reconcile() { throw new Error('fixture_reconcile_not_configured'); },
  async captureTurn(_binding: string, turn: { sourceEventId: string; revision: number }) {
    return { bindingId, sourceEventId: turn.sourceEventId, revision: turn.revision, recordId: turn.sourceEventId, replayed: false };
  },
  async close() { return { bindingId, callSid, state: 'closed' }; },
  async portfolio() { return fixturePortfolio(bindingId); },
};
const eligible = {
  async fetch() { return eligibleCall(); },
  async assert(_binding: unknown, requested: unknown, adm?: unknown) {
    return { bindingId, callSid, current: true, version: requested, ...(adm ? { admission: adm } : {}) };
  },
  async delivery(_binding: unknown, requestId: string, input: { version: unknown; admission?: unknown; source: { kind: string; messageId?: string } }) {
    const req = byId.get(requestId);
    if (!req || req.finalAt === undefined || req.script.final.state !== 'completed' || input.source.messageId !== req.messageId)
      return { bindingId, callSid, requestId, version: input.version, ...(input.admission ? { admission: input.admission } : {}),
        delivery: { state: 'unavailable', reason: 'not_ready' } };
    note('primary.delivery_read', { request: req.index + 1, requestId });
    return { bindingId, callSid, requestId, version: input.version, ...(input.admission ? { admission: input.admission } : {}),
      delivery: { state: 'ready', source: { kind: 'assistant', messageId: req.messageId, eventSequence: String(10 + req.index * 2),
        provenance: { messageCreatedAt: at, eventId: req.eventId, eventOccurredAt: at, runId } }, text: req.script.final.text! } };
  },
};
const provider = { calls: Object.assign((sid: string) => ({
  async update() { note('provider.update'); return { sid, status: 'completed' }; },
  async fetch() { return { sid, status: 'in-progress', from: '+12025550123', to: '+12025550100' }; },
}), { async create() { return { sid: callSid, status: 'queued' }; } }) };

// --- Live connection capture ---------------------------------------------------------------
let sessionStartedAt: number | undefined;
const wrap = (connection: any) => ({
  ...connection,
  send(event: any) {
    if (event?.type && event.type !== 'session.input_audio.append') {
      const record: Record<string, unknown> = { kind: event.type, eventId: event.event_id };
      if ('delegation_id' in event) record.delegationId = event.delegation_id;
      if (typeof event.content === 'string') record.content = event.content;
      if (event.type === 'session.start') record.instructionsBytes = Buffer.byteLength(event.session?.instructions ?? '');
      note('live.client', record);
    }
    return connection.send(event);
  },
  onEvent(listener: (value: unknown) => void) {
    connection.onEvent((value: any) => {
      const type = value?.type;
      if (type === 'session.started') { sessionStartedAt = now(); note('live.server', { kind: type, sessionId: value.session?.id }); }
      else if (type === 'session.output_transcript.delta' || type === 'session.input_transcript.delta')
        note('live.transcript', { role: type.startsWith('session.output') ? 'assistant' : 'user', delta: value.delta, startMs: value.start_ms, endMs: value.end_ms });
      else if (type === 'session.delegation.created') note('live.server', { kind: type, delegationId: value.delegation?.id, offsetMs: value.offset_ms });
      else if (typeof type === 'string' && /\.appended$/.test(type)) note('live.server', { kind: type, clientEventId: value.client_event_id, startMs: value.start_ms, endMs: value.end_ms });
      else if (type === 'error') note('live.server', { kind: type, error: value.error });
      else if (type === 'session.closed') { note('live.server', { kind: type, reason: value.reason, usage: value.usage }); spend.liveSeconds = Math.max(spend.liveSeconds, Number(value.usage?.seconds ?? 0)); }
      else if (type === 'session.usage.updated') spend.liveSeconds = Math.max(spend.liveSeconds, Number(value.usage?.seconds ?? 0));
      else if (type && type !== 'session.output_audio.delta') note('live.server', { kind: type });
      listener(value);
    });
  },
});

// --- Broker ------------------------------------------------------------------------------
const stateDir = path.join(out, 'state');
const store = new storeModule.BrokerStore(stateDir);
const reserved = store.reserve({ requestId: randomUUID(), number: '+12025550123', direction: 'inbound', purpose: 'The person called Dexter.', callSid }).call;
store.recordPrimaryBinding(reserved.id, eligibleCall());
const callId = reserved.id;
const config = { host: '127.0.0.1', port: 0, publicOrigin: 'https://phone-eval.example.test', stateDir,
  controlToken: 'eval-control-token-'.repeat(3), apiKey, model: 'gpt-live-1', voice: 'marin',
  twilioAccountSid: `AC${'1'.repeat(32)}`, twilioAuthToken: 'eval-twilio-dummy-secret', from: '+12025550100',
  allowedTo: ['+12025550123'], apiBaseUrl: 'http://127.0.0.1:1', phoneServiceSecret: 'eval-primary-secret-'.repeat(3),
  maxCallSeconds: Math.ceil(scenario.maxMs / 1000) + 30 };
const clip = greetingModule.loadGreetingClip(greetingModule.INBOUND_GREETING_CLIP_ASSET);
const broker = serverModule.createBrokerServer({ config, store, primary, eligibleContext: eligible, provider,
  ...(clip ? { inboundGreetingClip: clip } : {}),
  voiceFactory: (options: any) => liveModule.createBrokerLiveSession({ ...options,
    connectionFactory: (key: string) => wrap(liveModule.createOpenAILiveConnection(key)) }) });
await new Promise<void>(resolve => broker.server.listen(0, '127.0.0.1', resolve));
const port = (broker.server.address() as { port: number }).port;

// --- Simulated Twilio media stream -------------------------------------------------------
const route = `/broker/media/${callId}`;
const socket = new WebSocket(`ws://127.0.0.1:${port}${route}`, { headers: {
  'x-twilio-signature': Twilio.getExpectedTwilioSignature(config.twilioAuthToken, config.publicOrigin + route, {}) } });
const outputAudio: Buffer[] = [];
let playbackUntil = 0, leadMax = 0;
const markTimers = new Set<ReturnType<typeof setTimeout>>();
socket.on('message', (raw: Buffer) => {
  const frame = JSON.parse(raw.toString());
  if (frame.event === 'media') {
    const bytes = Buffer.from(frame.media.payload, 'base64');
    outputAudio.push(bytes);
    const startAt = Math.max(now(), playbackUntil);
    playbackUntil = startAt + bytes.length / 8;
    leadMax = Math.max(leadMax, playbackUntil - now());
    if (pcmuEnergy(bytes) > 0.01) {
      if (startAt - audibleUntil > 350) { speechStartedAt = startAt; note('audio.assistant_speech_start', { playbackAt: startAt }); }
      audibleUntil = playbackUntil; lastAudibleChunkAt = now();
    }
  } else if (frame.event === 'mark') {
    const timer = setTimeout(() => { markTimers.delete(timer);
      if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify({ event: 'mark', streamSid, mark: { name: frame.mark.name } })); },
    Math.max(0, playbackUntil - now()));
    markTimers.add(timer);
  } else if (frame.event === 'clear') {
    for (const timer of markTimers) clearTimeout(timer);
    markTimers.clear();
    note('audio.clear', { droppedMs: Math.max(0, playbackUntil - now()) });
    playbackUntil = now(); audibleUntil = Math.min(audibleUntil, now());
  }
});
await new Promise<void>((resolve, reject) => { socket.once('open', resolve); socket.once('error', reject); });
socket.send(JSON.stringify({ event: 'start', start: { accountSid: config.twilioAccountSid, callSid, streamSid,
  customParameters: { token: streamToken(config.controlToken, callId, callSid) }, mediaFormat: { encoding: 'audio/x-mulaw', sampleRate: 8000, channels: 1 } } }));
note('media.start', { voice: voiceName, clipMs: clip ? clip.length / 8 : 0 });

// --- Caller turn scheduler -----------------------------------------------------------------
let current: { turn: CallerTurn; audio: Buffer; offset: number } | undefined;
const pending = [...scenario.turns];
// Live streams near-silent frames continuously; only audible frames (at their playback time) count as speech.
const quietFor = (ms: number) => now() - Math.max(audibleUntil, 0) >= ms;
const liveActive = () => store.get(callId)?.liveGenerations?.phase === 'active';
const clipMs = clip ? clip.length / 8 : 0;
function due(turn: CallerTurn): boolean {
  const quiet = quietFor(turn.quietMs ?? 700);
  const when = turn.when;
  if (when.kind === 'greeted') return liveActive() && now() >= clipMs + 600 && quiet;
  if (when.kind === 'afterSubmit') { const req = requests[when.request - 1]; return !!req && now() - req.submittedAt >= when.delayMs && quiet; }
  if (when.kind === 'afterTurn') { const end = turnEnds.get(when.turn); return end !== undefined && now() - end >= when.delayMs && quiet; }
  const req = requests[when.request - 1];
  if (!req || req.finalAt === undefined) return false;
  const after = req.finalAt + (when.delayMs ?? 0);
  // The assistant has spoken since the result (or 15 s passed) and is now quiet.
  return now() >= after && ((lastAudibleChunkAt > req.finalAt && quiet) || now() - req.finalAt > 15_000);
}
const tick = setInterval(() => {
  if (socket.readyState !== WebSocket.OPEN) return;
  if (!current && pending.length && due(pending[0]!)) {
    const turn = pending.shift()!;
    current = { turn, audio: turnAudio.get(turn.id)!, offset: 0 };
    note('caller.turn_start', { turn: turn.id, text: turn.text });
  }
  const payload = Buffer.alloc(160, 0xff);
  if (current) {
    const end = Math.min(current.offset + 160, current.audio.length);
    current.audio.copy(payload, 0, current.offset, end); current.offset = end;
    if (current.offset >= current.audio.length) {
      turnEnds.set(current.turn.id, now()); note('caller.turn_end', { turn: current.turn.id }); current = undefined;
    }
  }
  socket.send(JSON.stringify({ event: 'media', streamSid, media: { track: 'inbound', payload: payload.toString('base64') } }));
}, 20);

// --- Run until the scenario settles ----------------------------------------------------------
// Ends once every result is in and the assistant has been quiet long enough to show it is not
// about to add or repeat anything. Same window for every variant, so they are compared alike.
const settled = () => !current && !pending.length && requests.length >= scenario.requests.length
  && requests.every(req => req.finalAt !== undefined) && quietFor(6000)
  && requests.every(req => now() - req.finalAt! > 8000);
let endReason = 'max_duration';
while (now() < scenario.maxMs) {
  await new Promise(resolve => setTimeout(resolve, 200));
  if (store.get(callId)?.endedAt) { endReason = 'call_ended'; break; }
  if (settled()) { endReason = 'settled'; break; }
}
note('harness.end', { reason: endReason });
clearInterval(tick);
if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify({ event: 'stop', streamSid }));
const closeDeadline = Date.now() + 20_000;
while (!store.get(callId)?.finalized && store.get(callId)?.primaryClosePending !== false && Date.now() < closeDeadline)
  await new Promise(resolve => setTimeout(resolve, 100));
await new Promise(resolve => setTimeout(resolve, 500));
socket.terminate();
for (const timer of markTimers) clearTimeout(timer);
try { await Promise.race([broker.close(), new Promise(resolve => setTimeout(resolve, 15_000))]); } catch { /* evidence still written */ }
const call = store.get(callId);
const evidence = {
  scenario: scenario.id, seed, moduleRoot, callerVoice: voiceName, endReason, startedAt: new Date(t0).toISOString(),
  durationMs: now(), sessionStartedAt, spend, playbackLeadMaxMs: Math.round(leadMax), timeline, brokerLogs: brokerLogs.filter(log => typeof log.event === 'string' || log.level === 'error'),
  requests: requests.map(req => ({ index: req.index + 1, requestId: req.requestId, label: req.script.label, scripted: req.index < scenario.requests.length,
    submittedAt: req.submittedAt, finalAt: req.finalAt ?? null, outcome: req.script.outcome, text: req.text })),
  call: call ? { failure: call.failure ?? null, finalized: call.finalized ?? null, voiceCloses: (call as any).voiceCloses ?? null,
    transcript: call.liveGenerations?.segments.map((segment: any) => segment.transcript) ?? [] } : null,
};
fs.writeFileSync(path.join(out, 'evidence.json'), JSON.stringify(evidence, null, 1), { mode: 0o600 });
fs.writeFileSync(path.join(out, 'output.pcmu'), Buffer.concat(outputAudio), { mode: 0o600 });
fs.rmSync(stateDir, { recursive: true, force: true });
process.stdout.write(JSON.stringify({ scenario: scenario.id, seed, endReason, liveSeconds: spend.liveSeconds, out }) + '\n');
process.exit(0);
