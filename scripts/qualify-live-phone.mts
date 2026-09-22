import fs from 'node:fs';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { randomUUID } from 'node:crypto';
import type { WebSocket as Socket } from 'ws';
import type { BrokerConfig } from '../src/broker/config.js';
import type { PrimaryApiClient, PrimaryRequestResult } from '../src/broker/primaryApi.js';
import type { BrokerLiveConnection, BrokerLiveSessionOptions, LiveSessionClosed, LiveTranscriptSnapshot } from '../src/live/types.js';
import type { createBrokerServer as ServerFactory } from '../src/broker/server.js';

// Executes funded Live calls only when an operator runs this script. Twilio,
// account identity, Primary requests, and playback receipts are local fixtures.
const usage = 'node --import tsx scripts/qualify-live-phone.mts --module-root /absolute/release --evidence-dir /new/absolute/directory --config-file /absolute/production.env [--input-audio /absolute/caller.pcmu]';
const args = new Map<string, string>();
for (let i = 2; i < process.argv.length; i += 2) {
  const key = process.argv[i];
  if (!['--module-root', '--module-variant', '--evidence-dir', '--config-file', '--input-audio', '--instructions-file'].includes(key) || !process.argv[i + 1] || args.has(key)) throw new Error(usage);
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
let greetingAt: number | undefined;
let controlledCloseAt: number | undefined;
let peakEnergy = 0;
let completed = false;
const code = (error: unknown) => {
  const candidate = (error as { code?: unknown })?.code;
  return typeof candidate === 'string' && /^[a-zA-Z0-9_:-]{1,100}$/.test(candidate) ? candidate : 'qualification_error';
};
const note = (value: Record<string, unknown>) => { events.push({ elapsedMs: Date.now() - startedAt, ...value }); };
function writeEvidence() {
  const call = callId ? broker?.store.get(callId) : broker?.store.list()[0];
  const callerTranscript = snapshot.fragments.filter(x => x.role === 'user').map(x => x.text).join('');
  const assistantTranscript = snapshot.fragments.filter(x => x.role === 'assistant').map(x => x.text).join('');
  const outputBytes = audio.reduce((n, chunk) => n + chunk.length, 0);
  const submissions = primaryCalls.filter(x => x.method === 'submit');
  const callerSubmissionComplete = !audioFile || (submissions.length === 1
    && String(submissions[0].text).trim() === callerTranscript.trim());
  const returnedResultSpoken = !audioFile || /blue lantern/i.test(assistantTranscript);
  const passed = !failure && !call?.failure && outputBytes > 0 && peakEnergy > 0.01 && assistantTranscript.length > 0
    && closed?.finalized === true && call?.finalized === true && call?.primaryClosePending === false
    && call?.transcriptCapture?.state === 'complete'
    && (!audioFile || callerTranscript.length > 0) && callerSubmissionComplete && returnedResultSpoken
    && greetingAt !== undefined && (!speechStartedAt || greetingAt < speechStartedAt);
  const report = {
    passed, stage, failure: failure ?? (call?.failure ? { stage: 'live_session', code: call.failure.code } : null),
    elapsedMs: Date.now() - startedAt, moduleRoot, moduleVariant,
    scope: { real: ['Broker server', 'Live preflight', 'Live session', 'provider audio and transcripts'],
      simulated: ['Twilio dispatch and callbacks', 'phone playback receipts', 'Primary account and results'],
      dummyPrimaryResult: 'Your test phrase is blue lantern.', productionAccountAccess: false },
    preflight, callId, outputBytes, peakEnergy, inputFrames, inputAudioFile: audioFile ?? null, instructionsOverride: instructionsFile ?? null,
    speechStartedAt, speechEndedAt, greetingAt, controlledCloseAt, simulatedPlayedBytes, simulatedClearedBytes,
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
}, 58_000);

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
    phoneServiceSecret: 'qualification-primary-'.repeat(3), maxCallSeconds: 45 };
  const sid = `CA${'2'.repeat(32)}`;
  const bindingId = randomUUID();
  const context = { primaryDexterId: randomUUID(), threadId: randomUUID(), displayName: 'Qualification owner', messages: [] };
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
      primaryCalls.push({ method: 'submit', requestId, text: input.text });
      const result: PrimaryRequestResult = { bindingId, requestId, state: 'completed', assistant: {
        messageId: randomUUID(), eventId: randomUUID(), sequence: '1', text: 'Your test phrase is blue lantern.' } };
      requests.set(requestId, result); return result;
    },
    async result(_id, requestId) {
      primaryCalls.push({ method: 'result', requestId });
      const result = requests.get(requestId);
      if (!result) throw Object.assign(new Error(), { code: 'fixture_request_missing' });
      return result;
    },
    async question() { throw Object.assign(new Error(), { code: 'fixture_question_not_configured' }); },
    async reply() { throw Object.assign(new Error(), { code: 'fixture_question_not_configured' }); },
    async captureTurn(_id, input) {
      primaryCalls.push({ method: 'captureTurn', role: input.role, text: input.text });
      return { bindingId, sourceEventId: input.sourceEventId, revision: input.revision, recordId: randomUUID(), replayed: false };
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
  const admission = await post('/internal/broker/calls', { requestId: randomUUID(), purpose: 'This is a brief voice qualification test. Greet the test caller and ask how you can help.' });
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
    }
    socket.send(JSON.stringify({ event: 'media', streamSid, media: { track: 'inbound', chunk: String(++inputFrames), timestamp: String((inputFrames - 1) * 20), payload: bytes.toString('base64') } }));
  }, 20);
  while (!closed && Date.now() - startedAt < 40_000) {
    await new Promise(resolve => setTimeout(resolve, 100));
    const quiet = lastAudioAt > 0 && Date.now() - lastAudioAt > 1500;
    const callerDone = !callerAudio || (speechEndedAt !== undefined && Date.now() - speechEndedAt > 5000 && snapshot.callerRevision > 0);
    if (quiet && callerDone && peakEnergy > 0.01 && snapshot.fragments.some(x => x.role === 'assistant') && Date.now() - mediaStarted > 5000) break;
  }
  stage = 'controlled_close';
  controlledCloseAt = Date.now();
  if (!closed) await post('/internal/broker/end', { id: callId }).then(response => response.arrayBuffer());
  while (broker.store.get(callId)?.primaryClosePending && Date.now() - startedAt < 56_000) await new Promise(resolve => setTimeout(resolve, 50));
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
  console.log(JSON.stringify({ passed: report.passed, completed, failure: report.failure, outputBytes: report.outputBytes,
    peakEnergy: report.peakEnergy, finalized: closed?.finalized ?? false, baselineInvalidOutputAudio: report.baselineInvalidOutputAudio, evidenceDir }));
  process.exitCode = report.passed ? 0 : 1;
}
