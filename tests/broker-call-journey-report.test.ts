import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { test } from 'node:test';
import { createCallJourneyReport, runCallJourneyReport } from '../scripts/call-journey-report.mjs';

const callId = '1af5ad36-c046-42d7-804c-2e782b657678';
const requestId = '2af5ad36-c046-42d7-804c-2e782b657678';
const attemptId = '3af5ad36-c046-42d7-804c-2e782b657678';
const spanId = '4af5ad36-c046-42d7-804c-2e782b657678';
const at = (seconds: number) => new Date(Date.UTC(2026, 8, 28, 12, 0, seconds)).toISOString();
const baseCall = () => ({ id: callId, direction: 'inbound', status: 'completed', createdAt: at(0), endedAt: at(50),
  inbound: { consumeAttemptedAt: at(3), remote: { state: 'bound', consumedAt: at(5) } }, mediaStartedAt: at(7),
  liveGenerations: { segments: [{ ordinal: 1, createdAt: at(8), readyAt: at(9) }] },
  journey: { liveReadyAt: at(10), firstAudioSentAt: at(11), firstPlaybackConfirmedAt: at(12) }, delegations: {} });
const journal = (call: unknown = baseCall()) => ({ version: 1, calls: { [callId]: call } });
const event = (patch: Record<string, unknown> = {}) => JSON.stringify({ event: 'broker_inbound_timing', version: 1,
  callId, attemptId, spanId, stage: 'open_context', phase: 'started', observedAt: at(3), ...patch });

test('joins startup, persisted consumption and first playback without equating provider ready with audio', () => {
  const report = createCallJourneyReport(journal());
  assert.deepEqual(report.durationsMs, { createdToMedia: 7000, consumption: 2000, consumedToMedia: 2000,
    mediaToFirstProviderOpen: 1000, createdToFirstProviderReady: 9000, mediaToLiveReady: 3000,
    createdToFirstAudio: 11000, createdToFirstPlayback: 12000, createdToStream: null, streamToGreetingClip: null,
    greetingClipPlayedToMedia: null });
  assert.equal(report.generations[0].providerOpenMs, 1000);
  assert.equal(report.generations[0].providerReadyAt, at(9));
  assert.equal(report.timestamps.liveReadyAt, at(10));
});

test('reports the greeting stream and clip separately from Live attaching and readiness', () => {
  const call: any = baseCall();
  call.journey = { ...call.journey, streamStartedAt: at(1), greetingClipStartedAt: at(1), greetingClipPlayedAt: at(4) };
  const report = createCallJourneyReport(journal(call));
  assert.equal(report.timestamps.streamStartedAt, at(1));
  assert.equal(report.timestamps.greetingClipStartedAt, at(1));
  assert.equal(report.timestamps.greetingClipPlayedAt, at(4));
  assert.equal(report.durationsMs.createdToStream, 1000);
  assert.equal(report.durationsMs.streamToGreetingClip, 0);
  assert.equal(report.durationsMs.greetingClipPlayedToMedia, 3000);
  assert.equal(report.durationsMs.mediaToLiveReady, 3000);
});

test('historical calls retain missing timing instead of inferring audio from transcripts', () => {
  const call: any = baseCall(); delete call.journey; delete call.liveGenerations;
  call.transcript = [{ audioOffsetMs: 100, text: 'private speech' }];
  call.delegations = { [requestId]: { requestId, status: 'completed', result: 'private result' } };
  const report = createCallJourneyReport(journal(call));
  assert.equal(report.durationsMs.createdToFirstAudio, null);
  assert.equal(report.durationsMs.createdToFirstPlayback, null);
  assert.equal(report.requests[0].dispatchToAnswerMs, null);
  assert.equal(report.coverage.requestsWithoutTiming, 1);
  assert.equal(report.coverage.firstPlaybackRecorded, false);
});

test('retains overlapping attempts individually, deduplicates repeated log lines and ignores foreign calls', () => {
  const otherSpan = '5af5ad36-c046-42d7-804c-2e782b657678';
  const text = [event(), event(), event({ spanId: otherSpan, stage: 'provider_identity' }),
    event({ spanId: otherSpan, stage: 'provider_identity', phase: 'completed', observedAt: at(4), elapsedMs: 1000 }),
    `957|broker | ${event({ phase: 'completed', observedAt: at(5), elapsedMs: 2000 })}`,
    event({ callId: requestId, phase: 'failed', elapsedMs: 9000 })].join('\n');
  const report = createCallJourneyReport(journal(), text);
  assert.equal(report.stages.length, 2);
  assert.deepEqual(report.stages.map((span: any) => span.elapsedMs).sort(), [1000, 2000]);
  assert.equal(report.stages[0].startedAt, at(3));
  assert.equal('totalStageMs' in report, false);
});

test('records a failed result poll and completed answer without inventing answer playback', () => {
  const call: any = baseCall();
  call.delegations[requestId] = { requestId, status: 'completed', primaryResult: { state: 'completed' }, delivery: 'unconfirmed',
    timing: { firstSavedAt: at(20), firstDispatchAt: at(21), firstAnswerAvailableAt: at(49) },
    lastResultReadError: { code: 'http_error', status: 503, retryable: true, observedAt: at(48), remoteCode: 'private remote detail' } };
  const request = createCallJourneyReport(journal(call)).requests[0];
  assert.equal(request.dispatchWaitMs, 1000); assert.equal(request.dispatchToAnswerMs, 28000);
  assert.equal(request.publicationAcceptedAt, null); assert.equal(request.answerToPublicationMs, null);
  assert.equal(request.lastResultReadError.status, 503);
  assert.equal('playbackConfirmedAt' in request, false);
});

test('publication timestamp remains distinct from carrier playback and negative clock intervals stay unknown', () => {
  const call: any = baseCall(); call.journey.firstAudioSentAt = at(-1);
  call.delegations[requestId] = { requestId, status: 'completed', delivery: 'accepted',
    timing: { firstSavedAt: at(20), firstDispatchAt: at(21), firstAnswerAvailableAt: at(30), firstDeliveryAcceptedAt: at(31) } };
  const report = createCallJourneyReport(journal(call));
  assert.equal(report.durationsMs.createdToFirstAudio, null);
  assert.equal(report.requests[0].answerToPublicationMs, 1000);
  assert.match(report.interpretation.results, /Per-tool spans and per-answer playback are absent/);
});

test('outputs only allowlisted fields even when input/logs contain identities, secrets and raw errors', () => {
  const secret = 'DO_NOT_PRINT_THIS_PRIVATE_VALUE'; const call: any = baseCall();
  Object.assign(call, { number: '+15555555555', callSid: 'CA_PRIVATE', transcript: secret, context: secret, proof: secret });
  call.delegations[requestId] = { requestId, status: secret, result: secret, submission: secret, delivery: secret,
    submitError: { code: secret, message: secret, remoteCode: secret, status: 503 } };
  const output = JSON.stringify(createCallJourneyReport(journal(call), [secret, event({ secret }), '{bad'].join('\n')));
  for (const excluded of [secret, '+15555555555', 'CA_PRIVATE', 'remoteCode', 'submission']) assert.equal(output.includes(excluded), false);
  assert.match(output, /"status":503/);
});

test('selects exact call or latest recorded call, rejecting absent identities', () => {
  const data = journal(); data.calls[requestId] = { ...baseCall(), id: requestId, createdAt: at(60) };
  assert.equal(createCallJourneyReport(data).callId, requestId);
  assert.equal(createCallJourneyReport(data, '', { callId }).callId, callId);
  assert.throws(() => createCallJourneyReport(data, '', { callId: attemptId }), /journey_call_not_found/);
  assert.throws(() => createCallJourneyReport(data, '', { callId: 'secret' }), /journey_call_id_invalid/);
});

test('bounds report rows and log spans with explicit coverage', () => {
  const call: any = baseCall();
  call.delegations = Object.fromEntries(Array.from({ length: 513 }, (_, index) => [String(index), { status: 'queued' }]));
  call.liveGenerations.segments = Array.from({ length: 129 }, (_, index) => ({ ordinal: index + 1 }));
  const lines = Array.from({ length: 2050 }, (_, index) => event({ spanId: `4af5ad36-c046-42d7-804c-${index.toString(16).padStart(12, '0')}` }));
  lines.push('x'.repeat(17000));
  const report = createCallJourneyReport(journal(call), lines.join('\n'));
  assert.equal(report.requests.length, 512); assert.equal(report.coverage.omittedRequests, 1);
  assert.equal(report.generations.length, 128); assert.equal(report.coverage.omittedGenerations, 1);
  assert.equal(report.stages.length, 2048); assert.equal(report.coverage.droppedSpans, 2); assert.equal(report.coverage.oversizedLogLines, 1);
});

test('CLI reads explicit files without acquiring a writer lock and reports bounded log-tail coverage', t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-journey-')); t.after(() => fs.rmSync(directory, { recursive: true }));
  const source = path.join(directory, 'calls.json'), log = path.join(directory, 'broker.log');
  fs.writeFileSync(source, JSON.stringify(journal()));
  fs.writeFileSync(log, 'x'.repeat(8 * 1024 * 1024) + '\n' + event());
  const before = fs.readFileSync(source);
  const child = spawnSync(process.execPath, ['scripts/calls.mjs', 'journey', '--journal', source, '--log', log], { encoding: 'utf8' });
  assert.equal(child.status, 0, child.stderr);
  const report = JSON.parse(child.stdout); assert.equal(report.coverage.logTailTruncated, true); assert.equal(report.stages.length, 1);
  assert.deepEqual(fs.readFileSync(source), before); assert.deepEqual(fs.readdirSync(directory).sort(), ['broker.log', 'calls.json']);
});

test('CLI rejects invalid and over-limit input without printing file paths or contents', t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-journey-')); t.after(() => fs.rmSync(directory, { recursive: true }));
  const source = path.join(directory, 'calls.json'); fs.writeFileSync(source, '{private malformed');
  const child = spawnSync(process.execPath, ['scripts/calls.mjs', 'journey', '--journal', source], { encoding: 'utf8' });
  assert.equal(child.status, 1); assert.equal(child.stdout, ''); assert.equal(child.stderr.trim(), '[calls] journey_journal_invalid');
  fs.truncateSync(source, 32 * 1024 * 1024 + 1);
  assert.throws(() => runCallJourneyReport(['--journal', source]), /journey_journal_too_large/);
  assert.throws(() => runCallJourneyReport(['--journal', 'relative']), /journey_path_must_be_absolute/);
  assert.throws(() => runCallJourneyReport(['--journal', source, '--journal', source]), /journey_arguments_invalid/);
});
