import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import test from 'node:test';
import { BrokerStore, SETTLED_QUIET_MS } from '../src/broker/store.js';
import { rollbackJournal, runRollbackJournalCompat, ROLLBACK_JOURNEY_KEYS } from '../scripts/rollback-journal-compat.mjs';

const script = path.resolve(import.meta.dirname, '../scripts/rollback-journal-compat.mjs');
const at = (seconds: number) => new Date(Date.UTC(2026, 9, 16, 12, 0, seconds)).toISOString();

/** The previous release's journey check (teledexter-50e476cb88d7, src/broker/store.ts), restated
 * independently of the script: a strict five-key schema, and only on a call with mediaStartedAt. */
function previousReleaseAccepts(call: Record<string, any>): boolean {
  if (call.journey === undefined) return true;
  const allowed = new Set(['liveReadyAt', 'firstAudioSentAt', 'firstPlaybackConfirmedAt', 'greetingClipStartedAt', 'greetingClipPlayedAt']);
  return Boolean(call.mediaStartedAt) && Object.keys(call.journey).every(key => allowed.has(key))
    && !(call.journey.firstPlaybackConfirmedAt && !call.journey.firstAudioSentAt)
    && !(call.journey.greetingClipPlayedAt && !call.journey.greetingClipStartedAt);
}

/** Writes calls through this branch's store: the exact journal a rollback would meet. */
function branchJournal(t: test.TestContext) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'rollback-journal-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const store = new BrokerStore(directory);
  const outbound = () => store.reserve({ requestId: randomUUID(), direction: 'outbound', purpose: 'Check the portfolio', number: '+12025550123' }).call;
  // Ended during the greeting: stream and clip evidence, no mediaStartedAt.
  const greeting = outbound();
  store.update(greeting.id, { callSid: 'CA' + 'a'.repeat(32), status: 'in-progress', streamSid: 'MZ' + 'a'.repeat(32),
    journey: { streamStartedAt: at(1), greetingClipStartedAt: at(1), firstAudioSentAt: at(1) } });
  store.update(greeting.id, { status: 'completed', endedAt: at(2) });
  // Live attached and spoke after the caller: every new key.
  const attached = outbound();
  store.update(attached.id, { callSid: 'CA' + 'b'.repeat(32), status: 'in-progress', streamSid: 'MZ' + 'b'.repeat(32),
    journey: { streamStartedAt: at(1), greetingClipStartedAt: at(1), firstAudioSentAt: at(1) } });
  store.update(attached.id, { mediaStartedAt: at(3), journey: { ...store.get(attached.id)!.journey,
    greetingClipPlayedAt: at(3), liveReadyAt: at(4), firstPlaybackConfirmedAt: at(4), callerSpeechEndedAt: at(5), firstSpeechAudioSentAt: at(6) } });
  store.update(attached.id, { status: 'completed', endedAt: at(9) });
  // Only the stream key: the journey is empty once it is removed.
  const streamOnly = outbound();
  store.update(streamOnly.id, { callSid: 'CA' + 'c'.repeat(32), status: 'in-progress', journey: { streamStartedAt: at(1) } });
  store.update(streamOnly.id, { mediaStartedAt: at(2) });
  // A record the previous release wrote: left exactly as saved.
  const previous = outbound();
  store.update(previous.id, { callSid: 'CA' + 'd'.repeat(32), status: 'in-progress', mediaStartedAt: at(1),
    journey: { liveReadyAt: at(2), firstAudioSentAt: at(2), firstPlaybackConfirmedAt: at(3) } });
  const plain = outbound();
  store.close();
  return { directory, file: path.join(directory, 'calls.json'), ids: { greeting: greeting.id, attached: attached.id,
    streamOnly: streamOnly.id, previous: previous.id, plain: plain.id } };
}
const run = (...args: string[]) => spawnSync(process.execPath, [script, ...args], { encoding: 'utf8' });
const read = (file: string) => JSON.parse(fs.readFileSync(file, 'utf8')) as { version: 1; calls: Record<string, Record<string, any>> };

test('a journal written by this release becomes one the previous release accepts, with a durable backup', t => {
  const f = branchJournal(t);
  const original = fs.readFileSync(f.file, 'utf8');
  assert.equal(Object.values(read(f.file).calls).every(previousReleaseAccepts), false, 'the previous release refuses it as written');
  const result = run(f.directory);
  assert.equal(result.status, 0, result.stderr);
  const summary = JSON.parse(result.stdout);
  assert.deepEqual({ ...summary, backup: path.dirname(summary.backup) }, { stateDir: f.directory, calls: 5, changedCalls: 3,
    journeysDropped: 2, journeyKeysRemoved: 4, settledCallsFolded: 0, settledSuperseded: 0, settledFiles: 0, observationsFolded: 0, backup: f.directory, written: true, dryRun: false });
  const before = JSON.parse(original).calls, after = read(f.file).calls;
  assert.ok(Object.values(after).every(previousReleaseAccepts));
  assert.equal(after[f.ids.greeting].journey, undefined, 'a call that ended during the greeting keeps no journey');
  assert.deepEqual(after[f.ids.attached].journey, { greetingClipStartedAt: at(1), firstAudioSentAt: at(1),
    greetingClipPlayedAt: at(3), liveReadyAt: at(4), firstPlaybackConfirmedAt: at(4) });
  assert.equal(after[f.ids.streamOnly].journey, undefined);
  assert.deepEqual(after[f.ids.previous], before[f.ids.previous]);
  assert.deepEqual(after[f.ids.plain], before[f.ids.plain]);
  for (const id of Object.keys(before)) {
    const { journey: _before, ...restBefore } = before[id], { journey: _after, ...restAfter } = after[id];
    assert.deepEqual(restAfter, restBefore, 'only the journey changes');
  }
  assert.ok(!JSON.stringify(after).match(new RegExp(ROLLBACK_JOURNEY_KEYS.join('|'))));
  assert.equal(fs.readFileSync(summary.backup, 'utf8'), original, 'the backup holds the journal exactly as it was');
  for (const file of [f.file, summary.backup]) assert.equal(fs.statSync(file).mode & 0o777, 0o600);
  assert.deepEqual(fs.readdirSync(f.directory).sort(), ['calls.json', path.basename(summary.backup)].sort(),
    'no temporary file and no writer lock remain');
  const reopened = new BrokerStore(f.directory); // Rolling forward again reads it too.
  assert.equal(reopened.list().length, 5); reopened.close();
  const again = run(f.directory);
  assert.equal(again.status, 0, again.stderr);
  assert.equal(JSON.parse(again.stdout).written, false, 'a second run finds nothing to change');
  assert.equal(fs.readdirSync(f.directory).filter(name => name.endsWith('.bak')).length, 1);
});

test('the script refuses while a live broker owns the journal and replaces only a stale writer lock', t => {
  const f = branchJournal(t);
  const original = fs.readFileSync(f.file, 'utf8');
  const lock = path.join(f.directory, 'writer.lock');
  fs.writeFileSync(lock, JSON.stringify({ pid: process.pid, id: randomUUID() }), { mode: 0o600 }); // This test process is alive.
  const refused = run(f.directory);
  assert.equal(refused.status, 1);
  assert.match(refused.stderr, /\[rollback-journal-compat\] rollback_broker_running/);
  assert.equal(fs.readFileSync(f.file, 'utf8'), original);
  assert.ok(fs.existsSync(lock), 'a live owner keeps its lock');
  const exited = spawnSync(process.execPath, ['-e', '']);
  fs.writeFileSync(lock, JSON.stringify({ pid: exited.pid, id: randomUUID() }), { mode: 0o600 });
  const stale = run(f.directory);
  assert.equal(stale.status, 0, stale.stderr);
  assert.equal(JSON.parse(stale.stdout).written, true);
  assert.equal(fs.existsSync(lock), false, 'the script releases the lock it took');
});

test('a dry run reports the change without the lock and writes nothing', t => {
  const f = branchJournal(t);
  const original = fs.readFileSync(f.file, 'utf8');
  fs.writeFileSync(path.join(f.directory, 'writer.lock'), JSON.stringify({ pid: process.pid, id: randomUUID() }), { mode: 0o600 });
  const result = run(f.directory, '--dry-run');
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(JSON.parse(result.stdout), { stateDir: f.directory, calls: 5, changedCalls: 3, journeysDropped: 2,
    journeyKeysRemoved: 4, settledCallsFolded: 0, settledSuperseded: 0, settledFiles: 0, observationsFolded: 0, backup: null, written: false, dryRun: true });
  assert.equal(fs.readFileSync(f.file, 'utf8'), original);
  assert.deepEqual(fs.readdirSync(f.directory).sort(), ['calls.json', 'writer.lock']);
  for (const args of [[], ['relative/state'], [f.directory, '--force']]) {
    const usage = run(...args);
    assert.equal(usage.status, 1); assert.match(usage.stderr, /rollback_usage/);
  }
});

test('the backup and the journal are each fsynced, renamed into place and their directory fsynced, backup first', t => {
  const f = branchJournal(t);
  const events: string[] = [];
  const names = new Map<number, string>();
  const fileSystem = {
    openSync(target: string, flags: string, mode?: number) { const fd = fs.openSync(target, flags, mode); names.set(fd, path.basename(target).replace(/\.\d+\.[0-9a-f]+\.tmp$/, '.tmp')); events.push(`open:${names.get(fd)}`); return fd; },
    writeFileSync(fd: number, data: string, options: { encoding: 'utf8' }) { events.push(`write:${names.get(fd)}`); fs.writeFileSync(fd, data, options); },
    chmodSync(target: string, mode: number) { fs.chmodSync(target, mode); },
    fsyncSync(fd: number) { events.push(`fsync:${names.get(fd)}`); fs.fsyncSync(fd); },
    closeSync(fd: number) { fs.closeSync(fd); },
    renameSync(from: string, to: string) { events.push(`rename:${path.basename(to)}`); fs.renameSync(from, to); },
    unlinkSync(target: string) { events.push(`unlink:${path.basename(target)}`); fs.unlinkSync(target); },
  };
  const summary = runRollbackJournalCompat([f.directory], { fileSystem, now: () => new Date(Date.UTC(2026, 9, 17)) });
  const backup = path.basename(summary.backup!), directory = path.basename(f.directory);
  assert.equal(backup, 'calls.json.rollback-2026-10-17T00-00-00-000Z.bak');
  assert.deepEqual(events, [
    'open:writer.lock', 'write:writer.lock', 'fsync:writer.lock',
    `open:${backup}.tmp`, `write:${backup}.tmp`, `fsync:${backup}.tmp`, `rename:${backup}`, `open:${directory}`, `fsync:${directory}`,
    'open:calls.json.tmp', 'write:calls.json.tmp', 'fsync:calls.json.tmp', 'rename:calls.json', `open:${directory}`, `fsync:${directory}`,
  ]);
});

test('a journey the previous release could not read even after the change is refused, not written', () => {
  const id = randomUUID();
  const call = { id, requestId: randomUUID(), number: '+12025550123', createdAt: at(0), delegations: {}, mediaStartedAt: at(1),
    journey: { streamStartedAt: at(1), greetingClipPlayedAt: at(2) } };
  assert.throws(() => rollbackJournal({ version: 1, calls: { [id]: call } }), /rollback_journey_unsupported/);
  assert.throws(() => rollbackJournal({ version: 2, calls: {} }), /rollback_journal_invalid/);
  const input = { version: 1, calls: { [id]: { ...call, journey: { streamStartedAt: at(1), liveReadyAt: at(2) } } } };
  const frozen = structuredClone(input);
  assert.deepEqual(rollbackJournal(input).journal.calls[id].journey, { liveReadyAt: at(2) });
  assert.deepEqual(input, frozen, 'the input is not modified');
});

test('a lock that cannot be written is removed, so the next broker still starts, and the journal is untouched', t => {
  const f = branchJournal(t);
  const original = fs.readFileSync(f.file, 'utf8');
  const fileSystem = {
    openSync: (target: string, flags: string, mode?: number) => fs.openSync(target, flags, mode),
    writeFileSync: (fd: number, data: string, options: { encoding: 'utf8' }) => fs.writeFileSync(fd, data, options),
    chmodSync: (target: string, mode: number) => fs.chmodSync(target, mode),
    fsyncSync: () => { throw Object.assign(new Error('no space left on device'), { code: 'ENOSPC' }); },
    closeSync: (fd: number) => fs.closeSync(fd),
    renameSync: (from: string, to: string) => fs.renameSync(from, to),
    unlinkSync: (target: string) => fs.unlinkSync(target),
  };
  assert.throws(() => runRollbackJournalCompat([f.directory], { fileSystem }), /no space left on device/);
  assert.deepEqual(fs.readdirSync(f.directory), ['calls.json'], 'no empty writer.lock is left behind');
  assert.equal(fs.readFileSync(f.file, 'utf8'), original);
  const reopened = new BrokerStore(f.directory); assert.equal(reopened.list().length, 5); reopened.close();
});

test('rolling back to 6e23b1b keeps every journey key, and only the side files are folded', t => {
  const f = branchJournal(t);
  const original = read(f.file);
  const summary = run(f.directory, '--target', '6e23b1be9103', '--dry-run');
  assert.equal(summary.status, 0, summary.stderr);
  assert.deepEqual(JSON.parse(summary.stdout), { stateDir: f.directory, calls: 5, changedCalls: 0, journeysDropped: 0, journeyKeysRemoved: 0,
    settledCallsFolded: 0, settledSuperseded: 0, settledFiles: 0, observationsFolded: 0, target: '6e23b1be9103', backup: null, written: false, dryRun: true });
  const written = runRollbackJournalCompat([f.directory, '--target', '6e23b1be9103'], { now: () => new Date(Date.UTC(2026, 9, 18)) });
  assert.equal(written.written, false, 'nothing to fold and nothing to convert: the journal is left as saved');
  assert.deepEqual(read(f.file), original);
  assert.equal(run(f.directory, '--target').status, 1, '--target needs a revision');
});

test('the dry run counts settled files, and the fold puts every settled call back where a previous release reads it', async t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'rollback-settled-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const store = new BrokerStore(directory, { observationIntervalMs: 0 });
  const ended = new Date(Date.now() - 2 * SETTLED_QUIET_MS).toISOString();
  const ids: string[] = [];
  for (let i = 0; i < 3; i++) {
    const call = store.reserve({ requestId: randomUUID(), direction: 'outbound', purpose: 'Check the portfolio', number: '+12025550123' }).call;
    store.update(call.id, { callSid: 'CA' + randomUUID().replace(/-/g, ''), status: 'completed', endedAt: ended });
    ids.push(call.id);
  }
  const live = store.reserve({ requestId: randomUUID(), direction: 'outbound', purpose: 'Check the portfolio', number: '+12025550123' }).call;
  store.update(live.id, { endReason: 'caller_disconnected',
    voiceCloses: [{ at: new Date().toISOString(), reason: 'connection_lost', finalized: false, providerError: { code: 'server_error' } }] });
  assert.equal(await store.settle(() => true, Date.now() + 2 * SETTLED_QUIET_MS), 3);
  store.close();
  // A previous release reads calls.json only: without the fold it would see none of the settled calls.
  assert.deepEqual(ids.filter(id => read(path.join(directory, 'calls.json')).calls[id]), []);
  const dry = JSON.parse(run(directory, '--dry-run', '--target', '6e23b1be9103').stdout);
  assert.equal(dry.settledFiles, 3);
  assert.equal(dry.settledCallsFolded + dry.settledSuperseded, dry.settledFiles, 'the runbook check: every settled file folds or is superseded');
  const summary = runRollbackJournalCompat([directory, '--target', '6e23b1be9103'], { now: () => new Date(Date.UTC(2026, 9, 18)) });
  assert.equal(summary.written, true);
  const journal = read(path.join(directory, 'calls.json'));
  assert.deepEqual(ids.filter(id => journal.calls[id]?.status === 'completed'), ids);
  assert.equal(journal.calls[live.id]!.endReason, 'caller_disconnected', 'new fields are plain fields the previous release keeps');
  assert.deepEqual(fs.readdirSync(directory).filter(name => name === 'calls.d' || name === 'live.d'), []);
});

test('a settled call that changed later is superseded by calls.json: counted, never folded over the newer copy', async t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'rollback-superseded-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const store = new BrokerStore(directory);
  const ended = new Date(Date.now() - 2 * SETTLED_QUIET_MS).toISOString();
  const ids = [0, 1].map(() => {
    const call = store.reserve({ requestId: randomUUID(), direction: 'outbound', purpose: 'Check the portfolio', number: '+12025550123' }).call;
    store.update(call.id, { callSid: 'CA' + randomUUID().replace(/-/g, ''), status: 'completed', endedAt: ended });
    return call.id;
  });
  assert.equal(await store.settle(() => true, Date.now() + 2 * SETTLED_QUIET_MS), 2);
  store.update(ids[0]!, { failure: { code: 'late_callback', message: 'A late provider callback' } });
  store.close();
  const dry = JSON.parse(run(directory, '--dry-run', '--target', '6e23b1be9103').stdout);
  assert.deepEqual([dry.settledFiles, dry.settledCallsFolded, dry.settledSuperseded], [2, 1, 1]);
  runRollbackJournalCompat([directory, '--target', '6e23b1be9103'], { now: () => new Date(Date.UTC(2026, 9, 18)) });
  const journal = read(path.join(directory, 'calls.json'));
  assert.equal(journal.calls[ids[0]!]!.failure?.code, 'late_callback', 'the newer calls.json copy is kept');
  assert.equal(journal.calls[ids[1]!]!.status, 'completed');
});
