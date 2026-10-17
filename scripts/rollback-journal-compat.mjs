#!/usr/bin/env node
// Prepares the broker journal for a rollback to the release before the greeting stream
// (teledexter-50e476cb88d7, tag prod-2026-10-13).
//
//   node scripts/rollback-journal-compat.mjs /var/lib/dexter-broker/state [--dry-run]
//
// That release checks every saved call when it starts and refuses the whole journal over one
// call it cannot read. Two things this release writes trip it:
//   - the journey keys streamStartedAt, firstSpeechAudioSentAt and callerSpeechEndedAt
//     (its journey schema is strict), and
//   - a journey on a call without mediaStartedAt: a call that ended during the greeting
//     ("Call journey lacks media evidence").
// This script removes exactly those three keys and drops the journey of calls without
// mediaStartedAt (a journey left empty is dropped too). Every other field is kept as saved.
// The journey is timing evidence only; no call, request or binding state depends on it.
//
// Run it after the current broker has stopped and before the previous release starts. It takes
// the broker's writer lock, so it refuses while a live broker owns the state directory.
// --dry-run only reads and reports what would change, and works while the broker runs. The
// original journal is kept beside it as calls.json.rollback-<time>.bak. Both files are written
// the way the store writes (src/outbound/DurableFile.ts): a new 0600 temporary file, fsync,
// rename over the target, then fsync of the directory. The script imports nothing from the
// release, so it runs from either release directory with plain node.

import fs from 'node:fs';
import path from 'node:path';
import { randomBytes, randomUUID } from 'node:crypto';
import { pathToFileURL } from 'node:url';

/** Journey keys the previous release does not know. */
export const ROLLBACK_JOURNEY_KEYS = Object.freeze(['streamStartedAt', 'firstSpeechAudioSentAt', 'callerSpeechEndedAt']);
/** The previous release's complete journey schema (strict). */
const PREVIOUS_JOURNEY_KEYS = new Set(['liveReadyAt', 'firstAudioSentAt', 'firstPlaybackConfirmedAt',
  'greetingClipStartedAt', 'greetingClipPlayedAt']);
const record = value => value !== null && typeof value === 'object' && !Array.isArray(value);

/** The previous release's journey rule, checked on the result before anything is written. */
function previousReleaseAccepts(call) {
  const journey = call.journey;
  if (journey === undefined) return true;
  if (!record(journey) || !call.mediaStartedAt) return false;
  if (Object.keys(journey).some(key => !PREVIOUS_JOURNEY_KEYS.has(key))) return false;
  return !(journey.firstPlaybackConfirmedAt && !journey.firstAudioSentAt)
    && !(journey.greetingClipPlayedAt && !journey.greetingClipStartedAt);
}

/** Returns the rolled-back journal and what changed. The input is not modified. */
export function rollbackJournal(state) {
  if (!record(state) || state.version !== 1 || !record(state.calls)) throw new Error('rollback_journal_invalid');
  const next = structuredClone(state);
  const changes = { calls: Object.keys(next.calls).length, changedCalls: [], journeysDropped: 0, journeyKeysRemoved: 0 };
  for (const [id, call] of Object.entries(next.calls)) {
    if (!record(call) || call.id !== id) throw new Error('rollback_journal_invalid');
    if (call.journey === undefined) continue;
    if (!record(call.journey)) throw new Error('rollback_journal_invalid');
    let changed = false;
    if (!call.mediaStartedAt) {
      delete call.journey; changes.journeysDropped++; changed = true;
    } else {
      for (const key of ROLLBACK_JOURNEY_KEYS) if (Object.hasOwn(call.journey, key)) {
        delete call.journey[key]; changes.journeyKeysRemoved++; changed = true;
      }
      if (changed && Object.keys(call.journey).length === 0) { delete call.journey; changes.journeysDropped++; }
    }
    if (changed) changes.changedCalls.push(id);
    if (!previousReleaseAccepts(call)) throw new Error('rollback_journey_unsupported');
  }
  return { journal: next, changes };
}

/** Mirrors bindDurableFileParent / assertDurableFileParent in src/outbound/DurableFile.ts. */
function bindParent(directory) {
  const parent = fs.lstatSync(directory);
  const uid = process.getuid?.();
  if (!parent.isDirectory() || parent.isSymbolicLink() || fs.realpathSync(directory) !== path.resolve(directory)
    || (parent.mode & 0o022) !== 0 || (uid !== undefined && parent.uid !== uid)) throw new Error('durable_parent_not_safe');
  return { directory, device: parent.dev, inode: parent.ino, uid: parent.uid };
}
function assertParent(binding) {
  let parent;
  try { parent = fs.lstatSync(binding.directory); } catch { throw new Error('durable_parent_identity_changed'); }
  if (!parent.isDirectory() || parent.isSymbolicLink() || parent.dev !== binding.device || parent.ino !== binding.inode
    || parent.uid !== binding.uid || (parent.mode & 0o022) !== 0
    || fs.realpathSync(binding.directory) !== path.resolve(binding.directory)) throw new Error('durable_parent_identity_changed');
}

/** Mirrors durablyReplaceJsonFile in src/outbound/DurableFile.ts: success means both the file
 * bytes and its directory entry crossed the kernel durability boundary. */
function durablyReplace(filePath, contents, fileSystem, binding) {
  assertParent(binding);
  const tempPath = `${filePath}.${process.pid}.${randomBytes(6).toString('hex')}.tmp`;
  let descriptor = null;
  try {
    descriptor = fileSystem.openSync(tempPath, 'wx', 0o600);
    fileSystem.writeFileSync(descriptor, contents, { encoding: 'utf8' });
    fileSystem.chmodSync(tempPath, 0o600);
    fileSystem.fsyncSync(descriptor);
    fileSystem.closeSync(descriptor);
    descriptor = null;
    assertParent(binding);
    fileSystem.renameSync(tempPath, filePath);
    assertParent(binding);
    const directoryDescriptor = fileSystem.openSync(binding.directory, 'r');
    try {
      const opened = fs.fstatSync(directoryDescriptor);
      if (!opened.isDirectory() || opened.dev !== binding.device || opened.ino !== binding.inode)
        throw new Error('durable_parent_identity_changed');
      fileSystem.fsyncSync(directoryDescriptor);
      assertParent(binding);
    } finally { fileSystem.closeSync(directoryDescriptor); }
  } catch (error) {
    if (descriptor !== null) { try { fileSystem.closeSync(descriptor); } catch { /* Already closed. */ } }
    try { fileSystem.unlinkSync(tempPath); } catch { /* Never created or already renamed. */ }
    throw error;
  }
}

/** The broker store's writer lock: one writer per state directory. A lock whose process is gone
 * is stale and replaced, exactly as BrokerStore does at startup. */
function acquireWriterLock(lockPath, fileSystem) {
  const id = randomUUID();
  const take = () => {
    const descriptor = fileSystem.openSync(lockPath, 'wx', 0o600);
    try { fileSystem.writeFileSync(descriptor, JSON.stringify({ pid: process.pid, id }), { encoding: 'utf8' }); fileSystem.fsyncSync(descriptor); }
    finally { fileSystem.closeSync(descriptor); }
  };
  try { take(); } catch (error) {
    if (error?.code !== 'EEXIST') throw error;
    let previous;
    try { previous = JSON.parse(fs.readFileSync(lockPath, 'utf8')); } catch { throw new Error('rollback_writer_lock_invalid'); }
    if (!Number.isInteger(previous?.pid) || previous.pid < 1) throw new Error('rollback_writer_lock_invalid');
    try { process.kill(previous.pid, 0); } catch (probe) {
      if (probe?.code !== 'ESRCH') throw new Error('rollback_broker_running');
      fs.unlinkSync(lockPath);
      take();
      return release;
    }
    throw new Error('rollback_broker_running');
  }
  return release;
  function release() {
    try { if (JSON.parse(fs.readFileSync(lockPath, 'utf8')).id === id) fs.unlinkSync(lockPath); }
    catch (error) { if (error?.code !== 'ENOENT') throw error; }
  }
}

/** Rewrites <stateDir>/calls.json for the previous release. `fileSystem` and `now` exist for tests. */
export function runRollbackJournalCompat(args, options = {}) {
  const fileSystem = options.fileSystem ?? fs;
  const now = options.now ?? (() => new Date());
  const dryRun = args.includes('--dry-run');
  const positional = args.filter(arg => arg !== '--dry-run');
  if (positional.length !== 1 || !path.isAbsolute(positional[0]) || args.some(arg => arg.startsWith('--') && arg !== '--dry-run'))
    throw new Error('rollback_usage');
  const directory = path.resolve(positional[0]);
  const binding = bindParent(directory);
  const file = path.join(directory, 'calls.json');
  // A dry run only reads (the store replaces the journal by rename, so any read is a whole
  // journal); it can run while the broker is still up. A real run needs the writer lock.
  const release = dryRun ? () => {} : acquireWriterLock(path.join(directory, 'writer.lock'), fileSystem);
  try {
    if (!fs.existsSync(file)) return { stateDir: directory, calls: 0, changedCalls: 0, journeysDropped: 0, journeyKeysRemoved: 0,
      backup: null, written: false, dryRun };
    const original = fs.readFileSync(file, 'utf8');
    let state;
    try { state = JSON.parse(original); } catch { throw new Error('rollback_journal_invalid'); }
    const { journal, changes } = rollbackJournal(state);
    const summary = { stateDir: directory, calls: changes.calls, changedCalls: changes.changedCalls.length,
      journeysDropped: changes.journeysDropped, journeyKeysRemoved: changes.journeyKeysRemoved, backup: null, written: false, dryRun };
    if (dryRun || changes.changedCalls.length === 0) return summary;
    const backup = path.join(directory, `calls.json.rollback-${now().toISOString().replace(/[:.]/g, '-')}.bak`);
    if (fs.existsSync(backup)) throw new Error('rollback_backup_exists');
    // The untouched journal is durable beside the original before the original is replaced.
    durablyReplace(backup, original, fileSystem, binding);
    const body = JSON.stringify(journal);
    durablyReplace(file, body, fileSystem, binding);
    if (fs.readFileSync(file, 'utf8') !== body || fs.readFileSync(backup, 'utf8') !== original) throw new Error('rollback_write_unverified');
    return { ...summary, backup, written: true };
  } finally { release(); }
}

if (process.argv[1] && import.meta.url === pathToFileURL(fs.realpathSync(process.argv[1])).href) {
  try { console.log(JSON.stringify(runRollbackJournalCompat(process.argv.slice(2)), null, 2)); }
  catch (error) {
    const code = error instanceof Error && /^(rollback|durable)_[a-z_]+$/.test(error.message) ? error.message : 'rollback_failed';
    console.error(`[rollback-journal-compat] ${code}${code === 'rollback_failed' ? `: ${error instanceof Error ? error.message : String(error)}` : ''}`);
    if (code === 'rollback_usage') console.error('usage: node scripts/rollback-journal-compat.mjs /absolute/state/dir [--dry-run]');
    process.exitCode = 1;
  }
}
