import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { BrokerStore, SETTLED_QUIET_MS, type BrokerCall } from '../src/broker/store.js';
import type { OverlayFileSystem } from '../src/broker/journalOverlay.js';
import { nodeOverlayFileSystem } from '../src/broker/journalOverlay.js';
import { readCallRequestReceipt } from '../src/broker/callRequestReceipt.js';
import { foldJournalFiles, runRollbackJournalCompat } from '../scripts/rollback-journal-compat.mjs';

const wait = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
function directory(t: test.TestContext) {
  const value = fs.mkdtempSync(path.join(os.tmpdir(), 'journal-observations-'));
  t.after(() => fs.rmSync(value, { recursive: true, force: true }));
  return value;
}
/** A disk whose writes never finish until released: what a stalled volume looks like to the store. */
function blockedDisk() {
  const started: string[] = [];
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  const fileSystem: OverlayFileSystem = { ...nodeOverlayFileSystem,
    async writeFile(file, data) { started.push(path.basename(file)); await gate; await nodeOverlayFileSystem.writeFile(file, data); } };
  return { fileSystem, started, release: () => release() };
}
const snapshot = (revision: number) => ({ revision, callerRevision: 0,
  fragments: Array.from({ length: revision }, (_, index) => ({ eventId: `e${index + 1}`, role: 'assistant', text: ` word${index + 1}`,
    startMs: index * 200, endMs: index * 200 + 200, sequence: index + 1 })) });
function outbound(store: BrokerStore) {
  return store.reserve({ requestId: randomUUID(), direction: 'outbound', purpose: 'Check the portfolio', number: '+12025550123' }).call;
}

test('a transcript observation returns at once on a blocked disk and never rewrites calls.json', async t => {
  const dir = directory(t);
  const disk = blockedDisk();
  const store = new BrokerStore(dir, { observationFs: disk.fileSystem, observationIntervalMs: 0 });
  const call = outbound(store);
  const journal = fs.readFileSync(path.join(dir, 'calls.json'), 'utf8');
  let slowest = 0;
  for (let revision = 1; revision <= 200; revision++) {
    const started = performance.now();
    store.observeTranscript(call.id, snapshot(revision));
    slowest = Math.max(slowest, performance.now() - started);
    if (revision % 20 === 0) await wait(1);
  }
  // A wait on the blocked disk would last until it is released, below; the bound leaves room for a
  // loaded full suite (single observations took 30 ms there).
  assert.ok(slowest < 100, `observation took ${slowest.toFixed(1)} ms`);
  assert.equal(fs.readFileSync(path.join(dir, 'calls.json'), 'utf8'), journal, 'calls.json is not rewritten for transcripts');
  assert.equal(store.get(call.id)!.transcriptRevision, 200, 'readers see the newest transcript at once');
  assert.deepEqual(disk.started, [`.${call.id}.tmp`], 'one write in flight; later observations wait and coalesce');
  // A durable write meanwhile is unaffected and carries the observations with it.
  store.update(call.id, { status: 'dialing' });
  const saved = JSON.parse(fs.readFileSync(path.join(dir, 'calls.json'), 'utf8')).calls[call.id] as BrokerCall;
  assert.equal(saved.status, 'dialing');
  assert.equal(store.readCallRequestJournal().state, 'available', 'observations never make the journal look inconsistent');
  disk.release(); await wait(50);
  store.close();
});

test('observations reach their own file, coalesced, and fold into calls.json at the next start after a crash', async t => {
  const dir = directory(t);
  const store = new BrokerStore(dir, { observationIntervalMs: 0 });
  const call = outbound(store);
  for (let revision = 1; revision <= 30; revision++) store.observeTranscript(call.id, snapshot(revision));
  for (let i = 0; i < 100 && !fs.existsSync(path.join(dir, 'live.d', `${call.id}.json`)); i++) await wait(10);
  await wait(50);
  const overlay = JSON.parse(fs.readFileSync(path.join(dir, 'live.d', `${call.id}.json`), 'utf8'));
  assert.equal(overlay.transcriptRevision, 30, 'the latest observation wins');
  assert.equal(JSON.parse(fs.readFileSync(path.join(dir, 'calls.json'), 'utf8')).calls[call.id].transcriptRevision, undefined);
  // Crash: the process ends without close(); the lock is stale.
  fs.writeFileSync(path.join(dir, 'writer.lock'), JSON.stringify({ pid: 999_999_999, id: 'gone' }));
  const reopened = new BrokerStore(dir);
  assert.equal(reopened.get(call.id)!.transcriptRevision, 30);
  assert.equal(JSON.parse(fs.readFileSync(path.join(dir, 'calls.json'), 'utf8')).calls[call.id].transcriptRevision, 30, 'folded into calls.json');
  assert.deepEqual(fs.readdirSync(path.join(dir, 'live.d')), []);
  reopened.close();
});

test('an observation older than the journal or for an unknown call is discarded at start', async t => {
  const dir = directory(t);
  const store = new BrokerStore(dir);
  const call = outbound(store);
  store.update(call.id, { transcript: snapshot(5), transcriptRevision: 5 });
  store.close();
  fs.mkdirSync(path.join(dir, 'live.d'));
  fs.writeFileSync(path.join(dir, 'live.d', `${call.id}.json`), JSON.stringify({ version: 1, callId: call.id, seq: 1, transcript: snapshot(3), transcriptRevision: 3, generations: {} }));
  const unknown = randomUUID();
  fs.writeFileSync(path.join(dir, 'live.d', `${unknown}.json`), JSON.stringify({ version: 1, callId: unknown, seq: 1, generations: {} }));
  const reopened = new BrokerStore(dir);
  assert.equal(reopened.get(call.id)!.transcriptRevision, 5);
  assert.deepEqual(fs.readdirSync(path.join(dir, 'live.d')), []);
  reopened.close();
});

test('a clean stop folds observations into calls.json, so a previous release reading only calls.json has them', async t => {
  const dir = directory(t);
  const store = new BrokerStore(dir, { observationIntervalMs: 60_000 });
  const call = outbound(store);
  store.observeTranscript(call.id, snapshot(7));
  store.close();
  const saved = JSON.parse(fs.readFileSync(path.join(dir, 'calls.json'), 'utf8')).calls[call.id] as BrokerCall;
  assert.equal(saved.transcriptRevision, 7);
  assert.equal(fs.existsSync(path.join(dir, 'live.d', `${call.id}.json`)), false);
});

function endedCall(store: BrokerStore, endedAt: string) {
  const call = outbound(store);
  store.update(call.id, { callSid: 'CA' + randomUUID().replace(/-/g, ''), status: 'completed', endedAt });
  return store.get(call.id)!;
}

test('settled calls leave calls.json for their own files; reads, dedupe and receipts still see them', async t => {
  const dir = directory(t);
  const store = new BrokerStore(dir);
  const old = endedCall(store, new Date(Date.now() - 2 * SETTLED_QUIET_MS).toISOString());
  const recent = endedCall(store, new Date().toISOString());
  const open = outbound(store);
  const later = Date.now() + 2 * SETTLED_QUIET_MS;
  // updatedAt is now; settle as if the quiet period has passed for the first call only.
  const moved = await store.settle(call => call.id === old.id, later);
  assert.equal(moved, 1);
  const journal = JSON.parse(fs.readFileSync(path.join(dir, 'calls.json'), 'utf8')).calls as Record<string, BrokerCall>;
  assert.deepEqual(Object.keys(journal).sort(), [recent.id, open.id].sort(), 'calls.json holds only calls not settled');
  assert.deepEqual(JSON.parse(fs.readFileSync(path.join(dir, 'calls.d', `${old.id}.json`), 'utf8')), { version: 1, call: store.get(old.id) });
  assert.equal(store.get(old.id)!.status, 'completed');
  assert.equal(store.bySid(old.callSid!)!.id, old.id);
  assert.equal(store.reserve({ requestId: old.requestId, direction: 'outbound', purpose: 'Check the portfolio', number: '+12025550123' }).existing, true,
    'a replayed request still finds its settled call');
  assert.equal(store.readCallRequestJournal().state, 'available');
  assert.throws(() => readCallRequestReceipt(store, old.requestId), /call_request_conflict/, 'the receipt lookup reaches the settled call');
  assert.equal(store.listInProgress().some(call => call.id === old.id), false);
  store.close();
  const reopened = new BrokerStore(dir);
  assert.equal(reopened.get(old.id)!.status, 'completed', 'settled calls load at start');
  reopened.close();
});

test('a settled call that changes returns to calls.json, and that newer copy wins over its old file', async t => {
  const dir = directory(t);
  const store = new BrokerStore(dir);
  const old = endedCall(store, new Date(Date.now() - 2 * SETTLED_QUIET_MS).toISOString());
  assert.equal(await store.settle(() => true, Date.now() + 2 * SETTLED_QUIET_MS), 1);
  store.update(old.id, { failure: { code: 'late_callback', message: 'A late provider callback' } });
  assert.equal(JSON.parse(fs.readFileSync(path.join(dir, 'calls.json'), 'utf8')).calls[old.id].failure.code, 'late_callback');
  store.close();
  const reopened = new BrokerStore(dir);
  assert.equal(reopened.get(old.id)!.failure?.code, 'late_callback');
  reopened.close();
});

test('a call still owed work never settles; a write during the move keeps the call in calls.json', async t => {
  const dir = directory(t);
  const store = new BrokerStore(dir);
  const later = Date.now() + 2 * SETTLED_QUIET_MS;
  const ended = new Date(Date.now() - 2 * SETTLED_QUIET_MS).toISOString();
  const unended = outbound(store);
  const pendingClose = endedCall(store, ended); store.update(pendingClose.id, { primaryClosePending: true });
  const capturing = endedCall(store, ended);
  store.update(capturing.id, { transcriptCapture: { state: 'pending', assistantPlaybackComplete: false, sessionFinalized: true, pending: 1, unsupported: [] } });
  assert.equal(await store.settle(() => true, later), 0);
  void unended;
  // The move writes the settled file first; a change meanwhile cancels the move for that call.
  const racing = endedCall(store, ended);
  let changed = false;
  const fileSystem: OverlayFileSystem = { ...nodeOverlayFileSystem, async writeFile(file, data) {
    await nodeOverlayFileSystem.writeFile(file, data);
    if (!changed && file.includes(racing.id)) { changed = true; store.update(racing.id, { failure: { code: 'late', message: 'late' } }); }
  } };
  assert.equal(await store.settle(call => call.id === racing.id, later, fileSystem), 0);
  assert.ok(JSON.parse(fs.readFileSync(path.join(dir, 'calls.json'), 'utf8')).calls[racing.id]);
  store.close();
  const reopened = new BrokerStore(dir);
  assert.equal(reopened.get(racing.id)!.failure?.code, 'late', 'calls.json wins over the file written before the change');
  reopened.close();
});

test('rollback folds settled calls and live observations back into calls.json for the previous release', async t => {
  const dir = directory(t);
  const store = new BrokerStore(dir, { observationIntervalMs: 0 });
  const old = endedCall(store, new Date(Date.now() - 2 * SETTLED_QUIET_MS).toISOString());
  assert.equal(await store.settle(() => true, Date.now() + 2 * SETTLED_QUIET_MS), 1);
  const live = outbound(store);
  store.observeTranscript(live.id, snapshot(4));
  for (let i = 0; i < 100 && !fs.existsSync(path.join(dir, 'live.d', `${live.id}.json`)); i++) await wait(10);
  await wait(30);
  // A crash: observations only in live.d, the settled call only in calls.d.
  fs.writeFileSync(path.join(dir, 'writer.lock'), JSON.stringify({ pid: 999_999_999, id: 'gone' }));
  const state = JSON.parse(fs.readFileSync(path.join(dir, 'calls.json'), 'utf8'));
  assert.equal(state.calls[old.id], undefined);
  const { journal, folded } = foldJournalFiles(state, dir);
  assert.deepEqual(folded, { settledCalls: 1, settledSuperseded: 0, observations: 1 });
  assert.equal(journal.calls[old.id].status, 'completed');
  assert.equal(journal.calls[live.id].transcriptRevision, 4);
  const summary = runRollbackJournalCompat([dir], { now: () => new Date(Date.UTC(2026, 9, 18)) });
  assert.equal(summary.written, true);
  const rolled = JSON.parse(fs.readFileSync(path.join(dir, 'calls.json'), 'utf8'));
  assert.ok(rolled.calls[old.id]); assert.equal(rolled.calls[live.id].transcriptRevision, 4);
  assert.deepEqual(fs.readdirSync(dir).filter(name => name.endsWith('.d')), [], 'side directories renamed to backups');
  // The previous release reads calls.json only: a store that ignores side files finds everything.
  const reopened = new BrokerStore(dir);
  assert.equal(reopened.get(old.id)!.status, 'completed'); assert.equal(reopened.get(live.id)!.transcriptRevision, 4);
  reopened.close();
});
