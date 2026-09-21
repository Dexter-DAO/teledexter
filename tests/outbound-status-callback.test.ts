import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import type { DurableFileSystem } from '../src/outbound/DurableFile.js';
import { OutboundAlertStore } from '../src/outbound/OutboundAlertStore.js';
import { PhoneApiCallbackOutbox } from '../src/outbound/PhoneApiCallbackOutbox.js';
import { persistOutboundStatusCallback } from '../src/outbound/OutboundStatusCallback.js';
import {
  PendingCallRegistry,
  pendingCallTokenHash,
} from '../src/outbound/PendingCallRegistry.js';
import {
  phoneFinancialReadiness,
  type DurablePathValidation,
} from '../src/outbound/PhoneFinancialReadiness.js';
import type { OutboundAlertRequest } from '../src/outbound/contract.js';

const SECRET = 'phone-api-callback-secret-longer-than-thirty-two-bytes';
const CALL_SID = `CA${'4'.repeat(32)}`;
const NOW_MS = Date.parse('2026-08-17T12:00:00.000Z');

function alertRequest(): OutboundAlertRequest {
  return {
    idempotencyKey: 'hark:publish-status:subscription-status',
    eventId: 'hark:publish-status:subscription-status',
    to: '+15615550100',
    recipientName: 'Branch',
    subject: 'Status durability',
    severity: 'high',
    briefing: 'Prove the lifecycle callback boundary.',
    observedAt: '2026-08-17T11:59:59.000Z',
    ttlSeconds: 300,
    followupContext: {},
  };
}

function mutableFileSystem(): {
  fileSystem: DurableFileSystem;
  failNextFileFsync(): void;
} {
  let failFileFsync = false;
  let fsyncCount = 0;
  return {
    failNextFileFsync() {
      failFileFsync = true;
      fsyncCount = 0;
    },
    fileSystem: {
      mkdirSync: (target, options) => fs.mkdirSync(target, options),
      openSync: (target, flags, mode) => fs.openSync(target, flags, mode),
      writeFileSync: (fd, data, options) => fs.writeFileSync(fd, data, options),
      chmodSync: (target, mode) => fs.chmodSync(target, mode),
      fsyncSync: (fd) => {
        fsyncCount += 1;
        if (failFileFsync && fsyncCount === 1) {
          failFileFsync = false;
          throw new Error('simulated_outbox_fsync_failure');
        }
        fs.fsyncSync(fd);
      },
      closeSync: (fd) => fs.closeSync(fd),
      renameSync: (from, to) => fs.renameSync(from, to),
      unlinkSync: (target) => fs.unlinkSync(target),
    },
  };
}

function queuedStore(
  statePath: string,
  fileSystem?: DurableFileSystem,
): { store: OutboundAlertStore; intentId: string } {
  const store = new OutboundAlertStore(
    statePath,
    undefined,
    fileSystem ? { durableFileSystem: fileSystem } : {},
  );
  const reserved = store.reserve(alertRequest(), NOW_MS);
  assert.equal(reserved.kind, 'new');
  if (reserved.kind !== 'new') throw new Error('reservation failed');
  store.markDialing(reserved.record.intentId);
  store.markQueued(reserved.record.intentId, CALL_SID);
  return { store, intentId: reserved.record.intentId };
}

test('outbox fsync failure is retryable and Twilio retry durably enqueues once', (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'status-outbox-fsync-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const { store, intentId } = queuedStore(path.join(directory, 'state.json'));
  const controlled = mutableFileSystem();
  const outboxPath = path.join(directory, 'outbox.json');
  const outbox = new PhoneApiCallbackOutbox({
    filePath: outboxPath,
    apiBaseUrl: 'http://localhost:3030',
    signingSecret: SECRET,
    durableFileSystem: controlled.fileSystem,
    retryDelayMs: null,
  });

  controlled.failNextFileFsync();
  const failed = persistOutboundStatusCallback({
    store,
    callbackOutbox: outbox,
    intentId,
    callSid: CALL_SID,
    callStatus: 'ringing',
    sequenceNumber: 1,
    now: () => NOW_MS + 1_000,
  });
  assert.equal(failed.kind, 'retry');
  assert.equal(store.get(intentId)?.lastTwilioSequenceNumber, null);
  assert.equal(outbox.pendingCount(), 0);
  assert.equal(outbox.isOperational(), false);

  const retried = persistOutboundStatusCallback({
    store,
    callbackOutbox: outbox,
    intentId,
    callSid: CALL_SID,
    callStatus: 'ringing',
    sequenceNumber: 1,
    now: () => NOW_MS + 2_000,
  });
  assert.equal(retried.kind, 'updated');
  assert.equal(store.get(intentId)?.lastTwilioSequenceNumber, 1);
  assert.equal(outbox.pendingCount(), 1);
  const persisted = JSON.parse(fs.readFileSync(outboxPath, 'utf8'));
  assert.equal(persisted.entries.length, 1);
  assert.equal(outbox.isOperational(), true);
});

test('store failure after callback reservation reuses the exact frozen body on retry', (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'status-store-fsync-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const controlledStore = mutableFileSystem();
  const { store, intentId } = queuedStore(
    path.join(directory, 'state.json'),
    controlledStore.fileSystem,
  );
  const outbox = new PhoneApiCallbackOutbox({
    filePath: path.join(directory, 'outbox.json'),
    apiBaseUrl: 'http://localhost:3030',
    signingSecret: SECRET,
    retryDelayMs: null,
  });

  controlledStore.failNextFileFsync();
  const failed = persistOutboundStatusCallback({
    store,
    callbackOutbox: outbox,
    intentId,
    callSid: CALL_SID,
    callStatus: 'answered',
    sequenceNumber: 2,
    now: () => NOW_MS + 2_000,
  });
  assert.equal(failed.kind, 'retry');
  const callbackId = `outbound-status:${intentId}:${CALL_SID}:2`;
  const frozenBeforeRetry = outbox.get(callbackId);
  assert.ok(frozenBeforeRetry);
  assert.equal(store.get(intentId)?.lastTwilioSequenceNumber, null);

  const retry = persistOutboundStatusCallback({
    store,
    callbackOutbox: outbox,
    intentId,
    callSid: CALL_SID,
    callStatus: 'answered',
    sequenceNumber: 2,
    now: () => NOW_MS + 9_000,
  });
  assert.equal(retry.kind, 'updated');
  assert.deepEqual(outbox.get(callbackId)?.body, frozenBeforeRetry?.body);
  const persisted = JSON.parse(
    fs.readFileSync(path.join(directory, 'outbox.json'), 'utf8'),
  );
  assert.equal(persisted.entries.length, 1);
});

test('first authenticated lifecycle adopts one SID for a dispatch-uncertain intent', (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'status-adopt-sid-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const store = new OutboundAlertStore(path.join(directory, 'state.json'));
  const reserved = store.reserve(alertRequest(), NOW_MS);
  assert.equal(reserved.kind, 'new');
  if (reserved.kind !== 'new') return;
  store.markDialing(reserved.record.intentId);
  store.markDispatchUncertain(reserved.record.intentId, 'provider_response_timeout');
  const outbox = new PhoneApiCallbackOutbox({
    filePath: path.join(directory, 'outbox.json'),
    apiBaseUrl: 'http://localhost:3030',
    signingSecret: SECRET,
    retryDelayMs: null,
  });

  const adopted = persistOutboundStatusCallback({
    store,
    callbackOutbox: outbox,
    intentId: reserved.record.intentId,
    callSid: CALL_SID,
    callStatus: 'ringing',
    sequenceNumber: 1,
    now: () => NOW_MS + 1_000,
  });
  assert.equal(adopted.kind, 'updated');
  assert.equal(store.get(reserved.record.intentId)?.callSid, CALL_SID);
  assert.equal(store.get(reserved.record.intentId)?.state, 'ringing');

  const conflictingSid = persistOutboundStatusCallback({
    store,
    callbackOutbox: outbox,
    intentId: reserved.record.intentId,
    callSid: `CA${'9'.repeat(32)}`,
    callStatus: 'completed',
    sequenceNumber: 2,
    now: () => NOW_MS + 2_000,
  });
  assert.equal(conflictingSid.kind, 'forbidden');
  assert.equal(store.get(reserved.record.intentId)?.callSid, CALL_SID);
});

test('signed terminal status finalizes a no-media call once and revokes its token', (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'status-no-media-finalize-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const { store, intentId } = queuedStore(path.join(directory, 'state.json'));
  const registry = new PendingCallRegistry<{ intentId: string }>();
  const streamToken = 'signed-terminal-no-media-token';
  registry.issueWithToken(
    CALL_SID,
    streamToken,
    { intentId },
    NOW_MS + 60_000,
    NOW_MS,
  );
  const outbox = new PhoneApiCallbackOutbox({
    filePath: path.join(directory, 'outbox.json'),
    apiBaseUrl: 'http://localhost:3030',
    signingSecret: SECRET,
    retryDelayMs: null,
    now: () => NOW_MS,
    onMediaTokenRevoked: (callSid) => registry.revoke(callSid),
  });
  outbox.beginCallFinalization({
    id: `call-ended:${CALL_SID}`,
    body: {
      source: 'phone',
      direction: 'outbound-alert',
      alert_intent_id: intentId,
      alert_event_id: alertRequest().eventId,
      external_sid: CALL_SID,
      caller_user_id: '00000000-0000-4000-8000-000000000001',
      started_at: new Date(NOW_MS).toISOString(),
      ended_at: new Date(NOW_MS).toISOString(),
      duration_seconds: 0,
      link_token_hash: 'exact-link-token-hash',
    },
    mediaTokenHash: pendingCallTokenHash(streamToken),
    mediaExpiresAtMs: NOW_MS + 60_000,
  });

  const terminal = persistOutboundStatusCallback({
    store,
    callbackOutbox: outbox,
    intentId,
    callSid: CALL_SID,
    callStatus: 'completed',
    sequenceNumber: 1,
    now: () => NOW_MS + 9_000,
  });
  assert.equal(terminal.kind, 'updated');
  assert.equal(outbox.getCallFinalization(`call-ended:${CALL_SID}`), null);
  assert.equal(outbox.get(`call-ended:${CALL_SID}`)?.body.duration_seconds, 0);
  assert.equal(
    outbox.get(`call-ended:${CALL_SID}`)?.body.ended_at,
    new Date(NOW_MS + 9_000).toISOString(),
  );
  assert.equal(outbox.pendingCount(), 2);
  assert.equal(registry.consume(CALL_SID, streamToken, NOW_MS + 9_001), null);
  assert.throws(
    () => outbox.startCallFinalization({
      id: `call-ended:${CALL_SID}`,
      mediaTokenHash: pendingCallTokenHash(streamToken),
    }),
    /phone_call_finalization_already_frozen/,
  );

  const replay = persistOutboundStatusCallback({
    store,
    callbackOutbox: outbox,
    intentId,
    callSid: CALL_SID,
    callStatus: 'completed',
    sequenceNumber: 1,
    now: () => NOW_MS + 20_000,
  });
  assert.equal(replay.kind, 'ignored');
  assert.equal(JSON.parse(
    fs.readFileSync(path.join(directory, 'outbox.json'), 'utf8'),
  ).entries.length, 2);
});

test('Realtime-degraded Twilio speech closes one zero-duration financial receipt', (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'status-twilio-speech-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const { store, intentId } = queuedStore(path.join(directory, 'state.json'));
  const outboxPath = path.join(directory, 'outbox.json');
  const outbox = new PhoneApiCallbackOutbox({
    filePath: outboxPath,
    apiBaseUrl: 'http://localhost:3030',
    signingSecret: SECRET,
    retryDelayMs: null,
    now: () => NOW_MS,
  });
  const reservedStreamToken = 'twilio-speech-no-media-token';
  outbox.beginCallFinalization({
    id: `call-ended:${CALL_SID}`,
    body: {
      source: 'phone',
      direction: 'outbound-alert',
      alert_intent_id: intentId,
      alert_event_id: alertRequest().eventId,
      external_sid: CALL_SID,
      caller_number: '+15615550100',
      started_at: new Date(NOW_MS).toISOString(),
      ended_at: new Date(NOW_MS).toISOString(),
      duration_seconds: 0,
    },
    mediaTokenHash: pendingCallTokenHash(reservedStreamToken),
    mediaExpiresAtMs: NOW_MS + 60_000,
  });

  const terminal = persistOutboundStatusCallback({
    store,
    callbackOutbox: outbox,
    intentId,
    callSid: CALL_SID,
    callStatus: 'completed',
    sequenceNumber: 1,
    now: () => NOW_MS + 5_000,
  });
  assert.equal(terminal.kind, 'updated');
  assert.equal(outbox.getCallFinalization(`call-ended:${CALL_SID}`), null);
  assert.equal(outbox.get(`call-ended:${CALL_SID}`)?.body.duration_seconds, 0);

  const replay = persistOutboundStatusCallback({
    store,
    callbackOutbox: outbox,
    intentId,
    callSid: CALL_SID,
    callStatus: 'completed',
    sequenceNumber: 1,
    now: () => NOW_MS + 9_000,
  });
  assert.equal(replay.kind, 'ignored');
  const persisted = JSON.parse(fs.readFileSync(outboxPath, 'utf8'));
  assert.equal(
    persisted.entries.filter((entry: { id: string }) => (
      entry.id === `call-ended:${CALL_SID}`
    )).length,
    1,
  );
});

test('a durable pending callback does not revoke its admitted media stream', async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'status-pending-media-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const { store, intentId } = queuedStore(path.join(directory, 'state.json'));
  let releaseApi!: () => void;
  const waitForApi = new Promise<void>((resolve) => { releaseApi = resolve; });
  const outbox = new PhoneApiCallbackOutbox({
    filePath: path.join(directory, 'outbox.json'),
    apiBaseUrl: 'http://localhost:3030',
    signingSecret: SECRET,
    retryDelayMs: null,
    fetchImpl: async () => {
      await waitForApi;
      return new Response(null, { status: 204 });
    },
  });
  const persisted = persistOutboundStatusCallback({
    store,
    callbackOutbox: outbox,
    intentId,
    callSid: CALL_SID,
    callStatus: 'ringing',
    sequenceNumber: 1,
    now: () => NOW_MS + 1_000,
  });
  assert.equal(persisted.kind, 'updated');
  if (persisted.kind !== 'updated') return;
  const delivery = outbox.deliver(persisted.callbackId);

  const validPath: DurablePathValidation = {
    ok: true,
    path: '/var/lib/dexter-phone/state.json',
  };
  assert.deepEqual(phoneFinancialReadiness({
    callbackSecretReady: true,
    outboundStatePath: validPath,
    callbackOutboxPath: validPath,
    outboundStoreOperational: store.isOperational(),
    callbackOutboxOperational: outbox.isOperational(),
  }), { ok: true, reasons: [] });
  assert.equal(outbox.pendingCount(), 1);

  const registry = new PendingCallRegistry<{ intentId: string }>();
  const streamToken = registry.issue(
    CALL_SID,
    { intentId },
    NOW_MS + 60_000,
    NOW_MS,
  );
  assert.deepEqual(
    registry.consume(CALL_SID, streamToken, NOW_MS + 2_000),
    { intentId },
  );

  releaseApi();
  assert.equal((await delivery).state, 'delivered');
  assert.equal(outbox.pendingCount(), 0);
});
