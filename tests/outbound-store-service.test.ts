import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import type { OutboundAlertRequest } from '../src/outbound/contract.js';
import type { DurableFileSystem } from '../src/outbound/DurableFile.js';
import {
  OutboundAlertService,
  type VoiceCallRequest,
  type VoiceDialer,
} from '../src/outbound/OutboundAlertService.js';
import { OutboundAlertStore } from '../src/outbound/OutboundAlertStore.js';

const CALL_SID = `CA${'1'.repeat(32)}`;
const EXPIRED_CALL_SID = `CA${'2'.repeat(32)}`;
const LEGACY_MEDIA_SECRET = 'legacy-media-secret-that-is-longer-than-thirty-two-bytes';
const HARK_MEDIA_SECRET = 'hark-media-secret-that-is-longer-than-thirty-two-bytes';
const MEDIA_SECRETS = {
  legacyProducerSecret: LEGACY_MEDIA_SECRET,
  internalApiSecret: HARK_MEDIA_SECRET,
};

type DurableFailureStage = 'write' | 'file-fsync' | 'rename' | 'dir-fsync';

function faultControlledFileSystem(): {
  fileSystem: DurableFileSystem;
  arm(stage: DurableFailureStage): void;
  disarm(): void;
} {
  let failure: DurableFailureStage | null = null;
  let fsyncCount = 0;
  return {
    arm(stage) {
      failure = stage;
      fsyncCount = 0;
    },
    disarm() {
      failure = null;
      fsyncCount = 0;
    },
    fileSystem: {
      mkdirSync: (target, options) => fs.mkdirSync(target, options),
      openSync: (target, flags, mode) => fs.openSync(target, flags, mode),
      writeFileSync: (fd, data, options) => {
        if (failure === 'write') throw new Error('fault:write');
        fs.writeFileSync(fd, data, options);
      },
      chmodSync: (target, mode) => fs.chmodSync(target, mode),
      fsyncSync: (fd) => {
        fsyncCount += 1;
        if (failure === 'file-fsync' && fsyncCount === 1) {
          throw new Error('fault:file-fsync');
        }
        if (failure === 'dir-fsync' && fsyncCount === 2) {
          throw new Error('fault:dir-fsync');
        }
        fs.fsyncSync(fd);
      },
      closeSync: (fd) => fs.closeSync(fd),
      renameSync: (from, to) => {
        if (failure === 'rename') throw new Error('fault:rename');
        fs.renameSync(from, to);
      },
      unlinkSync: (target) => fs.unlinkSync(target),
    },
  };
}

function alertRequest(overrides: Partial<OutboundAlertRequest> = {}): OutboundAlertRequest {
  return {
    idempotencyKey: 'cate:below:5.36m:episode-1',
    eventId: 'cate-episode-1-below-5.36m',
    to: '+15615550100',
    recipientName: 'Branch',
    subject: 'CATE',
    severity: 'critical',
    briefing: 'CATE crossed below 5.36 million on expanding sell flow.',
    observedAt: '2026-07-27T07:00:00.000Z',
    ttlSeconds: 180,
    followupContext: { marketCapUsd: 5_350_000, direction: 'down' },
    ...overrides,
  };
}

function serverAuthoredAlertRequest(
  overrides: Partial<OutboundAlertRequest> = {},
): OutboundAlertRequest {
  return alertRequest({
    idempotencyKey: 'hark:publish-1:subscription-1',
    eventId: 'hark:publish-1:subscription-1',
    followupContext: {
      notification: {
        source: 'hark',
        publishId: '11111111-1111-4111-8111-111111111111',
        topicId: '22222222-2222-4222-8222-222222222222',
        topicAddress: 'jordan-price',
        subscriptionId: '33333333-3333-4333-8333-333333333333',
        destinationId: '44444444-4444-4444-8444-444444444444',
        billingMandateId: '55555555-5555-4555-8555-555555555555',
        billingUnitPriceMicro: 100_000,
        billingFreeSeconds: 180,
        title: 'JORDAN crossed 20%',
        priority: 'high',
        tags: ['hark'],
      },
    },
    ...overrides,
  });
}

test('dispatch is durable and idempotent across service restart', async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'dexter-alert-test-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const statePath = path.join(directory, 'state.json');
  const calls: VoiceCallRequest[] = [];
  const dialer: VoiceDialer = {
    placeCall: async (request) => {
      calls.push(request);
      return { callSid: CALL_SID };
    },
  };
  const service = new OutboundAlertService({
    store: new OutboundAlertStore(statePath),
    dialer,
    publicBaseUrl: 'https://phone.dexter.cash',
    fromNumber: '+15615550200',
  });

  const first = await service.dispatch(alertRequest(), Date.parse('2026-07-27T07:00:01Z'));
  assert.equal(first.kind, 'accepted');
  if (first.kind === 'accepted') assert.equal(first.replay, false);
  assert.equal(calls.length, 1);
  const answerUrl = new URL(calls[0]!.answerUrl);
  const statusUrl = new URL(calls[0]!.statusUrl);
  assert.equal(answerUrl.search, '');
  assert.equal(statusUrl.search, '');
  assert.match(answerUrl.pathname, /^\/outbound-alerts\/[0-9a-f-]+\/answer$/);
  assert.match(statusUrl.pathname, /^\/outbound-alerts\/[0-9a-f-]+\/status$/);
  assert.equal(calls[0]!.answerUrl.includes('token'), false);
  assert.equal(calls[0]!.statusUrl.includes('token'), false);
  const persistedState = fs.readFileSync(statePath, 'utf8');
  assert.equal(persistedState.includes('answerToken'), false);
  assert.equal(persistedState.includes('statusToken'), false);
  assert.equal(persistedState.includes('?token='), false);

  const restarted = new OutboundAlertService({
    store: new OutboundAlertStore(statePath),
    dialer,
    publicBaseUrl: 'https://phone.dexter.cash',
    fromNumber: '+15615550200',
  });
  const retry = await restarted.dispatch(alertRequest(), Date.parse('2026-07-27T07:00:02Z'));
  assert.equal(retry.kind, 'accepted');
  if (retry.kind === 'accepted') assert.equal(retry.replay, true);
  assert.equal(calls.length, 1);

  const conflict = await restarted.dispatch(
    alertRequest({ briefing: 'Different event under the same key.' }),
    Date.parse('2026-07-27T07:00:03Z'),
  );
  assert.equal(conflict.kind, 'conflict');
  assert.equal(calls.length, 1);
});

for (const failureStage of [
  'write',
  'file-fsync',
  'rename',
  'dir-fsync',
] as const) {
  test(`provider dispatch never begins after ${failureStage} reservation failure`, async (t) => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'dexter-alert-fault-'));
    t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
    const controlled = faultControlledFileSystem();
    const store = new OutboundAlertStore(
      path.join(directory, 'state.json'),
      undefined,
      { durableFileSystem: controlled.fileSystem },
    );
    const calls: VoiceCallRequest[] = [];
    const service = new OutboundAlertService({
      store,
      dialer: {
        placeCall: async (request) => {
          calls.push(request);
          return { callSid: CALL_SID };
        },
      },
      publicBaseUrl: 'https://phone.dexter.cash',
      fromNumber: '+15615550200',
    });

    controlled.arm(failureStage);
    await assert.rejects(
      service.dispatch(alertRequest(), Date.parse('2026-07-27T07:00:01Z')),
      new RegExp(`fault:${failureStage}`),
    );
    assert.equal(calls.length, 0);
    assert.equal(store.isOperational(), false);
    controlled.disarm();
    assert.equal(store.reproveStorage(), true);
    assert.equal(store.isOperational(), true);
  });
}

for (const parentChange of ['removed', 'swapped'] as const) {
  test(`provider dispatch never begins when the approved parent is ${parentChange}`, async (t) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'dexter-alert-parent-'));
    t.after(() => fs.rmSync(root, { recursive: true, force: true }));
    const directory = path.join(root, 'ledger');
    fs.mkdirSync(directory, { mode: 0o700 });
    const statePath = path.join(directory, 'state.json');
    const store = new OutboundAlertStore(statePath);
    const providerCalls: VoiceCallRequest[] = [];
    const service = new OutboundAlertService({
      store,
      dialer: {
        placeCall: async (request) => {
          providerCalls.push(request);
          return { callSid: CALL_SID };
        },
      },
      publicBaseUrl: 'https://phone.dexter.cash',
      fromNumber: '+15615550200',
    });

    if (parentChange === 'removed') {
      fs.rmSync(directory, { recursive: true });
    } else {
      fs.renameSync(directory, path.join(root, 'displaced-ledger'));
      fs.mkdirSync(directory, { mode: 0o700 });
    }

    await assert.rejects(
      service.dispatch(alertRequest()),
      /durable_parent_identity_changed/,
    );
    assert.equal(providerCalls.length, 0);
    assert.equal(store.isOperational(), false);
  });
}

test('post-provider SID persistence fault is recovered by one signed lifecycle SID and never redials', async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'dexter-alert-post-dial-fault-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const statePath = path.join(directory, 'state.json');
  const controlled = faultControlledFileSystem();
  const store = new OutboundAlertStore(
    statePath,
    undefined,
    { durableFileSystem: controlled.fileSystem },
  );
  let providerCalls = 0;
  const dialer: VoiceDialer = {
    placeCall: async () => {
      providerCalls += 1;
      controlled.arm('file-fsync');
      return { callSid: CALL_SID };
    },
  };
  const service = new OutboundAlertService({
    store,
    dialer,
    publicBaseUrl: 'https://phone.dexter.cash',
    fromNumber: '+15615550200',
  });

  await assert.rejects(service.dispatch(alertRequest()), /fault:file-fsync/);
  assert.equal(providerCalls, 1);
  assert.equal(store.isOperational(), false);
  const intentId = JSON.parse(fs.readFileSync(statePath, 'utf8')).records[0].intentId;
  assert.equal(store.get(intentId)?.callSid, CALL_SID);
  assert.equal(store.get(intentId)?.state, 'queued');

  const durableBeforeRecovery = JSON.parse(fs.readFileSync(statePath, 'utf8'));
  assert.equal(durableBeforeRecovery.records[0].callSid, null);
  assert.equal(durableBeforeRecovery.records[0].state, 'dialing');

  controlled.disarm();
  const recoveredStore = new OutboundAlertStore(statePath);
  const adopted = recoveredStore.updateTwilioStatus(
    intentId,
    CALL_SID,
    'ringing',
    1,
  );
  assert.equal(adopted?.kind, 'updated');
  assert.equal(adopted?.record.callSid, CALL_SID);
  assert.equal(adopted?.record.state, 'ringing');
  assert.equal(
    recoveredStore.adoptCallSidFromSignedTwilio(intentId, EXPIRED_CALL_SID),
    null,
  );
  const restarted = new OutboundAlertService({
    store: recoveredStore,
    dialer,
    publicBaseUrl: 'https://phone.dexter.cash',
    fromNumber: '+15615550200',
  });
  const retry = await restarted.dispatch(alertRequest());
  assert.equal(retry.kind, 'accepted');
  if (retry.kind === 'accepted') assert.equal(retry.replay, true);
  assert.equal(providerCalls, 1);
});

test('provider timeout remains dispatch-uncertain until a signed lifecycle adopts one SID', async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'dexter-alert-timeout-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const statePath = path.join(directory, 'state.json');
  let providerCalls = 0;
  const dialer: VoiceDialer = {
    placeCall: async () => {
      providerCalls += 1;
      throw new Error('provider_response_timeout');
    },
  };
  const service = new OutboundAlertService({
    store: new OutboundAlertStore(statePath),
    dialer,
    publicBaseUrl: 'https://phone.dexter.cash',
    fromNumber: '+15615550200',
  });

  await assert.rejects(service.dispatch(alertRequest()), /provider_response_timeout/);
  const persisted = JSON.parse(fs.readFileSync(statePath, 'utf8'));
  const intentId = persisted.records[0].intentId as string;
  assert.equal(persisted.records[0].state, 'dispatch-uncertain');
  assert.equal(persisted.records[0].callSid, null);

  const restartedStore = new OutboundAlertStore(statePath);
  const adopted = restartedStore.updateTwilioStatus(
    intentId,
    CALL_SID,
    'queued',
    0,
  );
  assert.equal(adopted?.kind, 'updated');
  assert.equal(adopted?.record.callSid, CALL_SID);
  assert.equal(restartedStore.get(intentId)?.callSid, CALL_SID);

  const replayService = new OutboundAlertService({
    store: restartedStore,
    dialer,
    publicBaseUrl: 'https://phone.dexter.cash',
    fromNumber: '+15615550200',
  });
  const replay = await replayService.dispatch(alertRequest());
  assert.equal(replay.kind, 'accepted');
  if (replay.kind === 'accepted') assert.equal(replay.replay, true);
  assert.equal(providerCalls, 1);
});

test('server-authored mandate and tariff freeze before dialing and survive restart', async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'dexter-alert-billing-freeze-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const statePath = path.join(directory, 'state.json');
  const calls: VoiceCallRequest[] = [];
  const dialer: VoiceDialer = {
    placeCall: async (request) => {
      calls.push(request);
      const frozen = JSON.parse(fs.readFileSync(statePath, 'utf8'));
      assert.equal(frozen.version, 8);
      assert.equal(frozen.records[0].state, 'dialing');
      assert.equal(
        frozen.records[0].billingBinding.billingMandateId,
        '55555555-5555-4555-8555-555555555555',
      );
      assert.equal(frozen.records[0].billingBinding.billingUnitPriceMicro, 100_000);
      assert.equal(frozen.records[0].billingBinding.billingFreeSeconds, 180);
      return { callSid: CALL_SID };
    },
  };
  const service = new OutboundAlertService({
    store: new OutboundAlertStore(statePath),
    dialer,
    publicBaseUrl: 'https://phone.dexter.cash',
    fromNumber: '+15615550200',
  });
  const request = serverAuthoredAlertRequest();
  const dispatchedAt = Date.now();
  const first = await service.dispatch(request, dispatchedAt);
  assert.equal(first.kind, 'accepted');
  assert.equal(calls.length, 1);

  const restarted = new OutboundAlertStore(statePath);
  const restartedService = new OutboundAlertService({
    store: restarted,
    dialer,
    publicBaseUrl: 'https://phone.dexter.cash',
    fromNumber: '+15615550200',
  });
  const retriedRequest = structuredClone(request);
  retriedRequest.observedAt = '2026-07-27T07:00:05.000Z';
  const retry = await restartedService.dispatch(retriedRequest, dispatchedAt + 500);
  assert.equal(retry.kind, 'accepted');
  if (retry.kind !== 'accepted') return;
  assert.equal(retry.replay, true);
  assert.equal(retry.record.intentId, first.record.intentId);
  assert.equal(retry.record.request.observedAt, request.observedAt);
  assert.equal(calls.length, 1);
  assert.equal(
    restarted.get(first.record.intentId)?.billingBinding?.billingMandateId,
    '55555555-5555-4555-8555-555555555555',
  );
  const authorized = restarted.authorizeAnswer(
    first.record.intentId,
    CALL_SID,
    { legacyProducerSecret: '', internalApiSecret: HARK_MEDIA_SECRET },
    dispatchedAt + 1_000,
  );
  assert.equal(authorized.ok, true);
  if (!authorized.ok) return;
  assert.equal(
    (authorized.frozenAlert.followupContext.notification as Record<string, unknown>)
      .billingMandateId,
    '55555555-5555-4555-8555-555555555555',
  );
  const changed = structuredClone(request);
  (changed.followupContext.notification as Record<string, unknown>)
    .billingUnitPriceMicro = 125_000;
  assert.equal(restarted.reserve(changed).kind, 'conflict');

  const tampered = JSON.parse(fs.readFileSync(statePath, 'utf8'));
  tampered.records[0].billingBinding.billingUnitPriceMicro = 125_000;
  fs.writeFileSync(statePath, JSON.stringify(tampered));
  assert.throws(
    () => new OutboundAlertStore(statePath),
    /outbound alert billing binding mismatch/,
  );
});

test('answer intent is bound to CallSid, expires, and yields one stream', (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'dexter-alert-test-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const store = new OutboundAlertStore(path.join(directory, 'state.json'));
  const nowMs = Date.parse('2026-07-27T07:00:00Z');
  const reserved = store.reserve(alertRequest({ ttlSeconds: 30 }), nowMs);
  assert.equal(reserved.kind, 'new');
  if (reserved.kind !== 'new') return;
  store.markDialing(reserved.record.intentId);
  store.markQueued(reserved.record.intentId, CALL_SID);
  assert.equal(
    store.updateTwilioStatus(
      reserved.record.intentId,
      EXPIRED_CALL_SID,
      'ringing',
      1,
      nowMs + 500,
    ),
    null,
  );
  assert.equal(
    store.updateTwilioStatus(
      reserved.record.intentId,
      CALL_SID,
      'ringing',
      1,
      nowMs + 500,
    )?.record.state,
    'ringing',
  );

  assert.deepEqual(
    store.authorizeAnswer(
      reserved.record.intentId,
      EXPIRED_CALL_SID,
      MEDIA_SECRETS,
      nowMs + 1_000,
    ),
    { ok: false, reason: 'wrong_call' },
  );

  const authorized = store.authorizeAnswer(
    reserved.record.intentId,
    CALL_SID,
    MEDIA_SECRETS,
    nowMs + 1_000,
  );
  assert.equal(authorized.ok, true);
  if (!authorized.ok) return;
  assert.equal(authorized.frozenAlert.subject, 'CATE');
  assert.ok(authorized.streamToken.length > 30);
  assert.equal(store.markStreaming(reserved.record.intentId, CALL_SID, nowMs + 2_000)?.state, 'streaming');
  assert.equal(store.markStreaming(reserved.record.intentId, CALL_SID, nowMs + 3_000), null);
  assert.deepEqual(
    store.authorizeAnswer(
      reserved.record.intentId,
      CALL_SID,
      MEDIA_SECRETS,
      nowMs + 3_000,
    ),
    { ok: false, reason: 'terminal' },
  );

  const expiredReservation = store.reserve(
    alertRequest({ idempotencyKey: 'cate:expired:episode-2', eventId: 'expired-2' }),
    nowMs,
  );
  assert.equal(expiredReservation.kind, 'new');
  if (expiredReservation.kind !== 'new') return;
  store.markDialing(expiredReservation.record.intentId);
  store.markQueued(expiredReservation.record.intentId, EXPIRED_CALL_SID);
  assert.deepEqual(
    store.authorizeAnswer(
      expiredReservation.record.intentId,
      EXPIRED_CALL_SID,
      MEDIA_SECRETS,
      nowMs + 181_000,
    ),
    { ok: false, reason: 'expired' },
  );
});

test('Twilio status sequence is monotonic, terminal-sticky, and separate from local streaming', (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'dexter-alert-status-order-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const store = new OutboundAlertStore(path.join(directory, 'state.json'));
  const nowMs = Date.parse('2026-07-27T07:00:00Z');

  const terminal = store.reserve(alertRequest({
    idempotencyKey: 'status-order-terminal-1',
    eventId: 'status-order-terminal-1',
  }), nowMs);
  assert.equal(terminal.kind, 'new');
  if (terminal.kind !== 'new') return;
  store.markDialing(terminal.record.intentId);
  store.markQueued(terminal.record.intentId, CALL_SID);
  assert.equal(store.updateTwilioStatus(
    terminal.record.intentId,
    CALL_SID,
    'completed',
    4,
    nowMs + 4_000,
  )?.kind, 'updated');
  assert.equal(store.updateTwilioStatus(
    terminal.record.intentId,
    CALL_SID,
    'ringing',
    2,
    nowMs + 5_000,
  )?.kind, 'stale');
  assert.equal(store.updateTwilioStatus(
    terminal.record.intentId,
    CALL_SID,
    'ringing',
    5,
    nowMs + 6_000,
  )?.kind, 'terminal');
  const completed = store.get(terminal.record.intentId);
  assert.equal(completed?.state, 'completed');
  assert.equal(completed?.lastTwilioStatus, 'completed');
  assert.equal(completed?.lastTwilioSequenceNumber, 4);

  const streaming = store.reserve(alertRequest({
    idempotencyKey: 'status-order-streaming-1',
    eventId: 'status-order-streaming-1',
  }), nowMs);
  assert.equal(streaming.kind, 'new');
  if (streaming.kind !== 'new') return;
  store.markDialing(streaming.record.intentId);
  store.markQueued(streaming.record.intentId, EXPIRED_CALL_SID);
  assert.equal(store.markStreaming(
    streaming.record.intentId,
    EXPIRED_CALL_SID,
    nowMs + 1_000,
  )?.state, 'streaming');
  const answered = store.updateTwilioStatus(
    streaming.record.intentId,
    EXPIRED_CALL_SID,
    'answered',
    2,
    nowMs + 2_000,
  );
  assert.equal(answered?.kind, 'updated');
  assert.equal(answered?.record.state, 'streaming');
  assert.equal(answered?.record.lastTwilioStatus, 'answered');
  assert.equal(answered?.record.lastTwilioSequenceNumber, 2);
  assert.equal(store.updateTwilioStatus(
    streaming.record.intentId,
    EXPIRED_CALL_SID,
    'completed',
    3,
    nowMs + 3_000,
  )?.record.state, 'completed');
});

test('Hark binding survives the durable intent and bootstrap without entering model evidence', (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'dexter-alert-hark-binding-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const store = new OutboundAlertStore(path.join(directory, 'state.json'));
  const eventId = '33333333-3333-4333-8333-333333333333';
  const deliveryId = '44444444-4444-4444-8444-444444444444';
  const deliveryEventId = `hark:${deliveryId}`;
  const request = alertRequest({
    idempotencyKey: deliveryEventId,
    eventId: deliveryEventId,
    hark: {
      schemaVersion: 1,
      caseId: '11111111-1111-4111-8111-111111111111',
      watchId: '22222222-2222-4222-8222-222222222222',
      eventId,
      deliveryId,
      originTaskId: 'hermes-task-42',
    },
  });
  const reserved = store.reserve(request);
  assert.equal(reserved.kind, 'new');
  if (reserved.kind !== 'new') return;
  store.markDialing(reserved.record.intentId);
  store.markQueued(reserved.record.intentId, CALL_SID);

  const restarted = new OutboundAlertStore(path.join(directory, 'state.json'));
  const missingHarkMediaKey = restarted.authorizeAnswer(
    reserved.record.intentId,
    CALL_SID,
    { legacyProducerSecret: LEGACY_MEDIA_SECRET, internalApiSecret: '' },
  );
  assert.deepEqual(missingHarkMediaKey, { ok: false, reason: 'media_key_unavailable' });

  const authorized = restarted.authorizeAnswer(
    reserved.record.intentId,
    CALL_SID,
    { legacyProducerSecret: '', internalApiSecret: HARK_MEDIA_SECRET },
  );
  assert.equal(authorized.ok, true);
  if (!authorized.ok) return;
  assert.deepEqual(authorized.harkBinding, request.hark);
  assert.equal('hark' in authorized.frozenAlert, false);
  assert.equal(JSON.stringify(authorized.frozenAlert).includes('hermes-task-42'), false);
});

test('version 1 callback-token hashes are removed during ledger migration', (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'dexter-alert-migration-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const statePath = path.join(directory, 'state.json');
  const request = alertRequest();
  fs.writeFileSync(statePath, JSON.stringify({
    version: 1,
    records: [{
      intentId: '11111111-1111-4111-8111-111111111111',
      idempotencyKey: request.idempotencyKey,
      requestHash: 'hash',
      request,
      state: 'queued',
      createdAt: '2026-07-27T07:00:00.000Z',
      expiresAt: '2099-07-27T07:03:00.000Z',
      answerTokenHash: 'obsolete-answer-hash',
      statusTokenHash: 'obsolete-status-hash',
      callSid: CALL_SID,
      answerConsumedAt: null,
      streamConsumedAt: null,
      completedAt: null,
      lastTwilioStatus: 'queued',
      lastError: null,
    }],
  }));

  const store = new OutboundAlertStore(statePath);
  assert.equal(store.get('11111111-1111-4111-8111-111111111111')?.callSid, CALL_SID);
  const migrated = fs.readFileSync(statePath, 'utf8');
  assert.match(migrated, /"version": 8/);
  assert.equal(migrated.includes('answerTokenHash'), false);
  assert.equal(migrated.includes('statusTokenHash'), false);
  assert.equal(migrated.includes('obsolete-answer-hash'), false);
});

test('v5 API-shaped legacy intent stays legacy across two restarts', (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'dexter-alert-v5-provenance-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const statePath = path.join(directory, 'state.json');
  const request = serverAuthoredAlertRequest();
  const intentId = '99999999-9999-4999-8999-999999999999';
  fs.writeFileSync(statePath, JSON.stringify({
    version: 5,
    records: [{
      intentId,
      idempotencyKey: request.idempotencyKey,
      requestHash: 'legacy-hmac-request-hash',
      request,
      state: 'queued',
      createdAt: '2026-07-27T07:00:00.000Z',
      expiresAt: '2099-07-27T07:03:00.000Z',
      callSid: CALL_SID,
      answerConsumedAt: null,
      streamConsumedAt: null,
      completedAt: null,
      lastTwilioStatus: 'queued',
      lastTwilioSequenceNumber: null,
      lastError: null,
    }],
    harkOutbox: [],
  }));

  const migrated = new OutboundAlertStore(statePath);
  assert.equal(migrated.get(intentId)?.billingBinding, null);
  const persisted = JSON.parse(fs.readFileSync(statePath, 'utf8'));
  assert.equal(persisted.version, 8);
  assert.equal(persisted.records[0].billingBinding, null);

  const restarted = new OutboundAlertStore(statePath);
  assert.equal(restarted.get(intentId)?.billingBinding, null);
  assert.deepEqual(restarted.authorizeAnswer(
    intentId,
    CALL_SID,
    { legacyProducerSecret: '', internalApiSecret: HARK_MEDIA_SECRET },
  ), { ok: false, reason: 'media_key_unavailable' });
  const legacyAuthorized = restarted.authorizeAnswer(
    intentId,
    CALL_SID,
    { legacyProducerSecret: LEGACY_MEDIA_SECRET, internalApiSecret: '' },
  );
  assert.equal(legacyAuthorized.ok, true);
});
