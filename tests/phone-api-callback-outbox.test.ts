import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {
  durablyReplaceJsonFile,
  type DurableFileSystem,
} from '../src/outbound/DurableFile.js';
import {
  PendingCallRegistry,
  pendingCallTokenHash,
} from '../src/outbound/PendingCallRegistry.js';
import { PhoneApiCallbackOutbox } from '../src/outbound/PhoneApiCallbackOutbox.js';

const SECRET = 'phone-api-callback-secret-longer-than-thirty-two-bytes';
const EXTERNAL_SID = `CA${'7'.repeat(32)}`;
const CALLBACK_ID = `call-ended:${EXTERNAL_SID}`;
const MEDIA_TOKEN_HASH = 'a'.repeat(64);

function callEndedBody(durationSeconds = 181): Record<string, unknown> {
  return {
    source: 'phone',
    direction: 'outbound-alert',
    alert_intent_id: '11111111-1111-4111-8111-111111111111',
    alert_event_id: 'hark:publish-1:subscription-1',
    external_sid: EXTERNAL_SID,
    caller_user_id: '00000000-0000-4000-8000-000000000001',
    duration_seconds: durationSeconds,
    started_at: '2026-08-17T12:00:00.000Z',
    ended_at: '2026-08-17T12:03:01.000Z',
  };
}

function transientFsyncFileSystem(): {
  fileSystem: DurableFileSystem;
  failNextFileFsync(): void;
} {
  let failNext = false;
  let fsyncCount = 0;
  return {
    failNextFileFsync() {
      failNext = true;
      fsyncCount = 0;
    },
    fileSystem: {
      mkdirSync: (target, options) => fs.mkdirSync(target, options),
      openSync: (target, flags, mode) => fs.openSync(target, flags, mode),
      writeFileSync: (fd, data, options) => fs.writeFileSync(fd, data, options),
      chmodSync: (target, mode) => fs.chmodSync(target, mode),
      fsyncSync: (fd) => {
        fsyncCount += 1;
        if (failNext && fsyncCount === 1) {
          failNext = false;
          throw new Error('transient_finalization_fsync_failure');
        }
        fs.fsyncSync(fd);
      },
      closeSync: (fd) => fs.closeSync(fd),
      renameSync: (from, to) => fs.renameSync(from, to),
      unlinkSync: (target) => fs.unlinkSync(target),
    },
  };
}

async function waitUntil(predicate: () => boolean, timeoutMs = 1_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error('condition_timeout');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

test('lost call-ended response retries one frozen body after restart', async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'phone-api-outbox-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const filePath = path.join(directory, 'outbox.json');
  const body = callEndedBody();
  const transportedBodies: Record<string, unknown>[] = [];
  const transportedNonces: string[] = [];
  let nonceCounter = 0;

  const first = new PhoneApiCallbackOutbox({
    filePath,
    apiBaseUrl: 'http://localhost:3030',
    signingSecret: SECRET,
    retryDelayMs: null,
    now: () => Date.parse('2026-08-17T12:03:02.000Z'),
    nonce: () => `callback-nonce-${++nonceCounter}-1234567890`,
    fetchImpl: async (_url, init) => {
      const persisted = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      assert.equal(persisted.entries[0].state, 'pending');
      assert.equal(persisted.entries[0].attempts, 1);
      assert.deepEqual(persisted.entries[0].body, body);
      transportedBodies.push(JSON.parse(String(init?.body)));
      transportedNonces.push((init?.headers as Record<string, string>)['x-dexter-alert-nonce']!);
      // Model an API commit followed by a lost transport response.
      throw new Error('response_lost_after_accept');
    },
  });

  const afterLostResponse = await first.enqueue({
    id: CALLBACK_ID,
    path: '/internal/voice/call-ended',
    body,
  });
  assert.equal(afterLostResponse.state, 'pending');
  assert.equal(afterLostResponse.attempts, 1);
  assert.match(afterLostResponse.lastError ?? '', /response_lost_after_accept/);

  const restarted = new PhoneApiCallbackOutbox({
    filePath,
    apiBaseUrl: 'http://localhost:3030',
    signingSecret: SECRET,
    retryDelayMs: null,
    now: () => Date.parse('2026-08-17T12:03:07.000Z'),
    nonce: () => `callback-nonce-${++nonceCounter}-1234567890`,
    fetchImpl: async (_url, init) => {
      transportedBodies.push(JSON.parse(String(init?.body)));
      transportedNonces.push((init?.headers as Record<string, string>)['x-dexter-alert-nonce']!);
      return new Response(JSON.stringify({
        ok: true,
        id: '88888888-8888-4888-8888-888888888888',
        status: 'billable',
      }), { status: 200 });
    },
  });
  const [delivered] = await restarted.flushPending();
  assert.equal(delivered?.state, 'delivered');
  assert.equal(delivered?.attempts, 2);
  assert.deepEqual(transportedBodies, [body, body]);
  assert.equal(new Set(transportedNonces).size, 2);
  assert.equal(fs.statSync(filePath).mode & 0o077, 0);

  const replay = await restarted.enqueue({
    id: CALLBACK_ID,
    path: '/internal/voice/call-ended',
    body: structuredClone(body),
  });
  assert.equal(replay.state, 'delivered');
  assert.equal(transportedBodies.length, 2);

  await assert.rejects(
    restarted.enqueue({
      id: CALLBACK_ID,
      path: '/internal/voice/call-ended',
      body: callEndedBody(999),
    }),
    /phone_api_callback_body_conflict/,
  );
  assert.equal(transportedBodies.length, 2);
  assert.equal(restarted.get(CALLBACK_ID)?.body.duration_seconds, 181);
});

test('crash before close finalization promotes one checkpointed callback on restart', async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'phone-call-finalization-crash-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const filePath = path.join(directory, 'outbox.json');
  const started = callEndedBody(0);
  started.ended_at = started.started_at;
  const checkpoint = callEndedBody(12);
  checkpoint.ended_at = '2026-08-17T12:00:12.000Z';

  const beforeCrash = new PhoneApiCallbackOutbox({
    filePath,
    apiBaseUrl: 'http://localhost:3030',
    signingSecret: SECRET,
    retryDelayMs: null,
  });
  beforeCrash.beginCallFinalization({
    id: CALLBACK_ID,
    body: started,
    mediaTokenHash: MEDIA_TOKEN_HASH,
    mediaExpiresAtMs: Date.now() + 60_000,
  });
  beforeCrash.startCallFinalization({
    id: CALLBACK_ID,
    mediaTokenHash: MEDIA_TOKEN_HASH,
  });
  beforeCrash.checkpointCallFinalization({ id: CALLBACK_ID, body: checkpoint });
  assert.equal(beforeCrash.activeFinalizationCount(), 1);
  assert.equal(beforeCrash.pendingCount(), 0);
  assert.throws(
    () => beforeCrash.checkpointCallFinalization({
      id: CALLBACK_ID,
      body: { ...checkpoint, caller_user_id: 'different-owner' },
    }),
    /phone_call_finalization_identity_conflict/,
  );

  const transported: Record<string, unknown>[] = [];
  const restarted = new PhoneApiCallbackOutbox({
    filePath,
    apiBaseUrl: 'http://localhost:3030',
    signingSecret: SECRET,
    retryDelayMs: null,
    fetchImpl: async (_url, init) => {
      transported.push(JSON.parse(String(init?.body)));
      return new Response(JSON.stringify({
        ok: true,
        id: '99999999-9999-4999-8999-999999999999',
        status: 'billable',
      }), { status: 200 });
    },
  });
  assert.equal(restarted.activeFinalizationCount(), 0);
  assert.equal(restarted.pendingCount(), 1);
  assert.deepEqual(restarted.get(CALLBACK_ID)?.body, checkpoint);

  const delivered = await restarted.flushPending();
  assert.equal(delivered.length, 1);
  assert.equal(delivered[0]?.state, 'delivered');
  assert.deepEqual(transported, [checkpoint]);

  const secondRestart = new PhoneApiCallbackOutbox({
    filePath,
    apiBaseUrl: 'http://localhost:3030',
    signingSecret: SECRET,
    retryDelayMs: null,
    fetchImpl: async () => {
      throw new Error('delivered callback must not be sent twice');
    },
  });
  assert.deepEqual(await secondRestart.flushPending(), []);
});

test('transient close fsync failure stays unhealthy and heals in-process without restart', async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'phone-finalization-retry-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const controlled = transientFsyncFileSystem();
  const deliveredBodies: Record<string, unknown>[] = [];
  const started = callEndedBody(0);
  started.ended_at = started.started_at;
  const closed = callEndedBody(8);
  closed.ended_at = '2026-08-17T12:00:08.000Z';
  const outbox = new PhoneApiCallbackOutbox({
    filePath: path.join(directory, 'outbox.json'),
    apiBaseUrl: 'http://localhost:3030',
    signingSecret: SECRET,
    durableFileSystem: controlled.fileSystem,
    retryDelayMs: 20,
    fetchImpl: async (_url, init) => {
      deliveredBodies.push(JSON.parse(String(init?.body)));
      return new Response(JSON.stringify({
        ok: true,
        id: '77777777-7777-4777-8777-777777777777',
        status: 'billable',
      }), { status: 200 });
    },
  });
  outbox.beginCallFinalization({
    id: CALLBACK_ID,
    body: started,
    mediaTokenHash: MEDIA_TOKEN_HASH,
    mediaExpiresAtMs: Date.now() + 60_000,
  });
  outbox.startCallFinalization({
    id: CALLBACK_ID,
    mediaTokenHash: MEDIA_TOKEN_HASH,
  });

  controlled.failNextFileFsync();
  assert.throws(
    () => outbox.finalizeCall({
      id: CALLBACK_ID,
      path: '/internal/voice/call-ended',
      body: closed,
    }),
    /transient_finalization_fsync_failure/,
  );
  assert.equal(outbox.getCallFinalization(CALLBACK_ID)?.phase, 'closed');
  assert.equal(outbox.pendingCount(), 0);
  assert.equal(outbox.isOperational(), false);
  assert.equal(outbox.reproveStorage(), false);

  await waitUntil(() => outbox.get(CALLBACK_ID)?.state === 'delivered');
  assert.equal(outbox.getCallFinalization(CALLBACK_ID), null);
  assert.equal(outbox.isOperational(), true);
  assert.deepEqual(deliveredBodies, [closed]);
});

test('pre-TwiML token hash expires into one receipt and revokes late media', (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'phone-pre-twiml-ledger-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const registry = new PendingCallRegistry<{ kind: 'inbound' }>();
  const token = 'pre-twiml-one-shot-token';
  const tokenHash = pendingCallTokenHash(token);
  const startedAtMs = Date.parse('2026-08-17T12:00:00.000Z');
  const expiresAtMs = startedAtMs + 30_000;
  registry.issueWithToken(
    EXTERNAL_SID,
    token,
    { kind: 'inbound' },
    expiresAtMs,
    startedAtMs,
  );
  const started = {
    ...callEndedBody(0),
    direction: 'inbound',
    alert_intent_id: null,
    alert_event_id: null,
    ended_at: '2026-08-17T12:00:00.000Z',
    link_token_hash: 'frozen-link-token-hash',
  };
  const outbox = new PhoneApiCallbackOutbox({
    filePath: path.join(directory, 'outbox.json'),
    apiBaseUrl: 'http://localhost:3030',
    signingSecret: SECRET,
    retryDelayMs: null,
    now: () => startedAtMs,
    onMediaTokenRevoked: (callSid) => registry.revoke(callSid),
  });
  outbox.beginCallFinalization({
    id: CALLBACK_ID,
    body: started,
    mediaTokenHash: tokenHash,
    mediaExpiresAtMs: expiresAtMs,
  });
  const persistedBeforeTwiml = JSON.parse(
    fs.readFileSync(path.join(directory, 'outbox.json'), 'utf8'),
  );
  assert.equal(persistedBeforeTwiml.callFinalizations[0].phase, 'awaiting_media');
  assert.equal(persistedBeforeTwiml.callFinalizations[0].mediaTokenHash, tokenHash);
  assert.equal(persistedBeforeTwiml.callFinalizations[0].body.link_token_hash, 'frozen-link-token-hash');

  const frozen = outbox.finalizeAwaitingMedia({
    id: CALLBACK_ID,
    endedAtMs: expiresAtMs,
  });
  assert.ok(frozen);
  assert.equal(frozen.body.duration_seconds, 0);
  assert.equal(frozen.body.ended_at, new Date(expiresAtMs).toISOString());
  assert.equal(outbox.pendingCount(), 1);
  assert.equal(registry.consume(EXTERNAL_SID, token, expiresAtMs - 1), null);
  assert.throws(
    () => outbox.startCallFinalization({ id: CALLBACK_ID, mediaTokenHash: tokenHash }),
    /phone_call_finalization_already_frozen/,
  );
  assert.equal(
    outbox.finalizeAwaitingMedia({ id: CALLBACK_ID, endedAtMs: expiresAtMs + 10_000 })?.bodyHash,
    frozen?.bodyHash,
  );
  assert.equal(JSON.parse(
    fs.readFileSync(path.join(directory, 'outbox.json'), 'utf8'),
  ).entries.length, 1);
});

test('pre-TwiML restart freezes zero duration at the actual recovery time', (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'phone-pre-twiml-restart-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const filePath = path.join(directory, 'outbox.json');
  const startedAtMs = Date.parse('2026-08-17T12:00:00.000Z');
  const recoveredAtMs = startedAtMs + 20_000;
  const started = {
    ...callEndedBody(0),
    direction: 'inbound',
    alert_intent_id: null,
    alert_event_id: null,
    ended_at: new Date(startedAtMs).toISOString(),
    link_token_hash: 'frozen-link-token-hash',
  };

  const beforeCrash = new PhoneApiCallbackOutbox({
    filePath,
    apiBaseUrl: 'http://localhost:3030',
    signingSecret: SECRET,
    retryDelayMs: null,
    now: () => startedAtMs,
  });
  beforeCrash.beginCallFinalization({
    id: CALLBACK_ID,
    body: started,
    mediaTokenHash: MEDIA_TOKEN_HASH,
    mediaExpiresAtMs: startedAtMs + 60_000,
  });

  const restarted = new PhoneApiCallbackOutbox({
    filePath,
    apiBaseUrl: 'http://localhost:3030',
    signingSecret: SECRET,
    retryDelayMs: null,
    now: () => recoveredAtMs,
  });
  const recovered = restarted.get(CALLBACK_ID);
  assert.equal(recovered?.body.duration_seconds, 0);
  assert.equal(recovered?.body.ended_at, new Date(recoveredAtMs).toISOString());
  assert.equal(restarted.reproveStorage(), true);

  const secondRestart = new PhoneApiCallbackOutbox({
    filePath,
    apiBaseUrl: 'http://localhost:3030',
    signingSecret: SECRET,
    retryDelayMs: null,
    now: () => recoveredAtMs + 10_000,
  });
  assert.deepEqual(secondRestart.get(CALLBACK_ID)?.body, recovered?.body);
});

test('fake 2xx responses remain pending until the endpoint contract matches', async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'phone-api-outbox-contract-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const callEndedPath = path.join(directory, 'call-ended.json');
  const statusPath = path.join(directory, 'status.json');

  const badCallEnded = new PhoneApiCallbackOutbox({
    filePath: callEndedPath,
    apiBaseUrl: 'http://localhost:3030',
    signingSecret: SECRET,
    retryDelayMs: null,
    fetchImpl: async () => new Response(JSON.stringify({ ok: true }), { status: 200 }),
  });
  const callEnded = await badCallEnded.enqueue({
    id: CALLBACK_ID,
    path: '/internal/voice/call-ended',
    body: callEndedBody(),
  });
  assert.equal(callEnded.state, 'pending');
  assert.equal(callEnded.lastError, 'unexpected_call_ended_response_body');
  assert.equal(badCallEnded.isOperational(), true);
  assert.equal(badCallEnded.pendingCount(), 1);

  const badStatus = new PhoneApiCallbackOutbox({
    filePath: statusPath,
    apiBaseUrl: 'http://localhost:3030',
    signingSecret: SECRET,
    retryDelayMs: null,
    fetchImpl: async () => new Response('{}', { status: 200 }),
  });
  const status = await badStatus.enqueue({
    id: `outbound-status:${EXTERNAL_SID}:completed`,
    path: '/internal/voice/outbound-status',
    body: {
      intentId: '11111111-1111-4111-8111-111111111111',
      eventId: 'hark:publish-1:subscription-1',
      callSid: EXTERNAL_SID,
      state: 'completed',
      observedAt: '2026-08-17T12:03:01.000Z',
    },
  });
  assert.equal(status.state, 'pending');
  assert.equal(status.lastError, 'unexpected_status_response:200');
  assert.equal(badStatus.isOperational(), true);
  assert.equal(badStatus.pendingCount(), 1);
});

test('terminal callback identity conflict fails readiness while retryable failures do not', async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'phone-api-outbox-conflict-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const outbox = new PhoneApiCallbackOutbox({
    filePath: path.join(directory, 'outbox.json'),
    apiBaseUrl: 'http://localhost:3030',
    signingSecret: SECRET,
    retryDelayMs: null,
    fetchImpl: async () => new Response('{}', { status: 409 }),
  });

  const entry = await outbox.enqueue({
    id: CALLBACK_ID,
    path: '/internal/voice/call-ended',
    body: callEndedBody(),
  });
  assert.equal(entry.state, 'rejected');
  assert.equal(entry.lastError, 'callback_identity_conflict');
  assert.equal(outbox.isOperational(), false);
  assert.equal(outbox.rejectedCount(), 1);
});

test('callback persistence refuses a recreated approved parent', (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'phone-api-parent-swap-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const directory = path.join(root, 'ledger');
  fs.mkdirSync(directory, { mode: 0o700 });
  const outbox = new PhoneApiCallbackOutbox({
    filePath: path.join(directory, 'outbox.json'),
    apiBaseUrl: 'http://localhost:3030',
    signingSecret: SECRET,
    retryDelayMs: null,
  });
  fs.renameSync(directory, path.join(root, 'displaced-ledger'));
  fs.mkdirSync(directory, { mode: 0o700 });

  assert.throws(
    () => outbox.reserve({
      id: CALLBACK_ID,
      path: '/internal/voice/call-ended',
      body: callEndedBody(),
    }),
    /durable_parent_identity_changed/,
  );
  assert.equal(outbox.get(CALLBACK_ID), null);
  assert.equal(outbox.isOperational(), false);
});

test('durable publication fsyncs bytes before rename and directory after rename', (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'durable-order-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const filePath = path.join(directory, 'outbox.json');
  const events: string[] = [];
  const fileSystem = {
    mkdirSync: (target: string, options: { recursive: true }) => {
      events.push(`mkdir:${target}`);
      return fs.mkdirSync(target, options);
    },
    openSync: (target: string, flags: string, mode?: number) => {
      events.push(`open:${flags}:${target}`);
      return fs.openSync(target, flags, mode);
    },
    writeFileSync: (fd: number, data: string, options: { encoding: 'utf8' }) => {
      events.push(`write:${fd}`);
      fs.writeFileSync(fd, data, options);
    },
    chmodSync: (target: string, mode: number) => {
      events.push(`chmod:${target}`);
      fs.chmodSync(target, mode);
    },
    fsyncSync: (fd: number) => {
      events.push(`fsync:${fd}`);
      fs.fsyncSync(fd);
    },
    closeSync: (fd: number) => {
      events.push(`close:${fd}`);
      fs.closeSync(fd);
    },
    renameSync: (from: string, to: string) => {
      events.push(`rename:${from}:${to}`);
      fs.renameSync(from, to);
    },
    unlinkSync: (target: string) => {
      events.push(`unlink:${target}`);
      fs.unlinkSync(target);
    },
  };

  durablyReplaceJsonFile(filePath, '{}', fileSystem);

  const write = events.findIndex((event) => event.startsWith('write:'));
  const fileFd = events[write]?.split(':')[1];
  const fileSync = events.findIndex((event) => event === `fsync:${fileFd}`);
  const rename = events.findIndex((event) => event.startsWith('rename:'));
  const directoryOpen = events.findIndex(
    (event) => event === `open:r:${directory}`,
  );
  const directorySync = events.findIndex(
    (event, index) => index > directoryOpen && event.startsWith('fsync:'),
  );
  assert.ok(write >= 0 && write < fileSync);
  assert.ok(fileSync < rename);
  assert.ok(rename < directoryOpen);
  assert.ok(directoryOpen < directorySync);
  assert.equal(events.some((event) => event.startsWith('mkdir:')), false);
});

test('failed file fsync cannot publish or acknowledge the callback ledger', (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'durable-fsync-fail-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const filePath = path.join(directory, 'outbox.json');
  const events: string[] = [];
  const fileSystem = {
    mkdirSync: (target: string, options: { recursive: true }) => fs.mkdirSync(target, options),
    openSync: (target: string, flags: string, mode?: number) => fs.openSync(target, flags, mode),
    writeFileSync: (fd: number, data: string, options: { encoding: 'utf8' }) => {
      events.push('write');
      fs.writeFileSync(fd, data, options);
    },
    chmodSync: (target: string, mode: number) => fs.chmodSync(target, mode),
    fsyncSync: () => { throw new Error('simulated_power_boundary_failure'); },
    closeSync: (fd: number) => {
      events.push('close');
      fs.closeSync(fd);
    },
    renameSync: () => { events.push('rename'); },
    unlinkSync: (target: string) => {
      events.push('unlink');
      fs.unlinkSync(target);
    },
  };

  assert.throws(
    () => durablyReplaceJsonFile(filePath, '{}', fileSystem),
    /simulated_power_boundary_failure/,
  );
  assert.equal(events.includes('rename'), false);
  assert.equal(events.includes('unlink'), true);
});
