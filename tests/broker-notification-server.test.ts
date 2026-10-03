import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import Twilio from 'twilio';
import { createBrokerServer } from '../src/broker/server.js';
import type { BrokerConfig } from '../src/broker/config.js';
import type { PrimaryApiClient } from '../src/broker/primaryApi.js';
import { phoneNotificationSourceDigest } from '../src/broker/notificationAssignment.js';
import { makeResolvedNotificationAssignment, notificationWire } from './fixtures/broker-notification.js';

async function fixture(t: test.TestContext, options: { ambiguous?: boolean; registrationRefused?: boolean; preflightFails?: boolean } = {}) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-notification-http-'));
  const resolved = makeResolvedNotificationAssignment();
  let apiStatus = 200, creates = 0, resolutions = 0, readiness = 0;
  const bindings: unknown[] = [], savedBeforeDial: unknown[] = [], callbacks: { path: string; body: unknown }[] = [];
  const api = createServer(async (request, response) => {
    if (request.url === '/health') { response.writeHead(200, { 'content-type': 'application/json' }); response.end(JSON.stringify({ ok: true, service: 'dexter-api', release: { releaseId: 'fixture-api' } })); return; }
    const chunks = []; for await (const chunk of request) chunks.push(Buffer.from(chunk));
    const body = JSON.parse(Buffer.concat(chunks).toString());
    if (request.url === '/internal/phone/resolve-notification-assignment') {
      resolutions++; assert.equal(request.headers['x-internal-auth'], 'fixture-token');
      assert.deepEqual(body, { notification: notificationWire(resolved) });
      response.writeHead(apiStatus, { 'content-type': 'application/json' }); response.end(JSON.stringify(resolved));
    } else {
      callbacks.push({ path: request.url!, body });
      if (request.url === '/internal/voice/outbound-status') { response.writeHead(204); response.end(); }
      else { response.writeHead(500); response.end(); }
    }
  });
  await new Promise<void>(resolve => api.listen(0, '127.0.0.1', resolve));
  const apiBaseUrl = `http://127.0.0.1:${(api.address() as { port: number }).port}`;
  const config: BrokerConfig = { host: '127.0.0.1', port: 0, publicOrigin: 'https://fixture.invalid', stateDir: directory,
    controlToken: 'control-'.repeat(8), apiKey: 'fixture', model: 'gpt-live-1', voice: 'marin', twilioAccountSid: `AC${'1'.repeat(32)}`,
    twilioAuthToken: 'fixture-secret', from: '+12025550100', allowedTo: [], apiBaseUrl, phoneServiceSecret: 's'.repeat(40),
    harkApiBaseUrl: apiBaseUrl, harkCallbackSigningSecret: 's'.repeat(40), harkInternalToken: 'fixture-token', maxCallSeconds: 60 };
  const sid = `CA${'2'.repeat(32)}`, bindingId = randomUUID();
  const primary = { async readiness() { readiness++; return { eligible: true, reason: 'ready' }; },
    async openCall(_proof: unknown, assignment: unknown) {
      bindings.push(assignment); if (options.registrationRefused) throw new Error('source_revoked');
      return { bindingId, callSid: sid, state: 'open', callAssignmentRef: { assignmentId: resolved.callAssignment.assignmentId, revision: 1, digest: 'a'.repeat(64) },
        context: { primaryDexterId: randomUUID(), threadId: randomUUID(), displayName: 'Fixture Owner', messages: [] } };
    }, async close() { return { bindingId, callSid: sid, state: 'closed' }; } } as unknown as PrimaryApiClient;
  let broker: ReturnType<typeof createBrokerServer>;
  const providerRecord = () => ({ sid, accountSid: config.twilioAccountSid, from: config.from, to: resolved.to, status: 'completed', direction: 'outbound-api' });
  const provider = Object.assign(() => ({ async fetch() { return providerRecord(); }, async update() { return providerRecord(); } }), {
    async create() {
      creates++; const saved = broker.store.get(resolved.callAssignment.brokerCallId)!;
      assert.equal(saved.harkDispatch?.state, 'dispatch_started'); assert.equal(saved.callAssignment?.origin, 'authenticated_notification_delivery');
      savedBeforeDial.push(saved);
      if (options.ambiguous) throw new Error('provider_reply_lost');
      return { sid, status: 'queued' };
    } });
  async function start() {
    broker = createBrokerServer({ config, primary, provider: { calls: provider } as unknown as ReturnType<typeof Twilio>,
      probe: async () => { if (options.preflightFails) throw new Error('preflight_failed'); return { model: 'gpt-live-1', audioBytes: 800, sessionId: 'fixture', finalized: true }; },
      voiceFactory: () => { throw new Error('voice_before_media'); } });
    await new Promise<void>(resolve => broker.server.listen(0, '127.0.0.1', resolve));
    return `http://127.0.0.1:${(broker.server.address() as { port: number }).port}`;
  }
  let base = await start();
  t.after(async () => { await broker.close(); api.closeAllConnections(); api.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  return { resolved, bindings, callbacks, savedBeforeDial, get broker() { return broker; }, get creates() { return creates; },
    get resolutions() { return resolutions; }, get readiness() { return readiness; }, setApiStatus(status: number) { apiStatus = status; },
    async restart() { await broker.close(); base = await start(); },
    health() { return fetch(base + '/broker/health'); },
    post(value = notificationWire(resolved), token = 'fixture-token') { return fetch(base + '/internal/outbound-alerts', {
      method: 'POST', headers: { 'content-type': 'application/json', 'x-dexter-internal-auth': token }, body: JSON.stringify(value) }); },
    answer() {
      const route = `/broker/answer/${resolved.callAssignment.brokerCallId}`;
      const params = { AccountSid: config.twilioAccountSid, CallSid: sid, From: config.from, To: resolved.to,
        Direction: 'outbound-api', CallStatus: 'in-progress' };
      return fetch(base + route, { method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded',
        'x-twilio-signature': Twilio.getExpectedTwilioSignature(config.twilioAuthToken, config.publicOrigin + route, params) }, body: new URLSearchParams(params) });
    } };
}

test('generic HTTP dispatch resolves source, binds assignment and replays accepted identity after source revocation and restart', async t => {
  const f = await fixture(t);
  assert.equal((await f.post()).status, 202); assert.equal(f.creates, 1); assert.equal(f.resolutions, 1);
  const answer = await f.answer(); assert.equal(answer.status, 200); assert.match(await answer.text(), /<Stream/);
  assert.deepEqual(f.bindings, [f.resolved.callAssignment]);
  f.setApiStatus(403);
  const replay = await f.post(); assert.equal(replay.status, 200); assert.equal((await replay.json() as { replay: boolean }).replay, true);
  assert.equal(f.resolutions, 1); assert.equal(f.creates, 1);
  await f.restart(); assert.equal((await f.post()).status, 200); assert.equal(f.creates, 1); assert.equal(f.resolutions, 1);
  assert.ok(f.callbacks.every(value => value.path === '/internal/voice/outbound-status'));
  assert.ok(f.callbacks.length > 0);
});

test('ambiguous provider dispatch survives restart and never triggers a second resolver or provider attempt', async t => {
  const f = await fixture(t, { ambiguous: true });
  assert.equal((await f.post()).status, 503); assert.equal(f.creates, 1);
  await f.restart(); const replay = await f.post(); assert.equal(replay.status, 409);
  assert.equal(f.creates, 1); assert.equal(f.resolutions, 1); assert.equal(f.readiness, 1);
  assert.equal(f.broker.store.get(f.resolved.callAssignment.brokerCallId)?.harkDispatch?.state, 'dispatch_started');
});

test('failed preflight can retry admission, while expired or not-due source cannot dispatch', async t => {
  const options = { preflightFails: true }, f = await fixture(t, options);
  assert.equal((await f.post()).status, 503); assert.equal(f.creates, 0);
  options.preflightFails = false; f.setApiStatus(403);
  assert.equal((await f.post()).status, 403); assert.equal(f.creates, 0); assert.equal(f.resolutions, 2);
  for (const expiry of [true, false]) {
    const g = await fixture(t);
    const saved = g.resolved.callAssignment.notification;
    if (expiry) saved.expiresAt = '2026-09-27T00:00:01.000Z';
    else { saved.expiresAt = '2099-09-27T00:00:00.000Z'; saved.deliveryNotBefore = '2099-09-26T00:00:00.000Z'; }
    const { sourceDigest: _old, ...source } = saved; saved.sourceDigest = phoneNotificationSourceDigest(source);
    const result = await g.post(); assert.ok([403,409].includes(result.status)); assert.equal(g.creates, 0);
  }
});

test('unauthenticated dispatch and refused answer registration cannot obtain account context or start voice', async t => {
  const f = await fixture(t, { registrationRefused: true });
  assert.equal((await f.post(notificationWire(f.resolved), 'wrong-token')).status, 401);
  assert.equal(f.resolutions, 0); assert.equal(f.creates, 0);
  assert.equal((await f.post()).status, 202);
  const answer = await f.answer(); assert.doesNotMatch(await answer.text(), /<Stream/);
  assert.equal(f.broker.store.get(f.resolved.callAssignment.brokerCallId)?.primary, undefined);
});

test('health advertises watcher-source reader capability before a producer can transfer an event', async t => {
  const f = await fixture(t), response = await f.health();
  assert.equal(response.status, 200);
  const body = await response.json() as { notificationSourceKinds: string[]; notificationWatcherProducers: string[] };
  assert.deepEqual(body.notificationSourceKinds, ['publication_v1', 'owner_destination_test_v1', 'portfolio_watch_event_v1', 'watcher_event_v1']);
  assert.deepEqual(body.notificationWatcherProducers, ['market_pulse', 'founder_portfolio_pulse', 'cate_market_cap']);
  assert.equal(f.creates, 0);
});
