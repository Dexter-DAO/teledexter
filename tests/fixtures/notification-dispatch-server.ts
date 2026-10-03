/** Local-only cross-repository fixture. All provider and voice operations are test doubles. */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Twilio from 'twilio';
import { createBrokerServer } from '../../src/broker/server.js';
import type { BrokerConfig } from '../../src/broker/config.js';
import type { PrimaryApiClient } from '../../src/broker/primaryApi.js';

if (!process.send || !/^http:\/\/127\.0\.0\.1:\d+$/.test(process.env.NOTIFICATION_FIXTURE_API_ORIGIN ?? ''))
  throw new Error('local_fixture_guard');
const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'notification-broker-journey-'));
const config: BrokerConfig = { host: '127.0.0.1', port: 0, publicOrigin: 'https://fixture.invalid',
  stateDir: directory, controlToken: 'fixture-control-'.repeat(4), apiKey: 'fixture-key', model: 'gpt-live-1', voice: 'marin',
  twilioAccountSid: `AC${'1'.repeat(32)}`, twilioAuthToken: 'fixture-secret', from: '+12025550100',
  allowedTo: ['+12025550123'], apiBaseUrl: process.env.NOTIFICATION_FIXTURE_API_ORIGIN!, phoneServiceSecret: 's'.repeat(40), maxCallSeconds: 60,
  harkInternalToken: 'fixture-hark-token', harkApiBaseUrl: process.env.NOTIFICATION_FIXTURE_API_ORIGIN };
const dispatches: unknown[] = [];
const provider = Object.assign((_sid: string) => ({ async fetch() { throw new Error('unexpected_provider_read'); },
  async update() { throw new Error('unexpected_provider_update'); } }), {
  async create(input: { url: string; to: string }) {
    const callId = input.url.split('/').at(-1)!;
    const call = broker.store.get(callId)!;
    assert.equal(call.callAssignment?.origin, 'authenticated_notification_delivery');
    assert.equal(call.status, 'dialing');
    dispatches.push(structuredClone(call.callAssignment));
    return { sid: `CA${String(dispatches.length).padStart(32, '0')}`, status: 'queued' };
  },
});
const broker = createBrokerServer({ config, primary: { async readiness() { return { eligible: true, reason: 'ready' }; } } as unknown as PrimaryApiClient,
  provider: { calls: provider } as unknown as ReturnType<typeof Twilio>,
  probe: async () => ({ model: 'gpt-live-1', audioBytes: 800, sessionId: 'fixture', finalized: true }),
  voiceFactory: () => { throw new Error('no_voice_session_requested'); } });
await new Promise<void>(resolve => broker.server.listen(0, '127.0.0.1', resolve));
process.send({ ready: `http://127.0.0.1:${(broker.server.address() as { port: number }).port}/internal/outbound-alerts` });
let stopped = false;
async function stop() {
  if (stopped) return; stopped = true;
  await broker.close(); fs.rmSync(directory, { recursive: true, force: true }); process.exit(0);
}
process.on('message', message => {
  if (message === 'stats') process.send?.({ dispatches, calls: broker.store.list() });
  if (message === 'stop') void stop();
});
process.once('SIGTERM', () => { void stop(); });
process.once('disconnect', () => { void stop(); });
