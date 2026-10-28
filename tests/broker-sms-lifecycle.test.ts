import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { spawn } from 'node:child_process';
import Twilio from 'twilio';
import { closeBrokerSms } from '../src/broker/sms.js';

async function launch(t: test.TestContext, mode: 'startup' | 'deadline' | 'startup-error') {
  const dir = fs.mkdtempSync(path.join(os.homedir(), '.local/state/broker-sms-lifecycle-'));
  const portReservation = createServer();
  await new Promise<void>(resolve => portReservation.listen(0, '127.0.0.1', resolve));
  const port = (portReservation.address() as { port: number }).port;
  await new Promise<void>(resolve => portReservation.close(() => resolve()));
  const preload = path.join(dir, 'preload.cjs');
  fs.writeFileSync(preload, `
    const wt = require('node:worker_threads'), http = require('node:http');
    const { syncBuiltinESMExports } = require('node:module');
    // A real native worker stuck before the ready RPC, with no carrier/network dependency.
    wt.Worker = class extends wt.Worker {
      constructor() { super('Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);', { eval: true }); }
    };
    const timeout = global.setTimeout;
    global.setTimeout = (fn, ms, ...args) => timeout(fn,
      ${mode === 'deadline' ? 'ms === 60000 ? 100 : ms' : mode === 'startup-error' ? 'ms === 5000 ? 100 : ms' : 'ms'}, ...args);
    ${mode === 'startup-error' ? `const create = http.createServer;
      http.createServer = (...args) => {
        const server = create(...args);
        server.once('listening', () => setImmediate(() => server.emit('error', new Error('synthetic_startup_failure'))));
        return server;
      };` : ''}
    syncBuiltinESMExports();
  `);
  const child = spawn(process.execPath, ['--require', preload, '--import', 'tsx', process.env.BROKER_SMS_TEST_MAIN || 'src/broker/main.ts'], {
    cwd: process.cwd(), env: {
      PATH: process.env.PATH, BROKER_ENV_FILE: '/nonexistent-sms-test-env',
      BROKER_CONTROL_TOKEN: 'c'.repeat(40), PHONE_SERVICE_SECRET: 's'.repeat(40),
      BROKER_PUBLIC_ORIGIN: 'https://phone.example.test', DEXTER_API_URL: 'http://127.0.0.1:1',
      TWILIO_PHONE_NUMBER: '+15555550124', TWILIO_ACCOUNT_SID: 'AC' + '1'.repeat(32),
      TWILIO_AUTH_TOKEN: 'synthetic-webhook-token', OPENAI_API_KEY: 'synthetic-not-a-key',
      BROKER_STATE_DIR: path.join(dir, 'calls'), BROKER_PORT: String(port),
      DEXTER_SMS_STATE_PATH: path.join(dir, 'sms.json'),
    }, stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '', errors = '';
  child.stdout.on('data', chunk => { output += chunk; });
  child.stderr.on('data', chunk => { errors += chunk; });
  const exited = once(child, 'exit');
  t.after(async () => {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    await exited;
    fs.rmSync(dir, { recursive: true, force: true });
  });
  for (let i = 0; i < 500 && !output.includes('dexter-broker'); i++) {
    if (child.exitCode !== null || child.signalCode !== null) throw new Error('broker failed: ' + errors);
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  assert.match(output, /dexter-broker/, errors);
  return { child, exited, origin: `http://127.0.0.1:${port}`, errors: () => errors };
}

test('voice listens and answers signed incoming HTTP while SMS startup is hung', async t => {
  const f = await launch(t, 'startup');
  const input = { AccountSid: 'AC' + '1'.repeat(32), CallSid: 'CA' + '9'.repeat(32),
    From: '+15555550123', To: '+15555550124', Direction: 'inbound', CallStatus: 'ringing' };
  const route = '/broker/incoming-call';
  const started = performance.now();
  const response = await fetch(f.origin + route, { method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded',
      'x-twilio-signature': Twilio.getExpectedTwilioSignature('synthetic-webhook-token', 'https://phone.example.test' + route, input) },
    body: new URLSearchParams(input), signal: AbortSignal.timeout(1500),
  });
  assert.equal(response.status, 200); assert.match(await response.text(), /<Response>/);
  assert.equal(f.child.exitCode, null);
  t.diagnostic(`Signed voice HTTP during hung SMS initialization: ${(performance.now() - started).toFixed(1)}ms`);
});

test('shutdown deadline SIGKILL exits despite a native worker stuck before ready', async t => {
  const f = await launch(t, 'deadline');
  f.child.kill('SIGTERM');
  const result = await Promise.race([f.exited, new Promise<never>((_, reject) => setTimeout(() => reject(new Error('shutdown_deadline_failed')), 3000))]);
  assert.deepEqual(result, [null, 'SIGKILL']);
});

test('startup error logs before bounded SMS cleanup and hard exits a stuck native worker', async t => {
  const f = await launch(t, 'startup-error');
  for (let i = 0; i < 100 && !f.errors().includes('synthetic_startup_failure'); i++) await new Promise(resolve => setTimeout(resolve, 1));
  assert.match(f.errors(), /synthetic_startup_failure/, 'diagnostic appears while cleanup is still pending');
  assert.equal(f.child.signalCode, null);
  const result = await Promise.race([f.exited, new Promise<never>((_, reject) => setTimeout(() => reject(new Error('startup_cleanup_unbounded')), 3000))]);
  assert.deepEqual(result, [null, 'SIGKILL']);
});

test('SMS cleanup has a five-second ceiling independent of worker progress', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const closing = assert.rejects(closeBrokerSms({ close: () => new Promise(() => {}) }), /sms_shutdown_timeout/);
  t.mock.timers.tick(5000);
  await closing;
});
