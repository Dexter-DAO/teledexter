import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import Fastify from 'fastify';
import { registerCallLogRoutes } from '../src/admin/registerCallLogRoutes.js';

const ADMIN_SECRET = 'admin-secret-that-is-longer-than-thirty-two-bytes';

test('public-style and proxy-local requests cannot read call logs without the secret', async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'dexter-call-log-test-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const callLogPath = path.join(directory, 'call-log.json');
  fs.writeFileSync(callLogPath, JSON.stringify([{
    streamSid: 'MZ-secret',
    callerNumber: '+15615550100',
    stirVerstat: 'TN-Validation-Passed-A',
    startedAt: '2026-07-27T07:00:00.000Z',
    transcriptSegments: [{ role: 'user', text: 'private transcript', timestamp: 'now' }],
    toolCalls: [],
  }]));

  const app = Fastify({ trustProxy: true });
  registerCallLogRoutes(app, { callLogPath, adminSecret: ADMIN_SECRET });
  t.after(() => app.close());

  const publicRequest = await app.inject({
    method: 'GET',
    url: '/calls',
    headers: { 'x-forwarded-for': '203.0.113.10' },
  });
  assert.equal(publicRequest.statusCode, 404);
  assert.equal(publicRequest.body.includes('private transcript'), false);

  const nginxLocalRequest = await app.inject({
    method: 'GET',
    url: '/calls/0',
    remoteAddress: '127.0.0.1',
  });
  assert.equal(nginxLocalRequest.statusCode, 404);
  assert.equal(nginxLocalRequest.body.includes('private transcript'), false);

  const wrongSecret = await app.inject({
    method: 'GET',
    url: '/calls/0',
    headers: { 'x-dexter-phone-admin-secret': 'wrong' },
  });
  assert.equal(wrongSecret.statusCode, 404);

  const authorized = await app.inject({
    method: 'GET',
    url: '/calls/0',
    headers: { 'x-dexter-phone-admin-secret': ADMIN_SECRET },
  });
  assert.equal(authorized.statusCode, 200);
  assert.equal(authorized.json().call.transcriptSegments[0].text, 'private transcript');
});

test('call-log routes fail closed when no admin secret is configured', async (t) => {
  const app = Fastify();
  registerCallLogRoutes(app, { callLogPath: '/does/not/matter', adminSecret: '' });
  t.after(() => app.close());

  const response = await app.inject({ method: 'GET', url: '/calls' });
  assert.equal(response.statusCode, 404);
});
