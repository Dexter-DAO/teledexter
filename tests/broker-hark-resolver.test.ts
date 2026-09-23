import assert from 'node:assert/strict';
import { createServer, type ServerResponse } from 'node:http';
import test from 'node:test';
import { createHarkAssignmentResolver, HarkAssignmentError } from '../src/broker/harkResolver.js';
import { harkSourceDigest } from '../src/broker/harkAssignment.js';
import { makeResolvedHarkAssignment } from './fixtures/broker-hark.js';

function fixtureValue() {
  const value = makeResolvedHarkAssignment();
  value.callAssignment.hark.expiresAt = '2099-09-23T20:00:00.000Z';
  const { sourceDigest: _old, ...source } = value.callAssignment.hark;
  value.callAssignment.hark.sourceDigest = harkSourceDigest(source);
  const saved = value.callAssignment.hark;
  const notification = { idempotencyKey: `hark:${saved.deliveryId}`, eventId: `hark:${saved.deliveryId}`,
    to: value.to, subject: 'Untrusted transport headline', briefing: 'Untrusted caller-supplied briefing',
    severity: 'info', observedAt: '2026-09-23T19:30:00.000Z', ttlSeconds: 180, followupContext: {},
    hark: { schemaVersion: 1, caseId: saved.caseId, watchId: saved.watch.id, eventId: saved.event.id,
      deliveryId: saved.deliveryId, originTaskId: saved.originTaskId } };
  return { value, notification };
}

async function api(t: test.TestContext, handler: (response: ServerResponse) => void) {
  const requests: { url: string | undefined; token: string | string[] | undefined; body: unknown; method: string | undefined }[] = [];
  const server = createServer(async (request, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    const text = Buffer.concat(chunks).toString('utf8');
    requests.push({ url: request.url, token: request.headers['x-internal-auth'], body: text ? JSON.parse(text) : undefined,
      method: request.method });
    handler(response);
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => { server.closeAllConnections(); server.close(); });
  const base = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
  return { base, requests };
}

const error = (status: number, code?: string) => (value: unknown) => value instanceof HarkAssignmentError
  && value.status === status && (code === undefined || value.code === code);

test('resolver reflects the exact Hark request to local API and returns its saved assignment unchanged', async t => {
  const { value, notification } = fixtureValue();
  const f = await api(t, response => { response.writeHead(200, { 'content-type': 'application/json' }); response.end(JSON.stringify(value)); });
  const resolve = createHarkAssignmentResolver({ apiBaseUrl: f.base });
  assert.deepEqual(await resolve(notification, 'private-fixture-token'), value);
  assert.deepEqual(f.requests, [{ url: '/internal/phone/resolve-hark-assignment', method: 'POST',
    token: 'private-fixture-token', body: { notification } }]);
  assert.notEqual(value.callAssignment.hark.event.summary, notification.briefing);
});

test('invalid notification and absent token fail before resolver HTTP', async t => {
  const { notification } = fixtureValue();
  const f = await api(t, response => { response.statusCode = 500; response.end(); });
  const resolve = createHarkAssignmentResolver({ apiBaseUrl: f.base });
  const { hark: _hark, ...ordinary } = notification;
  await assert.rejects(resolve(ordinary, 'fixture-token'), error(400, 'invalid_hark_notification'));
  await assert.rejects(resolve({ ...notification, idempotencyKey: 'other-key' }, 'fixture-token'), error(400));
  await assert.rejects(resolve(notification, ''), error(401));
  await assert.rejects(resolve(notification, 'x'.repeat(4097)), error(401));
  assert.equal(f.requests.length, 0);
});

test('only local HTTP origins without credentials, paths, query or fragment qualify', () => {
  for (const base of ['https://127.0.0.1', 'http://api.example.test', 'http://secret@localhost',
    'http://localhost/prefix', 'http://localhost?token=secret', 'http://localhost#fragment'])
    assert.throws(() => createHarkAssignmentResolver({ apiBaseUrl: base }), /local API origin/);
  for (const base of ['http://localhost:3030', 'http://127.0.0.1:3030/', 'http://[::1]:3030'])
    assert.equal(typeof createHarkAssignmentResolver({ apiBaseUrl: base }), 'function');
});

test('response source identities must exactly match the reflected notification', async t => {
  for (const field of ['to', 'delivery', 'case', 'watch', 'event', 'task'] as const) await t.test(field, async child => {
    const { value, notification } = fixtureValue();
    const id = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
    if (field === 'to') value.to = '+12025550124';
    if (field === 'delivery') {
      value.callAssignment.assignmentId = id; value.callAssignment.brokerCallId = id;
      value.callAssignment.hark.deliveryId = id;
    }
    if (field === 'case') value.callAssignment.hark.caseId = id;
    if (field === 'watch') value.callAssignment.hark.watch.id = id;
    if (field === 'event') value.callAssignment.hark.event.id = id;
    if (field === 'task') value.callAssignment.hark.originTaskId = 'other-task';
    const { sourceDigest: _old, ...source } = value.callAssignment.hark;
    value.callAssignment.hark.sourceDigest = harkSourceDigest(source);
    const f = await api(child, response => { response.end(JSON.stringify(value)); });
    await assert.rejects(createHarkAssignmentResolver({ apiBaseUrl: f.base })(notification, 'fixture-token'),
      error(503, 'hark_assignment_identity_mismatch'));
    assert.equal(f.requests.length, 1);
  });
});

test('resolver preserves refusal classes and does not follow redirects or retry', async t => {
  const { notification } = fixtureValue();
  for (const status of [400, 401, 403, 409, 500, 503, 302]) await t.test(String(status), async child => {
    const f = await api(child, response => {
      response.writeHead(status, { location: '/redirect-target' }); response.end('{"error":"private-fixture-detail"}');
    });
    await assert.rejects(createHarkAssignmentResolver({ apiBaseUrl: f.base })(notification, 'fixture-token'),
      error([400, 401, 403, 409].includes(status) ? status : 503));
    assert.equal(f.requests.length, 1);
    assert.equal(f.requests[0].url, '/internal/phone/resolve-hark-assignment');
  });
});

test('successful transport still requires exact 200 and strict valid snapshot JSON', async t => {
  const { value, notification } = fixtureValue();
  const candidates: { status: number; body: unknown }[] = [
    { status: 201, body: value }, { status: 204, body: undefined },
    { status: 200, body: { ...value, extra: true } },
    { status: 200, body: { ...value, callAssignment: { ...value.callAssignment,
      hark: { ...value.callAssignment.hark, sourceDigest: '0'.repeat(64) } } } },
    { status: 200, body: 'broken-json' }, { status: 200, body: Buffer.from([0xff]) },
  ];
  for (const [index, candidate] of candidates.entries()) await t.test(String(index), async child => {
    const f = await api(child, response => {
      response.statusCode = candidate.status;
      response.end(Buffer.isBuffer(candidate.body) || typeof candidate.body === 'string' ? candidate.body : JSON.stringify(candidate.body));
    });
    await assert.rejects(createHarkAssignmentResolver({ apiBaseUrl: f.base })(notification, 'fixture-token'), error(503));
    assert.equal(f.requests.length, 1);
  });
});

test('advertised and streamed oversized resolver responses are refused', async t => {
  const { notification } = fixtureValue();
  for (const advertised of [true, false]) await t.test(String(advertised), async child => {
    const f = await api(child, response => {
      if (advertised) response.setHeader('content-length', 65_537);
      else response.setHeader('transfer-encoding', 'chunked');
      response.write('x'.repeat(32_768)); response.end('x'.repeat(32_769));
    });
    await assert.rejects(createHarkAssignmentResolver({ apiBaseUrl: f.base })(notification, 'fixture-token'),
      error(503, 'invalid_hark_assignment'));
    assert.equal(f.requests.length, 1);
  });
});

test('resolver timeout aborts an unanswered local request without retry', async t => {
  const { notification } = fixtureValue();
  const f = await api(t, () => {});
  await assert.rejects(createHarkAssignmentResolver({ apiBaseUrl: f.base, timeoutMs: 50 })(notification, 'fixture-token'),
    error(503, 'hark_assignment_unavailable'));
  assert.equal(f.requests.length, 1);
});
