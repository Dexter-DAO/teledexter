import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { randomUUID } from 'node:crypto';
import { createInboundAttemptTiming, observeInboundStreamResponse, observeInboundHoldResponse, type InboundTimingEvent } from '../src/broker/inboundTiming.js';
import { PrimaryApiError } from '../src/broker/primaryApi.js';
import { safeFailure } from '../src/broker/phone.js';

test('Phone Tab rejection retains its API reason in the call failure and correlated timing event', async () => {
  const events: InboundTimingEvent[] = [], callId = randomUUID();
  const timing = createInboundAttemptTiming(callId, event => events.push(event));
  const error = new PrimaryApiError('http_error', 403, { code: 'phone_tab_required', retryable: false });
  await assert.rejects(timing.measure('open_context', async () => { throw error; }), received => received === error);
  assert.equal(events[1].callId, callId); assert.equal(events[1].stage, 'open_context');
  assert.equal(events[1].primaryApiCode, 'http_error'); assert.equal(events[1].primaryApiStatus, 403);
  assert.equal(events[1].primaryApiRemoteCode, 'phone_tab_required'); assert.equal(events[1].primaryApiRetryable, false);
  assert.deepEqual(safeFailure(error), { code: 'http_error', message: error.message,
    status: 403, remoteCode: 'phone_tab_required', retryable: false });
  assert.equal(safeFailure(new PrimaryApiError('http_error', 403, { code: 'https://provider.test/?token=secret', retryable: false })).remoteCode, undefined);
  assert.equal(safeFailure(Object.assign(new Error('unknown failure'), { remoteCode: 'secret', retryable: false })).remoteCode, undefined);
});

test('a failed stage retains elapsed time across wall-clock movement and never logs the error body', async () => {
  let wall = Date.now(), monotonic = 10;
  const events: InboundTimingEvent[] = [];
  const timing = createInboundAttemptTiming(randomUUID(), event => events.push(event), { wall: () => wall, monotonic: () => monotonic });
  const error = new PrimaryApiError('http_error', 503, { code: 'secret-provider-token', retryable: true });
  await assert.rejects(timing.measure('open_context', async () => {
    wall -= 20000; monotonic += 1234.5; throw error;
  }), received => received === error);
  assert.equal(events.length, 2);
  assert.equal(events[1].elapsedMs, 1234.5);
  assert.equal(events[1].primaryApiStatus, 503);
  assert.equal(events[1].phase, 'failed');
  assert.equal(JSON.stringify(events).includes('secret-provider-token'), false);
});

test('unavailable diagnostic output preserves successful and failed admission results', async () => {
  const timing = createInboundAttemptTiming(randomUUID(), () => { throw new Error('log unavailable'); });
  const result = { bindingId: randomUUID() }, error = new Error('original failure');
  assert.equal(await timing.measure('open_context', async () => result), result);
  await assert.rejects(timing.measure('recover_binding', async () => { throw error; }), value => value === error);
});

test('stream timing records actual HTTP finish once without claiming media or playback', async t => {
  const events: InboundTimingEvent[] = [], callId = randomUUID();
  const server = createServer((_request, response) => {
    observeInboundStreamResponse(callId, response, event => events.push(event));
    response.end('<Response/>');
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())));
  const port = (server.address() as { port: number }).port;
  assert.equal(await (await fetch(`http://127.0.0.1:${port}`)).text(), '<Response/>');
  assert.deepEqual(events.map(event => event.phase), ['started', 'completed']);
  assert.ok(events.every(event => event.stage === 'stream_response' && event.callId === callId));
});

test('stream timing distinguishes a dropped HTTP response from a completed stream response', async t => {
  const events: InboundTimingEvent[] = [];
  const server = createServer((_request, response) => {
    observeInboundStreamResponse(randomUUID(), response, event => events.push(event));
    response.destroy();
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())));
  await assert.rejects(fetch(`http://127.0.0.1:${(server.address() as { port: number }).port}`));
  assert.deepEqual(events.map(event => event.phase), ['started', 'aborted']);
});

for (const abort of [false, true]) test(`hold response ${abort ? 'abort' : 'finish'} is timed without starting admission`, async t => {
  const events: InboundTimingEvent[] = [];
  const server = createServer((_request, response) => {
    observeInboundHoldResponse(randomUUID(), response, event => events.push(event));
    if (abort) response.destroy(); else response.end('<Response><Redirect/></Response>');
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())));
  const request = fetch(`http://127.0.0.1:${(server.address() as { port: number }).port}`);
  if (abort) await assert.rejects(request); else await (await request).text();
  assert.deepEqual(events.map(event => [event.stage, event.phase]), [['hold_response', 'started'], ['hold_response', abort ? 'aborted' : 'completed']]);
});
