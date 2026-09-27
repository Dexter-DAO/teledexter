import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { randomUUID } from 'node:crypto';
import { createInboundAttemptTiming, observeInboundStreamResponse, type InboundTimingEvent } from '../src/broker/inboundTiming.js';
import { PrimaryApiError } from '../src/broker/primaryApi.js';

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
