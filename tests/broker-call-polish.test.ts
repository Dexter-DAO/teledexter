import test from 'node:test';
import assert from 'node:assert/strict';
import { isConversationalAcknowledgement, resultAlreadyPresented, type BrokerDelegationRecord } from '../src/broker/backend.js';
import { createInboundHoldAdapter } from '../src/broker/inboundHold.js';
import { brokerInstructions } from '../src/broker/instructions.js';
import type { BrokerCall } from '../src/broker/store.js';

test('pure acknowledgements and waiting statements are conversational (call 2d141fec)', () => {
  for (const text of [" I'm good. I'll just wait till the buy to go through", 'Thanks.', 'Thank you', 'Ah, excellent',
    'Okay, I will wait for it', 'Cool. Take your time.', 'No worries', 'got it, thanks'])
    assert.equal(isConversationalAcknowledgement(text), true, text);
  for (const text of ['Okay', 'Cool', 'Sure', 'Yeah', ' Aight, Let\'s buy uh, $1 worth of Apple',
    " Cool, I'll sell a dollar worth of Apple now", 'Wait for the price to drop then buy', 'Thanks, now check my portfolio',
    'wait until it is done then sell', ''])
    assert.equal(isConversationalAcknowledgement(text), false, text);
});

const requestId = '26d933b5-5171-5d50-a4b0-f19ffd994347', messageId = '84b3b60e-eed4-4ad9-9142-b4154b56c4be';
const entry = { requestId, status: 'completed', primaryResult: { state: 'completed', assistant: { messageId } } } as unknown as BrokerDelegationRecord;
const record = (status: string, eventId: string, chunkIndex = 0, chunkCount = 1, attempt = 1) => ({ requestId, kind: 'result', sessionId: 's1',
  updateId: `${requestId}_${messageId}_caller_35_attempt_${attempt}_${chunkIndex}`,
  ordinaryDelivery: { chunkIndex, chunkCount }, outcome: { eventId, status, callerRevision: 37, transcriptSequence: 129 } });
const callWith = (records: unknown[], rejections: unknown[] = []) => ({ publications: { records, rejections } }) as unknown as BrokerCall;

test('a result appended to the provider counts as presented even when journaled stale', () => {
  assert.equal(resultAlreadyPresented(callWith([record('stale', 'broker_7')]), entry), true);
  assert.equal(resultAlreadyPresented(callWith([record('accepted', 'broker_7')]), entry), true);
  // No provider event: the append never happened.
  assert.equal(resultAlreadyPresented(callWith([record('stale', '')]), entry), false);
  // A late provider rejection means it was not spoken.
  assert.equal(resultAlreadyPresented(callWith([record('stale', 'broker_7')], [{ sessionId: 's1', eventId: 'broker_7' }]), entry), false);
  // Every chunk of one attempt must have landed.
  assert.equal(resultAlreadyPresented(callWith([record('accepted', 'broker_7', 0, 2)]), entry), false);
  assert.equal(resultAlreadyPresented(callWith([record('accepted', 'broker_7', 0, 2), record('stale', 'broker_8', 1, 2)]), entry), true);
  // A different result message for the request is not this result.
  assert.equal(resultAlreadyPresented(callWith([record('accepted', 'broker_7')]),
    { ...entry, primaryResult: { state: 'completed', assistant: { messageId: 'other' } } } as unknown as BrokerDelegationRecord), false);
  assert.equal(resultAlreadyPresented(callWith([record('accepted', 'broker_7')]), { ...entry, status: 'failed' } as BrokerDelegationRecord), false);
});

test('inbound hold answers silently instead of saying "One moment." when a greeting clip opens the call', () => {
  const origin = 'https://broker.example';
  const callId = '2d141fec-4a9d-4967-82f0-815492d7b334';
  const call = { id: callId, direction: 'inbound', inbound: { begin: { providerProof: { url: `${origin}/broker/incoming-call` } } } };
  const store = { get: () => call } as never;
  const input = { callId, continuationUrl: `${origin}/broker/inbound/${callId}/continue` };
  const withClip = createInboundHoldAdapter({ publicOrigin: origin, store, greetingClipAvailable: true }).waitingResponse(input);
  assert.doesNotMatch(withClip, /<Say>/); assert.doesNotMatch(withClip, /<Pause/);
  // The first verb must answer the call: a silent play, never a bare pause or redirect.
  assert.match(withClip, /^<\?xml[^>]*><Response><Play digits="w"\/><Redirect method="POST">/);
  assert.match(createInboundHoldAdapter({ publicOrigin: origin, store }).waitingResponse(input), /<Say>One moment\.<\/Say><Redirect/);
  (call.inbound as Record<string, unknown>).continuation = {};
  assert.match(createInboundHoldAdapter({ publicOrigin: origin, store, greetingClipAvailable: true }).waitingResponse(input), /<Pause length="1"\/><Redirect/);
});

test('voice instructions forbid delegating acknowledgements and volunteering recaps', () => {
  const text = brokerInstructions({ direction: 'inbound', purpose: 'inbound' });
  assert.match(text, /waiting statements such as "I'll wait for the buy to go through"; never delegate them/);
  assert.match(text, /Never volunteer a recap, repeat, or summary of a result you already gave on this call/);
});
