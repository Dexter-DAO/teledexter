import assert from 'node:assert/strict';
import test from 'node:test';
import { createPrimaryBackend, type PrimaryBackendOptions } from '../src/broker/backend.js';
import type { BrokerCall } from '../src/broker/store.js';
import type { LiveBackendChannel, LiveDelegationRequest, LiveTranscriptSnapshot } from '../src/live/types.js';
import { makeResolvedHarkAssignment } from './fixtures/broker-hark.js';

const bindingId = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
const otherId = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';
const sourceTime = '2026-09-24T12:00:00.000Z';

function savedCall(): BrokerCall {
  const assignment = makeResolvedHarkAssignment().callAssignment;
  return { id: assignment.brokerCallId, requestId: assignment.assignmentId,
    direction: 'outbound', purpose: assignment.purpose, number: '+12025550123', status: 'in-progress',
    createdAt: sourceTime, updatedAt: sourceTime, mediaStartedAt: sourceTime,
    callSid: 'CA' + '2'.repeat(32), liveSessionId: 'hark-source', delegations: {},
    callAssignment: { origin: 'authenticated_hark_delivery', value: assignment },
    primary: { bindingId, name: null, context: '', callAssignmentRef: {
      assignmentId: assignment.assignmentId, revision: 1, digest: 'a'.repeat(64),
    } },
  };
}

const invalidCalls: Array<{ name: string; change(call: BrokerCall): unknown }> = [
  { name: 'unknown assignment origin', change: call => ({ ...call,
    callAssignment: { ...call.callAssignment, origin: 'unverified_notification' } }) },
  { name: 'Hark snapshot relabelled as operator input', change: call => ({ ...call,
    callAssignment: { ...call.callAssignment, origin: 'authenticated_broker_control' } }) },
  { name: 'Hark assignment missing its snapshot', change: call => ({ ...call,
    callAssignment: { origin: 'authenticated_hark_delivery', value: {
      assignmentId: call.requestId, brokerCallId: call.id, revision: 1, purpose: call.purpose,
    } } }) },
  { name: 'changed Hark snapshot with old digest', change: call => {
    const assignment = makeResolvedHarkAssignment().callAssignment;
    return { ...call, callAssignment: { origin: 'authenticated_hark_delivery', value: { ...assignment,
      hark: { ...assignment.hark, event: { ...assignment.hark.event, summary: 'Changed event' } },
    } } };
  } },
  { name: 'saved request identity mismatch', change: call => ({ ...call, requestId: otherId }) },
  { name: 'saved call identity mismatch', change: call => ({ ...call, id: otherId }) },
  { name: 'saved purpose mismatch', change: call => ({ ...call, purpose: 'Different purpose' }) },
  { name: 'assignment revision changed', change: call => ({ ...call,
    callAssignment: { ...call.callAssignment, value: { ...call.callAssignment!.value, revision: 2 } } }) },
  { name: 'missing assignment reference', change: call => ({ ...call,
    primary: { ...call.primary, callAssignmentRef: undefined } }) },
  { name: 'reference without saved assignment', change: call => ({ ...call, callAssignment: undefined }) },
  { name: 'reference assignment identity mismatch', change: call => ({ ...call,
    primary: { ...call.primary, callAssignmentRef: { ...call.primary!.callAssignmentRef, assignmentId: otherId } } }) },
  { name: 'reference revision changed', change: call => ({ ...call,
    primary: { ...call.primary, callAssignmentRef: { ...call.primary!.callAssignmentRef, revision: 2 } } }) },
  { name: 'malformed reference digest', change: call => ({ ...call,
    primary: { ...call.primary, callAssignmentRef: { ...call.primary!.callAssignmentRef, digest: 'invalid' } } }) },
];

for (const invalid of invalidCalls) test(`Hark caller admission refuses ${invalid.name} before submit`, async () => {
  // Deliberately bypass BrokerStore's earlier validation to exercise the backend boundary itself.
  let saved = invalid.change(savedCall()) as BrokerCall;
  const originalAssignment = structuredClone(saved.callAssignment);
  let submitted = 0;
  let observed = 0;
  const api: PrimaryBackendOptions['api'] = {
    async submit() { submitted++; throw new Error('unexpected_submit'); },
    async result() { observed++; throw new Error('unexpected_result'); },
    async captureTurn() { throw new Error('unexpected_capture'); },
    async question() { throw new Error('unexpected_question'); },
    async reply() { throw new Error('unexpected_reply'); },
  };
  const snapshot: LiveTranscriptSnapshot = { revision: 1, callerRevision: 1, fragments: [
    { eventId: 'caller-event', role: 'user', text: ' Please check the watched price.\t', startMs: 1, endMs: 20, sequence: 1 },
  ] };
  const channel: LiveBackendChannel = {
    signal: new AbortController().signal, getContext: () => structuredClone(snapshot), subscribeContext: () => () => {},
    async publish() { throw new Error('unexpected_publication'); },
  };
  const backend = createPrimaryBackend({ callId: saved.id, callSid: saved.callSid!, bindingId, api,
    stabilityMs: 1, maxWaitMs: 100,
    store: { get: () => structuredClone(saved), update: (_id, patch) => {
      saved = { ...saved, ...structuredClone(patch) }; return structuredClone(saved);
    } },
  });
  backend.noteCallerSpeech(Date.now(), 20);
  const request: LiveDelegationRequest = { callId: saved.id, sessionId: saved.liveSessionId!,
    delegationId: 'explicit-hark-delegation', requestId: 'live-explicit-hark-delegation', offsetMs: 20, transcript: snapshot };
  await assert.rejects(backend.delegate(request, channel), (error: unknown) => error instanceof Error
    && (error.message === 'call_assignment_binding_mismatch' || error.name === 'ZodError'));
  assert.equal(submitted, 0);
  assert.equal(observed, 0);
  assert.deepEqual(saved.delegations, {});
  assert.deepEqual(saved.callAssignment, originalAssignment, 'validation must not rewrite assignment origin or value');
});
