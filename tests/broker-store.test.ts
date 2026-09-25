import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { BrokerStore, type BrokerCall } from '../src/broker/store.js';
import type { PrimaryCall } from '../src/broker/primaryApi.js';

function fixture(t: test.TestContext) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-store-test-'));
  const store = new BrokerStore(directory);
  t.after(() => { store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  return { store, directory };
}
const input = () => ({ requestId: randomUUID(), direction: 'outbound' as const, purpose: 'Test the account connection', number: '+12025550123' });

function returnedBinding(callSid: string): PrimaryCall {
  return { bindingId: randomUUID(), callSid, state: 'open', context: {
    primaryDexterId: randomUUID(), threadId: randomUUID(), displayName: 'Dexter',
    messages: [{ id: randomUUID(), role: 'assistant', text: 'Your saved account answer.' }],
  } };
}

test('API binding result identifiers survive restart and cannot move to another binding or conversation', t => {
  const { store, directory } = fixture(t);
  const call = store.reserve({ ...input(), callSid: `CA${'1'.repeat(32)}` }).call;
  const response = returnedBinding(call.callSid!);
  const saved = store.recordPrimaryBinding(call.id, response);
  assert.equal(saved.primary?.primaryDexterId, response.context.primaryDexterId);
  assert.equal(saved.primary?.threadId, response.context.threadId);
  assert.equal(saved.primary?.context, 'assistant: Your saved account answer.');
  const file = path.join(directory, 'calls.json');
  const bytes = fs.readFileSync(file, 'utf8');
  store.close(); const resumed = new BrokerStore(directory); t.after(() => resumed.close());
  assert.deepEqual(resumed.get(call.id), saved);
  assert.equal(fs.readFileSync(file, 'utf8'), bytes);
  for (const changed of [
    { ...response, bindingId: randomUUID() },
    { ...response, context: { ...response.context, primaryDexterId: randomUUID() } },
    { ...response, context: { ...response.context, threadId: randomUUID() } },
  ]) assert.throws(() => resumed.recordPrimaryBinding(call.id, changed), /cannot change/);
  for (const primary of [undefined, { ...saved.primary!, primaryDexterId: undefined, threadId: undefined },
    { ...saved.primary!, threadId: randomUUID() }]) {
    assert.throws(() => resumed.update(call.id, { primary }), /identity requires|cannot change/);
  }
  assert.equal(fs.readFileSync(file, 'utf8'), bytes);
  const refreshed = resumed.recordPrimaryBinding(call.id, { ...response, context: {
    ...response.context, messages: [{ id: randomUUID(), role: 'assistant', text: 'A newer account answer.' }],
  } });
  assert.equal(refreshed.primary?.threadId, response.context.threadId);
  assert.equal(refreshed.primary?.context, 'assistant: A newer account answer.');
});

test('legacy binding reads preserve bytes and missing identifiers require an actual binding response', t => {
  const { store, directory } = fixture(t);
  const call = store.reserve({ ...input(), callSid: `CA${'2'.repeat(32)}` }).call;
  const response = returnedBinding(call.callSid!);
  const legacy = store.update(call.id, { primary: { bindingId: response.bindingId, name: 'Dexter', context: 'Retained history' } });
  store.close();
  const file = path.join(directory, 'calls.json');
  const bytes = JSON.stringify({ version: 1, calls: { [call.id]: legacy } }, null, 2) + '\n';
  fs.writeFileSync(file, bytes);
  const resumed = new BrokerStore(directory); t.after(() => resumed.close());
  assert.deepEqual(resumed.get(call.id), legacy);
  assert.equal(fs.readFileSync(file, 'utf8'), bytes);
  assert.throws(() => resumed.update(call.id, { primary: { ...legacy.primary!,
    primaryDexterId: response.context.primaryDexterId, threadId: response.context.threadId } }), /API binding response/);
  assert.throws(() => resumed.update(call.id, { primary: { ...legacy.primary!, bindingId: randomUUID() } }), /binding cannot change/);
  assert.equal(fs.readFileSync(file, 'utf8'), bytes);
  const enriched = resumed.recordPrimaryBinding(call.id, response);
  assert.equal(enriched.primary?.bindingId, legacy.primary?.bindingId);
  assert.equal(enriched.primary?.primaryDexterId, response.context.primaryDexterId);
  assert.equal(enriched.primary?.threadId, response.context.threadId);
});

test('wrong-call, closed, or incomplete API context cannot introduce result identifiers', t => {
  const { store, directory } = fixture(t);
  const call = store.reserve({ ...input(), callSid: `CA${'3'.repeat(32)}` }).call;
  const response = returnedBinding(call.callSid!);
  const bytes = fs.readFileSync(path.join(directory, 'calls.json'), 'utf8');
  for (const changed of [
    { ...response, callSid: `CA${'4'.repeat(32)}` }, { ...response, state: 'closed' },
    { ...response, context: { ...response.context, primaryDexterId: undefined } },
    { ...response, context: { ...response.context, threadId: 'not-a-thread' } },
  ]) assert.throws(() => store.recordPrimaryBinding(call.id, changed as PrimaryCall));
  assert.deepEqual(store.get(call.id), call);
  assert.equal(fs.readFileSync(path.join(directory, 'calls.json'), 'utf8'), bytes);
});

for (const incomplete of ['primaryDexterId', 'threadId'] as const) {
  test(`restart refuses a saved result identity missing ${incomplete} without rewriting it`, t => {
    const { store, directory } = fixture(t);
    const call = store.reserve({ ...input(), callSid: `CA${'5'.repeat(32)}` }).call;
    const saved = store.recordPrimaryBinding(call.id, returnedBinding(call.callSid!));
    store.close(); delete saved.primary![incomplete];
    const file = path.join(directory, 'calls.json');
    const bytes = JSON.stringify({ version: 1, calls: { [call.id]: saved } });
    fs.writeFileSync(file, bytes);
    assert.throws(() => new BrokerStore(directory), /Invalid Primary result identity/);
    assert.equal(fs.readFileSync(file, 'utf8'), bytes);
  });
}

test('call reservation and uncertain dispatch survive restart without redial', t => {
  const { store, directory } = fixture(t);
  const request = input();
  const first = store.reserve(request).call;
  store.update(first.id, { status: 'dialing' });
  store.update(first.id, { status: 'unknown' });
  store.close();
  const resumed = new BrokerStore(directory);
  t.after(() => resumed.close());
  const replay = resumed.reserve(request);
  assert.equal(replay.existing, true);
  assert.equal(replay.call.id, first.id);
  assert.equal(replay.call.status, 'unknown');
  assert.throws(() => resumed.reserve({ ...request, number: '+12025550124' }), /identity conflicts/);
});

test('provider status cannot regress and exact CallSid cannot move to another call', t => {
  const { store } = fixture(t);
  const a = store.reserve(input()).call;
  const b = store.reserve(input()).call;
  const callSid = 'CA' + '1'.repeat(32);
  store.update(a.id, { callSid, status: 'in-progress' });
  assert.equal(store.update(a.id, { status: 'ringing' }).status, 'in-progress');
  assert.throws(() => store.update(b.id, { callSid }), /already bound/);
  store.update(a.id, { status: 'completed' });
  assert.equal(store.update(a.id, { status: 'queued' }).status, 'completed');
});

test('a second writer cannot dispatch against the same call store', t => {
  const { directory } = fixture(t);
  assert.throws(() => new BrokerStore(directory), /Another broker process/);
});

test('an old transcript checkpoint cannot replace a newer revision', t => {
  const { store } = fixture(t);
  const call = store.reserve(input()).call;
  store.update(call.id, { transcriptRevision: 2, transcript: { text: 'corrected' } });
  assert.deepEqual(store.update(call.id, { transcriptRevision: 1, transcript: { text: 'old' } }).transcript, { text: 'corrected' });
});

for (const [field, value] of [
  ['id', 'another-call'], ['requestId', 'another-request'], ['number', '+12025550124'],
  ['direction', 'inbound'], ['purpose', 'Execute a different task'], ['createdAt', '2000-01-01T00:00:00.000Z'],
  ['purpose', undefined],
] as const) {
  test(`runtime patch cannot replace ${field} with ${String(value)}`, t => {
    const { store, directory } = fixture(t);
    const call = store.reserve(input()).call;
    const before = fs.readFileSync(path.join(directory, 'calls.json'), 'utf8');
    const patch = { [field]: value, status: 'completed', delegations: { injected: {
      requestId: 'injected', status: 'completed', updatedAt: call.createdAt,
    } } } as unknown as Parameters<BrokerStore['update']>[1];
    assert.throws(() => store.update(call.id, patch), /Call identity cannot change/);
    assert.deepEqual(store.get(call.id), call);
    assert.equal(fs.readFileSync(path.join(directory, 'calls.json'), 'utf8'), before);
  });
}

test('exact identity repeats preserve valid status and delegation writes through restart', t => {
  const { store, directory } = fixture(t);
  const request = input();
  const call = store.reserve(request).call;
  const { id, requestId, number, direction, purpose, createdAt } = call;
  const identity = { id, requestId, number, direction, purpose, createdAt };
  const delegation = { requestId: randomUUID(), status: 'running', updatedAt: createdAt };
  // Identity fields can occur in an untyped runtime object despite Omit.
  const patch = { ...identity, status: 'in-progress', delegations: { work: delegation } } as unknown as Parameters<BrokerStore['update']>[1];
  const changed = store.update(call.id, patch);
  const replay = store.update(call.id, patch);
  for (const [key, value] of Object.entries(identity)) assert.equal(replay[key as keyof BrokerCall], value);
  assert.equal(changed.status, 'in-progress');
  assert.equal(replay.status, 'in-progress');
  assert.deepEqual(replay.delegations, { work: delegation });
  store.close();
  const resumed = new BrokerStore(directory);
  t.after(() => resumed.close());
  assert.deepEqual(resumed.get(call.id), replay);
  assert.equal(resumed.reserve(request).call.id, call.id);
});

test('identity validation and persistence consume one own-property snapshot', t => {
  const { store } = fixture(t);
  const call = store.reserve(input()).call;
  let reads = 0;
  const patch = Object.defineProperty({ status: 'ringing' }, 'purpose', {
    enumerable: true, get: () => ++reads === 1 ? call.purpose : 'Changed after validation',
  }) as Parameters<BrokerStore['update']>[1];
  const result = store.update(call.id, patch);
  assert.equal(reads, 1);
  assert.equal(result.purpose, call.purpose);
  assert.equal(result.status, 'ringing');
});

for (const sid of [undefined, null, '', 'CA' + '2'.repeat(32)] as const) {
  test(`an assigned CallSid cannot be cleared or changed to ${String(sid)}`, t => {
    const { store, directory } = fixture(t);
    const originalSid = 'CA' + '1'.repeat(32);
    const call = store.reserve(input()).call;
    const bound = store.update(call.id, { callSid: originalSid, status: 'queued' });
    const before = fs.readFileSync(path.join(directory, 'calls.json'), 'utf8');
    const patch = { callSid: sid, status: 'completed' } as unknown as Parameters<BrokerStore['update']>[1];
    assert.throws(() => store.update(call.id, patch), /Call SID cannot change/);
    assert.deepEqual(store.get(call.id), bound);
    assert.deepEqual(store.bySid(originalSid), bound);
    if (typeof sid === 'string') assert.equal(store.bySid(sid), undefined);
    assert.equal(fs.readFileSync(path.join(directory, 'calls.json'), 'utf8'), before);
    store.close();
    const resumed = new BrokerStore(directory);
    t.after(() => resumed.close());
    assert.deepEqual(resumed.bySid(originalSid), bound);
  });
}

test('first valid CallSid assignment and its exact repeat retain normal updates and lookup', t => {
  const { store } = fixture(t);
  const call = store.reserve(input()).call;
  assert.equal(store.update(call.id, { callSid: undefined }).callSid, undefined);
  const callSid = 'CA' + 'A1'.repeat(16);
  store.update(call.id, { callSid, status: 'queued' });
  const repeated = store.update(call.id, { callSid, status: 'ringing' });
  assert.equal(repeated.callSid, callSid);
  assert.equal(repeated.status, 'ringing');
  assert.deepEqual(store.bySid(callSid), repeated);
});

for (const callSid of ['', 'not-a-provider-id', null, 1] as const) {
  test(`invalid first CallSid ${String(callSid)} cannot alter the call`, t => {
    const { store, directory } = fixture(t);
    const call = store.reserve(input()).call;
    const before = fs.readFileSync(path.join(directory, 'calls.json'), 'utf8');
    assert.throws(() => store.update(call.id, { callSid, status: 'ringing' } as unknown as Parameters<BrokerStore['update']>[1]), /Invalid call SID/);
    assert.deepEqual(store.get(call.id), call);
    assert.equal(fs.readFileSync(path.join(directory, 'calls.json'), 'utf8'), before);
  });
}

test('authenticated control assignment is fixed at reservation, bound to its call, and survives restart', t => {
  const { store, directory } = fixture(t);
  const request = { ...input(), purpose: 'Read my current portfolio.' };
  const call = store.reserve(request, 'authenticated_broker_control').call;
  const assignment = { origin: 'authenticated_broker_control', value: {
    assignmentId: request.requestId, brokerCallId: call.id, revision: 1, purpose: request.purpose,
  } };
  assert.deepEqual(call.callAssignment, assignment);
  assert.equal(store.reserve(request, 'authenticated_broker_control').call.id, call.id);
  for (const replacement of [undefined, { ...assignment, origin: 'caller' },
    { ...assignment, value: { ...assignment.value, purpose: 'Trade now' } },
    { ...assignment, value: { ...assignment.value, brokerCallId: randomUUID() } }]) {
    const before = fs.readFileSync(path.join(directory, 'calls.json'), 'utf8');
    assert.throws(() => store.update(call.id, { callAssignment: replacement, status: 'completed' } as never));
    assert.equal(fs.readFileSync(path.join(directory, 'calls.json'), 'utf8'), before);
  }
  assert.deepEqual(store.update(call.id, { callAssignment: assignment } as never).callAssignment, assignment);
  store.close();
  const resumed = new BrokerStore(directory); t.after(() => resumed.close());
  assert.deepEqual(resumed.get(call.id)?.callAssignment, assignment);
});

test('legacy and callback reservations never acquire a control assignment on replay or update', t => {
  const { store } = fixture(t);
  const request = input();
  const call = store.reserve(request).call;
  assert.equal(store.reserve(request, 'authenticated_broker_control').call.callAssignment, undefined);
  assert.throws(() => store.update(call.id, { callAssignment: { origin: 'authenticated_broker_control',
    value: { assignmentId: request.requestId, brokerCallId: call.id, revision: 1, purpose: request.purpose } } } as never));
  assert.throws(() => store.reserve({ ...input(), direction: 'inbound' }, 'authenticated_broker_control'));
  assert.throws(() => store.reserve({ ...input(), callAssignment: { origin: 'authenticated_broker_control', value: {} } } as never), /control reservation/);
});

test('assignment reference is required at first binding and immutable through restart, with mutable context', t => {
  const { store, directory } = fixture(t);
  const call = store.reserve(input(), 'authenticated_broker_control').call;
  store.update(call.id, { callSid: 'CA' + '1'.repeat(32) });
  const reference = { assignmentId: call.requestId, revision: 1 as const, digest: 'a'.repeat(64) };
  const primary = { bindingId: randomUUID(), name: 'Dexter', context: '', callAssignmentRef: reference };
  const before = fs.readFileSync(path.join(directory, 'calls.json'), 'utf8');
  assert.throws(() => store.update(call.id, { primary: { ...primary, callAssignmentRef: undefined } }));
  assert.throws(() => store.update(call.id, { primary: { ...primary, callAssignmentRef: { ...reference, assignmentId: randomUUID() } } }));
  assert.equal(fs.readFileSync(path.join(directory, 'calls.json'), 'utf8'), before);
  store.update(call.id, { primary });
  reference.digest = 'b'.repeat(64); // Caller mutation cannot edit the saved receipt.
  const bound = store.get(call.id)!;
  assert.equal(bound.primary?.callAssignmentRef?.digest, 'a'.repeat(64));
  for (const patch of [undefined, { ...bound.primary!, bindingId: randomUUID() },
    { ...bound.primary!, callAssignmentRef: undefined }, { ...bound.primary!, callAssignmentRef: reference }]) {
    assert.throws(() => store.update(call.id, { primary: patch }));
  }
  store.update(call.id, { primary: { ...bound.primary!, context: 'Later context' } });
  store.close(); const resumed = new BrokerStore(directory); t.after(() => resumed.close());
  assert.equal(resumed.get(call.id)?.primary?.context, 'Later context');
  assert.equal(resumed.get(call.id)?.primary?.callAssignmentRef?.digest, 'a'.repeat(64));
});

const previouslyAcceptedAssignments = [
  { name: 'uppercase request UUID', requestId: '12345678-ABCD-4ABC-8ABC-123456789ABC', purpose: 'Read my portfolio.' },
  { name: 'NUL in purpose', requestId: '12345678-abcd-4abc-8abc-123456789abc', purpose: 'Read\u0000my portfolio.' },
  { name: 'unpaired high surrogate in purpose', requestId: '12345678-abcd-4abc-8abc-123456789abc', purpose: 'Read \uD800 portfolio.' },
  { name: 'unpaired low surrogate in purpose', requestId: '12345678-abcd-4abc-8abc-123456789abc', purpose: 'Read \uDC00 portfolio.' },
];

for (const historical of previouslyAcceptedAssignments) {
  test(`new assigned reservation rejects ${historical.name} without changing saved calls`, t => {
    const { store, directory } = fixture(t);
    const existing = store.reserve(input(), 'authenticated_broker_control').call;
    const before = fs.readFileSync(path.join(directory, 'calls.json'), 'utf8');
    assert.throws(() => store.reserve({ ...input(), requestId: historical.requestId, purpose: historical.purpose },
      'authenticated_broker_control'), error => error instanceof Error && error.name === 'ZodError');
    assert.deepEqual(store.list(), [existing]);
    assert.equal(fs.readFileSync(path.join(directory, 'calls.json'), 'utf8'), before);
  });

  test(`retained assigned call with ${historical.name} reads and replays exactly through restart`, t => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-store-retained-assignment-'));
    const file = path.join(directory, 'calls.json');
    const id = 'ABCDEFAB-CDEF-4ABC-8ABC-ABCDEFABCDEF';
    const call: BrokerCall = {
      id, requestId: historical.requestId, direction: 'outbound', purpose: historical.purpose,
      number: '+12025550123', callSid: 'CA' + '1'.repeat(32), status: 'unknown',
      createdAt: '2026-09-22T10:00:00.000Z', updatedAt: '2026-09-22T10:00:01.000Z',
      delegations: {},
      callAssignment: { origin: 'authenticated_broker_control', value: {
        assignmentId: historical.requestId, brokerCallId: id, revision: 1, purpose: historical.purpose,
      } },
      primary: { bindingId: 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee', name: 'Dexter', context: 'Retained context',
        callAssignmentRef: { assignmentId: historical.requestId, revision: 1, digest: 'a'.repeat(64) } },
    };
    // This represents a journal written by the previous admission schema. New
    // reservation deliberately cannot create these records with the new parser.
    const original = JSON.stringify({ version: 1, calls: { [id]: call } }, null, 2) + '\n';
    fs.writeFileSync(file, original, { mode: 0o600 });
    let store: BrokerStore | undefined;
    t.after(() => { store?.close(); fs.rmSync(directory, { recursive: true, force: true }); });
    store = new BrokerStore(directory);
    const request = { requestId: call.requestId, direction: call.direction, purpose: call.purpose, number: call.number };

    for (let restart = 0; restart < 2; restart++) {
      assert.deepEqual(store.get(id), call);
      assert.deepEqual(store.bySid(call.callSid!), call);
      assert.deepEqual(store.list(), [call]);
      assert.deepEqual(store.reserve(request, 'authenticated_broker_control'), { call, existing: true });
      assert.deepEqual(store.reserve({ ...request, callSid: call.callSid }, 'authenticated_broker_control'), { call, existing: true });
      assert.equal(fs.readFileSync(file, 'utf8'), original);

      assert.throws(() => store!.reserve({ ...request, purpose: 'Read a different account.' },
        'authenticated_broker_control'), /identity conflicts/);
      for (const patch of [
        { id: id.toLowerCase() }, { requestId: randomUUID() }, { purpose: 'Read a different account.' },
        { callAssignment: { ...call.callAssignment!, value: { ...call.callAssignment!.value, brokerCallId: id.toLowerCase() } } },
        { primary: { ...call.primary!, callAssignmentRef: { ...call.primary!.callAssignmentRef!, digest: 'b'.repeat(64) } } },
      ]) assert.throws(() => store!.update(id, patch as never), /cannot change/);
      assert.deepEqual(store.get(id), call);
      assert.equal(fs.readFileSync(file, 'utf8'), original);
      if (restart === 0) { store.close(); store = new BrokerStore(directory); }
    }
  });
}
