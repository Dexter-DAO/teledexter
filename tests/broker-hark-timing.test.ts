import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { harkProviderTiming } from '../src/broker/harkTiming.js';
import { BrokerStore } from '../src/broker/store.js';
import { makeResolvedHarkAssignment } from './fixtures/broker-hark.js';

const providerStart = '2026-09-23T19:31:00.000Z';
const providerEnd = '2026-09-23T19:32:05.000Z';
const completed = () => ({ status: 'completed', startTime: new Date(providerStart),
  endTime: new Date(providerEnd), duration: '65' });
const exact = { startedAt: providerStart, endedAt: providerEnd, durationSeconds: 65 };

test('completed SDK Date fields yield exact provider timing without consulting host time', t => {
  t.mock.method(Date, 'now', () => { throw new Error('Host clock must not determine provider duration'); });
  assert.deepEqual(harkProviderTiming(completed()), exact);
  const delayed = { ...completed(), observedAt: '2026-09-24T19:32:05.000Z', dateUpdated: new Date('2026-09-24T19:32:05.000Z') };
  assert.deepEqual(harkProviderTiming(delayed), exact);
});

test('provider duration shorter than dial-to-end interval is retained rather than replaced', () => {
  assert.deepEqual(harkProviderTiming({ ...completed(), duration: '40' }), { ...exact, durationSeconds: 40 });
  assert.deepEqual(harkProviderTiming({ ...completed(), duration: '00040' }), { ...exact, durationSeconds: 40 });
  assert.deepEqual(harkProviderTiming({ ...completed(), duration: '0' }), { ...exact, durationSeconds: 0 });
});

test('seconds consistency allows the interval ceiling but rejects a longer supplied duration', () => {
  const fractional = { ...completed(), startTime: new Date('2026-09-23T19:31:00.250Z'),
    endTime: new Date('2026-09-23T19:31:00.750Z'), duration: '1' };
  assert.deepEqual(harkProviderTiming(fractional), { startedAt: fractional.startTime.toISOString(),
    endedAt: fractional.endTime.toISOString(), durationSeconds: 1 });
  assert.equal(harkProviderTiming({ ...fractional, duration: '2' }), undefined);
  assert.equal(harkProviderTiming({ ...completed(), duration: '66' }), undefined);
  assert.deepEqual(harkProviderTiming({ ...completed(), endTime: new Date(providerStart), duration: '0' }),
    { startedAt: providerStart, endedAt: providerStart, durationSeconds: 0 });
});

for (const status of ['queued', 'ringing', 'in-progress', 'busy', 'failed', 'no-answer', 'canceled', undefined]) {
  test(`${String(status)} never manufactures ended timing from empty or even populated fields`, () => {
    assert.equal(harkProviderTiming({ ...completed(), status }), undefined);
    assert.equal(harkProviderTiming({ status, startTime: null, endTime: null, duration: '' }), undefined);
  });
}

test('partial and invalid SDK fields refuse without substituting host timestamps or zero duration', () => {
  const invalid: unknown[] = [null, undefined, false, [], 'completed', {},
    { ...completed(), startTime: undefined }, { ...completed(), endTime: null },
    { ...completed(), startTime: providerStart }, { ...completed(), endTime: providerEnd },
    { ...completed(), startTime: Date.parse(providerStart) },
    { ...completed(), startTime: new Date(NaN) }, { ...completed(), endTime: new Date(NaN) },
    { ...completed(), endTime: new Date('2026-09-23T19:30:59.000Z') },
    { ...completed(), startTime: { getTime: () => Date.parse(providerStart) } },
    { ...completed(), startTime: new Date('-000001-01-01T00:00:00.000Z') },
    ...[undefined, null, '', ' ', ' 65', '65 ', '-1', '+65', '1.5', '1e1', '0x10', 'NaN', 'Infinity',
      '9007199254740992', '9'.repeat(400), 65, 0, -1, NaN].map(duration => ({ ...completed(), duration })),
  ];
  for (const candidate of invalid) assert.equal(harkProviderTiming(candidate), undefined);
  assert.equal(harkProviderTiming(Object.defineProperty({}, 'status', { get() { throw new Error('unreadable'); } })), undefined);
});

test('date values are detached from later mutation and structurally valid future dates are not host-clock policy', () => {
  const input = completed();
  const result = harkProviderTiming(input);
  input.startTime.setUTCFullYear(2050); input.endTime.setUTCFullYear(2050); input.duration = '0';
  assert.deepEqual(result, exact);
  assert.deepEqual(harkProviderTiming({ status: 'completed', startTime: new Date('2099-01-01T00:00:00.000Z'),
    endTime: new Date('2099-01-01T00:00:10.000Z'), duration: '10' }),
  { startedAt: '2099-01-01T00:00:00.000Z', endedAt: '2099-01-01T00:00:10.000Z', durationSeconds: 10 });
});

test('delayed recovery freezes provider timing in the actual journal and exact retry survives restart', t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-hark-provider-timing-'));
  let store = new BrokerStore(directory);
  t.after(() => { store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  const call = store.reserveHark(makeResolvedHarkAssignment()).call;
  store.update(call.id, { callSid: `CA${'1'.repeat(32)}` });
  store.observeHarkProvider(call.id, 'in-progress', '2026-09-23T19:31:02.000Z');
  // The first terminal callback has no timing. A later provider read supplies it.
  store.observeHarkProvider(call.id, 'completed', '2026-09-23T19:32:06.000Z');
  const observedAt = '2026-09-24T19:32:05.000Z';
  const saved = store.observeHarkProvider(call.id, 'completed', observedAt, harkProviderTiming(completed()));
  const end = saved.harkJournal!.events[`voice-ended:${call.id}`]!;
  assert.equal(end.kind, 'call_ended');
  if (end.kind !== 'call_ended') throw new Error('Expected ended body');
  assert.equal(end.body.started_at, providerStart);
  assert.equal(end.body.ended_at, providerEnd);
  assert.equal(end.body.duration_seconds, 65);
  assert.equal(saved.harkJournal!.ended!.observedAt, '2026-09-23T19:32:06.000Z');
  const bytes = JSON.stringify(end);
  const journalBytes = fs.readFileSync(path.join(directory, 'calls.json'), 'utf8');
  store.close(); store = new BrokerStore(directory);
  assert.equal(fs.readFileSync(path.join(directory, 'calls.json'), 'utf8'), journalBytes);
  assert.equal(JSON.stringify(store.get(call.id)!.harkJournal!.events[end.id]), bytes);
  const replay = store.observeHarkProvider(call.id, 'completed', '2026-10-01T00:00:00.000Z', harkProviderTiming(completed()));
  assert.equal(JSON.stringify(replay.harkJournal!.events[end.id]), bytes);
  assert.equal(replay.harkJournal!.events[end.id]!.bodyDigest, end.bodyDigest);
  assert.throws(() => store.observeHarkProvider(call.id, 'completed', observedAt,
    harkProviderTiming({ ...completed(), duration: '40' })), /timing cannot change/);
  assert.equal(JSON.stringify(store.get(call.id)!.harkJournal!.events[end.id]), bytes);
});
