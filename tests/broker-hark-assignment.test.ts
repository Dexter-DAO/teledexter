import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';
import test from 'node:test';
import ts from 'typescript';
import {
  brokerCallAssignmentSchema, newPhoneCallControlSchema, phoneCallAssignmentSchema,
  retainedPhoneCallAssignmentRefSchema,
} from '../src/broker/assignment.js';
import {
  canonicalHarkValue, HARK_PHONE_PURPOSE, HARK_PHONE_SNAPSHOT_MAX_BYTES, harkSourceDigest,
  phoneHarkSnapshotSchema, resolvedHarkAssignmentSchema, type PhoneHarkSnapshot,
} from '../src/broker/harkAssignment.js';
import { makeHarkSnapshot, makeResolvedHarkAssignment } from './fixtures/broker-hark.js';

function resign(snapshot: PhoneHarkSnapshot): PhoneHarkSnapshot {
  const { sourceDigest: _digest, ...source } = snapshot;
  return { ...source, sourceDigest: harkSourceDigest(source) };
}

test('resolved wire preserves saved watch instructions, event attribution and exact values', () => {
  const resolved = makeResolvedHarkAssignment();
  const parsed = resolvedHarkAssignmentSchema.parse(resolved);
  assert.deepEqual(parsed, resolved);
  assert.deepEqual(phoneCallAssignmentSchema.parse(parsed.callAssignment), parsed.callAssignment);
  assert.deepEqual(brokerCallAssignmentSchema.parse({ origin: 'authenticated_hark_delivery', value: parsed.callAssignment }),
    { origin: 'authenticated_hark_delivery', value: parsed.callAssignment });
  assert.equal(parsed.callAssignment.hark.event.facts.note, '  Saved source text\t🙂');
  assert.equal(parsed.callAssignment.hark.event.facts.unitPriceUsd, '101.2300');
});

test('source digest ignores object key order while preserving arrays, strings and values', () => {
  const snapshot = makeHarkSnapshot();
  const { sourceDigest, ...source } = snapshot;
  const reordered = Object.fromEntries(Object.entries(source).reverse()) as typeof source;
  reordered.watch = { ...source.watch, condition: { b: { y: 2, x: 1 }, a: 0 } };
  const other = { ...reordered, watch: { ...reordered.watch, condition: { a: 0, b: { x: 1, y: 2 } } } };
  assert.equal(harkSourceDigest(source), sourceDigest);
  assert.equal(harkSourceDigest(reordered), harkSourceDigest(other));
  assert.deepEqual(canonicalHarkValue({ z: [3, 1, 2], a: '  x\t' }), { a: '  x\t', z: [3, 1, 2] });
  assert.notEqual(harkSourceDigest({ ...source, event: { ...source.event, evidence: [1, 2] } }),
    harkSourceDigest({ ...source, event: { ...source.event, evidence: [2, 1] } }));
  assert.notEqual(harkSourceDigest({ ...source, event: { ...source.event, summary: `${source.event.summary} ` } }), sourceDigest);
});

for (const field of ['ownerSupabaseUserId', 'destinationId', 'deliveryId', 'caseId', 'originTaskId', 'expiresAt',
  'watch', 'event', 'billing'] as const) {
  test(`changed ${field} fails with the original source digest`, () => {
    const original = makeHarkSnapshot();
    const candidate = structuredClone(original);
    switch (field) {
      case 'watch': candidate.watch.request = 'Watch a different asset.'; break;
      case 'event': candidate.event.facts.unitPriceUsd = '100000'; break;
      case 'billing': candidate.billing.unitPriceMicro += 1; break;
      case 'expiresAt': candidate.expiresAt = '2026-09-23T21:00:00.000Z'; break;
      case 'originTaskId': candidate.originTaskId += '-other'; break;
      default: candidate[field] = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
    }
    assert.equal(phoneHarkSnapshotSchema.safeParse(candidate).success, false);
    assert.equal(phoneHarkSnapshotSchema.safeParse(resign(candidate)).success, true);
  });
}

test('snapshot limit counts UTF-8 bytes and refuses oversized data without shortening it', () => {
  const snapshot = makeHarkSnapshot();
  snapshot.event.facts.padding = '';
  const empty = resign(snapshot);
  const available = HARK_PHONE_SNAPSHOT_MAX_BYTES - Buffer.byteLength(JSON.stringify(empty), 'utf8');
  snapshot.event.facts.padding = '🙂'.repeat(Math.floor(available / 4)) + 'x'.repeat(available % 4);
  const exact = resign(snapshot);
  assert.equal(Buffer.byteLength(JSON.stringify(exact), 'utf8'), HARK_PHONE_SNAPSHOT_MAX_BYTES);
  assert.deepEqual(phoneHarkSnapshotSchema.parse(exact), exact);
  const tooLarge = resign({ ...exact, event: { ...exact.event, facts: { ...exact.event.facts,
    padding: `${exact.event.facts.padding}x` } } });
  assert.equal(phoneHarkSnapshotSchema.safeParse(tooLarge).success, false);
  assert.equal(Buffer.byteLength(JSON.stringify(tooLarge), 'utf8'), HARK_PHONE_SNAPSHOT_MAX_BYTES + 1);
});

test('strict wire refuses unsolicited outer, assignment, snapshot and nested fields', () => {
  const resolved = makeResolvedHarkAssignment();
  const snapshot = resolved.callAssignment.hark;
  for (const candidate of [
    { ...resolved, sourceDigest: snapshot.sourceDigest },
    { ...resolved, callAssignment: { ...resolved.callAssignment, originTaskId: snapshot.originTaskId } },
    { ...resolved, callAssignment: { ...resolved.callAssignment, hark: { ...snapshot, briefing: 'Incoming text' } } },
    { ...resolved, callAssignment: { ...resolved.callAssignment, hark: { ...snapshot, watch: { ...snapshot.watch, grant: 'borrowed' } } } },
    { ...resolved, schemaVersion: 2 }, { ...resolved, to: '2025550123' },
  ]) assert.equal(resolvedHarkAssignmentSchema.safeParse(candidate).success, false);
});

test('delivery identity, fixed purpose and revision cannot be replaced', () => {
  const resolved = makeResolvedHarkAssignment();
  for (const change of [
    { assignmentId: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa' },
    { brokerCallId: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa' },
    { purpose: 'Read my portfolio instead.' }, { purpose: `${HARK_PHONE_PURPOSE} ` }, { revision: 2 },
  ]) assert.equal(resolvedHarkAssignmentSchema.safeParse({ ...resolved,
    callAssignment: { ...resolved.callAssignment, ...change } }).success, false);
  const upper = makeHarkSnapshot();
  upper.deliveryId = 'AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA';
  assert.equal(phoneHarkSnapshotSchema.safeParse(resign(upper)).success, false);
});

test('journal origins and control inputs cannot impersonate each other', () => {
  const hark = makeResolvedHarkAssignment().callAssignment;
  const { hark: _snapshot, ...operator } = hark;
  assert.equal(brokerCallAssignmentSchema.safeParse({ origin: 'authenticated_broker_control', value: hark }).success, false);
  assert.equal(brokerCallAssignmentSchema.safeParse({ origin: 'authenticated_hark_delivery', value: operator }).success, false);
  assert.equal(brokerCallAssignmentSchema.safeParse({ origin: 'hark', value: hark }).success, false);
  assert.equal(newPhoneCallControlSchema.safeParse({ assignmentId: hark.assignmentId, purpose: hark.purpose, hark: hark.hark }).success, false);
  assert.equal(newPhoneCallControlSchema.safeParse({ assignmentId: hark.assignmentId, purpose: hark.purpose }).success, true);
});

test('retained operator assignment and reference preserve uppercase IDs and old text exactly', () => {
  const value = { assignmentId: 'AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA',
    brokerCallId: 'BBBBBBBB-BBBB-4BBB-8BBB-BBBBBBBBBBBB', revision: 1, purpose: 'Retained\u0000purpose' };
  const journal = { origin: 'authenticated_broker_control', value };
  const original = JSON.stringify(journal);
  assert.equal(JSON.stringify(brokerCallAssignmentSchema.parse(journal)), original);
  assert.equal(phoneCallAssignmentSchema.safeParse(value).success, false);
  assert.equal(newPhoneCallControlSchema.safeParse({ assignmentId: value.assignmentId, purpose: value.purpose }).success, false);
  const ref = { assignmentId: value.assignmentId, revision: 1, digest: 'a'.repeat(64) };
  assert.deepEqual(retainedPhoneCallAssignmentRefSchema.parse(ref), ref);
});

const sharedConfigured = Boolean(process.env.BROKER_API_HARK_SOURCE || process.env.BROKER_API_DEPENDENCY_ROOT);
test('exact generated API Hark assignment fixture preserves its snapshot and digest in Broker', {
  skip: !process.env.BROKER_API_HARK_ASSIGNMENT_FIXTURE,
}, () => {
  const bytes = fs.readFileSync(process.env.BROKER_API_HARK_ASSIGNMENT_FIXTURE!);
  assert.equal(createHash('sha256').update(bytes).digest('hex'),
    'd9393bd1b49b4acec7adac514be7d7a756e970787bf2c2fe855ccf664cdbfe15');
  const fixture: unknown = JSON.parse(bytes.toString('utf8'));
  const parsed = resolvedHarkAssignmentSchema.parse(fixture);
  assert.deepEqual(parsed, fixture);
  assert.deepEqual(phoneCallAssignmentSchema.parse(parsed.callAssignment), parsed.callAssignment);
  assert.deepEqual(brokerCallAssignmentSchema.parse({ origin: 'authenticated_hark_delivery', value: parsed.callAssignment }),
    { origin: 'authenticated_hark_delivery', value: parsed.callAssignment });
  const { sourceDigest, ...source } = parsed.callAssignment.hark;
  assert.equal(sourceDigest, '1259ae05030f462c838856606649866de7167b2d34f8692bf4d14f956a6f46fa');
  assert.equal(harkSourceDigest(source), sourceDigest);
  const changed = structuredClone(parsed);
  changed.callAssignment.hark.event.facts.price = 102;
  assert.equal(resolvedHarkAssignmentSchema.safeParse(changed).success, false);
});

test('frozen API Zod 3 snapshot accepts the same Broker values and computes identical digests',
  { skip: !sharedConfigured }, () => {
    const sourcePath = process.env.BROKER_API_HARK_SOURCE;
    const dependencyRoot = process.env.BROKER_API_DEPENDENCY_ROOT;
    assert.ok(sourcePath && dependencyRoot, 'Provide both frozen source and API dependency root');
    const source = fs.readFileSync(sourcePath, 'utf8');
    assert.equal(createHash('sha256').update(source).digest('hex'), '47a5f4b87f285107ba3820105569e14657f30e999025bc11f0e042ccfbca8ff9');
    const apiRequire = createRequire(path.join(dependencyRoot, 'package.json'));
    assert.match(apiRequire('zod/package.json').version, /^3\./);
    const code = ts.transpileModule(source, { compilerOptions: {
      module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022,
    } }).outputText;
    const exports: Record<string, unknown> = {};
    new Function('require', 'exports', code)(apiRequire, exports);
    const schema = exports.phoneHarkSnapshotSchema as typeof phoneHarkSnapshotSchema;
    const digest = exports.harkSourceDigest as typeof harkSourceDigest;
    const saved = makeHarkSnapshot();
    const { sourceDigest, ...sourceValue } = saved;
    assert.equal(digest(sourceValue), sourceDigest);
    assert.deepEqual(schema.parse(saved), phoneHarkSnapshotSchema.parse(saved));
    for (const value of [saved, { ...saved, sourceDigest: '0'.repeat(64) },
      { ...saved, event: { ...saved.event, summary: '\ud800' } },
      { ...saved, deliveryId: saved.deliveryId.toUpperCase().replace(/^3/, 'A') },
      resign({ ...saved, event: { ...saved.event, evidence: Array(33).fill(null) } }),
    ]) assert.equal(schema.safeParse(value).success, phoneHarkSnapshotSchema.safeParse(value).success);
  });
