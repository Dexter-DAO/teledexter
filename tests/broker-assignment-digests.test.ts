import assert from 'node:assert/strict';
import test from 'node:test';
import fs from 'node:fs';
import { createHash } from 'node:crypto';
import { originalTurn, rawSubmissionDigest, turnBodyDigest } from '../src/broker/reconciliation.js';
import type { PrimarySubmission } from '../src/broker/primaryApi.js';

// Literal fixtures are also the cross-repository HTTP contract; the expected
// digests were calculated independently of rawSubmissionDigest.
const fixtures = JSON.parse(fs.readFileSync(new URL('./fixtures/broker-assignment-digests.json', import.meta.url), 'utf8')) as Array<{
  name: string; bindingId: string; input: PrimarySubmission;
  rawJson: string; rawDigest: string; renderedJson: string; renderedDigest: string;
}>;
const sha = (value: string) => createHash('sha256').update(value, 'utf8').digest('hex');
for (const fixture of fixtures) {
  test(`exact ${fixture.name} assignment/publication digest order preserves speech and appends rendering last`, () => {
    assert.equal(rawSubmissionDigest(fixture.bindingId, fixture.input), fixture.rawDigest);
    assert.equal(sha(fixture.rawJson), fixture.rawDigest);
    const raw = JSON.parse(fixture.rawJson);
    assert.deepEqual(Object.keys(raw), ['bindingId', 'callSid', 'itemId', 'sourceEventId', 'revision', 'receivedAt', 'role', 'text',
      ...(fixture.input.relatedPrimaryRequestId ? ['relatedPrimaryRequestId', 'referenceOrigin'] : []),
      ...(fixture.input.callAssignmentRef ? ['callAssignmentRef'] : [])]);
    if (raw.callAssignmentRef) assert.deepEqual(Object.keys(raw.callAssignmentRef), ['assignmentId', 'revision', 'digest']);
    assert.equal(JSON.stringify({ ...raw, primaryRendering: { text: 'Hello.', projection: { kind: 'derived' } } }), fixture.renderedJson);
    assert.equal(sha(fixture.renderedJson), fixture.renderedDigest);
    assert.notEqual(fixture.rawDigest, fixture.renderedDigest);
    assert.equal(turnBodyDigest(originalTurn(fixture.input)), turnBodyDigest(originalTurn(fixtures[0].input)));
    assert.equal(fixture.input.text, ' Hello.\t');
  });
}
