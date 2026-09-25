import assert from 'node:assert/strict';
import test from 'node:test';
import { createVerificationCallbackChallenge } from '../src/broker/verificationCallbackChallenge.js';
import { reserveVerificationCallback, prepareVerificationCallbackDispatch, recordVerificationCallbackGrant,
  fenceVerificationCallbackProvider, prepareVerificationCallbackAnswer, recordVerificationCallbackRemote } from '../src/broker/verificationCallbackJournal.js';
import type { PhoneVerificationCallbackPermit } from '../src/broker/verificationCallbackContract.js';

const id = '00000000-0000-4000-8000-000000000001', attempt = '00000000-0000-4000-8000-000000000002';
const sid = 'CA' + '1'.repeat(32), start = Date.parse('2026-09-25T06:00:00.000Z');
const permit: PhoneVerificationCallbackPermit = { v: 1, request: { requestId: id, requestDigest: 'a'.repeat(64) }, purpose: 'enrollment',
  providerAccountSid: 'AC' + '1'.repeat(32), from: '+15555550001', to: '+15555550002',
  answerUrl: `https://broker.example.test/broker/verification-callback/${id}`,
  responseUrl: `https://broker.example.test/broker/verification-callback-response/${id}`,
  createdAt: new Date(start).toISOString(), expiresAt: new Date(start + 300000).toISOString(), maxDurationSeconds: 180,
  billing: { intentId: id, eventId: `phone-verification:${id}`, mandateId: attempt, unitPriceMicro: 100, freeSeconds: 0 } };
function fixture() {
  const at = new Date(start + 1).toISOString();
  let journal = prepareVerificationCallbackDispatch(reserveVerificationCallback(permit, at), attempt, at);
  journal = recordVerificationCallbackGrant(journal, { v: 1, state: 'dispatch_granted', permit, dispatchAttemptId: attempt });
  journal = fenceVerificationCallbackProvider(journal, at);
  const proof = { url: permit.answerUrl, signature: 'synthetic', params: { AccountSid: permit.providerAccountSid,
    CallSid: sid, From: permit.from, To: permit.to, Direction: 'outbound-api', CallStatus: 'in-progress' } };
  journal = prepareVerificationCallbackAnswer(journal, proof, at);
  journal = recordVerificationCallbackRemote(journal, { v: 1, request: permit.request, state: 'answered',
    dispatchAttemptId: attempt, callSid: sid, purpose: 'enrollment', expiresAt: permit.expiresAt });
  let protectedInput = true, now = start + 2;
  const vault = createVerificationCallbackChallenge({ journal: () => journal,
    verifySignature: value => value.signature === 'synthetic', protectedInputActive: () => protectedInput, now: () => now });
  return { vault, journal, form: { ...proof, url: permit.responseUrl, params: { ...proof.params, Digits: '01234567' } },
    block() { protectedInput = false; }, expire() { now = start + 300000; } };
}

test('exact authenticated response is ephemeral, tied to the same leg and consumed once', () => {
  const f = fixture(), before = JSON.stringify(f.journal); f.vault.capture(f.form);
  assert.equal(f.vault.take(permit.request, sid), '01234567'); assert.equal(f.vault.take(permit.request, sid), undefined);
  assert.equal(JSON.stringify(f.journal), before); assert.equal(before.includes('01234567'), false);
  assert.throws(() => f.vault.capture(f.form), /protected_input_refused/);
});
test('wrong signature, form URL, provider identity, live-input state and spoken input are refused', () => {
  for (const key of ['signature', 'url', 'AccountSid', 'CallSid', 'From', 'To', 'Direction', 'CallStatus', 'Digits', 'SpeechResult']) {
    const f = fixture(), raw = structuredClone(f.form);
    if (key === 'signature') raw.signature = 'foreign'; else if (key === 'url') raw.url = permit.answerUrl;
    else Object.assign(raw.params, { [key]: 'foreign' });
    assert.throws(() => f.vault.capture(raw), /protected_input_refused/);
    assert.equal(f.vault.take(permit.request, sid), undefined);
  }
  const f = fixture(); f.block(); assert.throws(() => f.vault.capture(f.form), /protected_input_refused/);
});
test('expiry, wrong request, wrong SID and explicit clear never expose a retained challenge', () => {
  const expired = fixture(); expired.vault.capture(expired.form); expired.expire();
  assert.throws(() => expired.vault.take(permit.request, sid)); assert.equal(expired.vault.take(permit.request, sid), undefined);
  const wrong = fixture(); wrong.vault.capture(wrong.form);
  assert.throws(() => wrong.vault.take({ ...permit.request, requestDigest: 'b'.repeat(64) }, sid));
  assert.equal(wrong.vault.take(permit.request, sid), undefined);
  const other = fixture(); other.vault.capture(other.form); assert.throws(() => other.vault.take(permit.request, 'CA' + '2'.repeat(32)));
  const cleared = fixture(); cleared.vault.capture(cleared.form); cleared.vault.clear(); assert.equal(cleared.vault.take(permit.request, sid), undefined);
});
