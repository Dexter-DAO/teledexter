import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { callerIntentInputDigest, callerIntentV3InputSchema, callerIntentResponseSchema,
  validateCallerIntentResponse } from '../src/broker/callerIntentContract.js';
import { rawSubmissionDigest } from '../src/broker/reconciliation.js';

// Frozen API vectors copied into this repository so the release needs no sibling checkout.
const fixture=JSON.parse(readFileSync(new URL('./fixtures/phone-caller-intent-v3.json',import.meta.url),'utf8'));
for(const value of fixture.cases) test(`v3 API/Broker wire agreement: ${value.name}`,()=>{
  const input=callerIntentV3InputSchema.parse(value.input);
  assert.equal(callerIntentInputDigest(value.operationId,value.bindingId,input),value.sha256);
  const response=callerIntentResponseSchema.parse(value.response);
  assert.doesNotThrow(()=>validateCallerIntentResponse(value.bindingId,value.operationId,input,response));
  const changed=structuredClone(input);changed.taskCoverage.inventoryDigest='b'.repeat(64);
  assert.throws(()=>validateCallerIntentResponse(value.bindingId,value.operationId,changed,response));
});
test('combined SMS task raw digest agrees with the API literal vector',()=>{
  const value=JSON.parse(readFileSync(new URL('./fixtures/phone-result-sms-task-digests.json',import.meta.url),'utf8'));
  const {bindingId,requestId:_requestId,role,...submission}=value.input;
  assert.equal(rawSubmissionDigest(bindingId,{...submission,source:{...submission.source,role}}),value.rawSubmissionDigest);
  assert.notEqual(rawSubmissionDigest(bindingId,{...submission,resultSms:undefined,source:{...submission.source,role}}),value.rawSubmissionDigest);
});
