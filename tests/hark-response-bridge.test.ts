import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { randomUUID } from 'node:crypto';
import { z } from 'zod';
import { verifySignedAlertRequest } from '../src/outbound/auth.js';
import { canonicalJson, sha256Hex } from '../src/outbound/canonical.js';
import {
  OutboundAlertRequestSchema,
  serverAuthoredPhoneNotification,
} from '../src/outbound/contract.js';
import {
  HarkResponseBridge,
  type HarkCallBinding,
} from '../src/outbound/HarkResponseBridge.js';
import { HarkCallerItemTracker } from '../src/outbound/HarkCallerItemTracker.js';
import { HarkRealtimeLineage } from '../src/outbound/HarkRealtimeLineage.js';
import { HarkHumanTurnQualifier } from '../src/outbound/harkAcknowledgment.js';
import {
  dispatchAuthorizedOutbound,
  hasOutboundMediaTokenKey,
  isValidHarkCallbackSigningSecret,
  selectOutboundAuthorization,
} from '../src/outbound/ingressAuthorization.js';
import { OutboundAlertStore } from '../src/outbound/OutboundAlertStore.js';

const PRODUCER_SECRET = 'producer-alert-test-secret-that-is-longer-than-thirty-two-bytes';
const CALLBACK_SECRET = 'hark-callback-test-secret-that-is-longer-than-thirty-two-bytes';
const CASE_ID = '11111111-1111-4111-8111-111111111111';
const WATCH_ID = '22222222-2222-4222-8222-222222222222';
const EVENT_ID = '33333333-3333-4333-8333-333333333333';
const DELIVERY_ID = '44444444-4444-4444-8444-444444444444';
const INTENT_ID = '55555555-5555-4555-8555-555555555555';
const CALL_SID = `CA${'6'.repeat(32)}`;
const API_RESPONSE_ID = '77777777-7777-4777-8777-777777777777';
const API_CONTINUATION_ID = '88888888-8888-4888-8888-888888888888';

// Independent frozen copy of the API's strict owner decision / Phone request
// / callback response boundary. The fake API parses both directions through
// these schemas so a permissive mock cannot hide contract drift again.
const apiNonEmpty = (max: number) => z.string().trim().min(1).max(max);
const ApiOwnerDecisionSchema = z.discriminatedUnion('type', [
  z.object({ type: z.literal('acknowledge'), text: apiNonEmpty(4_000).optional() }).strict(),
  z.object({ type: z.literal('dismiss'), text: apiNonEmpty(4_000).optional() }).strict(),
  z.object({
    type: z.literal('snooze'),
    seconds: z.number().int().min(60).max(30 * 24 * 60 * 60),
    text: apiNonEmpty(4_000).optional(),
  }).strict(),
  z.object({ type: z.literal('reply'), text: apiNonEmpty(4_000) }).strict(),
  z.object({
    type: z.literal('choice'),
    value: apiNonEmpty(500),
    text: apiNonEmpty(4_000).optional(),
  }).strict(),
  z.object({ type: z.literal('instruction'), text: apiNonEmpty(4_000) }).strict(),
]);
const ApiPhoneResponseSchema = z.object({
  v: z.literal('hark.response.v1'),
  sourceResponseId: apiNonEmpty(200),
  decision: ApiOwnerDecisionSchema,
  supersedesResponseId: z.string().uuid().nullable(),
  caseId: z.string().uuid(),
  eventId: z.string().uuid(),
  deliveryId: z.string().uuid(),
  intentId: z.string().uuid(),
  callSid: apiNonEmpty(64),
  originTaskId: apiNonEmpty(300),
  observedAt: z.string().datetime({ offset: true }),
}).strict();
const ApiContinuationSchema = z.object({
  v: z.literal('hark.continuation.v1'),
  continuationId: z.string().uuid(),
  idempotencyKey: z.string().uuid(),
  origin: z.object({
    protocol: z.literal('hermes'),
    runtimeId: apiNonEmpty(200),
    taskId: apiNonEmpty(300),
    conversationId: apiNonEmpty(300),
  }).strict(),
  case: z.object({
    id: z.string().uuid(),
    eventId: z.string().uuid(),
    summary: z.string(),
  }).strict(),
  response: z.object({
    id: z.string().uuid(),
    decision: ApiOwnerDecisionSchema,
  }).strict(),
}).strict().superRefine((continuation, context) => {
  if (continuation.continuationId !== continuation.idempotencyKey) {
    context.addIssue({ code: 'custom', path: ['idempotencyKey'], message: 'must match' });
  }
});
const ApiPhoneCallbackResponseSchema = z.object({
  v: z.literal('hark.phone-callback-result.v1'),
  ok: z.literal(true),
  replay: z.boolean(),
  responseId: z.string().uuid(),
  caseId: z.string().uuid(),
  eventId: z.string().uuid(),
  deliveryId: z.string().uuid(),
  intentId: z.string().uuid(),
  callSid: apiNonEmpty(64),
  originTaskId: apiNonEmpty(300),
  sourceResponseId: apiNonEmpty(200),
  caseState: apiNonEmpty(80),
  continuation: ApiContinuationSchema.optional(),
}).strict();

function callBinding(): HarkCallBinding {
  return {
    intentId: INTENT_ID,
    callSid: CALL_SID,
    hark: {
      schemaVersion: 1,
      caseId: CASE_ID,
      watchId: WATCH_ID,
      eventId: EVENT_ID,
      deliveryId: DELIVERY_ID,
      originTaskId: 'hermes-task-42',
    },
  };
}

function temporaryStore(t: test.TestContext): OutboundAlertStore {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'dexter-hark-response-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  return new OutboundAlertStore(path.join(directory, 'state.json'));
}

function rawApiContinuation(
  requestBody: Record<string, any>,
  responseId = API_RESPONSE_ID,
): Record<string, unknown> {
  return {
    v: 'hark.continuation.v1',
    continuationId: API_CONTINUATION_ID,
    idempotencyKey: API_CONTINUATION_ID,
    origin: {
      protocol: 'hermes',
      runtimeId: 'hermes-runtime-1',
      taskId: requestBody.originTaskId,
      conversationId: 'hermes-conversation-1',
    },
    case: {
      id: requestBody.caseId,
      eventId: requestBody.eventId,
      summary: 'Exact Hark case summary.',
    },
    response: {
      id: responseId,
      decision: requestBody.decision,
    },
  };
}

function successfulApiResponse(
  requestBody: Record<string, any>,
  overrides: Record<string, unknown> = {},
): Response {
  const responseId = typeof overrides.responseId === 'string'
    ? overrides.responseId
    : API_RESPONSE_ID;
  const result: Record<string, unknown> = {
    v: 'hark.phone-callback-result.v1',
    ok: true,
    replay: false,
    responseId,
    caseId: requestBody.caseId,
    eventId: requestBody.eventId,
    deliveryId: requestBody.deliveryId,
    intentId: requestBody.intentId,
    callSid: requestBody.callSid,
    originTaskId: requestBody.originTaskId,
    sourceResponseId: requestBody.sourceResponseId,
    caseState: 'resuming',
    continuation: rawApiContinuation(requestBody, responseId),
    ...overrides,
  };
  if (result.continuation === null) delete result.continuation;
  return new Response(JSON.stringify(result), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });
}

function successfulStatusApiResponse(
  requestBody: Record<string, any>,
  overrides: Record<string, unknown> = {},
): Response {
  return new Response(JSON.stringify({
    v: 'hark.phone-callback-result.v1',
    ok: true,
    replay: false,
    caseId: requestBody.caseId,
    eventId: requestBody.eventId,
    deliveryId: requestBody.deliveryId,
    intentId: requestBody.intentId,
    callSid: requestBody.callSid,
    originTaskId: requestBody.originTaskId,
    sourceEventId: requestBody.sourceEventId,
    caseState: 'waiting_owner',
    ...overrides,
  }), { status: 200, headers: { 'content-type': 'application/json' } });
}

function callbackRequestBody(init?: RequestInit): Record<string, any> {
  return JSON.parse(String(init?.body));
}

function invocation(sequence: number, userItemId = `user-item-${sequence}`) {
  return {
    providerInvocationId: `provider-tool-call-${sequence}`,
    userItemId,
  };
}

test('API-shaped Hark request binds one outer delivery key and preserves the nested event UUID', (t) => {
  const deliveryEventId = `hark:${DELIVERY_ID}`;
  const base = {
    idempotencyKey: deliveryEventId,
    eventId: deliveryEventId,
    to: '+15615550100',
    subject: 'JORDAN crossed 20%',
    severity: 'warning' as const,
    briefing: 'JORDAN crossed the configured threshold.',
    observedAt: '2026-08-04T07:00:00.000Z',
    ttlSeconds: 180,
    followupContext: { notification: { source: 'hark' } },
    hark: callBinding().hark,
  };

  assert.equal(OutboundAlertRequestSchema.safeParse(base).success, true);
  assert.notEqual(base.eventId, base.hark.eventId);
  assert.equal(OutboundAlertRequestSchema.safeParse({
    ...base,
    eventId: EVENT_ID,
  }).success, false);
  assert.equal(OutboundAlertRequestSchema.safeParse({
    ...base,
    idempotencyKey: `hark:${WATCH_ID}`,
  }).success, false);
  assert.equal(OutboundAlertRequestSchema.safeParse({
    ...base,
    hark: { ...base.hark, deliveryId: WATCH_ID },
  }).success, false);

  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'dexter-hark-hash-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const store = new OutboundAlertStore(path.join(directory, 'state.json'));
  const first = store.reserve(OutboundAlertRequestSchema.parse(base));
  assert.equal(first.kind, 'new');
  const conflict = store.reserve(OutboundAlertRequestSchema.parse({
    ...base,
    hark: { ...base.hark, originTaskId: 'different-task' },
  }));
  assert.equal(conflict.kind, 'conflict');
});

test('API-authored Phone billing binding is exact and rejects omitted or extra tariff identity', () => {
  const request = OutboundAlertRequestSchema.parse({
    idempotencyKey: 'hark:publish-1:subscription-1',
    eventId: 'hark:publish-1:subscription-1',
    to: '+15615550100',
    subject: 'JORDAN crossed 20%',
    severity: 'warning',
    briefing: 'JORDAN crossed the configured threshold.',
    observedAt: '2026-08-04T07:00:00.000Z',
    ttlSeconds: 180,
    followupContext: {
      notification: {
        source: 'hark',
        publishId: '11111111-1111-4111-8111-111111111111',
        topicId: '22222222-2222-4222-8222-222222222222',
        topicAddress: 'jordan-price',
        subscriptionId: '33333333-3333-4333-8333-333333333333',
        destinationId: '44444444-4444-4444-8444-444444444444',
        billingMandateId: '55555555-5555-4555-8555-555555555555',
        billingUnitPriceMicro: 100_000,
        billingFreeSeconds: 180,
        title: 'JORDAN crossed 20%',
        priority: 'high',
        tags: ['hark'],
      },
    },
  });
  assert.deepEqual(serverAuthoredPhoneNotification(request), {
    ...(request.followupContext.notification as Record<string, unknown>),
  });

  const missingTariff = structuredClone(request);
  delete (missingTariff.followupContext.notification as Record<string, unknown>)
    .billingUnitPriceMicro;
  assert.equal(serverAuthoredPhoneNotification(missingTariff), null);

  const extraAuthority = structuredClone(request);
  (extraAuthority.followupContext.notification as Record<string, unknown>)
    .recipientOverride = '+15615550999';
  assert.equal(serverAuthoredPhoneNotification(extraAuthority), null);
});

test('first completed non-echo utterance creates one signed, stable acknowledgment', async (t) => {
  const store = temporaryStore(t);
  const requests: Array<{ url: string; body: any; headers: Headers }> = [];
  const fetchImpl = async (input: string | URL | Request, init?: RequestInit) => {
    const headers = new Headers(init?.headers);
    const body = JSON.parse(String(init?.body));
    requests.push({ url: String(input), body, headers });
    return successfulApiResponse(body, { continuation: null, caseState: 'waiting_owner' });
  };
  const bridge = new HarkResponseBridge({
    store,
    apiBaseUrl: 'http://api.test',
    signingSecret: CALLBACK_SECRET,
    fetchImpl: fetchImpl as typeof fetch,
    now: () => Date.parse('2026-08-04T07:01:00.000Z'),
    retryInProcess: false,
  });

  const human = new HarkHumanTurnQualifier('human');
  assert.equal(human.isQualified(), false);
  assert.equal(human.observe({ itemId: 'turn-echo', transcript: 'Dexter speaking', echo: true }), false);
  assert.equal(human.observe({ itemId: 'turn-empty', transcript: '   ', echo: false }), false);
  assert.equal(human.observe({ itemId: 'turn-human', transcript: 'Got it.', echo: false }), true);
  assert.equal(human.isQualified(), true);
  assert.equal(human.currentHumanItemId(), 'turn-human');

  const first = await bridge.acknowledge(callBinding(), 'turn-human');
  const replay = await bridge.acknowledge(callBinding(), 'turn-human');
  assert.equal(first.state, 'delivered');
  assert.equal(replay.replay, true);
  assert.equal(replay.sourceId, first.sourceId);
  assert.equal(requests.length, 1);
  assert.equal(requests[0]!.url, `http://api.test/internal/hark/v1/cases/${CASE_ID}/phone-responses`);
  assert.deepEqual(requests[0]!.body.decision, { type: 'acknowledge' });
  assert.equal(requests[0]!.body.caseId, CASE_ID);
  assert.equal(requests[0]!.body.eventId, EVENT_ID);
  assert.equal(requests[0]!.body.deliveryId, DELIVERY_ID);
  assert.equal(requests[0]!.body.intentId, INTENT_ID);
  assert.equal(requests[0]!.body.originTaskId, 'hermes-task-42');

  const verified = verifySignedAlertRequest({
    secret: CALLBACK_SECRET,
    body: requests[0]!.body,
    headers: {
      timestamp: requests[0]!.headers.get('x-dexter-alert-timestamp') || undefined,
      nonce: requests[0]!.headers.get('x-dexter-alert-nonce') || undefined,
      signature: requests[0]!.headers.get('x-dexter-alert-signature') || undefined,
    },
    nowMs: Date.parse('2026-08-04T07:01:00.000Z'),
  });
  assert.deepEqual(verified, { ok: true });
  const producerSecretRejected = verifySignedAlertRequest({
    secret: PRODUCER_SECRET,
    body: requests[0]!.body,
    headers: {
      timestamp: requests[0]!.headers.get('x-dexter-alert-timestamp') || undefined,
      nonce: requests[0]!.headers.get('x-dexter-alert-nonce') || undefined,
      signature: requests[0]!.headers.get('x-dexter-alert-signature') || undefined,
    },
    nowMs: Date.parse('2026-08-04T07:01:00.000Z'),
  });
  assert.equal(producerSecretRejected.ok, false);
});

test('every natural Phone decision normalizes to the exact API request and raw response schemas', async (t) => {
  const store = temporaryStore(t);
  const bodies: any[] = [];
  const nowMs = Date.parse('2026-08-04T07:02:00.000Z');
  const bridge = new HarkResponseBridge({
    store,
    apiBaseUrl: 'http://api.test',
    signingSecret: CALLBACK_SECRET,
    now: () => nowMs,
    fetchImpl: (async (_input: string | URL | Request, init?: RequestInit) => {
      const body = ApiPhoneResponseSchema.parse(callbackRequestBody(init));
      bodies.push(body);
      const noContinuation = body.decision.type === 'acknowledge'
        || body.decision.type === 'snooze';
      const response = successfulApiResponse(
        body,
        noContinuation
          ? { continuation: null, caseState: 'waiting_owner' }
          : { caseState: 'resuming' },
      );
      ApiPhoneCallbackResponseSchema.parse(await response.clone().json());
      return response;
    }) as typeof fetch,
    retryInProcess: false,
  });

  const cases: Array<{ input: any; expected: any }> = [
    { input: { type: 'acknowledge' }, expected: { type: 'acknowledge' } },
    {
      input: { type: 'dismiss', text: 'Dismiss this alert.' },
      expected: { type: 'dismiss', text: 'Dismiss this alert.' },
    },
    {
      input: {
        type: 'snooze',
        text: 'Remind me in ninety seconds.',
        snoozeUntil: '2026-08-04T07:03:30.000Z',
      },
      expected: {
        type: 'snooze',
        seconds: 90,
        text: 'Remind me in ninety seconds.',
      },
    },
    {
      input: {
        type: 'snooze',
        snoozeUntil: '2026-08-04T07:01:59.000Z',
      },
      expected: { type: 'snooze', seconds: 60 },
    },
    {
      input: {
        type: 'snooze',
        snoozeUntil: '2026-10-04T07:02:00.000Z',
      },
      expected: { type: 'snooze', seconds: 30 * 24 * 60 * 60 },
    },
    {
      input: { type: 'reply', text: 'Tell Jimothy I saw it.' },
      expected: { type: 'reply', text: 'Tell Jimothy I saw it.' },
    },
    {
      input: { type: 'choice', text: 'Use the conservative option.' },
      expected: { type: 'choice', value: 'Use the conservative option.' },
    },
    {
      input: { type: 'instruction', text: 'Buy only if it remains above the threshold.' },
      expected: { type: 'instruction', text: 'Buy only if it remains above the threshold.' },
    },
  ];

  for (const [index, item] of cases.entries()) {
    const result = await bridge.submitResponse(
      callBinding(),
      item.input,
      invocation(500 + index),
    );
    assert.equal(result.state, 'delivered');
    assert.deepEqual(bodies[index]!.decision, item.expected);
    assert.equal(bodies[index]!.caseId, CASE_ID);
    const narrated = result.continuationResult
      && 'continuation' in result.continuationResult
      ? result.continuationResult.continuation
      : undefined;
    if (item.expected.type === 'acknowledge' || item.expected.type === 'snooze') {
      assert.equal(narrated, undefined);
    } else {
      assert.deepEqual(narrated, {
        id: API_CONTINUATION_ID,
        originTaskId: 'hermes-task-42',
      });
    }
  }
  assert.equal(bodies.length, cases.length);
});

test('structured response correction supersedes the previous stable response', async (t) => {
  const store = temporaryStore(t);
  const responseBodies: any[] = [];
  const bridge = new HarkResponseBridge({
    store,
    apiBaseUrl: 'http://api.test',
    signingSecret: CALLBACK_SECRET,
    fetchImpl: (async (_input: string | URL | Request, init?: RequestInit) => {
      const body = callbackRequestBody(init);
      responseBodies.push(body);
      return successfulApiResponse(body);
    }) as typeof fetch,
    now: () => Date.parse('2026-08-04T07:02:00.000Z'),
    retryInProcess: false,
  });

  const first = await bridge.submitResponse(callBinding(), {
    type: 'reply',
    text: 'Buy $100 if it is still above the threshold.',
  }, invocation(1));
  const correction = await bridge.submitResponse(callBinding(), {
    type: 'instruction',
    text: 'Actually, do not buy it. Just send me the updated price.',
  }, invocation(2), true);
  const correctionReplay = await bridge.submitResponse(callBinding(), {
    type: 'instruction',
    text: 'Actually, do not buy it. Just send me the updated price.',
  }, invocation(2, 'later-observed-user-item'), true);

  assert.equal(first.state, 'delivered');
  assert.equal(correction.state, 'delivered');
  assert.equal(correctionReplay.replay, true);
  assert.equal(correctionReplay.sourceId, correction.sourceId);
  assert.equal(responseBodies.length, 2);
  assert.equal(responseBodies[0]!.supersedesResponseId, null);
  assert.equal(responseBodies[1]!.supersedesResponseId, API_RESPONSE_ID);
  assert.equal(responseBodies[1]!.decision.type, 'instruction');
  assert.equal(responseBodies[1]!.decision.text, 'Actually, do not buy it. Just send me the updated price.');
  await assert.rejects(
    bridge.submitResponse(callBinding(), {
      type: 'instruction',
      text: 'Changed arguments under the same provider invocation.',
    }, invocation(2), true),
    /provider invocation conflict/,
  );

  const intervening = await bridge.submitResponse(callBinding(), {
    type: 'reply',
    text: 'Give me the latest market summary first.',
  }, invocation(3));
  const sameWordsNewInvocation = await bridge.submitResponse(callBinding(), {
    type: 'instruction',
    text: 'Actually, do not buy it. Just send me the updated price.',
  }, invocation(4), true);
  assert.notEqual(sameWordsNewInvocation.sourceId, correction.sourceId);
  assert.equal(responseBodies.length, 4);
  assert.equal(responseBodies[3]!.supersedesResponseId, API_RESPONSE_ID);
});

test('pending response survives restart and retries with the same source response id and body', async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'dexter-hark-restart-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const statePath = path.join(directory, 'state.json');
  const failedBodies: string[] = [];
  const firstBridge = new HarkResponseBridge({
    store: new OutboundAlertStore(statePath),
    apiBaseUrl: 'http://api.test',
    signingSecret: CALLBACK_SECRET,
    fetchImpl: (async (_input: string | URL | Request, init?: RequestInit) => {
      failedBodies.push(String(init?.body));
      return new Response('unavailable', { status: 503 });
    }) as typeof fetch,
    retryInProcess: false,
  });

  const first = await firstBridge.submitResponse(callBinding(), {
    type: 'dismiss',
    text: 'Dismiss this one.',
  }, invocation(10));
  assert.equal(first.state, 'pending');

  const retriedBodies: string[] = [];
  const restartedStore = new OutboundAlertStore(statePath);
  const restartedBridge = new HarkResponseBridge({
    store: restartedStore,
    apiBaseUrl: 'http://api.test',
    signingSecret: CALLBACK_SECRET,
    fetchImpl: (async (_input: string | URL | Request, init?: RequestInit) => {
      retriedBodies.push(String(init?.body));
      return successfulApiResponse(callbackRequestBody(init), { replay: true });
    }) as typeof fetch,
    retryInProcess: false,
  });
  await restartedBridge.flushPending();

  assert.deepEqual(retriedBodies, failedBodies);
  const persisted = restartedStore.getHarkOutbox(`response:${first.sourceId}`);
  assert.equal(persisted?.state, 'delivered');
  assert.equal(persisted?.attempts, 2);
  assert.equal(persisted?.continuationResult?.replay, true);
});

test('non-auth callback 4xx remains terminal and never enters the retry backlog', async (t) => {
  const store = temporaryStore(t);
  let requestCount = 0;
  const bridge = new HarkResponseBridge({
    store,
    apiBaseUrl: 'http://api.test',
    signingSecret: CALLBACK_SECRET,
    fetchImpl: (async () => {
      requestCount += 1;
      return new Response(JSON.stringify({ ok: false, error: 'phone_intent_mismatch' }), {
        status: 409,
        headers: { 'content-type': 'application/json' },
      });
    }) as typeof fetch,
    retryInProcess: false,
  });

  const result = await bridge.submitResponse(callBinding(), {
    type: 'dismiss',
    text: 'Dismiss this one.',
  }, invocation(15));

  assert.equal(result.state, 'rejected');
  assert.match(result.error || '', /HTTP 409/);
  await bridge.flushPending();
  assert.equal(requestCount, 1);
  assert.equal(
    store.getHarkOutbox(`response:${result.sourceId}`)?.state,
    'rejected',
  );
});

test('delivery status has stable idempotency and matching route/body case and delivery binding', async (t) => {
  const store = temporaryStore(t);
  const requests: Array<{ url: string; body: any }> = [];
  const bridge = new HarkResponseBridge({
    store,
    apiBaseUrl: 'http://api.test',
    signingSecret: CALLBACK_SECRET,
    fetchImpl: (async (input: string | URL | Request, init?: RequestInit) => {
      const body = callbackRequestBody(init);
      requests.push({ url: String(input), body });
      return requests.length === 1
        ? successfulApiResponse(body, { continuation: null })
        : successfulStatusApiResponse(body);
    }) as typeof fetch,
    retryInProcess: false,
  });

  const first = await bridge.reportStatus(callBinding(), 'streaming');
  const replay = await bridge.reportStatus(callBinding(), 'streaming');
  assert.equal(first.state, 'pending');
  assert.equal(replay.state, 'delivered');
  assert.equal(first.sourceId, replay.sourceId);
  assert.equal(requests.length, 2);
  assert.deepEqual(requests[1]!.body, requests[0]!.body);
  assert.equal(requests[0]!.url, `http://api.test/internal/hark/v1/deliveries/${DELIVERY_ID}/status`);
  assert.equal(requests[0]!.body.caseId, CASE_ID);
  assert.equal(requests[0]!.body.eventId, EVENT_ID);
  assert.equal(requests[0]!.body.deliveryId, DELIVERY_ID);
  assert.equal(requests[0]!.body.intentId, INTENT_ID);
  assert.equal(requests[0]!.body.callSid, CALL_SID);
  assert.equal(requests[0]!.body.originTaskId, 'hermes-task-42');
  assert.equal(requests[0]!.body.status, 'streaming');
});

test('mismatched continuation origin is rejected and never exposed as success', async (t) => {
  const store = temporaryStore(t);
  const bridge = new HarkResponseBridge({
    store,
    apiBaseUrl: 'http://api.test',
    signingSecret: CALLBACK_SECRET,
    fetchImpl: (async (_input: string | URL | Request, init?: RequestInit) => successfulApiResponse(
      callbackRequestBody(init), {
      continuation: {
        v: 'hark.continuation.v1',
        continuationId: '88888888-8888-4888-8888-888888888888',
        idempotencyKey: '88888888-8888-4888-8888-888888888888',
        origin: {
          protocol: 'hermes',
          runtimeId: 'hermes-runtime-1',
          taskId: 'another-task',
          conversationId: 'hermes-conversation-1',
        },
        case: {
          id: CASE_ID,
          eventId: EVENT_ID,
          summary: 'Exact Hark case summary.',
        },
        response: {
          id: '77777777-7777-4777-8777-777777777777',
          decision: { type: 'dismiss', text: 'Dismiss it.' },
        },
      },
    })) as typeof fetch,
    retryInProcess: false,
  });

  const result = await bridge.submitResponse(callBinding(), {
    type: 'instruction',
    text: 'Resume the exact task.',
  }, invocation(11));
  assert.equal(result.state, 'rejected');
  assert.equal(result.continuationResult, null);
  assert.match(result.error || '', /origin\.taskId did not match/);
});

test('raw continuation case and response identities must match the exact accepted callback', async (t) => {
  const mutations: Array<(raw: any) => void> = [
    (raw) => { raw.case.id = '99999999-9999-4999-8999-999999999999'; },
    (raw) => { raw.case.eventId = '99999999-9999-4999-8999-999999999999'; },
    (raw) => { raw.response.id = '99999999-9999-4999-8999-999999999999'; },
    (raw) => { raw.response.decision = { type: 'instruction', text: 'Different decision.' }; },
  ];

  for (const [index, mutate] of mutations.entries()) {
    const store = temporaryStore(t);
    const bridge = new HarkResponseBridge({
      store,
      apiBaseUrl: 'http://api.test',
      signingSecret: CALLBACK_SECRET,
      fetchImpl: (async (_input: string | URL | Request, init?: RequestInit) => {
        const body = callbackRequestBody(init);
        const continuation: any = rawApiContinuation(body);
        mutate(continuation);
        return successfulApiResponse(body, { continuation });
      }) as typeof fetch,
      retryInProcess: false,
    });
    const result = await bridge.submitResponse(callBinding(), {
      type: 'instruction',
      text: 'Resume the exact task.',
    }, invocation(750 + index));
    assert.equal(result.state, 'rejected');
    assert.match(result.error || '', /continuation case or response did not match/);
  }
});

test('callback success must echo every exact response and status outbox binding', async (t) => {
  const responseMismatches: Array<[string, string]> = [
    ['caseId', '99999999-9999-4999-8999-999999999999'],
    ['eventId', '99999999-9999-4999-8999-999999999999'],
    ['deliveryId', '99999999-9999-4999-8999-999999999999'],
    ['intentId', '99999999-9999-4999-8999-999999999999'],
    ['callSid', `CA${'9'.repeat(32)}`],
    ['originTaskId', 'different-origin-task'],
    ['sourceResponseId', 'phone-response:mismatched-source-id'],
  ];
  for (const [index, [field, mismatchedValue]] of responseMismatches.entries()) {
    const store = temporaryStore(t);
    const bridge = new HarkResponseBridge({
      store,
      apiBaseUrl: 'http://api.test',
      signingSecret: CALLBACK_SECRET,
      fetchImpl: (async (_input: string | URL | Request, init?: RequestInit) =>
        successfulApiResponse(callbackRequestBody(init), { [field]: mismatchedValue })) as typeof fetch,
      retryInProcess: false,
    });
    const result = await bridge.submitResponse(callBinding(), {
      type: 'dismiss',
      text: `Mismatch ${field}.`,
    }, invocation(100 + index));
    assert.equal(result.state, 'rejected');
    assert.match(result.error || '', new RegExp(field));
  }

  const statusStore = temporaryStore(t);
  const statusBridge = new HarkResponseBridge({
    store: statusStore,
    apiBaseUrl: 'http://api.test',
    signingSecret: CALLBACK_SECRET,
    fetchImpl: (async (_input: string | URL | Request, init?: RequestInit) =>
      successfulStatusApiResponse(callbackRequestBody(init), {
        sourceEventId: 'phone-status:mismatched-source-id',
      })) as typeof fetch,
    retryInProcess: false,
  });
  const status = await statusBridge.reportStatus(callBinding(), 'streaming');
  assert.equal(status.state, 'rejected');
  assert.match(status.error || '', /sourceEventId/);
});

test('Hark ingress always selects exact API authorization even with a valid producer HMAC', () => {
  assert.deepEqual(selectOutboundAuthorization({
    harkBound: true,
    signedProducer: true,
    recipientAllowlisted: true,
    hasInternalAuthorization: false,
    hasServerAuthoredBinding: false,
  }), { kind: 'reject', status: 401, error: 'unauthorized' });
  assert.deepEqual(selectOutboundAuthorization({
    harkBound: true,
    signedProducer: false,
    recipientAllowlisted: false,
    hasInternalAuthorization: true,
    hasServerAuthoredBinding: true,
  }), { kind: 'authorize_server_binding' });
  assert.deepEqual(selectOutboundAuthorization({
    harkBound: false,
    signedProducer: true,
    recipientAllowlisted: true,
    hasInternalAuthorization: false,
    hasServerAuthoredBinding: true,
  }), { kind: 'reject', status: 401, error: 'unauthorized' });
  assert.deepEqual(selectOutboundAuthorization({
    harkBound: false,
    signedProducer: true,
    recipientAllowlisted: true,
    hasInternalAuthorization: true,
    hasServerAuthoredBinding: false,
  }), { kind: 'reject', status: 403, error: 'recipient_not_allowed' });
  assert.deepEqual(selectOutboundAuthorization({
    harkBound: false,
    signedProducer: true,
    recipientAllowlisted: true,
    hasInternalAuthorization: false,
    hasServerAuthoredBinding: false,
  }), { kind: 'accept_legacy' });
  assert.equal(isValidHarkCallbackSigningSecret({
    producerSecret: PRODUCER_SECRET,
    callbackSecret: CALLBACK_SECRET,
  }), true);
  assert.equal(isValidHarkCallbackSigningSecret({
    producerSecret: PRODUCER_SECRET,
    callbackSecret: PRODUCER_SECRET,
  }), false);
  assert.equal(hasOutboundMediaTokenKey({
    serverAuthored: true,
    legacyProducerSecret: '',
    internalApiSecret: CALLBACK_SECRET,
  }), true);
  assert.equal(hasOutboundMediaTokenKey({
    serverAuthored: false,
    legacyProducerSecret: '',
    internalApiSecret: CALLBACK_SECRET,
  }), false);
});

test('API-shaped paid ingress without an internal token cannot reach provider dispatch', async () => {
  let reflectionAttempts = 0;
  let providerDispatches = 0;
  const result = await dispatchAuthorizedOutbound({
    harkBound: false,
    signedProducer: true,
    recipientAllowlisted: true,
    internalToken: '',
    hasServerAuthoredBinding: true,
    authorizeServerBinding: async () => {
      reflectionAttempts += 1;
      return { ok: true, status: 204 };
    },
    dispatch: async () => {
      providerDispatches += 1;
      return { callSid: CALL_SID };
    },
  });

  assert.deepEqual(result, {
    kind: 'reject',
    status: 401,
    error: 'unauthorized',
  });
  assert.equal(reflectionAttempts, 0);
  assert.equal(providerDispatches, 0);
});

test('AMD qualification and its waiter require a completed caller turn and real Dexter playback', async () => {
  const machine = new HarkHumanTurnQualifier('machine_end_beep');
  assert.equal(machine.canAttemptConversation(), false);
  assert.equal(machine.observe({ itemId: 'machine-1', transcript: 'Yes, proceed.', echo: false }), false);
  assert.equal(machine.isQualified(), false);
  assert.equal(await machine.waitForQualifiedInvocation({
    providerInvocationId: 'machine-provider-1',
    userItemId: 'machine-1',
    timeoutMs: 250,
  }), null);

  const voicemail = new HarkHumanTurnQualifier('unknown');
  voicemail.noteAssistantPlaybackStarted();
  voicemail.noteCallerSpeechStarted('voicemail-vad-1');
  assert.equal(voicemail.observe({
    itemId: 'voicemail-vad-1',
    transcript: 'Hello, you have reached Jordan.',
    echo: false,
  }), false);
  // Finishing audio that began before the first fragment is not a reply turn.
  voicemail.noteAssistantPlaybackCompleted();
  voicemail.noteCallerSpeechStarted('voicemail-vad-2');
  assert.equal(voicemail.observe({
    itemId: 'voicemail-vad-2',
    transcript: 'I cannot take your call right now.',
    echo: false,
  }), false);
  voicemail.noteCallerSpeechStarted('voicemail-vad-3');
  assert.equal(voicemail.observe({
    itemId: 'voicemail-vad-3',
    transcript: 'Please leave a message after the tone.',
    echo: false,
  }), false);
  assert.equal(voicemail.isQualified(), false);

  const unknown = new HarkHumanTurnQualifier('unknown');
  assert.equal(unknown.canAttemptConversation(), true);
  unknown.noteAssistantPlaybackStarted();
  unknown.noteAssistantPlaybackCompleted();
  unknown.noteCallerSpeechStarted('unknown-1');
  assert.equal(unknown.observe({ itemId: 'unknown-1', transcript: 'Hello?', echo: false }), false);
  const unknownDecisionWait = unknown.waitForQualifiedInvocation({
    providerInvocationId: 'unknown-provider-1',
    userItemId: 'unknown-2',
    timeoutMs: 250,
  });
  assert.equal(unknown.observe({ itemId: 'unknown-1', transcript: 'Yes?', echo: false }), false);
  unknown.noteCallerSpeechStarted('unknown-vad-2');
  assert.equal(unknown.observe({ itemId: 'unknown-vad-2', transcript: 'Can you hear me?', echo: false }), false);
  unknown.noteCallerSpeechStarted('unknown-voicemail');
  assert.equal(unknown.observe({
    itemId: 'unknown-voicemail',
    transcript: 'Please leave a message after the tone.',
    echo: false,
  }), false);
  unknown.noteCallerSpeechStarted('unknown-echo');
  assert.equal(unknown.observe({ itemId: 'unknown-echo', transcript: 'What happened?', echo: true }), false);
  assert.equal(unknown.isQualified(), false);
  unknown.noteAssistantPlaybackStarted();
  unknown.noteAssistantPlaybackCompleted();
  assert.equal(unknown.observe({ itemId: 'unknown-vad-2', transcript: 'Can you hear me?', echo: false }), false);
  unknown.noteCallerSpeechStarted('unknown-2');
  assert.equal(unknown.observe({ itemId: 'unknown-2', transcript: 'What happened?', echo: false }), true);
  assert.equal(await unknownDecisionWait, 'unknown-2');
  assert.equal(unknown.isQualified(), true);
  assert.equal(unknown.currentHumanItemId(), 'unknown-2');
});

test('unknown AMD uses playback state at speech start, not delayed ASR completion time', () => {
  const unknown = new HarkHumanTurnQualifier('unknown');
  unknown.noteCallerSpeechStarted('pre-playback-a');
  assert.equal(unknown.observe({
    itemId: 'pre-playback-a',
    transcript: 'Hello?',
    echo: false,
  }), false);

  // The second utterance also began before Dexter played anything. Its ASR
  // completion arrives late, after playback, and must not manufacture a
  // between-turn playback boundary.
  unknown.noteCallerSpeechStarted('pre-playback-b');
  unknown.noteAssistantPlaybackStarted();
  unknown.noteAssistantPlaybackCompleted();
  assert.equal(unknown.observe({
    itemId: 'pre-playback-b',
    transcript: 'Are you there?',
    echo: false,
  }), false);
  assert.equal(unknown.isQualified(), false);

  unknown.noteCallerSpeechStarted('post-playback-c');
  assert.equal(unknown.observe({
    itemId: 'post-playback-c',
    transcript: 'What happened to the price?',
    echo: false,
  }), true);
  assert.equal(unknown.isQualified(), true);
});

test('caller item ordering advances only on known speech starts', () => {
  const tracker = new HarkCallerItemTracker();

  assert.equal(tracker.noteTranscriptionCompleted('orphan-item'), false);
  assert.equal(tracker.currentItemId(), null);
  assert.equal(tracker.noteSpeechStarted('orphan-item'), false);
  assert.equal(tracker.currentItemId(), null);

  assert.equal(tracker.noteSpeechStarted('caller-item-a'), true);
  assert.equal(tracker.currentItemId(), 'caller-item-a');
  assert.equal(tracker.noteTranscriptionCompleted('completion-without-start'), false);
  assert.equal(tracker.currentItemId(), null);
  assert.equal(tracker.noteSpeechStarted('caller-item-a'), false);
  assert.equal(tracker.currentItemId(), null);
  assert.equal(tracker.noteSpeechStarted('caller-item-b'), true);
  assert.equal(tracker.currentItemId(), 'caller-item-b');
});

test('completion without an ordered speech start cannot qualify or acknowledge a Hark caller', () => {
  const tracker = new HarkCallerItemTracker();
  const human = new HarkHumanTurnQualifier('human');
  const orderedCompletion = tracker.noteTranscriptionCompleted('orphan-caller-item');
  const shouldAcknowledge = orderedCompletion
    ? human.observe({
        itemId: 'orphan-caller-item',
        transcript: 'Dismiss it.',
        echo: false,
      })
    : false;

  assert.equal(orderedCompletion, false);
  assert.equal(shouldAcknowledge, false);
  assert.equal(human.isQualified(), false);
});

test('Realtime lineage resolves exact chains, waits for delayed raw items, and rejects broken chains', async () => {
  const tracker = new HarkCallerItemTracker();
  const lineage = new HarkRealtimeLineage();
  tracker.noteSpeechStarted('lineage-caller-a');
  tracker.noteSpeechStarted('lineage-caller-b');
  lineage.observe({
    type: 'input_audio_buffer.committed',
    item_id: 'lineage-caller-a',
    previous_item_id: null,
  });
  lineage.observe({
    type: 'conversation.item.added',
    previous_item_id: 'lineage-caller-a',
    item: { id: 'lineage-assistant-1', type: 'message', role: 'assistant' },
  });
  lineage.observe({
    type: 'input_audio_buffer.committed',
    item_id: 'lineage-caller-b',
    previous_item_id: 'lineage-assistant-1',
  });
  lineage.observe({
    type: 'conversation.item.added',
    previous_item_id: 'lineage-caller-a',
    item: {
      id: 'lineage-function-a',
      type: 'function_call',
      call_id: 'lineage-provider-a',
    },
  });

  assert.equal(lineage.resolveCausalCallerItemId(
    'lineage-provider-a',
    (itemId) => tracker.isOrderedItem(itemId),
  ), 'lineage-caller-a');
  assert.equal(lineage.resolveCausalCallerItemId(
    'missing-provider',
    (itemId) => tracker.isOrderedItem(itemId),
  ), null);

  const broken = new HarkRealtimeLineage();
  broken.observe({
    type: 'response.output_item.done',
    item: {
      id: 'broken-function',
      type: 'function_call',
      call_id: 'broken-provider',
    },
  });
  assert.equal(broken.resolveCausalCallerItemId(
    'broken-provider',
    (itemId) => tracker.isOrderedItem(itemId),
  ), null);
  assert.equal(await broken.waitForCausalCallerItemId(
    'broken-provider',
    (itemId) => tracker.isOrderedItem(itemId),
    10,
  ), null);

  const delayed = new HarkRealtimeLineage();
  const delayedResolution = delayed.waitForCausalCallerItemId(
    'delayed-provider',
    (itemId) => tracker.isOrderedItem(itemId),
    100,
  );
  await new Promise<void>((resolve) => setImmediate(resolve));
  delayed.observe({
    type: 'input_audio_buffer.committed',
    item_id: 'lineage-caller-b',
    previous_item_id: null,
  });
  delayed.observe({
    type: 'conversation.item.added',
    previous_item_id: 'lineage-caller-b',
    item: {
      id: 'delayed-function',
      type: 'function_call',
      call_id: 'delayed-provider',
    },
  });
  assert.equal(await delayedResolution, 'lineage-caller-b');
});

test('delayed completion A cannot retarget a tool emitted from later speech-start B', async (t) => {
  const store = temporaryStore(t);
  let requestCount = 0;
  const bridge = new HarkResponseBridge({
    store,
    apiBaseUrl: 'http://api.test',
    signingSecret: CALLBACK_SECRET,
    fetchImpl: (async (_input: string | URL | Request, init?: RequestInit) => {
      requestCount += 1;
      return successfulApiResponse(callbackRequestBody(init), {
        continuation: null,
        caseState: 'waiting_owner',
      });
    }) as typeof fetch,
    retryInProcess: false,
  });
  const tracker = new HarkCallerItemTracker();
  const lineage = new HarkRealtimeLineage();
  const human = new HarkHumanTurnQualifier('human');
  const responseTool = bridge.createRespondToHarkTool(
    callBinding(),
    undefined,
    human,
    (providerInvocationId) => lineage.waitForCausalCallerItemId(
      providerInvocationId,
      (itemId) => tracker.isOrderedItem(itemId),
      100,
    ),
    250,
  ) as any;
  const input = JSON.stringify({
    decisionType: 'dismiss',
    text: 'Dismiss this alert.',
    correctionOfPrevious: false,
  });

  assert.equal(tracker.noteSpeechStarted('caller-item-a'), true);
  lineage.observe({
    type: 'input_audio_buffer.committed',
    item_id: 'caller-item-a',
    previous_item_id: null,
  });
  assert.equal(tracker.noteSpeechStarted('caller-item-b'), true);
  lineage.observe({
    type: 'input_audio_buffer.committed',
    item_id: 'caller-item-b',
    previous_item_id: 'caller-item-a',
  });
  assert.equal(tracker.noteTranscriptionCompleted('caller-item-a'), true);
  assert.equal(human.observe({
    itemId: 'caller-item-a',
    transcript: 'Let me think.',
    echo: false,
  }), true);
  assert.equal(tracker.currentItemId(), 'caller-item-b');
  let settled = false;
  const pendingB = responseTool.invoke(undefined, input, {
    toolCall: {
      callId: 'provider-tool-call-ordered-b',
      name: 'respond_to_hark',
      arguments: input,
    },
  });
  void pendingB.then(
    () => { settled = true; },
    () => { settled = true; },
  );
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(settled, false);
  assert.equal(requestCount, 0);
  lineage.observe({
    type: 'conversation.item.added',
    previous_item_id: 'caller-item-b',
    item: {
      id: 'function-item-ordered-b',
      type: 'function_call',
      call_id: 'provider-tool-call-ordered-b',
    },
  });
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(settled, false);

  assert.equal(tracker.noteTranscriptionCompleted('caller-item-b'), true);
  assert.equal(human.observe({
    itemId: 'caller-item-b',
    transcript: 'Dismiss it.',
    echo: false,
  }), false);
  const delivered = JSON.parse(await pendingB);
  assert.equal(delivered.delivery, 'delivered');
  assert.equal(requestCount, 1);
});

test('completion A wakes its claimed waiter after B starts without waking B', async (t) => {
  const store = temporaryStore(t);
  let requestCount = 0;
  const bridge = new HarkResponseBridge({
    store,
    apiBaseUrl: 'http://api.test',
    signingSecret: CALLBACK_SECRET,
    fetchImpl: (async (_input: string | URL | Request, init?: RequestInit) => {
      requestCount += 1;
      return successfulApiResponse(callbackRequestBody(init), {
        continuation: null,
        caseState: 'waiting_owner',
      });
    }) as typeof fetch,
    retryInProcess: false,
  });
  const tracker = new HarkCallerItemTracker();
  const lineage = new HarkRealtimeLineage();
  const human = new HarkHumanTurnQualifier('human');
  const responseTool = bridge.createRespondToHarkTool(
    callBinding(),
    undefined,
    human,
    (providerInvocationId) => lineage.resolveCausalCallerItemId(
      providerInvocationId,
      (itemId) => tracker.isOrderedItem(itemId),
    ),
    250,
  ) as any;
  const input = JSON.stringify({
    decisionType: 'reply',
    text: 'Use this exact caller turn.',
    correctionOfPrevious: false,
  });

  tracker.noteSpeechStarted('claimed-caller-item-a');
  lineage.observe({
    type: 'input_audio_buffer.committed',
    item_id: 'claimed-caller-item-a',
    previous_item_id: null,
  });
  lineage.observe({
    type: 'conversation.item.added',
    previous_item_id: 'claimed-caller-item-a',
    item: {
      id: 'function-item-claimed-a',
      type: 'function_call',
      call_id: 'provider-tool-call-claimed-a',
    },
  });
  const pendingA = responseTool.invoke(undefined, input, {
    toolCall: {
      callId: 'provider-tool-call-claimed-a',
      name: 'respond_to_hark',
      arguments: input,
    },
  });
  await new Promise<void>((resolve) => setImmediate(resolve));

  tracker.noteSpeechStarted('claimed-caller-item-b');
  lineage.observe({
    type: 'input_audio_buffer.committed',
    item_id: 'claimed-caller-item-b',
    previous_item_id: 'function-item-claimed-a',
  });
  lineage.observe({
    type: 'conversation.item.added',
    previous_item_id: 'claimed-caller-item-b',
    item: {
      id: 'function-item-claimed-b',
      type: 'function_call',
      call_id: 'provider-tool-call-claimed-b',
    },
  });
  let bSettled = false;
  const pendingB = responseTool.invoke(undefined, input, {
    toolCall: {
      callId: 'provider-tool-call-claimed-b',
      name: 'respond_to_hark',
      arguments: input,
    },
  });
  void pendingB.then(
    () => { bSettled = true; },
    () => { bSettled = true; },
  );
  await new Promise<void>((resolve) => setImmediate(resolve));

  assert.equal(tracker.noteTranscriptionCompleted('claimed-caller-item-a'), true);
  assert.equal(human.observe({
    itemId: 'claimed-caller-item-a',
    transcript: 'Use the first answer.',
    echo: false,
  }), true);
  const deliveredA = JSON.parse(await pendingA);
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(deliveredA.delivery, 'delivered');
  assert.equal(tracker.currentItemId(), 'claimed-caller-item-b');
  assert.equal(bSettled, false);
  assert.equal(requestCount, 1);

  assert.equal(tracker.noteTranscriptionCompleted('claimed-caller-item-b'), true);
  assert.equal(human.observe({
    itemId: 'claimed-caller-item-b',
    transcript: 'Use the second answer.',
    echo: false,
  }), false);
  const deliveredB = JSON.parse(await pendingB);
  assert.equal(deliveredB.delivery, 'delivered');
  assert.equal(requestCount, 2);

  assert.equal(tracker.noteTranscriptionCompleted('unstarted-after-delivery'), false);
  assert.equal(tracker.currentItemId(), null);
  const replayB = JSON.parse(await responseTool.invoke(undefined, input, {
    toolCall: {
      callId: 'provider-tool-call-claimed-b',
      name: 'respond_to_hark',
      arguments: input,
    },
  }));
  assert.equal(replayB.replay, true);
  assert.equal(requestCount, 2);
});

test('respond_to_hark awaits its exact delayed caller transcript and replays the provider invocation', async (t) => {
  const store = temporaryStore(t);
  let requestCount = 0;
  const bridge = new HarkResponseBridge({
    store,
    apiBaseUrl: 'http://api.test',
    signingSecret: CALLBACK_SECRET,
    fetchImpl: (async (_input: string | URL | Request, init?: RequestInit) => {
      requestCount += 1;
      return successfulApiResponse(callbackRequestBody(init), {
        continuation: null,
        caseState: 'waiting_owner',
      });
    }) as typeof fetch,
    retryInProcess: false,
  });
  const human = new HarkHumanTurnQualifier('human');
  let pendingHumanItemId: string | null = 'completed-human-item-1';
  const responseTool = bridge.createRespondToHarkTool(
    callBinding(),
    undefined,
    human,
    () => pendingHumanItemId,
    250,
  ) as any;
  const input = JSON.stringify({
    decisionType: 'dismiss',
    text: 'Dismiss this alert.',
    correctionOfPrevious: false,
  });
  const details = {
    toolCall: {
      callId: 'provider-tool-call-live-1',
      name: 'respond_to_hark',
      arguments: input,
    },
  };

  const pendingDelivery = responseTool.invoke(undefined, input, details);
  await new Promise<void>((resolve) => setImmediate(resolve));

  const competingInvocation = responseTool.invoke(undefined, input, {
    toolCall: {
      ...details.toolCall,
      callId: 'provider-tool-call-live-2',
    },
  });

  assert.equal(human.observe({
    itemId: 'completed-human-item-1',
    transcript: 'Dismiss this one.',
    echo: false,
  }), true);
  const delivered = JSON.parse(await pendingDelivery);
  const blockedCompetitor = JSON.parse(await competingInvocation);
  assert.equal(delivered.delivery, 'delivered');
  assert.equal(delivered.replay, false);
  assert.equal(blockedCompetitor.recorded, false);
  assert.equal(blockedCompetitor.delivery, 'blocked');

  const missingProviderIdentity = JSON.parse(await responseTool.invoke(undefined, input, {
    toolCall: { callId: '', name: 'respond_to_hark', arguments: input },
  }));
  assert.equal(missingProviderIdentity.recorded, false);
  assert.equal(missingProviderIdentity.delivery, 'blocked');
  pendingHumanItemId = 'later-human-item-2';
  assert.equal(human.observe({
    itemId: 'later-human-item-2',
    transcript: 'Anything else?',
    echo: false,
  }), false);
  const replay = JSON.parse(await responseTool.invoke(undefined, input, details));
  assert.equal(replay.replay, true);
  assert.equal(requestCount, 1);
});

test('respond_to_hark times out fail-closed when the bound caller turn is unqualified', async (t) => {
  const store = temporaryStore(t);
  let requestCount = 0;
  const bridge = new HarkResponseBridge({
    store,
    apiBaseUrl: 'http://api.test',
    signingSecret: CALLBACK_SECRET,
    fetchImpl: (async (_input: string | URL | Request, init?: RequestInit) => {
      requestCount += 1;
      return successfulApiResponse(callbackRequestBody(init), {
        continuation: null,
        caseState: 'waiting_owner',
      });
    }) as typeof fetch,
    retryInProcess: false,
  });
  const human = new HarkHumanTurnQualifier('human');
  const responseTool = bridge.createRespondToHarkTool(
    callBinding(),
    undefined,
    human,
    () => 'unqualified-human-item-1',
    20,
  ) as any;
  const input = JSON.stringify({
    decisionType: 'dismiss',
    text: 'Dismiss this alert.',
    correctionOfPrevious: false,
  });
  const pending = responseTool.invoke(undefined, input, {
    toolCall: {
      callId: 'provider-tool-call-timeout-1',
      name: 'respond_to_hark',
      arguments: input,
    },
  });
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(human.observe({
    itemId: 'unqualified-human-item-1',
    transcript: 'Please leave a message after the tone.',
    echo: false,
  }), false);

  const blocked = JSON.parse(await pending);
  assert.equal(blocked.recorded, false);
  assert.equal(blocked.delivery, 'blocked');
  assert.equal(human.isQualified(), false);
  assert.equal(requestCount, 0);
});

test('response 2xx requires exact IDs, case state, and a valid raw continuation before delivery', async (t) => {
  const store = temporaryStore(t);
  let attempt = 0;
  const bridge = new HarkResponseBridge({
    store,
    apiBaseUrl: 'http://api.test',
    signingSecret: CALLBACK_SECRET,
    fetchImpl: (async (_input: string | URL | Request, init?: RequestInit) => {
      attempt += 1;
      const body = callbackRequestBody(init);
      if (attempt === 1) {
        return new Response(JSON.stringify({ ok: true }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        });
      }
      if (attempt === 2) {
        return new Response(JSON.stringify({
          v: 'hark.phone-callback-result.v1',
          ok: false,
          replay: false,
          responseId: null,
          caseState: null,
          continuation: null,
        }), { status: 200, headers: { 'content-type': 'application/json' } });
      }
      if (attempt === 3) return successfulStatusApiResponse(body);
      if (attempt === 4) {
        const continuation: any = rawApiContinuation(body);
        continuation.idempotencyKey = '99999999-9999-4999-8999-999999999999';
        return successfulApiResponse(body, {
          continuation,
        });
      }
      return successfulApiResponse(body, { continuation: null, caseState: 'waiting_owner' });
    }) as typeof fetch,
    retryInProcess: false,
  });

  const first = await bridge.submitResponse(callBinding(), {
    type: 'dismiss',
    text: 'Dismiss this alert.',
  }, invocation(12));
  assert.equal(first.state, 'pending');
  assert.match(first.error || '', /invalid hark\.phone-callback-result\.v1/);

  await bridge.flushPending();
  let persisted = store.getHarkOutbox(`response:${first.sourceId}`);
  assert.equal(persisted?.state, 'pending');
  assert.equal(persisted?.attempts, 2);

  await bridge.flushPending();
  persisted = store.getHarkOutbox(`response:${first.sourceId}`);
  assert.equal(persisted?.state, 'pending');
  assert.equal(persisted?.attempts, 3);

  await bridge.flushPending();
  persisted = store.getHarkOutbox(`response:${first.sourceId}`);
  assert.equal(persisted?.state, 'pending');
  assert.equal(persisted?.attempts, 4);

  await bridge.flushPending();
  persisted = store.getHarkOutbox(`response:${first.sourceId}`);
  assert.equal(persisted?.state, 'delivered');
  assert.equal(persisted?.attempts, 5);
});

test('401/403 auth skew survives restart and drains one exact predecessor, correction, and continuation each', async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'dexter-hark-correction-restart-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const statePath = path.join(directory, 'state.json');
  const unavailableBodies: any[] = [];
  const authSkewStatuses = [401, 403];
  const firstStore = new OutboundAlertStore(statePath);
  const firstBridge = new HarkResponseBridge({
    store: firstStore,
    apiBaseUrl: 'http://api.test',
    signingSecret: CALLBACK_SECRET,
    fetchImpl: (async (_input: string | URL | Request, init?: RequestInit) => {
      const body = callbackRequestBody(init);
      unavailableBodies.push(body);
      return new Response(JSON.stringify({ ok: false, error: 'invalid_signature' }), {
        status: authSkewStatuses[unavailableBodies.length - 1] ?? 403,
        headers: { 'content-type': 'application/json' },
      });
    }) as typeof fetch,
    retryInProcess: false,
  });

  const predecessor = await firstBridge.submitResponse(callBinding(), {
    type: 'reply',
    text: 'Buy $100.',
  }, invocation(13));
  const correction = await firstBridge.submitResponse(callBinding(), {
    type: 'instruction',
    text: 'Actually, do not buy it.',
  }, invocation(14), true);

  assert.equal(predecessor.state, 'pending');
  assert.equal(correction.state, 'pending');
  assert.equal(unavailableBodies.length, 2);
  assert.equal(unavailableBodies.every(
    (body) => body.sourceResponseId === predecessor.sourceId,
  ), true);
  assert.equal(
    firstStore.getHarkOutbox(`response:${predecessor.sourceId}`)?.attempts,
    2,
  );

  const correctionEntry = firstStore.getHarkOutbox(`response:${correction.sourceId}`);
  assert.equal(correctionEntry?.dependsOnId, `response:${predecessor.sourceId}`);
  assert.equal(correctionEntry?.attempts, 0);
  assert.equal(
    (correctionEntry?.body as Record<string, unknown> | undefined)?.supersedesResponseId,
    null,
  );
  const placeholderBodyHash = correctionEntry?.bodyHash;

  const predecessorApiResponseId = '99999999-9999-4999-8999-999999999991';
  const correctionApiResponseId = '99999999-9999-4999-8999-999999999992';
  const predecessorContinuationId = '99999999-9999-4999-8999-999999999993';
  const correctionContinuationId = '99999999-9999-4999-8999-999999999994';
  const restartedBodies: any[] = [];
  const restartedStore = new OutboundAlertStore(statePath);
  const restartedBridge = new HarkResponseBridge({
    store: restartedStore,
    apiBaseUrl: 'http://api.test',
    signingSecret: CALLBACK_SECRET,
    fetchImpl: (async (_input: string | URL | Request, init?: RequestInit) => {
      const body = callbackRequestBody(init);
      restartedBodies.push(body);
      const predecessorBody = body.sourceResponseId === predecessor.sourceId;
      const responseId = predecessorBody
        ? predecessorApiResponseId
        : correctionApiResponseId;
      const continuation = rawApiContinuation(body, responseId);
      continuation.continuationId = predecessorBody
        ? predecessorContinuationId
        : correctionContinuationId;
      continuation.idempotencyKey = continuation.continuationId;
      return successfulApiResponse(body, {
        responseId,
        continuation,
        caseState: 'resuming',
      });
    }) as typeof fetch,
    retryInProcess: false,
  });
  await restartedBridge.flushPending();
  await restartedBridge.flushPending();

  assert.equal(restartedBodies.length, 2);
  assert.equal(restartedBodies[0]!.sourceResponseId, predecessor.sourceId);
  assert.equal(restartedBodies[1]!.sourceResponseId, correction.sourceId);
  assert.equal(restartedBodies[1]!.supersedesResponseId, predecessorApiResponseId);
  assert.equal(
    restartedStore.getHarkOutbox(`response:${predecessor.sourceId}`)?.state,
    'delivered',
  );
  assert.deepEqual(
    restartedStore.getHarkOutbox(`response:${predecessor.sourceId}`)?.continuationResult,
    {
      v: 'hark.phone-callback-result.v1',
      ok: true,
      replay: false,
      responseId: predecessorApiResponseId,
      caseId: CASE_ID,
      eventId: EVENT_ID,
      deliveryId: DELIVERY_ID,
      intentId: INTENT_ID,
      callSid: CALL_SID,
      originTaskId: 'hermes-task-42',
      sourceResponseId: predecessor.sourceId,
      caseState: 'resuming',
      continuation: {
        id: predecessorContinuationId,
        originTaskId: 'hermes-task-42',
      },
    },
  );
  const hydratedCorrection = restartedStore.getHarkOutbox(`response:${correction.sourceId}`);
  assert.equal(hydratedCorrection?.state, 'delivered');
  assert.equal(
    (hydratedCorrection?.body as Record<string, unknown> | undefined)?.supersedesResponseId,
    predecessorApiResponseId,
  );
  assert.notEqual(hydratedCorrection?.bodyHash, placeholderBodyHash);
  assert.equal(
    hydratedCorrection?.bodyHash,
    sha256Hex(canonicalJson(hydratedCorrection?.body)),
  );
  assert.deepEqual(
    hydratedCorrection?.continuationResult && 'continuation' in hydratedCorrection.continuationResult
      ? hydratedCorrection.continuationResult.continuation
      : null,
    { id: correctionContinuationId, originTaskId: 'hermes-task-42' },
  );

  const replay = await restartedBridge.submitResponse(callBinding(), {
    type: 'instruction',
    text: 'Actually, do not buy it.',
  }, invocation(14), true);
  assert.equal(replay.replay, true);
  assert.equal(replay.sourceId, correction.sourceId);
  assert.deepEqual(
    replay.continuationResult && 'continuation' in replay.continuationResult
      ? replay.continuationResult.continuation
      : null,
    { id: correctionContinuationId, originTaskId: 'hermes-task-42' },
  );
  assert.equal(restartedBodies.length, 2);
});

test('a correction remains durable and unsent while its predecessor lacks a valid API UUID', async (t) => {
  const store = temporaryStore(t);
  const requestBodies: any[] = [];
  const bridge = new HarkResponseBridge({
    store,
    apiBaseUrl: 'http://api.test',
    signingSecret: CALLBACK_SECRET,
    fetchImpl: (async (_input: string | URL | Request, init?: RequestInit) => {
      const body = callbackRequestBody(init);
      requestBodies.push(body);
      return successfulApiResponse(body, {
        responseId: 'not-an-api-response-uuid',
        continuation: null,
      });
    }) as typeof fetch,
    retryInProcess: false,
  });

  const predecessor = await bridge.submitResponse(callBinding(), {
    type: 'reply',
    text: 'Buy $100.',
  }, invocation(700));
  const correction = await bridge.submitResponse(callBinding(), {
    type: 'instruction',
    text: 'Actually, do not buy it.',
  }, invocation(701), true);

  assert.equal(predecessor.state, 'pending');
  assert.equal(correction.state, 'pending');
  assert.equal(requestBodies.length, 2);
  assert.equal(requestBodies.every(
    (body) => body.sourceResponseId === predecessor.sourceId,
  ), true);
  const persisted = store.getHarkOutbox(`response:${correction.sourceId}`);
  assert.equal(persisted?.state, 'pending');
  assert.equal(persisted?.attempts, 0);
  assert.equal(
    (persisted?.body as Record<string, unknown> | undefined)?.supersedesResponseId,
    null,
  );
  assert.match(
    store.getHarkOutbox(`response:${predecessor.sourceId}`)?.lastError || '',
    /invalid hark\.phone-callback-result\.v1/,
  );
});

test('flushPending drains more than one hundred durable entries in bounded batches', async (t) => {
  const store = temporaryStore(t);
  const ids: string[] = [];
  for (let index = 0; index < 205; index += 1) {
    const eventId = randomUUID();
    const deliveryId = randomUUID();
    const intentId = randomUUID();
    const sourceEventId = `bulk-phone-status-source-${index}`;
    const id = `status:${sourceEventId}`;
    ids.push(id);
    const binding = {
      schemaVersion: 1 as const,
      caseId: randomUUID(),
      watchId: randomUUID(),
      eventId,
      deliveryId,
      originTaskId: `bulk-task-${index}`,
    };
    const reserved = store.reserveHarkOutbox({
      id,
      intentId,
      kind: 'status',
      binding,
      body: {
        v: 'hark.phone-status.v1',
        sourceEventId,
        caseId: binding.caseId,
        eventId,
        deliveryId,
        intentId,
        callSid: `CA${index.toString(16).padStart(32, '0')}`,
        originTaskId: binding.originTaskId,
        status: 'streaming',
        observedAt: '2026-08-04T07:05:00.000Z',
      },
    });
    assert.equal(reserved.kind, 'new');
  }

  let requestCount = 0;
  const bridge = new HarkResponseBridge({
    store,
    apiBaseUrl: 'http://api.test',
    signingSecret: CALLBACK_SECRET,
    fetchImpl: (async (_input: string | URL | Request, init?: RequestInit) => {
      requestCount += 1;
      return successfulStatusApiResponse(callbackRequestBody(init));
    }) as typeof fetch,
    retryInProcess: false,
  });

  await bridge.flushPending(100, 10);
  assert.equal(requestCount, 205);
  assert.equal(store.listPendingHarkOutbox(1).length, 0);
  assert.equal(ids.every((id) => store.getHarkOutbox(id)?.state === 'delivered'), true);
});
