import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';
import test from 'node:test';
import ts from 'typescript';
import {
  callerIntentInputDigest, callerIntentV1InputDigest, callerIntentV2InputDigest,
  callerIntentInputSchema, callerIntentResponseSchema, callerIntentV1InputSchema,
  callerIntentV1ResponseSchema, callerIntentV2InputSchema, callerIntentV2ResponseSchema,
  callerIntentOperationSchema, validateCallerIntentResponse,
  type CallerIntentV2Input, type CallerIntentResponse,
} from '../src/broker/callerIntentContract.js';

type Literal = { name: string; operationId: string; bindingId: string; input: unknown;
  orderedPreimage: unknown[]; jsonPreimage: string; sha256: string };
// Exact literal values from shared fixture aac3535005198cf1270e02cf5cbc472fe8bfa1ad58917d8bdba504fa573b2e77.
// The fixture is copied here so all digest and coverage cases run without an API checkout.
const literals: { cases: Literal[]; rejections: { name: string; input: unknown }[];
  v: string; proofBoundary: string; domain: string; maxPreparedMessageBytes: number } = {
  "v": "phone.caller-intent.v2.literal-fixtures.v1",
  "proofBoundary": "Literal wire and SHA-256 vectors only; no provider classification or authority proof.",
  "domain": "dexter-phone-caller-intent-input/v2",
  "maxPreparedMessageBytes": 24000,
  "cases": [
    {
      "name": "empty_complete",
      "operationId": "00000011-0000-4000-8000-000000000000",
      "bindingId": "00000001-0000-4000-8000-000000000000",
      "input": {
        "v": "phone.caller-intent.v2",
        "source": {
          "callSid": "CAaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          "sourceEventId": "00000012-0000-4000-8000-000000000000",
          "itemId": "item_1",
          "revision": 1,
          "receivedAt": "2026-09-24T17:00:00.000Z",
          "role": "user",
          "text": "  Keep café and café separate.\nThen answer.  "
        },
        "generationId": "00000013-0000-4000-8000-000000000000",
        "sourceSessionId": "session opaque 1",
        "brokerInputDigest": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "presentationDigest": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        "callDeadlineAt": "2026-09-24T17:00:30.000Z",
        "version": {
          "ownerPrincipalId": "00000014-0000-4000-8000-000000000000",
          "primaryDexterId": "00000015-0000-4000-8000-000000000000",
          "threadId": "00000016-0000-4000-8000-000000000000",
          "contextEpoch": "4",
          "threadEventSequence": "27",
          "digest": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        },
        "questions": [],
        "results": [],
        "resultCoverage": {
          "state": "complete",
          "count": 0
        }
      },
      "orderedPreimage": [
        "dexter-phone-caller-intent-input/v2",
        "00000011-0000-4000-8000-000000000000",
        "00000001-0000-4000-8000-000000000000",
        [
          "CAaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          "00000012-0000-4000-8000-000000000000",
          "item_1",
          1,
          "2026-09-24T17:00:00.000Z",
          "user",
          "  Keep café and café separate.\nThen answer.  "
        ],
        "00000013-0000-4000-8000-000000000000",
        "session opaque 1",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        "2026-09-24T17:00:30.000Z",
        [
          "00000014-0000-4000-8000-000000000000",
          "00000015-0000-4000-8000-000000000000",
          "00000016-0000-4000-8000-000000000000",
          "4",
          "27",
          "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        ],
        [],
        [],
        [
          "complete",
          0
        ]
      ],
      "jsonPreimage": "[\"dexter-phone-caller-intent-input/v2\",\"00000011-0000-4000-8000-000000000000\",\"00000001-0000-4000-8000-000000000000\",[\"CAaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"00000012-0000-4000-8000-000000000000\",\"item_1\",1,\"2026-09-24T17:00:00.000Z\",\"user\",\"  Keep café and café separate.\\nThen answer.  \"],\"00000013-0000-4000-8000-000000000000\",\"session opaque 1\",\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",\"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc\",\"2026-09-24T17:00:30.000Z\",[\"00000014-0000-4000-8000-000000000000\",\"00000015-0000-4000-8000-000000000000\",\"00000016-0000-4000-8000-000000000000\",\"4\",\"27\",\"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\"],[],[],[\"complete\",0]]",
      "sha256": "ed72cfb9466e3cf3a847fcf67df4a2095d9e1b29cb9474dcf04c1506d98e23df"
    },
    {
      "name": "complete_seven",
      "operationId": "00000011-0000-4000-8000-000000000000",
      "bindingId": "00000001-0000-4000-8000-000000000000",
      "input": {
        "v": "phone.caller-intent.v2",
        "source": {
          "callSid": "CAaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          "sourceEventId": "00000012-0000-4000-8000-000000000000",
          "itemId": "item_1",
          "revision": 1,
          "receivedAt": "2026-09-24T17:00:00.000Z",
          "role": "user",
          "text": "  Keep café and café separate.\nThen answer.  "
        },
        "generationId": "00000013-0000-4000-8000-000000000000",
        "sourceSessionId": "session opaque 1",
        "brokerInputDigest": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "presentationDigest": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        "callDeadlineAt": "2026-09-24T17:00:30.000Z",
        "version": {
          "ownerPrincipalId": "00000014-0000-4000-8000-000000000000",
          "primaryDexterId": "00000015-0000-4000-8000-000000000000",
          "threadId": "00000016-0000-4000-8000-000000000000",
          "contextEpoch": "4",
          "threadEventSequence": "27",
          "digest": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        },
        "questions": [
          {
            "kind": "ordinary",
            "execution": {
              "bindingId": "00000001-0000-4000-8000-000000000000",
              "requestId": "00000100-0000-4000-8000-000000000000",
              "runId": "00000101-0000-4000-8000-000000000000"
            },
            "question": {
              "id": "00000300-0000-4000-8000-000000000000",
              "revision": 1
            },
            "presentationId": "question ordinary"
          },
          {
            "kind": "selected",
            "execution": {
              "bindingId": "00000002-0000-4000-8000-000000000000",
              "requestId": "00000200-0000-4000-8000-000000000000",
              "runId": "00000201-0000-4000-8000-000000000000"
            },
            "question": {
              "id": "00000301-0000-4000-8000-000000000000",
              "revision": 1
            },
            "presentationId": "question selected",
            "deliveryAssociationId": "00000203-0000-4000-8000-000000000000"
          }
        ],
        "results": [
          {
            "kind": "ordinary",
            "execution": {
              "bindingId": "00000001-0000-4000-8000-000000000000",
              "requestId": "00000100-0000-4000-8000-000000000000",
              "runId": "00000101-0000-4000-8000-000000000000"
            },
            "assistantMessageId": "00000102-0000-4000-8000-000000000000",
            "presentationId": "result ordinary 0"
          },
          {
            "kind": "ordinary",
            "execution": {
              "bindingId": "00000001-0000-4000-8000-000000000000",
              "requestId": "00000110-0000-4000-8000-000000000000",
              "runId": "00000111-0000-4000-8000-000000000000"
            },
            "assistantMessageId": "00000112-0000-4000-8000-000000000000",
            "presentationId": "result ordinary 1"
          },
          {
            "kind": "ordinary",
            "execution": {
              "bindingId": "00000001-0000-4000-8000-000000000000",
              "requestId": "00000120-0000-4000-8000-000000000000",
              "runId": "00000121-0000-4000-8000-000000000000"
            },
            "assistantMessageId": "00000122-0000-4000-8000-000000000000",
            "presentationId": "result ordinary 2"
          },
          {
            "kind": "ordinary",
            "execution": {
              "bindingId": "00000001-0000-4000-8000-000000000000",
              "requestId": "00000130-0000-4000-8000-000000000000",
              "runId": "00000131-0000-4000-8000-000000000000"
            },
            "assistantMessageId": "00000132-0000-4000-8000-000000000000",
            "presentationId": "result ordinary 3"
          },
          {
            "kind": "ordinary",
            "execution": {
              "bindingId": "00000001-0000-4000-8000-000000000000",
              "requestId": "00000140-0000-4000-8000-000000000000",
              "runId": "00000141-0000-4000-8000-000000000000"
            },
            "assistantMessageId": "00000142-0000-4000-8000-000000000000",
            "presentationId": "result ordinary 4"
          },
          {
            "kind": "ordinary",
            "execution": {
              "bindingId": "00000001-0000-4000-8000-000000000000",
              "requestId": "00000150-0000-4000-8000-000000000000",
              "runId": "00000151-0000-4000-8000-000000000000"
            },
            "assistantMessageId": "00000152-0000-4000-8000-000000000000",
            "presentationId": "result ordinary 5"
          },
          {
            "kind": "selected",
            "execution": {
              "bindingId": "00000002-0000-4000-8000-000000000000",
              "requestId": "00000200-0000-4000-8000-000000000000",
              "runId": "00000201-0000-4000-8000-000000000000"
            },
            "assistantMessageId": "00000202-0000-4000-8000-000000000000",
            "presentationId": "result selected original",
            "deliveryAssociationId": "00000203-0000-4000-8000-000000000000"
          }
        ],
        "resultCoverage": {
          "state": "complete",
          "count": 7
        }
      },
      "orderedPreimage": [
        "dexter-phone-caller-intent-input/v2",
        "00000011-0000-4000-8000-000000000000",
        "00000001-0000-4000-8000-000000000000",
        [
          "CAaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          "00000012-0000-4000-8000-000000000000",
          "item_1",
          1,
          "2026-09-24T17:00:00.000Z",
          "user",
          "  Keep café and café separate.\nThen answer.  "
        ],
        "00000013-0000-4000-8000-000000000000",
        "session opaque 1",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        "2026-09-24T17:00:30.000Z",
        [
          "00000014-0000-4000-8000-000000000000",
          "00000015-0000-4000-8000-000000000000",
          "00000016-0000-4000-8000-000000000000",
          "4",
          "27",
          "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        ],
        [
          [
            "ordinary",
            "00000001-0000-4000-8000-000000000000",
            "00000100-0000-4000-8000-000000000000",
            "00000101-0000-4000-8000-000000000000",
            "00000300-0000-4000-8000-000000000000",
            1,
            "question ordinary",
            null
          ],
          [
            "selected",
            "00000002-0000-4000-8000-000000000000",
            "00000200-0000-4000-8000-000000000000",
            "00000201-0000-4000-8000-000000000000",
            "00000301-0000-4000-8000-000000000000",
            1,
            "question selected",
            "00000203-0000-4000-8000-000000000000"
          ]
        ],
        [
          [
            "ordinary",
            "00000001-0000-4000-8000-000000000000",
            "00000100-0000-4000-8000-000000000000",
            "00000101-0000-4000-8000-000000000000",
            "00000102-0000-4000-8000-000000000000",
            "result ordinary 0",
            null
          ],
          [
            "ordinary",
            "00000001-0000-4000-8000-000000000000",
            "00000110-0000-4000-8000-000000000000",
            "00000111-0000-4000-8000-000000000000",
            "00000112-0000-4000-8000-000000000000",
            "result ordinary 1",
            null
          ],
          [
            "ordinary",
            "00000001-0000-4000-8000-000000000000",
            "00000120-0000-4000-8000-000000000000",
            "00000121-0000-4000-8000-000000000000",
            "00000122-0000-4000-8000-000000000000",
            "result ordinary 2",
            null
          ],
          [
            "ordinary",
            "00000001-0000-4000-8000-000000000000",
            "00000130-0000-4000-8000-000000000000",
            "00000131-0000-4000-8000-000000000000",
            "00000132-0000-4000-8000-000000000000",
            "result ordinary 3",
            null
          ],
          [
            "ordinary",
            "00000001-0000-4000-8000-000000000000",
            "00000140-0000-4000-8000-000000000000",
            "00000141-0000-4000-8000-000000000000",
            "00000142-0000-4000-8000-000000000000",
            "result ordinary 4",
            null
          ],
          [
            "ordinary",
            "00000001-0000-4000-8000-000000000000",
            "00000150-0000-4000-8000-000000000000",
            "00000151-0000-4000-8000-000000000000",
            "00000152-0000-4000-8000-000000000000",
            "result ordinary 5",
            null
          ],
          [
            "selected",
            "00000002-0000-4000-8000-000000000000",
            "00000200-0000-4000-8000-000000000000",
            "00000201-0000-4000-8000-000000000000",
            "00000202-0000-4000-8000-000000000000",
            "result selected original",
            "00000203-0000-4000-8000-000000000000"
          ]
        ],
        [
          "complete",
          7
        ]
      ],
      "jsonPreimage": "[\"dexter-phone-caller-intent-input/v2\",\"00000011-0000-4000-8000-000000000000\",\"00000001-0000-4000-8000-000000000000\",[\"CAaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"00000012-0000-4000-8000-000000000000\",\"item_1\",1,\"2026-09-24T17:00:00.000Z\",\"user\",\"  Keep café and café separate.\\nThen answer.  \"],\"00000013-0000-4000-8000-000000000000\",\"session opaque 1\",\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",\"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc\",\"2026-09-24T17:00:30.000Z\",[\"00000014-0000-4000-8000-000000000000\",\"00000015-0000-4000-8000-000000000000\",\"00000016-0000-4000-8000-000000000000\",\"4\",\"27\",\"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\"],[[\"ordinary\",\"00000001-0000-4000-8000-000000000000\",\"00000100-0000-4000-8000-000000000000\",\"00000101-0000-4000-8000-000000000000\",\"00000300-0000-4000-8000-000000000000\",1,\"question ordinary\",null],[\"selected\",\"00000002-0000-4000-8000-000000000000\",\"00000200-0000-4000-8000-000000000000\",\"00000201-0000-4000-8000-000000000000\",\"00000301-0000-4000-8000-000000000000\",1,\"question selected\",\"00000203-0000-4000-8000-000000000000\"]],[[\"ordinary\",\"00000001-0000-4000-8000-000000000000\",\"00000100-0000-4000-8000-000000000000\",\"00000101-0000-4000-8000-000000000000\",\"00000102-0000-4000-8000-000000000000\",\"result ordinary 0\",null],[\"ordinary\",\"00000001-0000-4000-8000-000000000000\",\"00000110-0000-4000-8000-000000000000\",\"00000111-0000-4000-8000-000000000000\",\"00000112-0000-4000-8000-000000000000\",\"result ordinary 1\",null],[\"ordinary\",\"00000001-0000-4000-8000-000000000000\",\"00000120-0000-4000-8000-000000000000\",\"00000121-0000-4000-8000-000000000000\",\"00000122-0000-4000-8000-000000000000\",\"result ordinary 2\",null],[\"ordinary\",\"00000001-0000-4000-8000-000000000000\",\"00000130-0000-4000-8000-000000000000\",\"00000131-0000-4000-8000-000000000000\",\"00000132-0000-4000-8000-000000000000\",\"result ordinary 3\",null],[\"ordinary\",\"00000001-0000-4000-8000-000000000000\",\"00000140-0000-4000-8000-000000000000\",\"00000141-0000-4000-8000-000000000000\",\"00000142-0000-4000-8000-000000000000\",\"result ordinary 4\",null],[\"ordinary\",\"00000001-0000-4000-8000-000000000000\",\"00000150-0000-4000-8000-000000000000\",\"00000151-0000-4000-8000-000000000000\",\"00000152-0000-4000-8000-000000000000\",\"result ordinary 5\",null],[\"selected\",\"00000002-0000-4000-8000-000000000000\",\"00000200-0000-4000-8000-000000000000\",\"00000201-0000-4000-8000-000000000000\",\"00000202-0000-4000-8000-000000000000\",\"result selected original\",\"00000203-0000-4000-8000-000000000000\"]],[\"complete\",7]]",
      "sha256": "493ab2ab9ac8bb14736e7cf9df10f2493142519e4463ca2d472becc58a8325fd"
    },
    {
      "name": "overflow_eight",
      "operationId": "00000011-0000-4000-8000-000000000000",
      "bindingId": "00000001-0000-4000-8000-000000000000",
      "input": {
        "v": "phone.caller-intent.v2",
        "source": {
          "callSid": "CAaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          "sourceEventId": "00000012-0000-4000-8000-000000000000",
          "itemId": "item_1",
          "revision": 1,
          "receivedAt": "2026-09-24T17:00:00.000Z",
          "role": "user",
          "text": "  Keep café and café separate.\nThen answer.  "
        },
        "generationId": "00000013-0000-4000-8000-000000000000",
        "sourceSessionId": "session opaque 1",
        "brokerInputDigest": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "presentationDigest": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        "callDeadlineAt": "2026-09-24T17:00:30.000Z",
        "version": {
          "ownerPrincipalId": "00000014-0000-4000-8000-000000000000",
          "primaryDexterId": "00000015-0000-4000-8000-000000000000",
          "threadId": "00000016-0000-4000-8000-000000000000",
          "contextEpoch": "4",
          "threadEventSequence": "27",
          "digest": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        },
        "questions": [
          {
            "kind": "ordinary",
            "execution": {
              "bindingId": "00000001-0000-4000-8000-000000000000",
              "requestId": "00000100-0000-4000-8000-000000000000",
              "runId": "00000101-0000-4000-8000-000000000000"
            },
            "question": {
              "id": "00000300-0000-4000-8000-000000000000",
              "revision": 1
            },
            "presentationId": "question ordinary"
          },
          {
            "kind": "selected",
            "execution": {
              "bindingId": "00000002-0000-4000-8000-000000000000",
              "requestId": "00000200-0000-4000-8000-000000000000",
              "runId": "00000201-0000-4000-8000-000000000000"
            },
            "question": {
              "id": "00000301-0000-4000-8000-000000000000",
              "revision": 1
            },
            "presentationId": "question selected",
            "deliveryAssociationId": "00000203-0000-4000-8000-000000000000"
          }
        ],
        "results": [],
        "resultCoverage": {
          "state": "overflow",
          "count": 8
        }
      },
      "orderedPreimage": [
        "dexter-phone-caller-intent-input/v2",
        "00000011-0000-4000-8000-000000000000",
        "00000001-0000-4000-8000-000000000000",
        [
          "CAaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          "00000012-0000-4000-8000-000000000000",
          "item_1",
          1,
          "2026-09-24T17:00:00.000Z",
          "user",
          "  Keep café and café separate.\nThen answer.  "
        ],
        "00000013-0000-4000-8000-000000000000",
        "session opaque 1",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        "2026-09-24T17:00:30.000Z",
        [
          "00000014-0000-4000-8000-000000000000",
          "00000015-0000-4000-8000-000000000000",
          "00000016-0000-4000-8000-000000000000",
          "4",
          "27",
          "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        ],
        [
          [
            "ordinary",
            "00000001-0000-4000-8000-000000000000",
            "00000100-0000-4000-8000-000000000000",
            "00000101-0000-4000-8000-000000000000",
            "00000300-0000-4000-8000-000000000000",
            1,
            "question ordinary",
            null
          ],
          [
            "selected",
            "00000002-0000-4000-8000-000000000000",
            "00000200-0000-4000-8000-000000000000",
            "00000201-0000-4000-8000-000000000000",
            "00000301-0000-4000-8000-000000000000",
            1,
            "question selected",
            "00000203-0000-4000-8000-000000000000"
          ]
        ],
        [],
        [
          "overflow",
          8
        ]
      ],
      "jsonPreimage": "[\"dexter-phone-caller-intent-input/v2\",\"00000011-0000-4000-8000-000000000000\",\"00000001-0000-4000-8000-000000000000\",[\"CAaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"00000012-0000-4000-8000-000000000000\",\"item_1\",1,\"2026-09-24T17:00:00.000Z\",\"user\",\"  Keep café and café separate.\\nThen answer.  \"],\"00000013-0000-4000-8000-000000000000\",\"session opaque 1\",\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",\"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc\",\"2026-09-24T17:00:30.000Z\",[\"00000014-0000-4000-8000-000000000000\",\"00000015-0000-4000-8000-000000000000\",\"00000016-0000-4000-8000-000000000000\",\"4\",\"27\",\"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\"],[[\"ordinary\",\"00000001-0000-4000-8000-000000000000\",\"00000100-0000-4000-8000-000000000000\",\"00000101-0000-4000-8000-000000000000\",\"00000300-0000-4000-8000-000000000000\",1,\"question ordinary\",null],[\"selected\",\"00000002-0000-4000-8000-000000000000\",\"00000200-0000-4000-8000-000000000000\",\"00000201-0000-4000-8000-000000000000\",\"00000301-0000-4000-8000-000000000000\",1,\"question selected\",\"00000203-0000-4000-8000-000000000000\"]],[],[\"overflow\",8]]",
      "sha256": "d0483a965ce92540be3ae9e303d260bd0d028d9f5595632393ac5c993e860142"
    }
  ],
  "rejections": [
    {
      "name": "absent_result_coverage",
      "input": {
        "v": "phone.caller-intent.v2",
        "source": {
          "callSid": "CAaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          "sourceEventId": "00000012-0000-4000-8000-000000000000",
          "itemId": "item_1",
          "revision": 1,
          "receivedAt": "2026-09-24T17:00:00.000Z",
          "role": "user",
          "text": "  Keep café and café separate.\nThen answer.  "
        },
        "generationId": "00000013-0000-4000-8000-000000000000",
        "sourceSessionId": "session opaque 1",
        "brokerInputDigest": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "presentationDigest": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        "callDeadlineAt": "2026-09-24T17:00:30.000Z",
        "version": {
          "ownerPrincipalId": "00000014-0000-4000-8000-000000000000",
          "primaryDexterId": "00000015-0000-4000-8000-000000000000",
          "threadId": "00000016-0000-4000-8000-000000000000",
          "contextEpoch": "4",
          "threadEventSequence": "27",
          "digest": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        },
        "questions": [],
        "results": []
      }
    },
    {
      "name": "v1_historical_input",
      "input": {
        "v": "phone.caller-intent.v1",
        "source": {
          "callSid": "CAaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          "sourceEventId": "00000012-0000-4000-8000-000000000000",
          "itemId": "item_1",
          "revision": 1,
          "receivedAt": "2026-09-24T17:00:00.000Z",
          "role": "user",
          "text": "  Keep café and café separate.\nThen answer.  "
        },
        "generationId": "00000013-0000-4000-8000-000000000000",
        "sourceSessionId": "session opaque 1",
        "brokerInputDigest": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "presentationDigest": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        "callDeadlineAt": "2026-09-24T17:00:30.000Z",
        "version": {
          "ownerPrincipalId": "00000014-0000-4000-8000-000000000000",
          "primaryDexterId": "00000015-0000-4000-8000-000000000000",
          "threadId": "00000016-0000-4000-8000-000000000000",
          "contextEpoch": "4",
          "threadEventSequence": "27",
          "digest": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        },
        "questions": [],
        "results": []
      }
    },
    {
      "name": "v1_with_v2_coverage",
      "input": {
        "v": "phone.caller-intent.v1",
        "source": {
          "callSid": "CAaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          "sourceEventId": "00000012-0000-4000-8000-000000000000",
          "itemId": "item_1",
          "revision": 1,
          "receivedAt": "2026-09-24T17:00:00.000Z",
          "role": "user",
          "text": "  Keep café and café separate.\nThen answer.  "
        },
        "generationId": "00000013-0000-4000-8000-000000000000",
        "sourceSessionId": "session opaque 1",
        "brokerInputDigest": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "presentationDigest": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        "callDeadlineAt": "2026-09-24T17:00:30.000Z",
        "version": {
          "ownerPrincipalId": "00000014-0000-4000-8000-000000000000",
          "primaryDexterId": "00000015-0000-4000-8000-000000000000",
          "threadId": "00000016-0000-4000-8000-000000000000",
          "contextEpoch": "4",
          "threadEventSequence": "27",
          "digest": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        },
        "questions": [],
        "results": [],
        "resultCoverage": {
          "state": "complete",
          "count": 0
        }
      }
    }
  ]
};
const operationId = literals.cases[0]!.operationId, bindingId = literals.cases[0]!.bindingId;
const id = (n: number) => `10000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
function input(name = 'complete_seven'): CallerIntentV2Input {
  const literal = literals.cases.find(value => value.name === name);
  assert.ok(literal); return callerIntentV2InputSchema.parse(structuredClone(literal.input));
}
function pending(value = input(), resultContextCoverage: unknown = { state: 'complete' }) {
  return { v: value.v, operationId, bindingId, inputDigest: callerIntentInputDigest(operationId, bindingId, value),
    brokerInputDigest: value.brokerInputDigest, presentationDigest: value.presentationDigest,
    generationId: value.generationId, sourceSessionId: value.sourceSessionId, callDeadlineAt: value.callDeadlineAt,
    questions: value.questions, results: value.results, resultCoverage: value.resultCoverage,
    resultContextCoverage, reservedAt: '2026-09-24T17:00:01.000Z', validUntil: '2026-09-24T17:00:11.000Z',
    version: value.version, state: 'pending' as const };
}
function resolved(value = input(), coverage: unknown = { state: 'complete' }, decision: unknown = { kind: 'conversation' }) {
  return { ...pending(value, coverage), state: 'resolved' as const, decision };
}
const unavailable = (reason: string) => ({ state: 'unavailable', reason });
const clarify = { kind: 'clarify', reason: 'context_changed' };
const validate = (value: CallerIntentV2Input, body: unknown, previous?: CallerIntentResponse) =>
  validateCallerIntentResponse(bindingId, operationId, value, body, previous);

for (const vector of literals.cases) test(`v2 exact shared literal digest and ordered preimage: ${vector.name}`, () => {
  const value = callerIntentV2InputSchema.parse(vector.input);
  assert.deepEqual(value, vector.input); assert.deepEqual(callerIntentInputSchema.parse(value), value);
  assert.equal(JSON.stringify(vector.orderedPreimage), vector.jsonPreimage);
  assert.equal(createHash('sha256').update(vector.jsonPreimage, 'utf8').digest('hex'), vector.sha256);
  assert.equal(callerIntentV2InputDigest(vector.operationId, vector.bindingId, value), vector.sha256);
  assert.equal(callerIntentInputDigest(vector.operationId, vector.bindingId, value), vector.sha256);
  assert.deepEqual(vector.orderedPreimage.at(-1), [value.resultCoverage.state, value.resultCoverage.count]);
});
for (const vector of literals.rejections) test(`v2 refuses frozen incompatible input: ${vector.name}`, () => {
  assert.equal(callerIntentV2InputSchema.safeParse(vector.input).success, false);
});

test('v1 historical input remains v1 with its original digest and absent coverage', () => {
  const vector = literals.rejections.find(value => value.name === 'v1_historical_input'); assert.ok(vector);
  const value = callerIntentV1InputSchema.parse(vector.input);
  assert.deepEqual(callerIntentInputSchema.parse(value), value);
  assert.equal(Object.hasOwn(value, 'resultCoverage'), false);
  assert.equal(callerIntentInputDigest(operationId, bindingId, value), callerIntentV1InputDigest(operationId, bindingId, value));
  assert.equal(callerIntentV1InputSchema.safeParse(input('empty_complete')).success, false);
  assert.equal(callerIntentInputSchema.safeParse({ ...value, v: 'phone.caller-intent.v3' }).success, false);
});

test('complete inventory admits every count zero through seven without slicing its ordered results', () => {
  for (let count = 0; count <= 7; count++) {
    const base = input(); const value = { ...base, results: base.results.slice(0, count), resultCoverage: { state: 'complete', count } };
    assert.deepEqual(callerIntentV2InputSchema.parse(value), value);
    const parsed = callerIntentV2InputSchema.parse(value);
    assert.deepEqual(validate(parsed, pending(parsed)), pending(parsed));
  }
  const value = input(); value.results.reverse();
  assert.notEqual(callerIntentInputDigest(operationId, bindingId, value), literals.cases[1]!.sha256);
});

test('overflow carries no candidate subset and count remains exact safe integer evidence', () => {
  const value = input('overflow_eight');
  assert.equal(value.results.length, 0);
  assert.deepEqual(validate(value, pending(value, unavailable('inventory_overflow'))), pending(value, unavailable('inventory_overflow')));
  const maximum = { ...value, resultCoverage: { state: 'overflow', count: Number.MAX_SAFE_INTEGER } };
  assert.deepEqual(callerIntentV2InputSchema.parse(maximum), maximum);
  for (const coverage of [{ state: 'complete', count: 8 }, { state: 'overflow', count: 7 },
    { state: 'overflow', count: 8.5 }, { state: 'overflow', count: Number.MAX_SAFE_INTEGER + 1 },
    { state: 'overflow', count: Infinity }, { state: 'overflow', count: '8' }])
    assert.equal(callerIntentV2InputSchema.safeParse({ ...value, resultCoverage: coverage }).success, false);
  assert.equal(callerIntentV2InputSchema.safeParse({ ...value, results: input().results.slice(0, 1) }).success, false);
  const changed = { ...value, resultCoverage: { state: 'overflow' as const, count: 9 } };
  assert.notEqual(callerIntentInputDigest(operationId, bindingId, changed), callerIntentInputDigest(operationId, bindingId, value));
});

test('coverage and source schemas refuse missing, extra, contradictory and malformed content', () => {
  const value = input(), { resultCoverage: omitted, ...missing } = value;
  for (const changed of [missing, { ...value, resultCoverage: { state: 'complete', count: 6 } },
    { ...value, resultCoverage: { state: 'complete', count: 7, truncated: false } },
    { ...value, resultContextCoverage: { state: 'complete' } },
    { ...value, source: { ...value.source, text: 'x\u0000' } },
    { ...value, source: { ...value.source, text: '\ud800' } },
    { ...value, source: { ...value.source, text: ' ' } },
    { ...value, source: { ...value.source, approved: true } },
    { ...value, results: [...value.results, value.results[0]] }])
    assert.equal(callerIntentV2InputSchema.safeParse(changed).success, false);
  const scalarLength = { ...value, source: { ...value.source, text: '😀'.repeat(4000) } };
  assert.deepEqual(callerIntentV2InputSchema.parse(scalarLength), scalarLength);
});

test('candidate source identity, execution, current binding and selected association remain exact', () => {
  const value = input();
  for (const results of [
    [value.results[0], { ...value.results[0], presentationId: 'different' }],
    [value.results[0], { ...value.results[1], execution: { ...value.results[1]!.execution, requestId: value.results[0]!.execution.requestId } }],
    [value.results[0], { ...value.results[1], execution: { ...value.results[1]!.execution, runId: value.results[0]!.execution.runId } }],
  ]) assert.equal(callerIntentV2InputSchema.safeParse({ ...value, results, resultCoverage: { state: 'complete', count: 2 } }).success, false);
  const inconsistent = structuredClone(value);
  inconsistent.results[0]!.execution.bindingId = id(90);
  const refused = callerIntentV2InputSchema.safeParse(inconsistent);
  if (refused.success) assert.fail('Conflicting bindings for the same candidate execution must be refused');
  assert.deepEqual(refused.error.issues, [
    { code: 'custom', message: 'Candidate execution identities disagree', path: [] },
  ]);
  assert.throws(() => callerIntentInputDigest(operationId, bindingId, inconsistent), /Candidate execution identities disagree/);

  const otherCall = structuredClone(value);
  for (const candidate of [...otherCall.questions, ...otherCall.results])
    if (candidate.kind === 'ordinary') candidate.execution.bindingId = id(90);
  assert.deepEqual(callerIntentV2InputSchema.parse(otherCall), otherCall);
  assert.throws(() => callerIntentInputDigest(operationId, bindingId, otherCall),
    { name: 'Error', message: 'caller_intent_candidate_binding_mismatch' });
});

test('context coverage accepts only the frozen inventory-compatible modes', () => {
  for (const [value, coverage] of [[input('empty_complete'), { state: 'complete' }], [input(), { state: 'complete' }],
    [input(), unavailable('source_unavailable')], [input(), unavailable('context_budget')],
    [input('empty_complete'), unavailable('context_budget')], [input('overflow_eight'), unavailable('inventory_overflow')]] as const)
    assert.deepEqual(validate(value, pending(value, coverage)), pending(value, coverage));
  for (const [value, coverage] of [[input('empty_complete'), unavailable('source_unavailable')],
    [input(), unavailable('inventory_overflow')], [input('overflow_eight'), { state: 'complete' }],
    [input('overflow_eight'), unavailable('context_budget')], [input('overflow_eight'), unavailable('source_unavailable')]] as const) {
    assert.equal(callerIntentV2ResponseSchema.safeParse(pending(value, coverage)).success, false);
    assert.equal(callerIntentV2ResponseSchema.safeParse(resolved(value, coverage, clarify)).success, false);
  }
});

test('not_prepared is only a directly resolved clarification, never pending, task, answer or conversation', () => {
  for (const value of [input('empty_complete'), input(), input('overflow_eight')]) {
    const coverage = unavailable('not_prepared');
    assert.deepEqual(validate(value, resolved(value, coverage, clarify)), resolved(value, coverage, clarify));
    assert.equal(callerIntentV2ResponseSchema.safeParse(pending(value, coverage)).success, false);
    for (const decision of [{ kind: 'task' }, { kind: 'conversation' }, { kind: 'answer', target: input().questions[0] }])
      assert.equal(callerIntentV2ResponseSchema.safeParse(resolved(value, coverage, decision)).success, false);
  }
});

test('every supplied result stays referenceable only with complete API context', () => {
  const value = input();
  for (const result of value.results) {
    const reference = result.kind === 'ordinary'
      ? { kind: 'ordinary', relatedPrimaryRequestId: result.execution.requestId, referenceOrigin: 'broker_status_publication' }
      : { kind: 'associated', associatedResultReference: { v: 'phone.associated-result-reference.v1',
        deliveryAssociationId: result.deliveryAssociationId, originalRequestId: result.execution.requestId,
        assistantMessageId: result.assistantMessageId } };
    const decision = { kind: 'task', reference };
    assert.deepEqual(validate(value, resolved(value, { state: 'complete' }, decision)), resolved(value, { state: 'complete' }, decision));
    for (const reason of ['context_budget', 'source_unavailable'])
      assert.equal(callerIntentV2ResponseSchema.safeParse(resolved(value, unavailable(reason), decision)).success, false);
  }
  for (const value of [input(), input('overflow_eight')]) {
    const coverage = unavailable(value.resultCoverage.state === 'overflow' ? 'inventory_overflow' : 'context_budget');
    assert.deepEqual(validate(value, resolved(value, coverage, { kind: 'task' })), resolved(value, coverage, { kind: 'task' }));
    for (const target of value.questions)
      assert.deepEqual(validate(value, resolved(value, coverage, { kind: 'answer', target })), resolved(value, coverage, { kind: 'answer', target }));
  }
});

test('response echoes bind version, exact coverage, raw input and all operation identities', () => {
  const value = input('overflow_eight'), saved = pending(value, unavailable('inventory_overflow'));
  const inconsistent = { ...saved, bindingId: id(90) };
  const refused = callerIntentV2ResponseSchema.safeParse(inconsistent);
  if (refused.success) assert.fail('A response binding must agree with its ordinary candidates');
  assert.deepEqual(refused.error.issues, [
    { code: 'custom', path: ['bindingId'], message: 'Ordinary candidate belongs to another call' },
  ]);
  assert.throws(() => validate(value, inconsistent), /Ordinary candidate belongs to another call/);

  const otherCall = structuredClone(inconsistent);
  for (const candidate of [...otherCall.questions, ...otherCall.results])
    if (candidate.kind === 'ordinary') candidate.execution.bindingId = id(90);
  assert.deepEqual(callerIntentV2ResponseSchema.parse(otherCall), otherCall);
  assert.throws(() => validate(value, otherCall), { name: 'Error', message: 'caller_intent_response_identity_changed' });

  for (const changed of [{ ...saved, resultCoverage: { state: 'overflow', count: 9 } }, { ...saved, inputDigest: 'f'.repeat(64) },
    { ...saved, generationId: id(90) }, { ...saved, operationId: id(90) },
    { ...saved, brokerInputDigest: 'f'.repeat(64) }, { ...saved, sourceSessionId: 'another' },
    { ...saved, presentationDigest: 'f'.repeat(64) }, { ...saved, version: { ...value.version, contextEpoch: '5' } }]) {
    assert.deepEqual(callerIntentV2ResponseSchema.parse(changed), changed);
    assert.throws(() => validate(value, changed), { name: 'Error', message: 'caller_intent_response_identity_changed' });
  }
  const { resultCoverage: ignored, resultContextCoverage: ignoredContext, ...legacy } = saved;
  assert.throws(() => validate(value, { ...legacy, v: 'phone.caller-intent.v1' }));
  assert.equal(callerIntentV1ResponseSchema.safeParse(saved).success, false);
  assert.deepEqual(callerIntentResponseSchema.parse(saved), saved);
  assert.equal(callerIntentOperationSchema.safeParse({ operationId, input: value, response: { ...legacy, v: 'phone.caller-intent.v1' } }).success, false);
});

test('pending to terminal preserves reservation and coverage; resolved receipts cannot change or regress', () => {
  const value = input(), saved = validate(value, pending(value));
  const done = resolved(value);
  assert.deepEqual(validate(value, done, saved), done);
  const terminal = validate(value, done, saved); assert.deepEqual(validate(value, done, terminal), done);
  for (const body of [pending(value, unavailable('context_budget')), resolved(value, unavailable('context_budget')),
    { ...done, reservedAt: '2026-09-24T17:00:02.000Z', validUntil: '2026-09-24T17:00:12.000Z' }])
    assert.throws(() => validate(value, body, saved), /caller_intent_response_changed/);
  for (const body of [pending(value), resolved(value, { state: 'complete' }, { kind: 'task' })])
    assert.throws(() => validate(value, body, terminal), /caller_intent_response_changed/);
});

// Optional actual shared parser crossing. No provider/runtime modules execute:
// the full frozen pure contracts run with API Zod3 and exact history declarations.
const sharedRoot = process.env.BROKER_API_CALLER_INTENT_V2_SOURCE_ROOT;
const apiDependencies = process.env.BROKER_API_DEPENDENCY_ROOT;
test('actual pinned API Zod3 parsers and digest agree with Broker for v1/v2 literals and coverage refusals', {
  skip: !sharedRoot,
}, () => {
  assert.ok(sharedRoot && apiDependencies, 'Both frozen API source root and API dependency root are required');
  const hashes: Record<string, string> = {
  "phoneCallerIntentV2Contract.ts": "3c79d177d1d609838f1ca105de57212a1ac251b3b063b1e7425627a3a8c9f0cb",
  "phoneCallerIntentContract.ts": "4ed2b8cc4d6f6123365a66d968e9249360c74563650d05f70f2536b29a630e89",
  "phoneAssociatedResultContract.ts": "f1f7e36cc8b5ad670e6e41498d91d94a5714c20a4223bc910207684dc7060d2c",
  "phoneEligibleContext.ts": "a4122e257615749f34956546492563bf28758e8f3c0fe3bc2231d7832bce8334"
};
  const source = new Map<string, string>();
  for (const [name, hash] of Object.entries(hashes)) {
    const code = readFileSync(path.join(sharedRoot, name), 'utf8');
    assert.equal(createHash('sha256').update(code).digest('hex'), hash); source.set(name, code);
  }
  const apiRequire = createRequire(path.join(apiDependencies, 'package.json'));
  assert.equal(apiRequire('zod/package.json').version, '3.25.76');
  const cache = new Map<string, Record<string, unknown>>();
  function load(name: string): Record<string, unknown> {
    const saved = cache.get(name); if (saved) return saved;
    let code = source.get(name); assert.ok(code);
    if (name === 'phoneEligibleContext.ts') {
      const ast = ts.createSourceFile(name, code, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS);
      const names = ['MAX_SEQUENCE', 'uuid', 'sequence', 'phoneHistoryVersionSchema'];
      const declarations = ast.statements.filter(statement => ts.isVariableStatement(statement)
        && statement.declarationList.declarations.some(declaration => ts.isIdentifier(declaration.name)
          && names.includes(declaration.name.text)));
      assert.equal(declarations.length, names.length);
      code = `import { z } from 'zod';\n${declarations.map(statement => statement.getText(ast)).join('\n')}`;
    }
    const compiled = ts.transpileModule(code, { compilerOptions: {
      target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS,
    } }).outputText;
    const exports: Record<string, unknown> = {}; cache.set(name, exports);
    new Function('require', 'exports', compiled)((specifier: string) => {
      if (specifier === 'zod' || specifier === 'node:crypto') return apiRequire(specifier);
      if (specifier.startsWith('./') && specifier.endsWith('.js')) return load(specifier.slice(2, -3) + '.ts');
      throw new Error(`Unexpected shared module: ${specifier}`);
    }, exports);
    return exports;
  }
  type Schema = { parse(value: unknown): unknown; safeParse(value: unknown): { success: boolean } };
  const api = load('phoneCallerIntentV2Contract.ts') as {
    phoneCallerIntentV2InputSchema: Schema; phoneCallerIntentV2ResponseSchema: Schema;
    phoneCallerIntentV2InputDigest(operation: string, binding: string, value: unknown): string;
  };
  for (const vector of literals.cases) {
    assert.deepEqual(api.phoneCallerIntentV2InputSchema.parse(vector.input), callerIntentV2InputSchema.parse(vector.input));
    assert.equal(api.phoneCallerIntentV2InputDigest(vector.operationId, vector.bindingId, vector.input), vector.sha256);
  }
  for (const vector of literals.rejections)
    assert.equal(api.phoneCallerIntentV2InputSchema.safeParse(vector.input).success, false);
  const legacy = load('phoneCallerIntentContract.ts') as { phoneCallerIntentInputSchema: Schema };
  const old = literals.rejections.find(value => value.name === 'v1_historical_input'); assert.ok(old);
  assert.deepEqual(legacy.phoneCallerIntentInputSchema.parse(old.input), callerIntentV1InputSchema.parse(old.input));
  for (const value of [input(), input('empty_complete'), input('overflow_eight')]) {
    for (const coverage of [{ state: 'complete' }, ...['inventory_overflow', 'source_unavailable', 'context_budget', 'not_prepared'].map(unavailable)]) {
      for (const body of [pending(value, coverage), resolved(value, coverage, clarify),
        resolved(value, coverage, { kind: 'task' }), resolved(value, coverage, { kind: 'answer', target: input().questions[0] })]) {
        assert.equal(api.phoneCallerIntentV2ResponseSchema.safeParse(body).success,
          callerIntentV2ResponseSchema.safeParse(body).success, JSON.stringify({ coverage, state: body.state }));
      }
    }
  }
});
