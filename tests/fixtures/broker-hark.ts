import {
  HARK_PHONE_PURPOSE, harkSourceDigest, type PhoneHarkSnapshot, type ResolvedHarkAssignment,
} from '../../src/broker/harkAssignment.js';

export function makeHarkSnapshot(): PhoneHarkSnapshot {
  const source: Omit<PhoneHarkSnapshot, 'sourceDigest'> = {
    kind: 'hark_watch_event_v1',
    ownerSupabaseUserId: '11111111-1111-4111-8111-111111111111',
    destinationId: '22222222-2222-4222-8222-222222222222',
    deliveryId: '33333333-3333-4333-8333-333333333333',
    caseId: '44444444-4444-4444-8444-444444444444',
    originTaskId: 'watch-observation-17',
    expiresAt: '2026-09-23T20:00:00.000Z',
    watch: {
      id: '55555555-5555-4555-8555-555555555555', draftDigest: 'a'.repeat(64),
      request: 'Call me when the watched stock moves by five percent.', name: 'Stock movement',
      source: { kind: 'market_observation', mint: 'synthetic-mint' },
      condition: { percent: 5 }, attention: { channel: 'phone' },
    },
    event: {
      id: '66666666-6666-4666-8666-666666666666', bodyDigest: 'b'.repeat(64),
      observedAt: '2026-09-23T19:30:00.000Z', acceptedAt: '2026-09-23T19:30:01.000Z',
      kind: 'market_change', subject: { mint: 'synthetic-mint' },
      summary: 'The watched price moved by 5.2 percent.',
      facts: { priceChangePercent: 5.2, unitPriceUsd: '101.2300', note: '  Saved source text\t🙂' },
      evidence: [{ provider: 'synthetic-market-source', observedAt: '2026-09-23T19:30:00.000Z' }],
    },
    billing: { mandateId: '77777777-7777-4777-8777-777777777777', unitPriceMicro: 100_000, freeSeconds: 60 },
  };
  return { ...source, sourceDigest: harkSourceDigest(source) };
}

export function makeResolvedHarkAssignment(): ResolvedHarkAssignment {
  const hark = makeHarkSnapshot();
  return { schemaVersion: 1, to: '+12025550123', callAssignment: {
    assignmentId: hark.deliveryId, brokerCallId: hark.deliveryId, revision: 1, purpose: HARK_PHONE_PURPOSE, hark,
  } };
}
