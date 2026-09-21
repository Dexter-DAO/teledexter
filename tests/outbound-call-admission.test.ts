import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import type { OutboundAlertRequest } from '../src/outbound/contract.js';
import {
  OutboundAlertService,
  OutboundCallAdmissionError,
  type VoiceDialer,
} from '../src/outbound/OutboundAlertService.js';
import { OutboundAlertStore } from '../src/outbound/OutboundAlertStore.js';
import { buildTwilioFallbackTwiml } from '../src/outbound/TwilioMediaBootstrap.js';

const request: OutboundAlertRequest = {
  idempotencyKey: 'portfolio:event-1',
  eventId: 'portfolio:event-1',
  to: '+15615550100',
  recipientName: 'Branch',
  subject: 'FISH',
  severity: 'critical',
  briefing: 'FISH moved twenty percent.',
  observedAt: '2026-08-31T00:00:00.000Z',
  ttlSeconds: 180,
  followupContext: {},
};

test('API admission failure creates no intent and makes zero Twilio calls', async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'phone-admission-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const store = new OutboundAlertStore(path.join(directory, 'state.json'));
  let dials = 0;
  const dialer: VoiceDialer = {
    placeCall: async () => {
      dials += 1;
      return { callSid: `CA${'1'.repeat(32)}` };
    },
  };
  const service = new OutboundAlertService({
    store,
    dialer,
    publicBaseUrl: 'https://phone.dexter.cash',
    fromNumber: '+15615550200',
    admitNewCall: async () => ({ ok: false, reason: 'dexter_api_unreachable' }),
  });
  await assert.rejects(
    service.dispatch(request),
    (error: unknown) => error instanceof OutboundCallAdmissionError
      && error.reason === 'dexter_api_unreachable',
  );
  assert.equal(dials, 0);
  assert.deepEqual(store.inspectReservation(request), { kind: 'none' });
});

test('matching replay bypasses admission and never redials; conflict remains conflict', async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'phone-replay-admission-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const store = new OutboundAlertStore(path.join(directory, 'state.json'));
  let admissions = 0;
  let dials = 0;
  const service = new OutboundAlertService({
    store,
    dialer: {
      placeCall: async () => {
        dials += 1;
        return { callSid: `CA${'1'.repeat(32)}` };
      },
    },
    publicBaseUrl: 'https://phone.dexter.cash',
    fromNumber: '+15615550200',
    admitNewCall: async () => {
      admissions += 1;
      return { ok: true };
    },
  });
  assert.equal((await service.dispatch(request)).kind, 'accepted');
  const replay = await service.dispatch({
    ...request,
    observedAt: '2026-08-31T00:00:01.000Z',
  });
  assert.equal(replay.kind, 'accepted');
  if (replay.kind === 'accepted') assert.equal(replay.replay, true);
  const conflict = await service.dispatch({ ...request, subject: 'OTHER' });
  assert.equal(conflict.kind, 'conflict');
  assert.equal(admissions, 1);
  assert.equal(dials, 1);
});

test('concurrent matching requests cross admission but place exactly one call', async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'phone-concurrent-admission-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const store = new OutboundAlertStore(path.join(directory, 'state.json'));
  let releaseAdmission: (() => void) | null = null;
  const admission = new Promise<void>((resolve) => { releaseAdmission = resolve; });
  let dials = 0;
  const service = new OutboundAlertService({
    store,
    dialer: {
      placeCall: async () => {
        dials += 1;
        return { callSid: `CA${'1'.repeat(32)}` };
      },
    },
    publicBaseUrl: 'https://phone.dexter.cash',
    fromNumber: '+15615550200',
    admitNewCall: async () => {
      await admission;
      return { ok: true };
    },
  });

  const first = service.dispatch(request);
  const second = service.dispatch({
    ...request,
    observedAt: '2026-08-31T00:00:01.000Z',
  });
  await new Promise((resolve) => setImmediate(resolve));
  releaseAdmission?.();
  const results = await Promise.all([first, second]);

  assert.equal(dials, 1);
  assert.deepEqual(
    results.map((result) => result.kind === 'accepted' ? result.replay : null).sort(),
    [false, true],
  );
});

test('Twilio fallback speaks the alert safely without opening a media stream', () => {
  const twiml = buildTwilioFallbackTwiml('FISH moved 20% & needs attention.');
  assert.match(twiml, /<Say voice="Polly\.Matthew">/u);
  assert.match(twiml, /20% &amp; needs attention/u);
  assert.doesNotMatch(twiml, /<Stream/u);
});
