import assert from 'node:assert/strict';
import test from 'node:test';
import { inspect } from 'node:util';
import { brokerConfig } from '../src/broker/config.js';

const callbackSecret = 'hark-callback-secret-for-offline-tests';
const ingressSecret = 'independent-ingress-secret-for-offline-tests';
const baseEnv = (): NodeJS.ProcessEnv => ({
  BROKER_CONTROL_TOKEN: 'broker-control-secret-for-offline-tests',
  PHONE_SERVICE_SECRET: 'phone-service-secret-for-offline-tests',
  BROKER_PUBLIC_ORIGIN: 'https://phone.dexter.example',
  DEXTER_API_URL: 'https://api.dexter.example',
  TWILIO_PHONE_NUMBER: '+15555550101',
  BROKER_ALLOWED_TO: '+15555550102',
  BROKER_STATE_DIR: '/var/lib/dexter-broker-config-test',
  OPENAI_API_KEY: 'unused-offline-api-key',
  TWILIO_ACCOUNT_SID: `AC${'1'.repeat(32)}`,
  TWILIO_AUTH_TOKEN: 'unused-offline-twilio-token',
});
const harkEnv = (): NodeJS.ProcessEnv => ({
  ...baseEnv(),
  BROKER_HARK_API_URL: 'http://127.0.0.1:3030',
  HARK_PHONE_CALLBACK_SIGNING_SECRET: callbackSecret,
  OUTBOUND_ALERT_SIGNING_SECRET: ingressSecret,
});

test('unset optional Hark wiring preserves the existing broker configuration', () => {
  const config = brokerConfig(baseEnv());
  assert.equal(config.apiBaseUrl, 'https://api.dexter.example');
  assert.equal(config.port, 3213);
  assert.equal(config.phoneServiceSecret, baseEnv().PHONE_SERVICE_SECRET);
  assert.equal(Object.hasOwn(config, 'harkApiBaseUrl'), false);
  assert.equal(Object.hasOwn(config, 'harkCallbackSigningSecret'), false);
});

test('unused or blank Hark origin does not require or export a callback key', () => {
  for (const origin of [undefined, '', ' \n ']) {
    for (const secret of [undefined, '', 'short', ingressSecret]) {
      const config = brokerConfig({ ...baseEnv(), BROKER_HARK_API_URL: origin,
        HARK_PHONE_CALLBACK_SIGNING_SECRET: secret, OUTBOUND_ALERT_SIGNING_SECRET: ingressSecret });
      assert.equal(Object.hasOwn(config, 'harkApiBaseUrl'), false);
      assert.equal(Object.hasOwn(config, 'harkCallbackSigningSecret'), false);
    }
  }
});

for (const origin of ['http://127.0.0.1', 'http://127.0.0.1:3030/',
  'http://localhost:3030', 'http://[::1]:3030', 'http://[::1]/']) {
  test(`accepts local Hark origin ${origin} with its independent existing key`, () => {
    const config = brokerConfig({ ...harkEnv(), BROKER_HARK_API_URL: ` ${origin}\n`,
      HARK_PHONE_CALLBACK_SIGNING_SECRET: ` ${callbackSecret}\n` });
    assert.equal(config.harkApiBaseUrl, origin);
    assert.equal(config.harkCallbackSigningSecret, callbackSecret);
  });
}

for (const origin of ['http://remote.example', 'https://remote.example', 'http://127.0.0.1.remote.example',
  'http://0.0.0.0:3030', 'https://localhost:3030', 'http://localhost:3030/internal',
  'http://localhost:3030/?token=secret', 'http://localhost:3030/#secret',
  `http://user:${callbackSecret}@localhost:3030`]) {
  test(`refuses non-origin or non-local Hark URL ${origin.replace(callbackSecret, '[redacted]')}`, () => {
    assert.throws(() => brokerConfig({ ...harkEnv(), BROKER_HARK_API_URL: origin }),
      { message: 'BROKER_HARK_API_URL must be a local HTTP origin' });
  });
}

test('local Hark origin requires a callback key with at least 32 trimmed characters', () => {
  for (const secret of [undefined, '', ' '.repeat(32), 'x'.repeat(31), ` ${'x'.repeat(31)} `]) {
    assert.throws(() => brokerConfig({ ...harkEnv(), HARK_PHONE_CALLBACK_SIGNING_SECRET: secret }),
      { message: 'Hark requires its existing independent callback signing secret' });
  }
  const config = brokerConfig({ ...harkEnv(), HARK_PHONE_CALLBACK_SIGNING_SECRET: ` ${'x'.repeat(32)} ` });
  assert.equal(config.harkCallbackSigningSecret, 'x'.repeat(32));
});

test('callback key must differ from the trimmed inbound alert key', () => {
  assert.throws(() => brokerConfig({ ...harkEnv(), HARK_PHONE_CALLBACK_SIGNING_SECRET: ` ${callbackSecret}\n`,
    OUTBOUND_ALERT_SIGNING_SECRET: `\t${callbackSecret} ` }),
  { message: 'Hark requires its existing independent callback signing secret' });
  assert.equal(brokerConfig({ ...harkEnv(), OUTBOUND_ALERT_SIGNING_SECRET: undefined }).harkCallbackSigningSecret, callbackSecret);
});

test('configuration errors do not contain callback or ingress secrets', () => {
  for (const overrides of [
    { HARK_PHONE_CALLBACK_SIGNING_SECRET: callbackSecret, OUTBOUND_ALERT_SIGNING_SECRET: callbackSecret },
    { BROKER_HARK_API_URL: `http://user:${callbackSecret}@localhost:3030` },
    { BROKER_HARK_API_URL: `http://localhost:3030/?key=${callbackSecret}` },
    { BROKER_HARK_API_URL: `invalid-url-with-${callbackSecret}` },
  ]) {
    assert.throws(() => brokerConfig({ ...harkEnv(), ...overrides }), (error: unknown) => {
      assert.ok(error instanceof Error);
      const diagnostic = inspect(error, { depth: 4 });
      assert.equal(diagnostic.includes(callbackSecret), false, 'error object exposes the callback key');
      assert.equal(diagnostic.includes(ingressSecret), false, 'error object exposes the ingress key');
      return true;
    });
  }
});
