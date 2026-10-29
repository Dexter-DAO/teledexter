import dotenv from 'dotenv';
import { closeBrokerSms, startBrokerSmsBridge, type BrokerSmsBridge } from './sms.js';
import { BrokerStore } from './store.js';
import { createInboundHoldAdapter } from './inboundHold.js';
import { brokerConfig } from './config.js';
import { createBrokerServer } from './server.js';
import { createEligibleContextClient } from './eligibleContext.js';
import { createSelectedQuestionAssociationClient } from './selectedQuestion.js';
import { createRequestDeliveryClient } from './requestDelivery.js';
import { createPrimaryCallerIntentClient } from './callerIntentClient.js';
import { safeFailure } from './phone.js';
import { createOpenAILiveConnection } from '../live/session.js';
import { INBOUND_GREETING_CLIP_ASSET, loadGreetingClip } from './greetingClip.js';

dotenv.config({ path: process.env.BROKER_ENV_FILE || '.env' });
let sms: BrokerSmsBridge | undefined;
let smsStarting: Promise<void> | undefined;
let stopping = false;
const closeSms = () => closeBrokerSms({ async close() { await smsStarting; await sms?.close(); } });
const hardExit = () => process.kill(process.pid, 'SIGKILL');
async function startupFailed(error: unknown) {
  console.error(JSON.stringify(safeFailure(error)));
  stopping = true;
  // process.exit can itself wait for a worker blocked in a syscall.
  const deadline = setTimeout(hardExit, 5_000);
  try { await closeSms(); }
  catch (closeError) { console.error(JSON.stringify(safeFailure(closeError))); hardExit(); }
  clearTimeout(deadline);
  process.exit(1);
}
try {
  const config = brokerConfig(process.env);
  const setup = config.verificationSetup;
  const setupStore = setup ? new BrokerStore(config.stateDir) : undefined;
  // Both clips play on the media stream at pickup, while admission and binding run. A missing
  // or implausible asset leaves the Live-spoken greeting, heard once Live attaches.
  const greetingClip = loadGreetingClip();
  const inboundGreetingClip = loadGreetingClip(INBOUND_GREETING_CLIP_ASSET);
  const broker = createBrokerServer({ config, get sms() { return sms; },
    ...(greetingClip ? { greetingClip } : {}),
    ...(inboundGreetingClip ? { inboundGreetingClip } : {}),
    inboundPreconnect: () => createOpenAILiveConnection(config.apiKey),
    ...(setup && setupStore ? { store: setupStore,
      inboundAttachment: createInboundHoldAdapter({ publicOrigin: config.publicOrigin, store: setupStore }),
      restrictedSetup: { model: setup.model, receiptTransport: { apiBaseUrl: setup.apiBaseUrl, signingSecret: setup.signingSecret } },
    } : {}),
    eligibleContext: createEligibleContextClient({ apiBaseUrl: config.apiBaseUrl,
      phoneServiceSecret: config.phoneServiceSecret }),
    selectedQuestion: {
      association: createSelectedQuestionAssociationClient({ apiBaseUrl: config.apiBaseUrl,
        phoneServiceSecret: config.phoneServiceSecret }),
      requests: createRequestDeliveryClient({ apiBaseUrl: config.apiBaseUrl,
        phoneServiceSecret: config.phoneServiceSecret }),
    },
    callerIntent: {
      client: createPrimaryCallerIntentClient({ apiBaseUrl: config.apiBaseUrl,
        phoneServiceSecret: config.phoneServiceSecret }),
      // The API further bounds inference by current runtime policy and the
      // immutable original call deadline; this is only the host wait ceiling.
      timeoutMs: 10_000,
    },
  });
  const shutdown = () => {
    if (stopping) return;
    stopping = true;
    const deadline = setTimeout(hardExit, 60_000);
    void broker.close().then(closeSms).then(() => { clearTimeout(deadline); process.exit(0); }, error => {
      console.error(JSON.stringify(safeFailure(error))); hardExit();
    });
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
  broker.server.on('error', error => { void startupFailed(error); });
  broker.server.listen(config.port, config.host, () => {
    console.log(JSON.stringify({ service: 'dexter-broker', model: config.model, voice: config.voice, port: config.port, greetingClipMs: greetingClip ? greetingClip.length / 8 : 0,
      inboundGreetingClipMs: inboundGreetingClip ? inboundGreetingClip.length / 8 : 0 }));
    // A hung SMS startup cannot postpone voice listening, even for the admission timeout.
    smsStarting = startBrokerSmsBridge(process.env, process.cwd()).then(bridge => { sms = bridge; if (stopping) sms?.stop(); });
  });
} catch (error) { await startupFailed(error); }
