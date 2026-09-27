import dotenv from 'dotenv';
import { createBrokerSmsBridge, type BrokerSmsBridge } from './sms.js';
import { BrokerStore } from './store.js';
import { createInboundHoldAdapter } from './inboundHold.js';
import { brokerConfig } from './config.js';
import { createBrokerServer } from './server.js';
import { createEligibleContextClient } from './eligibleContext.js';
import { createSelectedQuestionAssociationClient } from './selectedQuestion.js';
import { createRequestDeliveryClient } from './requestDelivery.js';
import { createPrimaryCallerIntentClient } from './callerIntentClient.js';
import { safeFailure } from './phone.js';

dotenv.config({ path: process.env.BROKER_ENV_FILE || '.env' });
let sms: BrokerSmsBridge | undefined;
try {
  const config = brokerConfig(process.env);
  sms = await createBrokerSmsBridge(process.env, process.cwd());
  const setup = config.verificationSetup;
  const setupStore = setup ? new BrokerStore(config.stateDir) : undefined;
  const broker = createBrokerServer({ config, sms,
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
  let stopping = false;
  const shutdown = () => {
    if (stopping) return;
    stopping = true;
    const deadline = setTimeout(() => process.exit(1), 60_000);
    void broker.close().then(() => { clearTimeout(deadline); process.exit(0); }, error => { console.error(JSON.stringify(safeFailure(error))); process.exit(1); });
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
  broker.server.listen(config.port, config.host, () => {
    console.log(JSON.stringify({ service: 'dexter-broker', model: config.model, voice: config.voice, port: config.port }));
  });
} catch (error) { await sms?.close().catch(() => {}); console.error(JSON.stringify(safeFailure(error))); process.exitCode = 1; }
