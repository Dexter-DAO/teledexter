import dotenv from 'dotenv';
import { brokerConfig } from './config.js';
import { createBrokerServer } from './server.js';
import { safeFailure } from './phone.js';

dotenv.config({ path: process.env.BROKER_ENV_FILE || '.env' });
try {
  const config = brokerConfig(process.env);
  const broker = createBrokerServer({ config });
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
} catch (error) { console.error(JSON.stringify(safeFailure(error))); process.exitCode = 1; }
