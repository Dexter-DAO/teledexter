const fs = require('node:fs');
const path = require('node:path');

const releaseRoot = '/var/lib/dexter-broker/releases';
const supplied = process.env.BROKER_RELEASE_DIR;
if (!supplied || !path.isAbsolute(supplied)) {
  throw new Error('Set BROKER_RELEASE_DIR to the immutable broker release directory');
}
const releaseDir = fs.realpathSync(supplied);
const relative = path.relative(releaseRoot, releaseDir);
if (releaseDir !== path.resolve(supplied) || !relative || relative.startsWith('..') || path.isAbsolute(relative)) {
  throw new Error('BROKER_RELEASE_DIR must name an immutable directory under /var/lib/dexter-broker/releases');
}
const entrypoint = path.join(releaseDir, 'dist/broker/main.js');
if (!fs.statSync(entrypoint).isFile()) throw new Error('Build the broker release before starting it');

module.exports = {
  apps: [{
    name: 'dexter-broker',
    cwd: releaseDir,
    script: entrypoint,
    interpreter: process.execPath,
    exec_mode: 'fork',
    instances: 1,
    autorestart: true,
    restart_delay: 2000,
    min_uptime: 10000,
    max_restarts: 10,
    kill_timeout: 65000,
    env: {
      NODE_ENV: 'production',
      BROKER_HOST: '127.0.0.1',
      BROKER_PORT: '3213',
      BROKER_STATE_DIR: '/var/lib/dexter-broker/state',
      BROKER_ENV_FILE: '/var/lib/dexter-broker/config/production.env',
      BROKER_REVISION: process.env.BROKER_REVISION || path.basename(releaseDir),
    },
  }],
};
