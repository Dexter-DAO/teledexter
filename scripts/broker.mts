import fs from 'node:fs';
import { randomUUID } from 'node:crypto';
import dotenv from 'dotenv';

dotenv.config({ path: process.env.BROKER_ENV_FILE || '.env' });
const [command = 'status', ...args] = process.argv.slice(2);
const value = (flag: string) => { const index = args.indexOf(flag); return index < 0 ? undefined : args[index + 1]; };
const token = process.env.BROKER_CONTROL_TOKEN;
if (!token) throw new Error('Configure BROKER_CONTROL_TOKEN');
const origin = `http://127.0.0.1:${Number(process.env.BROKER_PORT || 3213)}`;
const routes: Record<string, string> = { status: 'calls', call: 'calls', preflight: 'preflight', end: 'end', reconcile: 'reconcile' };
if (!routes[command]) throw new Error('Use status, preflight, call --purpose TEXT, end --id UUID, or reconcile --id UUID');
let body: Record<string, string> | undefined;
if (command === 'call') {
  const purpose = value('--purpose');
  if (!purpose) throw new Error('A call needs --purpose TEXT');
  body = { requestId: value('--request-id') || randomUUID(), purpose };
  const to = value('--to'); if (to) body.to = to;
  // Keep the idempotency key available even when the HTTP response is lost.
  console.error(JSON.stringify({ requestId: body.requestId }));
} else if (command === 'end' || command === 'reconcile') {
  const id = value('--id'); if (!id) throw new Error('Provide --id UUID'); body = { id };
} else if (command === 'preflight') body = {};
const response = await fetch(`${origin}/internal/broker/${routes[command]}`, {
  method: command === 'status' ? 'GET' : 'POST',
  headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
  body: body && JSON.stringify(body), signal: AbortSignal.timeout(45_000), redirect: 'error',
});
const result = await response.json();
const output = JSON.stringify({ httpStatus: response.status, result }, null, 2);
const receipt = value('--receipt');
if (receipt) fs.writeFileSync(receipt, output + '\n', { mode: 0o600, flag: 'wx' });
console.log(output);
if (!response.ok) process.exitCode = 1;
