import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { once } from 'node:events';
import { SmsService, type SmsOptions } from '../src/sms/SmsService.js';

function fixture(t: test.TestContext) {
  const dir = fs.mkdtempSync(path.join(os.homedir(), '.local/state/sms-lock-test-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const options: SmsOptions = { statePath: path.join(dir, 'sms.json'), enrollmentEnabled: false,
    sendEnabled: false, campaignApproved: false, advancedOptOutConfigured: false,
    messagingServiceSid: '', publicBaseUrl: 'https://sms.invalid',
    provider: { async startVerification() { throw Error('unexpected_verify'); },
      async checkVerification() { throw Error('unexpected_verify'); }, async send() { throw Error('unexpected_send'); } } };
  return { options, lock: options.statePath + '.lock' };
}

function child(t: test.TestContext, code: string) {
  const process = spawn(globalThis.process.execPath, ['--import', 'tsx', '--input-type=module', '-e', code], { stdio: ['pipe', 'pipe', 'pipe'] });
  t.after(async () => {
    if (process.exitCode !== null || process.signalCode !== null) return;
    const exited = once(process, 'exit'); process.kill('SIGKILL'); await exited;
  });
  return process;
}
async function ready(process: ChildProcessWithoutNullStreams) {
  let output = '', errors = '';
  process.stdout.on('data', chunk => { output += chunk; });
  process.stderr.on('data', chunk => { errors += chunk; });
  for (let i = 0; i < 500; i++) {
    if (output.includes('READY')) return;
    if (process.exitCode !== null) throw Error('child_start_failed: ' + errors);
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  throw Error('child_start_timeout: ' + errors);
}
const provider = `{ async startVerification() { throw Error('unexpected_verify'); }, async checkVerification() { throw Error('unexpected_verify'); }, async send() { throw Error('unexpected_send'); } }`;
function serviceCode(options: SmsOptions) {
  return `import { SmsService } from './src/sms/SmsService.ts';
    const service = new SmsService({ ...${JSON.stringify(options)}, provider: ${provider} });`;
}

test('empty and malformed abandoned locks recover immediately without losing the ledger', t => {
  const f = fixture(t);
  const initial = new SmsService(f.options); initial.close();
  const ledger = fs.readFileSync(f.options.statePath);
  for (const contents of ['', '{broken', 'null', '{"pid":"invalid"}', '{"pid":99999999999999}']) {
    fs.writeFileSync(f.lock, contents, { mode: 0o600 });
    const recovered = new SmsService(f.options);
    assert.equal(JSON.parse(fs.readFileSync(f.lock, 'utf8')).pid, process.pid);
    assert.deepEqual(fs.readFileSync(f.options.statePath), ledger);
    recovered.close();
  }
});

test('a pre-upgrade writer paused before PID publication keeps its empty lock until it dies', async t => {
  const f = fixture(t);
  const owner = child(t, `import fs from 'node:fs';
    const fd = fs.openSync(${JSON.stringify(f.lock)}, 'wx', 0o600);
    console.log('READY'); setInterval(() => {}, 1000);`);
  await ready(owner);
  assert.throws(() => new SmsService(f.options), /EEXIST/);
  assert.equal(fs.readFileSync(f.lock, 'utf8'), '');
  const exited = once(owner, 'exit'); owner.kill('SIGKILL'); await exited;
  const recovered = new SmsService(f.options); recovered.close();
  assert.equal(fs.existsSync(f.lock), false);
});

test('same-process open malformed lock is preserved, then recovered after its holder closes', t => {
  const f = fixture(t), fd = fs.openSync(f.lock, 'wx', 0o600);
  try { assert.throws(() => new SmsService(f.options), /EEXIST/); }
  finally { fs.closeSync(fd); }
  const recovered = new SmsService(f.options); recovered.close();
});

test('simultaneous abandoned-lock recovery admits only one process writer', async t => {
  const f = fixture(t);
  fs.writeFileSync(f.lock, '{broken', { mode: 0o600 });
  const owner = child(t, `${serviceCode(f.options)} console.log('READY'); setInterval(() => {}, 1000);`);
  const contender = child(t, `${serviceCode(f.options)} console.log('READY'); setInterval(() => {}, 1000);`);
  let ownerOutput = '', contenderOutput = '';
  owner.stdout.on('data', chunk => { ownerOutput += chunk; });
  contender.stdout.on('data', chunk => { contenderOutput += chunk; });
  for (let i = 0; i < 500; i++) {
    if ((ownerOutput.includes('READY') || contenderOutput.includes('READY')) && (owner.exitCode !== null || contender.exitCode !== null)) break;
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  assert.equal(Number(ownerOutput.includes('READY')) + Number(contenderOutput.includes('READY')), 1);
  assert.equal([owner.exitCode, contender.exitCode].filter(code => code !== null).length, 1);
  assert.throws(() => new SmsService(f.options), /EEXIST/);
  const holder = ownerOutput.includes('READY') ? owner : contender;
  const exited = once(holder, 'exit'); holder.kill('SIGKILL'); await exited;
  const recovered = new SmsService(f.options); recovered.close();
});

test('missing legacy-holder probe refuses malformed-lock recovery', t => {
  const f = fixture(t), executablePath = process.env.PATH, bin = path.join(path.dirname(f.lock), 'bin');
  fs.mkdirSync(bin); fs.symlinkSync('/usr/bin/flock', path.join(bin, 'flock'));
  fs.writeFileSync(f.lock, '', { mode: 0o600 });
  try {
    process.env.PATH = bin;
    assert.throws(() => new SmsService(f.options), /sms_lock_fuser_unavailable/);
  } finally { process.env.PATH = executablePath; }
  assert.equal(fs.readFileSync(f.lock, 'utf8'), '');
  const recovered = new SmsService(f.options); recovered.close();
});

test('missing lock utilities fail closed with a useful diagnostic and preserve metadata', t => {
  const f = fixture(t), executablePath = process.env.PATH;
  const malformed = '{broken';
  fs.writeFileSync(f.lock, malformed, { mode: 0o600 });
  try {
    process.env.PATH = '/nonexistent-sms-lock-tools';
    assert.throws(() => new SmsService(f.options), /sms_lock_flock_unavailable/);
  } finally { process.env.PATH = executablePath; }
  assert.equal(fs.readFileSync(f.lock, 'utf8'), malformed);
  const recovered = new SmsService(f.options); recovered.close();
});
