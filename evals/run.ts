/**
 * Runs the voice-protocol bench: every scenario N times against one broker module root,
 * a few calls at a time, each in its own process (real gpt-live-1, scripted caller/Primary).
 *
 *   nice -n 19 ionice -c3 node --import tsx evals/run.ts --module-root /abs/root --label new \
 *     --runs 10 --concurrency 4 --out-root /abs/dir --config-file /abs/production.env \
 *     --audio-cache /abs/cache [--scenarios a,b] [--budget-usd 12]
 *
 * Each run writes <out-root>/<label>/<scenario>/<n>/evidence.json; score with evals/score.ts.
 * Spend counts Live seconds at $0.05/min; launching stops once --budget-usd is reached.
 */
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { scenarios } from './scenarios.js';

const args = new Map<string, string>();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i]!, process.argv[i + 1]!);
const need = (flag: string) => { const value = args.get(flag); if (!value) throw new Error(`missing ${flag}`); return value; };
const moduleRoot = need('--module-root'), label = need('--label'), outRoot = path.resolve(need('--out-root'));
const runs = Number(args.get('--runs') ?? '10'), concurrency = Number(args.get('--concurrency') ?? '4');
const budget = Number(args.get('--budget-usd') ?? '1000');
const selected = (args.get('--scenarios') ?? Object.keys(scenarios).join(',')).split(',');
const firstSeed = Number(args.get('--first-seed') ?? '1');
for (const id of selected) if (!scenarios[id]) throw new Error(`unknown scenario ${id}`);
const runOne = path.join(path.dirname(fileURLToPath(import.meta.url)), 'run-one.ts');

const jobs: Array<{ scenario: string; seed: number; out: string }> = [];
for (let n = 0; n < runs; n++) for (const scenario of selected) {
  const out = path.join(outRoot, label, scenario, String(firstSeed + n));
  if (fs.existsSync(path.join(out, 'evidence.json'))) continue;
  jobs.push({ scenario, seed: firstSeed + n, out });
}
let liveSeconds = 0, active = 0, failed = 0;
const ledger = path.join(outRoot, label, 'runs.jsonl');
fs.mkdirSync(path.dirname(ledger), { recursive: true, mode: 0o700 });
const spentUsd = () => liveSeconds / 60 * 0.05;
await new Promise<void>(resolve => {
  const next = () => {
    if (!jobs.length && !active) { resolve(); return; }
    while (active < concurrency && jobs.length && spentUsd() < budget) {
      const job = jobs.shift()!;
      active++;
      fs.mkdirSync(job.out, { recursive: true, mode: 0o700 });
      const child = spawn('node', ['--import', 'tsx', runOne, '--module-root', moduleRoot, '--scenario', job.scenario,
        '--out', job.out, '--seed', String(job.seed), '--config-file', need('--config-file'), '--audio-cache', need('--audio-cache')],
      { stdio: ['ignore', 'pipe', 'pipe'] });
      let stdout = '', stderr = '';
      child.stdout.on('data', chunk => { stdout += chunk; });
      child.stderr.on('data', chunk => { stderr += chunk; });
      const kill = setTimeout(() => child.kill('SIGKILL'), (scenarios[job.scenario]!.maxMs + 90_000));
      child.on('exit', code => {
        clearTimeout(kill);
        active--;
        let result: Record<string, unknown> = { scenario: job.scenario, seed: job.seed, code };
        try { result = { ...result, ...JSON.parse(stdout.trim().split('\n').at(-1)!) }; } catch { failed++; result.error = stderr.slice(-1500); }
        liveSeconds += Number(result.liveSeconds ?? 0);
        fs.appendFileSync(ledger, JSON.stringify({ ...result, at: new Date().toISOString() }) + '\n');
        process.stdout.write(`${label} ${job.scenario} #${job.seed} ${result.endReason ?? 'error'} live=${result.liveSeconds ?? '?'}s spent=$${spentUsd().toFixed(2)}\n`);
        next();
      });
    }
    if (!active && jobs.length && spentUsd() >= budget) { process.stdout.write(`budget reached with ${jobs.length} runs left\n`); resolve(); }
  };
  next();
});
process.stdout.write(`${label}: done. live seconds ${liveSeconds}, approx $${spentUsd().toFixed(2)}, failed ${failed}\n`);
