#!/usr/bin/env node
// Call journey report for the broker.
//
//   npm run calls -- journey --journal /absolute/calls.json [--log /absolute/broker.log]
//
// Prints a privacy-limited startup and result timing report built from the
// broker call journal and, optionally, the broker log. Read-only: it never
// opens the journal through the broker store or takes its writer lock.

const mode = process.argv[2];

if (mode === 'journey') {
  try {
    const { runCallJourneyReport } = await import('./call-journey-report.mjs');
    console.log(JSON.stringify(runCallJourneyReport(process.argv.slice(3)), null, 2));
  } catch (error) {
    const code = error instanceof Error && /^journey_[a-z_]+$/.test(error.message) ? error.message : 'journey_read_failed';
    console.error(`[calls] ${code}`); process.exitCode = 1;
  }
} else {
  console.error('usage: npm run calls -- journey --journal /absolute/calls.json [--log /absolute/broker.log]');
  process.exitCode = 1;
}
