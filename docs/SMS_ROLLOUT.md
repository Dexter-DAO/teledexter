# Conversational SMS rollout

These changes are prepared for review across Teledexter, API and website PRs. Deployment, production settings, PM2 and Twilio configuration remain with the operator after review.

1. Review both PRs together. Apply both prepared API migrations for SMS threads/policy and owner-bound Verify enrollment using the API database procedure, then verify its constraints and thread index before deploying API consumers. The migration is additive and preserves default home threads; it enables the SMS execution policy and separate SMS threads.
2. Build and deploy the reviewed API and its Primary worker with the same source. Confirm the authenticated `/internal/primary-dexter/channels/phone/sms/turns` route is installed and workers understand `sms_read_only`. Use a disposable account for integration checks. The account must complete the website's authenticated SMS Verify enrollment in addition to public texting consent; profile phone entry or a voice challenge is insufficient. Code requests share per-phone limits; the website must preserve HTTP 429 `Retry-After` through its signup and account-link steps.
3. Prepare the reviewed Teledexter immutable release with Node 22 or later. Run `nice -n 10 npm ci`, `nice -n 10 npm run typecheck`, `nice -n 10 npm test -- --test-concurrency=2` and `nice -n 10 npm run build` against that source. Retain the current broker journal and SMS ledger. Stop the prior broker before starting the successor against the same state; follow BROKER_OPERATIONS.md for PM2 and release handling.
4. Set these existing variables in `/var/lib/dexter-broker/config/production.env`. `TWILIO_MESSAGING_SERVICE_SID` must be the `MG...` SID of the approved **Dexter SMS** service holding the registered number; obtain the actual SID from the Twilio console. `DEXTER_SMS_CAMPAIGN_APPROVED=true`. Set `DEXTER_SMS_ADVANCED_OPT_OUT_CONFIGURED=true` only after the keyword setup in step 5. Set `DEXTER_SMS_SEND_ENABLED=true` when ready to activate sends. No new environment variables are introduced. Verify `DEXTER_SMS_ENROLLMENT_ENABLED=true` so the signup link can complete enrollment. Existing durable state, internal proxy secret, Twilio credentials, destination number, public HTTPS origin, `DEXTER_API_URL` and `PHONE_SERVICE_SECRET` remain required. Webhook validation must remain enabled.
5. In Twilio, confirm the approved campaign and registered number belong to Dexter SMS. Enable Advanced Opt Out with the STOP/START/HELP words and responses documented in sms.md. Change that service's incoming webhook from the demo responder to `https://<broker-public-host>/sms/incoming`, method POST. The hostname must equal `DEXTER_PHONE_PUBLIC_URL`; mismatched public URLs fail signature validation. Delivery callbacks are supplied by the sender as `/sms/status/<message-id>`.
6. Start the reviewed broker with the final env and save PM2 state. Confirm `/broker/health`, signed webhook admission, internal SMS readiness, voice reception and a separate test account's conversation after its owner-authenticated Verify binding and SMS consent are both recorded. Check signup-once, STOP silence, HELP/START handling, balance reply and a money request's passkey link. Confirm the financial action did not execute. Use an owned test number/account; keep Branch's phone thread out of tests.

Before each broker restart or rollback, confirm `/broker/health` reports `activeCalls: 0`. Save the current Twilio incoming-message webhook before changing it. Reverting that webhook stops new SMS intake without a voice restart; preserve the number's Voice configuration. Before applying the API migration, save the live definitions of `phone_inbound_admit_lock(jsonb)`, `phone_inbound_admit_write(jsonb)`, `agent_runs_execution_policy_check` and `agent_threads_one_active_primary_key` for comparison and recovery.

Voice listens before SMS initialization. The worker starts paused; the parent activates inbox draining after taking ownership, so shutdown during startup cannot dispatch retained texts. Shutdown stops active calls, closes Primary and provider legs, then waits at most five seconds for SMS cleanup. Stop during an API turn preserves its inbox SID for recovery and prevents another dispatch or retry mutation. The 60-second process deadline uses SIGKILL because Node's normal exit can block on a worker syscall.

The Linux SMS worker requires `flock` and `fuser` on its executable path. A persistent `<state>.lock.guard` serializes writer admission and recovery; preserve it with the ledger and never unlink it. Empty or malformed abandoned `.lock` files recover automatically. A live legacy process holding an incomplete lock is preserved. Missing utilities refuse SMS startup with a diagnostic while voice remains available. After a worker dies, inspect its failure and restart only in a no-call window.

SMS setup failure logs `service: sms, state: disabled` while voice remains available. Broker health alone does not prove texting works; inspect `/internal/sms/config` with the existing internal secret and the test account's carrier outcome. Pending API responses are polled at two-second intervals. Model and carrier latency have not been measured by local fake-provider tests.

For broker rollback to the previous release (`2c69df0`), remove **every `DEXTER_SMS_*` variable** from the old process environment before starting it, including inherited PM2 values. Setting send/enrollment flags to `false` still initializes the old reader and can prevent voice from listening. With all SMS variables absent, the old bridge returns disabled without opening the ledger, and voice can start. Preserve the complete current ledger, signup markers and carrier reservations; restore SMS variables only with a compatible reader. Never restore an older ledger snapshot. Verify voice reception after rollback. Keep the API's new thread column and execution policy while SMS runs or records remain. Queued and expired SMS runs retain a model tag that the old worker claim query skips; running work still needs to finish, and SMS threads must be archived before old API readers return. Follow the API PR's rollback notes.


## API rollback cleanup

Stop SMS admission and dispatch, then stop **every new worker** before running this cleanup. Wait until every running SMS lease has expired. Both preflight queries below must return zero rows. If a run has live child work or a durable assistant answer awaiting completion, recover and finish it with the current worker, then stop every new worker again and repeat the preflight checks. Preserve all schema, records and guards.

The first update fences expired running leases with a new token and generation. The second cancels queued and running SMS turns. Archive SMS threads in the same transaction before starting older API readers, which assume one active thread. Any error requires `ROLLBACK` and investigation. Both final counts must be zero before starting any older API reader or worker.

```sql
-- Prepared rollback only; never run while any new worker is alive.
-- Stop SMS admission/dispatch and every new worker first. Wait for every running
-- SMS lease to expire. Before fencing, finish/recover live child work and durable
-- assistant answers with the current worker, then stop every new worker again.
-- Both preflight queries must return zero rows. Do not disable any guard.
SELECT run.id, run.lease_expires_at
FROM public.agent_runs run
WHERE run.execution_policy = 'sms_read_only' AND run.state = 'running'
  AND run.lease_expires_at > clock_timestamp();
SELECT run.id
FROM public.agent_runs run
WHERE run.execution_policy = 'sms_read_only' AND run.state = 'running'
  AND (EXISTS (SELECT 1 FROM public.agent_run_provider_attempts attempt
               WHERE attempt.run_id = run.id AND attempt.state IN ('reserved', 'dispatched'))
    OR EXISTS (SELECT 1 FROM public.agent_run_tool_invocations tool
               WHERE tool.run_id = run.id AND tool.state IN ('proposed', 'dispatched'))
    OR EXISTS (SELECT 1 FROM public.agent_run_provider_attempts attempt
               WHERE attempt.run_id = run.id AND attempt.state = 'succeeded'
                 AND attempt.response_state->>'kind' = 'assistant_message'
                 AND attempt.ordinal = (SELECT max(latest.ordinal)
                   FROM public.agent_run_provider_attempts latest WHERE latest.run_id = run.id)));

-- Two fenced run updates: reclaim expired running leases, then cancel both
-- queued and running SMS. A running lease keeps its generation on cancellation.
-- Queued runs may have no provider session; supply the required terminal shape.
-- Keep this transaction brief; any error requires ROLLBACK and investigation.
BEGIN;
UPDATE public.agent_runs
SET lease_token = gen_random_uuid(), lease_generation = lease_generation + 1,
    lease_expires_at = clock_timestamp() + interval '5 minutes',
    updated_at = clock_timestamp()
WHERE execution_policy = 'sms_read_only' AND state = 'running';
UPDATE public.agent_runs
SET state = 'cancelled', lease_token = NULL, lease_expires_at = NULL,
    provider_state = COALESCE(provider_state,
      jsonb_build_object('providerSessionId', 'pdr_' || replace(gen_random_uuid()::text, '-', ''))),
    terminal_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE execution_policy = 'sms_read_only' AND state IN ('queued', 'running');
UPDATE public.agent_threads
SET state = 'archived', updated_at = clock_timestamp()
WHERE ingress_channel = 'sms' AND state = 'active';
COMMIT;

-- Both counts must be zero before starting any older API reader or worker.
SELECT count(*) AS unfinished_sms_runs FROM public.agent_runs
WHERE execution_policy = 'sms_read_only' AND state IN ('queued', 'running');
SELECT count(*) AS active_sms_threads FROM public.agent_threads
WHERE ingress_channel = 'sms' AND state = 'active';
```
