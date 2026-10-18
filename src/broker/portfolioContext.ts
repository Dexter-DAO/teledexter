import { liveResultChunks } from './backend.js';
import { portfolioFactsText } from './portfolio.js';
import type { PrimaryApiClient, PrimaryResult } from './primaryApi.js';
import { PrimaryApiError } from './primaryApi.js';
import type { BrokerLiveSession } from '../live/types.js';

/** One call-owned observation, replayed only into its currently admitted voice. */
export function createPortfolioContext(callId: string, bindingId: string,
  api: Pick<PrimaryApiClient, 'portfolio'>) {
  let voice: BrokerLiveSession | undefined;
  let note: { content: string; observedAt: string } | undefined;
  let stopped = false, revision = 0, publishedVersion: number | undefined;
  let read: AbortController | undefined, actionQueued = false;
  let timer: ReturnType<typeof setInterval> | undefined, expiry: ReturnType<typeof setTimeout> | undefined;
  let publishing = Promise.resolve();
  const refreshed = new Set<string>(), changedRequests = new Set<string>(), changing = new Set<string>();
  const publish = (unchanged = false) => {
    const target = voice, value = note, version = revision;
    if (!target || !value || stopped) return Promise.resolve(false);
    let complete = false;
    publishing = publishing.then(async () => {
      if (stopped || target !== voice || version !== revision || !note) return;
      const current = () => !stopped && target === voice && version === revision && !!note;
      const freshness = `As of ${value.observedAt}; valid until ${new Date(Date.parse(value.observedAt) + 60_000).toISOString()}. State this as-of age for a portfolio-only fresh view; after 60 seconds or for anything else, use Primary.`;
      const send = (text: string) => target.appendContext(text);
      // An unchanged observation needs only a new timestamp, not another holdings payload.
      if (unchanged && publishedVersion === version) {
        complete = (await send(`Portfolio note ${version} is unchanged. ${freshness}`)).status === 'accepted';
        return;
      }
      if ((await send(`Portfolio note ${version} replaces all earlier portfolio notes. Loading; use Primary for holdings until this note is complete.`)).status !== 'accepted') return;
      for (const part of liveResultChunks(`Caller's portfolio as of ${value.observedAt}: ${value.content}`)) {
        if (!current()) return;
        if ((await send(part)).status !== 'accepted') return;
      }
      if (current()) {
        complete = (await send(`Portfolio note ${version} is complete and is the only current portfolio note. ${freshness}`)).status === 'accepted';
        if (complete && current()) publishedVersion = version;
      }
    }).catch(() => { /* Optional context cannot interrupt the call. */ });
    return publishing.then(() => complete && !stopped && target === voice && version === revision && !!note);
  };
  const invalidate = (supersede = true) => {
    note = undefined; publishedVersion = undefined; clearTimeout(expiry);
    if (supersede) { revision++; read?.abort(); }
    const target = voice;
    if (target && !stopped) void target.appendContext('Portfolio note withdrawn. Use Primary for holdings and fresh views until a replacement note is complete.').catch(() => {});
  };
  const refresh = async (reason: 'pickup' | 'background' | 'post_action') => {
    if (stopped || !api.portfolio) return;
    if (read) { if (reason === 'post_action') actionQueued = true; return; }
    const controller = new AbortController(); read = controller;
    const version = revision, started = performance.now();
    let outcome = 'error', publication = 'none';
    try {
      const facts = await api.portfolio(bindingId, controller.signal);
      if (stopped || version !== revision) { outcome = 'superseded'; return; }
      const remaining = Date.parse(facts.observedAt) + 60_000 - Date.now();
      if (remaining <= 0 || remaining > 60_000) { outcome = 'stale'; return; }
      const content = portfolioFactsText(facts, true), unchanged = note?.content === content;
      if (!unchanged) { revision++; publishedVersion = undefined; }
      note = { content, observedAt: facts.observedAt };
      clearTimeout(expiry);
      // Expiry withdraws permission to answer; it need not cancel a newer read.
      expiry = setTimeout(() => invalidate(false), remaining); expiry.unref?.();
      outcome = 'success';
      publication = await publish(unchanged) ? unchanged ? 'unchanged' : 'published' : 'deferred';
    } catch (error) {
      outcome = stopped ? 'closed' : version !== revision ? 'superseded'
        : controller.signal.aborted || (error instanceof PrimaryApiError && error.code === 'request_timeout') ? 'timeout' : 'error';
    } finally {
      read = undefined;
      try { console.info(JSON.stringify({ event: 'broker_portfolio_fetch', callId,
        latencyMs: Math.round(performance.now() - started), outcome, publication, reason })); } catch { /* Telemetry only. */ }
      // Wait for even an aborted read to settle before issuing the post-action read.
      if (actionQueued && !stopped) { actionQueued = false; void refresh('post_action'); }
    }
  };
  void refresh('pickup');
  return {
    attach(next: BrokerLiveSession) {
      if (voice === next || stopped) return;
      voice = next; publishedVersion = undefined; publishing = Promise.resolve(); void publish();
      timer ??= setInterval(() => { if (voice && !changing.size) void refresh('background'); }, 30_000);
      timer.unref?.();
    },
    detach(previous: BrokerLiveSession) {
      if (voice === previous) { voice = undefined; clearInterval(timer); timer = undefined; }
    },
    observe(result: PrimaryResult) {
      if (stopped || result.bindingId !== bindingId) return;
      const action = result.partialResult;
      if (action?.kind === 'action_status' && ['submitted', 'pending', 'unknown', 'confirmed'].includes(action.actionState)) {
        const key = `${result.requestId}:${action.intentId}`;
        if (action.actionState === 'confirmed') changing.delete(result.requestId); else changing.add(result.requestId);
        if (!changedRequests.has(key)) { changedRequests.add(key); invalidate(); }
        if (action.actionState === 'confirmed' && !refreshed.has(key)) {
          refreshed.add(key); void refresh('post_action');
        }
      }
      if (['completed', 'failed', 'cancelled'].includes(result.state)) changing.delete(result.requestId);
      // Completion also covers transfers and multiple trades between observations.
      if (result.state === 'completed' && !refreshed.has(result.requestId)) {
        refreshed.add(result.requestId); invalidate(); void refresh('post_action');
      }
    },
    stop() { stopped = true; clearInterval(timer); clearTimeout(expiry); actionQueued = false; read?.abort(); voice = undefined; note = undefined; },
  };
}
export type PortfolioContext = ReturnType<typeof createPortfolioContext>;
