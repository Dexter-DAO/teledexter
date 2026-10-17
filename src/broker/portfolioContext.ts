import { liveResultChunks } from './backend.js';
import { portfolioFactsText } from './portfolio.js';
import type { PrimaryApiClient, PrimaryResult } from './primaryApi.js';
import { PrimaryApiError } from './primaryApi.js';
import type { BrokerLiveSession } from '../live/types.js';

/** One call-owned observation, replayed only into its currently admitted voice. */
export function createPortfolioContext(callId: string, bindingId: string,
  api: Pick<PrimaryApiClient, 'portfolio'>) {
  let voice: BrokerLiveSession | undefined, note: string | undefined;
  let stopped = false, revision = 0;
  let read: AbortController | undefined;
  let publishing = Promise.resolve();
  const refreshed = new Set<string>(), changedRequests = new Set<string>();
  const publish = () => {
    const target = voice, text = note, version = revision;
    if (!target || !text || stopped) return;
    publishing = publishing.then(async () => {
      if (stopped || target !== voice || version !== revision) return;
      // Each part names the same replacement. Completion activates it only
      // after every part is acknowledged; incomplete notes authorize no answer.
      const parts = liveResultChunks(text);
      const send = (text: string) => target.appendContext(text);
      if ((await send(`Portfolio note ${version} replaces all earlier portfolio notes. Loading; use Primary for holdings until this note is complete.`)).status !== 'accepted') return;
      for (const part of parts) {
        if (stopped || target !== voice || version !== revision) return;
        if ((await send(part)).status !== 'accepted') return;
      }
      if (!stopped && target === voice && version === revision)
        await send(`Portfolio note ${version} is complete and is the only current portfolio note.`);
    }).catch(() => { /* Optional context cannot interrupt the call. */ });
  };
  const invalidate = () => {
    note = undefined; revision++;
    read?.abort();
    const target = voice;
    if (target && !stopped) void target.appendContext('Portfolio note withdrawn. Use Primary for holdings until a replacement note is complete.').catch(() => {});
  };
  const refresh = async () => {
    if (stopped || !api.portfolio) return;
    read?.abort();
    const controller = new AbortController(); read = controller;
    const version = ++revision, started = performance.now();
    let outcome = 'error';
    try {
      const facts = await api.portfolio(bindingId, controller.signal);
      if (stopped || version !== revision) { outcome = 'superseded'; return; }
      note = `Caller's portfolio as of ${facts.observedAt}: ${portfolioFactsText(facts, true)}`;
      outcome = 'success'; publish();
    } catch (error) {
      outcome = stopped ? 'closed' : version !== revision ? 'superseded'
        : controller.signal.aborted || (error instanceof PrimaryApiError && error.code === 'request_timeout') ? 'timeout' : 'error';
    } finally {
      try { console.info(JSON.stringify({ event: 'broker_portfolio_fetch', callId,
        latencyMs: Math.round(performance.now() - started), outcome })); } catch { /* Telemetry only. */ }
    }
  };
  void refresh();
  return {
    attach(next: BrokerLiveSession) { if (voice === next || stopped) return; voice = next; publishing = Promise.resolve(); publish(); },
    detach(previous: BrokerLiveSession) { if (voice === previous) voice = undefined; },
    observe(result: PrimaryResult) {
      if (stopped || result.bindingId !== bindingId) return;
      const action = result.partialResult;
      if (action?.kind === 'action_status' && ['submitted', 'pending', 'unknown', 'confirmed'].includes(action.actionState)) {
        const key = `${result.requestId}:${action.intentId}`;
        if (!changedRequests.has(key)) { changedRequests.add(key); invalidate(); }
        if (action.actionState === 'confirmed' && !refreshed.has(key)) {
          refreshed.add(key); void refresh();
        }
      }
      // Completed results have no general action discriminator. Re-reading also
      // covers transfers, without classifying assistant text or caller speech.
      if (result.state === 'completed' && !refreshed.has(result.requestId)) {
        refreshed.add(result.requestId); invalidate(); void refresh();
      }
    },
    stop() { stopped = true; read?.abort(); voice = undefined; note = undefined; },
  };
}
export type PortfolioContext = ReturnType<typeof createPortfolioContext>;
