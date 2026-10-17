import type { BrokerLiveConnection } from '../../src/live/types.js';
import type { ClientEvent } from 'openai/resources/live/live';

/** Multicast transport events match the installed SDK while recording every command. */
export class PreconnectTransport implements BrokerLiveConnection {
  constructor(readonly sessionId = 'preconnect_test') {}
  sent: ClientEvent[] = [];
  opened = false;
  terminated = 0;
  closed = 0;
  opens: Array<() => void> = [];
  events: Array<(event: unknown) => void> = [];
  errors: Array<(error: unknown) => void> = [];
  closes: Array<() => void> = [];
  onOpen(fn: () => void) { this.opens.push(fn); }
  onEvent(fn: (event: unknown) => void) { this.events.push(fn); }
  onError(fn: (error: unknown) => void) { this.errors.push(fn); }
  onClose(fn: () => void) { this.closes.push(fn); }
  isOpen() { return this.opened; }
  open() { this.opened = true; for (const fn of this.opens) fn(); }
  fail() { for (const fn of this.errors) fn(new Error('fixture_transport_failure')); }
  end() { this.opened = false; for (const fn of this.closes) fn(); }
  receive(event: unknown) { for (const fn of this.events) fn(event); }
  send(event: ClientEvent) {
    this.sent.push(event);
    const session = { id: this.sessionId, model: 'gpt-live-1', status: 'active', expires_at: 1_800_000_000,
      delegation: { type: 'client' }, audio: { format: { type: 'audio/pcmu', rate: 8000 }, output: { voice: 'marin' } } };
    if (event.type === 'session.start') queueMicrotask(() => this.receive({ type: 'session.started', event_id: 'started', session }));
    if (event.type === 'session.close') queueMicrotask(() => this.receive({ type: 'session.closed', event_id: 'closed', session, reason: 'close_requested' }));
    if (event.type === 'session.instructions.append') queueMicrotask(() => this.receive({
      type: 'session.instructions.appended', event_id: `ack_${event.event_id}`, client_event_id: event.event_id, start_ms: 0, end_ms: 0,
    }));
    if (event.type === 'session.thinking.append') queueMicrotask(() => this.receive({
      type: 'session.thinking.appended', event_id: `ack_${event.event_id}`, client_event_id: event.event_id, start_ms: 0, end_ms: 0,
    }));
  }
  close() { this.closed++; this.end(); }
  terminate() { this.terminated++; this.end(); }
}
