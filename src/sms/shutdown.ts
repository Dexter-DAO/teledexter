import type { SmsService } from './SmsService.js';

/** Stop SMS mutations before releasing ownership, then drain the HTTP server. */
export function installPhoneShutdownHandlers(server: { close(): Promise<unknown> }, sms: SmsService | null) {
  let stopping = false;
  const shutdown = () => {
    if (stopping) return;
    stopping = true;
    sms?.close();
    const deadline = setTimeout(() => process.exit(0), 5_000);
    deadline.unref();
    void server.close().then(() => process.exit(0), () => process.exit(1));
  };
  process.once('SIGINT', shutdown);
  process.once('SIGTERM', shutdown);
}
