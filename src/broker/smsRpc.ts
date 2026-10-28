import type { MessagePort, Worker } from 'node:worker_threads';
import { SmsError } from '../sms/SmsService.js';
import { PrimaryApiError } from './primaryApi.js';

type WireError = { message: string; name: string; code?: string | number; status?: number; remoteCode?: string; retryAfterSeconds?: number; retryable?: boolean };
function encode(error: unknown): WireError {
  const e = error as Partial<WireError> | null;
  return { message: e?.message || 'sms_unavailable', name: error instanceof SmsError ? 'SmsError' : e?.name || 'Error', code: e?.code,
    status: e?.status, remoteCode: e?.remoteCode, retryable: e?.retryable, retryAfterSeconds: e?.retryAfterSeconds };
}
function decode(error: WireError): Error {
  if (error.name === 'SmsError') return new SmsError(String(error.code), error.status, error.retryAfterSeconds);
  if (error.name === 'PrimaryApiError') return new PrimaryApiError(error.code as ConstructorParameters<typeof PrimaryApiError>[0], error.status, error.remoteCode ? { code: error.remoteCode, retryable: error.retryable ?? false } : undefined);
  return Object.assign(new Error(error.message), error);
}

/** Small, trusted parent/worker RPC. Only bounded requests and receipts cross this port. */
export function smsRpc(port: MessagePort | Worker, handle: (method: string, args: any[]) => unknown) {
  let sequence = 0, failed: Error | undefined;
  const pending = new Map<number, { resolve(value: any): void; reject(error: Error): void }>();
  port.on('message', async (message: { id: number; method?: string; args?: any[]; value?: unknown; error?: WireError }) => {
    if (message.method) {
      let response;
      try { response = { id: message.id, value: await handle(message.method, message.args ?? []) }; }
      catch (error) { response = { id: message.id, error: encode(error) }; }
      try { port.postMessage(response); } catch {} // The peer may have exited while I/O was in flight.
    } else {
      const request = pending.get(message.id);
      pending.delete(message.id);
      if (message.error) request?.reject(decode(message.error)); else request?.resolve(message.value);
    }
  });
  return {
    call<T = any>(method: string, ...args: any[]): Promise<T> {
      if (failed) return Promise.reject(failed);
      const id = ++sequence;
      return new Promise((resolve, reject) => {
        pending.set(id, { resolve, reject });
        try { port.postMessage({ id, method, args }); }
        catch (error) { pending.delete(id); reject(error); }
      });
    },
    fail(error: Error) {
      failed = error;
      for (const request of pending.values()) request.reject(error);
      pending.clear();
    },
  };
}
