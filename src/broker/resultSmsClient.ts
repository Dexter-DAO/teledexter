import { createPrimaryApiTransport, PrimaryApiError, type PrimaryApiOptions } from './primaryApi.js';
import { phoneResultSmsDispatchRefSchema, phoneResultSmsResolvedSchema,
  type PhoneResultSmsDispatchRef, type PhoneResultSmsResolved } from './resultSmsContract.js';

export interface ResultSmsClient {
  /** Immutable identity lookup. This does not authorize a new carrier send. */
  resolveIdentity(ref: PhoneResultSmsDispatchRef): Promise<PhoneResultSmsResolved>;
  /** Rechecks deadline, current account authority, destination and result eligibility. */
  admitDispatch(ref: PhoneResultSmsDispatchRef): Promise<PhoneResultSmsResolved>;
}

export function createResultSmsClient(options: PrimaryApiOptions): ResultSmsClient {
  const request = createPrimaryApiTransport({ timeoutMs: 2_000, maxBodyBytes: 8_192, ...options });
  const perform = async (path: string, value: PhoneResultSmsDispatchRef) => {
    const input = phoneResultSmsDispatchRefSchema.safeParse(value);
    if (!input.success) throw new PrimaryApiError('invalid_request');
    let resolved: PhoneResultSmsResolved | undefined;
    await request('POST', `/result-sms/${path}`, input.data, undefined, 200, false, {
      validateResponse(_status, body) {
        const parsed = phoneResultSmsResolvedSchema.parse(body);
        if (parsed.identity.intentId !== input.data.intentId || parsed.intentDigest !== input.data.intentDigest
          || parsed.payload.messageId !== input.data.messageId) throw new Error('sms_identity_mismatch');
        resolved = parsed;
      },
    });
    if (!resolved) throw new PrimaryApiError('invalid_response');
    return resolved;
  };
  return { resolveIdentity: value => perform('identity', value), admitDispatch: value => perform('dispatch', value) };
}
