import { requireDexterApiBaseUrl } from '../outbound/DexterApiReadiness.js';
import { createHmac } from 'node:crypto';

export async function fetchCallerLookup(options: {
  apiBaseUrl: string;
  callerNumber: string;
  callSid?: string;
  serviceSecret: string;
  fetchImpl?: typeof fetch;
}): Promise<Response> {
  if (options.serviceSecret.trim().length < 32) {
    throw new Error('phone_lookup_auth_unavailable');
  }
  const query = new URLSearchParams({ number: options.callerNumber });
  if (options.callSid && options.callSid !== 'unknown') {
    query.set('callSid', options.callSid);
  }
  const credential = createHmac('sha256', options.serviceSecret.trim())
    .update('dexter-phone/caller-lookup/v1', 'utf8').digest('hex');
  const response = await (options.fetchImpl ?? fetch)(
    `${requireDexterApiBaseUrl(options.apiBaseUrl)}/internal/phone/lookup?${query}`,
    {
      headers: { authorization: `Bearer ${credential}` },
      signal: AbortSignal.timeout(4_000),
      redirect: 'error',
    },
  );
  if (!response.ok) throw new Error(`phone_lookup_http_${response.status}`);
  return response;
}
