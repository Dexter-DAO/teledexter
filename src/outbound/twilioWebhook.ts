import Twilio from 'twilio';

export interface TwilioWebhookRequestShape {
  headers: Record<string, unknown>;
  rawUrl: string;
  body: unknown;
}

export function canonicalTwilioWebhookUrl(publicBaseUrl: string, rawUrl: string): string {
  return new URL(rawUrl, publicBaseUrl.endsWith('/') ? publicBaseUrl : `${publicBaseUrl}/`).toString();
}

export function normalizeTwilioParams(body: unknown): Record<string, string> {
  if (!body || typeof body !== 'object' || Array.isArray(body)) return {};
  const params: Record<string, string> = {};
  for (const [key, value] of Object.entries(body as Record<string, unknown>)) {
    if (typeof value === 'string') params[key] = value;
    else if (typeof value === 'number' || typeof value === 'boolean') params[key] = String(value);
  }
  return params;
}

export function validateTwilioWebhook(
  authToken: string,
  publicBaseUrl: string,
  request: TwilioWebhookRequestShape,
): boolean {
  const signatureHeader = request.headers['x-twilio-signature'];
  const signature = Array.isArray(signatureHeader) ? signatureHeader[0] : signatureHeader;
  if (typeof signature !== 'string' || !signature) return false;

  const url = canonicalTwilioWebhookUrl(publicBaseUrl, request.rawUrl);
  return Twilio.validateRequest(
    authToken,
    signature,
    url,
    normalizeTwilioParams(request.body),
  );
}
