const CALL_SID_PATTERN = /^CA[0-9a-fA-F]{32}$/;
const STREAM_TOKEN_PATTERN = /^[A-Za-z0-9_-]{43}$/;

function xmlEscape(value: string): string {
  return value
    .replaceAll('&', '&amp;')
    .replaceAll('"', '&quot;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;');
}

export function isValidCallSid(callSid: string): boolean {
  return CALL_SID_PATTERN.test(callSid);
}

export function isValidStreamToken(streamToken: string): boolean {
  return STREAM_TOKEN_PATTERN.test(streamToken);
}

export function isValidMediaBootstrapParams(callSid: string, streamToken: string): boolean {
  return isValidCallSid(callSid) && isValidStreamToken(streamToken);
}

/**
 * Twilio Media Streams reject query strings, but support ordinary URL path
 * segments. Path-bound bootstrap makes the exact CallSid and one-shot token
 * available before RealtimeSession.connect(), so the SDK receives Twilio's
 * original start event directly with no interception or buffering.
 */
export function buildPathBoundMediaStreamUrl(
  websocketBaseUrl: string,
  callSid: string,
  streamToken: string,
): string {
  if (!isValidMediaBootstrapParams(callSid, streamToken)) {
    throw new Error('invalid media bootstrap parameters');
  }
  const base = websocketBaseUrl.endsWith('/') ? websocketBaseUrl : `${websocketBaseUrl}/`;
  return new URL(
    `media-stream/${encodeURIComponent(callSid)}/${encodeURIComponent(streamToken)}`,
    base,
  ).toString();
}

export function buildTwilioStreamTwiml(options: {
  streamUrl: string;
  fallbackMessage: string;
}): string {
  const { streamUrl, fallbackMessage } = options;
  return `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="${xmlEscape(streamUrl)}" />
  </Connect>
  <Say voice="Polly.Matthew">${xmlEscape(fallbackMessage)}</Say>
</Response>`.trim();
}

export function buildTwilioFallbackTwiml(message: string): string {
  return `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Say voice="Polly.Matthew">${xmlEscape(message)}</Say>
</Response>`.trim();
}
