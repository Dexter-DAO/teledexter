import path from 'node:path';

export interface BrokerConfig {
  host: string; port: number; publicOrigin: string; stateDir: string;
  controlToken: string; apiKey: string; model: string; voice: string;
  twilioAccountSid: string; twilioAuthToken: string; from: string;
  allowedTo: string[]; apiBaseUrl: string; phoneServiceSecret: string;
  harkApiBaseUrl?: string;
  harkCallbackSigningSecret?: string;
  harkInternalToken?: string;
  maxCallSeconds: number;
}

export const isPhoneNumber = (value: unknown): value is string =>
  typeof value === 'string' && /^\+[1-9]\d{7,14}$/.test(value);

export function brokerConfig(env: NodeJS.ProcessEnv): BrokerConfig {
  const required = (name: string) => {
    const value = env[name]?.trim();
    if (!value) throw new Error(`Missing ${name}`);
    return value;
  };
  const controlToken = required('BROKER_CONTROL_TOKEN');
  const phoneServiceSecret = required('PHONE_SERVICE_SECRET');
  if (controlToken.length < 32 || phoneServiceSecret.length < 32) throw new Error('Broker service credentials must have at least 32 characters');
  const publicOrigin = required('BROKER_PUBLIC_ORIGIN');
  const origin = new URL(publicOrigin);
  if (origin.protocol !== 'https:' || origin.origin !== publicOrigin || origin.username || origin.password) throw new Error('BROKER_PUBLIC_ORIGIN must be an HTTPS origin');
  const apiBaseUrl = required('DEXTER_API_URL').replace(/\/$/, '');
  const api = new URL(apiBaseUrl);
  if (api.username || api.password || api.search || api.hash || !['https:', 'http:'].includes(api.protocol)) throw new Error('Invalid DEXTER_API_URL');
  if (api.protocol === 'http:' && !['127.0.0.1', 'localhost', '[::1]'].includes(api.hostname)) throw new Error('Remote Dexter API requires HTTPS');
  const from = required('TWILIO_PHONE_NUMBER');
  const harkApiBaseUrl = env.BROKER_HARK_API_URL?.trim() || undefined;
  const harkCallbackSigningSecret = env.HARK_PHONE_CALLBACK_SIGNING_SECRET?.trim();
  const harkInternalToken = env.DEXTER_INTERNAL_TOKEN;
  if (harkApiBaseUrl) {
    let hark: URL;
    try { hark = new URL(harkApiBaseUrl); }
    catch { throw new Error('BROKER_HARK_API_URL must be a local HTTP origin'); }
    if (hark.protocol !== 'http:' || !['127.0.0.1', '[::1]', 'localhost'].includes(hark.hostname)
        || hark.username || hark.password || hark.search || hark.hash || hark.pathname !== '/')
      throw new Error('BROKER_HARK_API_URL must be a local HTTP origin');
    if (!harkCallbackSigningSecret || harkCallbackSigningSecret.length < 32
        || harkCallbackSigningSecret === env.OUTBOUND_ALERT_SIGNING_SECRET?.trim())
      throw new Error('Hark requires its existing independent callback signing secret');
    if (!harkInternalToken || Buffer.byteLength(harkInternalToken, 'utf8') > 4096)
      throw new Error('Hark requires the existing bounded internal token');
  }
  const allowedTo = (env.BROKER_ALLOWED_TO || '').split(',').map(v => v.trim()).filter(Boolean);
  if (!isPhoneNumber(from) || !allowedTo.every(isPhoneNumber) || allowedTo.includes(from)) throw new Error('Invalid broker phone configuration');
  const port = Number(env.BROKER_PORT || 3213);
  const maxCallSeconds = Number(env.BROKER_MAX_CALL_SECONDS || 1800);
  if (!Number.isInteger(port) || port < 1024 || port > 65535) throw new Error('Invalid BROKER_PORT');
  if (!Number.isInteger(maxCallSeconds) || maxCallSeconds < 30 || maxCallSeconds > 14400) throw new Error('Invalid BROKER_MAX_CALL_SECONDS');
  const stateDir = required('BROKER_STATE_DIR');
  if (!path.isAbsolute(stateDir)) throw new Error('BROKER_STATE_DIR must be absolute');
  const relative = path.relative(process.cwd(), stateDir);
  if (!relative || (!relative.startsWith('..' + path.sep) && relative !== '..' && !path.isAbsolute(relative))) throw new Error('Broker state must live outside the source or release directory');
  return {
    host: env.BROKER_HOST || '127.0.0.1', port, publicOrigin, stateDir, controlToken,
    apiKey: required('OPENAI_API_KEY'), model: env.BROKER_LIVE_MODEL || 'gpt-live-1',
    voice: env.BROKER_LIVE_VOICE || 'marin', twilioAccountSid: required('TWILIO_ACCOUNT_SID'),
    twilioAuthToken: required('TWILIO_AUTH_TOKEN'), from, allowedTo, apiBaseUrl,
    phoneServiceSecret, maxCallSeconds, ...(harkApiBaseUrl ? { harkApiBaseUrl, harkCallbackSigningSecret, harkInternalToken } : {}),
  };
}
