import type Twilio from 'twilio';
import { validateReleaseIndependentStatePath } from '../outbound/PhoneFinancialReadiness.js';
import { SmsService } from './SmsService.js';

export function configureSms(env: NodeJS.ProcessEnv, clients: { verify: ReturnType<typeof Twilio>; messaging: ReturnType<typeof Twilio> }, releaseRoot: string): SmsService | null {
  const state = validateReleaseIndependentStatePath(env.DEXTER_SMS_STATE_PATH, releaseRoot);
  const publicBaseUrl = (env.DEXTER_PHONE_PUBLIC_URL || '').replace(/\/+$/, '');
  const internalSecret = env.DEXTER_SMS_INTERNAL_SECRET || '';
  const verifySid = env.TWILIO_VERIFY_SID || '';
  if (!state.ok || internalSecret.length < 32 || !/^VA[0-9a-f]{32}$/i.test(verifySid)) return null;
  // Credentials must be explicitly configured; SMS never uses source-code fallbacks.
  const configuredVerifyAccount = env.TWILIO_VERIFY_ACCOUNT_SID || env.TWILIO_ACCOUNT_SID;
  const configuredVerifyToken = env.TWILIO_VERIFY_AUTH_TOKEN || env.TWILIO_AUTH_TOKEN;
  if (!configuredVerifyAccount || !configuredVerifyToken || !env.TWILIO_ACCOUNT_SID || !env.TWILIO_AUTH_TOKEN) return null;
  try {
    const url = new URL(publicBaseUrl);
    if (url.protocol !== 'https:' || url.username || url.password || url.pathname !== '/' || url.search || url.hash) return null;
    const consentRetentionDays = Number(env.DEXTER_SMS_CONSENT_RETENTION_DAYS || 1461);
    const messageRetentionDays = Number(env.DEXTER_SMS_MESSAGE_RETENTION_DAYS || 90);
    if (![consentRetentionDays, messageRetentionDays].every(x => Number.isInteger(x) && x >= 1 && x <= 36525)) return null;
    return new SmsService({
      statePath: state.path,
      enrollmentEnabled: env.DEXTER_SMS_ENROLLMENT_ENABLED === 'true',
      sendEnabled: env.DEXTER_SMS_SEND_ENABLED === 'true' && (env.TWILIO_VALIDATE_WEBHOOKS || 'true').toLowerCase() !== 'false',
      campaignApproved: env.DEXTER_SMS_CAMPAIGN_APPROVED === 'true',
      advancedOptOutConfigured: env.DEXTER_SMS_ADVANCED_OPT_OUT_CONFIGURED === 'true',
      messagingServiceSid: env.TWILIO_MESSAGING_SERVICE_SID || '',
      publicBaseUrl, consentRetentionDays, messageRetentionDays,
      provider: {
        startVerification: async phone => { await clients.verify.verify.v2.services(verifySid).verifications.create({ to: phone, channel: 'sms' }); },
        checkVerification: async (phone, code) => (await clients.verify.verify.v2.services(verifySid).verificationChecks.create({ to: phone, code })).status === 'approved',
        send: async input => { const m = await clients.messaging.messages.create(input); return { sid: m.sid, status: m.status }; },
      },
    });
  } catch { return null; }
}
