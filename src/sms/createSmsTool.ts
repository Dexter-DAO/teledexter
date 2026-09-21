import { tool } from '@openai/agents/realtime';
import { z } from 'zod';
import { SmsError, type SmsService } from './SmsService.js';

export function createCallerSmsSender(service: SmsService | null, callerPhone: string, callSid: string) {
  return async (message: string) => {
    if (!service) return { ok: false, error: 'sms_delivery_pending', enrollmentUrl: 'https://dexter.cash/sms' };
    try { return await service.sendRequested(callerPhone, callSid, message); }
    catch (e) { return { ok: false, error: e instanceof SmsError ? e.code : 'sms_unavailable', enrollmentUrl: 'https://dexter.cash/sms' }; }
  };
}

export function createSmsTool(service: SmsService | null, callerPhone: string, callSid: string) {
  const send = createCallerSmsSender(service, callerPhone, callSid);
  return tool({
    name: 'send_sms',
    description: 'Text information or links the caller asks to receive. The recipient is fixed to this call and must have opted in at dexter.cash/sms. Send only after the caller requests the text. A queued or accepted result means delivery is pending. SMS availability and consent are enforced by the server.',
    parameters: z.object({ message: z.string().min(1).max(1000).describe('The information or links the caller asked to receive by text. Dexter identification and opt-out instructions are added automatically.') }),
    execute: async ({ message }: { message: string }) => send(message),
  });
}
