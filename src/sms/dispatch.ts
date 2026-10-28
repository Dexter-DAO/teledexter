import { PrimaryApiError, type PrimarySmsClient } from '../broker/primaryApi.js';
import { SmsError, type SmsService } from './SmsService.js';

/** One drain at a time; pending API turns retain their SID and per-phone order. */
export async function dispatchSmsInbox(service: SmsService, primary: PrimarySmsClient, onError: (error: unknown) => void = () => {}) {
  if (!service.config().deliveryActive) return;
  const blocked = new Set<string>();
  for (const item of service.pendingTexts()) {
    if (blocked.has(item.phone)) continue;
    if (item.expired) { service.finishText(item.sid, 'suppressed'); continue; }
    if (item.deferred) { blocked.add(item.phone); continue; }
    try {
      let receipt: Awaited<ReturnType<SmsService['sendThreadReply']>> | undefined;
      if (item.state === 'pending_signup') receipt = await service.sendSignupReply(item.phone, item.sid);
      else {
        service.requestedEligibility(item.phone);
        const result = await primary.turn({ phone: item.phone, messageSid: item.sid, text: item.body });
        if (result.state === 'queued' || result.state === 'running') { blocked.add(item.phone); continue; }
        if (result.state === 'unregistered' || result.state === 'setup_required') receipt = await service.sendSignupReply(item.phone, item.sid, result.state === 'setup_required');
        else if (result.state === 'completed') receipt = await service.sendThreadReply(item.phone, item.sid, result.text);
      }
      service.finishText(item.sid, receipt?.status === 'uncertain' || receipt?.status === 'dispatching' ? 'uncertain' : 'replied');
    } catch (error) {
      if (error instanceof SmsError && ['sms_consent_required', 'carrier_opted_out'].includes(error.code))
        service.finishText(item.sid, 'suppressed');
      else if (error instanceof SmsError && error.code === 'sms_dispatch_uncertain')
        service.finishText(item.sid, 'uncertain');
      else if ((error instanceof SmsError && error.code === 'invalid_request')
        || (error instanceof PrimaryApiError && error.code === 'http_error' && [400, 401, 403, 404, 409, 410, 413, 415, 422].includes(error.status ?? 0))) {
        service.finishText(item.sid, 'suppressed'); onError(error);
      } else { service.retryText(item.sid); blocked.add(item.phone); onError(error); } // Bounded retry; carrier sends keep their durable reservation.
    }
  }
}
