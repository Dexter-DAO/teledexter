import type { PhoneResultSmsAdmissionReceipt } from './resultSmsContract.js';

/** Spoken acknowledgement follows the durable admission receipt, never classification alone. */
export function resultSmsAcknowledgement(receipt?: PhoneResultSmsAdmissionReceipt, error?: string) {
  if(receipt) {
    if(receipt.state==='delivered')return 'The text was delivered.';
    if(receipt.state==='accepted')return 'The text is awaiting delivery confirmation.';
    if(receipt.state==='uncertain'||receipt.state==='dispatching')return 'Your text request is saved. Delivery has not been confirmed.';
    if(receipt.state==='refused'||receipt.state==='expired')return 'Text delivery did not complete. You can check the task in your Dexter account.';
    return 'Your text request is saved. I will text a link when the answer is ready.';
  }
  if(error==='sms_consent_required')return 'Please enable texts at dexter.cash/sms before asking me to send an answer.';
  if(error==='carrier_opted_out')return 'Texts are stopped for this number. You can open your saved work in your Dexter account.';
  if(error==='sms_unsupported_number')return 'This number is not supported for text delivery yet.';
  if(error==='sms_delivery_pending')return 'Text delivery is unavailable right now. I could not save the text request.';
  if(error==='primary_run_failed'||error==='primary_run_cancelled')return 'That task ended without an answer to send.';
  return 'I could not confirm the text request. You can check your saved work in your Dexter account.';
}

export function resultSmsResearchRefusal(error?: string) {
  return `${resultSmsAcknowledgement(undefined,error)} Research has not started. You can ask me to research it without a text.`;
}
