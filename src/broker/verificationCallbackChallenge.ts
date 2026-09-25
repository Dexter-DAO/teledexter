import { callbackProviderProofSchema, sameCallbackRef, type CallbackProviderProof,
  type PhoneVerificationCallbackRef } from './verificationCallbackContract.js';
import { parseVerificationCallbackJournal, type VerificationCallbackJournal } from './verificationCallbackJournal.js';

/** Ephemeral protected form consumer. This module has no storage or logging port.
 * The route must authenticate the exact external URL/form before calling capture,
 * and must keep its body out of generic request/transcript logging. */
export function createVerificationCallbackChallenge(options: {
  journal(): VerificationCallbackJournal;
  verifySignature(proof: CallbackProviderProof): boolean;
  /** True only after restricted Live input is synchronously gated and retired. */
  protectedInputActive(): boolean;
  now?: () => number;
}) {
  const now = options.now ?? Date.now;
  let challenge: { request: PhoneVerificationCallbackRef; callSid: string; value: string } | undefined;
  let captured = false;
  function fail(): never { throw new Error('callback_protected_input_refused'); }
  function current() {
    const j = parseVerificationCallbackJournal(options.journal());
    if (j.permit.purpose !== 'enrollment' || j.remote?.state !== 'answered' || !j.answer
      || j.completion || j.stopRequestedAt || j.endRequestedAt || j.providerTerminal
      || now() >= Math.min(Date.parse(j.permit.expiresAt), Date.parse(j.answer.startedAt) + j.permit.maxDurationSeconds * 1000)) fail();
    return { journal: j, sid: j.answer.proof.params.CallSid };
  }
  return {
    capture(raw: CallbackProviderProof): void {
      const parsed = callbackProviderProofSchema.safeParse(raw); if (!parsed.success) fail();
      const proof = parsed.data, { journal: j, sid } = current(), p = proof.params;
      if (!options.protectedInputActive() || captured || !options.verifySignature(proof)
        || proof.url !== j.permit.responseUrl || p.CallSid !== sid || p.AccountSid !== j.permit.providerAccountSid
        || p.From !== j.permit.from || p.To !== j.permit.to || p.Direction !== 'outbound-api' || p.CallStatus !== 'in-progress'
        || !/^[0-9]{8}$/.test(p.Digits ?? '') || Object.hasOwn(p, 'SpeechResult')) fail();
      captured = true; challenge = { request: j.permit.request, callSid: sid, value: p.Digits };
    },
    take(request: PhoneVerificationCallbackRef, callSid: string): string | undefined {
      const saved = challenge; challenge = undefined;
      if (!saved) return;
      const { journal: j, sid } = current();
      if (!sameCallbackRef(saved.request, request) || !sameCallbackRef(j.permit.request, request)
        || saved.callSid !== callSid || sid !== callSid) fail();
      return saved.value;
    },
    clear(): void { challenge = undefined; },
  };
}
