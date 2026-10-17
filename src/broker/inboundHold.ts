import Twilio from 'twilio';
import type { BrokerStore } from './store.js';

/** Inbound hold responses for the same-call attachment flow; startup selects this when BROKER_VERIFICATION_SETUP_MODEL is set. */
export function createInboundHoldAdapter(options: { publicOrigin: string; store: Pick<BrokerStore, 'get'> }) {
  const origin = new URL(options.publicOrigin);
  if (origin.protocol !== 'https:' || origin.username || origin.password || origin.search || origin.hash
    || origin.pathname !== '/' || origin.origin !== options.publicOrigin) throw new Error('Invalid inbound hold origin');
  return {
    waitingResponse(input: { callId: string; continuationUrl: string }): string {
      if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(input.callId)
        || input.continuationUrl !== `${origin.origin}/broker/inbound/${input.callId}/continue`)
        throw new Error('Inbound hold continuation mismatch');
      const call = options.store.get(input.callId);
      if (!call?.inbound || call.direction !== 'inbound' || call.inbound.localStop || call.endedAt
        || call.inbound.begin.providerProof.url !== `${origin.origin}/broker/incoming-call`)
        throw new Error('Inbound hold call unavailable');
      // A bare redirect does not answer the leg: Twilio posts the signed continuation
      // while the caller still hears ringing (376-449 ms after the first webhook on calls
      // 28a563f6 and d1b57e33). The continuation answers with the media stream, so the
      // greeting clip starts as soon as the call is picked up.
      const response = new Twilio.twiml.VoiceResponse();
      response.redirect({ method: 'POST' }, input.continuationUrl);
      return response.toString();
    },
  };
}
