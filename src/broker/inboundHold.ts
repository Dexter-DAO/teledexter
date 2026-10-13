import Twilio from 'twilio';
import type { BrokerStore } from './store.js';

/** Inbound hold responses for the same-call attachment flow; startup selects this when BROKER_VERIFICATION_SETUP_MODEL is set. */
export function createInboundHoldAdapter(options: { publicOrigin: string; store: Pick<BrokerStore, 'get'>;
  /** A recorded greeting clip opens the call; a spoken hold before it only delays the greeting. */
  greetingClipAvailable?: boolean }) {
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
      const response = new Twilio.twiml.VoiceResponse();
      // Say answers the first leg before its authenticated continuation when no
      // greeting clip exists. With a clip, the redirect alone answers and the clip
      // is the first thing the caller hears (call 2d141fec spoke "One moment." first).
      // Only later pending holds need a pause to pace repeated redirects.
      if (!call.inbound.continuation) { if (!options.greetingClipAvailable) response.say('One moment.'); }
      else response.pause({ length: 1 });
      response.redirect({ method: 'POST' }, input.continuationUrl);
      return response.toString();
    },
  };
}
