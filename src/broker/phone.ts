import { timingSafeEqual, createHmac } from 'node:crypto';
import Twilio from 'twilio';
import type { BrokerConfig } from './config.js';

export function authorizedControl(header: string | undefined, token: string): boolean {
  const actual = Buffer.from(header || '');
  const expected = Buffer.from(`Bearer ${token}`);
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}

export interface ProviderCallProof {
  url: string; signature: string; params: Record<string, string>;
}

export function verifyTwilio(config: Pick<BrokerConfig, 'twilioAuthToken' | 'twilioAccountSid' | 'publicOrigin'>,
  signature: string | undefined, route: string, params: Record<string, string>, websocket = false): ProviderCallProof | undefined {
  if (!signature || !route.startsWith('/broker/') || route.includes('?') || route.includes('#')) return undefined;
  const url = config.publicOrigin + route;
  const valid = Twilio.validateRequest(config.twilioAuthToken, signature, url, params)
    || (websocket && Twilio.validateRequest(config.twilioAuthToken, signature, url.replace(/^https:/, 'wss:'), params));
  if (!valid || (!websocket && params.AccountSid !== config.twilioAccountSid)) return undefined;
  return { url, signature, params };
}

export const streamToken = (secret: string, id: string, callSid: string) => createHmac('sha256', secret).update(`dexter-broker/media/v1:${id}:${callSid}`).digest('base64url');

/** Media stream instructions. When the broker closes the stream, Twilio runs the next verb:
 * a hangup, or (`continuationUrl`) a signed redirect that lets an inbound leg whose admission
 * ended without account media receive its next instructions (restricted setup or hangup). */
export function callTwiml(origin: string, id: string, token: string, continuationUrl?: string): string {
  const response = new Twilio.twiml.VoiceResponse();
  const stream = response.connect().stream({ url: `${origin.replace(/^https:/, 'wss:')}/broker/media/${id}` });
  stream.parameter({ name: 'token', value: token });
  if (continuationUrl) response.redirect({ method: 'POST' }, continuationUrl);
  else response.hangup();
  return response.toString();
}

export function fallbackTwiml(text: string): string {
  const response = new Twilio.twiml.VoiceResponse();
  response.say({ voice: 'Polly.Matthew' }, text);
  response.hangup();
  return response.toString();
}

export function mediaStart(start: Record<string, unknown>, expected: { accountSid: string; callSid: string; token: string }): string {
  const format = start.mediaFormat as Record<string, unknown> | undefined;
  const custom = start.customParameters as Record<string, unknown> | undefined;
  if (start.accountSid !== expected.accountSid || start.callSid !== expected.callSid || typeof custom?.token !== 'string'
      || !authorizedControl(`Bearer ${custom.token}`, expected.token)) throw new Error('media_identity_mismatch');
  if (format?.encoding !== 'audio/x-mulaw' || format.sampleRate !== 8000 || format.channels !== 1) throw new Error('media_format_mismatch');
  if (typeof start.streamSid !== 'string' || !/^MZ[0-9a-f]{32}$/i.test(start.streamSid)) throw new Error('invalid_stream_id');
  return start.streamSid;
}

/** Adapted from fantasy interview playback receipts. Cleared audio is never counted as heard. */
export class PlaybackReceipts {
  sentBytes = 0; playedBytes = 0; clearedBytes = 0;
  private next = 0;
  audibleSentBytes = 0; audiblePlayedBytes = 0; audibleClearedBytes = 0;
  /** Speech-energy chunks (carriesSpeechPcmu), tracked only to time when speech is heard. */
  speechSentBytes = 0; speechPlayedBytes = 0;
  /** Unplayed audio dropped ahead of the first speech. It never held speech, so it is not clearedBytes. */
  preSpeechDroppedBytes = 0;
  private pending = new Map<string, { bytes: number; cleared: boolean; audible: boolean; speech: boolean }>();
  sent(bytes: number, audible = true, speech = false): string {
    this.sentBytes += bytes;
    if (audible) this.audibleSentBytes += bytes;
    if (speech) this.speechSentBytes += bytes;
    const id = `broker-audio-${++this.next}`;
    this.pending.set(id, { bytes, cleared: false, audible, speech });
    return id;
  }
  acknowledge(id: string) {
    const item = this.pending.get(id);
    if (!item) return;
    if (!item.cleared) {
      this.playedBytes += item.bytes;
      if (item.audible) this.audiblePlayedBytes += item.bytes;
      if (item.speech) this.speechPlayedBytes += item.bytes;
    }
    this.pending.delete(id);
  }
  clear() {
    for (const item of this.pending.values()) {
      if (!item.cleared) {
        this.clearedBytes += item.bytes;
        if (item.audible) this.audibleClearedBytes += item.bytes;
      }
      item.cleared = true;
    }
  }
  isPending(id: string) { const item = this.pending.get(id); return Boolean(item && !item.cleared); }
  /** Marks every unplayed chunk dropped when none of them carries speech; the
   * caller sends the carrier clear. Returns the dropped bytes (0 = nothing dropped). */
  dropPreSpeech(): number {
    const unplayed = [...this.pending.values()].filter(item => !item.cleared);
    if (!unplayed.length || unplayed.some(item => item.speech)) return 0;
    let dropped = 0;
    for (const item of unplayed) {
      item.cleared = true; dropped += item.bytes;
      if (item.audible) this.audibleClearedBytes += item.bytes;
    }
    this.preSpeechDroppedBytes += dropped;
    return dropped;
  }
  get backlogMs() { return (this.sentBytes - this.playedBytes - this.clearedBytes - this.preSpeechDroppedBytes) / 8; }
  get pendingCount() { return this.pending.size; }
}

/** Only the two exact PCMU zero encodings are silence. Quiet speech still
 * owns its playback mark; no energy threshold can discard it as inaudible. */
export function hasAudiblePcmu(bytes: Buffer): boolean {
  return bytes.length === 0 || bytes.some(byte => byte !== 0xff && byte !== 0x7f);
}

/** Output speech threshold: RMS -40 dBFS, the level preflight already treats as
 * audible Live output. Live's near-silent leading frames (breath/noise around
 * -58 dBFS) are nonzero PCMU, so hasAudiblePcmu counts them; this does not. */
export const OUTPUT_SPEECH_ENERGY = 0.01;
export function carriesSpeechPcmu(bytes: Buffer): boolean {
  return inputSpeechEnergy(bytes) >= OUTPUT_SPEECH_ENERGY;
}

/** PCMU energy helps avoid submitting while the caller is still speaking. */
export function inputSpeechEnergy(bytes: Buffer): number {
  if (!bytes.length) return 0;
  let squares = 0;
  for (const byte of bytes) {
    const value = (~byte) & 255;
    const magnitude = (((value & 15) << 3) + 132) << ((value & 112) >> 4);
    const sample = ((value & 128) ? 132 - magnitude : magnitude - 132) / 32768;
    squares += sample * sample;
  }
  return Math.sqrt(squares / bytes.length);
}

export function safeFailure(error: unknown): { code: string; message: string } {
  const value = error as { code?: string | number; message?: string };
  return {
    code: String(value?.code || 'broker_error').slice(0, 80),
    message: String(value?.message || 'The operation could not finish')
      .replace(/\+[1-9]\d{7,14}/g, '[phone]')
      .replace(/sk-[A-Za-z0-9_-]+/g, '[credential]')
      .replace(/Bearer\s+\S+/gi, 'Bearer [credential]').slice(0, 400),
  };
}
