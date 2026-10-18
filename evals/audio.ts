import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';

/** G.711 mu-law encoding of one signed 16-bit sample, as Twilio sends caller audio. */
function mulaw(sample: number): number {
  const sign = sample < 0 ? 0x80 : 0;
  let magnitude = Math.min(32635, Math.abs(sample)) + 132;
  let exponent = 7;
  for (let mask = 0x4000; exponent > 0 && !(magnitude & mask); exponent--, mask >>= 1) { /* segment */ }
  return ~(sign | (exponent << 4) | ((magnitude >> (exponent + 3)) & 15)) & 255;
}

/** 24 kHz PCM16LE to 8 kHz mu-law: a 3-tap average before decimation keeps the phone band. */
export function pcm24kToPcmu(pcm: Buffer): Buffer {
  const out = Buffer.alloc(Math.floor(pcm.length / 6));
  for (let i = 0; i < out.length; i++) {
    const sum = pcm.readInt16LE(i * 6) + pcm.readInt16LE(i * 6 + 2) + pcm.readInt16LE(i * 6 + 4);
    out[i] = mulaw(Math.round(sum / 3));
  }
  return out;
}

export function pcmuEnergy(bytes: Buffer): number {
  // Decode mu-law to linear and return RMS on a 0..1 scale.
  let sum = 0;
  for (const value of bytes) {
    const u = ~value & 255, sign = u & 0x80, exponent = (u >> 4) & 7, mantissa = u & 15;
    const magnitude = ((mantissa << 3) + 132) << exponent;
    const sample = (sign ? 132 - magnitude : magnitude - 132) / 32768;
    sum += sample * sample;
  }
  return bytes.length ? Math.sqrt(sum / bytes.length) : 0;
}

const CALLER_VOICES = ['alloy', 'ash', 'ballad', 'coral', 'echo', 'sage', 'verse'] as const;
export const callerVoice = (seed: number) => CALLER_VOICES[seed % CALLER_VOICES.length];

/** Caller speech for a scripted turn, rendered once per (text, voice) and cached outside git. */
export async function callerAudio(apiKey: string, text: string, voice: string, cacheDir: string,
  spend: { ttsChars: number }): Promise<Buffer> {
  const instructions = 'You are a person on a phone call with an assistant. Speak casually and naturally, at a normal conversational pace, like a real caller. Do not sound like an announcer.';
  const key = createHash('sha256').update(JSON.stringify(['gpt-4o-mini-tts', voice, instructions, text])).digest('hex').slice(0, 24);
  const file = path.join(cacheDir, `${key}.pcmu`);
  if (fs.existsSync(file)) return fs.readFileSync(file);
  const response = await fetch('https://api.openai.com/v1/audio/speech', { method: 'POST',
    headers: { authorization: `Bearer ${apiKey}`, 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'gpt-4o-mini-tts', voice, input: text, instructions, response_format: 'pcm' }) });
  if (!response.ok) throw new Error(`tts_failed_${response.status}`);
  spend.ttsChars += text.length;
  const pcmu = pcm24kToPcmu(Buffer.from(await response.arrayBuffer()));
  fs.mkdirSync(cacheDir, { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, pcmu, { mode: 0o600 });
  return pcmu;
}
