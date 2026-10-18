/** Reflected Live audio is signed little-endian mono at 24 kHz. Twilio takes 8 kHz G.711. */
export function pcmToPcmu(pcm: Buffer): Buffer {
  const output = Buffer.alloc(Math.floor(pcm.length / 6));
  for (let i = 0; i < output.length; i++) {
    let sample = Math.round((pcm.readInt16LE(i * 6) + pcm.readInt16LE(i * 6 + 2) + pcm.readInt16LE(i * 6 + 4)) / 3);
    const sign = sample < 0 ? 0x80 : 0;
    sample = Math.min(32635, Math.abs(sample)) + 132;
    let exponent = 7;
    for (let mask = 0x4000; exponent > 0 && !(sample & mask); exponent--, mask >>= 1) { /* G.711 segment */ }
    output[i] = ~(sign | (exponent << 4) | ((sample >> (exponent + 3)) & 15)) & 255;
  }
  return output;
}
