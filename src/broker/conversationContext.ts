import { z } from 'zod';
import type { LiveTranscriptFragment } from '../live/types.js';

// Same optional wire contract as Primary's phoneVoiceContextSchema. Advisory
// context is deliberately excluded from request identity and reconcile proofs.
export const voiceContextSchema = z.object({ recentSpeech: z.array(z.object({
  role: z.enum(['caller', 'dexter']), text: z.string().min(1).max(4000),
}).strict()).min(1).max(6) }).strict().refine(value =>
  value.recentSpeech.reduce((size, turn) => size + turn.text.length, 0) <= 6000);
export type VoiceContext = z.infer<typeof voiceContextSchema>;

export function conversationContext(fragments: readonly LiveTranscriptFragment[], beforeSequence: number,
  atMs: number): VoiceContext | undefined {
  const turns: VoiceContext['recentSpeech'] = [];
  for (const fragment of fragments) {
    if (fragment.sequence >= beforeSequence || fragment.startMs < atMs - 90_000 || fragment.endMs > atMs) continue;
    const role = fragment.role === 'user' ? 'caller' : 'dexter';
    if (turns.at(-1)?.role === role) turns.at(-1)!.text += fragment.text.replace(/[\0\uD800-\uDFFF]/gu, '');
    else turns.push({ role, text: fragment.text.replace(/[\0\uD800-\uDFFF]/gu, '') });
  }
  let remaining = 6000;
  const recentSpeech = turns.filter(turn => turn.text.trim()).slice(-6).reverse().flatMap(turn => {
    const limit = Math.min(4000, remaining);
    let bounded = '';
    for (const character of Array.from(turn.text).reverse()) {
      if (bounded.length + character.length > limit) break;
      bounded = character + bounded;
    }
    if (!bounded.trim()) return [];
    remaining -= bounded.length;
    return [{ role: turn.role, text: bounded }];
  }).reverse();
  return recentSpeech.length ? voiceContextSchema.parse({ recentSpeech }) : undefined;
}
