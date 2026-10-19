import { z } from 'zod';
import type { LiveTranscriptFragment } from '../live/types.js';
import { TOLD_BLOCK_BUDGET, toldBlock } from './toldFacts.js';

// Same optional wire contract as Primary's phoneVoiceContextSchema. Advisory
// context is deliberately excluded from request identity and reconcile proofs.
export const voiceContextSchema = z.object({ recentSpeech: z.array(z.object({
  role: z.enum(['caller', 'dexter']), text: z.string().min(1).max(4000),
}).strict()).min(1).max(6) }).strict().refine(value =>
  value.recentSpeech.reduce((size, turn) => size + turn.text.length, 0) <= 6000);
export type VoiceContext = z.infer<typeof voiceContextSchema>;

/**
 * The speech of the last 90 s before a request, at most six turns, for resolving short replies. When
 * Dexter told the caller account facts earlier in the call that this window no longer holds, the first
 * item is one compact 'dexter' block of them ("Earlier on this call, the voice already told the
 * caller: …") and the window keeps five turns. The same six-item, 6000-character contract either way.
 */
export function conversationContext(fragments: readonly LiveTranscriptFragment[], beforeSequence: number,
  atMs: number, told: { facts: readonly string[]; anchor: string } = { facts: [], anchor: '' }): VoiceContext | undefined {
  const turns: VoiceContext['recentSpeech'] = [];
  for (const fragment of fragments) {
    if (fragment.sequence >= beforeSequence || fragment.startMs < atMs - 90_000 || fragment.endMs > atMs) continue;
    const role = fragment.role === 'user' ? 'caller' : 'dexter';
    if (turns.at(-1)?.role === role) turns.at(-1)!.text += fragment.text.replace(/[\0\uD800-\uDFFF]/gu, '');
    else turns.push({ role, text: fragment.text.replace(/[\0\uD800-\uDFFF]/gu, '') });
  }
  const recent = (count: number, budget: number) => {
    let remaining = budget;
    return turns.filter(turn => turn.text.trim()).slice(-count).reverse().flatMap(turn => {
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
  };
  if (told.facts.length) {
    const window = recent(5, 6000 - TOLD_BLOCK_BUDGET);
    const block = toldBlock(told.facts, window.filter(turn => turn.role === 'dexter').map(turn => turn.text), told.anchor);
    if (block) return voiceContextSchema.parse({ recentSpeech: [{ role: 'dexter', text: block }, ...window] });
  }
  const recentSpeech = recent(6, 6000);
  return recentSpeech.length ? voiceContextSchema.parse({ recentSpeech }) : undefined;
}
