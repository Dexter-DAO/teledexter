import type { LiveTranscriptFragment } from '../live/types.js';

/**
 * What Dexter already told the caller on this call from account data: its own sentences that state an
 * amount, describe the portfolio or a holding, name a holding from a portfolio note, or report a
 * request's outcome. They are kept for the whole call, unlike the rolling recent-speech window. In
 * call 1325b55e the voice read the portfolio note in its first seconds; a minute later the caller asked
 * about a token Dexter had said they held, after that read-out had left the window, and Primary
 * answered that it had never mentioned the token.
 */
const AMOUNT = /[$€£]\s?\d|\d\.\d|\d[\d,]*\s*(?:%|percent\b|dollars?\b|cents?\b|usd\b|shares?\b|tokens?\b)|\b(?:dollars?|cents?|bucks|percent|twenty|thirty|forty|fifty|sixty|seventy|eighty|ninety|hundred|thousand|million|billion)\b/i;
const ACCOUNT = /\b(?:portfolio|holdings?|balance|worth|total|position|shares?|tokens?|cash)\b/i;
const OUTCOME = /\b(?:went through|go through|sold|bought|filled|executed|cancel(?:l)?ed|failed|no order|order was)\b/i;

const STOP_WORDS = new Set(['the', 'and', 'that', 'this', 'for', 'you', 'your', 'about', 'was', 'were', 'has', 'have', 'had', 'with',
  'its', 'it', 'is', 'are', 'not', 'yet', 'now', 'all', 'one', 'too', 'did', 'just', 'then', 'what', 'from', 'will']);
/** Content words of a sentence, for matching a claim to a task or to an earlier statement. */
export const contentWords = (text: string) => new Set((text.toLowerCase().match(/[a-z][a-z'-]{2,}/g) ?? [])
  .filter(word => !STOP_WORDS.has(word)));

const escapePattern = (value: string) => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
const compacted = (text: string) => text.toLowerCase().replace(/[^a-z0-9]/g, '');
// The guardrail checks every earlier sentence on each new one: build a call's matcher once, not per check.
let matcher: { key: string; test(text: string): boolean } | undefined;
/** Whether a sentence names one of the holdings a portfolio note gave the voice, spaced or not
 * ("Cedar Leaf" for CEDARLEAF). */
export function mentionsLabel(text: string, labels: readonly string[]): boolean {
  const key = labels.join('\0');
  if (matcher?.key !== key) {
    const usable = labels.filter(label => label.length >= 2);
    const pattern = usable.length ? new RegExp(`(?:^|[^A-Za-z0-9])(?:${usable.map(escapePattern).join('|')})(?![A-Za-z0-9])`, 'i') : undefined;
    const plain = usable.map(compacted).filter(value => value.length >= 5);
    matcher = { key, test: value => {
      if (pattern?.test(value)) return true;
      const text = plain.length ? compacted(value) : '';
      return plain.some(name => text.includes(name));
    } };
  }
  return matcher.test(text);
}

/** A statement of account fact; a question or progress talk ("On it.", "Checking that one.") is not. */
export function isToldFact(sentence: string, labels: readonly string[] = []): boolean {
  const text = sentence.trim();
  if (text.length < 4 || /\?["')\]]*$/.test(text)) return false;
  return AMOUNT.test(text) || ACCOUNT.test(text) || OUTCOME.test(text) || mentionsLabel(text, labels);
}

const normalized = (text: string) => text.toLowerCase().replace(/\s+/g, ' ').trim();
type SpokenFragment = Pick<LiveTranscriptFragment, 'role' | 'text' | 'startMs' | 'endMs'>;
/** Dexter's sentences in the order spoken. Caller speech between its fragments does not split one
 * ("That read didn't" / "list" / "every holding."); a pause of more than 1.2 s in Dexter's own speech
 * does, as in the guardrail. */
export function spokenSentences(fragments: readonly SpokenFragment[]): string[] {
  let text = '', endMs: number | undefined;
  for (const fragment of fragments) {
    if (fragment.role !== 'assistant') continue;
    if (endMs !== undefined && fragment.startMs - endMs > 1200) text += '\n';
    text += fragment.text.replace(/[\0\uD800-\uDFFF]/gu, '').replace(/\n/g, ' ');
    endMs = fragment.endMs;
  }
  return text.split(/(?<=[.!?…]["')\]]?)\s+|\s*\n\s*/).map(sentence => sentence.replace(/\s+/g, ' ').trim()).filter(Boolean);
}

/** The facts Dexter told the caller in these fragments, first-spoken order, each once. */
export function toldFacts(fragments: readonly SpokenFragment[], labels: readonly string[] = []): string[] {
  const seen = new Set<string>(), facts: string[] = [];
  for (const sentence of spokenSentences(fragments)) {
    const key = normalized(sentence);
    if (seen.has(key) || !isToldFact(sentence, labels)) continue;
    seen.add(key); facts.push(sentence);
  }
  return facts;
}

const clip = (text: string, limit: number) => text.length <= limit ? text : `${text.slice(0, limit - 1).trimEnd()}…`;
/** Facts within a character budget: those sharing the most words with the anchor (the request, or the
 * sentence being judged) first, then the newest. Returned in the order they were spoken. */
export function selectTold(facts: readonly string[], anchor: string, budget: number, perFact = 400): string[] {
  const said = contentWords(anchor);
  const ranked = facts.map((text, index) => ({ text: clip(text, perFact), index,
    score: [...contentWords(text)].filter(word => said.has(word)).length }))
    .sort((a, b) => b.score - a.score || b.index - a.index);
  const picked: typeof ranked = [];
  let used = 0;
  for (const fact of ranked) {
    const size = fact.text.length + (picked.length ? 1 : 0);
    if (used + size > budget) continue;
    picked.push(fact); used += size;
  }
  return picked.sort((a, b) => a.index - b.index).map(fact => fact.text);
}

/** Characters of the voice context (6000 in all) given to what the voice already told the caller. */
export const TOLD_BLOCK_BUDGET = 2000;
export const TOLD_BLOCK_PREFIX = 'Earlier on this call, the voice already told the caller: ';
/** Facts not already in the recent speech, quoted, as one compact block for Primary. */
export function toldBlock(facts: readonly string[], recentDexterSpeech: readonly string[], anchor: string): string | undefined {
  const recent = recentDexterSpeech.map(normalized);
  const unsaid = facts.filter(fact => !recent.some(text => text.includes(normalized(fact))));
  const picked = selectTold(unsaid.map(fact => `"${clip(fact, 1200)}"`), anchor, TOLD_BLOCK_BUDGET - TOLD_BLOCK_PREFIX.length, Infinity);
  return picked.length ? TOLD_BLOCK_PREFIX + picked.join(' ') : undefined;
}
