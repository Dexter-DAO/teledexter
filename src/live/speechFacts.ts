// Deliberately conservative: allow spoken paraphrases, but no new content words,
// quantities, asset identifiers or action polarity. Unrecognised wording is held.
const normalizeText = (text: string) => text.toLowerCase().replace(/[’]/g, "'")
  .replace(/\b(can't|cannot)\b/g, 'can not').replace(/\b(won't)\b/g, 'will not')
  .replace(/\b(did|does|do|has|have|had|is|was|were|would|could|should|are|will)n?['’]t\b/g, '$1 not')
  .replace(/\b(?:did not go through|was not successful|was unsuccessful)\b/g, 'failed')
  .replace(/\b(?:did not change|(?:has|have|had) not changed)\b/g, 'unchanged')
  .replace(/(?<=\d),(?=\d{3}\b)/g, '');
export const normalizeSpeech = (text: string) => normalizeText(text)
  .replace(/[^\p{L}\p{N}$%.'+\/-]/gu, ' ').replace(/(?<!\d)\.|\.(?!\d)/g, ' ')
  .replace(/\s+/g, ' ').trim();
export const speechSentences = (text: string) => text.trim().split(/(?<=[.!?])\s+(?=[\p{L}"'])/u).filter(Boolean);
const filler = new Set(('a an the your you i my me our we it its that this of for to at on in with by and so either since because ' +
  'is are was were be been being has have had do did does will would can could should unfortunately sadly actually please okay well about').split(' '));
const aliases: Record<string, string> = { nothing: 'not', no: 'not', none: 'not', stocks: 'stock', sold: 'sell', sale: 'sell', sells: 'sell', selling: 'sell', bought: 'buy', purchase: 'buy', buying: 'buy',
  sent: 'send', sending: 'send', placed: 'place', placing: 'place', failed: 'fail', failure: 'fail', completed: 'done', complete: 'done',
  started: 'start', starting: 'start', attempted: 'attempt', attempting: 'attempt', found: 'find', unchanged: 'unchanged' };
const tokens = (text: string) => normalizeSpeech(text).split(' ').filter(word => word && !filler.has(word)).map(word => aliases[word] ?? word);
// Compare action assertions within clauses: a failed sale must not license a
// completed sale, nor a failed buy license a failed send. Stage words are facts too.
function assertions(text: string): string[] {
  return normalizeText(text).split(/(?<!\d)[.!?;,]|[.!?;,](?!\d)|\b(?:and|but|so|since)\b/).flatMap(clause => {
    const normalized = normalizeSpeech(clause);
    const negative = /\b(?:not|nothing|never|no|unchanged)\b/.test(normalized);
    const doubleNegative = (normalized.match(/\b(?:not|nothing|never|no)\b/g) ?? []).length > 1;
    const pending = /\b(?:being|will|would|trying|attempting|pending|waiting)\b/.test(normalized);
    const verbs = normalized.match(/\b(?:sold|bought|sent|placed|done|completed|failed|fail|pending|submitted|confirmed|signed|executed|started|prepared|found|matched|unchanged|go through|went through|sell|buy|send|place)\b/g) ?? [];
    return verbs.map(verb => {
      const action = aliases[verb] ?? verb;
      const subject = [...new Set(tokens(clause).filter(token => token !== action && !['not', 'never'].includes(token)))].sort().join(' ');
      return `${subject}:${action}:${doubleNegative ? 'double_negative' : negative ? 'negative' : pending ? 'pending' : 'positive'}`;
    });
  });
}
// Preserve quantity/asset/name order, not the order of grammar or action words.
// This binds 3 SOL / 40 USDC and the direction of BONK-for-WIF exchanges.
const actionWords = new Set('not never sell buy send place fail done start attempt find unchanged pending swapped swap go through went submitted confirmed signed executed prepared matched'.split(' '));
const orderedFacts = (text: string) => tokens(text).filter(token => !actionWords.has(token));
export function faithfulRelay(spoken: string, delivered: string): boolean {
  const allowed = new Set(tokens(delivered));
  // Tickers that are also grammar words (CAN, IT, etc.) must not disappear in
  // the filler filter. Case changes remain harmless when the word is supplied.
  const words = new Set(normalizeSpeech(delivered).split(' '));
  if ((spoken.match(/\b[A-Z][A-Z0-9_-]+\b/g) ?? []).some(word => !words.has(word.toLowerCase()))) return false;
  if (!tokens(spoken).length || tokens(spoken).some(token => !allowed.has(token))) return false;
  const deliveredOrder = orderedFacts(delivered);
  let cursor = 0;
  for (const fact of orderedFacts(spoken)) {
    const index = deliveredOrder.indexOf(fact, cursor);
    if (index < 0) return false;
    cursor = index + 1;
  }
  const facts = assertions(delivered);
  return assertions(spoken).every(fact => facts.includes(fact));
}
