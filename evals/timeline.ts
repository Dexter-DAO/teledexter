/** Shared readers for bench evidence timelines. */
export type Event = { at: number; type: string; [key: string]: any };
export interface Sentence { i: number; at: number; end: number; text: string }

/** Assistant speech as sentences, timed by transcript arrival on the run clock. */
export function sentences(timeline: Event[]): Sentence[] {
  const out: Sentence[] = [];
  let text = '', at = 0, end = 0;
  const flush = () => { if (text.trim()) out.push({ i: out.length + 1, at, end, text: text.trim() }); text = ''; };
  for (const event of timeline) {
    if (event.type === 'live.transcript' && event.role === 'user') { flush(); continue; }
    if (event.type !== 'live.transcript') continue;
    if (text && event.at - end > 1500) flush();
    if (!text) at = event.at;
    text += event.delta; end = event.at;
    if (/[.!?]["')\]]?\s*$/.test(event.delta)) flush();
  }
  flush();
  return out;
}
