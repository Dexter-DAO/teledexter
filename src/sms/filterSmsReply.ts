/** Filter conversation answers once, immediately before the carrier send reservation. */
export function filterSmsReply(text: string): string {
  const normalized = text.normalize('NFKC').replace(/\p{Cf}/gu, '')
    .replace(/[\u3002\uff61\u2024\ufe52\u2027\u2219\u22c5\u00b7\u2022]/gu, '.').slice(0, 1000).replace(/[\uD800-\uDBFF]$/, '');
  const contacts = /(?:[a-z][a-z0-9+.-]*:(?:\/\/)?(?=[^$\p{Nd}\s<>"'])|\/\/|www\.|(?:[\p{L}\p{N}_-]+\.)+[\p{L}][\p{L}\p{N}-]*|(?:[\p{L}\p{N}_-]+\s+\.\s+)+[\p{L}][\p{L}\p{N}-]*|(?:\d{1,3}\.){3}\d{1,3}|(?<![\p{N}$])127(?:\.\d{1,8}){1,2}|0x[0-9a-f]{7,8}|\[[0-9a-f]*:[0-9a-f:]+\]|(?<![\p{L}\p{N}])(?:[0-9a-f]{0,4}:){2,}[0-9a-f]{0,4})[^\s<>"']*/giu;
  const withoutLinks = normalized.replace(contacts, candidate => {
    const link = candidate.replace(/[.,!?;:)]+$/, '');
    try {
      const url = new URL(link);
      if (/^https:\/\/dexter\.cash(?:[/?#]|$)/i.test(link) && !link.includes('\\')
        && url.origin === 'https://dexter.cash' && !url.username && !url.password
        && !decodeURIComponent(link.slice('https://dexter.cash'.length)).match(contacts)) return candidate;
    } catch { /* Bare domains, IPs and non-URL schemes are removed too. */ }
    return ' ';
  });
  return withoutLinks.replace(/(?<!\p{N})\+?(?:\(\s*)?\p{Nd}(?:[\s()/\\.\-\u2010-\u2014]*\p{Nd}){6,}(?:\s*\))?(?!\p{N})/gu, (phone, offset: number) =>
    withoutLinks[offset - 1] === '$' && /^\d+\.\d{1,2}$/.test(phone) ? phone : ' ')
    .replace(/\s+/g, ' ').trim() || 'Open https://dexter.cash and sign in with your passkey to continue.';
}
