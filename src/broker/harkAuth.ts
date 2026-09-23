import { timingSafeEqual } from 'node:crypto';
import type { IncomingMessage } from 'node:http';

const HEADER_NAME = 'x-dexter-internal-auth';
const MAX_TOKEN_BYTES = 4096;

/** Authenticate one producer header; the caller retains the loopback check. */
export function authorizedHarkIngress(
  request: Pick<IncomingMessage, 'headers' | 'rawHeaders'>,
  token: string | undefined,
): boolean {
  if (typeof token !== 'string' || !token || Buffer.byteLength(token, 'utf8') > MAX_TOKEN_BYTES) return false;
  const provided = request.headers[HEADER_NAME];
  if (typeof provided !== 'string' || !provided || Buffer.byteLength(provided, 'utf8') > MAX_TOKEN_BYTES) return false;
  if (request.rawHeaders.length % 2 !== 0) return false;
  let count = 0;
  let rawValue: string | undefined;
  for (let index = 0; index < request.rawHeaders.length; index += 2) {
    if (request.rawHeaders[index]!.toLowerCase() !== HEADER_NAME) continue;
    count++;
    rawValue = request.rawHeaders[index + 1];
  }
  if (count !== 1 || rawValue !== provided) return false;
  // Node removes HTTP optional whitespace before populating both header views.
  // Compare the resulting value exactly; never trim the configured credential.
  const actual = Buffer.from(provided, 'utf8');
  const expected = Buffer.from(token, 'utf8');
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}
