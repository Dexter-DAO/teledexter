import assert from 'node:assert/strict';
import test from 'node:test';
import { createServer, type IncomingMessage } from 'node:http';
import { createConnection, type AddressInfo } from 'node:net';
import { authorizedHarkIngress } from '../src/broker/harkAuth.js';

const token = 'existing-internal-token-for-offline-tests';
const name = 'x-dexter-internal-auth';
type Headers = Pick<IncomingMessage, 'headers' | 'rawHeaders'>;
const request = (value: string): Headers => ({ headers: { [name]: value }, rawHeaders: [name, value] });

async function receiveRawHeaders(lines: string[], configuredToken: string | undefined) {
  let observed: (Headers & { authorized: boolean }) | undefined;
  const server = createServer((req, res) => {
    observed = { headers: { ...req.headers }, rawHeaders: [...req.rawHeaders],
      authorized: authorizedHarkIngress(req, configuredToken) };
    res.writeHead(200, { connection: 'close' });
    res.end('ok');
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => resolve());
  });
  try {
    const { port } = server.address() as AddressInfo;
    await new Promise<void>((resolve, reject) => {
      const socket = createConnection({ host: '127.0.0.1', port });
      socket.once('connect', () => socket.write(`GET / HTTP/1.1\r\nHost: localhost\r\n${lines.join('\r\n')}\r\nConnection: close\r\n\r\n`));
      socket.setTimeout(2000, () => socket.destroy(new Error('local_http_test_timeout')));
      socket.on('data', () => {});
      socket.once('error', reject);
      socket.once('end', resolve);
    });
    assert.ok(observed);
    return observed;
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  }
}

test('one exact mixed-case header authenticates through Node HTTP parsing', async () => {
  const result = await receiveRawHeaders([`X-DeXtEr-InTeRnAl-AuTh: ${token}`], token);
  assert.equal(result.authorized, true);
  assert.equal(result.headers[name], token);
  assert.ok(result.rawHeaders.includes('X-DeXtEr-InTeRnAl-AuTh'));
});

test('duplicate header names with different casing fail even when their values match', async () => {
  const result = await receiveRawHeaders([`X-Dexter-Internal-Auth: ${token}`, `${name}: ${token}`], token);
  assert.equal(result.authorized, false);
  assert.equal(result.rawHeaders.filter((part, index) => index % 2 === 0 && part.toLowerCase() === name).length, 2);
  assert.equal(result.headers[name], `${token}, ${token}`);
});

test('HTTP optional whitespace is stripped in both Node header views, without trimming the configured token', async () => {
  const result = await receiveRawHeaders([`${name}:\t  ${token}\t `], token);
  assert.equal(result.authorized, true);
  assert.equal(result.headers[name], token);
  assert.equal(result.rawHeaders[result.rawHeaders.indexOf(name) + 1], token);
  assert.equal(authorizedHarkIngress(result, ` ${token}`), false);
  assert.equal(authorizedHarkIngress(result, `${token}\t`), false);
});

test('absent and empty headers fail through Node HTTP parsing', async () => {
  assert.equal((await receiveRawHeaders([], token)).authorized, false);
  assert.equal((await receiveRawHeaders([`${name}:`], token)).authorized, false);
});

test('oversized real header fails without changing the configured credential', async () => {
  const value = 'x'.repeat(4097);
  assert.equal((await receiveRawHeaders([`${name}: ${value}`], value)).authorized, false);
});

test('undefined, empty, or mismatched configured credentials do not authenticate', () => {
  for (const expected of [undefined, '', token.toUpperCase(), ` ${token}`, `${token} `, 'x'.repeat(4097)]) {
    assert.equal(authorizedHarkIngress(request(token), expected), false);
  }
  assert.equal(authorizedHarkIngress(request(token), token), true);
});

test('parsed header arrays, missing raw occurrence, and disagreement between header views fail', () => {
  for (const malformed of [
    { headers: { [name]: [token] }, rawHeaders: [name, token] },
    { headers: { [name]: token }, rawHeaders: [] },
    { headers: { [name]: token }, rawHeaders: [name, `${token} `] },
    { headers: {}, rawHeaders: [name, token] },
    { headers: { [name]: token }, rawHeaders: [name, token, 'unpaired'] },
    { headers: { [name]: token }, rawHeaders: [name, token, name.toUpperCase(), token] },
  ] satisfies Headers[]) assert.equal(authorizedHarkIngress(malformed, token), false);
});

test('the maximum is bytes, while exact supplied values are never normalized', () => {
  for (const value of ['x'.repeat(4096), 'é'.repeat(2048), ` ${token} `]) {
    assert.equal(authorizedHarkIngress(request(value), value), true);
  }
  for (const value of ['x'.repeat(4097), 'é'.repeat(2049)]) {
    assert.equal(authorizedHarkIngress(request(value), value), false);
  }
  assert.equal(authorizedHarkIngress(request(token), ` ${token} `), false);
});
