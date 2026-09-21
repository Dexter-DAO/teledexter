import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test, { type TestContext } from 'node:test';
import {
  phoneFinancialReadiness,
  validateReleaseIndependentStatePath,
} from '../src/outbound/PhoneFinancialReadiness.js';

const RELEASE_ROOT = '/srv/releases/dexter-phone-abc123';
const LEDGER_NAMES = ['outbound-state.json', 'callback-outbox.json'];

function temporaryPrivateDirectory(t: TestContext): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'phone-ledger-path-'));
  fs.chmodSync(directory, 0o700);
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  return directory;
}

function validateTestPath(filePath: string, expectedUid = process.getuid?.()) {
  return validateReleaseIndependentStatePath(filePath, RELEASE_ROOT, {
    forbiddenRoots: [],
    expectedUid,
  });
}

test('financial ledger configuration requires explicit safe paths outside release and temp', (t) => {
  const directory = temporaryPrivateDirectory(t);
  const filePath = path.join(directory, 'state.json');
  assert.deepEqual(
    validateReleaseIndependentStatePath(undefined, RELEASE_ROOT),
    { ok: false, reason: 'missing' },
  );
  assert.deepEqual(
    validateReleaseIndependentStatePath('storage/outbox.json', RELEASE_ROOT),
    { ok: false, reason: 'not_absolute' },
  );
  assert.deepEqual(
    validateReleaseIndependentStatePath(
      '/srv/releases/dexter-phone-abc123/storage/outbox.json',
      RELEASE_ROOT,
    ),
    { ok: false, reason: 'inside_release' },
  );
  assert.deepEqual(
    validateReleaseIndependentStatePath(filePath, RELEASE_ROOT),
    { ok: false, reason: 'forbidden_root' },
  );
  assert.deepEqual(validateTestPath(filePath), { ok: true, path: filePath });
});

for (const ledgerName of LEDGER_NAMES) {
  test(`${ledgerName} rejects symlink and hardlink destinations`, (t) => {
    const directory = temporaryPrivateDirectory(t);
    const target = path.join(directory, 'target.json');
    fs.writeFileSync(target, '{}', { mode: 0o600 });

    const symlink = path.join(directory, ledgerName);
    fs.symlinkSync(target, symlink);
    assert.deepEqual(validateTestPath(symlink), { ok: false, reason: 'file_symlink' });
    fs.unlinkSync(symlink);

    const hardlink = path.join(directory, ledgerName);
    fs.linkSync(target, hardlink);
    assert.deepEqual(validateTestPath(hardlink), { ok: false, reason: 'file_hardlinked' });
  });

  test(`${ledgerName} rejects wrong ownership, insecure file mode, and writable parent`, (t) => {
    const directory = temporaryPrivateDirectory(t);
    const filePath = path.join(directory, ledgerName);
    fs.writeFileSync(filePath, '{}', { mode: 0o600 });

    const uid = process.getuid?.();
    if (uid !== undefined) {
      assert.deepEqual(validateTestPath(filePath, uid + 1), {
        ok: false,
        reason: 'parent_wrong_owner',
      });
    }

    fs.chmodSync(filePath, 0o640);
    assert.deepEqual(validateTestPath(filePath), {
      ok: false,
      reason: 'file_insecure_mode',
    });

    fs.chmodSync(filePath, 0o600);
    fs.chmodSync(directory, 0o777);
    assert.deepEqual(validateTestPath(filePath), {
      ok: false,
      reason: 'parent_insecure_mode',
    });
  });
}

test('financial readiness permits durable pending callbacks but fails closed on storage/config', () => {
  const validPath = { ok: true as const, path: '/var/lib/dexter-phone/state.json' };
  assert.deepEqual(phoneFinancialReadiness({
    callbackSecretReady: true,
    outboundStatePath: validPath,
    callbackOutboxPath: validPath,
    outboundStoreOperational: true,
    callbackOutboxOperational: true,
  }), { ok: true, reasons: [] });

  const failed = phoneFinancialReadiness({
    callbackSecretReady: false,
    outboundStatePath: { ok: false, reason: 'inside_release' },
    callbackOutboxPath: { ok: false, reason: 'missing' },
    outboundStoreOperational: false,
    callbackOutboxOperational: false,
  });
  assert.equal(failed.ok, false);
  if (failed.ok) return;
  assert.deepEqual(failed.reasons, [
    'callback_secret_unavailable',
    'outbound_state_path_inside_release',
    'callback_outbox_path_missing',
    'outbound_store_unavailable',
    'callback_outbox_unavailable',
  ]);
});
