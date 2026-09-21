import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

export type DurablePathValidation =
  | { ok: true; path: string }
  | {
      ok: false;
      reason:
        | 'missing'
        | 'not_absolute'
        | 'inside_release'
        | 'forbidden_root'
        | 'parent_missing'
        | 'parent_not_directory'
        | 'parent_not_canonical'
        | 'parent_identity_changed'
        | 'parent_wrong_owner'
        | 'parent_insecure_mode'
        | 'file_not_regular'
        | 'file_symlink'
        | 'file_hardlinked'
        | 'file_wrong_owner'
        | 'file_insecure_mode'
        | 'file_identity_changed'
        | 'file_open_failed';
    };

export interface DurablePathValidationOptions {
  expectedUid?: number;
  forbiddenRoots?: string[];
}

function isInside(root: string, target: string): boolean {
  const relative = path.relative(path.resolve(root), path.resolve(target));
  return relative === ''
    || (!relative.startsWith(`..${path.sep}`)
      && relative !== '..'
      && !path.isAbsolute(relative));
}

function reject(
  reason: Extract<DurablePathValidation, { ok: false }>['reason'],
): DurablePathValidation {
  return { ok: false, reason };
}

function parentRemainsSafe(
  parentPath: string,
  before: fs.Stats,
  expectedUid: number | undefined,
): boolean {
  try {
    const after = fs.lstatSync(parentPath);
    return after.isDirectory()
      && !after.isSymbolicLink()
      && after.dev === before.dev
      && after.ino === before.ino
      && (expectedUid === undefined || after.uid === expectedUid)
      && (after.mode & 0o022) === 0
      && fs.realpathSync(parentPath) === path.resolve(parentPath);
  } catch {
    return false;
  }
}

/**
 * Financial ledgers must survive release replacement and must not be redirected
 * through a shared/symlinked path. The parent is required to exist so startup
 * cannot create a principal ledger in an arbitrary location by typo.
 */
export function validateReleaseIndependentStatePath(
  configuredPath: string | undefined,
  releaseRoot: string,
  options: DurablePathValidationOptions = {},
): DurablePathValidation {
  if (!configuredPath?.trim()) return reject('missing');
  if (!path.isAbsolute(configuredPath)) return reject('not_absolute');

  const resolvedPath = path.resolve(configuredPath);
  if (isInside(releaseRoot, resolvedPath)) return reject('inside_release');

  const forbiddenRoots = options.forbiddenRoots ?? [
    os.tmpdir(),
    '/var/tmp',
    '/dev/shm',
  ];
  if (forbiddenRoots.some((root) => isInside(root, resolvedPath))) {
    return reject('forbidden_root');
  }

  const expectedUid = options.expectedUid ?? process.getuid?.();
  const parentPath = path.dirname(resolvedPath);
  let parent;
  try {
    parent = fs.lstatSync(parentPath);
  } catch {
    return reject('parent_missing');
  }
  if (!parent.isDirectory() || parent.isSymbolicLink()) {
    return reject('parent_not_directory');
  }
  let canonicalParent: string;
  try {
    canonicalParent = fs.realpathSync(parentPath);
  } catch {
    return reject('parent_missing');
  }
  if (canonicalParent !== path.resolve(parentPath)) {
    return reject('parent_not_canonical');
  }
  if (expectedUid !== undefined && parent.uid !== expectedUid) {
    return reject('parent_wrong_owner');
  }
  if ((parent.mode & 0o022) !== 0) return reject('parent_insecure_mode');

  let before;
  try {
    before = fs.lstatSync(resolvedPath);
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code !== 'ENOENT') return reject('file_open_failed');
    return parentRemainsSafe(parentPath, parent, expectedUid)
      ? { ok: true, path: resolvedPath }
      : reject('parent_identity_changed');
  }
  if (before.isSymbolicLink()) return reject('file_symlink');
  if (!before.isFile()) return reject('file_not_regular');
  if (before.nlink !== 1) return reject('file_hardlinked');
  if (expectedUid !== undefined && before.uid !== expectedUid) {
    return reject('file_wrong_owner');
  }
  if ((before.mode & 0o777) !== 0o600) return reject('file_insecure_mode');

  let descriptor: number | null = null;
  try {
    descriptor = fs.openSync(
      resolvedPath,
      fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0),
    );
    const opened = fs.fstatSync(descriptor);
    const after = fs.lstatSync(resolvedPath);
    if (
      !opened.isFile()
      || opened.nlink !== 1
      || (expectedUid !== undefined && opened.uid !== expectedUid)
      || (opened.mode & 0o777) !== 0o600
      || !after.isFile()
      || after.isSymbolicLink()
      || after.nlink !== 1
      || (expectedUid !== undefined && after.uid !== expectedUid)
      || (after.mode & 0o777) !== 0o600
      || opened.dev !== before.dev
      || opened.ino !== before.ino
      || after.dev !== before.dev
      || after.ino !== before.ino
    ) {
      return reject('file_identity_changed');
    }
    if (!parentRemainsSafe(parentPath, parent, expectedUid)) {
      return reject('parent_identity_changed');
    }
  } catch {
    return reject('file_open_failed');
  } finally {
    if (descriptor !== null) {
      try { fs.closeSync(descriptor); } catch {}
    }
  }

  return { ok: true, path: resolvedPath };
}

export function phoneFinancialReadiness(input: {
  callbackSecretReady: boolean;
  outboundStatePath: DurablePathValidation;
  callbackOutboxPath: DurablePathValidation;
  outboundStoreOperational: boolean;
  callbackOutboxOperational: boolean;
}): { ok: true; reasons: [] } | { ok: false; reasons: string[] } {
  const reasons: string[] = [];
  if (!input.callbackSecretReady) reasons.push('callback_secret_unavailable');
  if (!input.outboundStatePath.ok) {
    reasons.push(`outbound_state_path_${input.outboundStatePath.reason}`);
  }
  if (!input.callbackOutboxPath.ok) {
    reasons.push(`callback_outbox_path_${input.callbackOutboxPath.reason}`);
  }
  if (!input.outboundStoreOperational) reasons.push('outbound_store_unavailable');
  if (!input.callbackOutboxOperational) reasons.push('callback_outbox_unavailable');
  return reasons.length === 0
    ? { ok: true, reasons: [] }
    : { ok: false, reasons };
}
