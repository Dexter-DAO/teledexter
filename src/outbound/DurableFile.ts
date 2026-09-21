import { randomBytes } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

export interface DurableFileSystem {
  mkdirSync(path: string, options: { recursive: true }): unknown;
  openSync(path: string, flags: string, mode?: number): number;
  writeFileSync(fd: number, data: string, options: { encoding: 'utf8' }): void;
  chmodSync(path: string, mode: number): void;
  fsyncSync(fd: number): void;
  closeSync(fd: number): void;
  renameSync(oldPath: string, newPath: string): void;
  unlinkSync(path: string): void;
}

export interface DurableParentBinding {
  directory: string;
  device: number;
  inode: number;
  uid: number;
}

/**
 * Capture the exact already-approved directory inode. Financial stores bind
 * this once at construction; later writes may not silently follow a recreated
 * or swapped path.
 */
export function bindDurableFileParent(filePath: string): DurableParentBinding {
  const directory = path.dirname(filePath);
  const parent = fs.lstatSync(directory);
  const expectedUid = process.getuid?.();
  if (
    !parent.isDirectory()
    || parent.isSymbolicLink()
    || fs.realpathSync(directory) !== path.resolve(directory)
    || (parent.mode & 0o022) !== 0
    || (expectedUid !== undefined && parent.uid !== expectedUid)
  ) {
    throw new Error('durable_parent_not_safe');
  }
  return {
    directory,
    device: parent.dev,
    inode: parent.ino,
    uid: parent.uid,
  };
}

function assertDurableFileParent(binding: DurableParentBinding): void {
  let parent: fs.Stats;
  try {
    parent = fs.lstatSync(binding.directory);
  } catch {
    throw new Error('durable_parent_identity_changed');
  }
  if (
    !parent.isDirectory()
    || parent.isSymbolicLink()
    || parent.dev !== binding.device
    || parent.ino !== binding.inode
    || parent.uid !== binding.uid
    || (parent.mode & 0o022) !== 0
    || fs.realpathSync(binding.directory) !== path.resolve(binding.directory)
  ) {
    throw new Error('durable_parent_identity_changed');
  }
}

export function durableFileParentIsCurrent(binding: DurableParentBinding): boolean {
  try {
    assertDurableFileParent(binding);
    return true;
  } catch {
    return false;
  }
}

/** Atomic publication whose success means both file bytes and directory entry
 * have crossed the kernel durability boundary. */
export function durablyReplaceJsonFile(
  filePath: string,
  contents: string,
  fileSystem: DurableFileSystem = fs,
  parentBinding: DurableParentBinding = bindDurableFileParent(filePath),
): void {
  const directory = path.dirname(filePath);
  if (directory !== parentBinding.directory) {
    throw new Error('durable_parent_binding_mismatch');
  }
  assertDurableFileParent(parentBinding);
  const tempPath = `${filePath}.${process.pid}.${randomBytes(6).toString('hex')}.tmp`;
  let fileDescriptor: number | null = null;
  try {
    fileDescriptor = fileSystem.openSync(tempPath, 'wx', 0o600);
    fileSystem.writeFileSync(fileDescriptor, contents, { encoding: 'utf8' });
    fileSystem.chmodSync(tempPath, 0o600);
    fileSystem.fsyncSync(fileDescriptor);
    fileSystem.closeSync(fileDescriptor);
    fileDescriptor = null;
    assertDurableFileParent(parentBinding);
    fileSystem.renameSync(tempPath, filePath);
    assertDurableFileParent(parentBinding);

    const directoryDescriptor = fileSystem.openSync(directory, 'r');
    try {
      const openedDirectory = fs.fstatSync(directoryDescriptor);
      if (
        !openedDirectory.isDirectory()
        || openedDirectory.dev !== parentBinding.device
        || openedDirectory.ino !== parentBinding.inode
      ) {
        throw new Error('durable_parent_identity_changed');
      }
      fileSystem.fsyncSync(directoryDescriptor);
      assertDurableFileParent(parentBinding);
    } finally {
      fileSystem.closeSync(directoryDescriptor);
    }
  } catch (error) {
    if (fileDescriptor !== null) {
      try { fileSystem.closeSync(fileDescriptor); } catch {}
    }
    try { fileSystem.unlinkSync(tempPath); } catch {}
    throw error;
  }
}
