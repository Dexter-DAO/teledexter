import fs from 'node:fs';
import os from 'node:os';

/**
 * Durable stores refuse a parent directory reached through a symlink. On macOS
 * os.tmpdir() sits behind /var -> /private/var, so point the test run at the
 * resolved directory. Test files inherit it; on Linux this changes nothing.
 */
process.env.TMPDIR = fs.realpathSync(os.tmpdir());
