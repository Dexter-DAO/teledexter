import fs from 'node:fs';
import path from 'node:path';

/**
 * Asynchronous, coalesced, per-call writes for live observations (call transcripts). The
 * phone media path never waits for them: every disk operation runs in the libuv threadpool,
 * at most one write per call is in flight, and observations that arrive meanwhile are folded
 * into the next write. A slow or blocked disk delays only this evidence, never call audio.
 */
export interface OverlayFileSystem {
  mkdir(directory: string): Promise<void>;
  /** Write and fsync a whole file. */
  writeFile(file: string, data: string): Promise<void>;
  rename(from: string, to: string): Promise<void>;
  /** fsync the directory so a rename survives a crash. */
  syncDirectory(directory: string): Promise<void>;
  unlink(file: string): Promise<void>;
}

export const nodeOverlayFileSystem: OverlayFileSystem = {
  async mkdir(directory) { await fs.promises.mkdir(directory, { recursive: true, mode: 0o700 }); },
  async writeFile(file, data) {
    const handle = await fs.promises.open(file, 'w', 0o600);
    try { await handle.writeFile(data, 'utf8'); await handle.sync(); }
    finally { await handle.close(); }
  },
  rename: (from, to) => fs.promises.rename(from, to),
  async syncDirectory(directory) {
    const handle = await fs.promises.open(directory, 'r');
    try { await handle.sync(); } finally { await handle.close(); }
  },
  async unlink(file) {
    try { await fs.promises.unlink(file); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
  },
};

export interface OverlayBody { seq: number; text: string }

export function overlayFile(directory: string, callId: string): string { return path.join(directory, `${callId}.json`); }

export function createOverlayWriter(options: {
  directory: string;
  /** The call's current observations, serialized when its write starts (latest wins), or
   * undefined when the journal already holds them all and the overlay can go. */
  body(callId: string): OverlayBody | undefined;
  fs?: OverlayFileSystem;
  /** Minimum time between two writes of one call's overlay. */
  minIntervalMs?: number;
  now?: () => number;
}) {
  const files = options.fs ?? nodeOverlayFileSystem;
  const minInterval = options.minIntervalMs ?? 250;
  const now = options.now ?? Date.now;
  const calls = new Map<string, { writing?: Promise<void>; dirty: boolean; lastStartedAt: number; timer?: ReturnType<typeof setTimeout>;
    written?: number; epoch: number }>();
  let ready: Promise<void> | undefined;
  let stopped = false;
  const state = (callId: string) => {
    let value = calls.get(callId);
    if (!value) { value = { dirty: false, lastStartedAt: -Infinity, epoch: 0 }; calls.set(callId, value); }
    return value;
  };
  const report = (callId: string, error: unknown) => {
    try { console.error(JSON.stringify({ event: 'broker_observation_write_failed', callId, message: (error as Error)?.message ?? String(error),
      code: (error as NodeJS.ErrnoException)?.code ?? null })); } catch { /* Telemetry only. */ }
  };
  const write = (callId: string) => {
    const call = state(callId);
    call.timer = undefined;
    if (stopped || call.writing) return;
    call.dirty = false;
    call.lastStartedAt = now();
    const body = options.body(callId);
    const file = overlayFile(options.directory, callId);
    const epoch = call.epoch;
    call.writing = (async () => {
      ready ??= files.mkdir(options.directory);
      await ready;
      if (!body) {
        if (call.written === undefined) return;
        await files.unlink(file);
        call.written = undefined;
        return;
      }
      if (call.written !== undefined && call.written >= body.seq) return;
      const temporary = path.join(options.directory, `.${callId}.tmp`);
      await files.writeFile(temporary, body.text);
      // A synchronous shutdown flush wrote newer content meanwhile; never rename over it.
      if (call.epoch !== epoch) { await files.unlink(temporary); return; }
      await files.rename(temporary, file);
      await files.syncDirectory(options.directory);
      call.written = body.seq;
    })().catch(error => { ready = undefined; report(callId, error); }).finally(() => {
      call.writing = undefined;
      if (call.dirty && !stopped) schedule(callId);
    });
  };
  const schedule = (callId: string) => {
    if (stopped) return;
    const call = state(callId);
    call.dirty = true;
    if (call.writing || call.timer) return;
    const wait = Math.max(0, call.lastStartedAt + minInterval - now());
    call.timer = setTimeout(() => write(callId), wait);
    call.timer.unref?.();
  };
  return {
    /** Something about this call's observations changed; persist it soon, off the media path. */
    schedule,
    /** Synchronous last write at shutdown, so a clean stop keeps every observation. */
    flushSync(): void {
      stopped = true;
      for (const [callId, call] of calls) {
        clearTimeout(call.timer); call.timer = undefined;
        if (!call.dirty && !call.writing) continue;
        call.epoch++;
        const body = options.body(callId);
        const file = overlayFile(options.directory, callId);
        try {
          if (!body) { fs.rmSync(file, { force: true }); call.written = undefined; continue; }
          fs.mkdirSync(options.directory, { recursive: true, mode: 0o700 });
          const temporary = path.join(options.directory, `.${callId}.sync.tmp`);
          const fd = fs.openSync(temporary, 'w', 0o600);
          try { fs.writeFileSync(fd, body.text, 'utf8'); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
          fs.renameSync(temporary, file);
          const directory = fs.openSync(options.directory, 'r');
          try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
          call.written = body.seq; call.dirty = false;
        } catch (error) { report(callId, error); }
      }
    },
    /** Writes in flight, for tests and shutdown evidence. */
    inFlight: () => [...calls.values()].filter(call => call.writing).length,
    /** Settles once every write started so far has finished (tests). */
    idle: async () => { while ([...calls.values()].some(call => call.writing || call.timer)) await Promise.all([...calls.values()].map(call => call.writing ?? new Promise(resolve => setTimeout(resolve, 5)))); },
    stop() { stopped = true; for (const call of calls.values()) { clearTimeout(call.timer); call.timer = undefined; } },
  };
}
export type OverlayWriter = ReturnType<typeof createOverlayWriter>;
