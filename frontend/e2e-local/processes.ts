import { spawnSync, type ChildProcess } from 'node:child_process';

/**
 * Ends a server that a test started, together with everything it started.
 *
 * On Windows the dev server is launched through a shell (`npx.cmd`), so `proc.kill()` only ends the shell and
 * leaves the `node` process running: it keeps its port and keeps the folder locked after the tests. The whole
 * process tree is ended instead.
 */
export function stopProcessTree(proc: ChildProcess | null): void {
  if (!proc || proc.pid === undefined) return;
  if (process.platform === 'win32') {
    spawnSync('taskkill', ['/pid', String(proc.pid), '/T', '/F'], { stdio: 'ignore' });
  } else {
    proc.kill();
  }
}
