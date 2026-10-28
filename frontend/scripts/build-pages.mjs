// Builds the static public demo exactly as the GitHub Pages workflow does.
// Cross-platform replacement for `VITE_BASE_PATH=/Quaero/ VITE_DEMO_ONLY=true npm run build`.
import { spawnSync } from 'node:child_process';

const env = { ...process.env, VITE_BASE_PATH: '/Quaero/', VITE_DEMO_ONLY: 'true' };

function run(args) {
  const result = spawnSync('npx', args, {
    stdio: 'inherit',
    env,
    shell: process.platform === 'win32',
  });
  if (result.status !== 0) process.exit(result.status ?? 1);
}

run(['tsc']);
run(['vite', 'build']);
