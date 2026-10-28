// Fails when the production bundle could leak a secret or talk to an unexpected service.
// Run after `npm run build`:  QUAERO_API_KEY=<sentinel> npm run check:bundle
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';

const DIST = new URL('../dist', import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1');

// Hosts that may legitimately appear as text or links in the bundle.
const ALLOWED_HOSTS = new Set([
  'www.w3.org', // SVG/XML namespaces
  'github.com', // links to the repository
  'react.dev', // React's own error-message documentation links
  'tailwindcss.com', // license banner
  'example.com', // placeholder shown in forms
  'raw.githubusercontent.com', // dataset URLs shown as text in the demo project config
  'api.worldbank.org', // dataset URLs shown as text in the demo project config
  'invalid-host-unreachable.test', // demo fixture for a failed run
  '127.0.0.1', // setup instructions
  'localhost',
]);

// Anything that would indicate a secret in the bundle or a call to an external AI/font service.
const FORBIDDEN_PATTERNS = [
  /x-api-key/i,
  /process\.env/,
  /fonts\.googleapis\.com/i,
  /fonts\.gstatic\.com/i,
  /generativelanguage\.googleapis\.com/i,
  /api\.openai\.com/i,
  /api\.anthropic\.com/i,
  /GEMINI_API_KEY/,
];

function* walk(dir) {
  for (const name of readdirSync(dir)) {
    const full = join(dir, name);
    if (statSync(full).isDirectory()) yield* walk(full);
    else yield full;
  }
}

const problems = [];
const key = process.env.QUAERO_API_KEY;
let files = 0;

for (const file of walk(DIST)) {
  files += 1;
  const text = readFileSync(file, 'utf8');
  if (key && key.length >= 8 && text.includes(key)) {
    problems.push(`${file}: contains the value of QUAERO_API_KEY`);
  }
  for (const pattern of FORBIDDEN_PATTERNS) {
    if (pattern.test(text)) problems.push(`${file}: matches forbidden pattern ${pattern}`);
  }
  for (const match of text.matchAll(/https?:\/\/([A-Za-z0-9.-]+)/g)) {
    if (!ALLOWED_HOSTS.has(match[1])) problems.push(`${file}: unexpected host ${match[1]}`);
  }
}

if (files === 0) problems.push('dist/ is empty: run `npm run build` first');

if (problems.length > 0) {
  console.error(`Bundle check failed:\n- ${[...new Set(problems)].join('\n- ')}`);
  process.exit(1);
}
console.log(`Bundle check passed (${files} files scanned).`);
