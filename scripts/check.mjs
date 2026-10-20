import { readdirSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
const root = fileURLToPath(new URL('../', import.meta.url));
function check(dir) {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if (['node_modules', '.git'].includes(entry.name)) continue;
    const path = `${dir}/${entry.name}`;
    if (entry.isDirectory()) check(path);
    else if (entry.name.endsWith('.mjs')) {
      const result = spawnSync(process.execPath, ['--check', path], { stdio: 'inherit' });
      if (result.status !== 0) process.exit(result.status ?? 1);
    }
  }
}
check(root);
