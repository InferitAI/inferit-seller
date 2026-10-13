#!/usr/bin/env node
// The inferit-seller command (package.json "bin"). See commands.ts.
import { main } from './commands.js';

main(process.argv.slice(2)).then(
  (code) => process.exit(code),
  (err: unknown) => {
    console.error(`inferit-seller: ${err instanceof Error ? err.message : String(err)}`);
    process.exit(1);
  },
);
