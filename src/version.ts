// The app's name and version, read from package.json (one level above both src/ and dist/), so the version in the
// hello, `--version` and the package can never disagree.
import { readFileSync } from 'node:fs';

function readPackage(): { name: string; version: string } {
  try {
    const pkg = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8')) as { name?: unknown; version?: unknown };
    return {
      name: typeof pkg.name === 'string' ? pkg.name : 'inferit-seller',
      version: typeof pkg.version === 'string' ? pkg.version : '0.0.0',
    };
  } catch {
    return { name: 'inferit-seller', version: '0.0.0' };
  }
}

const pkg = readPackage();

/** The agent name in the hello (`agent.name`). */
export const AGENT_NAME = 'inferit-seller';
export const PACKAGE_NAME = pkg.name;
export const PACKAGE_VERSION = pkg.version;
