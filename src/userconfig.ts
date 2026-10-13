// The app's own files, in ~/.inferit-seller (INFERIT_SELLER_HOME overrides; the directory is 0700):
//
//   config.json   written by `connect` (0600): the node token, the market's URLs, the machine's name and options
//   state.json    written by a running `start` (0600): connected or not, the last failure, the models it serves;
//                 `status` and `doctor` read it, and a second `start` refuses to run next to a live one
//   catalog.json  the last catalog read from the market, used when the market cannot be reached at boot
//   logs/         seller.log (launchd and the Windows task; systemd keeps its own journal)
import { randomBytes } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { NODE_TOKEN_RE } from './connector.js';

/** The market the app talks to unless told otherwise: the hosted testnet demo. */
export const DEFAULT_API_URL = 'https://api-production-c74b9.up.railway.app';
export const CONNECTOR_PATH = '/v1/seller/connect';

export interface SavedConfig {
  token: string;
  /** wss://<api>/v1/seller/connect */
  connectUrl: string;
  /** https://<api> */
  apiUrl: string;
  nodeName: string;
  /** An inference server to use instead of detecting one (--upstream). */
  upstream?: string;
  /** Keep the computer awake while the app runs (install-service --keep-awake). */
  keepAwake?: boolean;
  savedAt?: string;
}

export interface RunState {
  pid: number;
  /** When the machine booted (ms), so a pid from before a reboot is never taken for a live process. */
  bootAt: number;
  startedAt: string;
  updatedAt: string;
  version: string;
  tokenPrefix: string | null;
  service: string | null;
  phase: 'detecting' | 'connecting' | 'live' | 'waiting' | 'stopped';
  nodeId: string | null;
  sessionId: string | null;
  liveSince: string | null;
  offers: number | null;
  failedAttempts: number;
  lastFailure: string | null;
  lastFailureAt: string | null;
  /** When the next connection attempt is due, and why it waits ('auth': the market refused the token, 5 minutes). */
  nextAttemptAt?: string | null;
  retryReason?: string | null;
  /** What it serves; contextLength is the context the local server last said it serves the model with, coldLoadMs
   *  how long Ollama last took to load the model (measured by keep-warm, kept across restarts) and coldLoadAt when. */
  models: { id: string; local: string; server: string; contextLength?: number; coldLoadMs?: number; coldLoadAt?: string }[];
  /** Keep-warm for the Ollama models it serves (keepwarm.ts); null when it serves none. `models`: the local names it
   *  keeps loaded now (those with an offer; absent when keep-warm is off). */
  keepWarm?: { on: boolean; keepAlive: string; everyMs: number; models?: string[] } | null;
  /** The market's offers for this machine as of the last welcome or `offers` frame: the model and its context limit
   *  (null: the catalog's window; absent: the market did not say). Null before the first welcome. */
  offerList?: { model: string; maxContextTokens?: number | null }[] | null;
  message: string | null;
}

const LOOPBACK = new Set(['127.0.0.1', '::1', 'localhost', '[::1]']);

export function appHome(env: NodeJS.ProcessEnv = process.env, homedir: string = os.homedir()): string {
  const o = env.INFERIT_SELLER_HOME?.trim();
  return o ? path.resolve(o) : path.join(homedir, '.inferit-seller');
}

export const configPath = (home: string) => path.join(home, 'config.json');
export const statePath = (home: string) => path.join(home, 'state.json');
export const catalogCachePath = (home: string) => path.join(home, 'catalog.json');
export const logDir = (home: string) => path.join(home, 'logs');

/** https://host[:port] with no path; http only for a market on this machine. A trailing /v1 is dropped. */
export function normaliseApiUrl(raw: string): string {
  let u: URL;
  try {
    u = new URL(raw.trim());
  } catch {
    throw new Error(`--api must be a URL such as ${DEFAULT_API_URL}`);
  }
  if (u.protocol !== 'https:' && !(u.protocol === 'http:' && LOOPBACK.has(u.hostname))) throw new Error('--api must be an https:// URL (http:// only for a market on this machine)');
  if (u.username || u.password || u.search || u.hash) throw new Error('--api must not carry credentials, a query or a fragment');
  const p = u.pathname.replace(/\/+$/, '').replace(/\/v1$/, '');
  return `${u.origin}${p}`;
}

/** wss://<api>/v1/seller/connect for an API URL. */
export function connectUrlFromApi(apiUrl: string): string {
  return `${normaliseApiUrl(apiUrl).replace(/^http/, 'ws')}${CONNECTOR_PATH}`;
}

/** icn_AbCd…wxyz: enough to recognise a token, never enough to use it. */
export function tokenPrefix(token: string): string {
  return token.length > 12 ? `${token.slice(0, 8)}…${token.slice(-4)}` : '…';
}

/** A node name from the hostname: the market accepts letters, digits, spaces and . : @ ( ) - _, at most 64. */
export function defaultNodeName(hostname: string = os.hostname()): string {
  const s = hostname
    .replace(/\.local$/i, '')
    .replace(/[^\w .:@()-]/g, '-')
    .replace(/-+/g, '-')
    .slice(0, 64)
    .replace(/^[-\s]+|[-\s]+$/g, '');
  return s || 'my-machine';
}

export function validNodeName(name: string): boolean {
  return name.length >= 1 && name.length <= 64 && /^[\w .:@()-]+$/.test(name);
}

function ensureDir(dir: string): void {
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  if (process.platform !== 'win32') fs.chmodSync(dir, 0o700);
}

/** Writes a file atomically with mode 0600 (a temporary file in the same directory, then a rename). */
export function writePrivate(file: string, content: string): void {
  ensureDir(path.dirname(file));
  const tmp = `${file}.${process.pid}.${randomBytes(4).toString('hex')}.tmp`;
  fs.writeFileSync(tmp, content, { mode: 0o600 });
  if (process.platform !== 'win32') fs.chmodSync(tmp, 0o600);
  fs.renameSync(tmp, file);
}

export function loadConfig(home: string): SavedConfig | null {
  const file = configPath(home);
  let raw: string;
  try {
    raw = fs.readFileSync(file, 'utf8');
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code === 'ENOENT') return null;
    throw new Error(`cannot read ${file}: ${(e as Error).message}`);
  }
  let c: Partial<SavedConfig>;
  try {
    c = JSON.parse(raw) as Partial<SavedConfig>;
  } catch {
    throw new Error(`${file} is not valid JSON; run connect again to rewrite it`);
  }
  if (typeof c.token !== 'string' || !NODE_TOKEN_RE.test(c.token)) throw new Error(`${file} has no valid node token; run connect again`);
  const apiUrl = normaliseApiUrl(typeof c.apiUrl === 'string' ? c.apiUrl : DEFAULT_API_URL);
  const out: SavedConfig = {
    token: c.token,
    apiUrl,
    connectUrl: typeof c.connectUrl === 'string' && c.connectUrl ? c.connectUrl : connectUrlFromApi(apiUrl),
    nodeName: typeof c.nodeName === 'string' && validNodeName(c.nodeName) ? c.nodeName : defaultNodeName(),
  };
  if (typeof c.upstream === 'string' && c.upstream) out.upstream = c.upstream;
  if (c.keepAwake === true) out.keepAwake = true;
  if (typeof c.savedAt === 'string') out.savedAt = c.savedAt;
  return out;
}

export function saveConfig(home: string, c: SavedConfig): string {
  const file = configPath(home);
  writePrivate(file, `${JSON.stringify({ ...c, savedAt: new Date().toISOString() }, null, 2)}\n`);
  return file;
}

/** Whether `file` is readable by others (POSIX). */
export function isWorldReadable(file: string): boolean {
  if (process.platform === 'win32') return false;
  try {
    return (fs.statSync(file).mode & 0o077) !== 0;
  } catch {
    return false;
  }
}

export function bootAt(): number {
  return Date.now() - os.uptime() * 1000;
}

export function readState(home: string): RunState | null {
  try {
    const s = JSON.parse(fs.readFileSync(statePath(home), 'utf8')) as RunState;
    return typeof s?.pid === 'number' ? s : null;
  } catch {
    return null;
  }
}

export function writeState(home: string, s: RunState): void {
  try {
    writePrivate(statePath(home), `${JSON.stringify(s, null, 2)}\n`);
  } catch {
    /* status only: never fatal */
  }
}

/** A state file whose process still runs: same boot, pid alive, updated in the last 150 s. */
export function liveState(s: RunState | null, now = Date.now()): boolean {
  if (!s || s.phase === 'stopped') return false;
  if (Math.abs(s.bootAt - bootAt()) > 120_000) return false;
  if (now - Date.parse(s.updatedAt) > 150_000) return false;
  try {
    process.kill(s.pid, 0);
    return true;
  } catch (e) {
    return (e as NodeJS.ErrnoException).code === 'EPERM';
  }
}
