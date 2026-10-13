// The inferit-seller command line: connect, start, install-service, uninstall-service, status, doctor.
// cli.ts is the bin entry; everything here takes its I/O as a parameter so it can be tested.
import { spawn } from 'node:child_process';
import os from 'node:os';
import path from 'node:path';
import { parseArgs } from 'node:util';
import { NODE_TOKEN_RE } from './connector.js';
import { upstreamBase } from './detect.js';
import { RotatingFile, logSink, makeLogger, type LogFormat } from './log.js';
import { knownProviderOf } from './providers.js';
import { checkToken, doctor, formatDoctor, formatStatus, snapshot, statusJson, tokenFix, type ReportInput } from './report.js';
import {
  installService,
  isEphemeralInstall,
  packageRoot,
  realRunner,
  serviceContext,
  serviceOwner,
  servicePlan,
  stableCliPath,
  uninstallService,
  type Runner,
  type ServiceContext,
  type ServicePlatform,
} from './service.js';
import { SELL_URL, resolveTarget, startSeller } from './start.js';
import {
  DEFAULT_API_URL,
  appHome,
  connectUrlFromApi,
  defaultNodeName,
  liveState,
  loadConfig,
  logDir,
  normaliseApiUrl,
  readState,
  saveConfig,
  tokenPrefix,
  validNodeName,
  type SavedConfig,
} from './userconfig.js';
import { PACKAGE_VERSION } from './version.js';

export const USAGE = `inferit-seller ${PACKAGE_VERSION}: sell this machine's inference on Inferit (testnet demo: test tokens, no monetary value)

Usage: inferit-seller <command> [options]

Commands
  connect --token <icn_…> [--api <url>] [--name <name>] [--upstream <url>]
                       Save this machine's node token (from Add a machine in the Sell workspace)
                       to ~/.inferit-seller/config.json, readable by you only
  start                Run in this terminal: find the local server, connect to the market, serve
  install-service [--keep-awake]
                       Run in the background: start at login, restart if it stops
                       (macOS LaunchAgent, Linux systemd user unit, Windows scheduled task)
  uninstall-service    Stop the background service and remove it
  status [--json]      The token, the local servers and models, and the market connection
  doctor [--json]      Check everything and say how to fix what is wrong

Options
  --api <url>          The market (default ${DEFAULT_API_URL})
  --name <name>        This machine's name in the hello (default: the hostname)
  --upstream <url>     Use this OpenAI-compatible server instead of finding one
                       (default: Ollama :11434, LM Studio :1234, vLLM :8000, llama.cpp :8080)
  --keep-awake         Keep the computer from sleeping while the app runs (macOS, Linux)
  --dry-run            install-service / uninstall-service: show what would change, change nothing
  -h, --help           This help
  -v, --version        The version

Environment: INFERIT_TOKEN, INFERIT_API_URL, NODE_NAME, UPSTREAM_BASE_URL, UPSTREAM_API_KEY, MODEL_MAP,
MODEL_META, INFERIT_DETECT_HOSTS, INFERIT_WATCHDOG_MINUTES, INFERIT_KEEP_AWAKE, INFERIT_KEEP_WARM, INFERIT_SELLER_HOME,
LOG_LEVEL.
Docs: https://inferit.cc/docs/sellers/quickstart`;

export interface Io {
  out(s: string): void;
  err(s: string): void;
  env: NodeJS.ProcessEnv;
  platform: NodeJS.Platform;
  userHome: string;
  isTTY: boolean;
  run: Runner;
  exit(code: number): void;
  fetchImpl?: typeof fetch;
  checkTokenImpl?: typeof checkToken;
  /** The package root to install the service from (tests point it at a fixture). */
  pkgRoot?: string;
}

export function realIo(): Io {
  return {
    out: (s) => process.stdout.write(`${s}\n`),
    err: (s) => process.stderr.write(`${s}\n`),
    env: process.env,
    platform: process.platform,
    userHome: os.homedir(),
    isTTY: Boolean(process.stdout.isTTY),
    run: realRunner,
    exit: (code) => process.exit(code),
  };
}

export type Command = 'connect' | 'start' | 'install-service' | 'uninstall-service' | 'status' | 'doctor' | 'help' | 'version';

export interface Parsed {
  command: Command;
  token?: string;
  api?: string;
  name?: string;
  upstream?: string;
  json?: boolean;
  keepAwake?: boolean;
  dryRun?: boolean;
  force?: boolean;
  supervise?: boolean;
  skipCheck?: boolean;
}

const OPTIONS: Record<Exclude<Command, 'help' | 'version'>, Record<string, { type: 'string' | 'boolean'; short?: string }>> = {
  connect: { token: { type: 'string' }, api: { type: 'string' }, name: { type: 'string' }, upstream: { type: 'string' }, 'skip-check': { type: 'boolean' } },
  start: { upstream: { type: 'string' }, force: { type: 'boolean' }, supervise: { type: 'boolean' } },
  'install-service': { 'keep-awake': { type: 'boolean' }, 'dry-run': { type: 'boolean' } },
  'uninstall-service': { 'dry-run': { type: 'boolean' } },
  status: { json: { type: 'boolean' } },
  doctor: { json: { type: 'boolean' } },
};

/** Parses argv (without node and the script). Errors are plain sentences for the user. */
export function parseCommand(argv: string[]): Parsed | { error: string } {
  const [first, ...rest] = argv;
  if (first === undefined || first === 'help' || first === '--help' || first === '-h') return { command: 'help' };
  if (first === 'version' || first === '--version' || first === '-v') return { command: 'version' };
  if (!(first in OPTIONS)) return { error: `Unknown command "${first}". Run inferit-seller --help for the list.` };
  const command = first as keyof typeof OPTIONS;
  if (rest.includes('--help') || rest.includes('-h')) return { command: 'help' };
  let values: Record<string, string | boolean | undefined>;
  try {
    ({ values } = parseArgs({ args: rest, options: OPTIONS[command], strict: true, allowPositionals: false }));
  } catch (e) {
    const msg = (e as Error).message.replace(/\. To specify a positional argument.*$/s, '');
    return { error: `${command}: ${msg}` };
  }
  const p: Parsed = { command };
  const s = (k: string) => (typeof values[k] === 'string' ? (values[k] as string) : undefined);
  const b = (k: string) => values[k] === true;
  if (s('token') !== undefined) p.token = s('token')!.trim();
  if (s('api') !== undefined) p.api = s('api')!;
  if (s('name') !== undefined) p.name = s('name')!.trim();
  if (s('upstream') !== undefined) p.upstream = s('upstream')!;
  if (b('json')) p.json = true;
  if (b('keep-awake')) p.keepAwake = true;
  if (b('dry-run')) p.dryRun = true;
  if (b('force')) p.force = true;
  if (b('supervise')) p.supervise = true;
  if (b('skip-check')) p.skipCheck = true;
  if (command === 'connect' && !p.token) return { error: 'connect: --token is required. Copy the whole command from the Add a machine step in the Sell workspace.' };
  return p;
}

function servicePlatform(p: NodeJS.Platform): ServicePlatform | null {
  return p === 'darwin' || p === 'linux' || p === 'win32' ? p : null;
}

function reportInput(io: Io, home: string): ReportInput {
  return {
    env: io.env,
    appHome: home,
    platform: io.platform,
    userHome: io.userHome,
    ...(io.fetchImpl ? { fetchImpl: io.fetchImpl } : {}),
    ...(io.checkTokenImpl ? { checkTokenImpl: io.checkTokenImpl } : {}),
  };
}

async function cmdConnect(p: Parsed, io: Io): Promise<number> {
  const home = appHome(io.env, io.userHome);
  const token = p.token!;
  if (!NODE_TOKEN_RE.test(token)) {
    io.err('That is not a node token: it starts with icn_ and has 47 characters. Copy the whole command from the Add a machine step in the Sell workspace.');
    return 2;
  }
  let prev: SavedConfig | null = null;
  try {
    prev = loadConfig(home);
  } catch {
    /* rewritten below */
  }
  let apiUrl: string;
  try {
    apiUrl = normaliseApiUrl(p.api ?? io.env.INFERIT_API_URL ?? DEFAULT_API_URL);
  } catch (e) {
    io.err((e as Error).message);
    return 2;
  }
  const nodeName = p.name ?? prev?.nodeName ?? defaultNodeName();
  if (!validNodeName(nodeName)) {
    io.err('--name: at most 64 letters, digits, spaces and . : @ ( ) - _');
    return 2;
  }
  let upstream: string | undefined;
  if (p.upstream) {
    try {
      upstream = upstreamBase(p.upstream);
    } catch (e) {
      io.err(`--upstream: ${(e as Error).message}`);
      return 2;
    }
    const provider = knownProviderOf(upstream);
    if (provider) {
      io.err(`--upstream is on ${provider}, a hosted inference API. Sell your own server's inference (Ollama, LM Studio, vLLM, llama.cpp).`);
      return 2;
    }
  }
  const connectUrl = connectUrlFromApi(apiUrl);
  if (!p.skipCheck) {
    const r = await (io.checkTokenImpl ?? checkToken)(connectUrl, token);
    if (r.ok) io.out(`The market at ${apiUrl} accepts this token.`);
    else if (!r.reachable) io.out(`Could not reach the market to check the token (${r.error}). Saving it anyway; the app retries when it starts.`);
    else {
      const f = tokenFix(r);
      if (f.fatal) {
        io.err(`${f.problem}\n${f.fix}`);
        return 1;
      }
      io.out(`${f.problem}\n${f.fix}`);
    }
  }
  const cfg: SavedConfig = { token, connectUrl, apiUrl, nodeName, ...(upstream ? { upstream } : prev?.upstream && !p.upstream ? { upstream: prev.upstream } : {}), ...(prev?.keepAwake ? { keepAwake: true } : {}) };
  const file = saveConfig(home, cfg);
  io.out(`Saved ${tokenPrefix(token)} for "${nodeName}" to ${file} (readable by you only).`);
  const plat = servicePlatform(io.platform);
  const owner = plat
    ? serviceOwner(servicePlan({ platform: plat, userHome: io.userHome, appHome: home, nodePath: process.execPath, cliPath: 'cli.js', ...(io.env.XDG_CONFIG_HOME ? { xdgConfigHome: io.env.XDG_CONFIG_HOME } : {}) }), home)
    : null;
  io.out(
    owner?.installed && owner.ours
      ? 'The background service is installed: run inferit-seller install-service again so it restarts with this token.'
      : owner?.installed
        ? `The background service on this computer runs another app folder (${owner.home}), so it does not use this token. Run inferit-seller start to use it here (in this terminal).`
        : 'Next: inferit-seller install-service (runs in the background and at every login), or inferit-seller start (in this terminal).',
  );
  return 0;
}

async function waitWhileAnotherRuns(home: string, io: Io, log: (m: string) => void): Promise<void> {
  let said = false;
  for (;;) {
    const st = readState(home);
    if (!liveState(st) || st!.pid === process.pid) return;
    if (!said) log(`another inferit-seller (pid ${st!.pid}) is running; waiting for it to stop`);
    said = true;
    await new Promise((r) => setTimeout(r, 30_000));
  }
}

/** Windows: a small parent that runs `start` and restarts it when it exits, writing its output to the log file. */
async function supervise(io: Io, home: string): Promise<number> {
  const file = new RotatingFile(path.join(logDir(home), 'seller.log'));
  const note = (msg: string) => file.write(`${JSON.stringify({ level: 30, time: Date.now(), pid: process.pid, msg })}\n`);
  let child: ReturnType<typeof spawn> | null = null;
  let stopping = false;
  const stop = () => {
    stopping = true;
    child?.kill();
  };
  process.once('SIGINT', stop);
  process.once('SIGTERM', stop);
  let delay = 5_000;
  while (!stopping) {
    const started = Date.now();
    child = spawn(process.execPath, [process.argv[1]!, 'start'], {
      env: { ...io.env, INFERIT_SELLER_SUPERVISED: '1' },
      stdio: ['ignore', 'pipe', 'pipe'],
      windowsHide: true,
    });
    child.stdout?.on('data', (d: Buffer) => file.write(d.toString()));
    child.stderr?.on('data', (d: Buffer) => file.write(d.toString()));
    const code = await new Promise<number>((r) => {
      child!.on('error', () => r(1));
      child!.on('close', (c) => r(c ?? 1));
    });
    if (stopping) break;
    delay = Date.now() - started > 120_000 ? 5_000 : Math.min(delay * 2, 60_000);
    note(`the app exited with code ${code}; starting it again in ${Math.round(delay / 1000)} s`);
    await new Promise((r) => setTimeout(r, delay));
  }
  return 0;
}

async function cmdStart(p: Parsed, io: Io): Promise<number> {
  const home = appHome(io.env, io.userHome);
  const service: ServiceContext | null = serviceContext(io.env, io.platform);
  let cfg: SavedConfig | null;
  try {
    cfg = loadConfig(home);
  } catch (e) {
    io.err((e as Error).message);
    return 2;
  }
  let t: ReturnType<typeof resolveTarget>;
  try {
    t = resolveTarget(io.env, cfg, p.upstream ? { upstream: p.upstream } : {});
  } catch (e) {
    io.err((e as Error).message);
    return 2;
  }
  const mock = ['1', 'true', 'yes'].includes(io.env.MOCK?.trim().toLowerCase() ?? '');
  if (!t.token && !t.connectUrl && !mock) {
    io.err(`No node token yet. In the Sell workspace (${SELL_URL}), choose Add a machine and run the command it shows (inferit-seller connect --token icn_…). Run inferit-seller doctor to check this machine first.`);
    return 2;
  }
  if (p.supervise) return supervise(io, home);

  const supervised = io.env.INFERIT_SELLER_SUPERVISED === '1';
  const format: LogFormat =
    service === 'launchd' || (service === 'windows-task' && !supervised) ? 'file' : service || supervised || !io.isTTY ? 'json' : 'pretty';
  const sink = logSink({ format, file: path.join(logDir(home), 'seller.log') });
  const log = makeLogger(sink, io.env.LOG_LEVEL ?? 'info');

  if (!mock) {
    const st = readState(home);
    if (liveState(st) && st!.pid !== process.pid) {
      if (service) await waitWhileAnotherRuns(home, io, (m) => log.warn({}, m));
      else if (!p.force) {
        io.err(`inferit-seller is already running (pid ${st!.pid}${st!.service ? `, the ${st!.service} service` : ''}). Two copies with one token would replace each other on the market. Run inferit-seller status, or stop the other copy first (inferit-seller uninstall-service).`);
        return 1;
      }
    }
  }

  if (format === 'pretty') io.out(`inferit-seller ${PACKAGE_VERSION}. Testnet demo: test tokens, no monetary value. Stop with Ctrl+C.`);
  const running = await startSeller({
    env: io.env,
    appHome: home,
    config: cfg,
    flags: p.upstream ? { upstream: p.upstream } : {},
    log,
    sink,
    exit: io.exit,
    service,
    ...(io.fetchImpl ? { fetchImpl: io.fetchImpl } : {}),
  });
  return new Promise<number>((resolve) => {
    const shutdown = (signal: string) => {
      log.info({ signal }, 'shutting down');
      running.stop().then(
        () => resolve(0),
        () => resolve(1),
      );
    };
    process.once('SIGINT', () => shutdown('SIGINT'));
    process.once('SIGTERM', () => shutdown('SIGTERM'));
  });
}

async function cmdInstall(p: Parsed, io: Io): Promise<number> {
  const plat = servicePlatform(io.platform);
  if (!plat) {
    io.err(`install-service supports macOS, Linux and Windows; on ${io.platform} run inferit-seller start under your own supervisor, or use the Docker image.`);
    return 2;
  }
  const home = appHome(io.env, io.userHome);
  let cfg: SavedConfig | null;
  try {
    cfg = loadConfig(home);
  } catch (e) {
    io.err((e as Error).message);
    return 2;
  }
  if (!cfg) {
    io.err(`No node token saved yet. Run the connect command from the Add a machine step in the Sell workspace (${SELL_URL}) first.`);
    return 2;
  }
  // One service per user account: never replace one that runs another app folder (INFERIT_SELLER_HOME differs).
  // Checked before anything is copied or saved.
  const where = servicePlan({ platform: plat, userHome: io.userHome, appHome: home, nodePath: process.execPath, cliPath: 'cli.js', ...(io.env.XDG_CONFIG_HOME ? { xdgConfigHome: io.env.XDG_CONFIG_HOME } : {}) });
  const owner = serviceOwner(where, home);
  if (owner.installed && !owner.ours) {
    const msg = `A background service for another app folder is installed: ${where.file} runs ${owner.home}. There is one per user account, and installing it for ${home} would replace that one. To replace it on purpose, first run inferit-seller uninstall-service with INFERIT_SELLER_HOME=${owner.home} (or without INFERIT_SELLER_HOME, if that is the default folder).`;
    if (!p.dryRun) {
      io.err(msg);
      return 1;
    }
    io.out(`Note: ${msg}`);
  }
  const pkgRoot = io.pkgRoot ?? packageRoot();
  let cliPath: string;
  if (p.dryRun) {
    cliPath = isEphemeralInstall(pkgRoot) ? path.join(home, 'app', 'node_modules', path.basename(pkgRoot), 'dist', 'cli.js') : path.join(pkgRoot, 'dist', 'cli.js');
  } else {
    if (p.keepAwake && !cfg.keepAwake) saveConfig(home, { ...cfg, keepAwake: true });
    try {
      const r = stableCliPath({ pkgRoot, appHome: home });
      cliPath = r.cliPath;
      if (r.copied) io.out(`Copied the app to ${path.join(home, 'app')} (the npx cache can be cleaned at any time).`);
    } catch (e) {
      io.err((e as Error).message);
      return 1;
    }
  }
  const user = plat === 'win32' ? (io.env.USERNAME ? `${io.env.USERDOMAIN ? `${io.env.USERDOMAIN}\\` : ''}${io.env.USERNAME}` : undefined) : safeUser();
  const plan = servicePlan({
    platform: plat,
    userHome: io.userHome,
    appHome: home,
    nodePath: process.execPath,
    cliPath,
    ...(typeof process.getuid === 'function' ? { uid: process.getuid() } : {}),
    ...(user ? { user } : {}),
    ...(io.env.XDG_CONFIG_HOME ? { xdgConfigHome: io.env.XDG_CONFIG_HOME } : {}),
  });
  if (p.dryRun) {
    io.out(`Would write ${plan.file}:\n\n${plan.content}`);
    io.out(`Then run:\n${plan.install.map((s) => `  ${s.cmd} ${s.args.join(' ')}`).join('\n')}`);
    return 0;
  }
  try {
    const results = await installService(plan, io.run);
    for (const r of results) if (!r.ok && r.step.hint) io.out(r.step.hint);
  } catch (e) {
    io.err(`Could not install the service: ${(e as Error).message}`);
    return 1;
  }
  const st = readState(home);
  io.out(`Installed ${plan.file}. The app runs now, starts at every login and restarts if it stops.`);
  if (liveState(st) && !st!.service) io.out(`A copy started in a terminal is still running (pid ${st!.pid}); the service waits until you stop it (Ctrl+C there).`);
  io.out(`Logs: ${plan.logs}`);
  io.out('Check it in a few seconds: inferit-seller status');
  return 0;
}

function safeUser(): string | undefined {
  try {
    return os.userInfo().username;
  } catch {
    return undefined;
  }
}

async function cmdUninstall(p: Parsed, io: Io): Promise<number> {
  const plat = servicePlatform(io.platform);
  if (!plat) {
    io.err(`No service to remove on ${io.platform}.`);
    return 2;
  }
  const home = appHome(io.env, io.userHome);
  const plan = servicePlan({
    platform: plat,
    userHome: io.userHome,
    appHome: home,
    nodePath: process.execPath,
    cliPath: 'cli.js',
    ...(typeof process.getuid === 'function' ? { uid: process.getuid() } : {}),
    ...(io.env.XDG_CONFIG_HOME ? { xdgConfigHome: io.env.XDG_CONFIG_HOME } : {}),
  });
  const owner = serviceOwner(plan, home);
  if (owner.installed && !owner.ours) {
    io.err(
      `The installed service (${plan.file}) runs another app folder, ${owner.home}, not ${home}; it was left alone. To remove it, run inferit-seller uninstall-service with INFERIT_SELLER_HOME=${owner.home} (or without INFERIT_SELLER_HOME, if that is the default folder).`,
    );
    return 1;
  }
  if (p.dryRun) {
    io.out(`Would run:\n${plan.uninstall.map((s) => `  ${s.cmd} ${s.args.join(' ')}`).join('\n')}\nand remove ${plan.file}`);
    return 0;
  }
  const { removed } = await uninstallService(plan, io.run);
  io.out(removed ? `Stopped the service and removed ${plan.file}. Your token stays in ${path.join(home, 'config.json')}.` : 'No service was installed.');
  return 0;
}

async function cmdStatus(p: Parsed, io: Io): Promise<number> {
  const s = await snapshot(reportInput(io, appHome(io.env, io.userHome)));
  io.out(p.json ? JSON.stringify(statusJson(s), null, 2) : formatStatus(s));
  return 0;
}

async function cmdDoctor(p: Parsed, io: Io): Promise<number> {
  const r = await doctor(reportInput(io, appHome(io.env, io.userHome)));
  io.out(p.json ? JSON.stringify({ ok: r.ok, checks: r.checks }, null, 2) : formatDoctor(r));
  return r.ok ? 0 : 1;
}

/** Runs one command; resolves with the exit code (`start` resolves when it stops). */
export async function main(argv: string[], io: Io = realIo()): Promise<number> {
  const p = parseCommand(argv);
  if ('error' in p) {
    io.err(p.error);
    return 2;
  }
  try {
    switch (p.command) {
      case 'help':
        io.out(USAGE);
        return 0;
      case 'version':
        io.out(PACKAGE_VERSION);
        return 0;
      case 'connect':
        return await cmdConnect(p, io);
      case 'start':
        return await cmdStart(p, io);
      case 'install-service':
        return await cmdInstall(p, io);
      case 'uninstall-service':
        return await cmdUninstall(p, io);
      case 'status':
        return await cmdStatus(p, io);
      case 'doctor':
        return await cmdDoctor(p, io);
    }
  } catch (e) {
    io.err(`inferit-seller ${p.command}: ${(e as Error).message}`);
    return 1;
  }
}
