// The command line (src/commands.ts): argument parsing, connect (token check, config file), install-service into a
// temporary home with a recording runner, and status and doctor without a token, all with stubbed network access.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { main, parseCommand, USAGE, type Io } from '../src/commands.ts';
import type { checkToken } from '../src/report.ts';
import type { Runner } from '../src/service.ts';
import { bootAt, writeState, type RunState } from '../src/userconfig.ts';
import { PACKAGE_VERSION } from '../src/version.ts';
import { CATALOG_BODY, OLLAMA_ROUTES } from './fixtures.ts';

const TOKEN = `icn_${'C'.repeat(43)}`;
let dir: string;
beforeEach(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'inferit-seller-cli-'));
});
afterEach(() => {
  fs.rmSync(dir, { recursive: true, force: true });
});

/** Ollama on box:11434 with the fixture models, the market's model list; everything else refused. */
const fakeFetch = (async (input: string | URL) => {
  const u = new URL(String(input));
  if (u.hostname === 'box' && u.port === '11434' && OLLAMA_ROUTES[u.pathname] !== undefined) return new Response(JSON.stringify(OLLAMA_ROUTES[u.pathname]), { headers: { 'content-type': 'application/json' } });
  if (u.hostname === 'api.example' && u.pathname === '/v1/models') return new Response(JSON.stringify(CATALOG_BODY), { headers: { 'content-type': 'application/json' } });
  throw Object.assign(new TypeError('fetch failed'), { cause: { code: 'ECONNREFUSED' } });
}) as unknown as typeof fetch;

function io(extra: Partial<Io> = {}, env: NodeJS.ProcessEnv = {}): Io & { stdout: string[]; stderr: string[]; calls: string[] } {
  const stdout: string[] = [];
  const stderr: string[] = [];
  const calls: string[] = [];
  const run: Runner = async (cmd, args) => {
    calls.push(`${cmd} ${args.join(' ')}`);
    return { code: 0, stdout: '', stderr: '' };
  };
  return {
    stdout,
    stderr,
    calls,
    out: (s) => stdout.push(s),
    err: (s) => stderr.push(s),
    env: { INFERIT_SELLER_HOME: path.join(dir, '.inferit-seller'), INFERIT_DETECT_HOSTS: 'box', INFERIT_API_URL: 'https://api.example', ...env },
    platform: 'linux',
    userHome: dir,
    isTTY: false,
    run,
    exit: () => {
      throw new Error('exit called');
    },
    fetchImpl: fakeFetch,
    checkTokenImpl: (async () => ({ ok: true })) as typeof checkToken,
    ...extra,
  };
}

describe('parseCommand', () => {
  it('reads each command and its options', () => {
    expect(parseCommand([])).toEqual({ command: 'help' });
    expect(parseCommand(['--help'])).toEqual({ command: 'help' });
    expect(parseCommand(['doctor', '-h'])).toEqual({ command: 'help' });
    expect(parseCommand(['--version'])).toEqual({ command: 'version' });
    expect(parseCommand(['-v'])).toEqual({ command: 'version' });
    expect(parseCommand(['connect', '--token', TOKEN, '--api', 'https://api.example', '--name', 'Jean PC'])).toEqual({ command: 'connect', token: TOKEN, api: 'https://api.example', name: 'Jean PC' });
    expect(parseCommand(['connect', `--token=${TOKEN}`, '--upstream', 'http://127.0.0.1:1234', '--skip-check'])).toEqual({ command: 'connect', token: TOKEN, upstream: 'http://127.0.0.1:1234', skipCheck: true });
    expect(parseCommand(['start', '--upstream', 'http://127.0.0.1:8000/v1', '--force'])).toEqual({ command: 'start', upstream: 'http://127.0.0.1:8000/v1', force: true });
    expect(parseCommand(['start', '--supervise'])).toEqual({ command: 'start', supervise: true });
    expect(parseCommand(['install-service', '--keep-awake', '--dry-run'])).toEqual({ command: 'install-service', keepAwake: true, dryRun: true });
    expect(parseCommand(['uninstall-service'])).toEqual({ command: 'uninstall-service' });
    expect(parseCommand(['status', '--json'])).toEqual({ command: 'status', json: true });
    expect(parseCommand(['doctor'])).toEqual({ command: 'doctor' });
  });

  it('explains what is wrong', () => {
    expect(parseCommand(['sell'])).toEqual({ error: 'Unknown command "sell". Run inferit-seller --help for the list.' });
    expect(parseCommand(['connect'])).toMatchObject({ error: expect.stringContaining('--token is required') });
    expect(parseCommand(['connect', '--token'])).toMatchObject({ error: expect.stringContaining("connect: Option '--token <value>' argument missing") });
    expect(parseCommand(['status', '--verbose'])).toMatchObject({ error: expect.stringContaining("Unknown option '--verbose'") });
    expect(parseCommand(['start', 'now'])).toMatchObject({ error: expect.stringContaining('start: Unexpected argument') });
  });
});

describe('main', () => {
  it('prints the help and the version', async () => {
    const a = io();
    expect(await main(['--help'], a)).toBe(0);
    expect(a.stdout[0]).toBe(USAGE);
    expect(USAGE).toContain('connect --token <icn_…>');
    expect(USAGE).toContain('install-service [--keep-awake]');
    const b = io();
    expect(await main(['--version'], b)).toBe(0);
    expect(b.stdout).toEqual([PACKAGE_VERSION]);
    const c = io();
    expect(await main(['bogus'], c)).toBe(2);
  });

  it('connect checks the token with the market and saves it 0600', async () => {
    const seen: string[] = [];
    const a = io({ checkTokenImpl: (async (url: string) => (seen.push(url), { ok: true })) as typeof checkToken });
    expect(await main(['connect', '--token', TOKEN, '--name', 'jeans-pc'], a)).toBe(0);
    expect(seen).toEqual(['wss://api.example/v1/seller/connect']);
    const file = path.join(dir, '.inferit-seller', 'config.json');
    const saved = JSON.parse(fs.readFileSync(file, 'utf8')) as Record<string, unknown>;
    expect(saved).toMatchObject({ token: TOKEN, apiUrl: 'https://api.example', connectUrl: 'wss://api.example/v1/seller/connect', nodeName: 'jeans-pc' });
    if (process.platform !== 'win32') expect(fs.statSync(file).mode & 0o777).toBe(0o600);
    expect(a.stdout.join('\n')).toContain('accepts this token');
    expect(a.stdout.join('\n')).toContain('Next: inferit-seller install-service');
    expect(a.stdout.join('\n')).not.toContain(TOKEN); // only the prefix is printed
  });

  it('connect refuses a malformed, unknown or revoked token and saves nothing', async () => {
    const bad = io();
    expect(await main(['connect', '--token', 'ik_seller_abc'], bad)).toBe(2);
    expect(bad.stderr[0]).toMatch(/not a node token/);
    const refused = io({ checkTokenImpl: (async () => ({ ok: false, reachable: true, httpStatus: 403, code: 'node_revoked', message: 'This node token was revoked; mint a new one.' })) as typeof checkToken });
    expect(await main(['connect', '--token', TOKEN], refused)).toBe(1);
    expect(refused.stderr.join('\n')).toMatch(/This token was revoked\. The market says: "This node token was revoked; mint a new one\."/);
    expect(fs.existsSync(path.join(dir, '.inferit-seller', 'config.json'))).toBe(false);
  });

  it('connect saves a valid token that is not registered as a seller yet, or when the market is offline, and says so', async () => {
    const unregistered = io({ checkTokenImpl: (async () => ({ ok: false, reachable: true, httpStatus: 403, code: 'seller_not_registered', message: 'Register as a seller on the escrow first.' })) as typeof checkToken });
    expect(await main(['connect', '--token', TOKEN], unregistered)).toBe(0);
    expect(unregistered.stdout.join('\n')).toMatch(/not registered as a seller yet/);
    const offline = io({ checkTokenImpl: (async () => ({ ok: false, reachable: false, error: 'ENOTFOUND' })) as typeof checkToken });
    expect(await main(['connect', '--token', TOKEN, '--upstream', 'http://127.0.0.1:1234'], offline)).toBe(0);
    expect(offline.stdout[0]).toMatch(/Could not reach the market to check the token \(ENOTFOUND\)/);
    expect(JSON.parse(fs.readFileSync(path.join(dir, '.inferit-seller', 'config.json'), 'utf8'))).toMatchObject({ upstream: 'http://127.0.0.1:1234/v1' });
    const provider = io();
    expect(await main(['connect', '--token', TOKEN, '--upstream', 'https://api.openai.com/v1'], provider)).toBe(2);
    expect(provider.stderr[0]).toMatch(/openai\.com, a hosted inference API/);
  });

  it('start without a token says how to get one', async () => {
    const a = io({}, { INFERIT_API_URL: '' });
    expect(await main(['start'], a)).toBe(2);
    expect(a.stderr[0]).toMatch(/No node token yet\. In the Sell workspace \(https:\/\/inferit\.app\/sell\), choose Add a machine/);
  });

  it('status without a token: the local servers, the models and what they map to', async () => {
    const a = io();
    expect(await main(['status'], a)).toBe(0);
    const text = a.stdout.join('\n');
    expect(text).toContain('Token    none: run connect --token <icn_…>');
    expect(text).toMatch(/Ollama\s+http:\/\/box:11434\s+3 models/);
    expect(text).toMatch(/vLLM\s+http:\/\/box:8000\s+nothing listening/);
    expect(text).toContain('qwen2.5:14b-instruct  (Ollama, q4_k_m, 9.0 GB)  -> qwen/qwen-2.5-14b-instruct');
    expect(text).toContain('nomic-embed-text:latest  (Ollama, f16, 0.3 GB)  not in the catalog');
    expect(text).toContain('note: gpt-oss:120b-cloud: an Ollama cloud model');
    const j = io();
    expect(await main(['status', '--json'], j)).toBe(0);
    const json = JSON.parse(j.stdout[0]!) as { token: unknown; served: { catalogId: string }[]; running: boolean };
    expect(json.token).toBeNull();
    expect(json.running).toBe(false);
    expect(json.served.map((s) => s.catalogId)).toEqual(['qwen/qwen-2.5-14b-instruct']);
  });

  it('doctor without a token: the missing token is the one failure, with the fix', async () => {
    const a = io();
    expect(await main(['doctor'], a)).toBe(1);
    const text = a.stdout.join('\n');
    expect(text).toContain('[ok]   Local server: Ollama 0.40.3 at http://box:11434/v1 with 3 models.');
    expect(text).toContain('[ok]   Market: https://api.example answers (12 catalog models).');
    expect(text).toContain('[ok]   Models: 1 of 3 matched to the catalog: qwen2.5:14b-instruct -> qwen/qwen-2.5-14b-instruct.');
    // No listing to compare with: information (the listing's limit must be at most what is served), not a warning.
    expect(text).toContain("[info] Context: Ollama serves qwen2.5:14b-instruct with 4096 tokens of context; the catalog lists 32768. A listing's context limit must be at most 4096");
    expect(text).toContain('Fix: To sell longer prompts, set OLLAMA_CONTEXT_LENGTH=32768');
    expect(text).not.toContain('[warn] Context');
    expect(text).toContain('[fail] Token: No node token is saved');
    expect(text).toContain('Fix: In the Sell workspace (https://inferit.app/sell), choose Add a machine');
    expect(text).toContain('1 problem to fix.');
    const j = io();
    await main(['doctor', '--json'], j);
    const json = JSON.parse(j.stdout[0]!) as { ok: boolean; checks: { id: string; status: string }[] };
    expect(json.ok).toBe(false);
    expect(json.checks.filter((c) => c.status === 'fail').map((c) => c.id)).toEqual(['token']);
  });

  it('doctor with a saved token asks the market about it', async () => {
    await main(['connect', '--token', TOKEN, '--skip-check'], io());
    const a = io({ checkTokenImpl: (async () => ({ ok: false, reachable: true, httpStatus: 401, code: 'invalid_node_token', message: 'Unknown node token.' })) as typeof checkToken });
    expect(await main(['doctor'], a)).toBe(1);
    expect(a.stdout.join('\n')).toContain('[fail] Token: The market does not know this token. The market says: "Unknown node token."');
    const ok = io();
    expect(await main(['doctor'], ok)).toBe(0);
    expect(ok.stdout.join('\n')).toContain('[ok]   Token: The market accepts icn_CCCC…CCCC.');
  });

  it('doctor with no local server says how to start one', async () => {
    const a = io({}, { INFERIT_DETECT_HOSTS: 'nothing-here' });
    expect(await main(['doctor'], a)).toBe(1);
    expect(a.stdout.join('\n')).toContain('[fail] Local server: No local inference server answers on nothing-here (Ollama :11434');
    expect(a.stdout.join('\n')).toContain('Install Ollama (https://ollama.com)');
  });

  it('doctor in the Docker image looks on the host and leaves out the service and sleep checks', async () => {
    const a = io({}, { INFERIT_SELLER_DOCKER: '1', INFERIT_DETECT_HOSTS: '' });
    expect(await main(['doctor'], a)).toBe(1);
    const text = a.stdout.join('\n');
    expect(text).toContain('No local inference server answers on host.docker.internal');
    // Ollama stays on loopback: host networking, never OLLAMA_HOST=0.0.0.0 (it has no auth).
    expect(text).toContain('--network host and -e UPSTREAM_BASE_URL=http://127.0.0.1:11434/v1');
    expect(text).not.toContain('OLLAMA_HOST=0.0.0.0');
    expect(text).toContain('Fix: Start the container: docker start inferit-seller.');
    expect(text).not.toContain('Service:');
    expect(text).not.toContain('Sleep:');
  });

  it('install-service needs a token, then copies the app out of a temporary install, writes the unit into the temp home and runs the recorded commands', async () => {
    // The fixture lives under the OS temporary directory, so it is treated like the npx cache: copied first.
    const pkgRoot = path.join(dir, 'npx', 'node_modules', 'inferit-seller');
    fs.mkdirSync(path.join(pkgRoot, 'dist'), { recursive: true });
    fs.writeFileSync(path.join(pkgRoot, 'dist', 'cli.js'), '// cli');
    const none = io({ pkgRoot });
    expect(await main(['install-service'], none)).toBe(2);
    expect(none.stderr[0]).toMatch(/No node token saved yet/);
    await main(['connect', '--token', TOKEN, '--skip-check'], io());
    const copied = path.join(dir, '.inferit-seller', 'app', 'node_modules', 'inferit-seller', 'dist', 'cli.js');
    const dry = io({ pkgRoot });
    expect(await main(['install-service', '--dry-run'], dry)).toBe(0);
    expect(dry.calls).toEqual([]);
    const sd = (x: string) => x.replace(/\\/g, '\\\\'); // systemd escapes backslashes (a Windows test run)
    expect(dry.stdout.join('\n')).toContain(`ExecStart="${sd(process.execPath)}" "${sd(copied)}" "start"`);
    const unit = path.join(dir, '.config', 'systemd', 'user', 'inferit-seller.service');
    expect(fs.existsSync(unit)).toBe(false);
    expect(fs.existsSync(copied)).toBe(false);
    const real = io({ pkgRoot });
    expect(await main(['install-service', '--keep-awake'], real)).toBe(0);
    expect(real.stdout[0]).toMatch(/^Copied the app to /);
    expect(fs.readFileSync(copied, 'utf8')).toBe('// cli');
    expect(fs.readFileSync(unit, 'utf8')).toContain('Restart=always');
    expect(real.calls.slice(0, 3)).toEqual(['systemctl --user daemon-reload', 'systemctl --user enable inferit-seller.service', 'systemctl --user restart inferit-seller.service']);
    expect(JSON.parse(fs.readFileSync(path.join(dir, '.inferit-seller', 'config.json'), 'utf8'))).toMatchObject({ keepAwake: true });
    const un = io({ pkgRoot });
    expect(await main(['uninstall-service'], un)).toBe(0);
    expect(fs.existsSync(unit)).toBe(false);
  });

  it('doctor compares the served context with the listing\'s limit from the running app: a warning only when the listing accepts more', async () => {
    const home = path.join(dir, '.inferit-seller');
    const now = new Date().toISOString();
    const live = (offerList: RunState['offerList'], models: RunState['models'] = [{ id: 'qwen/qwen-2.5-14b-instruct', local: 'qwen2.5:14b-instruct', server: 'ollama', contextLength: 4096 }]): RunState => ({
      pid: process.pid,
      bootAt: bootAt(),
      startedAt: now,
      updatedAt: now,
      version: PACKAGE_VERSION,
      tokenPrefix: null,
      service: null,
      phase: 'live',
      nodeId: 'node_x',
      sessionId: 'cs_1',
      liveSince: now,
      offers: offerList?.length ?? null,
      failedAttempts: 0,
      lastFailure: null,
      lastFailureAt: null,
      models,
      offerList,
      message: null,
    });
    const run = async () => {
      const a = io();
      await main(['doctor'], a);
      return a.stdout.join('\n');
    };
    // The listing's limit is what Ollama serves: fine, with how to sell more.
    writeState(home, live([{ model: 'qwen/qwen-2.5-14b-instruct', maxContextTokens: 4096 }]));
    let text = await run();
    expect(text).toContain("[ok]   Context: Ollama serves qwen2.5:14b-instruct with 4096 tokens of context; your listing's limit (4096) is within it");
    expect(text).not.toContain('[warn] Context');
    expect(text).toContain('offers on the market for this machine: 1');
    // No limit on the listing (the catalog's 32768) while 4096 is served: the one case to warn about.
    writeState(home, live([{ model: 'qwen/qwen-2.5-14b-instruct', maxContextTokens: null }]));
    text = await run();
    expect(text).toContain('[warn] Context: Ollama serves qwen2.5:14b-instruct with 4096 tokens of context, but your listing for qwen/qwen-2.5-14b-instruct accepts prompts up to 32768 tokens.');
    expect(text).toContain('under Offers, choose Edit on that offer and set its context limit to at most 4096');
    // An older market that does not say the limit: information only.
    writeState(home, live([{ model: 'qwen/qwen-2.5-14b-instruct' }]));
    expect(await run()).toContain("[info] Context: Ollama serves qwen2.5:14b-instruct with 4096 tokens of context; the catalog lists 32768. A listing's context limit must be at most 4096");
  });

  it('doctor and status give the 5-minute wait after a refused token, not the 60 s backoff', async () => {
    const home = path.join(dir, '.inferit-seller');
    const now = Date.now();
    writeState(home, {
      pid: process.pid,
      bootAt: bootAt(),
      startedAt: new Date(now).toISOString(),
      updatedAt: new Date(now).toISOString(),
      version: PACKAGE_VERSION,
      tokenPrefix: null,
      service: null,
      phase: 'waiting',
      nodeId: null,
      sessionId: null,
      liveSince: null,
      offers: 1,
      failedAttempts: 1,
      lastFailure: 'HTTP 403 node_revoked: This node was revoked.',
      lastFailureAt: new Date(now).toISOString(),
      nextAttemptAt: new Date(now + 4 * 60_000).toISOString(),
      retryReason: 'auth',
      models: [],
      message: null,
    });
    const d = io();
    await main(['doctor'], d);
    const text = d.stdout.join('\n');
    expect(text).toContain('[warn] Running: The app runs');
    expect(text).toContain('the market refused the token; it tries again every 5 min (next try in 4 min)');
    expect(text).toContain('the app waits 5 min between tries');
    expect(text).not.toContain('at most every 60 s');
    const s = io();
    await main(['status'], s);
    expect(s.stdout.join('\n')).toContain('not connected: HTTP 403 node_revoked: This node was revoked.');
    expect(s.stdout.join('\n')).toContain('it tries again every 5 min');
  });

  // The plan here is a Linux systemd unit; on a Windows runner the temp paths have backslashes, which a real unit never has.
  it.skipIf(process.platform === 'win32')('with INFERIT_SELLER_HOME set to another folder, the installed service is not this folder\'s: shown so, never replaced or removed', async () => {
    const pkgRoot = path.join(dir, 'npx', 'node_modules', 'inferit-seller');
    fs.mkdirSync(path.join(pkgRoot, 'dist'), { recursive: true });
    fs.writeFileSync(path.join(pkgRoot, 'dist', 'cli.js'), '// cli');
    // The default folder installs the service.
    await main(['connect', '--token', TOKEN, '--skip-check'], io());
    expect(await main(['install-service'], io({ pkgRoot }))).toBe(0);
    const unit = path.join(dir, '.config', 'systemd', 'user', 'inferit-seller.service');
    const before = fs.readFileSync(unit, 'utf8');
    expect(before).toContain(`INFERIT_SELLER_HOME=${path.join(dir, '.inferit-seller')}`);

    // A test folder: connect says the service is not its own, and status and doctor agree.
    const other = { INFERIT_SELLER_HOME: path.join(dir, 'test-home') };
    const c = io({ pkgRoot }, other);
    expect(await main(['connect', '--token', `icn_${'D'.repeat(43)}`, '--skip-check'], c)).toBe(0);
    expect(c.stdout.join('\n')).toContain(`runs another app folder (${path.join(dir, '.inferit-seller')}), so it does not use this token`);
    expect(c.stdout.join('\n')).not.toContain('install-service again');
    const st = io({ pkgRoot }, other);
    await main(['status'], st);
    expect(st.stdout.join('\n')).toContain(`Service    not installed for this folder: ${unit} runs ${path.join(dir, '.inferit-seller')}`);
    const doc = io({ pkgRoot }, other);
    await main(['doctor'], doc);
    expect(doc.stdout.join('\n')).toContain(`[warn] Service: The background service (${unit}) runs another app folder, ${path.join(dir, '.inferit-seller')}, not this one`);
    expect(doc.stdout.join('\n')).not.toContain('[ok]   Service');

    // install-service and uninstall-service from the test folder refuse, and leave the unit as it was.
    const inst = io({ pkgRoot }, other);
    expect(await main(['install-service'], inst)).toBe(1);
    expect(inst.stderr.join('\n')).toContain('would replace that one');
    expect(inst.calls).toEqual([]);
    expect(fs.existsSync(path.join(dir, 'test-home', 'app'))).toBe(false); // refused before copying anything
    const uninst = io({ pkgRoot }, other);
    expect(await main(['uninstall-service'], uninst)).toBe(1);
    expect(uninst.stderr.join('\n')).toContain('it was left alone');
    expect(uninst.calls).toEqual([]);
    expect(fs.readFileSync(unit, 'utf8')).toBe(before);
  });
});
