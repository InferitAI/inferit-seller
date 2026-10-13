// Background service files (src/service.ts), written into temporary directories with a recording command runner:
// nothing here calls launchctl, systemctl or schtasks, and nothing is installed on the machine running the tests.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import {
  installService,
  isEphemeralInstall,
  serviceContext,
  serviceInstalled,
  servicePlan,
  stableCliPath,
  uninstallService,
  type Runner,
  type ServiceInput,
} from '../src/service.ts';

let dir: string;
beforeEach(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'inferit-seller-svc-'));
});
afterEach(() => {
  fs.rmSync(dir, { recursive: true, force: true });
});

const input = (platform: ServiceInput['platform'], extra: Partial<ServiceInput> = {}): ServiceInput => ({
  platform,
  userHome: path.join(dir, 'home'),
  appHome: path.join(dir, 'home', '.inferit-seller'),
  nodePath: '/opt/node 22/bin/node',
  cliPath: '/opt/inferit seller/dist/cli.js',
  uid: 501,
  user: 'jean',
  ...extra,
});

/** Records every command and answers from `codes` (default 0). */
function recorder(codes: Record<string, number[]> = {}): { run: Runner; calls: string[] } {
  const calls: string[] = [];
  const run: Runner = async (cmd, args) => {
    const line = `${cmd} ${args.join(' ')}`;
    calls.push(line);
    const key = Object.keys(codes).find((k) => line.startsWith(k));
    const code = key ? (codes[key]!.shift() ?? 0) : 0;
    return { code, stdout: '', stderr: code ? 'simulated failure' : '' };
  };
  return { run, calls };
}

describe('serviceContext', () => {
  it('knows launchd, systemd, the Windows task and Docker, and a terminal is none', () => {
    expect(serviceContext({ INFERIT_SELLER_SERVICE: 'launchd' }, 'darwin')).toBe('launchd');
    expect(serviceContext({ XPC_SERVICE_NAME: 'cc.inferit.seller' }, 'darwin')).toBe('launchd');
    expect(serviceContext({ XPC_SERVICE_NAME: '0' }, 'darwin')).toBeNull();
    expect(serviceContext({ INVOCATION_ID: 'abc', JOURNAL_STREAM: '8:1' }, 'linux', (j) => j === '8:1')).toBe('systemd');
    // A terminal inside a systemd user service (GNOME) inherits both variables, but its output is not the journal.
    expect(serviceContext({ INVOCATION_ID: 'abc', JOURNAL_STREAM: '8:1' }, 'linux', () => false)).toBeNull();
    expect(serviceContext({ INVOCATION_ID: 'abc', JOURNAL_STREAM: 'not-a-stream' }, 'linux')).toBeNull();
    expect(serviceContext({ INFERIT_SELLER_SERVICE: 'windows-task' }, 'win32')).toBe('windows-task');
    expect(serviceContext({ INFERIT_SELLER_DOCKER: '1' }, 'linux')).toBe('docker');
    expect(serviceContext({}, 'linux')).toBeNull();
  });
});

describe('macOS LaunchAgent', () => {
  it('runs node cli.js start at load, keeps it alive, and logs under ~/.inferit-seller', () => {
    const plan = servicePlan(input('darwin'));
    expect(plan.file).toBe(path.join(dir, 'home', 'Library', 'LaunchAgents', 'cc.inferit.seller.plist'));
    expect(plan.content).toContain('<key>Label</key>\n  <string>cc.inferit.seller</string>');
    expect(plan.content).toContain('<string>/opt/node 22/bin/node</string>\n    <string>/opt/inferit seller/dist/cli.js</string>\n    <string>start</string>');
    expect(plan.content).toContain('<key>RunAtLoad</key>\n  <true/>');
    expect(plan.content).toContain('<key>KeepAlive</key>\n  <true/>');
    expect(plan.content).toContain('<key>INFERIT_SELLER_SERVICE</key>\n    <string>launchd</string>');
    expect(plan.content).toContain(`<string>${path.join(dir, 'home', '.inferit-seller', 'logs', 'stderr.log')}</string>`);
    expect(plan.install.map((s) => `${s.cmd} ${s.args.join(' ')}`)).toEqual([
      'launchctl bootout gui/501/cc.inferit.seller',
      'launchctl enable gui/501/cc.inferit.seller',
      `launchctl bootstrap gui/501 ${plan.file}`,
    ]);
  });

  it('escapes XML in paths', () => {
    expect(servicePlan(input('darwin', { cliPath: '/a&b/<c>/cli.js' })).content).toContain('<string>/a&amp;b/&lt;c&gt;/cli.js</string>');
  });

  it('installs into a temp home with the recorded commands, retries bootstrap once, and uninstalls', async () => {
    const plan = servicePlan(input('darwin'));
    const r = recorder({ 'launchctl bootout': [113], 'launchctl bootstrap': [5] });
    const results = await installService(plan, r.run, { sleepMs: 1 });
    expect(fs.readFileSync(plan.file, 'utf8')).toBe(plan.content);
    expect(serviceInstalled(plan)).toBe(true);
    expect(fs.statSync(path.join(dir, 'home', '.inferit-seller', 'logs')).isDirectory()).toBe(true); // launchd needs it
    expect(r.calls).toEqual([
      'launchctl bootout gui/501/cc.inferit.seller', // not loaded yet: optional, ignored
      'launchctl enable gui/501/cc.inferit.seller',
      `launchctl bootstrap gui/501 ${plan.file}`, // fails once
      `launchctl bootstrap gui/501 ${plan.file}`,
    ]);
    expect(results.map((x) => x.ok)).toEqual([false, true, true]);
    const u = recorder();
    expect((await uninstallService(plan, u.run)).removed).toBe(true);
    expect(u.calls).toEqual(['launchctl bootout gui/501/cc.inferit.seller']);
    expect(serviceInstalled(plan)).toBe(false);
  });

  it('stops with the error when a required step keeps failing', async () => {
    const plan = servicePlan(input('darwin'));
    const r = recorder({ 'launchctl bootstrap': [5, 5, 5, 5] });
    await expect(installService(plan, r.run, { sleepMs: 1 })).rejects.toThrow(/launchctl bootstrap gui\/501 .* failed: simulated failure/);
  });
});

describe('Linux systemd user unit', () => {
  it('quotes paths, restarts always and is wanted by default.target', () => {
    const plan = servicePlan(input('linux', { nodePath: '/usr/bin/node', cliPath: '/home/jean/50%/cli.js' }));
    expect(plan.file).toBe(path.join(dir, 'home', '.config', 'systemd', 'user', 'inferit-seller.service'));
    expect(plan.content).toContain('ExecStart="/usr/bin/node" "/home/jean/50%%/cli.js" "start"');
    expect(plan.content).toContain('Environment="INFERIT_SELLER_SERVICE=systemd"');
    expect(plan.content).toContain('Restart=always');
    expect(plan.content).toContain('StartLimitIntervalSec=0');
    expect(plan.content).toContain('WantedBy=default.target');
    expect(plan.logs).toBe('journalctl --user -u inferit-seller.service -f');
    expect(servicePlan(input('linux', { xdgConfigHome: path.join(dir, 'xdg') })).file).toBe(path.join(dir, 'xdg', 'systemd', 'user', 'inferit-seller.service'));
  });

  it('enables and restarts the unit; linger is optional and its failure gives the hint', async () => {
    const plan = servicePlan(input('linux'));
    const r = recorder({ 'loginctl enable-linger': [1] });
    const results = await installService(plan, r.run);
    expect(r.calls).toEqual(['systemctl --user daemon-reload', 'systemctl --user enable inferit-seller.service', 'systemctl --user restart inferit-seller.service', 'loginctl enable-linger jean']);
    expect(results.at(-1)).toMatchObject({ ok: false, step: { hint: expect.stringContaining('sudo loginctl enable-linger') } });
    expect(fs.readFileSync(plan.file, 'utf8')).toBe(plan.content);
    const u = recorder();
    await uninstallService(plan, u.run);
    expect(u.calls).toEqual(['systemctl --user disable --now inferit-seller.service', 'systemctl --user daemon-reload']);
    expect(fs.existsSync(plan.file)).toBe(false);
  });
});

describe('Windows scheduled task', () => {
  it('runs at logon, restarts on failure, has no time limit and runs start --supervise hidden', () => {
    const plan = servicePlan(input('win32', { nodePath: 'C:\\Program Files\\nodejs\\node.exe', cliPath: "C:\\Users\\O'Neil\\.inferit-seller\\app\\cli.js", appHome: 'C:\\Users\\O\'Neil\\.inferit-seller', user: 'PC\\jean' }));
    expect(plan.file).toBe(path.join("C:\\Users\\O'Neil\\.inferit-seller", 'InferitSeller.xml'));
    expect(plan.encoding).toBe('utf16le');
    expect(plan.content).toContain('<LogonTrigger>\n      <Enabled>true</Enabled>\n      <UserId>PC\\jean</UserId>');
    expect(plan.content).toContain('<RestartOnFailure>\n      <Interval>PT1M</Interval>\n      <Count>999</Count>');
    expect(plan.content).toContain('<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>');
    expect(plan.content).toContain('<DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>');
    expect(plan.content).toContain('<Command>powershell.exe</Command>');
    expect(plan.content).toContain('-WindowStyle Hidden');
    expect(plan.content).toContain("&amp; &apos;C:\\Program Files\\nodejs\\node.exe&apos; &apos;C:\\Users\\O&apos;&apos;Neil\\.inferit-seller\\app\\cli.js&apos; start --supervise");
    expect(plan.install.map((s) => s.args.join(' '))).toEqual(['/End /TN InferitSeller', `/Create /TN InferitSeller /XML ${plan.file} /F`, '/Run /TN InferitSeller']);
  });

  it('writes the task XML as UTF-16 with a byte-order mark', async () => {
    const plan = servicePlan(input('win32', { appHome: path.join(dir, 'app') }));
    await installService(plan, recorder().run);
    const raw = fs.readFileSync(plan.file);
    expect([raw[0], raw[1]]).toEqual([0xff, 0xfe]);
    expect(raw.subarray(2).toString('utf16le')).toBe(plan.content);
  });
});

describe('a stable copy for the service', () => {
  it('copies the app out of the npx cache (dependencies included, .bin left out)', () => {
    const nm = path.join(dir, 'npm-cache', '_npx', 'abc123', 'node_modules');
    const pkg = path.join(nm, 'inferit-seller');
    fs.mkdirSync(path.join(pkg, 'dist'), { recursive: true });
    fs.writeFileSync(path.join(pkg, 'dist', 'cli.js'), '// cli');
    fs.mkdirSync(path.join(nm, 'ws'), { recursive: true });
    fs.writeFileSync(path.join(nm, 'ws', 'index.js'), '// ws');
    fs.mkdirSync(path.join(nm, '.bin'), { recursive: true });
    fs.writeFileSync(path.join(nm, '.bin', 'inferit-seller'), '#!/bin/sh');
    expect(isEphemeralInstall(pkg)).toBe(true);
    const appHome = path.join(dir, 'home', '.inferit-seller');
    const r = stableCliPath({ pkgRoot: pkg, appHome });
    expect(r).toEqual({ cliPath: path.join(appHome, 'app', 'node_modules', 'inferit-seller', 'dist', 'cli.js'), copied: true });
    expect(fs.readFileSync(r.cliPath, 'utf8')).toBe('// cli');
    expect(fs.existsSync(path.join(appHome, 'app', 'node_modules', 'ws', 'index.js'))).toBe(true);
    expect(fs.existsSync(path.join(appHome, 'app', 'node_modules', '.bin'))).toBe(false);
    // Again (an upgrade): replaced in place.
    fs.writeFileSync(path.join(pkg, 'dist', 'cli.js'), '// cli v2');
    expect(fs.readFileSync(stableCliPath({ pkgRoot: pkg, appHome }).cliPath, 'utf8')).toBe('// cli v2');
  });

  it('uses a global install where it is, and needs a build', () => {
    const pkg = path.join(dir, 'usr', 'lib', 'node_modules', 'inferit-seller');
    fs.mkdirSync(pkg, { recursive: true });
    expect(isEphemeralInstall(pkg, path.join(dir, 'elsewhere'))).toBe(false);
    expect(() => stableCliPath({ pkgRoot: pkg, appHome: path.join(dir, 'h'), tmpdir: path.join(dir, 'elsewhere') })).toThrow(/build the app first/);
    fs.mkdirSync(path.join(pkg, 'dist'));
    fs.writeFileSync(path.join(pkg, 'dist', 'cli.js'), '');
    expect(stableCliPath({ pkgRoot: pkg, appHome: path.join(dir, 'h'), tmpdir: path.join(dir, 'elsewhere') })).toEqual({ cliPath: path.join(pkg, 'dist', 'cli.js'), copied: false });
  });
});
