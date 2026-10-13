// Runs the app in the background, started at login and restarted when it exits:
//
//   macOS    a LaunchAgent, ~/Library/LaunchAgents/cc.inferit.seller.plist (RunAtLoad, KeepAlive), loaded with
//            launchctl bootstrap gui/<uid>; logs in ~/.inferit-seller/logs
//   Linux    a systemd user unit, ~/.config/systemd/user/inferit-seller.service (Restart=always), enabled with
//            systemctl --user; loginctl enable-linger lets it start at boot without a login; logs in the journal
//   Windows  a Task Scheduler task "InferitSeller" at logon (restart on failure), running `start --supervise`
//            (a small parent that restarts the app when it exits); logs in %USERPROFILE%\.inferit-seller\logs
//
// The service runs `node <cli.js> start`. When the app runs from the npx cache (which npm may clean at any time), it
// is first copied to ~/.inferit-seller/app so the service does not depend on that cache.
// The file generators are pure; installing takes a command runner, so tests never touch launchctl or systemctl.
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const SERVICE_LABEL = 'cc.inferit.seller';
export const SYSTEMD_UNIT = 'inferit-seller.service';
export const WINDOWS_TASK = 'InferitSeller';

export type ServicePlatform = 'darwin' | 'linux' | 'win32';
export type ServiceContext = 'launchd' | 'systemd' | 'windows-task' | 'docker';

/** Whether stdout or stderr is the journal stream JOURNAL_STREAM names ("<dev>:<inode>"). systemd's documented check:
 *  the variable is inherited, so a shell in a terminal that a systemd user service started (GNOME's) has it too. */
export function stdioOnJournal(journalStream: string): boolean {
  const m = /^(\d+):(\d+)$/.exec(journalStream.trim());
  if (!m) return false;
  for (const fd of [1, 2]) {
    try {
      const st = fs.fstatSync(fd);
      if (String(st.dev) === m[1] && String(st.ino) === m[2]) return true;
    } catch {
      /* not open */
    }
  }
  return false;
}

/** How this process was started: by a service manager, in a container, or by hand (null). The units this app writes
 *  set INFERIT_SELLER_SERVICE; the other signs cover a unit or plist written by hand. */
export function serviceContext(
  env: NodeJS.ProcessEnv = process.env,
  platform: NodeJS.Platform = process.platform,
  onJournal: (journalStream: string) => boolean = stdioOnJournal,
): ServiceContext | null {
  const explicit = env.INFERIT_SELLER_SERVICE?.trim();
  if (explicit === 'launchd' || explicit === 'systemd' || explicit === 'windows-task' || explicit === 'docker') return explicit;
  if (env.INFERIT_SELLER_DOCKER === '1') return 'docker';
  if (platform === 'darwin' && env.XPC_SERVICE_NAME === SERVICE_LABEL) return 'launchd';
  if (platform === 'linux' && env.INVOCATION_ID && env.JOURNAL_STREAM && onJournal(env.JOURNAL_STREAM)) return 'systemd';
  return null;
}

export interface ServiceInput {
  platform: ServicePlatform;
  /** The user's home directory. */
  userHome: string;
  /** The app's directory (~/.inferit-seller). */
  appHome: string;
  /** Absolute path of the node binary. */
  nodePath: string;
  /** Absolute path of dist/cli.js. */
  cliPath: string;
  /** For launchctl's gui/<uid> domain. */
  uid?: number;
  /** For loginctl enable-linger and the Windows task principal (DOMAIN\user). */
  user?: string;
  /** XDG_CONFIG_HOME, when set. */
  xdgConfigHome?: string;
}

export interface Step {
  cmd: string;
  args: string[];
  /** A failure is reported but does not stop the install. */
  optional?: boolean;
  /** Retries after a failure (1 s apart): launchctl bootstrap right after bootout can fail once. */
  retries?: number;
  /** Shown when an optional step fails. */
  hint?: string;
}

export interface ServicePlan {
  platform: ServicePlatform;
  /** The file the service is defined in. */
  file: string;
  content: string;
  /** 'utf16le' for the Windows task XML (schtasks /XML wants UTF-16). */
  encoding: 'utf8' | 'utf16le';
  install: Step[];
  uninstall: Step[];
  /** Where to read its logs. */
  logs: string;
  /** Directories that must exist before the service starts (launchd does not create its log directory). */
  dirs: string[];
}

const xml = (s: string) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&apos;');
/** One systemd ExecStart / Environment argument, double-quoted. */
const sd = (s: string) => `"${s.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/%/g, '%%')}"`;
/** A PowerShell single-quoted string. */
const ps = (s: string) => `'${s.replace(/'/g, "''")}'`;

export function launchdPlist(i: ServiceInput): string {
  const logs = path.join(i.appHome, 'logs');
  const args = [i.nodePath, i.cliPath, 'start'].map((a) => `    <string>${xml(a)}</string>`).join('\n');
  return `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${SERVICE_LABEL}</string>
  <key>ProgramArguments</key>
  <array>
${args}
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>INFERIT_SELLER_SERVICE</key>
    <string>launchd</string>
    <key>INFERIT_SELLER_HOME</key>
    <string>${xml(i.appHome)}</string>
  </dict>
  <key>WorkingDirectory</key>
  <string>${xml(i.appHome)}</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>10</integer>
  <key>StandardOutPath</key>
  <string>${xml(path.join(logs, 'stdout.log'))}</string>
  <key>StandardErrorPath</key>
  <string>${xml(path.join(logs, 'stderr.log'))}</string>
</dict>
</plist>
`;
}

export function systemdUnit(i: ServiceInput): string {
  return `[Unit]
Description=Inferit seller: sells this machine's inference on Inferit
Documentation=https://github.com/InferitAI/inferit-seller
StartLimitIntervalSec=0

[Service]
Type=simple
ExecStart=${[i.nodePath, i.cliPath, 'start'].map(sd).join(' ')}
Environment=${sd('INFERIT_SELLER_SERVICE=systemd')}
Environment=${sd(`INFERIT_SELLER_HOME=${i.appHome}`)}
WorkingDirectory=${sd(i.appHome)}
Restart=always
RestartSec=10
KillSignal=SIGTERM
TimeoutStopSec=15

[Install]
WantedBy=default.target
`;
}

export function windowsTaskXml(i: ServiceInput): string {
  const command = [
    `$env:INFERIT_SELLER_SERVICE=${ps('windows-task')}`,
    `$env:INFERIT_SELLER_HOME=${ps(i.appHome)}`,
    `& ${ps(i.nodePath)} ${ps(i.cliPath)} start --supervise`,
  ].join('; ');
  const args = `-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -Command "${command.replace(/"/g, '\\"')}"`;
  const user = i.user ? `\n      <UserId>${xml(i.user)}</UserId>` : '';
  return `<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>Inferit seller: sells this machine's inference on Inferit. https://github.com/InferitAI/inferit-seller</Description>
    <URI>\\${WINDOWS_TASK}</URI>
  </RegistrationInfo>
  <Triggers>
    <LogonTrigger>
      <Enabled>true</Enabled>${user}
    </LogonTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">${user.replace('\n      ', '\n      ')}
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <IdleSettings>
      <StopOnIdleEnd>false</StopOnIdleEnd>
      <RestartOnIdle>false</RestartOnIdle>
    </IdleSettings>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <WakeToRun>false</WakeToRun>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <Priority>7</Priority>
    <RestartOnFailure>
      <Interval>PT1M</Interval>
      <Count>999</Count>
    </RestartOnFailure>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>powershell.exe</Command>
      <Arguments>${xml(args)}</Arguments>
      <WorkingDirectory>${xml(i.appHome)}</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
`;
}

/** The service file, its content and the commands that install and remove it, for one platform. */
export function servicePlan(i: ServiceInput): ServicePlan {
  if (i.platform === 'darwin') {
    const file = path.join(i.userHome, 'Library', 'LaunchAgents', `${SERVICE_LABEL}.plist`);
    const domain = `gui/${i.uid ?? 501}`;
    return {
      platform: 'darwin',
      file,
      content: launchdPlist(i),
      encoding: 'utf8',
      install: [
        { cmd: 'launchctl', args: ['bootout', `${domain}/${SERVICE_LABEL}`], optional: true },
        { cmd: 'launchctl', args: ['enable', `${domain}/${SERVICE_LABEL}`], optional: true },
        { cmd: 'launchctl', args: ['bootstrap', domain, file], retries: 3 },
      ],
      uninstall: [{ cmd: 'launchctl', args: ['bootout', `${domain}/${SERVICE_LABEL}`], optional: true }],
      logs: path.join(i.appHome, 'logs', 'seller.log'),
      dirs: [path.join(i.appHome, 'logs')],
    };
  }
  if (i.platform === 'linux') {
    const file = path.join(i.xdgConfigHome || path.join(i.userHome, '.config'), 'systemd', 'user', SYSTEMD_UNIT);
    return {
      platform: 'linux',
      file,
      content: systemdUnit(i),
      encoding: 'utf8',
      install: [
        { cmd: 'systemctl', args: ['--user', 'daemon-reload'] },
        { cmd: 'systemctl', args: ['--user', 'enable', SYSTEMD_UNIT] },
        { cmd: 'systemctl', args: ['--user', 'restart', SYSTEMD_UNIT] },
        {
          cmd: 'loginctl',
          args: ['enable-linger', ...(i.user ? [i.user] : [])],
          optional: true,
          hint: 'Without linger the app starts when you log in, not at boot. To start it at boot, run: sudo loginctl enable-linger $USER',
        },
      ],
      uninstall: [
        { cmd: 'systemctl', args: ['--user', 'disable', '--now', SYSTEMD_UNIT], optional: true },
        { cmd: 'systemctl', args: ['--user', 'daemon-reload'], optional: true },
      ],
      logs: `journalctl --user -u ${SYSTEMD_UNIT} -f`,
      dirs: [i.appHome],
    };
  }
  const file = path.join(i.appHome, `${WINDOWS_TASK}.xml`);
  return {
    platform: 'win32',
    file,
    content: windowsTaskXml(i),
    encoding: 'utf16le',
    install: [
      { cmd: 'schtasks', args: ['/End', '/TN', WINDOWS_TASK], optional: true },
      { cmd: 'schtasks', args: ['/Create', '/TN', WINDOWS_TASK, '/XML', file, '/F'] },
      { cmd: 'schtasks', args: ['/Run', '/TN', WINDOWS_TASK] },
    ],
    uninstall: [
      { cmd: 'schtasks', args: ['/End', '/TN', WINDOWS_TASK], optional: true },
      { cmd: 'schtasks', args: ['/Delete', '/TN', WINDOWS_TASK, '/F'], optional: true },
    ],
    logs: path.join(i.appHome, 'logs', 'seller.log'),
    dirs: [path.join(i.appHome, 'logs')],
  };
}

export type Runner = (cmd: string, args: string[]) => Promise<{ code: number; stdout: string; stderr: string }>;

/** Runs a command without a shell. */
export const realRunner: Runner = (cmd, args) =>
  new Promise((resolve) => {
    const child = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (d: Buffer) => (stdout += d.toString()));
    child.stderr.on('data', (d: Buffer) => (stderr += d.toString()));
    child.on('error', (e) => resolve({ code: 127, stdout, stderr: e.message }));
    child.on('close', (code) => resolve({ code: code ?? 1, stdout, stderr }));
  });

export interface StepResult {
  step: Step;
  ok: boolean;
  output: string;
}

async function runSteps(steps: Step[], run: Runner, sleepMs = 1_000): Promise<StepResult[]> {
  const out: StepResult[] = [];
  for (const step of steps) {
    let r = await run(step.cmd, step.args);
    for (let n = 0; r.code !== 0 && n < (step.retries ?? 0); n++) {
      await new Promise((res) => setTimeout(res, sleepMs));
      r = await run(step.cmd, step.args);
    }
    const res: StepResult = { step, ok: r.code === 0, output: `${r.stdout}${r.stderr}`.trim().slice(0, 500) };
    out.push(res);
    if (!res.ok && !step.optional) throw Object.assign(new Error(`${step.cmd} ${step.args.join(' ')} failed: ${res.output || `exit ${r.code}`}`), { results: out });
  }
  return out;
}

/** Writes the service file and runs the install steps. `dryRun` only returns the plan. */
export async function installService(plan: ServicePlan, run: Runner, o: { dryRun?: boolean; sleepMs?: number } = {}): Promise<StepResult[]> {
  if (o.dryRun) return [];
  for (const d of plan.dirs) fs.mkdirSync(d, { recursive: true, mode: 0o700 });
  fs.mkdirSync(path.dirname(plan.file), { recursive: true });
  const body = plan.encoding === 'utf16le' ? Buffer.concat([Buffer.from([0xff, 0xfe]), Buffer.from(plan.content, 'utf16le')]) : Buffer.from(plan.content, 'utf8');
  fs.writeFileSync(plan.file, body, { mode: 0o644 });
  return runSteps(plan.install, run, o.sleepMs);
}

/** Runs the uninstall steps and removes the service file. */
export async function uninstallService(plan: ServicePlan, run: Runner, o: { sleepMs?: number } = {}): Promise<{ results: StepResult[]; removed: boolean }> {
  const results = await runSteps(plan.uninstall, run, o.sleepMs);
  let removed = false;
  try {
    fs.unlinkSync(plan.file);
    removed = true;
  } catch {
    /* not installed */
  }
  return { results, removed };
}

export function serviceInstalled(plan: ServicePlan): boolean {
  return fs.existsSync(plan.file);
}

const unXml = (s: string) => s.replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&apos;/g, "'").replace(/&amp;/g, '&');

/** The app folder (INFERIT_SELLER_HOME) a service file this app wrote runs, or null when the file names none. */
export function serviceHomeOf(platform: ServicePlatform, content: string): string | null {
  if (platform === 'darwin') {
    const m = /<key>INFERIT_SELLER_HOME<\/key>\s*<string>([^<]*)<\/string>/.exec(content);
    return m ? unXml(m[1]!) : null;
  }
  if (platform === 'linux') {
    const m = /^Environment="INFERIT_SELLER_HOME=((?:[^"\\]|\\.)*)"\s*$/m.exec(content);
    return m ? m[1]!.replace(/\\(.)/g, '$1').replace(/%%/g, '%') : null;
  }
  return null;
}

export interface ServiceOwner {
  /** A service file exists. */
  installed: boolean;
  /** The app folder it runs, when it says. */
  home: string | null;
  /** It runs this app folder (or names none, as a hand-written one may). */
  ours: boolean;
}

/**
 * Whether the per-user service (one per account: the LaunchAgent, the systemd user unit) runs this app folder. With
 * INFERIT_SELLER_HOME set to another folder, the installed service still belongs to the default one: status and
 * doctor must not report it as this folder's, and install-service or uninstall-service here would replace or remove
 * it. On Windows the task's file lives in the app folder itself.
 */
export function serviceOwner(plan: ServicePlan, appHome: string): ServiceOwner {
  let content: string;
  try {
    const raw = fs.readFileSync(plan.file);
    content = plan.encoding === 'utf16le' ? raw.toString('utf16le') : raw.toString('utf8');
  } catch {
    return { installed: false, home: null, ours: false };
  }
  const home = serviceHomeOf(plan.platform, content);
  return { installed: true, home, ours: home === null || path.resolve(home) === path.resolve(appHome) };
}

/** The package's root directory (one level above src/ and dist/). */
export function packageRoot(): string {
  return path.dirname(path.dirname(fileURLToPath(import.meta.url)));
}

/** Whether `dir` is somewhere npm may delete: the npx cache or a temporary directory. */
export function isEphemeralInstall(dir: string, tmpdir: string = os.tmpdir()): boolean {
  const parts = path.resolve(dir).split(/[\\/]+/);
  if (parts.includes('_npx')) return true;
  const real = (p: string) => {
    try {
      return fs.realpathSync(p);
    } catch {
      return path.resolve(p);
    }
  };
  const rel = path.relative(real(tmpdir), real(dir));
  return rel !== '' && !rel.startsWith('..') && !path.isAbsolute(rel);
}

/**
 * The cli.js the service should run. From the npx cache (or a temporary directory) the package and its dependencies
 * (the node_modules directory it was installed in) are copied to <appHome>/app/node_modules first.
 */
export function stableCliPath(o: { pkgRoot: string; appHome: string; tmpdir?: string }): { cliPath: string; copied: boolean } {
  const cli = (root: string) => path.join(root, 'dist', 'cli.js');
  if (!isEphemeralInstall(o.pkgRoot, o.tmpdir)) {
    if (!fs.existsSync(cli(o.pkgRoot))) throw new Error(`${cli(o.pkgRoot)} is missing: build the app first (npm run build)`);
    return { cliPath: cli(o.pkgRoot), copied: false };
  }
  const nm = path.dirname(o.pkgRoot);
  if (path.basename(nm) !== 'node_modules') throw new Error(`cannot copy the app out of ${o.pkgRoot}: install it with npm i -g inferit-seller and run install-service again`);
  const appDir = path.join(o.appHome, 'app');
  const target = path.join(appDir, 'node_modules');
  const staging = path.join(appDir, `node_modules.new-${process.pid}`);
  fs.rmSync(staging, { recursive: true, force: true });
  fs.mkdirSync(appDir, { recursive: true, mode: 0o700 });
  fs.cpSync(nm, staging, { recursive: true, dereference: true, filter: (src) => path.basename(src) !== '.bin' });
  fs.rmSync(target, { recursive: true, force: true });
  fs.renameSync(staging, target);
  return { cliPath: cli(path.join(target, path.basename(o.pkgRoot))), copied: true };
}
