// `status` and `doctor`: what this machine would sell, whether the app runs and is connected, and what to fix.
// Both work without a token. Doctor runs every check and prints a plain-English fix for each problem; its exit code
// is 1 when something must be fixed before the machine can sell.
import WebSocket from 'ws';
import { mapModels, type CatalogEntry, type ModelMapping } from './catalog.js';
import { AUTH_RETRY_MS, CONNECTOR_SUBPROTOCOL, NODE_TOKEN_RE } from './connector.js';
import { detectLocal, KNOWN_SERVERS, SERVER_LABEL, type LocalServer, type Probe } from './detect.js';
import { serviceOwner, servicePlan, type ServicePlan, type ServicePlatform } from './service.js';
import { detectHosts, loadCatalog, resolveTarget, SELL_URL } from './start.js';
import { configPath, isWorldReadable, liveState, loadConfig, readState, tokenPrefix, type RunState, type SavedConfig } from './userconfig.js';
import { PACKAGE_VERSION } from './version.js';

export type TokenCheck =
  | { ok: true }
  | { ok: false; reachable: true; httpStatus: number; code: string | null; message: string | null }
  | { ok: false; reachable: false; error: string };

/**
 * Asks the market whether it accepts a node token: opens the connector socket and closes it at once, without a
 * hello (so a running session for the same token is not replaced). The upgrade is where the market checks the token.
 */
export function checkToken(connectUrl: string, token: string, timeoutMs = 10_000): Promise<TokenCheck> {
  return new Promise((resolve) => {
    let done = false;
    const finish = (r: TokenCheck, kill = true) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      if (kill) {
        try {
          ws.terminate();
        } catch {
          /* closed */
        }
      }
      resolve(r);
    };
    const ws = new WebSocket(connectUrl, [CONNECTOR_SUBPROTOCOL], {
      headers: { authorization: `Bearer ${token}` },
      handshakeTimeout: timeoutMs,
      perMessageDeflate: false,
      followRedirects: false,
    });
    const timer = setTimeout(() => finish({ ok: false, reachable: false, error: `no answer within ${Math.round(timeoutMs / 1000)} s` }), timeoutMs + 1_000);
    ws.on('open', () => {
      finish({ ok: true }, false);
      ws.close(1000, 'token check');
      setTimeout(() => ws.terminate(), 2_000).unref();
    });
    ws.on('unexpected-response', (_req, res) => {
      const chunks: Buffer[] = [];
      res.on('data', (c: Buffer) => chunks.push(c));
      const end = () => {
        let code: string | null = null;
        let message: string | null = null;
        try {
          const b = JSON.parse(Buffer.concat(chunks).toString('utf8')) as { error?: { code?: unknown; message?: unknown } };
          if (typeof b.error?.code === 'string') code = b.error.code;
          if (typeof b.error?.message === 'string') message = b.error.message.slice(0, 300);
        } catch {
          /* no JSON body */
        }
        finish({ ok: false, reachable: true, httpStatus: res.statusCode ?? 0, code, message });
      };
      res.on('end', end);
      res.on('error', end);
    });
    ws.on('error', (err: Error & { code?: string }) => {
      if (done) return;
      finish({ ok: false, reachable: false, error: err.code ? `${err.code} ${err.message}`.slice(0, 200) : err.message.slice(0, 200) });
    });
  });
}

/** Plain English for a refused token. */
export function tokenFix(c: Extract<TokenCheck, { reachable: true }>): { problem: string; fix: string; fatal: boolean } {
  const said = c.message ? ` The market says: "${c.message}"` : '';
  switch (c.code) {
    case 'seller_not_registered':
      return { problem: `The token is valid, but this account is not registered as a seller yet.${said}`, fix: `Finish seller registration in the Sell workspace (${SELL_URL}). The app connects on its own once it is done.`, fatal: false };
    case 'node_revoked':
      return { problem: `This token was revoked.${said}`, fix: `In the Sell workspace (${SELL_URL}), add the machine again and run the new connect command.`, fatal: true };
    case 'invalid_node_token':
      return { problem: `The market does not know this token.${said}`, fix: `Copy the whole command again from the Add a machine step in the Sell workspace (${SELL_URL}). Check that --api names the same market.`, fatal: true };
    case 'tls_required':
      return { problem: `The market only accepts wss:// connections.${said}`, fix: 'Use an https:// address with --api.', fatal: true };
    default:
      return { problem: `The market refused the connection (HTTP ${c.httpStatus}${c.code ? `, ${c.code}` : ''}).${said}`, fix: c.httpStatus === 429 ? 'Wait a minute and try again.' : 'Run doctor again in a minute; if it persists, add the machine again in the Sell workspace.', fatal: c.httpStatus === 401 || c.httpStatus === 403 };
  }
}

export type CheckStatus = 'ok' | 'warn' | 'fail' | 'info';
export interface Check {
  id: string;
  status: CheckStatus;
  title: string;
  detail: string;
  fix?: string;
}

export interface ReportInput {
  env: NodeJS.ProcessEnv;
  appHome: string;
  platform: NodeJS.Platform;
  userHome: string;
  fetchImpl?: typeof fetch;
  /** For the token check (tests stub it). */
  checkTokenImpl?: typeof checkToken;
}

export interface Snapshot {
  version: string;
  configFile: string;
  config: SavedConfig | null;
  configError: string | null;
  target: ReturnType<typeof resolveTarget>;
  servers: LocalServer[];
  probes: Probe[];
  catalog: CatalogEntry[] | null;
  catalogCached: boolean;
  catalogError: string | null;
  mapping: ModelMapping | null;
  state: RunState | null;
  running: boolean;
  /** `installed`: a service runs this app folder. `otherHome`: the installed service runs another one (the per-user
   *  service is shared, so with INFERIT_SELLER_HOME set it may belong to the default folder). */
  service: { platform: ServicePlatform; file: string; installed: boolean; otherHome: string | null; logs: string } | null;
}

const inDocker = (i: ReportInput) => i.env.INFERIT_SELLER_DOCKER === '1';

function planFor(i: ReportInput): ServicePlan | null {
  if (inDocker(i)) return null; // the container runtime restarts it (--restart unless-stopped)
  if (i.platform !== 'darwin' && i.platform !== 'linux' && i.platform !== 'win32') return null;
  return servicePlan({ platform: i.platform, userHome: i.userHome, appHome: i.appHome, nodePath: process.execPath, cliPath: 'cli.js', ...(i.env.XDG_CONFIG_HOME ? { xdgConfigHome: i.env.XDG_CONFIG_HOME } : {}) });
}

/** Everything status and doctor show, gathered once. */
export async function snapshot(i: ReportInput): Promise<Snapshot> {
  let config: SavedConfig | null = null;
  let configError: string | null = null;
  try {
    config = loadConfig(i.appHome);
  } catch (e) {
    configError = (e as Error).message;
  }
  let target: ReturnType<typeof resolveTarget>;
  try {
    target = resolveTarget(i.env, config);
  } catch (e) {
    configError ??= (e as Error).message;
    target = resolveTarget({ ...i.env, INFERIT_API_URL: '' }, null);
  }
  const apiKey = i.env.UPSTREAM_API_KEY?.trim();
  let servers: LocalServer[] = [];
  let probes: Probe[] = [];
  try {
    ({ servers, probes } = await detectLocal({
      hosts: detectHosts(i.env, inDocker(i) ? 'docker' : null),
      ...(target.upstream ? { upstream: target.upstream } : {}),
      ...(i.fetchImpl ? { fetchImpl: i.fetchImpl } : {}),
      ...(apiKey ? { apiKey } : {}),
    }));
  } catch (e) {
    probes = [{ kind: 'custom', url: target.upstream ?? '', ok: false, error: (e as Error).message }];
  }
  let catalog: CatalogEntry[] | null = null;
  let catalogCached = false;
  let catalogError: string | null = null;
  try {
    ({ catalog, cached: catalogCached } = await loadCatalog(target.apiUrl, i.appHome, i.fetchImpl));
  } catch (e) {
    catalogError = (e as Error).message;
  }
  const mapping = catalog ? mapModels(servers.flatMap((s) => s.models), catalog) : null;
  const state = readState(i.appHome);
  const plan = planFor(i);
  const owner = plan ? serviceOwner(plan, i.appHome) : null;
  return {
    version: PACKAGE_VERSION,
    configFile: configPath(i.appHome),
    config,
    configError,
    target,
    servers,
    probes,
    catalog,
    catalogCached,
    catalogError,
    mapping,
    state,
    running: liveState(state),
    service:
      plan && owner
        ? { platform: plan.platform, file: plan.file, installed: owner.installed && owner.ours, otherHome: owner.installed && !owner.ours ? owner.home : null, logs: plan.logs }
        : null,
  };
}

const gb = (n: number) => `${(n / 1e9).toFixed(1)} GB`;

function hostsText(i: ReportInput): string {
  const hosts = detectHosts(i.env, inDocker(i) ? 'docker' : null);
  return hosts.length === 1 && hosts[0] === '127.0.0.1' ? 'this machine' : hosts.join(', ');
}

function inWords(ms: number): string {
  const s = Math.max(0, Math.round(ms / 1000));
  return s < 90 ? `${s} s` : `${Math.round(s / 60)} min`;
}

/** "the market refused the token; next try in 4 min" / "next try in 12 s": when a running app tries again. */
function retryText(st: RunState, now = Date.now()): string | null {
  if (st.phase === 'live' || !st.nextAttemptAt) return null;
  const left = Date.parse(st.nextAttemptAt) - now;
  const when = left > 0 ? `next try in ${inWords(left)}` : 'trying now';
  return st.retryReason === 'auth' ? `the market refused the token; it tries again every ${inWords(AUTH_RETRY_MS)} (${when})` : when;
}

function ago(iso: string | null): string {
  if (!iso) return '';
  const s = Math.max(0, Math.round((Date.now() - Date.parse(iso)) / 1000));
  if (s < 90) return `${s} s ago`;
  if (s < 5400) return `${Math.round(s / 60)} min ago`;
  return `${Math.round(s / 3600)} h ago`;
}

/** The `status` text. */
export function formatStatus(s: Snapshot): string {
  const L: string[] = [];
  L.push(`inferit-seller ${s.version}`);
  L.push('');
  L.push(`Config     ${s.configFile}${s.config ? '' : s.configError ? `  (${s.configError})` : '  (none yet)'}`);
  L.push(`  Token    ${s.target.token ? tokenPrefix(s.target.token) : 'none: run connect --token <icn_…> from the Add a machine step'}`);
  L.push(`  Market   ${s.target.apiUrl}`);
  L.push(`  Name     ${s.target.nodeName}`);
  if (s.target.upstream) L.push(`  Server   ${s.target.upstream} (set by you)`);
  if (s.service) {
    L.push(
      `Service    ${
        s.service.installed
          ? `installed (${s.service.file})`
          : s.service.otherHome
            ? `not installed for this folder: ${s.service.file} runs ${s.service.otherHome}`
            : 'not installed (run install-service to start it at login)'
      }`,
    );
  }
  const st = s.state;
  if (s.running && st) {
    const conn =
      st.phase === 'live'
        ? `connected to the market since ${ago(st.liveSince)}${st.offers !== null ? `, offers on the market for this machine: ${st.offers}` : ''}`
        : st.phase === 'detecting'
          ? `waiting for a local model: ${st.message ?? ''}`
          : `not connected${st.lastFailure ? `: ${st.lastFailure} (${ago(st.lastFailureAt)})` : ''}${retryText(st) ? `; ${retryText(st)}` : st.lastFailure ? '' : ', connecting'}`;
    L.push(`Running    yes (pid ${st.pid}${st.service ? `, ${st.service}` : ''}), ${conn}`);
  } else {
    L.push(`Running    no${st?.message ? ` (last run: ${st.message})` : ''}`);
  }
  L.push('');
  L.push('Local servers');
  if (s.target.upstream) {
    for (const p of s.probes) L.push(`  ${SERVER_LABEL[p.kind].padEnd(26)} ${p.url}  ${p.ok ? `${p.models} model${p.models === 1 ? '' : 's'}` : p.error}`);
  } else {
    for (const k of KNOWN_SERVERS) {
      const p = s.probes.find((x) => x.kind === k.kind && x.ok) ?? s.probes.find((x) => new URL(x.url).port === String(k.port));
      if (!p) continue;
      const label = p.ok ? SERVER_LABEL[p.kind] : SERVER_LABEL[k.kind];
      L.push(`  ${label.padEnd(26)} ${p.url}  ${p.ok ? `${p.models} model${p.models === 1 ? '' : 's'}` : p.error}`);
    }
  }
  L.push('');
  L.push('Models');
  if (!s.mapping) {
    L.push(`  cannot map to the catalog: ${s.catalogError}`);
    for (const m of s.servers.flatMap((x) => x.models)) L.push(`  ${m.name}`);
  } else if (s.mapping.detected.length === 0) {
    L.push('  none found');
  } else {
    const served = new Set(s.mapping.served.map((x) => `${x.local}\u0000${x.catalogId}`));
    for (const d of s.mapping.detected) {
      const facts = [SERVER_LABEL[d.server], d.quantization, d.sizeBytes ? gb(d.sizeBytes) : null].filter(Boolean).join(', ');
      const where = d.model ? `-> ${d.model}${served.has(`${d.local}\u0000${d.model}`) ? '' : ' (another local copy is served)'}` : 'not in the catalog';
      L.push(`  ${d.local}  (${facts})  ${where}`);
    }
    for (const n of s.mapping.notes) L.push(`  note: ${n}`);
  }
  if (s.catalogCached) L.push('  (catalog from the last saved copy: the market did not answer)');
  return L.join('\n');
}

/** Status as JSON (no token, only its prefix). */
export function statusJson(s: Snapshot): Record<string, unknown> {
  return {
    version: s.version,
    configFile: s.configFile,
    configError: s.configError,
    token: s.target.token ? tokenPrefix(s.target.token) : null,
    apiUrl: s.target.apiUrl,
    connectUrl: s.target.connectUrl ?? null,
    nodeName: s.target.nodeName,
    upstream: s.target.upstream ?? null,
    service: s.service,
    running: s.running,
    state: s.running ? s.state : null,
    servers: s.servers.map((x) => ({ kind: x.kind, baseUrl: x.baseUrl, version: x.version ?? null, models: x.models.length })),
    probes: s.probes,
    catalogError: s.catalogError,
    served: s.mapping?.served ?? [],
    detected: s.mapping?.detected ?? [],
    notes: s.mapping?.notes ?? [],
  };
}

/** Ollama library tags in the catalog (aliases with a ":"), as `ollama pull` suggestions. */
function ollamaSuggestions(catalog: CatalogEntry[] | null): string[] {
  const out: string[] = [];
  for (const c of catalog ?? []) for (const a of c.aliases) if (/^[a-z0-9.]+(?:-[a-z0-9.]+)*:[a-z0-9.-]+$/.test(a) && !out.includes(a)) out.push(a);
  return out.slice(0, 3);
}

/**
 * The context checks: what the local server serves each model with, against the context limit of the machine's
 * listings (from the running app's last welcome or `offers` frame) and the catalog's window. A warning only when a
 * listing accepts longer prompts than the server serves (the server would cut them); a server below the catalog's
 * window is information, since a listing whose limit is within what is served is correct.
 */
export function contextChecks(s: Snapshot): Check[] {
  const out: Check[] = [];
  if (!s.mapping) return out;
  const st = s.running ? s.state : null;
  const listings = st?.phase === 'live' ? (st.offerList ?? null) : null;
  for (const x of s.mapping.served) {
    const cat = s.catalog?.find((c) => c.id === x.catalogId)?.contextLength ?? null;
    // Served now, else the last value the running app saw (Ollama says it only while the model is loaded).
    const ctx = x.contextLength ?? st?.models?.find((m) => m.id === x.catalogId && m.local.toLowerCase() === x.local.toLowerCase())?.contextLength ?? null;
    if (!ctx) continue;
    const label = SERVER_LABEL[x.server];
    const raise = (to: number | null) =>
      x.server === 'ollama'
        ? `set OLLAMA_CONTEXT_LENGTH${to ? `=${to}` : ''} (as much as this machine's memory allows) for the Ollama server and restart Ollama`
        : `load the model with a longer context${to ? ` (up to ${to})` : ''} if memory allows`;
    const mine = listings?.filter((o) => o.model.toLowerCase() === x.catalogId.toLowerCase() || o.model.toLowerCase() === x.local.toLowerCase()) ?? [];
    // Each listing's limit: its own, else the catalog's window; undefined when the market did not say (an older API).
    const limits = mine.map((o) => (o.maxContextTokens === undefined ? undefined : (o.maxContextTokens ?? cat)));
    if (mine.length > 0 && limits.every((l): l is number => typeof l === 'number')) {
      const top = Math.max(...limits);
      if (top > ctx) {
        out.push({
          id: 'context',
          status: 'warn',
          title: 'Context',
          detail: `${label} serves ${x.local} with ${ctx} tokens of context, but your listing for ${x.catalogId} accepts prompts up to ${top} tokens. The server would cut anything longer than ${ctx}.`,
          fix: `In the Sell workspace (${SELL_URL}), under Offers, choose Edit on that offer and set its context limit to at most ${ctx}. Or ${raise(top)}.`,
        });
      } else {
        out.push({
          id: 'context',
          status: 'ok',
          title: 'Context',
          detail: `${label} serves ${x.local} with ${ctx} tokens of context; your listing's limit (${top}) is within it${cat && ctx < cat ? ` (the catalog lists ${cat}: to sell longer prompts, ${raise(cat)}, then raise the listing's limit under Offers, Edit)` : ''}.`,
        });
      }
      continue;
    }
    if (cat && ctx < cat) {
      out.push({
        id: 'context',
        status: 'info',
        title: 'Context',
        detail: `${label} serves ${x.local} with ${ctx} tokens of context; the catalog lists ${cat}. A listing's context limit must be at most ${ctx} (the Sell workspace's form fills it in).`,
        fix: `To sell longer prompts, ${raise(cat)}; then the listing's limit can go up to what it serves.`,
      });
    }
  }
  return out;
}

/** Runs every doctor check. */
export async function doctor(i: ReportInput, s?: Snapshot): Promise<{ checks: Check[]; ok: boolean; snapshot: Snapshot }> {
  s ??= await snapshot(i);
  const checks: Check[] = [];
  const add = (c: Check) => checks.push(c);

  // Config file.
  if (s.configError) add({ id: 'config', status: 'fail', title: 'Config', detail: s.configError, fix: 'Run the connect command from the Add a machine step again; it rewrites the file.' });
  else if (s.config && isWorldReadable(s.configFile)) add({ id: 'config', status: 'warn', title: 'Config', detail: `${s.configFile} can be read by other accounts on this computer.`, fix: `chmod 600 "${s.configFile}"` });

  // Local server.
  const local = s.servers.flatMap((x) => x.models);
  if (s.servers.length === 0) {
    add({
      id: 'server',
      status: 'fail',
      title: 'Local server',
      detail: s.target.upstream
        ? `${s.target.upstream} does not answer with a model list (${s.probes[0]?.error ?? 'no answer'}).`
        : `No local inference server answers on ${hostsText(i)} (Ollama :11434, LM Studio :1234, vLLM :8000, llama.cpp :8080).`,
      fix: s.target.upstream
        ? 'Start that server, or remove --upstream to let the app find Ollama or LM Studio.'
        : inDocker(i)
          ? 'Start the model server on the host. On Linux, where Ollama listens on 127.0.0.1 only, run the container with --network host and -e UPSTREAM_BASE_URL=http://127.0.0.1:11434/v1 (instead of --add-host), so Ollama stays closed to the network.'
          : 'Install Ollama (https://ollama.com) and start it, or start the local server in LM Studio (Developer tab, Start Server). Then run doctor again.',
    });
  } else {
    for (const sv of s.servers) add({ id: 'server', status: 'ok', title: 'Local server', detail: `${SERVER_LABEL[sv.kind]}${sv.version ? ` ${sv.version}` : ''} at ${sv.baseUrl} with ${sv.models.length} model${sv.models.length === 1 ? '' : 's'}.` });
  }

  // Market.
  if (s.catalogError) {
    add({ id: 'market', status: 'fail', title: 'Market', detail: `${s.catalogError}.`, fix: 'Check this computer is online. A VPN or a home router is fine: the app only dials out over HTTPS (port 443).' });
  } else {
    add({ id: 'market', status: s.catalogCached ? 'warn' : 'ok', title: 'Market', detail: `${s.target.apiUrl} ${s.catalogCached ? 'did not answer; using the last saved catalog' : `answers (${s.catalog?.length ?? 0} catalog models)`}.` });
  }

  // Models.
  if (s.servers.length > 0 && s.mapping) {
    const served = s.mapping.served;
    const unmatched = s.mapping.detected.filter((d) => d.model === null).map((d) => d.local);
    if (served.length > 0) {
      add({
        id: 'models',
        status: 'ok',
        title: 'Models',
        detail: `${served.length} of ${local.length} matched to the catalog: ${served.map((x) => `${x.local} -> ${x.catalogId}`).join('; ')}.${unmatched.length ? ` Not in the catalog (not offered): ${unmatched.join(', ')}.` : ''}`,
      });
    } else {
      const pulls = ollamaSuggestions(s.catalog);
      add({
        id: 'models',
        status: 'fail',
        title: 'Models',
        detail: local.length === 0 ? 'The local server has no models yet.' : `None of the ${local.length} local model${local.length === 1 ? '' : 's'} is in the Inferit catalog (${local.map((m) => m.name).slice(0, 8).join(', ')}).`,
        fix: pulls.length ? `Download a catalog model, for example: ollama pull ${pulls[0]}. The catalog is at ${s.target.apiUrl}/v1/models.` : `Load a model from the catalog (${s.target.apiUrl}/v1/models).`,
      });
    }
    for (const n of s.mapping.notes) add({ id: 'models', status: 'info', title: 'Models', detail: n });
    for (const c of contextChecks(s)) add(c);
  }

  // Token.
  const token = s.target.token;
  if (!token) {
    add({ id: 'token', status: 'fail', title: 'Token', detail: 'No node token is saved, so this machine cannot connect to the market.', fix: `In the Sell workspace (${SELL_URL}), choose Add a machine and run the command it shows (inferit-seller connect --token icn_…).` });
  } else if (!NODE_TOKEN_RE.test(token)) {
    add({ id: 'token', status: 'fail', title: 'Token', detail: 'The saved token is not a node token (icn_ followed by 43 characters).', fix: 'Run the connect command from the Add a machine step again.' });
  } else if (!s.target.connectUrl) {
    add({ id: 'token', status: 'fail', title: 'Token', detail: 'No market address is saved.', fix: 'Run the connect command again.' });
  } else {
    const r = await (i.checkTokenImpl ?? checkToken)(s.target.connectUrl, token);
    if (r.ok) add({ id: 'token', status: 'ok', title: 'Token', detail: `The market accepts ${tokenPrefix(token)}.` });
    else if (!r.reachable) add({ id: 'token', status: 'fail', title: 'Token', detail: `Could not reach ${s.target.connectUrl} (${r.error}).`, fix: 'Check this computer is online; the app dials out over HTTPS (port 443) and needs no open port.' });
    else {
      const f = tokenFix(r);
      add({ id: 'token', status: f.fatal ? 'fail' : 'warn', title: 'Token', detail: f.problem, fix: f.fix });
    }
  }

  // Running and connected.
  const st = s.state;
  if (s.running && st) {
    if (st.phase === 'live') add({ id: 'running', status: 'ok', title: 'Running', detail: `The app runs (pid ${st.pid}) and is connected to the market${st.offers !== null ? `; offers on the market for this machine: ${st.offers}` : ''}.` });
    else {
      const authWait = st.retryReason === 'auth';
      const retry = retryText(st);
      add({
        id: 'running',
        status: 'warn',
        title: 'Running',
        detail: `The app runs (pid ${st.pid}) but is not connected${st.lastFailure ? `: ${st.lastFailure}` : st.message ? `: ${st.message}` : ''}${retry ? `; ${retry}` : ''}.`,
        fix: authWait
          ? `Fix the token check above. After a refused token the app waits ${inWords(AUTH_RETRY_MS)} between tries; once the new connect command has run, restart it to connect at once (with the service: uninstall-service, then install-service).`
          : 'Fix the failed checks above; the app retries on its own (at most every 60 s).',
      });
    }
    if (st.phase === 'live' && st.offers === 0) add({ id: 'offers', status: 'info', title: 'Offers', detail: 'Connected, but no price is listed for this machine yet, so buyers cannot reach it.', fix: `Open ${SELL_URL}, find this machine under Your machines and list a price for each model.` });
  } else {
    add({
      id: 'running',
      status: 'info',
      title: 'Running',
      detail: 'The app is not running.',
      fix: inDocker(i)
        ? 'Start the container: docker start inferit-seller.'
        : s.service?.installed
          ? 'The background service should start it; check its logs.'
          : s.service?.otherHome
            ? `Run inferit-seller start (in this terminal). The background service runs ${s.service.otherHome}; install-service here would replace it.`
            : 'Run inferit-seller install-service (background, at every login) or inferit-seller start (in this terminal).',
    });
  }

  // Service and sleep.
  if (s.service) {
    add(
      s.service.installed
        ? { id: 'service', status: 'ok', title: 'Service', detail: `Installed: ${s.service.file}. Logs: ${s.service.logs}` }
        : s.service.otherHome
          ? {
              id: 'service',
              status: 'warn',
              title: 'Service',
              detail: `The background service (${s.service.file}) runs another app folder, ${s.service.otherHome}, not this one (${i.appHome}, from INFERIT_SELLER_HOME). There is one service per user account.`,
              fix: `To manage that service, run the command without INFERIT_SELLER_HOME (or with INFERIT_SELLER_HOME=${s.service.otherHome}). To move the service to this folder, uninstall it from there first, then run install-service here.`,
            }
          : { id: 'service', status: 'info', title: 'Service', detail: 'Not installed: the app stops when this terminal closes and does not start after a reboot.', fix: 'inferit-seller install-service' },
    );
  }
  if ((i.platform === 'darwin' || i.platform === 'linux') && !inDocker(i) && !s.config?.keepAwake && i.env.INFERIT_KEEP_AWAKE !== '1') {
    add({
      id: 'sleep',
      status: 'info',
      title: 'Sleep',
      detail: 'While this computer sleeps it cannot sell: the market sends buyers to other sellers and takes it back after it wakes.',
      fix: 'To keep it awake while the app runs: inferit-seller install-service --keep-awake',
    });
  }

  return { checks, ok: !checks.some((c) => c.status === 'fail'), snapshot: s };
}

const TAG: Record<CheckStatus, string> = { ok: '[ok]  ', warn: '[warn]', fail: '[fail]', info: '[info]' };

export function formatDoctor(r: { checks: Check[]; ok: boolean }): string {
  const L: string[] = [];
  for (const c of r.checks) {
    L.push(`${TAG[c.status]} ${c.title}: ${c.detail}`);
    if (c.fix && c.status !== 'ok') L.push(`       Fix: ${c.fix}`);
  }
  const fails = r.checks.filter((c) => c.status === 'fail').length;
  L.push('');
  L.push(fails === 0 ? 'No problems found.' : `${fails} problem${fails === 1 ? '' : 's'} to fix.`);
  return L.join('\n');
}

