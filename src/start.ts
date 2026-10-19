// `start`: finds the local inference server, maps its models to the catalog, starts the loopback proxy
// (createSellerNode) and dials out to the market (SellerConnector), with the watchdog on.
//
// Settings come from the environment first, then ~/.inferit-seller/config.json (CLI only; main.ts reads the
// environment alone):
//   INFERIT_TOKEN or CONNECT_TOKEN   the node token (icn_…)
//   INFERIT_API_URL                  the market (default: the hosted testnet demo); CONNECT_URL overrides the socket URL
//   NODE_NAME                        the name in the hello (default: the hostname)
//   UPSTREAM_BASE_URL / --upstream   use this server instead of detecting one; UPSTREAM_API_KEY if it needs a key
//   MODEL_MAP                        catalogId=localName pairs, instead of the automatic mapping
//   MODEL_META                       per-model context and quantisation (JSON), as before
//   INFERIT_DETECT_HOSTS             hosts to probe (default 127.0.0.1; the Docker image uses host.docker.internal)
//   INFERIT_WATCHDOG_MINUTES         after this long without a market session (default 10, 0 = never), exit 1 so the
//                                    service manager starts a fresh process. Only under launchd, systemd, the Windows
//                                    task or Docker, or when this variable is set; a terminal run warns and keeps trying
//   INFERIT_KEEP_AWAKE=1             keep the computer from sleeping while the app runs (macOS, Linux)
//   INFERIT_KEEP_WARM                Ollama only: keep each model with an offer loaded (default on, keep_alive 10m, every
//                                    4 min; 0 = off; 30m or 2h = that keep_alive). See keepwarm.ts
// MOCK=1 or no market URL at all keeps the old behaviour of main.ts (a plain HTTP seller on PORT).
//
// Open listing: a local chat model the catalog does not have is reported "not in the catalog yet: list it in the Sell
// workspace". A machine with only such models still connects (declaring none), so the Sell workspace shows them. When
// the market's `offers` frame names a model this app has not mapped (the seller just listed it, or an offer names it),
// the app asks its local servers and GET /v1/models again and serves what now maps: the loopback proxy's model map,
// the declared models (a new hello, make before break) and keep-warm take it in, with no restart. MODEL_MAP is the
// seller's own mapping and is never changed. While nothing is served, the app also maps again every minute.
import { spawn, type ChildProcess } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import type { Writable } from 'node:stream';
import { fetchCatalog, mapModels, NOT_LISTED_YET, parseCatalog, type CatalogEntry, type ServedModel } from './catalog.js';
import { connectorOptionsFromEnv, parseModelMap, parseModelMeta, sellerNodeOptionsFromEnv } from './config.js';
import { NODE_TOKEN_RE, SellerConnector, WATCHDOG_NO_SESSION_MS, describeFailure, type AttemptFailure, type DetectedModel, type ModelDecl, type NodeOffer } from './connector.js';
import { detectLocal, SERVER_LABEL, upstreamBase, type LocalServer } from './detect.js';
import { createSellerNode, SELLER_LOG_REDACT, type SellerNode, type SellerNodeOptions } from './index.js';
import { KEEP_WARM_HEAD_START_MS, keepWarmSetting, keepWarmTargets, startKeepWarm, type KeepWarm, type KeepWarmTarget } from './keepwarm.js';
import type { Logger } from './log.js';
import { knownProviderOf } from './providers.js';
import type { ServiceContext } from './service.js';
import {
  DEFAULT_API_URL,
  bootAt,
  catalogCachePath,
  readState,
  connectUrlFromApi,
  defaultNodeName,
  normaliseApiUrl,
  tokenPrefix,
  writePrivate,
  writeState,
  type RunState,
  type SavedConfig,
} from './userconfig.js';
import { AGENT_NAME, PACKAGE_VERSION } from './version.js';

export const SELL_URL = 'https://inferit.app/sell';
/** How often a running app asks its local server(s) again what context they serve each model with. */
export const CONTEXT_RECHECK_MS = 60_000;
/** While no model is served, how often the app maps its local models again (a catalog model pulled, a model listed). */
export const NOTHING_SERVED_REPLAN_MS = 60_000;
/** An offer naming a model the app could not map is looked for again at most this often (a `listed` frame: always). */
export const REMAP_RETRY_MS = 5 * 60_000;
/** A model mapped after start counts as wanted by keep-warm this long, so it is loaded before its first listing probe. */
export const NEWLY_MAPPED_WARM_MS = 10 * 60_000;
/**
 * A model the market says was just listed but that the model list does not show yet (another API instance, or a cache
 * in front of GET /v1/models, may lag a few seconds): looked for again after each of these delays, then given up.
 */
export const LISTED_RETRY_MS: readonly number[] = [5_000, 15_000, 30_000];

export interface StartDeps {
  env: NodeJS.ProcessEnv;
  /** ~/.inferit-seller for the CLI (state, catalog cache); null for main.ts (environment only). */
  appHome: string | null;
  config: SavedConfig | null;
  flags?: { upstream?: string };
  log: Logger;
  /** Where Fastify writes its JSON log lines. Default: stdout. */
  sink?: Writable;
  /** What the watchdog calls (the CLI: process.exit). Only used when something restarts the process: a service
   *  context, the Windows supervisor (INFERIT_SELLER_SUPERVISED=1) or an explicit INFERIT_WATCHDOG_MINUTES. */
  exit: (code: number) => void;
  service: ServiceContext | null;
  /** The watchdog's window when INFERIT_WATCHDOG_MINUTES is unset. Default 10 (tests shorten it). */
  watchdogMinutes?: number;
  fetchImpl?: typeof fetch;
  sleep?: (ms: number) => Promise<void>;
  /** Off in tests. */
  keepAwake?: (on: boolean) => (() => void) | null;
  /** How often to re-read the served context (default CONTEXT_RECHECK_MS; 0 = never). */
  contextRecheckMs?: number;
  /** While nothing is served, how often to map the local models again (default NOTHING_SERVED_REPLAN_MS; 0 = never). */
  replanMs?: number;
  /** The delays before looking again for a just-listed model the model list does not show yet (default LISTED_RETRY_MS). */
  listedRetryMs?: readonly number[];
  /** How long connecting waits for the keep-warm round at start (default KEEP_WARM_HEAD_START_MS). */
  keepWarmHeadStartMs?: number;
}

export interface RunningSeller {
  node: SellerNode;
  connector: SellerConnector | null;
  served: ServedModel[];
  stop(): Promise<void>;
}

function flag(v: string | undefined): boolean {
  const s = v?.trim().toLowerCase();
  return s === '1' || s === 'true' || s === 'yes';
}

export interface ResolvedTarget {
  token: string | undefined;
  apiUrl: string;
  connectUrl: string | undefined;
  nodeName: string;
  upstream: string | undefined;
}

/** Where to connect and as whom: the environment first, then the saved config. */
export function resolveTarget(env: NodeJS.ProcessEnv, cfg: SavedConfig | null, flags: { upstream?: string } = {}): ResolvedTarget {
  const token = env.CONNECT_TOKEN?.trim() || env.INFERIT_TOKEN?.trim() || cfg?.token || undefined;
  const apiEnv = env.INFERIT_API_URL?.trim();
  const apiUrl = normaliseApiUrl(apiEnv || cfg?.apiUrl || DEFAULT_API_URL);
  const connectUrl = env.CONNECT_URL?.trim() || (apiEnv ? connectUrlFromApi(apiUrl) : cfg?.connectUrl) || (token ? connectUrlFromApi(apiUrl) : undefined);
  const nodeName = env.NODE_NAME?.trim() || cfg?.nodeName || defaultNodeName();
  const upstream = flags.upstream?.trim() || env.UPSTREAM_BASE_URL?.trim() || cfg?.upstream || undefined;
  return { token, apiUrl, connectUrl, nodeName, upstream };
}

export function detectHosts(env: NodeJS.ProcessEnv, service: ServiceContext | null): string[] {
  const raw = env.INFERIT_DETECT_HOSTS?.trim();
  if (raw) return raw.split(',').map((h) => h.trim()).filter(Boolean);
  return service === 'docker' ? ['host.docker.internal'] : ['127.0.0.1'];
}

/** The catalog from the market, else the last one saved (CLI), else an error. */
export async function loadCatalog(apiUrl: string, appHome: string | null, fetchImpl?: typeof fetch): Promise<{ catalog: CatalogEntry[]; cached: boolean }> {
  try {
    const catalog = await fetchCatalog(apiUrl, fetchImpl);
    if (appHome) {
      try {
        writePrivate(catalogCachePath(appHome), `${JSON.stringify({ apiUrl, savedAt: new Date().toISOString(), data: catalog.map((c) => ({ id: c.id, aliases: c.aliases, context_length: c.contextLength, ...(c.listedBy ? { listed_by: c.listedBy } : {}) })) })}\n`);
      } catch {
        /* a cache only */
      }
    }
    return { catalog, cached: false };
  } catch (e) {
    if (appHome) {
      try {
        const cached = JSON.parse(fs.readFileSync(catalogCachePath(appHome), 'utf8')) as { apiUrl?: string };
        if (cached.apiUrl === apiUrl) return { catalog: parseCatalog(cached), cached: true };
      } catch {
        /* no cache */
      }
    }
    throw e;
  }
}

export interface Plan {
  served: ServedModel[];
  detected: DetectedModel[];
  /** Local chat models the market does not know yet (not in the catalog, not listed): the seller can list them. */
  unlisted: DetectedModel[];
  servers: LocalServer[];
  notes: string[];
  /** Why nothing can be served yet (no server, no catalog match, the market unreachable). */
  problem: string | null;
}

/** Detects the local servers and decides what to serve (the automatic mapping, or MODEL_MAP when set). */
export async function planModels(o: {
  env: NodeJS.ProcessEnv;
  apiUrl: string;
  upstream: string | undefined;
  hosts: string[];
  appHome: string | null;
  fetchImpl?: typeof fetch;
}): Promise<Plan> {
  if (o.upstream) {
    const provider = knownProviderOf(o.upstream);
    if (provider) throw new Error(`the upstream is on ${provider}, a hosted inference API. Sell your own server's inference (Ollama, LM Studio, vLLM, llama.cpp).`);
  }
  const apiKey = o.env.UPSTREAM_API_KEY?.trim();
  const { servers } = await detectLocal({
    hosts: o.hosts,
    ...(o.upstream ? { upstream: o.upstream } : {}),
    ...(o.fetchImpl ? { fetchImpl: o.fetchImpl } : {}),
    ...(apiKey ? { apiKey } : {}),
  });
  const locals = servers.flatMap((s) => s.models);
  const fixed = parseModelMap(o.env.MODEL_MAP);
  if (Object.keys(fixed).length > 0) {
    // MODEL_MAP: the seller's own mapping. Each local name goes to the server that lists it, else to the upstream
    // the seller named, else to the only server found.
    const fallback = o.upstream ? upstreamBase(o.upstream) : servers.length === 1 ? servers[0]!.baseUrl : undefined;
    const served: ServedModel[] = [];
    for (const [catalogId, local] of Object.entries(fixed)) {
      const holder = locals.find((m) => m.name.toLowerCase() === local.toLowerCase());
      const base = holder?.baseUrl ?? fallback;
      if (!base) {
        return { served: [], detected: [], unlisted: [], servers, notes: [], problem: servers.length === 0 ? 'no local inference server answers' : `MODEL_MAP names "${local}", which no local server lists` };
      }
      served.push({ catalogId, local, server: holder?.server ?? servers[0]?.kind ?? 'custom', baseUrl: base, ...(holder?.quantization ? { quantization: holder.quantization } : {}) });
    }
    const detected = locals
      .filter((m) => !m.remote)
      .map((m): DetectedModel => {
        return {
          local: m.name,
          server: m.server,
          model: served.find((x) => x.local.toLowerCase() === m.name.toLowerCase())?.catalogId ?? null,
          // What the server serves, never the model's maximum (see mapModels).
          ...(m.contextLength ? { contextLength: m.contextLength } : {}),
          ...(m.quantization ? { quantization: m.quantization } : {}),
          ...(m.sizeBytes !== undefined ? { sizeBytes: m.sizeBytes } : {}),
        };
      });
    return { served, detected, unlisted: [], servers, notes: [], problem: null };
  }
  if (servers.length === 0) {
    return {
      served: [],
      detected: [],
      unlisted: [],
      servers,
      notes: [],
      problem: o.upstream ? `the upstream ${o.upstream} does not answer with a model list` : 'no local inference server answers (Ollama :11434, LM Studio :1234, vLLM :8000, llama.cpp :8080)',
    };
  }
  let catalog: CatalogEntry[];
  try {
    catalog = (await loadCatalog(o.apiUrl, o.appHome, o.fetchImpl)).catalog;
  } catch (e) {
    return { served: [], detected: [], unlisted: [], servers, notes: [], problem: (e as Error).message };
  }
  const m = mapModels(locals, catalog);
  const chat = new Set(locals.filter((x) => x.chat && !x.remote).map((x) => `${x.server}\u0000${x.name}`));
  return {
    ...m,
    unlisted: m.detected.filter((x) => x.model === null && chat.has(`${x.server}\u0000${x.local}`)),
    servers,
    problem: m.served.length === 0 ? (locals.length === 0 ? 'the local server has no models yet' : 'none of the local models is in the Inferit catalog') : null,
  };
}

const contextKey = (server: string, local: string) => `${server}\u0000${local.toLowerCase()}`;

/**
 * The context each model is served with now, by server kind and local name, from the servers at `bases` (models the
 * server does not say it for, such as an Ollama model that is not loaded, are absent).
 */
export async function servedContexts(bases: string[], o: { apiKey?: string; fetchImpl?: typeof fetch } = {}): Promise<Map<string, number>> {
  const out = new Map<string, number>();
  for (const base of [...new Set(bases)]) {
    const { servers } = await detectLocal({ upstream: base, ...(o.fetchImpl ? { fetchImpl: o.fetchImpl } : {}), ...(o.apiKey ? { apiKey: o.apiKey } : {}) });
    for (const sv of servers) for (const m of sv.models) if (m.contextLength) out.set(contextKey(m.server, m.name), m.contextLength);
  }
  return out;
}

/** The state file's copy of the market's offers for this machine (no offer ids). */
function offersForState(list: NodeOffer[]): NonNullable<RunState['offerList']> {
  return list.map((o) => ({ model: o.model, ...(o.maxContextTokens !== undefined ? { maxContextTokens: o.maxContextTokens } : {}) }));
}

/** Keeps the computer awake while this process runs: caffeinate on macOS, systemd-inhibit on Linux. */
export function keepAwakeProcess(on: boolean): (() => void) | null {
  if (!on) return null;
  let child: ChildProcess | null = null;
  if (process.platform === 'darwin') {
    child = spawn('/usr/bin/caffeinate', ['-i', '-s', '-w', String(process.pid)], { stdio: 'ignore', detached: false });
  } else if (process.platform === 'linux') {
    child = spawn('systemd-inhibit', ['--what=idle:sleep', '--who=inferit-seller', '--why=Selling inference on Inferit', '--mode=block', 'tail', `--pid=${process.pid}`, '-f', '/dev/null'], { stdio: 'ignore' });
  }
  if (!child) return null;
  child.on('error', () => undefined);
  child.unref();
  return () => {
    try {
      child?.kill();
    } catch {
      /* gone */
    }
  };
}

function agentInfo() {
  return { name: AGENT_NAME, version: PACKAGE_VERSION, os: `${process.platform} ${os.release()}`.slice(0, 64), arch: process.arch };
}

/** Starts the seller. Resolves once the local proxy listens and the connector has started dialling. */
export async function startSeller(d: StartDeps): Promise<RunningSeller> {
  const env = d.env;
  const log = d.log;
  const t = resolveTarget(env, d.config, d.flags);
  const mock = flag(env.MOCK);
  const sleep = d.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const loggerOpt = { level: env.LOG_LEVEL ?? 'info', redact: { paths: SELLER_LOG_REDACT, censor: '[redacted]' }, ...(d.sink ? { stream: d.sink } : {}) };
  const watchEnv = env.INFERIT_WATCHDOG_MINUTES?.trim();
  const watchMinutes = watchEnv ? Number(watchEnv) : (d.watchdogMinutes ?? WATCHDOG_NO_SESSION_MS / 60_000);
  if (!Number.isFinite(watchMinutes) || watchMinutes < 0) throw new Error('INFERIT_WATCHDOG_MINUTES must be a number of minutes (0 = never)');
  // Exiting helps only when something starts the process again. A run in a terminal (or main.ts under nohup) that
  // exited would stay down, so there the watchdog warns and the app keeps trying.
  const restartable = d.service !== null || env.INFERIT_SELLER_SUPERVISED === '1' || Boolean(watchEnv);
  const keepWarm = keepWarmSetting(env);
  const startedAt = new Date().toISOString();
  // The last measured cold-load time of each model outlives a restart (doctor compares it with the health check's
  // deadline even when keep-warm is off and nothing measures it any more).
  const previous = d.appHome ? readState(d.appHome) : null;
  const coldLoads = new Map<string, { ms: number; at: string }>();
  for (const m of previous?.models ?? []) {
    if (typeof m.coldLoadMs === 'number') coldLoads.set(contextKey(m.server, m.local), { ms: m.coldLoadMs, at: m.coldLoadAt ?? previous!.updatedAt });
  }

  // State for `status` (CLI only).
  const state: RunState = {
    pid: process.pid,
    bootAt: bootAt(),
    startedAt,
    updatedAt: startedAt,
    version: PACKAGE_VERSION,
    tokenPrefix: t.token ? tokenPrefix(t.token) : null,
    service: d.service,
    phase: 'detecting',
    nodeId: null,
    sessionId: null,
    liveSince: null,
    offers: null,
    failedAttempts: 0,
    lastFailure: null,
    lastFailureAt: null,
    nextAttemptAt: null,
    retryReason: null,
    models: [],
    offerList: null,
    keepWarm: null,
    message: null,
  };
  const save = (patch: Partial<RunState> = {}) => {
    if (!d.appHome) return;
    Object.assign(state, patch, { updatedAt: new Date().toISOString() });
    writeState(d.appHome, state);
  };

  // The old modes: mock, or a plain HTTP seller with no market socket.
  if (mock || !t.connectUrl) {
    const e2: NodeJS.ProcessEnv = { ...env, ...(t.upstream ? { UPSTREAM_BASE_URL: t.upstream } : {}) };
    if (t.connectUrl) Object.assign(e2, { CONNECT_URL: t.connectUrl, ...(t.token ? { CONNECT_TOKEN: t.token } : {}), NODE_NAME: t.nodeName });
    if (t.connectUrl && !e2.SELLER_TOKEN) e2.SELLER_TOKEN = randomBytes(24).toString('base64url');
    const opts = sellerNodeOptionsFromEnv(e2);
    const connect = connectorOptionsFromEnv(e2, opts);
    opts.logger = loggerOpt;
    const node = await createSellerNode(opts);
    const mode = opts.mock ? 'mock' : `proxy -> ${new URL(opts.upstreamBaseUrl!).host}`;
    node.app.log.info({ mode, models: Object.keys(opts.modelMap ?? {}), auth: opts.sellerToken ? 'bearer' : 'none' }, `seller-node listening on ${node.url}`);
    if (!opts.sellerToken) node.app.log.warn('SELLER_TOKEN is not set: anyone who can reach this port can use it (mock mode only)');
    let connector: SellerConnector | null = null;
    if (connect) {
      connector = makeConnector(connect, node, opts, [], null);
      connector.start();
      log.info({ market: new URL(connect.url).host, node: connect.nodeName, models: connect.models.map((m) => m.id) }, 'connector mode: dialing out to the market');
    }
    return {
      node,
      connector,
      served: [],
      stop: async () => {
        await connector?.stop();
        await node.close();
      },
    };
  }

  if (!t.token || !NODE_TOKEN_RE.test(t.token)) throw new Error('the node token must be icn_ followed by 43 characters: copy it from the Add a machine step in the Sell workspace');
  const hosts = detectHosts(env, d.service);
  save({ phase: 'detecting', message: 'looking for a local inference server' });

  // Wait for a server with catalog models (at boot, Ollama or the network may not be up yet). A server whose chat
  // models the market does not know yet is enough: the app connects so the seller can list them (open listing).
  let plan: Plan;
  let wait = 5_000;
  let lastProblem = '';
  const fixedMap = Object.keys(parseModelMap(env.MODEL_MAP)).length > 0;
  const replan = () => planModels({ env, apiUrl: t.apiUrl, upstream: t.upstream, hosts, appHome: d.appHome, ...(d.fetchImpl ? { fetchImpl: d.fetchImpl } : {}) });
  for (;;) {
    plan = await replan();
    if (plan.served.length > 0) break;
    if (!fixedMap && plan.unlisted.length > 0) break;
    const problem = plan.problem ?? 'nothing to serve';
    if (problem !== lastProblem) {
      log.warn({ problem, retryInSeconds: Math.round(wait / 1000) }, `waiting: ${problem}. Run "inferit-seller doctor" for the fix.`);
      lastProblem = problem;
    }
    save({ phase: 'detecting', message: problem });
    await sleep(wait);
    wait = Math.min(wait * 2, 60_000);
  }
  for (const n of plan.notes) log.info({}, n);
  for (const dm of plan.unlisted) log.info({ local: dm.local, server: dm.server, sell: SELL_URL }, `${dm.local}: ${NOT_LISTED_YET} (${SELL_URL})`);
  if (plan.served.length === 0) log.info({ sell: SELL_URL }, `no local model is in the catalog yet; connecting so the Sell workspace (${SELL_URL}) can list one. This app serves it as soon as it is listed.`);

  // The loopback proxy in front of the local server(s): an ephemeral port, a per-run token. modelMap and upstreams are
  // read on every request, so a model mapped later (open listing) is added to them in place.
  const modelMap: Record<string, string> = Object.fromEntries(plan.served.map((s) => [s.catalogId, s.local]));
  const upstreams: Record<string, string> = Object.fromEntries(plan.served.map((s) => [s.catalogId, s.baseUrl]));
  // main.ts keeps honouring PORT (scripts that health-check it); the CLI takes any free loopback port.
  const port = d.appHome === null && env.PORT?.trim() ? Number(env.PORT) : 0;
  if (!Number.isInteger(port) || port < 0 || port > 65535) throw new Error(`PORT: expected a number in 0..65535, got "${env.PORT}"`);
  const opts: SellerNodeOptions = {
    host: '127.0.0.1',
    port,
    sellerToken: env.SELLER_TOKEN?.trim() || randomBytes(24).toString('base64url'),
    upstreamBaseUrl: plan.served[0]?.baseUrl ?? plan.servers[0]!.baseUrl,
    upstreams,
    modelMap,
    // Never "any model" for an empty map: until a model is mapped, this machine serves none.
    strictModelMap: true,
    logger: loggerOpt,
  };
  if (env.UPSTREAM_API_KEY?.trim()) opts.upstreamApiKey = env.UPSTREAM_API_KEY.trim();

  // MODEL_META keys that are not served are dropped (with a note) rather than refusing to start.
  const meta = parseModelMeta(env.MODEL_META);
  for (const k of Object.keys(meta)) {
    if (!(k in modelMap)) {
      log.warn({ model: k }, 'MODEL_META names a model this machine does not serve; ignored');
      delete meta[k];
    }
  }
  const connect = connectorOptionsFromEnv(
    { ...env, HOST: '127.0.0.1', CONNECT_URL: t.connectUrl, CONNECT_TOKEN: t.token, NODE_NAME: t.nodeName, MODEL_MAP: JSON.stringify(modelMap), MODEL_META: JSON.stringify(meta) },
    opts,
    { allowNoModels: true },
  )!;
  const declOf = (s: ServedModel | undefined, m: ModelDecl): ModelDecl => ({ ...m, ...(m.quantization === undefined && s?.quantization ? { quantization: s.quantization } : {}) });
  connect.models = connect.models.map((m) => declOf(plan.served.find((x) => x.catalogId === m.id), m));

  const node = await createSellerNode(opts);
  for (const s of plan.served) log.info({ model: s.catalogId, local: s.local, server: SERVER_LABEL[s.server] }, `serving ${s.catalogId} from ${SERVER_LABEL[s.server]} (${s.local})`);

  let detected = plan.detected;
  let stopped = false;
  const servedModels = (): RunState['models'] =>
    plan.served.map((s) => {
      const ctx = detected.find((x) => x.server === s.server && x.local.toLowerCase() === s.local.toLowerCase())?.contextLength;
      const cold = coldLoads.get(contextKey(s.server, s.local));
      return { id: s.catalogId, local: s.local, server: s.server, ...(ctx ? { contextLength: ctx } : {}), ...(cold ? { coldLoadMs: cold.ms, coldLoadAt: cold.at } : {}) };
    });
  const connector = makeConnector(connect, node, opts, detected, watchMinutes > 0 ? { minutes: watchMinutes, exit: restartable ? d.exit : null } : null);

  // Keep-warm (Ollama only) keeps the models with an offer on the market loaded: a model nobody can buy is not worth
  // the memory, and loading it could push out one that sells. Until the market says (its welcome), the last run's
  // offers; with none remembered (a first run, main.ts), only a lone Ollama model: with several, loading them all could
  // push out the one that sells.
  const warmTargets: KeepWarmTarget[] = keepWarmTargets(plan.served);
  const sameToken = previous !== null && previous.tokenPrefix === state.tokenPrefix && Array.isArray(previous.offerList);
  let offered: Set<string> | null = sameToken ? new Set(previous!.offerList!.map((o) => o.model.toLowerCase())) : null;
  // Local names mapped after start (open listing) -> when: wanted for a while, so the first listing probe finds them loaded.
  const newlyMapped = new Map<string, number>();
  const wantedLocal = (local: string) =>
    Date.now() - (newlyMapped.get(local.toLowerCase()) ?? -Infinity) < NEWLY_MAPPED_WARM_MS ||
    (offered === null
      ? warmTargets.length === 1
      : plan.served.some(
          (s) => s.server === 'ollama' && s.local.toLowerCase() === local.toLowerCase() && (offered!.has(s.catalogId.toLowerCase()) || offered!.has(s.local.toLowerCase())),
        ));
  const keepWarmState = (): RunState['keepWarm'] =>
    warmTargets.length === 0
      ? null
      : { on: keepWarm.on, keepAlive: keepWarm.keepAlive, everyMs: keepWarm.everyMs, ...(keepWarm.on ? { models: warmTargets.filter((w) => wantedLocal(w.local)).map((w) => w.local) } : {}) };
  let warm: KeepWarm | null = null;
  const offersNow = (list: NodeOffer[]) => {
    offered = new Set(list.map((o) => o.model.toLowerCase()));
    warm?.poke(); // a model with a new offer loads now; one that lost its offer is no longer refreshed
  };
  connector.on('live', (e: { nodeId: string; sessionId: string; offers: number; offerList: NodeOffer[] }) => {
    offersNow(e.offerList); // also: back after an outage, refresh what the skipped rounds left out
    void remap(e.offerList.map((o) => o.model), false);
    save({ phase: 'live', nodeId: e.nodeId, sessionId: e.sessionId, liveSince: new Date().toISOString(), offers: e.offers, offerList: offersForState(e.offerList), keepWarm: keepWarmState(), failedAttempts: 0, nextAttemptAt: null, retryReason: null, message: null });
    if (e.offers === 0) log.info({ sell: SELL_URL }, `connected. No price is listed for this machine yet: open ${SELL_URL}, find this machine under Your machines and list a price for each model.`);
    else log.info({ offers: e.offers }, `connected. Offers on the market for this machine: ${e.offers}.`);
  });
  // The seller listed, edited or delisted an offer on this machine while it was connected, or listed a model itself
  // (`listed`, open listing): a name this app has not mapped is mapped now if a local model has it.
  connector.on('offers', (e: { offers: number; offerList: NodeOffer[]; listed?: string[] }) => {
    offersNow(e.offerList);
    save({ offers: e.offers, offerList: offersForState(e.offerList), keepWarm: keepWarmState() });
    if (!e.listed?.length) log.info({ offers: e.offers }, e.offers === 0 ? 'no offer on the market points at this machine now' : `offers on the market for this machine: ${e.offers}`);
    void remap(e.offerList.map((o) => o.model), false);
    if (e.listed?.length) void remap(e.listed, true);
  });
  connector.on('failed', (f: AttemptFailure & { attempt: number }) => save({ phase: 'waiting', failedAttempts: f.attempt, lastFailure: describeFailure(f), lastFailureAt: new Date().toISOString(), nodeId: null, sessionId: null, liveSince: null }));
  connector.on('closed', () => {
    const st = connector.status();
    if (st.state !== 'live') save({ phase: st.state === 'stopped' ? 'stopped' : 'waiting', sessionId: null, liveSince: null });
  });
  connector.on('scheduled', (e: { delayMs: number; reason: string }) => {
    log.debug(e, 'connector: reconnect scheduled');
    save({ nextAttemptAt: new Date(Date.now() + e.delayMs).toISOString(), retryReason: e.reason });
  });

  // The keep-warm round at start goes before dialing out: the market checks a node's offers as soon as it connects,
  // and a cold load (about a minute for a 14B model on a laptop) would fail that check (it gives up after about 15 s).
  // Connecting waits for it up to KEEP_WARM_HEAD_START_MS; a slower load goes on in the background.
  // Keep-warm reads `warmTargets` on every round: a model mapped later is pushed onto it (startWarm starts it then, when
  // nothing needed it at start).
  const startWarm = (): KeepWarm => {
    const apiKey = env.UPSTREAM_API_KEY?.trim();
    return startKeepWarm({
      targets: warmTargets,
      keepAlive: keepWarm.keepAlive,
      everyMs: keepWarm.everyMs,
      log,
      ...(d.fetchImpl ? { fetchImpl: d.fetchImpl } : {}),
      ...(apiKey ? { apiKey } : {}),
      wanted: (w) => wantedLocal(w.local),
      active: () => {
        const st = connector.status().state;
        return st === 'live' || st === 'connecting';
      },
      onColdLoad: (c) => {
        coldLoads.set(contextKey('ollama', c.local), { ms: c.loadMs, at: c.at });
        save({ models: servedModels() });
      },
    });
  };
  if (keepWarm.on && warmTargets.length > 0) {
    warm = startWarm();
    save({ message: 'asking Ollama to load the models with an offer before connecting' });
    let headStart: NodeJS.Timeout | undefined;
    await Promise.race([warm.first, new Promise<void>((r) => (headStart = setTimeout(r, d.keepWarmHeadStartMs ?? KEEP_WARM_HEAD_START_MS)))]);
    clearTimeout(headStart);
  } else if (warmTargets.length > 0) {
    log.info({}, 'keep-warm is off (INFERIT_KEEP_WARM=0): Ollama unloads an idle model after its keep-alive (5 min by default), and a market health check that finds it unloaded can fail while it loads');
  }

  // Open listing: map the local models again and serve what now maps, without a restart. `names` are what the market
  // named (offer models, or the models the seller just listed); one this app already serves is skipped, and a name it
  // looked for in vain is looked for again only after REMAP_RETRY_MS, unless the market says it was just listed.
  const lookedFor = new Map<string, number>();
  let remapping = false;
  let queued: { names: string[]; listed: boolean } | null = null;
  const knows = (name: string) => {
    const n = name.toLowerCase();
    return plan.served.some((x) => x.catalogId.toLowerCase() === n || x.local.toLowerCase() === n);
  };
  const listedRetry = d.listedRetryMs ?? LISTED_RETRY_MS;
  const retryTimers = new Set<NodeJS.Timeout>();
  async function remap(names: readonly string[], listed: boolean, force = false, attempt = 0): Promise<void> {
    if (fixedMap || stopped) return;
    const now = Date.now();
    const unknown = [...new Set(names.map((n) => n.toLowerCase()))].filter((n) => !knows(n) && (listed || force || now - (lookedFor.get(n) ?? -Infinity) >= REMAP_RETRY_MS));
    if (unknown.length === 0 && !force) return;
    if (remapping) {
      queued = { names: [...(queued?.names ?? []), ...unknown], listed: listed || (queued?.listed ?? false) };
      return;
    }
    remapping = true;
    for (const n of unknown) lookedFor.set(n, now);
    try {
      const next = await replan();
      if (stopped) return;
      const added = next.served.filter((x) => !plan.served.some((y) => y.catalogId.toLowerCase() === x.catalogId.toLowerCase()));
      // What the servers report now, keeping a served context the recheck learned when this read lacks it.
      const merged = next.detected.map((x) => {
        const prev = detected.find((y) => y.server === x.server && y.local.toLowerCase() === x.local.toLowerCase());
        return x.contextLength === undefined && prev?.contextLength !== undefined && x.model !== null ? { ...x, contextLength: prev.contextLength } : x;
      });
      if (added.length === 0) {
        const missing = unknown.filter((n) => !knows(n));
        if (missing.length > 0 && listed) {
          const delay = listedRetry[attempt];
          if (delay !== undefined) {
            // The model list may not show the new listing yet: look again shortly.
            const t = setTimeout(() => {
              retryTimers.delete(t);
              void remap(missing, true, false, attempt + 1);
            }, delay);
            t.unref();
            retryTimers.add(t);
          } else {
            log.info({ models: missing }, `${missing.join(', ')}: no local model has that name. When you list a model, add this machine's local name for it (the Sell workspace does), or set MODEL_MAP.`);
          }
        }
        return;
      }
      for (const x of added) {
        plan.served.push(x);
        modelMap[x.catalogId] = x.local;
        upstreams[x.catalogId] = x.baseUrl;
        newlyMapped.set(x.local.toLowerCase(), Date.now());
        log.info({ model: x.catalogId, local: x.local, server: SERVER_LABEL[x.server] }, `serving ${x.catalogId} from ${SERVER_LABEL[x.server]} (${x.local}), now on the market's list`);
      }
      plan.served.sort((a, b) => a.catalogId.localeCompare(b.catalogId));
      detected = merged;
      connect.models = [...connect.models, ...added.map((x) => declOf(x, { id: x.catalogId }))].sort((a, b) => a.id.localeCompare(b.id));
      for (const w of keepWarmTargets(added)) if (!warmTargets.some((y) => y.origin === w.origin && y.local.toLowerCase() === w.local.toLowerCase())) warmTargets.push(w);
      if (keepWarm.on && warmTargets.length > 0) {
        if (warm) warm.poke();
        else warm = startWarm();
      }
      save({ models: servedModels(), keepWarm: keepWarmState() });
      connector.updateModels(connect.models, detected);
    } catch (e) {
      log.warn({ problem: (e as Error).message }, 'could not map the local models again; the next offers change or minute tries again');
    } finally {
      remapping = false;
      const q = queued as { names: string[]; listed: boolean } | null;
      queued = null;
      if (q && !stopped) void remap(q.names, q.listed);
    }
  }
  // While nothing is served (a machine whose models are not listed yet), map again every minute: a catalog model
  // pulled meanwhile, or a model listed while the market could not tell this app, is picked up.
  const replanMs = d.replanMs ?? NOTHING_SERVED_REPLAN_MS;
  const replanTimer =
    replanMs > 0
      ? setInterval(() => {
          if (plan.served.length === 0) void remap([], false, true);
        }, replanMs)
      : null;
  replanTimer?.unref();

  connector.start();
  save({ phase: 'connecting', message: null, models: servedModels(), keepWarm: keepWarmState() });
  log.info({ market: new URL(t.connectUrl).host, node: t.nodeName, models: plan.served.map((s) => s.catalogId) }, 'dialing out to the market (no inbound port needed)');

  const heartbeat = setInterval(() => save(), 60_000);
  heartbeat.unref();

  // Ollama and LM Studio say what context they serve a model with only while it is loaded, so at start it may be
  // unknown. Once the server says it (or says something new, after a restart with another context), the market hears
  // it in a new hello (the Sell workspace fills in the listing's context limit from it), and status and doctor read it
  // from the state file. An unloaded model keeps its last known value.
  const recheckMs = d.contextRecheckMs ?? CONTEXT_RECHECK_MS;
  let rechecking = false;
  const recheck = async () => {
    if (rechecking) return;
    rechecking = true;
    try {
      const seen = await servedContexts(
        plan.served.map((s) => s.baseUrl),
        { ...(d.fetchImpl ? { fetchImpl: d.fetchImpl } : {}), ...(env.UPSTREAM_API_KEY?.trim() ? { apiKey: env.UPSTREAM_API_KEY.trim() } : {}) },
      );
      const changed: DetectedModel[] = [];
      const next = detected.map((x) => {
        // Catalog models only: what an embedding model is loaded with never needs a new hello.
        const ctx = x.model === null ? undefined : seen.get(contextKey(x.server, x.local));
        if (ctx === undefined || ctx === x.contextLength) return x;
        const y = { ...x, contextLength: ctx };
        changed.push(y);
        return y;
      });
      if (changed.length === 0) return;
      detected = next;
      for (const x of changed) log.info({ local: x.local, contextLength: x.contextLength }, `${SERVER_LABEL[x.server]} serves ${x.local} with ${x.contextLength} tokens of context`);
      save({ models: servedModels() });
      connector.updateDetected(next);
    } catch {
      /* the next round tries again */
    } finally {
      rechecking = false;
    }
  };
  const recheckTimer = recheckMs > 0 ? setInterval(() => void recheck(), recheckMs) : null;
  recheckTimer?.unref();

  const awake = (d.keepAwake ?? keepAwakeProcess)(flag(env.INFERIT_KEEP_AWAKE) || d.config?.keepAwake === true);
  if (awake) log.info({}, 'keeping this computer awake while the app runs');

  return {
    node,
    connector,
    served: plan.served,
    stop: async () => {
      stopped = true;
      clearInterval(heartbeat);
      if (recheckTimer) clearInterval(recheckTimer);
      if (replanTimer) clearInterval(replanTimer);
      for (const t of retryTimers) clearTimeout(t);
      retryTimers.clear();
      warm?.stop();
      awake?.();
      await connector.stop();
      await node.close();
      save({ phase: 'stopped', sessionId: null, liveSince: null });
    },
  };

  function makeConnector(
    c: NonNullable<ReturnType<typeof connectorOptionsFromEnv>>,
    n: SellerNode,
    o: SellerNodeOptions,
    detected: DetectedModel[],
    watchdog: { minutes: number; exit: ((code: number) => void) | null } | null,
  ): SellerConnector {
    const nodeLog = n.app.log;
    return new SellerConnector({
      url: c.url,
      token: c.token,
      node: { name: c.nodeName, version: PACKAGE_VERSION },
      ...(c.wallet ? { wallet: c.wallet } : {}),
      models: c.models,
      maxConcurrent: c.maxConcurrent,
      local: { baseUrl: n.url, ...(o.sellerToken ? { token: o.sellerToken } : {}) },
      agent: agentInfo(),
      detected,
      log: {
        info: (obj, m) => nodeLog.info(obj, m),
        warn: (obj, m) => nodeLog.warn(obj, m),
        error: (obj, m) => nodeLog.error(obj, m),
      },
      ...(watchdog
        ? {
            watchdog: {
              noSessionMs: watchdog.minutes * 60_000,
              checkEveryMs: Math.min(15_000, Math.max(50, (watchdog.minutes * 60_000) / 4)),
              repeat: watchdog.exit === null,
              onTrip: (info) => {
                const facts = { failedAttempts: info.failedAttempts, lastFailure: info.lastFailure ? describeFailure(info.lastFailure) : null };
                const minutes = Math.round(info.noSessionMs / 60_000);
                const exit = watchdog.exit;
                if (!exit) {
                  nodeLog.error(facts, `no market session for ${minutes} minutes; still trying. Run "inferit-seller doctor" to see why. If it does not recover, restart the app; with install-service it restarts on its own.`);
                  return;
                }
                nodeLog.error(facts, `no market session for ${minutes} minutes: exiting so the service manager starts a fresh process`);
                save({ phase: 'stopped', message: 'watchdog: exited after too long without a market session' });
                setTimeout(() => exit(1), 200);
              },
            },
          }
        : {}),
    });
  }
}
