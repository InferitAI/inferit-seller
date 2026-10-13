// Finds the OpenAI-compatible inference servers running on this machine and the models they hold:
//
//   Ollama      :11434  /api/tags (names, sizes, quantisation, parameter count), /api/ps (loaded models and the
//                       context they are served with); requests go to /v1
//   LM Studio   :1234   /v1/models, plus /api/v0/models for quantisation and context when it answers
//   vLLM        :8000   /v1/models (max_model_len)
//   llama.cpp   :8080   /v1/models, plus /props for the served context
//
// A server is identified by what it answers, not only by its port: a list whose owned_by is "vllm" is vLLM wherever
// it listens, and an OpenAI-shaped list nobody claims is "custom". Every probe has a short timeout and never throws.
// Ollama cloud models (they run on Ollama's servers, not this machine) are listed as remote and never served.
import type { DetectedServer } from './connector.js';

export type ServerKind = DetectedServer;

export const SERVER_LABEL: Record<ServerKind, string> = {
  ollama: 'Ollama',
  lmstudio: 'LM Studio',
  vllm: 'vLLM',
  llamacpp: 'llama.cpp server',
  custom: 'OpenAI-compatible server',
};

/** The default ports, probed in this order. */
export const KNOWN_SERVERS: readonly { kind: Exclude<ServerKind, 'custom'>; port: number }[] = [
  { kind: 'ollama', port: 11434 },
  { kind: 'lmstudio', port: 1234 },
  { kind: 'vllm', port: 8000 },
  { kind: 'llamacpp', port: 8080 },
];

export interface LocalModel {
  /** The name the server uses for it (what goes in `model` upstream). */
  name: string;
  server: ServerKind;
  /** The OpenAI base URL that serves it, e.g. http://127.0.0.1:11434/v1. */
  baseUrl: string;
  /** The context the server serves it with, when the server says (Ollama: only while loaded). */
  contextLength?: number;
  /** The model's own maximum context, when the server says (informational). */
  maxContextLength?: number;
  /** Lowercase, e.g. q4_k_m. */
  quantization?: string;
  sizeBytes?: number;
  /** Parameter count in billions (Ollama's parameter_size), for the size check when a name carries no size. */
  parameterB?: number;
  loaded?: boolean;
  /** false for embedding-only models. */
  chat: boolean;
  /** An Ollama cloud model: it runs on Ollama's servers, not here. Never served. */
  remote?: boolean;
}

export interface LocalServer {
  kind: ServerKind;
  origin: string;
  baseUrl: string;
  version?: string;
  models: LocalModel[];
}

export interface Probe {
  kind: ServerKind;
  /** What was probed, e.g. http://127.0.0.1:11434. */
  url: string;
  ok: boolean;
  /** Why it did not answer (connection refused, timeout, not an OpenAI-compatible list). */
  error?: string;
  models?: number;
}

export interface DetectOptions {
  /** Hosts to probe on the default ports. Default ['127.0.0.1'] (a Docker image sets host.docker.internal). */
  hosts?: string[];
  /** Probe only this base URL (--upstream / UPSTREAM_BASE_URL), e.g. http://127.0.0.1:11434/v1. */
  upstream?: string;
  fetchImpl?: typeof fetch;
  /** Per request. Default 1500 ms. */
  timeoutMs?: number;
  /** Extra headers for the upstream (UPSTREAM_API_KEY). */
  apiKey?: string;
}

type Json = Record<string, unknown>;

const isObj = (v: unknown): v is Json => v !== null && typeof v === 'object' && !Array.isArray(v);
const str = (v: unknown): string | undefined => (typeof v === 'string' && v.trim() !== '' ? v.trim() : undefined);
const posInt = (v: unknown): number | undefined => (typeof v === 'number' && Number.isSafeInteger(v) && v > 0 ? v : undefined);

function errText(e: unknown): string {
  const err = e as { name?: string; message?: string; cause?: { code?: string; message?: string } };
  if (err?.name === 'TimeoutError' || err?.name === 'AbortError') return 'no answer (timed out)';
  const code = err?.cause?.code;
  if (code === 'ECONNREFUSED') return 'nothing listening (connection refused)';
  if (code) return code;
  return (err?.cause?.message ?? err?.message ?? String(e)).slice(0, 200);
}

async function getJson(url: string, o: DetectOptions): Promise<{ ok: true; json: unknown } | { ok: false; error: string }> {
  const f = o.fetchImpl ?? fetch;
  try {
    const headers: Record<string, string> = { accept: 'application/json' };
    if (o.apiKey) headers.authorization = `Bearer ${o.apiKey}`;
    const res = await f(url, { headers, signal: AbortSignal.timeout(o.timeoutMs ?? 1_500) });
    if (!res.ok) {
      await res.body?.cancel().catch(() => undefined);
      return { ok: false, error: `HTTP ${res.status}` };
    }
    const text = await res.text();
    try {
      return { ok: true, json: JSON.parse(text) as unknown };
    } catch {
      return { ok: false, error: 'not JSON' };
    }
  } catch (e) {
    return { ok: false, error: errText(e) };
  }
}

/** "14.8B" -> 14.8, "135M" -> 0.135, "1.2T" -> 1200. */
export function parseParameterSize(raw: unknown): number | undefined {
  const m = typeof raw === 'string' ? /^\s*(\d+(?:\.\d+)?)\s*([KMBT])\s*$/i.exec(raw) : null;
  if (!m) return undefined;
  const n = Number(m[1]);
  const unit = m[2]!.toUpperCase();
  const b = unit === 'T' ? n * 1000 : unit === 'B' ? n : unit === 'M' ? n / 1000 : n / 1e6;
  return Number.isFinite(b) && b > 0 ? b : undefined;
}

/** q4_K_M -> q4_k_m; anything that is not a short name is dropped. */
export function normaliseQuant(raw: unknown): string | undefined {
  const s = str(raw)?.toLowerCase();
  return s && /^[a-z0-9][a-z0-9_.-]{0,31}$/.test(s) ? s : undefined;
}

function isOllamaCloud(m: Json, name: string): boolean {
  return str(m.remote_host) !== undefined || str(m.remote_model) !== undefined || /(?:[:-])cloud$/i.test(name);
}

async function probeOllama(origin: string, o: DetectOptions): Promise<LocalServer | { error: string }> {
  const tags = await getJson(`${origin}/api/tags`, o);
  if (!tags.ok) return { error: tags.error };
  if (!isObj(tags.json) || !Array.isArray(tags.json.models)) return { error: 'not an Ollama server' };
  const [ps, ver] = await Promise.all([getJson(`${origin}/api/ps`, o), getJson(`${origin}/api/version`, o)]);
  const loaded = new Map<string, number | undefined>();
  if (ps.ok && isObj(ps.json) && Array.isArray(ps.json.models)) {
    for (const m of ps.json.models) if (isObj(m) && str(m.name)) loaded.set(str(m.name)!, posInt(m.context_length));
  }
  const baseUrl = `${origin}/v1`;
  const models: LocalModel[] = [];
  for (const m of tags.json.models) {
    if (!isObj(m)) continue;
    const name = str(m.name) ?? str(m.model);
    if (!name) continue;
    const details = isObj(m.details) ? m.details : {};
    const caps = Array.isArray(m.capabilities) ? m.capabilities.filter((c): c is string => typeof c === 'string') : null;
    const e: LocalModel = { name, server: 'ollama', baseUrl, chat: caps ? caps.includes('completion') : true };
    const q = normaliseQuant(details.quantization_level);
    if (q) e.quantization = q;
    const size = posInt(m.size);
    if (size) e.sizeBytes = size;
    const p = parseParameterSize(details.parameter_size);
    if (p) e.parameterB = p;
    const max = posInt(details.context_length);
    if (max) e.maxContextLength = max;
    if (loaded.has(name)) {
      e.loaded = true;
      const ctx = loaded.get(name);
      if (ctx) e.contextLength = ctx;
    }
    if (isOllamaCloud(m, name)) e.remote = true;
    models.push(e);
  }
  const version = ver.ok && isObj(ver.json) ? str(ver.json.version) : undefined;
  return { kind: 'ollama', origin, baseUrl, ...(version ? { version } : {}), models };
}

/** Each server stamps its model list: vLLM "vllm", llama.cpp "llamacpp", LM Studio "organization_owner", Ollama
 *  "library". Anything else is custom (LM Studio and llama.cpp are then recognised by their own endpoints). */
function kindOf(ownedBy: Set<string>): ServerKind {
  if (ownedBy.has('vllm')) return 'vllm';
  if (ownedBy.has('llamacpp')) return 'llamacpp';
  if (ownedBy.has('organization_owner')) return 'lmstudio';
  if (ownedBy.has('library') || ownedBy.has('ollama')) return 'ollama';
  return 'custom';
}

async function probeOpenAI(baseUrl: string, hint: ServerKind, o: DetectOptions): Promise<LocalServer | { error: string }> {
  const list = await getJson(`${baseUrl}/models`, o);
  if (!list.ok) return { error: list.error };
  if (!isObj(list.json) || !Array.isArray(list.json.data)) return { error: 'not an OpenAI-compatible model list' };
  const rows = list.json.data.filter((r): r is Json => isObj(r) && str(r.id) !== undefined);
  const ownedBy = new Set(rows.map((r) => str(r.owned_by)).filter((x): x is string => x !== undefined));
  let kind = kindOf(ownedBy);
  const origin = new URL(baseUrl).origin;
  const models: LocalModel[] = rows.map((r) => {
    const e: LocalModel = { name: str(r.id)!, server: kind, baseUrl, chat: true };
    const maxLen = posInt(r.max_model_len); // vLLM: what it serves
    if (maxLen) e.contextLength = maxLen;
    const meta = isObj(r.meta) ? r.meta : null; // llama.cpp
    if (meta) {
      const train = posInt(meta.n_ctx_train);
      if (train) e.maxContextLength = train;
      const size = posInt(meta.size);
      if (size) e.sizeBytes = size;
      const params = posInt(meta.n_params);
      if (params) e.parameterB = params / 1e9;
    }
    return e;
  });
  if (kind === 'lmstudio' || (kind === 'custom' && hint === 'lmstudio')) {
    // LM Studio's own REST API has the quantisation, the type and the loaded context.
    const v0 = await getJson(`${origin}/api/v0/models`, o);
    if (v0.ok && isObj(v0.json) && Array.isArray(v0.json.data)) {
      kind = 'lmstudio';
      const byId = new Map(v0.json.data.filter(isObj).map((d) => [str(d.id) ?? '', d]));
      for (const e of models) {
        e.server = 'lmstudio';
        const d = byId.get(e.name);
        if (!d) continue;
        if (d.type === 'embeddings') e.chat = false;
        const q = normaliseQuant(d.quantization);
        if (q) e.quantization = q;
        const max = posInt(d.max_context_length);
        if (max) e.maxContextLength = max;
        if (d.state === 'loaded') {
          e.loaded = true;
          const ctx = posInt(d.loaded_context_length);
          if (ctx) e.contextLength = ctx;
        }
      }
    }
  }
  if (kind === 'llamacpp' || (kind === 'custom' && hint === 'llamacpp')) {
    const props = await getJson(`${origin}/props`, o);
    const settings = props.ok && isObj(props.json) && isObj(props.json.default_generation_settings) ? props.json.default_generation_settings : null;
    if (settings) {
      kind = 'llamacpp';
      const ctx = posInt(settings.n_ctx);
      if (ctx) for (const e of models) e.contextLength = ctx;
    }
  }
  for (const e of models) e.server = kind;
  return { kind, origin, baseUrl, models };
}

function hostForUrl(host: string): string {
  return host.includes(':') && !host.startsWith('[') ? `[${host}]` : host;
}

/** The OpenAI base URL for an --upstream value: http://host:port and http://host:port/v1 both mean .../v1. */
export function upstreamBase(raw: string): string {
  const u = new URL(raw.trim());
  if (u.protocol !== 'http:' && u.protocol !== 'https:') throw new Error('the upstream must be an http(s) URL');
  if (u.username || u.password || u.search || u.hash) throw new Error('the upstream URL must not carry credentials, a query or a fragment');
  const path = u.pathname.replace(/\/+$/, '');
  return `${u.origin}${path === '' ? '/v1' : path}`;
}

async function probeOne(kind: ServerKind, origin: string, o: DetectOptions): Promise<{ server: LocalServer | null; probe: Probe }> {
  const r = kind === 'ollama' ? await probeOllama(origin, o) : await probeOpenAI(`${origin}/v1`, kind, o);
  if ('error' in r) {
    // Something answers on the Ollama port but not as Ollama: an OpenAI-compatible list is still usable.
    if (kind === 'ollama' && !/refused|timed out/.test(r.error)) {
      const alt = await probeOpenAI(`${origin}/v1`, 'custom', o);
      if (!('error' in alt)) return { server: alt, probe: { kind: alt.kind, url: origin, ok: true, models: alt.models.length } };
    }
    return { server: null, probe: { kind, url: origin, ok: false, error: r.error } };
  }
  return { server: r, probe: { kind: r.kind, url: origin, ok: true, models: r.models.length } };
}

/** Probes the local servers. Never throws for an unreachable server: it is reported in `probes`. */
export async function detectLocal(o: DetectOptions = {}): Promise<{ servers: LocalServer[]; probes: Probe[] }> {
  if (o.upstream) {
    const base = upstreamBase(o.upstream);
    const origin = new URL(base).origin;
    const port = Number(new URL(base).port || (base.startsWith('https:') ? 443 : 80));
    const hint = KNOWN_SERVERS.find((s) => s.port === port)?.kind ?? 'custom';
    // Ollama's native API only when the upstream is Ollama's own /v1.
    if (base === `${origin}/v1`) {
      const ol = await probeOllama(origin, o);
      if (!('error' in ol)) return { servers: [ol], probes: [{ kind: 'ollama', url: origin, ok: true, models: ol.models.length }] };
    }
    const r = await probeOpenAI(base, hint, o);
    if ('error' in r) return { servers: [], probes: [{ kind: hint, url: base, ok: false, error: r.error }] };
    return { servers: [r], probes: [{ kind: r.kind, url: base, ok: true, models: r.models.length }] };
  }
  const hosts = (o.hosts && o.hosts.length > 0 ? o.hosts : ['127.0.0.1']).map(hostForUrl);
  const jobs: Promise<{ server: LocalServer | null; probe: Probe }>[] = [];
  for (const host of hosts) {
    for (const s of KNOWN_SERVERS) {
      jobs.push(
        (async () => {
          const first = await probeOne(s.kind, `http://${host}:${s.port}`, o);
          // Some servers listen on IPv6 loopback only.
          if (!first.server && host === '127.0.0.1' && /refused/.test(first.probe.error ?? '')) {
            const v6 = await probeOne(s.kind, `http://[::1]:${s.port}`, o);
            if (v6.server) return v6;
          }
          return first;
        })(),
      );
    }
  }
  const results = await Promise.all(jobs);
  const servers: LocalServer[] = [];
  const seen = new Set<string>();
  for (const r of results) {
    if (!r.server || seen.has(r.server.baseUrl)) continue;
    seen.add(r.server.baseUrl);
    servers.push(r.server);
  }
  return { servers, probes: results.map((r) => r.probe) };
}
