// Env -> SellerNodeOptions (SPEC §6) and the connector options (CONNECT_URL, docs/specs/always-on-supply.md §5).
// Kept separate from main.ts so it can be tested.
import { NODE_TOKEN_RE, type ModelDecl } from './connector.js';
import type { SellerNodeOptions } from './index.js';
import { knownProviderOf } from './providers.js';

/**
 * MODEL_MAP: catalogId -> upstream model. Accepts a JSON object ({"qwen/qwen-2.5-14b-instruct":"qwen2.5:14b-instruct"})
 * or a comma list (qwen/qwen-2.5-14b-instruct=qwen2.5:14b-instruct,other=tag). Empty -> {}.
 */
export function parseModelMap(raw: string | undefined): Record<string, string> {
  if (raw === undefined || raw.trim() === '') return {};
  const s = raw.trim();
  if (s.startsWith('{')) {
    let parsed: unknown;
    try {
      parsed = JSON.parse(s);
    } catch {
      throw new Error('MODEL_MAP: invalid JSON');
    }
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) throw new Error('MODEL_MAP: expected a JSON object');
    const out: Record<string, string> = {};
    for (const [k, v] of Object.entries(parsed)) {
      if (typeof v !== 'string' || k.trim() === '' || v.trim() === '') throw new Error(`MODEL_MAP: bad entry "${k}"`);
      out[k.trim()] = v.trim();
    }
    return out;
  }
  const out: Record<string, string> = {};
  for (const part of s.split(',')) {
    const p = part.trim();
    if (p === '') continue;
    const i = p.indexOf('=');
    if (i <= 0 || i === p.length - 1) throw new Error(`MODEL_MAP: expected catalogId=upstreamModel, got "${p}"`);
    out[p.slice(0, i).trim()] = p.slice(i + 1).trim();
  }
  return out;
}

function num(env: NodeJS.ProcessEnv, name: string, fallback: number, min: number, max: number): number {
  const raw = env[name];
  if (raw === undefined || raw.trim() === '') return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < min || n > max) throw new Error(`${name}: expected a number in ${min}..${max}, got "${raw}"`);
  return n;
}

function flag(env: NodeJS.ProcessEnv, name: string): boolean {
  const v = env[name]?.trim().toLowerCase();
  return v === '1' || v === 'true' || v === 'yes';
}

export function sellerNodeOptionsFromEnv(env: NodeJS.ProcessEnv): SellerNodeOptions {
  const mock = flag(env, 'MOCK');
  const sellerToken = env.SELLER_TOKEN?.trim() || undefined;
  const upstreamBaseUrl = env.UPSTREAM_BASE_URL?.trim() || undefined;
  if (!mock && !upstreamBaseUrl) throw new Error('UPSTREAM_BASE_URL is required unless MOCK=1');
  if (!mock && !sellerToken) {
    throw new Error('SELLER_TOKEN is required when proxying a real upstream (it is the bearer the market presents)');
  }
  if (upstreamBaseUrl) {
    try {
      const u = new URL(upstreamBaseUrl);
      if (u.protocol !== 'http:' && u.protocol !== 'https:') throw new Error();
    } catch {
      throw new Error('UPSTREAM_BASE_URL must be an http(s) URL');
    }
    // Advisory, like the market's own check: an endpoint offer is your own inference server, not a hosted API.
    const provider = knownProviderOf(upstreamBaseUrl);
    if (provider) {
      throw new Error(
        `UPSTREAM_BASE_URL points at ${provider}, a hosted inference API. Sell your own server's inference (Ollama, vLLM, SGLang); reselling a provider's API needs that provider's written approval and the market's resale allowlist.`,
      );
    }
  }
  const opts: SellerNodeOptions = {
    port: num(env, 'PORT', mock ? 9101 : 9200, 0, 65535),
    host: env.HOST?.trim() || '127.0.0.1',
    modelMap: parseModelMap(env.MODEL_MAP),
  };
  if (sellerToken) opts.sellerToken = sellerToken;
  if (upstreamBaseUrl) opts.upstreamBaseUrl = upstreamBaseUrl;
  if (env.UPSTREAM_API_KEY?.trim()) opts.upstreamApiKey = env.UPSTREAM_API_KEY.trim();
  if (mock) {
    opts.mock = {
      latencyMs: num(env, 'MOCK_LATENCY_MS', 0, 0, 600_000),
      chunkDelayMs: num(env, 'MOCK_CHUNK_DELAY_MS', 0, 0, 60_000),
      failRate: num(env, 'MOCK_FAIL_RATE', 0, 0, 1),
      seed: num(env, 'MOCK_SEED', 1, 0, 2 ** 32 - 1),
      cachedTokens: num(env, 'MOCK_CACHED_TOKENS', 0, 0, 1e9),
    };
  }
  return opts;
}

export interface ConnectorEnv {
  url: string;
  token: string;
  nodeName: string;
  maxConcurrent: number;
  wallet?: string;
  models: ModelDecl[];
}

const LOOPBACK = new Set(['127.0.0.1', '::1', 'localhost', '[::1]']);

/**
 * MODEL_META: per-model context and quantisation the node actually serves, as JSON keyed by catalog id:
 * {"meta-llama/llama-3.1-8b-instruct":{"maxContextTokens":32768,"quantization":"w4a16"}}.
 */
export function parseModelMeta(raw: string | undefined): Record<string, { maxContextTokens?: number; quantization?: string }> {
  if (raw === undefined || raw.trim() === '') return {};
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error('MODEL_META: invalid JSON');
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) throw new Error('MODEL_META: expected a JSON object');
  const out: Record<string, { maxContextTokens?: number; quantization?: string }> = {};
  for (const [k, v] of Object.entries(parsed)) {
    if (v === null || typeof v !== 'object' || Array.isArray(v)) throw new Error(`MODEL_META: bad entry "${k}"`);
    const e = v as { maxContextTokens?: unknown; quantization?: unknown };
    const m: { maxContextTokens?: number; quantization?: string } = {};
    if (e.maxContextTokens !== undefined) {
      if (typeof e.maxContextTokens !== 'number' || !Number.isInteger(e.maxContextTokens) || e.maxContextTokens < 1) {
        throw new Error(`MODEL_META: "${k}".maxContextTokens must be a positive integer`);
      }
      m.maxContextTokens = e.maxContextTokens;
    }
    if (e.quantization !== undefined) {
      if (typeof e.quantization !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9_.-]{0,31}$/.test(e.quantization)) {
        throw new Error(`MODEL_META: "${k}".quantization must be a short name such as fp8, awq or q4_k_m`);
      }
      m.quantization = e.quantization;
    }
    out[k.trim()] = m;
  }
  return out;
}

/**
 * Connector mode, or null when CONNECT_URL is unset. CONNECT_URL must be wss:// (ws:// only for a market on this
 * machine), with no credentials or query; CONNECT_TOKEN is the node token (icn_…). MODEL_MAP must name the models the
 * node serves (they are declared to the market), and the node's own server must listen on loopback only: connector
 * mode needs no inbound port.
 */
export function connectorOptionsFromEnv(env: NodeJS.ProcessEnv, node: Pick<SellerNodeOptions, 'host' | 'modelMap'>): ConnectorEnv | null {
  const url = env.CONNECT_URL?.trim();
  const token = env.CONNECT_TOKEN?.trim();
  if (!url) {
    if (token) throw new Error('CONNECT_TOKEN is set but CONNECT_URL is not');
    return null;
  }
  let u: URL;
  try {
    u = new URL(url);
  } catch {
    throw new Error('CONNECT_URL must be a wss:// URL');
  }
  if (u.protocol !== 'wss:' && !(u.protocol === 'ws:' && LOOPBACK.has(u.hostname))) throw new Error('CONNECT_URL must use wss:// (ws:// only for a market on this machine)');
  if (u.username || u.password || u.search || u.hash) throw new Error('CONNECT_URL must not carry credentials, a query or a fragment; the token goes in CONNECT_TOKEN');
  if (!token || !NODE_TOKEN_RE.test(token)) throw new Error('CONNECT_TOKEN must be the node token (icn_…) from POST /v1/seller/nodes');
  if (!LOOPBACK.has(node.host ?? '127.0.0.1')) throw new Error('HOST must be 127.0.0.1 with CONNECT_URL: connector mode needs no inbound port');
  const map = node.modelMap ?? {};
  const ids = Object.keys(map);
  if (ids.length === 0) throw new Error('MODEL_MAP is required with CONNECT_URL: it declares the models this node serves');
  const meta = parseModelMeta(env.MODEL_META);
  for (const k of Object.keys(meta)) if (!ids.includes(k)) throw new Error(`MODEL_META: "${k}" is not in MODEL_MAP`);
  const nodeName = env.NODE_NAME?.trim() || 'seller-node';
  if (nodeName.length > 64 || !/^[\w .:@()-]+$/.test(nodeName)) throw new Error('NODE_NAME: at most 64 letters, digits, spaces and . : @ ( ) - _');
  const wallet = env.CONNECT_WALLET?.trim();
  if (wallet && !/^0x[0-9a-fA-F]{40}$/.test(wallet)) throw new Error('CONNECT_WALLET must be a 0x-prefixed 20-byte address');
  return {
    url,
    token,
    nodeName,
    maxConcurrent: num(env, 'CONNECT_MAX_CONCURRENCY', 4, 1, 256),
    ...(wallet ? { wallet } : {}),
    models: ids.map((id) => ({ id, ...(meta[id] ?? {}) })),
  };
}
