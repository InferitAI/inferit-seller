// Reference seller sidecar (SPEC §6): an OpenAI-compatible front for a seller's own inference server
// (Ollama / vLLM / SGLang), or a deterministic mock (MOCK=1) for local development and tests.
//
// The market presents `Authorization: Bearer <SELLER_TOKEN>`. Request and response bodies are never logged.
import { timingSafeEqual, createHash } from 'node:crypto';
import { Readable } from 'node:stream';
import { setTimeout as sleep } from 'node:timers/promises';
import Fastify, { type FastifyInstance, type FastifyReply, type FastifyRequest } from 'fastify';
import { type ChatRequest, mockCompletion, mockCompletionJson, mockStreamEvents, prng } from './mock.js';
import { fingerprintOf } from './providers.js';
import { PACKAGE_VERSION } from './version.js';

export { mockCompletion, mockCompletionJson, mockPromptTokens, mockReplyTokens, mockStreamEvents } from './mock.js';
export type { ChatMessage, ChatRequest, MockCompletion, MockUsage } from './mock.js';
export { sellerNodeOptionsFromEnv, parseModelMap, connectorOptionsFromEnv, parseModelMeta, type ConnectorEnv } from './config.js';
export {
  SellerConnector,
  reconnectDelayMs,
  encodeData,
  realTimers,
  buildHello,
  sanitizeDetected,
  describeFailure,
  BACKOFF_STEPS_MS,
  MAX_BACKOFF_MS,
  CONNECTOR_SUBPROTOCOL,
  NODE_TOKEN_RE,
  WATCHDOG_NO_SESSION_MS,
  type AgentInfo,
  type AttemptFailure,
  type ConnectorOptions,
  type ConnectorStatus,
  type DetectedModel,
  type DetectedServer,
  type ModelDecl,
  type Timers,
  type WatchdogInfo,
  type WatchdogOptions,
} from './connector.js';
export { fingerprintOf, isFingerprintHeader, knownProviderOf, PROVIDER_DOMAINS, FINGERPRINT_HEADERS, FINGERPRINT_PREFIXES } from './providers.js';
export { detectLocal, KNOWN_SERVERS, SERVER_LABEL, type LocalModel, type LocalServer, type Probe } from './detect.js';
export { fetchCatalog, parseCatalog, mapModels, matchKeys, type CatalogEntry, type ModelMapping, type ServedModel } from './catalog.js';
export { AGENT_NAME, PACKAGE_NAME, PACKAGE_VERSION } from './version.js';

/** Reported to the market in the connector hello (`node.version`): the package version. */
export const SELLER_NODE_VERSION = PACKAGE_VERSION;

export interface MockOptions {
  /** Delay before the first byte of every completion (simulated TTFT). Default 0. */
  latencyMs?: number;
  /** Delay between streamed chunks. Default 0. */
  chunkDelayMs?: number;
  /** Probability (0..1) that a completion fails with HTTP 500 before any byte. Default 0. */
  failRate?: number;
  /** Seed for the fail-rate PRNG. Default 1. */
  seed?: number;
  /** Prompt tokens reported as cache hits (`prompt_tokens_details.cached_tokens`). Default 0 (field omitted). */
  cachedTokens?: number;
  /** Clock (epoch ms) used for `created`; fix it to get byte-identical output. Default Date.now. */
  clock?: () => number;
}

export interface SellerNodeOptions {
  port?: number;
  host?: string;
  /** Bearer token the market must present. When unset, requests are not authenticated (mock/dev only). */
  sellerToken?: string;
  /** Deterministic mock mode. `true` uses defaults. */
  mock?: boolean | MockOptions;
  /** OpenAI-compatible upstream base URL, e.g. http://localhost:11434/v1 (required unless mock). */
  upstreamBaseUrl?: string;
  upstreamApiKey?: string;
  /** catalogId -> upstream model name. Requests for other models get 404 (empty map in mock mode = any model). */
  modelMap?: Record<string, string>;
  /** catalogId -> the OpenAI base URL that serves it, when this machine runs more than one server (Ollama and LM
   *  Studio, say). Models not listed here go to upstreamBaseUrl. */
  upstreams?: Record<string, string>;
  /** Fastify logger option. Default false (silent); main.ts enables pino with header redaction. */
  logger?: boolean | Record<string, unknown>;
  /** Max request body bytes. Default 10 MiB. */
  bodyLimit?: number;
  /** Upstream timeout for the response headers, ms. Default 600000. */
  upstreamTimeoutMs?: number;
  fetchImpl?: typeof fetch;
}

export interface SellerNode {
  /** Origin, e.g. http://127.0.0.1:9101. The OpenAI base URL to list as an offer endpoint is `${url}/v1`. */
  url: string;
  /** `${url}/v1` */
  baseUrl: string;
  app: FastifyInstance;
  close(): Promise<void>;
}

/** Pino options shared with main.ts: auth headers are redacted and bodies are never serialised. */
export const SELLER_LOG_REDACT = ['req.headers.authorization', 'req.headers["x-api-key"]', 'req.headers.cookie'];

function openAiError(reply: FastifyReply, status: number, message: string, type: string, code: string) {
  return reply.code(status).send({ error: { message, type, code } });
}

function tokenMatches(presented: string, expected: string): boolean {
  // Compare fixed-length digests so neither the length nor the content leaks through timing.
  const a = createHash('sha256').update(presented).digest();
  const b = createHash('sha256').update(expected).digest();
  return timingSafeEqual(a, b);
}

function bearer(req: FastifyRequest): string | undefined {
  const h = req.headers.authorization;
  if (typeof h !== 'string') return undefined;
  const m = /^Bearer\s+(.+)$/i.exec(h.trim());
  return m?.[1]?.trim();
}

/** The catalog key and upstream model for a requested catalog id; with no map the id passes through unchanged.
 *  null = not served. */
function resolveModel(map: Record<string, string>, requested: string): { key: string; upstream: string } | null {
  const entries = Object.entries(map);
  if (entries.length === 0) return { key: requested, upstream: requested };
  const hit = entries.find(([k]) => k.toLowerCase() === requested.toLowerCase());
  return hit ? { key: hit[0], upstream: hit[1] } : null;
}

function validateChat(body: unknown): { ok: true; req: ChatRequest } | { ok: false; message: string } {
  if (body === null || typeof body !== 'object' || Array.isArray(body)) return { ok: false, message: 'Body must be a JSON object.' };
  const b = body as Record<string, unknown>;
  if (typeof b.model !== 'string' || b.model.trim() === '') return { ok: false, message: '`model` is required.' };
  if (!Array.isArray(b.messages) || b.messages.length === 0) return { ok: false, message: '`messages` must be a non-empty array.' };
  for (const m of b.messages) {
    if (m === null || typeof m !== 'object' || typeof (m as { role?: unknown }).role !== 'string') {
      return { ok: false, message: 'Each message needs a string `role`.' };
    }
  }
  for (const k of ['max_tokens', 'max_completion_tokens'] as const) {
    const v = b[k];
    if (v !== undefined && v !== null && (typeof v !== 'number' || !Number.isInteger(v) || v < 0)) {
      return { ok: false, message: `\`${k}\` must be a non-negative integer.` };
    }
  }
  return { ok: true, req: b as ChatRequest };
}

/** Builds the Fastify app without listening (use createSellerNode to listen). */
export function buildSellerApp(opts: SellerNodeOptions): FastifyInstance {
  const mockOpts: MockOptions | null = opts.mock ? (opts.mock === true ? {} : opts.mock) : null;
  if (!mockOpts && !opts.upstreamBaseUrl) throw new Error('seller-node: upstreamBaseUrl is required unless mock mode is on');
  const failRate = mockOpts?.failRate ?? 0;
  if (!(failRate >= 0 && failRate <= 1)) throw new Error('seller-node: mock failRate must be within 0..1');
  const rand = prng(mockOpts?.seed ?? 1);
  const clock = mockOpts?.clock ?? Date.now;
  const modelMap = opts.modelMap ?? {};
  const upstreamBase = opts.upstreamBaseUrl?.replace(/\/+$/, '');
  const upstreams = Object.fromEntries(Object.entries(opts.upstreams ?? {}).map(([k, v]) => [k, v.replace(/\/+$/, '')]));
  const doFetch = opts.fetchImpl ?? fetch;

  const app = Fastify({
    logger: opts.logger ?? false,
    bodyLimit: opts.bodyLimit ?? 10 * 1024 * 1024,
  });

  app.addHook('onRequest', async (req, reply) => {
    if (req.url === '/health' || !opts.sellerToken) return;
    const tok = bearer(req);
    if (!tok || !tokenMatches(tok, opts.sellerToken)) {
      return openAiError(reply, 401, 'Invalid or missing seller token.', 'authentication_error', 'invalid_seller_token');
    }
  });

  app.get('/health', async () => ({ ok: true, mode: mockOpts ? 'mock' : 'proxy', models: Object.keys(modelMap) }));

  app.get('/v1/models', async () => ({
    object: 'list',
    data: Object.keys(modelMap).map((id) => ({ id, object: 'model', created: 0, owned_by: 'inferit-seller' })),
  }));

  app.post('/v1/chat/completions', async (request, reply) => {
    const v = validateChat(request.body);
    if (!v.ok) return openAiError(reply, 400, v.message, 'invalid_request_error', 'invalid_request');
    const req = v.req;
    const resolved = resolveModel(modelMap, req.model);
    if (resolved === null) {
      return openAiError(reply, 404, `Model "${req.model}" is not served by this seller.`, 'invalid_request_error', 'model_not_found');
    }

    if (mockOpts) {
      if ((mockOpts.latencyMs ?? 0) > 0) await sleep(mockOpts.latencyMs);
      if (failRate > 0 && rand() < failRate) {
        return openAiError(reply, 500, 'Mock failure (MOCK_FAIL_RATE).', 'server_error', 'mock_failure');
      }
      const c = mockCompletion(req, clock(), mockOpts.cachedTokens ?? 0);
      if (!req.stream) return reply.code(200).header('content-type', 'application/json').send(mockCompletionJson(c));
      const events = mockStreamEvents(c, req.stream_options?.include_usage === true);
      const delay = mockOpts.chunkDelayMs ?? 0;
      async function* gen() {
        for (let i = 0; i < events.length; i++) {
          if (i > 0 && delay > 0) await sleep(delay);
          yield Buffer.from(events[i]!, 'utf8');
        }
      }
      reply.code(200).header('content-type', 'text/event-stream; charset=utf-8').header('cache-control', 'no-cache');
      return reply.send(Readable.from(gen()));
    }

    // Proxy mode: forward to the seller's own OpenAI-compatible server with the model renamed.
    const ac = new AbortController();
    const onClose = () => {
      if (!reply.raw.writableFinished) ac.abort();
    };
    reply.raw.on('close', onClose);
    const timer = setTimeout(() => ac.abort(), opts.upstreamTimeoutMs ?? 600_000);
    let res: Response;
    try {
      const headers: Record<string, string> = { 'content-type': 'application/json', accept: req.stream ? 'text/event-stream' : 'application/json' };
      if (opts.upstreamApiKey) headers.authorization = `Bearer ${opts.upstreamApiKey}`;
      res = await doFetch(`${upstreams[resolved.key] ?? upstreamBase}/chat/completions`, {
        method: 'POST',
        headers,
        body: JSON.stringify({ ...req, model: resolved.upstream }),
        signal: ac.signal,
      });
    } catch {
      clearTimeout(timer);
      request.log.warn('upstream unreachable');
      return openAiError(reply, 502, 'Upstream inference server is unreachable.', 'server_error', 'upstream_unreachable');
    }
    clearTimeout(timer);
    const ctype = res.headers.get('content-type') ?? 'application/json';
    // A hosted provider's fingerprint headers (only those) are passed on, so the market can tell a proxy in front of a
    // provider API from a seller's own server (its probe holds such an offer for review).
    for (const [k, v] of Object.entries(fingerprintOf(res.headers.entries()))) reply.header(k, v);
    if (!res.ok || !req.stream) {
      const text = await res.text();
      if (res.ok && ctype.includes('application/json')) {
        try {
          const json = JSON.parse(text) as Record<string, unknown>;
          json.model = req.model; // report the catalog id, not the upstream tag
          return reply.code(200).header('content-type', 'application/json').send(json);
        } catch {
          return openAiError(reply, 502, 'Upstream returned malformed JSON.', 'server_error', 'upstream_bad_response');
        }
      }
      return reply.code(res.status).header('content-type', ctype).send(text);
    }
    if (!res.body) return openAiError(reply, 502, 'Upstream returned no body.', 'server_error', 'upstream_bad_response');
    reply.code(200).header('content-type', ctype).header('cache-control', 'no-cache');
    // Stream bytes through untouched (model names inside chunks stay as the upstream reports them).
    return reply.send(Readable.fromWeb(res.body as import('node:stream/web').ReadableStream<Uint8Array>));
  });

  return app;
}

/** Starts a seller node and resolves once it is listening (port 0 = ephemeral). */
export async function createSellerNode(opts: SellerNodeOptions = { mock: true }): Promise<SellerNode> {
  const app = buildSellerApp(opts);
  const host = opts.host ?? '127.0.0.1';
  await app.listen({ port: opts.port ?? 0, host });
  const addr = app.server.address();
  const port = typeof addr === 'object' && addr ? addr.port : opts.port ?? 0;
  const hostForUrl = host === '0.0.0.0' || host === '::' ? '127.0.0.1' : host.includes(':') ? `[${host}]` : host;
  const url = `http://${hostForUrl}:${port}`;
  return {
    url,
    baseUrl: `${url}/v1`,
    app,
    close: async () => {
      await app.close();
    },
  };
}
