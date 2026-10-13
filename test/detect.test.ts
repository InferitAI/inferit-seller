// Local server detection (src/detect.ts) against mock Ollama, LM Studio, vLLM and llama.cpp servers on ephemeral
// ports (through --upstream), and the default-port scan through an injected fetch (this machine may run a real Ollama).
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterEach, describe, expect, it } from 'vitest';
import { detectLocal, normaliseQuant, parseParameterSize, upstreamBase } from '../src/detect.ts';
import { OLLAMA_ROUTES, closedPort, jsonServer, type Routes } from './fixtures.ts';

const servers: Server[] = [];
afterEach(async () => {
  await Promise.all(servers.splice(0).map((s) => new Promise<void>((r) => s.close(() => r()))));
});
async function serve(routes: Routes) {
  const s = await jsonServer(routes);
  servers.push(s.server);
  return s;
}

describe('helpers', () => {
  it('parses parameter sizes and quantisation names', () => {
    expect(parseParameterSize('14.8B')).toBe(14.8);
    expect(parseParameterSize('137M')).toBeCloseTo(0.137);
    expect(parseParameterSize('1.2T')).toBe(1200);
    expect(parseParameterSize('big')).toBeUndefined();
    expect(normaliseQuant('Q4_K_M')).toBe('q4_k_m');
    expect(normaliseQuant('4 bit!')).toBeUndefined();
  });

  it('reads an --upstream as an OpenAI base URL', () => {
    expect(upstreamBase('http://127.0.0.1:11434')).toBe('http://127.0.0.1:11434/v1');
    expect(upstreamBase('http://127.0.0.1:11434/v1/')).toBe('http://127.0.0.1:11434/v1');
    expect(upstreamBase('https://box.lan/llm/v1')).toBe('https://box.lan/llm/v1');
    expect(() => upstreamBase('ftp://x')).toThrow(/http/);
    expect(() => upstreamBase('http://u:p@x/v1')).toThrow(/credentials/);
  });
});

describe('detectLocal', () => {
  it('reads Ollama: names, sizes, quantisation, parameter count, the loaded context, cloud and embedding models', async () => {
    const o = await serve(OLLAMA_ROUTES);
    const { servers: found, probes } = await detectLocal({ upstream: `${o.origin}/v1` });
    expect(probes).toEqual([{ kind: 'ollama', url: o.origin, ok: true, models: 3 }]);
    expect(found).toHaveLength(1);
    expect(found[0]).toMatchObject({ kind: 'ollama', baseUrl: `${o.origin}/v1`, version: '0.40.3' });
    expect(found[0]!.models).toEqual([
      { name: 'qwen2.5:14b-instruct', server: 'ollama', baseUrl: `${o.origin}/v1`, chat: true, quantization: 'q4_k_m', sizeBytes: 8988124069, parameterB: 14.8, maxContextLength: 32768, loaded: true, contextLength: 4096 },
      { name: 'nomic-embed-text:latest', server: 'ollama', baseUrl: `${o.origin}/v1`, chat: false, quantization: 'f16', sizeBytes: 274302450, parameterB: 0.137 },
      { name: 'gpt-oss:120b-cloud', server: 'ollama', baseUrl: `${o.origin}/v1`, chat: true, quantization: 'mxfp4', sizeBytes: 384, parameterB: 116.8, remote: true },
    ]);
  });

  it('reads LM Studio through its /api/v0 models (type, quantisation, loaded context)', async () => {
    const lm = await serve({
      '/v1/models': { object: 'list', data: [{ id: 'qwen2.5-14b-instruct', object: 'model', owned_by: 'organization_owner' }, { id: 'text-embedding-nomic-embed-text-v1.5', owned_by: 'organization_owner' }] },
      '/api/v0/models': {
        data: [
          { id: 'qwen2.5-14b-instruct', type: 'llm', quantization: 'Q4_K_M', state: 'loaded', max_context_length: 32768, loaded_context_length: 8192 },
          { id: 'text-embedding-nomic-embed-text-v1.5', type: 'embeddings', state: 'not-loaded', max_context_length: 2048 },
        ],
      },
    });
    const { servers: found } = await detectLocal({ upstream: `${lm.origin}/v1` });
    expect(found[0]!.kind).toBe('lmstudio');
    expect(found[0]!.models).toEqual([
      { name: 'qwen2.5-14b-instruct', server: 'lmstudio', baseUrl: `${lm.origin}/v1`, chat: true, quantization: 'q4_k_m', maxContextLength: 32768, loaded: true, contextLength: 8192 },
      { name: 'text-embedding-nomic-embed-text-v1.5', server: 'lmstudio', baseUrl: `${lm.origin}/v1`, chat: false, maxContextLength: 2048 },
    ]);
  });

  it('tells vLLM and llama.cpp apart by their model lists, and anything else is custom', async () => {
    const vllm = await serve({ '/v1/models': { object: 'list', data: [{ id: 'Qwen/Qwen2.5-14B-Instruct-AWQ', owned_by: 'vllm', max_model_len: 16384 }] } });
    const llama = await serve({
      '/v1/models': { object: 'list', data: [{ id: 'qwen2.5-14b-instruct-q4_k_m.gguf', owned_by: 'llamacpp', meta: { n_ctx_train: 32768, n_params: 14_770_033_664, size: 8_988_000_000 } }] },
      '/props': { default_generation_settings: { n_ctx: 8192 } },
    });
    const custom = await serve({ '/v1/models': { data: [{ id: 'my-model' }] } });
    const v = (await detectLocal({ upstream: `${vllm.origin}/v1` })).servers[0]!;
    expect(v.kind).toBe('vllm');
    expect(v.models[0]).toMatchObject({ server: 'vllm', contextLength: 16384 });
    const l = (await detectLocal({ upstream: llama.origin })).servers[0]!;
    expect(l.kind).toBe('llamacpp');
    expect(l.models[0]).toMatchObject({ server: 'llamacpp', contextLength: 8192, maxContextLength: 32768, sizeBytes: 8_988_000_000 });
    expect(l.models[0]!.parameterB).toBeCloseTo(14.77, 2);
    const c = (await detectLocal({ upstream: `${custom.origin}/v1` })).servers[0]!;
    expect(c.kind).toBe('custom');
    expect(c.models.map((m) => m.name)).toEqual(['my-model']);
  });

  it('sends UPSTREAM_API_KEY to a server that needs one', async () => {
    let auth: string | undefined;
    const keyed = createServer((req, res) => {
      auth = req.headers.authorization;
      if (req.url === '/v1/models' && auth === 'Bearer local-key') res.writeHead(200, { 'content-type': 'application/json' }).end('{"data":[{"id":"m","owned_by":"vllm"}]}');
      else res.writeHead(401).end();
    });
    servers.push(keyed);
    await new Promise<void>((r) => keyed.listen(0, '127.0.0.1', r));
    const base = `http://127.0.0.1:${(keyed.address() as AddressInfo).port}/v1`;
    expect((await detectLocal({ upstream: base })).servers).toEqual([]);
    expect((await detectLocal({ upstream: base, apiKey: 'local-key' })).servers[0]!.kind).toBe('vllm');
    expect(auth).toBe('Bearer local-key');
  });

  it('reports a server that is down, with the reason', async () => {
    const r = await detectLocal({ upstream: `http://127.0.0.1:${await closedPort()}/v1`, timeoutMs: 500 });
    expect(r.servers).toEqual([]);
    expect(r.probes[0]).toMatchObject({ ok: false, error: 'nothing listening (connection refused)' });
  });

  it('scans the default ports on each host: Ollama on 11434, LM Studio on 1234, the rest refused', async () => {
    const lmRoutes: Routes = {
      '/v1/models': { data: [{ id: 'qwen2.5-14b-instruct', owned_by: 'organization_owner' }] },
      '/api/v0/models': { data: [{ id: 'qwen2.5-14b-instruct', type: 'llm', quantization: 'Q8_0', state: 'not-loaded' }] },
    };
    const seen: string[] = [];
    const fakeFetch = (async (input: string | URL) => {
      const u = new URL(String(input));
      seen.push(`${u.port}${u.pathname}`);
      const routes = u.port === '11434' ? OLLAMA_ROUTES : u.port === '1234' ? lmRoutes : null;
      if (!routes) throw Object.assign(new TypeError('fetch failed'), { cause: { code: 'ECONNREFUSED' } });
      const body = routes[u.pathname];
      return body === undefined ? new Response('{}', { status: 404 }) : new Response(JSON.stringify(body), { headers: { 'content-type': 'application/json' } });
    }) as unknown as typeof fetch;
    const r = await detectLocal({ hosts: ['box.local'], fetchImpl: fakeFetch });
    expect(r.servers.map((s) => [s.kind, s.baseUrl, s.models.length])).toEqual([
      ['ollama', 'http://box.local:11434/v1', 3],
      ['lmstudio', 'http://box.local:1234/v1', 1],
    ]);
    expect(r.probes.filter((p) => !p.ok).map((p) => [p.kind, p.url, p.error])).toEqual([
      ['vllm', 'http://box.local:8000', 'nothing listening (connection refused)'],
      ['llamacpp', 'http://box.local:8080', 'nothing listening (connection refused)'],
    ]);
    expect(seen).toContain('11434/api/tags');
    expect(seen).not.toContain('11434/v1/models'); // Ollama is read through its own API
  });
});
