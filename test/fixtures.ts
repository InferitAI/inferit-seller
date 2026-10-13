// Shared test fixtures: small JSON servers, a closed port, an Ollama server's answers and a slice of the catalog.
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';

export type Routes = Record<string, unknown>;

/** An HTTP server answering GET <path> with the JSON in `routes`, 404 otherwise. */
export async function jsonServer(routes: Routes): Promise<{ server: Server; origin: string; hits: string[] }> {
  const hits: string[] = [];
  const server = createServer((req, res) => {
    hits.push(req.url ?? '');
    const body = routes[req.url ?? ''];
    if (body === undefined) {
      res.writeHead(404, { 'content-type': 'application/json' }).end('{"error":"not found"}');
      return;
    }
    res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify(body));
  });
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  return { server, origin: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, hits };
}

export const OLLAMA_ROUTES: Routes = {
  '/api/tags': {
    models: [
      {
        name: 'qwen2.5:14b-instruct',
        model: 'qwen2.5:14b-instruct',
        size: 8988124069,
        details: { format: 'gguf', family: 'qwen2', parameter_size: '14.8B', quantization_level: 'Q4_K_M', context_length: 32768 },
        capabilities: ['completion', 'tools'],
      },
      { name: 'nomic-embed-text:latest', size: 274302450, details: { parameter_size: '137M', quantization_level: 'F16' }, capabilities: ['embedding'] },
      { name: 'gpt-oss:120b-cloud', remote_model: 'gpt-oss:120b', remote_host: 'https://ollama.com:443', size: 384, details: { parameter_size: '116.8B', quantization_level: 'MXFP4' } },
    ],
  },
  '/api/ps': { models: [{ name: 'qwen2.5:14b-instruct', size: 9459613039, context_length: 4096 }] },
  '/api/version': { version: '0.40.3' },
  '/v1/models': { object: 'list', data: [{ id: 'qwen2.5:14b-instruct', object: 'model', owned_by: 'library' }] },
};

/** A loopback port nothing listens on. */
export async function closedPort(): Promise<number> {
  const s = createServer();
  await new Promise<void>((r) => s.listen(0, '127.0.0.1', r));
  const port = (s.address() as AddressInfo).port;
  await new Promise<void>((r) => s.close(() => r()));
  return port;
}

// A slice of packages/core/data/catalog.json as GET /v1/models serves it (ids, aliases, context).
export const CATALOG_BODY = {
  object: 'list',
  data: [
    { id: 'auto', owned_by: 'inferit', aliases: ['auto:balanced', 'inferit/auto'], servable: true, auto: { policy: 'balanced' } },
    { id: 'qwen/qwen-2.5-14b-instruct', aliases: ['qwen2.5:14b-instruct', 'qwen2.5:14b', 'qwen2.5-14b-instruct', 'Qwen/Qwen2.5-14B-Instruct'], context_length: 32768, servable: true },
    { id: 'qwen/qwen-2.5-72b-instruct', aliases: ['Qwen/Qwen2.5-72B-Instruct'], context_length: 32768, servable: true },
    { id: 'qwen/qwen3-14b', aliases: [], context_length: 131072, servable: true },
    { id: 'qwen/qwen3-coder', aliases: ['Qwen/Qwen3-Coder-480B-A35B-Instruct'], context_length: 262144, servable: true },
    { id: 'qwen/qwen3-235b-a22b-2507', aliases: ['Qwen/Qwen3-235B-A22B-Instruct-2507'], context_length: 262144, servable: true },
    { id: 'meta-llama/llama-3.1-8b-instruct', aliases: ['meta-llama/Meta-Llama-3.1-8B-Instruct'], context_length: 131072, servable: true },
    { id: 'meta-llama/llama-3.3-70b-instruct', aliases: [], context_length: 131072, servable: true },
    { id: 'meta-llama/llama-4-maverick', aliases: ['meta-llama/Llama-4-Maverick-17B-128E-Instruct'], context_length: 1048576, servable: true },
    { id: 'openai/gpt-oss-20b', aliases: [], context_length: 131072, servable: true },
    { id: 'mistralai/mistral-nemo', aliases: ['mistralai/Mistral-Nemo-Instruct-2407'], context_length: 131072, servable: true },
    { id: 'mistralai/mistral-small-3.2-24b-instruct', aliases: ['mistralai/Mistral-Small-3.2-24B-Instruct-2506'], context_length: 256000, servable: true },
    { id: 'google/gemma-3-27b-it', aliases: [], context_length: 131072, servable: true },
    { id: 'anthropic/claude-sonnet-5.5', aliases: [], context_length: 1000000, servable: false },
  ],
};
