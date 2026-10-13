// Mapping local model names to catalog ids (src/catalog.ts): exact aliases, tag and quantisation variants, Hugging
// Face and LM Studio names, Ollama's role-less library names, the size check, cloud and embedding models, duplicates.
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import { describe, expect, it } from 'vitest';
import { fetchCatalog, mapModels, matchKeys, matchModel, nameTokens, parseCatalog, type CatalogEntry } from '../src/catalog.ts';
import type { LocalModel } from '../src/detect.ts';
import { CATALOG_BODY } from './fixtures.ts';

const CATALOG = parseCatalog(CATALOG_BODY);

const ollama = (name: string, extra: Partial<LocalModel> = {}): LocalModel => ({ name, server: 'ollama', baseUrl: 'http://127.0.0.1:11434/v1', chat: true, ...extra });
const other = (name: string, server: LocalModel['server'] = 'vllm'): LocalModel => ({ name, server, baseUrl: 'http://127.0.0.1:8000/v1', chat: true });
const id = (m: LocalModel) => matchModel(m, CATALOG)?.id ?? null;

describe('parseCatalog', () => {
  it('keeps servable catalog rows and leaves out model auto and reference-only rows', () => {
    expect(CATALOG.map((c) => c.id)).not.toContain('auto');
    expect(CATALOG.map((c) => c.id)).not.toContain('anthropic/claude-sonnet-5.5');
    expect(CATALOG.find((c) => c.id === 'qwen/qwen-2.5-14b-instruct')).toEqual({
      id: 'qwen/qwen-2.5-14b-instruct',
      aliases: ['qwen2.5:14b-instruct', 'qwen2.5:14b', 'qwen2.5-14b-instruct', 'Qwen/Qwen2.5-14B-Instruct'],
      contextLength: 32768,
    });
    expect(() => parseCatalog({ nope: 1 })).toThrow(/unexpected model list/);
  });
});

describe('the normaliser', () => {
  it('drops the org, :latest, and format and quantisation tags', () => {
    expect(nameTokens('qwen2.5:14b-instruct-q4_K_M')).toEqual(['qwen2.5', '14b', 'instruct']);
    expect(nameTokens('lmstudio-community/Qwen2.5-14B-Instruct-GGUF')).toEqual(['qwen2.5', '14b', 'instruct']);
    expect(nameTokens('mlx-community/Qwen2.5-14B-Instruct-4bit')).toEqual(['qwen2.5', '14b', 'instruct']);
    expect(nameTokens('qwen2.5-14b-instruct@q8_0')).toEqual(['qwen2.5', '14b', 'instruct']);
    expect(nameTokens('hf.co/bartowski/Meta-Llama-3.1-8B-Instruct-GGUF:Q6_K')).toEqual(['meta', 'llama', '3.1', '8b', 'instruct']);
    expect(nameTokens('mistral-nemo:latest')).toEqual(['mistral', 'nemo']);
    expect(nameTokens('Meta-Llama-3.1-8B-Instruct.Q4_K_M.gguf')).toEqual(['meta', 'llama', '3.1', '8b', 'instruct']);
  });

  it('gives qwen2.5 and qwen-2.5 the same key, and marks base models', () => {
    expect(matchKeys('qwen2.5:14b-instruct').full).toBe(matchKeys('qwen/qwen-2.5-14b-instruct').full);
    expect(matchKeys('google/gemma-3-27b-it').noRole).toBe(matchKeys('gemma3:27b').noRole);
    expect(matchKeys('qwen2.5:14b-text').noRole).toBeNull();
    expect(matchKeys('qwen3-coder:30b')).toMatchObject({ size: 30, sizeKnown: true });
    expect(matchKeys('meta-llama/Llama-4-Maverick-17B-128E-Instruct')).toMatchObject({ size: null, sizeKnown: false });
  });
});

describe('matchModel', () => {
  it('matches exact catalog ids and aliases, case-insensitively, with :latest dropped', () => {
    expect(id(ollama('qwen2.5:14b-instruct'))).toBe('qwen/qwen-2.5-14b-instruct');
    expect(id(ollama('qwen2.5:14b'))).toBe('qwen/qwen-2.5-14b-instruct');
    expect(id(other('Qwen/Qwen2.5-14B-Instruct'))).toBe('qwen/qwen-2.5-14b-instruct');
    expect(id(other('qwen/qwen3-14b', 'custom'))).toBe('qwen/qwen3-14b');
    expect(matchModel(ollama('qwen2.5:14b-instruct'), CATALOG)).toEqual({ id: 'qwen/qwen-2.5-14b-instruct', exact: true });
  });

  it('matches tag and quantisation variants and the usual LM Studio, MLX and vLLM names', () => {
    expect(id(ollama('qwen2.5:14b-instruct-q8_0'))).toBe('qwen/qwen-2.5-14b-instruct');
    expect(id(other('Qwen/Qwen2.5-14B-Instruct-AWQ'))).toBe('qwen/qwen-2.5-14b-instruct');
    expect(id(other('lmstudio-community/Qwen2.5-14B-Instruct-GGUF', 'lmstudio'))).toBe('qwen/qwen-2.5-14b-instruct');
    expect(id(other('qwen2.5-14b-instruct@q4_k_m', 'lmstudio'))).toBe('qwen/qwen-2.5-14b-instruct');
    expect(id(other('mlx-community/Qwen2.5-72B-Instruct-4bit', 'lmstudio'))).toBe('qwen/qwen-2.5-72b-instruct');
    expect(id(other('meta-llama-3.1-8b-instruct', 'lmstudio'))).toBe('meta-llama/llama-3.1-8b-instruct');
    expect(id(other('openai/gpt-oss-20b'))).toBe('openai/gpt-oss-20b');
    expect(id(other('google/gemma-3-27b-it', 'llamacpp'))).toBe('google/gemma-3-27b-it');
  });

  it("matches Ollama's library names, which leave out -instruct and -it", () => {
    expect(id(ollama('llama3.1:8b'))).toBe('meta-llama/llama-3.1-8b-instruct');
    expect(id(ollama('llama3.1:8b-instruct-q4_K_M'))).toBe('meta-llama/llama-3.1-8b-instruct');
    expect(id(ollama('llama3.3:70b'))).toBe('meta-llama/llama-3.3-70b-instruct');
    expect(id(ollama('gemma3:27b'))).toBe('google/gemma-3-27b-it');
    expect(id(ollama('gemma3:27b-it-qat'))).toBe('google/gemma-3-27b-it');
    expect(id(ollama('gpt-oss:20b'))).toBe('openai/gpt-oss-20b');
    expect(id(ollama('qwen3:14b'))).toBe('qwen/qwen3-14b');
    expect(id(ollama('qwen2.5:72b'))).toBe('qwen/qwen-2.5-72b-instruct');
    expect(id(ollama('mistral-small3.2:24b'))).toBe('mistralai/mistral-small-3.2-24b-instruct');
  });

  it('never maps a base model to the instruct model', () => {
    expect(id(ollama('qwen2.5:14b-text'))).toBeNull();
    expect(id(other('Qwen/Qwen2.5-14B'))).toBeNull(); // a Hugging Face name with no -Instruct is the base model
    expect(id(other('Meta-Llama-3.1-8B', 'lmstudio'))).toBeNull();
  });

  it('checks the parameter count when an Ollama name carries no size', () => {
    expect(id(ollama('qwen3-coder:latest', { parameterB: 30.5 }))).toBeNull(); // the 30B model, not the 480B one
    expect(id(ollama('qwen3-coder', { parameterB: 480 }))).toBe('qwen/qwen3-coder');
    expect(id(ollama('mistral-nemo:latest', { parameterB: 12.2 }))).toBe('mistralai/mistral-nemo'); // the catalog names no size
    expect(id(ollama('mistral-nemo:12b', { parameterB: 12.2 }))).toBe('mistralai/mistral-nemo');
    expect(id(ollama('llama4:maverick', { parameterB: 401.6 }))).toBe('meta-llama/llama-4-maverick'); // 17b-128e: no total size
  });

  it("maps Ollama's default tags (no size in the name) by their parameter count, only when one catalog size fits", () => {
    expect(id(ollama('llama3.1:latest', { parameterB: 8.0 }))).toBe('meta-llama/llama-3.1-8b-instruct'); // ollama pull llama3.1
    expect(id(ollama('llama3.1', { parameterB: 8.0 }))).toBe('meta-llama/llama-3.1-8b-instruct');
    expect(id(ollama('qwen2.5:latest', { parameterB: 7.6 }))).toBeNull(); // the 7B: neither the 14B nor the 72B
    expect(id(ollama('qwen2.5:latest', { parameterB: 14.8 }))).toBe('qwen/qwen-2.5-14b-instruct');
    expect(id(ollama('qwen2.5:latest'))).toBeNull(); // no parameter count: no guess
    expect(id(ollama('llama3.1:text', { parameterB: 8.0 }))).toBeNull(); // the base model
    expect(id(other('llama3.1', 'vllm'))).toBeNull(); // Ollama names only
  });

  it('leaves models the catalog does not list unmatched, and never guesses between two ids', () => {
    expect(id(ollama('deepseek-r1:14b'))).toBeNull();
    expect(id(ollama('nomic-embed-text'))).toBeNull();
    const twins: CatalogEntry[] = [
      { id: 'a/model-7b', aliases: ['shared:7b'] },
      { id: 'b/model-7b', aliases: ['shared:7b'] },
    ];
    expect(matchModel(ollama('shared:7b'), twins)).toBeNull();
    expect(matchModel(ollama('model:7b'), twins)).toBeNull();
  });
});

describe('mapModels', () => {
  it('serves one local model per catalog id and reports the rest', () => {
    const m = mapModels(
      [
        ollama('qwen2.5:14b-instruct-q8_0', { quantization: 'q8_0', sizeBytes: 15_700_000_000 }),
        ollama('qwen2.5:14b-instruct', { quantization: 'q4_k_m', sizeBytes: 8_988_124_069, loaded: true, contextLength: 4096 }),
        ollama('deepseek-r1:14b'),
        ollama('nomic-embed-text', { chat: false }),
        ollama('gpt-oss:120b-cloud', { remote: true }),
        { ...other('meta-llama-3.1-8b-instruct', 'lmstudio'), baseUrl: 'http://127.0.0.1:1234/v1' },
      ],
      CATALOG,
    );
    expect(m.served).toEqual([
      { catalogId: 'meta-llama/llama-3.1-8b-instruct', local: 'meta-llama-3.1-8b-instruct', server: 'lmstudio', baseUrl: 'http://127.0.0.1:1234/v1' },
      { catalogId: 'qwen/qwen-2.5-14b-instruct', local: 'qwen2.5:14b-instruct', server: 'ollama', baseUrl: 'http://127.0.0.1:11434/v1', quantization: 'q4_k_m', contextLength: 4096 },
    ]);
    // The served copy comes first among the two that map to one catalog id (the Sell workspace keeps the first).
    expect(m.detected).toEqual([
      { local: 'qwen2.5:14b-instruct', server: 'ollama', model: 'qwen/qwen-2.5-14b-instruct', contextLength: 4096, quantization: 'q4_k_m', sizeBytes: 8_988_124_069 },
      { local: 'qwen2.5:14b-instruct-q8_0', server: 'ollama', model: 'qwen/qwen-2.5-14b-instruct', quantization: 'q8_0', sizeBytes: 15_700_000_000 },
      { local: 'deepseek-r1:14b', server: 'ollama', model: null },
      { local: 'nomic-embed-text', server: 'ollama', model: null },
      { local: 'meta-llama-3.1-8b-instruct', server: 'lmstudio', model: 'meta-llama/llama-3.1-8b-instruct' },
    ]);
    expect(m.notes.join('\n')).toMatch(/gpt-oss:120b-cloud: an Ollama cloud model/);
    expect(m.notes.join('\n')).toMatch(/nomic-embed-text: not a chat model/);
    expect(m.notes.join('\n')).toMatch(/qwen2.5:14b-instruct-q8_0: also qwen\/qwen-2.5-14b-instruct; serving qwen2.5:14b-instruct/);
  });

  it('prefers an exact name, then the shorter name, when none is loaded', () => {
    const m = mapModels([ollama('qwen2.5:14b-instruct-q8_0'), ollama('qwen2.5:14b-instruct')], CATALOG);
    expect(m.served.map((s) => s.local)).toEqual(['qwen2.5:14b-instruct']);
  });
});

describe('fetchCatalog', () => {
  it('reads GET <api>/v1/models and explains a market it cannot reach', async () => {
    const server = createServer((req, res) => {
      if (req.url === '/v1/models') {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify(CATALOG_BODY));
        return;
      }
      res.writeHead(500).end();
    });
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
    const api = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    try {
      expect((await fetchCatalog(`${api}/`)).length).toBe(CATALOG.length);
    } finally {
      await new Promise<void>((r) => server.close(() => r()));
    }
    await expect(fetchCatalog(api)).rejects.toThrow(/could not reach the market at http:\/\/127\.0\.0\.1:\d+ \(ECONNREFUSED\)/);
    const failing = (async () => new Response('x', { status: 503 })) as unknown as typeof fetch;
    await expect(fetchCatalog('https://api.example', failing)).rejects.toThrow(/answered HTTP 503/);
  });
});
