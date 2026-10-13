import { createServer, request, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { Writable } from 'node:stream';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  createSellerNode,
  mockCompletion,
  mockPromptTokens,
  mockReplyTokens,
  mockStreamEvents,
  modelRenamer,
  parseModelMap,
  sellerNodeOptionsFromEnv,
  type SellerNode,
} from '../src/index.ts';
import { closedPort } from './fixtures.ts';

const TOKEN = 'test-seller-token';
const auth = { authorization: `Bearer ${TOKEN}`, 'content-type': 'application/json' };
const FIXED_NOW = 1_790_000_000_000;

function chat(body: Record<string, unknown>) {
  return { model: 'qwen/qwen-2.5-14b-instruct', messages: [{ role: 'user', content: 'Hello there, how are you today?' }], ...body };
}

describe('mock completions (pure)', () => {
  it('counts prompt tokens deterministically', () => {
    expect(mockPromptTokens([{ role: 'user', content: 'abcd' }])).toBe(5);
    expect(mockPromptTokens([{ role: 'system', content: '' }, { role: 'user', content: [{ type: 'text', text: 'abcde' }] }])).toBe(4 + 4 + 2);
  });

  it('truncates at max_tokens and reports finish_reason length', () => {
    const full = mockReplyTokens([{ role: 'user', content: 'x' }]);
    const c = mockCompletion({ model: 'm', messages: [{ role: 'user', content: 'x' }], max_tokens: 3 }, FIXED_NOW);
    expect(c.tokens).toEqual(full.slice(0, 3));
    expect(c.finishReason).toBe('length');
    expect(c.usage.completion_tokens).toBe(3);
    const d = mockCompletion({ model: 'm', messages: [{ role: 'user', content: 'x' }] }, FIXED_NOW);
    expect(d.finishReason).toBe('stop');
    expect(d.usage.completion_tokens).toBe(full.length);
    expect(d.usage.total_tokens).toBe(d.usage.prompt_tokens + full.length);
  });

  it('reports cached tokens clamped to the prompt', () => {
    const c = mockCompletion({ model: 'm', messages: [{ role: 'user', content: 'abcd' }] }, FIXED_NOW, 999);
    expect(c.usage.prompt_tokens_details).toEqual({ cached_tokens: 5 });
  });

  it('emits a usage-only chunk only when include_usage', () => {
    const c = mockCompletion({ model: 'm', messages: [{ role: 'user', content: 'hi' }] }, FIXED_NOW);
    const without = mockStreamEvents(c, false);
    const withU = mockStreamEvents(c, true);
    expect(withU.length).toBe(without.length + 1);
    expect(without.at(-1)).toBe('data: [DONE]\n\n');
    const usageEvent = JSON.parse(withU.at(-2)!.slice(6)) as { choices: unknown[]; usage: unknown };
    expect(usageEvent.choices).toEqual([]);
    expect(usageEvent.usage).toEqual(c.usage);
  });
});

describe('config', () => {
  it('parses MODEL_MAP as JSON or a comma list', () => {
    expect(parseModelMap('{"a/b":"x:1"}')).toEqual({ 'a/b': 'x:1' });
    expect(parseModelMap('a/b=x:1, c=d')).toEqual({ 'a/b': 'x:1', c: 'd' });
    expect(parseModelMap(undefined)).toEqual({});
    expect(() => parseModelMap('nonsense')).toThrow(/MODEL_MAP/);
    expect(() => parseModelMap('{"a":1}')).toThrow(/MODEL_MAP/);
  });

  it('requires an upstream and a token outside mock mode', () => {
    expect(() => sellerNodeOptionsFromEnv({})).toThrow(/UPSTREAM_BASE_URL/);
    expect(() => sellerNodeOptionsFromEnv({ UPSTREAM_BASE_URL: 'http://localhost:11434/v1' })).toThrow(/SELLER_TOKEN/);
    const o = sellerNodeOptionsFromEnv({ MOCK: '1', MOCK_LATENCY_MS: '25', MOCK_FAIL_RATE: '0.5', PORT: '0' });
    expect(o.mock).toMatchObject({ latencyMs: 25, failRate: 0.5 });
    expect(() => sellerNodeOptionsFromEnv({ MOCK: '1', MOCK_FAIL_RATE: '2' })).toThrow(/MOCK_FAIL_RATE/);
  });
});

describe('seller node (mock mode, in-process)', () => {
  let node: SellerNode;
  beforeAll(async () => {
    node = await createSellerNode({ mock: { clock: () => FIXED_NOW }, sellerToken: TOKEN });
  });
  afterAll(async () => {
    await node.close();
  });

  it('serves /health without auth and rejects unauthenticated completions', async () => {
    expect((await fetch(`${node.url}/health`)).status).toBe(200);
    const r = await fetch(`${node.baseUrl}/chat/completions`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(chat({})) });
    expect(r.status).toBe(401);
    const bad = await fetch(`${node.baseUrl}/chat/completions`, {
      method: 'POST',
      headers: { ...auth, authorization: 'Bearer wrong' },
      body: JSON.stringify(chat({})),
    });
    expect(bad.status).toBe(401);
    expect(((await bad.json()) as { error: { code: string } }).error.code).toBe('invalid_seller_token');
  });

  it('returns exact usage on non-streaming requests', async () => {
    const body = chat({ max_tokens: 5 });
    const r = await fetch(`${node.baseUrl}/chat/completions`, { method: 'POST', headers: auth, body: JSON.stringify(body) });
    expect(r.status).toBe(200);
    const json = (await r.json()) as { model: string; usage: { prompt_tokens: number; completion_tokens: number }; choices: { finish_reason: string }[] };
    const expected = mockCompletion(body, FIXED_NOW);
    expect(json.model).toBe(body.model);
    expect(json.usage).toEqual(expected.usage);
    expect(json.usage.completion_tokens).toBe(5);
    expect(json.choices[0]!.finish_reason).toBe('length');
  });

  it('streams SSE with a final usage chunk when stream_options.include_usage', async () => {
    const body = chat({ stream: true, stream_options: { include_usage: true } });
    const r = await fetch(`${node.baseUrl}/chat/completions`, { method: 'POST', headers: auth, body: JSON.stringify(body) });
    expect(r.headers.get('content-type')).toMatch(/text\/event-stream/);
    const text = await r.text();
    expect(text).toBe(mockStreamEvents(mockCompletion(body, FIXED_NOW), true).join(''));
    expect(text).toContain('"choices":[],"usage":{');
  });

  it('streams without the usage chunk by default', async () => {
    const body = chat({ stream: true });
    const text = await (await fetch(`${node.baseUrl}/chat/completions`, { method: 'POST', headers: auth, body: JSON.stringify(body) })).text();
    expect(text).toBe(mockStreamEvents(mockCompletion(body, FIXED_NOW), false).join(''));
    expect(text).not.toContain('"usage"');
  });

  it('validates the request body', async () => {
    const r = await fetch(`${node.baseUrl}/chat/completions`, { method: 'POST', headers: auth, body: JSON.stringify({ model: 'x', messages: [] }) });
    expect(r.status).toBe(400);
  });
});

describe('seller node mock options', () => {
  it('fails every request at MOCK_FAIL_RATE=1 and honours MOCK_LATENCY_MS', async () => {
    const node = await createSellerNode({ mock: { failRate: 1, latencyMs: 60 } });
    try {
      const t0 = Date.now();
      const r = await fetch(`${node.baseUrl}/chat/completions`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(chat({})) });
      expect(r.status).toBe(500);
      expect(Date.now() - t0).toBeGreaterThanOrEqual(55);
    } finally {
      await node.close();
    }
  });

  it('enforces MODEL_MAP keys', async () => {
    const node = await createSellerNode({ mock: true, modelMap: { 'qwen/qwen-2.5-14b-instruct': 'qwen2.5:14b-instruct' } });
    try {
      const ok = await fetch(`${node.baseUrl}/chat/completions`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(chat({})) });
      expect(ok.status).toBe(200);
      const miss = await fetch(`${node.baseUrl}/chat/completions`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(chat({ model: 'other/model' })),
      });
      expect(miss.status).toBe(404);
      const models = (await (await fetch(`${node.baseUrl}/models`)).json()) as { data: { id: string }[] };
      expect(models.data.map((m) => m.id)).toEqual(['qwen/qwen-2.5-14b-instruct']);
    } finally {
      await node.close();
    }
  });
});

describe('seller node proxy mode', () => {
  let upstream: Server;
  let upstreamUrl: string;
  const seen: { auth?: string; model?: string }[] = [];
  beforeAll(async () => {
    upstream = createServer((req, res) => {
      let raw = '';
      req.on('data', (c: Buffer) => (raw += c.toString()));
      req.on('end', () => {
        const body = JSON.parse(raw) as { model: string; stream?: boolean };
        seen.push({ auth: req.headers.authorization, model: body.model });
        if (body.stream) {
          res.writeHead(200, { 'content-type': 'text/event-stream' });
          // Like Ollama: every chunk names the model by the server's own name.
          res.write(`data: {"id":"c1","model":${JSON.stringify(body.model)},"choices":[{"delta":{"content":"say \\"model\\":\\"${body.model}\\""}}]}\n\n`);
          res.end('data: [DONE]\n\n');
          return;
        }
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ id: 'x', model: body.model, choices: [{ message: { content: 'hi' } }], usage: { prompt_tokens: 3, completion_tokens: 1 } }));
      });
    });
    await new Promise<void>((r) => upstream.listen(0, '127.0.0.1', r));
    upstreamUrl = `http://127.0.0.1:${(upstream.address() as AddressInfo).port}/v1`;
  });
  afterAll(async () => {
    await new Promise<void>((r) => upstream.close(() => r()));
  });

  it('renames the model, adds the upstream key and rewrites the JSON model back', async () => {
    const node = await createSellerNode({
      upstreamBaseUrl: upstreamUrl,
      upstreamApiKey: 'up-key',
      sellerToken: TOKEN,
      modelMap: { 'qwen/qwen-2.5-14b-instruct': 'qwen2.5:14b-instruct' },
    });
    try {
      const r = await fetch(`${node.baseUrl}/chat/completions`, { method: 'POST', headers: auth, body: JSON.stringify(chat({})) });
      const json = (await r.json()) as { model: string };
      expect(json.model).toBe('qwen/qwen-2.5-14b-instruct');
      expect(seen.at(-1)).toEqual({ auth: 'Bearer up-key', model: 'qwen2.5:14b-instruct' });
      const s = await fetch(`${node.baseUrl}/chat/completions`, { method: 'POST', headers: auth, body: JSON.stringify(chat({ stream: true })) });
      // The stream reports the catalog id too; the same text inside the completion is left alone.
      expect(await s.text()).toBe(
        'data: {"id":"c1","model":"qwen/qwen-2.5-14b-instruct","choices":[{"delta":{"content":"say \\"model\\":\\"qwen2.5:14b-instruct\\""}}]}\n\ndata: [DONE]\n\n',
      );
    } finally {
      await node.close();
    }
  });

  it('modelRenamer rewrites the model member of data lines only, across any chunk boundaries', async () => {
    const run = async (parts: string[]) => {
      const t = modelRenamer('qwen2.5:14b-instruct', 'qwen/qwen-2.5-14b-instruct');
      const out: Buffer[] = [];
      t.on('data', (b: Buffer) => out.push(b));
      const done = new Promise((r) => t.on('end', r));
      for (const p of parts) t.write(Buffer.from(p));
      t.end();
      await done;
      return Buffer.concat(out).toString();
    };
    const src = 'data: {"model":"qwen2.5:14b-instruct","choices":[]}\r\n\r\n: keep-alive\n\ndata: {"model": "qwen2.5:14b-instruct"}\n\nevent: x\n"model":"qwen2.5:14b-instruct"\ndata: [DONE]';
    const want = 'data: {"model":"qwen/qwen-2.5-14b-instruct","choices":[]}\r\n\r\n: keep-alive\n\ndata: {"model":"qwen/qwen-2.5-14b-instruct"}\n\nevent: x\n"model":"qwen2.5:14b-instruct"\ndata: [DONE]';
    expect(await run([src])).toBe(want);
    // Split everywhere, one byte at a time: the same bytes.
    expect(await run(src.split(''))).toBe(want);
    // Another model, and a line with no newline at the end, are relayed unchanged.
    expect(await run(['data: {"model":"other"}'])).toBe('data: {"model":"other"}');
    // Bare CR line ends are lines too (nothing waits for an LF that never comes).
    expect(await run(['data: {"model":"qwen2.5:14b-instruct"}\r\r', 'data: [DONE]\r\r'])).toBe('data: {"model":"qwen/qwen-2.5-14b-instruct"}\r\rdata: [DONE]\r\r');
  });

  it('returns 502 when the upstream is down', async () => {
    const node = await createSellerNode({ upstreamBaseUrl: 'http://127.0.0.1:1/v1', sellerToken: TOKEN });
    try {
      const r = await fetch(`${node.baseUrl}/chat/completions`, { method: 'POST', headers: auth, body: JSON.stringify(chat({})) });
      expect(r.status).toBe(502);
    } finally {
      await node.close();
    }
  });
});

describe('seller node proxy logs: the market cancelling a request is not an unreachable server', () => {
  /** The node's JSON log lines. */
  function logged() {
    const lines: { level: number; msg: string; [k: string]: unknown }[] = [];
    const stream = new Writable({
      write(chunk: Buffer, _e, cb) {
        for (const l of chunk.toString().split('\n')) if (l.trim()) lines.push(JSON.parse(l) as (typeof lines)[number]);
        cb();
      },
    });
    return { lines, logger: { level: 'info', stream } };
  }
  const waitFor = async (ok: () => boolean, what: string) => {
    const t0 = Date.now();
    while (!ok()) {
      if (Date.now() - t0 > 5_000) throw new Error(`timed out waiting for ${what}`);
      await new Promise((r) => setTimeout(r, 10));
    }
  };

  it('the market closes the request while the server is still loading the model: "cancelled by the market", not "upstream unreachable"', async () => {
    // Ollama loading a model: it takes the request and sends nothing for a minute.
    let received = 0;
    const loading = createServer((req) => {
      received++;
      req.resume();
    });
    await new Promise<void>((r) => loading.listen(0, '127.0.0.1', r));
    const { lines, logger } = logged();
    const node = await createSellerNode({ upstreamBaseUrl: `http://127.0.0.1:${(loading.address() as AddressInfo).port}/v1`, sellerToken: TOKEN, logger });
    try {
      // As the connector relays it: a loopback request that is destroyed when the market cancels (its health check's
      // deadline).
      const body = JSON.stringify(chat({}));
      const req = request(`${node.baseUrl}/chat/completions`, { method: 'POST', headers: { ...auth, 'content-length': String(Buffer.byteLength(body)) } });
      req.on('error', () => undefined);
      req.end(body);
      await waitFor(() => received === 1, 'the upstream request');
      req.destroy();
      await waitFor(() => lines.some((l) => l.msg.startsWith('request cancelled')), 'the log line');
      const line = lines.find((l) => l.msg.startsWith('request cancelled'))!;
      expect(line).toMatchObject({ level: 30, msg: 'request cancelled by the market before the server answered (model loading?)', waitedMs: expect.any(Number) });
      expect(lines.some((l) => l.msg === 'upstream unreachable')).toBe(false);
    } finally {
      await node.close();
      loading.closeAllConnections();
      await new Promise<void>((r) => loading.close(() => r()));
    }
  });

  it('a server that refuses the connection is still "upstream unreachable", with the error code, and the reply is unchanged', async () => {
    const { lines, logger } = logged();
    const node = await createSellerNode({ upstreamBaseUrl: `http://127.0.0.1:${await closedPort()}/v1`, sellerToken: TOKEN, logger });
    try {
      const r = await fetch(`${node.baseUrl}/chat/completions`, { method: 'POST', headers: auth, body: JSON.stringify(chat({})) });
      expect(r.status).toBe(502);
      expect(await r.json()).toEqual({ error: { message: 'Upstream inference server is unreachable.', type: 'server_error', code: 'upstream_unreachable' } });
      await waitFor(() => lines.some((l) => l.msg === 'upstream unreachable'), 'the log line');
      expect(lines.find((l) => l.msg === 'upstream unreachable')).toMatchObject({ level: 40, code: 'ECONNREFUSED' });
      expect(lines.some((l) => l.msg.startsWith('request cancelled'))).toBe(false);
    } finally {
      await node.close();
    }
  });
});
