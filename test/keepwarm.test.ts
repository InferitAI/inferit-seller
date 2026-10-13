// Keep-warm (src/keepwarm.ts) against a mock Ollama on loopback, with fake timers for the 4-minute cadence and the
// load clock: the request shape, the cadence, one load at a time, the cold-load time, the off switch, the servers it
// leaves alone, the models with an offer only, the models Ollama unloaded to make room for others (and the ones that
// merely expired), and one warning per failure streak.
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { KEEP_WARM_EVERY_MS, keepWarmSetting, keepWarmTargets, startKeepWarm, type ColdLoad, type KeepWarm } from '../src/keepwarm.ts';

const LOCAL = 'qwen2.5:14b-instruct';

interface Hit {
  method: string;
  url: string;
  contentType: string | undefined;
  body: unknown;
}

/** Ollama's /api/ps and /api/generate. A load-only generate adds the model to /api/ps (`fits`: how many models it
 *  holds at once; loading one more pushes out the oldest); `hold` keeps it unanswered until release() (a cold load in
 *  progress). `maxOpen` is the most generate requests it had open at once. */
async function mockOllama(o: { loaded?: string[]; generateStatus?: number; generateBody?: unknown | (() => unknown); fits?: number } = {}) {
  const hits: Hit[] = [];
  const state = { loaded: [...(o.loaded ?? [])], hold: false, open: 0, maxOpen: 0, status: o.generateStatus ?? 200 };
  const held: (() => void)[] = [];
  const server: Server = createServer((req, res) => {
    let raw = '';
    req.on('data', (c: Buffer) => (raw += c.toString()));
    req.on('end', () => {
      const body = raw ? (JSON.parse(raw) as Record<string, unknown>) : null;
      hits.push({ method: req.method ?? '', url: req.url ?? '', contentType: req.headers['content-type'], body });
      if (req.method === 'GET' && req.url === '/api/ps') {
        res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ models: state.loaded.map((name) => ({ name, model: name, size: 9459613039 })) }));
        return;
      }
      if (req.method === 'POST' && req.url === '/api/generate') {
        state.open++;
        state.maxOpen = Math.max(state.maxOpen, state.open);
        const answer = () => {
          state.open--;
          if (state.status !== 200) {
            const b = typeof o.generateBody === 'function' ? (o.generateBody as () => unknown)() : o.generateBody;
            res.writeHead(state.status, { 'content-type': 'application/json' }).end(JSON.stringify(b ?? { error: 'boom' }));
            return;
          }
          const model = String(body?.model);
          if (!state.loaded.includes(model)) state.loaded.push(model);
          if (o.fits !== undefined) while (state.loaded.length > o.fits) state.loaded.shift();
          res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ model, created_at: new Date().toISOString(), response: '', done: true, done_reason: 'load' }));
        };
        if (state.hold) held.push(answer);
        else answer();
        return;
      }
      res.writeHead(404).end();
    });
  });
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  return {
    origin: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    hits,
    state,
    generates: () => hits.filter((h) => h.url === '/api/generate'),
    generatesFor: (model: string) => hits.filter((h) => h.url === '/api/generate' && (h.body as { model?: string })?.model === model),
    release: () => {
      for (const a of held.splice(0)) a();
    },
    close: () =>
      new Promise<void>((r) => {
        server.closeAllConnections();
        server.close(() => r());
      }),
  };
}

/** Waits on real time (setTimeout is not faked): the requests themselves go over loopback. */
async function waitFor(ok: () => boolean, what: string, timeoutMs = 3_000): Promise<void> {
  const t0 = performance.now();
  while (!ok()) {
    if (performance.now() - t0 > timeoutMs) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 5));
  }
}
const settle = () => new Promise((r) => setTimeout(r, 60));

interface Line {
  level: string;
  obj: object;
  msg: string;
}
let lines: Line[];
const log = {
  debug: (obj: object, msg: string) => lines.push({ level: 'debug', obj, msg }),
  info: (obj: object, msg: string) => lines.push({ level: 'info', obj, msg }),
  warn: (obj: object, msg: string) => lines.push({ level: 'warn', obj, msg }),
};

let warm: KeepWarm | null;
const toClose: (() => Promise<void>)[] = [];
beforeEach(() => {
  // Only the interval and the clock: setTimeout stays real, so fetch and the loopback servers work as usual.
  vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval', 'Date'] });
  lines = [];
  warm = null;
});
afterEach(async () => {
  warm?.stop();
  vi.useRealTimers();
  for (const c of toClose.splice(0)) await c();
});

const start = (origin: string, extra: Partial<Parameters<typeof startKeepWarm>[0]> = {}) => {
  warm = startKeepWarm({ targets: [{ local: LOCAL, origin }], log, now: () => Date.now(), ...extra });
  return warm;
};
const OTHER = 'llama3.1:8b';
const both = (origin: string) => [
  { local: LOCAL, origin },
  { local: OTHER, origin },
];

describe('keep-warm requests', () => {
  it('sends {model, keep_alive} with no prompt at start, then every 4 minutes', async () => {
    const ollama = await mockOllama({ loaded: [LOCAL] });
    toClose.push(ollama.close);
    await start(ollama.origin).first;
    expect(ollama.generates()).toHaveLength(1);
    const g = ollama.generates()[0]!;
    expect(g).toMatchObject({ method: 'POST', url: '/api/generate', contentType: 'application/json' });
    expect(g.body).toEqual({ model: LOCAL, keep_alive: '10m' }); // exactly: no prompt, no options
    // Already loaded: said once, and no cold load.
    expect(lines.filter((l) => l.level === 'info').map((l) => l.msg)).toEqual([
      `${LOCAL} is loaded in Ollama; keep-warm keeps it there while the app sells (keep_alive 10m, refreshed every 4 min)`,
    ]);

    vi.advanceTimersByTime(KEEP_WARM_EVERY_MS - 1);
    await settle();
    expect(ollama.generates()).toHaveLength(1);
    vi.advanceTimersByTime(1);
    await waitFor(() => ollama.generates().length === 2, 'the 4-minute refresh');
    vi.advanceTimersByTime(KEEP_WARM_EVERY_MS);
    await waitFor(() => ollama.generates().length === 3, 'the next refresh');
    expect(ollama.generates().every((h) => JSON.stringify(h.body) === JSON.stringify({ model: LOCAL, keep_alive: '10m' }))).toBe(true);
    // A refresh is quiet (debug only).
    expect(lines.filter((l) => l.level !== 'debug')).toHaveLength(1);
  });

  it('never sends a second request for a model while one is in flight; the rounds it skipped run once, right after', async () => {
    const ollama = await mockOllama();
    toClose.push(ollama.close);
    ollama.state.hold = true; // a cold load that takes a while
    let firstDone = false;
    const w = start(ollama.origin);
    void w.first.then(() => (firstDone = true));
    await waitFor(() => ollama.generates().length === 1, 'the first request');
    const before = ollama.hits.length;
    for (let i = 0; i < 3; i++) {
      vi.advanceTimersByTime(KEEP_WARM_EVERY_MS);
      w.poke(); // a reconnect during the load
      await settle();
    }
    expect(ollama.hits.length).toBe(before); // nothing at all, not even /api/ps
    expect(firstDone).toBe(false);
    ollama.state.hold = false;
    ollama.release();
    await waitFor(() => firstDone, 'the first round');
    // The skipped ticks and pokes become one catch-up round, not four.
    await waitFor(() => ollama.generates().length === 2, 'the catch-up refresh');
    await settle();
    expect(ollama.generates()).toHaveLength(2);
    vi.advanceTimersByTime(KEEP_WARM_EVERY_MS);
    await waitFor(() => ollama.generates().length === 3, 'the next refresh');
  });

  it('loads one model at a time: a poke or a tick during a load never starts another model', async () => {
    const ollama = await mockOllama();
    toClose.push(ollama.close);
    ollama.state.hold = true;
    const w = start(ollama.origin, { targets: both(ollama.origin) });
    await waitFor(() => ollama.generates().length === 1, 'the first load');
    w.poke(); // the welcome, while the first model still loads
    vi.advanceTimersByTime(KEEP_WARM_EVERY_MS);
    await settle();
    expect(ollama.generates().map((h) => (h.body as { model: string }).model)).toEqual([LOCAL]);
    ollama.state.hold = false;
    ollama.release();
    await w.first;
    await waitFor(() => ollama.generatesFor(OTHER).length >= 1, 'the second model');
    await settle();
    expect(ollama.state.maxOpen).toBe(1);
  });

  it('times a cold load and reports it once; a refresh of a loaded model is not a cold load', async () => {
    const ollama = await mockOllama(); // nothing loaded
    toClose.push(ollama.close);
    ollama.state.hold = true;
    const loads: ColdLoad[] = [];
    const first = start(ollama.origin, { onColdLoad: (c) => loads.push(c) }).first;
    await waitFor(() => ollama.generates().length === 1, 'the load request');
    vi.advanceTimersByTime(61_000); // Ollama takes 61 s to load it
    ollama.release();
    await first;
    expect(loads).toEqual([{ local: LOCAL, origin: ollama.origin, loadMs: 61_000, at: expect.any(String) }]);
    expect(lines.filter((l) => l.level === 'info').map((l) => l.msg)[0]).toBe(`keep-warm: Ollama is loading ${LOCAL} (not in memory; a cold load can take a minute)`);
    expect(lines.find((l) => l.msg.startsWith('Ollama loaded'))).toMatchObject({
      obj: { local: LOCAL, loadMs: 61_000 },
      msg: `Ollama loaded ${LOCAL} in 61.0 s; keep-warm keeps it in memory while the app sells (keep_alive 10m, refreshed every 4 min)`,
    });
    ollama.state.hold = false;
    vi.advanceTimersByTime(KEEP_WARM_EVERY_MS);
    await waitFor(() => ollama.generates().length === 2, 'the refresh');
    await settle();
    expect(loads).toHaveLength(1);
  });

  it('refreshes only while the app is connected or connecting, and catches up when it reconnects', async () => {
    const ollama = await mockOllama({ loaded: [LOCAL] });
    toClose.push(ollama.close);
    let connected = true;
    const w = start(ollama.origin, { active: () => connected });
    await w.first;
    connected = false;
    vi.advanceTimersByTime(2 * KEEP_WARM_EVERY_MS);
    await settle();
    expect(ollama.generates()).toHaveLength(1);
    connected = true;
    w.poke(); // the 'live' event: the last refresh is 8 minutes old
    await waitFor(() => ollama.generates().length === 2, 'the catch-up refresh');
    w.poke(); // just refreshed: nothing to do
    await settle();
    expect(ollama.generates()).toHaveLength(2);
  });

  it('leaves Ollama alone when it unloaded the model for another one; loads it again once Ollama holds none', async () => {
    const ollama = await mockOllama({ loaded: [LOCAL] });
    toClose.push(ollama.close);
    await start(ollama.origin).first;
    // Ollama unloaded it to make room for another model (its keep-alive still ran).
    ollama.state.loaded = ['llama3.1:8b'];
    for (let i = 2; i <= 4; i++) {
      vi.advanceTimersByTime(KEEP_WARM_EVERY_MS); // still waiting after its keep-alive would have run out
      await waitFor(() => ollama.hits.filter((h) => h.url === '/api/ps').length === i, `check ${i}`);
    }
    await settle();
    expect(ollama.generates()).toHaveLength(1);
    const said = lines.filter((l) => l.msg.includes('Ollama unloaded'));
    expect(said).toHaveLength(1); // said once, not every round
    expect(said[0]).toMatchObject({
      level: 'warn',
      msg: `keep-warm: Ollama unloaded ${LOCAL} to hold llama3.1:8b (it cannot hold them all, or it was told to stop it); the app does not push those out, and loads ${LOCAL} again once Ollama holds no model. Until then the market's health check can find ${LOCAL} unloaded and fail`,
    });
    // The other model's keep-alive ran out: Ollama holds none.
    ollama.state.loaded = [];
    vi.advanceTimersByTime(KEEP_WARM_EVERY_MS);
    await waitFor(() => ollama.generates().length === 2, 'the reload');
  });

  it('loads a model that expired after missed refreshes (the app was offline) even while Ollama holds another', async () => {
    const ollama = await mockOllama({ loaded: [LOCAL] });
    toClose.push(ollama.close);
    let connected = true;
    const w = start(ollama.origin, { active: () => connected });
    await w.first;
    connected = false;
    vi.advanceTimersByTime(2 * KEEP_WARM_EVERY_MS);
    await settle();
    // Offline for 8 minutes: its keep-alive ran out; an embedding model another app keeps loaded is still there.
    ollama.state.loaded = ['nomic-embed-text:latest'];
    connected = true;
    w.poke();
    await waitFor(() => ollama.generates().length === 2, 'the reload');
    expect(lines.some((l) => l.msg.includes('Ollama unloaded'))).toBe(false);
  });

  it('two models that do not fit together: the one pushed out waits, and they never take turns', async () => {
    const ollama = await mockOllama({ fits: 1 });
    toClose.push(ollama.close);
    await start(ollama.origin, { targets: both(ollama.origin) }).first; // loads LOCAL, then OTHER pushes it out
    expect(ollama.state.loaded).toEqual([OTHER]);
    for (let i = 0; i < 4; i++) {
      vi.advanceTimersByTime(KEEP_WARM_EVERY_MS);
      await waitFor(() => ollama.generatesFor(OTHER).length === i + 2, `refresh ${i + 1}`);
    }
    await settle();
    expect(ollama.generatesFor(LOCAL)).toHaveLength(1);
    expect(ollama.state.loaded).toEqual([OTHER]);
    expect(lines.filter((l) => l.msg.includes(`Ollama unloaded ${LOCAL}`))).toHaveLength(1);
  });

  it('keeps only the models it is told to (those with an offer); a poke loads a model that just got one', async () => {
    const ollama = await mockOllama();
    toClose.push(ollama.close);
    const offered = new Set([LOCAL]);
    const w = start(ollama.origin, { targets: both(ollama.origin), wanted: (t) => offered.has(t.local) });
    await w.first;
    expect(ollama.generates().map((h) => (h.body as { model: string }).model)).toEqual([LOCAL]);
    offered.add(OTHER); // the seller lists a price for it
    w.poke();
    await waitFor(() => ollama.generatesFor(OTHER).length === 1, 'the newly offered model');
    offered.delete(LOCAL); // delisted: no longer refreshed
    vi.advanceTimersByTime(KEEP_WARM_EVERY_MS);
    await waitFor(() => ollama.generatesFor(OTHER).length === 2, 'the refresh');
    await settle();
    expect(ollama.generatesFor(LOCAL)).toHaveLength(1);
  });

  it('warns once per failure streak even when Ollama\'s message changes, and says when it answers again', async () => {
    let free = 4.1;
    const ollama = await mockOllama({ loaded: [LOCAL], generateStatus: 500, generateBody: () => ({ error: `model requires more system memory (12.3 GiB) than is available (${(free -= 0.1).toFixed(1)} GiB)` }) });
    toClose.push(ollama.close);
    await start(ollama.origin).first;
    for (let i = 2; i <= 4; i++) {
      vi.advanceTimersByTime(KEEP_WARM_EVERY_MS);
      await waitFor(() => ollama.generates().length === i, `try ${i}`);
    }
    await settle();
    expect(lines.filter((l) => l.level === 'warn')).toHaveLength(1);
    expect(lines.filter((l) => l.level === 'debug' && l.msg.startsWith('keep-warm: could not ask'))).toHaveLength(3);
    ollama.state.status = 200;
    vi.advanceTimersByTime(KEEP_WARM_EVERY_MS);
    await waitFor(() => lines.some((l) => l.msg === `keep-warm: Ollama answers again for ${LOCAL}`), 'the recovery line');
  });

  it('logs a refused or failed request once per problem and keeps trying', async () => {
    const ollama = await mockOllama({ loaded: [LOCAL], generateStatus: 404, generateBody: { error: `model '${LOCAL}' not found` } });
    toClose.push(ollama.close);
    await start(ollama.origin).first;
    vi.advanceTimersByTime(KEEP_WARM_EVERY_MS);
    await waitFor(() => ollama.generates().length === 2, 'the retry');
    await settle();
    const warns = lines.filter((l) => l.level === 'warn');
    expect(warns).toHaveLength(1);
    expect(warns[0]!.msg).toBe(`keep-warm: could not ask Ollama to keep ${LOCAL} loaded (HTTP 404: model '${LOCAL}' not found); trying again in 4 min`);
  });

  it('stops: no more requests after stop()', async () => {
    const ollama = await mockOllama({ loaded: [LOCAL] });
    toClose.push(ollama.close);
    const w = start(ollama.origin);
    await w.first;
    w.stop();
    vi.advanceTimersByTime(3 * KEEP_WARM_EVERY_MS);
    w.poke();
    await settle();
    expect(ollama.generates()).toHaveLength(1);
  });
});

describe('keepWarmSetting (INFERIT_KEEP_WARM)', () => {
  it('is on by default with a 10-minute keep-alive every 4 minutes; 0 turns it off', () => {
    expect(keepWarmSetting({})).toEqual({ on: true, keepAlive: '10m', everyMs: 240_000 });
    for (const v of ['1', 'true', 'on', 'YES', ' 1 ']) expect(keepWarmSetting({ INFERIT_KEEP_WARM: v }), v).toMatchObject({ on: true, keepAlive: '10m' });
    for (const v of ['0', 'false', 'off', 'no', 'OFF']) expect(keepWarmSetting({ INFERIT_KEEP_WARM: v }).on, v).toBe(false);
  });

  it('takes a keep-alive longer than the refresh interval, and the interval for tests', () => {
    expect(keepWarmSetting({ INFERIT_KEEP_WARM: '30m' })).toEqual({ on: true, keepAlive: '30m', everyMs: 240_000 });
    expect(keepWarmSetting({ INFERIT_KEEP_WARM: '2h' }).keepAlive).toBe('2h');
    expect(keepWarmSetting({ INFERIT_KEEP_WARM_INTERVAL_MS: '50' })).toEqual({ on: true, keepAlive: '10m', everyMs: 50 });
    expect(() => keepWarmSetting({ INFERIT_KEEP_WARM: '3m' })).toThrow(/not longer than the refresh interval \(4 min\)/);
    expect(() => keepWarmSetting({ INFERIT_KEEP_WARM: 'forever' })).toThrow(/expected 1 \(on, the default\), 0 \(off\)/);
    expect(() => keepWarmSetting({ INFERIT_KEEP_WARM_INTERVAL_MS: 'soon' })).toThrow(/INFERIT_KEEP_WARM_INTERVAL_MS/);
    expect(() => keepWarmSetting({ INFERIT_KEEP_WARM_INTERVAL_MS: String(15 * 60_000) })).toThrow(/10 min is not longer than the refresh interval \(15 min\)/);
  });
});

describe('keepWarmTargets', () => {
  it('keeps Ollama models only, once each, at Ollama\'s own origin; LM Studio, vLLM and llama.cpp keep their models loaded', () => {
    expect(
      keepWarmTargets([
        { server: 'ollama', local: LOCAL, baseUrl: 'http://127.0.0.1:11434/v1' },
        { server: 'ollama', local: 'QWEN2.5:14b-instruct', baseUrl: 'http://127.0.0.1:11434/v1/' }, // two catalog ids, one model
        { server: 'ollama', local: 'llama3.1:8b', baseUrl: 'http://host.docker.internal:11434/v1' },
        { server: 'lmstudio', local: 'qwen2.5-14b-instruct', baseUrl: 'http://127.0.0.1:1234/v1' },
        { server: 'vllm', local: 'Qwen/Qwen2.5-14B-Instruct', baseUrl: 'http://127.0.0.1:8000/v1' },
        { server: 'llamacpp', local: 'qwen', baseUrl: 'http://127.0.0.1:8080/v1' },
        { server: 'custom', local: 'x', baseUrl: 'http://127.0.0.1:9000/v1' },
      ]),
    ).toEqual([
      { local: LOCAL, origin: 'http://127.0.0.1:11434' },
      { local: 'llama3.1:8b', origin: 'http://host.docker.internal:11434' },
    ]);
  });
});
