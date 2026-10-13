// Keep-warm for Ollama. Ollama unloads a model after 5 idle minutes by default (OLLAMA_KEEP_ALIVE), and loading a
// 14B model again can take a minute on a laptop. The market's health check gives up after about 15 s, so a check that
// arrives after an idle gap waits for a cold load, fails, and the market marks the machine unhealthy. Worse, Ollama
// stops a load when the request that started it goes away, so a health check alone may never get a slow model loaded.
//
// While the app sells, it asks Ollama to keep each model that has an offer loaded: POST <ollama>/api/generate with
// {"model": <local name>, "keep_alive": "10m"} and no prompt, which loads the model if it is not loaded and sets its
// keep-alive, without generating anything (Ollama answers {"done": true, "done_reason": "load"}; any 2xx counts). It does
// so at start (start.ts waits for that round before dialing out, so the market's first check finds the model loaded),
// then every 4 minutes, one model at a time and never two rounds at once. The request carries no options, so Ollama
// uses its own defaults (OLLAMA_CONTEXT_LENGTH and the rest), the same as the market's requests through /v1: it never
// makes Ollama reload a model with other settings. Each cold load is timed, and the app keeps the last cold-load time per
// model in the state file for `status` and `doctor`.
//
// Which models: start.ts says (`wanted`): those with an offer on the market. A model nobody can buy is not worth the
// memory, and loading it could push out one that sells.
//
// A model Ollama unloaded while its keep-alive still ran (to make room for another model, or `ollama stop`) is not
// loaded again until Ollama holds no model: loading it would push the others out, and two models that do not fit
// together would take turns loading every 4 minutes. A model that is gone because a refresh was missed (the app was
// offline or asleep) simply expired, and is loaded again at once.
//
// LM Studio, vLLM and the llama.cpp server do not unload a model after a few idle minutes (LM Studio unloads a model it
// loaded on demand after 60 idle minutes by default, and the market checks a connected offer at least every 15), so the
// app sends them nothing.
//
// INFERIT_KEEP_WARM: unset or 1 = on (keep_alive 10m), 0 = off, or a keep-alive such as 30m or 2h.
// INFERIT_KEEP_WARM_INTERVAL_MS changes the 4 minutes (tests).
import type { ServedModel } from './catalog.js';
import type { Logger } from './log.js';

/** How often the app asks Ollama again: under Ollama's default keep-alive (5 min), so a refresh always lands first. */
export const KEEP_WARM_EVERY_MS = 4 * 60_000;
/** The keep-alive each request sets: longer than the interval, so one missed refresh does not unload the model. */
export const KEEP_WARM_KEEP_ALIVE = '10m';
/** How long connecting waits for the round at start: long enough for a cold load of a 14B model on a laptop (about a
 *  minute), so the market's first check finds it loaded; a slower load goes on in the background. */
export const KEEP_WARM_HEAD_START_MS = 120_000;
/** The market's health check gives up after about this long: a slower cold load fails it. */
export const SLOW_COLD_LOAD_MS = 15_000;
/** When Ollama does not say whether the model was loaded, a load-only request this slow was a cold load. */
const COLD_GUESS_MS = 1_000;
const REQUEST_TIMEOUT_MS = 10 * 60_000;
const PS_TIMEOUT_MS = 1_500;

const ON = new Set(['1', 'true', 'yes', 'on']);
const OFF = new Set(['0', 'false', 'no', 'off']);
const UNIT_MS: Record<string, number> = { s: 1_000, m: 60_000, h: 3_600_000 };

/** "10m" -> 600000; null for anything that is not a whole number of s, m or h. */
export function durationMs(raw: string): number | null {
  const m = /^(\d+)([smh])$/.exec(raw.trim());
  return m ? Number(m[1]) * UNIT_MS[m[2]!]! : null;
}

/** "10m" -> "10 min", "2h" -> "2 h", "90s" -> "90 s". */
export function durationWords(raw: string): string {
  const m = /^(\d+)([smh])$/.exec(raw.trim());
  return m ? `${m[1]} ${m[2] === 'm' ? 'min' : m[2]}` : raw;
}

function msWords(ms: number): string {
  return ms % 60_000 === 0 ? `${ms / 60_000} min` : `${Math.round(ms / 1000)} s`;
}

export interface KeepWarmSetting {
  on: boolean;
  /** The keep_alive each request sets, e.g. "10m". */
  keepAlive: string;
  everyMs: number;
}

/** INFERIT_KEEP_WARM and INFERIT_KEEP_WARM_INTERVAL_MS. Throws on a value that would not keep the model loaded. */
export function keepWarmSetting(env: NodeJS.ProcessEnv): KeepWarmSetting {
  const rawEvery = env.INFERIT_KEEP_WARM_INTERVAL_MS?.trim();
  const everyMs = rawEvery ? Number(rawEvery) : KEEP_WARM_EVERY_MS;
  if (!Number.isSafeInteger(everyMs) || everyMs < 10) throw new Error('INFERIT_KEEP_WARM_INTERVAL_MS must be a whole number of milliseconds (at least 10)');
  const raw = env.INFERIT_KEEP_WARM?.trim().toLowerCase() ?? '';
  if (OFF.has(raw)) return { on: false, keepAlive: KEEP_WARM_KEEP_ALIVE, everyMs };
  const keepAlive = raw === '' || ON.has(raw) ? KEEP_WARM_KEEP_ALIVE : raw;
  const ms = durationMs(keepAlive);
  if (ms === null) throw new Error(`INFERIT_KEEP_WARM: expected 1 (on, the default), 0 (off) or a keep-alive such as 30m or 2h, got "${env.INFERIT_KEEP_WARM}"`);
  if (ms <= everyMs) throw new Error(`INFERIT_KEEP_WARM: a keep-alive of ${durationWords(keepAlive)} is not longer than the refresh interval (${msWords(everyMs)}), so Ollama would unload the model between refreshes`);
  return { on: true, keepAlive, everyMs };
}

export interface KeepWarmTarget {
  /** The name Ollama uses, e.g. qwen2.5:14b-instruct. */
  local: string;
  /** Ollama's origin, e.g. http://127.0.0.1:11434 (its OpenAI base URL without /v1). */
  origin: string;
}

/** The served models to keep warm: Ollama's only, once each. */
export function keepWarmTargets(served: readonly Pick<ServedModel, 'server' | 'local' | 'baseUrl'>[]): KeepWarmTarget[] {
  const out: KeepWarmTarget[] = [];
  for (const s of served) {
    // LM Studio, vLLM and the llama.cpp server keep a loaded model until it is unloaded: nothing to do.
    if (s.server !== 'ollama') continue;
    const origin = s.baseUrl.replace(/\/+$/, '').replace(/\/v1$/, '');
    if (!out.some((t) => t.origin === origin && t.local.toLowerCase() === s.local.toLowerCase())) out.push({ local: s.local, origin });
  }
  return out;
}

export interface ColdLoad {
  local: string;
  origin: string;
  /** How long Ollama took to load it, ms. */
  loadMs: number;
  /** When it finished (ISO). */
  at: string;
}

export interface KeepWarmOptions {
  targets: KeepWarmTarget[];
  keepAlive?: string;
  everyMs?: number;
  /** Sent as a bearer token when set (UPSTREAM_API_KEY, for an Ollama behind a proxy that wants one). */
  apiKey?: string;
  fetchImpl?: typeof fetch;
  log: Pick<Logger, 'debug' | 'info' | 'warn'>;
  /** Monotonic ms, for timing loads. Default performance.now (tests use the faked Date.now). */
  now?: () => number;
  /** Whether to send the 4-minute refresh: the app is connected or connecting. The round at start always runs. */
  active?: () => boolean;
  /** Whether to keep this model loaded now (start.ts: it has an offer on the market). Default: every target. */
  wanted?: (t: KeepWarmTarget) => boolean;
  onColdLoad?: (e: ColdLoad) => void;
  requestTimeoutMs?: number;
}

export interface KeepWarm {
  /** Settles (never rejects) when the round at start is done. */
  first: Promise<void>;
  /** A round now for the models not refreshed within the interval (after a reconnect, or a new offer); right after
   *  the current round when one is running. */
  poke(): void;
  stop(): void;
}

const norm = (name: string) => {
  const n = name.trim().toLowerCase();
  return n.includes(':') ? n : `${n}:latest`;
};

function errText(e: unknown): string {
  const err = e as { name?: string; message?: string; cause?: { code?: string; message?: string } };
  if (err?.name === 'TimeoutError' || err?.name === 'AbortError') return 'no answer (timed out)';
  if (err?.cause?.code === 'ECONNREFUSED') return 'nothing listening (connection refused)';
  return (err?.cause?.code ?? err?.cause?.message ?? err?.message ?? String(e)).slice(0, 200);
}

/** Starts keeping `targets` loaded: a round now, then one every `everyMs` while `active()` says so. */
export function startKeepWarm(o: KeepWarmOptions): KeepWarm {
  const f = o.fetchImpl ?? fetch;
  const keepAlive = o.keepAlive ?? KEEP_WARM_KEEP_ALIVE;
  const everyMs = o.everyMs ?? KEEP_WARM_EVERY_MS;
  // A model refreshed less than this long ago is still loaded unless something unloaded it: the refresh interval plus a
  // quarter (5 min for the default 4, Ollama's own default keep-alive, which a market request through /v1 sets).
  const heldForMs = everyMs + everyMs / 4;
  const now = o.now ?? (() => performance.now());
  const active = o.active ?? (() => true);
  const wanted = o.wanted ?? (() => true);
  const log = o.log;
  const key = (t: KeepWarmTarget) => `${t.origin}\u0000${norm(t.local)}`;
  const inFlight = new Set<string>();
  const lastOk = new Map<string, number>();
  const lastProblem = new Map<string, string>();
  const waiting = new Set<string>();
  const controllers = new Set<AbortController>();
  let stopped = false;
  const headers: Record<string, string> = { 'content-type': 'application/json', accept: 'application/json' };
  if (o.apiKey) headers.authorization = `Bearer ${o.apiKey}`;

  /** The models Ollama holds now (normalised names), or null when it does not say. */
  const loadedNow = async (t: KeepWarmTarget): Promise<string[] | null> => {
    try {
      const res = await f(`${t.origin}/api/ps`, { headers, signal: AbortSignal.timeout(PS_TIMEOUT_MS) });
      if (!res.ok) {
        await res.body?.cancel().catch(() => undefined);
        return null;
      }
      const json = (await res.json()) as { models?: { name?: unknown; model?: unknown }[] };
      if (!Array.isArray(json?.models)) return null;
      return json.models.map((m) => (typeof m?.name === 'string' ? m.name : typeof m?.model === 'string' ? m.model : '')).filter(Boolean).map(norm);
    } catch {
      return null;
    }
  };

  /** Logs a failure once per streak of the same kind (HTTP status or error code); the details can change each time
   *  (Ollama's out-of-memory message gives the free memory), so a repeat goes to debug. */
  const problem = (t: KeepWarmTarget, k: string, kind: string, what: string) => {
    const msg = `keep-warm: could not ask Ollama to keep ${t.local} loaded (${what}); trying again in ${msWords(everyMs)}`;
    if (lastProblem.get(k) === kind) return log.debug({ local: t.local, ollama: t.origin, problem: what }, msg);
    lastProblem.set(k, kind);
    log.warn({ local: t.local, ollama: t.origin, problem: what }, msg);
  };

  const warm = async (t: KeepWarmTarget, atStart: boolean): Promise<void> => {
    const k = key(t);
    if (stopped || inFlight.has(k)) return; // never two at once for one model
    inFlight.add(k);
    const ac = new AbortController();
    controllers.add(ac);
    const timer = setTimeout(() => ac.abort(), o.requestTimeoutMs ?? REQUEST_TIMEOUT_MS);
    timer.unref?.();
    try {
      const held = await loadedNow(t);
      if (stopped) return;
      const here = held === null ? null : held.includes(norm(t.local));
      if (!atStart && here === false && held!.length > 0) {
        // Gone while Ollama holds other models. If its keep-alive still ran, Ollama unloaded it for them (or someone ran
        // `ollama stop`): leave them be until Ollama holds no model. If a refresh was missed, it expired: load it.
        const last = lastOk.get(k);
        if (waiting.has(k) || (last !== undefined && now() - last < heldForMs)) {
          if (!waiting.has(k)) {
            waiting.add(k);
            log.warn(
              { local: t.local, loaded: held },
              `keep-warm: Ollama unloaded ${t.local} to hold ${held!.join(', ')} (it cannot hold them all, or it was told to stop it); the app does not push those out, and loads ${t.local} again once Ollama holds no model. Until then the market's health check can find ${t.local} unloaded and fail`,
            );
          }
          return;
        }
      }
      if (here === false) log.info({ local: t.local }, `keep-warm: Ollama is loading ${t.local} (not in memory; a cold load can take a minute)`);
      const t0 = now();
      const res = await f(`${t.origin}/api/generate`, { method: 'POST', headers, body: JSON.stringify({ model: t.local, keep_alive: keepAlive }), signal: ac.signal });
      const text = await res.text();
      if (!res.ok) {
        let said = '';
        try {
          const e = (JSON.parse(text) as { error?: unknown }).error;
          if (typeof e === 'string') said = `: ${e.slice(0, 200)}`;
        } catch {
          /* not JSON */
        }
        return problem(t, k, `HTTP ${res.status}`, `HTTP ${res.status}${said}`);
      }
      // Any 2xx: Ollama took the request, so the model is loaded with the keep-alive set.
      const loadMs = Math.max(0, Math.round(now() - t0));
      lastOk.set(k, now());
      if (lastProblem.delete(k)) log.info({ local: t.local }, `keep-warm: Ollama answers again for ${t.local}`);
      waiting.delete(k);
      const cold = here === false || (here === null && loadMs >= COLD_GUESS_MS);
      if (cold) {
        log.info(
          { local: t.local, loadMs },
          `Ollama loaded ${t.local} in ${(loadMs / 1000).toFixed(1)} s; keep-warm keeps it in memory while the app sells (keep_alive ${keepAlive}, refreshed every ${msWords(everyMs)})`,
        );
        try {
          o.onColdLoad?.({ local: t.local, origin: t.origin, loadMs, at: new Date().toISOString() });
        } catch {
          /* the state file is best effort */
        }
      } else if (atStart) {
        log.info({ local: t.local }, `${t.local} is loaded in Ollama; keep-warm keeps it there while the app sells (keep_alive ${keepAlive}, refreshed every ${msWords(everyMs)})`);
      } else {
        log.debug({ local: t.local, ms: loadMs }, 'keep-warm: refreshed');
      }
    } catch (e) {
      if (!stopped) {
        const what = errText(e);
        problem(t, k, what, what);
      }
    } finally {
      clearTimeout(timer);
      controllers.delete(ac);
      inFlight.delete(k);
    }
  };

  // One round at a time, one model at a time: two loads at once would compete for the same memory. A round asked for
  // while one runs (a reconnect, a new offer, a tick during a long load) runs right after it.
  let busy = false;
  let again = false;
  const run = async (atStart: boolean, only?: (t: KeepWarmTarget) => boolean): Promise<void> => {
    if (stopped) return;
    if (busy) {
      again = true;
      return;
    }
    busy = true;
    try {
      for (const t of o.targets) if (!stopped && (!only || only(t)) && wanted(t)) await warm(t, atStart);
    } catch (e) {
      log.debug({ problem: errText(e) }, 'keep-warm: round failed');
    } finally {
      busy = false;
    }
    if (again && !stopped) {
      again = false;
      void run(false);
    }
  };

  const first = run(true);
  const interval = setInterval(() => {
    if (!stopped && active()) void run(false);
  }, everyMs);
  interval.unref?.();

  return {
    first,
    poke: () => {
      if (stopped) return;
      const due = (t: KeepWarmTarget) => {
        const at = lastOk.get(key(t));
        return at === undefined || now() - at >= everyMs;
      };
      void run(false, due);
    },
    stop: () => {
      stopped = true;
      clearInterval(interval);
      for (const ac of controllers) ac.abort();
    },
  };
}
