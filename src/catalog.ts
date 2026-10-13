// Maps the models found on this machine to Inferit catalog ids, using the market's own list (GET <api>/v1/models:
// each row's id and aliases, e.g. alias "qwen2.5:14b-instruct" -> "qwen/qwen-2.5-14b-instruct").
//
// 1. Exact: the local name (lowercase, ":latest" dropped) equals a catalog id or alias.
// 2. Normalised: both sides reduce to name tokens (the org prefix, ":latest", format and quantisation tags such as
//    q4_K_M, gguf, awq, mlx, 4bit dropped; ":" "-" "_" "@" and spaces ignored), so qwen2.5:14b-instruct-q8_0 and
//    Qwen/Qwen2.5-14B-Instruct-AWQ both reach qwen/qwen-2.5-14b-instruct.
// 3. Ollama only: its library names leave out "-instruct" / "-it" for the chat model (llama3.1:8b is the instruct
//    model), so for Ollama the role tag is optional on both sides, unless the tag says it is a base model ("text").
//    A Hugging Face name without "-instruct" is the base model, so for the other servers the role tag must match.
// 4. Size check: when an Ollama name carries no size (mistral-nemo, qwen3-coder:latest), its parameter count must be
//    within 0.5x-1.6x of the catalog's (qwen3-coder:latest is 30B and never becomes the 480B qwen/qwen3-coder).
// 5. Ollama's default tags: `ollama pull llama3.1` gives llama3.1:latest, which names no size. Its parameter count
//    picks the catalog size within ±25% (8.0B -> llama-3.1-8b, 20.9B -> gpt-oss-20b), and only when exactly one
//    catalog size fits (qwen2.5:latest is 7.6B: neither the 14B nor the 72B, so it stays unmatched).
//
// A name that reaches two catalog ids is not matched. Unmatched models are reported "not in the catalog" and never
// served. Several local models for one catalog id: the loaded one wins, then an exact match, then the shorter name.
import type { DetectedModel } from './connector.js';
import type { LocalModel, ServerKind } from './detect.js';

export interface CatalogEntry {
  id: string;
  aliases: string[];
  contextLength?: number;
}

export interface ServedModel {
  catalogId: string;
  local: string;
  server: ServerKind;
  baseUrl: string;
  quantization?: string;
  contextLength?: number;
}

export interface ModelMapping {
  /** One local model per catalog id: what the node declares and serves. */
  served: ServedModel[];
  /** Every local model (Ollama cloud models left out), with its catalog id or null. For the hello. */
  detected: DetectedModel[];
  /** Plain-English notes on models that were left out, for status and doctor. */
  notes: string[];
}

type Json = Record<string, unknown>;
const isObj = (v: unknown): v is Json => v !== null && typeof v === 'object' && !Array.isArray(v);

/** The servable catalog rows of a GET /v1/models body (model auto rows and reference-only rows left out). */
export function parseCatalog(body: unknown): CatalogEntry[] {
  if (!isObj(body) || !Array.isArray(body.data)) throw new Error('the market returned an unexpected model list');
  const out: CatalogEntry[] = [];
  for (const r of body.data) {
    if (!isObj(r) || typeof r.id !== 'string' || r.id.trim() === '') continue;
    if (r.auto !== undefined || r.servable === false) continue;
    const aliases = Array.isArray(r.aliases) ? r.aliases.filter((a): a is string => typeof a === 'string' && a.trim() !== '') : [];
    const e: CatalogEntry = { id: r.id, aliases };
    if (typeof r.context_length === 'number' && Number.isSafeInteger(r.context_length) && r.context_length > 0) e.contextLength = r.context_length;
    out.push(e);
  }
  return out;
}

/** GET <api>/v1/models. Throws a plain-English error when the market cannot be reached. */
export async function fetchCatalog(apiUrl: string, fetchImpl: typeof fetch = fetch, timeoutMs = 10_000): Promise<CatalogEntry[]> {
  const url = `${apiUrl.replace(/\/+$/, '')}/v1/models`;
  let res: Response;
  try {
    res = await fetchImpl(url, { headers: { accept: 'application/json' }, signal: AbortSignal.timeout(timeoutMs) });
  } catch (e) {
    const err = e as { name?: string; message?: string; cause?: { code?: string } };
    const why = err?.name === 'TimeoutError' ? 'no answer within 10 s' : (err?.cause?.code ?? err?.message ?? String(e));
    throw new Error(`could not reach the market at ${apiUrl} (${why})`);
  }
  if (!res.ok) {
    await res.body?.cancel().catch(() => undefined);
    throw new Error(`the market at ${apiUrl} answered HTTP ${res.status} for its model list`);
  }
  return parseCatalog(await res.json());
}

const QUANT_TOKEN =
  /^(?:i?q\d(?:_[a-z0-9]+)*|f16|fp16|bf16|f32|fp32|fp8|fp4|nvfp4|mxfp4|int4|int8|awq|gptq|gguf|ggml|mlx|qat|exl2|dwq|\d+bit|\d+(?:\.\d+)?bpw|w\d+a\d+|unsloth|imatrix)$/;
const ROLE_TOKEN = /^(?:instruct|it|chat)$/;
const BASE_TOKEN = /^(?:text|base|pretrain|pt)$/;
const SIZE_TOKEN = /^(\d+(?:\.\d+)?)([bm])$/;
const EXPERTS_TOKEN = /^\d+e$/;

/** Name tokens: lowercase, org prefix and ":latest" dropped, format and quantisation tags dropped. */
export function nameTokens(raw: string): string[] {
  let s = raw.trim().toLowerCase();
  s = s.replace(/\.gguf$/, '').replace(/\.(?:i?q\d(?:_[a-z0-9]+)*|f16|bf16|fp16|f32|fp32)$/, ''); // file names: x.Q4_K_M.gguf
  s = s.replace(/^(?:registry\.ollama\.ai|hf\.co|huggingface\.co)\//, '');
  s = s.slice(s.lastIndexOf('/') + 1);
  s = s.replace(/:latest$/, '');
  return s
    .split(/[-:@\s]+/)
    .map((t) => t.trim())
    .filter((t) => t !== '' && !QUANT_TOKEN.test(t));
}

const join = (toks: string[]) => toks.join('').replace(/_/g, '');

/** The comparison keys of a name: `full`, and `noRole` (role tags dropped; null for a base model). */
export function matchKeys(raw: string): { full: string; noRole: string | null; size: number | null; sizeKnown: boolean } {
  const toks = nameTokens(raw);
  const isBase = toks.some((t) => BASE_TOKEN.test(t));
  let size: number | null = null;
  let sizeKnown = true;
  for (const t of toks) {
    if (EXPERTS_TOKEN.test(t)) sizeKnown = false; // 17b-128e: the size is the active count, not the total
    const m = SIZE_TOKEN.exec(t);
    if (m && size === null) size = m[2] === 'm' ? Number(m[1]) / 1000 : Number(m[1]);
  }
  return {
    full: join(toks),
    noRole: isBase ? null : join(toks.filter((t) => !ROLE_TOKEN.test(t))),
    size: sizeKnown ? size : null,
    sizeKnown: sizeKnown && size !== null,
  };
}

const OLLAMA_TAG = /^[^/\s]+:[^/\s]+$/;
const exactName = (raw: string) => raw.trim().toLowerCase().replace(/:latest$/, '');

interface CatalogIndex {
  exact: Map<string, string | null>;
  full: Map<string, string | null>;
  /** Keys of the catalog's Ollama tags (qwen2.5:14b): Ollama's naming, so only for models found on Ollama. A
   *  Hugging Face "Qwen2.5-14B" is the base model and must not reach them. */
  fullOllama: Map<string, string | null>;
  noRole: Map<string, string | null>;
  /** Without size tokens, for catalog entries that carry no size at all (Ollama "mistral-nemo:12b"). */
  sizeless: Map<string, string | null>;
  size: Map<string, number | null>;
  /** Name without its size and role tokens -> catalog id -> its size, for Ollama tags that name no size (step 5). */
  sized: Map<string, Map<string, number>>;
}

function put(map: Map<string, string | null>, key: string | null, id: string): void {
  if (!key) return;
  const prev = map.get(key);
  if (prev === undefined) map.set(key, id);
  else if (prev !== id) map.set(key, null); // ambiguous: never matched
}

function indexCatalog(catalog: CatalogEntry[]): CatalogIndex {
  const ix: CatalogIndex = { exact: new Map(), full: new Map(), fullOllama: new Map(), noRole: new Map(), sizeless: new Map(), size: new Map(), sized: new Map() };
  for (const e of catalog) {
    const names = [e.id, ...e.aliases];
    let size: number | null = null;
    let namesASize = false;
    for (const name of names) {
      put(ix.exact, exactName(name), e.id);
      const k = matchKeys(name);
      put(OLLAMA_TAG.test(name) ? ix.fullOllama : ix.full, k.full, e.id);
      put(ix.noRole, k.noRole, e.id);
      if (nameTokens(name).some((t) => SIZE_TOKEN.test(t) || EXPERTS_TOKEN.test(t))) namesASize = true;
      if (size === null && k.sizeKnown) size = k.size;
      if (k.noRole !== null && k.sizeKnown && k.size !== null) {
        const key = join(nameTokens(name).filter((t) => !SIZE_TOKEN.test(t) && !ROLE_TOKEN.test(t)));
        const ids = ix.sized.get(key) ?? new Map<string, number>();
        ids.set(e.id, k.size);
        ix.sized.set(key, ids);
      }
    }
    ix.size.set(e.id, size);
    if (!namesASize) for (const name of names) put(ix.sizeless, matchKeys(name).noRole, e.id);
  }
  return ix;
}

function sizeOk(local: LocalModel, catalogSize: number | null): boolean {
  if (catalogSize === null || local.parameterB === undefined) return true;
  return local.parameterB >= catalogSize * 0.5 && local.parameterB <= catalogSize * 1.6;
}

/** The catalog id for one local model, and whether it was an exact name match; null = not in the catalog. */
export function matchModel(local: LocalModel, catalog: CatalogEntry[] | CatalogIndex): { id: string; exact: boolean } | null {
  const ix = Array.isArray(catalog) ? indexCatalog(catalog) : catalog;
  const exact = ix.exact.get(exactName(local.name));
  if (exact) return { id: exact, exact: true };
  if (exact === null) return null;
  const k = matchKeys(local.name);
  const check = (id: string | null | undefined) => {
    if (!id) return null;
    if (!k.sizeKnown && !sizeOk(local, ix.size.get(id) ?? null)) return null;
    return { id, exact: false };
  };
  let byFull = ix.full.get(k.full);
  if (byFull === undefined && local.server === 'ollama') byFull = ix.fullOllama.get(k.full);
  if (byFull !== undefined) return check(byFull);
  if (local.server === 'ollama' && k.noRole) {
    const byRole = ix.noRole.get(k.noRole);
    if (byRole !== undefined) return check(byRole);
    // "mistral-nemo:12b" for a catalog entry that names no size: drop the size tag, then check the parameter count.
    const toks = nameTokens(local.name).filter((t) => !SIZE_TOKEN.test(t) && !ROLE_TOKEN.test(t));
    const bySizeless = ix.sizeless.get(join(toks));
    if (bySizeless) return { id: bySizeless, exact: false };
    // An Ollama tag with no size at all (llama3.1:latest): the parameter count picks the one catalog size that fits.
    const p = local.parameterB;
    if (p !== undefined && !nameTokens(local.name).some((t) => SIZE_TOKEN.test(t) || EXPERTS_TOKEN.test(t))) {
      const fits = [...(ix.sized.get(join(toks))?.entries() ?? [])].filter(([, size]) => Math.abs(p - size) <= size * 0.25);
      if (fits.length === 1) return { id: fits[0]![0], exact: false };
    }
  }
  return null;
}

/** Maps every local model; picks one local model per catalog id. */
export function mapModels(locals: LocalModel[], catalog: CatalogEntry[]): ModelMapping {
  const ix = indexCatalog(catalog);
  const notes: string[] = [];
  const detected: DetectedModel[] = [];
  const candidates = new Map<string, { m: LocalModel; exact: boolean }[]>();
  for (const m of locals) {
    if (m.remote) {
      notes.push(`${m.name}: an Ollama cloud model (it runs on Ollama's servers, not this machine), so it is not offered`);
      continue;
    }
    const hit = m.chat ? matchModel(m, ix) : null;
    const d: DetectedModel = { local: m.name, server: m.server, model: hit?.id ?? null };
    const ctx = m.contextLength ?? m.maxContextLength;
    if (ctx) d.contextLength = ctx;
    if (m.quantization) d.quantization = m.quantization;
    if (m.sizeBytes !== undefined) d.sizeBytes = m.sizeBytes;
    detected.push(d);
    if (!m.chat) {
      notes.push(`${m.name}: not a chat model, so it is not offered`);
      continue;
    }
    if (!hit) continue;
    const list = candidates.get(hit.id) ?? [];
    list.push({ m, exact: hit.exact });
    candidates.set(hit.id, list);
  }
  const served: ServedModel[] = [];
  for (const [catalogId, list] of candidates) {
    list.sort((a, b) => Number(b.m.loaded ?? false) - Number(a.m.loaded ?? false) || Number(b.exact) - Number(a.exact) || a.m.name.length - b.m.name.length);
    const pick = list[0]!.m;
    for (const other of list.slice(1)) notes.push(`${other.m.name}: also ${catalogId}; serving ${pick.name} for it`);
    const s: ServedModel = { catalogId, local: pick.name, server: pick.server, baseUrl: pick.baseUrl };
    if (pick.quantization) s.quantization = pick.quantization;
    if (pick.contextLength) s.contextLength = pick.contextLength;
    served.push(s);
    // The served copy first among the local models that map to this catalog id, so a reader that keeps one entry per
    // id (the Sell workspace) shows the one the market reaches, with its quantisation and context.
    const first = detected.findIndex((d) => d.model === catalogId);
    const at = detected.findIndex((d) => d.model === catalogId && d.local === pick.name && d.server === pick.server);
    if (first >= 0 && at > first) detected.splice(first, 0, ...detected.splice(at, 1));
  }
  served.sort((a, b) => a.catalogId.localeCompare(b.catalogId));
  return { served, detected, notes };
}
