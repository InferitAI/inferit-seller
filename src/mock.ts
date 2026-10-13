// Deterministic mock completions (MOCK=1). Pure functions: the same request always yields the same tokens,
// the same usage numbers and (with a fixed clock) the same bytes, so the market's tests can assert exact
// accounting and byte-faithful relaying.
import { createHash } from 'node:crypto';

export interface ChatMessage {
  role: string;
  content?: unknown;
}

export interface ChatRequest {
  model: string;
  messages: ChatMessage[];
  stream?: boolean;
  stream_options?: { include_usage?: boolean } & Record<string, unknown>;
  max_tokens?: number;
  max_completion_tokens?: number;
  [k: string]: unknown;
}

export interface MockUsage {
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  prompt_tokens_details?: { cached_tokens: number };
}

export interface MockCompletion {
  id: string;
  created: number;
  model: string;
  /** Completion tokens in order; joining them gives the full content. */
  tokens: string[];
  content: string;
  finishReason: 'stop' | 'length';
  usage: MockUsage;
}

const PREFIX = 'This is a deterministic mock completion from the Inferit seller node.';
const MAX_ECHO_WORDS = 12;

function textOf(content: unknown): string {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  return content
    .map((p) => (typeof p === 'string' ? p : p && typeof p === 'object' && typeof (p as { text?: unknown }).text === 'string' ? (p as { text: string }).text : ''))
    .join('');
}

/** Prompt tokens the mock reports: 4 per message plus ceil(chars / 4) of its text content. */
export function mockPromptTokens(messages: ChatMessage[]): number {
  let n = 0;
  for (const m of messages) n += 4 + Math.ceil(textOf(m.content).length / 4);
  return n;
}

/** Full (untruncated) reply tokens: the fixed prefix, then up to 12 words echoed from the last user message. */
export function mockReplyTokens(messages: ChatMessage[]): string[] {
  const lastUser = [...messages].reverse().find((m) => m.role === 'user');
  const echo = textOf(lastUser?.content).split(/\s+/).filter(Boolean).slice(0, MAX_ECHO_WORDS);
  const words = [...PREFIX.split(' '), 'You', 'said:', ...(echo.length > 0 ? echo : ['nothing.'])];
  return words.map((w, i) => (i === 0 ? w : ` ${w}`));
}

export function maxTokensOf(req: Pick<ChatRequest, 'max_tokens' | 'max_completion_tokens'>): number | undefined {
  const v = req.max_completion_tokens ?? req.max_tokens;
  return typeof v === 'number' && Number.isInteger(v) && v >= 0 ? v : undefined;
}

/** Builds the deterministic completion for a request. `cachedTokens` is reported as prompt cache hits (clamped). */
export function mockCompletion(req: ChatRequest, nowMs: number, cachedTokens = 0): MockCompletion {
  const all = mockReplyTokens(req.messages);
  const max = maxTokensOf(req);
  const tokens = max === undefined ? all : all.slice(0, max);
  const promptTokens = mockPromptTokens(req.messages);
  const usage: MockUsage = {
    prompt_tokens: promptTokens,
    completion_tokens: tokens.length,
    total_tokens: promptTokens + tokens.length,
  };
  if (cachedTokens > 0) usage.prompt_tokens_details = { cached_tokens: Math.min(Math.trunc(cachedTokens), promptTokens) };
  const digest = createHash('sha256')
    .update(JSON.stringify({ model: req.model, messages: req.messages, max }))
    .digest('hex')
    .slice(0, 24);
  return {
    id: `chatcmpl-mock-${digest}`,
    created: Math.floor(nowMs / 1000),
    model: req.model,
    tokens,
    content: tokens.join(''),
    finishReason: tokens.length < all.length ? 'length' : 'stop',
    usage,
  };
}

/** Non-streaming OpenAI chat.completion body. */
export function mockCompletionJson(c: MockCompletion): Record<string, unknown> {
  return {
    id: c.id,
    object: 'chat.completion',
    created: c.created,
    model: c.model,
    choices: [{ index: 0, message: { role: 'assistant', content: c.content }, finish_reason: c.finishReason }],
    usage: c.usage,
  };
}

function sse(obj: unknown): string {
  return `data: ${JSON.stringify(obj)}\n\n`;
}

/**
 * SSE events (each a complete `data: ...\n\n` string) for a streamed completion: a role chunk, one chunk per token,
 * a finish chunk, then, when `includeUsage`, a usage-only chunk (`choices: []`), then `data: [DONE]`.
 * As OpenAI does, every chunk carries `"usage": null` when usage was requested.
 */
export function mockStreamEvents(c: MockCompletion, includeUsage: boolean): string[] {
  const base = { id: c.id, object: 'chat.completion.chunk', created: c.created, model: c.model };
  const u = includeUsage ? { usage: null } : {};
  const events = [sse({ ...base, choices: [{ index: 0, delta: { role: 'assistant', content: '' }, finish_reason: null }], ...u })];
  for (const t of c.tokens) events.push(sse({ ...base, choices: [{ index: 0, delta: { content: t }, finish_reason: null }], ...u }));
  events.push(sse({ ...base, choices: [{ index: 0, delta: {}, finish_reason: c.finishReason }], ...u }));
  if (includeUsage) events.push(sse({ ...base, choices: [], usage: c.usage }));
  events.push('data: [DONE]\n\n');
  return events;
}

/** Small deterministic PRNG (mulberry32) for MOCK_FAIL_RATE. */
export function prng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
