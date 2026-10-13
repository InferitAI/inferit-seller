// Hosted inference APIs and their fingerprint headers: a mirror of apps/api/src/security/providers.ts (the API's
// provider-denylist test checks the two lists stay equal).
//
// - The node refuses an UPSTREAM_BASE_URL on a hosted provider's API (config.ts): an endpoint offer is the seller's own
//   server, and reselling a provider's API needs kind "upstream_key" on the market's resale allowlist. Advisory: a
//   seller can still put a proxy on its own domain.
// - In proxy mode the node forwards a provider's fingerprint headers (and only those), over HTTP and over the
//   connector, so the market's probe can hold an offer that is really a proxy in front of a provider for review.

export const PROVIDER_DOMAINS = [
  'openai.com', 'anthropic.com', 'together.xyz', 'together.ai', 'openrouter.ai', 'groq.com',
  'fireworks.ai', 'deepinfra.com', 'mistral.ai', 'generativelanguage.googleapis.com', 'aiplatform.googleapis.com', 'deepseek.com',
  'x.ai', 'perplexity.ai', 'cohere.ai', 'cohere.com', 'openai.azure.com', 'ai.azure.com',
  'hyperbolic.xyz', 'novita.ai', 'lambdalabs.com', 'lambda.ai', 'sambanova.ai', 'cerebras.ai',
  'nebius.ai', 'nebius.com', 'featherless.ai', 'anyscale.com', 'replicate.com', 'api-inference.huggingface.co',
  'router.huggingface.co', 'ai21.com', 'moonshot.ai', 'moonshot.cn', 'dashscope.aliyuncs.com', 'dashscope-intl.aliyuncs.com',
  'bigmodel.cn', 'siliconflow.cn', 'siliconflow.com', 'inference.net', 'kluster.ai', 'parasail.io',
  'chutes.ai', 'avian.io', 'nvidia.com', 'cognitiveservices.azure.com', 'api.cloudflare.com', 'gateway.ai.cloudflare.com',
  'z.ai', 'minimax.io', 'minimaxi.com', 'minimax.chat', 'volces.com', 'bytepluses.com',
  'baidubce.com', 'models.github.ai', 'friendli.ai', 'ai-gateway.vercel.sh', 'upstage.ai', 'stepfun.com',
  'lingyiwanwu.com', '01.ai', 'baseten.co', 'llama.com', 'inflection.ai', 'reka.ai',
  'writer.com', 'venice.ai', 'nscale.com', 'scaleway.ai', 'endpoints.kepler.ai.cloud.ovh.net', 'ml.cloud.ibm.com',
  'hunyuan.cloud.tencent.com', 'lkeap.cloud.tencent.com', 'xf-yun.com', 'sensenova.cn', 'baichuan-ai.com', 'ppinfra.com',
  'infini-ai.com', 'modelscope.cn',
] as const;

export const BEDROCK_RE = /^bedrock(?:-runtime)?(?:-fips)?\.[a-z0-9-]+\.amazonaws\.com$/;
export const VERTEX_RE = /^[a-z0-9-]+-aiplatform\.googleapis\.com$/;
export const DASHSCOPE_RE = /^(?:[a-z0-9-]+\.)*dashscope(?:-[a-z0-9]+)*\.aliyuncs\.com$/;

/** The provider domain `url` belongs to, or null when it is not a known hosted inference API. */
export function knownProviderOf(url: string): string | null {
  let host: string;
  try {
    host = new URL(url).hostname.toLowerCase().replace(/\.+$/, '');
  } catch {
    return null;
  }
  if (BEDROCK_RE.test(host)) return 'amazonaws.com (Bedrock)';
  if (VERTEX_RE.test(host)) return 'aiplatform.googleapis.com';
  if (DASHSCOPE_RE.test(host)) return 'dashscope.aliyuncs.com';
  for (const d of PROVIDER_DOMAINS) if (host === d || host.endsWith(`.${d}`)) return d;
  return null;
}

export const FINGERPRINT_HEADERS = new Set([
  'openai-organization', 'openai-project', 'openai-processing-ms', 'openai-version',
  'x-ratelimit-limit-requests', 'x-ratelimit-limit-tokens', 'anthropic-organization-id', 'x-groq-region',
  'azureml-model-session', 'x-ms-rai-invoked', 'x-ms-deployment-name',
]);
export const FINGERPRINT_PREFIXES = ['anthropic-ratelimit-', 'cf-aig-'];
export const FORWARDED_PREFIX = 'llm_provider-';

/** Whether a response header name is a hosted provider's fingerprint. */
export function isFingerprintHeader(raw: string): boolean {
  const lower = raw.toLowerCase();
  const name = lower.startsWith(FORWARDED_PREFIX) ? lower.slice(FORWARDED_PREFIX.length) : lower;
  return FINGERPRINT_HEADERS.has(name) || FINGERPRINT_PREFIXES.some((p) => name.startsWith(p));
}

/** The fingerprint headers among `headers` (lowercased names, string values), at most 16. */
export function fingerprintOf(headers: Iterable<[string, string | string[] | undefined]>): Record<string, string> {
  const out: Record<string, string> = {};
  let n = 0;
  for (const [k, v] of headers) {
    if (v === undefined || !isFingerprintHeader(k)) continue;
    out[k.toLowerCase()] = (Array.isArray(v) ? v.join(', ') : v).slice(0, 512);
    if (++n >= 16) break;
  }
  return out;
}
