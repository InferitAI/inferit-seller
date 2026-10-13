# syntax=docker/dockerfile:1.7
# inferit-seller in a container: sells the inference of a server on the host (Ollama, LM Studio, vLLM, llama.cpp).
#
#   docker run -d --restart unless-stopped --name inferit-seller -e INFERIT_TOKEN=icn_... \
#     --add-host=host.docker.internal:host-gateway ghcr.io/inferitai/inferit-seller
#
# The container dials out to the market and listens on no port. It finds the host's server through
# host.docker.internal. On Linux, where Ollama listens on 127.0.0.1 only, use the host's network instead and keep
# Ollama on loopback (it has no authentication):
#
#   docker run -d --restart unless-stopped --name inferit-seller -e INFERIT_TOKEN=icn_... \
#     --network host -e UPSTREAM_BASE_URL=http://127.0.0.1:11434/v1 ghcr.io/inferitai/inferit-seller
#
# The token is read from the environment at run time only; nothing secret is in the image.

# The build runs once on the runner's own platform: the output (dist/ and the production node_modules: fastify, ws,
# pino) is plain JavaScript, so both images copy it. npm ci under arm64 emulation stalls on GitHub's runners.
FROM --platform=$BUILDPLATFORM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts --no-audit --no-fund
COPY tsconfig.json tsconfig.build.json ./
COPY src ./src
RUN npm run build \
 && npm prune --omit=dev --ignore-scripts

FROM node:22-alpine AS runtime
ENV NODE_ENV=production \
    INFERIT_SELLER_DOCKER=1 \
    INFERIT_DETECT_HOSTS=host.docker.internal
WORKDIR /app
COPY --from=build /app/package.json ./package.json
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY LICENSE NOTICE README.md ./
USER node
# `status` reads the state file the running app keeps; the check fails while it is not connected to the market.
HEALTHCHECK --interval=60s --timeout=10s --start-period=120s --retries=3 \
  CMD node -e "try{const s=JSON.parse(require('fs').readFileSync(require('os').homedir()+'/.inferit-seller/state.json','utf8'));process.exit(s.phase==='live'?0:1)}catch{process.exit(1)}"
ENTRYPOINT ["node", "/app/dist/cli.js"]
CMD ["start"]
