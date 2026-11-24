# The console image: the static build of web/ served by unprivileged nginx,
# which also forwards the gateway paths (deploy/console.nginx.conf).
#
#   docker build -f deploy/console.Dockerfile --build-context deploy=deploy \
#     -t threadroute-console web
#
# The build context is web/ (see web/.dockerignore). The nginx files come
# from the named context "deploy", so nothing outside web/ is sent as the
# main context.

# Node and pnpm as web.yml (NODE_VERSION) and web/package.json
# (packageManager) name them. Change them together.
FROM node:24.11.1-alpine AS build
ENV COREPACK_ENABLE_DOWNLOAD_PROMPT=0
RUN corepack enable && corepack prepare pnpm@10.4.1 --activate
WORKDIR /web

# Dependencies change less often than the code, so they get their own layer.
COPY package.json pnpm-lock.yaml .npmrc ./
RUN --mount=type=cache,target=/root/.local/share/pnpm/store \
    pnpm install --frozen-lockfile

COPY . .
# The localStorage key of the site token (web/README.md). Empty: the
# console never sends a token.
ARG VITE_SITE_TOKEN_KEY=""
ENV VITE_SITE_TOKEN_KEY=${VITE_SITE_TOKEN_KEY}
RUN pnpm build

# The unprivileged image runs as uid 101, listens on 8080 and keeps its pid
# file and temporary files in /tmp, so the root filesystem can be read-only.
FROM nginxinc/nginx-unprivileged:1.30.6-alpine
COPY --from=deploy console.nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=deploy console.security-headers.conf /etc/nginx/snippets/security-headers.conf
COPY --from=build /web/dist/ /usr/share/nginx/html/
EXPOSE 8080
