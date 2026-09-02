# syntax=docker/dockerfile:1.4@sha256:9ba7531bd80fb0a858632727cf7a112fbfd19b17e94c4e84ced81e24ef1a0dbc
# Multi-stage build for the Symba operator console. Stage 1 builds the
# Vite bundle; stage 2 serves it with nginx. The engine image stays API-only — this
# is a wholly separate deployable.

# ===== build =====
FROM node:22-alpine@sha256:c610fcdfb1d5b4740dd70c284ed3cb16bb857e0f7166196e36a5501df7a3aa32 AS build
WORKDIR /app

# Install deps first (layer-cached until package.json/lock changes).
COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts

# Generate the typed client from the committed schema, then build. If openapi.json
# drifted, CI's openapi-check gate already failed upstream — this just consumes it.
COPY . .
RUN npm run gen:api && npm run build

# ===== serve =====
FROM nginx:1.27-alpine@sha256:65645c7bb6a0661892a8b03b89d0743208a18dd2f3f17a54ef4b76fb8e2f2a10 AS serve
ENV ENGINE_HOST=symba-engine
ENV ENGINE_PORT=7300
ENV NGINX_LISTEN_PORT=8080

COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/templates/default.conf.template
COPY --chmod=0555 release-entrypoint.sh /usr/local/bin/symba-frontend-entrypoint

# The stock entrypoint still performs envsubst, but it does not need root when
# its output, cache, pid, and temporary directories are owned by nginx. The
# production overlay replaces those paths with UID-101-owned bounded tmpfs.
RUN rm -f /etc/nginx/conf.d/default.conf && \
    chown -R 101:101 /etc/nginx/conf.d /var/cache/nginx /run

# Tie the separately built operator UI to the exact coordinated release
# bundle. Production Compose requires these values; local builds use defaults.
ARG KNOR_RELEASE_ID=development
ARG KNOR_RELEASE_MANIFEST_SHA256=unreleased
RUN mkdir -p /usr/local/share/symba && \
    printf '%s\n' "$KNOR_RELEASE_ID" > /usr/local/share/symba/release-id && \
    printf '%s\n' "$KNOR_RELEASE_MANIFEST_SHA256" > /usr/local/share/symba/release-manifest-sha256 && \
    printf '{"release_id":"%s","release_manifest_sha256":"%s"}\n' \
        "$KNOR_RELEASE_ID" "$KNOR_RELEASE_MANIFEST_SHA256" \
        > /usr/share/nginx/html/release-provenance.json && \
    chmod 0444 \
        /usr/local/share/symba/release-id \
        /usr/local/share/symba/release-manifest-sha256 \
        /usr/share/nginx/html/release-provenance.json
LABEL org.opencontainers.image.version="${KNOR_RELEASE_ID}" \
    org.opencontainers.image.revision="${KNOR_RELEASE_MANIFEST_SHA256}" \
    com.amplior.release.id="${KNOR_RELEASE_ID}" \
    com.amplior.release.manifest-sha256="${KNOR_RELEASE_MANIFEST_SHA256}"
ENV KNOR_IMAGE_RELEASE_ID="${KNOR_RELEASE_ID}" \
    KNOR_IMAGE_RELEASE_MANIFEST_SHA256="${KNOR_RELEASE_MANIFEST_SHA256}"

# nginx:alpine runs /docker-entrypoint.d/20-envsubst-on-templates.sh, which renders
# templates/*.template with the ENGINE_* env vars into conf.d before startup.
USER 101:101
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/symba-frontend-entrypoint"]
CMD ["nginx", "-g", "daemon off;"]
