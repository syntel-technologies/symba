# Multi-stage build for the Symba operator console. Stage 1 builds the
# Vite bundle; stage 2 serves it with nginx. The engine image stays API-only — this
# is a wholly separate deployable.

# ===== build =====
FROM node:22-alpine AS build
WORKDIR /app

# Install deps first (layer-cached until package.json/lock changes).
COPY package.json ./
RUN npm install

# Generate the typed client from the committed schema, then build. If openapi.json
# drifted, CI's openapi-check gate already failed upstream — this just consumes it.
COPY . .
RUN npm run gen:api && npm run build

# ===== serve =====
FROM nginx:1.27-alpine AS serve
ENV ENGINE_HOST=symba-engine
ENV ENGINE_PORT=7300

COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/templates/default.conf.template

# nginx:alpine runs /docker-entrypoint.d/20-envsubst-on-templates.sh, which renders
# templates/*.template with the ENGINE_* env vars into conf.d before startup.
EXPOSE 80
