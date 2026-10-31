FROM node:20.19-alpine AS build
WORKDIR /src/web/admin
COPY web/admin/package.json web/admin/package-lock.json ./
RUN npm ci
COPY web/admin/index.html web/admin/tsconfig.json web/admin/vite.config.ts ./
COPY web/admin/src ./src
COPY web/admin/tests ./tests
RUN npm run typecheck && npm test && npm run build

FROM nginxinc/nginx-unprivileged:1.27-alpine
COPY --from=build /src/web/admin/dist/ /usr/share/nginx/html/
COPY deploy/production/admin-nginx.conf /etc/nginx/conf.d/default.conf
