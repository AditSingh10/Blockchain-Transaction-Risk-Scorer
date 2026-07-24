FROM node:20.18.1-alpine3.21 AS build

WORKDIR /app
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ .
ARG REACT_APP_WS_URL=ws://localhost:8000/api/v1/ws
ARG REACT_APP_API_URL=http://localhost:8000
ENV REACT_APP_WS_URL=${REACT_APP_WS_URL}
ENV REACT_APP_API_URL=${REACT_APP_API_URL}
RUN npm run build

FROM nginx:1.27.3-alpine3.20
COPY infra/docker/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/build /usr/share/nginx/html
RUN mkdir -p /var/cache/nginx/client_temp \
    && touch /var/run/nginx.pid \
    && chown -R nginx:nginx /var/cache/nginx /var/log/nginx /usr/share/nginx/html \
    && chown nginx:nginx /var/run/nginx.pid
EXPOSE 8080
USER nginx
