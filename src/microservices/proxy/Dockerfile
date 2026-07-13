FROM nginx:stable-alpine  

RUN mkdir -p /etc/nginx/templates

COPY 05-derived-env-vars.envsh /docker-entrypoint.d/
COPY nginx.conf.template /etc/nginx/templates/cinemaabyss-proxy.conf.template

