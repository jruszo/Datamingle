#!/bin/bash

set -eo pipefail

cd /opt/datamingle/backend

echo Switch Python runtime environment
source /opt/venv4datamingle/bin/activate
#pip install -r requirements.txt -i https://mirrors.ustc.edu.cn/pypi/web/simple/

echo Update redirect port
if [[ -z $NGINX_PORT ]]; then
    sed -i "s/:nginx_port//g" /etc/nginx/nginx.conf
else
    sed -i "s/nginx_port/$NGINX_PORT/g" /etc/nginx/nginx.conf
fi

if [[ "${RUN_MIGRATIONS_ON_START:-0}" == "1" ]]; then
    echo Generate Django migrations
    python3 manage.py makemigrations

    echo Apply Django migrations
    python3 manage.py migrate --noinput
fi

if [[ "${RUN_LOCAL_DEMO_SEED:-0}" == "1" ]]; then
    echo Seed local demo environment
    python3 manage.py seed_local_demo
fi

echo Start nginx
/usr/sbin/nginx

echo Collect all static files into STATIC_ROOT
python3 manage.py collectstatic -v0 --noinput

echo Start services
GUNICORN_RELOAD_ARGS=""
if [[ "${GUNICORN_RELOAD:-0}" == "1" ]]; then
    GUNICORN_RELOAD_ARGS="--reload"
fi
if [[ "${DATAMINGLE_SERVE_ASGI:-1}" == "1" ]]; then
    daphne -b 127.0.0.1 -p 8888 --proxy-headers archery.asgi:application
else
    gunicorn -w 4 -b 127.0.0.1:8888 --timeout 600 ${GUNICORN_RELOAD_ARGS} archery.wsgi:application
fi
