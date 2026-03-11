#!/bin/bash

cd /opt/archery

echo Switch Python runtime environment
source /opt/venv4archery/bin/activate
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

echo Start nginx
/usr/sbin/nginx

echo Collect all static files into STATIC_ROOT
python3 manage.py collectstatic -v0 --noinput

echo Start Django Q cluster
supervisord -c /etc/supervisord.conf

echo Start services
gunicorn -w 4 -b 127.0.0.1:8888 --timeout 600 archery.wsgi:application






