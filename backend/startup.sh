#!/bin/bash

# Collect all static files into STATIC_ROOT
python3 manage.py collectstatic -v0 --noinput

# Start services
supervisord -c supervisord.conf
