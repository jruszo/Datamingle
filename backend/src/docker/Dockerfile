# Datamingle intentionally inherits the pinned upstream Archery base image while
# the Docker build migrates /opt/venv4a* to /opt/venv4datamingle below.
ARG BASE_IMAGE="hhyo/archery-base:sha-d8159f4"
FROM ${BASE_IMAGE}
SHELL ["/bin/bash", "-c"]
COPY . /opt/datamingle/

WORKDIR /opt/datamingle/backend

RUN useradd nginx
# Datamingle
RUN apt-get update \
    && apt-get install -yq --no-install-recommends nginx mariadb-client \
    && if [ ! -d /opt/venv4datamingle ]; then for old_venv in /opt/venv4a*; do [ -d "$old_venv" ] && mv "$old_venv" /opt/venv4datamingle && break; done; fi \
    && if [ ! -d /opt/venv4datamingle ]; then echo "ERROR: No Python virtualenv found at /opt/venv4datamingle or /opt/venv4a*" >&2; exit 1; fi \
    && source /opt/venv4datamingle/bin/activate \
    && pip install -r /opt/datamingle/backend/requirements.txt \
    && cp -f /opt/datamingle/backend/src/docker/nginx.conf /etc/nginx/ \
    && cp -f /opt/datamingle/backend/supervisord.conf /etc/ \
    && apt-get -yq remove gcc curl \
    && apt-get clean \
    && rm -rf /var/cache/apt/* \
    && rm -rf /root/.cache

#port
EXPOSE 9123

#start service
ENTRYPOINT ["bash", "/opt/datamingle/backend/src/docker/startup.sh"]
