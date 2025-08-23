FROM erlang:26-alpine

# build deps + python for metrics (erlport)
RUN apk add --no-cache \
      bash git build-base cmake perl curl \
      python3 py3-pip linux-headers numactl-dev openssl-dev pkgconf

# isolate python packages 
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir erlport paho-mqtt

WORKDIR /app
COPY . /app

# clean any host artifacts, ensure output dir exists
RUN rm -rf _build deps && mkdir -p /app/out

# build the release
RUN rebar3 release

# make configs/out visible to the release
RUN set -eux; rel="/app/_build/default/rel/ps_bench"; \
    ln -s /app/configs "$rel/configs"; \
    ln -s /app/out     "$rel/out"

# ports for epmd + distribution 
EXPOSE 4369 15000-15010

# clean ERL flags, set cookie + metrics dir default
ENV ERL_AFLAGS="-kernel inet_dist_listen_min 15000 inet_dist_listen_max 15010" \
    RELEASE_COOKIE="ps_bench_cookie" \
    METRICS_DIR="/app/out" \
    PATH="/opt/venv/bin:$PATH"

# run the release
COPY /entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]