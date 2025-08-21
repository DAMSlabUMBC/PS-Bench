# ---- builder/runtime combined (simple) ----
FROM erlang:24-alpine

RUN apk add --no-cache \
      bash git build-base cmake perl curl \
      python3 py3-pip linux-headers numactl-dev openssl-dev pkgconf

RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir erlport paho-mqtt

WORKDIR /app
COPY . /app
RUN rm -rf _build deps
RUN mkdir -p /app/out

# Build the release
RUN rebar3 release

# Make relative paths in ps_bench.config work inside the release
RUN set -eux; rel="/app/_build/default/rel/ps_bench"; \
    ln -s /app/configs "$rel/configs"; \
    ln -s /app/out     "$rel/out"

EXPOSE 4369 15000-15010

# Clean ERL flags (avoid duplicate -sname warning)
ENV ERL_AFLAGS="-kernel inet_dist_listen_min 15000 inet_dist_listen_max 15010" \
    RELEASE_COOKIE="ps_bench_cookie" \
    METRICS_DIR="/app/out" \
    PATH="/opt/venv/bin:$PATH"

CMD sh -lc '_build/default/rel/ps_bench/bin/ps_bench foreground'
