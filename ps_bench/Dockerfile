# ---- builder/runtime combined (simple) ----
FROM erlang:24-alpine

# System deps needed by Erlang deps and Python side
# - build-base: gcc, g++, make, libc-dev
# - openssl-dev: common Erlang TLS dep
# - bash, git: quality-of-life + rebar fetch
# - curl: needed for downloading dependencies
# - cmake: needed for building quicer/msquic
# - python3, py3-pip: for window_engine + plugins
RUN apk add --no-cache \
      bash git build-base openssl-dev curl cmake \
      python3 py3-pip

# Create and use a virtual environment for Python packages
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python packages in the virtual environment
RUN pip install --no-cache-dir erlport paho-mqtt

# App
WORKDIR /app
COPY . /app

# Clean any existing build artifacts from the host
RUN rm -rf _build deps

# Optional: ensure the output dir exists in the image
RUN mkdir -p /app/out

# Build release
RUN rebar3 release

# Expose Erlang dist ports to other *containers* (no host publish here)
EXPOSE 4369 15000-15010

# Environment for node name/cookie and metrics dir
# Keep the PATH to include the virtual environment
ENV ERL_AFLAGS="-sname runner1 -setcookie ps_bench_cookie -kernel inet_dist_listen_min 15000 inet_dist_listen_max 15010" \
    METRICS_DIR="/app/out" \
    PATH="/opt/venv/bin:$PATH"

# Start the release
CMD sh -lc '_build/default/rel/ps_bench/bin/ps_bench foreground -config /app/configs/ps_bench.config'